"""模型仓库：内存 + 磁盘双缓存。

原 TrainingLoop 轮询训练已并入 analysis_engine._get_or_train_forecaster，
此处只保留被 engine 复用的 ModelStore（AR 模型已移除）。
"""

from __future__ import annotations

import logging
import re
import threading
from pathlib import Path
from typing import Any, Callable

from project import DEFAULT_PROJECT, normalize_project, scoped_key

logger = logging.getLogger(__name__)


def _version_of(key: str) -> int | None:
    """从 key 尾部取配置版本；无 @v 后缀（legacy 未带版本）返回 None。"""
    m = re.search(r"@v(\d+)$", key)
    return int(m.group(1)) if m else None


def _belongs_to_scoped(key: str, project_id: str, task_id: str) -> bool:
    """key 是否属于该 (project, task)：预测 {scoped}@v{ver} / 异常 {scoped}:{method}@v{ver}。

    default 项目额外兼容清理 legacy 无 project 前缀的旧 key（{task_id}@v{ver} 等），
    保证旧版本升级后仍能清掉历史模型文件。
    """
    scoped = scoped_key(project_id, task_id)
    if (key == scoped
            or key.startswith(f"{scoped}@v")
            or key.startswith(f"{scoped}:")):
        return True
    if normalize_project(project_id) == DEFAULT_PROJECT:
        return (key == task_id
                or key.startswith(f"{task_id}@v")
                or key.startswith(f"{task_id}:"))
    return False


class ModelStore:
    """模型仓库：内存 + 磁盘双缓存。

    - save: 内存存一份 + 写 models/{key}.pt（含模型 + 标准化参数）；
    - get: 内存命中直接返回；未命中且磁盘有则加载；
    - invalidate: 删内存 + 磁盘文件（任务版本变化/删除时调用）。
    """

    def __init__(self, model_dir: str | Path | None = None):
        self._models: dict[str, object] = {}
        self._lock = threading.RLock()   # 内存缓存锁：save/get/invalidate 并发安全
        self._model_dir = Path(model_dir) if model_dir else Path("models")

    def _path(self, key: str) -> Path:
        return self._model_dir / f"{key}.pt"

    def save(self, key: str, model: Any) -> None:
        # 先写内存（立刻可用），再原子写磁盘（tmp + rename，避免读到半截文件）
        with self._lock:
            self._models[key] = model
        self._model_dir.mkdir(parents=True, exist_ok=True)
        p = self._path(key)
        if hasattr(model, "save"):
            tmp = p.with_name(p.name + ".tmp")
            try:
                model.save(tmp)
                tmp.replace(p)          # rename 原子替换，旧模型文件被覆盖
            except Exception:
                if tmp.exists():
                    tmp.unlink()        # 写失败清掉临时文件，不留半截
                raise
        logger.info("[ModelStore] save %s（内存 + %s）", key, p)

    def get(self, key: str) -> Any:
        with self._lock:
            if key in self._models:
                return self._models[key]
        p = self._path(key)
        loader = getattr(self, "_loader", None)
        if p.exists() and loader is not None:
            model = loader(key, p)          # 磁盘加载放锁外（避免持锁做 IO）
            with self._lock:
                self._models[key] = model
            return model
        return None

    def is_ready(self, key: str) -> bool:
        with self._lock:
            if key in self._models:
                return True
        return self._path(key).exists()

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._models.pop(key, None)
        p = self._path(key)
        if p.exists():
            p.unlink()
        logger.info("[ModelStore] invalidate %s", key)

    def invalidate_task(self, project_id: str, task_id: str,
                        keep_version: int | None = None) -> None:
        """按 (project, task) 清理版本化模型，保留最近 2 个版本（keep_version 和 keep_version-1）。

        语义：keep_version=None 全删（任务删除）；keep_version=N 保留 {N, N-1}
        （发布回滚到上一个配置可秒级复用旧模型，磁盘有界）。
        legacy 无版本 key 一律视为待清理（default 项目兼容旧前缀，见 _belongs_to_scoped）。
        """
        keep = {keep_version, keep_version - 1} if keep_version is not None else set()
        stale = []
        with self._lock:
            for key in list(self._models):
                if not _belongs_to_scoped(key, project_id, task_id):
                    continue
                v = _version_of(key)
                if keep_version is not None and v is not None and v in keep:
                    continue
                stale.append(key)
            for key in stale:
                self._models.pop(key, None)
        # 磁盘文件：内存之外还可能有历史遗留（重启前的旧版本）
        if self._model_dir.exists():
            for p in self._model_dir.iterdir():
                if p.suffix != ".pt":
                    continue
                if not _belongs_to_scoped(p.stem, project_id, task_id):
                    continue
                v = _version_of(p.stem)
                if keep_version is not None and v is not None and v in keep:
                    continue
                try:
                    p.unlink()
                except OSError:
                    pass
        if stale:
            logger.info("[ModelStore] invalidate_task %s::%s 清理 %d 个旧版本（保留 %s）",
                        project_id, task_id, len(stale),
                        sorted(keep) if keep else "全部")

    # ---- 具体模型类型的加载方式（由上层注入）----

    def set_loader(self, loader: Callable[[str, Path], Any]) -> None:
        """loader(key, path) -> model：磁盘缓存加载函数，让 store 不依赖具体模型类。"""
        self._loader = loader
