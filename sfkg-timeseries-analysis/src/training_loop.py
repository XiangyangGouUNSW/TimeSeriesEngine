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
    """从 key 尾部取版本号（@v{ver}，后面可能跟 :k{knowledge_version} 后缀）。

    版本号是 int64（config_version 或 task_timestamp_ms）；无 @v 后缀
    （legacy 未带版本）返回 None。`:k` 后缀是知识版本片段（字符集 [A-Za-z0-9_.-]，
    不含 @/:），锚定到行尾时需把它一并吃掉，否则带知识版本的 key 解析失败。
    """
    m = re.search(r"@v(\d+)(?::k[A-Za-z0-9_.\-]*)?$", key)
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
            try:
                model = loader(key, p)      # 磁盘加载放锁外（避免持锁做 IO）
            except Exception:
                # 缓存损坏 / 格式不符（torch.load 失败、缺 key、model_type 未知）→
                # 删掉坏文件、返回 None，上层据此重训重建；不因一个坏 .pt 让整轮崩溃。
                logger.exception("[ModelStore] 加载 %s 失败，删除损坏缓存并重建", key)
                try:
                    p.unlink()
                except OSError:
                    pass
                return None
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
        """按 (project, task) 清理版本化模型，保留 keep_version 及它之前的最近 1 个版本。

        语义：keep_version=None 全删（任务删除）；keep_version=N 保留「≤N 的版本里最大的
        2 个」（N 是新版本 → 实际就是 {当前 N, 上一个版本}，回滚复用且磁盘有界）。
        版本号用数值大小比（时间戳越大越新、config_version 越大越新，二者同构），
        不再用旧的 {N, N-1} 整数相邻假设（时间戳下 N-1 是 1ms 前、无意义）。
        legacy 无版本 key 一律视为待清理（default 项目兼容旧前缀，见 _belongs_to_scoped）。
        """
        # 收集该 (project, task) 的所有版本化 key（内存 + 磁盘），统一算保留集。
        with self._lock:
            mem_keys = [k for k in list(self._models)
                        if _belongs_to_scoped(k, project_id, task_id)]
        disk_stems: list[str] = []
        if self._model_dir.exists():
            disk_stems = [p.stem for p in self._model_dir.iterdir()
                          if p.suffix == ".pt"
                          and _belongs_to_scoped(p.stem, project_id, task_id)]
        all_keys = set(mem_keys) | set(disk_stems)

        keep: set[int] = set()
        if keep_version is not None:
            versions = sorted({v for k in all_keys
                               if (v := _version_of(k)) is not None
                               and v <= keep_version},
                              reverse=True)
            keep = set(versions[:2])

        stale_mem = [k for k in mem_keys if _version_of(k) not in keep]
        if stale_mem:
            with self._lock:
                for k in stale_mem:
                    self._models.pop(k, None)

        deleted_disk = 0
        for stem in disk_stems:
            if _version_of(stem) in keep:
                continue
            try:
                (self._model_dir / f"{stem}.pt").unlink()
                deleted_disk += 1
            except OSError:
                pass

        total_deleted = len(stale_mem) + deleted_disk
        if total_deleted:
            logger.info("[ModelStore] invalidate_task %s::%s 清理 %d 个旧版本（保留 %s）",
                        project_id, task_id, total_deleted,
                        sorted(keep, reverse=True) if keep else "全部")

    # ---- 具体模型类型的加载方式（由上层注入）----

    def set_loader(self, loader: Callable[[str, Path], Any]) -> None:
        """loader(key, path) -> model：磁盘缓存加载函数，让 store 不依赖具体模型类。"""
        self._loader = loader
