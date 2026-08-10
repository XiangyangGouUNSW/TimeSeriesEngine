"""模型仓库：内存 + 磁盘双缓存。

原 TrainingLoop 轮询训练已并入 analysis_engine._get_or_train_forecaster，
此处只保留被 engine 复用的 ModelStore（AR 模型已移除）。
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


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

    def save(self, key: str, model) -> None:
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

    def get(self, key: str):
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

    # ---- 具体模型类型的加载方式（由上层注入）----

    def set_loader(self, loader) -> None:
        """loader(key, path) -> model：磁盘缓存加载函数，让 store 不依赖具体模型类。"""
        self._loader = loader
