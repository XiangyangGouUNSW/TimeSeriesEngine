"""训练循环：轮询数据规模 → 达标就拉历史 → 训练模型 → 存进模型仓库。

对应需求：
  1. 通过轮询调用检查 C 端的数据规模；
  2. 规模达标后调用 C 端拉历史数据；
  3. 训练模型（自回归 AR 或 PatchTST），训练好存起来供推理复用。
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from core_client import CoreDataClient
from ar_model import AutoregressiveModel
from patchtst_forecaster import PatchTSTForecaster

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
        if hasattr(model, "save"):
            p = self._path(key)
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


class TrainingLoop:
    def __init__(self, client: CoreDataClient, config: dict):
        self.client = client
        self.cfg = config
        self.store = ModelStore()

    def poll_and_train(self) -> bool:
        """轮询数据规模，够了就训练。返回是否训练成功。"""
        cfg = self.cfg
        target = cfg["data"]["target_sequence"]
        max_attempts = cfg["training"]["max_poll_attempts"]
        interval = cfg["training"]["poll_interval_seconds"]

        for attempt in range(1, max_attempts + 1):
            print(f"\n[{attempt}/{max_attempts}] 轮询数据规模……")
            scales = self.client.get_sequence_data_scale(self.all_sequence_ids())
            for s in scales:
                print(f"    {s.sequence_id}: {s.point_count} 个点")
            min_count = min(s.point_count for s in scales)

            if min_count >= cfg["training"]["min_train_points"]:
                print(f"  ✓ 数据规模达标（最少 {min_count} 点 >= "
                      f"{cfg['training']['min_train_points']}），开始训练")
                self._train(target)
                return True

            print(f"  ✗ 数据还不够（最少 {min_count} < "
                  f"{cfg['training']['min_train_points']}），"
                  f"{interval} 秒后再试")
            time.sleep(interval)

        print("轮询结束，数据始终不够，放弃训练")
        return False

    def all_sequence_ids(self) -> list[str]:
        """目标序列 + 特征序列，全都要有数据才训练。"""
        return ([self.cfg["data"]["target_sequence"]]
                + self.cfg["data"]["feature_sequences"])

    def _train(self, target: str) -> None:
        """拉历史数据（前 train_ratio 段）→ 训练目标序列的 AR 模型。"""
        cfg = self.cfg

        # 1. 拉历史：用前 train_ratio 的数据训练，留后面做推理对比
        scales = {s.sequence_id: s for s in
                  self.client.get_sequence_data_scale(self.all_sequence_ids())}
        start_ms = scales[target].start_time_ms
        end_ms = scales[target].end_time_ms
        cut_ms = start_ms + int((end_ms - start_ms) * cfg["training"]["train_ratio"])
        chunk = self.client.get_history(self.all_sequence_ids(), end_time_ms=cut_ms)
        print(f"  拉到历史数据：{len(chunk.timestamps_ms)} 个时间点，"
              f"列顺序 {chunk.sequence_ids}")

        # 2. 取出目标序列那一列，训练 AR 模型
        col = chunk.sequence_ids.index(target)
        series = [row[col] for row in chunk.values]
        model = AutoregressiveModel(order=cfg["training"]["ar_order"])
        model.fit(series)

        # 3. 存进模型仓库
        self.store.save(target, model)
        print(f"  ✓ 训练完成：{target} 的 AR(order={cfg['training']['ar_order']}) "
              f"模型已就绪")
