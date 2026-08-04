"""训练循环：轮询数据规模 → 达标就拉历史 → 训练 AR 模型 → 存进模型仓库。

对应需求：
  1. 通过轮询调用检查 C 端的数据规模；
  2. 规模达标后调用 C 端拉历史数据；
  3. 训练一个小模型（这里用自回归 AR）。
"""

from __future__ import annotations

import time

from core_client import CoreDataClient
from ar_model import AutoregressiveModel


class ModelStore:
    """最简单的模型仓库：训练好的模型放这，推理时取。"""

    def __init__(self):
        self._models: dict[str, AutoregressiveModel] = {}

    def save(self, sequence_id: str, model: AutoregressiveModel) -> None:
        self._models[sequence_id] = model

    def get(self, sequence_id: str) -> AutoregressiveModel | None:
        return self._models.get(sequence_id)

    def is_ready(self, sequence_id: str) -> bool:
        return sequence_id in self._models


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
