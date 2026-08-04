"""推理服务：取最近窗口 → 用训练好的模型 → 输出未来预测。

对应需求：训练好的模型可以轮询访问进行 infer，给一个输出。
现在 C 端是空壳没有实时数据，推理就用同一份 ETT 数据当输入，
目的是证明整条链路通了、有输出。
"""

from __future__ import annotations

from datetime import datetime, timezone

from core_client import CoreDataClient
from data_types import ForecastOutput
from training_loop import ModelStore


class InferenceService:
    def __init__(self, client: CoreDataClient, model_store: ModelStore, config: dict):
        self.client = client
        self.store = model_store
        self.cfg = config

    def run_inference(self) -> ForecastOutput | None:
        cfg = self.cfg
        target = cfg["data"]["target_sequence"]
        model = self.store.get(target)
        if model is None:
            print("模型还没训练好，不能推理")
            return None

        # 1. 取最近一个窗口（这就是以后 C 端给实时数据的动作）
        #    演示时可以通过 pretend_today_ratio 假装"现在是"某时间点，
        #    好让后面还有真实值可以对比预测准不准。
        window_size = cfg["inference"]["window_size"]
        ratio = cfg["inference"].get("pretend_today_ratio")
        end_time_ms = None
        if ratio is not None:
            scales = {s.sequence_id: s for s in
                      self.client.get_sequence_data_scale([target])}
            s0 = scales[target].start_time_ms
            s1 = scales[target].end_time_ms
            end_time_ms = s0 + int((s1 - s0) * ratio)
        window = self.client.get_aligned_window(
            [target], window_size, end_time_ms=end_time_ms)
        history = [row[0] for row in window.values]
        print(f"取到对齐窗口：{len(window.timestamps_ms)} 行，"
              f"最后时间 {fmt(window.timestamps_ms[-1])}")

        # 2. 模型预测未来 horizon_steps 步
        horizon = cfg["inference"]["horizon_steps"]
        preds = model.forecast(history, horizon)

        # 3. 造未来时间点：按 ETT 的采样间隔（1 小时）往后推
        step_ms = (window.timestamps_ms[-1] - window.timestamps_ms[-2]
                   if len(window.timestamps_ms) >= 2 else 3600_000)
        last_ts = window.timestamps_ms[-1]
        out_ts = [last_ts + step_ms * (i + 1) for i in range(horizon)]

        return ForecastOutput(
            sequence_id=target,
            timestamps_ms=out_ts,
            values=preds,
            window_last_time_ms=last_ts,
        )


def fmt(ts_ms: int) -> str:
    """毫秒时间戳 -> 可读时间，打印用。"""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M")
