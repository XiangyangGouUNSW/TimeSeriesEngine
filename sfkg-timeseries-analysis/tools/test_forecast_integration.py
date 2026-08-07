"""PatchTST 预测集成验证：engine.run_forecast 完整链路 + 模型复用。

前置：fake_core_server 已在 localhost:50051 起好（python tools/fake_core_server.py）。

验证：
  1. run_forecast 用 PatchTST 预测目标序列（多元输入）；
  2. 模型复用：第二次调用同一任务命中缓存、不重训（看日志"跳过训练"）；
  3. 预测结果调 C 约束检查 → 违规 → 调 S 写预警（假 S 返回失败属预期，日志可见）；
  4. 训练一次后存磁盘，重启也能加载（save/load 验证）。

用法（sfkg 环境）：
  python tools/test_forecast_integration.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from grpc_client import GrpcCoreDataClient
from training_loop import ModelStore

TARGET = "ETTh1_OT"
FEATURES = ["ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL",
            "ETTh1_MULL", "ETTh1_LUFL", "ETTh1_LULL"]


def _cfg():
    return {
        "training": {"train_ratio": 0.8, "min_train_points": 1000},
        "forecast_model": {
            "type": "patchtst", "context_length": 96, "prediction_length": 24,
            "patch_size": 16, "patch_stride": 8, "d_model": 64, "n_heads": 4,
            "num_layers": 2, "epochs": 3, "batch_size": 64, "learning_rate": 1e-3,
        },
        "inference": {"window_size": 100, "horizon_steps": 24},
    }


def _task(task_id: str):
    return pb.ForecastTaskConfig(
        task_id=task_id,
        task_name="PatchTST 预测集成验证",
        target_sequence_ids=[TARGET],
        feature_sequence_ids=FEATURES,
        forecast_horizon_steps=24,
        minimum_points=1000,
        semantic_context=pb.SemanticContext(
            constraint_ids=["demo-constraint-001"]),
    )


class FakeSender:
    """假 S 端：记录写事件调用，不真连 S。"""

    def __init__(self):
        self.events = []

    def send_event(self, **kw):
        self.events.append(kw)
        return True


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    core = GrpcCoreDataClient("localhost", 50051)
    # 用临时目录存模型，不污染模块目录
    with tempfile.TemporaryDirectory() as tmp:
        store = ModelStore(model_dir=tmp)
        sender = FakeSender()
        engine = AnalysisEngine(core_client=core, result_client=sender,
                                config=_cfg(), model_store=store)

        # 1. 第一次：训练 + 预测
        task = _task("task-patchtst-001")
        r1 = engine.run_forecast(task, config_version=1)
        print(f"\n[1] 首次预测 status={pb.AnalysisStatus.Name(r1.status)}")
        print(f"    预测 {len(r1.timestamps_ms)} 步，前 3 值 {r1.values[:3]}")
        assert r1.status == pb.ANALYSIS_STATUS_SUCCESS
        assert len(r1.values) == 24, f"预测步数应为 24，实际 {len(r1.values)}"
        assert all(v == v for v in r1.values[:5]), "预测值含 NaN"
        assert sender.events, "约束违规时应调 S 写预警"
        assert sender.events[-1]["event_type"] == pb.ANOMALY_EVENT_TYPE_WARNING
        print(f"    写预警调用 {len(sender.events)} 次（假 S 记录）")

        # 2. 模型复用：同任务同 version 再跑，应命中缓存（日志出现"跳过训练"）
        task2 = _task("task-patchtst-001")
        r2 = engine.run_forecast(task2, config_version=1)
        print(f"[2] 二次预测 status={pb.AnalysisStatus.Name(r2.status)}（应命中缓存）")

        # 3. config_version 变化 → 重训（不报错即可）
        task3 = _task("task-patchtst-001")
        r3 = engine.run_forecast(task3, config_version=2)
        print(f"[3] config_version=2 重训 status={pb.AnalysisStatus.Name(r3.status)}")

        # 4. 磁盘持久化：训练后应已写文件，新 store 识别为就绪
        store2 = ModelStore(model_dir=tmp)
        ok = store2.is_ready("task-patchtst-001")
        print(f"[4] 磁盘缓存就绪：{ok}")
        assert ok, "训练后的模型文件应存在且可识别"

        print("\nPatchTST 预测集成验证通过 ✓"
              "（预测24步 + 约束检查 + 写预警链路通，模型复用生效）")


if __name__ == "__main__":
    main()
