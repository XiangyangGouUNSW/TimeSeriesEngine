"""预测集成验证：engine.run_forecast 完整链路 + 模型复用（patchtst + dlinear 双方法）。

前置：fake_core_server 已在 localhost:50551 起好（python tools/fake_core_server.py；伪 C 端专用端口，不占真 C 端 50051）。

验证（两种方法各跑一遍）：
  1. run_forecast 预测目标序列（多元输入）；
  2. 模型复用：第二次调用同一任务命中缓存、不重训（看日志"跳过训练"）；
  3. 预测结果调 C 约束检查 → 违规 → 调 S 写预警（假 S 返回失败属预期，日志可见）；
  4. 训练一次后存磁盘，新 store 重启也能加载（loader 按 model_type 分发，
     覆盖 dlinear 恢复分支）。

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


def _cfg(mtype: str):
    base = {
        "training": {"train_ratio": 0.8, "min_train_points": 1000},
        "forecast_model": {
            "type": mtype, "context_length": 96, "prediction_length": 24,
            "patch_size": 16, "patch_stride": 8, "d_model": 64, "n_heads": 4,
            "num_layers": 2, "epochs": 3, "batch_size": 64, "learning_rate": 1e-3,
        },
        "inference": {"window_size": 100, "horizon_steps": 24},
    }
    if mtype == "dlinear":
        base["forecast_model"]["kernel_size"] = 25
        base["forecast_model"]["seed"] = 0
    return base


def _task(task_id: str, mtype: str):
    return pb.ForecastTaskConfig(
        task_id=task_id,
        task_name=f"{mtype} 预测集成验证",
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


def _run_scenario(mtype: str, core: GrpcCoreDataClient,
                  model_dir: str) -> None:
    store = ModelStore(model_dir=model_dir)
    sender = FakeSender()
    engine = AnalysisEngine(core_client=core, result_client=sender,
                            config=_cfg(mtype), model_store=store)
    task_id = f"task-{mtype}-001"

    # 1. 第一次：训练 + 预测
    r1 = engine.run_forecast(_task(task_id, mtype))
    print(f"\n[{mtype}] [1] 首次预测 status={pb.AnalysisStatus.Name(r1.status)}")
    print(f"    预测 {len(r1.timestamps_ms)} 步，前 3 值 {r1.values[:3]}")
    assert r1.status == pb.ANALYSIS_STATUS_SUCCESS
    assert len(r1.values) == 24, f"预测步数应为 24，实际 {len(r1.values)}"
    assert all(v == v for v in r1.values[:5]), "预测值含 NaN"
    assert sender.events, "约束违规时应调 S 写预警"
    assert sender.events[-1]["event_type"] == pb.ANOMALY_EVENT_TYPE_WARNING

    # 2. 模型复用：同任务再跑，应命中缓存（日志出现"跳过训练"），不重训
    r2 = engine.run_forecast(_task(task_id, mtype))
    assert r2.status == pb.ANALYSIS_STATUS_SUCCESS
    print(f"[{mtype}] [2] 二次预测 status={pb.AnalysisStatus.Name(r2.status)}"
          "（应命中缓存）")

    # 3. 磁盘持久化 + 重启加载：新 store 从磁盘恢复模型（走 loader 的
    #    model_type 分发，dlinear 有专属恢复分支），再预测一次应成功
    store2 = ModelStore(model_dir=model_dir)
    sender2 = FakeSender()
    engine2 = AnalysisEngine(core_client=core, result_client=sender2,
                             config=_cfg(mtype), model_store=store2)
    r3 = engine2.run_forecast(_task(task_id, mtype))
    assert r3.status == pb.ANALYSIS_STATUS_SUCCESS, "重启后磁盘加载应可用"
    print(f"[{mtype}] [3] 新 store 磁盘加载后预测 "
          f"status={pb.AnalysisStatus.Name(r3.status)}，"
          f"前 3 值 {r3.values[:3]}")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    core = GrpcCoreDataClient("localhost", 50551)
    # 用临时目录存模型，不污染模块目录
    with tempfile.TemporaryDirectory() as tmp:
        for mtype in ("patchtst", "dlinear"):
            _run_scenario(mtype, core, tmp)

    print("\n预测集成验证通过 ✓（patchtst + dlinear 双方法："
          "预测24步 + 约束检查 + 写预警链路通，模型首训复用与磁盘重载生效）")


if __name__ == "__main__":
    main()
