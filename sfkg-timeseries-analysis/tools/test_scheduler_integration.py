"""调度器集成测试：注册 → 周期执行 → 结果可查，启停真正生效。

自包含：在测试内起假 C（端口 50052，避免和正在跑的 50051 冲突）。
验证：
  1. Sync 注册异常任务（模型检测）→ Scheduler 周期跑 → Query 拿到结果；
  2. 预测任务：首训一次，之后每周期复用（日志"跳过训练"）；
  3. UpdateTaskStatus(DISABLED) 后该任务不再执行；
  4. 结果仓库保留最近多条历史。

用法（sfkg 环境）：
  python tools/test_scheduler_integration.py
"""

from __future__ import annotations

import logging
import sys
import time
from concurrent import futures
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated"), str(ROOT / "tools")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import grpc

import timeseries_analysis_pb2 as pb
import timeseries_core_pb2_grpc as core_pb_grpc

from analysis_engine import AnalysisEngine
from analysis_servicer import AnalysisServicer
from grpc_client import GrpcCoreDataClient
from task_registry import TaskRegistry
from result_repository import ResultRepository
from scheduler import Scheduler
from fake_core_server import FakeCoreService

TARGET = "ETTh1_OT"
FEATURES = ["ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL",
            "ETTh1_MULL", "ETTh1_LUFL", "ETTh1_LULL"]
CORE_PORT = 50052
INTERVAL = 1.0     # 调度周期（测试里调短，加速验证）


def _cfg():
    return {
        "training": {"train_ratio": 0.8},
        "forecast_model": {
            "type": "patchtst", "context_length": 96, "prediction_length": 24,
            "patch_size": 16, "patch_stride": 8, "d_model": 64, "n_heads": 4,
            "num_layers": 2, "epochs": 2, "batch_size": 64, "learning_rate": 1e-3,
        },
        "inference": {"window_size": 100, "horizon_steps": 24},
    }


class FakeSender:
    """假 S 端：记录写事件调用，不真连 S。"""

    def __init__(self):
        self.events = []

    def send_event(self, **kw):
        self.events.append(kw)
        return True


def _anomaly_task(task_id):
    # 约束检查已迁 C（S 直接下发 C），P 的异常任务只含模型检测方法
    return pb.AnomalyTaskConfig(
        task_id=task_id, task_name="调度器-异常检测",
        sequence_ids=[TARGET],
        methods=["CAUSAL_PATTERN"],
        semantic_context=pb.SemanticContext(constraint_ids=["demo-constraint-001"]),
    )


def _forecast_task(task_id):
    return pb.ForecastTaskConfig(
        task_id=task_id, task_name="调度器-预测",
        target_sequence_ids=[TARGET], feature_sequence_ids=FEATURES,
        forecast_horizon_steps=24, minimum_points=1000,
        semantic_context=pb.SemanticContext(constraint_ids=["demo-constraint-001"]),
    )


def wait_until(cond, timeout: float, desc: str) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    print(f"  ✗ 超时：{desc}")
    return False


def start_fake_core():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    core_pb_grpc.add_TimeseriesCoreServiceServicer_to_server(
        FakeCoreService(str(ROOT / "data" / "ETT-small" / "ETTh1.csv")), server)
    server.add_insecure_port(f"0.0.0.0:{CORE_PORT}")
    server.start()
    return server


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    engine_log = logging.getLogger("analysis_engine")

    core_server = start_fake_core()
    print(f"假 C 已起（localhost:{CORE_PORT}）")
    scheduler = None
    try:
        core = GrpcCoreDataClient("localhost", CORE_PORT)
        sender = FakeSender()
        engine = AnalysisEngine(core_client=core, result_client=sender, config=_cfg())
        registry = TaskRegistry()
        repository = ResultRepository(maxlen=5)
        scheduler = Scheduler(engine, registry, repository, interval_seconds=INTERVAL)
        servicer = AnalysisServicer(registry=registry, repository=repository,
                                    engine=engine)

        # 1. 注册异常任务（模型检测）
        ack = servicer.SyncAnomalyTask(
            pb.AnalysisSyncAnomalyTaskRequest(
                task=_anomaly_task("task-sched-anomaly-001")), None)
        assert ack.accepted, "异常任务应注册成功"
        print(f"[1] 注册异常任务：{ack.message}")

        # 2. 注册预测任务
        ack2 = servicer.SyncForecastTask(
            pb.AnalysisSyncForecastTaskRequest(
                task=_forecast_task("task-sched-forecast-001")), None)
        assert ack2.accepted
        print(f"[2] 注册预测任务：{ack2.message}")

        # 启动调度器（后台线程）
        scheduler.start()

        # 3. 异常任务周期执行 → 结果可查。
        #    单序列下 GCAD 无自变量不产出 finding，但模型腿应真实跑通（GCAD 首训入 store）
        ok = wait_until(lambda: repository.latest("task-sched-anomaly-001") is not None,
                        15, "异常任务出结果")
        assert ok, "异常任务应在调度周期内出结果"
        a_res = repository.latest("task-sched-anomaly-001")
        assert a_res.status == pb.ANALYSIS_STATUS_SUCCESS
        assert engine.store.get("task-sched-anomaly-001:CAUSAL_PATTERN") is not None, \
            "GCAD 模型应已首训并存 store"
        print(f"[3] 异常任务出结果 ✓ status=SUCCESS，"
              f"findings={len(a_res.findings)} 条，GCAD 已入 store")

        # 4. 预测任务：首训（数据达标自动训）→ 出结果
        ok = wait_until(lambda: repository.latest("task-sched-forecast-001") is not None,
                        60, "预测任务出结果（含首训）")
        assert ok, "预测任务应在调度周期内完成首训并出结果"
        f_res = repository.latest("task-sched-forecast-001")
        assert f_res.status == pb.ANALYSIS_STATUS_SUCCESS
        assert len(f_res.values) == 24, "预测 24 步"
        assert engine.store.get("task-sched-forecast-001") is not None, \
            "预测模型应已存 store"
        print(f"[4] 预测任务首训出结果 ✓ 模型已就绪")

        # 4b. 再跑几个周期：应命中缓存不重训（捕获"跳过训练"日志）
        skip_logs = []
        handler = logging.Handler()
        handler.emit = lambda r: skip_logs.append(r.getMessage()) \
            if "跳过训练" in r.getMessage() else None
        handler.setLevel(logging.INFO)
        engine_log.addHandler(handler)
        try:
            time.sleep(INTERVAL * 2 + 0.5)
            assert len(skip_logs) >= 1, f"应出现'跳过训练'复用日志，实际 {skip_logs}"
            print(f"[4b] 预测任务多周期复用 ✓ '跳过训练'出现 {len(skip_logs)} 次")
        finally:
            engine_log.removeHandler(handler)

        # 5. UpdateTaskStatus(DISABLED) → 不再执行（历史不再增长）
        n_before = len(repository.history("task-sched-anomaly-001"))
        ack3 = servicer.UpdateTaskStatus(
            pb.AnalysisUpdateTaskStatusRequest(
                task_id="task-sched-anomaly-001",
                status=pb.TASK_STATUS_DISABLED), None)
        assert ack3.accepted
        print(f"[5] 停用异常任务（停用前历史 {n_before} 条）")
        time.sleep(INTERVAL * 2 + 0.5)
        n_after = len(repository.history("task-sched-anomaly-001"))
        assert n_after == n_before, f"停用后不应再执行：{n_before} → {n_after}"
        print(f"    停用后历史仍是 {n_after} 条 ✓ 不再执行")

        # 6. 结果仓库保留最近多条（maxlen=5）
        hist = repository.history("task-sched-anomaly-001")
        assert len(hist) == n_before <= 5, "结果应保留最近且不超过 maxlen"
        print(f"[6] 结果仓库保留最近 {len(hist)} 条（maxlen=5）✓")

        print("\n调度器集成测试通过 ✓"
              "（注册制 + 周期执行 + 结果可查 + 启停生效 + 首训复用）")
    finally:
        if scheduler is not None:
            scheduler.stop()
            scheduler.join(timeout=5)
        core_server.stop(0)


if __name__ == "__main__":
    main()
