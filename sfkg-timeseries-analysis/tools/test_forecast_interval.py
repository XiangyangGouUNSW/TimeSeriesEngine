"""预测任务动态运行间隔（next_due）专项测试（负责人 08-13 定，不连服务）。

机制：每次成功预测后，engine 按 interval_ms = horizon × interval_percent × step_ms
（step_ms = 实时窗口相邻时间戳差，frequency = 1/step_ms，即「步数 × 百分比 ÷
频率」）排下一次预测的到期时间；scheduler 每 tick 对预测任务做 next_due 门控，
未到期跳过推理。异常任务无 horizon，不受门控（维持固定周期）。

覆盖：
  a) 真实 engine：成功预测 → next_due ≈ now + horizon×0.7×step_ms（公式数值）；
  b) 数据不足（DATA_NOT_READY）→ 不设到期（保持立即到期，默认周期重试）；
  c) reset_forecast_due → 立即到期（版本变化/重训解除门控）；
  d) scheduler：预测任务未到期跳过推理，异常任务不受门控照常跑；
  e) scheduler：needs_training → reset due + 训完立刻预测（不等旧间隔）。

用法（sfkg 环境）：
  python tools/test_forecast_interval.py
"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from result_repository import ResultRepository
from scheduler import Scheduler
from task_registry import TaskKind, TaskRegistry, TaskStatus
from training_loop import ModelStore

from test_discrete_forecast import CFG, StubCore, _ftask, _make_data

_PASS = 0
_INTERVAL_PERCENT = 0.7          # 与 config.yaml forecast_model.interval_percent 一致
_STEP_MS = 3_600_000             # StubCore 固定每步 1 小时（frequency = 1/step_ms）
_HORIZON = 8                     # _ftask 固定 forecast_horizon_steps


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def wait_until(cond, timeout: float, desc: str) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    print(f"  ✗ 超时：{desc}")
    return False


def _engine() -> AnalysisEngine:
    return AnalysisEngine(core_client=StubCore(_make_data()[0]), result_client=None,
                          config=CFG, model_store=ModelStore(
                              model_dir=tempfile.mkdtemp(prefix="interval-test-")))


# ================= a) 成功预测 → 按公式排 next_due =================

def test_engine_records_due() -> None:
    print("\n[engine：成功预测 → next_due = now + horizon×percent×step_ms]")
    eng = _engine()
    now0 = int(time.time() * 1000)
    res = eng.run_forecast(_ftask("t-int", "OT", features=["T"], minimum_points=60))
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"预测应成功：{res.status}"
    due = eng.forecast_due_epoch("t-int")
    expected = int(_HORIZON * _INTERVAL_PERCENT * _STEP_MS)      # 8×0.7×3.6e6 = 20.16e6
    assert due > now0 + expected - 1_000, \
        f"到期应 ≈ now+间隔：due-now={due - now0}ms，期望 ≥ {expected}"
    assert due <= now0 + expected + 60_000, \
        f"到期不应显著超过 now+间隔：due-now={due - now0}ms，期望 ≤ {expected}"
    _ok(f"成功预测 → next_due = now + {expected / 1e3:.0f}s（{_HORIZON}×{_INTERVAL_PERCENT}×{_STEP_MS / 1e3:.0f}s/步）")


# ================= b) 数据不足 → 不设到期（保持立即到期） =================

def test_engine_no_due_on_not_ready() -> None:
    print("\n[engine：数据不足 → 不设到期（保持立即到期重试）]")
    eng = _engine()
    res = eng.run_forecast(_ftask("t-nodue", "OT", features=["T"], minimum_points=10000))
    assert res.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        f"数据不足应 DATA_NOT_READY：{res.status}"
    assert eng.forecast_due_epoch("t-nodue") == 0, \
        "数据不足轮不应设未来到期（任务保持立即到期，默认周期重试）"
    _ok("数据不足 → next_due = 0（不节流，数据到位后立即重试）")


# ================= c) reset → 立即到期 =================

def test_engine_reset_due() -> None:
    print("\n[engine：reset_forecast_due → 立即到期]")
    eng = _engine()
    assert eng.run_forecast(_ftask("t-reset", "OT", features=["T"])).status == \
        pb.ANALYSIS_STATUS_SUCCESS
    assert eng.forecast_due_epoch("t-reset") > 0, "成功预测后应已设到期"
    eng.reset_forecast_due("t-reset")
    assert eng.forecast_due_epoch("t-reset") == 0, "reset 后应立即到期"
    assert eng.forecast_due_epoch("unknown-task") == 0, "未知任务默认为立即到期"
    _ok("reset_forecast_due → next_due = 0（重训/版本变化解除门控）")


class GateEngine:
    """假引擎：可控 next_due / needs_training，验证调度器门控行为。"""

    def __init__(self):
        self.due = 0.0
        self.a_due = 0.0
        self.needs_train = False
        self.forecast_runs = 0
        self.anomaly_runs = 0
        self.reset_calls = 0

    def needs_training(self, task, kind, config_version: int = 0) -> bool:
        return self.needs_train

    def forecast_due_epoch(self, task_id: str) -> int:
        return self.due

    def reset_forecast_due(self, task_id: str) -> None:
        self.reset_calls += 1
        self.due = 0.0

    def anomaly_due_epoch(self, task_id: str) -> int:
        return self.a_due

    def reset_anomaly_due(self, task_id: str) -> None:
        self.reset_calls += 1
        self.a_due = 0.0

    def run_forecast(self, task, config_version: int = 0):
        self.forecast_runs += 1
        self.needs_train = False          # 模拟"训练完成"（下一次 tick 走推理）
        return ("forecast", task.task_id, config_version)

    def run_anomaly(self, task, config_version: int = 0):
        self.anomaly_runs += 1
        return ("anomaly", task.task_id, config_version)


# ================= d) scheduler：预测未到期跳过，异常不受影响 =================

def test_scheduler_gate() -> None:
    print("\n[scheduler：预测任务 next_due 门控（异常任务不受影响）]")
    engine = GateEngine()
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=10)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.1,
                      train_queue_size=8, infer_queue_size=8,
                      train_workers=1, infer_workers=1)
    registry.register(pb.ForecastTaskConfig(task_id="t-f", target_sequence_ids=["a"]),
                      TaskKind.FORECAST, 0)
    registry.register(pb.AnomalyTaskConfig(task_id="t-a", sequence_ids=["a"],
                                           methods=["TREND_SHIFT"]),
                      TaskKind.ANOMALY, 0)
    engine.due = int(time.time() * 1000) + 10_000_000   # 预测任务很久后才到期
    sched.start()
    try:
        assert wait_until(lambda: engine.anomaly_runs >= 1, 3, "异常任务照常跑"), \
            "异常任务不应受 next_due 门控"
        time.sleep(0.5)                                  # 多个 tick，预测应一直被门控
        assert engine.forecast_runs == 0, \
            f"未到期预测任务不应执行，实际 {engine.forecast_runs}"
        _ok("预测任务未到期 → 跳过推理；异常任务照常执行（固定周期不受影响）")

        engine.due = 0                                   # 到期 → 预测恢复
        assert wait_until(lambda: engine.forecast_runs >= 1, 3, "到期后预测恢复"), \
            "到期后预测任务应恢复执行"
        _ok("到期后预测任务恢复执行")
    finally:
        sched.stop()
        sched.join(timeout=5)


# ================= e) scheduler：需重训 → 解除门控，训完立刻预测 =================

def test_scheduler_retrain_resets_due() -> None:
    print("\n[scheduler：needs_training → 解除门控 + 训完立刻预测]")
    engine = GateEngine()
    engine.due = int(time.time() * 1000) + 10_000_000   # 旧到期很远
    engine.needs_train = True                            # 模型失效/版本变化
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=10)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.1,
                      train_queue_size=8, infer_queue_size=8,
                      train_workers=1, infer_workers=1)
    registry.register(pb.ForecastTaskConfig(task_id="t-f", target_sequence_ids=["a"]),
                      TaskKind.FORECAST, 0)
    sched.start()
    try:
        assert wait_until(lambda: engine.forecast_runs >= 2, 3,
                          "重训解除门控后连续预测"), \
            "needs_training 应 reset due 并执行（训练 + 预测至少各一轮）"
        assert engine.reset_calls >= 1, "需重训时调用 reset_forecast_due"
        _ok(f"needs_training → reset_forecast_due（{engine.reset_calls} 次）"
            f"+ 解除门控立即执行（{engine.forecast_runs} 次）")
    finally:
        sched.stop()
        sched.join(timeout=5)


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_engine_records_due()
    test_engine_no_due_on_not_ready()
    test_engine_reset_due()
    test_scheduler_gate()
    test_scheduler_retrain_resets_due()
    print(f"\n预测动态间隔专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
