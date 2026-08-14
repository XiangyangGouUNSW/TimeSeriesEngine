"""异常任务动态检测间隔（next_due）专项测试（负责人 08-13 定，不连服务）。

机制：一次检测 = 一个窗口（吃最近 window_size 行）。每次真检测后 engine 按
interval_ms = window_size × recheck_fraction × step_ms（step_ms = 实时窗口相邻
时间戳差，frequency = 1/step_ms）排下一次到期：
  - 无异常 → recheck_fraction_normal（默认 1.0）= 等一整个新窗口再查；
  - 有异常 → recheck_fraction_hot（默认 0.05）= 等 5% 窗口（window=100 → 5 点）
    盯住事件；连续 hot_confirm_clean_runs 轮无异常 → 回正常节奏（确认恢复防抖动）。
scheduler 每 tick 对异常任务做 next_due 门控，未到期跳过推理。

覆盖：
  a) _record_anomaly_due 纯逻辑：无异常 → 正常间隔；有异常 → 热间隔；
     连续 hot_confirm_clean_runs 轮干净 → 回正常节奏；
  b) step_ms=0（窗口不足 2 个时间戳）→ 不设到期（保持立即到期）；
  c) 真实 engine 全链路：run_anomaly 成功跑完 → 设了到期；数据不足 → 不设；
  d) slide_step_ms 窗口未推进（_SlideNotAdvanced）→ 排到 slide 步长之后，
     避免每 tick 都去 fetch 空跑；
  e) scheduler：异常任务未到期跳过推理、到期恢复执行、needs_training 解除门控。

用法（sfkg 环境）：
  python tools/test_anomaly_interval.py
"""

from __future__ import annotations

import sys
import tempfile
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
from task_registry import TaskKind, TaskRegistry
from training_loop import ModelStore

from test_discrete_forecast import CFG, StubCore, _make_data

_PASS = 0
_WINDOW = 100                 # 与 CFG inference.window_size 一致
_STEP_MS = 3_600_000          # StubCore 固定每步 1 小时（frequency = 1/step_ms）
_NORMAL = 1.0                 # 与 config.yaml anomaly.recheck_fraction_normal 一致
_HOT = 0.05                   # 与 config.yaml anomaly.recheck_fraction_hot 一致
_CONFIRM = 2                  # 与 config.yaml anomaly.hot_confirm_clean_runs 一致


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


class _FakeSender:
    """假 S 端写事件客户端：只实现 send_event（engine 用作 self.sender）。"""

    def __init__(self):
        self.sent = 0

    def send_event(self, **kwargs) -> bool:
        self.sent += 1
        return True


def _engine() -> AnalysisEngine:
    return AnalysisEngine(core_client=StubCore(_make_data()[0]),
                          result_client=_FakeSender(),
                          config=CFG, model_store=ModelStore(
                              model_dir=tempfile.mkdtemp(prefix="anomaly-int-test-")))


def _atask(task_id: str, methods=("TREND_SHIFT",),
           minimum_points: int = 0, slide_step_ms: int = 0):
    return pb.AnomalyTaskConfig(
        task_id=task_id, sequence_ids=["OT", "T"], methods=list(methods),
        minimum_points=minimum_points, slide_step_ms=slide_step_ms)


# ================= a/b) _record_anomaly_due 纯逻辑 =================

def test_record_due_logic() -> None:
    print("\n[engine：_record_anomaly_due 纯逻辑（正常/热/回正常）]")
    eng = _engine()
    tid = "t-unit"
    now = 1_700_000_000_000
    normal_ms = int(_WINDOW * _NORMAL * _STEP_MS)     # 100 × 1.0 × 3.6e6
    hot_ms = int(_WINDOW * _HOT * _STEP_MS)           # 100 × 0.05 × 3.6e6

    # 无异常 → 正常间隔；连续干净保持正常
    eng._record_anomaly_due(tid, now, 0, _STEP_MS)
    assert eng.anomaly_due_epoch(tid) == now + normal_ms, "无异常 → 正常间隔"
    eng._record_anomaly_due(tid, now, 0, _STEP_MS)
    assert eng.anomaly_due_epoch(tid) == now + normal_ms, "连续无异常 → 维持正常间隔"
    _ok(f"无异常 → next_due = now + window×{_NORMAL}×step = +{normal_ms}ms")

    # 有异常 → 热间隔
    eng._record_anomaly_due(tid, now, 3, _STEP_MS)
    assert eng.anomaly_due_epoch(tid) == now + hot_ms, "有异常 → 热间隔"
    _ok(f"有异常 → next_due = now + window×{_HOT}×step = +{hot_ms}ms（盯住事件）")

    # 热后 1 轮干净（streak=1 < confirm=2）仍热；第 2 轮干净 → 回正常
    eng._record_anomaly_due(tid, now, 0, _STEP_MS)
    assert eng.anomaly_due_epoch(tid) == now + hot_ms, \
        "热后第 1 轮干净仍按热节奏（防抖动确认中）"
    eng._record_anomaly_due(tid, now, 0, _STEP_MS)
    assert eng.anomaly_due_epoch(tid) == now + normal_ms, \
        f"连续 {_CONFIRM} 轮干净 → 回正常节奏"
    _ok(f"热后连续 {_CONFIRM} 轮干净 → 回正常节奏（防抖动）")

    # step_ms=0（窗口不足）→ 不设到期
    eng._record_anomaly_due(tid, now, 0, 0)
    assert eng.anomaly_due_epoch(tid) == now + normal_ms, \
        "step_ms=0 不应改到期（保持上一轮的值）"
    eng.reset_anomaly_due(tid)
    assert eng.anomaly_due_epoch(tid) == 0, "reset → 立即到期"
    eng._record_anomaly_due(tid, now, 5, 0)
    assert eng.anomaly_due_epoch(tid) == 0, "step_ms=0 → 不设到期（不节流）"
    _ok("step_ms=0（窗口不足）→ 不设到期；reset → 立即到期")


# ================= c) 真实 engine 全链路 =================

def test_engine_full_path() -> None:
    print("\n[engine：run_anomaly 全链路 → 设到期 / 数据不足 → 不设]")
    eng = _engine()
    res = eng.run_anomaly(_atask("t-full"), config_version=0)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"检测应成功：{res.status}"
    due = eng.anomaly_due_epoch("t-full")
    now0 = int(time.time() * 1000)
    assert due > now0, "成功跑完检测 → 应设了未来到期"
    assert due - now0 <= int(_WINDOW * _NORMAL * _STEP_MS) + 2_000, \
        f"到期应在正常间隔内：due-now={due - now0}ms"
    _ok(f"run_anomaly 成功 → next_due = {due - now0}ms 后（正常间隔内）")

    # 数据不足（minimum_points 拉满）→ DATA_NOT_READY，不设到期
    eng2 = _engine()
    res2 = eng2.run_anomaly(_atask("t-nodata", minimum_points=10 ** 9),
                            config_version=0)
    assert res2.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, res2.status
    assert eng2.anomaly_due_epoch("t-nodata") == 0, \
        "数据不足 → 不设到期（保持立即到期重试）"
    _ok("数据不足（DATA_NOT_READY）→ 不设到期（数据到位后立即重试）")


# ================= d) slide 未推进 → 排到 slide 步长之后 =================

def test_slide_skip_sets_due() -> None:
    print("\n[engine：slide_step_ms 窗口未推进 → next_due = now + slide_ms]")
    eng = _engine()
    tid = "t-slide"
    slide_ms = 30_000
    # 首跑：数据足 → 真检测 → 记正常间隔
    res = eng.run_anomaly(_atask(tid, slide_step_ms=slide_ms), config_version=0)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, res.status
    # 隔离：清掉首跑的到期，模拟"下一轮到点但窗口没推进"
    eng.reset_anomaly_due(tid)
    res2 = eng.run_anomaly(_atask(tid, slide_step_ms=slide_ms), config_version=0)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS, \
        "slide 跳过是正常结果（非失败）"
    assert res2.message and "未推进" in res2.message, res2.message
    due = eng.anomaly_due_epoch(tid)
    now0 = int(time.time() * 1000)
    assert 0 < due - now0 <= slide_ms, \
        f"slide 跳过 → 应排到 slide 步长后：due-now={due - now0}ms"
    _ok(f"slide 未推进 → next_due = now + slide_ms（≈{slide_ms}ms，避免每 tick 空 fetch）")


# ================= e) scheduler 门控 =================

class GateEngine:
    """假引擎：可控 anomaly next_due / needs_training，验证调度器门控行为。"""

    def __init__(self):
        self.a_due = 0.0
        self.needs_train = False
        self.forecast_runs = 0
        self.anomaly_runs = 0
        self.reset_calls = 0

    def needs_training(self, task, kind, config_version: int = 0) -> bool:
        return self.needs_train

    def forecast_due_epoch(self, task_id: str) -> int:
        return 0

    def reset_forecast_due(self, task_id: str) -> None:
        self.reset_calls += 1

    def anomaly_due_epoch(self, task_id: str) -> int:
        return self.a_due

    def reset_anomaly_due(self, task_id: str) -> None:
        self.reset_calls += 1
        self.a_due = 0.0

    def run_forecast(self, task, config_version: int = 0):
        self.forecast_runs += 1
        return ("forecast", task.task_id, config_version)

    def run_anomaly(self, task, config_version: int = 0):
        self.anomaly_runs += 1
        self.needs_train = False
        return ("anomaly", task.task_id, config_version)


def _make_sched(engine):
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=10)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.1,
                      train_queue_size=8, infer_queue_size=8,
                      train_workers=1, infer_workers=1)
    registry.register(pb.AnomalyTaskConfig(task_id="t-a", sequence_ids=["OT"],
                                           methods=["TREND_SHIFT"]),
                      TaskKind.ANOMALY, 0)
    return sched, registry


def test_scheduler_anomaly_gate() -> None:
    print("\n[scheduler：异常任务 next_due 门控]")
    engine = GateEngine()
    sched, _ = _make_sched(engine)
    engine.a_due = int(time.time() * 1000) + 10_000_000   # 很久后才到期
    sched.start()
    try:
        # 未到期：让调度器跑若干 tick，异常任务应一次推理都没跑
        time.sleep(0.5)
        assert engine.anomaly_runs == 0, \
            f"未到期 → 不应跑推理（实际跑了 {engine.anomaly_runs} 次）"
        # 到期 → 恢复执行
        engine.a_due = 0
        assert wait_until(lambda: engine.anomaly_runs >= 1, 3.0,
                          "异常任务到期后应恢复执行"), "到期后应恢复执行"
        _ok(f"未到期 → 跳过推理（{engine.anomaly_runs} 次跑）；到期 → 恢复执行")
    finally:
        sched.stop()
        sched.join(timeout=3.0)


def test_scheduler_anomaly_retrain_unlocks() -> None:
    print("\n[scheduler：needs_training → reset_anomaly_due + 训完立刻检测]")
    engine = GateEngine()
    sched, _ = _make_sched(engine)
    engine.a_due = int(time.time() * 1000) + 10_000_000   # 门控中
    engine.needs_train = True                              # 模型未训 → 走训练队列
    sched.start()
    try:
        assert wait_until(lambda: engine.reset_calls >= 1, 3.0,
                          "needs_training → 应调 reset_anomaly_due"), \
            "需重训时应解除 next_due 门控"
        assert engine.a_due == 0, "reset 后应立即到期"
        before = engine.anomaly_runs
        assert wait_until(lambda: engine.anomaly_runs > before, 3.0,
                          "训完应立刻出新一轮检测"), "训完应立即检测（不等旧间隔）"
        _ok(f"needs_training → reset_anomaly_due（{engine.reset_calls} 次）+ "
            f"训完立刻检测（{engine.anomaly_runs} 次）")
    finally:
        sched.stop()
        sched.join(timeout=3.0)


def wait_until(cond, timeout: float, desc: str) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    print(f"  ✗ 超时：{desc}")
    return False


if __name__ == "__main__":
    test_record_due_logic()
    test_engine_full_path()
    test_slide_skip_sets_due()
    test_scheduler_anomaly_gate()
    test_scheduler_anomaly_retrain_unlocks()
    print(f"\n异常任务动态间隔专项测试通过 ✓（{_PASS} 项断言）")
