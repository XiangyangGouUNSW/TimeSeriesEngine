"""任务队列 + worker 池 + 配置版本专项测试（不连服务，假引擎/假模型）。

覆盖 P0 关键行为：
  1. config_version 失效重训 + 保留最近 2 版本回滚复用（真实 engine/store/servicer）；
  2. disable 中途：已在队列未执行的 job 被重校验丢弃，不产生结果；
  3. 队列满不崩：有界队列满 → 跳过下轮，inflight 无泄漏；
  4. 训练不阻塞推理：训练重型任务占训练 worker，推理任务独立出结果；
  5. 优雅退出：stop() 后 worker 收哨兵退出，无残留线程；
  6. needs_training 未知方法/只约束 → 永不进训练队列。

用法（sfkg 环境）：
  python tools/test_scheduler_queues.py
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
from analysis_servicer import AnalysisServicer
from result_repository import ResultRepository
from scheduler import Scheduler
from task_registry import TaskKind, TaskRegistry, TaskStatus
from training_loop import ModelStore

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def _ftask(task_id: str) -> pb.ForecastTaskConfig:
    return pb.ForecastTaskConfig(
        task_id=task_id, task_name="队列测试",
        target_sequence_ids=["ETTh1_OT"],
        feature_sequence_ids=["ETTh1_HUFL"],
    )


class FakeEngine:
    """假引擎：可控训练/推理耗时 + 真实 ModelStore，验证调度器队列/worker/隔离行为。

    模型 key 与真实引擎一致（{task_id}@v{ver} / {task_id}:{method}@v{ver}），
    这样 needs_training 判定、缓存命中语义和真实引擎等价。
    """

    def __init__(self, train_delay: float = 0.0, infer_delay: float = 0.0):
        self.store = ModelStore(model_dir=tempfile.mkdtemp(prefix="queue-test-"))
        self.train_delay = train_delay
        self.infer_delay = infer_delay
        self.train_events: list[tuple[str, int]] = []   # (task_id, ver) 训练开始
        self.run_events: list[tuple[str, int]] = []     # (task_id, ver) 推理完成

    def needs_training(self, task, kind, config_version: int = 0) -> bool:
        if kind == TaskKind.FORECAST:
            return not self.store.is_ready(f"{task.task_id}@v{config_version}")
        methods = list(task.methods) or ["CAUSAL_PATTERN"]
        return any(not self.store.is_ready(
            f"{task.task_id}:{m}@v{config_version}") for m in methods)

    def run_forecast(self, task, config_version: int = 0):
        key = f"{task.task_id}@v{config_version}"
        if not self.store.is_ready(key):
            self.train_events.append((task.task_id, config_version))
            time.sleep(self.train_delay)
            self.store.save(key, object())
        time.sleep(self.infer_delay)
        self.run_events.append((task.task_id, config_version))
        return ("forecast", task.task_id, config_version)

    def run_anomaly(self, task, config_version: int = 0):
        for m in (list(task.methods) or ["CAUSAL_PATTERN"]):
            key = f"{task.task_id}:{m}@v{config_version}"
            if not self.store.is_ready(key):
                self.train_events.append((task.task_id, config_version))
                time.sleep(self.train_delay)
                self.store.save(key, object())
        time.sleep(self.infer_delay)
        self.run_events.append((task.task_id, config_version))
        return ("anomaly", task.task_id, config_version)


def wait_until(cond, timeout: float, desc: str) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    print(f"  ✗ 超时：{desc}")
    return False


# ================= 1. config_version 失效重训 + 回滚复用 =================

def test_config_version() -> None:
    print("\n[config_version 失效重训 + 保留最近 2 版]")
    engine = AnalysisEngine(core_client=object(), result_client=None,
                            config={}, model_store=ModelStore(
                                model_dir=tempfile.mkdtemp()))
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=10)
    servicer = AnalysisServicer(registry, repo, engine)
    task = _ftask("t-ver")

    # 注册 v1 → 模拟 v1 训练完成
    ack1 = servicer.SyncForecastTask(
        pb.AnalysisSyncForecastTaskRequest(config_version=1, task=task), None)
    assert ack1.accepted
    engine.store.save("t-ver@v1", object())

    # 注册 v2 → 版本变：v1 保留（回滚复用）、v2 需重训
    ack2 = servicer.SyncForecastTask(
        pb.AnalysisSyncForecastTaskRequest(config_version=2, task=task), None)
    assert ack2.accepted
    assert engine.store.get("t-ver@v1") is not None, "v1 应保留（回滚秒级复用）"
    assert engine.needs_training(task, TaskKind.FORECAST, 2) is True, \
        "v2 无模型 → 应重训"
    assert engine.needs_training(task, TaskKind.FORECAST, 1) is False, \
        "回滚到 v1 → 复用旧模型不重训"
    assert registry.get("t-ver").config_version == 2, "registry 应记录 v2"
    _ok("版本变 → v1 保留 + v2 重训 + 回滚复用")

    # keep-2 清理：造 v1/v2/v3，invalidate 保留最近 2
    for v in (1, 2, 3):
        engine.store.save(f"t-ver@v{v}", object())
    engine.invalidate_task("t-ver", keep_version=3)
    assert engine.store.get("t-ver@v3") is not None
    assert engine.store.get("t-ver@v2") is not None
    assert engine.store.get("t-ver@v1") is None, "更旧版本应清理（磁盘有界）"
    _ok("keep-2：invalidate 保留当前+上一个，清更旧")


# ================= 2. disable 中途：已排队 job 被丢弃 =================

def test_disable_queued_job() -> None:
    print("\n[disable 中途：已排队 job 不执行]")
    engine = FakeEngine(train_delay=1.0)
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=5)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.2,
                      train_queue_size=4, infer_queue_size=4,
                      train_workers=1, infer_workers=1)
    registry.register(_ftask("t-a"), TaskKind.FORECAST, 0)
    registry.register(_ftask("t-b"), TaskKind.FORECAST, 0)
    sched.start()
    try:
        assert wait_until(lambda: len(engine.train_events) >= 1, 3,
                          "t-a 开始训练"), "t-a 应进训练队列"
        time.sleep(0.3)                            # t-b 应已在 t-a 后排队
        registry.set_status("t-b", TaskStatus.DISABLED)
        assert wait_until(lambda: repo.latest("t-a") is not None, 3,
                          "t-a 出结果"), "t-a 应正常完成"
        time.sleep(0.5)                            # 等 worker 弹出 t-b
        assert repo.latest("t-b") is None, "disable 的排队 job 不应产生结果"
        assert not any("t-b" in str(e) for e in engine.run_events), \
            "t-b 不应执行推理"
        _ok("disable 后已排队 job 被重校验丢弃")
    finally:
        sched.stop()
        sched.join(timeout=5)


# ================= 3. 队列满不崩 =================

def test_queue_full() -> None:
    print("\n[有界队列满不崩]")
    engine = FakeEngine(train_delay=0.3)
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=5)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.2,
                      train_queue_size=1, infer_queue_size=1,
                      train_workers=1, infer_workers=1)
    for i in range(5):
        registry.register(_ftask(f"t-{i}"), TaskKind.FORECAST, 0)
    sched.start()
    try:
        assert wait_until(lambda: len(engine.run_events) >= 1, 5,
                          "队列满时至少一个任务完成"), "队列满不应卡死"
        # 多 tick 后每个任务都应轮到过（被跳过的不丢配置，下轮重试）
        assert wait_until(lambda: len(engine.run_events) >= 3, 5,
                          "多个任务轮转执行"), "被跳过的任务应下轮恢复"
        _ok("队列满 → 跳过下轮重试，不阻塞不崩溃")
    finally:
        sched.stop()
        sched.join(timeout=5)
        assert len(sched._train_inflight) == 0 and len(sched._infer_inflight) == 0, \
            "停止后 inflight 无泄漏"
        _ok("停止后 inflight 无泄漏")


# ================= 4. 训练不阻塞推理 =================

def test_train_not_blocking_inference() -> None:
    print("\n[训练不阻塞在线推理]")
    engine = FakeEngine(train_delay=3.0, infer_delay=0.0)
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=5)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.2,
                      train_queue_size=4, infer_queue_size=4,
                      train_workers=1, infer_workers=1)
    registry.register(_ftask("t-slow"), TaskKind.FORECAST, 0)    # 需 3s 训练
    engine.store.save("t-fast@v0", object())                      # 模型已就绪
    registry.register(_ftask("t-fast"), TaskKind.FORECAST, 0)
    sched.start()
    try:
        t0 = time.time()
        assert wait_until(lambda: repo.latest("t-fast") is not None, 2.5,
                          "就绪任务出结果"), "推理任务应独立于训练 worker"
        elapsed = time.time() - t0
        assert elapsed < 3.0, f"推理被训练阻塞：t-fast 花了 {elapsed:.1f}s"
        _ok(f"训练 worker 忙 3s 时，推理任务 {elapsed:.1f}s 内出结果（未阻塞）")
    finally:
        sched.stop()
        sched.join(timeout=5)


# ================= 5. 优雅退出 =================

def test_graceful_shutdown() -> None:
    print("\n[优雅退出]")
    engine = FakeEngine(train_delay=2.0)
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=5)
    sched = Scheduler(engine, registry, repo, interval_seconds=0.2,
                      train_queue_size=4, infer_queue_size=4,
                      train_workers=1, infer_workers=1)
    registry.register(_ftask("t-exit"), TaskKind.FORECAST, 0)
    sched.start()
    assert wait_until(lambda: len(engine.train_events) >= 1, 3,
                      "worker 在飞"), "训练 job 应已开始"
    sched.stop()
    sched.join(timeout=6)
    assert not sched.is_alive(), "producer 线程应退出"
    for w in sched._train_workers + sched._infer_workers:
        assert not w.is_alive(), f"{w.name} 应收到哨兵退出"
    _ok("stop() 后 producer 与全部 worker 线程退出")


# ================= 6. needs_training 判定 =================

def test_needs_training_edges() -> None:
    print("\n[needs_training 判定边界]")
    engine = AnalysisEngine(core_client=object(), result_client=None,
                            config={}, model_store=ModelStore(
                                model_dir=tempfile.mkdtemp()))
    task_unknown = pb.AnomalyTaskConfig(task_id="t-u", sequence_ids=["a"],
                                        methods=["FOO"])
    assert engine.needs_training(task_unknown, TaskKind.ANOMALY, 0) is False
    _ok("只含未知方法 → 不进训练队列")

    task_constraint = pb.AnomalyTaskConfig(task_id="t-c", sequence_ids=["a"],
                                           methods=["CONSTRAINT_CHECK"])
    assert engine.needs_training(task_constraint, TaskKind.ANOMALY, 0) is False
    _ok("只约束 → 不进训练队列")

    task_empty = pb.AnomalyTaskConfig(task_id="t-e", sequence_ids=["a"], methods=[])
    assert engine.needs_training(task_empty, TaskKind.ANOMALY, 0) is True
    _ok("空 methods → 默认 CAUSAL_PATTERN → 需训练")

    task_forecast = _ftask("t-f")
    assert engine.needs_training(task_forecast, TaskKind.FORECAST, 0) is True
    _ok("预测模型未训 → 需训练")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_config_version()
    test_disable_queued_job()
    test_queue_full()
    test_train_not_blocking_inference()
    test_graceful_shutdown()
    test_needs_training_edges()
    print(f"\n队列/隔离专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
