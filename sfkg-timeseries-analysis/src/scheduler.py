"""任务调度器：固定周期扫描 + 有界任务队列 + 训练/推理双 worker 池。

职责：
  - producer 线程（Scheduler 本身）：每 interval_seconds 扫描所有 ENABLED 任务，
    按 engine.needs_training 路由到训练队列或推理队列；
  - train worker 池：模型未就绪的任务（重型训练，如 PatchTST 首训）串行执行，
    绝不占推理 worker → 其他任务的在线检测/预测不被训练阻塞（训练/推理资源隔离）；
  - infer worker 池：模型已就绪任务的在线检测/预测，并行执行。

并发正确性：
  - inflight 去重：同一任务同一周期只入队一次（先加 inflight 再 put_nowait）；
  - 队列满：put_nowait + queue.Full 捕获 → 跳过，下个 tick 重试，不阻塞不崩溃；
  - 消费前重校验：worker 弹任务后重取 rec，检查存在/ENABLED/kind/版本一致，
    丢弃 disable 中途、版本变更中途、已删除任务的过时 job。
"""

from __future__ import annotations

import logging
import queue
import threading
from collections import namedtuple

from task_registry import TaskKind, TaskStatus

logger = logging.getLogger(__name__)

# 队列任务：只带 id/kind/版本，worker 消费时从 registry 拉最新 task
# （不把可变 proto 塞进队列造成跨线程共享，S 更新配置后 worker 拿到的就是新配置）。
Job = namedtuple("Job", ["task_id", "kind", "config_version"])

_SENTINEL = object()   # 优雅退出哨兵


class Scheduler(threading.Thread):
    """固定周期扫描 + 双队列双 worker 池。"""

    def __init__(self, engine, registry, repository,
                 interval_seconds: float = 10.0,
                 train_queue_size: int = 8,
                 infer_queue_size: int = 32,
                 train_workers: int = 1,
                 infer_workers: int = 2):
        super().__init__(name="analysis-scheduler", daemon=True)
        self.engine = engine
        self.registry = registry
        self.repository = repository
        self.interval = interval_seconds
        # 注意：不能叫 _stop——Thread 内部有同名方法（join 依赖），覆盖会崩
        self._stop_event = threading.Event()

        self._train_queue = queue.Queue(maxsize=train_queue_size)
        self._infer_queue = queue.Queue(maxsize=infer_queue_size)
        self._train_inflight: set[str] = set()
        self._infer_inflight: set[str] = set()
        self._inflight_lock = threading.Lock()

        self._train_workers = [
            threading.Thread(target=self._worker_loop,
                             args=(self._train_queue, self._train_inflight, "train"),
                             daemon=True, name=f"train-worker-{i}")
            for i in range(train_workers)]
        self._infer_workers = [
            threading.Thread(target=self._worker_loop,
                             args=(self._infer_queue, self._infer_inflight, "infer"),
                             daemon=True, name=f"infer-worker-{i}")
            for i in range(infer_workers)]

    def stop(self) -> None:
        """请求停止；producer 退出循环并优雅收尾 worker。"""
        self._stop_event.set()

    # ---- producer：周期扫描入队 ----

    def run(self) -> None:
        logger.info("[scheduler] 启动，周期 %.1fs，train_workers=%d infer_workers=%d",
                    self.interval, len(self._train_workers), len(self._infer_workers))
        for w in self._train_workers + self._infer_workers:
            w.start()
        while not self._stop_event.wait(self.interval):
            self._tick()
        self._shutdown_workers()

    def _tick(self) -> None:
        for rec in self.registry.enabled_tasks():
            try:
                if self.engine.needs_training(rec.task, rec.kind,
                                              rec.config_version):
                    self._enqueue(self._train_queue, self._train_inflight,
                                  rec, "train")
                else:
                    self._enqueue(self._infer_queue, self._infer_inflight,
                                  rec, "infer")
            except Exception:
                logger.exception("[scheduler] tick 中任务 %s 分派失败", rec.task_id)

    def _enqueue(self, q, inflight, rec, pool) -> None:
        """入队：先加 inflight 再 put_nowait；队列满回滚 inflight 并跳过。

        先加 inflight 防「worker 已消费完、producer 后加」造成永久残留；
        队列满回滚并跳过，下个 tick 重试。
        """
        with self._inflight_lock:
            if rec.task_id in inflight:
                return
            inflight.add(rec.task_id)
        try:
            q.put_nowait(Job(rec.task_id, rec.kind, rec.config_version))
        except queue.Full:
            with self._inflight_lock:
                inflight.discard(rec.task_id)
            logger.warning("[scheduler] %s 队列满（maxsize=%d），跳过 %s，下个 tick 重试",
                           pool, q.maxsize, rec.task_id)

    # ---- worker：消费执行 ----

    def _worker_loop(self, q, inflight, pool) -> None:
        while True:
            job = q.get()
            if job is _SENTINEL:
                q.task_done()
                return
            try:
                self._run_job(job, pool)
            finally:
                with self._inflight_lock:
                    inflight.discard(job.task_id)
                q.task_done()

    def _run_job(self, job, pool) -> None:
        """消费前重校验：disable/删除/类型重建/版本变更中途的任务一律丢弃。"""
        rec = self.registry.get(job.task_id)
        if rec is None:
            return                                  # 已 DELETED
        if rec.status != TaskStatus.ENABLED:
            return                                  # disable/删除中途
        if rec.kind != job.kind:
            return                                  # 同 id 被删后以别的类型重建
        if rec.config_version != job.config_version:
            logger.info("[scheduler] 任务 %s 版本已变 %d→%d，丢弃过时 %s job",
                        job.task_id, job.config_version, rec.config_version, pool)
            return
        try:
            if rec.kind == TaskKind.ANOMALY:
                result = self.engine.run_anomaly(rec.task,
                                                 config_version=job.config_version)
            else:
                result = self.engine.run_forecast(rec.task,
                                                  config_version=job.config_version)
            self.repository.put(rec.task_id, result)
        except Exception:
            rec.error_count += 1
            logger.exception("[scheduler] 任务 %s 执行失败（第 %d 次）",
                             rec.task_id, rec.error_count)

    # ---- 优雅退出 ----

    def _shutdown_workers(self) -> None:
        """丢已排队 job，每 worker 放一个哨兵，短 join（超时靠 daemon 兜底）。"""
        for q, workers in ((self._train_queue, self._train_workers),
                           (self._infer_queue, self._infer_workers)):
            while True:
                try:
                    q.get_nowait()
                except queue.Empty:
                    break
            for _ in workers:
                q.put_nowait(_SENTINEL)
        for w in self._train_workers + self._infer_workers:
            w.join(timeout=5)
