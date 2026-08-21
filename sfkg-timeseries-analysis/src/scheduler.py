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
    丢弃 disable 中途、版本变更中途、已删除任务的过时 job；
  - 单任务超时：train/infer 池各自超时上限，到点丢弃本轮、下个 tick 重试，释放 worker；
  - worker 崩溃保护：worker 循环兜底 except，单次处理异常不杀 worker 线程。
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections import namedtuple

from analysis_engine import AnalysisEngine
from project import scoped_key
from result_repository import ResultRepository
from task_registry import TaskKind, TaskRecord, TaskRegistry, TaskStatus

logger = logging.getLogger(__name__)

# 队列任务：只带 project/id/kind/版本，worker 消费时从 registry 拉最新 task
# （不把可变 proto 塞进队列造成跨线程共享，S 更新配置后 worker 拿到的就是新配置）。
Job = namedtuple("Job", ["project_id", "task_id", "kind", "config_version"])

# inflight 去重键：跨项目隔离（同 task_id 不同 project 是两条独立任务，可并发入队）
_job_key = lambda project_id, task_id: scoped_key(project_id, task_id)

_SENTINEL = object()   # 优雅退出哨兵


class Scheduler(threading.Thread):
    """固定周期扫描 + 双队列双 worker 池。"""

    def __init__(self, engine: AnalysisEngine, registry: TaskRegistry,
                 repository: ResultRepository,
                 interval_seconds: float = 10.0,
                 train_queue_size: int = 8,
                 infer_queue_size: int = 32,
                 train_workers: int = 1,
                 infer_workers: int = 2,
                 train_timeout_s: float = 300.0,
                 infer_timeout_s: float = 60.0):
        super().__init__(name="analysis-scheduler", daemon=True)
        self.engine = engine
        self.registry = registry
        self.repository = repository
        self.interval = interval_seconds
        # 单任务执行超时上限（秒）：0 或负 = 不限制。训练/推理耗时差一个量级，
        # 分开配置更合理（训练分钟级、推理秒级）。超时丢弃本轮、下 tick 重试。
        self._train_timeout_s = train_timeout_s
        self._infer_timeout_s = infer_timeout_s
        # producer 轮转起点：有界队列 + 固定顺序遍历会让前面任务占死队列、后面饿死，
        # 每 tick 从不同位置开始遍历，饱和时所有任务都有机会进队列（公平性）。
        self._tick_start = 0
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
        tasks = self.registry.enabled_tasks()
        n = len(tasks)
        if n:
            start = self._tick_start % n
            self._tick_start += 1
            tasks = tasks[start:] + tasks[:start]   # 轮转起点：防固定顺序饿死
        now_ms = int(time.time() * 1000)
        enq_train = enq_infer = skip_due = 0
        for rec in tasks:
            try:
                if self.engine.needs_training(rec.task, rec.kind,
                                              rec.config_version):
                    if rec.kind == TaskKind.FORECAST:
                        # 模型失效/版本变化需重训 → 解除预测 next_due 门控，
                        # 训完立刻出新一轮预测，不等旧的动态间隔
                        self.engine.reset_forecast_due(rec.project_id, rec.task_id)
                    elif rec.kind == TaskKind.ANOMALY:
                        # 异常同理：解除 next_due，重训完成立刻出新一轮检测
                        self.engine.reset_anomaly_due(rec.project_id, rec.task_id)
                    if self._enqueue(self._train_queue, self._train_inflight,
                                     rec, "train"):
                        enq_train += 1
                elif rec.kind == TaskKind.FORECAST and \
                        now_ms < self.engine.forecast_due_epoch(rec.project_id,
                                                                rec.task_id):
                    # 预测任务动态间隔：成功预测轮按「horizon × percent ÷ 频率」排了
                    # next_due，未到期 → 本轮跳过推理（省推理成本）
                    logger.debug("[scheduler] 任务 %s 未到预测到期时间，本轮跳过",
                                 rec.task_id)
                    skip_due += 1
                elif rec.kind == TaskKind.ANOMALY and \
                        now_ms < self.engine.anomaly_due_epoch(rec.project_id,
                                                               rec.task_id):
                    # 异常任务动态间隔：每次真检测按「窗口 × recheck_fraction ÷ 频率」
                    # 排 next_due（有异常→热节奏盯住，无→等整个新窗口）。未到期跳过；
                    # 数据不足/失败轮不设 due → 维持固定周期重试。
                    logger.debug("[scheduler] 任务 %s 未到异常检测到期时间，本轮跳过",
                                 rec.task_id)
                    skip_due += 1
                else:
                    if self._enqueue(self._infer_queue, self._infer_inflight,
                                     rec, "infer"):
                        enq_infer += 1
            except Exception:
                logger.exception("[scheduler] tick 中任务 %s 分派失败", rec.task_id)
        # 心跳：每秒一行（interval_seconds 粒度），联调看全局吞吐——
        # 本轮入队率 + 队列积压（满 = 吞吐跟不上）+ 动态间隔跳过数。
        if n:
            logger.info("[scheduler] tick：任务 %d 个，本轮入队 train=%d / infer=%d，"
                        "动态间隔跳过 %d，train_q=%d/%d infer_q=%d/%d",
                        n, enq_train, enq_infer, skip_due,
                        self._train_queue.qsize(), self._train_queue.maxsize,
                        self._infer_queue.qsize(), self._infer_queue.maxsize)

    def _enqueue(self, q: queue.Queue, inflight: set[str], rec: TaskRecord,
                 pool: str) -> bool:
        """入队：先加 inflight 再 put_nowait；队列满回滚 inflight 并跳过。

        先加 inflight 防「worker 已消费完、producer 后加」造成永久残留；
        队列满回滚并跳过，下个 tick 重试。返回是否真正入队（inflight 去重 / 满 = False）。
        去重键 = 复合键 project::task_id：同 task_id 不同 project 互不占位。
        """
        key = _job_key(rec.project_id, rec.task_id)
        with self._inflight_lock:
            if key in inflight:
                return False
            inflight.add(key)
        try:
            q.put_nowait(Job(rec.project_id, rec.task_id, rec.kind,
                             rec.config_version))
            return True
        except queue.Full:
            with self._inflight_lock:
                inflight.discard(key)
            logger.warning("[scheduler] %s 队列满（maxsize=%d），跳过 %s，下个 tick 重试",
                           pool, q.maxsize, rec.task_id)
            return False

    # ---- worker：消费执行 ----

    def _worker_loop(self, q: queue.Queue, inflight: set[str],
                     pool: str) -> None:
        while True:
            job = q.get()
            if job is _SENTINEL:
                q.task_done()
                return
            try:
                self._run_job(job, pool)
            except Exception:
                # 崩溃保护：_run_job 的重校验段在自身 try 之外，任何异常到这儿兜底，
                # 记日志 + 清 inflight + task_done，worker 线程永不死亡（继续消费）。
                logger.exception("[scheduler] %s worker 处理 %s 异常（worker 继续存活）",
                                 pool, job.task_id)
            finally:
                with self._inflight_lock:
                    inflight.discard(_job_key(job.project_id, job.task_id))
                q.task_done()

    def _run_job(self, job: Job, pool: str) -> None:
        """消费前重校验：disable/删除/类型重建/版本变更中途的任务一律丢弃。"""
        rec = self.registry.get(job.project_id, job.task_id)
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
        timeout = self._train_timeout_s if pool == "train" else self._infer_timeout_s
        if timeout and timeout > 0:
            self._run_job_with_timeout(rec, job, timeout, pool)
        else:
            self._execute_job(rec, job, settled=None)

    def _execute_job(self, rec: TaskRecord, job: Job,
                     settled: threading.Event | None = None) -> None:
        """执行一次任务：engine 调用 + 落库 + 失败计数。

        settled：超时路径的"本轮结算"标记。后台线程跑完时若已被置位（超时作废），
        结果不落库（超时后晚到的结果没有意义）；异常仍记 error_count（确实失败过）。
        非超时路径（settled=None）行为与旧版完全一致。
        """
        try:
            if rec.kind == TaskKind.ANOMALY:
                result = self.engine.run_anomaly(rec.task,
                                                 config_version=job.config_version)
            else:
                result = self.engine.run_forecast(rec.task,
                                                  config_version=job.config_version)
            if settled is None or not settled.is_set():
                self.repository.put(rec.project_id, rec.task_id, result)
        except Exception:
            rec.error_count += 1
            logger.exception("[scheduler] 任务 %s 执行失败（第 %d 次）",
                             rec.task_id, rec.error_count)
        finally:
            if settled is not None:
                settled.set()

    def _run_job_with_timeout(self, rec: TaskRecord, job: Job, timeout: float,
                              pool: str) -> None:
        """给单次任务执行设超时上限：到点丢弃本轮、释放 worker，下个 tick 重试。

        Python 杀不掉正在跑的线程，"超时"不是中断而是弃权：
          - 起一条 daemon 线程真正执行，worker 主线程 join(timeout) 等待；
          - 到点没结束 → 置作废标记，本轮不落库，worker 立即接下一个任务；
          - 后台线程最后自己跑完/失败（内部 gRPC 超时兜底，不会无限挂）。
        超时不计 error_count：是"这轮没跑完"，不是任务失败（默认任务一定成功的语义）。
        """
        settled = threading.Event()
        bg = threading.Thread(
            target=self._execute_job, args=(rec, job, settled),
            name=f"job-{pool}-{job.task_id}", daemon=True)
        bg.start()
        bg.join(timeout)
        if settled.is_set():
            return                                  # 限时内正常完成/失败
        settled.set()                               # 作废本轮：后台线程到点后不再落库
        logger.warning("[scheduler] 任务 %s 执行超时（>%.1fs，%s 池），丢弃本轮，下 tick 重试",
                       job.task_id, timeout, pool)

    # ---- 优雅退出 ----

    def _shutdown_workers(self) -> None:
        """丢已排队 job，每 worker 放一个哨兵，短 join（超时靠 daemon 兜底）。

        被丢弃的排队 job 同步从 inflight 摘除：它们从没被 worker 执行，
        finally 不会清它们，不摘会在停服后留下残留（无泄漏不变量）。
        """
        for q, workers, inflight in (
                (self._train_queue, self._train_workers, self._train_inflight),
                (self._infer_queue, self._infer_workers, self._infer_inflight)):
            while True:
                try:
                    job = q.get_nowait()
                except queue.Empty:
                    break
                if job is not _SENTINEL:
                    with self._inflight_lock:
                        inflight.discard(_job_key(job.project_id, job.task_id))
            for _ in workers:
                q.put_nowait(_SENTINEL)
        for w in self._train_workers + self._infer_workers:
            w.join(timeout=5)
