"""固定周期调度器：后台线程，周期跑所有 ENABLED 任务。

异常任务 → engine.run_anomaly；预测任务 → engine.run_forecast。
结果写 ResultRepository；单任务失败捕获，不影响其他任务。
"""

from __future__ import annotations

import logging
import threading

from task_registry import TaskKind

logger = logging.getLogger(__name__)


class Scheduler(threading.Thread):
    """固定周期触发：每 interval_seconds 跑一次所有 ENABLED 任务。"""

    def __init__(self, engine, registry, repository,
                 interval_seconds: float = 10.0):
        super().__init__(name="analysis-scheduler", daemon=True)
        self.engine = engine
        self.registry = registry
        self.repository = repository
        self.interval = interval_seconds
        # 注意：不能叫 _stop——Thread 内部有同名方法（join 依赖），覆盖会崩
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """请求停止；当前 tick 跑完后退出。"""
        self._stop_event.set()

    def run(self) -> None:
        logger.info("[scheduler] 启动，周期 %.1fs", self.interval)
        while not self._stop_event.wait(self.interval):
            self._tick()

    def _tick(self) -> None:
        for rec in self.registry.enabled_tasks():
            try:
                if rec.kind == TaskKind.ANOMALY:
                    result = self.engine.run_anomaly(rec.task)
                else:
                    result = self.engine.run_forecast(rec.task)
                self.repository.put(rec.task_id, result)
            except Exception:
                rec.error_count += 1
                logger.exception("[scheduler] 任务 %s 执行失败（第 %d 次）",
                                 rec.task_id, rec.error_count)
