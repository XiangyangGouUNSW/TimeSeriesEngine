"""任务注册表：任务配置 + 启停状态机，供 Scheduler 周期执行。

任务生命周期：
  SyncXxxTask 注册（默认 ENABLED）→ Scheduler 周期跑 ENABLED 任务
  → UpdateTaskStatus 启停/删除 → DELETED 移除。

隔离：project_id 是项目/租户隔离键。业务 ID（task_id）保持原样，容器按
`project::task_id` 复合键分桶（project.scoped_key）——不同项目的同 task_id
互不串。空 project_id 由调用方归一化为 "default"。
"""

from __future__ import annotations

import enum
import threading
from typing import Any

from project import scoped_key


class TaskKind(str, enum.Enum):
    ANOMALY = "anomaly"
    FORECAST = "forecast"


class TaskStatus(str, enum.Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"
    DELETED = "deleted"


class TaskRecord:
    """一条任务：配置 + 当前状态 + 配置版本。

    config_version 来自 Sync 请求外层（int64 config_version），
    模型缓存 key 带版本（{task_id}@v{ver}），版本变 → key 变 → 必然重训。
    project_id 为该任务所属项目（已归一化），调度器/引擎据此取复合键。
    """

    def __init__(self, project_id: str, task_id: str, kind: TaskKind,
                 task: Any,  # proto 任务配置：pb.AnomalyTaskConfig | pb.ForecastTaskConfig
                 status: TaskStatus = TaskStatus.ENABLED,
                 config_version: int = 0):
        self.project_id = project_id
        self.task_id = task_id
        self.kind = kind
        self.task = task
        self.status = status
        self.config_version = config_version
        self.error_count = 0


class TaskRegistry:
    """任务注册表：新建=ENABLED；更新配置保留原状态；启停/删除真正生效。"""

    def __init__(self):
        self._tasks: dict[str, TaskRecord] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(project_id: str, task_id: str) -> str:
        return scoped_key(project_id, task_id)

    def register(self, project_id: str,
                 task: Any,  # proto 任务配置：pb.AnomalyTaskConfig | pb.ForecastTaskConfig
                 kind: TaskKind, config_version: int = 0) -> TaskRecord:
        """注册（新建=ENABLED）或更新（保留原状态 + 同步配置版本）。"""
        with self._lock:
            key = self._key(project_id, task.task_id)
            rec = self._tasks.get(key)
            if rec is None:
                rec = TaskRecord(project_id=project_id, task_id=task.task_id,
                                 kind=kind, task=task,
                                 config_version=config_version)
                self._tasks[key] = rec
            else:
                rec.task = task      # 更新配置，不改变启停状态
                rec.config_version = config_version
            return rec

    def set_status(self, project_id: str, task_id: str, status: TaskStatus) -> bool:
        """改状态；DELETED 后移除。任务不存在返回 False。"""
        with self._lock:
            key = self._key(project_id, task_id)
            rec = self._tasks.get(key)
            if rec is None:
                return False
            if status == TaskStatus.DELETED:
                del self._tasks[key]
            else:
                rec.status = status
            return True

    def enabled_tasks(self) -> list[TaskRecord]:
        """Scheduler 遍历用：ENABLED 任务快照（DELETED 已移除，DISABLED/ERROR 不执行）。"""
        with self._lock:
            return [r for r in self._tasks.values() if r.status == TaskStatus.ENABLED]

    def get(self, project_id: str, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._tasks.get(self._key(project_id, task_id))

    def is_enabled(self, project_id: str, task_id: str) -> bool:
        """任务存在且 ENABLED（worker 消费前校验：disable/删除中途的任务不执行）。"""
        with self._lock:
            rec = self._tasks.get(self._key(project_id, task_id))
            return rec is not None and rec.status == TaskStatus.ENABLED

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)
