"""结果仓库：每任务保留最近 N 条结果，供 Query 轮询查询。

替代 servicer 裸 dict：锁内聚到仓库类，put/latest/history 线程安全。
按 `project::task_id` 复合键分桶（project.scoped_key）——不同项目的同
task_id 结果互不可见。
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any

from project import scoped_key


class ResultRepository:
    """(project_id, task_id) -> deque(maxlen)，自动丢最旧结果。"""

    def __init__(self, maxlen: int = 50):
        self._maxlen = maxlen
        self._results: dict[str, deque] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(project_id: str, task_id: str) -> str:
        return scoped_key(project_id, task_id)

    def put(self, project_id: str, task_id: str, result: Any) -> None:
        """追加一条结果；超 maxlen 自动丢最旧。"""
        with self._lock:
            key = self._key(project_id, task_id)
            q = self._results.get(key)
            if q is None:
                q = deque(maxlen=self._maxlen)
                self._results[key] = q
            q.append(result)

    def latest(self, project_id: str, task_id: str) -> Any:
        """最近一条结果；任务无结果返回 None。"""
        with self._lock:
            q = self._results.get(self._key(project_id, task_id))
            return q[-1] if q else None

    def history(self, project_id: str, task_id: str,
                limit: int | None = None) -> list:
        """结果列表（旧→新）；limit 限最近 N 条。"""
        with self._lock:
            q = self._results.get(self._key(project_id, task_id))
            if not q:
                return []
            items = list(q)
            return items[-limit:] if limit else items
