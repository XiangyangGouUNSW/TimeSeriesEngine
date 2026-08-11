"""结果仓库：每任务保留最近 N 条结果，供 Query 轮询查询。

替代 servicer 裸 dict：锁内聚到仓库类，put/latest/history 线程安全。
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any


class ResultRepository:
    """task_id -> deque(maxlen)，自动丢最旧结果。"""

    def __init__(self, maxlen: int = 50):
        self._maxlen = maxlen
        self._results: dict[str, deque] = {}
        self._lock = threading.RLock()

    def put(self, task_id: str, result: Any) -> None:
        """追加一条结果；超 maxlen 自动丢最旧。"""
        with self._lock:
            q = self._results.get(task_id)
            if q is None:
                q = deque(maxlen=self._maxlen)
                self._results[task_id] = q
            q.append(result)

    def latest(self, task_id: str) -> Any:
        """最近一条结果；任务无结果返回 None。"""
        with self._lock:
            q = self._results.get(task_id)
            return q[-1] if q else None

    def history(self, task_id: str, limit: int | None = None) -> list:
        """结果列表（旧→新）；limit 限最近 N 条。"""
        with self._lock:
            q = self._results.get(task_id)
            if not q:
                return []
            items = list(q)
            return items[-limit:] if limit else items
