"""project_id 项目/租户隔离辅助。

参考分支（timeseries-core C++ / timeseries-service Java）的落地模式：
  - 业务 ID（task_id / relation_id 等）保持原样，不把 project 拼进业务 ID；
  - 隔离通过扁平容器 + 复合键实现（project_id + 分隔符 + id）；
  - 空 project_id 统一归一化为 "default"（兼容旧客户端）；
  - 请求级字段比嵌套字段权威（调用方决定用哪个候选值）。

分隔符用 "::"（与 Java 参考的 cacheKey 一致）；task_id 为业务生成的 UUID，
不含分隔符。
"""

from __future__ import annotations

DEFAULT_PROJECT = "default"
_SEP = "::"


def normalize_project(project_id: str | None) -> str:
    """空值归一化为 DEFAULT_PROJECT；非空原样返回。"""
    return project_id if project_id else DEFAULT_PROJECT


def scoped_key(project_id: str, *ids: str) -> str:
    """复合键：`project::id[::...]`，业务 ID 保持原样。"""
    return _SEP.join([normalize_project(project_id), *ids])
