"""P 端对外 gRPC 服务：实现 TimeseriesAnalysisService 的 7 个 RPC。

注册制：SyncXxxTask 只把任务登记进 TaskRegistry（立即 ACK，不跑），
执行交给 Scheduler 周期调度；Query 从 ResultRepository 轮询结果；
UpdateTaskStatus 真正改变任务启停。归因两 RPC 保持空壳（NOT_IMPLEMENTED）。
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_analysis_pb2 as pb
import timeseries_analysis_pb2_grpc as pb_grpc

from task_registry import TaskKind, TaskStatus

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    """当前 UTC 时间（毫秒）。"""
    return int(time.time() * 1000)


# S 端任务状态枚举 → 本地状态机（proto 无 ERROR，暂不映射）
_STATUS_MAP = {
    pb.TASK_STATUS_ENABLED: TaskStatus.ENABLED,
    pb.TASK_STATUS_DISABLED: TaskStatus.DISABLED,
    pb.TASK_STATUS_DELETED: TaskStatus.DELETED,
}


def _ack(task_id: str, accepted: bool, status, message: str) -> pb.TaskAck:
    return pb.TaskAck(task_id=task_id, accepted=accepted,
                      status=status, message=message, updated_at_ms=_now_ms())


class AnalysisServicer(pb_grpc.TimeseriesAnalysisServiceServicer):
    """实现 S 定义的 TimeseriesAnalysisService 的 7 个 RPC。

    职责边界：注册任务、改状态、查结果。执行在 Scheduler，这里不跑引擎，
    每个 RPC 都是快路径（毫秒级返回，不阻塞 S）。
    """

    def __init__(self, registry, repository, engine=None):
        self._registry = registry        # TaskRegistry：任务配置 + 启停状态
        self._repository = repository    # ResultRepository：每任务最近结果
        self._engine = engine            # 保留引用（供工具/测试直接调用）

    # ---- 任务配置同步（注册制：只登记不跑，立即 ACK）----

    def SyncAnomalyTask(self, request, context):
        task = request.task
        self._registry.register(task, TaskKind.ANOMALY)
        logger.info("SyncAnomalyTask: task_id=%s 已注册（调度器周期执行）",
                    task.task_id)
        return _ack(task.task_id, True, pb.ANALYSIS_STATUS_SUCCESS,
                    "任务已注册，由调度器周期执行")

    def SyncForecastTask(self, request, context):
        task = request.task
        self._registry.register(task, TaskKind.FORECAST)
        logger.info("SyncForecastTask: task_id=%s 已注册（调度器周期执行）",
                    task.task_id)
        return _ack(task.task_id, True, pb.ANALYSIS_STATUS_SUCCESS,
                    "任务已注册，由调度器周期执行")

    # ---- 任务状态（真正生效：停用/删除/恢复）----

    def UpdateTaskStatus(self, request, context):
        local = _STATUS_MAP.get(request.status)
        if local is None:
            logger.warning("UpdateTaskStatus: 未知任务状态 %s（task_id=%s）",
                           request.status, request.task_id)
            return _ack(request.task_id, False,
                        pb.ANALYSIS_STATUS_INVALID_REQUEST, "未知任务状态")
        ok = self._registry.set_status(request.task_id, local)
        if not ok:
            return _ack(request.task_id, False, pb.ANALYSIS_STATUS_FAILED,
                        "任务不存在")
        name = pb.AnalysisTaskStatus.Name(request.status)
        logger.info("UpdateTaskStatus: task_id=%s → %s", request.task_id, name)
        return _ack(request.task_id, True, pb.ANALYSIS_STATUS_SUCCESS,
                    f"任务状态已更新为 {name}")

    # ---- 结果查询（轮询 ResultRepository）----

    def QueryAnomalyResults(self, request, context):
        q = request.query
        logger.info("QueryAnomalyResults: task_id=%s", q.task_id)
        results = self._query_results(q)
        return pb.QueryAnomalyResultsResponse(task_id=q.task_id, results=results)

    def QueryForecastResults(self, request, context):
        q = request.query
        logger.info("QueryForecastResults: task_id=%s", q.task_id)
        results = self._query_results(q)
        return pb.QueryForecastResultsResponse(task_id=q.task_id, results=results)

    def _query_results(self, q) -> list:
        """按 ResultQuery 取结果：latest_only 取最近一条，否则取历史（limit 限条数）。"""
        if q.latest_only:
            latest = self._repository.latest(q.task_id)
            return [latest] if latest is not None else []
        return self._repository.history(q.task_id, limit=q.limit or None)

    # ---- 归因建议（空壳，返回 NOT_IMPLEMENTED）----

    def GenerateDiagnosis(self, request, context):
        logger.info("GenerateDiagnosis: event_id=%s", request.event_id)
        return pb.AnalysisDecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="异常归因尚未实现", content="", basis_ids=[])

    def GenerateSuggestion(self, request, context):
        logger.info("GenerateSuggestion: event_id=%s", request.event_id)
        return pb.AnalysisDecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="优化建议尚未实现", content="", basis_ids=[])
