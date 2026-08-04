"""P 端对外 gRPC 服务（空壳版）。

实现 TimeseriesAnalysisService 的 7 个 RPC，供 S 端调用。
当前都是空壳：能接收请求、返回合理响应，证明 S↔P 通讯通了。
后续逐个填充真实业务逻辑。
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

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    """当前 UTC 时间（毫秒）。"""
    return int(time.time() * 1000)


class AnalysisServicer(pb_grpc.TimeseriesAnalysisServiceServicer):
    """7 个 RPC 的空壳实现。"""

    # ---- 任务配置同步 ----

    def SyncAnomalyTask(self, request, context):
        logger.info("SyncAnomalyTask: task_id=%s", request.task.task_id)
        return pb.TaskAck(
            task_id=request.task.task_id,
            accepted=True,
            status=pb.ANALYSIS_STATUS_SUCCESS,
            message="异常任务已接收（空壳）",
            updated_at_ms=_now_ms(),
        )

    def SyncForecastTask(self, request, context):
        logger.info("SyncForecastTask: task_id=%s", request.task.task_id)
        return pb.TaskAck(
            task_id=request.task.task_id,
            accepted=True,
            status=pb.ANALYSIS_STATUS_SUCCESS,
            message="预测任务已接收（空壳）",
            updated_at_ms=_now_ms(),
        )

    # ---- 任务状态 ----

    def UpdateTaskStatus(self, request, context):
        logger.info("UpdateTaskStatus: task_id=%s status=%s",
                    request.task_id, pb.TaskStatus.Name(request.status))
        return pb.TaskAck(
            task_id=request.task_id,
            accepted=True,
            status=pb.ANALYSIS_STATUS_SUCCESS,
            message=f"任务状态已更新为 {pb.TaskStatus.Name(request.status)}（空壳）",
            updated_at_ms=_now_ms(),
        )

    # ---- 结果查询 ----

    def QueryAnomalyResults(self, request, context):
        logger.info("QueryAnomalyResults: task_id=%s", request.query.task_id)
        return pb.QueryAnomalyResultsResponse(
            task_id=request.query.task_id,
            results=[],
        )

    def QueryForecastResults(self, request, context):
        logger.info("QueryForecastResults: task_id=%s", request.query.task_id)
        return pb.QueryForecastResultsResponse(
            task_id=request.query.task_id,
            results=[],
        )

    # ---- 归因建议（空壳，返回 NOT_IMPLEMENTED）----

    def GenerateDiagnosis(self, request, context):
        logger.info("GenerateDiagnosis: event_id=%s", request.event_id)
        return pb.DecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="异常归因尚未实现",
            content="",
            basis_ids=[],
        )

    def GenerateSuggestion(self, request, context):
        logger.info("GenerateSuggestion: event_id=%s", request.event_id)
        return pb.DecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="优化建议尚未实现",
            content="",
            basis_ids=[],
        )
