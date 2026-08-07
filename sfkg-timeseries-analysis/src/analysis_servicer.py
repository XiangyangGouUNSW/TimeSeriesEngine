"""P 端对外 gRPC 服务：实现 TimeseriesAnalysisService 的 7 个 RPC。

接上 AnalysisEngine 后，收到任务会真正走调用链：
  收任务 → 查C数据规模 → 训练 → 预测 → 调C约束检查 → 调S写事件
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
    """实现 S 定义的 TimeseriesAnalysisService 的 7 个 RPC。"""

    def __init__(self, engine=None):
        self._engine = engine           # AnalysisEngine（调用链）
        self._tasks = {}                # task_id -> 任务配置
        self._forecast_results = {}     # task_id -> 最新 ForecastResult
        self._anomaly_results = {}      # task_id -> 最新 AnomalyResult

    # ---- 任务配置同步 ----

    def SyncAnomalyTask(self, request, context):
        task = request.task
        self._tasks[task.task_id] = task
        logger.info("SyncAnomalyTask: task_id=%s", task.task_id)
        if self._engine:
            result = self._engine.run_anomaly(task)
            self._anomaly_results[task.task_id] = result
            return pb.TaskAck(task_id=task.task_id, accepted=True,
                              status=result.status, message=result.message,
                              updated_at_ms=_now_ms())
        return pb.TaskAck(task_id=task.task_id, accepted=True,
                          status=pb.ANALYSIS_STATUS_SUCCESS,
                          message="异常任务已接收（无引擎）", updated_at_ms=_now_ms())

    def SyncForecastTask(self, request, context):
        task = request.task
        self._tasks[task.task_id] = task
        logger.info("SyncForecastTask: task_id=%s config_version=%s",
                    task.task_id, request.config_version)
        if self._engine:
            result = self._engine.run_forecast(task,
                                               config_version=request.config_version)
            self._forecast_results[task.task_id] = result
            return pb.TaskAck(task_id=task.task_id, accepted=True,
                              status=result.status, message=result.message,
                              updated_at_ms=_now_ms())
        return pb.TaskAck(task_id=task.task_id, accepted=True,
                          status=pb.ANALYSIS_STATUS_SUCCESS,
                          message="预测任务已接收（无引擎）", updated_at_ms=_now_ms())

    # ---- 任务状态 ----

    def UpdateTaskStatus(self, request, context):
        logger.info("UpdateTaskStatus: task_id=%s status=%s",
                    request.task_id, pb.AnalysisTaskStatus.Name(request.status))
        return pb.TaskAck(task_id=request.task_id, accepted=True,
                          status=pb.ANALYSIS_STATUS_SUCCESS,
                          message=f"任务状态已更新为 {pb.AnalysisTaskStatus.Name(request.status)}",
                          updated_at_ms=_now_ms())

    # ---- 结果查询 ----

    def QueryAnomalyResults(self, request, context):
        logger.info("QueryAnomalyResults: task_id=%s", request.query.task_id)
        result = self._anomaly_results.get(request.query.task_id)
        return pb.QueryAnomalyResultsResponse(
            task_id=request.query.task_id,
            results=[result] if result else [])

    def QueryForecastResults(self, request, context):
        logger.info("QueryForecastResults: task_id=%s", request.query.task_id)
        result = self._forecast_results.get(request.query.task_id)
        return pb.QueryForecastResultsResponse(
            task_id=request.query.task_id,
            results=[result] if result else [])

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
