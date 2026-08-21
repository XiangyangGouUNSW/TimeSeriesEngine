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

import grpc

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_analysis_pb2 as pb
import timeseries_analysis_pb2_grpc as pb_grpc

from analysis_engine import AnalysisEngine
from project import normalize_project
from result_repository import ResultRepository
from task_registry import TaskKind, TaskRegistry, TaskStatus

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


def _ack(task_id: str, accepted: bool, status: int, message: str) -> pb.TaskAck:
    return pb.TaskAck(task_id=task_id, accepted=accepted,
                      status=status, message=message, updated_at_ms=_now_ms())


class AnalysisServicer(pb_grpc.TimeseriesAnalysisServiceServicer):
    """实现 S 定义的 TimeseriesAnalysisService 的 7 个 RPC。

    职责边界：注册任务、改状态、查结果。执行在 Scheduler，这里不跑引擎，
    每个 RPC 都是快路径（毫秒级返回，不阻塞 S）。
    """

    def __init__(self, registry: TaskRegistry, repository: ResultRepository,
                 engine: AnalysisEngine | None = None):
        self._registry = registry        # TaskRegistry：任务配置 + 启停状态
        self._repository = repository    # ResultRepository：每任务最近结果
        self._engine = engine            # 保留引用（供工具/测试直接调用）

    # ---- 项目归属解析（请求级权威，回写 task 供下游直接使用）----

    def _resolve_project(self, meta, task=None, project_id="") -> str:
        """解析并回写任务归属项目，返回归一化 project_id。

        权威顺序：显式 project_id（UpdateTaskStatus/Query/Decision 请求的直接字段）
        → meta.project_id → task.project_id → task.semantic_context.project_id
        → "default"。归一化后回写 task 及其 semantic_context 的 project_id——
        下游（引擎/调度器）读 task 对象即可拿对项目，不必每处重解析。
        task 为 None（查询/状态类请求无任务体）时只归一化返回。
        """
        pid = (project_id or "").strip()
        if not pid and meta is not None:
            pid = (getattr(meta, "project_id", "") or "").strip()
        if not pid and task is not None:
            pid = (getattr(task, "project_id", "") or "").strip()
        if not pid and task is not None:
            sc = getattr(task, "semantic_context", None)
            if sc is not None:
                pid = (getattr(sc, "project_id", "") or "").strip()
        pid = normalize_project(pid)
        if task is not None:
            task.project_id = pid
            sc = getattr(task, "semantic_context", None)
            if sc is not None:
                sc.project_id = pid
        return pid

    # ---- 任务配置同步（注册制：只登记不跑，立即 ACK）----

    def _sync_task(
        self,
        request: pb.AnalysisSyncAnomalyTaskRequest | pb.AnalysisSyncForecastTaskRequest,
        kind: TaskKind,
        name: str,
    ) -> pb.TaskAck:
        """注册/更新任务并同步配置版本；版本变化时清理旧版本模型（保留最近 2 版）。

        模型缓存 key 带版本，版本变 → key 变 → 下个 tick 必然重训；
        invalidate_task 只做磁盘/内存清理（保留当前版本，回滚到上一个版本可复用）。
        project_id 由 meta/任务/语义上下文逐级解析并回写 task。
        """
        task = request.task
        ver = int(request.config_version)
        pid = self._resolve_project(request.meta, task)
        old = self._registry.get(pid, task.task_id)
        self._registry.register(pid, task, kind, ver)
        if (old is not None and old.config_version != ver
                and self._engine is not None):
            self._engine.invalidate_task(pid, task.task_id, keep_version=ver)
        logger.info("Sync%s: project=%s task_id=%s config_version=%d 已注册"
                    "（调度器周期执行）",
                    name, pid, task.task_id, ver)
        return _ack(task.task_id, True, pb.ANALYSIS_STATUS_SUCCESS,
                    "任务已注册，由调度器周期执行")

    def SyncAnomalyTask(self, request: pb.AnalysisSyncAnomalyTaskRequest,
                        context: grpc.ServicerContext) -> pb.TaskAck:
        return self._sync_task(request, TaskKind.ANOMALY, "AnomalyTask")

    def SyncForecastTask(self, request: pb.AnalysisSyncForecastTaskRequest,
                         context: grpc.ServicerContext) -> pb.TaskAck:
        return self._sync_task(request, TaskKind.FORECAST, "ForecastTask")

    # ---- 任务状态（真正生效：停用/删除/恢复）----

    def UpdateTaskStatus(self, request: pb.AnalysisUpdateTaskStatusRequest,
                         context: grpc.ServicerContext) -> pb.TaskAck:
        pid = self._resolve_project(request.meta, project_id=request.project_id)
        local = _STATUS_MAP.get(request.status)
        if local is None:
            logger.warning("UpdateTaskStatus: 未知任务状态 %s（project=%s task_id=%s）",
                           request.status, pid, request.task_id)
            return _ack(request.task_id, False,
                        pb.ANALYSIS_STATUS_INVALID_REQUEST, "未知任务状态")
        ok = self._registry.set_status(pid, request.task_id, local)
        if not ok:
            return _ack(request.task_id, False, pb.ANALYSIS_STATUS_FAILED,
                        "任务不存在")
        # 删除任务时清掉它的模型（生命周期卫生：删任务不该留孤儿模型文件）
        if local == TaskStatus.DELETED and self._engine is not None:
            self._engine.invalidate_task(pid, request.task_id)
        name = pb.AnalysisTaskStatus.Name(request.status)
        logger.info("UpdateTaskStatus: project=%s task_id=%s → %s",
                    pid, request.task_id, name)
        return _ack(request.task_id, True, pb.ANALYSIS_STATUS_SUCCESS,
                    f"任务状态已更新为 {name}")

    # ---- 结果查询（轮询 ResultRepository）----

    def QueryAnomalyResults(self, request: pb.QueryAnomalyResultsRequest,
                            context: grpc.ServicerContext) -> pb.QueryAnomalyResultsResponse:
        q = request.query
        pid = self._resolve_project(q.meta, project_id=q.project_id)
        logger.info("QueryAnomalyResults: project=%s task_id=%s", pid, q.task_id)
        results = self._query_results(q, pid, pb.AnomalyResult)
        return pb.QueryAnomalyResultsResponse(task_id=q.task_id, results=results)

    def QueryForecastResults(self, request: pb.QueryForecastResultsRequest,
                             context: grpc.ServicerContext) -> pb.QueryForecastResultsResponse:
        q = request.query
        pid = self._resolve_project(q.meta, project_id=q.project_id)
        logger.info("QueryForecastResults: project=%s task_id=%s", pid, q.task_id)
        results = self._query_results(q, pid, pb.ForecastResult)
        return pb.QueryForecastResultsResponse(task_id=q.task_id, results=results)

    def _query_results(self, q: pb.ResultQuery, project_id: str,
                       expected: type) -> list:
        """按 ResultQuery 取结果：latest_only 取最近一条，否则取历史（limit 限条数）。

        仓库为空但任务已注册且模型未就绪 → 合成一条 MODEL_NOT_READY 结果（正常结果，
        表示「任务在跑、首训还没完成」，S 可继续轮询，不必报错）。未知任务 /
        模型已就绪 → 返回空。expected 过滤类型不匹配的合成结果（查异常端点的预测任务
        不返回预测格式，返回空而非崩溃）。
        """
        """按 ResultQuery 取结果：latest_only 取最近一条，否则取历史（limit 限条数）。

        仓库为空但任务已注册且模型未就绪 → 合成一条 MODEL_NOT_READY 结果（正常结果，
        表示「任务在跑、首训还没完成」，S 可继续轮询，不必报错）。未知任务 /
        模型已就绪 → 返回空。
        """
        if q.latest_only:
            latest = self._repository.latest(project_id, q.task_id)
            if latest is not None:
                results = [latest]
            else:
                not_ready = self._model_not_ready_result(project_id, q.task_id)
                results = [not_ready] if not_ready is not None else []
        else:
            results = self._repository.history(project_id, q.task_id,
                                               limit=q.limit or None)
            if not results:
                not_ready = self._model_not_ready_result(project_id, q.task_id)
                results = [not_ready] if not_ready is not None else []
        return [r for r in results if isinstance(r, expected)]

    def _model_not_ready_result(self, project_id: str,
                                task_id: str) -> pb.AnomalyResult | pb.ForecastResult | None:
        """任务已注册且模型未就绪 → 合成一条 MODEL_NOT_READY 结果；否则 None。

        判断模型未就绪 = 引擎已注入 且 needs_training 为 True（模型按当前配置版本
        还没训好）。覆盖「注册后、首训完成前」的查询窗口；数据不足时引擎会写
        DATA_NOT_READY 进仓库，S 先拿到那条真实结果，不走到这里。按任务类型返回
        对应格式（AnomalyResult / ForecastResult），status 都是 MODEL_NOT_READY。
        """
        rec = self._registry.get(project_id, task_id)
        if rec is None or self._engine is None:
            return None
        if not self._engine.needs_training(rec.task, rec.kind, rec.config_version):
            return None
        message = "模型未就绪（等待训练完成），请稍后再查"
        if rec.kind == TaskKind.FORECAST:
            return pb.ForecastResult(
                task_id=task_id, run_id="", generated_at_ms=_now_ms(),
                status=pb.ANALYSIS_STATUS_MODEL_NOT_READY, message=message,
                timestamps_ms=[], sequence_ids=[], values=[],
                risk_findings=[], model_version="")
        return pb.AnomalyResult(
            task_id=task_id, run_id="", generated_at_ms=_now_ms(),
            status=pb.ANALYSIS_STATUS_MODEL_NOT_READY, message=message,
            findings=[], model_version="")

    # ---- 归因建议（空壳，返回 NOT_IMPLEMENTED）----

    def GenerateDiagnosis(self, request: pb.AnalysisDecisionRequest,
                          context: grpc.ServicerContext) -> pb.AnalysisDecisionResult:
        pid = self._resolve_project(request.meta, project_id=request.project_id)
        logger.info("GenerateDiagnosis: project=%s event_id=%s", pid,
                    request.event_id)
        return pb.AnalysisDecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="异常归因尚未实现", content="", basis_ids=[])

    def GenerateSuggestion(self, request: pb.AnalysisDecisionRequest,
                           context: grpc.ServicerContext) -> pb.AnalysisDecisionResult:
        pid = self._resolve_project(request.meta, project_id=request.project_id)
        logger.info("GenerateSuggestion: project=%s event_id=%s", pid,
                    request.event_id)
        return pb.AnalysisDecisionResult(
            event_id=request.event_id,
            status=pb.ANALYSIS_STATUS_NOT_IMPLEMENTED,
            message="优化建议尚未实现", content="", basis_ids=[])
