"""S 端客户端：P 检测到异常/预警后，调 S 的 ReceiveAnomalyResult（写事件）。

S 端提供 AnalysisResultReceiverService 服务（proto 见 timeseries_analysis.proto），
P 端作为客户端调用，告诉 S"发现异常/预警"，S 负责写图谱。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import grpc

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_analysis_pb2 as pb
import timeseries_analysis_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)


class AnalysisResultClient:
    """调 S 端 AnalysisResultReceiverService.ReceiveAnomalyResult 的客户端。"""

    def __init__(self, address: str = "localhost", port: int = 50054,
                 timeout_seconds: float = 10.0):
        self._stub = pb_grpc.AnalysisResultReceiverServiceStub(
            grpc.insecure_channel(f"{address}:{port}"))
        self._timeout = timeout_seconds

    def send_event(
        self,
        *,
        task_id: str,               # 产生该事件的任务（S 端写图谱时关联任务）
        project_id: str = "",       # 归属项目（S 端按项目写图谱/隔离知识）
        event_type: int,            # pb.ANOMALY_EVENT_TYPE_ANOMALY / _WARNING
        event_time_ms: int,         # 实际发生或预测触发时刻
        sequence_ids: list[str],    # 关联序列
        values: list[float],        # 异常/预警数值
        severity: int,              # pb.SEVERITY_LOW / _MEDIUM / _HIGH
        source: int,                # pb.ANOMALY_SOURCE_...
    ) -> bool:
        """把异常/预警结果发给 S，S 写图谱。成功返回 True。"""
        msg = pb.AnomalyResultMessage(
            task_id=task_id,
            project_id=project_id,
            event_type=event_type,
            event_time_ms=event_time_ms,
            sequence_ids=list(sequence_ids),
            values=list(values),
            severity=severity,
            source=source,
        )
        try:
            self._stub.ReceiveAnomalyResult(msg, timeout=self._timeout)
            return True
        except grpc.RpcError as e:
            logger.info("[AnalysisResultClient] 调 S 失败（S 未起或连不上）: %s", e)
            return False
