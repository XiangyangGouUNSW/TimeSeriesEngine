"""GrpcCoreDataClient：通过 gRPC 访问真正的 C 端（sfkg-timeseries-core）。

实现 CoreDataClient 的方法：
  get_sequence_data_scale → queryHistoryOverview（数据规模）
  get_history             → queryHistoryData + raw_points_to_aligned（历史数据）
  get_aligned_window      → queryHistoryData 拉末尾拼（窗口）
  get_real_time_window    → queryWindowData（实时窗口，异常检测输入）
  check_constraints       → checkConstraints（约束检查，异常/预测共用）

用法：config.yaml 的 core.provider 改成 "grpc" 后，main.py 自动用这个类。
"""

from __future__ import annotations

import sys
from pathlib import Path

import grpc

from core_client import CoreDataClient, raw_points_to_aligned
from data_types import SequenceDataScale, HistoricalDataChunk, AlignedWindow

# 让生成的 stub 可以直接 import（flat 方式，避免生成代码的绝对 import 出问题）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_core_pb2 as pb
import timeseries_core_pb2_grpc as pb_grpc


class CoreDataException(Exception):
    """C 端 gRPC 调用失败。上层 catch 后转成 UPSTREAM_UNAVAILABLE。"""


class GrpcCoreDataClient(CoreDataClient):
    def __init__(self, address: str = "localhost", port: int = 50051,
                 timeout_seconds: float = 30.0):
        # 调大接收上限：历史数据一次可能很大（ETTm1 约 30MB）
        channel = grpc.insecure_channel(
            f"{address}:{port}",
            options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)],
        )
        self._stub = pb_grpc.TimeseriesCoreServiceStub(channel)
        self._timeout = timeout_seconds

    # ---- 工具 ----

    def _check(self, response) -> None:
        """检查 operation.code；不是 OK 就抛异常。"""
        if response.operation.code != pb.OPERATION_CODE_OK:
            raise CoreDataException(
                f"Core 返回 {pb.OperationCode.Name(response.operation.code)}: "
                f"{response.operation.message}")

    def _call(self, method, request):
        """统一包一层：gRPC 网络错误转成 CoreDataException。"""
        try:
            resp = method(request, timeout=self._timeout)
            self._check(resp)
            return resp
        except grpc.RpcError as e:
            raise CoreDataException(f"gRPC 调用失败: {e}") from e

    # ---- 三个能力 ----

    def get_sequence_data_scale(self, sequence_ids: list[str]) -> list[SequenceDataScale]:
        resp = self._call(
            self._stub.queryHistoryOverview,
            pb.QueryHistoryOverviewRequest(sequence_ids=sequence_ids),
        )
        by_id = {s.sequence_id: s for s in resp.overview.series}
        result = []
        for sid in sequence_ids:
            s = by_id.get(sid)
            result.append(SequenceDataScale(
                sequence_id=sid,
                point_count=s.point_count if s else 0,
                start_time_ms=s.first_time if s else None,
                end_time_ms=s.last_time if s else None,
            ))
        return result

    def get_history(
        self,
        sequence_ids: list[str],
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> HistoricalDataChunk:
        # C 的 queryHistoryData 必须给时间范围；没给就用 overview 补全
        if start_time_ms is None or end_time_ms is None:
            scales = self.get_sequence_data_scale(sequence_ids)
            starts = [s.start_time_ms for s in scales if s.start_time_ms is not None]
            ends = [s.end_time_ms for s in scales if s.end_time_ms is not None]
            start_time_ms = start_time_ms if start_time_ms is not None else (min(starts) if starts else None)
            end_time_ms = end_time_ms if end_time_ms is not None else (max(ends) if ends else None)
        if start_time_ms is None or end_time_ms is None:
            raise CoreDataException("查询不到数据范围，无法拉历史")

        resp = self._call(
            self._stub.queryHistoryData,
            pb.QueryHistoryDataRequest(
                sequence_ids=sequence_ids,
                start_time=start_time_ms,
                end_time=end_time_ms,
            ),
        )
        points = [p for p in (self._to_point(p) for p in resp.data.points) if p is not None]
        timestamps, rows = raw_points_to_aligned(points, sequence_ids)
        return HistoricalDataChunk(
            timestamps_ms=timestamps,
            sequence_ids=sequence_ids,
            values=rows,
            is_last_chunk=True,
        )

    def get_aligned_window(
        self,
        sequence_ids: list[str],
        window_size: int,
        end_time_ms: int | None = None,
    ) -> AlignedWindow:
        # 暂时用历史数据拉末尾一段拼（等 C 的 queryWindowData 做好再换）
        scales = self.get_sequence_data_scale(sequence_ids)
        ends = [s.end_time_ms for s in scales if s.end_time_ms is not None]
        end = end_time_ms if end_time_ms is not None else (max(ends) if ends else None)
        if end is None:
            raise CoreDataException("查询不到数据范围，无法取窗口")
        # 按平均采样间隔推一个足够大的范围（2 倍余量）
        span = 0
        for s in scales:
            if s.point_count > 1 and s.start_time_ms is not None and s.end_time_ms is not None:
                interval = (s.end_time_ms - s.start_time_ms) / (s.point_count - 1)
                span = max(span, int(interval * window_size * 2) + 1)
        start = end - span if span else end - 3600_000 * window_size
        chunk = self.get_history(sequence_ids, start_time_ms=start, end_time_ms=end)
        return AlignedWindow(
            timestamps_ms=chunk.timestamps_ms[-window_size:],
            sequence_ids=sequence_ids,
            values=chunk.values[-window_size:],
        )

    def get_real_time_window(self, sequence_ids: list[str]):
        """调 C 的 queryWindowData 取实时窗口（异常检测的模型输入）。

        返回 C 的 WindowData（按序列组织的原始点，未对齐）。
        """
        resp = self._call(
            self._stub.queryWindowData,
            pb.QueryWindowDataRequest(sequence_ids=list(sequence_ids)),
        )
        return resp.data

    def check_constraints(
        self,
        constraint_ids: list[str],
        sequence_ids: list[str] | None = None,
        aligned_data=None,
    ):
        """调 C 的 checkConstraints 做约束检查。

        - 异常检测：传 sequence_ids，让 C 检查它自己的实时窗口；
        - 预测预警：传 aligned_data（把预测值包成 AlignedWindowData），
          让 C 检查未来预测值是否会违反约束。
        constraint_ids: 本次要检查的约束 ID 列表（来自任务配置的 semantic_context）。
        返回 (satisfied, violations)。
        """
        if aligned_data is not None:
            request = pb.CheckConstraintsRequest(
                constraint_ids=list(constraint_ids), aligned_data=aligned_data)
        else:
            request = pb.CheckConstraintsRequest(
                constraint_ids=list(constraint_ids),
                window_query=pb.QueryWindowDataRequest(
                    sequence_ids=list(sequence_ids or [])),
            )
        resp = self._call(self._stub.checkConstraints, request)
        return resp.satisfied, list(resp.violations)

    def _to_point(self, raw):
        """proto RawTimeseriesPoint → (time, sequence_id, value)。
        value 的 oneof 没设置（空值）返回 None，调用处过滤。
        """
        value = raw.value
        kind = value.WhichOneof("kind")
        if kind is None:
            return None
        return (raw.time, raw.sequence_id, getattr(value, kind))
