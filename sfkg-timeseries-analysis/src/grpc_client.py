"""GrpcCoreDataClient：通过 gRPC 访问真正的 C 端（sfkg-timeseries-core）。

实现 CoreDataClient 的方法：
  get_sequence_data_scale       → queryHistoryOverview（数据规模）
  get_history                   → queryHistoryData + raw_points_to_aligned（历史数据，训练用）
  get_aligned_real_time_window  → queryWindowData 实时窗口对齐（检测/预测输入）
  check_constraints             → checkConstraints（约束检查，仅预测预警用）

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

    def get_real_time_window(self, sequence_ids: list[str]):
        """调 C 的 queryWindowData 取实时窗口（检测/预测的模型输入）。

        返回 C 的 WindowData（按序列组织的原始点，未对齐）。
        """
        resp = self._call(
            self._stub.queryWindowData,
            pb.QueryWindowDataRequest(sequence_ids=list(sequence_ids)),
        )
        return resp.data

    def get_aligned_real_time_window(self, sequence_ids: list[str]) -> AlignedWindow:
        """取 C 实时窗口（queryWindowData）并对齐成 [时间×序列] 矩阵。

        检测/预测的输入窗口（训练才用历史 get_history）。C 返回最近一个热窗口，
        行数由 C 决定（queryWindowData 无窗口大小参数），P 端按模型需要取尾部。
        这里只负责「取实时窗口 + 对齐」，不做行数截断。
        """
        data = self.get_real_time_window(sequence_ids)   # WindowData
        points = []
        for seq in data.sequences:
            for p in seq.points:
                value = p.value
                kind = value.WhichOneof("kind")
                if kind is None:
                    continue
                points.append((p.time, seq.sequence_id, getattr(value, kind)))
        timestamps, rows = raw_points_to_aligned(points, sequence_ids)
        return AlignedWindow(
            timestamps_ms=timestamps,
            sequence_ids=sequence_ids,
            values=rows,
        )

    def check_constraints(self, constraint_ids: list[str], aligned_data):
        """调 C 的 checkConstraints 检查预测值是否违反约束（仅预测预警用）。

        aligned_data: 把预测值包成 AlignedWindowData（P 端 _build_aligned 生成），
        C 检查这些未来值是否会违反约束。返回 (satisfied, violations)。
        实时约束检查已迁 C（C 自执行、违规 C 直接写 S），不再有 window_query 分支。
        """
        request = pb.CheckConstraintsRequest(
            constraint_ids=list(constraint_ids), aligned_data=aligned_data)
        resp = self._call(self._stub.checkConstraints, request)
        return resp.satisfied, list(resp.violations)

    def get_correlation_vector(
        self,
        target_sequence_id: str,
        independent_sequence_ids: list[str],
        relation_id: str | None = None,
    ) -> dict[str, float] | None:
        """调 C 的 computeBasicStatistics 拿相关性向量（GCAD 的相关性先验）。

        因变量/自变量通过 alignment_config 的角色（DEPENDENT/INDEPENDENT）告诉 C，
        relation_id 指定对应的时序关联；C 用窗口数据算因变量与每个自变量的
        Pearson 相关系数。返回 {independent_sequence_id: coefficient}。
        """
        alignment_config = pb.AlignmentConfig(
            sequences=[
                pb.SequenceAlignmentConfig(
                    sequence_id=target_sequence_id, role=pb.VARIABLE_ROLE_DEPENDENT),
            ] + [
                pb.SequenceAlignmentConfig(
                    sequence_id=sid, role=pb.VARIABLE_ROLE_INDEPENDENT)
                for sid in independent_sequence_ids
            ],
        )
        request = pb.ComputeStatisticsRequest(
            relation_id=relation_id or "",
            alignment_config=alignment_config,
            window_query=pb.QueryWindowDataRequest(
                sequence_ids=[target_sequence_id] + list(independent_sequence_ids)),
        )
        resp = self._call(self._stub.computeBasicStatistics, request)
        cv = resp.correlation_vector
        return {c.independent_sequence_id: c.coefficient for c in cv.correlations}

    def _to_point(self, raw):
        """proto RawTimeseriesPoint → (time, sequence_id, value)。
        value 的 oneof 没设置（空值）返回 None，调用处过滤。
        """
        value = raw.value
        kind = value.WhichOneof("kind")
        if kind is None:
            return None
        return (raw.time, raw.sequence_id, getattr(value, kind))
