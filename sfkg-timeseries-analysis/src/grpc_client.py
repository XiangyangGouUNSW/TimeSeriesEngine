"""GrpcCoreDataClient：通过 gRPC 访问真正的 C 端（sfkg-timeseries-core）。

实现 CoreDataClient 的方法：
  get_sequence_data_scale       → queryHistoryOverview（数据规模）
  get_history                   → queryHistoryData + raw_points_to_aligned（历史数据，训练用）
  get_aligned_real_time_window  → alignWindowData（C 端对齐，检测/预测输入）
  check_constraints             → checkConstraints（约束检查，仅预测预警用）

用法：config.yaml 的 core.provider 改成 "grpc" 后，main.py 自动用这个类。
"""

from __future__ import annotations

import sys
from pathlib import Path

import grpc
from typing import Any, Callable

from core_client import CoreDataClient, raw_points_to_aligned
from data_types import RawValue, SequenceDataScale, HistoricalDataChunk, AlignedWindow

# 让生成的 stub 可以直接 import（flat 方式，避免生成代码的绝对 import 出问题）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_core_pb2 as pb
import timeseries_core_pb2_grpc as pb_grpc


class CoreDataException(Exception):
    """C 端 gRPC 调用失败。上层 catch 后转成 UPSTREAM_UNAVAILABLE。"""


class GrpcCoreDataClient(CoreDataClient):
    def __init__(self, address: str = "localhost", port: int = 50051,
                 timeout_seconds: float = 30.0):
        # 调大接收上限：历史数据一次可能很大（真 C 端全量历史实测 >130MB）。
        # 512MB 是「顶住当前 + 留余量」的临时值；根治要限幅取数（见 engine 训练取数）。
        channel = grpc.insecure_channel(
            f"{address}:{port}",
            options=[("grpc.max_receive_message_length", 512 * 1024 * 1024)],
        )
        self._stub = pb_grpc.TimeseriesCoreServiceStub(channel)
        self._timeout = timeout_seconds
        self._bucket_cache: dict = {}   # (project_id, frozenset(sequence_ids)) -> 采样周期 ms
        # 缓存键带 project：不同项目可对同名序列有不同采样周期，串了会污染对齐输入。

    # ---- 工具 ----

    def _check(self, response: Any) -> None:
        """检查 operation.code；不是 OK 就抛异常。"""
        if response.operation.code != pb.OPERATION_CODE_OK:
            raise CoreDataException(
                f"Core 返回 {pb.OperationCode.Name(response.operation.code)}: "
                f"{response.operation.message}")

    def _call(self, method: Callable[..., Any], request: Any) -> Any:
        """统一包一层：gRPC 网络错误转成 CoreDataException。"""
        try:
            resp = method(request, timeout=self._timeout)
            self._check(resp)
            return resp
        except grpc.RpcError as e:
            raise CoreDataException(f"gRPC 调用失败: {e}") from e

    # ---- 三个能力 ----

    def get_sequence_data_scale(
        self, sequence_ids: list[str], project_id: str = ""
    ) -> list[SequenceDataScale]:
        resp = self._call(
            self._stub.queryHistoryOverview,
            pb.QueryHistoryOverviewRequest(sequence_ids=sequence_ids,
                                           project_id=project_id),
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
        project_id: str = "",
    ) -> HistoricalDataChunk:
        # C 的 queryHistoryData 必须给时间范围；没给就用 overview 补全
        if start_time_ms is None or end_time_ms is None:
            scales = self.get_sequence_data_scale(sequence_ids,
                                                  project_id=project_id)
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
                project_id=project_id,
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

    def get_real_time_window(self, sequence_ids: list[str],
                             project_id: str = "") -> pb.WindowData:
        """调 C 的 queryWindowData 取实时窗口（检测/预测的模型输入）。

        返回 C 的 WindowData（按序列组织的原始点，未对齐）。
        """
        resp = self._call(
            self._stub.queryWindowData,
            pb.QueryWindowDataRequest(sequence_ids=list(sequence_ids),
                                      project_id=project_id),
        )
        return resp.data

    def get_aligned_real_time_window(
        self,
        sequence_ids: list[str],
        bucket_interval_ms: int | None = None,
        project_id: str = "",
    ) -> AlignedWindow:
        """取 C 实时窗口并对齐成 [时间×序列] 矩阵（检测/预测输入）。

        对齐由 C 端 alignWindowData 完成（分桶、聚合、缺失填充、lag 调整），
        P 不做对齐。window_query 让 C 先读自己的热窗口再对齐；返回统一时间轴的
        AlignedWindowData，这里只转成模型要的矩阵（行=时间，列=sequence_ids）。
        行数由 C 的窗口和 bucket_interval 决定，引擎按模型需要取尾部。
        bucket_interval_ms 缺省时从历史概览推断采样周期（取最粗的），并缓存。
        """
        if bucket_interval_ms is None:
            bucket_interval_ms = self._infer_bucket_interval(sequence_ids,
                                                             project_id=project_id)
        config = pb.AlignmentConfig(
            bucket_interval=bucket_interval_ms,
            sequences=[
                pb.SequenceAlignmentConfig(
                    sequence_id=sid,
                    aggregation=pb.BUCKET_AGGREGATION_LAST,
                    fill_method=pb.GAP_FILL_METHOD_LINEAR,
                )
                for sid in sequence_ids
            ],
        )
        resp = self._call(
            self._stub.alignWindowData,
            pb.AlignWindowDataRequest(
                window_query=pb.QueryWindowDataRequest(
                    sequence_ids=list(sequence_ids), project_id=project_id),
                config=config,
            ),
        )
        return self._aligned_to_window(resp.aligned_data, sequence_ids)

    def _infer_bucket_interval(self, sequence_ids: list[str],
                               project_id: str = "") -> int:
        """从历史概览推断采样周期（平均间隔，取最粗的），并缓存。

        缓存键 (project_id, frozenset(sequence_ids))：不同项目对同名序列可有
        不同采样周期，跨项目复用缓存会拿到错误的桶宽。
        """
        key = (project_id, frozenset(sequence_ids))
        if key in self._bucket_cache:
            return self._bucket_cache[key]
        intervals = []
        for s in self.get_sequence_data_scale(sequence_ids,
                                              project_id=project_id):
            if (s.point_count and s.point_count > 1
                    and s.start_time_ms is not None and s.end_time_ms is not None):
                intervals.append((s.end_time_ms - s.start_time_ms) / (s.point_count - 1))
        if not intervals:
            raise CoreDataException("推断不出采样间隔，无法对齐实时窗口")
        bucket = int(max(intervals))
        self._bucket_cache[key] = bucket
        return bucket

    def _aligned_to_window(
        self, aligned: pb.AlignedWindowData, sequence_ids: list[str]) -> AlignedWindow:
        """C 的 AlignedWindowData（统一时间轴）→ AlignedWindow（行=时间，列=序列）。

        对齐已由 C 完成，这里只是把「按时间组织的多值样本」摆成模型要的矩阵。
        """
        timestamps: list[int] = []
        rows: list[list[float]] = []
        for sample in aligned.samples:
            by_id = {v.sequence_id: v.value for v in sample.values}
            row = []
            for sid in sequence_ids:
                v = by_id.get(sid)
                kind = v.WhichOneof("kind") if v is not None else None
                if kind in ("double_value", "int64_value", "bool_value"):
                    row.append(float(getattr(v, kind)))
                else:
                    row.append(float("nan"))   # string / 缺失值
            timestamps.append(sample.time)
            rows.append(row)
        return AlignedWindow(
            timestamps_ms=timestamps,
            sequence_ids=sequence_ids,
            values=rows,
        )

    def check_constraints(
        self, constraint_ids: list[str],
        aligned_data: pb.AlignedWindowData,
        project_id: str = "",
    ) -> tuple[bool, list[pb.ConstraintViolation]]:
        """调 C 的 checkConstraints 检查预测值是否违反约束（仅预测预警用）。

        aligned_data: 把预测值包成 AlignedWindowData（P 端 _build_aligned 生成），
        C 检查这些未来值是否会违反约束。返回 (satisfied, violations)。
        实时约束检查已迁 C（C 自执行、违规 C 直接写 S），不再有 window_query 分支。
        """
        request = pb.CheckConstraintsRequest(
            constraint_ids=list(constraint_ids), aligned_data=aligned_data,
            project_id=project_id)
        resp = self._call(self._stub.checkConstraints, request)
        return resp.satisfied, list(resp.violations)

    def get_correlation_vector(
        self,
        target_sequence_id: str,
        independent_sequence_ids: list[str],
        project_id: str = "",
    ) -> dict[str, float] | None:
        """调 C 的 computeBasicStatistics 拿相关性向量（GCAD 的相关性先验）。

        因变量/自变量通过 alignment_config 的角色（DEPENDENT/INDEPENDENT）告诉 C，
        C 按角色匹配已注册关系（relation 字段已规范为 source_sequence_id /
        target_sequence_id），用窗口数据算因变量与每个
        自变量的 Pearson 相关系数。返回 {independent_sequence_id: coefficient}。
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
            alignment_config=alignment_config,
            window_query=pb.QueryWindowDataRequest(
                sequence_ids=[target_sequence_id] + list(independent_sequence_ids),
                project_id=project_id),
        )
        resp = self._call(self._stub.computeBasicStatistics, request)
        cv = resp.correlation_vector
        return {c.independent_sequence_id: c.coefficient for c in cv.correlations}

    def _to_point(self, raw: pb.RawTimeseriesPoint) -> tuple[int, str, RawValue] | None:
        """proto RawTimeseriesPoint → (time, sequence_id, value)。
        value 的 oneof 没设置（空值）返回 None，调用处过滤。
        """
        value = raw.value
        kind = value.WhichOneof("kind")
        if kind is None:
            return None
        return (raw.time, raw.sequence_id, getattr(value, kind))
