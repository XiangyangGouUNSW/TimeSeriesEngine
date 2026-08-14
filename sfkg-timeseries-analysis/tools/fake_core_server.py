"""假 C 端 gRPC 服务（联调预演用）。

在本地起一个和 C 端接口一模一样的 gRPC server，数据来自 ETT CSV。
等 C 端真服务跑起来之前，用它和 P 端的 GrpcCoreDataClient 联调。

运行（终端 1）：
    python fake_core_server.py            # 监听 0.0.0.0:50551（伪 C 端独立端口）
运行（终端 2）：
    python main.py --provider grpc --core-port 50551

端口约定：50551 是**伪 C 端专用**，不占真 C 端 50051（C/S 端自身联调要用 50051）。
"""

from __future__ import annotations

import csv
import sys
from concurrent import futures
from pathlib import Path

import grpc
import numpy as np

# 模块根目录 = tools 的上一级
ROOT = Path(__file__).resolve().parent.parent
# 把 src/ 和 generated/ 加进模块搜索路径
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from core_client import parse_utc_ms

import timeseries_core_pb2 as pb
import timeseries_core_pb2_grpc as pb_grpc


class FakeCoreService(pb_grpc.TimeseriesCoreServiceServicer):
    """只实现 P 端要用的两个接口，别的都会返回 UNIMPLEMENTED。"""

    def __init__(self, csv_path: str, seq_prefix: str = "ETTh1",
                 window_rows: int = 500):
        """window_rows: queryWindowData 假实时窗口返回最近多少行（模拟 C 热窗口）。
        行数由 C 端决定（请求里没有窗口大小参数），P 端对齐后按模型需要取尾部。
        """
        self._prefix = seq_prefix
        self._window_rows = window_rows
        self._timestamps_ms, self._columns = self._load_csv(csv_path)

    def _load_csv(self, path: str):
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)            # 第一行：date,HUFL,HULL,...
            col_names = header[1:]           # 去掉 date
            timestamps_ms = []
            col_values = {name: [] for name in col_names}
            for row in reader:
                if not row or not row[0]:
                    continue
                timestamps_ms.append(parse_utc_ms(row[0]))
                for i, name in enumerate(col_names):
                    col_values[name].append(float(row[i + 1]))
        return timestamps_ms, col_values

    def _col(self, sequence_id: str) -> str:
        """'ETTh1_OT' -> 'OT'；前缀对不上就报错。"""
        prefix = self._prefix + "_"
        if not sequence_id.startswith(prefix):
            raise ValueError(
                f"sequence_id '{sequence_id}' 不在假 C 端数据里（前缀应为 '{self._prefix}_'）")
        return sequence_id[len(prefix):]

    @staticmethod
    def _ok():
        return pb.OperationResult(code=pb.OPERATION_CODE_OK)

    @staticmethod
    def _value_of(v) -> float | None:
        """TimeseriesValue（oneof）→ float；非数值类型返回 None。"""
        kind = v.WhichOneof("kind")
        if kind in ("double_value", "int64_value", "bool_value"):
            return float(getattr(v, kind))
        return None

    def queryHistoryOverview(self, request, context):
        ids = list(request.sequence_ids) if request.sequence_ids else list(self._columns.keys())
        names = [self._col(sid) for sid in ids]
        idx = [
            i for i, ts in enumerate(self._timestamps_ms)
            if (not request.HasField("start_time") or ts >= request.start_time)
            and (not request.HasField("end_time") or ts < request.end_time)
        ]
        series = []
        for sid, name in zip(ids, names):
            vals = [self._columns[name][i] for i in idx]
            if vals:                         # 范围内没数据就不出现（和 C 一致）
                s = pb.HistorySeriesSummary(sequence_id=sid, point_count=len(vals))
                s.first_time = self._timestamps_ms[idx[0]]
                s.last_time = self._timestamps_ms[idx[-1]]
                series.append(s)
        overview = pb.HistoryOverview(
            total_point_count=sum(s.point_count for s in series),
            sequence_count=len(series),
            column_names=[s.sequence_id for s in series],
        )
        if series:
            overview.first_time = series[0].first_time
            overview.last_time = series[-1].last_time
        overview.series.extend(series)
        return pb.QueryHistoryOverviewResponse(operation=self._ok(), overview=overview)

    def queryHistoryData(self, request, context):
        names = [self._col(sid) for sid in request.sequence_ids]
        idx = [i for i, ts in enumerate(self._timestamps_ms)
               if request.start_time <= ts < request.end_time]
        points = []
        for i in idx:
            for sid, name in zip(request.sequence_ids, names):
                points.append(pb.RawTimeseriesPoint(
                    time=self._timestamps_ms[i],
                    sequence_id=sid,
                    value=pb.TimeseriesValue(double_value=self._columns[name][i]),
                ))
        return pb.QueryHistoryDataResponse(
            operation=self._ok(),
            data=pb.TimeseriesBatch(points=points),
        )

    def queryWindowData(self, request, context):
        """假实时窗口：返回每条序列最近 window_rows 个点（模拟 C 的热窗口）。"""
        names = [self._col(sid) for sid in request.sequence_ids]
        n = len(self._timestamps_ms)
        idx = list(range(max(0, n - self._window_rows), n))
        sequences = []
        for sid, name in zip(request.sequence_ids, names):
            points = [
                pb.TimeValuePoint(
                    time=self._timestamps_ms[i],
                    value=pb.TimeseriesValue(double_value=self._columns[name][i]),
                )
                for i in idx
            ]
            sequences.append(pb.SequenceWindow(sequence_id=sid, points=points))
        return pb.QueryWindowDataResponse(
            operation=self._ok(),
            data=pb.WindowData(
                window_start_time=self._timestamps_ms[idx[0]],
                window_end_time=self._timestamps_ms[idx[-1]],
                sequences=sequences,
            ),
        )

    def alignWindowData(self, request, context):
        """对齐：分桶 + 聚合 + 缺失填充，返回统一时间轴的 AlignedWindowData。

        对应 C 端 alignWindowData。source 两种都支持：
          request.data          调用方直接给窗口；
          request.window_query  让 C 先读自己的热窗口再对齐（P 端默认走这个）。
        """
        if request.HasField("data"):
            window = request.data
        elif request.HasField("window_query"):
            window = self.queryWindowData(request.window_query, context).data
        else:
            return pb.AlignWindowDataResponse(
                operation=pb.OperationResult(
                    code=pb.OPERATION_CODE_INVALID_ARGUMENT,
                    message="alignWindowData 需要 data 或 window_query"),
            )

        interval = request.config.bucket_interval
        if interval <= 0:
            return pb.AlignWindowDataResponse(
                operation=pb.OperationResult(
                    code=pb.OPERATION_CODE_INVALID_ARGUMENT,
                    message="bucket_interval 必须 > 0"),
            )

        # 每条序列的原始点 {time: float}（非数值被 _value_of 过滤成 None）
        raw = {}
        for seq in window.sequences:
            raw[seq.sequence_id] = {p.time: v for p in seq.points
                                    if (v := self._value_of(p.value)) is not None}

        # 对齐配置：按 sequence_id 找聚合/填充，缺省 LAST / LINEAR
        agg_map = {sc.sequence_id: sc.aggregation for sc in request.config.sequences}
        fill_map = {sc.sequence_id: sc.fill_method for sc in request.config.sequences}
        seq_ids = [seq.sequence_id for seq in window.sequences]

        # 统一时间轴：全部原始点覆盖范围，按 interval 分桶
        all_times = sorted({t for pts in raw.values() for t in pts})
        if not all_times:
            return pb.AlignWindowDataResponse(
                operation=self._ok(),
                aligned_data=pb.AlignedWindowData(
                    window_start_time=0, window_end_time=0),
            )
        t0, t1 = all_times[0], all_times[-1]
        buckets = list(range(t0, t1 + 1, interval))

        # 每条序列：先按桶聚合，再对缺失桶填充
        aligned = {}
        for sid in seq_ids:
            pts = raw.get(sid, {})
            agg = agg_map.get(sid, pb.BUCKET_AGGREGATION_LAST)
            fill = fill_map.get(sid, pb.GAP_FILL_METHOD_LINEAR)
            bucket_vals = []
            for b in buckets:
                vals = [v for t, v in pts.items() if b <= t < b + interval]
                bucket_vals.append(self._aggregate(vals, agg) if vals else None)
            aligned[sid] = self._fill_gaps(bucket_vals, fill)

        samples = []
        for i, b in enumerate(buckets):
            sample = pb.AlignedSample(time=b)
            sample.values.extend(
                pb.AlignedValue(sequence_id=sid,
                                value=pb.TimeseriesValue(double_value=aligned[sid][i]))
                for sid in seq_ids if aligned[sid][i] is not None)
            samples.append(sample)
        return pb.AlignWindowDataResponse(
            operation=self._ok(),
            aligned_data=pb.AlignedWindowData(
                window_start_time=t0, window_end_time=t1, samples=samples),
        )

    @staticmethod
    def _aggregate(vals: list[float], agg) -> float:
        """桶内聚合：LAST 缺省；数值聚合先过滤非有限值。"""
        finite = [v for v in vals if np.isfinite(v)]
        use = finite if finite else vals
        if agg == pb.BUCKET_AGGREGATION_FIRST:
            return use[0]
        if agg == pb.BUCKET_AGGREGATION_AVERAGE:
            return float(np.mean(use))
        if agg == pb.BUCKET_AGGREGATION_MAXIMUM:
            return float(np.max(use))
        if agg == pb.BUCKET_AGGREGATION_MINIMUM:
            return float(np.min(use))
        return use[-1]                      # LAST 或 UNSPECIFIED

    @staticmethod
    def _fill_gaps(vals: list[float | None], fill) -> list[float | None]:
        """对缺失桶做填充：LINEAR 缺省；PREVIOUS/NEXT/NEAR 就近取值。"""
        known = [(i, v) for i, v in enumerate(vals) if v is not None]
        if len(known) == len(vals):         # 没有缺失，直接返回
            return vals
        out = list(vals)
        for i in range(len(out)):
            if out[i] is not None:
                continue
            prev_j = max((j for j, _ in known if j < i), default=None)
            nxt_j = min((j for j, _ in known if j > i), default=None)
            prev = out[prev_j] if prev_j is not None else None
            nxt = out[nxt_j] if nxt_j is not None else None
            if fill == pb.GAP_FILL_METHOD_PREVIOUS:
                out[i] = prev if prev is not None else nxt
            elif fill == pb.GAP_FILL_METHOD_NEXT:
                out[i] = nxt if nxt is not None else prev
            elif fill == pb.GAP_FILL_METHOD_NEAR:
                nearest = min((abs(j - i), v) for j, v in known)
                out[i] = nearest[1]
            elif prev_j is not None and nxt_j is not None:   # LINEAR
                out[i] = prev + (nxt - prev) * (i - prev_j) / (nxt_j - prev_j)
            else:
                out[i] = prev if prev is not None else nxt
        return out

    def computeBasicStatistics(self, request, context):
        """相关性向量：因变量(DEPENDENT) 与每个自变量(INDEPENDENT) 的 Pearson 相关。

        用训练段（前 1000 点）算，避免被注入异常的实时段污染。
        对应 C 端 computeBasicStatistics 的 correlation_vector（GCAD 相关性先验）。
        """
        dependent_id = None
        independent_ids = []
        for sc in request.alignment_config.sequences:
            if sc.role == pb.VARIABLE_ROLE_DEPENDENT:
                dependent_id = sc.sequence_id
            elif sc.role == pb.VARIABLE_ROLE_INDEPENDENT:
                independent_ids.append(sc.sequence_id)
        if dependent_id is None:
            return pb.ComputeStatisticsResponse(
                operation=pb.OperationResult(
                    code=pb.OPERATION_CODE_INVALID_ARGUMENT,
                    message="computeBasicStatistics 缺少因变量(DEPENDENT)"),
            )
        dep = np.asarray(self._columns[self._col(dependent_id)], dtype=float)
        seg = min(len(dep), 1000)
        correlations = []
        for sid in independent_ids:
            ind = np.asarray(self._columns[self._col(sid)], dtype=float)
            if seg >= 2 and dep[:seg].std() > 0 and ind[:seg].std() > 0:
                coef = float(np.corrcoef(dep[:seg], ind[:seg])[0, 1])
            else:
                coef = 0.0
            correlations.append(pb.SequenceCorrelation(
                independent_sequence_id=sid, coefficient=coef))
        cv = pb.CorrelationVector(
            dependent_sequence_id=dependent_id, correlations=correlations)
        return pb.ComputeStatisticsResponse(operation=self._ok(), correlation_vector=cv)

    def checkConstraints(self, request, context):
        """空壳约束检查：直接返回"违反约束"（假数据，演示通讯用）。"""
        violation = pb.ConstraintViolation(
            constraint_id="demo-constraint-001",
            anchor_time=0,
            lower_bound=0.0,
            upper_bound=80.0,
            evaluated_value=85.0,
        )
        return pb.CheckConstraintsResponse(
            operation=self._ok(),
            satisfied=False,
            violations=[violation],
            evaluated_count=1,
        )


def serve(csv_path: str, port: int = 50551) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_TimeseriesCoreServiceServicer_to_server(FakeCoreService(csv_path), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"假 C 端已启动，监听 0.0.0.0:{port}（Ctrl+C 停止）")
    server.wait_for_termination()


if __name__ == "__main__":
    serve(str(ROOT / "data" / "ETT-small" / "ETTh1.csv"))
