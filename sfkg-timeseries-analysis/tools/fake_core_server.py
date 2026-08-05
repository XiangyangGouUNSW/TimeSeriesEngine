"""假 C 端 gRPC 服务（联调预演用）。

在本地起一个和 C 端接口一模一样的 gRPC server，数据来自 ETT CSV。
等 C 端真服务跑起来之前，用它和 P 端的 GrpcCoreDataClient 联调。

运行（终端 1）：
    python fake_core_server.py            # 监听 0.0.0.0:50051
运行（终端 2）：
    python main.py --provider grpc
"""

from __future__ import annotations

import csv
import sys
from concurrent import futures
from pathlib import Path

import grpc

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

    def __init__(self, csv_path: str, seq_prefix: str = "ETTh1"):
        self._prefix = seq_prefix
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
        """假实时窗口：返回每条序列最后 3 个点（模拟热窗口）。"""
        names = [self._col(sid) for sid in request.sequence_ids]
        n = len(self._timestamps_ms)
        idx = list(range(max(0, n - 3), n))
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


def serve(csv_path: str, port: int = 50051) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_TimeseriesCoreServiceServicer_to_server(FakeCoreService(csv_path), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"假 C 端已启动，监听 0.0.0.0:{port}（Ctrl+C 停止）")
    server.wait_for_termination()


if __name__ == "__main__":
    serve(str(ROOT / "data" / "ETT-small" / "ETTh1.csv"))
