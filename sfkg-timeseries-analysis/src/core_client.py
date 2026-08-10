"""CoreDataClient：P 端访问 C 端数据的唯一入口。

现在的状态：C 端的 proto 还没定，所以先用 MockCoreDataClient 读本地
ETT 数据集，假装自己连的是 C 端。

Mock 的接口形态和 C 端保持一致：
  - query_history_overview()  → 对应 C 的 queryHistoryOverview（数据规模）
  - query_history_data()      → 对应 C 的 queryHistoryData（返回原始点）
P 端再用 raw_points_to_aligned() 把原始点拼成对齐表（行=时间，列=序列）。

C 端 proto 就绪后要做的事：
  1. 写一个 GrpcCoreDataClient 类，实现下面 CoreDataClient 的 3 个方法，
     内部调 C 的 queryHistoryOverview / queryHistoryData，再走同一套
     raw_points_to_aligned 转换；
  2. 在 main.py 的 build_core_client() 里把 provider 换成 "grpc"。
其他代码一行都不用改。
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone

from data_types import (
    SequenceDataScale,
    HistoricalDataChunk,
    AlignedWindow,
    HistoryOverview,
    HistorySeriesSummary,
)


class CoreDataClient:
    """抽象接口：三个方法 = 我们要 C 端提供的三个能力。"""

    def get_sequence_data_scale(self, sequence_ids: list[str]) -> list[SequenceDataScale]:
        raise NotImplementedError

    def get_history(
        self,
        sequence_ids: list[str],
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> HistoricalDataChunk:
        raise NotImplementedError

    def get_aligned_real_time_window(self, sequence_ids: list[str]) -> AlignedWindow:
        """取 C 实时窗口（queryWindowData）并对齐成 [时间×序列] 矩阵。

        这是模型 detect / 预测的输入；训练才用 get_history（历史数据）。
        返回 AlignedWindow（时间升序、缺失值 NaN）。窗口行数由 C 端决定
        （queryWindowData 无窗口大小参数），P 端按模型需要取尾部。
        """
        raise NotImplementedError

    def get_correlation_vector(
        self,
        target_sequence_id: str,
        independent_sequence_ids: list[str],
    ) -> dict[str, float] | None:
        """相关性先验：因变量与每个自变量的相关系数 {independent_id: coef}。

        默认返回 None（无此能力时上层降级为不用先验）。Grpc 客户端实现真实调用。
        """
        return None


def parse_utc_ms(text: str) -> int:
    """把 '2016-07-01 00:00:00' 转成 UTC 毫秒整数。"""
    dt = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def raw_points_to_aligned(points, sequence_ids: list[str]):
    """把 C 端返回的原始点列表拼成对齐表。

    points: [(time_ms, sequence_id, value), ...]（C 的 queryHistoryData 返回形态）
    sequence_ids: 列顺序
    返回 (timestamps_ms, values_rows)：行=时间（升序），列=sequence_ids；
    某个序列在某时刻没有点 = 缺失（NaN）。
    """
    by_seq = {sid: {} for sid in sequence_ids}
    times = set()
    for time_ms, sid, value in points:
        if sid in by_seq:                    # 只收我们要的序列
            by_seq[sid][time_ms] = value
            times.add(time_ms)
    timestamps_ms = sorted(times)
    rows = [
        [by_seq[sid].get(t, float("nan")) for sid in sequence_ids]
        for t in timestamps_ms
    ]
    return timestamps_ms, rows


class MockCoreDataClient(CoreDataClient):
    """假装是 C 端：数据来自本地 ETT 的 CSV 文件。"""

    def __init__(self, csv_path: str, seq_prefix: str = "ETTh1"):
        self._prefix = seq_prefix
        self._timestamps_ms, self._columns = self._load_csv(csv_path)

    def _load_csv(self, path: str):
        """读 CSV。返回 (时间戳列表, {列名: 数值列表})。"""
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
        """'ETTh1_OT' -> 'OT'；前缀对不上就报错，方便你发现写错了 ID。"""
        prefix = self._prefix + "_"
        if not sequence_id.startswith(prefix):
            raise ValueError(
                f"sequence_id '{sequence_id}' 不在 mock 数据里（前缀应为 '{self._prefix}_'）"
            )
        return sequence_id[len(prefix):]

    # ---------- 和 C 端一样的两个接口 ----------

    def query_history_overview(
        self,
        sequence_ids: list[str] | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> HistoryOverview:
        """对应 C 的 queryHistoryOverview。参数都可省略；时间范围左闭右开。"""
        ids = list(sequence_ids) if sequence_ids else list(self._columns.keys())
        names = [self._col(sid) for sid in ids]
        idx = [
            i for i, ts in enumerate(self._timestamps_ms)
            if (start_time_ms is None or ts >= start_time_ms)
            and (end_time_ms is None or ts < end_time_ms)
        ]
        series = []
        for sid, name in zip(ids, names):
            vals = [self._columns[name][i] for i in idx]
            if vals:                         # 范围内没数据就不出现在 column_names 里（和 C 一致）
                series.append(HistorySeriesSummary(
                    sequence_id=sid,
                    point_count=len(vals),
                    first_time_ms=self._timestamps_ms[idx[0]],
                    last_time_ms=self._timestamps_ms[idx[-1]],
                ))
        return HistoryOverview(
            total_point_count=sum(s.point_count for s in series),
            sequence_count=len(series),
            column_names=[s.sequence_id for s in series],
            first_time_ms=series[0].first_time_ms if series else None,
            last_time_ms=series[-1].last_time_ms if series else None,
            series=series,
        )

    def query_history_data(
        self,
        sequence_ids: list[str],
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[tuple[int, str, float]]:
        """对应 C 的 queryHistoryData。返回原始点，按时间升序。
        时间范围左闭右开 [start, end)；缺失的点不返回（和 C 一致）。
        """
        names = [self._col(sid) for sid in sequence_ids]
        idx = [i for i, ts in enumerate(self._timestamps_ms)
               if start_time_ms <= ts < end_time_ms]
        points = []
        for i in idx:
            for sid, name in zip(sequence_ids, names):
                points.append((self._timestamps_ms[i], sid, self._columns[name][i]))
        return points

    # ---------- P 端三个能力（用上面两个接口 + 转换实现） ----------

    def get_sequence_data_scale(self, sequence_ids: list[str]) -> list[SequenceDataScale]:
        ov = self.query_history_overview(sequence_ids)
        by_id = {s.sequence_id: s for s in ov.series}
        result = []
        for sid in sequence_ids:
            s = by_id.get(sid)
            result.append(SequenceDataScale(
                sequence_id=sid,
                point_count=s.point_count if s else 0,
                start_time_ms=s.first_time_ms if s else None,
                end_time_ms=s.last_time_ms if s else None,
            ))
        return result

    def get_history(
        self,
        sequence_ids: list[str],
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> HistoricalDataChunk:
        # C 的 queryHistoryData 要求必须给时间范围；没给就用 overview 补全
        if start_time_ms is None or end_time_ms is None:
            ov = self.query_history_overview(sequence_ids)
            start_time_ms = start_time_ms if start_time_ms is not None else ov.first_time_ms
            end_time_ms = end_time_ms if end_time_ms is not None else ov.last_time_ms
        points = self.query_history_data(sequence_ids, start_time_ms, end_time_ms)
        timestamps, rows = raw_points_to_aligned(points, sequence_ids)
        return HistoricalDataChunk(
            timestamps_ms=timestamps,
            sequence_ids=sequence_ids,
            values=rows,
            is_last_chunk=True,
        )

    def get_aligned_real_time_window(
        self,
        sequence_ids: list[str],
        window_rows: int = 500,
    ) -> AlignedWindow:
        """假实时窗口：取每条序列最后 window_rows 行（模拟 C 的热窗口）。

        对应 C 的 queryWindowData：P 传 sequence_ids，C 返回最近一个热窗口，
        P 对齐成 [时间×序列] 矩阵。行数由 C 决定（这里固定 window_rows），
        引擎按模型需要取尾部。
        """
        names = [self._col(sid) for sid in sequence_ids]
        n = len(self._timestamps_ms)
        idx = list(range(max(0, n - window_rows), n))
        points = [
            (self._timestamps_ms[i], sid, self._columns[name][i])
            for i in idx
            for sid, name in zip(sequence_ids, names)
        ]
        timestamps, rows = raw_points_to_aligned(points, sequence_ids)
        return AlignedWindow(
            timestamps_ms=timestamps,
            sequence_ids=sequence_ids,
            values=rows,
        )

    # ---- 演示专用：取某时刻之后的真实值（用来和预测对比） ----

    def get_values_after(self, sequence_id: str, after_time_ms: int, count: int) -> list[float]:
        name = self._col(sequence_id)
        idx = [i for i, ts in enumerate(self._timestamps_ms) if ts > after_time_ms]
        return [self._columns[name][i] for i in idx[:count]]
