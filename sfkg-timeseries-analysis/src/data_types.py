"""P 端空壳用的简单数据结构。

C 端 proto 就绪后，这些 dataclass 会和 proto message 互相转换；
现在先用它们把流程跑起来。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# C 端原始取值类型：int/bool/float/string；缺失统一用 NaN（float）。喂模型前才转 float32。
RawValue = float | int | bool | str


@dataclass
class SequenceDataScale:
    """一条序列的数据规模，用来判断数据够不够训练。"""
    sequence_id: str
    point_count: int                    # 数据点数
    start_time_ms: int | None = None    # 最早时间
    end_time_ms: int | None = None      # 最晚时间


@dataclass
class HistoricalDataChunk:
    """一段历史数据。行 = 时间，列 = sequence_ids。

    values 保留 C 端原始类型（int/bool/float/string，缺失=NaN），
    离散/连续路由靠它推断（_infer_column_kinds 在转 float 前调用）。
    """
    timestamps_ms: list[int]
    sequence_ids: list[str]             # 列顺序
    values: list[list[RawValue]]        # 每行是 [列1, 列2, ...]
    is_last_chunk: bool = True          # 分块用；现在一次给完


@dataclass
class AlignedWindow:
    """对齐窗口：多条序列对齐到同一时间轴（推理的模型输入）。"""
    timestamps_ms: list[int]
    sequence_ids: list[str]             # 列顺序
    values: list[list[float]]           # 每行是 [列1, 列2, ...]


@dataclass
class HistorySeriesSummary:
    """一条序列的历史数据规模摘要（对应 C 端 overview.series）。"""
    sequence_id: str
    point_count: int
    first_time_ms: int | None = None
    last_time_ms: int | None = None


@dataclass
class HistoryOverview:
    """历史数据总体信息（对应 C 端 queryHistoryOverview 返回）。"""
    total_point_count: int
    sequence_count: int
    column_names: list[str]             # 范围内实际有数据的 sequence_id
    first_time_ms: int | None = None
    last_time_ms: int | None = None
    series: list[HistorySeriesSummary] = field(default_factory=list)
