"""历史异常匹配（HISTORICAL_MATCH）：当前窗口与已确认历史异常事件相似度匹配。

技术方案（docs/预测异常模块详细设计.md §1.1-4、docs/全貌文档 §10.1）把历史异常
匹配列为第四种异常检测方法：输入 = 当前窗口 + 确认历史事件索引，输出 = 相似事件与
分数。文档标注"仅空壳 / 计划后续实现"——本模块就是那个空壳的框架：

  1. HistoricalEvent     —— 一条已确认历史异常事件的语义结构（序列集合/类型/时间/值）；
  2. HistoricalEventIndex—— 确认事件索引，可增量添加（幂等），支持持久化；
  3. HistoricalEventMatcher—— 检测模型，接口对齐其他异常模型（fit/detect/save/load），
                              匹配核心 _match 是占位逻辑，后续接真实相似度模型
                              （窗口形状编码 / 前兆匹配）。

数据来源：确认历史事件的**特征**当前拿不到——S 只通过 SemanticContext.
confirmed_historical_event_ids 给事件 ID 引用，P 不检索图谱（设计原则），也没有
事件明细。所以框架把"确认事件从哪来"做成可插拔的 provider（engine 构造时注入，
见 analysis_engine._confirmed_historical_events）；provider 未就绪时索引为空 →
detect 必然无命中，安全。写入仍走 S 的同一 AnalysisResultReceiverService（异常/
预警同 RPC，见 engine.run_anomaly / run_forecast）。

v1 匹配规则（保守、可解释、低误报，明确为占位逻辑）：
  - 只匹配序列集合 ⊆ 当前窗口任务序列集合的确认事件；
  - 事件涉及的序列在窗口内必须有显著偏差（最大 z-score ≥ min_deviation_z）；
  - 取 top-K 个命中，score = 该事件涉及序列在窗口内的最大 z-score，
    index = 最大偏差点（供逐点写映射到时间戳）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import torch

logger = __import__("logging").getLogger(__name__)


# ================= 确认历史事件与索引 =================

@dataclass(frozen=True)
class HistoricalEvent:
    """一条已确认的历史异常/预警事件（匹配索引的元素）。

    sequence_ids 存排序去重后的元组，作为匹配指纹的一部分。
    values 是写 S 时的异常点值（v1 不参与匹配，仅保留供后续特征编码）。
    """

    event_id: str
    event_type: str                       # "ANOMALY" / "WARNING"
    sequence_ids: tuple[str, ...] = ()
    severity: str = "MEDIUM"
    source: str = ""
    event_time_ms: int | None = None
    score: float | None = None
    values: tuple[float, ...] = ()


class HistoricalEventIndex:
    """确认历史事件索引：增量添加（按 event_id 幂等）、查询、持久化。

    生产注意：add 是 O(1) 幂等插入，重复同步同一事件不会产生重复项。
    """

    def __init__(self, events: list[HistoricalEvent] | None = None):
        self._events: list[HistoricalEvent] = []
        self._by_id: dict[str, int] = {}
        for e in (events or []):
            self.add(e)

    def add(self, event: HistoricalEvent) -> bool:
        """插入事件；event_id 已存在则忽略（幂等）。返回是否新增。"""
        if not event.event_id or event.event_id in self._by_id:
            return False
        self._by_id[event.event_id] = len(self._events)
        self._events.append(event)
        return True

    def get(self, event_id: str) -> HistoricalEvent | None:
        i = self._by_id.get(event_id)
        return self._events[i] if i is not None else None

    def all(self) -> list[HistoricalEvent]:
        return list(self._events)

    def __len__(self) -> int:
        return len(self._events)

    def to_dict(self) -> dict:
        return {"events": [
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "sequence_ids": list(e.sequence_ids),
                "severity": e.severity,
                "source": e.source,
                "event_time_ms": e.event_time_ms,
                "score": e.score,
                "values": list(e.values),
            } for e in self._events
        ]}

    @classmethod
    def from_dict(cls, d: dict | None) -> "HistoricalEventIndex":
        idx = cls()
        for ed in (d or {}).get("events", []):
            idx.add(HistoricalEvent(
                event_id=ed.get("event_id", ""),
                event_type=ed.get("event_type", ""),
                sequence_ids=tuple(ed.get("sequence_ids", [])),
                severity=ed.get("severity", "MEDIUM"),
                source=ed.get("source", ""),
                event_time_ms=ed.get("event_time_ms"),
                score=ed.get("score"),
                values=tuple(ed.get("values", [])),
            ))
        return idx


# ================= 检测模型（框架） =================

class HistoricalEventMatcher:
    """历史语义匹配检测模型。

    接口对齐其他异常模型（fit/detect + save/load_dict/load），engine 统一按方法名
    构建、缓存、落盘（ModelStore 磁盘缓存，loader 按 model_type="historical-match"
    分发重建）。fit 是 no-op：索引由确认事件增量构建（load_confirmed_events），
    不走矩阵训练。
    """

    model_type = "historical-match"

    def __init__(self, sequence_ids: list[str] | None = None,
                 min_deviation_z: float = 2.0, top_k: int = 3):
        self.sequence_ids = list(sequence_ids or [])
        self.min_deviation_z = min_deviation_z    # v1 匹配阈值（窗口内最大 z-score）
        self.top_k = top_k
        self.index = HistoricalEventIndex()

    # ---- 索引构建（确认事件增量进索引，不是矩阵训练） ----

    def fit(self, history: np.ndarray) -> None:
        """no-op。历史匹配不训练矩阵模型；索引由确认事件增量构建。"""

    def load_confirmed_events(self, events: list[HistoricalEvent]) -> int:
        """把确认事件并入索引（幂等），返回新增条数。"""
        added = 0
        for e in events:
            if self.index.add(e):
                added += 1
        if added:
            logger.info("[historical] 索引新增 %d 条确认事件，共 %d 条",
                        added, len(self.index))
        return added

    # ---- 检测 ----

    def detect(self, window: np.ndarray) -> list[dict]:
        """匹配当前窗口与确认历史事件，返回命中（list[dict]）。

        v1 占位匹配（_match）：只做结构化匹配——事件序列集合 ⊆ 窗口任务序列集合，
        且事件涉及序列在窗口内有显著偏差（z ≥ min_deviation_z）。真实相似度模型
        （窗口形状编码 / 前兆匹配）后续替换 _match 内部即可，契约不变。
        """
        window = np.asarray(window, dtype=float)
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        if not self.index:
            return []                                  # 空索引：无确认事件 → 无命中
        if window.ndim != 2 or window.shape[1] != len(self.sequence_ids):
            return []                                  # 列数对不上，语义不匹配
        matches = self._match(window)
        matches.sort(key=lambda m: m[1], reverse=True) # 相似度降序，取 top-K
        return [self._to_finding(e, z, i)
                for e, z, i in matches[:self.top_k]]

    def _match(self, window: np.ndarray) -> list[tuple[HistoricalEvent, float, int]]:
        """占位匹配核心：返回 [(事件, 相似度 z, 最大偏差点 index), ...]。

        后续实现方向（技术方案 [58]）：积累足够已确认事件标签后，训练窗口形状
        编码器/前兆编码器，把"当前窗口"与"确认事件前的窗口"做语义相似匹配。
        """
        seq_index = {sid: i for i, sid in enumerate(self.sequence_ids)}
        out: list[tuple[HistoricalEvent, float, int]] = []
        for ev in self.index.all():
            involved = [seq_index[s] for s in ev.sequence_ids if s in seq_index]
            if not involved:
                continue                                # 事件序列不在当前任务 → 不匹配
            # 窗口内事件涉及序列的最大 z-score（相对该列自身均值/标准差）
            best_i, best_z = 0, 0.0
            for c in involved:
                col = window[:, c]
                sd = float(np.std(col))
                if sd < 1e-12:
                    continue
                zs = np.abs(col - np.mean(col)) / sd
                i = int(np.argmax(zs))
                if float(zs[i]) > best_z:
                    best_z, best_i = float(zs[i]), i
            if best_z >= self.min_deviation_z:
                out.append((ev, best_z, best_i))
        return out

    @staticmethod
    def _to_finding(ev: HistoricalEvent, z: float, i: int) -> dict:
        seqs = ",".join(ev.sequence_ids) or "-"
        return {
            "anomaly_type": "HISTORICAL_MATCH",
            "severity": ev.severity or "MEDIUM",
            "description": (f"当前窗口与已确认历史事件 {ev.event_id} 相似"
                            f"（事件类型 {ev.event_type or '-'}，序列 {seqs}，"
                            f"偏差 z={z:.2f}）"),
            "score": z,
            "index": i,
            "matched_event_id": ev.event_id,
        }

    # ---- 持久化（ModelStore 磁盘缓存 + loader 分发） ----

    def save(self, path) -> None:
        torch.save({
            "model_type": self.model_type,
            "sequence_ids": self.sequence_ids,
            "min_deviation_z": self.min_deviation_z,
            "top_k": self.top_k,
            "index": self.index.to_dict(),
        }, path)

    def load_dict(self, ckpt: dict) -> None:
        self.sequence_ids = list(ckpt.get("sequence_ids", []))
        self.min_deviation_z = float(ckpt.get("min_deviation_z", 2.0))
        self.top_k = int(ckpt.get("top_k", 3))
        self.index = HistoricalEventIndex.from_dict(ckpt.get("index"))

    def load(self, path) -> None:
        self.load_dict(torch.load(path, map_location="cpu"))
