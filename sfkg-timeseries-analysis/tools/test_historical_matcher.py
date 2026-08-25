"""历史语义匹配（HISTORICAL_MATCH）框架测试。

验证框架各层：
  1. 方法识别：KNOWN_METHODS 含 HISTORICAL_MATCH，工厂可构建；
  2. 空索引 → detect 无命中（安全空壳）；
  3. 注入确认事件 + 偏差窗口 → 命中（matched_event_id）；
  4. 事件序列集合与任务不重叠 → 不命中；
  5. 持久化：save/load 索引完整，ModelStore 磁盘重建走 loader 分发；
  6. engine 集成（无 provider）：任务跑通 → SUCCESS、0 发现、模型落盘、
     is_ready、needs_training 变 False、磁盘重建为 HistoricalEventMatcher；
  7. 全链路（provider 注入确认事件）：匹配命中 → 同一 S RPC（send_event）写事件。

用法（sfkg 环境）：
  python tools/test_historical_matcher.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from anomaly_models import KNOWN_METHODS, build_anomaly_model
from data_types import SequenceDataScale
from historical_matcher import HistoricalEvent, HistoricalEventMatcher
from task_registry import TaskKind
from training_loop import ModelStore

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def _spike_window() -> np.ndarray:
    """单列窗口 [0,0,0,100,0]：idx3 最大 z-score = 2.0（min_deviation_z 默认命中线）。"""
    return np.array([[0.0], [0.0], [0.0], [100.0], [0.0]])


# ================= 1. 方法识别 =================

def test_known_and_factory() -> None:
    print("\n[方法识别]")
    assert "HISTORICAL_MATCH" in KNOWN_METHODS, "HISTORICAL_MATCH 应在 KNOWN_METHODS"
    _ok("KNOWN_METHODS 含 HISTORICAL_MATCH")

    m = build_anomaly_model("HISTORICAL_MATCH", sequence_ids=["a", "b"])
    assert m is not None and isinstance(m, HistoricalEventMatcher)
    assert m.model_type == "historical-match"
    assert m.sequence_ids == ["a", "b"]
    _ok("工厂可构建 HistoricalEventMatcher")

    assert build_anomaly_model("NOT_A_METHOD") is None
    _ok("未知方法仍返回 None")


# ================= 2/3/4. 匹配器单元 =================

def test_empty_index_no_match() -> None:
    print("\n[空索引]")
    m = HistoricalEventMatcher(sequence_ids=["a"])
    assert m.detect(_spike_window()) == [], "空索引不应命中"
    _ok("空索引 → 无命中")


def test_match_on_confirmed_event() -> None:
    print("\n[命中]")
    m = HistoricalEventMatcher(sequence_ids=["a"])
    m.load_confirmed_events([
        HistoricalEvent(event_id="ev-1", event_type="ANOMALY",
                        sequence_ids=("a",), severity="HIGH"),
    ])
    findings = m.detect(_spike_window())
    assert len(findings) == 1, f"应有 1 条命中，实际 {findings}"
    f = findings[0]
    assert f["anomaly_type"] == "HISTORICAL_MATCH"
    assert f["matched_event_id"] == "ev-1", "命中应携带确认事件 ID"
    assert f["index"] == 3, "最大偏差点应在 idx3"
    assert f["score"] >= 2.0 and "ev-1" in f["description"]
    _ok("同序列集合 + 窗口偏差 → 命中（matched_event_id/index/score）")


def test_sequence_mismatch_no_match() -> None:
    print("\n[序列不重叠]")
    m = HistoricalEventMatcher(sequence_ids=["a"])
    m.load_confirmed_events([
        HistoricalEvent(event_id="ev-x", event_type="ANOMALY",
                        sequence_ids=("other",)),
    ])
    assert m.detect(_spike_window()) == [], "事件序列不在任务里不应命中"
    _ok("序列集合不重叠 → 不命中")


def test_persistence_roundtrip() -> None:
    print("\n[持久化]")
    m = HistoricalEventMatcher(sequence_ids=["a"])
    m.load_confirmed_events([
        HistoricalEvent(event_id="ev-1", event_type="ANOMALY", sequence_ids=("a",)),
    ])
    tmp = Path(tempfile.mkdtemp()) / "m.pt"
    m.save(tmp)
    m2 = HistoricalEventMatcher()
    m2.load(tmp)
    assert len(m2.index) == 1 and m2.index.get("ev-1") is not None, "索引应完整恢复"
    assert len(m2.detect(_spike_window())) == 1, "恢复后仍能命中"
    _ok("save/load 索引完整，恢复后可命中")


# ================= 6. engine 集成（空壳路径） =================

class FakeCore:
    """只实现 _run_anomaly_models 用到的 C 调用；窗口带一个尖峰（idx60，z≈10）。"""

    def __init__(self):
        self._win = np.zeros((100, 1))
        self._win[60, 0] = 100.0

    def get_aligned_real_time_window(self, seq_ids, project_id=""):
        return SimpleNamespace(
            timestamps_ms=list(range(100)),
            values=[[float(v)] for v in self._win[:, 0]])

    def get_history(self, seq_ids, end_time_ms=None, project_id=""):
        return SimpleNamespace(
            sequence_ids=list(seq_ids),
            values=[[float(v)] for v in np.arange(200, dtype=float).reshape(-1, 1)[:, 0]])

    def get_sequence_data_scale(self, seq_ids, project_id=""):
        # 数据规模 200 点 ≥ 默认门槛 100，不触发数据门槛（门槛专项见 test_anomaly_data_gate）。
        # 用真 SequenceDataScale（含 start_time_ms/end_time_ms 字段，访问不报错）——
        # 引擎历史取数路径会读这两个字段，SimpleNamespace 缺字段会崩（2026-08-26 修复）。
        return [SequenceDataScale(sequence_id=sid, point_count=200) for sid in seq_ids]


def _cfg():
    # minimum_confirmed_events=0：本测试把 HISTORICAL_MATCH 当 no-op 模型测
    # 框架/持久化/loader 分发，事件数门槛（默认 1）专项见 test_anomaly_data_gate。
    return {"inference": {"window_size": 100},
            "anomaly": {"minimum_confirmed_events": 0}}


def test_engine_integration() -> None:
    print("\n[engine 集成·空壳路径]")
    model_dir = Path(tempfile.mkdtemp())
    core = FakeCore()
    engine = AnalysisEngine(core_client=core, result_client=None, config=_cfg(),
                            model_store=ModelStore(model_dir=model_dir))
    task = pb.AnomalyTaskConfig(task_id="t-hist", sequence_ids=["a"],
                                methods=["HISTORICAL_MATCH"])
    res = engine.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"空壳应成功，实际 {res.status}"
    assert res.findings == [], "无确认事件 → 不应有发现"
    _ok("无 provider 任务跑通 → SUCCESS、0 发现")

    key = engine._anomaly_key(task, "HISTORICAL_MATCH", 1)
    assert engine.store.is_ready(key), "首跑后模型应落盘（空索引）"
    assert not engine.needs_training(task, TaskKind.ANOMALY, config_version=1), \
        "落盘后不应再进训练队列"
    _ok("模型落盘 + is_ready + needs_training False")

    engine2 = AnalysisEngine(core_client=core, result_client=None, config=_cfg(),
                             model_store=ModelStore(model_dir=model_dir))
    m = engine2.store.get(key)
    assert isinstance(m, HistoricalEventMatcher), "磁盘重建应走 loader 分发"
    assert len(m.index) == 0
    _ok("磁盘重建 → HistoricalEventMatcher（loader 分发）")


# ================= 7. 全链路（provider 注入确认事件） =================

class FakeSender:
    """记录 send_event 调用（验证与异常/预警走同一 S RPC）。"""

    def __init__(self):
        self.sent = []

    def send_event(self, **kw):
        self.sent.append(kw)
        return True


def test_provider_feeds_and_writes() -> None:
    print("\n[全链路·provider 注入确认事件]")
    model_dir = Path(tempfile.mkdtemp())
    core = FakeCore()
    sender = FakeSender()
    provider = lambda task: [          # 框架预留的确认事件来源（v1 空，测试注入）
        HistoricalEvent(event_id="ev-1", event_type="ANOMALY", sequence_ids=("a",)),
    ]
    engine = AnalysisEngine(core_client=core, result_client=sender, config=_cfg(),
                            model_store=ModelStore(model_dir=model_dir),
                            historical_event_provider=provider)
    task = pb.AnomalyTaskConfig(task_id="t-hist2", sequence_ids=["a"],
                                methods=["HISTORICAL_MATCH"])
    res = engine.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS
    assert len(res.findings) == 1, f"应命中 1 条，实际 {len(res.findings)}"
    assert res.findings[0].anomaly_type == "HISTORICAL_MATCH"
    assert "ev-1" in res.findings[0].description
    _ok("provider → 索引 → detect 命中")

    assert len(sender.sent) == 1, "命中后应写事件"
    sent = sender.sent[0]
    assert sent["event_type"] == pb.ANOMALY_EVENT_TYPE_ANOMALY
    assert sent["source"] == pb.ANOMALY_SOURCE_MODEL_ANOMALY_DETECTION
    assert sent["task_id"] == "t-hist2", "写事件应带任务标识（task_id）"
    _ok("命中 → 同一 S RPC（send_event）写异常事件（含 task_id）")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_known_and_factory()
    test_empty_index_no_match()
    test_match_on_confirmed_event()
    test_sequence_mismatch_no_match()
    test_persistence_roundtrip()
    test_engine_integration()
    test_provider_feeds_and_writes()
    print(f"\n历史语义匹配框架测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
