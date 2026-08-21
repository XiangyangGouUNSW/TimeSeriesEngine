"""异常任务数据门槛 + 语义知识失效 + MODEL_NOT_READY 查询合成 专项测试。

覆盖链路：
  A. 数据门槛（run_anomaly 内，按方法）：
     1. 数据不足 → DATA_NOT_READY（正常结果，非失败）、不训练不检测、
        needs_training 仍 True；
     2. 任务 minimum_points 优先于 config；
     3. 无 anomaly config → 默认门槛 100；
     4. 数据足够 → SUCCESS、模型落盘、needs_training 变 False；
     5. 模型已就绪（纯推理）不再查数据规模；
     6. 按方法门槛：数据够的方法先训，不够的推迟（SUCCESS + 推迟说明），
        数据增长后自动开始 → 训练在不同 tick 摊开、不全挤在一个 tick。
  C. knowledge_version 进模型缓存 key：
     7. 语义知识版本变化 → 旧模型失效需重训（异常/预测 key 都带 kv）。
  D. HISTORICAL_MATCH 确认事件数启用门槛：
     8. 点数够但确认事件 0 条 < 默认 1 → DATA_NOT_READY（消息带"确认事件…需要 1"）；
     9. 事件数达到门槛 → 放行 + 落盘。
  E. provider 默认接 semantic_context 事件 ID：
     10. 无注入 provider 时，解析 semantic_context.confirmed_historical_event_ids
         → 命中并写事件（S 下发的 ID 不再浪费）。
  F. GCAD relations 静态关联先验（技术方案 [42]）：
     11. _extract_relations_prior 只取指向因变量且在任务里的 relation（confidence 保留）；
     12. GcadAnomalyModel 先验边门：relations 边阈值降 h/4 保留、|corr|<阈值 清零、
         对角保留、无先验退回全图统一阈值。
  G. 事件写入峰值保护（生产加固）：
     13. 每任务每轮上限 max_events_per_run → 超上限截断并进消息（逐点写不聚合）；
     14. 发送失败 → 计数进消息，不重试（失败语义）。
  B. 查询侧 MODEL_NOT_READY 合成（servicer）：
     15. 任务已注册 + 模型未就绪 + 仓库空 → 合成一条 MODEL_NOT_READY（Anomaly 格式）；
     16. 预测任务 → ForecastResult 格式（含 timestamps_ms）；
     17. 仓库有真实结果 → 返回真实结果，不合成；
     18. 未知任务 → 空；
     19. 模型已就绪 + 仓库空 → 空（不合成）。

用法（sfkg 环境）：
  python tools/test_anomaly_data_gate.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from analysis_servicer import AnalysisServicer
from core_client import CoreDataClient
from data_types import AlignedWindow, HistoricalDataChunk, SequenceDataScale
from historical_matcher import HistoricalEvent
from result_repository import ResultRepository
from task_registry import TaskKind, TaskRegistry
from training_loop import ModelStore

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


class StubCore(CoreDataClient):
    """数据规模可配置；数据足够时训练/检测给常量序列（HISTORICAL_MATCH 无命中）。"""

    def __init__(self, counts: dict[str, int]):
        self.counts = counts

    def get_sequence_data_scale(self, sequence_ids, project_id=""):
        return [SequenceDataScale(sequence_id=sid,
                                  point_count=self.counts.get(sid, 0))
                for sid in sequence_ids]

    def get_history(self, sequence_ids, start_time_ms=None, end_time_ms=None, project_id=""):
        n = max(self.counts.get(sid, 0) for sid in sequence_ids) or 0
        return HistoricalDataChunk(
            timestamps_ms=list(range(n)),
            sequence_ids=list(sequence_ids),
            values=[[1.0] * len(sequence_ids)] * n,
            is_last_chunk=True)

    def get_aligned_real_time_window(self, sequence_ids, project_id=""):
        n = max(self.counts.get(sid, 0) for sid in sequence_ids) or 1
        return AlignedWindow(
            timestamps_ms=list(range(n)),
            sequence_ids=list(sequence_ids),
            values=[[1.0] * len(sequence_ids)] * n)


class SpikeCore(StubCore):
    """窗口尾部带一个尖峰 → HISTORICAL_MATCH 命中，用于测事件 Severity 透传。

    尖峰放在末尾（倒数第二行）：engine 检测只取窗口最后 window_size=10 行，
    放头部会被截掉。"""

    def get_aligned_real_time_window(self, sequence_ids, project_id=""):
        n = max(self.counts.get(sid, 0) for sid in sequence_ids) or 20
        vals = [[1.0]] * n
        if n > 3:
            vals[-2] = [100.0]
        return AlignedWindow(timestamps_ms=list(range(n)),
                             sequence_ids=list(sequence_ids), values=vals)


class FakeSender:
    """记录 send_event 调用（验证任务等级名 → Severity 透传）。"""

    def __init__(self):
        self.sent = []

    def send_event(self, **kw):
        self.sent.append(kw)
        return True


class FailingSender:
    """send_event 恒失败（模拟 S 未起/连不上），验证失败计数可观测。"""

    def __init__(self):
        self.sent = []

    def send_event(self, **kw):
        self.sent.append(kw)
        return False


def _engine(counts: dict[str, int], cfg: dict | None = None):
    base = {"inference": {"window_size": 10}}
    if cfg:
        base.update(cfg)
    return AnalysisEngine(core_client=StubCore(counts), result_client=None,
                          config=base,
                          model_store=ModelStore(model_dir=tempfile.mkdtemp()))


def _task(task_id: str, methods: list[str] | None = None,
          minimum_points: int = 0) -> pb.AnomalyTaskConfig:
    return pb.AnomalyTaskConfig(
        task_id=task_id, sequence_ids=["a"], methods=methods or ["HISTORICAL_MATCH"],
        minimum_points=minimum_points)


# ================= A. 数据门槛 =================

def test_insufficient_data_not_ready() -> None:
    print("\n[数据不足 → DATA_NOT_READY]")
    eng = _engine({"a": 4}, {"anomaly": {"minimum_points": 5}})
    t = _task("t-gate-low")
    res = eng.run_anomaly(t, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        f"4 < 5 应 DATA_NOT_READY，实际 {pb.AnalysisStatus.Name(res.status)}"
    assert "a=4" in res.message and "需要 5" in res.message, f"消息应带缺多少，实际 {res.message!r}"
    assert res.findings == [], "未训练未检测 → 不应有 finding"
    _ok("数据不足 → DATA_NOT_READY + 消息带缺多少")

    key = eng._anomaly_key(t, "HISTORICAL_MATCH", 1)
    assert not eng.store.is_ready(key), "门槛拦下 → 模型不应落盘"
    assert eng.needs_training(t, TaskKind.ANOMALY, config_version=1), \
        "未落盘 → needs_training 仍 True（下个 tick 继续进训练队列）"
    _ok("不训练不检测 + needs_training 仍 True")


def test_task_minimum_points_wins() -> None:
    print("\n[任务 minimum_points 优先于 config]")
    # minimum_confirmed_events=0：本测试测点数门槛，HISTORICAL_MATCH 当 no-op
    # （事件数门槛专项见 test_historical_match_event_gate）
    eng = _engine({"a": 4}, {"anomaly": {"minimum_points": 5,
                                         "minimum_confirmed_events": 0}})
    t = _task("t-gate-override", minimum_points=3)      # 任务门槛 3 < 数据 4
    res = eng.run_anomaly(t, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, \
        f"任务门槛 3 应放行（4≥3），实际 {pb.AnalysisStatus.Name(res.status)}"
    _ok("任务 minimum_points=3 覆盖 config=5 → 放行")

    eng2 = _engine({"a": 4}, {"anomaly": {"minimum_points": 5,
                                          "minimum_confirmed_events": 0}})
    t0 = _task("t-gate-override0", minimum_points=0)    # 0 → 退回 config=5
    res0 = eng2.run_anomaly(t0, config_version=1)
    assert res0.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        "minimum_points=0 应退回 config 门槛"
    _ok("minimum_points=0 → 退回 config 门槛")


def test_default_threshold_100() -> None:
    print("\n[无 anomaly config → 默认门槛 100]")
    # 未配 minimum_confirmed_events → 默认事件门槛 1 也生效；两条理由都在消息里
    eng = _engine({"a": 50})                            # 无 anomaly 节
    t = _task("t-gate-default")
    res = eng.run_anomaly(t, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        "50 < 默认 100 应 DATA_NOT_READY"
    assert "需要 100" in res.message
    _ok("无 config → 默认 100，50 点被拦（含事件门槛理由）")

    # minimum_confirmed_events=0：本段测默认点数门槛 100，事件门槛关闭
    eng2 = _engine({"a": 1000}, {"anomaly": {"minimum_confirmed_events": 0}})
    res2 = eng2.run_anomaly(_task("t-gate-default2"), config_version=1)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS, \
        "1000 ≥ 默认 100 应放行"
    _ok("无 config → 默认 100，1000 点放行")


def test_sufficient_data_trains_and_ready() -> None:
    print("\n[数据足够 → SUCCESS + 落盘 + 就绪]")
    # minimum_confirmed_events=0：本测试测点数门槛，事件门槛专项见
    # test_historical_match_event_gate / test_confirmed_event_ids_default_provider
    eng = _engine({"a": 50}, {"anomaly": {"minimum_points": 5,
                                          "minimum_confirmed_events": 0}})
    t = _task("t-gate-ok")
    res = eng.run_anomaly(t, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, \
        f"50 ≥ 5 应 SUCCESS，实际 {pb.AnalysisStatus.Name(res.status)}"
    key = eng._anomaly_key(t, "HISTORICAL_MATCH", 1)
    assert eng.store.is_ready(key), "训练后模型应落盘"
    assert not eng.needs_training(t, TaskKind.ANOMALY, config_version=1), \
        "落盘后 needs_training 应 False"
    _ok("数据足够 → SUCCESS + 模型落盘 + needs_training False")

    # 模型已就绪 → 纯推理：不再查数据规模（门槛条件 any(not ready) 为 False 短路）。
    # 用计数为 0 的 core 复跑：若门槛被触发会 DATA_NOT_READY，这里应 SUCCESS。
    eng2 = _engine({"a": 0}, {"anomaly": {"minimum_points": 5,
                                          "minimum_confirmed_events": 0}})
    eng2.store = eng.store                          # 复用已训模型（同 key 同 store）
    res2 = eng2.run_anomaly(t, config_version=1)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS, \
        "模型已就绪 → 纯推理不查数据规模，即使数据为 0 也 SUCCESS"
    _ok("模型已就绪 → 纯推理，不查数据规模")


def test_per_method_thresholds_stagger() -> None:
    print("\n[按方法门槛 → 数据够的先训，不够的推迟]")
    cfg = {"anomaly": {"minimum_points_by_method": {
        "DISCRETE_OUTLIER": 50, "MUTUAL_COUPLING": 200}}}
    eng = _engine({"a": 80, "b": 80}, cfg)
    # MUTUAL_COUPLING 需要成对双向因果边才有可训的互耦对（无 relations → 无对 → fitted=False
    # → 引擎不落盘，此测试的"数据增长后自动训练"就无从谈起），故补两条反向因果边。
    task = pb.AnomalyTaskConfig(
        task_id="t-gate-multi", sequence_ids=["a", "b"],
        methods=["DISCRETE_OUTLIER", "MUTUAL_COUPLING"],
        semantic_context=pb.SemanticContext(
            relations=[
                pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                                    relation_type="CAUSE"),
                pb.SequenceRelation(source_sequence_id="b", target_sequence_id="a",
                                    relation_type="CAUSAL"),
            ]))

    res = eng.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, \
        "DISCRETE(50) 数据够 → 应跑，返回 SUCCESS（不是 DATA_NOT_READY）"
    assert "MUTUAL_COUPLING" in res.message and "推迟" in res.message, \
        f"MUTUAL(200) 不够 → 消息应说明推迟，实际 {res.message!r}"
    key_d = eng._anomaly_key(task, "DISCRETE_OUTLIER", 1)
    key_m = eng._anomaly_key(task, "MUTUAL_COUPLING", 1)
    assert eng.store.is_ready(key_d), "够数据的方法应训练落盘"
    assert not eng.store.is_ready(key_m), "不够数据的方法不应训练"
    assert eng.needs_training(task, TaskKind.ANOMALY, config_version=1), \
        "还有未就绪方法 → 仍进训练队列（下 tick 继续）"
    _ok("门槛分方法：够的先训 + 不够的推迟（落盘状态和消息都对）")

    # 数据增长到 250 → MUTUAL 自动开始训练（不同 tick 陆续就绪，训练摊开）
    eng.core.counts["a"] = 250
    eng.core.counts["b"] = 250
    res2 = eng.run_anomaly(task, config_version=1)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS
    assert "推迟" not in res2.message, "数据够了 → 不再推迟"
    assert eng.store.is_ready(key_m), "数据增长后 MUTUAL 应训练落盘"
    assert not eng.needs_training(task, TaskKind.ANOMALY, config_version=1), \
        "全部就绪 → needs_training False（转入推理队列）"
    _ok("数据增长 → 推迟的方法自动开始训练，全部就绪")


# ================= warning_rule 等级名 → Severity =================

def test_severity_resolution() -> None:
    print("\n[等级名 → Severity 枚举]")
    eng = _engine({"a": 50}, {"anomaly": {"minimum_points": 5}})
    assert eng._resolve_severity("HIGH", pb.SEVERITY_MEDIUM) == pb.SEVERITY_HIGH
    assert eng._resolve_severity("SEVERITY_HIGH", pb.SEVERITY_LOW) == pb.SEVERITY_HIGH
    assert eng._resolve_severity("medium", pb.SEVERITY_HIGH) == pb.SEVERITY_MEDIUM, \
        "小写等级名也应认"
    assert eng._resolve_severity("", pb.SEVERITY_LOW) == pb.SEVERITY_LOW, "空 → 默认"
    assert eng._resolve_severity(None, pb.SEVERITY_MEDIUM) == pb.SEVERITY_MEDIUM, "None → 默认"
    assert eng._resolve_severity("紧急", pb.SEVERITY_MEDIUM) == pb.SEVERITY_MEDIUM, \
        "未知等级名 → 默认（不崩）"
    _ok("HIGH/SEVERITY_HIGH/小写/空/未知 → 枚举值正确")


def test_warning_rule_event_severity() -> None:
    print("\n[warning_rule 等级名 → 写事件 Severity]")
    provider = lambda task: [HistoricalEvent(event_id="ev-1", event_type="ANOMALY",
                                             sequence_ids=("a",))]
    sender = FakeSender()
    eng = AnalysisEngine(core_client=SpikeCore({"a": 50}), result_client=sender,
                         config={"inference": {"window_size": 10},
                                 "anomaly": {"minimum_points": 5}},
                         model_store=ModelStore(model_dir=tempfile.mkdtemp()),
                         historical_event_provider=provider)
    task = pb.AnomalyTaskConfig(task_id="t-sev", sequence_ids=["a"],
                                methods=["HISTORICAL_MATCH"], warning_rule="HIGH")
    res = eng.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS and len(res.findings) == 1, \
        f"尖峰应命中 1 条，实际 status={res.status} findings={len(res.findings)}"
    assert res.findings[0].severity == "HIGH", "finding severity 应带任务等级名"
    assert len(sender.sent) == 1
    assert sender.sent[0]["severity"] == pb.SEVERITY_HIGH, \
        f"事件 Severity 应来自 warning_rule=HIGH，实际 {sender.sent[0]['severity']}"
    _ok("异常事件 Severity = 任务 warning_rule（HIGH → SEVERITY_HIGH）")

    # 未配 warning_rule → 异常默认 HIGH（不破坏旧行为）
    sender2 = FakeSender()
    eng2 = AnalysisEngine(core_client=SpikeCore({"a": 50}), result_client=sender2,
                          config={"inference": {"window_size": 10},
                                  "anomaly": {"minimum_points": 5}},
                          model_store=ModelStore(model_dir=tempfile.mkdtemp()),
                          historical_event_provider=provider)
    t2 = pb.AnomalyTaskConfig(task_id="t-sev2", sequence_ids=["a"],
                              methods=["HISTORICAL_MATCH"])
    eng2.run_anomaly(t2, config_version=1)
    assert len(sender2.sent) == 1
    assert sender2.sent[0]["severity"] == pb.SEVERITY_HIGH, "无 warning_rule → 异常默认 HIGH"
    _ok("未配 warning_rule → 异常事件默认 HIGH（不破坏旧行为）")


# ================= knowledge_version 进模型缓存 key =================

def test_knowledge_version_invalidates_model() -> None:
    print("\n[knowledge_version 进模型缓存 key → 语义知识变化使模型失效]")
    # minimum_confirmed_events=0：本测试只测 key 失效，不测事件门槛
    eng = _engine({"a": 50}, {"anomaly": {"minimum_points": 5,
                                          "minimum_confirmed_events": 0}})
    task = pb.AnomalyTaskConfig(
        task_id="t-kv", sequence_ids=["a"], methods=["HISTORICAL_MATCH"],
        semantic_context=pb.SemanticContext(knowledge_version="v1"))
    eng.run_anomaly(task, config_version=1)

    key_v1 = eng._anomaly_key(task, "HISTORICAL_MATCH", 1, "v1")
    assert eng.store.is_ready(key_v1), "v1 训练后模型应就绪（key 带 kv）"

    key_v2 = eng._anomaly_key(task, "HISTORICAL_MATCH", 1, "v2")
    assert not eng.store.is_ready(key_v2), \
        "knowledge_version 变化 → 旧模型不算数，需重训"
    _ok("异常 key 带 kv：v1 就绪、v2 失效需重训")

    # 无 kv 的裸 key 与带 kv 的 key 必须不同（防止绕过）
    assert eng._anomaly_key(task, "HISTORICAL_MATCH", 1) != key_v1, \
        "无 kv key ≠ 有 kv key"
    assert eng._forecast_key(task, 1, "v1") != eng._forecast_key(task, 1, "v2"), \
        "预测 key 也应带 kv"
    _ok("裸 key ≠ 带 kv key；预测 key 同样区分 kv")


# ================= HISTORICAL_MATCH 确认事件数门槛 =================

def test_historical_match_event_gate() -> None:
    print("\n[HISTORICAL_MATCH 确认事件数启用门槛]")
    # 点数足够但事件库为空（无 provider、无 semantic_context）→ 推迟
    eng = _engine({"a": 50}, {"anomaly": {"minimum_points": 5}})
    t = _task("t-ev-gate")
    res = eng.run_anomaly(t, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        f"点数够但事件 0 条 < 默认 1 → 应 DATA_NOT_READY，" \
        f"实际 {pb.AnalysisStatus.Name(res.status)}"
    assert "确认事件" in res.message and "需要 1" in res.message, \
        f"消息应说明事件数门槛，实际 {res.message!r}"
    key = eng._anomaly_key(t, "HISTORICAL_MATCH", 1)
    assert not eng.store.is_ready(key), "门槛拦下 → 模型不应落盘"
    _ok("0 事件 + 点数够 → DATA_NOT_READY（消息带确认事件门槛）")

    # 事件数达到门槛 → 放行（provider 注入 1 条 = 默认 1）
    provider = lambda task: [HistoricalEvent(event_id="ev-1", event_type="ANOMALY",
                                             sequence_ids=("a",))]
    eng2 = AnalysisEngine(core_client=StubCore({"a": 50}), result_client=None,
                          config={"inference": {"window_size": 10},
                                  "anomaly": {"minimum_points": 5}},
                          model_store=ModelStore(model_dir=tempfile.mkdtemp()),
                          historical_event_provider=provider)
    t2 = _task("t-ev-gate2")
    res2 = eng2.run_anomaly(t2, config_version=1)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS, \
        f"1 事件 ≥ 默认 1 → 放行，实际 {pb.AnalysisStatus.Name(res2.status)}"
    assert eng2.store.is_ready(eng2._anomaly_key(t2, "HISTORICAL_MATCH", 1))
    _ok("事件数达到门槛 → 放行 + 落盘")


# ================= provider 默认接 semantic_context 事件 ID =================

def test_confirmed_event_ids_default_provider() -> None:
    print("\n[默认事件来源：semantic_context.confirmed_historical_event_ids]")
    sender = FakeSender()
    eng = AnalysisEngine(core_client=SpikeCore({"a": 50}), result_client=sender,
                         config={"inference": {"window_size": 10},
                                 "anomaly": {"minimum_points": 5}},
                         model_store=ModelStore(model_dir=tempfile.mkdtemp()))
    task = pb.AnomalyTaskConfig(
        task_id="t-prov", sequence_ids=["a"], methods=["HISTORICAL_MATCH"],
        semantic_context=pb.SemanticContext(
            confirmed_historical_event_ids=["ev-1"]))
    res = eng.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS and len(res.findings) == 1, \
        f"S 下发的事件 ID 应被默认解析 → 命中 1 条，" \
        f"实际 status={res.status} findings={len(res.findings)}"
    assert res.findings[0].anomaly_type == "HISTORICAL_MATCH"
    assert "ev-1" in res.findings[0].description, "finding 应携带事件 ID"
    assert len(sender.sent) == 1, "命中 → 应写事件"
    assert sender.sent[0]["source"] == pb.ANOMALY_SOURCE_MODEL_ANOMALY_DETECTION
    _ok("S 下发 confirmed_historical_event_ids → 默认解析 → 命中并写事件")


# ================= GCAD relations 静态关联先验（技术方案 [42]） =================

def test_relations_prior_gates_gcad_sources() -> None:
    print("\n[GCAD relations 静态关联先验 → 候选列结构门]")
    eng = _engine({"a": 50, "b": 50}, {"anomaly": {"minimum_points": 5}})
    task = pb.AnomalyTaskConfig(
        task_id="t-rel", sequence_ids=["a", "b"],
        methods=["CAUSAL_PATTERN"],
        semantic_context=pb.SemanticContext(
            sequences=[pb.SequenceMetadata(sequence_id="b", role="TARGET")],
            relations=[
                pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                                    relation_type="CAUSAL", confidence=0.8),
                pb.SequenceRelation(source_sequence_id="x", target_sequence_id="b",
                                    relation_type="CAUSAL", confidence=0.5),  # 不在任务里→跳过
                # 同 a→b 但非因果型（S 端规范里无向相关/关联）→ 不算因果候选
                pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                                    relation_type="CORRELATION", confidence=0.9),
            ]))
    prior = eng._extract_relations_prior(task, "b", ["a", "b"])
    assert prior == {0: 0.8}, f"应只取因果型 a→b（CORRELATION 不算），实际 {prior}"
    _ok("_extract_relations_prior：只取指向因变量且为因果型（CAUSE/CAUSAL）的 relation")

    # 只有相关/关联边 → 门为空 → 退回原始候选（不削减）
    only_corr = pb.AnomalyTaskConfig(
        task_id="t-rel2", sequence_ids=["a", "b"],
        methods=["CAUSAL_PATTERN"],
        semantic_context=pb.SemanticContext(
            relations=[
                pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                                    relation_type="ASSOCIATION", confidence=0.7)]))
    prior2 = eng._extract_relations_prior(only_corr, "b", ["a", "b"])
    assert prior2 == {}, f"ASSOCIATION 不算因果候选，实际 {prior2}"
    _ok("无因果边 → relations_prior 空（engine 退回全部非目标列）")

    from anomaly_models import GcadAnomalyModel
    # 深度 GCAD 的先验以"逐边稀疏阈值矩阵 H"生效（不再删自变量列）：
    #   relations 边 i→target 阈值降为 h/4（知识库标注的因果边优先保留）；
    #   |corr|<corr_threshold 的 i→target 边置 +∞（清零）；对角永远保留。
    m = GcadAnomalyModel(target_index=1, source_indices=[0],
                         relations_prior={0: 0.8})
    H = m._build_H(2, h=0.4)
    assert H[0, 1] == 0.1, "relations 边 i→target 阈值应降为 h/4=0.1"
    assert H[1, 0] == 0.4, "非先验边 → 默认阈值 h"
    assert H[0, 0] == 0 and H[1, 1] == 0, "对角自相关永远保留（阈值 0）"
    m2 = GcadAnomalyModel(target_index=2, source_indices=[0, 1],
                          correlation_prior={0: 0.05, 1: 0.9}, corr_threshold=0.1)
    H2 = m2._build_H(3, h=0.4)
    assert np.isinf(H2[0, 2]), "|corr|<corr_threshold → i→target 边清零"
    assert H2[1, 2] == 0.4, "|corr|≥阈值 → 保留默认阈值 h"
    assert H2[2, 2] == 0, "对角自相关不受相关性先验影响"
    m3 = GcadAnomalyModel(target_index=2, source_indices=[0, 1], relations_prior={})
    H3 = m3._build_H(3, h=0.4)
    assert (H3[:2, 2] == 0.4).all() and H3[2, 2] == 0, \
        "无先验 → 全图统一阈值（纯论文）+ 对角保留"
    _ok("GCAD 先验边门：relations 降阈值保留、低相关清零、对角保留、无先验全图统一")


# ================= 互耦对识别（relation_type 规范后） =================

def test_coupled_pairs_under_new_relation_spec() -> None:
    print("\n[互耦对识别：S 端 relation_type 规范 CAUSE/CAUSAL/CORRELATION/ASSOCIATION]")
    eng = _engine({"a": 50, "b": 50, "c": 50}, {"anomaly": {"minimum_points": 5}})
    seq_ids = ["a", "b", "c"]

    def pairs(relations: list) -> list:
        task = pb.AnomalyTaskConfig(
            task_id="t-cpl", sequence_ids=seq_ids, methods=["MUTUAL_COUPLING"],
            semantic_context=pb.SemanticContext(relations=relations))
        return eng._extract_coupled_pairs(task, seq_ids)

    # 1. 规范后主信号：成对反向因果边（A→B CAUSE + B→A CAUSE）→ 互耦
    got = pairs([
        pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                            relation_type="CAUSE"),
        pb.SequenceRelation(source_sequence_id="b", target_sequence_id="a",
                            relation_type="CAUSAL"),
    ])
    assert got == [(0, 1)], f"双向因果边应判互耦，实际 {got}"
    _ok("双向因果边（CAUSE+CAUSAL 反向成对）→ 互耦")

    # 2. 单向因果边（只有 A→B）→ 不算互耦
    got = pairs([
        pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                            relation_type="CAUSE"),
    ])
    assert got == [], f"单向因果边不算互耦，实际 {got}"
    _ok("单向因果边不算互耦（互耦=双向因果）")

    # 3. 无向相关/关联（CORRELATION/ASSOCIATION）反向成对 → 不算互耦
    got = pairs([
        pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                            relation_type="CORRELATION"),
        pb.SequenceRelation(source_sequence_id="b", target_sequence_id="a",
                            relation_type="ASSOCIATION"),
    ])
    assert got == [], f"相关/关联反向成对不算互耦，实际 {got}"
    _ok("CORRELATION/ASSOCIATION 反向成对不算互耦（无向）")

    # 4. 老自由字符串互耦标记（兼容）仍识别
    got = pairs([
        pb.SequenceRelation(source_sequence_id="a", target_sequence_id="b",
                            relation_type="MUTUAL_COUPLING"),
    ])
    assert got == [(0, 1)], f"老互耦标记应兼容识别，实际 {got}"
    _ok("老自由字符串互耦标记（MUTUAL/COUPLING/…）兼容保留")

    # 5. 未覆盖任务序列的边 → 跳过
    got = pairs([
        pb.SequenceRelation(source_sequence_id="a", target_sequence_id="x",
                            relation_type="CAUSE"),
        pb.SequenceRelation(source_sequence_id="x", target_sequence_id="a",
                            relation_type="CAUSE"),
    ])
    assert got == [], f"含不在任务里的序列应跳过，实际 {got}"
    _ok("不在任务里的序列的互耦对跳过")


# ================= 事件写入峰值保护（上限截断 + 失败计数） =================

def test_event_write_cap_and_failure_count() -> None:
    print("\n[事件写入峰值保护：上限截断 + 失败计数]")
    # 3 条确认事件 → HISTORICAL_MATCH 最多报 3 条；cap=2 → 写 2、截断 1
    sender = FakeSender()
    eng = AnalysisEngine(
        core_client=SpikeCore({"a": 50}), result_client=sender,
        config={"inference": {"window_size": 10},
                "anomaly": {"minimum_points": 5,
                            "max_events_per_run": 2}},
        model_store=ModelStore(model_dir=tempfile.mkdtemp()))
    task = pb.AnomalyTaskConfig(
        task_id="t-cap", sequence_ids=["a"], methods=["HISTORICAL_MATCH"],
        semantic_context=pb.SemanticContext(
            confirmed_historical_event_ids=["ev-1", "ev-2", "ev-3"]))
    res = eng.run_anomaly(task, config_version=1)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS
    assert len(res.findings) == 3, f"应检出 3 条（top_k=3），实际 {len(res.findings)}"
    assert len(sender.sent) == 2, "cap=2 → 只写 2 条"
    assert "成功 2 条" in res.message and "截断 1 条" in res.message, \
        f"消息应带截断说明，实际 {res.message!r}"
    assert eng._event_stats["anomaly_ok"] == 2 and eng._event_stats["anomaly_cap"] == 1
    _ok("超上限 → 截断并进消息（写 2 截 1）")

    # 发送失败 → 计数进消息，不重试
    failing = FailingSender()
    eng2 = AnalysisEngine(
        core_client=SpikeCore({"a": 50}), result_client=failing,
        config={"inference": {"window_size": 10},
                "anomaly": {"minimum_points": 5}},
        model_store=ModelStore(model_dir=tempfile.mkdtemp()))
    res2 = eng2.run_anomaly(
        pb.AnomalyTaskConfig(
            task_id="t-fail", sequence_ids=["a"], methods=["HISTORICAL_MATCH"],
            semantic_context=pb.SemanticContext(
                confirmed_historical_event_ids=["ev-1", "ev-2", "ev-3"])),
        config_version=1)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS
    assert "失败 3 条" in res2.message, f"失败应计数进消息，实际 {res2.message!r}"
    assert eng2._event_stats["anomaly_fail"] == 3
    _ok("发送失败 → 计数进消息（不重试）")


# ================= B. MODEL_NOT_READY 查询合成 =================

def _servicer_with(engine: AnalysisEngine, task_id: str, kind: TaskKind,
                   task, config_version: int = 1) -> AnalysisServicer:
    registry = TaskRegistry()
    registry.register("default", task, kind, config_version=config_version)
    return AnalysisServicer(registry, ResultRepository(), engine)


def test_query_model_not_ready_anomaly() -> None:
    print("\n[查询合成 MODEL_NOT_READY·异常]")
    eng = _engine({"a": 0})                          # 模型从未训练
    task = _task("t-q-a")
    serv = _servicer_with(eng, "t-q-a", TaskKind.ANOMALY, task)

    resp = serv.QueryAnomalyResults(
        pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(task_id="t-q-a")), None)
    assert len(resp.results) == 1, "任务注册 + 模型未就绪 → 应合成 1 条"
    r = resp.results[0]
    assert r.status == pb.ANALYSIS_STATUS_MODEL_NOT_READY, \
        f"应 MODEL_NOT_READY，实际 {pb.AnalysisStatus.Name(r.status)}"
    assert r.task_id == "t-q-a"
    assert "训练" in r.message
    assert len(r.findings) == 0, "AnomalyResult 格式：findings 为空"
    _ok("异常任务已注册 + 模型未就绪 → 合成 AnomalyResult(MODEL_NOT_READY)")

    resp2 = serv.QueryAnomalyResults(
        pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(task_id="t-q-a",
                                                           latest_only=True)), None)
    assert len(resp2.results) == 1 and resp2.results[0].status == \
        pb.ANALYSIS_STATUS_MODEL_NOT_READY, "latest_only 也应合成"
    _ok("latest_only 同样合成")


def test_query_model_not_ready_forecast_format() -> None:
    print("\n[查询合成 MODEL_NOT_READY·预测格式]")
    eng = _engine({"a": 0})
    task = pb.ForecastTaskConfig(task_id="t-q-f", target_sequence_ids=["a"])
    serv = _servicer_with(eng, "t-q-f", TaskKind.FORECAST, task)

    resp = serv.QueryForecastResults(
        pb.QueryForecastResultsRequest(query=pb.ResultQuery(task_id="t-q-f")), None)
    assert len(resp.results) == 1
    r = resp.results[0]
    assert r.status == pb.ANALYSIS_STATUS_MODEL_NOT_READY
    assert hasattr(r, "timestamps_ms") and r.timestamps_ms == [], \
        "ForecastResult 格式应带 timestamps_ms（且为空）"
    assert r.model_version == ""
    _ok("预测任务 → ForecastResult 格式（status=MODEL_NOT_READY）")


def test_real_result_not_overridden() -> None:
    print("\n[仓库有真实结果 → 返回真实结果]")
    eng = _engine({"a": 0})
    task = _task("t-q-real")
    registry = TaskRegistry()
    registry.register("default", task, TaskKind.ANOMALY, config_version=1)
    repo = ResultRepository()
    real = pb.AnomalyResult(task_id="t-q-real", run_id="run-x",
                            generated_at_ms=1, status=pb.ANALYSIS_STATUS_SUCCESS,
                            message="真实结果", findings=[])
    repo.put("default", "t-q-real", real)
    serv = AnalysisServicer(registry, repo, eng)

    resp = serv.QueryAnomalyResults(
        pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(task_id="t-q-real",
                                                           latest_only=True)), None)
    assert len(resp.results) == 1
    assert resp.results[0].status == pb.ANALYSIS_STATUS_SUCCESS, \
        "仓库有真实结果 → 返回真实结果，不合成 MODEL_NOT_READY"
    assert resp.results[0].message == "真实结果"
    _ok("真实结果优先，不合成")


def test_unknown_task_empty() -> None:
    print("\n[未知任务 → 空]")
    serv = AnalysisServicer(TaskRegistry(), ResultRepository(), _engine({"a": 0}))
    resp = serv.QueryAnomalyResults(
        pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(task_id="t-none")), None)
    assert resp.results == [], "未注册任务不应合成（查不到就返回空）"
    _ok("未知任务 → 空（不合成）")


def test_ready_but_no_result_empty() -> None:
    print("\n[模型已就绪 + 仓库空 → 空]")
    # minimum_confirmed_events=0：让 HISTORICAL_MATCH 训练落盘（否则模型未就绪，
    # 查询会合成 MODEL_NOT_READY，测不到"仓库空返回空"）
    eng = _engine({"a": 50}, {"anomaly": {"minimum_points": 5,
                                          "minimum_confirmed_events": 0}})
    task = _task("t-q-ready")
    eng.run_anomaly(task, config_version=1)          # 训练完成 → 模型就绪
    serv = _servicer_with(eng, "t-q-ready", TaskKind.ANOMALY, task)
    resp = serv.QueryAnomalyResults(
        pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(task_id="t-q-ready")), None)
    assert resp.results == [], "模型已就绪 → 不合成 MODEL_NOT_READY（返回空，继续轮询）"
    _ok("模型已就绪 + 仓库空 → 空（不合成）")


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    test_insufficient_data_not_ready()
    test_task_minimum_points_wins()
    test_default_threshold_100()
    test_sufficient_data_trains_and_ready()
    test_per_method_thresholds_stagger()
    test_severity_resolution()
    test_warning_rule_event_severity()
    test_knowledge_version_invalidates_model()
    test_historical_match_event_gate()
    test_confirmed_event_ids_default_provider()
    test_relations_prior_gates_gcad_sources()
    test_coupled_pairs_under_new_relation_spec()
    test_event_write_cap_and_failure_count()
    test_query_model_not_ready_anomaly()
    test_query_model_not_ready_forecast_format()
    test_real_result_not_overridden()
    test_unknown_task_empty()
    test_ready_but_no_result_empty()
    print(f"\n异常数据门槛 + MODEL_NOT_READY 查询专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
