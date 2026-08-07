"""GCAD 多变量集成验证：engine._run_anomaly_models 完整链路。

前置：fake_core_server 已在 localhost:50051 起好（python tools/fake_core_server.py）。

验证：
  1. GrpcCoreDataClient.get_correlation_vector → C computeBasicStatistics（相关性先验）；
  2. engine._extract_roles 从 semantic_context.role 提取 TARGET/FEATURE；
  3. engine._get_correlation_prior 把 {序列ID: 系数} 转成列索引先验；
  4. _run_anomaly_models 训练 GCAD 并检测模式偏离。

用法（sfkg 环境）：
  python tools/test_gcad_integration.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from grpc_client import GrpcCoreDataClient

TARGET = "ETTh1_OT"
FEATURES = ["ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL",
            "ETTh1_MULL", "ETTh1_LUFL", "ETTh1_LULL"]


def _cfg():
    return {"inference": {"window_size": 100}}


def main() -> None:
    core = GrpcCoreDataClient("localhost", 50051)

    # 1. 相关性先验
    corr = core.get_correlation_vector(TARGET, FEATURES)
    print("[1] get_correlation_vector 相关性先验（因变量 = %s）：" % TARGET)
    for sid, c in corr.items():
        print(f"    {sid:<14} corr = {c:+.3f}")
    if not corr:
        print("    未返回任何相关性 → 链路异常")
        return

    # 2. 构造带 role 的异常任务（因变量在列顺序最后）
    seq_ids = FEATURES + [TARGET]
    task = pb.AnomalyTaskConfig(
        task_id="task-gcad-mv-001",
        task_name="GCAD 多变量集成验证",
        sequence_ids=seq_ids,
        methods=["CAUSAL_PATTERN", "TREND_SHIFT", "MUTUAL_COUPLING"],
        semantic_context=pb.SemanticContext(
            sequences=(
                [pb.SequenceMetadata(sequence_id=sid, role="FEATURE") for sid in FEATURES]
                + [pb.SequenceMetadata(sequence_id=TARGET, role="TARGET")]
            ),
            # 互耦对：HUFL↔HULL（relation_type 带 COUPLING 标记 + 成对反向，两种识别方式都覆盖）
            relations=[
                pb.SequenceRelation(
                    relation_id="rel-ett-ot", target_sequence_id=TARGET,
                    source_sequence_id=FEATURES[0], relation_type="CAUSE",
                    lag_steps=1, confidence=0.9),
                pb.SequenceRelation(
                    relation_id="rel-cpl-01", target_sequence_id=FEATURES[1],
                    source_sequence_id=FEATURES[0], relation_type="COUPLING"),
                pb.SequenceRelation(
                    relation_id="rel-cpl-02", target_sequence_id=FEATURES[0],
                    source_sequence_id=FEATURES[1], relation_type="COUPLING"),
            ],
        ),
    )

    # 3. 引擎内部：确认角色提取 + 先验转列索引
    engine = AnalysisEngine(core_client=core, result_client=None, config=_cfg())
    target_id, source_ids = engine._extract_roles(task, seq_ids)
    print(f"[2] _extract_roles → 因变量 {target_id}，自变量 {source_ids}")
    prior = engine._get_correlation_prior(target_id, source_ids, seq_ids)
    print(f"[3] _get_correlation_prior → 列索引先验 {prior}")

    # 4. 互耦对识别（MUTUAL_COUPLING 用）
    pairs = engine._extract_coupled_pairs(task, seq_ids)
    print(f"[3b] _extract_coupled_pairs → 互耦对列索引 {pairs}")

    # 5. 跑模型检测（CAUSAL_PATTERN + TREND_SHIFT + MUTUAL_COUPLING）
    #    检测类型显式化后，_run_anomaly_models 接收 model_methods 参数
    findings = engine._run_anomaly_models(
        task, ["CAUSAL_PATTERN", "TREND_SHIFT", "MUTUAL_COUPLING"])
    print(f"[4] _run_anomaly_models 检出 {len(findings)} 条模式偏离")
    for f in findings[:8]:
        print(f"    {f['anomaly_type']} @点{f['index']} 分数 {f['score']:.3f}")
    kinds = sorted({f["anomaly_type"] for f in findings})
    print(f"    命中方法：{kinds}")
    print("\n集成验证通过 ✓（链路通；GCAD 无注入应少报，TREND_SHIFT 可能检出 OT 自身趋势漂移）")


if __name__ == "__main__":
    main()
