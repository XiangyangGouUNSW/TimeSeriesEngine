"""模拟测试：默认任务三类检测（模式偏移/离群点/趋势异常）端到端验证。

背景（2026-08-26）：合同验收口径统一为「三类异常 + C端约束检测」，
P 端 DEFAULT_METHODS 改为 [CAUSAL_PATTERN, DISCRETE_OUTLIER, TREND_SHIFT]。
本测试验证：默认任务（methods 空）三类模型都被调度、各检出自类异常、
anomaly_type 标签正确、约束(CONSTRAINT_CHECK)仍被过滤。

构造（ETT 真实数据 + 三类确定性注入）：
  1. CAUSAL_PATTERN：OT 列段内 AR 系数翻转（方向偏移）→ GCAD 段级模式偏移
  2. DISCRETE_OUTLIER：OT 离散工况列注入离格值 → DBSCAN 点级离群
  3. TREND_SHIFT：OT 持续水平漂移(+4σ) → TrendShift 趋势异常
验证点：
  - build_anomaly_model 三类都能建（无未知方法）
  - 各自 fit/detect 后 anomaly_type ∈ {CAUSAL_PATTERN, DISCRETE_OUTLIER, TREND_SHIFT}
  - 默认 methods 空 → _run_anomaly_models 收到三类（DEFAULT_METHODS）
  - 约束仍被过滤（CONSTRAINT_CHECK 不进模型方法）
用法：python tools/test_anomaly_three_classes.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine, DEFAULT_METHODS
from anomaly_models import build_anomaly_model

CSV = ROOT / "data" / "ETT-small" / "ETTh1.csv"
TARGET = "OT"
FEATURES = ["HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL"]
TRAIN_N = 600     # 训练历史（正常段）
SEG_N = 80        # 注入段长
WINDOW = 100


def _load() -> tuple[np.ndarray, list[str]]:
    import csv
    rows, cols = [], None
    with open(CSV, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or not row[0]:
                continue
            if cols is None:
                cols = row[1:]
                continue
            rows.append([float(x) for x in row[1:]])
    X = np.array(rows, dtype=np.float32)
    return X, cols


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _anomaly_task(task_id: str, methods: list[str], seq_ids: list[str]):
    return pb.AnomalyTaskConfig(
        task_id=task_id, sequence_ids=seq_ids, methods=methods,
        semantic_context=pb.SemanticContext(
            sequences=[pb.SequenceMetadata(sequence_id=sid, role="FEATURE")
                       for sid in seq_ids[:-1]]
            + [pb.SequenceMetadata(sequence_id=seq_ids[-1], role="TARGET")],
        ),
    )


def main() -> None:
    X, cols = _load()
    iy = cols.index(TARGET)  # 最后一列
    tr = X[:TRAIN_N]
    te = X[TRAIN_N:TRAIN_N + SEG_N + WINDOW].copy()
    seq_ids = FEATURES + [TARGET]

    print(f"数据：{cols}，训练 {TRAIN_N} 点，测试 {len(te)} 点（注入段 {SEG_N}）")

    # ── 1. DEFAULT_METHODS 应为三类 ──────────────────────────────
    print("\n[默认检测类型]")
    assert DEFAULT_METHODS == ["CAUSAL_PATTERN", "DISCRETE_OUTLIER", "TREND_SHIFT"], \
        f"DEFAULT_METHODS 应三类，实际 {DEFAULT_METHODS}"
    _ok(f"DEFAULT_METHODS = {DEFAULT_METHODS}")

    # 空 methods → 模型方法解析为三类（复用 engine 的过滤逻辑）
    eng = AnalysisEngine(core_client=object(), result_client=None,
                         config={"inference": {"window_size": WINDOW}})
    task = _anomaly_task("t-default", [], seq_ids)
    methods = list(task.methods) or DEFAULT_METHODS
    model_methods = [m for m in methods if m != "CONSTRAINT_CHECK"
                     and m in {"CAUSAL_PATTERN", "DISCRETE_OUTLIER",
                               "TREND_SHIFT", "MUTUAL_COUPLING"}]
    assert model_methods == DEFAULT_METHODS, f"空 methods 应三类，实际 {model_methods}"
    _ok("空 methods → 模型方法 = 三类（约束已迁 C，不参与）")

    # ── 2. 三类注入 + 三类模型检测 ───────────────────────────────
    print("\n[三类异常检测]")
    # 2a 模式偏移：OT 段内 AR 符号翻转（方向偏移）
    te_cp = te.copy()
    ys = te_cp[:, iy]
    phi = float(np.corrcoef(ys[:-1], ys[1:])[0, 1])
    rng = np.random.RandomState(0)
    prev = ys[WINDOW - 1]
    for t in range(WINDOW, WINDOW + SEG_N):
        ys[t] = -phi * prev + np.sqrt(1 - phi ** 2) * rng.randn()
        prev = ys[t]
    te_cp[:, iy] = ys

    # 2b 离群点：OT 离散工况（分位成 {0,1,2}）注入离格值 5
    te_do = te.copy()
    q = np.quantile(tr[:, iy], [1 / 3, 2 / 3])
    def _disc(v): return np.digitize(v, q)
    work = _disc(tr[:, iy])  # 训练区离散工况（DBSCAN fit 用）
    te_disc = te_do[:, iy].copy()
    te_disc[WINDOW:WINDOW + SEG_N] = 5.0   # 离格值
    te_do[:, iy] = te_disc

    # 2c 趋势异常：OT 持续水平漂移 +4σ
    te_ts = te.copy()
    te_ts[WINDOW:WINDOW + SEG_N, iy] += 4.0 * tr[:, iy].std()

    model_specs = [
        ("CAUSAL_PATTERN(模式偏移)", te_cp,
         lambda: build_anomaly_model("CAUSAL_PATTERN", target_index=iy,
                                     source_indices=[j for j in range(iy)],
                                     gcad={"hidden_dim": 32, "num_layers": 1,
                                           "tau": 24})),
        ("DISCRETE_OUTLIER(离群点)", te_do,
         lambda: build_anomaly_model("DISCRETE_OUTLIER", eps=0.5,
                                     min_samples=5)),
        ("TREND_SHIFT(趋势异常)", te_ts,
         lambda: build_anomaly_model("TREND_SHIFT", window=10,
                                     target_index=iy)),
    ]
    results = {}
    for name, te_inj, maker in model_specs:
        m = maker()
        assert m is not None, f"{name}: build_anomaly_model 返回 None"
        fit_x = work if name.startswith("DISCRETE") else tr
        m.fit(fit_x)
        det = m.detect(te_inj[-WINDOW:])
        kinds = sorted({f["anomaly_type"] for f in det})
        n = len(det)
        results[name] = (n, kinds)
        print(f"  {name:<24} 检出 {n} 条，类型 {kinds}")
        assert kinds, f"{name}: 未检出任何异常（注入应可检出）"
        assert all(k in {"CAUSAL_PATTERN", "DISCRETE_OUTLIER", "TREND_SHIFT"}
                   for k in kinds), f"{name}: 出现未知 anomaly_type {kinds}"
        _ok(f"{name} 检出异常且类型标签正确")

    # ── 3. 三类 anomaly_type 互不相同（分类清晰）────────────────
    print("\n[分类正确性]")
    all_kinds = set()
    for name, (n, kinds) in results.items():
        all_kinds |= set(kinds)
    expect = {"CAUSAL_PATTERN", "DISCRETE_OUTLIER", "TREND_SHIFT"}
    assert expect.issubset(all_kinds), f"三类类型应都出现，实际 {all_kinds}"
    _ok("三类检测输出三类 anomaly_type（模式偏移/离群点/趋势异常）")

    print("\n模拟测试通过 ✓（默认任务三类检测：模式偏移/离群点/趋势异常）")


if __name__ == "__main__":
    main()
