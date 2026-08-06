"""异常检测模型评估：DBSCAN 离群 + GCAD 多自变量→单因变量。

用法（sfkg 环境）：
  python tools/evaluate_anomaly_models.py

思路：
  - DBSCAN：OT 列注入尖峰，看离散离群检出。
  - GCAD：合成"多自变量→单因变量"因果数据
        y[t] = a1*x1[t-1] + a2*x2[t-1] + a3*x3[t-1] + ε
    （x1..x3 取 ETT 真实列，另加 1 个与 y 无关的干扰列）。
    注入三种模式偏移（系数变化 / 关系反转 / 变量失效），
    对比【有 / 无相关性先验】时的检出能力（召回 / 精确 / 正常窗口误报）。
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

from anomaly_models import (
    DbscanAnomalyModel,
    GcadAnomalyModel,
    TrendShiftAnomalyModel,
)


def load_ett(csv_path: str) -> np.ndarray:
    """读 ETTh1，返回 [time, seq_count] 的数值矩阵。"""
    with open(csv_path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        n_seq = len(header) - 1
        cols = [[] for _ in range(n_seq)]
        for row in reader:
            if not row or not row[0]:
                continue
            for i in range(n_seq):
                cols[i].append(float(row[i + 1]))
    return np.array(cols).T   # [time, seq_count]


def zscore(data: np.ndarray) -> np.ndarray:
    """按列标准化到零均值、单位方差。"""
    return (data - data.mean(axis=0)) / (data.std(axis=0) + 1e-8)


def point_metrics(detected: set[int], true_anomalous: set[int], total: int):
    """按点计算 召回/精确/准确。"""
    tp = len(detected & true_anomalous)
    fn = len(true_anomalous - detected)
    fp = len(detected - true_anomalous)
    tn = total - len(true_anomalous | detected)
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    accuracy = (tp + tn) / total
    return recall, precision, accuracy


def eval_dbscan(data: np.ndarray):
    """DBSCAN：OT 列注入尖峰。"""
    ot = data[:, -1].reshape(-1, 1)
    train = ot[:1000]                      # 正常段训练
    win = ot[1500:1550].copy()             # 测试窗口（50 点）
    true_anomalous = {45, 46, 47}          # 注入 3 个尖峰
    win[45:48] = float(win.max()) + 100.0

    model = DbscanAnomalyModel(eps=5.0, min_samples=3)
    model.fit(train)
    findings = model.detect(win)
    detected = {f["index"] for f in findings}
    r, p, a = point_metrics(detected, true_anomalous, len(win))
    return r, p, a, len(findings)


# ---------------- GCAD 多自变量→单因变量 ----------------

def build_causal_data(data: np.ndarray, seed: int = 0):
    """合成"多自变量→单因变量"因果数据。

    自变量取 ETT 前三列 x1,x2,x3 + 1 个与因变量无关的干扰列 noise_x。
    因变量 y[t] = a1*x1[t-1] + a2*x2[t-1] + a3*x3[t-1] + ε（滞后 1 的格兰杰因果）。
    返回 [x1,x2,x3,noise_x,y] 的 [time, 5] 矩阵和系数 (a1,a2,a3)。
    """
    x = zscore(data)
    x1, x2, x3 = x[:, 0], x[:, 1], x[:, 2]
    rng = np.random.RandomState(seed)
    noise_x = rng.normal(0, 1.0, len(x))       # 干扰列：和 y 无关
    noise = rng.normal(0, 0.05, len(x))        # 观测噪声
    a1, a2, a3 = 1.0, 0.8, -0.6
    y = np.empty(len(x))
    y[0] = 0.0
    y[1:] = a1 * x1[:-1] + a2 * x2[:-1] + a3 * x3[:-1]
    y += noise
    M = np.column_stack([x1, x2, x3, noise_x, y])
    return M, (a1, a2, a3)


def inject_causal_anomaly(M: np.ndarray, a, kind: str, start: int = 25):
    """在窗口后半段注入模式偏移。返回 [50 行] 异常窗口。

    窗口内 y[t] 依赖 x[t-1]：注入段局部 25..49 需用 x 局部 24..48 重算 y。
    """
    win = M[1500:1550].copy()
    a1, a2, a3 = a
    x1, x2, x3 = win[:, 0], win[:, 1], win[:, 2]
    s1, s2, s3 = x1[24:49], x2[24:49], x3[24:49]   # x[t-1]
    if kind == "coef_shift":      # 系数变化：a1 翻倍（关系破坏的一种）
        win[start:, 4] = 2 * a1 * s1 + a2 * s2 + a3 * s3
    elif kind == "rel_break":     # 关系反转：x1 影响方向反向
        win[start:, 4] = -a1 * s1 + a2 * s2 + a3 * s3
    elif kind == "source_drop":   # 变量失效：x1 不再影响 y
        win[start:, 4] = a2 * s2 + a3 * s3
    else:
        raise ValueError(kind)
    return win


def corr_prior_from_train(train: np.ndarray, target_idx: int) -> dict[int, float]:
    """模拟 C 端 computeBasicStatistics：用训练段算因变量与各自变量的相关。"""
    y = train[:, target_idx]
    return {j: float(np.corrcoef(train[:, j], y)[0, 1])
            for j in range(train.shape[1]) if j != target_idx}


KINDS = {"coef_shift": "系数变化a1×2", "rel_break": "关系反转a1→-a1",
         "source_drop": "变量失效a1→0"}


def eval_gcad_multivariate(data: np.ndarray):
    """GCAD 调优：①相关性先验筛选验证 ②阈值(quantile)权衡扫描。

    ① 对比有无先验时模型实际使用的自变量列（_sources），确认筛选真的剔除了干扰列；
    ② 不同 residual_quantile 下三种注入异常的平均召回 + 正常窗口误报，找误报/召回平衡点。
    """
    M, a = build_causal_data(data)
    train = M[:1000]                          # 正常段训练
    target_idx = 4
    prior = corr_prior_from_train(train, target_idx)
    kept = sorted(j for j, c in prior.items() if abs(c) >= 0.1)
    dropped = sorted(j for j in prior if j not in kept)
    win_normal = M[1500:1550].copy()          # 正常窗口（测误报）

    # ① 先验筛选验证：模型最终使用的自变量列
    m_no_prior = GcadAnomalyModel(lag=3, residual_quantile=0.95,
                                  target_index=target_idx)
    m_no_prior.fit(train)
    m_prior = GcadAnomalyModel(lag=3, residual_quantile=0.95,
                               target_index=target_idx,
                               correlation_prior=prior, corr_threshold=0.1)
    m_prior.fit(train)
    sel_info = (m_no_prior._sources, m_prior._sources)

    # ② 阈值权衡：quantile 越高越严格（误报↓但可能漏检）
    rows = []
    for q in (0.90, 0.95, 0.98, 0.99):
        recalls, fp_list = [], []
        for kind in KINDS:
            model = GcadAnomalyModel(lag=3, residual_quantile=q,
                                     target_index=target_idx,
                                     correlation_prior=prior, corr_threshold=0.1)
            model.fit(train)
            win = inject_causal_anomaly(M, a, kind)
            findings = model.detect(win)
            r, _, _ = point_metrics({f["index"] for f in findings},
                                    set(range(25, 50)), len(win))
            recalls.append(r)
            fp_list.append(len(model.detect(win_normal)))
        rows.append((q, float(np.mean(recalls)), fp_list))
    return rows, sel_info, kept, dropped


# ---------------- TREND_SHIFT 单变量趋势异常 ----------------

TREND_KINDS = {"ramp": "缓慢漂移", "step": "阶跃", "reversal": "斜率突变"}


def build_trend_data(n: int, seed: int = 1) -> np.ndarray:
    """平滑单变量序列：平坦基线 + 小噪声（趋势漂移测试用，独立于因果数据）。"""
    rng = np.random.RandomState(seed)
    return (50.0 + rng.normal(0, 0.3, n)).reshape(-1, 1)


def eval_trend(n: int):
    """TREND_SHIFT：水平漂移/阶跃/斜率突变三类注入 + 参数扫描。

    返回 (scan, best, best_detail)：扫描表、最优参数、最优参数下三场景指标。
    最优选择：平均召回≥0.9 里总误报最小；没有达标的就取误报最小。
    """
    y = build_trend_data(n)
    train = y[:1000]
    sigma = float(train.std())
    win_normal = y[1500:1550]
    true_anomalous = set(range(25, 50))

    def inject(kind):
        win = win_normal.copy().flatten()
        if kind == "ramp":        # 缓慢漂移：每点 +0.10σ，累计 2.5σ
            win[25:] += 0.10 * sigma * np.arange(25, 50)
        elif kind == "step":      # 阶跃：后半段整体上移 2σ
            win[25:] += 2.0 * sigma
        elif kind == "reversal":  # 斜率突变：后半段转强上升斜坡（0.10σ/点）
            ramp = 0.10 * sigma * np.arange(25, 50)
            win[25:] = win[24] + ramp
        return win.reshape(-1, 1)

    scan = []
    for w in (4, 6, 10):
        for lv in (2.0, 2.5, 3.0):
            for sm in (2.0, 2.5, 3.0):
                recalls, fps = [], []
                for kind in TREND_KINDS:
                    model = TrendShiftAnomalyModel(
                        window=w, level_limit=lv, slope_std_mult=sm)
                    model.fit(train)
                    findings = model.detect(inject(kind))
                    r, _, _ = point_metrics(
                        {f["index"] for f in findings}, true_anomalous, len(win_normal))
                    recalls.append(r)
                    fps.append(len(model.detect(win_normal)))
                scan.append((w, lv, sm, float(np.mean(recalls)), fps, sum(fps)))
    ok = [s for s in scan if s[3] >= 0.9]
    best = min(ok if ok else scan, key=lambda s: (s[5], -s[3]))

    # 最优参数下各场景详细指标（对照验收线）
    model = TrendShiftAnomalyModel(
        window=best[0], level_limit=best[1], slope_std_mult=best[2])
    model.fit(train)
    detail = []
    for kind in TREND_KINDS:
        findings = model.detect(inject(kind))
        r, p, acc = point_metrics(
            {f["index"] for f in findings}, true_anomalous, len(win_normal))
        detail.append((kind, r, p, acc, len(model.detect(win_normal))))
    return scan, best, detail


def main() -> None:
    data = load_ett(str(ROOT / "data" / "ETT-small" / "ETTh1.csv"))
    print(f"数据：{data.shape[0]} 点 × {data.shape[1]} 序列（ETTh1）")
    print("=" * 60)

    # DBSCAN
    r, p, a, n = eval_dbscan(data)
    print("[DBSCAN] 离散离群（OT 注入 3 尖峰）")
    print(f"  检出 {n} 条 | 召回 {r:.2f} | 精确 {p:.2f} | 准确 {a:.2f}")

    # GCAD 多自变量→单因变量
    rows, (src_no_prior, src_prior), kept, dropped = eval_gcad_multivariate(data)
    print("\n[GCAD] 多自变量→单因变量（y = a1·x1[t-1] + a2·x2[t-1] + a3·x3[t-1]）")
    print("  列：0=x1  1=x2  2=x3  3=干扰列(与y无关)  4=因变量y")
    print("  ①相关性先验筛选验证：")
    print(f"     无先验：模型使用全部自变量列 {src_no_prior}")
    print(f"     有先验：剔除 {dropped}（|corr|<0.1），保留 {src_prior}")
    print(f"  ②阈值权衡（三种模式偏移平均召回，正常窗口 50 点）：")
    print(f"     {'quantile':<10}{'平均召回':>8}  各异常误报(系数变化/关系反转/变量失效)")
    for q, avg_r, fp_list in rows:
        print(f"     {q:<10.2f}{avg_r:>8.2f}  {fp_list}")

    # TREND_SHIFT 单变量趋势异常
    scan, best, detail = eval_trend(data.shape[0])
    print("\n[TREND_SHIFT] 单变量趋势异常（水平漂移 / 阶跃 / 斜率突变）")
    print("  参数扫描（window/level_limit/slope_std_mult → 平均召回, 误报[漂移,阶跃,突变]）：")
    for w, lv, sm, avg_r, fps, tot in scan:
        print(f"    {w}/{lv:.1f}/{sm:.1f} → 召回 {avg_r:.2f}  误报{tot} {fps}")
    w0, lv0, sm0, avg0, _, _ = best
    print(f"  最优参数 window={w0} level_limit={lv0} slope_std_mult={sm0}"
          f" → 平均召回 {avg0:.2f}")
    print(f"  {'场景':<10}{'召回':>6}{'精确':>6}{'准确':>6}{'正常误报':>8}")
    for kind, r, p, acc, fp in detail:
        print(f"  {TREND_KINDS[kind]:<10}{r:>6.2f}{p:>6.2f}{acc:>6.2f}{fp:>8}")
    print("=" * 60)
    print("说明：三种模式偏移 = 破坏因变量与自变量的因果关系（模式偏移/趋势异常）。")
    print("quantile 是训练残差的分位数阈值：越高越严格（误报低但可能漏检），")
    print("召回=注入异常检出比例，误报=正常窗口里被误报的点数（应为 0 为佳）。")


if __name__ == "__main__":
    main()
