"""异常检测模型基线评估：DBSCAN vs GCAD，在 ETT 数据上对比。

用法（sfkg 环境）：
  python tools/evaluate_anomaly_models.py

思路：
  用 ETTh1 数据，构造"正常段 + 注入异常段"的测试：
  - DBSCAN：在 OT 列注入尖峰（离群点），看能否检出
  - GCAD：破坏序列间的相关性（模式偏离），看能否检出
  输出召回率 / 精确率 / 准确率，作为后续对比 SOTA 模型的基线。
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

from anomaly_models import DbscanAnomalyModel, GcadAnomalyModel


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


def eval_gcad(data: np.ndarray):
    """GCAD：破坏序列相关性（模式偏离）。"""
    x = zscore(data)                       # 标准化
    train = x[:1000]                       # 正常段训练
    # 正常窗口（不注入，测误报）
    win_normal = x[1500:1550].copy()
    # 异常窗口：后半段破坏序列 0 的相关性（反转）
    win_anom = x[1500:1550].copy()
    win_anom[25:, 0] = -win_anom[25:, 0]
    true_anomalous = set(range(25, 50))    # 后半 25 点

    model = GcadAnomalyModel(lag=3, residual_quantile=0.9)
    model.fit(train)
    findings = model.detect(win_anom)
    detected = {f["index"] for f in findings}
    r, p, a = point_metrics(detected, true_anomalous, len(win_anom))
    # 正常窗口误报
    findings_normal = model.detect(win_normal)
    fp_normal = len(findings_normal)
    return r, p, a, len(findings), fp_normal


def main() -> None:
    data = load_ett(str(ROOT / "data" / "ETT-small" / "ETTh1.csv"))
    print(f"数据：{data.shape[0]} 点 × {data.shape[1]} 序列（ETTh1）")
    print("=" * 60)

    # DBSCAN
    r, p, a, n = eval_dbscan(data)
    print("[DBSCAN] 离散离群（OT 注入 3 尖峰）")
    print(f"  检出 {n} 条 | 召回 {r:.2f} | 精确 {p:.2f} | 准确 {a:.2f}")

    # GCAD
    r, p, a, n, fp = eval_gcad(data)
    print("[GCAD] 多变量模式偏离（破坏序列相关性）")
    print(f"  检出 {n} 条 | 召回 {r:.2f} | 精确 {p:.2f} | 准确 {a:.2f}")
    print(f"  正常窗口误报 {fp} 条（应为 0 为佳）")
    print("=" * 60)
    print("说明：这是注入异常测试的基线。召回=注入异常检出比例，")
    print("精确=检出的点里真异常比例，准确=全部点判断对的比例。")


if __name__ == "__main__":
    main()
