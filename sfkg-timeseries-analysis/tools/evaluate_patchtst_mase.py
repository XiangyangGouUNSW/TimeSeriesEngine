#!/usr/bin/env python
"""PatchTST 预测 MASE 评测：ETTh1 前 80% 训练、后 20% 滚动预测，全列报。

背景（core 需求说明书 FUNC-TSA04 验收标准第 4 条）：
  「在指定测试集、预测步长和评价方法下，时序预测结果 MASE 不高于 0.6。」

MASE（Hyndman & Koehler 2006）：对每个预测步 h，MASE_h = MAE_h / scale，
scale = 训练集上 naive 预测的 MAE（in-sample）；总 MASE = mean_h(MASE_h)。

口径：context_length=168, prediction_length=24（工厂 config）；测试集 = 后 20%；
滚动预测（窗口滑 24 步不重叠）；scale 报 lag-1（非季节）与 lag-24（季节）两种。

用法：taskset -c 0-19,40-59 env SFKG_MAX_THREADS=4 python tools/evaluate_patchtst_mase.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from patchtst_forecaster import PatchTSTForecaster  # noqa: E402


def load_ett(csv_path: str, prefix: str = "ETTh1"):
    import csv
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        cols = [prefix + "_" + c for c in header[1:]]
        rows = [[float(v) for v in row[1:]] for row in reader if row and row[0]]
    return cols, np.array(rows, dtype=np.float32)


def naive_scale(y: np.ndarray, seasonal: bool, m: int = 24) -> float:
    lag = m if seasonal else 1
    return float(np.mean(np.abs(y[lag:] - y[:-lag])))


def lag1_autocorr(y: np.ndarray) -> float:
    return float(np.corrcoef(y[:-1], y[1:])[0, 1])


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    path = str(ROOT / "data/ETT-small/ETTh1.csv")
    seq_ids, X = load_ett(path)
    T, C = X.shape

    train_ratio = 0.8
    split = int(T * train_ratio)
    train, test = X[:split], X[split:]
    print(f"ETTh1: {T} 点 × {C} 列，训练 {split} / 测试 {T - split}")

    ctx, pred = 168, 24
    fc = PatchTSTForecaster(
        sequence_ids=seq_ids, context_length=ctx, prediction_length=pred,
        patch_size=16, patch_stride=8, d_model=64, n_heads=4,
        num_layers=2, epochs=20, batch_size=64,
    )
    fc.fit(train)

    # ---- 滚动预测，收集所有列的逐步误差 ----
    errs = {c: {h: [] for h in range(pred)} for c in range(C)}
    for t in range(split, T, pred):
        if t - ctx < 0:
            continue
        pmap = fc.forecast(X[t - ctx:t], steps=pred)
        for c in range(C):
            p = np.array(pmap[seq_ids[c]], dtype=np.float64)
            for h in range(pred):
                if t + h < T:
                    errs[c][h].append(float(abs(X[t + h, c] - p[h])))

    print(f"\n{'列':<14}{'lag1自相关':>10}{'scale_lag1':>11}{'scale_lag24':>12}"
          f"{'MASE_lag1':>11}{'MASE_lag24':>12}")
    for c in range(C):
        ytr = train[:, c].astype(np.float64)
        s1 = naive_scale(ytr, False)
        s24 = naive_scale(ytr, True)
        mae_c = float(np.mean([np.mean(errs[c][h]) for h in range(pred)]))
        m1 = mae_c / s1
        m24 = mae_c / s24
        print(f"{seq_ids[c]:<14}{lag1_autocorr(ytr):>10.3f}{s1:>11.4f}{s24:>12.4f}"
              f"{m1:>11.3f}{m24:>12.3f}")

    # ---- 逐步 MASE（只看 OT 目标列）----
    ot = seq_ids.index("ETTh1_OT")
    ytr_ot = train[:, ot].astype(np.float64)
    s1, s24 = naive_scale(ytr_ot, False), naive_scale(ytr_ot, True)
    m1s = [float(np.mean(errs[ot][h])) / s1 for h in range(pred)]
    m24s = [float(np.mean(errs[ot][h])) / s24 for h in range(pred)]
    print(f"\n[OT 逐步 MASE / lag-1] 均值 {np.mean(m1s):.4f}")
    print("  " + " ".join(f"h{h+1}={v:.2f}" for h, v in enumerate(m1s)))
    print(f"[OT 逐步 MASE / lag-24] 均值 {np.mean(m24s):.4f}")
    print("  " + " ".join(f"h{h+1}={v:.2f}" for h, v in enumerate(m24s)))


if __name__ == "__main__":
    main()
