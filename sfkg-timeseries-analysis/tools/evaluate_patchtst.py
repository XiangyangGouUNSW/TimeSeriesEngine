"""PatchTST 预测评估：ETTh1 前 80% 训练、后 20% 预测，报 MSE/MAE。

用法（sfkg 环境）：
  python tools/evaluate_patchtst.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"),):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from patchtst_forecaster import PatchTSTForecaster


def load_ett(csv_path: str, prefix: str = "ETTh1"):
    """读 ETT CSV，返回 (列名列表, [T,C] 数值矩阵)。"""
    import csv
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        cols = [prefix + "_" + c for c in header[1:]]
        rows = [[float(v) for v in row[1:]] for row in reader if row and row[0]]
    return cols, np.array(rows, dtype=np.float32)


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)
    path = str(ROOT / "data/ETT-small/ETTh1.csv")
    seq_ids, X = load_ett(path)
    T, C = X.shape
    print(f"ETTh1: {T} 点 × {C} 列 {seq_ids}")

    train_ratio = 0.8
    split = int(T * train_ratio)
    train, test = X[:split], X[split:]
    print(f"训练 {len(train)} 点，测试 {len(test)} 点")

    ctx, pred = 96, 24
    # 目标列：OT（最后一列）
    target_idx = seq_ids.index("ETTh1_OT")

    # ---- PatchTST ----
    fc = PatchTSTForecaster(
        sequence_ids=seq_ids, context_length=ctx, prediction_length=pred,
        patch_size=16, patch_stride=8, d_model=64, n_heads=4,
        num_layers=2, epochs=20, batch_size=64,
    )
    fc.fit(train)
    # 测试：从测试段开头切一个 context 窗口，预测后续
    window = test[:ctx]
    pred_map = fc.forecast(window, steps=pred)
    p = np.array(pred_map[seq_ids[target_idx]])
    y = test[ctx:ctx + pred, target_idx]
    p_mse = float(np.mean((p - y) ** 2))
    p_mae = float(np.mean(np.abs(p - y)))
    print(f"[PatchTST] OT 预测 MSE={p_mse:.4f} MAE={p_mae:.4f}")

    # 前 5 步明细
    print("前 5 步：真实 / PatchTST")
    for i in range(5):
        print(f"  step{i+1}: {y[i]:8.3f} / {p[i]:8.3f}")


if __name__ == "__main__":
    main()
