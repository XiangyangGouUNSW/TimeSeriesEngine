#!/usr/bin/env python
"""1 步任务 naive 基线对照（纯 numpy，不需模型）：同一测试窗口集上，
前值复制与季节 naive 的 MASE——检验 1 步口径的 ≤0.6 是模型贡献还是
协议结构（师兄警告：MASE<1 不等于在测试窗上真比 naive 好）。

数据集与窗口集与 mase_pred1_battery.py 一致：80/20 切分、t≥split+168、
逐步评估、逐通道平均。solar 模型实测 lag1=0.649/lag24=0.051/lag144=0.160。
"""
import csv
import gzip
from pathlib import Path

import numpy as np

DATA = Path.home() / "gcad" / "data" / "public"
ETT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis" / "data"


def load_txt_gz(path: Path, n_cols: int):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        rows = []
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 2:
                rows.append([float(v) for v in parts[1:1 + n_cols]])
    return np.array(rows, dtype=np.float32)


def load_csv(path: Path, prefix: str):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        rows = [[float(v) for v in row[1:]] for row in reader
                if row and row[0]]
    return np.array(rows, dtype=np.float32)


def naive_scale(y: np.ndarray, m: int) -> float:
    return float(np.mean(np.abs(y[m:] - y[:-m])))


def baselines(name: str, X: np.ndarray, natural_m: int) -> None:
    T, C = X.shape
    split = int(T * 0.8)
    train, test = X[:split], X[split:]
    # 逐步测试窗口：t 从 split 起（与电池脚本一致，前 168 步跳过不影响均值量级）
    pers = np.abs(np.diff(test, axis=0))[168:].mean(axis=0)      # |x_t - x_{t-1}|
    s_nat = np.abs(test[natural_m:] - test[:-natural_m])[168:].mean(axis=0)
    s24 = np.abs(test[24:] - test[:-24])[168:].mean(axis=0)
    print(f"== {name}: T={T} C={C} natural_m={natural_m}", flush=True)

    def mase_avg(errs, m):
        ms = []
        for c in range(C):
            s = naive_scale(train[:, c].astype(np.float64), m)
            if s > 0:
                ms.append(float(errs[c]) / s)
        return float(np.mean(ms))

    print(f"{'baseline':<20}{'m=1':>9}{'m=24':>9}{f'm={natural_m}':>9}",
          flush=True)
    print(f"{'last-value':<20}{mase_avg(pers,1):>9.3f}"
          f"{mase_avg(pers,24):>9.3f}{mase_avg(pers,natural_m):>9.3f}",
          flush=True)
    print(f"{'seasonal naive':<20}{'-':>9}"
          f"{mase_avg(s24,24):>9.3f}{mase_avg(s_nat,natural_m):>9.3f}",
          flush=True)


def main() -> None:
    baselines("solar", load_txt_gz(DATA / "solar_AL.txt.gz", 137), 144)
    baselines("electricity", load_txt_gz(DATA / "electricity.txt.gz", 321), 24)
    baselines("traffic", load_txt_gz(DATA / "traffic.txt.gz", 862), 24)
    baselines("ETTh1", load_csv(ETT / "ETTh1.csv", "ETTh1"), 24)
    print("PRED1-NAIVE-DONE")


if __name__ == "__main__":
    main()
