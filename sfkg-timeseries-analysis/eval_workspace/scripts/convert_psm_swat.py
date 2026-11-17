#!/usr/bin/env python
"""PSM/SWaT → 工厂 AllInOne 布局 {train,test,test_label}.npy，并交叉核对 TSL SMAP。

来源：thuml/Time-Series-Library HF 镜像（经 hf-mirror.com）——原评测"data.py"
约定出处。
  - PSM: train.csv/test.csv（首列 timestamp_(min) + 25 特征）、test_label.csv
  - SWaT: swat_train2.csv（495000 行）/ swat2.csv（449919 行），末列已数值化
    0/1 标签；51 特征
锚点：PSM train=132481/test=87841；SWaT train=495000/test=449919。
交叉核对：TSL SMAP_test.npy 的 D-3 切片应与我方 telemanom 逐实体转换一致
（8640 点、3276 异常点、事件起点 5225）。
"""
import csv
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis" / "eval_workspace"
TSL = ROOT / "data" / "raw" / "tsl"
DST = ROOT / "data"


def load_csv_no_header(path: Path):
    df = pd.read_csv(path)  # 行宽不一致由 pandas 统一补 NaN
    return df.to_numpy(dtype=np.float32)


def main() -> None:
    # ---- PSM ----
    tr = load_csv_no_header(TSL / "PSM" / "train.csv")[:, 1:]     # 去时间戳
    te = load_csv_no_header(TSL / "PSM" / "test.csv")[:, 1:]
    lb = load_csv_no_header(TSL / "PSM" / "test_label.csv")[:, 1].astype(np.int64)
    out = DST / "PSM" / "AllInOne"
    out.mkdir(parents=True, exist_ok=True)
    np.save(out / "train.npy", tr)
    np.save(out / "test.npy", te)
    np.save(out / "test_label.npy", lb)
    print(f"PSM AllInOne: train={tr.shape} test={te.shape} "
          f"异常点={int(lb.sum())} ({lb.mean():.2%})", flush=True)
    assert tr.shape == (132481, 25) and te.shape == (87841, 25), "PSM 尺寸不符"

    # ---- SWaT ----
    tr = load_csv_no_header(TSL / "SWaT" / "swat_train2.csv")[:, :-1]
    te_all = load_csv_no_header(TSL / "SWaT" / "swat2.csv")
    te, lb = te_all[:, :-1], te_all[:, -1].astype(np.int64)
    out = DST / "SWaT" / "AllInOne"
    out.mkdir(parents=True, exist_ok=True)
    np.save(out / "train.npy", tr)
    np.save(out / "test.npy", te)
    np.save(out / "test_label.npy", lb)
    print(f"SWaT AllInOne: train={tr.shape} test={te.shape} "
          f"异常点={int(lb.sum())} ({lb.mean():.2%})", flush=True)
    assert tr.shape == (495000, 51) and te.shape == (449919, 51), "SWaT 尺寸不符"

    # ---- TSL SMAP 与我方逐实体转换交叉核对（D-3）----
    te_tsl = np.load(TSL / "SMAP" / "SMAP_test.npy")
    lb_tsl = np.load(TSL / "SMAP" / "SMAP_test_label.npy").reshape(-1)
    # 我方实体按名排序的拼接起始偏移
    ents = sorted(p.name for p in (DST / "SMAP").iterdir())
    lengths = [len(np.load(DST / "SMAP" / n / "test.npy")) for n in ents]
    i = ents.index("D-3")
    off = int(sum(lengths[:i]))
    seg_tsl = lb_tsl[off:off + 8640]
    seg_mine = np.load(DST / "SMAP" / "D-3" / "test_label.npy")
    # TSL 与我方可能有整体窗口偏移：滑动 -3..+3 找最大匹配
    best = max(
        ((seg_tsl == np.roll(seg_mine, s)).mean(), s) for s in range(-3, 4))
    print(f"[交叉核对] D-3 标签序列与 TSL 对齐：最大匹配率={best[0]:.4f} "
          f"偏移={best[1]}（-3..+3 内）", flush=True)
    assert best[0] > 0.99, "D-3 交叉核对失败"
    print("PSM-SWAT-CONVERT-DONE", flush=True)


if __name__ == "__main__":
    main()
