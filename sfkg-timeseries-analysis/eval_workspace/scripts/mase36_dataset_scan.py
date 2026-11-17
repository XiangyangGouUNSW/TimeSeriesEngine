#!/usr/bin/env python
"""36 步跨数据集 MASE 扫描（DLinear 探针 + naive 基线）：找 36 步口径下最友好的
数据集/实体（导师新要求：步长最小 36、希望 72；用户决定先聚焦 36 步）。

差距测量结论（SMD 2-4，36 步，m24 正常段）：PatchTST 1.251、DLinear 0.960——
线性族方向对但 SMD 2-4 上过不了 0.6 线。本脚本用 DLinear（当前最佳+最快）扫
全部候选数据集，定位"哪个数据集 36 步能过线"，再对好实体上更强方法。

口径（与 mase_horizon_sweep.py 一致）：
  - CTX=336 输入、stride=PRED=36 不重叠、每窗口误差=36 步平均 |实际-预测|；
  - MASE 分母=训练段 naive 尺度 m∈{1,24,144,1440}（Hyndman-Koehler）；
  - 全段 + 正常段（预测段 [t,t+36) 标签全 0；无标签数据集全段=正常段）。
对照基线（同窗）：last-value（m=1 尺度）、季节 naive（m=natural 尺度）——
模型必须既 ≤0.6 又明显好于 naive 才算真达标（solar 1 步教训）。

数据集顺序（按 36 步友好度先验）：solar(10min,36步=6h) → ETTh1(1h,36h) →
electricity(1h) → traffic(1h) → PSM(1min) → SMD×28(1min) → SMAP×54(1min) →
MSL×28(1min)。SWaT（1 秒采样）第一轮跳过：36 步=36 秒，m=24 分母在秒级数据上
趋近 0 → MASE 结构性爆炸，不是有希望的候选；且体量最大（51 通道×49.5 万点）。
"""
import argparse
import csv
import gzip
import sys
import time
from pathlib import Path

import numpy as np
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
sys.path.insert(0, str(WS / "scripts"))
sys.path.insert(0, str(ROOT / "tools"))

from mase_horizon_sweep import DLinearForecaster, naive_scale  # noqa: E402
import evaluate_contract_benchmarks as ev  # noqa: E402

PRED, CTX, SEED = 36, 336, 0
MS = (1, 24, 144, 1440)
NATURAL = {"solar": 144, "ETTh1": 24, "electricity": 24, "traffic": 24,
           "PSM": 1440, "SMD": 1440, "SMAP": 1440, "MSL": 1440}


def load_chan_entities(ds: str) -> list[ev.Entity]:
    """SMAP/MSL 逐通道目录实体。"""
    ents = []
    for d in sorted((WS / "data" / ds).iterdir()):
        if d.is_dir() and (d / "train.npy").exists():
            ents.append(ev.Entity(
                name=d.name,
                train=np.load(d / "train.npy").astype(np.float32),
                test=np.load(d / "test.npy").astype(np.float32),
                labels=np.load(d / "test_label.npy").reshape(-1).astype(
                    np.int64),
            ))
    return ents


def load_public(name: str, n_cols: int) -> ev.Entity:
    """TSL 原始 gz（首列日期跳过，80/20 切分，无标签）。solar 文件名带 _AL。"""
    rows = []
    fname = "solar_AL.txt.gz" if name == "solar" else f"{name}.txt.gz"
    with gzip.open(WS / "data" / "public" / fname, "rt") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 2:
                vals = [float(v) for v in parts[1:1 + n_cols]]
                if len(vals) < n_cols:  # 参差行补零（防 np.array 报错）
                    vals += [0.0] * (n_cols - len(vals))
                rows.append(vals)
    X = np.array(rows, dtype=np.float32)
    split = int(len(X) * 0.8)
    return ev.Entity(name, X[:split], X[split:],
                     np.zeros(len(X) - split, dtype=np.int64))


def load_etth1() -> ev.Entity:
    rows = []
    with open(ROOT / "data" / "ETTh1.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        rows = [[float(v) for v in row[1:]] for row in reader
                if row and row[0]]
    X = np.array(rows, dtype=np.float32)
    split = int(len(X) * 0.8)
    return ev.Entity("ETTh1", X[:split], X[split:],
                     np.zeros(len(X) - split, dtype=np.int64))


def scan_entity(ds: str, ent: ev.Entity, device: str, writer, rows_seen: int,
                row_limit: int) -> int:
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    ws = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    if not ws:
        print(f"[{ds}/{ent.name}] 窗口不足，跳过", flush=True)
        return rows_seen
    norm = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws])
    mae_all = np.zeros(C)
    mae_norm = np.zeros(C)
    cnt = np.zeros(C, dtype=np.int64)
    for c in range(C):
        torch.manual_seed(SEED)
        np.random.seed(SEED)
        fc = DLinearForecaster(CTX, PRED, seed=SEED, device=device).fit(
            train[:, c:c + 1])
        for i, t in enumerate(ws):
            p = fc.forecast(test[t - CTX:t, c:c + 1], PRED)
            e = np.mean(np.abs(test[t:t + PRED, c] - p))
            mae_all[c] += e
            if norm[i]:
                mae_norm[c] += e
                cnt[c] += 1
    mae_all /= len(ws)
    mae_norm /= np.maximum(cnt, 1)

    # 同窗 naive 基线：W 窗 × 36 步 × C 通道
    ts = np.array(ws)
    win = np.stack([test[t:t + PRED] for t in ws])          # (W, 36, C)
    # 前值复制 = 把 t-1 观测值复制到未来 36 步全部（不是连续步差）
    last = np.stack([np.broadcast_to(test[t - 1], (PRED, C))
                     for t in ws])
    m_nat = NATURAL[ds]
    # 季节 naive：|x[t+h] - x[t+h-m]|。窗口 t<m 时滞后点落到训练段——
    # 用 train+test 拼接坐标取。训练段太短（SMAP 个别通道仅 682-705 点）
    # 时滞后够不到的窗口从季节基线剔除（模型 MASE 不受影响，仍用全窗）。
    full = np.vstack([train, test])
    off = len(train)
    ws_s = [t for t in ws if off + t - m_nat >= 0]
    seas = None
    if ws_s:
        win_s = np.stack([test[t:t + PRED] for t in ws_s])
        seas = np.stack(
            [full[off + t - m_nat: off + t - m_nat + PRED] for t in ws_s])
        seas_win = win_s
    else:
        seas_win = None

    def mase_vec(errs, m):
        ms = []
        for c in range(C):
            s = naive_scale(train[:, c].astype(np.float64), m)
            if s > 0:
                ms.append(float(errs[c]) / s)
        return float(np.mean(ms)) if ms else float("nan")

    def row(kind, errs_all, errs_norm):
        nonlocal rows_seen
        for m in MS:
            writer.writerow(dict(ds=ds, entity=ent.name, m=m, kind=kind,
                                 mase_all=round(mase_vec(errs_all, m), 4),
                                 mase_norm=round(mase_vec(errs_norm, m), 4)))
        rows_seen += len(MS)

    row("dlinear", mae_all, mae_norm)
    err_last = np.abs(win - last).mean(axis=(0, 1))
    row("last-value", err_last, err_last)
    if seas is not None:
        err_seas = np.abs(seas_win - seas).mean(axis=(0, 1))
        row("seasonal-naive", err_seas, err_seas)

    for m in (24, 1440):
        v = mase_vec(mae_norm, m)
        print(f"[{ds}/{ent.name}] DLinear m={m:<4} 正常段={v:.3f} "
              f"({rows_seen}/{row_limit})", flush=True)
    return rows_seen


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--start-idx", type=int, default=0,
                    help="从任务表的第 N 个实体续跑（0=solar,1=ETTh1,"
                         "2=electricity,3=traffic,4=PSM,5..32=SMD,"
                         "33..86=SMAP,87..114=MSL）")
    args = ap.parse_args()
    device = torch.device(args.device)

    out = WS / "results" / "horizon36_scan_2026-10-28.csv"
    new_file = not out.exists()
    f = open(out, "a", newline="")
    writer = csv.DictWriter(f, fieldnames=["ds", "entity", "m", "kind",
                                           "mase_all", "mase_norm"])
    if new_file:
        writer.writeheader()
    rows_seen = 0
    row_limit = len(MS) * 3  # 每实体 3 种 kind

    jobs = [(ds, n_cols, None)
            for ds, n_cols in (("solar", 137), ("ETTh1", 0),
                               ("electricity", 321), ("traffic", 862))]
    for ds in ("PSM", "SMD", "SMAP", "MSL"):
        ents = (ev.load_entities(ds) if ds in ("PSM", "SMD")
                else load_chan_entities(ds))
        for ent in ents:
            jobs.append((ds, 0, ent))

    for ji, (ds, n_cols, ent) in enumerate(jobs):
        if ji < args.start_idx:
            continue
        if ent is None:
            ent = (load_etth1() if ds == "ETTh1"
                   else load_public(ds, n_cols))
        t0 = time.monotonic()
        rows_seen = scan_entity(ds, ent, device, writer, rows_seen, row_limit)
        f.flush()
        print(f"[{ji}/{len(jobs)} {ds}/{ent.name} "
              f"{time.monotonic() - t0:.0f}s]", flush=True)
    f.close()
    print("SCAN36-DONE", flush=True)


if __name__ == "__main__":
    main()
