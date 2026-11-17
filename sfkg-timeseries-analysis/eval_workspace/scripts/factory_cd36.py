#!/usr/bin/env python
"""工厂 CD（多通道 PatchTST，工厂方法原配置）@ SMD 的 36 步重测。

导师新要求步长最小 36 → 工厂现配置 prediction_length=24 的口径作废。
本脚本把工厂 CD 评测换到 PRED=36（CTX=336，与 36 步协议一致），多通道
联合建模（工厂实现形态，区别于 CI 逐通道），多种子，全档 m，附同窗
naive 基线（前值复制/季节 naive，全段/正常段分开）——诚实对照。
预期不达标（工厂遥测无日周期），但需要实测数字留档。

口径：CTX=336、stride=36、完整窗（t+35<T）、每窗误差=36 步平均
|实际−预测|；MASE 分母=训练段 naive 尺度 m∈{1,24,144,1440}
（Hyndman-Koehler）；全段 + 正常段（预测段 [t,t+36) 标签全 0）。
"""
import argparse
import random
import sys
import time
from pathlib import Path

import numpy as np
import torch

PROJ = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
sys.path.insert(0, str(PROJ / "tools"))
sys.path.insert(0, str(PROJ / "src"))
sys.path.insert(0, str(PROJ / "generated"))
sys.path.insert(0, str(PROJ / "eval_workspace" / "scripts"))

import evaluate_contract_benchmarks as ev  # noqa: E402
from patchtst_forecaster import PatchTSTForecaster  # noqa: E402
from mase_horizon_sweep import naive_scale  # noqa: E402

PRED, CTX = 36, 336
MS = (1, 24, 144, 1440)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity-idx", type=int, default=11,
                    help="SMD 索引（11=machine-2-4，工厂 CD 历史评测机）")
    ap.add_argument("--seed-start", type=int, default=0)
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()

    ent = ev.load_entities("SMD")[args.entity_idx]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    ws = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws])
    print(f"== 工厂CD {ent.name} T={T} C={C} "
          f"[windows all={len(ws)} norm={int(norm.sum())}]", flush=True)

    # 同窗 naive 基线（一次，与 mase72_psm_naive.py 同法，全段/正常段分开）
    win = np.stack([test[t:t + PRED] for t in ws])
    last = np.stack([np.broadcast_to(test[t - 1], (PRED, C)) for t in ws])
    full = np.vstack([train, test])
    off = len(train)
    ws_s = [t for t in ws if off + t - 1440 >= 0]
    idx_s = np.array([ws.index(t) for t in ws_s])  # 窗口序号（非时间值）
    seas_win = win[idx_s]
    seas = np.stack([full[off + t - 1440: off + t - 1440 + PRED]
                     for t in ws_s])
    norm_s = norm[idx_s]
    base = {("last", None): last, ("last", "norm"): last[norm],
            ("seas", None): seas, ("seas", "norm"): seas[norm_s]}
    target = {("last", None): win, ("last", "norm"): win[norm],
              ("seas", None): seas_win, ("seas", "norm"): seas_win[norm_s]}
    for m in MS:
        ns = [naive_scale(train[:, c].astype(np.float64), m) for c in range(C)]
        for kind, tag in (("last", "前值复制"), ("seas", "季节naive")):
            for sect, s2 in ((None, "全段"), ("norm", "正常段")):
                errs = np.abs(target[(kind, sect)]
                              - base[(kind, sect)]).mean(axis=(0, 1))
                ms_ = [errs[c] / s for c, s in enumerate(ns) if s > 0]
                print(f"[{tag} m={m:<4} {s2}] MASE={np.mean(ms_):.3f}",
                      flush=True)

    for seed in range(args.seed_start, args.seeds):
        t0 = time.monotonic()
        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        seq_ids = [f"c{i}" for i in range(C)]
        fc = PatchTSTForecaster(
            sequence_ids=seq_ids, context_length=CTX, prediction_length=PRED,
            patch_size=16, patch_stride=8, d_model=64, n_heads=4,
            num_layers=2, epochs=20, batch_size=64, device=args.device)
        fc.fit(train)
        fit_s = time.monotonic() - t0
        mae_all = np.zeros(C)
        mae_norm = np.zeros(C)
        cnt = np.zeros(C, dtype=np.int64)
        t1 = time.monotonic()
        for i, t in enumerate(ws):
            pmap = fc.forecast(test[t - CTX:t], steps=PRED)
            for c in range(C):
                p = np.array(pmap[seq_ids[c]], dtype=np.float64)
                e = np.mean(np.abs(test[t:t + PRED, c] - p))
                mae_all[c] += e
                if norm[i]:
                    mae_norm[c] += e
                    cnt[c] += 1
        mae_all /= len(ws)
        mae_norm /= np.maximum(cnt, 1)
        for m in MS:
            ns = [naive_scale(train[:, c].astype(np.float64), m)
                  for c in range(C)]
            for tag, mae in (("全段", mae_all), ("正常段", mae_norm)):
                ms_ = [mae[c] / s for c, s in enumerate(ns) if s > 0]
                print(f"[CD seed={seed} m={m:<4} {tag}] "
                      f"MASE={np.mean(ms_):.3f}", flush=True)
        print(f"[CD seed={seed} fit={fit_s:.0f}s "
              f"infer={time.monotonic() - t1:.0f}s]", flush=True)
    print("FACTORY-CD36-DONE", flush=True)


if __name__ == "__main__":
    main()
