#!/usr/bin/env python
"""PSM naive 基线对照（补扫描盲区：扫描 CSV 里 naive 的"正常段"列实为全段值，
且只算过 36 步）：前值复制 + 季节 naive，同协议
（CTX=336、stride=PRED、MASE 分母 m∈{144,1440}、全段+正常段 分开算）。
"""
import argparse
import sys
from pathlib import Path

import numpy as np

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
sys.path.insert(0, str(WS / "scripts"))
sys.path.insert(0, str(ROOT / "tools"))

from mase_horizon_sweep import naive_scale  # noqa: E402
import evaluate_contract_benchmarks as ev  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--pred", type=int, default=72)
args = ap.parse_args()
PRED, CTX = args.pred, 336

ent = ev.load_entities("PSM")[0]
train, test, labels = ent.train, ent.test, ent.labels
T, C = test.shape
ws = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
norm = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws])
print(f"PSM T={T} C={C} windows={len(ws)} norm={int(norm.sum())}", flush=True)

win = np.stack([test[t:t + PRED] for t in ws])                       # (W,72,C)
last = np.stack([np.broadcast_to(test[t - 1], (PRED, C)) for t in ws])
full = np.vstack([train, test])
off = len(train)
seas = np.stack([full[off + t - 1440: off + t - 1440 + PRED]
                 for t in ws])

for m in (144, 1440):
    ns = [naive_scale(train[:, c].astype(np.float64), m) for c in range(C)]
    for tag, w, base in (("全段", win, None), ("正常段", win[norm], norm)):
        b = last if base is None else last[norm]
        errs = np.abs(w - b).mean(axis=(0, 1))
        ms = [errs[c] / s for c, s in enumerate(ns) if s > 0]
        print(f"[last-value m={m} {tag}] MASE={np.mean(ms):.3f}", flush=True)
    for tag, w, base in (("全段", win, None), ("正常段", win[norm], norm)):
        b = seas if base is None else seas[norm]
        errs = np.abs(w - b).mean(axis=(0, 1))
        ms = [errs[c] / s for c, s in enumerate(ns) if s > 0]
        print(f"[seasonal-naive m={m} {tag}] MASE={np.mean(ms):.3f}",
              flush=True)
print("MASE72-NAIVE-DONE", flush=True)
