#!/usr/bin/env python
"""36 步验证：扫描发现的达标实体上，更强方法交叉验证 + 多种子。

背景：36 步跨数据集扫描（DLinear 探针）发现 PSM 真达标（m1440 正常段 0.433，
优于前值复制 0.478 / 季节 naive 1.008）。本脚本对指定数据集上 PatchTST
（一次性直出 36 步，prediction_length=36）多种子复测，确认不是种子运气。

口径（与扫描/mase_horizon_sweep 一致）：
  - CTX=336、stride=36、每窗误差=36 步平均；MASE 分母=训练段 naive 尺度
    m∈{1,24,144,1440}；全段+正常段。naive 基线同扫描（含 train 段滞后）。
"""
import argparse
import gzip
import random
import sys
import time
from pathlib import Path

import numpy as np
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
sys.path.insert(0, str(WS / "scripts"))
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

from mase36_dataset_scan import load_public  # noqa: E402
from mase_horizon_sweep import DLinearForecaster, naive_scale  # noqa: E402
from patchtst_forecaster import PatchTSTForecaster  # noqa: E402
import evaluate_contract_benchmarks as ev  # noqa: E402

CTX = 336
MS = (1, 24, 144, 1440)


def load_entity(ds: str) -> ev.Entity:
    if ds == "PSM":
        return ev.load_entities("PSM")[0]
    if ds == "solar":
        return load_public("solar", 137)
    raise SystemExit(f"未支持的数据集: {ds}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="PSM")
    ap.add_argument("--methods", default="patchtst")
    ap.add_argument("--pred", type=int, default=36)
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--seed-start", type=int, default=0)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()
    PRED = args.pred

    ent = load_entity(args.dataset)
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    ws = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws])
    print(f"== {args.dataset} T={T} C={C} "
          f"[windows all={len(ws)} norm={int(norm.sum())}]", flush=True)

    for mname in args.methods.split(","):
        for seed in range(args.seed_start, args.seeds):
            t0 = time.monotonic()
            mae_all = np.zeros(C)
            mae_norm = np.zeros(C)
            cnt = np.zeros(C, dtype=np.int64)
            for c in range(C):
                random.seed(seed)
                np.random.seed(seed)
                torch.manual_seed(seed)
                if mname == "patchtst":
                    fc = PatchTSTForecaster(
                        sequence_ids=["c0"], context_length=CTX,
                        prediction_length=PRED, patch_size=8, patch_stride=4,
                        d_model=64, n_heads=4, num_layers=2, epochs=20,
                        batch_size=64, device=args.device)
                    fc.fit(train[:, c:c + 1])

                    def predict(fc, x):
                        return np.array(fc.forecast(x, steps=PRED)["c0"],
                                        dtype=np.float64)
                else:
                    fc = DLinearForecaster(
                        CTX, PRED, seed=seed, device=args.device).fit(
                        train[:, c:c + 1])

                    def predict(fc, x):
                        return fc.forecast(x, steps=PRED)
                for i, t in enumerate(ws):
                    p = predict(fc, test[t - CTX:t, c:c + 1])
                    e = np.mean(np.abs(test[t:t + PRED, c] - p))
                    mae_all[c] += e
                    if norm[i]:
                        mae_norm[c] += e
                        cnt[c] += 1
                if (c + 1) % 10 == 0:
                    print(f"[{mname} seed {seed} ch {c + 1}/{C}]", flush=True)
            mae_all /= len(ws)
            mae_norm /= np.maximum(cnt, 1)
            for m in MS:
                ns = [naive_scale(train[:, c].astype(np.float64), m)
                      for c in range(C)]
                for tag, mae in (("全段", mae_all), ("正常段", mae_norm)):
                    ms = [mae[c] / s for c, s in enumerate(ns) if s > 0]
                    print(f"[{mname} seed={seed} m={m:<4} {tag}] "
                          f"MASE={np.mean(ms):.3f} "
                          f"(min {np.min(ms):.3f}/max {np.max(ms):.3f})",
                          flush=True)
            print(f"[{mname} seed={seed} {time.monotonic() - t0:.0f}s]",
                  flush=True)
    print("MASE36-VERIFY-DONE", flush=True)


if __name__ == "__main__":
    main()
