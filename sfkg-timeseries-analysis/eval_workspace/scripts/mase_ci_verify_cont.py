#!/usr/bin/env python
"""CI 续跑：种子 3-9 补齐 10 种子（跳过 CD 配对对照，原验证点①②④）。

协议与 mase_ci_verify.py 完全一致：ctx168/pred3/patch8-4/d64/L2/e20、
stride=3、t≥168。正常段=目标窗无异常标签；全段=全部目标窗。
"""
import argparse
import random
import sys
import time
from pathlib import Path

import numpy as np
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

import evaluate_contract_benchmarks as ev  # noqa: E402
from patchtst_forecaster import PatchTSTForecaster  # noqa: E402

PRED, CTX = 3, 168


def naive_scale(y: np.ndarray, m: int) -> float:
    return float(np.mean(np.abs(y[m:] - y[:-m])))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity-index", type=int, default=11)
    ap.add_argument("--seeds", type=int, default=10)
    ap.add_argument("--seed-start", type=int, default=3)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()
    ent = ev.load_entities("SMD")[args.entity_index]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    print(f"== SMD / {ent.name} CI-verify-cont seeds={args.seed_start}.."
          f"{args.seeds - 1} device={args.device}", flush=True)

    ws_all = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm_mask = np.array([bool(labels[t:t + PRED].sum() == 0)
                          for t in ws_all])
    print(f"[windows all={len(ws_all)} norm={int(norm_mask.sum())}]",
          flush=True)

    def run_ci(seed):
        mae_all = np.zeros(C)
        mae_norm = np.zeros(C)
        cnt_norm = np.zeros(C, dtype=np.int64)
        for c in range(C):
            random.seed(seed)
            np.random.seed(seed)
            torch.manual_seed(seed)
            fc = PatchTSTForecaster(sequence_ids=["c0"],
                                    context_length=CTX,
                                    prediction_length=PRED, patch_size=8,
                                    patch_stride=4, d_model=64, n_heads=4,
                                    num_layers=2, epochs=20, batch_size=64,
                                    device=args.device)
            fc.fit(train[:, c:c + 1])
            for i, t in enumerate(ws_all):
                pmap = fc.forecast(test[t - CTX:t, c:c + 1], steps=PRED)
                p = np.array(pmap["c0"], dtype=np.float64)
                e = np.mean(
                    [abs(float(test[t + h, c]) - p[h]) for h in range(PRED)])
                mae_all[c] += e
                if norm_mask[i]:
                    mae_norm[c] += e
                    cnt_norm[c] += 1
            if (c + 1) % 19 == 0:
                print(f"[seed {seed} ch {c+1}/{C}]", flush=True)
        mae_all /= len(ws_all)
        mae_norm /= np.maximum(cnt_norm, 1)
        return mae_all, mae_norm

    def mase(mae, m):
        ms = []
        for c in range(C):
            s = naive_scale(train[:, c].astype(np.float64), m)
            if s > 0:
                ms.append(float(mae[c]) / s)
        return np.array(ms)

    for seed in range(args.seed_start, args.seeds):
        t0 = time.monotonic()
        mae_all, mae_norm = run_ci(seed)
        for m in (24, 1440):
            arr_all, arr_norm = mase(mae_all, m), mase(mae_norm, m)
            print(f"[seed {seed}] m={m:<4} CI 全段={arr_all.mean():.3f} "
                  f"正常段={arr_norm.mean():.3f}", flush=True)
        print(f"[seed {seed} {time.monotonic()-t0:.0f}s]", flush=True)
    print("CI-VERIFY-CONT-DONE", flush=True)


if __name__ == "__main__":
    main()
