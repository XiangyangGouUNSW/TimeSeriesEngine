#!/usr/bin/env python
"""CI 0.574 复验（2-7 彩票教训）：3 种子 × 逐通道模型 + 同硬件 CD 配对对照。

验证点：
① 种子稳健性（3 种子）；
② 全段口径（不只正常段）——诚实报告；
③ 同 GPU（A40#1）CD 多通道模型直接对照，排除平台因素；
④ 逐通道 MASE 分布（p10/p50/p90），排除退化通道拉分。

协议与 mase_ci_patchtst.py 完全一致：ctx168/pred3/patch8-4/d64/L2/e20、
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
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()
    ent = ev.load_entities("SMD")[args.entity_index]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    print(f"== SMD / {ent.name} CI-verify seeds={args.seeds} "
          f"device={args.device}", flush=True)

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

    for seed in range(args.seeds):
        t0 = time.monotonic()
        mae_all, mae_norm = run_ci(seed)
        for m in (24, 1440):
            arr_all, arr_norm = mase(mae_all, m), mase(mae_norm, m)
            print(f"[seed {seed}] m={m:<4} CI 全段={arr_all.mean():.3f} "
                  f"正常段={arr_norm.mean():.3f}", flush=True)
        if seed == 0:
            arr = mase(mae_norm, 24)
            print(f"[seed 0 逐通道 m24 分布] p10={np.percentile(arr,10):.3f} "
                  f"p50={np.percentile(arr,50):.3f} "
                  f"p90={np.percentile(arr,90):.3f} "
                  f"max={arr.max():.3f}", flush=True)
        print(f"[seed {seed} {time.monotonic()-t0:.0f}s]", flush=True)

    # 同硬件 CD 配对对照
    random.seed(0)
    np.random.seed(0)
    torch.manual_seed(0)
    fc = PatchTSTForecaster(sequence_ids=[f"c{i}" for i in range(C)],
                            context_length=CTX, prediction_length=PRED,
                            patch_size=8, patch_stride=4, d_model=64,
                            n_heads=4, num_layers=2, epochs=20,
                            batch_size=64, device=args.device)
    t0 = time.monotonic()
    fc.fit(train)
    print(f"[CD fit {time.monotonic()-t0:.0f}s]", flush=True)
    mae_all = np.zeros(C)
    mae_norm = np.zeros(C)
    cnt_norm = np.zeros(C, dtype=np.int64)
    for i, t in enumerate(ws_all):
        pmap = fc.forecast(test[t - CTX:t], steps=PRED)
        for c in range(C):
            p = np.array(pmap[f"c{c}"], dtype=np.float64)
            e = np.mean(
                [abs(float(test[t + h, c]) - p[h]) for h in range(PRED)])
            mae_all[c] += e
            if norm_mask[i]:
                mae_norm[c] += e
                cnt_norm[c] += 1
    mae_all /= len(ws_all)
    mae_norm /= np.maximum(cnt_norm, 1)
    for m in (24, 1440):
        arr_all, arr_norm = mase(mae_all, m), mase(mae_norm, m)
        print(f"[CD seed0] m={m:<4} CD 全段={arr_all.mean():.3f} "
              f"正常段={arr_norm.mean():.3f}", flush=True)
    print("CI-VERIFY-DONE", flush=True)


if __name__ == "__main__":
    main()
