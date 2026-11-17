#!/usr/bin/env python
"""工厂代码一致性复验：用 src/dlinear_forecaster.py（工厂实现）重跑 PSM 72 步验收协议。

背景（2026-10-30 用户要求）：评测方法必须与工厂实际方法一致。验收达标数字
（m1440 全段 0.526±0.004 / 正常段 0.489±0.004，10 种子）此前出自评测脚本
`mase_horizon_sweep.py` 的 DLinearForecaster；工厂已装 `src/dlinear_forecaster.py`
（同构移植 + 工厂接口），本脚本用工厂类按原协议逐行复跑，验证数字逐位对齐——
对齐即"工厂实际方法 = 验收方法"的直接证据。

协议（与 mase36_verify.py / mase72 验证完全一致）：
  - PSM（1 分钟采样），CTX=336、stride=PRED=72 不重叠、完整窗（t+71<T）；
  - 每窗误差 = 72 步平均 |实际−预测|（逐通道）；
  - MASE 分母 = 训练段 naive 尺度（Hyndman-Koehler），m∈{1,24,144,1440}；
  - 全段 + 正常段（预测段 [t,t+72) 标签全 0）；
  - 同窗 naive 基线（前值复制 / 季节 naive m=1440）对照；
  - 10 种子。参照值（评测脚本原数）：m1440 全段 0.526±0.004、正常段 0.489±0.004；
    m144 全段 0.596、正常段 0.530（均名义贴线但劣于前值复制）。
"""
import argparse
import sys
import time
from pathlib import Path

import numpy as np
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

from dlinear_forecaster import DLinearForecaster  # noqa: E402  ← 工厂实现
import evaluate_contract_benchmarks as ev  # noqa: E402
from mase_horizon_sweep import naive_scale  # noqa: E402

CTX = 336
MS = (1, 24, 144, 1440)
M_NAT = 1440  # PSM 自然周期（1 分钟 → 日周期）


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", type=int, default=72, choices=[36, 72])
    ap.add_argument("--seeds", type=int, default=10)
    ap.add_argument("--seed-start", type=int, default=0)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()
    PRED = args.pred

    ent = ev.load_entities("PSM")[0]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    ws = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws])
    print(f"== PSM T={T} C={C} "
          f"[windows all={len(ws)} norm={int(norm.sum())}]", flush=True)

    # 同窗 naive 基线（与 factory_cd36.py / mase72_psm_naive.py 同法，全段/正常段分开）
    win = np.stack([test[t:t + PRED] for t in ws])
    last = np.stack([np.broadcast_to(test[t - 1], (PRED, C)) for t in ws])
    full = np.vstack([train, test])
    off = len(train)
    ws_s = [t for t in ws if off + t - M_NAT >= 0]
    idx_s = np.array([ws.index(t) for t in ws_s])
    seas_win = win[idx_s]
    seas = np.stack([full[off + t - M_NAT: off + t - M_NAT + PRED]
                     for t in ws_s])
    norm_s = norm[idx_s]
    base = {("last", None): last, ("last", "norm"): last[norm],
            ("seas", None): seas, ("seas", "norm"): seas[norm_s]}
    target = {("last", None): win, ("last", "norm"): win[norm],
              ("seas", None): seas_win, ("seas", "norm"): seas_win[norm_s]}
    ns_m = [naive_scale(train[:, c].astype(np.float64), M_NAT)
            for c in range(C)]
    for kind, tag in (("last", "前值复制"), ("seas", "季节naive")):
        for sect, s2 in ((None, "全段"), ("norm", "正常段")):
            errs = np.abs(target[(kind, sect)]
                          - base[(kind, sect)]).mean(axis=(0, 1))
            ms_ = [errs[c] / s for c, s in enumerate(ns_m) if s > 0]
            print(f"[{tag} m={M_NAT} {s2}] MASE={np.mean(ms_):.3f}",
                  flush=True)

    per_seed = {tag: {m: [] for m in MS} for tag in ("全段", "正常段")}
    for seed in range(args.seed_start, args.seeds):
        t0 = time.monotonic()
        mae_all = np.zeros(C)
        mae_norm = np.zeros(C)
        cnt = np.zeros(C, dtype=np.int64)
        for c in range(C):
            # 工厂实现：sequence_ids 接口 + 逐通道训练（协议同构）
            fc = DLinearForecaster(
                sequence_ids=["c0"], context_length=CTX,
                prediction_length=PRED, kernel_size=25, epochs=20,
                batch_size=64, learning_rate=1e-3, seed=seed,
                device=args.device)
            fc.fit(train[:, c:c + 1])
            for i, t in enumerate(ws):
                p = np.array(fc.forecast(test[t - CTX:t, c:c + 1],
                                         steps=PRED)["c0"], dtype=np.float64)
                e = np.mean(np.abs(test[t:t + PRED, c] - p))
                mae_all[c] += e
                if norm[i]:
                    mae_norm[c] += e
                    cnt[c] += 1
            if (c + 1) % 5 == 0:
                print(f"[factory-dlinear seed {seed} ch {c + 1}/{C}]",
                      flush=True)
        mae_all /= len(ws)
        mae_norm /= np.maximum(cnt, 1)
        for m in MS:
            ns = [naive_scale(train[:, c].astype(np.float64), m)
                  for c in range(C)]
            for tag, mae in (("全段", mae_all), ("正常段", mae_norm)):
                ms_ = [mae[c] / s for c, s in enumerate(ns) if s > 0]
                v = float(np.mean(ms_))
                per_seed[tag][m].append(v)
                print(f"[factory-dlinear seed={seed} m={m:<4} {tag}] "
                      f"MASE={v:.3f} (min {np.min(ms_):.3f}/max {np.max(ms_):.3f})",
                      flush=True)
        print(f"[factory-dlinear seed={seed} {time.monotonic() - t0:.0f}s]",
              flush=True)

    print("== 10 种子汇总（mean ± std，10/10 ≤0.6 计数）==", flush=True)
    for m in MS:
        for tag in ("全段", "正常段"):
            v = per_seed[tag][m]
            n_ok = sum(1 for x in v if x <= 0.6)
            print(f"[m={m:<4} {tag}] {np.mean(v):.3f} ± {np.std(v):.3f} "
                  f"({n_ok}/{len(v)})", flush=True)
    print("MASE72-FACTORY-DONE", flush=True)


if __name__ == "__main__":
    main()
