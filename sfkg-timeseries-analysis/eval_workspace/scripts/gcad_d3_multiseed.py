#!/usr/bin/env python
"""D-3 多种子工厂复验：量化官方方法自然波动带。

官方 set_seed 忽略传入参数、以 random.randint(0,1000) 随机种子（utils/general.py），
即官方每次运行都是随机轨迹；本脚本以同样语义随机 N 个种子，用工厂代码
（构造默认值 = 官方 run.sh SMAP 配置）复跑 D-3 验收协议，统计 8 档阶梯的
合同通过率与噪声带（|ΔAUROC|≤0.01、各档 |ΔP|,|ΔR|≤0.02 vs 官方 10-28 记录）。

对照锚点（ladder.json，seed=0 实为随机轨迹）：AUROC 0.9486；
q95 0.853/0.977，q98 0.875/0.977，q99 0.892/0.976，q995 0.896/0.976，
q999 0.900/0.974，q9999 0.903/0.974，q99999 0.904/0.974，max 0.904/0.974。
合同：R≥90% 且 P≥85%（点级）。
"""
import json
import random
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
sys.path.insert(0, str(ROOT / "src"))

from anomaly_models import GcadAnomalyModel, _sma3  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402

SEQ = 70
TIERS = ["q0.95", "q0.98", "q0.99", "q0.995", "q0.999", "q0.9999",
         "q0.99999", "max(val)"]
QS = [0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 0.99999, None]
RECORDED_AUC = 0.9486
RECORDED = {t: (p, r) for t, p, r in [
    ("q0.95", 0.853, 0.977), ("q0.98", 0.875, 0.977),
    ("q0.99", 0.892, 0.976), ("q0.995", 0.896, 0.976),
    ("q0.999", 0.900, 0.974), ("q0.9999", 0.903, 0.974),
    ("q0.99999", 0.904, 0.974), ("max(val)", 0.904, 0.974)]}
CONTRACT_P, CONTRACT_R = 0.85, 0.90
N_RUNS = 5

train = np.load(WS / "data" / "SMAP" / "D-3" / "train.npy")
test = np.load(WS / "data" / "SMAP" / "D-3" / "test.npy")
lb = np.load(WS / "data" / "SMAP" / "D-3" / "test_label.npy").astype(np.int64)


def point_scores(window_scores: np.ndarray, t: int) -> np.ndarray:
    pts = np.empty(t, dtype=np.float64)
    n = len(window_scores)
    for i in range(n):
        pts[i + SEQ] = window_scores[i]
    pts[:SEQ] = window_scores[0]
    if n + SEQ < t:
        pts[n + SEQ:] = window_scores[-1]
    return pts


runs = []
for run in range(N_RUNS):
    seed = random.randint(0, 1000)     # 官方 set_seed 语义
    m = GcadAnomalyModel(seed=seed)
    m._chunk = 128
    t0 = time.monotonic()
    m.fit(train)
    raw = m.score_segment(test)
    v_s = _sma3(m._val_S)
    t_s = _sma3(raw)
    pts = point_scores(t_s, len(test))
    auc = float(roc_auc_score(lb, pts))
    tiers = {}
    all_ok = True
    in_band = abs(auc - RECORDED_AUC) <= 0.01
    for tier, q in zip(TIERS, QS):
        th = float(np.max(v_s)) if q is None else float(np.quantile(v_s, q))
        pred = (pts >= th).astype(np.int64)
        tp = int(((pred == 1) & (lb == 1)).sum())
        fp = int(((pred == 1) & (lb == 0)).sum())
        fn = int(((pred == 0) & (lb == 1)).sum())
        p = tp / max(tp + fp, 1)
        r = tp / max(tp + fn, 1)
        rp, rr = RECORDED[tier]
        ok = p >= CONTRACT_P and r >= CONTRACT_R
        all_ok &= ok
        in_band &= abs(p - rp) <= 0.02 and abs(r - rr) <= 0.02
        tiers[tier] = dict(th=th, p=round(p, 4), r=round(r, 4), contract=bool(ok))
    runs.append(dict(seed=seed, epochs=m._epochs_run, auc=round(auc, 4),
                     contract_all=bool(all_ok), in_noise_band=bool(in_band),
                     tiers=tiers))
    print(f"seed={seed:4d} 轮数={m._epochs_run:2d} AUROC={auc:.4f} "
          f"8档全过={'✓' if all_ok else '✗'} 噪声带内={'✓' if in_band else '✗'}  "
          f"q95 P={tiers['q0.95']['p']:.3f} q99 P={tiers['q0.99']['p']:.3f} "
          f"max P={tiers['max(val)']['p']:.3f}", flush=True)

aucs = [r["auc"] for r in runs]
n_ok = sum(r["contract_all"] for r in runs)
n_band = sum(r["in_noise_band"] for r in runs)
print(f"\nAUROC min={min(aucs):.4f} max={max(aucs):.4f}（记录 {RECORDED_AUC}）")
for tier in TIERS:
    ps = [r["tiers"][tier]["p"] for r in runs]
    rs = [r["tiers"][tier]["r"] for r in runs]
    nk = sum(r["tiers"][tier]["contract"] for r in runs)
    print(f"{tier:<9} P {min(ps):.3f}..{max(ps):.3f}（记录 "
          f"{RECORDED[tier][0]:.3f}）  R {min(rs):.3f}..{max(rs):.3f}  "
          f"合同 {nk}/{N_RUNS}")
print(f"\n8 档全过 {n_ok}/{N_RUNS}；噪声带内 {n_band}/{N_RUNS}")
out = WS / "results" / "gcad_d3_multiseed"
out.mkdir(parents=True, exist_ok=True)
with open(out / "report.json", "w") as f:
    json.dump(dict(n_runs=N_RUNS, recorded_auc=RECORDED_AUC, runs=runs), f,
              indent=2)
print(f"落盘：{out / 'report.json'}")
print("GCAD-D3-MULTISEED-DONE", flush=True)
