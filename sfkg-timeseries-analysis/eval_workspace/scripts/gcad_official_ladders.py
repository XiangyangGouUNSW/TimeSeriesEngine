#!/usr/bin/env python
"""官方轨迹 ladder 重算：从已存官方 checkpoint 确定性重算 8 档阶梯并落盘。

打分链无随机性（Ā_norm 已固化在 causal_parms.csv，forward/backward 确定），
故每个 checkpoint 的阶梯可逐位重算。用工厂打分链灌官方权重（已验证与官方
autograd 逐位同构：官方权重→工厂打分 AUROC=0.9486 = 官方记录）。

组语义：
  - 同 seed 组：smap_d3_1028_backup + smap_d3_rerun2..6（官方 --seed 0；官方
    set_seed 忽略参数、random.randint 随机 → Ā_norm 批级采样仍随机 → 轨迹波动）；
  - 不同训练种子组：smap_d3_s1..s4（--seed 1..4 → 洗牌/初始化不同）。
自检：1028_backup 重算应与 10-28 记录逐位一致（AUROC 0.9486 等）。
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import torch
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
REPO = WS / "gcad_repro"
sys.path.insert(0, str(ROOT / "src"))

from anomaly_models import GcadAnomalyModel, _TSMixerRevIN, _sma3  # noqa: E402

SEQ = 70
TIERS = ["q0.95", "q0.98", "q0.99", "q0.995", "q0.999", "q0.9999",
         "q0.99999", "max(val)"]
QS = [0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 0.99999, None]
GROUPS = {
    "same_seed": ["smap_d3_1028_backup", "smap_d3_rerun2", "smap_d3_rerun3",
                  "smap_d3_rerun4", "smap_d3_rerun5", "smap_d3_rerun6"],
    "diff_seed": ["smap_d3_s1", "smap_d3_s2", "smap_d3_s3", "smap_d3_s4"],
}

train = np.load(WS / "data" / "SMAP" / "D-3" / "train.npy")
test = np.load(WS / "data" / "SMAP" / "D-3" / "test.npy")
lb = np.load(WS / "data" / "SMAP" / "D-3" / "test_label.npy").astype(np.int64)
t_test = len(test)


def point_scores(window_scores: np.ndarray, t: int) -> np.ndarray:
    pts = np.empty(t, dtype=np.float64)
    n = len(window_scores)
    for i in range(n):
        pts[i + SEQ] = window_scores[i]
    pts[:SEQ] = window_scores[0]
    if n + SEQ < t:
        pts[n + SEQ:] = window_scores[-1]
    return pts


def ladder_of(name: str) -> dict:
    ckpt = REPO / "checkpoints" / name
    m = GcadAnomalyModel()
    m._chunk = 128
    sc = StandardScaler()
    sc.fit(train[:2304].astype(np.float64))
    m._n_seq = 25
    m._norm_mean = sc.mean_.astype(np.float32)
    m._norm_std = sc.scale_.astype(np.float32)
    m._model = _TSMixerRevIN(25, SEQ, 1, 6, 0.0, 1024).to("cuda")
    m._model.load_state_dict(torch.load(ckpt / "best.pt", map_location="cuda",
                                        weights_only=False))
    m._model.eval()
    m._H = m._build_H(25, 0.008)
    m._A_norm = np.loadtxt(ckpt / "causal_parms.csv",
                           delimiter=",").astype(np.float32)
    m.fitted = True

    val_seg = train[2234:]                       # 官方 val_df 同段（646 行）
    v_raw = m.score_segment(val_seg)             # 576 窗
    t_raw = m.score_segment(test)                # 8570 窗
    v_s = _sma3(v_raw[:512])                     # drop_last 4 批 → 510
    t_s = _sma3(t_raw[:8448])                    # drop_last 66 批 → 8446
    pts = point_scores(t_s, t_test)
    auc = float(roc_auc_score(lb, pts))
    rows = []
    for tier, q in zip(TIERS, QS):
        th = float(np.max(v_s)) if q is None else float(np.quantile(v_s, q))
        pred = (pts >= th).astype(np.int64)
        tp = int(((pred == 1) & (lb == 1)).sum())
        fp = int(((pred == 1) & (lb == 0)).sum())
        fn = int(((pred == 0) & (lb == 1)).sum())
        tn = int(((pred == 0) & (lb == 0)).sum())
        rows.append(dict(tier=tier, th=round(th, 4),
                         tp=tp, fp=fp, fn=fn, tn=tn,
                         p=round(tp / max(tp + fp, 1), 4),
                         r=round(tp / max(tp + fn, 1), 4),
                         acc=round((tp + tn) / t_test, 4)))
    return dict(name=name, auc=round(auc, 4), val_max=round(float(v_s.max()), 4),
                rows=rows)


def main() -> None:
    out = {}
    for group, names in GROUPS.items():
        out[group] = []
        for name in names:
            t0 = time.monotonic()
            res = ladder_of(name)
            res["seconds"] = round(time.monotonic() - t0, 1)
            out[group].append(res)
            print(f"{name:<22} AUROC={res['auc']:.4f} "
                  f"q95 P={res['rows'][0]['p']:.3f} "
                  f"max P={res['rows'][-1]['p']:.3f}/R={res['rows'][-1]['r']:.3f} "
                  f"({res['seconds']}s)", flush=True)
    dest = WS / "results" / "gcad_official_rerun_ladders.json"
    with open(dest, "w") as f:
        json.dump(dict(protocol="th=quantile(SMA3(val))，点分=SMA3(窗口)→i+70",
                       groups=out), f, indent=2)
    print(f"落盘：{dest}")
    print("GCAD-OFFICIAL-LADDERS-DONE", flush=True)


if __name__ == "__main__":
    main()
