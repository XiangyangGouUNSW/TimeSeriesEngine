#!/usr/bin/env python
"""D-3 工厂 vs 官方诊断：定位工厂打分链尾部爆炸的根因。

三个对照：
  1) 工厂自训模型的 Ā_norm / val 分数分布 vs 官方 causal_parms.csv / val_scores.npy；
  2) 官方 best.pt + causal parms 灌进工厂模型打分 → 应复现官方记录阶梯
     （AUROC≈0.9486、val 平滑 max≈5.96）→ 证明工厂打分链与官方同构；
  3) 工厂自训的轮数与 val 损失轨迹（观测 _epochs_run）。
"""
import sys
from pathlib import Path

import numpy as np
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
REPO = WS / "gcad_repro"
sys.path.insert(0, str(ROOT / "src"))

from anomaly_models import GcadAnomalyModel, _TSMixerRevIN, _sma3  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402
from sklearn.preprocessing import StandardScaler  # noqa: E402

SEQ = 70


def stats(name: str, a: np.ndarray) -> None:
    nz = a[a > 0]
    print(f"[{name}] shape={a.shape} n>0={nz.size} "
          f"min={nz.min():.3g} p50={np.median(nz):.3g} p90={np.percentile(nz, 90):.3g} "
          f"max={nz.max():.3g} n>0.008={int((nz > 0.008).sum())} n>1={int((nz > 1).sum())}",
          flush=True)


def main() -> None:
    train = np.load(WS / "data" / "SMAP" / "D-3" / "train.npy")
    test = np.load(WS / "data" / "SMAP" / "D-3" / "test.npy")
    lb = np.load(WS / "data" / "SMAP" / "D-3" / "test_label.npy").astype(np.int64)

    # ── 1) 工厂自训 ──────────────────────────────────────────────
    m = GcadAnomalyModel()
    m._chunk = 128
    m.fit(train)
    print(f"[工厂自训] 轮数={m._epochs_run}", flush=True)
    stats("工厂 Ā_norm", m._A_norm)
    vs = m._val_S
    print(f"[工厂 val 原始分数] p50={np.median(vs):.4g} p90={np.percentile(vs, 90):.4g} "
          f"p99={np.percentile(vs, 99):.4g} max={vs.max():.4g}", flush=True)

    # ── 官方 10-28 产物 ──────────────────────────────────────────
    par = np.loadtxt(REPO / "checkpoints" / "smap_d3" / "causal_parms.csv",
                     delimiter=",")
    stats("官方 causal_parms", par)
    off_val = np.load(WS / "results" / "gcad_d3_channel" / "val_scores.npy")
    print(f"[官方 val 平滑分数] p50={np.median(off_val):.4g} "
          f"p90={np.percentile(off_val, 90):.4g} p99={np.percentile(off_val, 99):.4g} "
          f"max={off_val.max():.4g}", flush=True)

    # ── 2) 官方权重灌工厂模型打分 ────────────────────────────────
    m2 = GcadAnomalyModel()
    m2._chunk = 128
    sc = StandardScaler()
    sc.fit(train[:2304].astype(np.float64))
    m2._n_seq = 25
    m2._norm_mean = sc.mean_.astype(np.float32)
    m2._norm_std = sc.scale_.astype(np.float32)
    m2._model = _TSMixerRevIN(25, SEQ, 1, 6, 0.0, 1024).to("cuda")
    m2._model.load_state_dict(torch.load(
        REPO / "checkpoints" / "smap_d3" / "best.pt",
        map_location="cuda", weights_only=False))
    m2._model.eval()
    m2._H = m2._build_H(25, 0.008)
    m2._A_norm = par.astype(np.float32)
    m2.fitted = True

    val_seg = train[2234:]                      # 官方 val_df 同段（646 行）
    v_raw = m2.score_segment(val_seg)           # 576 窗
    t_raw = m2.score_segment(test)              # 8570 窗
    v_s = _sma3(v_raw[:512])                    # 官方 drop_last 前 512 → 510
    t_s = _sma3(t_raw[:8448])                   # 8446
    print(f"[官方权重→工厂打分] val 平滑 max={v_s.max():.4g}（官方记录 5.96）", flush=True)
    pts = np.empty(len(test))
    n = len(t_s)
    for i in range(n):
        pts[i + SEQ] = t_s[i]
    pts[:SEQ] = t_s[0]
    pts[n + SEQ:] = t_s[-1]
    auc = roc_auc_score(lb, pts)
    print(f"[官方权重→工厂打分] 点级 AUROC={auc:.4f}（官方记录 0.9486）", flush=True)

    # ── 3) 工厂自训的分数对照（同口径：前 512 平滑）────────────────
    v_f = _sma3(m._val_S[:512])
    print(f"[工厂自训 val 平滑] p99={np.percentile(v_f, 99):.4g} max={v_f.max():.4g} "
          f"（官方记录 p99≈5.9 max≈5.96）", flush=True)


if __name__ == "__main__":
    main()
