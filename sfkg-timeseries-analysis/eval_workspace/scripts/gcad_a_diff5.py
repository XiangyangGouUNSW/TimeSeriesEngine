#!/usr/bin/env python
"""25 特征 retain_graph 循环是否污染梯度：循环内第 f 次反传 vs 单独反传 f。"""
import sys
from pathlib import Path

import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
REPO = ROOT / "eval_workspace" / "gcad_repro"
sys.path.insert(0, str(REPO))

from models.tsmixer import TSMixerRevIN as OffModel  # noqa: E402
from utils.dataloader import SwatDataLoader_AD  # noqa: E402

CKPT = REPO / "checkpoints" / "smap_d3" / "best.pt"
DEV = "cuda"
NW = 4

dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / "D-3"), 128, 70, 1,
                       "M", target="OT")
bx, by = next(iter(dl.get_val()))
bx, by = bx[:NW].to(DEV), by[:NW].to(DEV)

off = OffModel((70, 25), 1, 6, 0.0, 1024, slice(0, None)).to(DEV)
off.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
off.eval()
criterion = torch.nn.MSELoss(reduction="sum").to(DEV)

# 单独反传每个特征（各用自己的前向+图，互不干扰）
solo = []
for f in range(25):
    bxw = bx.clone().requires_grad_(True)
    outw = off(bxw)
    lossw = criterion(outw[:, :, f], by[:, :, f])
    off.zero_grad()
    lossw.backward()
    solo.append(bxw.grad.abs().detach())
    del bxw, outw

# 循环反传（一张图 + retain_graph，完全复刻 gcad_a_diff 结构）
bx2 = bx.clone().requires_grad_(True)
out2 = off(bx2)
loop = []
for f in range(25):
    off.zero_grad()
    loss = criterion(out2[:, :, f], by[:, :, f])
    loss.backward(retain_graph=True)
    loop.append(bx2.grad.abs().detach())
    bx2.grad = None

for f in range(25):
    r = (loop[f].reshape(-1) / (solo[f].reshape(-1) + 1e-12))
    bad = (r > 1.01) | (r < 0.99)
    print(f"f={f:2d}  坏元素 {int(bad.sum())}/{r.numel()}  "
          f"比值 p50={r.median():.4g} max={r.max():.4g}", flush=True)
