#!/usr/bin/env python
"""三连测：官方模型批式 vs 单窗的前向/损失/单样本反传，定位 4× 的来源。"""
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
F = 7

# A) 官方批式 vs 官方单窗 前向
with torch.no_grad():
    out_b = off(bx)                        # [4,1,25]
    out_w = torch.stack([off(bx[w:w + 1])[0] for w in range(NW)], dim=0)
print(f"[前向] 批式 vs 单窗 diff max = {(out_b - out_w).abs().max().item():.3g}")

# B) loss 值：批式 sum-over-batch vs Σ 单窗
with torch.no_grad():
    loss_b = criterion(out_b[:, :, F], by[:, :, F])
    loss_s = sum(criterion(out_w[w:w + 1, :, F], by[w:w + 1, :, F]).item()
                 for w in range(NW))
print(f"[loss] 批式={loss_b.item():.6g}  Σ单窗={loss_s:.6g}")

# C) 单样本反传（batch=4 内只对窗口 0 的 loss 反传）vs 单窗反传
bx1 = bx.clone().requires_grad_(True)
out1 = off(bx1)
loss1 = criterion(out1[0:1, :, F], by[0:1, :, F])   # 只窗口 0
off.zero_grad()
loss1.backward()
g1 = bx1.grad.abs().detach()[0]                     # [70,25]

bxw = bx[0:1].clone().requires_grad_(True)
outw = off(bxw)
lossw = criterion(outw[:, :, F], by[0:1, :, F])
off.zero_grad()
lossw.backward()
gw = bxw.grad.abs().detach()[0]
r = (g1.reshape(-1) / (gw.reshape(-1) + 1e-12))
print(f"[单样本反传] 批内窗口0 vs 单窗 比值 p10={r.quantile(0.1):.4g} "
      f"p50={r.median():.4g} p90={r.quantile(0.9):.4g} max={r.max():.4g}")

# D) 批式 sum-over-batch 反传 vs 单窗反传（复现 4×）
bx2 = bx.clone().requires_grad_(True)
out2 = off(bx2)
loss2 = criterion(out2[:, :, F], by[:, :, F])
off.zero_grad()
loss2.backward()
g2 = bx2.grad.abs().detach()[0]
r2 = (g2.reshape(-1) / (gw.reshape(-1) + 1e-12))
print(f"[批式sum反传] 批内窗口0 vs 单窗 比值 p10={r2.quantile(0.1):.4g} "
      f"p50={r2.median():.4g} p90={r2.quantile(0.9):.4g} max={r2.max():.4g}")
