#!/usr/bin/env python
"""单批 A 矩阵对照：官方 autograd vs 工厂 vmap(jacrev)，同权重同输入。

对照语义（两边都含 2|ŷ−y| 因子）：
  官方  gs_stack[b,φ,i,f] = |∂(Σ_b(ŷ_f−y_f)²)/∂x_{b,φ,i}| = factor[f]·|∂ŷ_f/∂x_{φ,i}|
  工厂  Jp[b,φ,i,f]       = |∂ŷ_f/∂x_{φ,i}|·factor[f]（因子沿输出维 f 广播）
A 层：A_off[b,i,f] = mean_φ(gs_stack)；A_fac[b,i,f] = mean_φ(Jp)。
"""
import sys
from pathlib import Path

import torch
from torch.func import jacrev, vmap

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
REPO = WS / "gcad_repro"
sys.path.insert(0, str(REPO))

from models.tsmixer import TSMixerRevIN as OffModel  # noqa: E402
from utils.dataloader import SwatDataLoader_AD  # noqa: E402

sys.path.insert(0, str(ROOT / "src"))
from anomaly_models import _TSMixerRevIN as FacModel  # noqa: E402

CKPT = REPO / "checkpoints" / "smap_d3" / "best.pt"
DEV = "cuda"

dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / "D-3"), 128, 70, 1,
                       "M", target="OT")
bx, by = next(iter(dl.get_val()))
bx, by = bx[:8].to(DEV), by[:8].to(DEV)
by2 = by[:, 0, :]                                # [B,N]（pred_len 维压掉）

off = OffModel((70, 25), 1, 6, 0.0, 1024, slice(0, None)).to(DEV)
off.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
off.eval()
fac = FacModel(25, 70, 1, 6, 0.0, 1024).to(DEV)
fac.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
fac.eval()

# ── 官方 autograd（同 test.py：reduction=sum 逐特征反传）──
criterion = torch.nn.MSELoss(reduction="sum").to(DEV)
bx2 = bx.clone().requires_grad_(True)
out = off(bx2)
gs = []
for f in range(25):
    off.zero_grad()
    loss = criterion(out[:, :, f], by[:, :, f])
    loss.backward(retain_graph=True)
    gs.append(bx2.grad.abs().detach())
    bx2.grad = None
gs_stack = torch.stack(gs, dim=-1)               # [B,φ,i,f]（含因子）

# ── 工厂 vmap(jacrev)（同 _graphs_batch 修复后公式）──
fw = lambda x: fac(x.unsqueeze(0)).reshape(-1)   # noqa: E731
with torch.no_grad():
    yh = vmap(fw)(bx)
    J = vmap(jacrev(fw))(bx)
factor = 2.0 * (yh - by2).abs()                  # [B,N] 按输出维
Jp = J.abs().permute(0, 2, 3, 1) * factor.reshape(-1, 1, 1, 25)   # [B,φ,i,f]
A_off = torch.mean(gs_stack, dim=1)              # [B,i,f]
A_fac = Jp.mean(dim=1)                           # [B,i,f]

dphi = (gs_stack - Jp).abs()
d = (A_off - A_fac).abs()
print(f"[逐φ·含因子] diff max = {dphi.max().item():.3g}  mean = {dphi.mean().item():.3g}")
print(f"[A] diff max = {d.max().item():.3g}  mean = {d.mean().item():.3g}  "
      f"官方 max = {A_off.max().item():.3g}  工厂 max = {A_fac.max().item():.3g}")
mask = (A_off > 1e-6) | (A_fac > 1e-6)
ratio = A_off[mask] / (A_fac[mask] + 1e-12)
print(f"[A] 非零元素 {int(mask.sum())}/{A_off.numel()}，比值 p10={ratio.quantile(0.1):.4g} "
      f"p50={ratio.median():.4g} p90={ratio.quantile(0.9):.4g}")
# 相对误差（对量级大的元素）
rel = (d[mask] / (A_off[mask] + 1e-12))
print(f"[A] 相对误差 p90={rel.quantile(0.9):.4g} max={rel.max():.4g}")
