#!/usr/bin/env python
"""三方对照（batch=1 单窗口单特征）：定位官方 autograd vs 工厂 jacrev 的差异层。

  1) 官方模型 off + 官方 autograd（loss backward → |bx.grad|）
  2) 工厂模型 fac + 手动 autograd（autograd.grad(loss, x)）
  3) 工厂模型 fac + jacrev(fw)
三者同窗口 x [70,25] 同特征 f → 逐 φ 打印，比值看差异在哪一层。
"""
import sys
from pathlib import Path

import torch
from torch.func import jacrev

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
REPO = ROOT / "eval_workspace" / "gcad_repro"
sys.path.insert(0, str(REPO))

from models.tsmixer import TSMixerRevIN as OffModel  # noqa: E402
from utils.dataloader import SwatDataLoader_AD  # noqa: E402

sys.path.insert(0, str(ROOT / "src"))
from anomaly_models import _TSMixerRevIN as FacModel  # noqa: E402

CKPT = REPO / "checkpoints" / "smap_d3" / "best.pt"
DEV = "cuda"
F, I = 7, 0          # 特征 f、输入通道 i（逐 φ 曲线打印这两个）

dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / "D-3"), 128, 70, 1,
                       "M", target="OT")
bx, by = next(iter(dl.get_val()))
x0, y0 = bx[0:1].to(DEV), by[0:1].to(DEV)      # 单窗口 [1,70,25]

off = OffModel((70, 25), 1, 6, 0.0, 1024, slice(0, None)).to(DEV)
off.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
off.eval()
fac = FacModel(25, 70, 1, 6, 0.0, 1024).to(DEV)
fac.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
fac.eval()

criterion = torch.nn.MSELoss(reduction="sum").to(DEV)

# 1) 官方 autograd
bx1 = x0.clone().requires_grad_(True)
out1 = off(bx1)
loss1 = criterion(out1[:, :, F], y0[:, :, F])
loss1.backward()
g1 = bx1.grad.abs().detach()[0, :, I]           # 逐 φ [70]

# 2) 工厂手动 autograd
bx2 = x0.clone().requires_grad_(True)
out2 = fac(bx2)
loss2 = criterion(out2[:, :, F], y0[:, :, F])
g2 = torch.autograd.grad(loss2, bx2, retain_graph=False)[0].abs().detach()[0, :, I]

# 3) 工厂 jacrev（单窗口，不 vmap）
fw = lambda x: fac(x.unsqueeze(0)).reshape(-1)  # noqa: E731
xh = x0[0].clone()
J3 = jacrev(fw)(xh).abs().detach()[F, :, I]     # ∂ŷ_F/∂x_φ,i 逐 φ
g3 = 2.0 * (out2[0, 0, F].detach() - y0[0, 0, F]).abs() * J3.detach()

print(f"[前向] off={out1[0,0,F].item():.6g} fac={out2[0,0,F].item():.6g}")
print(f"[曲线 φ=0..69] (f={F}, i={I})")
print("官方autograd:", g1.cpu().numpy())
print("工厂autograd:", g2.cpu().numpy())
print("工厂jacrev  :", g3.cpu().numpy())
r12 = (g1 / (g2 + 1e-12))
r23 = (g2 / (g3 + 1e-12))
print(f"[比值] 官方/工厂autograd: p10={r12.quantile(0.1):.4g} "
      f"p50={r12.median():.4g} p90={r12.quantile(0.9):.4g} max={r12.max():.4g}")
print(f"[比值] 工厂autograd/jacrev: p10={r23.quantile(0.1):.4g} "
      f"p50={r23.median():.4g} p90={r23.quantile(0.9):.4g} max={r23.max():.4g}")
