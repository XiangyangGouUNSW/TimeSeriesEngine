#!/usr/bin/env python
"""四路对照（同 4 窗口 × 全 25 特征）：定位批式与逐窗的分歧侧。

  1) 官方批式 autograd（batch=4 一次前向 + 逐特征反传）
  2) 官方逐窗 autograd（batch=1 × 4 窗，逐特征反传）
  3) 工厂 vmap(jacrev)（batch=4 一次 vmap）
  4) 工厂逐窗 jacrev（batch=1 × 4 窗）
打印两两比值分布，确定哪一路在批处理时偏离其余三路。
"""
import sys
from pathlib import Path

import torch
from torch.func import jacrev, vmap

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
REPO = ROOT / "eval_workspace" / "gcad_repro"
sys.path.insert(0, str(REPO))

from models.tsmixer import TSMixerRevIN as OffModel  # noqa: E402
from utils.dataloader import SwatDataLoader_AD  # noqa: E402

sys.path.insert(0, str(ROOT / "src"))
from anomaly_models import _TSMixerRevIN as FacModel  # noqa: E402

CKPT = REPO / "checkpoints" / "smap_d3" / "best.pt"
DEV = "cuda"
NW = 4                    # 取前 4 个窗口

dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / "D-3"), 128, 70, 1,
                       "M", target="OT")
bx, by = next(iter(dl.get_val()))
bx, by = bx[:NW].to(DEV), by[:NW, 0, :].to(DEV)      # [4,70,25] / [4,25]

off = OffModel((70, 25), 1, 6, 0.0, 1024, slice(0, None)).to(DEV)
off.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
off.eval()
fac = FacModel(25, 70, 1, 6, 0.0, 1024).to(DEV)
fac.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
fac.eval()
criterion = torch.nn.MSELoss(reduction="sum").to(DEV)

# 1) 官方批式（batch=4）：[4,70,25,25]（φ,i,f 语义同 gcad_a_diff）
bx1 = bx.clone().requires_grad_(True)
out1 = off(bx1)
gs = []
for f in range(25):
    off.zero_grad()
    loss = criterion(out1[:, :, f], by[:, f])
    loss.backward(retain_graph=True)
    gs.append(bx1.grad.abs().detach())
    bx1.grad = None
A1 = torch.stack(gs, dim=-1)                        # [4,70,25,25]

# 2) 官方逐窗（batch=1 × 4）：[4,70,25,25]
gss = []
for w in range(NW):
    bxw = bx[w:w + 1].clone().requires_grad_(True)
    outw = off(bxw)
    gsw = []
    for f in range(25):
        off.zero_grad()
        lossw = criterion(outw[:, :, f], by[w:w + 1, f])
        lossw.backward(retain_graph=True)
        gsw.append(bxw.grad.abs().detach()[0])
        bxw.grad = None
    gss.append(torch.stack(gsw, dim=-1))
A2 = torch.stack(gss, dim=0)                        # [4,70,25,25]

# 3) 工厂 vmap(jacrev)（batch=4）：[4,25,70,25] → permute
fw = lambda x: fac(x.unsqueeze(0)).reshape(-1)      # noqa: E731
with torch.no_grad():
    Jv = vmap(jacrev(fw))(bx)
A3 = Jv.abs().permute(0, 2, 3, 1)                   # [4,70,25,25] = |∂ŷ/∂x|
A3 = A3 * (2.0 * (fac(bx)[:, 0, :] - by).abs()).reshape(NW, 1, 1, 25)

# 4) 工厂逐窗 jacrev（batch=1 × 4）
Jws = [jacrev(fw)(bx[w]).abs() for w in range(NW)]
A4 = torch.stack([J.permute(1, 2, 0) * (2.0 * (fac(bx[w:w + 1])[0, 0, :]
                 .detach() - by[w]).abs()).reshape(1, 1, 25)
                 for w, J in enumerate(Jws)], dim=0)  # [4,70,25,25]


def cmp(name: str, a: torch.Tensor, b: torch.Tensor) -> None:
    r = (a / (b + 1e-12)).reshape(-1)
    d = (a - b).abs().reshape(-1)
    m = (a.reshape(-1) > 1e-9) | (b.reshape(-1) > 1e-9)
    print(f"[{name}] max_abs_diff={d.max().item():.3g} "
          f"比值(非零元素 n={int(m.sum())}) p10={r[m].quantile(0.1):.4g} "
          f"p50={r[m].median():.4g} p90={r[m].quantile(0.9):.4g}", flush=True)


cmp("官方批式 vs 官方逐窗", A1, A2)
cmp("官方批式 vs 工厂vmap", A1, A3)
cmp("官方逐窗 vs 工厂逐窗", A2, A4)
cmp("工厂vmap vs 工厂逐窗", A3, A4)
