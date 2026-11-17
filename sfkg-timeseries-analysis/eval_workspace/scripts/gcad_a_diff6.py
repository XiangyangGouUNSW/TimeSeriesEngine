#!/usr/bin/env python
"""同进程三路对照：solo（基准）vs 官方循环 vs 工厂 vmap(jacrev)，batch=4。"""
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
NW = 4

dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / "D-3"), 128, 70, 1,
                       "M", target="OT")
bx, by = next(iter(dl.get_val()))
bx, by = bx[:NW].to(DEV), by[:NW].to(DEV)
by2 = by[:, 0, :]

off = OffModel((70, 25), 1, 6, 0.0, 1024, slice(0, None)).to(DEV)
off.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
off.eval()
fac = FacModel(25, 70, 1, 6, 0.0, 1024).to(DEV)
fac.load_state_dict(torch.load(CKPT, map_location=DEV, weights_only=False))
fac.eval()
criterion = torch.nn.MSELoss(reduction="sum").to(DEV)

# gcad_a_diff 第 1 段：前向预检查（嫌疑变量）
with torch.no_grad():
    yo = off(bx)
    yf = fac(bx)
    yf1 = fac(bx[0:1])
print(f"[前向] 批式 diff max = {(yo - yf).abs().max().item():.3g}")

# 基准：逐窗逐特征 solo（每特征独立前向+图）
solo = torch.zeros(NW, 70, 25, 25, device=DEV)
for f in range(25):
    for w in range(NW):
        bxw = bx[w:w + 1].clone().requires_grad_(True)
        outw = off(bxw)
        lossw = criterion(outw[:, :, f], by[w:w + 1, :, f])
        off.zero_grad()
        lossw.backward()
        solo[w, :, :, f] = bxw.grad.abs().detach()[0]
        del bxw, outw

# 官方批式循环
bx2 = bx.clone().requires_grad_(True)
out2 = off(bx2)
gs = []
for f in range(25):
    off.zero_grad()
    loss = criterion(out2[:, :, f], by[:, :, f])
    loss.backward(retain_graph=True)
    gs.append(bx2.grad.abs().detach())
    bx2.grad = None
Aoff = torch.stack(gs, dim=-1)                     # [4,70,25,25]

# 工厂 vmap(jacrev)
fw = lambda x: fac(x.unsqueeze(0)).reshape(-1)     # noqa: E731
with torch.no_grad():
    yh = vmap(fw)(bx)
    J = vmap(jacrev(fw))(bx)
Afac = J.abs().permute(0, 2, 3, 1) * (2.0 * (yh - by2).abs()).reshape(NW, 1, 1, 25)


def cmp(name: str, a: torch.Tensor, b: torch.Tensor) -> None:
    ar, br = a.reshape(-1), b.reshape(-1)
    d = (a - b).abs().reshape(-1)
    m = (ar > 1e-9) | (br > 1e-9)
    r = ar[m] / (br[m] + 1e-12)
    print(f"[{name}] max_diff={d.max().item():.3g} mean={d.mean().item():.3g} "
          f"比值 p10={r.quantile(0.1):.4g} p50={r.median():.4g} "
          f"p90={r.quantile(0.9):.4g}", flush=True)
    if d.max().item() > 1e-3:
        idx = d.argmax()
        bidx = torch.unravel_index(idx, a.shape)
        print(f"    最大差异在 {list(bidx)}: a={ar[idx].item():.6g} "
              f"b={br[idx].item():.6g}", flush=True)


cmp("官方循环 vs solo基准", Aoff, solo)
cmp("工厂vmap vs solo基准", Afac, solo)
cmp("官方循环 vs 工厂vmap", Aoff, Afac)
