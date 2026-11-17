#!/usr/bin/env python
"""MASE 36/72 步差距测量（2026-10-28 导师新要求：预测步长最小 36、希望 72）。

背景：预测步长 ≥36（目标 72）成为硬要求（SMD 1 分钟采样 → 36/72 分钟提前）。
此前全部预测数字均不满足步长口径（CI=3 步、工厂=24 步、solar=1 步），需在
36/72 步口径下重测。本脚本先量当前方法位置（1 种子快读），供换方法决策。

方法：
  - patchtst：CI PatchTST（当前最佳路径，SMD 2-4 逐通道）——
    直接把 prediction_length 设为 PRED 一次性直出（不用滚动补足，滚动=自回归
    累积误差，不是长步长直出）。
  - dlinear：DLinear（AAAI 2023）——长步长文献强基线、调研候选；逐通道
    z-score、分解 trend/residual 双线性层、Adam 20 epoch。

口径（沿用 mase_ci_verify_cont.py，仅改步长）：
  - CTX=336 输入、测试滑窗 stride=PRED 不重叠；
  - 每窗口误差 = PRED 步平均 |实际-预测|（逐通道平均）；
  - MASE 分母 = 训练段 naive 尺度（Hyndman-Koehler），m∈{1,24,1440}；
  - 全段 + 正常段（预测段 [t,t+PRED) 标签全 0）两口径。
"""
import argparse
import random
import sys
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

import evaluate_contract_benchmarks as ev  # noqa: E402
from patchtst_forecaster import PatchTSTForecaster  # noqa: E402

CTX = 336


def naive_scale(y: np.ndarray, m: int) -> float:
    return float(np.mean(np.abs(y[m:] - y[:-m])))


class DLinear(nn.Module):
    """AAAI 2023 DLinear：序列分解（trend/residual）+ 两个线性层。"""

    def __init__(self, ctx: int, pred: int, kernel: int = 25):
        super().__init__()
        self.avg = nn.AvgPool1d(kernel_size=kernel, stride=1,
                                padding=kernel // 2)
        self.lin_t = nn.Linear(ctx, pred)
        self.lin_s = nn.Linear(ctx, pred)

    def forward(self, x):  # (B, L, 1) -> (B, H)
        x = x.squeeze(-1)
        xs = self.avg(x)          # 趋势 = 移动平均
        xt = x - xs               # 残差
        return self.lin_t(xt) + self.lin_s(xs)


class DLinearForecaster:
    """逐通道 DLinear：训练段 z-score；滑窗 stride=pred；Adam 20 epoch。"""

    def __init__(self, ctx, pred, seed=0, epochs=20, batch_size=64, lr=1e-3,
                 device="cuda"):
        self.ctx, self.pred = ctx, pred
        self.epochs, self.bs, self.lr = epochs, batch_size, lr
        self.device = device
        self.seed = seed

    def fit(self, series: np.ndarray) -> "DLinearForecaster":
        x = series[:, 0].astype(np.float64)
        self.mu, self.sd = float(x.mean()), float(x.std())
        z = (x - self.mu) / (self.sd + 1e-8)
        X, Y = [], []
        for i in range(0, len(z) - self.ctx - self.pred + 1, self.pred):
            X.append(z[i:i + self.ctx])
            Y.append(z[i + self.ctx:i + self.ctx + self.pred])
        X = torch.tensor(np.array(X), dtype=torch.float32,
                         device=self.device).unsqueeze(-1)
        Y = torch.tensor(np.array(Y), dtype=torch.float32, device=self.device)
        torch.manual_seed(self.seed)
        model = DLinear(self.ctx, self.pred).to(self.device)
        opt = torch.optim.Adam(model.parameters(), lr=self.lr)
        lossf = nn.MSELoss()
        model.train()
        for _ in range(self.epochs):
            perm = torch.randperm(len(X))
            for b in range(0, len(X), self.bs):
                idx = perm[b:b + self.bs]
                opt.zero_grad()
                lossf(model(X[idx]), Y[idx]).backward()
                opt.step()
        model.eval()
        self.model = model
        return self

    def forecast(self, series: np.ndarray, steps: int) -> np.ndarray:
        z = (series[:, 0].astype(np.float64) - self.mu) / (self.sd + 1e-8)
        w = torch.tensor(z[-self.ctx:], dtype=torch.float32,
                         device=self.device).view(1, -1, 1)
        with torch.no_grad():
            p = self.model(w)[0].cpu().numpy()
        return p * self.sd + self.mu


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity-index", type=int, default=11)
    ap.add_argument("--pred", type=int, default=36, choices=[36, 72])
    ap.add_argument("--methods", default="patchtst,dlinear")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--device", default="cuda")
    args = ap.parse_args()

    ent = ev.load_entities("SMD")[args.entity_index]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    PRED = args.pred
    ws_all = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm_mask = np.array([bool(labels[t:t + PRED].sum() == 0)
                          for t in ws_all])
    print(f"== SMD/{ent.name} pred={PRED} ctx={CTX} "
          f"[windows all={len(ws_all)} norm={int(norm_mask.sum())}]",
          flush=True)

    for mname in args.methods.split(","):
        t0 = time.monotonic()
        mae_all = np.zeros(C)
        mae_norm = np.zeros(C)
        cnt_norm = np.zeros(C, dtype=np.int64)
        for c in range(C):
            random.seed(args.seed)
            np.random.seed(args.seed)
            torch.manual_seed(args.seed)
            if mname == "patchtst":
                fc = PatchTSTForecaster(sequence_ids=["c0"],
                                        context_length=CTX,
                                        prediction_length=PRED, patch_size=8,
                                        patch_stride=4, d_model=64, n_heads=4,
                                        num_layers=2, epochs=20, batch_size=64,
                                        device=args.device)
                fc.fit(train[:, c:c + 1])

                def predict(fc, x):
                    return np.array(fc.forecast(x, steps=PRED)["c0"],
                                    dtype=np.float64)
            else:  # dlinear
                fc = DLinearForecaster(CTX, PRED, seed=args.seed,
                                       device=args.device).fit(train[:, c:c + 1])

                def predict(fc, x):
                    return fc.forecast(x, steps=PRED)
            for i, t in enumerate(ws_all):
                p = predict(fc, test[t - CTX:t, c:c + 1])
                e = np.mean([abs(float(test[t + h, c]) - p[h])
                             for h in range(PRED)])
                mae_all[c] += e
                if norm_mask[i]:
                    mae_norm[c] += e
                    cnt_norm[c] += 1
            if (c + 1) % 19 == 0:
                print(f"[{mname} ch {c+1}/{C}]", flush=True)
        mae_all /= len(ws_all)
        mae_norm /= np.maximum(cnt_norm, 1)
        for m in (1, 24, 1440):
            ns = [naive_scale(train[:, c].astype(np.float64), m)
                  for c in range(C)]
            for tag, mae in (("全段", mae_all), ("正常段", mae_norm)):
                ms = [mae[c] / s for c, s in enumerate(ns) if s > 0]
                print(f"[{mname} pred={PRED} m={m:<4} {tag}] "
                      f"MASE={np.mean(ms):.3f} "
                      f"(min {np.min(ms):.3f}/max {np.max(ms):.3f})",
                      flush=True)
        print(f"[{mname} pred={PRED} {time.monotonic()-t0:.0f}s]", flush=True)
    print("HORIZON-SWEEP-DONE", flush=True)


if __name__ == "__main__":
    main()
