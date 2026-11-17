"""DLinear 预测模型（AAAI 2023 线性族）：合同验收方法（PSM 72 步 m1440 达标）。

与 PatchTSTForecaster 接口一致（fit / forecast / save / load），engine 改动最小。
实现与评测协议逐行对齐（`eval_workspace/scripts/mase_horizon_sweep.py` 的
DLinearForecaster，PSM 36/72 步 10 种子达标数字即出自该协议）：

- 逐通道独立建模（每列一个 DLinear，z-score 标准化）；
- 序列分解 trend/residual 双线性层（AvgPool1d kernel=25）；
- 训练滑窗 stride = prediction_length（不重叠），Adam 20 epoch；
- fit 里 torch.manual_seed(seed) 再建模型 → 同种子完全可复现（验收复验依赖此性质）；
- steps 未给 = prediction_length；比 prediction_length 短截断，长则自回归滚动补足
  （滚动在 z-score 空间进行，最后统一反标准化）。
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# 80 核机器上 torch 默认开满 OpenMP 线程，训练反而被线程调度拖慢；
# 训练限线程数，避免和 gRPC 服务线程争抢 CPU（生产并发是硬性要求）。
_TRAIN_THREADS = int(os.environ.get("SFKG_MAX_THREADS", "4"))

try:
    import torch
    import torch.nn as nn
except ImportError as e:  # pragma: no cover - 依赖缺失时给清晰错误
    raise ImportError(
        "DLinearForecaster 需要 torch。先安装：\n  pip install torch\n"
    ) from e


class _DLinear(nn.Module):
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


@dataclass
class DLinearForecaster:
    """逐通道 DLinear 预测器。

    sequence_ids: 列顺序（目标列 + 特征列），和输入矩阵的列一一对应。
    context_length / prediction_length: 输入窗口长度 / 预测步数。
    """

    sequence_ids: list[str]
    context_length: int = 336
    prediction_length: int = 72
    kernel_size: int = 25
    epochs: int = 20
    batch_size: int = 64
    learning_rate: float = 1e-3
    seed: int = 0
    device: str | None = None
    model_type: str = "dlinear"

    def __post_init__(self) -> None:
        self._models: list[_DLinear] | None = None
        self._mu: np.ndarray | None = None
        self._sd: np.ndarray | None = None
        if self.device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

    # ================= 训练 =================

    def fit(self, history_matrix: np.ndarray) -> None:
        """history_matrix: [T, C] 多元历史（行=时间，列=sequence_ids）。

        逐通道独立训练：z-score 标准化、滑窗 stride=prediction_length、
        torch.manual_seed(self.seed) 后建模型 → 同种子逐位可复现（评测协议同构）。
        """
        torch.set_num_threads(_TRAIN_THREADS)
        X = np.asarray(history_matrix, dtype=np.float32)
        if X.ndim != 2:
            raise ValueError(f"history_matrix 应为 [T, C]，实际 {X.shape}")
        T, C = X.shape
        need = self.context_length + self.prediction_length
        if T < need:
            raise ValueError(
                f"历史数据不足：{T} 点 < 需要 {need}（context {self.context_length}"
                f" + prediction {self.prediction_length}）"
            )
        if C != len(self.sequence_ids):
            raise ValueError(
                f"列数 {C} != sequence_ids 数量 {len(self.sequence_ids)}"
            )

        dev = self.device
        models: list[_DLinear] = []
        mu, sd = [], []
        for c in range(C):
            x = X[:, c].astype(np.float64)
            mu_c, sd_c = float(x.mean()), float(x.std())
            z = (x - mu_c) / (sd_c + 1e-8)
            xs, ys = [], []
            for i in range(0, len(z) - self.context_length
                           - self.prediction_length + 1,
                           self.prediction_length):
                xs.append(z[i:i + self.context_length])
                ys.append(z[i + self.context_length:
                            i + self.context_length + self.prediction_length])
            if not xs:
                raise ValueError("滑窗采样没有产生训练样本，历史太短")
            Xt = torch.tensor(np.array(xs), dtype=torch.float32,
                              device=dev).unsqueeze(-1)       # [N, ctx, 1]
            Yt = torch.tensor(np.array(ys), dtype=torch.float32, device=dev)

            torch.manual_seed(self.seed)   # 建模型前固定种子 → 逐位可复现
            model = _DLinear(self.context_length, self.prediction_length,
                             self.kernel_size).to(dev)
            opt = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
            lossf = nn.MSELoss()
            model.train()
            for _ in range(self.epochs):
                perm = torch.randperm(len(Xt))
                for b in range(0, len(Xt), self.batch_size):
                    idx = perm[b:b + self.batch_size]
                    opt.zero_grad()
                    lossf(model(Xt[idx]), Yt[idx]).backward()
                    opt.step()
            model.eval()
            models.append(model)
            mu.append(mu_c)
            sd.append(sd_c)
        self._models = models
        self._mu = np.array(mu, dtype=np.float64)
        self._sd = np.array(sd, dtype=np.float64)
        logger.info("[dlinear] fit: %d 通道完成（ctx=%d pred=%d kernel=%d）",
                    C, self.context_length, self.prediction_length,
                    self.kernel_size)

    # ================= 推理 =================

    def forecast(self, window_matrix: np.ndarray,
                 steps: int | None = None) -> dict[str, list[float]]:
        """window_matrix: [context_length, C] 最近对齐窗口。

        返回 {sequence_id: 未来 steps 步的预测值}。全部列都预测，engine 取目标列。
        steps 未给 = prediction_length；比 prediction_length 短截断，长则滚动补足。
        """
        if self._models is None:
            raise RuntimeError("还没训练，先调用 fit()")
        if steps is None:
            steps = self.prediction_length
        X = np.asarray(window_matrix, dtype=np.float32)
        if X.ndim != 2:
            raise ValueError(f"window_matrix 应为 [context, C]，实际 {X.shape}")

        dev = self.device if self.device == "cuda" and torch.cuda.is_available() else "cpu"

        result = {}
        for i, sid in enumerate(self.sequence_ids):
            model = self._models[i].to(dev)
            model.eval()
            col = X[:, i].astype(np.float64)
            z = (col - self._mu[i]) / (self._sd[i] + 1e-8)
            w = torch.tensor(z[-self.context_length:], dtype=torch.float32,
                             device=dev).view(1, -1, 1)
            with torch.no_grad():
                p = model(w)[0].cpu().numpy()                  # [prediction]

            # steps 对齐（滚动在 z-score 空间进行，最后统一反标准化）
            if steps < self.prediction_length:
                p = p[:steps]
            elif steps > self.prediction_length:
                p = self._roll_forward(z[-self.context_length:], p, steps, model,
                                       dev)
            result[sid] = [float(v) for v in p * self._sd[i] + self._mu[i]]
        return result

    def _roll_forward(self, window: np.ndarray, first: np.ndarray, steps: int,
                      model: _DLinear, dev) -> np.ndarray:
        """steps > prediction_length：把预测值接回窗口尾部反复滚动，凑够 steps 步。

        与 PatchTSTForecaster._roll_forward 同语义；窗口与预测块都在 z-score 空间。
        多轮自回归误差逐轮累积，但保证输出长度 == steps。
        """
        chunks = [first]
        total = self.prediction_length
        while total < steps:
            window = np.concatenate([window[self.prediction_length:], chunks[-1]])
            x = torch.tensor(window[None], dtype=torch.float32,
                             device=dev).view(1, -1, 1)
            with torch.no_grad():
                nxt = model(x)[0].cpu().numpy()
            chunks.append(nxt)
            total += self.prediction_length
        return np.concatenate(chunks)[:steps]

    # ================= 持久化 =================

    def to_dict(self) -> dict:
        """序列化到 dict（含 model_type，供 engine loader 分发）。"""
        if self._models is None:
            raise RuntimeError("还没训练，先调用 fit()")
        return {
            "model_type": self.model_type,
            "state_dicts": [m.state_dict() for m in self._models],
            "mu": self._mu.tolist(),
            "sd": self._sd.tolist(),
            "sequence_ids": self.sequence_ids,
            "context_length": self.context_length,
            "prediction_length": self.prediction_length,
            "kernel_size": self.kernel_size,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DLinearForecaster":
        """从 to_dict 的 dict 重建模型。"""
        fc = cls(sequence_ids=d["sequence_ids"],
                 context_length=d["context_length"],
                 prediction_length=d["prediction_length"],
                 kernel_size=d["kernel_size"])
        fc._models = []
        for sd in d["state_dicts"]:
            m = _DLinear(d["context_length"], d["prediction_length"],
                         d["kernel_size"])
            m.load_state_dict(sd)
            m.eval()
            fc._models.append(m)
        fc._mu = np.array(d["mu"], dtype=np.float64)
        fc._sd = np.array(d["sd"], dtype=np.float64)
        return fc

    def save(self, path: str | Path) -> None:
        """存各通道 state_dict + 标准化参数 + 元信息到 path（供磁盘缓存）。"""
        torch.save(self.to_dict(), path)

    def load(self, path: str | Path) -> None:
        """从 path 恢复模型。"""
        restored = DLinearForecaster.from_dict(
            torch.load(path, map_location="cpu"))
        self.sequence_ids = restored.sequence_ids
        self.context_length = restored.context_length
        self.prediction_length = restored.prediction_length
        self.kernel_size = restored.kernel_size
        self._models = restored._models
        self._mu = restored._mu
        self._sd = restored._sd
        logger.info("[dlinear] load %s 完成", path)
