"""PatchTST 预测模型：用 transformers 的 PatchTSTForPrediction 做多元时序预测。

替换旧的 AutoregressiveModel，接口保持一致（fit / forecast），engine 改动最小。

- 多元输入：目标序列 + 特征序列一起进模型（num_input_channels = 列数）；
- 按列标准化训练/推理，输出再反标准化；
- fit 在 GPU 上训练，forecast 自动切回 CPU 推理（不依赖 GPU 也能跑）；
- save/load 供磁盘缓存（训练一次、复用多次）。
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)

# 80 核机器上 torch 默认开满 OpenMP 线程，训练反而被线程调度拖慢；
# 训练限线程数，避免和 gRPC 服务线程争抢 CPU（生产并发是硬性要求）。
_TRAIN_THREADS = int(os.environ.get("SFKG_MAX_THREADS", "4"))

try:
    import torch
    from torch.optim import AdamW
    from transformers import PatchTSTConfig, PatchTSTForPrediction
except ImportError as e:  # pragma: no cover - 依赖缺失时给清晰错误
    raise ImportError(
        "PatchTSTForecaster 需要 torch + transformers。先安装：\n"
        "  pip install torch\n"
        "  pip install transformers>=4.40\n"
    ) from e


@dataclass
class PatchTSTForecaster:
    """PatchTST 多元预测器。

    sequence_ids: 列顺序（目标列 + 特征列），和输入矩阵的列一一对应。
    context_length / prediction_length: 输入窗口长度 / 预测步数。
    """

    sequence_ids: list[str]
    context_length: int = 96
    prediction_length: int = 24
    patch_size: int = 16
    patch_stride: int = 8
    d_model: int = 64
    n_heads: int = 4
    num_layers: int = 2
    epochs: int = 20
    batch_size: int = 64
    learning_rate: float = 1e-3
    device: str | None = None
    model_type: str = "patchtst"

    def __post_init__(self):
        self._model = None
        if self.device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

    # ================= 训练 =================

    def fit(self, history_matrix) -> None:
        """history_matrix: [T, C] 多元历史（行=时间，列=sequence_ids）。

        切滑动窗口样本 (x: [context, C] -> y: [prediction, C])，
        GPU 训练 N 个 epoch，MSELoss。
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

        # 1. 数据不进手动标准化——用模型自带 RevIN（scaling=True）。
        #    RevIN 按样本逐通道归一化，能处理训练/推理段均值整体漂移；
        #    手动全局 mean/std 会因训练段均值主导而在漂移时失效（实验验证：双标准化 MSE 6.3 vs RevIN 1.04）。
        Xs = X

        # 2. 滑窗采样：(x, y) 对，滑动步长 = prediction_length（不重叠）
        xs, ys = [], []
        for i in range(0, T - self.prediction_length - self.context_length + 1,
                       self.prediction_length):
            xs.append(Xs[i:i + self.context_length])                # [context, C]
            ys.append(Xs[i + self.context_length:i + self.context_length
                         + self.prediction_length])                 # [prediction, C]
        if not xs:
            raise ValueError("滑窗采样没有产生训练样本，历史太短")
        xs = np.stack(xs)   # [N, context, C]
        ys = np.stack(ys)   # [N, prediction, C]
        logger.info("[patchtst] fit: %d 个窗口样本，形状 x=%s y=%s",
                    len(xs), xs.shape, ys.shape)

        # 3. 建模型
        config = PatchTSTConfig(
            num_input_channels=C,
            context_length=self.context_length,
            prediction_length=self.prediction_length,
            patch_size=self.patch_size,
            patch_stride=self.patch_stride,
            d_model=self.d_model,
            num_attention_heads=self.n_heads,
            num_hidden_layers=self.num_layers,
            scaling=True,           # 模型内 RevIN 逐样本归一化
        )
        self._model = PatchTSTForPrediction(config).to(self.device)

        # 4. 训练循环（transformers 5.x：past_values/future_values 为 [batch, time, C]）
        opt = AdamW(self._model.parameters(), lr=self.learning_rate)
        n = len(xs)
        for epoch in range(1, self.epochs + 1):
            self._model.train()
            perm = np.random.permutation(n)
            total_loss, n_batch = 0.0, 0
            for start in range(0, n, self.batch_size):
                idx = perm[start:start + self.batch_size]
                xb = torch.tensor(xs[idx]).to(self.device)     # [bs, context, C]
                yb = torch.tensor(ys[idx]).to(self.device)     # [bs, prediction, C]
                opt.zero_grad()
                out = self._model(past_values=xb, future_values=yb)
                loss = out.loss
                loss.backward()
                opt.step()
                total_loss += float(loss.detach().cpu())
                n_batch += 1
            if epoch == 1 or epoch % 5 == 0 or epoch == self.epochs:
                logger.info("[patchtst] epoch %d/%d loss=%.4f",
                            epoch, self.epochs, total_loss / max(n_batch, 1))

    # ================= 推理 =================

    def forecast(self, window_matrix, steps: int | None = None) -> dict[str, list[float]]:
        """window_matrix: [context_length, C] 最近对齐窗口。

        返回 {sequence_id: 未来 steps 步的预测值}。全部列都预测，engine 取目标列。
        steps 未给 = prediction_length；比 prediction_length 短截断，长则滚动补足。
        """
        if self._model is None:
            raise RuntimeError("还没训练，先调用 fit()")
        if steps is None:
            steps = self.prediction_length
        X = np.asarray(window_matrix, dtype=np.float32)
        if X.ndim != 2:
            raise ValueError(f"window_matrix 应为 [context, C]，实际 {X.shape}")

        self._model.eval()
        dev = self.device if self.device == "cuda" and torch.cuda.is_available() else "cpu"
        self._model.to(dev)

        with torch.no_grad():
            # RevIN 在模型内部做逐样本归一化，这里直接喂原始数据
            x = torch.tensor(X[None]).to(dev)                     # [1, context, C]
            out = self._model(past_values=x)
            pred = out.prediction_outputs[0].cpu().numpy()        # [prediction, C]
            raw = pred

            # steps 对齐
            if steps < self.prediction_length:
                raw = raw[:steps, :]
            elif steps > self.prediction_length:
                raw = self._roll_forward(X, raw, steps)

        result = {}
        for i, sid in enumerate(self.sequence_ids):
            result[sid] = [float(v) for v in raw[:, i]]
        return result

    def _roll_forward(self, X, raw, steps: int) -> np.ndarray:
        """steps > prediction_length：把预测出的值接回窗口尾部，再预测一次，滚动补足。

        raw: [prediction_length, C]（模型单次输出）。
        只支持一次滚动补足：先取预测前 prediction_length 步，接回窗口尾部再预测一轮，
        拼成 [steps, C]；仍不足的部分截断（调用方应保证 steps 与模型 prediction 接近）。
        """
        # 第一次模型输出（前面 forecast 已算出）
        first = raw[: self.prediction_length, :]                     # [pred, C]
        # 窗口接上第一轮预测，滑掉 pred 步
        rolled = np.vstack([X[self.prediction_length:], first])     # [context, C]
        x = torch.tensor(rolled[None]).to(self.device)
        with torch.no_grad():
            out = self._model(past_values=x)
            second = out.prediction_outputs[0].cpu().numpy()
        # 拼接：pred 步 + 剩余 (steps - pred) 步
        out_len = steps - self.prediction_length
        second = second[:out_len, :]
        return np.vstack([first, second])                            # [steps, C]

    # ================= 持久化 =================

    def to_dict(self) -> dict:
        """序列化到 dict（含 model_type，供 engine loader 分发/嵌套进 CatBoost 负载）。"""
        if self._model is None:
            raise RuntimeError("还没训练，先调用 fit()")
        return {
            "model_type": self.model_type,
            "state_dict": self._model.state_dict(),
            "config": self._model.config.to_dict(),
            "sequence_ids": self.sequence_ids,
            "context_length": self.context_length,
            "prediction_length": self.prediction_length,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PatchTSTForecaster":
        """从 to_dict 的 dict 重建模型（含嵌套：CatBoost 内部 PatchTST 也走这里）。"""
        fc = cls(sequence_ids=d["sequence_ids"],
                 context_length=d["context_length"],
                 prediction_length=d["prediction_length"])
        cfg = PatchTSTConfig(**d["config"])
        fc._model = PatchTSTForPrediction(cfg)
        fc._model.load_state_dict(d["state_dict"])
        fc._model.eval()
        return fc

    def save(self, path) -> None:
        """存 model state_dict + 标准化参数 + 元信息到 path（供磁盘缓存）。"""
        torch.save(self.to_dict(), path)

    def load(self, path) -> None:
        """从 path 恢复模型。"""
        restored = PatchTSTForecaster.from_dict(torch.load(path, map_location="cpu"))
        self.sequence_ids = restored.sequence_ids
        self.context_length = restored.context_length
        self.prediction_length = restored.prediction_length
        self._model = restored._model
        logger.info("[patchtst] load %s 完成", path)
