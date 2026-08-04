"""自回归（AR）小模型：用前 p 个值预测下一个值。

公式：x[t] = a1*x[t-1] + a2*x[t-2] + ... + ap*x[t-p] + 截距
训练就是用最小二乘把系数 a1..ap 和截距拟合出来。

以后可以把这个类换成 Transformer 模型，接口保持一致：
    fit(history) / forecast(history, steps)
"""

from __future__ import annotations

import numpy as np


class AutoregressiveModel:
    def __init__(self, order: int = 5):
        self.order = order
        self._coef: np.ndarray | None = None   # 拟合出的系数（最后一个是截距）

    def fit(self, series) -> None:
        """series: 这条序列的一维历史值。"""
        series = np.asarray(series, dtype=float)
        n = len(series)
        if n < self.order + 10:
            raise ValueError("数据太少，没法拟合自回归模型")

        # 造训练样本：(x[t-p..t-1]) -> x[t]
        X, y = [], []
        for t in range(self.order, n):
            X.append(series[t - self.order:t])
            y.append(series[t])
        X = np.array(X)
        Xb = np.hstack([X, np.ones((len(X), 1))])   # 最后一列都是 1，当截距
        self._coef = np.linalg.lstsq(Xb, np.array(y), rcond=None)[0]

    def _predict_one(self, history) -> float:
        """用最近 order 个值，预测下一个。"""
        last = np.asarray(history[-self.order:], dtype=float)
        x = np.hstack([last, [1.0]])
        return float(self._coef @ x)

    def forecast(self, history, steps: int) -> list[float]:
        """从 history 开始，一步一步滚动预测 steps 步。"""
        if self._coef is None:
            raise RuntimeError("还没训练，先调用 fit()")
        current = list(history)
        preds = []
        for _ in range(steps):
            v = self._predict_one(current)
            preds.append(v)
            current.append(v)      # 预测值当作下一步的输入
        return preds
