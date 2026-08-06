"""异常检测模型：统一接口 fit/detect，可插拔。

接口约定（方便后续替换/对比其他 SOTA 模型）：
  fit(history)   history: [time, features] 的正常历史数据，学习"正常模式"
  detect(window) window:  [time, features] 的新窗口，返回 findings 列表

core 指定的两个模型：
  DbscanAnomalyModel  # 离散数值离群（DBSCAN 聚类）
  GcadAnomalyModel    # 连续多变量模式偏离（GCAD 因果）
"""

from __future__ import annotations

import numpy as np
from sklearn.cluster import DBSCAN


class AnomalyModel:
    """异常模型接口。所有异常模型实现 fit/detect，方便替换测试。"""

    def fit(self, history: np.ndarray) -> None:
        raise NotImplementedError

    def detect(self, window: np.ndarray) -> list[dict]:
        """检测一个窗口，返回 findings（list[dict]）。"""
        raise NotImplementedError


class DbscanAnomalyModel(AnomalyModel):
    """DBSCAN 离群检测：离散数值序列的离群点。

    原理：用正常历史拟合 DBSCAN，记录"核心样本"（非噪声点）；
    新点离所有核心样本都远（> eps）→ 判为离群。
    """

    def __init__(self, eps: float = 0.5, min_samples: int = 5):
        self.eps = eps
        self.min_samples = min_samples
        self._core_samples: np.ndarray | None = None

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        if history.ndim == 1:
            history = history.reshape(-1, 1)
        model = DBSCAN(eps=self.eps, min_samples=self.min_samples).fit(history)
        # 核心样本 = 标签不为 -1（不是噪声）的点
        self._core_samples = history[model.labels_ != -1]

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        findings = []
        if self._core_samples is None or len(self._core_samples) == 0:
            return findings
        for i, point in enumerate(window):
            dist = float(np.min(np.linalg.norm(self._core_samples - point, axis=1)))
            if dist > self.eps:
                findings.append({
                    "anomaly_type": "DISCRETE_OUTLIER",
                    "severity": "MEDIUM",
                    "description": f"点 {i} 离群（距正常核心样本 {dist:.2f} > {self.eps}）",
                    "score": dist,
                })
        return findings


class GcadAnomalyModel(AnomalyModel):
    """GCAD（格兰杰因果）异常检测：连续多变量序列。

    简化实现：
      fit：对每条目标序列，用"所有序列过去 lag 步的值"线性预测当前值，
           得到因果结构（哪些序列的过去影响它），并记录训练残差阈值；
      detect：用学到的因果模型预测新窗口，残差超阈值 → 模式偏离异常。
    """

    def __init__(self, lag: int = 3, residual_quantile: float = 0.9):
        self.lag = lag
        self.residual_quantile = residual_quantile
        self._coefs: list[np.ndarray] = []   # 每条序列的因果系数
        self._thresholds: list[float] = []   # 每条序列的残差阈值

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        n_time, n_seq = history.shape
        if n_time <= self.lag + 2:
            return
        self._coefs, self._thresholds = [], []
        for t in range(n_seq):
            # 特征：所有序列过去 lag 步的值（展平）+ 截距
            X, y = [], []
            for i in range(self.lag, n_time):
                X.append(history[i - self.lag:i].flatten())
                y.append(history[i, t])
            X = np.array(X)
            Xb = np.hstack([X, np.ones((len(X), 1))])
            coef, *_ = np.linalg.lstsq(Xb, np.array(y), rcond=None)
            self._coefs.append(coef)
            residuals = np.abs(np.array(y) - Xb @ coef)
            self._thresholds.append(float(np.quantile(residuals, self.residual_quantile)))

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        findings = []
        n_time, n_seq = window.shape
        if n_time <= self.lag or not self._coefs:
            return findings
        for t in range(n_seq):
            for i in range(self.lag, n_time):
                feat = np.hstack([window[i - self.lag:i].flatten(), [1.0]])
                pred = feat @ self._coefs[t]
                residual = float(abs(window[i, t] - pred))
                if residual > self._thresholds[t]:
                    findings.append({
                        "anomaly_type": "CAUSAL_PATTERN",
                        "severity": "MEDIUM",
                        "description": f"序列{t} 在点{i} 因果预测残差 {residual:.3f} 超阈值",
                        "score": residual,
                    })
        return findings


# 模型工厂：按方法名创建模型（便于后续替换/扩展）
def build_anomaly_model(method: str, **kwargs) -> AnomalyModel | None:
    """根据方法名返回模型实例；未知方法返回 None。"""
    if method == "DISCRETE_OUTLIER":
        return DbscanAnomalyModel(**kwargs)
    if method == "CAUSAL_PATTERN":
        return GcadAnomalyModel(**kwargs)
    return None
