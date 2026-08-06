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
        self._normal_points: np.ndarray | None = None

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        if history.ndim == 1:
            history = history.reshape(-1, 1)
        model = DBSCAN(eps=self.eps, min_samples=self.min_samples).fit(history)
        # 正常点 = 所有被分进簇的点（核心点 + 边界点），排除噪声(-1)。
        # 注意：这不是"严格核心点"（严格核心点要用 model.core_sample_indices_），
        # 而是"非噪声点"——正常区域的完整形态（含边缘），检测更宽容。
        self._normal_points = history[model.labels_ != -1]

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        findings = []
        if self._normal_points is None or len(self._normal_points) == 0:
            return findings
        for i, point in enumerate(window):
            dist = float(np.min(np.linalg.norm(self._normal_points - point, axis=1)))
            if dist > self.eps:
                findings.append({
                    "anomaly_type": "DISCRETE_OUTLIER",
                    "severity": "MEDIUM",
                    "description": f"点 {i} 离群（距最近正常点 {dist:.2f} > {self.eps}）",
                    "score": dist,
                    "index": i,
                })
        return findings


class GcadAnomalyModel(AnomalyModel):
    """GCAD（格兰杰因果）异常检测：多自变量 → 单因变量。

    核心场景：因变量（如产品合格率）受多个自变量（温度、压力、转速）影响。
    fit：用【自变量的过去 lag 步】预测【因变量的当前值】（lasso 稀疏回归），
         得到因果系数（哪些自变量真正影响因变量），并记录训练残差阈值；
    detect：新窗口里用学到的因果模型预测因变量，残差超阈值 → 模式偏移。

    支持先验：
      - relations（谁影响谁）→ 通过 target/source 列索引体现
      - 相关性矩阵（C 算的）→ 筛选自变量（只保留和因变量相关性较高的）
    """

    def __init__(self, lag: int = 3, residual_quantile: float = 0.95,
                 target_index: int | None = None,
                 source_indices: list[int] | None = None,
                 correlation_prior: dict[int, float] | None = None,
                 corr_threshold: float = 0.1, alpha: float = 0.01):
        self.lag = lag
        self.residual_quantile = residual_quantile
        self.target_index = target_index
        self.corr_threshold = corr_threshold
        self.alpha = alpha
        # 原始自变量列（None=默认全部非目标列）；先验在 fit 时统一解析
        self.source_indices = list(source_indices) if source_indices else None
        self.correlation_prior = dict(correlation_prior) if correlation_prior else {}
        self._sources: list[int] = []
        self._coef: np.ndarray | None = None
        self._threshold: float | None = None

    def _resolve_sources(self, n_seq: int) -> list[int]:
        """确定最终自变量列：显式 source_indices 或全部非目标列，再按相关性先验筛选。

        先验只在 |相关系数| >= corr_threshold 时保留；全被筛掉就退回筛选前（避免过度削减）。
        """
        target = self.target_index if self.target_index is not None else n_seq - 1
        srcs = ([s for s in self.source_indices if s != target]
                if self.source_indices is not None
                else [i for i in range(n_seq) if i != target])
        if srcs and self.correlation_prior:
            selected = [s for s in srcs
                        if abs(self.correlation_prior.get(s, 0.0)) >= self.corr_threshold]
            if selected:
                srcs = selected
        return srcs

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        n_time, n_seq = history.shape
        if self.target_index is None:
            self.target_index = n_seq - 1
        srcs = self._resolve_sources(n_seq)
        self._sources = srcs
        if not srcs or n_time <= self.lag + 2:
            return
        # 特征：自变量的过去 lag 步（展平）
        X, y = [], []
        for i in range(self.lag, n_time):
            X.append(history[i - self.lag:i][:, srcs].flatten())
            y.append(history[i, self.target_index])
        X = np.array(X)
        y = np.array(y)
        # lasso 稀疏回归：只有真正影响因变量的自变量有非零系数
        from sklearn.linear_model import Lasso
        lasso = Lasso(alpha=self.alpha, max_iter=3000, tol=1e-4)
        lasso.fit(X, y)
        self._coef = np.concatenate([lasso.coef_, [lasso.intercept_]])
        residuals = np.abs(y - lasso.predict(X))
        self._threshold = float(np.quantile(residuals, self.residual_quantile))

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        findings = []
        n_time, n_seq = window.shape
        if self._coef is None or n_time <= self.lag:
            return findings
        srcs = self._sources or self._resolve_sources(n_seq)
        for i in range(self.lag, n_time):
            feat = np.hstack([window[i - self.lag:i][:, srcs].flatten(), [1.0]])
            pred = feat @ self._coef
            residual = float(abs(window[i, self.target_index] - pred))
            if residual > self._threshold:
                findings.append({
                    "anomaly_type": "CAUSAL_PATTERN",
                    "severity": "MEDIUM",
                    "description": (f"因变量(列{self.target_index}) 在点{i} "
                                    f"因果预测残差 {residual:.3f} 超阈值"),
                    "score": residual,
                    "index": i,
                })
        return findings


# 模型工厂：按方法名创建模型（便于后续替换/扩展）
def build_anomaly_model(method: str, **kwargs) -> AnomalyModel | None:
    """根据方法名返回模型实例；未知方法返回 None。"""
    if method == "DISCRETE_OUTLIER":
        return DbscanAnomalyModel(eps=kwargs.get("eps", 0.5),
                                  min_samples=kwargs.get("min_samples", 5))
    if method == "CAUSAL_PATTERN":
        return GcadAnomalyModel(
            lag=kwargs.get("lag", 3),
            residual_quantile=kwargs.get("residual_quantile", 0.95),
            target_index=kwargs.get("target_index"),
            source_indices=kwargs.get("source_indices"),
            correlation_prior=kwargs.get("correlation_prior"),
            corr_threshold=kwargs.get("corr_threshold", 0.1),
            alpha=kwargs.get("alpha", 0.01),
        )
    return None
