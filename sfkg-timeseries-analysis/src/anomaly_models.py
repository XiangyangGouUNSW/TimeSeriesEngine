"""异常检测模型：统一接口 fit/detect，可插拔。

接口约定（方便后续替换/对比其他 SOTA 模型）：
  fit(history)   history: [time, features] 的正常历史数据，学习"正常模式"
  detect(window) window:  [time, features] 的新窗口，返回 findings 列表

core 指定的两个模型：
  DbscanAnomalyModel  # 离散数值离群（DBSCAN 聚类）
  GcadAnomalyModel    # 连续多变量模式偏离（GCAD 因果）
"""

from __future__ import annotations

import os

import numpy as np
from sklearn.cluster import DBSCAN
from threadpoolctl import threadpool_limits

# 80 核机器上 sklearn/numpy 默认开满 OpenMP 线程，小矩阵反而被线程调度拖到
# 60s+ 不收敛。训练一律限线程数，避免和 gRPC 服务线程争抢 CPU（生产并发是硬性要求）。
_MAX_THREADS = int(os.environ.get("SFKG_MAX_THREADS", "4"))


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
        with threadpool_limits(limits=_MAX_THREADS):
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
        with threadpool_limits(limits=_MAX_THREADS):
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


class TrendShiftAnomalyModel(AnomalyModel):
    """单变量趋势异常：滑动窗口水平漂移 + 窗口线性斜率突变检测。

    场景：序列自身趋势/水平偏离历史正常范围（缓慢爬升/跌落、阶跃、斜率突变），
    不依赖其他自变量（与 GCAD 的因果模式检测互补）。

    fit：从历史正常段估计
      - 全局均值 μ 与标准差 σ（窗口均值的水平漂移判定基准）；
      - 正常窗口线性斜率分布 → 斜率控制限。
    detect：逐点取固定长度滑动窗口
      - 窗口均值相对全局均值的偏离（按标准误缩放）超阈值 → 水平漂移；
      - 窗口线性斜率相对历史斜率分布超限 → 趋势突变。
    任一超限 → TREND_SHIFT 异常，报告方向与强度（score=最大偏离 z-score）。
    """

    def __init__(self, window: int = 10, level_limit: float = 3.0,
                 slope_std_mult: float = 3.0, diff_std_mult: float = 4.0,
                 target_index: int | None = None):
        self.window = window              # 滑动窗口长度（水平 + 斜率估计）
        self.level_limit = level_limit    # 水平漂移阈值（标准误倍数）
        self.slope_std_mult = slope_std_mult  # 斜率控制限（历史斜率标准差倍数）
        self.diff_std_mult = diff_std_mult    # 点间跳变控制限（历史差分标准差倍数）
        self.target_index = target_index
        self._baseline_mean: float | None = None
        self._baseline_std: float | None = None
        self._slope_mean: float | None = None
        self._slope_std: float | None = None
        self._diff_mean: float = 0.0
        self._diff_std: float = 1e-8
        self._w: int = 2

    def _target_col(self, matrix: np.ndarray) -> int:
        n_seq = matrix.shape[1]
        if self.target_index is not None and self.target_index < n_seq:
            return self.target_index
        return 0 if n_seq == 1 else n_seq - 1

    @staticmethod
    def _window_slope(yw: np.ndarray) -> float:
        """窗口内线性回归斜率（闭式解）。"""
        n = len(yw)
        if n < 2:
            return 0.0
        t = np.arange(n, dtype=float)
        dt = t - t.mean()
        denom = float(np.dot(dt, dt))
        if denom == 0:
            return 0.0
        return float(np.dot(dt, yw - yw.mean()) / denom)

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        if history.ndim == 1:
            history = history.reshape(-1, 1)
        idx = self._target_col(history)
        y = history[:, idx]
        n = len(y)
        if n < 2:
            return
        self._baseline_mean = float(np.mean(y))
        self._baseline_std = float(np.std(y) + 1e-8)
        # 正常窗口斜率分布（历史段的每个窗口做线性拟合）
        w = max(2, min(self.window, n - 1))
        slopes = [self._window_slope(y[t - w:t]) for t in range(w, n)]
        self._w = w
        if slopes:
            self._slope_mean = float(np.mean(slopes))
            self._slope_std = float(np.std(slopes) + 1e-8)
        else:
            self._slope_mean = 0.0
            self._slope_std = 1e-8
        # 点间差分分布（跳变检测基准）
        diffs = y[1:] - y[:-1]
        self._diff_mean = float(np.mean(diffs))
        self._diff_std = float(np.std(diffs) + 1e-8)

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        findings = []
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        n_time, n_seq = window.shape
        if self._baseline_mean is None or n_time < 2:
            return findings
        idx = self._target_col(window)
        y = window[:, idx]
        w = self._w
        for i in range(w, n_time):
            yw = y[i - w:i]
            # ① 水平漂移：窗口均值相对全局均值的偏离（标准误缩放）
            m = float(np.mean(yw))
            z_level = (m - self._baseline_mean) / (self._baseline_std / np.sqrt(w))
            # ② 趋势突变：窗口线性斜率相对历史斜率分布的偏离
            s = self._window_slope(yw)
            z_slope = (s - self._slope_mean) / self._slope_std
            # ③ 点间跳变：一阶差分相对历史差分分布的偏离（抓阶跃起点）
            d = float(y[i] - y[i - 1])
            z_diff = (d - self._diff_mean) / self._diff_std
            parts = []
            if abs(z_level) > self.level_limit:
                parts.append(f"水平{'上升' if z_level > 0 else '下降'}漂移 z={z_level:.2f}")
            if abs(z_slope) > self.slope_std_mult:
                parts.append(f"趋势{'加速上升' if z_slope > 0 else '加速下降'} z={z_slope:.2f}")
            if abs(z_diff) > self.diff_std_mult:
                parts.append(f"点间跳变 z={z_diff:.2f}")
            if parts:
                findings.append({
                    "anomaly_type": "TREND_SHIFT",
                    "severity": "MEDIUM",
                    "description": f"点{i} " + "；".join(parts),
                    "score": float(max(abs(z_level), abs(z_slope), abs(z_diff))),
                    "index": i,
                })
        return findings


class MutualCouplingModel(AnomalyModel):
    """双变量互耦异常：滑动因果系数 + 双向 GCAD 组合。

    互耦 = 两变量互相影响（A 的过去→B 当前，B 的过去→A 当前，如温度↔压力反馈）。
    检测互耦关系被破坏（单向断裂 / 双向解耦 / 耦合系数变化）：
      ① 滑动因果系数：窗口内滞后回归系数 β(A→B)、β(B→A)，相对历史正常分布
         偏离超阈值 → 耦合强度变化 / 断裂（方向可辨）；
      ② 双向 GCAD：每个方向的因果预测残差超阈值 → 模式偏移（方向可辨）。
    任一超限 → MUTUAL_COUPLING 异常，标注耦合方向（服务归因诊断：
    单向断裂时只有对应方向检出，能区分"A 不再驱动 B"还是"B 不再驱动 A"）。
    """

    def __init__(self, coupled_pairs: list[tuple[int, int]] | None = None,
                 window: int = 10, beta_std_mult: float = 2.5,
                 lag: int = 3, residual_quantile: float = 0.95,
                 alpha: float = 0.01):
        self.pairs = [tuple(sorted((a, b))) for a, b in (coupled_pairs or [])]
        self.window = window              # 因果系数估计的滑动窗口
        self.beta_std_mult = beta_std_mult    # 系数偏离阈值（历史标准差倍数）
        self.lag = lag
        self.residual_quantile = residual_quantile
        self.alpha = alpha
        self._w: int = 2
        self._models: dict[tuple[int, int], tuple] = {}
        self._beta: dict[tuple[int, int], tuple] = {}

    @staticmethod
    def _slide_lag(src: np.ndarray, tgt: np.ndarray, w: int) -> np.ndarray:
        """滑动滞后回归系数：tgt[t] 对 src[t-1] 的回归系数（窗口 w）。"""
        n = len(src)
        out = np.full(n, np.nan)
        for t in range(w, n):
            xw = src[t - w:t]
            tx = xw - xw.mean()
            den = float(np.dot(tx, tx))
            if den > 1e-12:
                out[t] = float(np.dot(tx, tgt[t - w:t] - tgt[t - w:t].mean()) / den)
            else:
                out[t] = 0.0
        return out

    def _fit_pair(self, history: np.ndarray, a: int, b: int) -> None:
        # 双向 GCAD
        fwd = GcadAnomalyModel(lag=self.lag, residual_quantile=self.residual_quantile,
                               alpha=self.alpha, target_index=b, source_indices=[a])
        bwd = GcadAnomalyModel(lag=self.lag, residual_quantile=self.residual_quantile,
                               alpha=self.alpha, target_index=a, source_indices=[b])
        fwd.fit(history)
        bwd.fit(history)
        # 正常因果系数分布（A 过去→B 当前、B 过去→A 当前）
        ba = self._slide_lag(history[:, a], history[:, b], self._w)
        bb = self._slide_lag(history[:, b], history[:, a], self._w)
        valid = ~np.isnan(ba)
        if valid.sum() > 2:
            m_ab, s_ab = float(np.nanmean(ba)), float(np.nanstd(ba) + 1e-8)
            m_ba, s_ba = float(np.nanmean(bb)), float(np.nanstd(bb) + 1e-8)
        else:
            m_ab, s_ab, m_ba, s_ba = 0.0, 1e-8, 0.0, 1e-8
        self._models[(a, b)] = (fwd, bwd)
        self._beta[(a, b)] = (m_ab, s_ab, m_ba, s_ba)

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=float)
        self._w = max(2, min(self.window, len(history) - 1))
        self._models = {}
        self._beta = {}
        for a, b in self.pairs:
            if a < history.shape[1] and b < history.shape[1]:
                self._fit_pair(history, a, b)

    def detect(self, window: np.ndarray) -> list[dict]:
        window = np.asarray(window, dtype=float)
        findings = []
        n_time, n_seq = window.shape
        w = self._w
        for (a, b), (fwd, bwd) in self._models.items():
            # ① 双向 GCAD：逐点因果预测残差
            for f in fwd.detect(window):
                item = dict(f)
                item["anomaly_type"] = "MUTUAL_COUPLING"
                item["description"] = f"耦合 列{a}→列{b} 被破坏：" + f["description"]
                findings.append(item)
            for f in bwd.detect(window):
                item = dict(f)
                item["anomaly_type"] = "MUTUAL_COUPLING"
                item["description"] = f"耦合 列{b}→列{a} 被破坏：" + f["description"]
                findings.append(item)
            # ② 滑动因果系数：耦合强度/方向变化
            m_ab, s_ab, m_ba, s_ba = self._beta[(a, b)]
            beta_ab = self._slide_lag(window[:, a], window[:, b], w)
            beta_ba = self._slide_lag(window[:, b], window[:, a], w)
            for t in range(w, n_time):
                if not np.isnan(beta_ab[t]) and abs(beta_ab[t] - m_ab) / s_ab > self.beta_std_mult:
                    findings.append({
                        "anomaly_type": "MUTUAL_COUPLING", "severity": "MEDIUM",
                        "description": (f"耦合 列{a}→列{b} 因果系数变化 "
                                        f"β={beta_ab[t]:.3f}（正常 {m_ab:.2f}±{s_ab:.2f}）"),
                        "score": float(abs(beta_ab[t] - m_ab) / s_ab), "index": t,
                    })
                if not np.isnan(beta_ba[t]) and abs(beta_ba[t] - m_ba) / s_ba > self.beta_std_mult:
                    findings.append({
                        "anomaly_type": "MUTUAL_COUPLING", "severity": "MEDIUM",
                        "description": (f"耦合 列{b}→列{a} 因果系数变化 "
                                        f"β={beta_ba[t]:.3f}（正常 {m_ba:.2f}±{s_ba:.2f}）"),
                        "score": float(abs(beta_ba[t] - m_ba) / s_ba), "index": t,
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
    if method == "TREND_SHIFT":
        return TrendShiftAnomalyModel(
            window=kwargs.get("window", 10),
            level_limit=kwargs.get("level_limit", 3.0),
            slope_std_mult=kwargs.get("slope_std_mult", 3.0),
            diff_std_mult=kwargs.get("diff_std_mult", 4.0),
            target_index=kwargs.get("target_index"),
        )
    if method == "MUTUAL_COUPLING":
        return MutualCouplingModel(
            coupled_pairs=kwargs.get("coupled_pairs"),
            window=kwargs.get("window", 10),
            beta_std_mult=kwargs.get("beta_std_mult", 2.5),
            lag=kwargs.get("lag", 3),
            residual_quantile=kwargs.get("residual_quantile", 0.95),
            alpha=kwargs.get("alpha", 0.01),
        )
    return None
