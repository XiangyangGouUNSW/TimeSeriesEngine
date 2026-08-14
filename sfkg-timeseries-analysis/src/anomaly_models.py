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
import time

import numpy as np
from sklearn.cluster import DBSCAN
from threadpoolctl import threadpool_limits

# 80 核机器上 sklearn/numpy 默认开满 OpenMP 线程，小矩阵反而被线程调度拖到
# 60s+ 不收敛。训练一律限线程数，避免和 gRPC 服务线程争抢 CPU（生产并发是硬性要求）。
_MAX_THREADS = int(os.environ.get("SFKG_MAX_THREADS", "4"))

# 深度 GCAD 用 torch；懒导入：非 GCAD 方法（DBSCAN/趋势/历史匹配）不依赖 torch，
# 在无 torch 的环境里这些方法仍可用。缺 torch 时调深度 GCAD 给清晰报错。
try:
    import torch
    from torch.func import jacrev, vmap
except ImportError:  # pragma: no cover - 依赖缺失时给清晰错误
    torch = None
    jacrev = vmap = None


def _require_torch() -> None:
    """深度 GCAD 的 torch 前置检查，缺依赖时给出明确安装指引。"""
    if torch is None:
        raise ImportError(
            "深度 GCAD（GcadAnomalyModel）需要 torch：\n"
            "  pip install torch>=2.0\n"
        )

# 已知检测方法（engine.needs_training 据此判断任务是否需要训练；
# build_anomaly_model 顶部据此早退，两处判定不漂移）
KNOWN_METHODS = frozenset({"DISCRETE_OUTLIER", "CAUSAL_PATTERN",
                           "TREND_SHIFT", "MUTUAL_COUPLING",
                           "HISTORICAL_MATCH"})


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


class _TSMixer(torch.nn.Module):
    """TSMixer 预测器（Chen 2023）：全 MLP 的 Mixer 结构，输入滑窗 → 全通道下一时刻。

    - 时间维 MLP（沿 τ，跨通道共享）+ 特征维 MLP（沿 N，跨时间共享）+ LayerNorm（沿特征维）。
    - **head 按论文接收所有层输出**（"The output of each layer is fed through skip connections
      into a fully connected layer"）：每层输出经 skip connection 累加进残差流 stream，head 取
      stream 最后时间步过 Linear(N, N) → ŷ ∈ R^N。梯度经整个 Mixer 栈回传，
      ∂ŷ_j/∂x_{φ,i} 对所有 (φ, i) 一般非零，不产生人为零梯度。
    - **必须全 out-of-place**（`h = h + ...`，不用 `+=`/原地 op）：torch.func.vmap/jacrev
      对输入别名上的 in-place 修改会报错或算错。无 Dropout。
    """

    def __init__(self, n_seq: int, tau: int, hidden_dim: int = 64,
                 num_layers: int = 2):
        super().__init__()
        self.n_seq, self.tau = n_seq, tau
        self.temporal_mlps = torch.nn.ModuleList([
            torch.nn.Sequential(torch.nn.Linear(tau, hidden_dim),
                                torch.nn.ReLU(),
                                torch.nn.Linear(hidden_dim, tau))
            for _ in range(num_layers)])
        self.feature_mlps = torch.nn.ModuleList([
            torch.nn.Sequential(torch.nn.Linear(n_seq, hidden_dim),
                                torch.nn.ReLU(),
                                torch.nn.Linear(hidden_dim, n_seq))
            for _ in range(num_layers)])
        self.norms = torch.nn.ModuleList(
            [torch.nn.LayerNorm(n_seq) for _ in range(num_layers)])
        self.head = torch.nn.Linear(n_seq, n_seq)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """x: [tau, N] → ŷ: [N]。"""
        h = x
        stream = x  # 每层输出经 skip connection 汇聚（论文原版 head 输入）
        for tl, fl, ln in zip(self.temporal_mlps, self.feature_mlps, self.norms):
            h = h + tl(ln(h).transpose(0, 1)).transpose(0, 1)  # 时间混洗
            h = h + fl(ln(h))                                  # 特征混洗
            stream = stream + h
        return self.head(stream[-1])


class GcadAnomalyModel(AnomalyModel):
    """深度 GCAD（2025，arXiv 2501.13493）异常检测：TSMixer 预测器 + 梯度格兰杰因果图。

    fit：TSMixer 用滑动窗口 X∈R^{N×τ} 预测全通道下一时刻 ŷ∈R^N（MSE 训练）；
        逐通道损失 L_j=(ŷ_j-y_j)²，反传得 ∂L_j/∂X，因果强度
        a_{i,j}=Σ_φ|∂L_j/∂x_{φ,i}|，构成因果矩阵 A∈R^{N×N}（torch.func.vmap(jacrev) 批量）；
        训练窗口采样平均得正常因果图 Ā_norm，训练偏离分数分位数作阈值。
    detect：实时窗口算因果图 → 对称稀疏化 → 因果偏差打分
        S = Sc + β·St（Sc=Σ|Ã-Ā|/(Ā+ε)，St=Σ|diag差|/diag(Ā)+ε）超阈值 → 模式偏移。

    先验（技术方案 [42]，作边稀疏门）：
      - relations_prior（语义关系图：谁影响因变量）→ 该边阈值降为 h/4 优先保留
      - correlation_prior（C 端相关性矩阵）+ corr_threshold → |corr|<阈值 的边清零
    target_index=None（MUTUAL_COUPLING 用）→ 无先验纯论文全图。

    持久化：save(path)（torch.save dict + model_type="gcad"）/ load_dict(ckpt)，engine
    _register_model_loader 按 model_type 分发，重启不重训。
    """

    model_type = "gcad"

    def __init__(
        self,
        # ---- 深度 GCAD 超参（config anomaly.gcad）----
        tau: int = 8,
        hidden_dim: int = 64,
        num_layers: int = 2,
        epochs: int = 20,
        batch_size: int = 64,
        learning_rate: float = 1e-3,
        p: float = 0.5,
        n_norm_samples: int = 300,
        h: float = 0.0,
        h_quantile: float = 0.3,
        beta: float = 1.0,
        score_quantile: float = 0.95,
        eps: float = 1e-6,
        max_train_windows: int = 5000,
        train_budget_s: float = 240.0,
        val_frac: float = 0.2,
        seed: int = 0,
        # ---- 先验（engine 透传，契约不变）----
        target_index: int | None = None,
        source_indices: list[int] | None = None,
        correlation_prior: dict[int, float] | None = None,
        relations_prior: dict[int, float] | None = None,
        corr_threshold: float = 0.1,
    ):
        _require_torch()
        # 防御：engine 从 config.yaml 以 **gcad 透传这些参数，PyYAML 会把科学计数
        # 无小数点的写法（如 1e-6）解析成字符串 → 这里统一按类型强转，杜绝
        # "An + eps" 之类的 dtype 崩溃（bench_throughput 曾因 eps='1e-6' 崩）。
        self.tau = int(tau)
        self.hidden_dim = int(hidden_dim)
        self.num_layers = int(num_layers)
        self.epochs = int(epochs)
        self.batch_size = int(batch_size)
        self.learning_rate = float(learning_rate)
        self.p = float(p)
        self.n_norm_samples = int(n_norm_samples)
        self.h = float(h)
        self.h_quantile = float(h_quantile)
        self.beta = float(beta)
        self.score_quantile = float(score_quantile)
        self.eps = float(eps)
        self.max_train_windows = int(max_train_windows)
        self.train_budget_s = float(train_budget_s)
        self.val_frac = float(val_frac)
        self.seed = int(seed)
        self.corr_threshold = float(corr_threshold)
        self.target_index = target_index
        self.source_indices = list(source_indices) if source_indices else None
        self.correlation_prior = dict(correlation_prior) if correlation_prior else {}
        self.relations_prior = dict(relations_prior) if relations_prior else {}
        self._n_seq = 0
        self._model: _TSMixer | None = None
        self._norm_mean: np.ndarray | None = None
        self._norm_std: np.ndarray | None = None
        self._A_norm: np.ndarray | None = None
        self._H: np.ndarray | None = None
        self._threshold: float | None = None
        self.fitted: bool = False
        self._chunk = 32          # detect/正常图估计的分块大小（压 vmap 峰值内存）

    # ---------- 先验稀疏门 ----------

    def _build_H(self, n_seq: int, h: float) -> np.ndarray:
        """逐边稀疏阈值矩阵 H[N,N]（fit 与 detect 共用同一 H，保证可比）。

        默认 H=h（无先验时全图统一 → 纯论文）；对角 H=0（时序自相关保留）；
        relations 边 i→target 阈值降为 h/4（知识库标注的因果边优先保留）；
        correlation |corr|<corr_threshold 的 i→target 边置 +∞（清零）。
        """
        H = np.full((n_seq, n_seq), float(h), dtype=np.float64)
        np.fill_diagonal(H, 0.0)
        target = self.target_index
        if target is None or not (0 <= target < n_seq):
            return H
        if self.relations_prior:
            for i in self.relations_prior:
                if 0 <= i < n_seq and i != target:
                    H[i, target] = (h / 4.0) if h > 0 else 0.0
        if self.correlation_prior:
            for i, c in self.correlation_prior.items():
                if (0 <= i < n_seq and i != target
                        and abs(c) < self.corr_threshold):
                    H[i, target] = np.inf
        return H

    # ---------- 因果图（vmap(jacrev)） ----------

    def _graphs_batch(self, windows: np.ndarray,
                      ys: np.ndarray) -> np.ndarray:
        """批量算原始因果强度矩阵 A[B,N,N]（论文式 5）。

        windows: [B, tau, N]，ys: [B, N]（窗口**之后**的下一时刻真值）。
        y 必须是窗口下一行：a_{i,j}=2|ŷ_j-y_j|·Σ_φ|∂ŷ_j/∂x_{φ,i}|，y_j 不在窗口内。
        vmap(jacrev) 直接平铺（不嵌套在 vmap 函数内）：J [B,N,tau,N]
        （∂ŷ_j/∂x_{φ,i}），sum(dim=2) 求和 φ → S[j,i]。
        """
        model = self._model
        outs = []
        with torch.no_grad():
            for start in range(0, len(windows), self._chunk):
                chunk_w = torch.tensor(windows[start:start + self._chunk])
                chunk_y = torch.tensor(ys[start:start + self._chunk])
                yh = vmap(model)(chunk_w)                    # [B,N]
                J = vmap(jacrev(model))(chunk_w)             # [B,N,tau,N]
                s = J.abs().sum(dim=2)                       # [B,N,N] S[j,i]
                factor = 2.0 * (yh - chunk_y).abs()          # [B,N]
                outs.append((s * factor.unsqueeze(1)).transpose(1, 2))
        return torch.cat(outs, dim=0).cpu().numpy()          # [B, N, N] A[i,j]

    # ---------- 稀疏化 ----------

    @staticmethod
    def _sym_elim(A: np.ndarray) -> np.ndarray:
        """对称消除（论文式 6）：Ã[i,j]=max(0,A[i,j]-A[j,i])（i≠j），对角保留。"""
        B = np.maximum(0.0, A - np.swapaxes(A, -1, -2))
        if A.ndim == 2:
            np.fill_diagonal(B, np.diag(A))
        else:
            idx = np.arange(A.shape[-1])
            B[:, idx, idx] = A[:, idx, idx]
        return B

    def _sparsify(self, A: np.ndarray, H: np.ndarray) -> np.ndarray:
        """对称消除 + 逐边阈值：|Ã[i,j]|<H[i,j] → 0。"""
        B = self._sym_elim(A)
        return np.where(np.abs(B) < H, 0.0, B)

    # ---------- 偏差打分 ----------

    def _deviation_components(self, A_sparse: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """论文式 10-12：返回 (S, Sc, St)，S=Sc+β·St（全 N×N 图）。"""
        eps = self.eps
        An = self._A_norm
        sc = np.abs(A_sparse - An) / (An + eps)          # [B,N,N]
        Sc = sc.sum(axis=(1, 2))
        diag_norm = np.diag(An)
        diag_diff = np.abs(np.diagonal(A_sparse, axis1=1, axis2=2) - diag_norm)
        St = (diag_diff / (diag_norm + eps)).sum(axis=1)
        return Sc + self.beta * St, Sc, St

    def _deviation_scores(self, A_sparse: np.ndarray) -> np.ndarray:
        return self._deviation_components(A_sparse)[0]

    def _deviation_on(self, X: np.ndarray):
        """对已标准化的数据 X[T,N] 算滑窗稀疏图 → 返回 (S, Sc, St, dev)。

        dev[B,N,N]=|Ã-Ā|/(Ā+ε) 逐边偏离矩阵（detect 取最大边、MutualCoupling 取方向边）。
        """
        windows = np.stack([X[t - self.tau:t] for t in range(self.tau, len(X))])
        ys = np.stack([X[t] for t in range(self.tau, len(X))])
        A_sparse = self._sparsify(self._graphs_batch(windows, ys), self._H)
        S, Sc, St = self._deviation_components(A_sparse)
        dev = np.abs(A_sparse - self._A_norm) / (self._A_norm + self.eps)
        return S, Sc, St, dev

    # ---------- fit / detect ----------

    def fit(self, history: np.ndarray) -> None:
        _require_torch()
        history = np.asarray(history, dtype=np.float32)
        if history.ndim == 1:
            history = history.reshape(-1, 1)
        n_time, n_seq = history.shape
        self._n_seq = n_seq
        # 数据不足：静默不训练（engine 据此不落盘、下轮重试），不 raise
        if n_seq == 0 or n_time < self.tau + 2:
            self.fitted = False
            return
        if self.target_index is None and n_seq == 1:
            self.target_index = 0
        torch.set_num_threads(_MAX_THREADS)
        torch.manual_seed(self.seed)

        # 逐通道标准化（fit/detect 共用，存 checkpoint）
        mu = history.mean(axis=0).astype(np.float32)
        std = history.std(axis=0).astype(np.float32) + 1e-8
        X = (history - mu) / std

        # 滑窗样本：x=X[t-tau:t]，y=X[t]（窗口后的下一行）。
        # 按 val_frac 分割：训练段训 TSMixer + 估计正常图；验证段（模型未见过）定阈值。
        # 论文用 80/20 train/validation 分割——阈值来自验证分数，避免模型对训练窗口
        # 过拟合导致阈值偏低、正常实时窗口误报偏高。
        n_train = int(n_time * (1 - self.val_frac))
        t0v = max(self.tau, n_train)
        # 内部最小窗口数（engine 的 minimum_points 门槛之上的兜底）：训练样本太少
        # 学不出有意义的因果图 → 静默不训练（engine 据此不落盘、数据增长后自动重训）。
        # 必须放在建窗口之前：n_train <= tau 时 np.stack 空列表会抛 ValueError。
        if n_train - self.tau < max(8, 4 * self.tau):
            self.fitted = False
            return
        train_windows = np.stack(
            [X[t - self.tau:t] for t in range(self.tau, n_train)])
        train_ys = np.stack([X[t] for t in range(self.tau, n_train)])
        has_val = n_time > t0v
        val_windows = (np.stack([X[t - self.tau:t] for t in range(t0v, n_time)])
                       if has_val else None)
        val_ys = (np.stack([X[t] for t in range(t0v, n_time)])
                  if has_val else None)
        nwin = len(train_windows)
        if nwin > self.max_train_windows:
            rng = np.random.RandomState(self.seed)
            idx = rng.choice(nwin, self.max_train_windows, replace=False)
            train_windows, train_ys = train_windows[idx], train_ys[idx]
            nwin = len(train_windows)

        # 训练 TSMixer（MSE），墙钟预算内自停
        model = _TSMixer(n_seq, self.tau, self.hidden_dim, self.num_layers)
        opt = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
        wt = torch.tensor(train_windows)
        yt = torch.tensor(train_ys)
        t0 = time.monotonic()
        model.train()
        for _ in range(1, self.epochs + 1):
            if time.monotonic() - t0 > self.train_budget_s:
                break
            perm = torch.randperm(nwin)
            for start in range(0, nwin, self.batch_size):
                idx = perm[start:start + self.batch_size]
                xb, yb = wt[idx], yt[idx]
                opt.zero_grad()
                pred = vmap(model)(xb)                     # [bs, N]
                loss = ((pred - yb) ** 2).mean()
                loss.backward()
                opt.step()
                if time.monotonic() - t0 > self.train_budget_s:
                    break
        model.eval()
        self._model = model
        self._norm_mean, self._norm_std = mu, std

        # 估计正常因果图：Bernoulli(p) 采样（可复现）→ 批量因果图 → 稀疏化均值
        rng = np.random.RandomState(self.seed)
        n_samp = min(self.n_norm_samples, nwin)
        mask = rng.random(nwin) < self.p
        if int(mask.sum()) < max(4, self.tau):            # 兜底：p 采样太少则取前 n_samp
            mask = np.zeros(nwin, dtype=bool)
            mask[:n_samp] = True
        idx_norm = np.nonzero(mask)[0][:n_samp]
        A_raw = self._graphs_batch(train_windows[idx_norm],
                                   train_ys[idx_norm])                # [n,N,N]

        # 稀疏阈值 h：h>0 用绝对值；否则用正常窗口原始 A 非对角分位自适应。
        # （对称消除后约一半非对角是 0，对它取分位会退化回 0 → 不稀疏化；
        #   原始 A 全正，分位才是"保留 top 边"的有效阈值。）
        h = self.h
        if h <= 0 and self.h_quantile > 0:
            off = np.abs(A_raw[:, ~np.eye(n_seq, dtype=bool)] if n_seq > 1
                         else A_raw[:, 0:1, 0:1])
            if off.size:
                h = float(np.quantile(off, self.h_quantile))
        self._H = self._build_H(n_seq, h)
        self._A_norm = self._sparsify(A_raw, self._H).mean(axis=0)    # [N,N]

        # 检测阈值：优先验证段（模型未见过的数据）的正常分数分布；验证段不足则退回
        # 训练段中【与 Ā_norm 集不相交】的窗口。
        if val_windows is not None and len(val_windows) >= max(4, self.tau):
            if len(val_windows) > n_samp:                # 打分窗口数上限，控成本
                v_idx = rng.choice(len(val_windows), n_samp, replace=False)
                val_windows, val_ys = val_windows[v_idx], val_ys[v_idx]
            A_score = self._sparsify(
                self._graphs_batch(val_windows, val_ys), self._H)
        else:
            other = np.setdiff1d(np.arange(nwin), idx_norm)
            n_score = min(n_samp, len(other))
            idx_score = (other[:n_score]
                         if n_score >= max(4, self.tau) else idx_norm[:n_score])
            A_score = self._sparsify(
                self._graphs_batch(train_windows[idx_score],
                                   train_ys[idx_score]), self._H)
        S_score = self._deviation_scores(A_score)
        self._threshold = float(np.quantile(S_score, self.score_quantile))
        self.fitted = True

    def detect(self, window: np.ndarray) -> list[dict]:
        _require_torch()
        findings = []
        if not self.fitted or self._model is None:
            return findings
        window = np.asarray(window, dtype=np.float32)
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        n_time, n_seq = window.shape
        if n_time < self.tau or n_seq != self._n_seq:
            return findings
        X = (window - self._norm_mean) / self._norm_std
        windows = np.stack([X[t - self.tau:t] for t in range(self.tau, n_time)])
        ys = np.stack([X[t] for t in range(self.tau, n_time)])
        A_raw = self._graphs_batch(windows, ys)                       # [B,N,N]
        A_sparse = self._sparsify(A_raw, self._H)
        S, Sc, St = self._deviation_components(A_sparse)
        dev = np.abs(A_sparse - self._A_norm) / (self._A_norm + self.eps)
        for k in range(len(S)):
            if S[k] > self._threshold:
                t = k + self.tau
                i, j = np.unravel_index(np.argmax(dev[k]), dev[k].shape)
                findings.append({
                    "anomaly_type": "CAUSAL_PATTERN",
                    "severity": "MEDIUM",
                    "description": (f"点{t} 因果图偏离 Sc={Sc[k]:.2f} St={St[k]:.2f}"
                                    f"（最大偏离边 列{i}→列{j}）"),
                    "score": float(S[k]),
                    "index": t,
                })
        return findings

    # ---------- 持久化 ----------

    def _ckpt(self) -> dict:
        """构造 checkpoint dict（save 落盘 / MutualCoupling 嵌套复用）。"""
        _require_torch()
        return {
            "model_type": self.model_type,
            "n_seq": self._n_seq,
            "tau": self.tau,
            "hidden_dim": self.hidden_dim,
            "num_layers": self.num_layers,
            "state_dict": self._model.state_dict(),
            "norm_mean": self._norm_mean,
            "norm_std": self._norm_std,
            "A_norm": self._A_norm,
            "H": self._H,
            "threshold": self._threshold,
            "target_index": self.target_index,
            "beta": self.beta,
            "eps": self.eps,
        }

    def save(self, path) -> None:
        if self._model is None or not self.fitted:
            raise RuntimeError("还没训练，先调用 fit()")
        torch.save(self._ckpt(), path)

    def load_dict(self, ckpt: dict) -> None:
        _require_torch()
        self._n_seq = int(ckpt["n_seq"])
        self.tau = int(ckpt["tau"])
        self.hidden_dim = int(ckpt["hidden_dim"])
        self.num_layers = int(ckpt["num_layers"])
        self._model = _TSMixer(self._n_seq, self.tau,
                               self.hidden_dim, self.num_layers)
        self._model.load_state_dict(ckpt["state_dict"])
        self._model.eval()
        self._norm_mean = np.asarray(ckpt["norm_mean"], dtype=np.float32)
        self._norm_std = np.asarray(ckpt["norm_std"], dtype=np.float32)
        # A_norm 保持 fit 时的 float32（_graphs_batch 均值），别升到 float64：
        # 否则 loaded 与 fitted 的偏离分数在 1e-5 级有差异，save/load 往返不位一致。
        self._A_norm = np.asarray(ckpt["A_norm"], dtype=np.float32)
        self._H = np.asarray(ckpt["H"], dtype=np.float64)
        self._threshold = float(ckpt["threshold"])
        self.target_index = ckpt.get("target_index")
        self.beta = float(ckpt.get("beta", 1.0))
        self.eps = float(ckpt.get("eps", 1e-6))
        self.fitted = True


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
    """双变量互耦异常：每对 2 通道深度 GCAD + 方向因果偏离（论文式全图）。

    互耦 = 两变量互相影响（A 的过去→B 当前，B 的过去→A 当前，如温度↔压力反馈）。
    每对取 history[:,[a,b]] 两列，用深度 GCAD（target_index=None，纯论文 2×2 全图）
    学正常因果图 Ā_pair，检测因果图方向边偏离：
      - dev_ab = |Ã[a,b]-Ā[a,b]|/(Ā[a,b]+ε)   （A 不再驱动 B）
      - dev_ba = |Ã[b,a]-Ā[b,a]|/(Ā[b,a]+ε)   （B 不再驱动 A）
    方向阈值 = 训练/验证段方向偏离的 score_quantile 分位。任一方向超阈 → MUTUAL_COUPLING，
    标注耦合方向（服务归因诊断：单向断裂时只有对应方向检出，区分"A 不再驱动 B"还是
    "B 不再驱动 A"）。
    """

    model_type = "mutual-coupling"

    def __init__(
        self,
        coupled_pairs: list[tuple[int, int]] | None = None,
        # ---- 深度 GCAD 超参（复用 config anomaly.gcad）----
        tau: int = 8,
        hidden_dim: int = 64,
        num_layers: int = 2,
        epochs: int = 20,
        batch_size: int = 64,
        learning_rate: float = 1e-3,
        p: float = 0.5,
        n_norm_samples: int = 300,
        h: float = 0.0,
        h_quantile: float = 0.3,
        beta: float = 1.0,
        score_quantile: float = 0.95,
        eps: float = 1e-6,
        max_train_windows: int = 5000,
        train_budget_s: float = 240.0,
        val_frac: float = 0.2,
        seed: int = 0,
    ):
        self.pairs = [tuple(sorted((a, b))) for a, b in (coupled_pairs or [])]
        self.tau = tau
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.epochs = epochs
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.p = p
        self.n_norm_samples = n_norm_samples
        self.h = h
        self.h_quantile = h_quantile
        self.beta = beta
        self.score_quantile = score_quantile
        self.eps = eps
        self.max_train_windows = max_train_windows
        self.train_budget_s = train_budget_s
        self.val_frac = val_frac
        self.seed = seed
        self._deep: dict[tuple[int, int], GcadAnomalyModel] = {}
        self.fitted: bool = False

    def _fit_pair(self, history: np.ndarray, a: int, b: int) -> None:
        # 每对取两列 → 深度 GCAD（target_index=None，纯论文 2×2 全图）。
        # deep.fit 内部已做 80/20 验证切分：阈值来自模型未见过的验证段，避免过拟合。
        sub = np.asarray(history[:, [a, b]], dtype=np.float32)
        deep = GcadAnomalyModel(
            target_index=None,
            tau=self.tau, hidden_dim=self.hidden_dim, num_layers=self.num_layers,
            epochs=self.epochs, batch_size=self.batch_size,
            learning_rate=self.learning_rate, p=self.p,
            n_norm_samples=self.n_norm_samples, h=self.h,
            h_quantile=self.h_quantile, beta=self.beta,
            score_quantile=self.score_quantile, eps=self.eps,
            max_train_windows=self.max_train_windows,
            train_budget_s=self.train_budget_s, val_frac=self.val_frac,
            seed=self.seed,
        )
        deep.fit(sub)
        if not deep.fitted:
            return  # 数据不足 → 该对跳过（engine 据此不落盘整体）
        self._deep[(a, b)] = deep

    def fit(self, history: np.ndarray) -> None:
        history = np.asarray(history, dtype=np.float32)
        self._deep = {}
        for a, b in self.pairs:
            if a < history.shape[1] and b < history.shape[1]:
                self._fit_pair(history, a, b)
        self.fitted = bool(self._deep)

    def detect(self, window: np.ndarray) -> list[dict]:
        findings = []
        if not self.fitted:
            return findings
        window = np.asarray(window, dtype=np.float32)
        if window.ndim == 1:
            window = window.reshape(-1, 1)
        n_time, n_seq = window.shape
        for (a, b), deep in self._deep.items():
            if max(a, b) >= n_seq:
                continue
            sub = np.asarray(window[:, [a, b]], dtype=np.float32)
            X = (sub - deep._norm_mean) / deep._norm_std
            if len(X) < deep.tau:
                continue
            # 门控 = 全图因果偏离分数 S（验证段阈值，稳健）；方向 = dev_ab vs dev_ba 相对归因
            # （单边 dev 对近零边病态——Ā≈0 时 |Ã-Ā|/(Ā+ε) 爆炸，不能做绝对阈值）。
            S, Sc, St, dev = deep._deviation_on(X)     # dev[B,2,2]
            th = deep._threshold
            for k in range(len(S)):
                if S[k] <= th:
                    continue
                t = k + deep.tau
                dab, dba = dev[k, 0, 1], dev[k, 1, 0]
                if dab >= dba:
                    direction = f"列{a}→列{b} 偏离更大（dev_ab={dab:.2f} vs dev_ba={dba:.2f}）"
                else:
                    direction = f"列{b}→列{a} 偏离更大（dev_ba={dba:.2f} vs dev_ab={dab:.2f}）"
                findings.append({
                    "anomaly_type": "MUTUAL_COUPLING", "severity": "MEDIUM",
                    "description": (f"点{t} 耦合方向偏离：{direction}"
                                    f"；因果图 Sc={Sc[k]:.2f} St={St[k]:.2f}"),
                    "score": float(S[k]), "index": t,
                })
        return findings

    # ---------- 持久化 ----------

    def save(self, path) -> None:
        if not self.fitted or not self._deep:
            raise RuntimeError("还没训练，先调用 fit()")
        torch.save({
            "model_type": self.model_type,
            "pairs": [list(p) for p in self._deep],
            "deep": [self._deep[p]._ckpt() for p in self._deep],
        }, path)

    def load_dict(self, ckpt: dict) -> None:
        _require_torch()
        self._deep = {}
        for pair, dc in zip(ckpt["pairs"], ckpt["deep"]):
            deep = GcadAnomalyModel(target_index=None)
            deep.load_dict(dc)
            self._deep[tuple(pair)] = deep
        self.fitted = bool(self._deep)


# 模型工厂：按方法名创建模型（便于后续替换/扩展）
def build_anomaly_model(method: str, **kwargs) -> AnomalyModel | None:
    """根据方法名返回模型实例；未知方法返回 None。"""
    if method not in KNOWN_METHODS:
        return None
    if method == "DISCRETE_OUTLIER":
        return DbscanAnomalyModel(eps=kwargs.get("eps", 0.5),
                                  min_samples=kwargs.get("min_samples", 5))
    if method == "CAUSAL_PATTERN":
        gcad = dict(kwargs.get("gcad") or {})
        # 先验（engine 透传，契约不变）优先于 config
        return GcadAnomalyModel(
            **gcad,
            target_index=kwargs.get("target_index"),
            source_indices=kwargs.get("source_indices"),
            correlation_prior=kwargs.get("correlation_prior"),
            relations_prior=kwargs.get("relations_prior"),
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
        # MUTUAL_COUPLING 不需要先验（target_index=None），corr_threshold 过滤掉
        gcad = dict(kwargs.get("gcad") or {})
        gcad.pop("corr_threshold", None)
        return MutualCouplingModel(
            coupled_pairs=kwargs.get("coupled_pairs"),
            **gcad,
        )
    if method == "HISTORICAL_MATCH":
        # 历史语义匹配（框架）：懒导入避免循环依赖；索引由确认事件增量构建，
        # 匹配核心为占位逻辑（见 historical_matcher.HistoricalEventMatcher）。
        from historical_matcher import HistoricalEventMatcher
        return HistoricalEventMatcher(
            sequence_ids=kwargs.get("sequence_ids"),
            min_deviation_z=kwargs.get("min_deviation_z", 2.0),
            top_k=kwargs.get("top_k", 3),
        )
    return None
