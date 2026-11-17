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


def _sma3(arr: np.ndarray) -> np.ndarray:
    """论文 simple_moving_average（window_size=3）：中心对齐三点均值，头尾各丢 1 窗。

    官方实现返回 (moving_averages, label[1:-1])：smoothed[m] 对应原窗口 m+1
    （窗口分数序列的长度从 n 变 n-2）。
    """
    return np.convolve(arr, np.ones(3) / 3.0, mode="valid")


class _RevIN(torch.nn.Module):
    """RevIN（Kim et al. 2022，官方仓库 models/common.py 原样移植）：实例归一化 +
    可学习仿射。norm 时记窗口统计（detach，不参与梯度），denorm 时还原
    （先逆仿射再乘回统计量，affine 权重分母加 eps² 是官方原式）。"""

    def __init__(self, num_features: int, eps: float = 1e-5,
                 affine: bool = True):
        super().__init__()
        self.num_features = num_features
        self.eps = eps
        self.affine = affine
        if self.affine:
            self.affine_weight = torch.nn.Parameter(torch.ones(num_features))
            self.affine_bias = torch.nn.Parameter(torch.zeros(num_features))

    def forward(self, x, mode: str, target_slice=None):
        if mode == "norm":
            self._get_statistics(x)
            x = self._normalize(x)
        elif mode == "denorm":
            x = self._denormalize(x, target_slice)
        else:
            raise NotImplementedError
        return x

    def _get_statistics(self, x):
        dim2reduce = tuple(range(1, x.ndim - 1))
        self.mean = torch.mean(x, dim=dim2reduce, keepdim=True).detach()
        self.stdev = torch.sqrt(
            torch.var(x, dim=dim2reduce, keepdim=True, unbiased=False)
            + self.eps).detach()

    def _normalize(self, x):
        x = x - self.mean
        x = x / self.stdev
        if self.affine:
            x = x * self.affine_weight
            x = x + self.affine_bias
        return x

    def _denormalize(self, x, target_slice=None):
        if self.affine:
            x = x - self.affine_bias[target_slice]
            x = x / (self.affine_weight + self.eps * self.eps)[target_slice]
        x = x * self.stdev[:, :, target_slice]
        x = x + self.mean[:, :, target_slice]
        return x


class _ResBlock(torch.nn.Module):
    """TSMixer 残差块（官方仓库 models/common.py 原样移植）：时间线性（BatchNorm
    展平归一，官方原样）+ 特征线性（ff_dim 隐维）。全部 out-of-place
    （vmap/jacrev 对原地修改会报错/算错）；无 Dropout（论文 dropout=0）。"""

    def __init__(self, input_shape, dropout, ff_dim):
        super().__init__()
        self.norm1 = torch.nn.BatchNorm1d(input_shape[0] * input_shape[1])
        self.linear1 = torch.nn.Linear(input_shape[0], input_shape[0])
        self.dropout1 = torch.nn.Dropout(dropout)
        self.norm2 = torch.nn.BatchNorm1d(input_shape[0] * input_shape[1])
        self.linear2 = torch.nn.Linear(input_shape[-1], ff_dim)
        self.dropout2 = torch.nn.Dropout(dropout)
        self.linear3 = torch.nn.Linear(ff_dim, input_shape[-1])
        self.dropout3 = torch.nn.Dropout(dropout)

    def forward(self, x):
        inputs = x
        x = self.norm1(torch.flatten(x, 1, -1)).reshape(x.shape)
        x = torch.transpose(x, 1, 2)
        x = torch.nn.functional.relu(self.linear1(x))
        x = torch.transpose(x, 1, 2)
        x = self.dropout1(x)
        res = x + inputs
        x = self.norm2(torch.flatten(res, 1, -1)).reshape(res.shape)
        x = torch.nn.functional.relu(self.linear2(x))
        x = self.dropout2(x)
        x = self.linear3(x)
        x = self.dropout3(x)
        return x + res


class _TSMixerRevIN(torch.nn.Module):
    """TSMixerRevIN 预测器（官方仓库 models/tsmixer.py 原样移植，GCAD AAAI 2025）：
    RevIN 实例归一 → n_block 个 ResBlock → Linear(seq_len→pred_len) → denorm。
    输入 [B, tau, N] → 输出 [B, pred_len, N]（pred_len=1：窗口 → 全通道下一时刻）。"""

    def __init__(self, n_seq: int, tau: int, pred_len: int = 1,
                 n_block: int = 6, dropout: float = 0.0, ff_dim: int = 1024,
                 target_slice=None):
        super().__init__()
        self.target_slice = (slice(None) if target_slice is None
                             else target_slice)
        self.rev_norm = _RevIN(n_seq)
        self.res_blocks = torch.nn.ModuleList(
            [_ResBlock((tau, n_seq), dropout, ff_dim) for _ in range(n_block)])
        self.linear = torch.nn.Linear(tau, pred_len)

    def forward(self, x):
        x = self.rev_norm(x, "norm")
        for res_block in self.res_blocks:
            x = res_block(x)
        x = x[:, :, self.target_slice]
        x = torch.transpose(x, 1, 2)
        x = self.linear(x)
        x = torch.transpose(x, 1, 2)
        x = self.rev_norm(x, "denorm", self.target_slice)
        return x


class GcadAnomalyModel(AnomalyModel):
    """深度 GCAD（AAAI 2025，arXiv 2501.13493）异常检测：TSMixerRevIN 预测器 +
    梯度格兰杰因果图。**2026-10-30 起为论文官方链路原样实现**（此前是缩小简化版，
    与合同验收数字不一致，已按用户"评测方法 = 工厂实际方法"原则重写）：
    架构（RevIN+ResBlock×n_block）、训练协议（80/20 切分、早停、drop_last 批）、
    打分链（对称稀疏化 sparse_th、相对偏差 mean+eps、SMA-3 平滑）逐环节对应官方仓库
    Tc99m/GCAD；构造默认值 = 官方 run.sh SMAP 配置 = 合同验收达标配置
    （SMAP D-3 点级验收已用工厂代码复验，见 eval_workspace/results 指标卡）。

    fit：TSMixerRevIN 用滑动窗口 X∈R^{B×τ×N} 预测全通道下一时刻 ŷ∈R^N（MSE 训练，
        验证段早停回滚最优）；逐通道损失 L_j=(ŷ_j-y_j)²，反传得 ∂L_j/∂X，因果强度
        a_{i,j}=2|ŷ_j-y_j|·mean_φ|∂L_j/∂x_{φ,i}|（官方 batch 反传逐窗等价，工厂用
        jacrev 逐窗同式计算），构成因果矩阵 A∈R^{N×N}；Bernoulli(p) 采样训练窗口
        平均得正常因果图 Ā_norm；验证段分数 SMA-3 平滑后取分位作阈值。
    detect：实时窗口算因果图 → 对称稀疏化 → 因果偏差打分
        S = Sc + β·St（Sc=mean|Ã-Ā|/(Ā+ε)，论文链 β=0；St=对角偏离为工厂扩展项）
        → SMA-3 平滑 → 超阈值 → 模式偏移。

    先验（技术方案 [42]，作边稀疏门，论文链全关）：
      - relations_prior（语义关系图：谁影响因变量）→ 该边阈值降为 h/4 优先保留
      - correlation_prior（C 端相关性矩阵）+ corr_threshold → |corr|<阈值 的边清零
    target_index=None（MUTUAL_COUPLING 用）→ 无先验纯论文全图。

    持久化：save(path)（torch.save dict + model_type="gcad"）/ load_dict(ckpt)，engine
    _register_model_loader 按 model_type 分发，重启不重训。旧版（缩小配置）checkpoint
    无 n_block 字段 → load_dict 抛错，ModelStore 捕获后删缓存重训（training_loop）。
    """

    model_type = "gcad"

    def __init__(
        self,
        # ---- 深度 GCAD 超参（默认 = 论文官方 run.sh SMAP 配置 = 合同验收配置）----
        tau: int = 70,                # 最大因果滞后（滑窗长度）= 论文 seq_len
        n_block: int = 6,             # TSMixerRevIN 残差块数
        ff_dim: int = 1024,           # 特征 MLP 隐维
        dropout: float = 0.0,
        pred_len: int = 1,            # 预测步（论文固定 1）
        epochs: int = 100,            # 论文 train_epochs（验证段早停，D-3 实测数轮即停）
        batch_size: int = 128,
        learning_rate: float = 1e-4,
        patience: int = 2,            # 验证损失连续不降 patience 轮即停、回滚最优
        p: float = 0.2,               # 正常图采样 Bernoulli 概率（论文 sample_p）
        n_norm_samples: int = 0,      # 正常图采样窗口上限；0 = 不限（论文无上限）
        h: float = 0.008,             # 显式稀疏阈值（论文 sparse_th）；0 → 自适应
        h_quantile: float = 0.3,      # h≤0 时自适应：正常窗口 |A| 非对角分位
        beta: float = 0.0,            # 论文打分链无时间偏离项（S=Sc；官方 pd_beta 未入链）
        score_quantile: float = 0.95, # 检测阈值 = 验证段 SMA-3 平滑后分数分位
        eps: float = 1e-4,            # 偏离分母（论文 standard+1e-4）
        sma_window: int = 3,          # 论文 SMA-3 分数平滑；0/1 = 关闭
        max_train_windows: int = 0,   # 训练窗口上限；0 = 不限（论文无截断）
        train_budget_s: float = 0.0,  # fit 墙钟预算（秒）；0 = 关闭（论文训练须完整跑完）
        val_frac: float = 0.2,        # 80/20 切分：训练段训模型+正常图，验证段定阈值
        seed: int = 0,
        device: str | None = None,    # None = 自动（有 GPU 用 GPU；论文协议官方跑 GPU）
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
        self.n_block = int(n_block)
        self.ff_dim = int(ff_dim)
        self.dropout = float(dropout)
        self.pred_len = int(pred_len)
        self.epochs = int(epochs)
        self.batch_size = int(batch_size)
        self.learning_rate = float(learning_rate)
        self.patience = int(patience)
        self.p = float(p)
        self.n_norm_samples = int(n_norm_samples)
        self.h = float(h)
        self.h_quantile = float(h_quantile)
        self.beta = float(beta)
        self.score_quantile = float(score_quantile)
        self.eps = float(eps)
        self.sma_window = int(sma_window)
        self.max_train_windows = int(max_train_windows)
        self.train_budget_s = float(train_budget_s)
        self.val_frac = float(val_frac)
        self.seed = int(seed)
        self.device = device
        self._device = (torch.device(device) if device else
                        (torch.device("cuda") if torch.cuda.is_available()
                         else torch.device("cpu")))
        self.corr_threshold = float(corr_threshold)
        self.target_index = target_index
        self.source_indices = list(source_indices) if source_indices else None
        self.correlation_prior = dict(correlation_prior) if correlation_prior else {}
        self.relations_prior = dict(relations_prior) if relations_prior else {}
        self._n_seq = 0
        self._model: _TSMixerRevIN | None = None
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

    def _forward_one(self, x: torch.Tensor) -> torch.Tensor:
        """单窗口前向：x [tau,N] → ŷ [N]（论文 pred_len=1 全通道下一时刻）。

        BatchNorm1d(tau·N) 展平要求输入带批维，单窗口 unsqueeze 成 [1,tau,N]
        再前向。官方批量前向与逐窗口前向数值一致（BN eval 用 running stats、
        RevIN 统计逐窗 detach），jacrev 才能逐窗口取雅可比。
        """
        return self._model(x.unsqueeze(0)).reshape(-1)

    def _graphs_batch(self, windows: np.ndarray,
                      ys: np.ndarray) -> np.ndarray:
        """批量算原始因果强度矩阵 A[B,N,N]（论文式 5，与官方实现逐窗等价）。

        windows: [B, tau, N]，ys: [B, N]（窗口**之后**的下一时刻真值）。
        y 必须是窗口下一行：a_{i,j}=2|ŷ_j-y_j|·mean_φ|∂ŷ_j/∂x_{φ,i}|（官方
        torch.mean(grad_causal_mat, dim=1) 对滞后维取均值；其 batch 反传
        ∂(Σ_b L_j)/∂x 逐样本可分解、与逐窗 jacrev 同式，工厂用 vmap(jacrev)
        逐窗算同一量）。J [B,N,tau,N]（∂ŷ_j/∂x_{φ,i}），mean(dim=2) 聚合 φ → S[j,i]。
        """
        dev = self._device
        outs = []
        with torch.no_grad():
            for start in range(0, len(windows), self._chunk):
                chunk_w = torch.tensor(windows[start:start + self._chunk],
                                       device=dev)
                chunk_y = torch.tensor(ys[start:start + self._chunk],
                                       device=dev)
                yh = vmap(self._forward_one)(chunk_w)        # [B,N]
                J = vmap(jacrev(self._forward_one))(chunk_w) # [B,N,tau,N]
                s = J.abs().mean(dim=2)                      # [B,N,N] S[j,i]
                factor = 2.0 * (yh - chunk_y).abs()          # [B,N] 残差因子按输出通道 j
                # A[i,j] = factor[j]·S[j,i]：因子须沿输出维 j（dim1）广播，
                # unsqueeze(2) → [B,N,1] 对齐 j；transpose 得 A[B,i,j]。
                outs.append((s * factor.unsqueeze(2)).transpose(1, 2))
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
        """论文式 10-12：返回 (S, Sc, St)，S=Sc+β·St（全 N×N 图）。

        Sc=mean(|Ã-Ā|/(Ā+ε)) 与官方 err_score 同式（官方 standard=Ā+1e-4，逐元素
        相对偏差后 torch.mean）；β=0（论文链）时 S=Sc。St（对角偏离）是工厂扩展项。
        """
        eps = self.eps
        An = self._A_norm
        sc = np.abs(A_sparse - An) / (An + eps)          # [B,N,N]
        Sc = sc.mean(axis=(1, 2))
        diag_norm = np.diag(An)
        diag_diff = np.abs(np.diagonal(A_sparse, axis1=1, axis2=2) - diag_norm)
        St = (diag_diff / (diag_norm + eps)).mean(axis=1)
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

        # 80/20 切分（论文协议，官方 loader 同构）：训练段标定标准化参数——
        # 官方 StandardScaler 只 fit 训练段，验证/测试共用（fit/detect 共用，存
        # checkpoint）。
        n_train = int(n_time * (1 - self.val_frac))
        # 内部最小窗口数（engine 的 minimum_points 门槛之上的兜底）：训练样本太少
        # 学不出有意义的因果图 → 静默不训练（engine 据此不落盘、数据增长后自动重训）。
        # 必须放在建窗口之前：n_train <= tau 时 np.stack 空列表会抛 ValueError。
        if n_train - self.tau < max(8, 4 * self.tau):
            self.fitted = False
            return
        mu = history[:n_train].mean(axis=0).astype(np.float32)
        std = history[:n_train].std(axis=0).astype(np.float32) + 1e-8
        X = (history - mu) / std

        # 滑窗样本：x=X[t-tau:t]，y=X[t]（窗口后的下一行），stride 1 完整窗。
        # 训练段训模型 + 估计正常图；验证段（模型未见过）定阈值——阈值来自验证
        # 分数，避免模型对训练窗口过拟合导致阈值偏低、正常实时窗口误报偏高。
        train_windows = np.stack(
            [X[t - self.tau:t] for t in range(self.tau, n_train)])
        train_ys = np.stack([X[t] for t in range(self.tau, n_train)])
        has_val = n_time > n_train
        val_windows = (np.stack([X[t - self.tau:t] for t in range(n_train, n_time)])
                       if has_val else None)
        val_ys = (np.stack([X[t] for t in range(n_train, n_time)])
                  if has_val else None)
        nwin = len(train_windows)
        rng = np.random.RandomState(self.seed)
        # 生产护栏：训练窗口上限（0 = 论文无截断）。D-3 验收训练窗 2234 < 上限，
        # 验收协议不受影响；生产 10 万点历史靠它把训练时长封在可控范围。
        if self.max_train_windows > 0 and nwin > self.max_train_windows:
            idx = rng.choice(nwin, self.max_train_windows, replace=False)
            train_windows, train_ys = train_windows[idx], train_ys[idx]
            nwin = len(train_windows)

        # 训练 TSMixerRevIN（论文协议：MSE、每 epoch 洗牌、batch 整批 drop_last、
        # 验证段早停 patience 轮、回滚最优 checkpoint；train_budget_s=0 关闭预算，
        # 论文训练必须完整跑完——早停已兜底，D-3 实测数轮即停）。
        model = _TSMixerRevIN(n_seq, self.tau, self.pred_len, self.n_block,
                              self.dropout, self.ff_dim).to(self._device)
        opt = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
        wt = torch.tensor(train_windows, device=self._device)
        yt = torch.tensor(train_ys, device=self._device)
        if val_windows is not None and len(val_windows):
            vt = torch.tensor(val_windows, device=self._device)
            vy = torch.tensor(val_ys, device=self._device)
        n_full = nwin // self.batch_size            # drop_last：洗牌后整批之外的余数不参与
        n_val_full = (len(val_windows) // self.batch_size) if has_val else 0
        best_loss = float("inf")
        best_state = None
        patience_left = self.patience
        self._epochs_run = 0                        # 观测：实际训练轮数（早停/预算截断后）
        t0 = time.monotonic()
        for epoch in range(1, self.epochs + 1):
            if self.train_budget_s > 0 and time.monotonic() - t0 > self.train_budget_s:
                break
            model.train()
            perm = torch.randperm(nwin, device=self._device)
            for start in range(0, n_full * self.batch_size, self.batch_size):
                idx = perm[start:start + self.batch_size]
                opt.zero_grad()
                loss = ((model(wt[idx]) - yt[idx]) ** 2).mean()
                loss.backward()
                opt.step()
                if self.train_budget_s > 0 and time.monotonic() - t0 > self.train_budget_s:
                    break
            # 验证（官方：不洗牌、drop_last、逐批均值等权平均；无整批时退回全窗口均值）
            model.eval()
            with torch.no_grad():
                if n_val_full > 0:
                    val_mloss = 0.0
                    for i, start in enumerate(
                            range(0, n_val_full * self.batch_size,
                                  self.batch_size)):
                        val_mloss = (val_mloss * i
                                     + ((model(vt[start:start + self.batch_size])
                                         - vy[start:start + self.batch_size]) ** 2)
                                     .mean().item()) / (i + 1)
                elif has_val and len(val_windows):
                    val_mloss = float(((model(vt) - vy) ** 2).mean().item())
                else:
                    val_mloss = float("inf")
            if val_mloss < best_loss:
                best_loss = val_mloss
                best_state = {k: v.detach().clone()
                              for k, v in model.state_dict().items()}
                patience_left = self.patience
            else:
                patience_left -= 1
            self._epochs_run = epoch
            if patience_left <= 0 or epoch >= self.epochs:
                break
        if best_state is not None:
            model.load_state_dict(best_state)
        model.eval()
        self._model = model
        self._norm_mean, self._norm_std = mu, std

        # 估计正常因果图：Bernoulli(p) 采样 → 批量因果图 → 稀疏化均值。
        # 官方 test.py save_train_mean_causal：训练 DataLoader（RandomSampler 洗牌后
        # 序列）按批（batch_size 连续窗，drop_last 尾部块弃）逐批 random.random()≤p
        # 整块采样、首块强制；工厂固定 seed 复刻同分布语义（洗牌 + 批级 + 首块
        # 强制 + 尾部块弃），生产可复现。数据不足一个批（官方 drop_last 下为 0 批、
        # 无采样窗口）退回逐窗采样兜底。
        perm = rng.permutation(nwin)
        n_blocks = nwin // self.batch_size
        if n_blocks == 0:
            draw = rng.random(nwin) < self.p
            idx_norm = perm[np.nonzero(draw)[0]]
        else:
            draw = rng.random(n_blocks) < self.p
            draw[0] = True                               # 官方 first_tag 首批判入
            mask = np.zeros(nwin, dtype=bool)
            mask[:n_blocks * self.batch_size] = np.repeat(draw, self.batch_size)
            idx_norm = perm[np.nonzero(mask)[0]]
        if self.n_norm_samples > 0 and len(idx_norm) > self.n_norm_samples:
            idx_norm = idx_norm[:self.n_norm_samples]
        if len(idx_norm) < max(4, self.tau):            # 兜底：p 采样太少则取前 n 窗
            idx_norm = np.arange(min(nwin, max(4, self.tau)))
        A_raw = self._graphs_batch(train_windows[idx_norm],
                                   train_ys[idx_norm])                # [n,N,N]

        # 稀疏阈值 h：论文 = 固定绝对值 sparse_th（h>0）；h≤0 时用正常窗口原始 A
        # 非对角分位自适应（工厂扩展，小数据/合成数据用）。
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
        # 训练段中【与 Ā_norm 集不相交】的窗口。验证段打分窗口数按
        # max_train_windows 封顶（生产护栏，D-3 验证窗 576 < 上限不受影响）。
        if val_windows is not None and len(val_windows) >= max(4, self.tau):
            if self.max_train_windows > 0 \
                    and len(val_windows) > self.max_train_windows:
                v_idx = rng.choice(len(val_windows), self.max_train_windows,
                                   replace=False)
                val_windows, val_ys = val_windows[v_idx], val_ys[v_idx]
            A_score = self._sparsify(
                self._graphs_batch(val_windows, val_ys), self._H)
        else:
            other = np.setdiff1d(np.arange(nwin), idx_norm)
            idx_score = (other if len(other) >= max(4, self.tau) else idx_norm)
            A_score = self._sparsify(
                self._graphs_batch(train_windows[idx_score],
                                   train_ys[idx_score]), self._H)
        S_score = self._deviation_scores(A_score)
        # 论文 SMA-3：分数序列三点平滑后再取阈值（官方 simple_moving_average 同构，
        # 中心对齐、头尾各丢 1 窗）。
        S_th = _sma3(S_score) if self.sma_window > 1 else S_score
        self._threshold = float(np.quantile(S_th, self.score_quantile))
        self._S_score = S_th    # 阈值分序列（评测扫描复用：分位阈值曲线）
        self._val_S = S_score   # 验证段原始窗口分数（评测复验/调阈复用）
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
        # 严格大于 tau：滑窗 range(tau, n_time) 至少产出 1 个窗口；n_time == tau 时
        # np.stack([]) 会抛 ValueError（边界 off-by-one）。
        if n_time <= self.tau or n_seq != self._n_seq:
            return findings
        X = (window - self._norm_mean) / self._norm_std
        windows = np.stack([X[t - self.tau:t] for t in range(self.tau, n_time)])
        ys = np.stack([X[t] for t in range(self.tau, n_time)])
        A_raw = self._graphs_batch(windows, ys)                       # [B,N,N]
        A_sparse = self._sparsify(A_raw, self._H)
        S, Sc, St = self._deviation_components(A_sparse)
        dev = np.abs(A_sparse - self._A_norm) / (self._A_norm + self.eps)
        # 论文 SMA-3：smoothed[m]=mean(S[m:m+3]) ↔ 窗口 m+1 ↔ 点 m+1+tau（官方
        # simple_moving_average 同构：中心对齐、头尾各丢 1 窗）。
        scored = ([(m + 1, s) for m, s in enumerate(_sma3(S))]
                  if self.sma_window > 1 and len(S) >= self.sma_window
                  else [(k, S[k]) for k in range(len(S))])
        for k, s in scored:
            if s > self._threshold:
                t = k + self.tau
                i, j = np.unravel_index(np.argmax(dev[k]), dev[k].shape)
                findings.append({
                    "anomaly_type": "CAUSAL_PATTERN",
                    "severity": "MEDIUM",
                    "description": (f"点{t} 因果图偏离 Sc={Sc[k]:.2f} St={St[k]:.2f}"
                                    f"（最大偏离边 列{i}→列{j}）"),
                    "score": float(s),
                    "index": t,
                })
        return findings

    def score_segment(self, X: np.ndarray) -> np.ndarray:
        """对原始数据段逐窗口打分（工厂公共接口）：返回原始因果偏离 S（未平滑）。

        与 detect 同一链路（标准化→滑窗→稀疏因果图→偏差），供评测复验/生产调阈：
        论文协议里验证段分位阈值与测试段逐点分数都从这条链出。
        X: [time, features] 原始（未标准化）数据；返回 [time-tau] 窗口分数
        （窗口 t ↔ 预测点 t+tau）。
        """
        if not self.fitted or self._model is None:
            raise RuntimeError("还没训练，先调用 fit()")
        X = np.asarray(X, dtype=np.float32)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        if X.shape[1] != self._n_seq:
            raise ValueError(f"列数不符：模型 {self._n_seq} 列，输入 {X.shape[1]} 列")
        if len(X) <= self.tau:
            return np.zeros(0, dtype=np.float64)
        Z = (X - self._norm_mean) / self._norm_std
        windows = np.stack([Z[t - self.tau:t] for t in range(self.tau, len(Z))])
        ys = np.stack([Z[t] for t in range(self.tau, len(Z))])
        return self._deviation_scores(
            self._sparsify(self._graphs_batch(windows, ys), self._H))

    # ---------- 持久化 ----------

    def _ckpt(self) -> dict:
        """构造 checkpoint dict（save 落盘 / MutualCoupling 嵌套复用）。"""
        _require_torch()
        return {
            "model_type": self.model_type,
            "n_seq": self._n_seq,
            "tau": self.tau,
            "pred_len": self.pred_len,
            "n_block": self.n_block,
            "ff_dim": self.ff_dim,
            "dropout": self.dropout,
            "state_dict": self._model.state_dict(),
            "norm_mean": self._norm_mean,
            "norm_std": self._norm_std,
            "A_norm": self._A_norm,
            "H": self._H,
            "threshold": self._threshold,
            "target_index": self.target_index,
            "beta": self.beta,
            "eps": self.eps,
            "sma_window": self.sma_window,
            "h": self.h,
            "h_quantile": self.h_quantile,
        }

    def save(self, path) -> None:
        if self._model is None or not self.fitted:
            raise RuntimeError("还没训练，先调用 fit()")
        torch.save(self._ckpt(), path)

    def load_dict(self, ckpt: dict) -> None:
        _require_torch()
        if "n_block" not in ckpt:
            # 旧版（缩小配置 _TSMixer）checkpoint 与论文架构不兼容；ModelStore 捕获
            # 本异常后删缓存重训（training_loop.ModelStore.get），不因坏 .pt 整轮崩。
            raise RuntimeError(
                "旧版 GCAD checkpoint（缩小配置）与论文架构不兼容，请删除缓存重训")
        self._n_seq = int(ckpt["n_seq"])
        self.tau = int(ckpt["tau"])
        self.pred_len = int(ckpt.get("pred_len", 1))
        self.n_block = int(ckpt["n_block"])
        self.ff_dim = int(ckpt["ff_dim"])
        self.dropout = float(ckpt.get("dropout", 0.0))
        self._model = _TSMixerRevIN(self._n_seq, self.tau, self.pred_len,
                                    self.n_block, self.dropout,
                                    self.ff_dim).to(self._device)
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
        self.beta = float(ckpt.get("beta", 0.0))
        self.eps = float(ckpt.get("eps", 1e-4))
        self.sma_window = int(ckpt.get("sma_window", 3))
        self.h = float(ckpt.get("h", 0.008))
        self.h_quantile = float(ckpt.get("h_quantile", 0.3))
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
        # ---- 深度 GCAD 超参（复用 config anomaly.gcad，默认 = 论文 SMAP 配置）----
        tau: int = 70,
        n_block: int = 6,
        ff_dim: int = 1024,
        dropout: float = 0.0,
        pred_len: int = 1,
        epochs: int = 100,
        batch_size: int = 128,
        learning_rate: float = 1e-4,
        patience: int = 2,
        p: float = 0.2,
        n_norm_samples: int = 0,
        h: float = 0.008,
        h_quantile: float = 0.3,
        beta: float = 0.0,
        score_quantile: float = 0.95,
        eps: float = 1e-4,
        sma_window: int = 3,
        max_train_windows: int = 0,
        train_budget_s: float = 0.0,
        val_frac: float = 0.2,
        seed: int = 0,
        device: str | None = None,
    ):
        self.pairs = [tuple(sorted((a, b))) for a, b in (coupled_pairs or [])]
        # 防御：与 GcadAnomalyModel 一致，config 透传的数值统一按类型强转，杜绝
        # PyYAML 把科学计数法（1e-6）解析成字符串后的 dtype 崩溃。
        self.tau = int(tau)
        self.n_block = int(n_block)
        self.ff_dim = int(ff_dim)
        self.dropout = float(dropout)
        self.pred_len = int(pred_len)
        self.epochs = int(epochs)
        self.batch_size = int(batch_size)
        self.learning_rate = float(learning_rate)
        self.patience = int(patience)
        self.p = float(p)
        self.n_norm_samples = int(n_norm_samples)
        self.h = float(h)
        self.h_quantile = float(h_quantile)
        self.beta = float(beta)
        self.score_quantile = float(score_quantile)
        self.eps = float(eps)
        self.sma_window = int(sma_window)
        self.max_train_windows = int(max_train_windows)
        self.train_budget_s = float(train_budget_s)
        self.val_frac = float(val_frac)
        self.seed = int(seed)
        self.device = device
        self._deep: dict[tuple[int, int], GcadAnomalyModel] = {}
        self.fitted: bool = False

    def _fit_pair(self, history: np.ndarray, a: int, b: int) -> None:
        # 每对取两列 → 深度 GCAD（target_index=None，纯论文 2×2 全图）。
        # deep.fit 内部已做 80/20 验证切分：阈值来自模型未见过的验证段，避免过拟合。
        sub = np.asarray(history[:, [a, b]], dtype=np.float32)
        deep = GcadAnomalyModel(
            target_index=None,
            tau=self.tau, n_block=self.n_block, ff_dim=self.ff_dim,
            dropout=self.dropout, pred_len=self.pred_len,
            epochs=self.epochs, batch_size=self.batch_size,
            learning_rate=self.learning_rate, patience=self.patience,
            p=self.p, n_norm_samples=self.n_norm_samples, h=self.h,
            h_quantile=self.h_quantile, beta=self.beta,
            score_quantile=self.score_quantile, eps=self.eps,
            sma_window=self.sma_window,
            max_train_windows=self.max_train_windows,
            train_budget_s=self.train_budget_s, val_frac=self.val_frac,
            seed=self.seed, device=self.device,
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
            # 严格大于 tau（同 GcadAnomalyModel.detect：len == tau 时 _deviation_on 的
            # np.stack 空列表会崩）。
            if len(X) <= deep.tau:
                continue
            # 门控 = 全图因果偏离分数 S（验证段阈值，稳健；SMA-3 同 CAUSAL_PATTERN
            # 对齐——阈值来自平滑后验证分数，detect 侧同样平滑再比）；
            # 方向 = dev_ab vs dev_ba 相对归因（单边 dev 对近零边病态——Ā≈0 时
            # |Ã-Ā|/(Ā+ε) 爆炸，不能做绝对阈值）。
            S, Sc, St, dev = deep._deviation_on(X)     # dev[B,2,2]
            th = deep._threshold
            scored = ([(m + 1, s) for m, s in enumerate(_sma3(S))]
                      if deep.sma_window > 1 and len(S) >= deep.sma_window
                      else [(k, S[k]) for k in range(len(S))])
            for k, s in scored:
                if s <= th:
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
                    "score": float(s), "index": t,
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
