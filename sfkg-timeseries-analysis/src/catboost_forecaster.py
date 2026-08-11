"""因变量类离散序列的条件期望预测（CatBoost）+ 自变量类离散的保持当前值预测。

技术方案 [54][56]：三种数据类型三种预测法——连续→PatchTST（已有）；
自变量类离散→计划值否则保持当前值；因变量类离散→CatBoost（输入=连续序列预测值 +
自变量离散预测值，输出条件期望值）。

接口与 PatchTSTForecaster 对齐：fit(history_matrix) / forecast(window_matrix, steps) /
save(path) / load(path)，engine 按需构建，ModelStore 统一内存缓存/磁盘持久化。
"""

from __future__ import annotations

import logging
import os
import pickle
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import torch

from patchtst_forecaster import PatchTSTForecaster

logger = logging.getLogger(__name__)

# 80 核机器上线程过订阅会卡死（见 anomaly_models.py），CatBoost 训练同样限线程。
_TRAIN_THREADS = int(os.environ.get("SFKG_MAX_THREADS", "4"))

try:
    from catboost import CatBoostRegressor as _CatBoostRegressor
except ImportError:  # pragma: no cover - 依赖缺失时模块可导入，构造时才报错
    _CatBoostRegressor = None


class UnsupportedTargetError(Exception):
    """目标/特征为标签类离散（字符串）时抛出，engine 转 NOT_IMPLEMENTED。"""


# ---------- CatBoost 序列化：pickle 主、.cbm 临时文件兜底（带格式标签） ----------

# model 是 catboost.CatBoostRegressor 实例；catboost 为惰性导入（可能缺失），
# 静态类型无法引用其真实类，用 Any。
def _catboost_to_bytes(model: Any) -> tuple[str, bytes]:
    """序列化 CatBoost 模型。catboost 1.x 的 save_model 只保证路径入参，
    BytesIO 未文档化；pickle 优先（现代版本 CatBoostRegressor 可 pickle），
    失败则落到 .cbm 临时文件读字节。"""
    try:
        return "pickle", pickle.dumps(model)
    except Exception:
        with tempfile.NamedTemporaryFile(suffix=".cbm", delete=False) as f:
            tmp = f.name
        try:
            model.save_model(tmp)          # 格式由 .cbm 后缀推断
            with open(tmp, "rb") as f:
                return "cbm", f.read()
        finally:
            os.unlink(tmp)


def _catboost_from_bytes(data: bytes, fmt: str) -> Any:
    if fmt == "cbm":
        with tempfile.NamedTemporaryFile(suffix=".cbm", delete=False) as f:
            tmp = f.name
        try:
            with open(tmp, "wb") as f:
                f.write(data)
            m = _CatBoostRegressor()
            m.load_model(tmp)
            return m
        finally:
            os.unlink(tmp)
    return pickle.loads(data)


@dataclass
class CatBoostForecaster:
    """因变量类离散序列预测器。

    内部两级模型：
      1. 连续列 → 内部 PatchTST（供连续特征预测值）；
      2. CatBoostRegressor：X=当前工况（连续列值 + 自变量离散列值），y=因变量值。
    推理时 =（PatchTST 连续预测 + 自变量离散保持当前值）逐步喂 CatBoost 得条件期望。

    column_kinds: {sequence_id: "continuous"|"discrete"}，由 engine 从历史值类型推断；
    磁盘加载时可省略（load_dict 从 checkpoint 恢复 continuous_ids / discrete_indep_ids）。
    """

    sequence_ids: list[str]
    target_sequence_id: str
    column_kinds: dict[str, str] | None = None
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
    catboost_params: dict | None = None
    model_type: str = "catboost"

    def __post_init__(self) -> None:
        if _CatBoostRegressor is None:
            raise ImportError(
                "CatBoostForecaster 需要 catboost。先安装：pip install 'catboost>=1.2'")
        if self.target_sequence_id not in self.sequence_ids:
            raise ValueError(
                f"target_sequence_id {self.target_sequence_id} 不在 sequence_ids")
        self._model = None
        self._patchtst = None
        self.target_idx = self.sequence_ids.index(self.target_sequence_id)
        if self.column_kinds:
            self._continuous_ids = [s for s in self.sequence_ids
                                    if self.column_kinds.get(s) == "continuous"]
            self._disc_indep_ids = [s for s in self.sequence_ids
                                    if self.column_kinds.get(s) == "discrete"
                                    and s != self.target_sequence_id]
        else:
            self._continuous_ids = []
            self._disc_indep_ids = []

    # ================= 训练 =================

    def fit(self, history_matrix: np.ndarray) -> None:
        """history_matrix: [T, C] 历史（行=时间，列=sequence_ids，engine 已 _clean_matrix）。

        连续列喂内部 PatchTST（连续特征来源）；CatBoost 训练集 = 每行当前工况
        （连续列值 + 自变量离散列值）→ 目标列值。因变量取值与历史结果无关（[56]），
        只用当前工况，无 lag。
        """
        Xm = np.asarray(history_matrix, dtype=np.float32)
        if Xm.ndim != 2:
            raise ValueError(f"history_matrix 应为 [T, C]，实际 {Xm.shape}")
        if Xm.shape[1] != len(self.sequence_ids):
            raise ValueError(f"列数 {Xm.shape[1]} != sequence_ids 数量 {len(self.sequence_ids)}")

        # ① 连续列 → 内部 PatchTST
        if self._continuous_ids:
            cont_idx = [self.sequence_ids.index(s) for s in self._continuous_ids]
            self._patchtst = PatchTSTForecaster(
                sequence_ids=self._continuous_ids,
                context_length=self.context_length,
                prediction_length=self.prediction_length,
                patch_size=self.patch_size,
                patch_stride=self.patch_stride,
                d_model=self.d_model,
                n_heads=self.n_heads,
                num_layers=self.num_layers,
                epochs=self.epochs,
                batch_size=self.batch_size,
                learning_rate=self.learning_rate,
            )
            self._patchtst.fit(Xm[:, cont_idx])

        # ② CatBoost 训练集：X=当前工况（连续列+自变量离散列），y=目标列
        feat_ids = self._continuous_ids + self._disc_indep_ids
        if not feat_ids:
            raise ValueError("因变量离散任务缺少特征列（连续或自变量离散），无法训练 CatBoost")
        feat_idx = [self.sequence_ids.index(s) for s in feat_ids]
        X = Xm[:, feat_idx]
        y = Xm[:, self.target_idx]
        keep = ~np.isnan(y)          # 兜底：丢弃目标缺失行
        X, y = X[keep], y[keep]
        if len(X) == 0:
            raise ValueError("因变量离散序列无可训练样本（目标列全缺失）")
        params = dict(self.catboost_params or {})
        params.setdefault("loss_function", "RMSE")
        params.setdefault("iterations", 300)
        params.setdefault("learning_rate", 0.05)
        params.setdefault("depth", 6)
        params.setdefault("l2_leaf_reg", 3.0)
        params.setdefault("thread_count", _TRAIN_THREADS)
        params.setdefault("verbose", False)
        params.setdefault("allow_writing_files", False)   # 不落 catboost_info/ 训练日志
        self._model = _CatBoostRegressor(**params)
        self._model.fit(X, y)
        logger.info("[catboost] fit: %d 样本 × %d 特征（连续 %d，离散自变量 %d）",
                    len(X), X.shape[1], len(self._continuous_ids),
                    len(self._disc_indep_ids))

    # ================= 推理 =================

    def forecast(self, window_matrix: np.ndarray,
                 steps: int | None = None) -> dict[str, list[float]]:
        """window_matrix: [context, C] 最近对齐窗口（engine 已 _clean_matrix）。

        连续列 PatchTST 预测 → 自变量离散列保持窗口末值 → 逐步组特征喂 CatBoost
        得因变量条件期望。返回 {target_sequence_id: [steps 个值]}。
        """
        if self._model is None:
            raise RuntimeError("还没训练，先调用 fit()")
        if steps is None:
            steps = self.prediction_length
        Xw = np.asarray(window_matrix, dtype=np.float32)
        if Xw.ndim != 2:
            raise ValueError(f"window_matrix 应为 [context, C]，实际 {Xw.shape}")

        # ① 连续列预测
        cont_pred: dict[str, list[float]] = {}
        if self._continuous_ids and self._patchtst is not None:
            cont_idx = [self.sequence_ids.index(s) for s in self._continuous_ids]
            cont_pred = self._patchtst.forecast(Xw[:, cont_idx], steps=steps)

        # ② 自变量离散列：保持当前值（窗口末有效值）
        disc_const: dict[str, float] = {}
        for sid in self._disc_indep_ids:
            col = Xw[:, self.sequence_ids.index(sid)]
            valid = col[~np.isnan(col)]
            disc_const[sid] = float(valid[-1]) if len(valid) else 0.0

        # ③ 逐步组特征 [steps, n_feat]（永远二维，避免 CatBoost predict 单行歧义）
        feat_ids = self._continuous_ids + self._disc_indep_ids
        Xs = np.zeros((steps, len(feat_ids)), dtype=np.float32)
        for t in range(steps):
            for j, sid in enumerate(feat_ids):
                if sid in self._continuous_ids:
                    p = cont_pred.get(sid) or [0.0] * steps
                    Xs[t, j] = p[t] if t < len(p) else 0.0
                else:
                    Xs[t, j] = disc_const[sid]
        pred = np.atleast_1d(np.asarray(self._model.predict(Xs), dtype=float))
        if len(pred) > steps:
            pred = pred[:steps]
        return {self.target_sequence_id: [float(v) for v in pred]}

    # ================= 持久化 =================

    def save(self, path: str | Path) -> None:
        if self._model is None:
            raise RuntimeError("还没训练，先调用 fit()")
        fmt, blob = _catboost_to_bytes(self._model)
        torch.save({
            "model_type": self.model_type,
            "catboost_format": fmt,
            "catboost_bytes": blob,
            "sequence_ids": self.sequence_ids,
            "target_sequence_id": self.target_sequence_id,
            "continuous_ids": self._continuous_ids,
            "discrete_indep_ids": self._disc_indep_ids,
            "context_length": self.context_length,
            "prediction_length": self.prediction_length,
            "internal_patchtst": self._patchtst.to_dict() if self._patchtst else None,
        }, path)

    def load_dict(self, ckpt: dict) -> None:
        """从 torch.load 出的 dict 恢复（engine loader 用；构造时可不给 column_kinds）。"""
        self.sequence_ids = ckpt["sequence_ids"]
        self.target_sequence_id = ckpt["target_sequence_id"]
        self._continuous_ids = list(ckpt["continuous_ids"])
        self._disc_indep_ids = list(ckpt["discrete_indep_ids"])
        self.context_length = ckpt["context_length"]
        self.prediction_length = ckpt["prediction_length"]
        self.target_idx = self.sequence_ids.index(self.target_sequence_id)
        self._model = _catboost_from_bytes(ckpt["catboost_bytes"],
                                           ckpt.get("catboost_format"))
        d = ckpt.get("internal_patchtst")
        self._patchtst = PatchTSTForecaster.from_dict(d) if d else None

    def load(self, path: str | Path) -> None:
        self.load_dict(torch.load(path, map_location="cpu"))


@dataclass
class ConstantForecaster:
    """自变量类离散序列预测器（[54]）：无特征 → 预测 = 保持当前值。

    知识库计划值接口未来接入；现在预测 = 窗口末值（窗口没值兜底 fit 时的末值）。
    """

    sequence_ids: list[str]
    target_sequence_id: str | None = None
    model_type: str = "constant"

    def __post_init__(self) -> None:
        self._last_value: float | None = None

    def fit(self, history_matrix: np.ndarray) -> None:
        m = np.asarray(history_matrix, dtype=np.float32)
        if (self.target_sequence_id is not None
                and self.target_sequence_id in self.sequence_ids):
            col = m[:, self.sequence_ids.index(self.target_sequence_id)]
            valid = col[~np.isnan(col)]
            if len(valid):
                self._last_value = float(valid[-1])

    def forecast(self, window_matrix: np.ndarray,
                 steps: int | None = None) -> dict[str, list[float]]:
        m = np.asarray(window_matrix, dtype=np.float32)
        last = self._last_value if self._last_value is not None else 0.0
        if (self.target_sequence_id is not None
                and self.target_sequence_id in self.sequence_ids):
            col = m[:, self.sequence_ids.index(self.target_sequence_id)]
            valid = col[~np.isnan(col)]
            if len(valid):
                last = float(valid[-1])
        n = steps if steps is not None else 24
        return {self.target_sequence_id: [last] * n}

    def save(self, path: str | Path) -> None:
        torch.save({"model_type": self.model_type,
                    "sequence_ids": self.sequence_ids,
                    "target_sequence_id": self.target_sequence_id,
                    "last_value": self._last_value}, path)

    def load_dict(self, ckpt: dict) -> None:
        self.sequence_ids = ckpt["sequence_ids"]
        self.target_sequence_id = ckpt.get("target_sequence_id")
        self._last_value = ckpt.get("last_value")

    def load(self, path: str | Path) -> None:
        self.load_dict(torch.load(path, map_location="cpu"))
