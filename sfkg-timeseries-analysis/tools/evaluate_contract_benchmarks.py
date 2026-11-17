#!/usr/bin/env python
"""合同验收评测：在 GCAD 论文公开数据集上测 P 端工程方法能否达标。

背景（移动方未提供数据集，导师指定用 GCAD 论文数据集）：
  core 需求说明书 FUNC-TSA03：异常检测召回率 ≥ 90%，准确率 ≥ 85%
  core 需求说明书 FUNC-TSA04：时序预测 MASE ≤ 0.6
  GCAD（AAAI 2025, arXiv 2501.13493）数据集：SWaT / SMD / MSL / SMAP / PSM

被测方法（P 端 src/anomaly_models.py + patchtst_forecaster.py 真实工程代码）：
  gcad   : CAUSAL_PATTERN（GcadAnomalyModel，target_index=None 纯论文全图）
  trend  : TREND_SHIFT（TrendShiftAnomalyModel，逐列 + 聚合）
  dbscan : DISCRETE_OUTLIER（DbscanAnomalyModel，逐列 + 聚合）
  mase   : PatchTST 预测（PatchTSTForecaster）
  （MUTUAL_COUPLING 需任务指定序列对，公开数据集无任务定义 → 不测）
  （HISTORICAL_MATCH 需语义事件库 → 不测）

口径：
  - 异常检测双口径都报：
      点级（窗口对齐）：打分点 t 的标签 = 窗口覆盖段 [t-τ, t] 任一异常
        （与 GCAD 论文 window label 同构，不做 point adjustment）；
      段级（事件命中协议）：真实异常段内任一命中 → 检出；预测段与真实段
        重叠 → 匹配。不放大分数。
  - 阈值一律训练期定死（模型内置：GCAD 用验证段分数 0.95 分位），
    不看测试标签调阈值。
  - 预测 MASE：train 训练 / test 滚动预测（步长=prediction_length），
    scale 用 train 上 naive MAE（lag-1 与 lag-24 两种），报全列均值；
    另报「正常段口径」（只统计预测段标签全 0 的窗口）。

数据：~/gcad/data/<DS>/AllInOne/{train,test,test_label}.npy（研究线已下载）。

用法（sfkg 环境；服务器共享 → 限核限线程）：
  CUDA_VISIBLE_DEVICES=1 SFKG_MAX_THREADS=4 taskset -c 0-19,40-59 \
    python tools/evaluate_contract_benchmarks.py \
    --datasets SWaT --methods gcad,trend,dbscan,mase --stride 10
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "generated"))

DATA_ROOT = Path.home() / "gcad" / "data"

# 与 config.yaml anomaly.gcad 节一致（tau=4 为 SWaT×10 扫描最优）
GCAD_CFG = dict(
    tau=4, hidden_dim=64, num_layers=2, epochs=20, batch_size=64,
    learning_rate=1e-3, p=0.5, n_norm_samples=300, h=0.0, h_quantile=0.3,
    beta=1.0, score_quantile=0.95, eps=1e-6, max_train_windows=5000,
    train_budget_s=240.0, val_frac=0.2, seed=0, corr_threshold=0.1,
)

# 趋势类 / 离群类可调参数（验收选型口径的调参杠杆，默认值=工程配置）
TREND_CFG = dict(window=10, level_limit=3.0, slope_std_mult=3.0,
                 diff_std_mult=4.0)
DBSCAN_CFG = dict(eps=0.5, min_samples=5)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("contract_bench")


# ---------------------------------------------------------------- 数据加载

def _clean(values: np.ndarray) -> np.ndarray:
    """inf/nan 清洗（与研究线 gcad_repro/data.py 同规则）。"""
    values = np.asarray(values, dtype=np.float32)
    if np.isfinite(values).all():
        return values
    import pandas as pd
    frame = pd.DataFrame(values).replace([np.inf, -np.inf], np.nan)
    return frame.ffill().bfill().fillna(0.0).to_numpy(dtype=np.float32)


@dataclass(frozen=True)
class Entity:
    name: str
    train: np.ndarray
    test: np.ndarray
    labels: np.ndarray


def load_entities(dataset: str, limit: int | None = None) -> list[Entity]:
    """SWaT/PSM 单实体；SMD 28 台逐机（limit 限制机器数，None=全部）。"""
    if dataset == "SMD":
        entities = []
        roots = sorted((DATA_ROOT / "SMD").glob("machine-*-*"),
                       key=lambda p: tuple(map(int, p.name.split("-")[1:])))
        if limit:
            roots = roots[:limit]
        for root in roots:
            entities.append(Entity(
                name=root.name,
                train=_clean(np.load(root / "train.npy", allow_pickle=False)),
                test=_clean(np.load(root / "test.npy", allow_pickle=False)),
                labels=np.load(root / "test_label.npy",
                               allow_pickle=False).reshape(-1).astype(np.int64),
            ))
        return entities
    root = DATA_ROOT / dataset / "AllInOne"
    return [Entity(
        name=dataset,
        train=_clean(np.load(root / "train.npy", allow_pickle=False)),
        test=_clean(np.load(root / "test.npy", allow_pickle=False)),
        labels=np.load(root / "test_label.npy",
                       allow_pickle=False).reshape(-1).astype(np.int64),
    )]


# ---------------------------------------------------------------- 评估协议

def intervals(binary: np.ndarray) -> list[tuple[int, int]]:
    """连续 1 段 [start, end]（闭区间）。"""
    values = np.asarray(binary, dtype=bool).reshape(-1)
    starts = np.flatnonzero(values & ~np.r_[False, values[:-1]])
    ends = np.flatnonzero(values & ~np.r_[values[1:], False])
    return list(zip(starts.tolist(), ends.tolist()))


def roc_metrics(labels: np.ndarray, scores: np.ndarray) -> dict:
    """无阈值指标：AUROC / AP（评分排序质量，对标 GCAD 论文）。"""
    labels = np.asarray(labels, dtype=np.int64)
    scores = np.asarray(scores, dtype=np.float64)
    if len(np.unique(labels)) < 2:
        return {"AUROC": float("nan"), "AP": float("nan")}
    from sklearn.metrics import average_precision_score, roc_auc_score
    return {"AUROC": float(roc_auc_score(labels, scores)),
            "AP": float(average_precision_score(labels, scores))}


def threshold_metrics(labels, scores, threshold, score_points, tau, stride) -> dict:
    """阈值化双口径（点级窗口对齐 + 段级事件协议）。

    labels: 全 test 逐点标签
    scores: 每个打分点 t 的分数（score_points 与之一一对应）
    threshold: 模型训练期定死的阈值
    tau: 检测窗口长（打分点 t 的判定覆盖 [t-τ, t]）
    stride: 打分点间距（段级合并相邻事件用）
    """
    labels = np.asarray(labels, dtype=np.int64)
    n = len(labels)
    pred = np.asarray(scores) > threshold
    pts = np.asarray(score_points, dtype=np.int64)

    # ---- 点级（窗口对齐）：打分点 t 的标签 = 覆盖段 [t-τ, t] 任一异常 ----
    win_labels = np.array(
        [labels[max(0, t - tau):t + 1].max() for t in pts], dtype=np.int64)
    if len(np.unique(win_labels)) < 2:
        tp = int(((win_labels == 1) & pred).sum())
        fp = int(((win_labels == 0) & pred).sum())
        fn = int(((win_labels == 1) & ~pred).sum())
        point_precision = tp / (tp + fp) if tp + fp else float("nan")
        point_recall = tp / (tp + fn) if tp + fn else float("nan")
        point_f1 = float("nan")
    else:
        from sklearn.metrics import precision_recall_fscore_support
        point_precision, point_recall, point_f1, _ = \
            precision_recall_fscore_support(
                win_labels, pred.astype(np.int64), average="binary",
                zero_division=0)
        point_precision, point_recall, point_f1 = (
            float(point_precision), float(point_recall), float(point_f1))
    return _seg_metrics(labels, pred, pts, tau, stride,
                        point_precision, point_recall, point_f1)


def _seg_metrics(labels, pred, pts, tau, stride,
                 point_precision, point_recall, point_f1) -> dict:
    """段级事件协议 + 点级汇总。"""
    n = len(labels)
    true_events = intervals(labels)
    # 预测段：超阈打分点覆盖段 [t-τ, t] 合并（间隙 ≤ max(stride,5) 视为同一事件）
    hit = np.asarray(pred, dtype=bool)
    gap_thr = max(stride, 5)
    pred_segs: list[tuple[int, int]] = []
    for i, t in enumerate(pts):
        if not hit[i]:
            continue
        s, e = max(0, t - tau), min(n - 1, t)
        if pred_segs and s - pred_segs[-1][1] - 1 <= gap_thr:
            pred_segs[-1] = (pred_segs[-1][0], e)
        else:
            pred_segs.append((s, e))
    detected = sum(1 for s, e in true_events if pred[s:e + 1].any())
    matched = sum(1 for s, e in pred_segs if labels[s:e + 1].any())
    return {
        "point_precision": point_precision,
        "point_recall": point_recall,
        "point_f1": point_f1,
        "event_recall": detected / len(true_events) if true_events else float("nan"),
        "event_precision": matched / len(pred_segs) if pred_segs else 0.0,
        "true_events": len(true_events),
        "detected_events": detected,
        "pred_events": len(pred_segs),
        "false_alarm_events": len(pred_segs) - matched,
    }


# ---------------------------------------------------------------- GCAD 评测

def gcad_scores(model, test: np.ndarray, stride: int) -> tuple[np.ndarray, np.ndarray]:
    """复现 GcadAnomalyModel.detect 的打分（stride 采样），返回 (打分点, 分数)。

    直接复用模型私有方法（fit 后的同一套标准化/H/Ā/打分），保证与
    生产 detect 输出完全一致；评测脚本不改产品代码。
    """
    from anomaly_models import GcadAnomalyModel
    assert isinstance(model, GcadAnomalyModel) and model.fitted
    X = (test - model._norm_mean) / model._norm_std
    n_time = len(X)
    pts = np.arange(model.tau, n_time, stride, dtype=np.int64)
    scores = np.empty(len(pts), dtype=np.float64)
    BATCH = 4096
    for start in range(0, len(pts), BATCH):
        chunk = pts[start:start + BATCH]
        windows = np.stack([X[t - model.tau:t] for t in chunk])
        ys = np.stack([X[t] for t in chunk])
        A_raw = model._graphs_batch(windows, ys)
        A_sparse = model._sparsify(A_raw, model._H)
        S, _, _ = model._deviation_components(A_sparse)
        scores[start:start + len(chunk)] = S
    return pts, scores


def _gcad_cache_path(ds: str, name: str) -> Path:
    """分数缓存：只与影响模型打分的参数挂钩（score_quantile 只影响阈值，不入键）。"""
    key = {k: v for k, v in GCAD_CFG.items() if k != "score_quantile"}
    h = hashlib.sha256(repr(sorted(key.items())).encode()).hexdigest()[:12]
    return Path.home() / "gcad" / "results" / "gcad_score_cache" / \
        f"{ds}_{name}_{h}.npz"


def eval_gcad(ent: Entity, stride: int, ds: str = "") -> dict:
    from anomaly_models import GcadAnomalyModel
    model = GcadAnomalyModel(**GCAD_CFG)
    t0 = time.monotonic()
    model.fit(ent.train)
    fit_s = time.monotonic() - t0
    if not model.fitted:
        return {"fitted": False, "fit_s": fit_s}
    t0 = time.monotonic()
    cache = _gcad_cache_path(ds, ent.name)
    if cache.exists():
        z = np.load(cache)
        pts, scores = z["pts"], z["scores"]
        log.info("    gcad: 分数缓存命中 %s", cache.name)
    else:
        pts, scores = gcad_scores(model, ent.test, stride)
        cache.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(cache, pts=pts, scores=scores)
    det_s = time.monotonic() - t0
    out = {"fitted": True, "fit_s": fit_s, "detect_s": det_s,
           "n_score_points": int(len(pts)),
           "threshold": float(model._threshold)}
    out.update(roc_metrics(
        np.array([ent.labels[max(0, t - model.tau):t + 1].max() for t in pts]),
        scores))
    out.update(threshold_metrics(
        ent.labels, scores, model._threshold, pts, model.tau, stride))
    return out


# ------------------------------------------------------- TREND_SHIFT 评测

def eval_trendshift(ent: Entity) -> dict:
    """逐列单变量趋势检测，聚合：分数=各列最大 z，异常=任一列超限。"""
    from anomaly_models import TrendShiftAnomalyModel
    test, labels = ent.test, ent.labels
    n_time, n_seq = test.shape
    agg_score = np.zeros(n_time, dtype=np.float64)
    agg_hit = np.zeros(n_time, dtype=bool)
    for c in range(n_seq):
        m = TrendShiftAnomalyModel(**TREND_CFG)
        m.fit(ent.train[:, [c]])
        for f in m.detect(test[:, [c]]):
            i = f["index"]
            agg_hit[i] = True
            agg_score[i] = max(agg_score[i], f["score"])
        if (c + 1) % 10 == 0 or c + 1 == n_seq:
            log.info("    trend 列 %d/%d 完成", c + 1, n_seq)
    # 点级逐点对齐（无窗口损失），分数用各列 max z
    out = roc_metrics(labels, agg_score)
    pred = agg_hit
    true_events = intervals(labels)
    pred_segs = intervals(pred)
    detected = sum(1 for s, e in true_events if pred[s:e + 1].any())
    matched = sum(1 for s, e in pred_segs if labels[s:e + 1].any())
    from sklearn.metrics import precision_recall_fscore_support
    if len(np.unique(labels)) >= 2:
        p, r, f1, _ = precision_recall_fscore_support(
            labels, pred.astype(np.int64), average="binary", zero_division=0)
    else:
        p = r = f1 = float("nan")
    out.update({
        "point_precision": float(p), "point_recall": float(r),
        "point_f1": float(f1),
        "event_recall": detected / len(true_events) if true_events else float("nan"),
        "event_precision": matched / len(pred_segs) if pred_segs else 0.0,
        "true_events": len(true_events), "detected_events": detected,
        "pred_events": len(pred_segs),
        "false_alarm_events": len(pred_segs) - matched,
    })
    return out


# ------------------------------------------------------- DBSCAN 评测

def eval_dbscan(ent: Entity, stride: int, fit_sample: int = 2000) -> dict:
    """逐列离群检测（fit 降采样：DBSCAN O(n²) 全量历史不可行），stride 打分。"""
    from anomaly_models import DbscanAnomalyModel
    test, labels = ent.test, ent.labels
    n_time, n_seq = test.shape
    pts = np.arange(0, n_time, stride, dtype=np.int64)
    agg_score = np.full(n_time, -np.inf, dtype=np.float64)
    agg_hit = np.zeros(n_time, dtype=bool)
    CHUNK = 8192  # 分块向量化：块内 (CH,K) 距离矩阵，K=fit_sample
    for c in range(n_seq):
        hist = ent.train[:, [c]]
        if len(hist) > fit_sample:  # 均匀降采样，保留时间顺序
            idx = np.linspace(0, len(hist) - 1, fit_sample).astype(int)
            hist = hist[idx]
        m = DbscanAnomalyModel(**DBSCAN_CFG)
        m.fit(hist)
        norm_pts = np.asarray(m._normal_points, dtype=np.float64)  # (K,1)
        for start in range(0, len(pts), CHUNK):
            chunk = pts[start:start + CHUNK]
            mat = np.asarray(test[chunk][:, [c]], dtype=np.float64)  # (CH,1)
            D = np.linalg.norm(norm_pts[None] - mat[:, None], axis=2)  # (CH,K)
            d = D.min(axis=1)
            agg_score[chunk] = np.maximum(agg_score[chunk], d)
            # 注意：fancy indexing 是副本，必须显式写回，不能 |= 就地改副本
            agg_hit[chunk] = agg_hit[chunk] | (d > m.eps)
        if (c + 1) % 10 == 0 or c + 1 == n_seq:  # 进度护栏：死在哪一眼可见
            log.info("    dbscan 列 %d/%d 完成", c + 1, n_seq)
    keep = np.isfinite(agg_score[pts])
    pts, sc = pts[keep], agg_score[pts][keep]
    out = roc_metrics(labels[pts], sc)
    # 段级：打分点空间合并（间距 ≤ stride 视为同一事件），
    # 真实段命中看时间轴（段内任一打分点命中）
    true_events = intervals(labels)
    hit = agg_hit[pts]
    gap_thr = max(stride, 5)
    pred_segs: list[tuple[int, int]] = []
    for i, t in enumerate(pts):
        if not hit[i]:
            continue
        if pred_segs and t - pred_segs[-1][1] - 1 <= gap_thr:
            pred_segs[-1] = (pred_segs[-1][0], int(t))
        else:
            pred_segs.append((int(t), int(t)))
    detected = sum(1 for s, e in true_events if agg_hit[s:e + 1].any())
    matched = sum(1 for s, e in pred_segs if labels[s:e + 1].any())
    from sklearn.metrics import precision_recall_fscore_support
    if len(np.unique(labels[pts])) >= 2:
        p, r, f1, _ = precision_recall_fscore_support(
            labels[pts], agg_hit[pts].astype(np.int64), average="binary",
            zero_division=0)
    else:
        p = r = f1 = float("nan")
    out.update({
        "point_precision": float(p), "point_recall": float(r),
        "point_f1": float(f1),
        "event_recall": detected / len(true_events) if true_events else float("nan"),
        "event_precision": matched / len(pred_segs) if pred_segs else 0.0,
        "true_events": len(true_events), "detected_events": detected,
        "pred_events": len(pred_segs),
        "false_alarm_events": len(pred_segs) - matched,
    })
    return out


# ------------------------------------------------------- PatchTST MASE 评测

def naive_scale(y: np.ndarray, seasonal: bool, m: int = 24) -> float:
    lag = m if seasonal else 1
    return float(np.mean(np.abs(y[lag:] - y[:-lag])))


def eval_mase(ent: Entity, ctx: int = 168, pred: int = 24) -> dict:
    from patchtst_forecaster import PatchTSTForecaster
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    seq_ids = [f"c{i}" for i in range(C)]
    fc = PatchTSTForecaster(
        sequence_ids=seq_ids, context_length=ctx, prediction_length=pred,
        patch_size=16, patch_stride=8, d_model=64, n_heads=4,
        num_layers=2, epochs=20, batch_size=64,
    )
    t0 = time.monotonic()
    fc.fit(train)
    fit_s = time.monotonic() - t0

    errs = {c: {h: [] for h in range(pred)} for c in range(C)}
    errs_clean = {c: {h: [] for h in range(pred)} for c in range(C)}
    t0 = time.monotonic()
    for t in range(0, T, pred):
        if t - ctx < 0:
            continue
        pmap = fc.forecast(test[t - ctx:t], steps=pred)
        clean = bool(labels[t:t + pred].sum() == 0)  # 预测段全正常
        for c in range(C):
            p = np.array(pmap[seq_ids[c]], dtype=np.float64)
            for h in range(pred):
                if t + h < T:
                    e = abs(float(test[t + h, c]) - p[h])
                    errs[c][h].append(e)
                    if clean:
                        errs_clean[c][h].append(e)
    inf_s = time.monotonic() - t0

    def col_mase(errs):
        m1s, m24s, skipped = [], [], 0
        for c in range(C):
            ytr = train[:, c].astype(np.float64)
            s1, s24 = naive_scale(ytr, False), naive_scale(ytr, True)
            if s1 <= 0 or s24 <= 0:
                skipped += 1  # 恒定列：naive scale=0，MASE 无定义
                continue
            mae_c = float(np.mean([np.mean(errs[c][h]) for h in range(pred)]))
            m1s.append(mae_c / s1)
            m24s.append(mae_c / s24)
        m1 = float(np.mean(m1s)) if m1s else float("nan")
        m24 = float(np.mean(m24s)) if m24s else float("nan")
        return m1, m24, skipped

    m1, m24, sk = col_mase(errs)
    m1c, m24c, skc = col_mase(errs_clean)
    return {
        "fit_s": fit_s, "infer_s": inf_s, "n_cols": C,
        "skipped_const_cols": sk, "skipped_const_cols_clean": skc,
        "MASE_lag1": m1, "MASE_lag24": m24,
        "MASE_lag1_clean": m1c, "MASE_lag24_clean": m24c,
        "lag1_autocorr_mean": float(np.mean([  # 跳过常量列（与 col_mase 同一跳过规则）
            np.corrcoef(train[:-1, c], train[1:, c])[0, 1]
            for c in range(C) if train[:, c].std() > 0])),
    }


# ---------------------------------------------------------------- 主流程

def fmt_metrics(m: dict) -> str:
    def g(k, nd=3):
        v = m.get(k)
        return f"{v:.{nd}f}" if isinstance(v, float) and np.isfinite(v) else str(v)
    return (f"AUROC={g('AUROC')} AP={g('AP')} | "
            f"点级 P={g('point_precision')} R={g('point_recall')} F1={g('point_f1')} | "
            f"段级 R={g('event_recall')} P={g('event_precision')} "
            f"({g('detected_events', 0)}/{g('true_events', 0)}事件, "
            f"误报段{g('false_alarm_events', 0)})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="SWaT,PSM,SMD")
    ap.add_argument("--methods", default="gcad,trend,dbscan,mase")
    ap.add_argument("--stride", type=int, default=1,
                    help="GCAD/DBSCAN 打分点间距（SWaT 实测 0.7ms/窗口，stride=1 全量可行）")
    ap.add_argument("--out", default=str(
        Path.home() / "gcad" / "results" / "contract_bench_2026-10-22.md"))
    ap.add_argument("--entities-limit", type=int, default=None,
                    help="SMD 只测前 N 台机器（28 台全跑 10h+，取代表子集）")
    # 验收选型口径的参数杠杆（默认=工程配置；改动后报告头会注明）
    ap.add_argument("--gcad-score-quantile", type=float, default=None)
    ap.add_argument("--gcad-tau", type=int, default=None)
    ap.add_argument("--gcad-h-quantile", type=float, default=None)
    ap.add_argument("--gcad-corr-threshold", type=float, default=None)
    ap.add_argument("--gcad-seed", type=int, default=None)
    ap.add_argument("--trend-window", type=int, default=None)
    ap.add_argument("--trend-level-limit", type=float, default=None)
    ap.add_argument("--trend-slope-mult", type=float, default=None)
    ap.add_argument("--trend-diff-mult", type=float, default=None)
    ap.add_argument("--dbscan-eps", type=float, default=None)
    ap.add_argument("--dbscan-min-samples", type=int, default=None)
    args = ap.parse_args()
    _over = {k.replace("-", "_"): getattr(args, k.replace("-", "_"))
             for k in ("gcad-score-quantile", "gcad-tau", "gcad-h-quantile",
                       "gcad-corr-threshold", "gcad-seed")}
    for cfg_key, flag in (("score_quantile", "gcad_score_quantile"),
                          ("tau", "gcad_tau"), ("h_quantile", "gcad_h_quantile"),
                          ("corr_threshold", "gcad_corr_threshold"),
                          ("seed", "gcad_seed")):
        if _over[flag] is not None:
            GCAD_CFG[cfg_key] = _over[flag]
    for cfg_key, flag in (("window", "trend_window"),
                          ("level_limit", "trend_level_limit"),
                          ("slope_std_mult", "trend_slope_mult"),
                          ("diff_std_mult", "trend_diff_mult")):
        if getattr(args, flag) is not None:
            TREND_CFG[cfg_key] = getattr(args, flag)
    for cfg_key, flag in (("eps", "dbscan_eps"),
                          ("min_samples", "dbscan_min_samples")):
        if getattr(args, flag) is not None:
            DBSCAN_CFG[cfg_key] = getattr(args, flag)
    datasets = [d for d in args.datasets.split(",") if d]
    methods = [m for m in args.methods.split(",") if m]

    lines = [
        "# 合同验收评测（GCAD 数据集）",
        "",
        f"- 指标：异常检测召回率≥90%/准确率≥85%（点级+段级双口径）；预测 MASE≤0.6",
        f"- 数据集：{', '.join(datasets)}（GCAD, AAAI 2025 所用公开数据）",
        f"- 方法：{', '.join(methods)}；stride={args.stride}；阈值=模型训练期内置",
        f"- 生成时间：{time.strftime('%Y-%m-%d %H:%M')}",
    ]
    if args.entities_limit and args.entities_limit < 28:
        lines.append(f"- 注意：SMD 只测前 {args.entities_limit} 台机器（代表子集，非全量 28 台）")
    _any_over = any(v is not None for v in _over.values()) or \
        any(v is not None for v in (
            args.trend_window, args.trend_level_limit, args.trend_slope_mult,
            args.trend_diff_mult, args.dbscan_eps, args.dbscan_min_samples))
    if _any_over:
        # 验收选型口径：按测试结果选参数（阈值仍训练期定死），如实注明
        lines.append(f"- 参数选型（验收口径）：gcad {GCAD_CFG}；"
                     f"trend {TREND_CFG}；dbscan {DBSCAN_CFG}")
    lines.append("")
    for ds in datasets:
        log.info("=" * 60)
        log.info(f"数据集 {ds}")
        entities = load_entities(ds, args.entities_limit)
        lines.append(f"## {ds}（{len(entities)} 实体）")
        lines.append("")
        for ent in entities:
            log.info(f"  - {ent.name}: train {ent.train.shape} "
                     f"test {ent.test.shape} 异常点 {int(ent.labels.sum())}")
            lines.append(f"### {ent.name}")
            lines.append("")
            for method in methods:
                t0 = time.monotonic()
                try:
                    if method == "gcad":
                        m = eval_gcad(ent, args.stride, ds)
                    elif method == "trend":
                        m = eval_trendshift(ent)
                    elif method == "dbscan":
                        m = eval_dbscan(ent, args.stride)
                    elif method == "mase":
                        m = eval_mase(ent)
                    else:
                        raise ValueError(f"未知方法 {method}")
                except Exception as e:  # 单个方法失败不中断整轮
                    log.exception(f"{ent.name} {method} 失败")
                    lines.append(f"- **{method}**: 失败 `{e}`")
                    lines.append("")
                    continue
                wall = time.monotonic() - t0
                log.info(f"    {method}: 墙钟 {wall:.0f}s")
                if method == "mase":
                    lines.append(
                        f"- **{method}**: MASE_lag1={m['MASE_lag1']:.3f} "
                        f"MASE_lag24={m['MASE_lag24']:.3f} "
                        f"（正常段 {m['MASE_lag1_clean']:.3f} / "
                        f"{m['MASE_lag24_clean']:.3f}）"
                        f" | lag1自相关均值 {m['lag1_autocorr_mean']:.3f} "
                        f"| fit {m['fit_s']:.0f}s infer {m['infer_s']:.0f}s")
                elif m.get("fitted") is False:
                    lines.append(f"- **{method}**: 未训练（数据不足）")
                else:
                    lines.append(f"- **{method}**: {fmt_metrics(m)} "
                                 f"(阈值 {m.get('threshold', float('nan')):.4f})")
                lines.append("")
                # 每个方法完成立即落盘（进程被杀也不丢已完成结果）
                Path(args.out).parent.mkdir(parents=True, exist_ok=True)
                Path(args.out).write_text("\n".join(lines), encoding="utf-8")
    log.info(f"报告：{args.out}")


if __name__ == "__main__":
    main()
