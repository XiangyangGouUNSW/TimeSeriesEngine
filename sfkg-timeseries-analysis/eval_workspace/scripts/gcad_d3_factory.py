#!/usr/bin/env python
"""工厂 GcadAnomalyModel（= 论文官方链路原样实现）SMAP D-3 点级验收复验。

背景（2026-10-30 一致性改造，用户原则"评测方法 = 工厂实际方法"）：
  合同验收用官方仓库（Tc99m/GCAD，AAAI 2025）方法在 SMAP D-3 达标；
  工厂 GcadAnomalyModel 已按官方链路重写（TSMixerRevIN、80/20、早停、
  sparse_th 对称稀疏、SMA-3 打分链），本脚本用工厂代码（构造默认值、
  零覆盖）复跑 D-3 验收协议，与官方链路记录阶梯对照。

双口径（如实标注的协议差异）：
  1) strict-replica 严格复刻：官方 DataLoader drop_last=True——验证打分只
     取前 512 窗、测试打分只取前 8448 窗，尾部窗口被丢弃/延展（官方实现
     的批处理副作用，非方法语义）；
  2) production-style 生产口径：工厂生产语义 = 全部窗口打分（无批次概念），
     验证 576 窗、测试 8570 窗全用。

对照锚点（官方链路 2026-10-28 重跑，eval_workspace/results/gcad_d3_channel/
ladder.json，seed=0）：AUROC 0.9486；8 档阶梯 P/R 见 RECORDED。
合同指标：R≥90% 且 P≥85%（点级）。噪声带：|ΔAUROC|≤0.01、各档 |ΔP|,|ΔR|≤0.02
（官方 Ā_norm 采样未播种 + 训练洗牌顺序不同 → 逐位复现本就不可能，官方自己
重跑也在带内，见 gcad_d3_retrain.py docstring）。

用法（sfkg 环境，GPU 约束）：
  CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=8 SFKG_MAX_THREADS=8 \
  taskset -c 0-19,40-59 python eval_workspace/scripts/gcad_d3_factory.py
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
sys.path.insert(0, str(ROOT / "src"))

from anomaly_models import GcadAnomalyModel, _sma3  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402

SEQ = 70                    # 论文 seq_len
TIERS = ["q0.95", "q0.98", "q0.99", "q0.995", "q0.999", "q0.9999",
         "q0.99999", "max(val)"]
QS = [0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 0.99999, None]
# 官方链路 2026-10-28 记录（ladder.json，seed=0）
RECORDED_AUC = 0.9486
RECORDED = {t: (p, r) for t, p, r in [
    ("q0.95", 0.853, 0.977), ("q0.98", 0.875, 0.977),
    ("q0.99", 0.892, 0.976), ("q0.995", 0.896, 0.976),
    ("q0.999", 0.900, 0.974), ("q0.9999", 0.903, 0.974),
    ("q0.99999", 0.904, 0.974), ("max(val)", 0.904, 0.974)]}
CONTRACT_P, CONTRACT_R = 0.85, 0.90   # 合同：R≥90% 且 P≥85%


def point_scores(window_scores: np.ndarray, t: int, seq_len: int) -> np.ndarray:
    """窗口分数 → 逐点分数（覆盖全部 t 点，与 gcad_d3_retrain.py 同式）。"""
    pts = np.empty(t, dtype=np.float64)
    n = len(window_scores)
    for i in range(n):  # 窗口 i → 预测点 i+seq_len
        pts[i + seq_len] = window_scores[i]
    pts[:seq_len] = window_scores[0]  # 前端回填
    if n + seq_len < t:
        pts[n + seq_len:] = window_scores[-1]
    return pts


def ladder(all_scores: np.ndarray, val_smooth: np.ndarray, lb: np.ndarray,
           name: str) -> dict:
    """8 档阶梯 + AUROC（阈值 = 验证段 SMA-3 平滑分数分位，无测试泄漏）。"""
    auc = float(roc_auc_score(lb, all_scores))
    print(f"\n[{name}] 点级 AUROC = {auc:.4f}（记录 {RECORDED_AUC:.4f}）")
    print(f"{'档':<10}{'阈值':>10}{'P':>8}{'R':>8}{'Acc':>8}"
          f"{'ΔP':>7}{'ΔR':>7}{'合同':>6}", flush=True)
    rows = []
    for tier, q in zip(TIERS, QS):
        th = float(np.max(val_smooth)) if q is None else \
            float(np.quantile(val_smooth, q))
        pred = (all_scores >= th).astype(np.int64)
        tp = int(((pred == 1) & (lb == 1)).sum())
        fp = int(((pred == 1) & (lb == 0)).sum())
        fn = int(((pred == 0) & (lb == 1)).sum())
        tn = int(((pred == 0) & (lb == 0)).sum())
        p = tp / max(tp + fp, 1)
        r = tp / max(tp + fn, 1)
        acc = (tp + tn) / len(lb)
        rp, rr = RECORDED[tier]
        ok = "✓" if p >= CONTRACT_P and r >= CONTRACT_R else "✗"
        print(f"{tier:<10}{th:>10.4g}{p:>8.3f}{r:>8.3f}{acc:>8.3f}"
              f"{p - rp:>+7.3f}{r - rr:>+7.3f}{ok:>6}", flush=True)
        rows.append(dict(tier=tier, th=th, tp=tp, fp=fp, fn=fn, tn=tn,
                         p=p, r=r, acc=acc))
    return dict(auc=auc, rows=rows)


def main() -> None:
    train = np.load(WS / "data" / "SMAP" / "D-3" / "train.npy")   # (2880,25)
    test = np.load(WS / "data" / "SMAP" / "D-3" / "test.npy")     # (8640,25)
    lb = np.load(WS / "data" / "SMAP" / "D-3" / "test_label.npy").astype(np.int64)
    print(f"[D-3] train {train.shape} test {test.shape} "
          f"异常点 {int(lb.sum())}/{len(lb)} ({lb.mean():.2%})", flush=True)
    # 官方 drop_last 尾部（测试窗 8448..8569 被丢）标签分布——两口径差异区
    print(f"[尾部标签] 点8516..8639 异常 {int(lb[8516:].sum())}/124；"
          f"点8516..8637 异常 {int(lb[8516:8638].sum())}/122", flush=True)

    # ── 工厂模型：构造默认值、零覆盖（= 论文 run.sh SMAP 配置 = 合同验收配置）
    m = GcadAnomalyModel()
    m._chunk = 128     # 论文窗口 [70,25] 小，加大分块提升 vmap 吞吐（生产护栏值 32 不动）
    assert (m.tau, m.n_block, m.ff_dim, m.p, m.h, m.epochs, m.batch_size,
            m.learning_rate, m.patience, m.score_quantile, m.eps,
            m.sma_window, m.val_frac, m.beta) == \
        (70, 6, 1024, 0.2, 0.008, 100, 128, 1e-4, 2, 0.95, 1e-4, 3, 0.2, 0.0), \
        "构造默认值应 = 论文 SMAP 配置"
    print("\n[工厂模型] 构造默认值 = 论文 run.sh SMAP 配置 ✓（零覆盖）", flush=True)

    t0 = time.monotonic()
    m.fit(train)
    print(f"[fit {time.monotonic() - t0:.0f}s] fitted={m.fitted} "
          f"阈值(q{m.score_quantile:g})={m._threshold:.4g}", flush=True)
    assert m.fitted, "D-3 数据充足应训练成功"
    assert np.allclose(m._S_score, _sma3(m._val_S)), \
        "fit 内阈值分序列应 = 验证段原始分数 SMA-3"
    print(f"[内部一致] _S_score = SMA-3(_val_S) ✓（验证段 {len(m._val_S)} 窗 → "
          f"平滑 {len(m._S_score)}）", flush=True)

    # ── 工厂打分链（score_segment = 生产公共接口，与 detect 同一链路）
    t0 = time.monotonic()
    test_raw = m.score_segment(test)          # 原始因果偏离 S（未平滑），8570 窗
    print(f"[打分 {time.monotonic() - t0:.0f}s] 测试段 {len(test_raw)} 窗", flush=True)
    val_raw = m._val_S                        # 验证段原始窗口分数，576 窗

    out = WS / "results" / "gcad_d3_factory"
    out.mkdir(parents=True, exist_ok=True)
    report = {"train": train.shape, "test": test.shape,
              "n_anomaly": int(lb.sum()), "threshold_q95": float(m._threshold),
              "val_windows": len(val_raw), "test_windows": len(test_raw)}

    # ── 口径 1：严格复刻官方 drop_last（验证前 512 窗 / 测试前 8448 窗）
    val_rep = _sma3(val_raw[:512])            # 510
    test_rep = _sma3(test_raw[:8448])         # 8446
    pts_rep = point_scores(test_rep, len(test), SEQ)
    assert len(pts_rep) == len(test) == len(lb)
    np.save(out / "points_strict.npy", pts_rep)
    report["strict_replica"] = ladder(pts_rep, val_rep, lb, "严格复刻(drop_last)")

    # ── 口径 2：生产语义全窗口打分（工厂生产无批次概念）
    val_prod = _sma3(val_raw)                 # 574
    test_prod = _sma3(test_raw)               # 8568
    pts_prod = point_scores(test_prod, len(test), SEQ)
    np.save(out / "points_production.npy", pts_prod)
    report["production_style"] = ladder(pts_prod, val_prod, lb, "生产口径(全窗口)")
    # 工厂自身检测阈值 = 生产口径验证分数 q95（与生产 detect 行为一致）
    th_prod = float(np.quantile(val_prod, 0.95))
    print(f"[生产阈值] 模型 _threshold={m._threshold:.4g} vs 复算 q95={th_prod:.4g} "
          f"(Δ={abs(m._threshold - th_prod):.2g})", flush=True)

    # ── 达标判定（如实，不夸大）：两口径 8 档全过合同 + 数字在噪声带
    def verdict(d: dict) -> tuple[bool, bool]:
        contract = all(r["p"] >= CONTRACT_P and r["r"] >= CONTRACT_R
                       for r in d["rows"])
        band = abs(d["auc"] - RECORDED_AUC) <= 0.01 and all(
            abs(r["p"] - RECORDED[r["tier"]][0]) <= 0.02
            and abs(r["r"] - RECORDED[r["tier"]][1]) <= 0.02
            for r in d["rows"])
        return contract, band

    print("\n[达标判定]（合同 R≥90% 且 P≥85%；噪声带 |ΔAUROC|≤0.01、|ΔP|,|ΔR|≤0.02）",
          flush=True)
    for key in ("strict_replica", "production_style"):
        contract, band = verdict(report[key])
        print(f"  {key:<28} 合同 8 档全过: {'✓' if contract else '✗'}"
              f"   记录噪声带内: {'✓' if band else '✗'}", flush=True)
    report["recorded"] = {"auc": RECORDED_AUC, "tiers": RECORDED}
    report["verdict"] = {k: {"contract_ok": verdict(report[k])[0],
                             "in_noise_band": verdict(report[k])[1]}
                         for k in ("strict_replica", "production_style")}
    with open(out / "report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n结果落盘：{out / 'report.json'}", flush=True)
    print("GCAD-D3-FACTORY-DONE", flush=True)


if __name__ == "__main__":
    main()
