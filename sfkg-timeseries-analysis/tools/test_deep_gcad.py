"""深度 GCAD（2025 arXiv 2501.13493）专项测试：GcadAnomalyModel 行为契约。

覆盖：
  1. 合成因果数据 fit + 注入模式偏移 detect：
     - 正常因果图最强边指向真实因变量（梯度格兰杰机制）；
     - 注入偏移显著抬升因果偏离（相对判别）；
     - detect 检出结构正确（type/index/score/description），index 起于 tau。
  2. save/load_dict 往返：加载后 detect 结果与原模型一致；
  3. 先验边门（H 矩阵）：relations 边阈值降 h/4、|corr|<corr_threshold 清零、对角保留；
  4. 常量/零方差数据不崩（detect 空）；
  5. 数据不足 → fitted=False 且 detect 空。

注：合成小图数据对"绝对阈值检测"是已知病态（near-zero-edge 放大，gacd/README 已证），
判别能力以 gacd 的 PSM/SWaT benchmark 为权威（PSM AUROC 0.747 / SWaT 0.846）；本测试
只验证机制契约（图结构、相对偏离、detect 结构、往返、先验门、健壮性）。

用法（sfkg 环境）：
  python tools/test_deep_gcad.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "tools")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from anomaly_models import GcadAnomalyModel  # noqa: E402

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


# 轻量配置：单元测试够训可用即可，控时长（工厂默认看 config.yaml anomaly.gcad）
CFG = dict(tau=8, hidden_dim=64, num_layers=2, epochs=10, batch_size=64,
           learning_rate=1e-3, p=0.5, n_norm_samples=64, h=0.0, h_quantile=0.3,
           beta=1.0, score_quantile=0.95, eps=1e-6, max_train_windows=2000,
           train_budget_s=60, val_frac=0.2, seed=0)


def _pattern_causal(n: int = 2400, seed: int = 0) -> np.ndarray:
    """确定性重复模式因果数据：y[t] = x1[t-1] + 0.6·x2[t-1]，40 行模式循环。

    模型能近似学会 → 正常窗口因果偏离小、注入后显著抬升（相对判别稳定）。
    """
    rng = np.random.RandomState(seed)
    P = 40
    t = np.arange(P)
    x1 = np.sin(t / 6) + 0.05 * rng.normal(0, 1, P)
    x2 = np.cos(t / 7) * 0.8 + 0.05 * rng.normal(0, 1, P)
    y = np.empty(P)
    y[0] = 0.0
    y[1:] = 1.0 * x1[:-1] + 0.6 * x2[:-1]
    return np.tile(np.column_stack([x1, x2, y]), (n // P + 1, 1))[:n].astype(np.float32)


def _inject(win: np.ndarray, kind: str, tcol: int = 2, start: int = 25) -> np.ndarray:
    """窗口后半段注入模式偏移（同 evaluate_anomaly_models 的注入语义）。

    窗口内 y[t] 依赖 x[t-1]：注入段 25..49 用 x 局部 24..48 重算 y。
    """
    w = win.copy()
    a1, a2 = 1.0, 0.6
    s1, s2 = w[24:49, 0], w[24:49, 1]        # x1[t-1], x2[t-1]
    if kind == "coef_shift":        # 系数变化：a1 翻倍
        w[start:, tcol] = 2 * a1 * s1 + a2 * s2
    elif kind == "rel_break":       # 关系反转：x1 影响方向反向
        w[start:, tcol] = -a1 * s1 + a2 * s2
    elif kind == "source_drop":     # 变量失效：x1 不再影响 y
        w[start:, tcol] = a2 * s2
    return w


def test_fit_detect_index_mapping() -> None:
    print("\n[① 合成因果 fit + 注入偏移 detect]")
    M = _pattern_causal()
    m = GcadAnomalyModel(**CFG, target_index=2)
    m.fit(M[:1000])
    assert m.fitted, "训练数据充足应 fitted"
    _ok("合成因果数据 fit → fitted=True")

    # ① 因果图结构（格兰杰方向性）：真实因果入边（x1/x2→y）应强于反向边（y→x1/x2）
    real_in = max(m._A_norm[0, 2], m._A_norm[1, 2])     # 指向因变量的真实因果边
    rev_out = max(m._A_norm[2, 0], m._A_norm[2, 1])     # 反向边（y 反向影响预测列）
    assert real_in > rev_out, \
        f"真实因果入边应强于反向边（Granger 方向性）：入 {real_in:.2f} vs 反 {rev_out:.2f}"
    _ok(f"因果方向性：真实入边 {real_in:.2f} ≫ 反向边 {rev_out:.2f}（梯度格兰杰成立）")

    # ② 注入偏移抬升因果偏离（相对判别）：注入窗口最大偏离 ≫ 正常窗口
    norm = M[2000:2050]
    X = lambda w: (w - m._norm_mean) / m._norm_std          # noqa: E731
    S_norm, *_ = m._deviation_on(X(norm))
    for kind in ("coef_shift", "source_drop"):
        w = _inject(norm, kind)
        S_inj, *_ = m._deviation_on(X(w))
        assert S_inj.max() > 2.0 * S_norm.max(), \
            f"{kind} 注入应显著抬升因果偏离（{S_inj.max():.1f} vs 正常 {S_norm.max():.1f}）"
        _ok(f"{kind} 注入 → 最大因果偏离 {S_inj.max():.1f} ≫ 正常 {S_norm.max():.1f}")

        # ③ detect 结构：阈值取"正常最大偏离" → 正常 0 检出、注入有检出，index 映射正确
        m._threshold = float(S_norm.max())
        findings = m.detect(w)
        assert findings, f"{kind} 注入在阈值=正常最大值时应检出"
        assert m.detect(norm) == [], "正常窗口在自身最大值阈值下应 0 检出"
        for f in findings:
            assert f["anomaly_type"] == "CAUSAL_PATTERN"
            assert m.tau <= f["index"] < len(w), f"index 越界 {f['index']}"
            assert f["score"] > 0
            assert "列" in f["description"], "描述应带最大偏离边"
        _ok(f"{kind} → detect 检出 {len(findings)} 条，结构/索引映射正确")


def test_save_load_roundtrip() -> None:
    print("\n[② save/load_dict 往返]")
    M = _pattern_causal()
    m = GcadAnomalyModel(**CFG, target_index=2)
    m.fit(M[:1000])
    norm = M[2000:2050]
    X = lambda w: (w - m._norm_mean) / m._norm_std          # noqa: E731
    S_norm, *_ = m._deviation_on(X(norm))
    m._threshold = float(S_norm.max())       # 让注入窗口在往返前后都稳定检出
    win = _inject(norm, "coef_shift")
    with tempfile.NamedTemporaryFile(suffix=".pt", delete=False) as tf:
        path = tf.name
    try:
        m.save(path)
        m2 = GcadAnomalyModel(**CFG, target_index=2)
        m2.load_dict(_torch_load(path))
        assert m2.fitted, "load 后应 fitted"
        d1 = [(f["index"], round(f["score"], 6)) for f in m.detect(win)]
        d2 = [(f["index"], round(f["score"], 6)) for f in m2.detect(win)]
        assert d1 and d2, "往返前后都应检出注入"
        assert d1 == d2, f"load 后 detect 应与原模型一致\n原模型 {d1}\n加载后 {d2}"
        _ok(f"save → load_dict → detect 一致（{len(d1)} 条）")
    finally:
        Path(path).unlink(missing_ok=True)


def test_prior_edge_gate() -> None:
    print("\n[③ 先验边门：relations / correlation / 对角]")
    # relations 边 → 阈值 h/4（知识库标注的因果边优先保留）
    m = GcadAnomalyModel(**CFG, target_index=2, relations_prior={0: 0.9})
    H = m._build_H(3, h=0.4)
    assert H[0, 2] == 0.1, "relations 边 i→target 阈值应降为 h/4=0.1"
    assert H[1, 2] == 0.4, "非先验边 → 默认阈值 h"
    assert H[0, 0] == 0 and H[2, 2] == 0, "对角自相关永远保留（阈值 0）"
    # 低相关 → 清零；无先验 → 全图统一
    m2 = GcadAnomalyModel(**CFG, target_index=2,
                          correlation_prior={1: 0.05}, corr_threshold=0.1)
    H2 = m2._build_H(3, h=0.4)
    assert np.isinf(H2[1, 2]), "|corr|<corr_threshold → i→target 边清零"
    assert H2[0, 2] == 0.4, "无该列先验 → 默认阈值 h"
    H3 = GcadAnomalyModel(**CFG, target_index=None)._build_H(3, h=0.4)
    assert (H3 == 0.4).sum() == 6 and (np.diag(H3) == 0).all(), \
        "无先验 → 全图统一阈值 h + 对角 0"
    _ok("relations→h/4、低相关→inf、对角→0、无先验→全图统一")


def test_constant_data_no_crash() -> None:
    print("\n[④ 常量/零方差数据不崩]")
    hist = np.ones((120, 2), dtype=np.float32)
    m = GcadAnomalyModel(**CFG, target_index=1)
    m.fit(hist)
    assert m.fitted, "常量数据也应能完成 fit（σ+1e-8 兜底）"
    assert m.detect(np.ones((30, 2), dtype=np.float32)) == [], \
        "常量窗口 → 与正常图同构 → 无检出"
    single = GcadAnomalyModel(**CFG, target_index=0)
    single.fit(np.ones((80, 1), dtype=np.float32))
    assert single.fitted, "单列常量也应能 fit（target_index 自动落到 0）"
    assert single.detect(np.ones((30, 1), dtype=np.float32)) == [], \
        "单列常量窗口 → 无检出"
    _ok("常量/单列常量 fit 不崩，detect 空")


def test_short_data_not_fitted() -> None:
    print("\n[⑤ 数据不足 → fitted=False + detect 空]")
    m = GcadAnomalyModel(**CFG, target_index=1)
    m.fit(np.ones((10, 2), dtype=np.float32))       # tau+2=10 但 nwin=0
    assert not m.fitted, "训练窗口不足应 fitted=False"
    assert m.detect(np.ones((30, 2), dtype=np.float32)) == [], "未训练 → detect 空"
    m2 = GcadAnomalyModel(**CFG, target_index=1)
    m2.fit(np.ones((20, 2), dtype=np.float32))      # nwin=8 < max(8,4τ)=32
    assert not m2.fitted, "nwin<max(8,4τ) 应 fitted=False"
    _ok("短数据 → 静默不训练（fitted=False）且 detect 空")


def _torch_load(path: str) -> dict:
    import torch
    return torch.load(path, weights_only=False)


def main() -> None:
    test_fit_detect_index_mapping()
    test_save_load_roundtrip()
    test_prior_edge_gate()
    test_constant_data_no_crash()
    test_short_data_not_fitted()
    print(f"\n深度 GCAD 专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
