"""DLinear 预测器专项测试：接口契约 + 可复现性 + 持久化往返。

覆盖：
  1. fit/forecast 基本链路（多通道、输出长度 = steps）；
  2. steps 对齐（短截断 / 长滚动补足）；
  3. 同种子逐位可复现、异种子有差异（验收复验依赖）；
  4. save/load 与 to_dict/from_dict 往返一致；
  5. 历史不足显式抛 ValueError。
"""
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dlinear_forecaster import DLinearForecaster  # noqa: E402

CTX, PRED = 48, 12
SIDS = ["target", "feat"]


def _data(seed: int = 0, T: int = 300) -> np.ndarray:
    rng = np.random.default_rng(seed)
    t = np.arange(T, dtype=np.float64)
    c0 = 50 + 3 * np.sin(t / 10) + 0.05 * t + rng.normal(0, 0.5, T)
    c1 = 10 + 2 * np.cos(t / 15) + rng.normal(0, 0.3, T)
    return np.stack([c0, c1], axis=1).astype(np.float32)


def _fit(seed: int = 1, device: str = "cpu") -> DLinearForecaster:
    fc = DLinearForecaster(sequence_ids=SIDS, context_length=CTX,
                           prediction_length=PRED, kernel_size=7,
                           epochs=3, batch_size=8, seed=seed, device=device)
    fc.fit(_data())
    return fc


def main() -> None:
    hist = _data()

    # 1. 基本链路
    fc = _fit()
    out = fc.forecast(hist[-CTX:])
    assert set(out) == set(SIDS), "输出键应为全部 sequence_ids"
    assert all(len(v) == PRED for v in out.values()), "默认 steps = prediction_length"
    print("[1] fit/forecast 基本链路 ✓（%d 通道 × %d 步）" % (len(SIDS), PRED))

    # 2. steps 对齐
    short = fc.forecast(hist[-CTX:], steps=4)
    assert all(len(v) == 4 for v in short.values()), "steps 短 → 截断"
    long = fc.forecast(hist[-CTX:], steps=2 * PRED + 3)
    assert all(len(v) == 2 * PRED + 3 for v in long.values()), "steps 长 → 滚动补足"
    print("[2] steps 对齐 ✓（4 截断 / %d 滚动）" % (2 * PRED + 3))

    # 3. 可复现性
    a = _fit(seed=7)
    b = _fit(seed=7)
    c = _fit(seed=8)
    va, vb = a.forecast(hist[-CTX:])["target"], b.forecast(hist[-CTX:])["target"]
    vc = c.forecast(hist[-CTX:])["target"]
    assert va == vb, "同种子应逐位一致"
    assert va != vc, "异种子应有差异"
    print("[3] 可复现性 ✓（seed=7 逐位一致，seed=8 有差异）")

    # 4. 持久化往返（数值容差：load 后 device 按本机自动选择，GPU/CPU 浮点差 ~5e-7）
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "dlinear.pt"
        fc.save(p)
        loaded = DLinearForecaster(sequence_ids=[])
        loaded.load(p)
        vl = loaded.forecast(hist[-CTX:])["target"]
        assert np.allclose(vl, fc.forecast(hist[-CTX:])["target"],
                           atol=1e-5), "save/load 往返应一致"
        rebuilt = DLinearForecaster.from_dict(fc.to_dict())
        vr = rebuilt.forecast(hist[-CTX:])["target"]
        assert np.allclose(vr, vl, atol=1e-5), "to_dict/from_dict 往返应一致"
        assert loaded.model_type == "dlinear"
    print("[4] save/load + to_dict/from_dict 往返 ✓")

    # 5. 历史不足
    try:
        DLinearForecaster(sequence_ids=SIDS, context_length=CTX,
                          prediction_length=PRED, device="cpu").fit(
                              _data(T=CTX + PRED - 1))
        raise AssertionError("历史不足应抛 ValueError")
    except ValueError:
        pass
    print("[5] 历史不足显式抛 ValueError ✓")

    print("\nDLinear 预测器专项测试通过 ✓（5 项）")


if __name__ == "__main__":
    main()
