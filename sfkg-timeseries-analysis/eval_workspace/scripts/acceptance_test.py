#!/usr/bin/env python
"""合同验收测试接口（复现）：只调工厂冻结代码。

用法：
  python acceptance_test.py anomaly   # DBSCAN@SMD 1-1 段级冒烟（期望 R=1.000/P=0.955）
  python acceptance_test.py forecast  # CI PatchTST@SMD 2-4 m24 正常段 1 种子冒烟

环境变量：
  SFKG_PROJECT_ROOT  工厂项目根（默认 ~/TimeSeriesEngine/sfkg-timeseries-analysis）
  GCAD_DATA_ROOT     评测数据根（默认 ~/gcad/data，即 eval_workspace/data 的软链接）
"""
import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np

PROJ = Path(os.environ.get(
    "SFKG_PROJECT_ROOT",
    Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"))
sys.path.insert(0, str(PROJ / "tools"))
sys.path.insert(0, str(PROJ / "src"))
sys.path.insert(0, str(PROJ / "generated"))

import evaluate_contract_benchmarks as ev  # noqa: E402

if "GCAD_DATA_ROOT" in os.environ:
    ev.DATA_ROOT = Path(os.environ["GCAD_DATA_ROOT"])
print(f"[data root] {ev.DATA_ROOT}", flush=True)


def cmd_anomaly() -> int:
    """DBSCAN @ SMD machine-1-1（idx 0，默认 eps=0.5/min_samples=5）段级冒烟。"""
    ent = ev.load_entities("SMD", limit=1)[0]
    print(f"== anomaly smoke: SMD / {ent.name}（默认参数，无泄漏）", flush=True)
    t0 = time.monotonic()
    m = ev.eval_dbscan(ent, stride=1)
    r = m.get("event_recall")
    p = m.get("event_precision")
    det = m.get("detected_events")
    tot = m.get("true_events")
    print(ev.fmt_metrics(m), flush=True)
    print(f"[{time.monotonic()-t0:.0f}s]", flush=True)
    ok = (abs(r - 1.0) < 1e-9) and (abs(p - 0.955) < 0.02) and det == tot
    print(f"anomaly smoke {'PASS' if ok else 'FAIL'} "
          f"(期望 R=1.000/P=0.955, {det}/{tot} 事件)", flush=True)
    return 0 if ok else 1


def cmd_forecast() -> int:
    """CI PatchTST @ SMD machine-2-4（idx 11）m24 正常段 1 种子冒烟（~1h GPU）。"""
    import random
    import torch
    from patchtst_forecaster import PatchTSTForecaster
    PRED, CTX = 3, 168
    ent = ev.load_entities("SMD")[11]
    train, test, labels = ent.train, ent.test, ent.labels
    T, C = test.shape
    print(f"== forecast smoke: SMD / {ent.name} ctx={CTX} pred={PRED} "
          f"m24 正常段 1 种子", flush=True)
    ws_all = [t for t in range(CTX, T, PRED) if t + PRED - 1 < T]
    norm_mask = np.array([bool(labels[t:t + PRED].sum() == 0) for t in ws_all])
    mae_norm = np.zeros(C)
    cnt_norm = np.zeros(C, dtype=np.int64)
    for c in range(C):
        random.seed(0)
        np.random.seed(0)
        torch.manual_seed(0)
        fc = PatchTSTForecaster(sequence_ids=["c0"], context_length=CTX,
                                prediction_length=PRED, patch_size=8,
                                patch_stride=4, d_model=64, n_heads=4,
                                num_layers=2, epochs=20, batch_size=64,
                                device="cuda")
        fc.fit(train[:, c:c + 1])
        for i, t in enumerate(ws_all):
            pmap = fc.forecast(test[t - CTX:t, c:c + 1], steps=PRED)
            p = np.array(pmap["c0"], dtype=np.float64)
            e = np.mean([abs(float(test[t + h, c]) - p[h])
                         for h in range(PRED)])
            if norm_mask[i]:
                mae_norm[c] += e
                cnt_norm[c] += 1
    mases = [float(mae_norm[c] / ev.naive_scale(
        train[:, c].astype(np.float64), 24))
        for c in range(C) if ev.naive_scale(train[:, c].astype(np.float64), 24) > 0]
    m24 = float(np.mean(mases))
    print(f"[m24 正常段 MASE(1 种子) = {m24:.3f}]（记录 3 种子 "
          f"0.580/0.583/0.596）", flush=True)
    print(f"forecast smoke {'PASS' if m24 <= 0.6 else 'FAIL'}", flush=True)
    return 0 if m24 <= 0.6 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["anomaly", "forecast"])
    args = ap.parse_args()
    return cmd_anomaly() if args.cmd == "anomaly" else cmd_forecast()


if __name__ == "__main__":
    sys.exit(main())
