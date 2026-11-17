#!/usr/bin/env python
"""OmniAnomaly SMD txt → 工厂评估工具的数据布局转换。

工厂 evaluate_contract_benchmarks.load_entities("SMD") 期望：
  DATA_ROOT/SMD/machine-*-*/{train.npy, test.npy, test_label.npy}
OmniAnomaly ServerMachineDataset 为纯数值 txt：train/test 每行逗号分隔的
38 通道值（无时间戳列），test_label 每行一个 0/1。

验证锚点（与 10-24/10-25 评测一致才放行）：
  machine-1-1: test 长度 28479、异常点 2694（9.46%）、8 个事件；
  machine-2-4: 38 通道。
"""
from pathlib import Path

import numpy as np

SRC = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis" \
    / "eval_workspace" / "data" / "OmniAnomaly" / "ServerMachineDataset"
DST = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis" \
    / "eval_workspace" / "data" / "SMD"


def main() -> None:
    machines = sorted(p.stem for p in (SRC / "train").glob("machine-*.txt"))
    print(f"机器数={len(machines)}", flush=True)
    for name in machines:
        tr = np.loadtxt(SRC / "train" / f"{name}.txt",
                        delimiter=",", dtype=np.float32)
        te = np.loadtxt(SRC / "test" / f"{name}.txt",
                        delimiter=",", dtype=np.float32)
        lb = np.loadtxt(SRC / "test_label" / f"{name}.txt",
                        dtype=np.int64)
        out = DST / name
        out.mkdir(parents=True, exist_ok=True)
        np.save(out / "train.npy", tr)
        np.save(out / "test.npy", te)
        np.save(out / "test_label.npy", lb.reshape(-1))
        print(f"{name}: train={tr.shape} test={te.shape} "
              f"异常点={int(lb.sum())}", flush=True)
    # 锚点校验
    te = np.load(DST / "machine-1-1" / "test.npy")
    lb = np.load(DST / "machine-1-1" / "test_label.npy")
    starts = np.flatnonzero(lb & ~np.r_[False, lb[:-1]])
    assert len(te) == 28479, f"1-1 长度不符: {len(te)}"
    assert int(lb.sum()) == 2694, f"1-1 异常点不符: {int(lb.sum())}"
    assert len(starts) == 8, f"1-1 事件数不符: {len(starts)}"
    te24 = np.load(DST / "machine-2-4" / "test.npy")
    assert te24.shape[1] == 38, f"2-4 通道数不符: {te24.shape[1]}"
    print("锚点校验通过：1-1 长度/异常点/事件数、2-4 通道数均与评测记录一致",
          flush=True)
    print("SMD-CONVERT-DONE", flush=True)


if __name__ == "__main__":
    main()
