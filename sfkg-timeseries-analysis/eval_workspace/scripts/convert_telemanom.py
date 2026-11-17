#!/usr/bin/env python
"""telemanom（HF 镜像）逐通道 npy → 评测布局：<DS>/<chan>/{train,test,test_label}.npy。

来源：huggingface appleparan/telemanom（hf-mirror.com 下载），与 telemanom
data.zip 原始 npy 一致 + labeled_anomalies.csv（chan_id/spacecraft/序列）。

序列区间 [start, end] 按闭区间转标签（telemanom 约定）——锚点：SMAP D-3
测试 8640 点、序列 [5225, 8500] → 3276 异常点、异常率 37.92%（与评测记录一致）。

同时为 GCAD 研究线输出官方格式 train.csv/test.csv（test.csv 末列=标签，
train.csv 纯数值；研究线 dataloader 再自行 80/20 切分+标准化）。
"""
import ast
import csv
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis" / "eval_workspace"
RAW = ROOT / "data" / "raw" / "telemanom_hf"
DST = ROOT / "data"


def main() -> None:
    rows = list(csv.reader(open(RAW / "labeled_anomalies.csv")))[1:]
    # P-2 在 labels CSV 里有两行（telemanom 已知瑕疵，区间不同）——
    # 按 dict 语义后写覆盖；T-10 无标签行（SMAP 通道，全干净）。
    labels_by_chan: dict[str, list] = {}
    for chan_id, spacecraft, seqs_s, _cls, _n in rows:
        labels_by_chan[chan_id] = (spacecraft.upper(), ast.literal_eval(seqs_s))
    n_rows = len(rows)
    print(f"标签行数={n_rows}（含 P-2 重复） 唯一通道={len(labels_by_chan)}",
          flush=True)
    n_converted = 0
    for name in sorted(p.stem for p in (RAW / "data" / "data" / "train").glob("*.npy")):
        if name in labels_by_chan:
            ds, seqs = labels_by_chan[name]
        else:
            # 唯一无标签通道 T-10：55 特征 = MSL 格式（T 系列 4..13），干净无异常。
            ds = "MSL"
            seqs = []
        tr = np.load(RAW / "data" / "data" / "train" / f"{name}.npy")
        te = np.load(RAW / "data" / "data" / "test" / f"{name}.npy")
        label = np.zeros(len(te), dtype=np.int64)
        for s, e in seqs:
            label[s:e + 1] = 1  # 闭区间
        out = DST / ds / name
        out.mkdir(parents=True, exist_ok=True)
        np.save(out / "train.npy", tr.astype(np.float32))
        np.save(out / "test.npy", te.astype(np.float32))
        np.save(out / "test_label.npy", label)
        # GCAD 研究线 CSV（官方 dataloader：index_col=0，test 末列=标签，
        # train 末列=占位 0 被 iloc[:,:-1] 剥掉）。必须带表头：np.savetxt 无表头
        # 会被 read_csv 把首行数据当列名吃掉、首列特征当行号吃掉（2026-10-28 教训）。
        csv_dir = ROOT / "gcad_repro" / "dataset" / ds / name
        csv_dir.mkdir(parents=True, exist_ok=True)
        tr_csv = np.column_stack([np.arange(len(tr)), tr, np.zeros(len(tr))])
        te_csv = np.column_stack([np.arange(len(te)), te, label])
        pd.DataFrame(tr_csv).to_csv(csv_dir / "train.csv", index=False,
                                    header=True)
        pd.DataFrame(te_csv).to_csv(csv_dir / "test.csv", index=False,
                                    header=True)
        n_converted += 1
        if name == "D-3" and ds == "SMAP":
            print(f"[锚点] SMAP D-3: test={len(te)} 异常={int(label.sum())} "
                  f"事件={len(seqs)}", flush=True)
    # 锚点校验
    te = np.load(DST / "SMAP" / "D-3" / "test.npy")
    lb = np.load(DST / "SMAP" / "D-3" / "test_label.npy")
    assert len(te) == 8640, f"D-3 长度不符: {len(te)}"
    assert int(lb.sum()) == 3276, f"D-3 异常点不符: {int(lb.sum())}"
    assert float(lb.mean()) == 3276 / 8640, "D-3 异常率不符"
    assert te.shape[1] == 25, f"D-3 通道数不符: {te.shape[1]}"
    assert len(list((DST / "SMAP").iterdir())) == 54, "SMAP 实体数不符"
    assert len(list((DST / "MSL").iterdir())) == 28, "MSL 实体数不符"
    print(f"锚点校验通过：D-3 8640/3276/37.92%/25 通道、SMAP 54 + MSL 28（含干净通道 T-10）="
          f"110 实体（含 SMD 28）",
          flush=True)
    print(f"TELEMANOM-CONVERT-DONE ({n_converted} 实体)", flush=True)


if __name__ == "__main__":
    main()
