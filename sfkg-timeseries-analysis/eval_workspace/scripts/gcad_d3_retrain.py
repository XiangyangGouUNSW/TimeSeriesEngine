#!/usr/bin/env python
"""GCAD 研究线重训：SMAP D-3 单通道（官方仓库形态）+ D-3 合同协议验证。

训练与打分逐操作复刻官方仓库（Tc99m/GCAD，AAAI 2025）main.py/test.py：
  - SMAP 配置（run.sh）：seq_len=70, pred_len=1, sample_p=0.2, sparse_th=0.008,
    test_stride=1, n_block=6, ff_dim=1024, dropout=0, lr=1e-4, epochs=100,
    patience=2, batch_size=128；
  - 官方调用形态=逐通道目录（--data dataset/smap/<chan>）：train.csv 前 80%
    训练、后 20% 与前段重叠 seq_len 作验证、StandardScaler 按训练段标准化；
  - 打分链：逐窗口梯度因果矩阵 → 反称化 → sparse_th 稀疏化 → 对训练均值
    因果矩阵的相对偏差 → SMA-3；
  - 窗口 i ↔ 预测点 i+seq_len；窗口标签 = [i, i+seq_len+pred_len) 内任一异常。

重建过程记录（如实）：
  - 第一版 whole-SMAP 拼接训练（54 通道混入）→ D-3 AUROC=0.5067，事件内分数
    低于背景、完全不分离——拼接路径被否，改为逐通道（与官方 --data 形态及原
    评测"110 实体逐实体"结构一致；SMD 研究线 1-1/2-9 亦为逐机）。
  - 转换脚本 CSV 曾无表头（np.savetxt）→ read_csv 把首行数据当列名、首列
    特征当行号吃掉（train 23 特征/test 24 特征），已修为带表头+行号列标准
    CSV（convert_telemanom.py 同修）。

合同协议（记录锚点，见报告 v2 §2.1）：
  - 阈值 = 验证段分数 q95/q98/q99/q995/q999/q9999/q99999/max（无测试泄漏）；
  - 点级 P/R/Acc 在 D-3 测试段（8640 点，3276 异常点）上计算；
  - 目标：sanity AUROC(D-3)≈0.9506（论文 SMAP AUROC 0.953）；
    paper q99 P=0.891/R=0.974；paper max(val) P=0.908/R=0.962。

点对齐：窗口 i 的分数赋给预测点 i+seq_len；前端 0..seq_len 用首个分数回填，
末点用末分数延长——覆盖全部 8640 点（原实现细节已失，本处为如实标注的重实现）。

内存修复（2026-10-27/28 OOM×2，whole-SMAP 版）：逐批先 mean(dim=1) 再在 CPU
累积（mean 与后续 triu/where/abs 逐窗口逐元素等价，数值一致）；GPU 只留每批
反向图 ~2 GiB，抗共享服务器他人在卡上抢内存。逐通道数据小，此设计同样适用。
"""
import argparse
import sys
import time
from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
import torch

ROOT = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
WS = ROOT / "eval_workspace"
REPO = WS / "gcad_repro"
sys.path.insert(0, str(REPO))

from utils.dataloader import SwatDataLoader_AD  # noqa: E402
from utils.general import set_seed  # noqa: E402
from models.tsmixer import TSMixerRevIN  # noqa: E402
from test import save_train_mean_causal, simple_moving_average  # noqa: E402

CFG = dict(seq_len=70, pred_len=1, sample_p=0.2, sparse_th=0.008,
           test_stride=1, n_block=6, ff_dim=1024, dropout=0.0,
           learning_rate=1e-4, train_epochs=100, patience=2, batch_size=128)


def build_channel_csv(name: str) -> None:
    """逐实体 npy → 官方格式通道 CSV（带表头+行号列；test 末列=标签，
    train 末列=占位 0 被 loader iloc[:,:-1] 剥掉）。"""
    tr = np.load(WS / "data" / "SMAP" / name / "train.npy")
    te = np.load(WS / "data" / "SMAP" / name / "test.npy")
    lb = np.load(WS / "data" / "SMAP" / name / "test_label.npy")
    out = REPO / "dataset" / "SMAP" / name
    out.mkdir(parents=True, exist_ok=True)
    tr_csv = np.column_stack([np.arange(len(tr)), tr, np.zeros(len(tr))])
    te_csv = np.column_stack([np.arange(len(te)), te, lb])
    pd.DataFrame(tr_csv).to_csv(out / "train.csv", index=False, header=True)
    pd.DataFrame(te_csv).to_csv(out / "test.csv", index=False, header=True)
    print(f"[channel {name}] train={tr.shape} test={te.shape} "
          f"异常={int(lb.sum())} ({lb.mean():.2%})", flush=True)


def extract_scores(model, save_path, dataloader, device, parms_path, sparse_th):
    """复刻 test.py::test 的打分链，返回 (err_score, window_labels)。

    testing=False（val 段）：窗口无标签，labels 用哑 0 数组仅作 SMA 对齐
    （SMA 只移动/截断边界，平滑本身只用分数）。

    与官方差异（数值逐窗口等价，如实标注）：
      a) 逐批先 mean(dim=1) 再在 CPU 累积——官方全量 concat 到 GPU 后 mean，
         whole-SMAP 场景 76 GiB 必爆（内存修复，见模块 docstring）；
      b) 相对偏差向量化 = 原逐窗口循环同式（float32/float64 提升路径不变；
         舍入可达 ulp 级差异，不影响任何 3 位有效数字结论）。
    """
    saved = torch.load(save_path, map_location=device)
    model.load_state_dict(saved)
    model.eval()
    criterion = torch.nn.MSELoss(reduction="sum").to(device)
    first = 0
    for item in dataloader:
        if len(item) == 3:
            batch_x, batch_y, batch_labels = item
            label, _ = torch.max(batch_labels, dim=1)
        else:
            batch_x, batch_y = item
            label = torch.zeros(batch_x.shape[0])
        batch_x, batch_y = batch_x.to(device), batch_y.to(device)
        batch_x.requires_grad = True
        outputs = model(batch_x).float()
        for f in range(outputs.shape[-1]):
            model.zero_grad()
            loss_i = criterion(outputs[:, :, f], batch_y[:, :, f])
            loss_i.requires_grad_(True)
            loss_i.backward(retain_graph=True)
            Grad_i = torch.abs(batch_x.grad)
            batch_x.grad = None
            if f == 0:
                grad_causal_mat = torch.unsqueeze(Grad_i, dim=3)
            else:
                grad_causal_mat = torch.cat(
                    [grad_causal_mat, torch.unsqueeze(Grad_i, dim=3)], dim=3)
        batch_mat = torch.mean(grad_causal_mat, dim=1).cpu()
        if first == 0:
            input_grad_causal_mat = batch_mat
            test_labels = label
            first = 1
        else:
            input_grad_causal_mat = torch.cat(
                [input_grad_causal_mat, batch_mat], dim=0)
            test_labels = torch.cat((test_labels, label), dim=0)
        del batch_x, batch_y, outputs, grad_causal_mat
    upper = torch.triu(input_grad_causal_mat, diagonal=0)
    lower_t = torch.tril(input_grad_causal_mat, diagonal=-1).transpose(1, 2)
    result = torch.triu(upper - lower_t, diagonal=0)
    result_upper = torch.where(result < 0, torch.zeros_like(result), result)
    result_lower = torch.where(result < 0, torch.abs(result),
                               torch.zeros_like(result)).transpose(1, 2)
    input_grad_causal_mat = result_upper + result_lower
    zero = torch.zeros_like(input_grad_causal_mat)
    input_grad_causal_mat = torch.where(
        input_grad_causal_mat < sparse_th, zero, input_grad_causal_mat)

    standard = torch.tensor(
        np.array(pd.read_csv(parms_path, index_col=None, header=None))
    ) + 1e-4
    err_score = torch.mean(
        torch.abs(input_grad_causal_mat - standard) / standard,
        dim=(1, 2)).numpy()
    labels = test_labels.numpy()
    smoothed, lab = simple_moving_average(err_score, labels, window_size=3)
    return np.array(smoothed), np.array(lab)


def point_scores(window_scores: np.ndarray, t: int, seq_len: int) -> np.ndarray:
    """窗口分数 → 逐点分数（覆盖全部 t 点）。"""
    pts = np.empty(t, dtype=np.float64)
    n = len(window_scores)
    for i in range(n):  # 窗口 i → 预测点 i+seq_len
        pts[i + seq_len] = window_scores[i]
    pts[:seq_len] = window_scores[0]  # 前端回填
    if n + seq_len < t:
        pts[n + seq_len:] = window_scores[-1]
    return pts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--channel", default="D-3")
    ap.add_argument("--name", default="smap_d3")
    ap.add_argument("--resume", action="store_true",
                    help="跳过训练与 causal parms，复用已有 best.pt/"
                         "causal_parms.csv 直接打分")
    args = ap.parse_args()
    device = torch.device(args.device)
    set_seed(args.seed)
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)

    build_channel_csv(args.channel)
    chan = args.channel
    t_test = len(np.load(WS / "data" / "SMAP" / chan / "test.npy"))
    lb = np.load(WS / "data" / "SMAP" / chan / "test_label.npy")

    dl = SwatDataLoader_AD(str(REPO / "dataset" / "SMAP" / chan),
                           CFG["batch_size"], CFG["seq_len"], CFG["pred_len"],
                           "M", target="OT")
    train_data = dl.get_train()
    val_data = dl.get_val()
    test_data = dl.get_test()
    print(f"[train {len(train_data)}批 val {len(val_data)}批 "
          f"test {len(test_data)}批] n_feature={dl.n_feature}", flush=True)

    model = TSMixerRevIN(input_shape=(CFG["seq_len"], dl.n_feature),
                         pred_len=CFG["pred_len"], dropout=CFG["dropout"],
                         n_block=CFG["n_block"], ff_dim=CFG["ff_dim"],
                         target_slice=dl.target_slice).to(device)
    ckpt_dir = REPO / "checkpoints" / args.name
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    save_path = ckpt_dir / "best.pt"
    parms_path = ckpt_dir / "causal_parms.csv"

    if args.resume:
        assert save_path.exists() and parms_path.exists(), \
            "--resume 但 best.pt/causal_parms.csv 不存在"
        print("[resume] 跳过训练与 causal parms，复用已有 checkpoint",
              flush=True)
    else:
        opt = torch.optim.Adam(model.parameters(), lr=CFG["learning_rate"])
        criterion = torch.nn.MSELoss()
        best_loss = torch.tensor(float("inf"))
        patience = 0
        t0 = time.monotonic()
        for epoch in range(CFG["train_epochs"]):
            model.train()
            train_mloss = torch.zeros(1, device=device)
            for i, (bx, by) in enumerate(train_data):
                bx, by = bx.to(device), by.to(device)
                out = model(bx)
                loss = criterion(out, by)
                opt.zero_grad()
                loss.backward()
                opt.step()
                train_mloss = (train_mloss * i + loss.detach()) / (i + 1)
            model.eval()
            val_mloss = torch.zeros(1, device=device)
            with torch.no_grad():
                for i, (bx, by) in enumerate(val_data):
                    bx, by = bx.to(device), by.to(device)
                    val_mloss = (val_mloss * i
                                 + criterion(model(bx), by)) / (i + 1)
            if val_mloss < best_loss:
                best_loss = val_mloss
                torch.save(deepcopy(model.state_dict()), save_path)
                patience = 0
            else:
                patience += 1
            print(f"[epoch {epoch+1}/{CFG['train_epochs']}] "
                  f"train={float(train_mloss):.4g} val={float(val_mloss):.4g}",
                  flush=True)
            if patience == CFG["patience"] or epoch >= CFG["train_epochs"] - 1:
                break
        print(f"[fit {time.monotonic()-t0:.0f}s]", flush=True)

        save_train_mean_causal(model, str(save_path), train_data, args.device,
                               parms_path=str(parms_path),
                               sparse_th=CFG["sparse_th"],
                               sample_p=CFG["sample_p"])
        print("[causal parms saved]", flush=True)

    t0 = time.monotonic()
    val_scores, _ = extract_scores(model, str(save_path), val_data, args.device,
                                   str(parms_path), CFG["sparse_th"])
    print(f"[val scores {len(val_scores)} {time.monotonic()-t0:.0f}s]",
          flush=True)
    t0 = time.monotonic()
    test_scores, _ = extract_scores(model, str(save_path), test_data,
                                    args.device, str(parms_path),
                                    CFG["sparse_th"])
    print(f"[test scores {len(test_scores)} {time.monotonic()-t0:.0f}s]",
          flush=True)

    out = WS / "results" / "gcad_d3_channel"
    out.mkdir(parents=True, exist_ok=True)
    np.save(out / "val_scores.npy", val_scores)
    np.save(out / "test_scores.npy", test_scores)

    all_scores = point_scores(test_scores, t_test, CFG["seq_len"])
    assert len(all_scores) == t_test == len(lb), "逐点分数长度不符"

    from sklearn.metrics import roc_auc_score
    auc = roc_auc_score(lb, all_scores)
    print(f"[sanity] D-3 点级 AUROC={auc:.4f}（记录 0.9506，论文 SMAP 0.953）",
          flush=True)

    tiers = ["q0.95", "q0.98", "q0.99", "q0.995", "q0.999", "q0.9999",
             "q0.99999", "max(val)"]
    qs = [0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 0.99999, None]
    print(f"{'档':<10}{'阈值':>10}{'TP':>6}{'FP':>6}{'FN':>6}{'TN':>6}"
          f"{'P':>8}{'R':>8}{'Acc':>8}", flush=True)
    rows = []
    for tier, q in zip(tiers, qs):
        th = float(np.max(val_scores)) if q is None else \
            float(np.quantile(val_scores, q))
        pred = (all_scores >= th).astype(np.int64)
        tp = int(((pred == 1) & (lb == 1)).sum())
        fp = int(((pred == 1) & (lb == 0)).sum())
        fn = int(((pred == 0) & (lb == 1)).sum())
        tn = int(((pred == 0) & (lb == 0)).sum())
        p = tp / max(tp + fp, 1)
        r = tp / max(tp + fn, 1)
        acc = (tp + tn) / len(lb)
        print(f"{tier:<10}{th:>10.4g}{tp:>6}{fp:>6}{fn:>6}{tn:>6}"
              f"{p:>8.3f}{r:>8.3f}{acc:>8.3f}", flush=True)
        rows.append(dict(tier=tier, th=th, tp=tp, fp=fp, fn=fn, tn=tn,
                         p=p, r=r, acc=acc))
    import json
    with open(out / "ladder.json", "w") as f:
        json.dump(dict(seed=args.seed, channel=chan, auc=auc, rows=rows),
                  f, indent=2)
    print("GCAD-D3-CHANNEL-DONE", flush=True)


if __name__ == "__main__":
    main()
