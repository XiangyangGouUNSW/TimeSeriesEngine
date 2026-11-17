#!/usr/bin/env python
"""DBSCAN @ SMD machine-1-1 标签无关报警合并规则实验（师兄第三轮反馈落地）。

问题：口径 A（gap≤5 合并是评测协议）会把同一事件的多个分离报警段都算正确，
口径 C（一对一）全算误报——重复告警合并应该由系统规则决定，而不是由测试
标签事后决定。

方案：预先定义状态机（不看标签）：
  ① 最小持续时长 min_dur ∈ {1(关), 3, 5, 10}：短于 d 点的报警段丢弃（抑制抖动）；
  ② 冷却锁存 cooldown ∈ {5,...,1440}：告警关闭后 c 点内再报警并入同一告警
     （锁存语义，即"若系统带冷却 c 再合并"的本意——注意与 anomaly_seg_supplement
     的点抽稀语义不同，本脚本是段级锁存合并）；
  ③ 输出后按评测协议 gap≤5 合并。
共 4×10=40 个规则单元格，全部预定义、无标签参与。

评估（统一事件匹配，口径与补充脚本一致）：A 按命中事件的报警段计；
B 事件去重（每事件计 1 次命中 + 未命中任何事件的段各计 1 次误报，
分母 = matched + n_false）；C 一对一。报告段召回 R、误报段数、误报频率
（次/机器日，1 分钟采样）、告警跨度占比（防锁存饱和假象），逐口径判 90/85。

全程只调工厂代码（与 anomaly_seg_supplement.py 同一路径）。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np

FACTORY = Path.home() / "TimeSeriesEngine" / "sfkg-timeseries-analysis"
sys.path.insert(0, str(FACTORY / "tools"))
sys.path.insert(0, str(FACTORY / "src"))
sys.path.insert(0, str(FACTORY / "generated"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("dbscan-merge")

from evaluate_contract_benchmarks import (  # noqa: E402
    DATA_ROOT, DBSCAN_CFG, Entity, load_entities, intervals,
)

MIN_PER_DAY = 1440  # SMD machine-1-1 官方 1 分钟采样


def dbscan_flags(ent: Entity, fit_sample: int = 2000) -> np.ndarray:
    """与 eval_dbscan 完全相同的逐列检测路径，返回逐点 agg_hit。"""
    from anomaly_models import DbscanAnomalyModel
    test = ent.test
    n_time, n_seq = test.shape
    agg_hit = np.zeros(n_time, dtype=bool)
    CHUNK = 8192
    for c in range(n_seq):
        hist = ent.train[:, [c]]
        if len(hist) > fit_sample:
            idx = np.linspace(0, len(hist) - 1, fit_sample).astype(int)
            hist = hist[idx]
        m = DbscanAnomalyModel(**DBSCAN_CFG)
        m.fit(hist)
        norm_pts = np.asarray(m._normal_points, dtype=np.float64)
        for start in range(0, n_time, CHUNK):
            chunk = np.arange(start, min(start + CHUNK, n_time))
            mat = np.asarray(test[chunk][:, [c]], dtype=np.float64)
            d = np.linalg.norm(norm_pts[None] - mat[:, None],
                               axis=2).min(axis=1)
            agg_hit[chunk] = agg_hit[chunk] | (d > m.eps)
        if (c + 1) % 10 == 0 or c + 1 == n_seq:
            log.info("  dbscan 列 %d/%d 完成", c + 1, n_seq)
    return agg_hit


def merge_segs(segs: list[tuple[int, int]], gap: int = 5):
    out: list[tuple[int, int]] = []
    for s, e in segs:
        if out and s - out[-1][1] - 1 <= gap:
            out[-1] = (out[-1][0], int(e))
        else:
            out.append((int(s), int(e)))
    return out


def apply_rule(flags: np.ndarray, min_dur: int, cooldown: int):
    """状态机：先最小时长抑制，再冷却锁存，最后协议 gap≤5 合并。"""
    raw = [(int(s), int(e)) for s, e in intervals(flags.astype(np.int64))]
    # ① 最小持续时长：短段丢弃（抖动抑制）
    kept = [(s, e) for s, e in raw if e - s + 1 >= min_dur]
    if not kept:
        return []
    # ② 冷却锁存：相邻段间隔 ≤ cooldown 则并入前一段
    merged: list[tuple[int, int]] = []
    for s, e in kept:
        if merged and s - merged[-1][1] - 1 <= cooldown:
            merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))
    # ③ 评测协议 gap≤5 合并
    return merge_segs(merged, gap=5)


def one_to_one(labels: np.ndarray, pred_segs):
    """每个真事件至多被 1 个报警段命中；贪婪按报警段起点分配（口径 C）。"""
    true_events = intervals(labels)
    claimed = np.zeros(len(true_events), dtype=bool)
    n_claim = 0
    for s, e in sorted(pred_segs):
        for j, (ts, te) in enumerate(true_events):
            if not claimed[j] and s <= te and e >= ts:
                claimed[j] = True
                n_claim += 1
                break
    return n_claim, len(pred_segs)


def main() -> None:
    ents = load_entities("SMD", limit=None)
    ent = next(e for e in ents if e.name == "machine-1-1")
    tr, te, lb = ent.train, ent.test, ent.labels
    days = len(lb) / MIN_PER_DAY
    true_events = intervals(lb)
    print(f"== machine-1-1 测试 {len(lb)} 点 = {days:.2f} 机器日，"
          f"事件 {len(true_events)} 个", flush=True)

    flags = dbscan_flags(ent)
    print("[dbscan flags 完成]", flush=True)

    min_durs = (1, 3, 5, 10)
    cooldowns = (5, 10, 20, 60, 120, 240, 360, 480, 720, 1440)
    print(f"\n规则网格：min_dur×cooldown = {len(min_durs)}×"
          f"{len(cooldowns)} = {len(min_durs) * len(cooldowns)} 单元格",
          flush=True)
    hdr = (f"{'min_dur':>7} {'cool':>5} {'段数':>4} "
           f"{'P(A)':>6} {'P(B)':>6} {'P(C)':>6} {'R':>5} "
           f"{'误报段':>5} {'次/机器日':>9} {'跨度%':>6}")
    print(hdr, flush=True)

    pass_rows = {"A": [], "B": [], "C": []}
    for d in min_durs:
        for c in cooldowns:
            segs = apply_rule(flags, d, c)
            n = len(segs)
            if n == 0:
                print(f"{d:>7} {c:>5} {0:>4}   -     -     -    -     -"
                      f"     -    -", flush=True)
                continue
            hit_ev = [any(s <= te and e >= ts for s, e in segs)
                      for ts, te in true_events]
            matched = sum(hit_ev)
            hit_seg = [any(s <= te and e >= ts for ts, te in true_events)
                       for s, e in segs]
            n_false = sum(1 for h in hit_seg if not h)
            n_c, _ = one_to_one(lb, segs)
            # 口径 A：命中事件的报警段数 / 总段数
            pA = sum(hit_seg) / n
            # 口径 B（事件去重，与补充脚本一致）：每事件计 1 次命中，
            # 未命中任何事件的段各计 1 次误报 → 分母 = matched + n_false
            pB = matched / (matched + n_false)
            pC = n_c / n
            r = matched / len(true_events)
            fpr_day = n_false / days
            # 告警跨度占比 = 合并后段总跨度 / 测试长度（锁存语义，防饱和假象）
            span = 100 * sum(e - s + 1 for s, e in segs) / len(lb)
            print(f"{d:>7} {c:>5} {n:>4} {pA:>6.3f} {pB:>6.3f} "
                  f"{pC:>6.3f} {r:>5.2f} {n_false:>5} {fpr_day:>9.3f} "
                  f"{span:>6.2f}", flush=True)
            for key, p in (("A", pA), ("B", pB), ("C", pC)):
                if r >= 0.9 and p >= 0.85:
                    pass_rows[key].append((d, c, p, r, n, n_false))

    print("\n== 达标单元格（R≥0.9 且 P≥0.85）")
    for key in ("A", "B", "C"):
        rows = pass_rows[key]
        print(f"  口径 {key}：{len(rows)}/{len(min_durs) * len(cooldowns)}"
              f" 个单元格达标", flush=True)
        for d, c, p, r, n, fp in rows:
            print(f"    min_dur={d} cooldown={c}: P={p:.3f} R={r:.3f} "
                  f"段数={n} 误报段={fp}", flush=True)
    print("DBSCAN-MERGE-DONE", flush=True)


if __name__ == "__main__":
    main()
