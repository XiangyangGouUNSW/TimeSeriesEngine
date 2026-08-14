"""P 端分析吞吐基准：模拟工厂实际负载，测单轮（取窗口 + 模型推理）真实耗时。

背景：C/S 端数据写入 + 约束检查 pipeline 吞吐 100k 条/s。P 端不逐条消费，
一次检测 = 吃一个实时窗口（window_size 行），间隔按数据频率动态排。判断
"跟不跟得上"看的是：单轮耗时 vs 窗口落满所需墙钟时间 + 推理 worker 聚合吞吐。

本脚本实测（不连真服务）：
  A. 各异常模型 detect() 单窗口耗时：window=100，序列数 N ∈ {1,4,8,16,32}；
  B. 预测 forecast() 单窗口耗时（PatchTST 生产参数 + Constant/CatBoost）；
  C. gRPC 取实时窗口耗时（真 GrpcCoreDataClient + 进程内假 C，含序列化）；
  D. 用实测值核算工厂容量：必需检测节奏、2 worker 聚合吞吐、能否跟得上。

用法（sfkg 环境，需先起 fake_core_server 供 C 段测量，若未起会自动跳过 C 段）：
  python tools/bench_throughput.py
"""

from __future__ import annotations

import sys
import time
from concurrent import futures
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated"), str(ROOT / "tools")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import grpc

import timeseries_core_pb2_grpc as core_pb_grpc
from fake_core_server import FakeCoreService
from grpc_client import GrpcCoreDataClient
from anomaly_models import (build_anomaly_model, KNOWN_METHODS)
from historical_matcher import HistoricalEventMatcher, HistoricalEvent
from patchtst_forecaster import PatchTSTForecaster
from catboost_forecaster import ConstantForecaster, CatBoostForecaster

WINDOW = 100          # 异常检测窗口行数（config inference.window_size）
SEQ_COUNTS = [1, 4, 8, 16, 32]
N_REPEAT = 20         # 每配置重复次数取中位数（扛调度抖动）


def _synthetic(n: int, n_seq: int, seed: int = 0):
    """平滑 + 噪声的连续多序列数据（形似工厂测点）。"""
    rng = np.random.default_rng(seed)
    t = np.arange(n)
    cols = []
    for s in range(n_seq):
        base = np.sin(t / (15.0 + s) + s) * 10.0 + 50.0 + 0.3 * s
        cols.append(base + 0.05 * rng.standard_normal(n))
    return np.stack(cols, axis=1).astype(np.float32)


def _med(fn, repeat=N_REPEAT):
    ts = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t0) * 1e3)
    ts.sort()
    return ts[len(ts) // 2]


def _prod_anomaly_cfg():
    import yaml
    with open(ROOT / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("anomaly", {}).get("gcad", {})


def bench_anomaly():
    print("=" * 74)
    print("A. 异常模型 detect() 单窗口耗时（window=100，单位 ms，中位数）")
    print("=" * 74)
    print(f"{'方法':<16}{'N=1':>8}{'N=4':>8}{'N=8':>8}{'N=16':>9}{'N=32':>9}")
    results = {}
    gcad = dict(_prod_anomaly_cfg())
    # 深度 GCAD 只测 detect 成本（fit 不计时）：轻量配置拿"能用"的权重，避免全量
    # fit 拖长基准（生产 fit 成本由 train_budget_s 控，见 bench 说明）。
    gcad["epochs"] = 2
    gcad["n_norm_samples"] = 64
    for method in sorted(KNOWN_METHODS):
        row = []
        for n_seq in SEQ_COUNTS:
            hist = _synthetic(600, n_seq, seed=1)
            win = _synthetic(WINDOW, n_seq, seed=2)
            if method == "HISTORICAL_MATCH":
                model = HistoricalEventMatcher(sequence_ids=[f"s{i}" for i in range(n_seq)])
                model.load_confirmed_events(
                    [HistoricalEvent(event_id=f"e{i}", event_type="ANOMALY",
                                     event_time_ms=i * 3600_000,
                                     sequence_ids=(f"s{i % n_seq}",))
                     for i in range(5)])
                model.fit(hist)
            else:
                extra = {}
                if method == "CAUSAL_PATTERN":
                    extra["target_index"] = n_seq - 1
                elif method == "MUTUAL_COUPLING":
                    extra["coupled_pairs"] = [(0, 1)] if n_seq >= 2 else []
                model = build_anomaly_model(
                    method, sequence_ids=[f"s{i}" for i in range(n_seq)],
                    gcad=gcad, **extra)
                model.fit(hist)
            row.append(round(_med(lambda: model.detect(win)), 2))
        results[method] = row
        print(f"{method:<16}" + "".join(f"{v:>8.2f}" for v in row))
    return results


def _prod_forecast_cfg():
    import yaml
    with open(ROOT / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    fm = cfg["forecast_model"]
    return fm


def bench_forecast():
    print()
    print("=" * 74)
    print("B. 预测 forecast() 单窗口耗时（PatchTST 生产参数 / Constant / CatBoost，ms）")
    print("=" * 74)
    fm = _prod_forecast_cfg()
    ctx, pred = fm["context_length"], fm["prediction_length"]
    print(f"{'N':<6}{'PatchTST(推理)':>16}{'Constant':>14}{'CatBoost(全链路)':>20}")
    for n_seq in SEQ_COUNTS:
        # PatchTST：生产参数，训 2 epoch 拿可用权重（推理耗时与训练轮数无关）
        fc = PatchTSTForecaster(
            sequence_ids=[f"s{i}" for i in range(n_seq)],
            context_length=ctx, prediction_length=pred,
            patch_size=fm["patch_size"], patch_stride=fm["patch_stride"],
            d_model=fm["d_model"], n_heads=fm["n_heads"], num_layers=fm["num_layers"],
            epochs=2, batch_size=fm["batch_size"],
            learning_rate=fm["learning_rate"],
        )
        hist = _synthetic(600, n_seq, seed=1)
        fc.fit(hist)
        win = _synthetic(ctx, n_seq, seed=2)
        t_patch = _med(lambda: fc.forecast(win), repeat=10)

        const = ConstantForecaster(sequence_ids=[f"s{i}" for i in range(n_seq)])
        t_const = _med(lambda: const.forecast(win))

        # CatBoost 只服务"因变量类离散"（int 码）序列：目标列改离散码、其余列标连续。
        disc_hist = hist.copy()
        disc_hist[:, -1] = np.round(50 + 5 * np.mean(disc_hist[:, :-1], axis=1)).astype(int)
        ck = {f"s{i}": "continuous" for i in range(n_seq)}
        ck[f"s{n_seq - 1}"] = "discrete"
        cat = CatBoostForecaster(sequence_ids=[f"s{i}" for i in range(n_seq)],
                                 target_sequence_id=f"s{n_seq - 1}",
                                 column_kinds=ck)
        try:
            cat.fit(disc_hist)
            win_cat = disc_hist[-cat.context_length:]
            t_cat = _med(lambda: cat.forecast(win_cat), repeat=10)
        except Exception as e:
            t_cat = float("nan")
        print(f"{n_seq:<6}{t_patch:>14.2f}{t_const:>14.2f}{t_cat:>18.2f}")


def bench_fetch():
    print()
    print("=" * 74)
    print("C. gRPC 取实时窗口耗时（真 client + 进程内假 C，含序列化，ms）")
    print("=" * 74)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    core_pb_grpc.add_TimeseriesCoreServiceServicer_to_server(
        FakeCoreService(str(ROOT / "data" / "ETT-small" / "ETTh1.csv")), server)
    port = server.add_insecure_port("0.0.0.0:0")
    server.start()
    client = GrpcCoreDataClient("localhost", port)
    ids = [f"ETTh1_{s}" for s in
           ("OT", "HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL")]
    pts = []
    try:
        for n in (1, 7):
            sub = ids[:n]
            t_w = _med(lambda: client.get_aligned_real_time_window(sub), repeat=30)
            t_h = _med(lambda: client.get_history(sub), repeat=5)
            pts.append((n, t_w))
            print(f"N={n:<3} 窗口取数 {t_w:>8.2f}   全历史训练取数 {t_h:>8.2f}")
    finally:
        server.stop(0)
    return pts


def capacity_model(anomaly_ms: dict[str, list[float]], fetch_pts: list[tuple[int, float]]):
    """D. 容量核算：100k 条/s 写入下，分析端跟不跟得上。

    核心量纲：
      - 每轮成本 C_round = gRPC 取窗口 + detect（窗口取数按序列数线性外推）；
      - 正常节奏要求：T_span = window_size ÷ 每序列速率（一个窗口落满所需墙钟）；
      - 单任务可行 ⇔ C_round ≤ T_span；聚合可行 ⇔ N_tasks / T_span ≤ infer_workers / C_round。
    """
    print()
    print("=" * 74)
    print("D. 工厂容量核算（写入 100k 条/s；稳定态 = 模型已训，只跑 detect/推理）")
    print("=" * 74)
    R = 100_000.0
    K = 2                                   # scheduler.infer_workers
    worst_detect = max(row[-1] for row in anomaly_ms.values())   # ~1.5ms
    # 窗口取数按序列数线性外推（实测点 N=1、N=7）：
    #   测量 = 假 C（Python 对齐，悲观上界）；真实 = C++ 对齐 + 序列化 + RTT（估 /10）
    import numpy as _np
    xs = _np.array([p[0] for p in fetch_pts]); ys = _np.array([p[1] for p in fetch_pts])
    slope, inter = _np.polyfit(xs, ys, 1)
    def fetch_ms(k): return max(0.0, slope * k + inter)

    print(f"（window={WINDOW} 行；最贵 detect ≈ {worst_detect:.1f}ms；")
    print(f"  取窗口 1 序列实测 {fetch_ms(1):.0f}ms、7 序列 {fetch_ms(7):.0f}ms，按序列数线性）")
    print()
    TICK_S = 10.0                             # scheduler.interval_seconds（硬性调度下限）
    print(f"（调度下限：producer 每 {TICK_S:.0f}s tick 一次 → 任务实际最多每 {TICK_S:.0f}s 跑一轮，")
    print(f"  动态 next_due < {TICK_S:.0f}s 的部分被吞掉）")
    print()
    print(f"{'场景':<22}{'每序列速率':>10}{'窗口跨度':>9}{'实际间隔':>10}{'单轮成本':>10}"
          f"{'负荷(需/能)':>14}")
    scenarios = [
        ("5000序列/50任务", R / 5000, 10),     # 大工厂低频
        ("1000序列/100任务", R / 1000, 10),    # 中频
        ("500序列/50任务", R / 500, 10),       # 中高频
        ("100序列/20任务", R / 100, 5),        # 关键测点高频
    ]
    for name, r, k in scenarios:
        span_s = WINDOW / r                   # 正常节奏（等一整个新窗口）
        n_tasks = int(name.split("/")[1].replace("任务", ""))
        eff_s = max(span_s, TICK_S)           # 实际间隔 = 取窗口跨度与调度下限的较大者
        c_round = fetch_ms(k) / 10 + worst_detect   # 真 C 取窗口（估）+ detect
        load = n_tasks / eff_s                # 轮/s
        cap = K / (c_round / 1e3)             # 轮/s
        note = "余量足" if load < 0.5 * cap else ("接近上限" if load < cap else "超算力")
        print(f"{name:<22}{r:>10.1f}{span_s:>7.1f}s{eff_s:>8.1f}s{c_round:>9.1f}ms"
              f"{load:>8.0f}/{cap:.0f} {note}")
    print()
    print("说明：")
    print("  1. 取窗口是成本大头（非模型）；真 C++ 对齐后取窗口约 RTT+序列化 → 估算列。")
    print("  2. 负荷被调度下限压住：10s tick → 最多 N_tasks/10 轮/s，2 worker 轻松覆盖。")
    print("  3. 想要数据率对齐的检测延迟（快数据快速查），须把 interval_seconds 调小到目标")
    print("     间隔以内（如 1s）；否则动态间隔只在慢数据（每序列 <10 条/s）真正生效。")


if __name__ == "__main__":
    a = bench_anomaly()
    bench_forecast()
    f = bench_fetch()
    capacity_model(a, f)
    print()
    print("基准完成。数值受本机影响，用于相对量级判断。")
