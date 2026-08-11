"""离散序列预测专项测试（P1-6 + P1-5 + P1-4，不连服务，假 C 保留原始类型）。

覆盖：
  a) 数据推断路由：float 目标→PatchTST、int64 目标（有特征）→CatBoost，
     model_version 反映实际模型；
  b) 自变量类离散（无特征）→ ConstantForecaster：预测 = 窗口末值（保持当前值）；
  c) 因变量类离散（有特征）→ CatBoost：拟合 + 预测形状/有限/合理码域；
  d) store 版本 key 缓存：二次 run_forecast 不重训；新建 store 从磁盘重建
     CatBoostForecaster（验 loader 分发）可再预测；
  e) NaN 窗口注入：预测不崩、输出有限（P1-5 _clean_matrix）；
  f) string 目标/特征 → ANALYSIS_STATUS_NOT_IMPLEMENTED（干净结果非异常）；
  g) P1-4：数据不足 → DATA_NOT_READY 正常结果（非异常），needs_training 语义正确。

用法（sfkg 环境）：
  python tools/test_discrete_forecast.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from catboost_forecaster import CatBoostForecaster, ConstantForecaster
from core_client import raw_points_to_aligned
from data_types import AlignedWindow, HistoricalDataChunk, SequenceDataScale
from patchtst_forecaster import PatchTSTForecaster
from task_registry import TaskKind
from training_loop import ModelStore

_PASS = 0

# 快速训练配置：小窗口/少轮数，测试可秒级完成
CFG = {
    "forecast_model": {
        "context_length": 32, "prediction_length": 8,
        "patch_size": 8, "patch_stride": 4,
        "d_model": 16, "n_heads": 2, "num_layers": 1,
        "epochs": 1, "batch_size": 32, "learning_rate": 1e-3,
        "catboost": {"iterations": 50, "learning_rate": 0.05,
                     "depth": 4, "l2_leaf_reg": 3.0},
    },
    "training": {"train_ratio": 0.8},
    "inference": {"window_size": 100, "horizon_steps": 8},
}
HORIZON = 8


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def _make_data(n: int = 600, gap: tuple[int, int] | None = None):
    """确定性合成数据：连续特征 T、离散自变量 setpoint、离散因变量 pass_rate、
    纯连续 OT、字符串列 stage。gap=(start,count) 挖缺失点（对齐成 NaN）。"""
    rng = np.random.default_rng(0)
    t = np.arange(n)
    T = np.sin(t / 20.0) + 0.05 * rng.standard_normal(n)          # 连续特征（float）
    setpoint = np.tile([10, 20, 30], n // 3 + 1)[:n]              # 离散自变量（int 码）
    pass_rate = np.round(90 + 5 * T + 0.2 * setpoint).astype(int)  # 离散因变量（int 码）
    OT = np.cos(t / 15.0) * 10.0 + 40.0 + 0.1 * rng.standard_normal(n)  # 纯连续 float
    stage = np.tile(["A", "B", "C"], n // 3 + 1)[:n]              # 标签类离散（string）
    data = {"T": T.tolist(), "setpoint": setpoint.tolist(),
            "pass_rate": pass_rate.tolist(), "OT": OT.tolist(),
            "stage": stage.tolist()}
    return data, gap


class StubCore:
    """假 C 端：只实现预测面方法。get_history/get_aligned_real_time_window 经
    raw_points_to_aligned 保留原始 Python 类型（int/bool/float/str），缺失=NaN，
    这是数据推断路由成立的关键（和真 C gRPC 客户端一致）。"""

    def __init__(self, data: dict[str, list], window_rows: int = 500,
                 gap: tuple[int, int] | None = None):
        self._data = data
        self._window_rows = window_rows
        self._gap = gap
        self._t0 = 1_600_000_000_000
        self._step = 3_600_000
        self._n = len(next(iter(data.values())))
        self.check_calls = 0

    def _times(self):
        return [self._t0 + i * self._step for i in range(self._n)]

    def _points(self, ids, idx):
        pts = []
        for i in idx:
            if self._gap and self._gap[0] <= i < self._gap[0] + self._gap[1]:
                continue                 # 该时间点缺失 → 对齐成 NaN
            for sid in ids:
                pts.append((self._times()[i], sid, self._data[sid][i]))
        return pts

    def get_sequence_data_scale(self, ids):
        return [SequenceDataScale(sequence_id=s, point_count=self._n,
                                  start_time_ms=self._t0,
                                  end_time_ms=self._t0 + (self._n - 1) * self._step)
                for s in ids]

    def get_history(self, ids, start_time_ms=None, end_time_ms=None):
        idx = [i for i in range(self._n)
               if (start_time_ms is None or self._times()[i] >= start_time_ms)
               and (end_time_ms is None or self._times()[i] < end_time_ms)]
        ts, rows = raw_points_to_aligned(self._points(ids, idx), ids)
        return HistoricalDataChunk(timestamps_ms=ts, sequence_ids=ids, values=rows)

    def get_aligned_real_time_window(self, ids, window_rows=None):
        w = window_rows or self._window_rows
        idx = list(range(max(0, self._n - w), self._n))
        ts, rows = raw_points_to_aligned(self._points(ids, idx), ids)
        return AlignedWindow(timestamps_ms=ts, sequence_ids=ids, values=rows)

    def check_constraints(self, constraint_ids, aligned_data=None):
        self.check_calls += 1
        return True, []


def _make_engine(data, gap=None):
    core = StubCore(data, gap=gap)
    return core, AnalysisEngine(core_client=core, result_client=None, config=CFG,
                                model_store=ModelStore(
                                    model_dir=tempfile.mkdtemp(prefix="disc-test-")))


def _ftask(task_id: str, target: str, features=(), minimum_points: int = 60):
    return pb.ForecastTaskConfig(
        task_id=task_id,
        target_sequence_ids=[target],
        feature_sequence_ids=list(features),
        forecast_horizon_steps=HORIZON,
        minimum_points=minimum_points,
    )


# ============ a) 数据推断路由 + c) CatBoost 行为 + d) 缓存/磁盘持久化 ============

def test_routing_and_catboost() -> None:
    print("\n[数据推断路由 + CatBoost 行为 + 持久化]")
    data, _ = _make_data()

    # float 目标 → PatchTST
    core, eng = _make_engine(data)
    res = eng.run_forecast(_ftask("t-cont", "OT", features=["T"]))
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"连续目标应成功：{res.status}"
    assert res.model_version == "patchtst", f"float 目标应路由 PatchTST，实际 {res.model_version}"
    assert isinstance(eng.store.get("t-cont@v0"), PatchTSTForecaster)
    assert len(res.values) == HORIZON and all(np.isfinite(v) for v in res.values)
    _ok("float 目标 → PatchTST（model_version=patchtst）")

    # int64 目标（有特征）→ CatBoost
    core, eng = _make_engine(data)
    task = _ftask("t-disc", "pass_rate", features=["T", "setpoint"])
    res = eng.run_forecast(task)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"离散目标应成功：{res.status}"
    assert res.model_version == "catboost", f"int 目标应路由 CatBoost，实际 {res.model_version}"
    m = eng.store.get("t-disc@v0")
    assert isinstance(m, CatBoostForecaster), f"应是 CatBoostForecaster，实际 {type(m)}"
    assert len(res.values) == HORIZON, f"预测步数应 = {HORIZON}"
    assert all(np.isfinite(v) for v in res.values), "输出应全有限"
    assert all(75.0 <= v <= 115.0 for v in res.values), \
        f"条件期望应在合理码域：{res.values}"
    _ok("int64 目标（有特征）→ CatBoost：SUCCESS + 有限 + 合理码域")

    # d) 二次 run_forecast 不重训（store 内存命中同一对象）
    m1 = eng.store.get("t-disc@v0")
    res2 = eng.run_forecast(task)
    assert res2.status == pb.ANALYSIS_STATUS_SUCCESS
    assert eng.store.get("t-disc@v0") is m1, "二次调用应命中缓存对象，不重训"
    _ok("store 版本 key 缓存：二次调用不重训")

    # d) 新 store 从磁盘重建（验 loader 按 model_type 分发）
    eng2 = AnalysisEngine(core_client=StubCore(data), result_client=None, config=CFG,
                          model_store=ModelStore(model_dir=eng.store._model_dir))
    m2 = eng2.store.get("t-disc@v0")
    assert isinstance(m2, CatBoostForecaster), "磁盘重建应为 CatBoostForecaster"
    assert m2.model_type == "catboost"
    assert m2._model is not None and m2._patchtst is not None, "内部 PatchTST 应一并恢复"
    win = np.array(StubCore(data).get_aligned_real_time_window(
        ["pass_rate", "T", "setpoint"]).values[-32:], dtype=np.float32)
    out2 = m2.forecast(win, steps=HORIZON)
    assert all(np.isfinite(v) for v in out2["pass_rate"])
    _ok("新 store 从磁盘重建 CatBoostForecaster 可再预测（loader 分发）")


# ============ b) 自变量类离散 → 保持当前值 ============

def test_constant_forecast() -> None:
    print("\n[自变量类离散（无特征）→ 保持当前值]")
    data, _ = _make_data()
    core, eng = _make_engine(data)
    task = _ftask("t-const", "setpoint")
    res = eng.run_forecast(task)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS
    assert res.model_version == "constant"
    assert isinstance(eng.store.get("t-const@v0"), ConstantForecaster)
    assert len(res.values) == HORIZON
    assert res.values == [res.values[0]] * HORIZON, "自变量类离散预测应全相等（保持当前值）"
    assert res.values[0] == float(data["setpoint"][-1]), \
        "预测值应等于目标窗口末值（当前值）"
    _ok("离散目标无特征 → ConstantForecaster：全等 + 等于末值")


# ============ e) NaN 窗口注入 ============

def test_nan_window() -> None:
    print("\n[NaN 清洗（P1-5）：带缺失点的窗口预测不崩]")
    data, _ = _make_data(gap=(580, 10))          # 末 20 点中间挖 10 个缺失时间点
    core, eng = _make_engine(data)
    task = _ftask("t-nan", "pass_rate", features=["T", "setpoint"])
    res = eng.run_forecast(task)
    assert res.status == pb.ANALYSIS_STATUS_SUCCESS, f"NaN 窗口不应崩：{res.status}"
    assert all(np.isfinite(v) for v in res.values), "NaN 清洗后输出应全有限"
    _ok("历史+窗口带 NaN → 前值填充后预测成功、输出有限")


# ============ f) string 目标/特征 → NOT_IMPLEMENTED ============

def test_string_not_supported() -> None:
    print("\n[string（标签类离散）→ NOT_IMPLEMENTED]")
    data, _ = _make_data()

    core, eng = _make_engine(data)
    res = eng.run_forecast(_ftask("t-str-tgt", "stage", features=["T"]))
    assert res.status == pb.ANALYSIS_STATUS_NOT_IMPLEMENTED, \
        f"string 目标应 NOT_IMPLEMENTED，实际 {pb.AnalysisStatus.Name(res.status)}"
    assert "标签" in res.message or "字符串" in res.message
    _ok("string 目标 → NOT_IMPLEMENTED（干净结果非异常）")

    core, eng = _make_engine(data)
    res = eng.run_forecast(_ftask("t-str-feat", "pass_rate", features=["stage"]))
    assert res.status == pb.ANALYSIS_STATUS_NOT_IMPLEMENTED, \
        f"string 特征应 NOT_IMPLEMENTED，实际 {pb.AnalysisStatus.Name(res.status)}"
    _ok("string 特征 → NOT_IMPLEMENTED（防 np.array 崩）")


# ============ g) P1-4：数据不足 → DATA_NOT_READY 正常结果 ============

def test_data_not_ready() -> None:
    print("\n[数据不足 → DATA_NOT_READY 正常结果（P1-4）]")
    data, _ = _make_data(n=200)
    core, eng = _make_engine(data)
    task = _ftask("t-nodata", "OT", features=["T"], minimum_points=10000)
    assert eng.needs_training(task, TaskKind.FORECAST, 0) is True, "未训应 needs_training"
    res = eng.run_forecast(task)
    assert res.status == pb.ANALYSIS_STATUS_DATA_NOT_READY, \
        f"数据不足应返回 DATA_NOT_READY（正常结果非异常），实际 {pb.AnalysisStatus.Name(res.status)}"
    assert "数据不足" in res.message
    assert eng.store.get("t-nodata@v0") is None, "数据不足不应训练/存模型"
    _ok("数据不足 → DATA_NOT_READY 正常结果 + 不存模型")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_routing_and_catboost()
    test_constant_forecast()
    test_nan_window()
    test_string_not_supported()
    test_data_not_ready()
    print(f"\n离散序列预测专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
