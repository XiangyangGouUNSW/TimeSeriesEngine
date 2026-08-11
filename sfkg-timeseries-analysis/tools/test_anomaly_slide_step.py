"""slide_step_ms 每任务节流 + 窗口水位去重专项测试（不连服务，StubCore）。

覆盖：
  1. 首跑（无水位）正常检测，记录窗口水位；
  2. 窗口未推进 → 本轮跳过（SUCCESS + 跳过消息，**非 FAILED**）；
  3. 推进 < slide_step_ms → 仍跳过；
  4. 推进 ≥ slide_step_ms → 恢复检测；
  5. slide_step_ms=0（未配置）→ 永不跳过。

用 HISTORICAL_MATCH 方法：fit 是 no-op、空确认事件索引 → detect 无命中，
测的就是调度语义本身（跳过/运行），不掺模型行为。

用法（sfkg 环境）：
  python tools/test_anomaly_slide_step.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from core_client import CoreDataClient
from data_types import AlignedWindow, HistoricalDataChunk, SequenceDataScale
from training_loop import ModelStore

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


class StubCore(CoreDataClient):
    """窗口时间轴可推进：模拟 C 端新数据到达（初始 10 点、步长 1000ms）。"""

    def __init__(self) -> None:
        self.ts = [1000 * i for i in range(10)]

    def advance(self, n_ms: int) -> None:
        """C 端新数据到达：窗口最新时间往后推 n_ms。"""
        self.ts.append(self.ts[-1] + n_ms)

    def get_sequence_data_scale(self, sequence_ids):
        return [SequenceDataScale(sequence_id=sid, point_count=len(self.ts))
                for sid in sequence_ids]

    def get_history(self, sequence_ids, start_time_ms=None, end_time_ms=None):
        return HistoricalDataChunk(
            timestamps_ms=list(self.ts),
            sequence_ids=list(sequence_ids),
            values=[[1.0] * len(sequence_ids)] * len(self.ts),
            is_last_chunk=True)

    def get_aligned_real_time_window(self, sequence_ids):
        return AlignedWindow(
            timestamps_ms=list(self.ts),
            sequence_ids=list(sequence_ids),
            values=[[1.0] * len(sequence_ids)] * len(self.ts))


def _task(slide_ms: int) -> pb.AnomalyTaskConfig:
    return pb.AnomalyTaskConfig(
        task_id="t-slide", task_name="slide测试",
        sequence_ids=["s1"], methods=["HISTORICAL_MATCH"],
        slide_step_ms=slide_ms)


def test_slide_throttle() -> None:
    print("\n[slide_step_ms 每任务节流 + 水位去重]")
    core = StubCore()
    engine = AnalysisEngine(
        core_client=core, result_client=None,
        config={"inference": {"window_size": 10}},
        model_store=ModelStore(model_dir=tempfile.mkdtemp(prefix="slide-test-")))
    t = _task(slide_ms=2000)

    r1 = engine.run_anomaly(t)
    assert r1.status == pb.ANALYSIS_STATUS_SUCCESS
    assert "跳过" not in r1.message, "首跑（无水位）不应跳过"
    _ok("首跑正常检测，记录窗口水位")

    r2 = engine.run_anomaly(t)                 # 窗口未推进
    assert r2.status == pb.ANALYSIS_STATUS_SUCCESS, "跳过不是失败"
    assert "跳过" in r2.message, "窗口未推进应跳过"
    _ok("窗口未推进 → 本轮跳过（SUCCESS，非 FAILED）")

    core.advance(500)                          # 推进 500ms < 2000ms
    r3 = engine.run_anomaly(t)
    assert r3.status == pb.ANALYSIS_STATUS_SUCCESS
    assert "跳过" in r3.message, "推进不足应仍跳过"
    _ok("推进 500ms < slide_step_ms → 仍跳过")

    core.advance(2000)                         # 累计推进 2500ms ≥ 2000ms
    r4 = engine.run_anomaly(t)
    assert r4.status == pb.ANALYSIS_STATUS_SUCCESS
    assert "跳过" not in r4.message, "推进足够应恢复检测"
    _ok("推进 ≥ slide_step_ms → 恢复检测")

    # slide_step_ms=0（未配置）→ 不节流，每次都检测
    core0 = StubCore()
    engine0 = AnalysisEngine(
        core_client=core0, result_client=None,
        config={"inference": {"window_size": 10}},
        model_store=ModelStore(model_dir=tempfile.mkdtemp(prefix="slide0-test-")))
    t0 = _task(slide_ms=0)
    a = engine0.run_anomaly(t0)
    b = engine0.run_anomaly(t0)
    assert "跳过" not in a.message and "跳过" not in b.message, \
        "slide_step_ms=0 不应节流"
    _ok("slide_step_ms=0（未配置）→ 不节流，每次都检测")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_slide_throttle()
    print(f"\nslide_step_ms 专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
