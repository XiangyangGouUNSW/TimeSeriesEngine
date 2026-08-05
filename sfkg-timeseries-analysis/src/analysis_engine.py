"""P 端分析引擎：把「收任务 → 查C → 训练 → 预测 → 调C检查 → 调S写」串起来。

框架版：内部逻辑简单（AR 模型 + 假约束检查 + S 写事件），
目标是跑通三方调用链、产生可见输出，证明通讯成功。
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_analysis_pb2 as pb        # P↔S 的消息（ForecastResult/AnomalyResult）
import timeseries_core_pb2 as cpb           # P↔C 的消息（AlignedWindowData）

from ar_model import AutoregressiveModel

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """把调用链串起来的引擎。"""

    def __init__(self, core_client, result_client, config: dict):
        self.core = core_client        # GrpcCoreDataClient（调 C 取数据/检查）
        self.sender = result_client    # AnalysisResultClient（调 S 写事件）
        self.cfg = config

    # ================= 预测链路 =================

    def run_forecast(self, task) -> pb.ForecastResult:
        now = int(time.time() * 1000)
        target_ids = list(task.target_sequence_ids)
        if not target_ids:
            return self._forecast_result(task, pb.ANALYSIS_STATUS_INVALID_REQUEST,
                                         "预测任务没有目标序列")
        target = target_ids[0]
        try:
            # ① 查 C 数据规模，够不够训练
            all_ids = list(dict.fromkeys(
                list(task.target_sequence_ids) + list(task.feature_sequence_ids)))
            scales = {s.sequence_id: s for s in self.core.get_sequence_data_scale(all_ids)}
            min_points = task.minimum_points or self.cfg["training"]["min_train_points"]
            count = scales.get(target).point_count if target in scales else 0
            logger.info(f"[engine] ①查数据规模：{target} 有 {count} 点（需要 {min_points}）")
            if count < min_points:
                return self._forecast_result(task, pb.ANALYSIS_STATUS_DATA_NOT_READY,
                                             f"数据不足：{target} 只有 {count} 点")

            # ② 拉历史训练 AR 模型
            start_ms = scales[target].start_time_ms
            end_ms = scales[target].end_time_ms
            cut_ms = start_ms + int((end_ms - start_ms) * self.cfg["training"]["train_ratio"])
            chunk = self.core.get_history([target], end_time_ms=cut_ms)
            series = [row[0] for row in chunk.values]
            model = AutoregressiveModel(order=self.cfg["training"]["ar_order"])
            model.fit(series)
            logger.info(f"[engine] ②训练 AR 完成（{len(series)} 个点）")

            # ③ 取最近窗口，预测未来
            window_size = self.cfg["inference"]["window_size"]
            window = self.core.get_aligned_window([target], window_size)
            history = [row[0] for row in window.values]
            horizon = task.forecast_horizon_steps or self.cfg["inference"]["horizon_steps"]
            preds = model.forecast(history, horizon)
            step_ms = 3600_000
            if len(window.timestamps_ms) >= 2:
                step_ms = window.timestamps_ms[-1] - window.timestamps_ms[-2]
            last_ts = window.timestamps_ms[-1]
            out_ts = [last_ts + step_ms * (i + 1) for i in range(horizon)]
            logger.info(f"[engine] ③预测 {target} 未来 {horizon} 步，前 3 个值 {[round(v,2) for v in preds[:3]]}")

            # ④ 调 C 约束检查（把预测值包成 AlignedWindowData 传给 C）
            aligned = self._build_aligned(target, out_ts, preds)
            satisfied, violations = self.core.check_constraints(
                task.task_id, aligned_data=aligned)
            logger.info(f"[engine] ④调 C 约束检查：satisfied={satisfied}，违规 {len(violations)} 条")

            # ⑤ 有风险 → 调 S 写预警
            if not satisfied and violations:
                ok = self.sender.send_event(
                    event_type=pb.ANOMALY_EVENT_TYPE_WARNING,
                    event_time_ms=out_ts[0],
                    sequence_ids=[target],
                    values=preds[:5],
                    severity=pb.SEVERITY_MEDIUM,
                    source=pb.ANOMALY_SOURCE_FORECAST,
                )
                logger.info(f"[engine] ⑤调 S 写预警：{'成功' if ok else '失败（S 未起/连不上）'}")

            return self._forecast_result(task, pb.ANALYSIS_STATUS_SUCCESS,
                                         f"预测完成（{horizon} 步，违规 {len(violations)} 条）",
                                         out_ts, [target], preds)
        except Exception as e:
            logger.info(f"[engine] 预测链路异常：{e}")
            return self._forecast_result(task, pb.ANALYSIS_STATUS_FAILED, f"预测失败：{e}")

    # ================= 异常链路 =================

    def run_anomaly(self, task) -> pb.AnomalyResult:
        now = int(time.time() * 1000)
        try:
            # ① 取 C 实时窗口
            window = self.core.get_real_time_window(list(task.sequence_ids))
            logger.info(f"[engine] ①取实时窗口：{len(window.sequences)} 条序列")

            # ② 调 C 约束检查
            satisfied, violations = self.core.check_constraints(
                task.task_id, sequence_ids=list(task.sequence_ids))
            logger.info(f"[engine] ②调 C 约束检查：satisfied={satisfied}，违规 {len(violations)} 条")

            # ③ 有异常 → 调 S 写异常
            if not satisfied and violations:
                ok = self.sender.send_event(
                    event_type=pb.ANOMALY_EVENT_TYPE_ANOMALY,
                    event_time_ms=now,
                    sequence_ids=list(task.sequence_ids),
                    values=[],
                    severity=pb.SEVERITY_HIGH,
                    source=pb.ANOMALY_SOURCE_CONSTRAINT_CHECK,
                )
                logger.info(f"[engine] ③调 S 写异常：{'成功' if ok else '失败（S 未起/连不上）'}")

            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_SUCCESS,
                message=f"异常检测完成，违规 {len(violations)} 条",
                findings=[], model_version="shell-v1")
        except Exception as e:
            logger.info(f"[engine] 异常链路异常：{e}")
            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_FAILED,
                message=f"异常检测失败：{e}", findings=[], model_version="")

    # ================= 工具 =================

    def _forecast_result(self, task, status, message,
                         timestamps=None, sequence_ids=None, values=None):
        return pb.ForecastResult(
            task_id=task.task_id, run_id=f"run-{int(time.time()*1000)}",
            generated_at_ms=int(time.time()*1000), status=status, message=message,
            timestamps_ms=timestamps or [], sequence_ids=sequence_ids or [],
            values=values or [], risk_findings=[], model_version="ar-shell-v1")

    def _build_aligned(self, target, timestamps, preds):
        """把预测值包成 C 端认识的 AlignedWindowData（供 checkConstraints）。"""
        samples = [
            cpb.AlignedSample(time=t, values=[
                cpb.AlignedValue(sequence_id=target,
                                 value=cpb.TimeseriesValue(double_value=v))
            ])
            for t, v in zip(timestamps, preds)
        ]
        return cpb.AlignedWindowData(
            window_start_time=timestamps[0] if timestamps else 0,
            window_end_time=timestamps[-1] if timestamps else 0,
            samples=samples,
        )
