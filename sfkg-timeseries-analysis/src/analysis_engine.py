"""P 端分析引擎：把「收任务 → 查C → 训练 → 预测 → 调C检查 → 调S写」串起来。

异常链路：methods 决定检测类型（KNOWN_METHODS 过滤，空 → DEFAULT_METHODS），
训练用历史、检测用 C 对齐后的实时窗口，逐点写 S 异常事件（不聚合）。
预测链路：数据推断路由目标类型（连续→PatchTST / 因变量离散→CatBoost /
自变量离散→保持当前值），实时窗口预测未来，包成 AlignedWindowData 调 C
checkConstraints，违规写 S 预警。
模型复用 ModelStore（内存+磁盘，config_version 进 key 失效重训，loader 按
model_type 分发）。训练只走历史，推理只走实时窗口（对齐归属 C）。
"""

from __future__ import annotations

import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import numpy as np

import timeseries_analysis_pb2 as pb        # P↔S 的消息（ForecastResult/AnomalyResult）
import timeseries_core_pb2 as cpb           # P↔C 的消息（AlignedWindowData）
import torch

from analysis_result_client import AnalysisResultClient
from anomaly_models import (
    GcadAnomalyModel,
    KNOWN_METHODS,
    MutualCouplingModel,
    build_anomaly_model,
)
from catboost_forecaster import (
    CatBoostForecaster,
    ConstantForecaster,
    UnsupportedTargetError,
)
from core_client import CoreDataClient
from data_types import HistoricalDataChunk, SequenceDataScale
from historical_matcher import HistoricalEvent, HistoricalEventMatcher
from patchtst_forecaster import PatchTSTForecaster
from project import scoped_key
from task_registry import TaskKind
from training_loop import ModelStore

logger = logging.getLogger(__name__)

# 异常检测类型：methods 空时的默认组合。实时约束检查已迁 C（P 不参与）；
# 2026-08-26 起默认三类全跑——模式偏移(GCAD)/离群点(DBSCAN)/趋势异常(TrendShift)，
# 对应合同验收「三类异常」口径（S 不配置 methods 时三类都检测）。
DEFAULT_METHODS = ["CAUSAL_PATTERN", "DISCRETE_OUTLIER", "TREND_SHIFT"]

# S 端 relation_type 规范取值（2026-08-13 定）：CAUSE / CAUSAL / CORRELATION /
# ASSOCIATION。因果型（有方向的因果边）用于 GCAD 候选结构门和互耦双向识别；
# CORRELATION/ASSOCIATION 是无向相关/关联，不做因果候选也不成互耦对。
_CAUSAL_TYPES = frozenset({"CAUSE", "CAUSAL"})
# 老自由字符串的互耦标记，兼容规范前下发的 relation_type
_MUTUAL_KEYWORDS = ("MUTUAL", "COUPLING", "BIDIRECTIONAL", "COUPLED")


def _is_missing(v: Any) -> bool:
    """判断值是否缺失（float NaN / None；string 不触发 np.isnan 报错）。"""
    if v is None:
        return True
    try:
        return bool(np.isnan(v))
    except (TypeError, ValueError):
        return False


class _SlideNotAdvanced(Exception):
    """窗口最新时间推进不足 slide_step_ms，本轮检测跳过（不是失败）。

    用异常做控制流：_run_anomaly_models 内部拿不到"跳过"的合适返回值，
    run_anomaly 捕获后转成 SUCCESS + 跳过消息（默认任务一定成功的语义）。
    """


class AnalysisEngine:
    """把调用链串起来的引擎。"""

    def __init__(self,
                 core_client: CoreDataClient,
                 result_client: AnalysisResultClient | None,
                 config: dict,
                 model_store: ModelStore | None = None,
                 historical_event_provider: Callable[[pb.AnomalyTaskConfig],
                                                     list[HistoricalEvent]] | None = None):
        self.core = core_client        # GrpcCoreDataClient（调 C 取数据/检查）
        self.sender = result_client    # AnalysisResultClient（调 S 写事件）
        self.cfg = config
        self.store = model_store or ModelStore()   # 模型复用缓存（内存+磁盘）
        self._historical_event_provider = historical_event_provider  # 确认历史事件源（框架预留）
        # 事件写入累计计数（峰值保护可观测）：异常/预测预警各分成功/失败 + 截断数。
        # 失败语义不做重试，报数代替重试——S 高峰被打爆时能从计数看到丢了多少。
        self._event_stats: dict[str, int] = {
            "anomaly_ok": 0, "anomaly_fail": 0, "anomaly_cap": 0,
            "forecast_ok": 0, "forecast_fail": 0,
        }
        # per-task 窗口水位 {scoped_key: 上次处理的窗口最新时间 ms}。
        # slide_step_ms 节流靠它判定"窗口推进了多少新数据"（防同一窗口重复检测）。
        self._anomaly_watermarks: dict[str, int] = {}
        # 预测任务动态运行间隔（负责人 08-13 定）：{scoped_key: 下次预测到期 epoch ms}。
        # 每次成功预测轮按 interval_ms = horizon × interval_percent × step_ms 排下一次
        # （step_ms = 实时窗口相邻时间戳差，frequency = 1/step_ms，即「步数 × 百分比 ÷
        # 频率」）；数据不足/不支持/失败轮不设 → 保持立即到期（默认周期重试）。
        # 只对预测任务生效，异常任务无 horizon，维持固定周期。多推理 worker 并发写，
        # 用锁保护。
        self._forecast_due: dict[str, int] = {}
        self._forecast_due_lock = threading.Lock()
        fm = self.cfg.get("forecast_model", {})
        self._forecast_interval_percent = float(fm.get("interval_percent", 0.7))
        # 异常任务动态检测间隔（与预测同构，负责人 08-13 定）：{scoped_key: 下次到期 epoch ms}。
        # interval = window_size × recheck_fraction ÷ 频率（frequency = 1/step_ms）——
        # 一次检测吃一个窗口，所以按"检测窗口的百分比"定间隔：无异常 → 等一整个新窗口
        # （recheck_fraction_normal=1.0）；有异常 → 等 5% 窗口（recheck_fraction_hot=0.05）
        # 盯住事件；连续 hot_confirm_clean_runs 轮无异常 → 回正常节奏（确认恢复防抖动）。
        # 数据不足/窗口未推进/失败轮不设 → 保持立即到期（默认周期重试）。
        self._anomaly_due: dict[str, int] = {}
        self._anomaly_due_lock = threading.Lock()
        self._anomaly_clean_streak: dict[str, int] = {}   # {scoped_key: 热状态中连续无异常轮数}
        self._anomaly_hot: dict[str, bool] = {}           # {scoped_key: 是否处于热节奏（盯事件）}
        am = self.cfg.get("anomaly", {})
        self._window_size = int(self.cfg.get("inference", {}).get("window_size", 100))
        self._anomaly_recheck_fraction_normal = float(am.get("recheck_fraction_normal", 1.0))
        self._anomaly_recheck_fraction_hot = float(am.get("recheck_fraction_hot", 0.05))
        self._anomaly_hot_confirm_clean_runs = int(am.get("hot_confirm_clean_runs", 2))
        self._train_locks: dict = {}   # {key: Lock}，同 key 并发训练只训一次
        self._train_locks_lock = threading.RLock()  # 保护 _train_locks
        self._register_model_loader()

    # ================= 预测链路 =================

    def run_forecast(self, task: pb.ForecastTaskConfig,
                     config_version: int = 0) -> pb.ForecastResult:
        now = int(time.time() * 1000)
        t0 = time.perf_counter()   # 本轮总耗时起点（联调看每轮吞吐）
        target_ids = list(task.target_sequence_ids)
        if not target_ids:
            return self._forecast_result(task, pb.ANALYSIS_STATUS_INVALID_REQUEST,
                                         "预测任务没有目标序列")
        target = target_ids[0]
        try:
            # ① 查 C 数据规模，够不够训练（目标 + 特征全列）
            all_ids = list(dict.fromkeys(
                list(task.target_sequence_ids) + list(task.feature_sequence_ids)))
            scales = {s.sequence_id: s
                      for s in self.core.get_sequence_data_scale(
                          all_ids, project_id=task.project_id)}
            need = self._min_train_points(task)
            counts = {sid: scales.get(sid).point_count if sid in scales else 0
                      for sid in all_ids}
            if any(counts[sid] < need for sid in all_ids):
                missing = [f"{sid}={counts[sid]}" for sid in all_ids
                           if counts[sid] < need]
                return self._forecast_result(task, pb.ANALYSIS_STATUS_DATA_NOT_READY,
                                             f"数据不足：{'，'.join(missing)}（需要 {need}）")

            # ② 预测模型复用：缓存命中就跳过训练，否则训练并保存
            _t = time.perf_counter()
            model = self._get_or_train_forecaster(task, all_ids, scales,
                                                  config_version)
            train_ms = (time.perf_counter() - _t) * 1e3

            # ③ 取实时窗口（对齐成矩阵）预测未来。训练才用历史，推理一律实时窗口
            ctx = self._context_length(task)
            _t = time.perf_counter()
            window = self.core.get_aligned_real_time_window(
                all_ids, project_id=task.project_id)
            fetch_ms = (time.perf_counter() - _t) * 1e3
            times = window.timestamps_ms[-ctx:]
            matrix = self._clean_matrix(np.array(window.values[-ctx:], dtype=np.float32))
            horizon = task.forecast_horizon_steps or self.cfg["inference"]["horizon_steps"]
            _t = time.perf_counter()
            pred_map = model.forecast(matrix, steps=horizon)
            pred_ms = (time.perf_counter() - _t) * 1e3
            preds = pred_map[target]
            step_ms = 3600_000
            if len(times) >= 2:
                step_ms = times[-1] - times[-2]
            last_ts = times[-1]
            out_ts = [last_ts + step_ms * (i + 1) for i in range(horizon)]
            # 预测成功 → 按「horizon × interval_percent ÷ 频率」排下一次预测的到期时间
            # （动态间隔，只对成功预测轮生效；数据不足/失败轮不设，保持默认周期）
            self._record_forecast_due(task.project_id, task.task_id, now, horizon, step_ms)
            logger.info(f"[engine] ③预测 {target} 未来 {horizon} 步，前 3 个值 {[round(v,2) for v in preds[:3]]}")

            # ④ 调 C 约束检查（把预测值包成 AlignedWindowData 传给 C）
            constraint_ids = list(task.semantic_context.constraint_ids)
            logger.info("[engine] 任务 %s 收到的 constraint_ids = %s",
                        task.task_id, constraint_ids)
            aligned = self._build_aligned(target, out_ts, preds)
            _t = time.perf_counter()
            satisfied, violations = self.core.check_constraints(
                constraint_ids, aligned_data=aligned,
                project_id=task.project_id)
            check_ms = (time.perf_counter() - _t) * 1e3
            logger.info(f"[engine] ④调 C 约束检查：satisfied={satisfied}，违规 {len(violations)} 条")

            # ⑤ 有风险 → 调 S 写预警
            warn_note = ""
            if not satisfied and violations:
                ok = self.sender.send_event(
                    task_id=task.task_id,
                    project_id=task.project_id,
                    event_type=pb.ANOMALY_EVENT_TYPE_WARNING,
                    event_time_ms=out_ts[0],
                    sequence_ids=[target],
                    values=preds[:5],
                    # S 端 ForecastTaskConfig 无 warning_rule 字段（只有 AnomalyTaskConfig
                    # 有），预测预警固定默认 MEDIUM。异常事件才映射任务等级名。
                    severity=pb.SEVERITY_MEDIUM,
                    source=pb.ANOMALY_SOURCE_FORECAST,
                )
                # 失败不重试（失败语义拍板），只计数可观测（事件写入峰值保护）
                if ok:
                    self._event_stats["forecast_ok"] += 1
                else:
                    self._event_stats["forecast_fail"] += 1
                    warn_note = "；写 S 预警失败"
                    self._log_event_stats()
                logger.info(f"[engine] ⑤调 S 写预警：{'成功' if ok else '失败（S 未起/连不上）'}")

            round_ms = (time.perf_counter() - t0) * 1e3
            logger.info("[engine] 预测任务 %s 完成：总 %.1fms（训练 %.1fms / 取窗口 "
                        "%.1fms / 预测 %.1fms / 约束检查 %.1fms）",
                        task.task_id, round_ms, train_ms, fetch_ms, pred_ms, check_ms)
            return self._forecast_result(task, pb.ANALYSIS_STATUS_SUCCESS,
                                         f"预测完成（{horizon} 步，违规 {len(violations)} 条）{warn_note}",
                                         out_ts, [target], preds,
                                         model_version=getattr(model, "model_type",
                                                               "patchtst-shell-v1"))
        except UnsupportedTargetError as e:
            logger.info(f"[engine] 预测不支持：{e}")
            return self._forecast_result(task, pb.ANALYSIS_STATUS_NOT_IMPLEMENTED, str(e))
        except Exception as e:
            logger.info(f"[engine] 预测链路异常：{e}")
            return self._forecast_result(task, pb.ANALYSIS_STATUS_FAILED, f"预测失败：{e}")

    # ================= 预测任务动态运行间隔（next_due）=================

    def forecast_due_epoch(self, project_id: str, task_id: str) -> int:
        """任务下一次预测的到期时间（epoch ms）。0 = 立即到期。

        新任务 / 未设（数据不足、失败轮）/ 异常任务 → 返回 0，调度器不门控。
        """
        with self._forecast_due_lock:
            return self._forecast_due.get(scoped_key(project_id, task_id), 0)

    def reset_forecast_due(self, project_id: str, task_id: str) -> None:
        """解除预测门控：模型需重训/版本变化 → 任务立即恢复可运行。

        调度器在 needs_training 为 True 时调用，保证重训完成立刻出新一轮预测，
        不等旧的动态间隔（否则新配置/新知识要等一个周期才生效）。
        """
        with self._forecast_due_lock:
            self._forecast_due.pop(scoped_key(project_id, task_id), None)

    def _record_forecast_due(self, project_id: str, task_id: str, now_ms: int,
                             horizon: int, step_ms: int) -> None:
        """成功预测后按「预测周期 × 百分比」排下一次运行。

        interval_ms = horizon × interval_percent × step_ms（step_ms 每步毫秒数，
        frequency = 1/step_ms，即「步数 × 百分比 ÷ 频率」）。只对真正产出预测值的
        轮设置未来到期；数据不足/不支持/失败轮不调此方法 → 任务保持立即到期，
        节流只对成功预测生效。
        """
        if horizon > 0 and step_ms and step_ms > 0:
            interval_ms = int(horizon * self._forecast_interval_percent * step_ms)
            with self._forecast_due_lock:
                self._forecast_due[scoped_key(project_id, task_id)] = now_ms + interval_ms

    # ================= 异常任务动态检测间隔（next_due）=================

    def anomaly_due_epoch(self, project_id: str, task_id: str) -> int:
        """任务下一次异常检测的到期时间（epoch ms）。0 = 立即到期。

        新任务 / 未设（数据不足、失败轮）/ 预测任务 → 返回 0，调度器不门控。
        """
        with self._anomaly_due_lock:
            return self._anomaly_due.get(scoped_key(project_id, task_id), 0)

    def reset_anomaly_due(self, project_id: str, task_id: str) -> None:
        """解除异常门控：模型需重训/版本变化 → 任务立即恢复可运行（同步清干净轮数与热标记）。"""
        with self._anomaly_due_lock:
            key = scoped_key(project_id, task_id)
            self._anomaly_due.pop(key, None)
            self._anomaly_clean_streak.pop(key, None)
            self._anomaly_hot.pop(key, None)

    def _record_anomaly_due(self, project_id: str, task_id: str, now_ms: int,
                            n_findings: int, step_ms: int) -> None:
        """按「检测窗口 × 百分比」排下一次异常检测（与预测同构）。

        interval_ms = window_size × recheck_fraction × step_ms（一次检测吃一个窗口）：
          - 无异常（n_findings==0）：正常节奏 = 等一整个新窗口；
          - 有异常：热节奏 = 等 5% 窗口（盯住事件）；连续 hot_confirm_clean_runs 轮
            无异常 → 回正常节奏（确认恢复，防"异常→干净→正常→又异常"抖动）；
          - step_ms = 实时窗口相邻时间戳差（frequency = 1/step_ms）。
        数据不足/窗口未推进/失败轮不调此方法 → 保持立即到期（默认周期重试）。
        """
        if not (step_ms and step_ms > 0):
            return
        key = scoped_key(project_id, task_id)
        if n_findings > 0:
            # 有异常 → 热节奏盯住；任务进入热状态（标记用于防抖确认）
            self._anomaly_hot[key] = True
            self._anomaly_clean_streak.pop(key, None)
            fraction = self._anomaly_recheck_fraction_hot
        elif self._anomaly_hot.get(key):
            # 热状态中连续干净 → 计数；达到确认轮数 → 回正常节奏（确认恢复防抖动）。
            # 从未热过的任务不走这里 → 直接正常节奏（不把"新任务首轮"当热后恢复）。
            streak = self._anomaly_clean_streak.get(key, 0) + 1
            self._anomaly_clean_streak[key] = streak
            if streak >= self._anomaly_hot_confirm_clean_runs:
                self._anomaly_hot.pop(key, None)
                self._anomaly_clean_streak.pop(key, None)
                fraction = self._anomaly_recheck_fraction_normal
            else:
                fraction = self._anomaly_recheck_fraction_hot
        else:
            fraction = self._anomaly_recheck_fraction_normal
        interval_ms = int(self._window_size * fraction * step_ms)
        with self._anomaly_due_lock:
            self._anomaly_due[key] = now_ms + interval_ms

    # ================= PatchTST 预测模型复用 =================

    def _register_model_loader(self) -> None:
        """磁盘缓存加载：按存盘负载的 model_type 分发。

        catboost/constant → 各自 load_dict；无 model_type 的旧文件（legacy PatchTST）
        回退到 PatchTSTForecaster.load。
        """

        def loader(key: str, path):
            ckpt = torch.load(path, map_location="cpu")
            mtype = ckpt.get("model_type")
            if mtype == "catboost":
                fc = CatBoostForecaster(sequence_ids=ckpt["sequence_ids"],
                                        target_sequence_id=ckpt["target_sequence_id"])
                fc.load_dict(ckpt)
                return fc
            if mtype == "constant":
                fc = ConstantForecaster(sequence_ids=ckpt["sequence_ids"],
                                        target_sequence_id=ckpt.get("target_sequence_id"))
                fc.load_dict(ckpt)
                return fc
            if mtype == "historical-match":
                fc = HistoricalEventMatcher()
                fc.load_dict(ckpt)
                return fc
            if mtype == "gcad":
                fc = GcadAnomalyModel()
                fc.load_dict(ckpt)
                return fc
            if mtype == "mutual-coupling":
                fc = MutualCouplingModel()
                fc.load_dict(ckpt)
                return fc
            fc = PatchTSTForecaster(sequence_ids=[])
            fc.load(path)
            return fc

        self.store.set_loader(loader)

    def _min_train_points(self, task: pb.ForecastTaskConfig) -> int:
        """训练最少点数 = context + prediction（用配置），任务给了 minimum_points 优先。"""
        if task.minimum_points and task.minimum_points > 0:
            return task.minimum_points
        f = self.cfg.get("forecast_model", {})
        return (int(f.get("context_length", 96))
                + int(f.get("prediction_length", 24)))

    def _context_length(self, task: pb.ForecastTaskConfig) -> int:
        # ForecastTaskConfig 无 context_length（那是 AnomalyTaskConfig 的），用 getattr 兜底
        ctx = getattr(task, "context_length", None) or 0
        if ctx > 0:
            return ctx
        return int(self.cfg.get("forecast_model", {}).get("context_length", 96))

    def _forecast_key(self, task, ver: int,
                      knowledge_version: str = "") -> str:
        """预测模型缓存 key：项目 + 版本 + 语义知识版本进 key，任一变 → 必然重训。"""
        return f"{scoped_key(task.project_id, task.task_id)}@v{ver}{self._kv_part(knowledge_version)}"

    def _anomaly_key(self, task, method: str, ver: int,
                     knowledge_version: str = "") -> str:
        """异常模型缓存 key：项目 + 版本 + 语义知识版本进 key，任一变 → 必然重训。"""
        return f"{scoped_key(task.project_id, task.task_id)}:{method}@v{ver}{self._kv_part(knowledge_version)}"

    @staticmethod
    def _kv_part(knowledge_version: str) -> str:
        """语义知识版本进 key 的安全片段。空 → 不参与 key（兼容旧缓存文件）；
        非空 → `:k<版本>`。知识版本可能是 "2026-08-11-v3" 之类，只留安全字符防
        污染文件名（ModelStore 用 key 落盘）。"""
        kv = (knowledge_version or "").strip()
        if not kv:
            return ""
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", kv)[:32]
        return f":k{safe}"

    @staticmethod
    def _knowledge_version(task) -> str:
        """任务语义上下文的 knowledge_version（S 更新语义知识后递增 → 触发重训）。"""
        return getattr(task.semantic_context, "knowledge_version", "") or ""

    def _get_or_train_forecaster(
            self, task: pb.ForecastTaskConfig, all_ids: list[str],
            scales: list[SequenceDataScale],
            config_version: int = 0
    ) -> PatchTSTForecaster | CatBoostForecaster | ConstantForecaster:
        """缓存命中返回模型；未命中训练 PatchTST 并存入 store。

        版本进 key：{task_id}@v{config_version}。版本变 → key 变 → 必然重训，
        天然免疫「旧版本在飞训练覆盖新版本 key」的竞态。同 key 并发到达时
        拿 per-task 锁 double-check，只训一次。
        """
        key = self._forecast_key(task, config_version,
                                 self._knowledge_version(task))
        model = self.store.get(key)
        if model is not None:
            logger.info(f"[engine] ②命中预测模型缓存 task_id={key}，跳过训练")
            return model

        # 未命中：拿 per-task 锁，再确认一次（防并发重复训练，训练在锁内串行）
        with self._train_lock(key):
            model = self.store.get(key)
            if model is not None:
                logger.info(f"[engine] ②等待后命中缓存 task_id={key}，跳过训练")
                return model

            # 拉多元历史训练（前 train_ratio，再按 max_train_points 截最近 N 点）
            start_ms = min(s.start_time_ms for s in scales.values()
                           if s.start_time_ms is not None)
            end_ms = max(s.end_time_ms for s in scales.values()
                         if s.end_time_ms is not None)
            cut_ms = start_ms + int((end_ms - start_ms)
                                    * self.cfg["training"]["train_ratio"])
            start_ms = self._cap_train_start(start_ms, cut_ms, scales)
            chunk = self.core.get_history(all_ids, start_time_ms=start_ms,
                                          end_time_ms=cut_ms,
                                          project_id=task.project_id)

            # 数据推断路由（#6）：先看原始取值类型再转 float——chunk.values 保留
            # C 端原始 Python 类型（int/bool/float/string），缺失=NaN，类型不丢。
            col_kinds = self._infer_column_kinds(chunk)
            offender = next((s for s, k in col_kinds.items() if k == "string"), None)
            if offender is not None:
                raise UnsupportedTargetError(
                    f"序列 {offender} 为标签类离散（字符串），暂不支持预测")
            target = task.target_sequence_ids[0]
            target_kind = col_kinds.get(target, "continuous")
            history = self._clean_matrix(np.array(chunk.values, dtype=np.float32))
            logger.info("[engine] ②训练预测模型：%d 行 × %d 列，目标 %s=%s",
                        len(history), len(all_ids), target, target_kind)

            f = self.cfg.get("forecast_model", {})
            if target_kind == "discrete" and not task.feature_sequence_ids:
                # 自变量类离散（[54]）：无特征 → 保持当前值（预测 = 窗口末值，
                # 用户 08-13 确认不做计划值查询接口，直接用过去值）。
                fc = ConstantForecaster(sequence_ids=all_ids,
                                        target_sequence_id=target)
            elif target_kind == "discrete":
                # 因变量类离散（[56]）：有特征 → CatBoost 条件期望
                fc = CatBoostForecaster(
                    sequence_ids=all_ids, target_sequence_id=target,
                    column_kinds=col_kinds,
                    context_length=int(f.get("context_length", 96)),
                    prediction_length=int(f.get("prediction_length", 24)),
                    patch_size=int(f.get("patch_size", 16)),
                    patch_stride=int(f.get("patch_stride", 8)),
                    d_model=int(f.get("d_model", 64)),
                    n_heads=int(f.get("n_heads", 4)),
                    num_layers=int(f.get("num_layers", 2)),
                    epochs=int(f.get("epochs", 20)),
                    batch_size=int(f.get("batch_size", 64)),
                    learning_rate=float(f.get("learning_rate", 1e-3)),
                    catboost_params=dict(f.get("catboost", {})),
                )
            else:
                fc = PatchTSTForecaster(
                    sequence_ids=all_ids,
                    context_length=int(f.get("context_length", 96)),
                    prediction_length=int(f.get("prediction_length", 24)),
                    patch_size=int(f.get("patch_size", 16)),
                    patch_stride=int(f.get("patch_stride", 8)),
                    d_model=int(f.get("d_model", 64)),
                    n_heads=int(f.get("n_heads", 4)),
                    num_layers=int(f.get("num_layers", 2)),
                    epochs=int(f.get("epochs", 20)),
                    batch_size=int(f.get("batch_size", 64)),
                    learning_rate=float(f.get("learning_rate", 1e-3)),
                )
            fc.fit(history)
            self.store.save(key, fc)
            return fc

    def _train_lock(self, key: str) -> threading.Lock:
        """取 key 对应的训练锁（惰性创建）。"""
        with self._train_locks_lock:
            lock = self._train_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._train_locks[key] = lock
            return lock

    def invalidate_task(self, project_id: str, task_id: str,
                        keep_version: int | None = None) -> None:
        """清理任务旧版本模型（保留最近 2 版，见 ModelStore.invalidate_task）。

        keep_version=None 全删（任务删除）。顺带 prune _train_locks，
        防其随版本数缓慢增长。复合键前缀只清本 project 的任务，不误伤他项目。
        """
        self.store.invalidate_task(project_id, task_id, keep_version)
        with self._train_locks_lock:
            prefix = scoped_key(project_id, task_id)
            stale = [k for k in self._train_locks
                     if k.startswith(f"{prefix}:") or k.startswith(f"{prefix}@v")]
            for k in stale:
                self._train_locks.pop(k, None)

    # ================= 异常链路 =================

    def run_anomaly(self, task: pb.AnomalyTaskConfig,
                    config_version: int = 0) -> pb.AnomalyResult:
        now = int(time.time() * 1000)
        t0 = time.perf_counter()   # 本轮总耗时起点（联调看每轮吞吐）
        findings = []   # 收集异常（模型检测）

        # 检测类型显式化：methods 决定检什么（异常约束检查已迁 C，P 只做模型方法）。
        # 与 needs_training 同一过滤（KNOWN_METHODS）：保证「needs_training=False →
        # 一定不会走到数据门槛」，纯未知方法/只约束任务永不进训练队列也不查数据规模。
        methods = list(task.methods) or DEFAULT_METHODS
        model_methods = [m for m in methods
                         if m != "CONSTRAINT_CHECK" and m in KNOWN_METHODS]
        logger.info("[engine] 任务 %s 检测类型：methods=%s → 模型=%s",
                    task.task_id, methods, model_methods)
        try:
            # ① 数据门槛（按方法）：各方法有各自的训练数据门槛（config 可调）。
            #    未就绪的方法：数据够 → 本轮训练+检测；不够 → 推迟（不训练不检测），
            #    等数据增长后自动开始。已就绪的方法只检测、不查数据规模（纯推理）。
            #    本轮一个方法都跑不了 → DATA_NOT_READY；能跑一部分 → SUCCESS + 推迟说明。
            #    查 C 数据规模失败 → 走外层 except 报 FAILED（与预测链路一致）。
            run_methods: list[str] = []
            deferred: list[str] = []
            kv = self._knowledge_version(task)
            if model_methods:
                if any(not self.store.is_ready(
                        self._anomaly_key(task, m, config_version, kv))
                        for m in model_methods):
                    all_ids = list(task.sequence_ids)
                    scales = {s.sequence_id: s
                              for s in self.core.get_sequence_data_scale(
                                  all_ids, project_id=task.project_id)}
                    counts = {sid: scales.get(sid).point_count if sid in scales else 0
                              for sid in all_ids}
                    for m in model_methods:
                        if self.store.is_ready(
                                self._anomaly_key(task, m, config_version, kv)):
                            run_methods.append(m)        # 已就绪：只检测，不卡门槛
                            continue
                        if m == "HISTORICAL_MATCH":
                            # 语义事件异常双门槛：点数（默认 0 不卡）+ 确认事件数
                            # （技术方案 [43]：事件库达到一定规模才能启用）。
                            reasons = self._historical_match_gate_reasons(
                                task, counts, all_ids)
                            if reasons:
                                deferred.extend(reasons)
                            else:
                                run_methods.append(m)
                            continue
                        need = self._anomaly_min_points(task, m)
                        if all(counts[sid] >= need for sid in all_ids):
                            run_methods.append(m)
                        else:
                            missing = [f"{sid}={counts[sid]}" for sid in all_ids
                                       if counts[sid] < need]
                            deferred.append(f"{m}（{'，'.join(missing)} 需要 {need}）")
                else:
                    run_methods = list(model_methods)

                if not run_methods:
                    return pb.AnomalyResult(
                        task_id=task.task_id, run_id=f"run-{now}",
                        generated_at_ms=now,
                        status=pb.ANALYSIS_STATUS_DATA_NOT_READY,
                        message=f"数据不足：{'；'.join(deferred)}（暂不训练不检测）",
                        findings=[], model_version="")

                # ② 模型检测（模式偏离异常；无模型方法则跳过）。
                #    推理输入 = 实时窗口对齐矩阵（见 _run_anomaly_models ③）；训练才用历史。
                detected, step_ms = self._run_anomaly_models(task, run_methods,
                                                             config_version)
                for f in detected:
                    finding = pb.AnomalyFinding(
                        anomaly_type=f["anomaly_type"],
                        # 任务级等级名优先（S 定的），模型默认 severity 兜底
                        severity=task.warning_rule or f["severity"],
                        description=f["description"])
                    if f.get("score") is not None:
                        finding.score = f["score"]
                    if f.get("time") is not None:
                        finding.detected_time_ms = f["time"]
                    findings.append(finding)
                # 本轮真实跑了检测 → 按「检测窗口 × 百分比」排下一次（有异常→热节奏盯住）
                self._record_anomaly_due(task.project_id, task.task_id, now,
                                         len(findings), step_ms)

            # ③ 逐点写异常（老师确认 08-10）：一个异常点一个事件，不聚合。
            #    每个点用自己的时间戳（模型取 index→窗口时间，兜底 now）。
            #    峰值保护（技术方案 §11.2 之外的生产加固）：每任务每轮最多写 cap 条，
            #    逐点写不聚合、超上限只能截断（合并事件会丢"逐点"语义）；发送失败不重试
            #    （失败语义拍板），只计数进结果 message 可观测。
            cap = self._event_write_cap()
            sent_ok = sent_fail = capped = 0
            for i, f in enumerate(findings):
                if i >= cap:
                    capped += 1
                    continue
                ts = getattr(f, "detected_time_ms", None) or now
                ok = self.sender.send_event(
                    task_id=task.task_id,
                    project_id=task.project_id,
                    event_type=pb.ANOMALY_EVENT_TYPE_ANOMALY,
                    event_time_ms=ts,
                    sequence_ids=list(task.sequence_ids),
                    values=[f.score] if f.score is not None else [],
                    severity=self._resolve_severity(task.warning_rule,
                                                    pb.SEVERITY_HIGH),
                    source=pb.ANOMALY_SOURCE_MODEL_ANOMALY_DETECTION,
                )
                if ok:
                    sent_ok += 1
                    self._event_stats["anomaly_ok"] += 1
                else:
                    sent_fail += 1
                    self._event_stats["anomaly_fail"] += 1
                    logger.info("[engine] ③逐点写 S 异常失败：点 %s（S 未起/连不上）", ts)
            if capped:
                self._event_stats["anomaly_cap"] += capped
                logger.info("[engine] 任务 %s 单轮写事件超上限：截断 %d 条（cap=%d）",
                            task.task_id, capped, cap)
            if sent_fail or capped:
                self._log_event_stats()

            note = (f"；推迟：{'；'.join(deferred)}（数据不足，待增长后自动开始）"
                    if deferred else "")
            write_note = ""
            if sent_fail or capped:
                write_note = (f"；写 S 成功 {sent_ok} 条"
                              + (f"，失败 {sent_fail} 条" if sent_fail else "")
                              + (f"，超上限截断 {capped} 条" if capped else ""))
            round_ms = (time.perf_counter() - t0) * 1e3
            logger.info("[engine] 异常任务 %s 完成：总 %.1fms，检出 %d 条，"
                        "写 S 成功 %d / 失败 %d",
                        task.task_id, round_ms, len(findings), sent_ok, sent_fail)
            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_SUCCESS,
                message=f"异常检测完成，发现 {len(findings)} 条{note}{write_note}",
                findings=findings, model_version="shell-v1")
        except _SlideNotAdvanced as e:
            # 窗口未推进到 slide 步长：本轮跳过（正常结果，非失败，可查询可观察）。
            # 已 fetch 了窗口、但没真检测——把下次排到 slide 步长之后，避免
            # 每个 tick 都去 fetch 空跑（动态间隔下任务平时是"到点才 fetch"）。
            slide_ms = int(getattr(task, "slide_step_ms", 0) or 0)
            if slide_ms > 0:
                with self._anomaly_due_lock:
                    self._anomaly_due[scoped_key(task.project_id, task.task_id)] = \
                        now + slide_ms
            logger.info("[engine] 任务 %s %s（本轮跳过）", task.task_id, e)
            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_SUCCESS,
                message=f"窗口未推进到 slide_step_ms，本轮跳过（{e}）",
                findings=[], model_version="")
        except Exception as e:
            logger.info(f"[engine] 异常链路异常：{e}")
            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_FAILED,
                message=f"异常检测失败：{e}", findings=[], model_version="")

    def needs_training(self, task: pb.AnomalyTaskConfig | pb.ForecastTaskConfig,
                       kind: TaskKind, config_version: int = 0) -> bool:
        """任务模型（按当前版本）是否未就绪——决定进训练队列还是推理队列。

        调度器 producer 每 tick 调一次。判定与 _get_or_train_forecaster /
        _run_anomaly_models 内部一致（KNOWN_METHODS 过滤未知方法，
        只含未知方法/只约束的任务永不进训练队列）。
        """
        kv = self._knowledge_version(task)
        if kind == TaskKind.FORECAST:
            return not self.store.is_ready(
                self._forecast_key(task, config_version, kv))
        methods = list(task.methods) or DEFAULT_METHODS
        model_methods = [m for m in methods
                         if m != "CONSTRAINT_CHECK" and m in KNOWN_METHODS]
        if not model_methods:
            return False
        return any(not self.store.is_ready(
            self._anomaly_key(task, m, config_version, kv))
            for m in model_methods)

    def _anomaly_min_points(self, task: pb.AnomalyTaskConfig, method: str) -> int:
        """异常模型训练最少点数（按方法）。

        优先级：任务显式 minimum_points > 方法级门槛（config.anomaly.minimum_points_by_method）
        > 全局默认（config.anomaly.minimum_points）。各方法数据需求不同（HISTORICAL_MATCH
        索引式、不依赖历史训练数据 → 0），统一放 config 可调，不需要改代码。
        """
        if task.minimum_points and task.minimum_points > 0:
            return task.minimum_points
        by_method = self.cfg.get("anomaly", {}).get("minimum_points_by_method", {})
        return int(by_method.get(method,
                                 self.cfg.get("anomaly", {}).get("minimum_points", 100)))

    def _anomaly_min_confirmed_events(self, task: pb.AnomalyTaskConfig) -> int:
        """HISTORICAL_MATCH（语义事件异常）启用门槛：已确认历史事件条数。

        技术方案 [43]：语义事件异常只有在知识库已确认异常事件达到一定规模后才能
        启用。默认 1 = 至少 1 条确认事件；生产按事件库积累情况调 config。
        任务没有"事件数"字段（minimum_points 是点数门槛），只走 config。
        """
        return int(self.cfg.get("anomaly", {}).get("minimum_confirmed_events", 1))

    def _event_write_cap(self) -> int:
        """写事件峰值保护：每任务每轮最多写多少条事件。

        逐点写不聚合（老师确认 08-10，一个异常点一个事件）→ 超上限只能截断不能合并；
        上限值按联调真实负载再调（现在是占位默认 50）。
        """
        return int(self.cfg.get("anomaly", {}).get("max_events_per_run", 50))

    def _log_event_stats(self) -> None:
        """打印累计写事件计数（只在有失败/截断时调，避免每 tick 刷屏）。"""
        s = self._event_stats
        logger.info("[engine] 写事件累计：异常成功 %d/失败 %d/截断 %d；"
                    "预警成功 %d/失败 %d",
                    s["anomaly_ok"], s["anomaly_fail"], s["anomaly_cap"],
                    s["forecast_ok"], s["forecast_fail"])

    def _historical_match_gate_reasons(self, task: pb.AnomalyTaskConfig,
                                       counts: dict[str, int],
                                       all_ids: list[str]) -> list[str]:
        """HISTORICAL_MATCH 双门槛（返回失败原因列表，空 = 都过）。

        - 点数：复用 _anomaly_min_points（config 里 HISTORICAL_MATCH: 0 → 默认不卡）；
        - 确认事件数：_anomaly_min_confirmed_events（语义事件异常依赖事件库规模）。
        """
        reasons: list[str] = []
        need = self._anomaly_min_points(task, "HISTORICAL_MATCH")
        if not all(counts[sid] >= need for sid in all_ids):
            missing = [f"{sid}={counts[sid]}" for sid in all_ids if counts[sid] < need]
            reasons.append(f"HISTORICAL_MATCH（{'，'.join(missing)} 需要 {need}）")
        need_ev = self._anomaly_min_confirmed_events(task)
        have_ev = len(self._confirmed_historical_events(task))
        if have_ev < need_ev:
            reasons.append(
                f"HISTORICAL_MATCH（确认事件 {have_ev} 条，需要 {need_ev} 条）")
        return reasons

    def _run_anomaly_models(self, task: pb.AnomalyTaskConfig,
                            model_methods: list[str],
                            config_version: int = 0) -> tuple[list[dict], int]:
        """按检测类型复用/训练异常模型，检测模式偏离。

        返回 (findings, step_ms)：step_ms = 实时窗口相邻时间戳差
        （frequency = 1/step_ms），给 run_anomaly 排 next_due 动态间隔用；
        取不到 → 0（调用方保持立即到期，不节流）。


        模型首训复用：key = "{task_id}:{method}@v{ver}"，训好一直复用；
        版本变 → key 变 → 必然重训。未知方法名 → logger.warning 跳过；
        仅当存在未训方法时才拉历史数据。

        多自变量→单因变量场景：
          - 因变量/自变量由 semantic_context.sequences 的 role（TARGET/FEATURE）确定；
          - 相关性先验：调 C computeBasicStatistics 拿 {序列ID: 系数}，
            转成列索引后传给 GCAD 筛选自变量；拿不到先验就降级为不用。
        """
        seq_ids = list(task.sequence_ids)
        if not seq_ids or not model_methods:
            return [], 0
        findings = []

        # ① 语义上下文 → 因变量/自变量（列索引）、相关性先验、互耦对
        target_id, source_ids = self._extract_roles(task, seq_ids)
        target_index = seq_ids.index(target_id) if target_id in seq_ids else None
        source_indices = [seq_ids.index(sid) for sid in source_ids if sid in seq_ids]
        corr_prior = None
        if target_id and source_ids:
            try:
                corr_prior = self._get_correlation_prior(task.project_id,
                                                         target_id, source_ids, seq_ids)
            except Exception as e:
                logger.info(f"[engine] 拿相关性先验失败，降级为不用先验：{e}")
        # 语义关系图先验（技术方案 [42]）：source→target 关联指向因变量的列当 GCAD 候选
        relations_prior = self._extract_relations_prior(task, target_id, seq_ids) \
            if target_id else {}
        coupled_pairs = self._extract_coupled_pairs(task, seq_ids)

        # ② 方法 → 模型：缓存命中直接复用；未命中才建模型待训
        kv = self._knowledge_version(task)
        ready: dict[str, object] = {}
        to_train: list[str] = []
        for method in model_methods:
            key = self._anomaly_key(task, method, config_version, kv)
            model = self.store.get(key)
            if model is not None:
                ready[method] = model
                continue
            model = build_anomaly_model(
                method,
                target_index=target_index,
                source_indices=source_indices,
                correlation_prior=corr_prior,
                relations_prior=relations_prior,
                coupled_pairs=coupled_pairs,
                sequence_ids=seq_ids,
                # 深度 GCAD 超参（CAUSAL_PATTERN/MUTUAL_COUPLING 用；其他方法忽略）
                gcad=self.cfg.get("anomaly", {}).get("gcad", {}),
                # 历史语义匹配超参（其他方法忽略这些 kwargs）；命中阈值可调低误报
                min_deviation_z=self.cfg.get("historical_match", {}).get(
                    "min_deviation_z", 2.0),
                top_k=self.cfg.get("historical_match", {}).get("top_k", 3),
            )
            if model is None:
                logger.warning("[engine] 未知检测方法 %s，跳过", method)
                continue
            ready[method] = model
            to_train.append(method)

        # 历史语义匹配：每次检测前把当前确认事件并进索引（幂等增量）。
        # 新模型（空索引）和已缓存模型都刷新——S 每次同步的事件 ID 都会生效，
        # 索引始终反映最新事件库，不只停留在首训那一刻。
        for model in ready.values():
            if isinstance(model, HistoricalEventMatcher):
                model.load_confirmed_events(self._confirmed_historical_events(task))

        if not ready:
            return [], 0

        # ③ 拉数据：训练用历史矩阵；检测用实时窗口对齐矩阵（统一 [时间×序列]）。
        #    C 不可达/无数据 → 异常向上抛，run_anomaly 报 FAILED（而不是"没检出"）。
        history = self._clean_matrix(self._get_history_matrix(task.project_id,
                                                              seq_ids)) if to_train else None
        window = self.core.get_aligned_real_time_window(seq_ids,
                                                        project_id=task.project_id)
        ws = int(self.cfg["inference"]["window_size"])
        matrix = self._clean_matrix(np.array(window.values[-ws:], dtype=np.float32))
        times = window.timestamps_ms[-ws:]
        # 数据步长（每步毫秒数，frequency = 1/step_ms）：给 next_due 动态间隔用。
        # 取不到（窗口 <2 个时间戳）→ 0，调用方保持立即到期（不节流）。
        step_ms = 0
        if len(times) >= 2:
            step_ms = times[-1] - times[-2]

        # slide_step_ms 节流：窗口最新时间推进不足步长 → 本轮跳过检测。
        # 防"同一异常窗口在 C 端还没滑走前反复检出、反复写 S"（全貌文档 §11.2 水位去重）。
        # 首跑无水位 → 照跑并记水位；跳过轮不更新水位（等窗口推进）。
        slide_ms = int(getattr(task, "slide_step_ms", 0) or 0)
        if slide_ms > 0 and times:
            latest = times[-1]
            wm_key = scoped_key(task.project_id, task.task_id)
            prev = self._anomaly_watermarks.get(wm_key)
            if prev is not None and latest - prev < slide_ms:
                raise _SlideNotAdvanced(
                    f"窗口最新时间推进 {latest - prev}ms < slide_step_ms={slide_ms}ms")
            self._anomaly_watermarks[wm_key] = latest

        # ④ 逐模型训练/检测：单模型异常不影响其他模型
        for method, model in ready.items():
            try:
                if method in to_train:
                    model.fit(history)
                    # fitted 守卫：数据不足时模型静默不训练（fitted=False）→ 不落盘，
                    # 保持 needs_training=True，数据增长后自动重训。否则会持久化一个空模型、
                    # is_ready=True，永远不重训、detect 恒空（历史 bug，见 GCAD 计划）。
                    if not getattr(model, "fitted", True):
                        logger.info("[engine] 模型 %s 数据不足未训练，暂不落盘（等数据增长）",
                                    method)
                        continue
                    self.store.save(
                        self._anomaly_key(task, method, config_version, kv),
                        model)
                    logger.info("[engine] 首训异常模型 %s（key=%s）",
                                method,
                                self._anomaly_key(task, method, config_version, kv))
                detected = model.detect(matrix)
                for f in detected:
                    idx = f.get("index")
                    if idx is not None and 0 <= idx < len(times):
                        f["time"] = times[idx]   # 逐点写：index → 真实时间戳
                if detected:
                    logger.info(f"[engine] 模型 {method} 检出 {len(detected)} 条模式偏离")
                findings += detected
            except Exception as e:
                logger.info(f"[engine] 模型 {method} 检测异常：{e}")
        return findings, step_ms

    def _confirmed_historical_events(self,
                                     task: pb.AnomalyTaskConfig) -> list[HistoricalEvent]:
        """确认历史事件 → 历史语义匹配索引的输入源。

        数据源优先级：
          1. 构造时注入的 provider（AnalysisEngine 构造参数，测试/框架预留），取
             task → list[HistoricalEvent]，可拿到完整事件特征；
          2. 默认 = 解析 S 下发的 semantic_context.confirmed_historical_event_ids
             （proto 已有字段）。P 不检索图谱（设计原则）拿不到事件特征，先用
             「事件 ID + 任务序列集合」建轮廓索引——至少让 S 同步的事件 ID 生效，
             当前窗口有显著偏差时可命中并指向该历史事件；等 S 确认事件特征协议
             后再填真实特征（换 provider 或默认来源升级）。

        无 provider 也无 ID → 空索引 → HISTORICAL_MATCH 无命中（安全）。
        """
        if self._historical_event_provider is not None:
            try:
                evs = self._historical_event_provider(task)
                if evs:
                    return list(evs)
            except Exception as e:
                logger.info("[engine] 历史事件 provider 异常，按空处理：%s", e)
        ids = list(getattr(task.semantic_context,
                           "confirmed_historical_event_ids", []) or [])
        if not ids:
            return []
        return [
            HistoricalEvent(event_id=eid, event_type="ANOMALY",
                            sequence_ids=tuple(task.sequence_ids),
                            source="semantic-context")
            for eid in ids
        ]

    def _extract_roles(self, task: pb.AnomalyTaskConfig,
                       seq_ids: list[str]) -> tuple[str | None, list[str]]:
        """从 semantic_context.sequences 的 role 确定因变量/自变量序列 ID。

        返回 (target_sequence_id, source_sequence_ids)。
        没有 TARGET/FEATURE 标注时退回默认：最后一条序列当因变量，其余当自变量。
        """
        seq_set = set(seq_ids)
        target_id = None
        source_ids: list[str] = []
        for meta in getattr(task.semantic_context, "sequences", []):
            role = (getattr(meta, "role", "") or "").upper()
            if meta.sequence_id not in seq_set:
                continue
            if role == "TARGET":
                target_id = meta.sequence_id
            elif role == "FEATURE":
                source_ids.append(meta.sequence_id)
        if target_id is None and seq_ids:
            target_id = seq_ids[-1]
        if not source_ids:
            source_ids = [sid for sid in seq_ids if sid != target_id]
        return target_id, source_ids

    def _extract_coupled_pairs(self, task: pb.AnomalyTaskConfig,
                               seq_ids: list[str]) -> list[tuple[int, int]]:
        """从 semantic_context.relations 提取互耦对（列索引）。

        relation_type 已按 S 端规范取值：CAUSE / CAUSAL / CORRELATION / ASSOCIATION。
        互耦 = 双向因果，规范后主信号是**成对反向因果边**：A→B 且 B→A 同时存在
        （都属因果型 CAUSE/CAUSAL）→ 推断互耦。CORRELATION/ASSOCIATION 是无向
        相关/关联，不成互耦对。老自由字符串的互耦标记（MUTUAL/COUPLING/
        BIDIRECTIONAL/COUPLED 关键词）保留兼容，两边都算标记对。
        返回按列索引排序的 [(a_idx, b_idx), ...]，序列不在任务里的跳过。
        """
        seq_index = {sid: i for i, sid in enumerate(seq_ids)}
        relations = list(getattr(task.semantic_context, "relations", []))
        causal_out: dict[str, set[str]] = {}     # 因果边 src → {tgt}（新规范主信号）
        marked = set()                           # 老关键词标的互耦边（兼容）
        for r in relations:
            src = getattr(r, "source_sequence_id", "")
            tgt = getattr(r, "target_sequence_id", "")
            if not src or not tgt or src == tgt:
                continue
            rtype = (getattr(r, "relation_type", "") or "").upper()
            if any(k in rtype for k in _MUTUAL_KEYWORDS):
                marked.add((src, tgt))
                marked.add((tgt, src))
            if rtype in _CAUSAL_TYPES:
                causal_out.setdefault(src, set()).add(tgt)
        pairs = set()
        # 规范后主信号：A→B 且 B→A 都是因果边 → 互耦
        for src, tgts in causal_out.items():
            for tgt in tgts:
                if src in causal_out.get(tgt, ()):
                    if src in seq_index and tgt in seq_index:
                        pairs.add(tuple(sorted((seq_index[src], seq_index[tgt]))))
        # 兼容老自由字符串互耦标记
        for src, tgt in marked:
            if src in seq_index and tgt in seq_index:
                pairs.add(tuple(sorted((seq_index[src], seq_index[tgt]))))
        return sorted(pairs)

    def _get_correlation_prior(self, project_id: str, target_id: str,
                               source_ids: list[str],
                               seq_ids: list[str]) -> dict[int, float] | None:
        """调 C computeBasicStatistics 拿相关性先验，转成 {列索引: 系数}。"""
        by_id = self.core.get_correlation_vector(target_id, source_ids,
                                                 project_id=project_id)
        if not by_id:
            return None
        seq_index = {sid: i for i, sid in enumerate(seq_ids)}
        return {seq_index[sid]: coef for sid, coef in by_id.items() if sid in seq_index}

    def _extract_relations_prior(self, task: pb.AnomalyTaskConfig,
                                 target_id: str,
                                 seq_ids: list[str]) -> dict[int, float]:
        """从 semantic_context.relations 提取指向因变量的静态关联先验（技术方案 [42]）。

        只取"方向指向因变量、且为因果型（CAUSE/CAUSAL）"的 relation
        （source→target=target_id），转成 {source列索引: confidence}，序列不在任务里的
        跳过。GCAD 用它做候选列的结构门：知识库里标注了"谁影响因变量"，因果发现就只在
        这批列里找，不跑偏到无关列。CORRELATION/ASSOCIATION（无向相关/关联）不算因果
        候选；门全被筛掉 → 返回空，engine 退回全部非目标列（抗不完整，不削减）。
        confidence 的软权重语义待 S 冻结，v1 只用"有没有 relation"，值存着备用。
        """
        seq_index = {sid: i for i, sid in enumerate(seq_ids)}
        if target_id not in seq_index:
            return {}
        prior: dict[int, float] = {}
        for r in getattr(task.semantic_context, "relations", []):
            src = getattr(r, "source_sequence_id", "")
            tgt = getattr(r, "target_sequence_id", "")
            if not src or tgt != target_id or src not in seq_index:
                continue
            rtype = (getattr(r, "relation_type", "") or "").upper()
            if rtype not in _CAUSAL_TYPES:
                continue   # 相关/关联不算因果候选
            prior[seq_index[src]] = float(getattr(r, "confidence", 0.0) or 0.0)
        return prior

    def _get_history_matrix(self, project_id: str,
                            seq_ids: list[str]) -> np.ndarray:
        """从 C 拉历史数据，转成 [time, seq_count] 矩阵。

        按 training.max_train_points 截最近 N 点（防全量历史把 gRPC 消息撑爆，
        与预测训练取数同口径）；上限 0 = 不截。
        """
        scales = {s.sequence_id: s
                  for s in self.core.get_sequence_data_scale(seq_ids,
                                                             project_id=project_id)}
        starts = [s.start_time_ms for s in scales.values()
                  if s.start_time_ms is not None]
        ends = [s.end_time_ms for s in scales.values()
                if s.end_time_ms is not None]
        if not starts or not ends:
            chunk = self.core.get_history(seq_ids, project_id=project_id)
            return np.array(chunk.values, dtype=float)
        start_ms = self._cap_train_start(min(starts), max(ends), scales)
        chunk = self.core.get_history(seq_ids, start_time_ms=start_ms,
                                      end_time_ms=max(ends), project_id=project_id)
        return np.array(chunk.values, dtype=float)

    def _cap_train_start(self, start_ms: int, end_ms: int,
                         scales: dict) -> int:
        """训练取数点数上限：把起点往后抬，只取最近 max_train_points 点。

        用最细采样间隔换算时间窗（窗口偏小 → 点数偏少，宁可少取也别超上限）；
        无上限（max_train_points=0）或无采样间隔可推断 → 原样返回 start_ms。
        """
        max_pts = int(self.cfg.get("training", {}).get("max_train_points", 0) or 0)
        if max_pts <= 0:
            return start_ms
        intervals = [
            (s.end_time_ms - s.start_time_ms) / (s.point_count - 1)
            for s in scales.values()
            if s.point_count and s.point_count > 1
            and s.start_time_ms is not None and s.end_time_ms is not None
        ]
        if not intervals:
            return start_ms
        step_ms = min(intervals)   # 最细间隔 → 时间窗最小 → 最保守点数
        return max(start_ms, end_ms - int(max_pts * step_ms))

    # ================= 工具 =================

    @staticmethod
    def _clean_matrix(matrix: np.ndarray) -> np.ndarray:
        """模型入口 NaN 清洗（P1-5）：逐列前值填充 NaN，仍缺的补 0。

        C 对齐补的 NaN 统一在这里处理，不喂进模型（PatchTST 训练/推理、异常检测
        训练矩阵都走这里）。返回 float32 [T, C]。
        """
        m = np.array(matrix, dtype=np.float32)
        if m.ndim == 1:
            m = m.reshape(-1, 1)
        if m.ndim != 2 or m.shape[1] == 0:
            return m
        for c in range(m.shape[1]):
            col = m[:, c]
            last = np.nan
            for i in range(len(col)):
                v = col[i]
                if np.isnan(v):
                    if not np.isnan(last):
                        col[i] = last
                else:
                    last = v
            col[np.isnan(col)] = 0.0
        return m

    def _infer_column_kinds(self, chunk: HistoricalDataChunk) -> dict[str, str]:
        """按历史原始取值类型推断每列是连续还是离散（数据推断路由，不改 proto）。

        必须在 np.array(chunk.values, float32) 之前调用——chunk.values 保留 C 端
        原始 Python 类型（int/bool/float/string），缺失=NaN，类型不丢。
        int/bool → discrete；string → string（标签类离散，暂不支持）；其余
        （float/混合/空列）→ continuous。
        """
        kinds: dict[str, str] = {}
        for c, sid in enumerate(chunk.sequence_ids):
            vals = [row[c] for row in chunk.values]
            non_missing = [v for v in vals if not _is_missing(v)]
            if not non_missing:
                kinds[sid] = "continuous"            # 空列保守默认连续
            elif any(isinstance(v, str) for v in non_missing):
                kinds[sid] = "string"
            elif all(isinstance(v, (int, bool)) for v in non_missing):
                kinds[sid] = "discrete"
            else:
                kinds[sid] = "continuous"
        return kinds

    def _resolve_severity(self, rule: str | None, default: int) -> int:
        """任务 warning_rule（S 设任务时直接给的等级名）→ proto Severity 枚举值。

        危险等级由 S 定、P 只存着用，不自己分级。proto 枚举名带 SEVERITY_ 前缀
        （SEVERITY_HIGH）且字段只认全名，这里兼容 "HIGH" / "SEVERITY_HIGH" 两种
        形态；空/未知等级名 → default（保持既有默认，不崩）。
        """
        name = (rule or "").strip().upper()
        if not name:
            return default
        if not name.startswith("SEVERITY_"):
            name = "SEVERITY_" + name
        try:
            return pb.Severity.Value(name)
        except ValueError:
            logger.info("[engine] 未知等级名 %r，severity 用默认", rule)
            return default

    def _forecast_result(self, task: pb.ForecastTaskConfig, status: int,
                         message: str, timestamps: list[int] | None = None,
                         sequence_ids: list[str] | None = None,
                         values: list[float] | None = None,
                         model_version: str = "patchtst-shell-v1"
                         ) -> pb.ForecastResult:
        return pb.ForecastResult(
            task_id=task.task_id, run_id=f"run-{int(time.time()*1000)}",
            generated_at_ms=int(time.time()*1000), status=status, message=message,
            timestamps_ms=timestamps or [], sequence_ids=sequence_ids or [],
            values=values or [], risk_findings=[], model_version=model_version)

    def _build_aligned(self, target: str, timestamps: list[int],
                       preds: list[float]) -> cpb.AlignedWindowData:
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
