"""P 端分析引擎：把「收任务 → 查C → 训练 → 预测 → 调C检查 → 调S写」串起来。

框架版：内部逻辑简单（AR 模型 + 假约束检查 + S 写事件），
目标是跑通三方调用链、产生可见输出，证明通讯成功。
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from pathlib import Path

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import numpy as np

import timeseries_analysis_pb2 as pb        # P↔S 的消息（ForecastResult/AnomalyResult）
import timeseries_core_pb2 as cpb           # P↔C 的消息（AlignedWindowData）

from anomaly_models import KNOWN_METHODS, build_anomaly_model
from patchtst_forecaster import PatchTSTForecaster
from task_registry import TaskKind
from training_loop import ModelStore

logger = logging.getLogger(__name__)

# 异常检测类型：methods 空时的默认组合（实时约束检查已迁 C，默认只做模式偏移检测）
DEFAULT_METHODS = ["CAUSAL_PATTERN"]


class AnalysisEngine:
    """把调用链串起来的引擎。"""

    def __init__(self, core_client, result_client, config: dict, model_store=None):
        self.core = core_client        # GrpcCoreDataClient（调 C 取数据/检查）
        self.sender = result_client    # AnalysisResultClient（调 S 写事件）
        self.cfg = config
        self.store = model_store or ModelStore()   # 模型复用缓存（内存+磁盘）
        self._train_locks: dict = {}   # {key: Lock}，同 key 并发训练只训一次
        self._train_locks_lock = threading.RLock()  # 保护 _train_locks
        self._register_model_loader()

    # ================= 预测链路 =================

    def run_forecast(self, task, config_version: int = 0) -> pb.ForecastResult:
        now = int(time.time() * 1000)
        target_ids = list(task.target_sequence_ids)
        if not target_ids:
            return self._forecast_result(task, pb.ANALYSIS_STATUS_INVALID_REQUEST,
                                         "预测任务没有目标序列")
        target = target_ids[0]
        try:
            # ① 查 C 数据规模，够不够训练（目标 + 特征全列）
            all_ids = list(dict.fromkeys(
                list(task.target_sequence_ids) + list(task.feature_sequence_ids)))
            scales = {s.sequence_id: s for s in self.core.get_sequence_data_scale(all_ids)}
            need = self._min_train_points(task)
            counts = {sid: scales.get(sid).point_count if sid in scales else 0
                      for sid in all_ids}
            if any(counts[sid] < need for sid in all_ids):
                missing = [f"{sid}={counts[sid]}" for sid in all_ids
                           if counts[sid] < need]
                return self._forecast_result(task, pb.ANALYSIS_STATUS_DATA_NOT_READY,
                                             f"数据不足：{'，'.join(missing)}（需要 {need}）")

            # ② 预测模型复用：缓存命中就跳过训练，否则训练并保存
            model = self._get_or_train_forecaster(task, all_ids, scales,
                                                  config_version)

            # ③ 取实时窗口（对齐成矩阵）预测未来。训练才用历史，推理一律实时窗口
            ctx = self._context_length(task)
            window = self.core.get_aligned_real_time_window(all_ids)
            times = window.timestamps_ms[-ctx:]
            matrix = np.array(window.values[-ctx:], dtype=np.float32)
            horizon = task.forecast_horizon_steps or self.cfg["inference"]["horizon_steps"]
            pred_map = model.forecast(matrix, steps=horizon)
            preds = pred_map[target]
            step_ms = 3600_000
            if len(times) >= 2:
                step_ms = times[-1] - times[-2]
            last_ts = times[-1]
            out_ts = [last_ts + step_ms * (i + 1) for i in range(horizon)]
            logger.info(f"[engine] ③预测 {target} 未来 {horizon} 步，前 3 个值 {[round(v,2) for v in preds[:3]]}")

            # ④ 调 C 约束检查（把预测值包成 AlignedWindowData 传给 C）
            constraint_ids = list(task.semantic_context.constraint_ids)
            logger.info("[engine] 任务 %s 收到的 constraint_ids = %s",
                        task.task_id, constraint_ids)
            aligned = self._build_aligned(target, out_ts, preds)
            satisfied, violations = self.core.check_constraints(
                constraint_ids, aligned_data=aligned)
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

    # ================= PatchTST 预测模型复用 =================

    def _register_model_loader(self) -> None:
        """磁盘缓存加载：重建 PatchTSTForecaster 并从文件恢复。"""

        def loader(key: str, path):
            fc = PatchTSTForecaster(sequence_ids=[])
            fc.load(path)
            return fc

        self.store.set_loader(loader)

    def _min_train_points(self, task) -> int:
        """训练最少点数 = context + prediction（用配置），任务给了 minimum_points 优先。"""
        if task.minimum_points and task.minimum_points > 0:
            return task.minimum_points
        f = self.cfg.get("forecast_model", {})
        return (int(f.get("context_length", 96))
                + int(f.get("prediction_length", 24)))

    def _context_length(self, task) -> int:
        # ForecastTaskConfig 无 context_length（那是 AnomalyTaskConfig 的），用 getattr 兜底
        ctx = getattr(task, "context_length", None) or 0
        if ctx > 0:
            return ctx
        return int(self.cfg.get("forecast_model", {}).get("context_length", 96))

    def _forecast_key(self, task_id: str, ver: int) -> str:
        """预测模型缓存 key：版本进 key，版本变 → key 变 → 必然重训。"""
        return f"{task_id}@v{ver}"

    def _anomaly_key(self, task_id: str, method: str, ver: int) -> str:
        """异常模型缓存 key：版本进 key，版本变 → key 变 → 必然重训。"""
        return f"{task_id}:{method}@v{ver}"

    def _get_or_train_forecaster(self, task, all_ids, scales,
                                 config_version: int = 0):
        """缓存命中返回模型；未命中训练 PatchTST 并存入 store。

        版本进 key：{task_id}@v{config_version}。版本变 → key 变 → 必然重训，
        天然免疫「旧版本在飞训练覆盖新版本 key」的竞态。同 key 并发到达时
        拿 per-task 锁 double-check，只训一次。
        """
        key = self._forecast_key(task.task_id, config_version)
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

            # 拉多元历史训练（前 train_ratio）
            start_ms = min(s.start_time_ms for s in scales.values()
                           if s.start_time_ms is not None)
            end_ms = max(s.end_time_ms for s in scales.values()
                         if s.end_time_ms is not None)
            cut_ms = start_ms + int((end_ms - start_ms)
                                    * self.cfg["training"]["train_ratio"])
            chunk = self.core.get_history(all_ids, end_time_ms=cut_ms)
            history = np.array(chunk.values, dtype=np.float32)
            logger.info(f"[engine] ②训练 PatchTST：{len(history)} 行 × {len(all_ids)} 列")

            f = self.cfg.get("forecast_model", {})
            fc = PatchTSTForecaster(
                sequence_ids=all_ids,
                context_length=int(f.get("context_length", 96)),
                prediction_length=int(f.get("prediction_length", 24)),
                patch_size=int(f.get("patch_size", 16)),
                patch_stride=int(f.get("stride", 8)),
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

    def invalidate_task(self, task_id: str, keep_version: int | None = None) -> None:
        """清理任务的旧版本模型（保留最近 2 版，见 ModelStore.invalidate_task）。

        keep_version=None 全删（任务删除）。顺带 prune _train_locks，
        防其随版本数缓慢增长。
        """
        self.store.invalidate_task(task_id, keep_version)
        with self._train_locks_lock:
            stale = [k for k in self._train_locks
                     if k.startswith(f"{task_id}:") or k.startswith(f"{task_id}@v")]
            for k in stale:
                self._train_locks.pop(k, None)

    # ================= 异常链路 =================

    def run_anomaly(self, task, config_version: int = 0) -> pb.AnomalyResult:
        now = int(time.time() * 1000)
        findings = []   # 收集异常（模型检测）

        # 检测类型显式化：methods 决定检什么（异常约束检查已迁 C，P 只做模型方法）
        methods = list(task.methods) or DEFAULT_METHODS
        model_methods = [m for m in methods if m != "CONSTRAINT_CHECK"]
        logger.info("[engine] 任务 %s 检测类型：methods=%s → 模型=%s",
                    task.task_id, methods, model_methods)
        try:
            # ① 模型检测（模式偏离异常；无模型方法则跳过）。
            #    推理输入 = 实时窗口对齐矩阵（见 _run_anomaly_models ③）；训练才用历史。
            if model_methods:
                for f in self._run_anomaly_models(task, model_methods,
                                                  config_version):
                    finding = pb.AnomalyFinding(
                        anomaly_type=f["anomaly_type"], severity=f["severity"],
                        description=f["description"])
                    if f.get("score") is not None:
                        finding.score = f["score"]
                    if f.get("time") is not None:
                        finding.detected_time_ms = f["time"]
                    findings.append(finding)

            # ③ 逐点写异常（老师确认 08-10）：一个异常点一个事件，不聚合。
            #    每个点用自己的时间戳（模型取 index→窗口时间，兜底 now）。
            for f in findings:
                ts = getattr(f, "detected_time_ms", None) or now
                ok = self.sender.send_event(
                    event_type=pb.ANOMALY_EVENT_TYPE_ANOMALY,
                    event_time_ms=ts,
                    sequence_ids=list(task.sequence_ids),
                    values=[f.score] if f.score is not None else [],
                    severity=pb.SEVERITY_HIGH,
                    source=pb.ANOMALY_SOURCE_MODEL_ANOMALY_DETECTION,
                )
                if not ok:
                    logger.info("[engine] ③逐点写 S 异常失败：点 %s（S 未起/连不上）", ts)

            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_SUCCESS,
                message=f"异常检测完成，发现 {len(findings)} 条",
                findings=findings, model_version="shell-v1")
        except Exception as e:
            logger.info(f"[engine] 异常链路异常：{e}")
            return pb.AnomalyResult(
                task_id=task.task_id, run_id=f"run-{now}",
                generated_at_ms=now, status=pb.ANALYSIS_STATUS_FAILED,
                message=f"异常检测失败：{e}", findings=[], model_version="")

    def needs_training(self, task, kind: TaskKind, config_version: int = 0) -> bool:
        """任务模型（按当前版本）是否未就绪——决定进训练队列还是推理队列。

        调度器 producer 每 tick 调一次。判定与 _get_or_train_forecaster /
        _run_anomaly_models 内部一致（KNOWN_METHODS 过滤未知方法，
        只含未知方法/只约束的任务永不进训练队列）。
        """
        if kind == TaskKind.FORECAST:
            return not self.store.is_ready(
                self._forecast_key(task.task_id, config_version))
        methods = list(task.methods) or DEFAULT_METHODS
        model_methods = [m for m in methods
                         if m != "CONSTRAINT_CHECK" and m in KNOWN_METHODS]
        if not model_methods:
            return False
        return any(not self.store.is_ready(
            self._anomaly_key(task.task_id, m, config_version))
            for m in model_methods)

    def _run_anomaly_models(self, task, model_methods: list[str],
                            config_version: int = 0) -> list[dict]:
        """按检测类型复用/训练异常模型，检测模式偏离。

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
            return []
        findings = []

        # ① 语义上下文 → 因变量/自变量（列索引）、相关性先验、互耦对
        target_id, source_ids = self._extract_roles(task, seq_ids)
        target_index = seq_ids.index(target_id) if target_id in seq_ids else None
        source_indices = [seq_ids.index(sid) for sid in source_ids if sid in seq_ids]
        corr_prior = None
        if target_id and source_ids:
            try:
                corr_prior = self._get_correlation_prior(target_id, source_ids, seq_ids)
            except Exception as e:
                logger.info(f"[engine] 拿相关性先验失败，降级为不用先验：{e}")
        coupled_pairs = self._extract_coupled_pairs(task, seq_ids)

        # ② 方法 → 模型：缓存命中直接复用；未命中才建模型待训
        ready: dict[str, object] = {}
        to_train: list[str] = []
        for method in model_methods:
            key = self._anomaly_key(task.task_id, method, config_version)
            model = self.store.get(key)
            if model is not None:
                ready[method] = model
                continue
            model = build_anomaly_model(
                method,
                target_index=target_index,
                source_indices=source_indices,
                correlation_prior=corr_prior,
                coupled_pairs=coupled_pairs,
            )
            if model is None:
                logger.warning("[engine] 未知检测方法 %s，跳过", method)
                continue
            ready[method] = model
            to_train.append(method)

        if not ready:
            return []

        # ③ 拉数据：训练用历史矩阵；检测用实时窗口对齐矩阵（统一 [时间×序列]）。
        #    C 不可达/无数据 → 异常向上抛，run_anomaly 报 FAILED（而不是"没检出"）。
        history = self._get_history_matrix(seq_ids) if to_train else None
        window = self.core.get_aligned_real_time_window(seq_ids)
        ws = int(self.cfg["inference"]["window_size"])
        matrix = np.array(window.values[-ws:], dtype=np.float32)
        times = window.timestamps_ms[-ws:]

        # ④ 逐模型训练/检测：单模型异常不影响其他模型
        for method, model in ready.items():
            try:
                if method in to_train:
                    model.fit(history)
                    self.store.save(
                        self._anomaly_key(task.task_id, method, config_version),
                        model)
                    logger.info("[engine] 首训异常模型 %s（key=%s）",
                                method,
                                self._anomaly_key(task.task_id, method,
                                                  config_version))
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
        return findings

    def _extract_roles(self, task, seq_ids) -> tuple[str | None, list[str]]:
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

    def _extract_coupled_pairs(self, task, seq_ids) -> list[tuple[int, int]]:
        """从 semantic_context.relations 提取互耦对（列索引）。

        互耦识别两种方式（S 端互耦取值对齐前先都支持）：
          1. relation_type 带互耦标记（MUTUAL/COUPLING/BIDIRECTIONAL/COUPLED 关键词）；
          2. 成对反向 relation（A→B 和 B→A 同时存在）→ 推断互耦。
        返回按列索引排序的 [(a_idx, b_idx), ...]，序列不在任务里的跳过。
        """
        seq_index = {sid: i for i, sid in enumerate(seq_ids)}
        relations = list(getattr(task.semantic_context, "relations", []))
        marked = set()
        reverse_pairs = set()
        for r in relations:
            src = getattr(r, "source_sequence_id", "")
            tgt = getattr(r, "target_sequence_id", "")
            if not src or not tgt or src == tgt:
                continue
            rtype = (getattr(r, "relation_type", "") or "").upper()
            if any(k in rtype for k in ("MUTUAL", "COUPLING", "BIDIRECTIONAL", "COUPLED")):
                marked.add((src, tgt))
                marked.add((tgt, src))
            reverse_pairs.add((src, tgt))
        pairs = set()
        for src, tgt in marked:
            if src in seq_index and tgt in seq_index:
                pairs.add(tuple(sorted((seq_index[src], seq_index[tgt]))))
        for src, tgt in reverse_pairs:
            if (tgt, src) in reverse_pairs and src in seq_index and tgt in seq_index:
                pairs.add(tuple(sorted((seq_index[src], seq_index[tgt]))))
        return sorted(pairs)

    def _get_correlation_prior(self, target_id, source_ids, seq_ids) -> dict[int, float] | None:
        """调 C computeBasicStatistics 拿相关性先验，转成 {列索引: 系数}。"""
        by_id = self.core.get_correlation_vector(target_id, source_ids)
        if not by_id:
            return None
        seq_index = {sid: i for i, sid in enumerate(seq_ids)}
        return {seq_index[sid]: coef for sid, coef in by_id.items() if sid in seq_index}

    def _get_history_matrix(self, seq_ids) -> np.ndarray:
        """从 C 拉历史数据，转成 [time, seq_count] 矩阵。"""
        chunk = self.core.get_history(seq_ids)
        return np.array(chunk.values, dtype=float)

    # ================= 工具 =================

    def _forecast_result(self, task, status, message,
                         timestamps=None, sequence_ids=None, values=None):
        return pb.ForecastResult(
            task_id=task.task_id, run_id=f"run-{int(time.time()*1000)}",
            generated_at_ms=int(time.time()*1000), status=status, message=message,
            timestamps_ms=timestamps or [], sequence_ids=sequence_ids or [],
            values=values or [], risk_findings=[], model_version="patchtst-shell-v1")

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
