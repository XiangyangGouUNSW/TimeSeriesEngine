"""project_id 项目/租户隔离专项测试（负责人 08-21 定，不连服务）。

隔离的核心不变量：业务 ID（task_id）保持不变，隔离靠
`project_id + "::" + task_id` 复合键（scoped_key）。同 task_id 在不同项目
= 两条独立任务：配置、结果、模型、invalidate 全部互不影响。

覆盖：
  a) TaskRegistry：同 task_id 在 A/B 注册 → 两条独立记录，互不覆盖；
  b) ResultRepository：A/B 结果互不可见（latest 各取各的）；
  c) ModelStore：key 带项目前缀，A 的 invalidate 不误伤 B；
  d) AnalysisEngine：needs_training / invalidate_task 按项目隔离；
  e) servicer：请求级 project_id 权威 → 同 task_id 两项目各自注册 + 查询隔离。

用法（sfkg 环境）：
  python tools/test_project_isolation.py
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
from analysis_servicer import AnalysisServicer
from project import DEFAULT_PROJECT, normalize_project, scoped_key
from result_repository import ResultRepository
from task_registry import TaskKind, TaskRegistry, TaskStatus
from training_loop import ModelStore

_PASS = 0
PROJ_A = "project-alpha"
PROJ_B = "project-beta"
TASK_ID = "task-shared-001"


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def _ftask(project_id: str, task_id: str = TASK_ID) -> pb.ForecastTaskConfig:
    return pb.ForecastTaskConfig(
        task_id=task_id, project_id=project_id,
        task_name="隔离测试", target_sequence_ids=["ETTh1_OT"],
    )


def _store() -> ModelStore:
    return ModelStore(model_dir=tempfile.mkdtemp(prefix="iso-test-"))


def _engine(store: ModelStore | None = None) -> AnalysisEngine:
    return AnalysisEngine(
        core_client=object(), result_client=None,
        config={"forecast_model": {"type": "linear"},
                "anomaly": {"minimum_points": 5}},
        model_store=store or _store())


# ================= a) TaskRegistry 复合键 =================

def test_registry_isolation() -> None:
    print("\n[a) TaskRegistry：同 task_id 不同项目 → 两条独立记录]")
    assert normalize_project("") == DEFAULT_PROJECT, "空 project_id 归一化为 default"
    reg = TaskRegistry()
    reg.register(PROJ_A, _ftask(PROJ_A), TaskKind.FORECAST, 0)
    reg.register(PROJ_B, _ftask(PROJ_B), TaskKind.FORECAST, 0)

    ra = reg.get(PROJ_A, TASK_ID)
    rb = reg.get(PROJ_B, TASK_ID)
    assert ra is not None and rb is not None, "两项目各自应有一条记录"
    assert ra.project_id == PROJ_A and rb.project_id == PROJ_B
    assert ra is not rb, "两条独立记录（同一对象会被误伤）"

    # 项目 A 置为禁用，不影响 B
    assert reg.set_status(PROJ_A, TASK_ID, TaskStatus.DISABLED)
    assert not reg.is_enabled(PROJ_A, TASK_ID)
    assert reg.is_enabled(PROJ_B, TASK_ID), "A 禁用不应影响 B"
    en = [r for r in reg.enabled_tasks() if r.task_id == TASK_ID]
    assert len(en) == 1 and en[0].project_id == PROJ_B, "enabled_tasks 只含 B"
    _ok("两条独立记录：get/set_status/is_enabled/enabled_tasks 互不影响")

    # 空 project_id 归一化为 default（兼容旧客户端）
    reg.register("", _ftask(""), TaskKind.FORECAST, 0)
    assert reg.get(DEFAULT_PROJECT, TASK_ID) is not None, "空 → default 归一化"
    _ok("空 project_id 归一化为 'default'")


# ================= b) ResultRepository 结果互不可见 =================

def test_repository_isolation() -> None:
    print("\n[b) ResultRepository：同 task_id 结果按项目隔离]")
    repo = ResultRepository(maxlen=10)
    ra = pb.AnomalyResult(task_id=TASK_ID, run_id="run-A",
                          generated_at_ms=1, status=pb.ANALYSIS_STATUS_SUCCESS,
                          message="A 的结果", findings=[])
    rb = pb.AnomalyResult(task_id=TASK_ID, run_id="run-B",
                          generated_at_ms=2, status=pb.ANALYSIS_STATUS_SUCCESS,
                          message="B 的结果", findings=[])
    repo.put(PROJ_A, TASK_ID, ra)
    repo.put(PROJ_B, TASK_ID, rb)

    la = repo.latest(PROJ_A, TASK_ID)
    lb = repo.latest(PROJ_B, TASK_ID)
    assert la is ra and lb is rb, "latest 应各取各的"
    assert la.message == "A 的结果" and lb.message == "B 的结果"
    assert repo.latest(DEFAULT_PROJECT, TASK_ID) is None, "default 项目查不到 A/B 的结果"
    assert len(repo.history(PROJ_A, TASK_ID, 5)) == 1
    assert len(repo.history(PROJ_B, TASK_ID, 5)) == 1
    _ok("A/B 结果互不可见，default 项目也不可见")


# ================= c) ModelStore 复合键 + invalidate 不误伤 =================

def test_model_store_isolation() -> None:
    print("\n[c) ModelStore：key 带项目前缀，invalidate 只清本项目]")
    store = _store()
    store.save(scoped_key(PROJ_A, TASK_ID) + "@v1", object())
    store.save(scoped_key(PROJ_B, TASK_ID) + "@v1", object())

    assert store.is_ready(scoped_key(PROJ_A, TASK_ID) + "@v1")
    assert store.is_ready(scoped_key(PROJ_B, TASK_ID) + "@v1")
    assert not store.is_ready(scoped_key("another", TASK_ID) + "@v1"), \
        "第三项目查不到 A/B 的模型"

    # A 清理只清 A（keep_version=0 不保留 v1）：B 的同 task_id 模型不动
    store.invalidate_task(PROJ_A, TASK_ID, keep_version=0)
    assert not store.is_ready(scoped_key(PROJ_A, TASK_ID) + "@v1"), "A 模型已清"
    assert store.is_ready(scoped_key(PROJ_B, TASK_ID) + "@v1"), "B 模型不受影响"
    _ok("invalidate_task(PROJ_A) 只清 A，B 同 task_id 模型保留")


# ================= d) 引擎 needs_training / invalidate 按项目 =================

def test_engine_isolation() -> None:
    print("\n[d) AnalysisEngine：模型 key / invalidate 按项目隔离]")
    store = _store()
    eng = _engine(store)
    task_a = _ftask(PROJ_A)
    task_b = _ftask(PROJ_B)
    assert task_a.project_id == PROJ_A and task_b.project_id == PROJ_B

    # 相同 task_id：训练 A → 只 A 就绪，B 仍需训练
    store.save(eng._forecast_key(task_a, 0), object())
    assert not eng.needs_training(task_a, TaskKind.FORECAST, 0), "A 已就绪"
    assert eng.needs_training(task_b, TaskKind.FORECAST, 0), "B 同 task_id 仍需训练"
    _ok("同 task_id：A 有模型 → 不训练；B 无模型 → 需训练")

    # invalidate A（keep_version=0 不保留 v1）→ A 清、B 模型保留
    store.save(eng._forecast_key(task_a, 1), object())
    store.save(eng._forecast_key(task_b, 1), object())
    eng.invalidate_task(PROJ_A, TASK_ID, keep_version=0)
    assert not store.is_ready(eng._forecast_key(task_a, 1)), "A 已清（同 key 同版本）"
    assert store.is_ready(eng._forecast_key(task_b, 1)), "invalidate A 不误伤 B"
    _ok("invalidate_task 按项目清，A 的清理不误伤 B 的同 task_id")


# ================= e) servicer 请求级 project_id 穿线 =================

def test_servicer_project_threading() -> None:
    print("\n[e) servicer：请求级 project_id 权威 → 注册/查询按项目隔离]")
    registry = TaskRegistry()
    repo = ResultRepository(maxlen=10)
    eng = _engine()
    serv = AnalysisServicer(registry, repo, eng)

    # 同 task_id 分别在 A/B sync（请求级 project_id 权威，任务内为空也归 A/B）
    ack_a = serv.SyncForecastTask(
        pb.AnalysisSyncForecastTaskRequest(
            meta=pb.RequestMeta(project_id=PROJ_A),
            task=_ftask("", TASK_ID)), None)
    ack_b = serv.SyncForecastTask(
        pb.AnalysisSyncForecastTaskRequest(
            meta=pb.RequestMeta(project_id=PROJ_B),
            task=_ftask("", TASK_ID)), None)
    assert ack_a.accepted and ack_b.accepted, "两项目各注册成功"
    ra = registry.get(PROJ_A, TASK_ID)
    rb = registry.get(PROJ_B, TASK_ID)
    assert ra is not None and rb is not None, "两项目各有一条记录"
    assert ra.project_id == PROJ_A and rb.project_id == PROJ_B, \
        "请求级 project_id 回写到任务记录"
    _ok("请求级 project_id 权威：同 task_id 两项目各自注册 + 回写")

    # 查询按项目隔离：A 的查询不返回 B 的结果
    repo.put(PROJ_A, TASK_ID, pb.AnomalyResult(
        task_id=TASK_ID, run_id="run-A", generated_at_ms=1,
        status=pb.ANALYSIS_STATUS_SUCCESS, message="A", findings=[]))
    qa = serv.QueryAnomalyResults(pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(
        task_id=TASK_ID, project_id=PROJ_A, latest_only=True)), None)
    qb = serv.QueryAnomalyResults(pb.QueryAnomalyResultsRequest(query=pb.ResultQuery(
        task_id=TASK_ID, project_id=PROJ_B, latest_only=True)), None)
    assert len(qa.results) == 1 and qa.results[0].message == "A", "A 查到自己的结果"
    assert qb.results == [], "B 查不到 A 的结果"
    _ok("查询隔离：同 task_id，A 查得到、B 查不到")


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)
    test_registry_isolation()
    test_repository_isolation()
    test_model_store_isolation()
    test_engine_isolation()
    test_servicer_project_threading()
    print(f"\nproject_id 隔离专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
