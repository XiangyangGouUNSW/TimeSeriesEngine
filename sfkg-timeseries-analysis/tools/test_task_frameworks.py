"""三大框架单元测试：TaskRegistry / 检测类型解析 / ResultRepository。

不连任何服务，纯逻辑验证。
  - TaskRegistry：注册=ENABLED、更新保留状态、启停/删除真正生效；
  - run_anomaly 检测类型解析：约束(迁C)过滤 / 只约束不参与 / 只模型 / 空默认 / 未知方法记日志；
  - ResultRepository：put/latest/history、超 maxlen 丢最旧。

用法（sfkg 环境）：
  python tools/test_task_frameworks.py
"""

from __future__ import annotations

import logging
import sys
import types
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2 as pb
from analysis_engine import AnalysisEngine
from data_types import SequenceDataScale
from task_registry import TaskKind, TaskRegistry, TaskStatus
from result_repository import ResultRepository

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


# ================= TaskRegistry =================

def test_registry() -> None:
    print("\n[TaskRegistry]")
    reg = TaskRegistry()
    rec = reg.register(types.SimpleNamespace(task_id="t-1"), TaskKind.ANOMALY)
    assert rec.status == TaskStatus.ENABLED, "新建任务应默认 ENABLED"
    _ok("新建任务默认 ENABLED")

    # 更新配置（同 task_id 再注册）→ 保留原状态，不复活
    reg.set_status("t-1", TaskStatus.DISABLED)
    reg.register(types.SimpleNamespace(task_id="t-1"), TaskKind.ANOMALY)
    assert reg.get("t-1").status == TaskStatus.DISABLED, "更新配置不应复活 DISABLED 任务"
    _ok("更新配置保留原状态")

    assert all(r.task_id != "t-1" for r in reg.enabled_tasks()), "DISABLED 不进 enabled_tasks"
    _ok("DISABLED 不进 enabled_tasks")

    reg.set_status("t-1", TaskStatus.ENABLED)
    assert any(r.task_id == "t-1" for r in reg.enabled_tasks())
    _ok("ENABLED 恢复调度")

    assert reg.set_status("t-1", TaskStatus.DELETED)
    assert reg.get("t-1") is None, "DELETED 后应移除"
    _ok("DELETED 移除")

    assert not reg.set_status("t-not-exist", TaskStatus.DISABLED), "任务不存在返回 False"
    _ok("任务不存在返回 False")

    # config_version：register 更新版本、保留启停状态；is_enabled 反映实时状态
    rec = reg.register(types.SimpleNamespace(task_id="t-ver"), TaskKind.FORECAST,
                       config_version=1)
    assert rec.config_version == 1 and reg.get("t-ver").config_version == 1, \
        "register 应记录 config_version"
    reg.set_status("t-ver", TaskStatus.DISABLED)
    reg.register(types.SimpleNamespace(task_id="t-ver"), TaskKind.FORECAST,
                 config_version=2)
    assert reg.get("t-ver").config_version == 2, "版本更新应覆盖旧版本"
    assert reg.get("t-ver").status == TaskStatus.DISABLED, "版本更新保留启停状态"
    assert not reg.is_enabled("t-ver"), "is_enabled 应反映 DISABLED"
    reg.set_status("t-ver", TaskStatus.ENABLED)
    assert reg.is_enabled("t-ver"), "is_enabled 应反映 ENABLED"
    assert not reg.is_enabled("t-not-exist"), "不存在任务 is_enabled=False"
    _ok("config_version 记录/更新 + is_enabled")


# ================= 检测类型解析 =================

class FakeCore:
    """只实现 run_anomaly 用到的 C 调用；约束结果全部"满足"。

    result_client 用 None（无异常时不会写 S 事件）。
    窗口获取在 _run_anomaly_models 内（本测试 stub 掉，真路径由 gcad 集成测试覆盖）。
    数据规模给足（≥ 默认门槛 100），不触发数据门槛（门槛专项见 test_anomaly_data_gate）。
    """

    def __init__(self):
        self.constraint_calls = 0

    def check_constraints(self, constraint_ids, aligned_data=None):
        self.constraint_calls += 1
        return True, []

    def get_sequence_data_scale(self, sequence_ids):
        return [SequenceDataScale(sequence_id=sid, point_count=1000)
                for sid in sequence_ids]


def _parse_engine():
    engine = AnalysisEngine(core_client=FakeCore(), result_client=None,
                            config={"inference": {"window_size": 100}})
    seen = []
    # 2026-08-13 起 _run_anomaly_models 返回 (findings, step_ms)，stub 同步
    engine._run_anomaly_models = lambda t, mm, cv=0: seen.append(list(mm)) or ([], 0)
    return engine, seen


def _anomaly_task(task_id: str, methods: list[str]):
    return pb.AnomalyTaskConfig(
        task_id=task_id, sequence_ids=["a", "b"],
        methods=methods,
        semantic_context=pb.SemanticContext(constraint_ids=["c1"]),
    )


def test_methods_parsing() -> None:
    print("\n[检测类型解析]")
    # 组合：约束(迁C，P过滤掉) + 模型
    eng, seen = _parse_engine()
    eng.run_anomaly(_anomaly_task("t-combo", ["CONSTRAINT_CHECK", "CAUSAL_PATTERN"]))
    assert eng.core.constraint_calls == 0, "约束检查已迁 C，P 不应再调 check_constraints"
    assert seen == [["CAUSAL_PATTERN"]], f"模型方法应只剩 CAUSAL_PATTERN，实际 {seen}"
    _ok("组合：CONSTRAINT_CHECK 被过滤，只跑模型")

    # 只约束：P 不参与（S 直接下发 C）→ 不跑模型、无 finding
    eng, seen = _parse_engine()
    res = eng.run_anomaly(_anomaly_task("t-constraint", ["CONSTRAINT_CHECK"]))
    assert eng.core.constraint_calls == 0
    assert seen == [], "只约束不应跑模型"
    assert res.findings == [], "只约束 P 不产 finding（C 自检测自写 S）"
    _ok("只约束：P 不参与，不跑模型、无 finding")

    # 只模型：不调约束检查，模型方法完整传给检测
    eng, seen = _parse_engine()
    eng.run_anomaly(_anomaly_task("t-model", ["CAUSAL_PATTERN"]))
    assert eng.core.constraint_calls == 0, "只模型不应调约束检查"
    assert seen == [["CAUSAL_PATTERN"]]
    _ok("只模型：不调约束检查")

    # 空 methods → 默认只跑模型（DEFAULT_METHODS 已去掉 CONSTRAINT_CHECK）
    eng, seen = _parse_engine()
    eng.run_anomaly(_anomaly_task("t-empty", []))
    assert eng.core.constraint_calls == 0
    assert seen == [["CAUSAL_PATTERN"]], "空 methods 应默认走 CAUSAL_PATTERN"
    _ok("空 methods → 默认 CAUSAL_PATTERN")


def test_unknown_method() -> None:
    print("\n[未知检测方法]")
    engine = AnalysisEngine(core_client=FakeCore(), result_client=None,
                            config={"inference": {"window_size": 100}})
    task = pb.AnomalyTaskConfig(task_id="t-unknown", sequence_ids=["a"], methods=["FOO"])
    records = []
    handler = logging.Handler()
    handler.emit = lambda r: records.append(r.getMessage())
    logging.getLogger("analysis_engine").addHandler(handler)
    try:
        findings, step_ms = engine._run_anomaly_models(task, ["FOO"])
    finally:
        logging.getLogger("analysis_engine").removeHandler(handler)
    assert findings == [], "未知方法应跳过且不报错"
    assert any("未知检测方法" in m for m in records), f"应记录 warning，实际 {records}"
    _ok("未知方法记录 warning 并跳过")


# ================= _clean_matrix（P1-5 NaN 清洗） =================

def test_clean_matrix() -> None:
    print("\n[模型入口 NaN 清洗]")
    m = np.array([[np.nan, 1.0], [np.nan, np.nan], [3.0, np.nan], [np.nan, 2.0]],
                 dtype=np.float32)
    out = AnalysisEngine._clean_matrix(m)
    assert out.shape == (4, 2) and out.dtype == np.float32
    assert out[0, 0] == 0.0 and out[1, 0] == 0.0 and out[2, 0] == 3.0 \
        and out[3, 0] == 3.0, "前缀无值 → 补 0；有值后 NaN → 前值填充"
    assert out[0, 1] == 1.0 and out[1, 1] == 1.0 and out[2, 1] == 1.0 \
        and out[3, 1] == 2.0, "前值填充中间/末尾 NaN，末尾 2.0 覆盖填充"
    _ok("前值填充 NaN + 前缀补 0")

    m2 = np.array([[np.nan], [np.nan]], dtype=np.float32)     # 全 NaN 列 → 补 0
    out2 = AnalysisEngine._clean_matrix(m2)
    assert out2.shape == (2, 1) and float(out2[0, 0]) == 0.0 \
        and float(out2[1, 0]) == 0.0, "全缺失列应补 0"
    _ok("全缺失列补 0")

    m3 = np.array([1.0, np.nan, 3.0], dtype=np.float32)        # 1D 输入
    out3 = AnalysisEngine._clean_matrix(m3)
    assert out3.shape == (3, 1), "1D 应 reshape 成列"
    assert float(out3[1, 0]) == 1.0, "1D 也应前值填充"
    _ok("1D 输入 reshape + 填充")


# ================= ResultRepository =================

def test_repository() -> None:
    print("\n[ResultRepository]")
    repo = ResultRepository(maxlen=3)
    for i in range(5):
        repo.put("t-1", i)
    assert repo.latest("t-1") == 4, "latest 应取最近一条"
    _ok("put/latest")
    assert repo.history("t-1") == [2, 3, 4], "超 maxlen 应丢最旧，留最近 3 条"
    _ok("超 maxlen 丢最旧")
    assert repo.history("t-1", limit=2) == [3, 4], "limit 应限最近 N 条"
    _ok("history limit")
    assert repo.latest("t-none") is None and repo.history("t-none") == []
    _ok("无结果任务返回空")


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    test_registry()
    test_methods_parsing()
    test_unknown_method()
    test_clean_matrix()
    test_repository()
    print(f"\n框架单元测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
