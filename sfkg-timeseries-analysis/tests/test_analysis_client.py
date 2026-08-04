"""测试客户端：连上 P 端空壳服务，调用全部 7 个 RPC，验证 S↔P 通讯。

用法（先起 analysis_server.py，再另开终端跑本文件）：
    python test_analysis_client.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import grpc

# 生成的 stub 在模块根目录的 generated/ 下
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generated"))
import timeseries_analysis_pb2 as pb
import timeseries_analysis_pb2_grpc as pb_grpc

ADDRESS = "localhost:50053"   # 和 config.yaml 的 server 节一致


def _meta(req_id: str) -> pb.RequestMeta:
    return pb.RequestMeta(request_id=req_id, trace_id="t1", sent_at_ms=0)


def main() -> None:
    stub = pb_grpc.TimeseriesAnalysisServiceStub(grpc.insecure_channel(ADDRESS))

    # 1. 同步异常任务
    ack = stub.SyncAnomalyTask(pb.SyncAnomalyTaskRequest(
        meta=_meta("r1"),
        task=pb.AnomalyTaskConfig(task_id="task-anomaly-001", task_name="测试异常任务"),
    ))
    print("SyncAnomalyTask    ->", pb.AnalysisStatus.Name(ack.status), ack.message)

    # 2. 同步预测任务
    ack = stub.SyncForecastTask(pb.SyncForecastTaskRequest(
        meta=_meta("r2"),
        task=pb.ForecastTaskConfig(task_id="task-forecast-001", task_name="测试预测任务"),
    ))
    print("SyncForecastTask   ->", pb.AnalysisStatus.Name(ack.status), ack.message)

    # 3. 任务状态
    ack = stub.UpdateTaskStatus(pb.UpdateTaskStatusRequest(
        meta=_meta("r3"),
        task_id="task-anomaly-001",
        status=pb.TASK_STATUS_ENABLED,
    ))
    print("UpdateTaskStatus   ->", pb.AnalysisStatus.Name(ack.status), ack.message)

    # 4. 异常结果查询
    resp = stub.QueryAnomalyResults(pb.QueryAnomalyResultsRequest(
        query=pb.ResultQuery(meta=_meta("r4"), task_id="task-anomaly-001")))
    print("QueryAnomalyResults -> 结果数 =", len(resp.results))

    # 5. 预测结果查询
    resp = stub.QueryForecastResults(pb.QueryForecastResultsRequest(
        query=pb.ResultQuery(meta=_meta("r5"), task_id="task-forecast-001")))
    print("QueryForecastResults -> 结果数 =", len(resp.results))

    # 6. 异常归因（空壳）
    dr = stub.GenerateDiagnosis(pb.DecisionRequest(
        meta=_meta("r6"), event_id="evt-001"))
    print("GenerateDiagnosis  ->", pb.AnalysisStatus.Name(dr.status), dr.message)

    # 7. 优化建议（空壳）
    dr = stub.GenerateSuggestion(pb.DecisionRequest(
        meta=_meta("r7"), event_id="evt-001"))
    print("GenerateSuggestion ->", pb.AnalysisStatus.Name(dr.status), dr.message)

    print("\n全部 7 个 RPC 调用完成 ✓（空壳服务通讯正常）")


if __name__ == "__main__":
    main()
