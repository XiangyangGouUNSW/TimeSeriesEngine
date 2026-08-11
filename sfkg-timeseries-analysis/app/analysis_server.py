"""P 端 gRPC 服务入口：启动后供 S 端调用。

运行（模块根目录下）：
    python app/analysis_server.py [--core-address IP] [--core-port PORT]

默认监听 0.0.0.0:50053（config.yaml 的 server 节）。
C 端地址默认取 config.yaml 的 core 节，可用参数覆盖（联调时指向假 C / 真 C）。

启动时拉起三件套：TaskRegistry（注册表）+ ResultRepository（结果仓库）
+ Scheduler（固定周期后台线程），Sync 任务注册后由 Scheduler 周期执行。
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent import futures
from pathlib import Path

import grpc
import yaml

# 模块根目录 = app 的上一级
ROOT = Path(__file__).resolve().parent.parent
# 把 src/ 和 generated/ 加进模块搜索路径
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2_grpc as pb_grpc

from analysis_servicer import AnalysisServicer
from analysis_engine import AnalysisEngine
from grpc_client import GrpcCoreDataClient
from analysis_result_client import AnalysisResultClient
from task_registry import TaskRegistry
from result_repository import ResultRepository
from scheduler import Scheduler


def load_config() -> dict:
    with open(ROOT / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def serve() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="P 端对外服务")
    parser.add_argument("--core-address", default=None, help="覆盖 C 端地址")
    parser.add_argument("--core-port", type=int, default=None, help="覆盖 C 端端口")
    args = parser.parse_args()

    cfg = load_config()
    server_cfg = cfg.get("server", {"address": "0.0.0.0", "port": 50053})
    core_cfg = cfg.get("core", {})
    s_cfg = cfg.get("s", {"address": "localhost", "port": 50054})
    sched_cfg = cfg.get("scheduler", {"interval_seconds": 10})
    repo_cfg = cfg.get("result_repository", {"maxlen": 50})

    # C 端客户端（地址可用参数覆盖，便于联调指向假 C/真 C）
    core_address = args.core_address or core_cfg.get("address", "localhost")
    core_port = args.core_port or core_cfg.get("port", 50051)
    core_client = GrpcCoreDataClient(
        address=core_address, port=core_port,
        timeout_seconds=core_cfg.get("timeout_seconds", 30.0))

    # S 端客户端（写事件）
    s_client = AnalysisResultClient(
        address=s_cfg.get("address", "localhost"),
        port=s_cfg.get("port", 50054),
        timeout_seconds=s_cfg.get("timeout_seconds", 10.0))

    # 三大框架组件：注册表 + 结果仓库 + 周期调度器（Scheduler 由服务拉起）
    engine = AnalysisEngine(core_client, s_client, cfg)
    registry = TaskRegistry()
    repository = ResultRepository(maxlen=repo_cfg.get("maxlen", 50))
    scheduler = Scheduler(
        engine, registry, repository,
        interval_seconds=sched_cfg.get("interval_seconds", 10.0),
        train_queue_size=sched_cfg.get("train_queue_size", 8),
        infer_queue_size=sched_cfg.get("infer_queue_size", 32),
        train_workers=sched_cfg.get("train_workers", 1),
        infer_workers=sched_cfg.get("infer_workers", 2),
        train_timeout_s=sched_cfg.get("train_timeout_s", 300.0),
        infer_timeout_s=sched_cfg.get("infer_timeout_s", 60.0))
    servicer = AnalysisServicer(registry=registry, repository=repository,
                                engine=engine)

    server = grpc.server(futures.ThreadPoolExecutor(
        max_workers=server_cfg.get("max_workers", 4)))
    pb_grpc.add_TimeseriesAnalysisServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"{server_cfg['address']}:{server_cfg['port']}")
    server.start()
    scheduler.start()   # 后台调度：周期跑所有 ENABLED 任务
    print(f"P 端服务已启动，监听 {server_cfg['address']}:{server_cfg['port']}")
    print(f"  C 端: {core_address}:{core_port} | S 端: "
          f"{s_cfg.get('address')}:{s_cfg.get('port')}")
    print(f"  调度器: 每 {sched_cfg.get('interval_seconds', 10)}s 扫描 ENABLED 任务，"
          f"训练 worker {sched_cfg.get('train_workers', 1)} 个 / "
          f"推理 worker {sched_cfg.get('infer_workers', 2)} 个")

    server.wait_for_termination()

    # 优雅退出：停调度器，等当前 tick 跑完
    scheduler.stop()
    scheduler.join(timeout=5)
    print("P 端服务已退出")


if __name__ == "__main__":
    serve()
