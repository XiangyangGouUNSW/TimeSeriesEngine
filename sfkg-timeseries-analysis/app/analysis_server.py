"""P 端 gRPC 服务入口：启动后供 S 端调用。

运行（模块根目录下）：
    python app/analysis_server.py [--core-address IP] [--core-port PORT]

默认监听 0.0.0.0:50053（config.yaml 的 server 节）。
C 端地址默认取 config.yaml 的 core 节，可用参数覆盖（联调时指向假 C / 真 C）。
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

    # 调用链引擎 + 服务
    engine = AnalysisEngine(core_client, s_client, cfg)
    servicer = AnalysisServicer(engine=engine)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_TimeseriesAnalysisServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"{server_cfg['address']}:{server_cfg['port']}")
    server.start()
    print(f"P 端服务已启动，监听 {server_cfg['address']}:{server_cfg['port']}")
    print(f"  C 端: {core_address}:{core_port} | S 端: "
          f"{s_cfg.get('address')}:{s_cfg.get('port')}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
