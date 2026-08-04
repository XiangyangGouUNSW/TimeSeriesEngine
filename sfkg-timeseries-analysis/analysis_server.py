"""P 端 gRPC 服务入口（空壳）：启动后供 S 端调用。

运行（conda 环境已激活）：
    python analysis_server.py
默认监听 0.0.0.0:50053（可在 config.yaml 的 server 节配置）。
"""

from __future__ import annotations

import logging
import sys
from concurrent import futures
from pathlib import Path

import grpc
import yaml

# 让生成的 stub 可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))
import timeseries_analysis_pb2_grpc as pb_grpc

from analysis_servicer import AnalysisServicer

SHELL_DIR = Path(__file__).resolve().parent


def load_server_config() -> dict:
    with open(SHELL_DIR / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("server", {"address": "0.0.0.0", "port": 50053})


def serve() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    server_cfg = load_server_config()
    address = server_cfg["address"]
    port = server_cfg["port"]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_TimeseriesAnalysisServiceServicer_to_server(
        AnalysisServicer(), server)
    server.add_insecure_port(f"{address}:{port}")
    server.start()
    print(f"P 端空壳服务已启动，监听 {address}:{port}（Ctrl+C 停止）")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
