"""P 端 gRPC 服务入口（空壳）：启动后供 S 端调用。

运行（模块根目录下）：
    python app/analysis_server.py
默认监听 0.0.0.0:50053（可在 config.yaml 的 server 节配置）。
"""

from __future__ import annotations

import logging
import sys
from concurrent import futures
from pathlib import Path

import grpc
import yaml

# 模块根目录 = app 的上一级
ROOT = Path(__file__).resolve().parent.parent
# 把 core/ 和 generated/ 加进模块搜索路径
for _p in (str(ROOT / "core"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import timeseries_analysis_pb2_grpc as pb_grpc

from analysis_servicer import AnalysisServicer


def load_server_config() -> dict:
    with open(ROOT / "config.yaml", encoding="utf-8") as f:
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
