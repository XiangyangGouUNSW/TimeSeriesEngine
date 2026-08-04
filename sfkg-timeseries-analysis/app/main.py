"""P 端入口：跑通「数据规模 → 训练 → 推理」的最小闭环。

运行方法（模块根目录下）：
    python app/main.py

现在 provider=mock，用本地 ETT 数据假装 C 端；
C 端服务就绪后：python app/main.py --provider grpc
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

# 模块根目录 = app 的上一级
ROOT = Path(__file__).resolve().parent.parent
# 把 core/ 和 generated/ 加进模块搜索路径
for _p in (str(ROOT / "core"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from core_client import MockCoreDataClient
from training_loop import TrainingLoop
from inference_service import InferenceService, fmt


def load_config() -> dict:
    with open(ROOT / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_core_client(cfg: dict, provider_override: str | None = None):
    """根据配置创建 CoreDataClient。provider_override 可覆盖配置文件。"""
    provider = provider_override or cfg["core"]["provider"]
    if provider == "mock":
        csv_path = ROOT / cfg["data"]["csv_path"]
        return MockCoreDataClient(csv_path=str(csv_path),
                                  seq_prefix=cfg["data"]["seq_prefix"])
    elif provider == "grpc":
        from grpc_client import GrpcCoreDataClient
        return GrpcCoreDataClient(
            address=cfg["core"]["address"],
            port=cfg["core"]["port"],
            timeout_seconds=cfg["core"].get("timeout_seconds", 30.0),
        )
    raise ValueError(f"未知的 core.provider: {provider}")


def main() -> None:
    parser = argparse.ArgumentParser(description="P 端空壳演示")
    parser.add_argument("--provider", choices=["mock", "grpc"], default=None,
                        help="覆盖 config.yaml 里的 core.provider")
    args = parser.parse_args()

    print("======== P 端空壳启动 ========")
    cfg = load_config()
    client = build_core_client(cfg, provider_override=args.provider)

    # ---- 第 1 步：轮询数据规模 → 达标就训练 ----
    loop = TrainingLoop(client, cfg)
    if not loop.poll_and_train():
        print("数据始终不够，演示结束")
        return

    # ---- 第 2 步：推理，必须有输出 ----
    print("\n---------- 开始推理 ----------")
    svc = InferenceService(client, loop.store, cfg)
    output = svc.run_inference()
    if output is None:
        return

    print(f"\n===== 推理输出（证明链路跑通）=====")
    print(f"目标序列: {output.sequence_id}")
    print(f"{'时间':<16} {'预测值':>10}")
    for ts, v in zip(output.timestamps_ms, output.values):
        print(f"{fmt(ts):<16} {v:>10.2f}")

    # ---- 第 3 步（加分）：拿真实值对比，证明预测不是瞎编的 ----
    if hasattr(client, "get_values_after"):
        actuals = client.get_values_after(
            output.sequence_id, after_time_ms=output.window_last_time_ms,
            count=len(output.values))
        errors = [abs(p - a) for p, a in zip(output.values, actuals)]
        print(f"\n前 {min(6, len(actuals))} 步 预测 vs 真实：")
        for i in range(min(6, len(actuals))):
            print(f"  {fmt(output.timestamps_ms[i])}: 预测 {output.values[i]:8.2f}  "
                  f"真实 {actuals[i]:8.2f}  误差 {errors[i]:.2f}")
        print(f"\n平均绝对误差 MAE = {sum(errors) / len(errors):.2f}")

    print("\n======== 闭环跑通：数据规模 → 训练 → 推理 ✓ ========")


if __name__ == "__main__":
    main()
