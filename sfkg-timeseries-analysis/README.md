# sfkg-timeseries-analysis 时序分析（P 端）

Python 时序分析模块：模型异常检测、时序预测与预警、归因建议。
通过 gRPC 从 C 端（sfkg-timeseries-core）获取数据，向 S 端（统一业务服务）
提供分析服务。

## 当前实现范围

- **P↔C 通讯**：`queryHistoryOverview`（数据规模）、`queryHistoryData`（历史数据），
  已与 C 端实际联调跑通「数据规模 → 训练 → 推理」闭环（ETT 测试数据）；
- **P↔S 服务（空壳）**：7 个 RPC（任务同步 / 任务状态 / 结果查询 / 归因），S 端可调通；
- **模型**：自回归（AR）小模型，`查规模 → 达标训练一次 → 可推理`，不做多轮/手动重训；
- **归因建议**：接口返回 `NOT_IMPLEMENTED`（空壳）。

## 环境准备

需要 Python 3.10+、gRPC、numpy、pyyaml。推荐 conda：

```bash
conda create -n sfkg python=3.11 -y -c conda-forge --override-channels
conda run -n sfkg pip install -r requirements.txt
```

## 生成 gRPC stub

proto 是跨模块接口合同。改动 proto 后重新生成（结果在 `generated/`，已加入
`.gitignore`，各端自己生成，不提交）：

```bash
python -m grpc_tools.protoc -I proto \
  --python_out=generated --grpc_python_out=generated \
  proto/timeseries_core.proto proto/timeseries_analysis.proto
```

## 运行

```bash
# 1. mock 演示（本地 ETT 假装 C 端，不需要网络）
python app/main.py

# 2. 起 P 端对外服务（供 S 端调用，默认 0.0.0.0:50053）
python app/analysis_server.py
# 另开终端，模拟 S 端调用 7 个 RPC：
python tests/test_analysis_client.py

# 3. 连真实 C 端（config.yaml 配好 C 端地址端口后）
python app/main.py --provider grpc
```

联调预演时也可以起一个"假 C 端"走真实 gRPC 传输：

```bash
python tools/fake_core_server.py     # 假 C 端，监听 50051
python app/main.py --provider grpc
```

## 配置

见 `config.yaml`：C 端地址/端口、数据规模门槛、模型参数、对外服务端口。

## 目录结构

- `app/`        入口程序：`main.py`（训练推理演示）、`analysis_server.py`（对外服务）
- `core/`       核心逻辑：CoreDataClient/Mock、grpc_client、servicer、训练、推理、模型、数据结构
- `tools/`      工具：`fake_core_server.py`（假 C 端，联调预演）
- `tests/`      测试：`test_analysis_client.py`（模拟 S 端调用）
- `proto/`      接口合同（`timeseries_core.proto` 与 C 共享、`timeseries_analysis.proto` 与 S）
- `generated/`  生成的 gRPC stub（gitignore，各端自己生成）
- `data/`       ETT 测试数据（模块自包含）

## 说明

- 模型是自回归 AR(order=5)：用前 5 个值预测下一个值，训练 = numpy 最小二乘。
  以后换 Transformer，只需保证接口 `fit(history)` / `forecast(history, steps)` 一致。
- 数据用公开数据集 ETT-small（`data/ETT-small/ETTh1.csv`，变压器油温等 7 列），只用于测试流程。
