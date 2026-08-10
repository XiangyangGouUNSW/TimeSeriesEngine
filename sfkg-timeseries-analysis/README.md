# sfkg-timeseries-analysis 时序分析（P 端）

Python 时序分析模块：模型异常检测、时序预测与预警、归因建议。
通过 gRPC 从 C 端（sfkg-timeseries-core）获取数据，向 S 端（统一业务服务）
提供分析服务。

## 当前实现范围

- **P↔C 通讯**：`queryHistoryOverview`（数据规模）、`queryHistoryData`（历史数据，
  训练用）、`queryWindowData`（实时窗口）、`alignWindowData`（C 端对齐）、
  `computeBasicStatistics`（相关性先验）、`checkConstraints`（约束检查）；
- **P↔S 服务（空壳）**：7 个 RPC（任务同步 / 任务状态 / 结果查询 / 归因），S 端可调通；
- **模型**：异常检测（GCAD / DBSCAN / MUTUAL_COUPLING 等）+ 时序预测（PatchTST）；
  `查规模 → 达标训练一次 → 可推理`，训练结果内存/磁盘缓存，首训复用；
- **数据规则**：训练用历史数据；检测/预测输入一律用 C 端对齐后的实时窗口
  （P 只 reshape，不自己做对齐）；
- **归因建议**：接口返回 `NOT_IMPLEMENTED`（空壳）。

## 环境准备

需要 Python 3.10+、gRPC、numpy、pyyaml。推荐 conda：

```bash
conda create -n sfkg python=3.11 -y -c conda-forge --override-channels
conda run -n sfkg pip install -r requirements.txt
```

## 生成 gRPC stub

proto 是跨模块接口合同。改动 proto（或从 C 端/S 端同步新 proto）后运行脚本重新生成
（结果在 `generated/`，已加入 `.gitignore`，各端自己生成、不提交）：

```bash
tools/gen_proto.sh                       # 用当前环境 python
PYTHON=/path/to/python tools/gen_proto.sh   # 指定解释器（例如 conda 的 sfkg 环境）
```

依赖 `grpcio-tools`（`requirements.txt` 已含）。

> proto 同步提醒：仓库根在 `TimeSeriesEngine/`，三个分支各自带模块目录。P 端 proto 已
> 与 C 端最新版对齐（`alignWindowData` 无 `relation_ids`、`computeBasicStatistics` 无
> `relation_id`、关系用 `category_id`），`timeseries_analysis.proto` 用 S 端版。改 proto
> 属于三端接口合同，需三端讨论后定。

## 运行

```bash
# 1. 起 P 端对外服务（供 S 端调用，默认 0.0.0.0:50053）
python app/analysis_server.py

# 2. 模拟 S 端调用 7 个 RPC（另开终端）：
python tests/test_analysis_client.py
```

联调预演时先起一个"假 C 端"走真实 gRPC 传输（默认 `config.yaml` 的
`core.provider: mock`，连真 C 端时改成 `grpc`）：

```bash
python tools/fake_core_server.py     # 假 C 端，监听 50051
python app/analysis_server.py
```

## 配置

见 `config.yaml`：C 端地址/端口、数据规模门槛、模型参数、对外服务端口。

## 目录结构

- `app/`        入口程序（对外 gRPC 服务，供 S 端调用）
- `src/`        核心逻辑：CoreDataClient/Mock、grpc_client、servicer、训练、推理、模型、数据结构
- `tools/`      工具：`fake_core_server.py`（假 C 端，联调预演）、`gen_proto.sh`（重新生成 stub）、集成测试脚本
- `tests/`      测试：`test_analysis_client.py`（模拟 S 端调用）
- `proto/`      接口合同（`timeseries_core.proto` 与 C 共享、`timeseries_analysis.proto` 与 S）
- `generated/`  生成的 gRPC stub（gitignore，各端自己生成）
- `data/`       ETT 测试数据（模块自包含）
- `core/` `docs/`  文档（权威文档 + 理解文档，gitignore 不上传）

## 说明

- **对齐归属 C 端**：检测/预测输入调 `alignWindowData`，分桶/聚合/缺失填充/lag 调整
  由 C 完成，P 只把返回的 `AlignedWindowData` 转成 [时间×序列] 矩阵并按模型需要取尾部
  （异常检测取 `window_size` 行，预测取 `context_length` 行）。
- 数据用公开数据集 ETT-small（`data/ETT-small/ETTh1.csv`，变压器油温等 7 列），只用于测试流程。
- 80 核机器上需限 OpenMP/torch 线程（`SFKG_MAX_THREADS`，默认 4），否则 sklearn 拟合
  会因线程过订阅卡死（见 `anomaly_models.py` / `patchtst_forecaster.py`）。
