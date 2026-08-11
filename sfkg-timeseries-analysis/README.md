# sfkg-timeseries-analysis 时序分析（P 端）

Python 时序分析模块：模型异常检测、时序预测与预警、归因建议。
通过 gRPC 从 C 端（sfkg-timeseries-core）获取数据，向 S 端（统一业务服务）
提供分析服务。

## 当前实现范围

- **P↔C 通讯**：`queryHistoryOverview`（数据规模）、`queryHistoryData`（历史数据，
  训练用）、`queryWindowData`（实时窗口）、`alignWindowData`（C 端对齐）、
  `computeBasicStatistics`（相关性先验）、`checkConstraints`（约束检查）；
- **P↔S 服务（空壳）**：7 个 RPC（任务同步 / 任务状态 / 结果查询 / 归因），S 端可调通；
- **模型**：异常检测（GCAD / DBSCAN / TREND_SHIFT / MUTUAL_COUPLING /
  HISTORICAL_MATCH 历史语义匹配框架）+ 时序预测（PatchTST + 离散序列 CatBoost）；
  `查规模 → 达标训练一次 → 可推理`，训练结果内存/磁盘缓存，首训复用；
- **历史语义匹配（HISTORICAL_MATCH）**：第四种异常检测方法（框架已就位）——
  确认历史事件增量进索引（`HistoricalEventIndex`，按 event_id 幂等），
  `HistoricalEventMatcher` 接口对齐其他异常模型（fit/detect/save/load），存盘带
  `model_type="historical-match"`，loader 按标记分发；**匹配核心为占位逻辑**（v1 只做
  序列集合 ⊆ 窗口任务 + 窗口内最大 z-score 偏差的结构化匹配），真实相似度模型
  （前兆匹配 / 窗口形状编码）后续接入；确认事件数据源可插拔（engine 构造时注入
  `historical_event_provider`），无来源时为空索引 → 无命中（安全）；命中写入与异常/
  预警共用 S 的 ReceiveAnomalyResult 同一 RPC；
- **离散序列预测**（按技术方案 [54][56] 三种数据类型三种预测法）：训练时按目标列历史
  原始取值类型**数据推断路由**——int64/bool → 离散、double → 连续、string（标签类）→
  `NOT_IMPLEMENTED` 干净结果；**自变量类离散**（无特征）→ 保持当前值（计划值接口未来
  接入）；**因变量类离散**（有特征）→ CatBoost 条件期望（内部 PatchTST 预测连续特征 +
  CatBoost 当前工况→因变量值）；模型存盘带 `model_type`，loader 按标记分发；
- **模型入口 NaN 清洗**：C 对齐补的 NaN 在喂模型前统一前值填充 + 补 0（预测与异常
  检测同规则）；
- **失败语义**：`data not ready` / `model not ready` 一律算成功（正常结果，不
  error_count、不报 S），默认任务一定成功，不做失败上报；
- **模型版本**：`config_version` 进模型缓存 key（`{task_id}@v{ver}` /
  `{task_id}:{method}@v{ver}`），版本变 → key 变 → 自动重训；**保留最近 2 个版本**
  （回滚到上一配置秒级复用旧模型不重训，磁盘有界）；
- **任务调度**：注册制生命周期（Sync 注册 → Scheduler 周期扫描 → 结果查询）；
  **双队列双 worker 池**——训练队列（默认 1 worker 串行）与推理队列（默认 2 worker 并行）
  分离，重型训练不阻塞其他任务的在线检测/预测；队列有界满则跳过下轮、inflight 去重、
  worker 消费前重校验（启停/删除/类型重建/版本变更中途的过时任务丢弃）、优雅退出；
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

见 `config.yaml`：C 端地址/端口、数据规模门槛、模型参数、对外服务端口、调度器参数
（`scheduler` 节：扫描周期、训练/推理队列深度与 worker 数）。

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
- **训练/推理隔离**：`scheduler.train_workers`（默认 1，重型训练串行）与
  `scheduler.infer_workers`（默认 2，在线推理并行）分离——训练再重也不抢推理 worker；
  队列有界，满则跳过下轮不阻塞，worker 消费前重校验保证启停/版本变更即时生效。
- **离散列需 int64 编码**：离散/连续路由按历史取值 Python 类型推断（int/bool→离散、
  float→连续、string→不支持）。float 类型存的离散值（如 98.0）会被判为连续走 PatchTST
  ——离散序列在 C 端请用 `int64_value` 编码，勿用 double_value 存整数。
