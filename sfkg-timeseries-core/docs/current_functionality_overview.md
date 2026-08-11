# 时序核心当前功能介绍

## 1. 模块定位

`sfkg-timeseries-core` 是时序数据处理核心服务，主要负责：

- 接收统一服务同步的运行时配置；
- 接入、识别和标准化时序数据；
- 将原始数据写入 TDengine，并维护内存热窗口；
- 提供窗口查询、对齐、统计、约束检查和历史数据查询；
- 通过 Protobuf/gRPC 对外提供统一接口。

Core 只保存运行时配置和内存窗口，不负责业务配置持久化，也不负责启动
TDengine。Core 重启后需要由统一服务重新同步配置，TDengine 中的历史数据不会
因此被清除。

## 2. 数据模型

### 2.1 时序值

当前 `TimeseriesValue` 支持四种物理值类型：

- `double`；
- `int64`；
- `bool`；
- `string`。

`RuntimeInstanceConfig.data_type` 用于兼容和校验物理类型名称，例如
`double`、`int64`、`bool`、`string` 及其部分别名。`SeriesKind` 是业务语义分类，
包括连续、离散和类别值，与物理存储类型不是同一个概念。

### 2.2 窗口和对齐数据

- `WindowData`：按 `sequence_id` 保存各序列的原始窗口点；
- `AlignedWindowData`：按统一时间采样点保存多个序列的值；
- `AlignmentConfig`：可选指定序列角色、分桶间隔、聚合方式和缺失值填充方式；
  未指定时，Core 从本地 `RuntimeConfigRegistry` 的 `SeriesKind` 补全默认策略，
  并从当前窗口推导最小正时间间隔；
- `RuntimeRelationConfig`：明确一个 target 序列以及一个或多个 source 序列，可
  附带权重、关系类型、置信度和 lag。

## 3. 运行时配置管理

Core 通过以下 gRPC 接口接收配置，并保存在内存注册表中：

### 3.1 实例配置

`syncInstanceConfigs` 支持按 `sequence_id` 增量新增或更新实例配置，同时建立
`data_source_id + external_sequence_id` 到内部序列 ID 的索引。

接入数据时可以直接提供 `sequence_id`，也可以只提供数据源和外部序列 ID，Core
负责解析为内部序列 ID，并校验外部标识是否与内部 ID 一致。

### 3.2 约束配置

`syncConstraints` 支持约束规则的增量更新。Core 会校验约束 ID、上下界、变量映射、
系数和采样偏移等结构，只有有效配置才会进入运行时注册表。

### 3.3 关系配置

`syncRelations` 支持关系的增量更新。关系中明确保存 target 和 source，Core 会
校验关系 ID、目标序列、来源序列、重复序列、权重和启用状态等内容。

### 3.4 窗口配置

`syncWindowConfig` 用于设置当前 Core 实例的热窗口长度。当前一个 Core 实例
使用一份全局窗口配置；如果统一服务没有同步窗口配置，则默认使用 3 天。

该配置不会根据单次 `ingestData` 请求中的点数变化，生产接入请求也不再携带窗口
长度。不同任务需要不同窗口长度时，当前建议使用不同 Core 实例；按 `task_id`
在同一 Core 内维护多份窗口配置尚未实现。

### 3.5 派生序列配置

`syncDerivedSeriesConfigs` 用于注册由已有连续数值序列计算得到的派生序列。当前
支持两种公式：

- 线性组合：`bias + c1 * source1 + c2 * source2 + ...`；
- 二叉表达式树：常量、序列叶子，以及加减乘除运算节点，可以表达多个变量的组合。

Core 会检查来源序列已经注册为连续数值序列，并在配置同步成功后立即根据当前热窗口
计算。后续 `ingestData` 更新热窗口时也会自动刷新派生结果。不同来源的时间点不完全
一致时，连续序列在相邻点之间采用线性插值；没有足够邻点时该时刻暂不产生结果。

派生序列只保存在内存热窗口中，不写入 TDengine 原始数据超级表，也不会出现在历史
查询结果中。派生序列 ID 不能与原始实例序列 ID 冲突。

## 4. 数据接入和存储

### 4.1 完整接入流程

`ingestData` 是主要生产接入接口，处理流程为：

```text
gRPC 请求
  → protobuf 转换
  → 序列解析与数据类型校验
  → 有界冷热任务队列准入
  → TDengine 原始数据写入       ┐
  → 内存热窗口更新               ├ 并行执行
  → 刷新派生序列热窗口视图
  → 检查当前窗口中的已启用约束
  → 有违反时调用统一服务的 ReceiveConstraintResult
  → 返回总结果和各阶段结果
```

该接口支持一次请求携带一批点，并分别返回：

- `resolve_result`：数据识别和标准化结果；
- `storage_result`：TDengine 写入结果；
- `window_result`：热窗口更新结果；
- `constraint_notification_result`：约束检查及异常通知结果；没有违反时不发送通知；
- `derived_result`：派生序列热窗口刷新结果；派生结果不落盘；
- `operation`：综合结果。

冷热写入不是数据库事务；任务进入有界队列前会同时为冷热两条通道预留容量，任一通道
没有容量时整个请求返回 `OPERATION_CODE_UNAVAILABLE`，不会发生只写冷库或只写热窗口。
任务被接纳后，冷写和热写并行执行，Core 仍等待两条通道完成后返回原有的分阶段结果。
一处成功、另一处失败时会通过分阶段结果报告。约束异常通知失败不会回滚已经完成的
冷、热写入，但会使综合结果变为部分成功。`return_resolved_data` 可用于控制是否返回
标准化后的数据副本。

### 4.2 细粒度接入接口

以下接口保留用于测试、数据回放和补偿流程：

- `ingestAndResolveData`：只执行数据解析和序列识别；
- `writeRawData`：直接将已经标准化的原始点写入 TDengine；
- `buildTimeWindow`：只更新内存热窗口。

### 4.3 TDengine 原始数据存储

原始数据使用可配置的数据库和超级表，配置项为：

- `SFKG_TAOS_DB`：业务数据库；
- `SFKG_TAOS_RAW_STABLE`：原始数据超级表，默认是 `raw_timeseries_data`。

超级表按物理类型保存 `double`、`int64`、`bool`、`string` 四类值列，并使用
`sequence_id` 和 `value_type` 作为标签。写入路径采用批量绑定方式，并支持多个
序列在同一批次中写入。冷写默认使用 4 条 TDengine 写连接；同一批次按其首个序列
固定选择连接，独立序列可以分布到不同连接。

## 5. 内存热窗口

`WindowService` 负责维护最近一段时间的时序数据：

- 按序列和时间保存数据；
- 使用最新时间点作为 watermark；
- 按配置的窗口长度自动删除过期数据；
- 支持按序列和时间范围查询窗口数据；
- 对并发写入和查询使用互斥保护。

窗口长度由 `syncWindowConfig` 设置，未设置时默认是 3 天。窗口配置属于当前 Core
实例，而不是单次写入批次；`buildTimeWindow` 细粒度接口仍保留显式窗口长度参数，
用于测试、回放和补偿流程。

对应 gRPC 接口为：

- `buildTimeWindow`；
- `queryWindowData`。

## 6. 对齐功能

`alignWindowData` 支持从调用方提供 `WindowData`，或者根据窗口查询条件由 Core
先读取当前窗口再进行对齐。

当前已实现：

- 按固定时间间隔分桶；
- `FIRST`、`LAST`、`AVERAGE`、`MAXIMUM`、`MINIMUM` 聚合；
- `NEAR`、`PREVIOUS`、`NEXT`、`LINEAR` 缺失值填充；
- 使用 Relation 中的固定 lag 对 target/source 序列进行时间调整；
- 返回带统一时间轴的 `AlignedWindowData`；
- 对数值聚合和线性插值进行有限值检查。

当前明确限制：

- lag range 尚未实现；
- 未设置 lag 的关系尚未作为动态 lag 处理；
- `AVERAGE`、`MAXIMUM`、`MINIMUM` 和线性填充要求数据具有有限数值含义；
- 连续型序列默认使用 `AVERAGE + LINEAR`，离散型和类别型序列默认使用
  `LAST + PREVIOUS`；未指定类型时使用兼容性更强的 `FIRST + NEAR`；显式传入的
  聚合和填充策略优先于默认值。

## 7. 基本统计和相关性

底层 `StatisticsService` 支持两种数据结构；gRPC 接口另外支持传入窗口查询条件，
由 Core 先读取窗口再计算统计结果：

### 7.1 WindowData 统计

对每个序列计算：

- `count`；
- `first_time`、`last_time`；
- `sum`；
- `mean`；
- `min`、`max`；
- `variance`；
- `stddev`。

方差和标准差使用总体统计口径，即除以样本数量，而不是样本数量减一。非数值
序列会返回部分成功或失败结果，但不会被强行转换为数值。

### 7.2 AlignedWindowData 相关性

对齐数据配合已注册 Relation 时，Core 会：

- 根据 Relation 明确 target 和 source；
- 计算 target 与每个 source 的 Pearson 相关系数；
- 返回以 target 为因变量、source 为自变量的相关性向量；
- 同时返回各序列的基本统计结果。

相关性要求存在足够的、有效且有变化的数值配对；常数序列或有效配对不足时会
报告失败或部分成功。

## 8. 约束检查

`checkConstraints` 支持使用以下三种数据来源：

- 直接传入 `WindowData`；
- 直接传入 `AlignedWindowData`；
- 传入窗口查询条件，由 Core 先读取窗口。

当前支持：

- 一个或多个线性约束规则；
- 变量到序列的映射；
- 系数和非递减采样偏移；
- 上下界判断；
- 约束是否满足、评估次数和详细违反信息；
- 违反时返回 anchor time、实际计算值、上下界和各项取值。

`WindowData` 适合检查映射到单个序列的规则；跨多个序列的规则应使用
`AlignedWindowData`。该功能是基于规则的约束检查，不是机器学习意义上的异常检测。

## 9. 历史数据查询

### 9.1 历史明细查询

`queryHistoryData` 支持：

- 指定一个或多个序列；
- 指定半开时间区间 `[start_time, end_time)`；
- 返回原始时间点；
- 可选按毫秒粒度分桶；
- 分桶后每个序列每个时间桶保留最后一个值；
- 返回按时间升序排列的数据，相同时间按请求中的序列顺序排列。

查询会结合已注册序列的物理类型选择对应值列；混合类型或类型未知时使用通用
字段读取路径，以保证兼容性。

### 9.2 历史概览查询

`queryHistoryOverview` 支持查询：

- 总点数；
- 序列数量；
- 当前有数据的序列名；
- 总体最早和最晚时间；
- 每个序列的点数、最早时间和最晚时间。

序列和时间条件可以省略，省略时查询当前数据库中已有的全部历史数据。

## 10. gRPC 接口总览

| 分类 | 接口 |
| --- | --- |
| 配置同步 | `syncInstanceConfigs`、`syncWindowConfig`、`syncConstraints`、`syncRelations`、`syncDerivedSeriesConfigs` |
| 数据接入 | `ingestData`、`ingestAndResolveData` |
| 存储和窗口 | `writeRawData`、`buildTimeWindow`、`queryWindowData` |
| 数据处理 | `alignWindowData`、`computeBasicStatistics`、`checkConstraints` |
| 历史查询 | `queryHistoryData`、`queryHistoryOverview` |

所有接口通过 `OperationResult` 返回统一状态码、成功数量、失败数量和说明信息。

## 11. 并发和性能现状

- gRPC 服务支持多个 RPC 并发进入；
- 运行时配置注册表使用读写锁，允许并发读取并保证配置更新的原子可见性；
- 热窗口使用互斥锁保护写入和查询；
- TDengine 写入使用批量写入和类型分列绑定；
- 当前没有独立的异步写入队列、背压 buffer 或任务调度线程池；
- 当前写入优化已减少批次内无用内存分配，并缩短写入锁范围；
- 本地 8 并发 worker、每批 1000 点、持续 60 秒的 gRPC 压测达到约 29.5 万条/秒，
  该结果是本机和局域网部署能力的参考，不等同于所有远程网络环境下的吞吐。

## 12. 当前未实现的功能边界

当前代码中没有独立的预测 RPC 或预测模型实现，也没有通用机器学习异常检测模块。
现有“异常”相关能力主要是：

- 约束规则违反检测；
- 数据类型、时间范围、配置和输入合法性检查；
- 对齐和统计过程中的有限值、缺失值检查。

跨机器部署时，Core 仍需由统一服务通过 gRPC 重新同步运行时配置；网络延迟、批次
大小和并发数会影响实际吞吐。详细协议定义以
[`proto/timeseries_core.proto`](../proto/timeseries_core.proto) 为准。
