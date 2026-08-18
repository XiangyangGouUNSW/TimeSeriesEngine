# 当前实现与数据组织简表

本文以当前代码为准，概括生产接入路径、主要性能手段，以及窗口、约束、对齐、
派生序列和历史数据使用的 C++ 结构体。完整协议定义见
[`proto/timeseries_core.proto`](../proto/timeseries_core.proto)，领域结构体主要位于
[`types.hpp`](../include/sfkg/timeseries/core/types.hpp)。

## 1. 基础类型

```cpp
using Timestamp = std::int64_t;       // 毫秒时间戳
using ProjectId = std::string;        // 项目/租户隔离键
using SequenceId = std::string;
using TimeseriesValue =
    std::variant<double, std::int64_t, bool, std::string>;
```

`TimeseriesValue` 是物理值类型；`SeriesKind`（`Continuous`、`Discrete`、
`Categorical`）是序列的业务语义，两者不是同一个概念。

## 2. IngestData 输入格式

生产入口为 gRPC：

```proto
message IngestDataRequest {
  repeated TimeseriesIngestData points = 1;
  bool return_resolved_data = 3;
  string project_id = 4;
}

message TimeseriesIngestData {
  optional string sequence_id = 1;
  string data_source_id = 2;
  string external_sequence_id = 3;
  int64 time = 4;
  TimeseriesValue value = 5;
  string project_id = 6;
}
```

其中 `TimeseriesValue` 是以下 oneof 之一：`double_value`、`int64_value`、
`bool_value`、`string_value`。

gRPC 转换后的 C++ 输入为：

```cpp
struct TimeseriesIngestData {
    std::optional<SequenceId> sequence_id;
    std::string data_source_id;
    std::string external_sequence_id;
    Timestamp time{};
    TimeseriesValue value;
    ProjectId project_id;
};
```

每个点必须满足以下一种标识方式：

- 提供非空 `sequence_id`；或者
- 同时提供 `data_source_id` 和 `external_sequence_id`，由
  `RuntimeConfigRegistry::resolveSequenceId()` 解析内部序列 ID。

解析时还会按 `RuntimeInstanceConfig.data_type` 校验物理类型，并拒绝非有限的
`double`。解析后的数据结构为：

```cpp
struct RawTimeseriesPoint {
    Timestamp time{};
    SequenceId sequence_id;
    TimeseriesValue value;
    ProjectId project_id;
};

struct TimeseriesBatch {
    std::vector<RawTimeseriesPoint> points;
    ProjectId project_id;
};

struct IngestResult {
    OperationResult operation;
    TimeseriesBatch resolved_data;
    ProjectId project_id;
};
```

这里的 `IngestResult` 是 Core 内部服务对象，不等于每次 RPC 都必须返回给调用者的
消息。`resolved_data` 由后续冷热流水线继续使用：冷路径写 TDengine，热路径更新
`WindowService`。因此，`TimeseriesIngestData` 是“外部标识可选、尚未解析”的输入，
进入 Core 内部处理的统一格式是 `TimeseriesBatch`，但它不需要每次通过网络返回。

对外有两种行为：

- 生产 RPC `ingestData`：默认只返回各阶段 `OperationResult`；只有请求中的
  `return_resolved_data=true` 时，才填充 `IngestDataResponse.resolved_data`。
- 细粒度 RPC `ingestAndResolveData`：它本身就是“解析并返回”的测试/回放接口，当前
  会返回 `IngestResponse.resolved_data`。

## 3. IngestData 处理流程

`ingestData` 的生产路径如下：

```text
IngestDataRequest
  -> TimeseriesIngestData
  -> IngestService::ingestAndResolveData()
  -> TimeseriesBatch
  -> 有界冷热任务队列
       ├─ 冷路径：TDengine 批量写入
       └─ 热路径：WindowService -> 派生序列 -> 约束检查/通知
```

响应按阶段返回：`resolve_result`、`storage_result`、`window_result`、
`derived_result`、`constraint_notification_result` 和总的 `operation`。
`storage_queued=true` 只表示冷写入已进入后台队列，不表示 TDengine 已完成落盘。

## 4. 算法层

本节只描述“数据如何被处理”，不展开结构体字段和并发优化。

### 4.1 接入标准化

`IngestService` 对每个输入点执行：标识解析 → 配置查找 → 类型/有限值校验 →
生成 `RawTimeseriesPoint`。合法点进入 `TimeseriesBatch`，非法点单独计数；这一层
不写 TDengine，也不更新窗口。

### 4.2 热窗口更新与查询

`WindowService` 按序列分组并更新 watermark：递增点追加，乱序点排序去重后进入迟到
缓冲，过期点从窗口前端淘汰。更新同时生成 `WindowUpdateResult`，记录受影响序列和
时间范围。查询对有序数据做时间二分，只返回目标范围及必要前后文，形成 `WindowData`。

### 4.3 对齐

`AlignmentService` 先确定时间桶，再对每个序列执行：固定 lag 平移 → bucket 聚合
（FIRST/LAST/AVERAGE/MAXIMUM/MINIMUM）→ 缺失值填充（NEAR/PREVIOUS/NEXT/LINEAR），
最后按统一时间轴生成 `AlignedWindowData`。增量对齐只处理受影响 bucket；缺少边界
上下文时回退到全量对齐。

### 4.4 约束检查

约束计算的是：

```text
Σ(coefficient × sample(variable, sample_offset))
```

结果必须落在 `[lower_bound, upper_bound]`。单序列规则直接读取 `WindowData`，多序列
规则读取 `AlignedWindowData`；增量接入时只检查受影响规则和范围，并补齐 offset 所需
的前后文。越界结果组织为 `ConstraintViolation`。

### 4.5 派生序列

Core 先收集公式依赖，再选择局部或全窗口数据。连续数值源使用前后点插值，随后按
时间评价线性组合或二叉表达式树。局部结果通过 `patchDerivedSequence()` 写回，全量
结果通过 `replaceDerivedSequence()` 写回；新旧刷新通过 generation 判断，避免旧结果
覆盖新结果。

### 4.6 统计与历史查询

统计对每个序列提取有效数值后计算基本指标；相关性从对齐样本中收集 target/source
数值对并计算 Pearson 系数。历史查询按注册的数据类型选择 TDengine 值列，必要时按
时间粒度取每个桶的最后值，最后整理为时间升序的 `TimeseriesBatch`。

## 5. 当前用于提高效率的实现

### 5.1 接入、队列和存储

- 输入批次和各类临时容器会预先 `reserve`，避免逐点扩容。
- `IngestTaskExecutor` 将冷写和热处理拆成两条并行通道；默认 4 个冷写 worker、
  1 个热 worker，队列默认容量为 128，可由
  `SFKG_INGEST_COLD_WORKERS`、`SFKG_INGEST_HOT_WORKERS`、
  `SFKG_INGEST_QUEUE_CAPACITY` 调整。
- 任务准入时同时检查冷热队列容量，避免出现只写热窗口或只写冷库的半任务。
- 同一 `sequence_id` 固定路由到同一个冷写 worker，保证该序列的写入顺序；不同
  序列可分片并行写入。
- TDengine 写入按序列分组，使用批量绑定语句；绑定缓冲区在拿连接锁之前构造，
  缩短连接锁持有时间。默认 4 条写连接，可由 `SFKG_TAOS_WRITE_CONNECTIONS`
  调整。

### 5.2 热窗口

`WindowService` 内部不是每个点一个 `std::map`，而是每个序列一个：

```cpp
struct SequenceWindow {
    mutable std::shared_mutex mutex;
    std::vector<RawTimeseriesPoint> points; // 已排序的主数据
    std::map<Timestamp, RawTimeseriesPoint> late_points; // 稀疏迟到/修正点
    std::size_t active_begin{0};             // 惰性过期边界
    std::optional<Timestamp> latest_time;
};
```

主要优化是：

- 正常递增写入直接追加到 `vector`，不需要树查找和逐点搬移；
- 迟到点先进入 `late_points`，达到 64 个后再批量合并，避免一次修正移动整个
  主 vector；
- 窗口淘汰通常只推进 `active_begin`，达到条件后才物理 compact；
- 全局索引锁只保护序列索引、watermark 和窗口元数据，点更新使用每序列锁；
- 不同序列的更新和宽序列集合的淘汰可通过内部 sequence executor 并行执行，
  默认最多 8 个窗口序列 worker（`SFKG_INGEST_HOT_SEQUENCE_WORKERS`）；
- 查询对有序 vector 使用 `lower_bound`，只合并请求范围及必要的前后文，不复制
  整个窗口。

每次增量更新返回：

```cpp
struct SequenceWindowUpdate {
    std::optional<Timestamp> affected_start_time;
    std::optional<Timestamp> affected_end_time;
    bool incremental_safe{false};
    bool window_evicted{false};
};

struct WindowUpdateResult {
    OperationResult operation;
    std::vector<SequenceId> changed_sequence_ids;
    std::unordered_map<SequenceId, SequenceWindowUpdate> sequence_updates;
    std::optional<Timestamp> affected_start_time;
    std::optional<Timestamp> affected_end_time;
    std::optional<Timestamp> window_start_time;
    std::uint64_t update_generation{0};
    bool incremental_safe{false};
    bool window_evicted{false};
};
```

### 5.3 派生序列和约束的增量计算

- 只有受影响的派生序列和约束才会重新计算；不相关序列不会触发查询。
- 连续数值派生序列只查询受影响时间范围，并额外取前后各一个点作为插值上下文；
  迟到、离散序列或无法证明安全时自动回退到全窗口计算。
- 派生计算将 `RawTimeseriesPoint` 转为连续的
  `std::vector<NumericPoint>`，使用单调 cursor 查找和插值，避免每个计算点都做
  树查找。
- 多个独立派生公式最多并行 4 路，计算完成后统一串行发布；
  `WindowUpdateResult.update_generation` 防止旧计算覆盖新窗口结果。
- 约束按单序列/多序列、增量安全/全量路径分组；每组只查询所需序列和范围，组间
  可并行检查。带 `sample_offset` 时会额外查询足够的前后文。
- 如果约束不依赖派生序列，派生刷新和约束检查可以并行执行。

### 5.4 配置、对齐和其他通用优化

- `RuntimeConfigRegistry` 使用 `std::shared_mutex`；实例、外部标识、约束、关系和
  派生配置均使用 `std::unordered_map` 索引，读取可并发，配置替换原子生效。
- `AlignmentService` 以每个序列为独立任务进行分桶，序列数量较大时并行；增量
  对齐只处理受影响 bucket，并保留边界 bucket 用于填充/插值。
- 统计、查询和转换阶段按批次预分配容器，避免逐点分配。

### 5.5 线程模型

当前不是“一条约束或一条派生规则对应一个线程”，而是分层分组：

```text
IngestTaskExecutor 热 worker
  └─ WindowService：按受影响 sequence 分发
       ├─ 派生刷新：按派生配置并行（最多 4 个计算 worker）
       └─ 约束检查：按约束组并行
            └─ AlignmentService：按序列并行（序列数 >= 16，最多 8 worker）
```

- 热 worker 默认 1 个；可通过 `SFKG_INGEST_HOT_WORKERS` 调整。相同序列的热任务
  通过 future 依赖保持提交顺序，不同序列可以并行。
- `WindowService` 默认使用 8 个 sequence worker，主要并行处理不同序列的窗口更新
  和淘汰。
- 派生计算单位是“派生配置”，不是表达式树中的每个节点；计算完成后统一串行发布。
- 连续接入时，约束先过滤未受影响规则，再按“单/多序列”和“增量安全/全量”形成
  约束组；组内规则顺序检查，组之间通过 `std::async` 并行。
- 多序列约束需要对齐；当参与对齐的序列达到 16 个时，才启用最多 8 个对齐 worker。
- 约束违反通知还有独立的单 worker 队列，通知网络服务不会阻塞热计算。直接调用
  `checkConstraints` 或 `alignWindowData` RPC 时，不会额外按约束组创建外层任务。

## 6. 窗口数据结构

对外领域结构为：

```cpp
struct WindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::unordered_map<SequenceId,
        std::vector<RawTimeseriesPoint>> sequence_values;
};

struct WindowQuery {
    std::vector<SequenceId> sequence_ids;
    std::optional<Timestamp> start_time;
    std::optional<Timestamp> end_time;
    std::size_t preceding_points{0};
    std::size_t following_points{0};
    bool preserve_window_bounds{false};
};

struct WindowQueryResult {
    OperationResult operation;
    WindowData data;
};
```

`SequenceWindow` 和 `WindowData` 不是继承关系，也不是同一个类型：前者是
`WindowService` 内部按 `sequence_id` 保存的可变状态，后者是查询生成的领域快照。
典型转换链路是：

```text
WindowService::sequence_windows_
  map<SequenceId, shared_ptr<SequenceWindow>>
  -> queryWindowData()
  -> WindowData.sequence_values
```

查询时会只输出当前有效范围，并把主 vector 与迟到点缓冲合并；因此调用方不需要知道
`active_begin`、`late_points`、锁和 watermark 等内部细节。

`sequence_values[sequence_id]` 中的点按时间升序排列，查询时间范围为半开区间
`[start_time, end_time)`；前后文点用于插值或按位置偏移的约束计算。

对齐后的数据不再按序列存储，而是按统一时间样本存储：

```cpp
struct AlignedSample {
    Timestamp time{};
    std::unordered_map<SequenceId, TimeseriesValue> values;
};

struct AlignedWindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::vector<AlignedSample> samples; // 时间严格递增
};
```

对齐配置为 `AlignmentConfig { std::vector<SequenceAlignmentConfig> sequences;
std::optional<std::int64_t> bucket_interval; }`。每个序列配置包含
`sequence_id`、`VariableRole`、可选 `BucketAggregation` 和可选 `GapFillMethod`。

## 7. 约束数据结构

一条约束由变量映射、上下界和线性项组成：

```cpp
struct ConstraintTerm {
    std::string variable;
    double coefficient{};
    std::size_t sample_offset{};
};

struct ConstraintRule {
    std::string constraint_id;
    std::unordered_map<std::string, SequenceId> variable_mapping;
    double lower_bound{};
    double upper_bound{};
    std::vector<ConstraintTerm> terms;
};
```

检查时支持 `WindowData` 或 `AlignedWindowData`，可选范围为：

```cpp
struct ConstraintCheckRange {
    Timestamp start_time{};
    Timestamp end_time{};
};
```

输出组织为：

```cpp
struct ConstraintTermValue {
    std::string variable;
    SequenceId sequence_id;
    double coefficient{};
    std::size_t sample_offset{};
    Timestamp sample_time{};
    double value{};
};

struct ConstraintViolation {
    std::string constraint_id;
    Timestamp anchor_time{};
    double lower_bound{};
    double upper_bound{};
    double evaluated_value{};
    std::vector<ConstraintTermValue> term_values;
};

struct ConstraintCheckResult {
    OperationResult operation;
    std::size_t evaluated_count{};
    bool satisfied{false};
    std::vector<ConstraintViolation> violations;
};
```

单序列规则可以直接使用 `WindowData`；映射到多个序列的规则需要先得到
`AlignedWindowData`。`sample_offset` 从 0 开始且非递减。

## 8. 运行时配置和关系数据

```cpp
struct RuntimeInstanceConfig {
    SequenceId sequence_id;
    std::string data_source_id;
    std::string external_sequence_id;
    std::string category_id;
    std::string data_type;
    SeriesKind series_kind{SeriesKind::Unspecified};
};

struct RuntimeConstraintConfig {
    ConstraintRule rule;
    bool enabled{false};
};

struct RelationLagRange { std::int64_t min{}; std::int64_t max{}; };
using RelationLagSpec = std::variant<std::monostate,
    std::int64_t, RelationLagRange>;

struct RuntimeRelationSource {
    SequenceId source_sequence_id;
    double weight{};
    RelationLagSpec lag;
};

struct RuntimeRelationConfig {
    std::string relation_id;
    std::vector<RuntimeRelationSource> sources;
    SequenceId target_sequence_id;
    std::string relation_type;
    double confidence{};
    bool enabled{false};
};

struct RuntimeWindowConfig {
    Timestamp window_size{kDefaultWindowSizeMs};
};
```

当前对齐实现只支持关系中的固定 lag；lag range 和无 lag 的动态处理仍不是完整实现。

## 9. 派生序列数据结构

```cpp
struct DerivedLinearTerm {
    SequenceId sequence_id;
    double coefficient{};
};

struct DerivedLinearCombination {
    std::vector<DerivedLinearTerm> terms;
    double bias{};
};

struct DerivedExpression; // 二叉表达式树节点
struct DerivedBinaryExpression {
    DerivedOperator operation{DerivedOperator::Unspecified};
    std::shared_ptr<DerivedExpression> left;
    std::shared_ptr<DerivedExpression> right;
};

struct DerivedExpression {
    enum class NodeKind { Sequence, Constant, Binary };
    NodeKind kind{NodeKind::Sequence};
    SequenceId sequence_id;
    double constant{};
    DerivedBinaryExpression binary;
};

using DerivedFormula =
    std::variant<DerivedLinearCombination, DerivedExpression>;

struct RuntimeDerivedSeriesConfig {
    SequenceId derived_sequence_id;
    bool enabled{false};
    DerivedFormula formula;
};
```

派生序列只写入 `WindowService` 的内存窗口，不写入 TDengine，因此不会出现在历史
查询结果中。

## 10. 历史和统计结果

历史明细使用同一个 `TimeseriesBatch`：

```cpp
struct HistoryQuery {
    std::vector<SequenceId> sequence_ids;
    Timestamp start_time{};
    Timestamp end_time{};
    std::optional<std::int64_t> granularity;
};

struct HistoryQueryResult {
    OperationResult operation;
    TimeseriesBatch data;
};
```

`granularity` 指定时，每个序列每个时间桶保留最后一个值。历史概览使用：

```cpp
struct HistorySeriesSummary {
    SequenceId sequence_id;
    std::size_t point_count{0};
    std::optional<Timestamp> first_time;
    std::optional<Timestamp> last_time;
};

struct HistoryOverview {
    std::size_t total_point_count{0};
    std::size_t sequence_count{0};
    std::vector<SequenceId> column_names;
    std::optional<Timestamp> first_time;
    std::optional<Timestamp> last_time;
    std::vector<HistorySeriesSummary> series;
};
```

统计结果按序列保存指标，并可附带关系相关性向量：

```cpp
struct SequenceCorrelation {
    SequenceId independent_sequence_id;
    double coefficient{};
};

struct CorrelationVector {
    SequenceId dependent_sequence_id;
    std::vector<SequenceCorrelation> correlations;
};

struct StatisticsResult {
    OperationResult operation;
    std::unordered_map<SequenceId,
        std::unordered_map<std::string, TimeseriesValue>> sequence_metrics;
    std::optional<CorrelationVector> correlation_vector;
};
```

## 11. 持久化组织

原始数据在 TDengine 超级表中按四种物理值列保存：`d_value`、`i_value`、`b_value`、
`s_value`，并使用 `sequence_id`、`value_type` 作为标签。内存热窗口和派生窗口不
依赖 TDengine；Core 重启后需要重新同步运行时配置，但 TDengine 中的历史原始数据
仍然保留。
