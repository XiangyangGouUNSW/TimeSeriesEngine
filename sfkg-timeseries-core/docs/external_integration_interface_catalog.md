# sfkg-timeseries-core 外部对接接口完整清单

本文是当前代码版本面向外部调用方的完整对接清单，覆盖 Proto/gRPC、Core 的约束回调、
可嵌入的 C++ 公共函数、启动与环境变量、TDengine 和运维入口。完整线协议以
[`proto/timeseries_core.proto`](../proto/timeseries_core.proto) 为最终依据；本文补充
源码中没有直接表达的校验、异步和生命周期语义。

## 1. 对接边界

| 对接方 | 方向 | 接口 | 用途 |
|---|---|---|---|
| 统一服务/数据客户端 | 调用 Core | `TimeseriesCoreService` | 配置同步、写入、窗口、对齐、统计、约束和历史查询 |
| Core | 调用统一服务 | `ReceiveConstraintResult` | 约束违反异步通知 |
| C++ 嵌入方 | 调用 Core 库 | `sfkg::timeseries::core` 公共头文件 | 同进程测试、回放或嵌入式集成 |
| Core | 连接 TDengine | `TaosClient` 和环境变量 | 原始数据持久化、历史查询 |
| 运维方 | 启动/清理 | `main.cpp`、shell 脚本 | 进程监听、数据库连接和测试清理 |

Core 不保存统一服务的业务配置持久化副本。配置同步后只保存在当前进程内存中；Core
重启后必须重新同步，TDengine 中的原始历史数据不会因此删除。

## 1.1 Project ID 隔离约定

`project_id` 是所有有状态数据的项目/租户隔离键。同步实例、约束、关系、窗口和派生配置，
以及 ingest、窗口、对齐、统计、约束检查和历史查询请求，都应显式携带同一个
`project_id`。点、批次、窗口数据和响应也带有该字段，便于在跨层转换时保持上下文。

Core 进程和 TDengine 数据库仍然各启动一份；Core 按项目在同一个数据库中按需创建独立的
原始数据超级表，项目之间不共享表、运行时配置或内存热窗口。项目 ID 不直接作为 SQL
标识符拼接，而是转换为稳定、安全的表名后缀。

空 `project_id` 为旧客户端兼容起见映射为 `default`。新客户端不要依赖这个兼容行为，
并应在每个请求中显式传值。项目 ID 在请求级字段和嵌套对象同时出现时，请求级字段为
权威值。

## 2. 传输共性

### 2.1 包、连接和错误

```proto
syntax = "proto3";
package sfkg.timeseries.core.v1;
```

生成 C++ 代码：

```cpp
#include "timeseries_core.pb.h"
#include "timeseries_core.grpc.pb.h"
namespace pb = ::sfkg::timeseries::core::v1;
```

Core 当前使用 gRPC 明文连接 `grpc::InsecureServerCredentials()`，默认监听
`0.0.0.0:50051`；代码没有启用 TLS、鉴权或 reflection，访问控制由部署环境负责。
标准 gRPC health check service 已启用。

健康检查使用 gRPC 标准服务 `grpc.health.v1.Health`（`Check`/`Watch`），没有额外的
Core 私有健康检查消息；部署探针应同时确认 TDengine 可用，因为 Core 启动时会先初始化
TDengine schema。

绝大多数业务错误仍返回 gRPC transport `OK`，调用方必须检查响应中的
`OperationResult.code`：

```proto
enum OperationCode {
  OPERATION_CODE_OK = 0;
  OPERATION_CODE_PARTIAL_SUCCESS = 1;
  OPERATION_CODE_INVALID_ARGUMENT = 2;
  OPERATION_CODE_NOT_FOUND = 3;
  OPERATION_CODE_FAILED_PRECONDITION = 4;
  OPERATION_CODE_UNAVAILABLE = 5;
  OPERATION_CODE_INTERNAL_ERROR = 6;
  OPERATION_CODE_NOT_IMPLEMENTED = 7;
}

message OperationResult {
  OperationCode code = 1;
  uint64 success_count = 2;
  uint64 failed_count = 3;
  string message = 4;
}

message SyncResponse {
  bool success = 1;
  string message = 2;
}
```

只有未处理异常才会返回 gRPC `INTERNAL`；参数、状态、存储和配置错误写在业务结果中。
`success_count`/`failed_count` 的单位随 RPC 不同，通常是点数、配置项数、序列数或样本数，
不能跨 RPC 直接比较。

### 2.2 时间和值约定

- 所有时间和窗口长度均为毫秒 `int64`；
- 查询范围统一为半开区间 `[start_time, end_time)`；
- 时序值只允许 `double`、`int64`、`bool`、`string`；double 必须有限；
- 调用方应保证同一序列按时间递增发送；Core 对同一序列的多个 IngestData 任务保持提交顺序；
- 多个独立 RPC 之间不承诺全局原子快照；跨序列约束可能因数据分批到达而延后检查。

## 3. Core 主 gRPC 服务

### 3.1 完整 RPC 列表

```proto
service TimeseriesCoreService {
  rpc syncInstanceConfigs(SyncInstanceConfigsRequest)
      returns (SyncConfigResponse);
  rpc syncConstraints(SyncConstraintsRequest)
      returns (SyncConfigResponse);
  rpc syncRelations(SyncRelationsRequest)
      returns (SyncConfigResponse);
  rpc syncWindowConfig(SyncWindowConfigRequest)
      returns (SyncConfigResponse);
  rpc syncDerivedSeriesConfigs(SyncDerivedSeriesConfigsRequest)
      returns (SyncConfigResponse);
  rpc ingestData(IngestDataRequest)
      returns (IngestDataResponse);
  rpc ingestAndResolveData(IngestRequest)
      returns (IngestResponse);
  rpc writeRawData(WriteRawDataRequest)
      returns (WriteRawDataResponse);
  rpc buildTimeWindow(BuildTimeWindowRequest)
      returns (BuildTimeWindowResponse);
  rpc queryWindowData(QueryWindowDataRequest)
      returns (QueryWindowDataResponse);
  rpc alignWindowData(AlignWindowDataRequest)
      returns (AlignWindowDataResponse);
  rpc computeBasicStatistics(ComputeStatisticsRequest)
      returns (ComputeStatisticsResponse);
  rpc checkConstraints(CheckConstraintsRequest)
      returns (CheckConstraintsResponse);
  rpc queryHistoryData(QueryHistoryDataRequest)
      returns (QueryHistoryDataResponse);
  rpc queryHistoryOverview(QueryHistoryOverviewRequest)
      returns (QueryHistoryOverviewResponse);
}
```

### 3.2 公共基础消息

下面的公共消息均在正式协议中带有 `project_id`：`TimeseriesIngestData`、
`RawTimeseriesPoint`、`TimeseriesBatch` 分别用于点、原始点和批次的项目上下文传递。
为便于阅读，以下基础字段片段只展示原有字段；完整字段编号和所有请求级
`project_id` 以 proto 文件为准。

```proto
message TimeseriesValue {
  oneof kind {
    double double_value = 1;
    int64 int64_value = 2;
    bool bool_value = 3;
    string string_value = 4;
  }
}

message TimeseriesIngestData {
  optional string sequence_id = 1;
  string data_source_id = 2;
  string external_sequence_id = 3;
  int64 time = 4;
  TimeseriesValue value = 5;
  string project_id = 6;
}

message RawTimeseriesPoint {
  int64 time = 1;
  string sequence_id = 2;
  TimeseriesValue value = 3;
  string project_id = 4;
}

message TimeseriesBatch {
  repeated RawTimeseriesPoint points = 1;
  string project_id = 2;
}
```

`TimeseriesIngestData` 可以直接传 `sequence_id`，也可以省略它并同时传
`data_source_id`、`external_sequence_id`。若三者同时存在，外部二元标识必须解析到同一
内部序列。序列必须已注册，值必须符合注册的 `data_type`；解析失败的点会从结果中排除，
至少一个点成功时返回 `PARTIAL_SUCCESS`。

### 3.3 配置同步接口

#### `syncInstanceConfigs`

```proto
rpc syncInstanceConfigs(SyncInstanceConfigsRequest)
    returns (SyncConfigResponse);
message SyncInstanceConfigsRequest {
  repeated RuntimeInstanceConfig items = 1;
}
message RuntimeInstanceConfig {
  string sequence_id = 1;
  string data_source_id = 2;
  string external_sequence_id = 3;
  string category_id = 4;
  string data_type = 5;
  SeriesKind series_kind = 6;
}
enum SeriesKind {
  SERIES_KIND_UNSPECIFIED = 0;
  SERIES_KIND_CONTINUOUS = 1;
  SERIES_KIND_DISCRETE = 2;
  SERIES_KIND_CATEGORICAL = 3;
}
```

这是增量 upsert，不删除未出现在本次请求中的实例。ID 必须非空，同批 `sequence_id` 和
`data_source_id + external_sequence_id` 不能重复。

`data_type` 兼容值：`double/float/continuous` → double，`int/int64/integer/discrete`
→ int64，`bool/boolean` → bool，`string/text/label/categorical` → string；空值或未知
值保留兼容性，由实际值类型决定。`series_kind` 是业务分类，不等同于物理值类型。

#### `syncConstraints`

```proto
rpc syncConstraints(SyncConstraintsRequest)
    returns (SyncConfigResponse);
message SyncConstraintsRequest {
  repeated RuntimeConstraintConfig items = 1;
}
message RuntimeConstraintConfig {
  ConstraintRule rule = 1;
  bool enabled = 2;
}
message ConstraintRule {
  string constraint_id = 1;
  map<string, string> variable_mapping = 2;
  double lower_bound = 3;
  double upper_bound = 4;
  repeated ConstraintTerm terms = 5;
}
message ConstraintTerm {
  string variable = 1;
  double coefficient = 2;
  uint64 sample_offset = 3;
}
```

规则 ID、mapping、terms 非空；上下界和系数有限且上下界有序；每个 term 的 variable 必须
有 mapping；offset 从 0 开始并非递减；mapping 中的序列必须已注册。`enabled=false` 的
规则保留但不参与检查。本 RPC 是增量 upsert。

#### `syncRelations`

```proto
rpc syncRelations(SyncRelationsRequest)
    returns (SyncConfigResponse);
message SyncRelationsRequest {
  repeated RuntimeRelationConfig items = 1;
}
message RelationLagRange {
  int64 min = 1;
  int64 max = 2;
}
message RuntimeRelationSource {
  string source_sequence_id = 1;
  double weight = 2;
  oneof lag_spec {
    int64 fixed_lag = 3;
    RelationLagRange lag_range = 4;
  }
}
message RuntimeRelationConfig {
  string relation_id = 1;
  reserved 2;
  string target_sequence_id = 3;
  string relation_type = 4;
  reserved 5, 6;
  double confidence = 7;
  bool enabled = 8;
  repeated RuntimeRelationSource sources = 9;
}
```

关系 ID、target、sources 非空；source 不重复；weight/confidence 有限；lag range 满足
`min <= max`。注册表接受三种 lag 状态，但当前实际对齐只支持 `fixed_lag`；lag range 或
未设置 lag 的关系在对齐时返回 `NOT_IMPLEMENTED`。本 RPC 是增量 upsert。

#### `syncWindowConfig`

```proto
rpc syncWindowConfig(SyncWindowConfigRequest)
    returns (SyncConfigResponse);
message SyncWindowConfigRequest {
  RuntimeWindowConfig config = 1;
}
message RuntimeWindowConfig {
  int64 window_size = 1;
}
```

`config` 必须存在且 `window_size > 0`，单位毫秒。它替换当前 `project_id` 范围的窗口长度；
默认值为 3 天。

#### `syncDerivedSeriesConfigs`

```proto
rpc syncDerivedSeriesConfigs(SyncDerivedSeriesConfigsRequest)
    returns (SyncConfigResponse);
message SyncDerivedSeriesConfigsRequest {
  repeated DerivedSeriesConfig items = 1;
}
message LinearTerm {
  string sequence_id = 1;
  double coefficient = 2;
}
message LinearCombinationConfig {
  repeated LinearTerm terms = 1;
  double bias = 2;
}
enum DerivedOperator {
  DERIVED_OPERATOR_UNSPECIFIED = 0;
  DERIVED_OPERATOR_ADD = 1;
  DERIVED_OPERATOR_SUBTRACT = 2;
  DERIVED_OPERATOR_MULTIPLY = 3;
  DERIVED_OPERATOR_DIVIDE = 4;
}
message DerivedExpression {
  oneof node {
    string sequence_id = 1;
    double constant = 2;
    DerivedBinaryExpression binary = 3;
  }
}
message DerivedBinaryExpression {
  DerivedOperator operator = 1;
  DerivedExpression left = 2;
  DerivedExpression right = 3;
}
message DerivedSeriesConfig {
  string derived_sequence_id = 1;
  bool enabled = 2;
  oneof formula {
    LinearCombinationConfig linear_combination = 3;
    DerivedExpression expression = 4;
  }
}
```

派生 ID 必须非空且不能和原始实例冲突；线性组合至少一项且系数/bias 有限；表达式必须
有至少一个已注册连续数值序列叶子，二元节点必须有合法 operator、left、right。同步
成功后立即刷新当前热窗口；派生结果只在内存中，不写 TDengine、也不出现在历史查询中。
`enabled=false` 会停用并清理对应热窗口数据。本 RPC 是增量 upsert。

配置同步的返回格式统一为：

```proto
message SyncConfigResponse {
  OperationResult operation = 1;
}

### 3.4 数据接入接口

#### `ingestData`：生产主入口

```proto
rpc ingestData(IngestDataRequest)
    returns (IngestDataResponse);
message IngestDataRequest {
  repeated TimeseriesIngestData points = 1;
  reserved 2;
  bool return_resolved_data = 3;
}
message IngestDataResponse {
  OperationResult operation = 1;
  OperationResult resolve_result = 2;
  OperationResult storage_result = 3;
  OperationResult window_result = 4;
  OperationResult constraint_notification_result = 6;
  TimeseriesBatch resolved_data = 5;
  OperationResult derived_result = 7;
  bool storage_queued = 8;
}
```

流程为：解析/识别 → 冷热任务准入 → 热窗口更新 → 派生刷新 → 约束检查和异步通知；冷
数据进入 TDengine 后台队列。响应字段语义如下：

| 字段 | 含义 |
|---|---|
| `resolve_result` | 输入点解析、外部 ID 解析和类型校验 |
| `storage_result` | 冷写任务接纳结果；通常表示已排队，不表示已持久化 |
| `window_result` | 热窗口更新 |
| `derived_result` | 派生热窗口刷新 |
| `constraint_notification_result` | 违反结果通知入队结果；无违反时为 OK |
| `operation` | 解析、冷写接纳、热窗口、派生及通知的综合结果 |
| `resolved_data` | 仅 `return_resolved_data=true` 时返回 |
| `storage_queued` | true 表示冷写进入 Core 有界队列 |

一个请求只有冷热通道都预留成功才会接纳；队列满返回 `UNAVAILABLE`，调用方可重试。接纳
后冷写和热处理并行，RPC 返回可能早于 TDengine 完成。后台冷写失败通过 Core 日志报告，
不能再通过当前 RPC 返回。约束违反通知失败不会回滚已经完成的热处理。

同一序列的任务保持提交顺序，但不同序列/不同 RPC 不提供全局原子快照。跨序列约束在一
个 RPC 尚未看到其他序列时，暂时缺失的样本会保留为待后续数据到达后重检，不应作为请求
格式错误处理。

#### `ingestAndResolveData`：只解析

```proto
rpc ingestAndResolveData(IngestRequest)
    returns (IngestResponse);
message IngestRequest {
  repeated TimeseriesIngestData points = 1;
}
message IngestResponse {
  OperationResult operation = 1;
  TimeseriesBatch resolved_data = 2;
}
```

只执行序列解析、外部 ID 解析和数据类型校验，不写 TDengine、不更新热窗口、不计算派生
和约束。用于测试、回放和补偿。

#### `writeRawData`：直接冷写

```proto
rpc writeRawData(WriteRawDataRequest)
    returns (WriteRawDataResponse);
message WriteRawDataRequest {
  TimeseriesBatch data = 1;
}
message WriteRawDataResponse {
  OperationResult operation = 1;
}
```

`data` 和 `points` 不能为空；每个点必须有非空 `sequence_id` 和已设置的 value。该接口
直接写原始 TDengine，不做外部 ID 解析和热窗口更新。

#### `buildTimeWindow`：只更新热窗口

```proto
rpc buildTimeWindow(BuildTimeWindowRequest)
    returns (BuildTimeWindowResponse);
message BuildTimeWindowRequest {
  TimeseriesBatch data = 1;
  int64 window_size = 2;
}
message BuildTimeWindowResponse {
  OperationResult operation = 1;
}
```

`data` 非空且 `window_size > 0`。只更新内存热窗口，使用请求给出的显式窗口长度，不写
TDengine、不触发派生和约束。

### 3.5 热窗口与对齐

#### `queryWindowData`

```proto
rpc queryWindowData(QueryWindowDataRequest)
    returns (QueryWindowDataResponse);
message QueryWindowDataRequest {
  repeated string sequence_ids = 1;
  optional int64 start_time = 2;
  optional int64 end_time = 3;
}
message QueryWindowDataResponse {
  OperationResult operation = 1;
  WindowData data = 2;
}
message WindowData {
  int64 window_start_time = 1;
  int64 window_end_time = 2;
  repeated SequenceWindow sequences = 3;
}
message SequenceWindow {
  string sequence_id = 1;
  repeated TimeValuePoint points = 2;
}
message TimeValuePoint {
  int64 time = 1;
  TimeseriesValue value = 2;
}
```

`sequence_ids` 不能为空，时间范围必须有序；省略边界时使用当前热窗口边界，指定范围
按 `[start_time,end_time)` 过滤。请求的序列尚无数据时可不出现在结果中；窗口为空返回
OK 和空数据。

#### `alignWindowData`

```proto
rpc alignWindowData(AlignWindowDataRequest)
    returns (AlignWindowDataResponse);
message AlignWindowDataRequest {
  oneof source {
    WindowData data = 1;
    QueryWindowDataRequest window_query = 3;
  }
  AlignmentConfig config = 2;
  repeated string relation_ids = 4;
}
message AlignWindowDataResponse {
  OperationResult operation = 1;
  AlignedWindowData aligned_data = 2;
}
message AlignmentConfig {
  repeated SequenceAlignmentConfig sequences = 1;
  optional int64 bucket_interval = 2;
}
message SequenceAlignmentConfig {
  string sequence_id = 1;
  VariableRole role = 2;
  optional BucketAggregation aggregation = 3;
  optional GapFillMethod fill_method = 4;
}
enum VariableRole {
  VARIABLE_ROLE_UNSPECIFIED = 0;
  VARIABLE_ROLE_INDEPENDENT = 1;
  VARIABLE_ROLE_DEPENDENT = 2;
}
enum BucketAggregation {
  BUCKET_AGGREGATION_UNSPECIFIED = 0;
  BUCKET_AGGREGATION_FIRST = 1;
  BUCKET_AGGREGATION_LAST = 2;
  BUCKET_AGGREGATION_AVERAGE = 3;
  BUCKET_AGGREGATION_MAXIMUM = 4;
  BUCKET_AGGREGATION_MINIMUM = 5;
}
enum GapFillMethod {
  GAP_FILL_METHOD_UNSPECIFIED = 0;
  GAP_FILL_METHOD_NEAR = 1;
  GAP_FILL_METHOD_PREVIOUS = 2;
  GAP_FILL_METHOD_NEXT = 3;
  GAP_FILL_METHOD_LINEAR = 4;
}
message AlignedWindowData {
  int64 window_start_time = 1;
  int64 window_end_time = 2;
  repeated AlignedSample samples = 3;
}
message AlignedSample {
  int64 time = 1;
  repeated AlignedValue values = 2;
}
message AlignedValue {
  string sequence_id = 1;
  TimeseriesValue value = 2;
}
```

`source` 必须选择 `data` 或 `window_query` 之一。`config` 可省略；省略时按输入序列和
已注册 `SeriesKind` 推导策略：连续型默认 `AVERAGE + LINEAR`，离散/类别型默认
`LAST + PREVIOUS`，未指定类型使用兼容默认策略。显式 aggregation/fill_method 优先。

`bucket_interval` 省略时推导最小正时间间隔；无法推导时必须给正数。`relation_ids` 中的
关系必须已注册且启用，target/source 必须出现在 alignment config 中。当前实际对齐只
支持 `fixed_lag`，lag range 和未设置 lag 返回 `NOT_IMPLEMENTED`。

### 3.6 统计、相关性和约束

#### `computeBasicStatistics`

```proto
rpc computeBasicStatistics(ComputeStatisticsRequest)
    returns (ComputeStatisticsResponse);
message ComputeStatisticsRequest {
  oneof source {
    WindowData window_data = 1;
    AlignedWindowData aligned_data = 2;
    QueryWindowDataRequest window_query = 4;
  }
  AlignmentConfig alignment_config = 3;
  string relation_id = 5;
}
message ComputeStatisticsResponse {
  OperationResult operation = 1;
  repeated SequenceMetric sequence_metrics = 2;
  CorrelationVector correlation_vector = 3;
}
message SequenceMetric {
  string sequence_id = 1;
  repeated NamedMetric metrics = 2;
}
message NamedMetric {
  string name = 1;
  TimeseriesValue value = 2;
}
message CorrelationVector {
  string dependent_sequence_id = 1;
  repeated SequenceCorrelation correlations = 2;
}
message SequenceCorrelation {
  string independent_sequence_id = 1;
  double coefficient = 2;
}
```

`window_data` 或 `window_query` 只能计算基础统计，`relation_id` 必须为空；
`aligned_data` 必须带已启用的 `relation_id`，用于基础统计和 Pearson 相关性。指标名
固定为 `count`、`first_time`、`last_time`、`sum`、`mean`、`min`、`max`、`variance`、
`stddev`，方差按总体口径除以 N；非数值序列或不足的数值配对会造成部分成功/失败。

#### `checkConstraints`

```proto
rpc checkConstraints(CheckConstraintsRequest)
    returns (CheckConstraintsResponse);
message CheckConstraintsRequest {
  repeated string constraint_ids = 1;
  oneof source {
    WindowData window_data = 2;
    AlignedWindowData aligned_data = 3;
    QueryWindowDataRequest window_query = 4;
  }
  AlignmentConfig alignment_config = 5;
}
message CheckConstraintsResponse {
  OperationResult operation = 1;
  bool satisfied = 2;
  repeated ConstraintViolation violations = 3;
  uint64 evaluated_count = 4;
}
message ConstraintViolation {
  string constraint_id = 1;
  int64 anchor_time = 2;
  double lower_bound = 3;
  double upper_bound = 4;
  double evaluated_value = 5;
  repeated ConstraintTermValue term_values = 6;
}
message ConstraintTermValue {
  string variable = 1;
  string sequence_id = 2;
  double coefficient = 3;
  uint64 sample_offset = 4;
  int64 sample_time = 5;
  double value = 6;
}
```

`constraint_ids` 必须非空，且每个 ID 已注册并启用，否则返回 `FAILED_PRECONDITION`。
source 三选一。跨多个序列的规则应使用 `AlignedWindowData`；违反项通过 violations
返回 anchor、上下界、实际计算值和 term 取值。

### 3.7 历史查询

```proto
rpc queryHistoryData(QueryHistoryDataRequest)
    returns (QueryHistoryDataResponse);
message QueryHistoryDataRequest {
  repeated string sequence_ids = 1;
  int64 start_time = 2;
  int64 end_time = 3;
  optional int64 granularity = 4;
}
message QueryHistoryDataResponse {
  OperationResult operation = 1;
  TimeseriesBatch data = 2;
}

rpc queryHistoryOverview(QueryHistoryOverviewRequest)
    returns (QueryHistoryOverviewResponse);
message QueryHistoryOverviewRequest {
  repeated string sequence_ids = 1;
  optional int64 start_time = 2;
  optional int64 end_time = 3;
}
message QueryHistoryOverviewResponse {
  OperationResult operation = 1;
  HistoryOverview overview = 2;
}
message HistoryOverview {
  uint64 total_point_count = 1;
  uint64 sequence_count = 2;
  repeated string column_names = 3;
  optional int64 first_time = 4;
  optional int64 last_time = 5;
  repeated HistorySeriesSummary series = 6;
}
message HistorySeriesSummary {
  string sequence_id = 1;
  uint64 point_count = 2;
  optional int64 first_time = 3;
  optional int64 last_time = 4;
}
```

`queryHistoryData` 要求序列非空、已注册、时间有序，granularity（若提供）为正数；范围
为 `[start_time,end_time)`，granularity 模式按序列和 bucket 返回最后值。概览接口的序列
和时间边界都可省略；全部省略时查询 TDengine 当前已有的全部原始历史。派生序列不进入
任一历史接口。

## 4. 约束结果回调接口

Core 作为异步客户端，要求统一服务在配置地址提供：

```proto
service TimeseriesConstraintResultReceiverService {
  rpc ReceiveConstraintResult(ConstraintResultMessage)
      returns (SyncResponse);
}
message ConstraintResultMessage {
  int64 check_time_ms = 1;
  repeated string violated_constraint_ids = 2;
  repeated string sequence_ids = 3;
}
```

统一服务若用 C++ 实现该回调，需要实现生成服务基类的函数：

```cpp
::grpc::Status ReceiveConstraintResult(
    ::grpc::ServerContext* context,
    const ::sfkg::timeseries::core::v1::ConstraintResultMessage* request,
    ::sfkg::timeseries::core::v1::SyncResponse* response) override;
```

Core 只有在发现违反时入队通知；无违反不调用远端 RPC。通知使用独立有界队列和线程，
`IngestDataResponse.constraint_notification_result` 表示本地是否成功入队，而不是远端
业务处理已经完成。队列满或 Core 关闭时返回 `UNAVAILABLE`；已完成的热窗口和冷写不会
回滚。地址由 `SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS` 配置，默认
`222.29.156.142:9105`。

## 5. 直接 C++ 库接口

以下接口位于 `include/sfkg/timeseries/core/`，命名空间为
`sfkg::timeseries::core`。它们是进程内接口，不是 gRPC 调用方必须实现的接口。

### 5.1 核心类型

```cpp
using Timestamp = std::int64_t;
using SequenceId = std::string;
using TimeseriesValue =
    std::variant<double, std::int64_t, bool, std::string>;

enum class TimeseriesValueKind {
    Unknown, Double, Int64, Bool, String
};
enum class OperationCode {
    Ok, PartialSuccess, InvalidArgument, NotFound, FailedPrecondition,
    Unavailable, InternalError, NotImplemented
};

struct OperationResult {
    OperationCode code{OperationCode::Ok};
    std::size_t success_count{0};
    std::size_t failed_count{0};
    std::string message;
};

struct TimeseriesIngestData {
    std::optional<SequenceId> sequence_id;
    std::string data_source_id;
    std::string external_sequence_id;
    Timestamp time{};
    TimeseriesValue value;
};
struct RawTimeseriesPoint {
    Timestamp time{};
    SequenceId sequence_id;
    TimeseriesValue value;
};
struct TimeseriesBatch { std::vector<RawTimeseriesPoint> points; };

struct WindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::unordered_map<SequenceId, std::vector<RawTimeseriesPoint>>
        sequence_values;
};
struct AlignedSample {
    Timestamp time{};
    std::unordered_map<SequenceId, TimeseriesValue> values;
};
struct AlignedWindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::vector<AlignedSample> samples;
};
```

### 5.2 查询、配置和结果类型

```cpp
struct WindowQuery {
    std::vector<SequenceId> sequence_ids;
    std::optional<Timestamp> start_time;
    std::optional<Timestamp> end_time;
    std::size_t preceding_points{0};
    std::size_t following_points{0};
    bool preserve_window_bounds{false};
};
struct HistoryQuery {
    std::vector<SequenceId> sequence_ids;
    Timestamp start_time{};
    Timestamp end_time{};
    std::optional<std::int64_t> granularity;
};
struct HistoryOverviewQuery {
    std::vector<SequenceId> sequence_ids;
    std::optional<Timestamp> start_time;
    std::optional<Timestamp> end_time;
};

enum class VariableRole { Independent, Dependent };
enum class SeriesKind { Unspecified, Continuous, Discrete, Categorical };
enum class BucketAggregation { First, Last, Average, Maximum, Minimum };
enum class GapFillMethod { Near, Previous, Next, Linear };
struct SequenceAlignmentConfig {
    SequenceId sequence_id;
    VariableRole role{VariableRole::Independent};
    std::optional<BucketAggregation> aggregation;
    std::optional<GapFillMethod> fill_method;
};
struct AlignmentConfig {
    std::vector<SequenceAlignmentConfig> sequences;
    std::optional<std::int64_t> bucket_interval;
};
struct AlignmentRange {
    Timestamp start_time{};
    Timestamp end_time{};
    std::size_t prefix_samples{0};
};
struct ConstraintCheckRange {
    Timestamp start_time{};
    Timestamp end_time{};
};
```

`WindowQuery.preceding_points`/`following_points` 是 C++ 专有的插值或 offset 上下文，
当前 gRPC QueryWindowDataRequest 不暴露这两个字段。

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
    std::size_t pending_count{};
    bool satisfied{false};
    std::vector<ConstraintViolation> violations;
};
```

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
using RelationLagSpec = std::variant<
    std::monostate, std::int64_t, RelationLagRange>;
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

派生类型：

```cpp
enum class DerivedOperator { Unspecified, Add, Subtract, Multiply, Divide };
struct DerivedExpression;
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
struct DerivedLinearTerm {
    SequenceId sequence_id;
    double coefficient{};
};
struct DerivedLinearCombination {
    std::vector<DerivedLinearTerm> terms;
    double bias{};
};
using DerivedFormula =
    std::variant<DerivedLinearCombination, DerivedExpression>;
struct RuntimeDerivedSeriesConfig {
    SequenceId derived_sequence_id;
    bool enabled{false};
    DerivedFormula formula;
};
template <typename T>
struct RuntimeConfigSnapshot { std::vector<T> items; };
```

结果类型：

```cpp
struct IngestResult { OperationResult operation; TimeseriesBatch resolved_data; };
struct WindowQueryResult { OperationResult operation; WindowData data; };
struct AlignmentResult { OperationResult operation; AlignedWindowData aligned_data; };
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
struct HistoryQueryResult { OperationResult operation; TimeseriesBatch data; };
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
struct HistoryOverviewResult { OperationResult operation; HistoryOverview overview; };

### 5.3 公共领域服务函数

```cpp
class RuntimeConfigRegistry {
public:
    OperationResult replaceInstanceConfigs(
        const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot);
    OperationResult upsertInstanceConfigs(
        const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot);
    OperationResult replaceConstraints(
        const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot);
    OperationResult upsertConstraints(
        const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot);
    OperationResult replaceRelations(
        const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot);
    OperationResult upsertRelations(
        const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot);
    OperationResult upsertDerivedSeriesConfigs(
        const RuntimeConfigSnapshot<RuntimeDerivedSeriesConfig>& snapshot);

    std::optional<RuntimeInstanceConfig> findInstance(
        const SequenceId& sequence_id) const;
    std::optional<SequenceId> resolveSequenceId(
        const std::string& data_source_id,
        const std::string& external_sequence_id) const;
    std::optional<RuntimeRelationConfig> findRelation(
        const std::string& relation_id) const;
    std::vector<RuntimeDerivedSeriesConfig> allDerivedSeries() const;
    ConstraintLookupResult lookupConstraints(
        const std::vector<std::string>& constraint_ids) const;
    std::vector<ConstraintRule> enabledConstraints(
        const std::vector<std::string>& constraint_ids) const;
    std::vector<ConstraintRule> allEnabledConstraints() const;
};
```

`replace*` 原子替换整类配置；`upsert*` 只更新请求中的 ID，保留其他配置。gRPC 同步接口
使用 upsert 语义。`ConstraintLookupResult` 为：

```cpp
struct ConstraintLookupResult {
    std::vector<ConstraintRule> enabled_rules;
    std::vector<std::string> missing_ids;
    std::vector<std::string> disabled_ids;
};
```

其余领域服务的完整公共函数签名：

```cpp
class IngestService {
public:
    explicit IngestService(const RuntimeConfigRegistry& configs);
    IngestResult ingestAndResolveData(
        const std::vector<TimeseriesIngestData>& input) const;
};

class WindowService {
public:
    WindowService();
    ~WindowService();
    WindowService(const WindowService&) = delete;
    WindowService& operator=(const WindowService&) = delete;
    OperationResult configureWindowSize(std::int64_t window_size);
    std::int64_t windowSize() const;
    OperationResult buildTimeWindow(const TimeseriesBatch& data);
    OperationResult buildTimeWindow(
        const TimeseriesBatch& data, std::int64_t window_size);
    WindowUpdateResult buildTimeWindowIncremental(
        const TimeseriesBatch& data);
    OperationResult replaceDerivedSequence(
        const SequenceId& sequence_id, const TimeseriesBatch& data);
    OperationResult patchDerivedSequence(
        const SequenceId& sequence_id,
        Timestamp start_time,
        Timestamp end_time,
        const TimeseriesBatch& data);
    WindowQueryResult queryWindowData(const WindowQuery& query) const;
};

class AlignmentService {
public:
    explicit AlignmentService(const RuntimeConfigRegistry& configs);
    AlignmentResult alignWindowData(
        const WindowData& window_data) const;
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentConfig& config) const;
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const std::vector<RuntimeRelationConfig>& relations) const;
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentConfig& config,
        const std::vector<RuntimeRelationConfig>& relations) const;
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentRange& range) const;
};

class StatisticsService {
public:
    StatisticsResult computeBasicStatistics(
        const WindowData& data) const;
    StatisticsResult computeBasicStatistics(
        const AlignedWindowData& data,
        const RuntimeRelationConfig& relation) const;
};

class ConstraintCheckEngine {
public:
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const WindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const WindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
};

class HistoryQueryService {
public:
    HistoryQueryService(
        const RuntimeConfigRegistry& configs,
        internal::TaosClient& taos_client);
    HistoryQueryResult queryHistoryData(
        const HistoryQuery& query) const;
    HistoryOverviewResult queryHistoryOverview(
        const HistoryOverviewQuery& query) const;
};

class DerivedSeriesService {
public:
    DerivedSeriesService(
        const RuntimeConfigRegistry& configs,
        WindowService& window_service);
    OperationResult refresh();
    OperationResult refresh(const WindowUpdateResult& update);
};
```

窗口增量结果的公开数据结构：

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
    double sequence_lock_wait_ms{0.0};
    double sequence_update_ms{0.0};
    std::size_t sequence_task_count{0};
    std::size_t sequence_group_count{0};
    double eviction_lock_wait_ms{0.0};
    double eviction_update_ms{0.0};
};
```

增量消费者应使用每序列的 `sequence_updates`，不能只根据聚合的 `incremental_safe` 做
判断；`update_generation` 用于防止旧的派生计算覆盖更新的数据。

### 5.4 存储和 gRPC 辅助 C++ 接口

这些接口位于内部适配层公共头文件，只有同进程组装 Core 时才需要直接调用：

```cpp
class StorageService {
public:
    explicit StorageService(internal::TaosClient& taos_client);
    OperationResult writeRawData(const TimeseriesBatch& data);
    OperationResult writeRawDataOnConnection(
        std::size_t connection_index,
        const TimeseriesBatch& data);
};

namespace internal {
class TaosClient {
public:
    TaosClient();
    ~TaosClient();
    TaosClient(const TaosClient&) = delete;
    TaosClient& operator=(const TaosClient&) = delete;
    OperationResult ensureSchema();
    OperationResult dropDatabaseForTesting();
    OperationResult insertRaw(const TimeseriesBatch& batch);
    OperationResult insertRawOnConnection(
        std::size_t connection_index,
        const TimeseriesBatch& batch);
    OperationResult queryRaw(
        const std::vector<SequenceId>& sequence_ids,
        Timestamp start,
        Timestamp end,
        TimeseriesBatch* out,
        std::optional<std::int64_t> granularity = std::nullopt,
        const std::unordered_map<SequenceId, TimeseriesValueKind>* value_kinds =
            nullptr) const;
    OperationResult queryHistoryOverview(
        const HistoryOverviewQuery& query,
        HistoryOverview* out) const;
};
}  // namespace internal
```

`internal::TaosClient` 不是稳定业务 SDK；`dropDatabaseForTesting()` 只用于测试，会删除
配置数据库，正常服务流程不得调用。

Core 发出约束结果的 C++ 客户端和异步入队接口：

```cpp
namespace sfkg::timeseries::core::grpc {
namespace pb = ::sfkg::timeseries::core::v1;
class ConstraintResultReceiverClient {
public:
    explicit ConstraintResultReceiverClient(std::string address);
    OperationResult receiveConstraintResult(
        Timestamp check_time_ms,
        const std::vector<std::string>& violated_constraint_ids,
        const std::vector<SequenceId>& sequence_ids);
};

class ConstraintResultNotificationExecutor {
public:
    explicit ConstraintResultNotificationExecutor(
        ConstraintResultReceiverClient& receiver);
    ~ConstraintResultNotificationExecutor();
    OperationResult tryEnqueue(
        Timestamp check_time_ms,
        std::vector<std::string> violated_constraint_ids,
        std::vector<SequenceId> sequence_ids);
};
}  // namespace sfkg::timeseries::core::grpc
```

`tryEnqueue` 是非阻塞入队；队列容量由 `SFKG_CONSTRAINT_NOTIFY_QUEUE_CAPACITY` 控制。
`IngestTaskExecutor` 仅是 gRPC 适配层内部调度器，不建议当作外部业务 API；若确实直接
组装适配层，其入口为：

```cpp
struct IngestPipelineResult {
    OperationResult storage_result;
    OperationResult window_result;
    OperationResult derived_result;
    OperationResult constraint_notification_result;
};
struct IngestTaskSubmission {
    bool accepted{false};
    OperationResult admission;
    std::future<IngestPipelineResult> hot_completion;
    std::future<IngestPipelineResult> completion;
};
class IngestTaskExecutor {
public:
    IngestTaskExecutor();
    ~IngestTaskExecutor();
    IngestTaskExecutor(const IngestTaskExecutor&) = delete;
    IngestTaskExecutor& operator=(const IngestTaskExecutor&) = delete;
    using ColdWriteFunction = std::function<OperationResult(
        std::size_t, const TimeseriesBatch&)>;
    using HotUpdateFunction = std::function<IngestPipelineResult(
        const TimeseriesBatch&)>;
    IngestTaskSubmission trySubmit(
        std::shared_ptr<const IngestResult> resolved,
        ColdWriteFunction cold_write,
        HotUpdateFunction hot_update);
};

class TimeseriesCoreGrpcService
    : public pb::TimeseriesCoreService::Service {
public:
    TimeseriesCoreGrpcService(
        IngestService& ingest_service,
        StorageService& storage_service,
        WindowService& window_service,
        AlignmentService& alignment_service,
        StatisticsService& statistics_service,
        ConstraintCheckEngine& constraint_engine,
        HistoryQueryService& history_service,
        RuntimeConfigRegistry& config_registry,
        ConstraintResultReceiverClient& constraint_result_receiver);
    ::grpc::Status syncInstanceConfigs(
        ::grpc::ServerContext*, const pb::SyncInstanceConfigsRequest*,
        pb::SyncConfigResponse*) override;
    ::grpc::Status syncConstraints(
        ::grpc::ServerContext*, const pb::SyncConstraintsRequest*,
        pb::SyncConfigResponse*) override;
    ::grpc::Status syncRelations(
        ::grpc::ServerContext*, const pb::SyncRelationsRequest*,
        pb::SyncConfigResponse*) override;
    ::grpc::Status syncWindowConfig(
        ::grpc::ServerContext*, const pb::SyncWindowConfigRequest*,
        pb::SyncConfigResponse*) override;
    ::grpc::Status syncDerivedSeriesConfigs(
        ::grpc::ServerContext*, const pb::SyncDerivedSeriesConfigsRequest*,
        pb::SyncConfigResponse*) override;
    ::grpc::Status ingestData(
        ::grpc::ServerContext*, const pb::IngestDataRequest*,
        pb::IngestDataResponse*) override;
    ::grpc::Status ingestAndResolveData(
        ::grpc::ServerContext*, const pb::IngestRequest*,
        pb::IngestResponse*) override;
    ::grpc::Status writeRawData(
        ::grpc::ServerContext*, const pb::WriteRawDataRequest*,
        pb::WriteRawDataResponse*) override;
    ::grpc::Status buildTimeWindow(
        ::grpc::ServerContext*, const pb::BuildTimeWindowRequest*,
        pb::BuildTimeWindowResponse*) override;
    ::grpc::Status queryWindowData(
        ::grpc::ServerContext*, const pb::QueryWindowDataRequest*,
        pb::QueryWindowDataResponse*) override;
    ::grpc::Status alignWindowData(
        ::grpc::ServerContext*, const pb::AlignWindowDataRequest*,
        pb::AlignWindowDataResponse*) override;
    ::grpc::Status computeBasicStatistics(
        ::grpc::ServerContext*, const pb::ComputeStatisticsRequest*,
        pb::ComputeStatisticsResponse*) override;
    ::grpc::Status checkConstraints(
        ::grpc::ServerContext*, const pb::CheckConstraintsRequest*,
        pb::CheckConstraintsResponse*) override;
    ::grpc::Status queryHistoryData(
        ::grpc::ServerContext*, const pb::QueryHistoryDataRequest*,
        pb::QueryHistoryDataResponse*) override;
    ::grpc::Status queryHistoryOverview(
        ::grpc::ServerContext*, const pb::QueryHistoryOverviewRequest*,
        pb::QueryHistoryOverviewResponse*) override;
};
}  // namespace sfkg::timeseries::core::grpc
```

## 6. 启动、TDengine、并发和构建对接

### 6.1 启动命令和地址优先级

```bash
./build-taos/sfkg-timeseries-core-server [address]
```

Core 监听地址优先级：命令行第一个参数 > `SFKG_TIMESERIES_CORE_ADDRESS` > 默认
`0.0.0.0:50051`。约束回调地址没有命令行参数，读取
`SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS`，默认 `222.29.156.142:9105`。

Core 启动时会先执行 `TaosClient::ensureSchema()`；TDengine 不可用或配置非法时，gRPC
server 不会启动。

### 6.2 TDengine 环境变量

| 变量 | 默认值 | 作用/约束 |
|---|---|---|
| `SFKG_TAOS_HOST` | `127.0.0.1` | TDengine 主机 |
| `SFKG_TAOS_PORT` | `6030` | 1～65535 的端口 |
| `SFKG_TAOS_USER` | `root` | TDengine 用户 |
| `SFKG_TAOS_PASSWORD` | `taosdata` | TDengine 密码 |
| `SFKG_TAOS_DB` | `sfkg_timeseries` | 非空、最长192字符、不能含反引号 |
| `SFKG_TAOS_RAW_STABLE` | `raw_timeseries_data` | 原始超级表名，同样不能含反引号 |
| `SFKG_TAOS_KEEP_DAYS` | TDengine 默认 | 正整数，用于 schema 保留策略 |
| `SFKG_TAOS_WRITE_CONNECTIONS` | `4` | 1～64；推荐不小于冷写 worker 数 |
| `SFKG_TAOS_QUERY_TIMING` | 关闭 | `1/true/on` 时输出历史查询耗时 |
| `SFKG_TAOS_CONFIG_DIR` | 构建时 `SFKG_TAOS_ROOT/cfg` | 清理脚本使用的 TDengine 客户端配置目录 |

Core 使用一个独立查询连接和多个冷写连接。原始 schema 保存时间、sequence_id、
value_type 以及 double/int64/bool/string 四类值列；派生序列不写入原始超级表。

### 6.3 Ingest 并发、队列和诊断环境变量

| 变量 | 默认值 | 作用 |
|---|---:|---|
| `SFKG_INGEST_COLD_WORKERS` | `4` | 冷写 worker，范围1～64 |
| `SFKG_INGEST_HOT_WORKERS` | `1` | 不同批次的热处理 worker，范围1～64 |
| `SFKG_INGEST_HOT_SEQUENCE_WORKERS` | `8` | 单批次内部序列级线程池，最大64 |
| `SFKG_INGEST_QUEUE_CAPACITY` | `128` | 同时接纳的 IngestData 批次数，最大100000 |
| `SFKG_CONSTRAINT_NOTIFY_QUEUE_CAPACITY` | `1024` | 约束违反通知队列，最大100000 |
| `SFKG_INGEST_DIAGNOSTIC_LOG` | 关闭 | `1/true/on` 启用 `ingest_diag` |
| `SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY` | `1` | 每 N 个 IngestData 请求记录一次 |
| `SFKG_INGEST_DIAGNOSTIC_WRITERS` | 关闭 | `1` 时附加冷写 writer 分片和耗时 |

`storage_queued=true` 只表示冷写进入 Core 有界队列，不表示 TDengine 已完成持久化。
诊断输出在 Core 标准错误/日志流中，不是 gRPC 响应字段；生产吞吐测试建议关闭全量诊断
或提高采样间隔。

### 6.4 构建和链接选项

```cmake
option(SFKG_BUILD_GRPC ON)
option(SFKG_BUILD_TESTS ON)
option(SFKG_BUILD_DEMOS ON)
option(SFKG_BUILD_BENCHMARKS OFF)
option(SFKG_WITH_TAOS ON)
set(SFKG_TAOS_ROOT ...)
```

对接方需要：

- `SFKG_BUILD_GRPC=ON` 才生成 Proto/gRPC 代码并构建 server；
- `SFKG_BUILD_TESTS=ON` 构建本地回归测试；
- `SFKG_BUILD_DEMOS=ON` 且启用 TDengine 时构建数据导入 demo；
- `SFKG_BUILD_BENCHMARKS=ON` 构建手动 benchmark 和 `grpc_ordered_load_test`；
- `SFKG_WITH_TAOS=ON` 才启用真实存储和历史查询；
- `SFKG_TAOS_ROOT` 下有 `include/taos.h` 和 `lib` 或 `driver` 下的 `libtaos.so`；
- Protobuf、gRPC C++、C++17 和 Threads。

### 6.5 数据清理脚本

```bash
./scripts/clear_sfkg_timeseries_db.sh
./scripts/clear_sfkg_timeseries_db.sh --yes
```

该运维脚本只允许清空 `sfkg_timeseries.raw_timeseries_data` 的行，保留数据库、超级表
和子表结构。默认要求输入确认；`--yes` 才直接执行。可通过
`SFKG_TAOS_ROOT`、`SFKG_TAOS_BIN`、`SFKG_TAOS_CONFIG_DIR`、`SFKG_TAOS_LIB_DIR` 指定
CLI 位置，但数据库和超级表目标由脚本固定保护。

## 7. 推荐调用顺序和生命周期

### 7.1 配置顺序

1. `syncInstanceConfigs`：注册所有原始序列；
2. `syncWindowConfig`：设置当前项目的热窗口；
3. `syncConstraints`：引用已注册序列；
4. `syncRelations`：注册对齐/相关性关系；
5. `syncDerivedSeriesConfigs`：引用已注册连续数值序列；
6. 调用 `ingestData`；
7. 按需调用窗口、对齐、统计、约束和历史接口。

### 7.2 接口选择

| 需求 | RPC |
|---|---|
| 正常生产接入 | `ingestData` |
| 只做 ID/类型解析 | `ingestAndResolveData` |
| 标准化点直接落盘 | `writeRawData` |
| 只更新热窗口 | `buildTimeWindow` |
| 读取热窗口 | `queryWindowData` |
| 分桶、聚合、填充、固定 lag | `alignWindowData` |
| 基础统计 | `computeBasicStatistics` |
| 关系相关性 | `computeBasicStatistics` + aligned data + relation |
| 规则检查 | `checkConstraints` |
| 历史原始点 | `queryHistoryData` |
| 历史规模概览 | `queryHistoryOverview` |

### 7.3 数据生命周期

```text
TimeseriesIngestData
  -> sequence_id 解析和类型校验
  -> 原始数据进入 TDengine 异步冷写队列
  -> 原始数据进入内存热窗口
  -> 派生序列只更新内存热窗口
  -> 已启用约束检查热窗口
  -> 有违反时异步通知统一服务
```

冷写和热窗口不是同一个跨 TDengine 事务；RPC 返回时历史查询可能暂时读不到刚排队的冷写。
派生序列仅存在热窗口，Core 重启后需要重新同步配置。使用多个 worker 时，调用方需保证
每条序列顺序；若约束要求跨序列完整快照，应把相关序列放进同一个请求，或接受缺序列样本
延后检查。

## 8. 完整性核对

- [x] 15 个 `TimeseriesCoreService` RPC；
- [x] 1 个 `TimeseriesConstraintResultReceiverService` 回调 RPC；
- [x] 所有 Proto message、enum、oneof、字段和保留字段；
- [x] Proto 转换、参数校验和业务错误语义；
- [x] 所有公共领域 C++ 函数签名；
- [x] 存储、回调客户端和任务适配层公共函数签名；
- [x] Core 启动参数、TDengine、并发、队列、诊断和构建变量；
- [x] 数据清理运维入口；
- [x] 推荐同步顺序、异步持久化和数据生命周期。

除本文列出的 gRPC、C++、TDengine、启动和运维边界外，当前代码没有其他需要外部实现
或调用的业务接口。
