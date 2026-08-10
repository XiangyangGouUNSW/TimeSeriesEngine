# Core 对外 gRPC / Proto 接口说明

本文面向统一服务调用方，说明 Core 对外提供什么接口、请求如何组织，以及派生序列
如何通过 Proto 传入。完整字段定义以
[`proto/timeseries_core.proto`](../proto/timeseries_core.proto) 为准。

## 1. 服务和基本约定

服务名为 `sfkg.timeseries.core.v1.TimeseriesCoreService`。

- 同机调用：`127.0.0.1:50051`；跨机器调用：Core 服务器实际 IP 和端口。
- Core 服务端监听可以使用 `0.0.0.0:50051`，但客户端不要连接 `0.0.0.0`。
- 时间戳统一使用 Unix 毫秒 `int64`。
- 所有主要接口都通过 `OperationResult` 返回处理状态。
- Core 重启后运行时配置不会保留，需要统一服务重新同步。

## 2. 推荐调用顺序

```text
syncInstanceConfigs
        ↓
syncWindowConfig（可选，默认窗口为 3 天）
        ↓
syncConstraints / syncRelations（按需）
        ↓
syncDerivedSeriesConfigs（按需）
        ↓
ingestData
        ↓
queryWindowData / alignWindowData / computeBasicStatistics
        ↓
checkConstraints / queryHistoryData
```

## 3. 公共数据结构

### 3.1 时序值

```proto
message TimeseriesValue {
  oneof kind {
    double double_value = 1;
    int64 int64_value = 2;
    bool bool_value = 3;
    string string_value = 4;
  }
}
```

一次 `TimeseriesValue` 只能设置一种物理类型。`oneof` 的含义是“几种可能的字段中
只能选择一个”，不是同时传入所有类型。

### 3.2 序列和业务类型

`RuntimeInstanceConfig` 描述一个已注册序列：

| 字段 | 含义 |
| --- | --- |
| `sequence_id` | Core 内部使用的稳定序列 ID |
| `data_source_id` | 外部数据源 ID |
| `external_sequence_id` | 数据源中的原始序列 ID |
| `category_id` | 业务分类，可为空 |
| `data_type` | 物理类型名称，如 `double`、`int64`、`bool`、`string` |
| `series_kind` | 业务语义：连续、离散或类别 |

调用写入接口时，可以直接传 `sequence_id`，也可以传
`data_source_id + external_sequence_id`，由 Core 解析内部序列 ID。

### 3.3 处理结果

```proto
message OperationResult {
  OperationCode code = 1;
  uint64 success_count = 2;
  uint64 failed_count = 3;
  string message = 4;
}
```

常用状态为：

- `OPERATION_CODE_OK`：全部完成；
- `OPERATION_CODE_PARTIAL_SUCCESS`：部分完成；
- `OPERATION_CODE_INVALID_ARGUMENT`：请求字段或配置不合法；
- `OPERATION_CODE_NOT_FOUND`：请求的数据或配置不存在；
- `OPERATION_CODE_FAILED_PRECONDITION`：前置条件不满足；
- `OPERATION_CODE_INTERNAL_ERROR`：Core 内部错误。

## 4. 配置同步接口

### 4.1 实例配置

```proto
rpc syncInstanceConfigs(SyncInstanceConfigsRequest)
    returns (SyncConfigResponse);
```

请求中的 `items` 是一组实例配置。当前接口按 `sequence_id` 增量新增或更新，成功
后 Core 才能识别后续写入数据。

### 4.2 热窗口配置

```proto
rpc syncWindowConfig(SyncWindowConfigRequest)
    returns (SyncConfigResponse);

message RuntimeWindowConfig {
  int64 window_size = 1; // 毫秒
}
```

一个 Core 进程使用一份全局窗口长度；未同步时默认保留 3 天。窗口长度不从每次
`ingestData` 请求传入。

### 4.3 约束和关系

```proto
rpc syncConstraints(SyncConstraintsRequest)
    returns (SyncConfigResponse);
rpc syncRelations(SyncRelationsRequest)
    returns (SyncConfigResponse);
```

约束用于范围和线性规则检查；关系用于明确 target/source、关系类型、权重和固定
lag 等信息。相关配置由统一服务管理，Core 只保存运行时副本。

## 5. 派生序列接口

### 5.1 功能和限制

```proto
rpc syncDerivedSeriesConfigs(SyncDerivedSeriesConfigsRequest)
    returns (SyncConfigResponse);
```

派生序列是由已有连续数值序列计算出来的内存视图：

- 配置同步成功后，立即根据当前热窗口计算；
- 后续 `ingestData` 更新原始窗口后自动刷新；
- 支持线性组合和加减乘除表达式树；
- 不写入 TDengine，因此不会出现在 `queryHistoryData` 中；
- 输出 ID 不能和原始序列 ID 冲突；
- 当前只能引用原始实例序列，暂不支持派生序列之间继续嵌套引用。

### 5.2 线性组合 Proto

```proto
message LinearTerm {
  string sequence_id = 1;
  double coefficient = 2;
}

message LinearCombinationConfig {
  repeated LinearTerm terms = 1;
  double bias = 2;
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

例如，计算 `total = 2 * temperature + 3 * pressure + 1`：

```text
DerivedSeriesConfig {
  derived_sequence_id: "total"
  enabled: true
  linear_combination {
    terms { sequence_id: "temperature" coefficient: 2 }
    terms { sequence_id: "pressure" coefficient: 3 }
    bias: 1
  }
}
```

### 5.3 表达式树 Proto

```proto
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
```

`oneof node` 表示一个表达式节点只能是“序列、常量、二元运算”中的一种。比如
`(temperature + pressure) * 2` 的结构是：

```text
binary(MULTIPLY,
  binary(ADD, sequence("temperature"), sequence("pressure")),
  constant(2))
```

Core 会检查所有序列叶子是否已注册为连续数值序列，并拒绝空节点、未知运算符、
非有限常量和除零结果。

## 6. 数据写入和窗口接口

### 6.1 生产写入

```proto
rpc ingestData(IngestDataRequest) returns (IngestDataResponse);
```

`IngestDataRequest.points` 可以一次携带多个点。每个点包含时间、值，以及内部序列
ID或外部序列标识。

Core 依次完成：序列解析 → 原始数据写入 TDengine → 热窗口更新 → 派生序列刷新 →
约束检查和异常通知。

`IngestDataResponse` 会分别返回：

- `resolve_result`：序列解析结果；
- `storage_result`：冷数据写入结果；
- `window_result`：原始数据进入热窗口的结果；
- `derived_result`：派生热窗口刷新结果；
- `constraint_notification_result`：约束检查及通知结果；
- `operation`：上述阶段的综合结果；
- `resolved_data`：仅当 `return_resolved_data = true` 时返回。

### 6.2 窗口查询和更新

```proto
rpc buildTimeWindow(BuildTimeWindowRequest)
    returns (BuildTimeWindowResponse);
rpc queryWindowData(QueryWindowDataRequest)
    returns (QueryWindowDataResponse);
```

`buildTimeWindow` 是用于回放、测试和补偿的细粒度接口；生产场景优先使用
`ingestData`。`queryWindowData` 可以指定序列以及可选的起止时间。

## 7. 对齐、统计和约束检查

```proto
rpc alignWindowData(AlignWindowDataRequest)
    returns (AlignWindowDataResponse);
rpc computeBasicStatistics(ComputeStatisticsRequest)
    returns (ComputeStatisticsResponse);
rpc checkConstraints(CheckConstraintsRequest)
    returns (CheckConstraintsResponse);
```

这些接口的 `source` 使用 `oneof`，调用方三选一：

- 直接传 `WindowData`；
- 直接传 `AlignedWindowData`；
- 传 `QueryWindowDataRequest`，由 Core 自己读取热窗口。

对齐支持分桶、聚合、缺失值填充和已配置的固定 lag。统计返回各序列的 count、均值、
方差、标准差等；使用 Relation 时还可返回 target/source 的 Pearson 相关系数。
约束检查返回是否满足、评估次数和违反明细。

## 8. 历史查询

```proto
rpc queryHistoryData(QueryHistoryDataRequest)
    returns (QueryHistoryDataResponse);
rpc queryHistoryOverview(QueryHistoryOverviewRequest)
    returns (QueryHistoryOverviewResponse);
```

`queryHistoryData` 的时间范围为半开区间 `[start_time, end_time)`。可选的
`granularity` 单位为毫秒；不设置时返回原始点，设置后每个序列每个时间桶返回最后
一个值。历史查询只读取已写入 TDengine 的原始数据，不包含派生热窗口视图。

## 9. 调用方最小检查清单

- 是否已经先同步 `RuntimeInstanceConfig`；
- `sequence_id`、外部序列 ID 和 `data_source_id` 是否匹配；
- 连续序列的 `data_type` 和 `series_kind` 是否正确；
- 派生公式是否只引用连续数值序列；
- 是否把时间戳按 Unix 毫秒传入；
- 是否检查了 `grpc::Status` 和 `OperationResult.code`；
- 是否区分了热窗口查询和 TDengine 历史查询；
- 是否在 Core 重启后重新同步所有运行时配置。
