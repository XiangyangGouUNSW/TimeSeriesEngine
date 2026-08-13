# 派生序列对外接口说明

本文只说明最近新增的派生序列功能。完整字段定义以
[`proto/timeseries_core.proto`](../proto/timeseries_core.proto) 为准。

## 1. 功能说明

统一服务可以给 Core 配置一条或多条派生序列。Core 根据已有的连续数值序列计算新
序列，并把结果放入内存热窗口。

- 派生结果只存在于热窗口中，不写入 TDengine；
- 后续查询热窗口时可以直接查询派生序列；
- 历史查询不会返回派生序列；
- 原始数据每次通过 `ingestData` 更新后，Core 会自动刷新派生结果；
- 派生序列 ID 不能与原始序列 ID 重复。

## 2. 配置接口

```proto
rpc syncDerivedSeriesConfigs(SyncDerivedSeriesConfigsRequest)
    returns (SyncConfigResponse);

message SyncDerivedSeriesConfigsRequest {
  repeated DerivedSeriesConfig items = 1;
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

调用前，公式中引用的原始序列必须已经通过 `syncInstanceConfigs` 注册，并且配置为
连续数值序列。

`enabled = false` 表示停用该派生序列，同时清除其热窗口数据。

## 3. 线性组合

```proto
message LinearTerm {
  string sequence_id = 1;
  double coefficient = 2;
}

message LinearCombinationConfig {
  repeated LinearTerm terms = 1;
  double bias = 2;
}
```

计算公式为：

```text
bias + coefficient1 * sequence1 + coefficient2 * sequence2 + ...
```

例如计算 `total = 2 * temperature + 3 * pressure + 1`：

```text
derived_sequence_id: "total"
enabled: true
linear_combination {
  terms { sequence_id: "temperature" coefficient: 2 }
  terms { sequence_id: "pressure" coefficient: 3 }
  bias: 1
}
```

## 4. 表达式树

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

`oneof node` 表示一个节点只能选择一种类型：序列、常量或二元运算。例如：

```text
(temperature + pressure) * 2

binary(MULTIPLY,
  binary(ADD, sequence("temperature"), sequence("pressure")),
  constant(2))
```

## 5. 计算和返回结果

调用 `syncDerivedSeriesConfigs` 后，Core 会立即根据当前热窗口计算一次。之后调用
`ingestData` 写入原始序列时，Core 会再次刷新派生序列。

不同来源序列的时间点不一致时：

- 连续序列在前后两个有效点之间采用线性插值；
- 找不到足够的有效点时，该时间点不生成派生结果；
- 除零、非有限值和无效表达式会返回失败或部分成功。

`IngestDataResponse` 新增：

```proto
OperationResult derived_result = 7;
bool storage_queued = 8;
```

`storage_queued=true` 表示冷数据已经进入 Core 的有界后台写入队列；它不表示 TDengine
已经完成持久化。TDengine 冷写失败会由 Core 后台 writer 记录到终端日志。

它表示本次写入之后派生热窗口的刷新结果；`operation` 会综合写入、热窗口、派生
刷新和约束通知等阶段的结果。

## 6. 当前限制

- 只允许引用已注册的连续数值原始序列；
- 当前不支持派生序列继续作为另一条派生序列的输入；
- 派生序列不进入 TDengine，也不参与历史数据查询；
- 派生配置保存在 Core 内存中，Core 重启后需要重新同步。
