# 对齐接口：具体序列关系调整说明

## 目的

对齐服务不再根据 `category_id` 猜测多个车间或设备之间的对应关系。
统一服务负责把类别级关系实例化为具体的 `sequence_id` 关系，Core 只负责
按照已经确定的序列关系执行时间对齐、固定 lag 映射、分桶、聚合和填补。

## 当前问题

当前关系结构使用：

```text
target_category_id
source_category_id
```

当一个类别对应多个序列时，Core 无法知道同一关系中的序列是否属于同一个
车间、机器或设备上下文。例如：

```text
quality-A <- temperature-A, humidity-A
quality-B <- temperature-B, humidity-B
```

仅使用 `quality`、`temperature`、`humidity` 三个类别无法排除错误的交叉
匹配，例如 `quality-A <- temperature-B`。

## 调整后的关系语义

统一服务应传入已经实例化的具体关系：

```text
relation-A:
  target_sequence_id: quality-A
  sources:
    - source_sequence_id: temperature-A
      fixed_lag: 3
    - source_sequence_id: humidity-A
      fixed_lag: 3

relation-B:
  target_sequence_id: quality-B
  sources:
    - source_sequence_id: temperature-B
      fixed_lag: 3
    - source_sequence_id: humidity-B
      fixed_lag: 3
```

Core 不再负责 `category_id -> sequence_id` 的关系展开，也不需要解析
`sequence_id` 字符串来推断车间或设备归属。

## C++ 数据结构调整

`RuntimeRelationSource`：

```cpp
source_category_id  -> source_sequence_id
```

`RuntimeRelationConfig`：

```cpp
target_category_id  -> target_sequence_id
```

关系仍然保留 `relation_id`、`relation_type`、`confidence`、`enabled` 等字段。

当前基础实现只处理确定的固定 lag：

```cpp
fixed_lag: std::int64_t
```

未设置 lag 或 `lag_range` 当前返回明确的不支持状态，不能由 Core 自行猜测。

## AlignmentService 当前接口

当前接口为：

```cpp
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
```

语义如下：

```text
relations 为空：普通对齐，不使用 lag
relations 非空：按具体 sequence 关系应用固定 lag
```

使用 `vector` 是因为一个窗口中可能同时包含多个车间或设备，需要同时处理
多个目标序列关系。每条关系的 `target_sequence_id` 应唯一。

对齐结果仍然是 `AlignedWindowData`，输出时间网格由因变量窗口和
`AlignmentConfig.bucket_interval` 决定；省略时 Core 根据当前窗口中所有参与序列的
最小正时间间隔推导，不会因为不同 relation 产生额外输出桶。

`AlignmentConfig` 可以整体省略。省略的 sequence 列表默认使用当前窗口中的全部序列，
省略的聚合和填补策略由本地 `RuntimeConfigRegistry` 中的 `SeriesKind` 决定：连续型
默认 `AVERAGE + LINEAR`，离散型和类别型默认 `LAST + PREVIOUS`，未指定类型默认
`FIRST + NEAR`。显式传入的配置优先。

## relation 对齐操作

固定 lag 采用当前约定：

```text
source_time = target_time - lag
effective_target_time = source_time + lag
```

处理流程：

```text
1. 根据目标窗口和 bucket_interval 建立目标时间桶
2. 对每条具体关系找到 target_sequence_id 和 source_sequence_id
3. 将每个 source 点按 fixed_lag 映射到目标时间轴
4. 将映射后的点放入目标桶
5. 按 SequenceAlignmentConfig.aggregation 选择代表值
6. 对剩余空桶按 fill_method 填补
7. 输出 AlignedWindowData
```

`weight` 第一版只保存，不参与时间桶匹配；后续统计或关系计算可以使用它。

## Proto 调整

`RuntimeRelationSource` 的字段语义改为具体序列：

```proto
string source_sequence_id = 1;
```

`RuntimeRelationConfig` 的目标字段改为：

```proto
string target_sequence_id = 3;
```

字段编号可以保持不变，以减少 wire format 变化；但字段名和业务语义改变，
统一服务和 Core 必须同时重新生成 protobuf 代码并重新编译。

`lag_spec` 的 proto 结构可以暂时保留，以便后续扩展，但第一版对未设置 lag
和 `lag_range` 返回不支持，不执行范围选择。

`AlignWindowDataRequest` 已增加关系 ID：

```proto
repeated string relation_ids = 4;
```

调用时只传已通过 `syncRelations` 注册的具体关系 ID。Core 根据 ID 读取关系，
再传给 `AlignmentService`。本地 C++ 直接调用时可以直接传关系对象集合。

## Registry 和 gRPC 当前实现

`RuntimeConfigRegistry` 已提供关系查询能力：

```cpp
std::optional<RuntimeRelationConfig> findRelation(
    const std::string& relation_id) const;
```

关系同步时当前校验：

- `target_sequence_id` 非空；
- 每个 `source_sequence_id` 非空；
- source 序列不重复；
- 当前只接受 fixed lag；
- target 不得同时作为自己的 source。

`alignWindowData` gRPC 当前控制流程为：

```text
接收 WindowData 或 window_query
接收可选的 AlignmentConfig；省略时由 Core 本地补全默认配置
接收 relation_ids
查询具体 RuntimeRelationConfig
调用 AlignmentService
返回 AlignedWindowData
```

## 不需要调整的内容

- `WindowData` 不需要增加车间或实体字段；
- `AlignedWindowData` 不需要增加 relation 字段；
- `ConstraintCheckEngine` 不负责关系展开和 lag 计算；
- `AlignmentConfig` 的 bucket、aggregation、fill_method 仍然保留，但都可以按约定省略；
- 无 relation 时的普通分桶对齐仍然可以使用同一个接口。

## 对统一服务模块的影响

统一服务需要完成以下工作：

1. 根据业务实体把类别关系展开为具体 `sequence_id`；
2. 为每个目标序列生成一条具体 relation；
3. 为每个 source 指定固定 lag；
4. 先调用 `syncRelations` 注册具体关系；
5. 调用 `alignWindowData` 时传入对应的 `relation_ids`；
6. 同步更新 proto 客户端代码。

## 时序序列类型配置约定（新增）

`RuntimeInstanceConfig` 新增 `series_kind`，用于描述整个序列的业务含义，
不随每个时序点重复传入。当前枚举为：

```text
SERIES_KIND_UNSPECIFIED
SERIES_KIND_CONTINUOUS
SERIES_KIND_DISCRETE
SERIES_KIND_CATEGORICAL
```

统一服务负责保存并传入该字段。现有 `data_type` 字段保留兼容旧客户端，
仍用于接入阶段的实际值类型校验，并继续接受原有取值约定。Core 的本地
`AlignmentService` 会根据 Registry 中的 `series_kind` 自动补全未指定的对齐策略；
统一服务仍可以在 `AlignmentConfig` 中显式覆盖默认值。因此该字段不会改变
`TimeseriesIngestData`、`WindowData` 或历史查询接口。

受影响的调用方只有传输 `RuntimeInstanceConfig` 的配置同步调用：

1. 统一服务需要重新生成 proto 客户端，并可在 `syncInstanceConfigs` 中传入
   `series_kind`；省略时 Core 按 `UNSPECIFIED` 处理。
2. Core 的 proto 转换和运行时注册表会保存该字段，并在本地对齐时使用它补全默认策略。
3. 旧客户端不传该字段仍可联调；现有 `data_type` 的兼容行为保持不变。
