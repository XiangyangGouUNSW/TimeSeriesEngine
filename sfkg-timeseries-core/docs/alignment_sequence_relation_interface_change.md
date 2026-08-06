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
`AlignmentConfig.bucket_interval` 决定，不会因为不同 relation 产生额外输出桶。

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
接收 AlignmentConfig
接收 relation_ids
查询具体 RuntimeRelationConfig
调用 AlignmentService
返回 AlignedWindowData
```

## 不需要调整的内容

- `WindowData` 不需要增加车间或实体字段；
- `AlignedWindowData` 不需要增加 relation 字段；
- `ConstraintCheckEngine` 不负责关系展开和 lag 计算；
- `AlignmentConfig` 的 bucket、aggregation、fill_method 仍然保留；
- 无 relation 时的普通分桶对齐仍然可以使用同一个接口。

## 对统一服务模块的影响

统一服务需要完成以下工作：

1. 根据业务实体把类别关系展开为具体 `sequence_id`；
2. 为每个目标序列生成一条具体 relation；
3. 为每个 source 指定固定 lag；
4. 先调用 `syncRelations` 注册具体关系；
5. 调用 `alignWindowData` 时传入对应的 `relation_ids`；
6. 同步更新 proto 客户端代码。
