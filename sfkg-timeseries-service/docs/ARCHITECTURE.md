# timeseries-service 调用链路文档

## 概述

`sfkg-timeseries-service`（Java Spring Boot）通过 gRPC 与 `TimeSeriesEngine`（C++）通信，实现时序数据的配置管理与数据入库。

```
┌─────────────────────────────────────────────────────────────────────┐
│                        REST API (JSON over HTTP)                     │
│   POST /api/timeseries/instances                                     │
│   POST /api/timeseries/semantic/constraints                          │
│   POST /api/timeseries/data/ingest                                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  sfkg-timeseries-service (Java)                      │
│                                                                      │
│  Controller  ──▶  Service  ──▶  TimeseriesCoreGrpcClient            │
│  (反序列化JSON)   (业务逻辑+持久化)   (构建protobuf并发送gRPC请求)         │
└────────────────────────────┬────────────────────────────────────────┘
                             │  gRPC (protobuf over HTTP/2)
                             │  target: ${TIMESERIES_CORE_GRPC_ADDRESS}
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   TimeSeriesEngine (C++)                             │
│                                                                      │
│  TimeseriesCoreGrpcService                                           │
│  ├── syncInstanceConfigs → RuntimeConfigRegistry::replaceInstanceConfigs
│  ├── syncConstraints    → RuntimeConfigRegistry::replaceConstraints
│  └── ingestData → IngestService → StorageService + WindowService     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. 实例配置 (Instance Config)

### 1.1 REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/timeseries/instances` | 创建实例配置 |
| `PUT` | `/api/timeseries/instances` | 更新实例配置 |

**请求 JSON 示例**（`instance-save.json`）：

```json
{
  "sequenceId": "temperature-1",
  "instanceName": "main transformer oil temperature",
  "externalSequenceId": "temp",
  "categoryId": "temperature",
  "deviceInstanceId": "device-3001",
  "dataSourceId": "source-a",
  "dataType": "continuous",
  "accessStatus": "ENABLE"
}
```

### 1.2 Java 端调用链

```
TimeseriesInstanceController.createInstanceConfig()
  └── TimeseriesInstanceServiceImpl.saveInstanceConfig()
        ├── instanceConfigMapper.insert(entity)    → 持久化到 DB
        ├── memoryCache.putInstanceConfig(entity)  → 更新内存缓存
        └── coreGrpcClient.syncInstanceConfig(entity)  → gRPC 同步到 Core
```

### 1.3 gRPC 调用详情

**文件**：`TimeseriesCoreGrpcClient.java`

1. 将 `TimeseriesInstanceConfig` entity 转换为 protobuf `RuntimeInstanceConfig`：
   ```
   RuntimeInstanceConfig {
     sequence_id       ← entity.sequenceId
     data_source_id    ← entity.dataSourceId
     external_sequence_id ← entity.externalSequenceId
     category_id       ← entity.categoryId
     data_type         ← entity.dataType
   }
   ```

2. 包装为 `SyncInstanceConfigsRequest { repeated RuntimeInstanceConfig items = 1; }`

3. 调用 `TimeseriesCoreServiceGrpc.syncInstanceConfigs(req)`，超时 3 秒

4. 解析返回的 `SyncConfigResponse { OperationResult operation }` 中的 `OperationCode`

### 1.4 C++ 端处理

**文件**：`timeseries_core_grpc_service.cpp` → `syncInstanceConfigs()`

1. 遍历 `request->items()`，通过 `conversion::fromProto()` 将每个 protobuf 消息转为 C++ `RuntimeInstanceConfig` 结构体
2. 调用 `config_registry_.replaceInstanceConfigs(snapshot)` 原子替换实例配置索引
3. 返回 `SyncConfigResponse` 含 `OperationResult`

---

## 2. 约束配置 (Constraint Config)

### 2.1 REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/timeseries/semantic/constraints` | 创建约束 |
| `PUT` | `/api/timeseries/semantic/constraints` | 更新约束 |

**请求 JSON 示例**（`constraint-save.json`）：

```json
{
  "constraintId": "constraint-5001",
  "constraintName": "temperature upper limit",
  "categoryId": "temperature",
  "variableMapping": { "x": "temperature-1" },
  "constraintExpression": "x < 100",
  "lowerBound": null,
  "upperBound": 100.0,
  "terms": [
    { "variable": "x", "coefficient": 1.0, "sampleOffset": 0 }
  ],
  "effectiveStatus": "ENABLE",
  "confirmStatus": "CONFIRMED"
}
```

### 2.2 Java 端调用链

```
TimeseriesSemanticController.saveConstraint()
  └── TimeseriesSemanticServiceImpl.saveConstraint()
        ├── constraintMapper.insert(entity)       → 持久化到 DB
        ├── memoryCache.putConstraint(entity)     → 更新内存缓存
        └── syncSemanticToCore(constraintId)
              └── coreGrpcClient.syncConstraintConfig(entity)  → gRPC 同步到 Core
```

### 2.3 gRPC 调用详情

**文件**：`TimeseriesCoreGrpcClient.java`

1. 将 `TimeseriesConstraint` entity 转换为 protobuf：
   ```
   ConstraintRule {
     constraint_id   ← entity.constraintId
     variable_mapping ← entity.variableMapping   (Map<string, string>)
     lower_bound     ← entity.lowerBound
     upper_bound     ← entity.upperBound
     terms[]         ← entity.terms → ConstraintTerm { variable, coefficient, sample_offset }
   }
   RuntimeConstraintConfig {
     rule    = ConstraintRule
     enabled = entity.effectiveStatus == "ENABLE"
   }
   ```

2. 包装为 `SyncConstraintsRequest { repeated RuntimeConstraintConfig items = 1; }`

3. 调用 `TimeseriesCoreServiceGrpc.syncConstraints(req)`

### 2.4 C++ 端处理

**文件**：`timeseries_core_grpc_service.cpp` → `syncConstraints()`

1. 遍历 `request->items()`，通过 `conversion::fromProto()` 转为 C++ `RuntimeConstraintConfig`
2. 调用 `config_registry_.replaceConstraints(snapshot)` — 验证规则结构、变量映射、数值序列后原子替换约束索引
3. 返回 `SyncConfigResponse`

---

## 3. 数据 Ingest

### 3.1 REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/timeseries/data/ingest` | 时序数据入库（推荐） |
| `POST` | `/api/timeseries/data/points` | 时序数据入库（兼容旧路径） |

**请求 JSON 示例**（`timeseries-data-save.json`）：

```json
{
  "windowSize": 3600000,
  "returnResolvedData": false,
  "points": [
    {
      "data_source_id": "source-a",
      "external_sequence_id": "temp",
      "time": 1722700800000,
      "double_value": 32.5
    },
    {
      "sequence_id": "temperature-1",
      "data_source_id": "source-a",
      "external_sequence_id": "temp",
      "time": 1722700920000,
      "double_value": 32.8
    }
  ]
}
```

> **字段说明**：
> - `sequence_id` — 可选，若未指定则由 Core 通过 `(data_source_id, external_sequence_id)` 查找已注册的实例
> - `time` — epoch 毫秒时间戳
> - 值字段使用 `double_value` / `int64_value` / `bool_value` / `string_value` 四选一，匹配 protobuf `TimeseriesValue.oneof`

### 3.2 Java 端调用链

```
TimeseriesDataController.ingestData()
  └── TimeseriesDataServiceImpl.saveTimeseriesData()
        ├── dataFileMapper.appendDataPoints(localPoints)   → 本地文件持久化
        ├── memoryCache.putTimeseriesDataPoints(localPoints) → 更新内存缓存
        └── coreGrpcClient.ingestData(request)             → gRPC 发送到 Core
```

### 3.3 gRPC 调用详情

**文件**：`TimeseriesCoreGrpcClient.java`

1. 遍历 `request.points`，对每个 `IngestPointDTO` 构建 `TimeseriesIngestData`：
   ```
   TimeseriesIngestData {
     sequence_id         ← point.sequenceId (optional)
     data_source_id      ← point.dataSourceId
     external_sequence_id ← point.externalSequenceId
     time                ← point.time (epoch millis → int64)
     value               ← TimeseriesValue {
       oneof: double_value | int64_value | bool_value | string_value
     }
   }
   ```

2. 包装为 `IngestDataRequest`：
   ```
   IngestDataRequest {
     repeated TimeseriesIngestData points = 1;
     int64 window_size = 2;
     bool return_resolved_data = 3;
   }
   ```

3. 调用 `TimeseriesCoreServiceGrpc.ingestData(req)`，超时 5 秒

4. 返回 `IngestDataResponse` 中 `operation.code` 为 `OK` 或 `PARTIAL_SUCCESS` 即视为成功

### 3.4 C++ 端处理

**文件**：`timeseries_core_grpc_service.cpp` → `ingestData()`

```
ingestData()
  │
  ├── [1] 验证 points 非空且 window_size > 0
  │
  ├── [2] 遍历 points，通过 conversion::fromProto() 转为 C++ TimeseriesIngestData
  │
  ├── [3] ingestService_.ingestAndResolveData(input)
  │       └── 通过 RuntimeConfigRegistry 解析 sequence_id：
  │           若未提供 sequence_id，则查 (data_source_id, external_sequence_id)
  │           映射找到已注册实例 → 补充 sequence_id
  │       └── 返回 IngestResult { operation, resolved_data }
  │
  ├── [4] 若 [3] 成功：
  │       ├── storageService_.writeRawData(resolved_data)
  │       │   └── 将解析后的数据写入 TDengine 持久存储
  │       └── windowService_.buildTimeWindow(resolved_data, window_size)
  │           └── 根据 window_size 构建/更新热数据时间窗口
  │
  ├── [5] 汇总 OperationResult：
  │       ├── resolve_result
  │       ├── storage_result
  │       ├── window_result
  │       └── operation (总体结果)
  │
  └── [6] 若 return_resolved_data=true，将 resolved_data 序列化到响应中
```

---

## 4. 关键数据类型对照

| REST JSON 字段 | Java DTO / Entity | Protobuf 消息 | C++ struct |
|---|---|---|---|
| `sequenceId` | `String sequenceId` | `RuntimeInstanceConfig.sequence_id` | `SequenceId sequence_id` |
| `dataSourceId` | `String dataSourceId` | `RuntimeInstanceConfig.data_source_id` | `string data_source_id` |
| `externalSequenceId` | `String externalSequenceId` | `RuntimeInstanceConfig.external_sequence_id` | `string external_sequence_id` |
| `dataType` | `String dataType` | `RuntimeInstanceConfig.data_type` | `string data_type` |
| `constraintId` | `String constraintId` | `ConstraintRule.constraint_id` | `string constraint_id` |
| `lowerBound/upperBound` | `Double` | `ConstraintRule.lower_bound/upper_bound` | `double` |
| `terms[].variable` | `ConstraintTermDTO.variable` | `ConstraintTerm.variable` | `string variable` |
| `terms[].coefficient` | `Double coefficient` | `ConstraintTerm.coefficient` | `double coefficient` |
| `terms[].sampleOffset` | `Long sampleOffset` | `ConstraintTerm.sample_offset` | `size_t sample_offset` |
| `points[].time` | `Long time` (epoch ms) | `TimeseriesIngestData.time` (int64) | `Timestamp time` (int64_t) |
| `points[].double_value` | `Double doubleValue` | `TimeseriesValue.double_value` | `variant<double,...>` |
| `points[].sequence_id` | `String sequenceId` | `TimeseriesIngestData.sequence_id` (optional) | `optional<SequenceId>` |

---

## 5. Proto 文件关系

```
TimeSeriesEngine/proto/timeseries_core.proto   ← C++ 侧定义（权威源）
        │
        │  手动对齐
        ▼
sfkg-timeseries-service/src/main/proto/timeseries_internal.proto  ← Java 侧定义
```

Java 侧 proto 文件名虽为 `timeseries_internal`，但 **package 已设为 `sfkg.timeseries.core.v1`**，与 C++ 侧完全一致，确保 gRPC 通信时的服务名和方法名匹配。编译后生成的 Java 类位于 `com.sfkg.timeseries.grpc` 包下。

### 核心 RPC 方法对照

| C++ proto (`timeseries_core.proto`) | Java proto (`timeseries_internal.proto`) |
|---|---|
| `syncInstanceConfigs` | `syncInstanceConfigs` ✅ |
| `syncConstraints` | `syncConstraints` ✅ |
| `syncRelations` | `syncRelations` ✅ |
| `ingestData` | `ingestData` ✅ |
| （未实现） | `SyncTaskStatus` (legacy, 保留) |
| `queryHistoryData` | `QueryHistoryData` (legacy, 保留) |

---

## 6. 配置

**Java 端** `application.yaml`：

```yaml
timeseries:
  grpc:
    core-address: ${TIMESERIES_CORE_GRPC_ADDRESS:localhost:9101}
```

通过环境变量 `TIMESERIES_CORE_GRPC_ADDRESS` 指定 C++ Core 引擎的 gRPC 地址，默认为 `localhost:9101`。

**C++ 端** 默认监听端口由 CMakeLists.txt / main.cpp 中 gRPC server 配置决定。
