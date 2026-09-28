# 外部调用、输入校验与超时重试函数清单

## 等待其他进程响应与超时重试

1. `ConstraintResultReceiverClient::receiveConstraintResult`
   - 文件：`src/grpc/constraint_result_receiver_client.cpp`
   - 外部调用：`ReceiveConstraintResult` gRPC 接口；同步等待统一服务响应。
   - 总重试预算：10 秒；单次 deadline：1 秒；重试间隔：1 秒、2 秒、3 秒。
   - 最大调用次数：首次调用加 3 次重试，共 4 次；最终失败返回 `OperationCode::Unavailable`。

默认项目重载只是转调上述函数，不单独计为一个重试函数。

## 输入参数与数据合规性校验函数

1. `grpc::conversion::fromProto`
   - 文件：`src/grpc/proto_conversion.cpp`
   - 范围：时序值、接入点/批次、窗口数据、对齐数据、实例/约束/关系/窗口/派生配置等。
   - 作用：检查必填字段、`oneof` 值类型、枚举值、标识符和基本结构；失败返回错误信息，RPC 不进入业务处理。

2. `validation::validateValue`、`validateRawPoint`、`validateBatchContext`
   - 文件：`src/data_validation.cpp`
   - 作用：检查数值有限性、值类型、字符串和序列 ID 长度，以及请求/批次/点的 `project_id` 一致性。

3. `validation::validateWindowData`、`validateAlignedWindowData`
   - 文件：`src/data_validation.cpp`
   - 作用：检查窗口项目归属、时间范围、序列映射、点类型和对齐时间是否严格递增。

4. `IngestService::ingestAndResolveData`
   - 文件：`src/ingest_service.cpp`
   - 作用：检查接入批次非空、序列或外部标识、注册状态、数据类型及项目隔离；非法点按失败/部分成功返回，不进入写入和窗口处理。

5. `RuntimeConfigRegistry::replace*` / `upsert*` 配置函数
   - 文件：`src/runtime_config_registry.cpp`
   - 作用：检查运行时配置的标识、引用关系、约束上下界/项、派生表达式和序列类型，校验通过后才更新配置。
