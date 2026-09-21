# 外部调用超时重试函数清单

当前只有以下一个函数实际实现了外部调用的超时重试：

1. `ConstraintResultReceiverClient::receiveConstraintResult`
   - 文件：`src/grpc/constraint_result_receiver_client.cpp`
   - 外部调用：`ReceiveConstraintResult` gRPC 接口
   - 重试间隔：10 秒、20 秒、40 秒
   - 最大调用次数：首次调用加 3 次重试，共 4 次
   - 第 4 次仍失败后返回 `OperationCode::Unavailable`

`receiveConstraintResult` 的默认项目重载只是转调上述函数，不单独计为一个重试函数。
