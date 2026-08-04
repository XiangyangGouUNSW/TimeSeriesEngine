# 实现目录

本目录包含运行时配置注册表、TDengine 客户端、历史查询和其他业务服务的
实现，以及 gRPC 消息转换和服务适配。

当前 `storage_service.cpp`、`taos_client.cpp` 和
`history_query_service.cpp` 已支持原始数据写入、历史点查询和历史总体信息
查询；窗口、对齐、统计、约束等部分仍保留占位实现。

对外接口请查看 `include/` 和 `proto/`，不要直接依赖 `src/grpc/internal/`
中的内部转换头文件。
