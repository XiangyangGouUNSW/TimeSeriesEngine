# 测试

- `runtime_config_registry_test.cpp`：不依赖 gRPC 的配置闭环测试；
- `runtime_instance_config_proto_test.cpp`：验证 `RuntimeInstanceConfig.series_kind`
  的 proto 转换和旧客户端省略字段时的兼容行为；
- `taos_client_smoke.cpp`：依赖本地 TDengine，验证写入、历史查询和多种值类型；
- `smoke_client.cpp`：面向运行中服务的全 RPC 冒烟客户端；
- `data/smoke_scenario.json`：便于其他语言客户端复用的固定测试场景。

其中前两个测试由 CTest 注册；`smoke_client.cpp` 需要先启动 Core 服务，
再手动运行对应的可执行文件。`taos_client_smoke.cpp` 只有在 TDengine
可连接时才适合运行。

`tools/` 目录中的程序是面向 ETTh1 数据的手动演示，不属于自动化测试：
`etth1_taos_demo` 绕过 gRPC 直接写入 TDengine，`etth1_grpc_history_demo`
通过 gRPC 演示历史概览和历史点查询。

`etth1-direct-ingest-demo` 不经过 gRPC，直接模拟完整的本地接入链路：
读取 ETTh1 CSV、通过 `IngestService` 解析外部序列、调用
`StorageService` 和 `WindowService` 双写，最后调用历史和热窗口查询函数；
启动后还会提供一个简单的终端查询菜单。
程序默认把 `SFKG_TAOS_KEEP_DAYS` 设为 `20000`，以覆盖 ETTh1 的历史时间戳。
