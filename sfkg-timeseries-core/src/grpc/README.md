# gRPC 适配层

`proto_conversion.cpp` 与 `internal/proto_conversion.hpp` 负责 Protobuf 与领域
结构的双向转换；该头文件只供适配层内部使用，不属于对外安装接口。
`timeseries_core_grpc_service.cpp` 负责请求校验、调用编排、响应组装、日志和
异常边界。

跨模块接口的源文件是 `proto/timeseries_core.proto`。CMake 会在构建目录中
自动生成 `timeseries_core.pb.*` 和 `timeseries_core.grpc.pb.*`，这些生成文件
不需要手动修改或提交。
