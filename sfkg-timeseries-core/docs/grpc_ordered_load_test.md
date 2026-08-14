# 有序多序列 gRPC 吞吐测试

`grpc_ordered_load_test` 是一个独立的 Core gRPC 负载客户端。它不会启动 Core，也不直接连接 TDengine；先启动 Core 主进程，再运行该客户端即可。

## 构建

需要打开 gRPC 和 benchmark：

```bash
cmake -S . -B build-taos \
  -DSFKG_WITH_TAOS=ON \
  -DSFKG_BUILD_GRPC=ON \
  -DSFKG_BUILD_BENCHMARKS=ON \
  -DSFKG_TAOS_ROOT=/home/yumiduo/sfkg/tdengine
cmake --build build-taos -j2 --target grpc_ordered_load_test
```

## 运行

Core 主进程仍然监听自己的 gRPC 地址，例如：

```bash
SFKG_TIMESERIES_CORE_ADDRESS=0.0.0.0:50051 \
SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS=222.29.156.142:9105 \
./build-taos/sfkg-timeseries-core-server
```

测试客户端连接的是 Core 的 `50051`，不是约束结果接收端的 `9105`：

```bash
./scripts/run_grpc_ordered_load_test.sh \
  --address 127.0.0.1:50051 \
  --sequences 32 \
  --rows 50000 \
  --batch-rows 300 \
  --workers 1 \
  --window-ms 36000000 \
  --sample-ms 1000 \
  --derived 4 \
  --constraints 40
```

如果 Core 在另一台机器上，将 `--address` 改成 Core 的可访问地址。测试数据的序列 ID 会自动带一个本次运行唯一的前缀；也可以显式指定：

```bash
./build-taos/grpc_ordered_load_test \
  --address 192.168.1.20:50051 \
  --prefix throughput-test-a \
  --sequences 64 \
  --rows 100000 \
  --batch-rows 1000 \
  --workers 4
```

## 时序保证

每个逻辑批次覆盖同一段时间，并包含所有原始序列。`--workers N` 只把当前批次的序列分片给 N 个 gRPC 客户端线程；批次屏障会阻止任何线程提前发送下一个时间批次。因此：

- 同一序列严格按时间递增；
- 不同序列的逻辑时间戳完全对齐，跨序列时间偏差为 0；
- 不会出现某个序列已经推进很多批、其他序列仍停留在旧时间的情况；
- `rows × sequences` 是本次测试精确提交的原始点数。

约束使用宽安全范围，默认不会产生通知回调，但每次写入仍会执行约束检查；派生配置和约束数量可以通过命令行调整。

## 输出

客户端会输出：

```text
submitted_points
accepted_points
failed_points
points_per_sec
rows_per_sec
rpc_avg_ms
rpc_p50_ms
rpc_p95_ms
```

只有提交点数等于预期点数、所有点被 Core 接受且没有 RPC/业务错误时才返回退出码 `0`；失败时返回非零退出码。

`accepted_points` 表示 Core 已接受并完成热窗口/派生处理的点数。Core 的 `storage_queued=true` 只表示冷写进入后台队列，因此该工具的吞吐量是 gRPC 到 Core 的处理吞吐量，不是 TDengine 最终落盘确认吞吐量。
