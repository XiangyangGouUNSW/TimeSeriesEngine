# gRPC 写入压力测试

`grpc_ingest_benchmark` 是独立的手动压测客户端，不属于默认 CTest，也不
修改 Core 的生产业务逻辑。它通过 gRPC 调用正在运行的
`sfkg-timeseries-core-server`，默认测试完整的 `ingestData` 链路：

```text
gRPC → 数据识别 → TDengine 原始数据写入 → 内存窗口更新
```

## 构建

```bash
cmake -S . -B build-benchmark \
  -DSFKG_WITH_TAOS=ON \
  -DSFKG_BUILD_GRPC=ON \
  -DSFKG_BUILD_BENCHMARKS=ON \
  -DSFKG_TAOS_ROOT=/home/yumiduo/sfkg/tdengine
cmake --build build-benchmark -j2
```

## 运行

先启动 TDengine 和 Core，再在另一个终端运行压测客户端。建议使用独立的
`sfkg_timeseries_test` 数据库，并在正式测试前清空旧数据。

```bash
./build-benchmark/grpc_ingest_benchmark \
  --address 127.0.0.1:50051 \
  --workers 8 \
  --batch-size 1000 \
  --sequences-per-worker 8 \
  --duration-sec 60 \
  --window-size-ms 3600000 \
  --output tests/benchmarks/results/grpc_ingest_benchmark.csv \
  --log-file tests/benchmarks/results/grpc_ingest_benchmark.log
```

每个 worker 使用独立的测试序列，并先通过 `syncInstanceConfigs` 注册这些
序列，再通过 `syncWindowConfig` 设置热窗口长度。压测期间只输出固定间隔的吞吐汇总，以及最多 5 条错误样例，不输出
每个数据点的日志。结果会同时写入标准输出、日志文件和 CSV 文件。

主要指标包括：

- `persisted_points_per_sec`：Core 返回 TDengine 写入成功的实际点数吞吐；
- gRPC 调用平均延迟、p50、p95、p99；
- resolve、storage、window 三个阶段的成功/失败点数；
- 测试前后的历史点数，用于校验实际落库数量。

100,000 条/秒的判断应以 `persisted_points_per_sec` 和历史查询校验为准，
不能只看客户端生成或发送速率。

## 混合 RPC 场景

`grpc_mixed_benchmark` 用独立的写入线程和查询线程模拟稍微综合的 Core
负载：写入线程持续调用 `ingestData`，查询线程轮流调用：

- `queryHistoryData`
- `queryHistoryOverview`
- `queryWindowData`
- `alignWindowData`
- `computeBasicStatistics`（窗口数据和对齐数据两种路径）
- `checkConstraints`

工具会在开始时通过 gRPC 注册测试序列、一个关系和一个约束，测试期间每类
RPC 都单独统计调用数、成功数、失败数和延迟分位数。它只输出阶段汇总，不
输出每个数据点或每次 RPC 的明细。

每次运行默认自动生成唯一的序列命名空间；也可以通过 `--run-id` 手动指定。
这样重复压测时不会因为相同序列和重叠时间戳而覆盖旧测试点。

```bash
./build-benchmark/grpc_mixed_benchmark \
  --address 127.0.0.1:50051 \
  --ingest-workers 6 \
  --query-workers 2 \
  --batch-size 1000 \
  --sequences-per-worker 4 \
  --duration-sec 30 \
  --window-size-ms 3600000 \
  --output tests/benchmarks/results/grpc_mixed_benchmark.csv \
  --log-file tests/benchmarks/results/grpc_mixed_benchmark.log
```

该场景的写入吞吐会低于纯写入压测，这是预期现象；应同时观察各类 RPC 的
失败数和延迟，判断查询、对齐、统计、约束检查是否影响写入链路。

## 阶段级耗时测试

`grpc_stage_benchmark` 用不同的测试序列分别调用以下接口：

- `ingestAndResolveData`：只测数据解析和配置匹配；
- `writeRawData`：只测 TDengine 原始数据写入；
- `buildTimeWindow`：只测内存窗口更新；
- `ingestData`：测完整写入链路；
- `queryHistoryData`、`queryHistoryOverview`：测历史查询。

每个阶段默认执行 10 个 1000 点批次，历史查询默认重复 10 次。该测试是
分阶段的基准，不代表所有接口同时并发运行，适合先定位单阶段耗时来源：

```bash
./build-benchmark/grpc_stage_benchmark \
  --address 127.0.0.1:50051 \
  --batch-size 1000 \
  --batches 10 \
  --query-repetitions 10 \
  --output tests/benchmarks/results/grpc_stage_benchmark.csv \
  --log-file tests/benchmarks/results/grpc_stage_benchmark.log
```

如果需要观察 `queryHistoryData` 内部的 `queryRaw` 分阶段耗时，启动 Core 时
增加环境变量：

```bash
SFKG_TAOS_QUERY_TIMING=1 ./build-taos/sfkg-timeseries-core-server 0.0.0.0:50051
```

服务端日志中的 `queryRaw_timing` 会记录锁等待、SQL 执行、TDengine 行读取与
类型转换、排序以及总耗时；默认不开启，不会输出这些诊断日志。

当本次请求中的已注册序列具有相同物理值类型时，日志中的
`typed_projection=true` 表示只查询对应的值列；混合类型或未知类型请求会
自动使用通用字段投影。`fetch_mode=block` 表示结果使用TDengine批量块接口读取。
