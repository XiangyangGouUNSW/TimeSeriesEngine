# sfkg-timeseries-core 时序核心

本目录是时序数据核心服务，负责运行时配置接收、数据接入与冷热双写、内存窗口、约束检查、历史数据查询，以及通过 Protobuf/gRPC 向统一服务提供接口。

跨模块接口以 [proto/timeseries_core.proto](proto/timeseries_core.proto) 为准。
调用方简明说明见 [docs/external_grpc_api_overview.md](docs/external_grpc_api_overview.md)。

## 当前实现范围

- 运行时实例配置、约束和关联关系支持增量同步与更新；
- `IngestData` 已完成识别、标准化、冷热并行写入和热窗口更新的控制流程；
- 接入任务使用有界冷热队列，队列满时返回 `OPERATION_CODE_UNAVAILABLE`；
- TDengine 原始数据写入、历史数据查询和历史概览查询可运行；
- `ConstraintCheckEngine` 支持单序列 `WindowData`、多序列 `AlignedWindowData`、固定采样偏移和约束违反明细；
- `AlignmentService` 已支持普通分桶对齐和固定 lag 的具体序列关系对齐；
- `StatisticsService` 已支持 WindowData 基本统计，以及
  AlignedWindowData 的总体统计和基于 Relation 的 Pearson 相关系数；
- 对齐中的 lag range 和未设置 lag 仍明确返回不支持状态；
- Core 不负责启动 TDengine，也不保存统一服务的业务配置；Core 启动后由统一服务通过同步 RPC 写入运行时配置。

## 构建

需要 CMake 3.20+、C++17 编译器、Protobuf、gRPC，以及 TDengine Native Client。生成文件和二进制必须放在 `build-*` 目录中。

### 仅构建不依赖 TDengine/gRPC 的基础测试

```bash
cmake -S . -B build-core \
  -DSFKG_WITH_TAOS=OFF \
  -DSFKG_BUILD_GRPC=OFF
cmake --build build-core
./build-core/runtime_config_registry_test
```

### 构建完整 Core 服务

当前环境中的 TDengine 安装目录为 `/home/yumiduo/sfkg/tdengine`；其他机器请替换为实际路径。

```bash
cmake -S . -B build-taos \
  -DSFKG_WITH_TAOS=ON \
  -DSFKG_BUILD_GRPC=ON \
  -DSFKG_BUILD_TESTS=ON \
  -DSFKG_TAOS_ROOT=/home/yumiduo/sfkg/tdengine
cmake --build build-taos -j2
```

运行测试：

```bash
ctest --test-dir build-taos --output-on-failure
```

本地验证冷写分片和各阶段耗时时，可以运行独立压力工具。它不连接统一服务。工具的参数为
`批次数 每批点数 序列数 生产者数 writer数 window_size_ms profile hot_worker_count`；当模拟
ETTh1 时，7 个序列
共同组成一行时间戳，因此每批 1000 行应传入 7000 个点，并把窗口设置为 3600000 ms：

```bash
env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  ./build-taos/local-ingest-parallel-benchmark 50 7000 7 1 8 3600000 standard 1
```

上面的 `standard` 档位会注册一个简单单变量派生、一个单变量约束和一个多变量约束，
是建议固定使用的 Core 压测口径。`mixed` 档位会在此基础上再增加一个多变量派生，用于
额外测试派生复杂度。
50 批等价于总计 50000 个时间戳、350000 个数据点，正好对应
“持续 5 秒、总计 10000 个时间戳/秒、每个时间戳 7 个变量”的验收数据量。工具会尽快
处理这批数据并报告实际吞吐量（不是人为限速 5 秒），同时输出时间戳行/秒和数据点/秒；
因此验收换算关系是 `10000 行/秒 = 70000 点/秒`。若只想做短 smoke test，可以把 50
改成 20；`profile` 可选 `none`、`single`、`standard`、`mixed`，分别表示不配置、只配置单变量、
标准派生/约束配置、同时配置单变量和多变量派生/约束。最后一个参数控制热 worker 数；设为 4 可以测试不同批次并发处理，
但同一序列仍由窗口内部的序列锁保护。工具会分别输出 `hot window`、`derived`、`constraint query` 和
`constraint check` 的阶段耗时。若只想运行原来的通用规模，也可以省略参数，但其默认值不是
ETTh1 验收口径。

例如将总批次数扩大到 100：

```bash
env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  ./build-taos/local-ingest-parallel-benchmark 100 7000 7 1 8 3600000 standard 1
```

工具会输出每个 `sequence_id` 的固定 writer 分配、各 writer 的批次数和点数、路由一致性校验、
解析、冷写、热窗口/派生刷新以及提交到完成的耗时。该工具不包含统一服务网络和约束
异常通知 RPC 的耗时。

上面的固定 baseline 使用一个有序 producer 和一个 hot worker，保证每个序列严格按时间递增，
用于比较派生、约束和窗口本身的稳定耗时。若要额外测试跨批次热并发，可以使用：

```bash
env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  ./build-taos/local-ingest-parallel-benchmark 50 7000 7 4 8 3600000 standard 4
```

但这个并发命令中的输入生产者若没有额外的按序调度，就不保证同一序列严格有序。该工具的输入生产者虽然可以并发，但若要验证“同一序列严格按时间递增”的增量路径，
调用方必须保证同一个 `sequence_id` 的批次按时间顺序提交。多个线程本身不提供这个保证；
如果发送端让后来的时间批次先到达，工具会把它记录为增量不安全并走保守路径。Core 本地
吞吐量是从所有任务开始前的 `wall_start` 到所有任务完成后的 `wall_end` 统计的，公式为：

```text
throughput_points_per_sec = written_points / wall_seconds
throughput_rows_per_sec = (written_points / sequence_count) / wall_seconds
```

因此它测量的是 Core 本地处理、热窗口和 TDengine 冷写的综合耗时，不包含统一服务到 Core
之间的 gRPC 网络耗时。需要测网络链路时，应使用 `grpc_*_benchmark` 或统一服务的四线程
调用日志，分别统计客户端请求耗时和 Core 侧 `rpc=ingestData elapsed_ms`。

本仓库还提供了固定 ETTh1 口径的本机 gRPC 压测客户端。它从 `ETTh1.csv` 读取变量值，
将时间轴重建为每秒一个点，使用 4 个 gRPC 客户端线程；每个序列固定归属于一个客户端线程，
因此同一序列的时间戳严格递增，4 个线程之间仍然并发。总量为 50000 行、350000 点，
冷写配置应与 Core 的 8/8 配置一致：

```bash
env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  ./build-taos/etth1_grpc_ingest_benchmark \
  --address 127.0.0.1:50051 \
  --csv ../ETTh1.csv \
  --rows 50000 \
  --batch-rows 1000 \
  --workers 4 \
  --window-ms 3600000 \
  --with-constraint \
  --with-derived
```

该工具输出 `persisted_points`、完整 RPC 的平均/p50/p95 延迟以及
`throughput_rows_per_sec` 和 `throughput_points_per_sec`。它的吞吐量包含客户端到 Core 的
本机 gRPC 往返、Core 排队、冷热处理和 TDengine 写入；Core 端应同时打开采样诊断日志，
用 `ingest_diag` 对照拆分 `hot_window_ms`、`derived_ms`、`constraint_check_ms` 和
`handler_ms`。

## 正式运行方式

运行时分成两个进程：

```text
TDengine       tmux 中长期运行，默认监听 6030
Core gRPC 服务 普通终端前台运行，默认监听 50051
```

Core 主服务不会自动启动 TDengine。启动 Core 前必须先确认 TDengine 可连接。

### 1. 在 tmux 中启动 TDengine

创建专用会话：

```bash
tmux new -s sfkg-tdengine
```

进入 tmux 后，在其中执行：

```bash
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
/home/yumiduo/sfkg/tdengine/bin/taosd \
-c /home/yumiduo/sfkg/tdengine/cfg
```

看到 `The daemon initialized successfully` 后，按 `Ctrl-B`，再按 `D`，退出 tmux 但保持 TDengine 运行。

检查服务状态：

```bash
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
/home/yumiduo/sfkg/tdengine/bin/taos \
-c /home/yumiduo/sfkg/tdengine/cfg -k
```

预期返回：

```text
2: service ok
```

也可以检查端口：

```bash
ss -lntp | grep ':6030'
```

重新查看 TDengine 日志：

```bash
tmux attach -t sfkg-tdengine
```

### 2. 在普通终端启动 Core

TDengine 确认就绪后，在普通终端执行：

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core

env \
  -u grpc_proxy \
  -u http_proxy \
  -u https_proxy \
  -u HTTP_PROXY \
  -u HTTPS_PROXY \
  -u ALL_PROXY \
  -u all_proxy \
  LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  no_grpc_proxy=222.29.156.142 \
  no_proxy=localhost,127.0.0.1,222.29.156.142 \
  NO_PROXY=localhost,127.0.0.1,222.29.156.142 \
  SFKG_TAOS_HOST=127.0.0.1 \
  SFKG_TAOS_PORT=6030 \
  SFKG_TAOS_USER=root \
  SFKG_TAOS_PASSWORD=taosdata \
  SFKG_TAOS_DB=sfkg_timeseries \
  SFKG_TAOS_KEEP_DAYS=365000 \
  SFKG_TAOS_RAW_STABLE=raw_timeseries_data \
  SFKG_TAOS_WRITE_CONNECTIONS=8 \
  SFKG_INGEST_COLD_WORKERS=8 \
  SFKG_INGEST_HOT_WORKERS=8 \
  SFKG_INGEST_QUEUE_CAPACITY=10 \
  SFKG_INGEST_DIAGNOSTIC_LOG=1 \
  SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY=50 \
  SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS=222.29.156.142:9105 \
  SFKG_TIMESERIES_CORE_ADDRESS=0.0.0.0:50051 \
  ./build-taos/sfkg-timeseries-core-server 0.0.0.0:50051
```

启动 Core 时需要为统一服务地址绕过本机 HTTP 代理，否则 gRPC 可能会错误连接到
本地代理端口而无法调用 `ReceiveConstraintResult`。上述命令只对 Core 进程清除代理，
不会改变当前终端或其他程序的代理配置。

上面的启动命令会把 Core 的普通 RPC 日志和未处理异常直接输出到当前终端，例如
`rpc=ingestData grpc_code=0 elapsed_ms=...`。如果需要同时观察 IngestData 的解析、排队、
冷写、热窗口和业务错误详情，可以在上面命令的 `SFKG_INGEST_QUEUE_CAPACITY` 后面加入：

```bash
  SFKG_INGEST_DIAGNOSTIC_LOG=1 \
  SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY=100 \
```

此时每 100 个 IngestData 请求会额外输出一条 `ingest_diag`；如果该请求是业务失败，
日志中还会包含 `operation_message`。如需同时保存日志，把启动命令的最后一行改成：

```bash
./build-taos/sfkg-timeseries-core-server 0.0.0.0:50051 \
  2>&1 | tee /tmp/sfkg-timeseries-core.log
```

诊断日志默认关闭；正式吞吐测试不要开启全量诊断。

`SFKG_TAOS_RAW_STABLE` 默认值为 `raw_timeseries_data`，用于指定原始时序数据的超级表名称；
自定义名称时，Core 会按当前原始数据结构创建并查询对应超级表。

`SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS` 用于指定约束异常结果接收服务，默认值为
`222.29.156.142:9105`。如果不设置，Core 仍会使用该默认地址。

写入并发相关配置为：

- `SFKG_TAOS_WRITE_CONNECTIONS`：TDengine 写连接数，默认 4，范围为 1～64；
- `SFKG_INGEST_COLD_WORKERS`：冷数据写入工作线程数，默认 4；
- `SFKG_INGEST_HOT_WORKERS`：不同 IngestData 批次的热处理工作线程数，默认 1；热窗口内部还会按
  `sequence_id` 分片，同一批次中不同序列可以并行更新，同一序列仍由序列级写锁保护；
  设置大于 1 后，不同批次也可以并行进入热处理。Core 会在任务接纳时为每个序列建立 FIFO
  依赖，同一序列的后续批次必须等待前一批热处理完成；涉及不同序列的批次仍可并行。这样在
  调用方保证同一序列按时间递增发送时，多 HOT worker 不会自行打乱增量更新顺序。跨序列查询
  不承诺一个全局原子快照；
- `SFKG_INGEST_QUEUE_CAPACITY`：最多同时接纳的 IngestData 批次数，默认 128。
- `SFKG_INGEST_DIAGNOSTIC_LOG`：设为 `1` 开启 IngestData 阶段诊断日志，默认关闭；
- `SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY`：诊断采样间隔，默认 1。联调压力测试建议设为
  `100`，即每 100 个 IngestData 请求打印一条详细日志。
- `SFKG_INGEST_DIAGNOSTIC_WRITERS`：设为 `1` 时追加每个冷写 writer 的分片和耗时明细，
  默认关闭。默认 `ingest_diag` 会保留窗口序列锁等待、序列更新和窗口淘汰耗时，便于定位
  热窗口瓶颈。
- `SFKG_CONSTRAINT_NOTIFY_QUEUE_CAPACITY`：约束违反异步通知队列容量，默认 1024。约束检查
  仍在热处理阶段完成，但向统一服务的 `ReceiveConstraintResult` RPC 由独立通知线程发送；
  IngestData 返回的通知结果表示“已入队”，不再等待远端 RPC 完成。队列满时返回不可用状态，
  不会静默丢弃异常事件。

队列容量按批次而不是单条记录计算。一个请求只有在冷热两条通道都成功预留容量后才会
执行；队列满时不会执行部分写入，调用方可以根据 `OPERATION_CODE_UNAVAILABLE` 重试。
当前实现要求 `SFKG_TAOS_WRITE_CONNECTIONS` 不小于 `SFKG_INGEST_COLD_WORKERS`，推荐两者设置为
相同值。每个冷写线程拥有固定队列和固定连接；Core
首次看到一个 `sequence_id` 时按轮询方式分配 writer，之后一个 Batch 会按 `sequence_id`
分组后投递到对应 writer。不同序列可以并行，同一序列不会在多个 writer 之间迁移。

诊断日志只输出请求数量、各阶段耗时和结果状态，不输出具体数据值。`hot_window_ms` 是热窗口
更新的墙钟耗时；`window_seq_wait_ms`/`window_evict_wait_ms` 是序列锁等待时间之和，可能因为
多个序列并发而大于墙钟耗时。需要冷写分片明细时再额外设置
`SFKG_INGEST_DIAGNOSTIC_WRITERS=1`。示例：

```bash
SFKG_INGEST_DIAGNOSTIC_LOG=1 \
SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY=100
```

详细日志会出现在 Core 进程的标准错误输出中；开启全量诊断会增加日志 I/O，不应作为正式
吞吐测试配置。

Core 正常启动后应打印：

```text
sfkg-timeseries-core listening on 0.0.0.0:50051
```

Core 保持前台运行，方便观察日志；停止时在该终端按 `Ctrl-C`。

检查 Core 端口：

```bash
ss -lntp | grep ':50051'
```

### 3. 统一服务连接方式

统一服务连接 Core 时使用服务器实际 IP 和端口：

```text
同一台机器：127.0.0.1:50051
跨机器联调：<Core服务器实际IP>:50051
```

不要把 `0.0.0.0:50051` 作为客户端连接地址；`0.0.0.0` 只表示 Core 的监听地址。

统一服务启动后，建议按以下顺序调用：

1. `syncInstanceConfigs` 同步实例配置；
2. `syncWindowConfig` 同步任务窗口长度；未同步时 Core 默认使用 3 天；
3. `syncConstraints`、`syncRelations` 同步约束和关联关系；
4. 如有需要，调用 `syncDerivedSeriesConfigs` 同步派生序列公式；
5. 调用 `ingestData` 或 `ingestAndResolveData` 接入数据；
6. 调用窗口、约束检查或历史查询接口。

`queryHistoryData` 的 `granularity` 单位为毫秒。未设置时返回原始点；设置后
按时间桶聚合，每个序列每个时间桶返回最后一个值。返回结果仍按时间升序，
相同时间戳按请求中的 `sequence_ids` 顺序排列。

Core 重启后运行时配置会清空，因此每次启动后都需要由统一服务重新同步配置。TDengine 中的历史数据不会因此清空。

### 4. 数据库保留期

TDengine 的时间戳仍然必须使用数据库的 `ms` 精度，数据库也不能提供数学意义上的无限时间范围。TDengine 的 `KEEP` 最大可配置为 `365000d`；长期历史数据场景建议在新数据库首次创建时显式设置：

```bash
SFKG_TAOS_KEEP_DAYS=365000
```

该环境变量只在数据库首次创建时参与 `CREATE DATABASE`。如果数据库已经存在，重新启动 Core 不会修改原有 `KEEP`，需要单独执行：

```sql
ALTER DATABASE sfkg_timeseries KEEP 365000d;
```

### 5. 清空测试数据库数据

测试需要清空数据时，可以使用仓库自带脚本。脚本只删除
`sfkg_timeseries.raw_timeseries_data` 中的数据，保留数据库、超级表和子表结构：

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core
./scripts/clear_sfkg_timeseries_db.sh
```

脚本会先要求输入 `sfkg_timeseries` 确认。确认 Core 当前没有继续写入后，
也可以使用 `--yes` 跳过交互确认：

```bash
./scripts/clear_sfkg_timeseries_db.sh --yes
```

该脚本可以在普通终端执行，也可以在 `sfkg-tdengine` tmux 会话中新开窗口执行；
tmux 中的 TDengine 进程不需要停止。若 Core 正在接收数据，清理期间可能又产生新数据，
因此测试重置时应先停止 Core 或暂停统一服务的写入。

调整 `KEEP` 不会恢复之前已经被拒绝或删除的数据；这类数据需要在调整后重新接入。

### 5. 停止服务

先在 Core 普通终端按 `Ctrl-C`，再进入 TDengine tmux 会话：

```bash
tmux attach -t sfkg-tdengine
```

在 tmux 中按 `Ctrl-C` 停止 TDengine。确认不再需要该会话时，可以退出 shell：

```bash
exit
```

如果只是断开 SSH，不要按 `Ctrl-C`，直接使用 `Ctrl-B`、`D` 分离即可。

## 目录说明

- `app/main.cpp`：正式 Core gRPC 服务入口；
- `src/`：领域服务和 gRPC 适配实现；
- `include/`：公共 C++ 接口和数据结构；
- `proto/`：跨模块 Protobuf/gRPC 协议；
- `tests/`：单元测试和本地测试程序；
- `tools/`：开发阶段演示和辅助工具，不作为正式服务入口。

## Git 节点管理

修改应在一个可独立解释和验证的节点完成后提交。提交前至少执行适用的编译或测试，并保持工作区不混入生成文件和构建产物。
