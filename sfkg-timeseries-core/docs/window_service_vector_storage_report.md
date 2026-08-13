# WindowService vector-like 存储改造报告

## 结论

热窗口现在按 `sequence_id` 分组存储。每个序列的正常数据使用按时间追加的
`std::vector<RawTimeseriesPoint>`，每个点仍然携带自己的毫秒时间戳；少量迟到、乱序或
同时间戳修正点进入该序列的临时有序修正区。这样不要求序列等间隔采样，也不会因为一个
迟到点搬移整个主 vector。

本次改造没有修改 gRPC/proto 接口、TDengine 表结构或外部调用方式。

## 为什么能提速

原实现是每个序列一个 `std::map<Timestamp, RawTimeseriesPoint>`。每个点都要进行树查找、
节点分配和指针跳转，写入复杂度为 `O(log n)`，缓存局部性也较差。

现在的正常路径满足“同一序列新点时间戳严格晚于当前尾部”时，直接 `vector::push_back`：

- 写入是摊销 `O(1)`，批量 reserve 后减少扩容；
- 数据连续存放，遍历、派生计算和后续查询更容易命中 CPU cache；
- 查询用 `lower_bound` 定位时间范围；
- 淘汰只先推进 `active_begin` 逻辑头指针，达到阈值后再批量物理压缩，避免每次删除都移动
  大段数据。

乱序点并不是完全不能处理：它们会被放进 `late_points`，查询时与主 vector 做有序归并，
同一时间戳的修正值覆盖旧值。需要完整刷新或派生区间修补时，再把修正区合并回主 vector。
因此乱序会使本次更新标记为 `incremental_safe=false`，提醒派生计算和约束检查走安全的完整
刷新路径，但不会丢数据。滑动窗口淘汰通过独立的 `window_evicted` 标志报告，不再破坏追加
更新的增量安全性；下游只需从新的窗口边界重算受影响后缀。

## 保留的语义

- 每个 `RawTimeseriesPoint` 自带时间戳，不从采样间隔推导时间；
- 查询结果按时间升序返回，范围仍为 `[start_time, end_time)`；
- 相同 `sequence_id + timestamp` 的后写入值覆盖先写入值；
- 滑动窗口按真实时间戳淘汰，左边界保留；
- 派生序列的全量替换和区间 patch 仍支持乱序输入，并保持去重语义；
- 共享窗口索引的查询使用共享锁，多个客户端并发调用不会互相阻塞；写入只在更新索引、
  watermark 和淘汰边界快照时使用短时唯一锁，真正的点追加、修正和淘汰使用每个序列
  自己的读写锁。因此同一批中不同 `sequence_id` 可以并行处理，同一序列仍保持串行。

## 验证结果

新增了 `window_service_test`，覆盖：不规则时间戳、首批乱序、重复时间戳、迟到修正、
实际时间淘汰、vector 逻辑压缩、派生序列替换/patch，以及四线程写不同序列。新增的
`window_service_differential_test` 使用旧的 `map<timestamp,value>` 参考模型，随机执行
3,000 轮写入、重复/乱序点、窗口大小变化和查询，逐轮比较结果。

已有领域测试也通过：

```text
runtime_config_registry_test
constraint_check_engine_test
alignment_service_test
statistics_service_test
derived_series_service_test
window_service_test
window_service_differential_test
```

窗口两个测试还在 AddressSanitizer/UBSan 构建下通过；LeakSanitizer 因当前运行环境的
ptrace 限制关闭，但 AddressSanitizer 和 UBSan 本身正常运行。

手工基准命令：

```bash
/home/yumiduo/miniconda3/envs/llm-env/bin/cmake -S . -B build-taos \
  -DSFKG_BUILD_TESTS=ON -DSFKG_BUILD_BENCHMARKS=ON
/home/yumiduo/miniconda3/envs/llm-env/bin/cmake --build build-taos -j4 \
  --target window_service_benchmark
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/home/yumiduo/sfkg/tdengine/lib \
  ./build-taos/window_service_benchmark
```

以下为连续 3 次运行的耗时范围，`map` 是包含批次分组和写入步骤的按序列时间树基准，
不是 TDengine 写入基准：

| 情形 | 点数 | vector 更新 | map 更新 | 说明 |
|---|---:|---:|---:|---|
| 严格递增 | 35,000 | 23.377–26.465 ms | 52.130–58.946 ms | vector 约快 2 倍以上 |
| 不等间隔递增 | 35,000 | 22.469–27.564 ms | 53.896–63.648 ms | 不依赖固定采样间隔 |
| 序列扩展（64 序列） | 64,000 | 41.749–45.289 ms | 84.751–92.300 ms | 约快 2 倍，观察序列数量影响 |
| 首批乱序 | 7,000 | 18.372–20.195 ms | 10.305–10.973 ms | 需要排序/归并，非主路径 |
| 少量迟到修正 | 350 | 0.798–0.980 ms | 0.267–0.479 ms | 不复制 35,000 个主数据点，绝对开销很小 |

连续小批次追加也单独测量：7 个序列、每批每序列 10 点时，100 批共 7,000 点耗时
约 11.231 ms，1,000 批共 70,000 点耗时约 110.132 ms，点数扩大 10 倍时耗时约扩大
9.8 倍，没有出现每批复制全部历史导致的平方级增长。这个场景包含每批分组和锁开销，
所以 vector 不一定比简化的 map 基准更快；它验证的是扩容行为没有退化。

首批乱序和少量修正的绝对耗时受哈希、分组、排序和测试机器负载影响；它们不是连续实时
追加的主要路径。真正需要关注的是：正常追加不再对每个点执行树节点分配，偶发乱序只影响
对应序列，并触发安全刷新标记。基准中迟到修正的 vector 处理仍有少量分组和锁开销，但
已经不会因为预先 `reserve` 而复制整个主 vector。

另外使用独立测试数据库进行了最终 gRPC/TDengine 回归：ETTh1 240 行、6 批、共 1,680 个
点，同时开启派生配置和约束配置；6 批均返回 `OPERATION_CODE_OK`，每批成功写入 280 点；
测试数据库已删除。TDengine smoke、direct ingest/history round-trip 和 sharded write 三个
独立测试也通过。

完整 CTest 在当前受限沙箱中有 4 项不能直接运行：本地 gRPC 测试不能绑定临时端口，3 项
TDengine 测试不能写 TDengine 日志目录。将这 4 项放到允许本地绑定和 TDengine 日志写入的
环境后均通过；这不是窗口实现失败。

## 后续边界

当前改造解决的是窗口内存索引和淘汰成本，不等于已经完成所有吞吐优化。窗口的元数据仍
需要一个短时共享锁；每个批次当前按受影响序列启动异步更新任务，超大序列数场景后续可
换成复用线程池以减少线程创建开销。派生刷新已经允许不同 IngestData 批次并发计算，并
用更新代次避免旧结果覆盖新结果；约束检查和 gRPC 往返仍需单独测量。当前增量对齐接口
使用有序窗口数据的二分定位，只复制受影响桶及其边界上下文，并在桶内流式聚合；只有
发现乱序、稀疏边界无法由局部上下文填补等情况时才回退到完整对齐。普通对齐还会在
序列数量较大时按序列并行，最终按配置顺序合并以保持结果确定性。若未来乱序比例很高，
可以再评估更专门的分段 vector、
时间桶或批量归并结构，但不应为了极少量乱序牺牲正常追加路径。
