# sfkg-timeseries-core 时序核心

本目录是时序数据引擎的 C++ 核心代码。它提供跨模块 Protobuf/gRPC
协议、运行时配置注册表、TDengine 原始数据存储、历史查询、服务启动入口
和测试/演示程序。

第一次接触项目时，可以先看上一级目录的
`异常检测模块对接说明.md`；只关心跨模块字段和调用方式时，直接查看
`proto/timeseries_core.proto`。

## 当前实现范围

- 五类运行时配置支持完整快照替换、基础引用校验和并发查询；
- 所有 RPC 均完成 Protobuf 与 C++ 类型转换和参数边界检查；
- TDengine 原始时序写入、历史数据查询和历史概览查询已可运行；
- 尚未完整实现的窗口、对齐、统计和约束功能明确返回
  `OPERATION_CODE_NOT_IMPLEMENTED`；
- gRPC 正常完成而业务尚未实现时，gRPC 状态仍为 `OK`，业务结果写入
  `OperationResult`；
- `ingestData` 已搭出识别、持久化和窗口更新三个阶段的组合控制流。

## 构建

需要 CMake 3.20+、C++17 编译器、Protobuf 和 gRPC 的 CMake config 包。
如果只想验证配置注册表，可以不安装 TDengine 和 gRPC：

```bash
cmake -S . -B build-core \
  -DSFKG_WITH_TAOS=OFF \
  -DSFKG_BUILD_GRPC=OFF
cmake --build build-core
./build-core/runtime_config_registry_test
```

完整构建默认会生成服务器、gRPC 冒烟客户端、测试和 ETTh1 演示程序。

```bash
cmake -S . -B build
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

Windows PowerShell 也可以使用：

```powershell
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

项目禁止在源码目录内直接运行 `cmake .`；所有生成文件、目标文件和二进制
产物必须位于 `build/` 或 `build-*` 目录。上述目录已加入 Git 忽略规则。

启用 TDengine 时，CMake 会严格检查 `taos.h` 和 `libtaos.so`，不会静默降级：

```bash
cmake -S . -B build-taos \
  -DSFKG_WITH_TAOS=ON \
  -DSFKG_TAOS_ROOT="$HOME/sfkg/tdengine"
cmake --build build-taos -j2
```

运行 Core 前设置 `SFKG_TAOS_HOST`、`SFKG_TAOS_PORT`、`SFKG_TAOS_USER`、
`SFKG_TAOS_PASSWORD` 和 `SFKG_TAOS_DB`；未设置时默认连接本机 6030、
`root/taosdata` 和 `sfkg_timeseries`。Core 启动时创建 `ms` 精度的
`raw_timeseries_data` 超级表；子表名由 sequence ID 的稳定 FNV-1a 哈希生成，
原始 sequence ID 保存在 TAG 中。

## 运行

完整服务启动前需要 TDengine 正常运行。服务器默认监听 `0.0.0.0:50051`，
可通过环境变量或第一个命令行参数修改：

```bash
export SFKG_TIMESERIES_CORE_ADDRESS='0.0.0.0:50051'
export LD_LIBRARY_PATH="$HOME/sfkg/tdengine/lib"
./build-taos/sfkg-timeseries-core-server
```

另开终端运行 gRPC 冒烟客户端：

```bash
./build-taos/sfkg-timeseries-core-smoke localhost:50051
```

客户端依次同步实例、约束、关联、任务和状态，再覆盖全部业务 RPC、四种
`TimeseriesValue` 以及一个非法请求。配置同步应返回 `OK`，尚未实现的业务
应返回 `NOT_IMPLEMENTED`。

## 从全部关闭状态开始运行

下面是 Core 服务器从没有启动任何相关进程时的顺序。两台服务器联调时，前
三步在 `222.29.156.141` 执行，最后一步在 `222.29.156.142` 执行。

### 1. 启动 TDengine

先准备 TDengine 路径：

```bash
export TDENGINE_ROOT="$HOME/sfkg/tdengine"
export PATH="$TDENGINE_ROOT/bin:$PATH"
export LD_LIBRARY_PATH="$TDENGINE_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

如果 TDengine 是系统服务：

```bash
sudo systemctl start taosd
```

如果没有系统服务，可以在 `tmux` 中直接启动：

```bash
tmux new -s tdengine
"$TDENGINE_ROOT/bin/taosd" -c "$TDENGINE_ROOT/cfg"
```

启动后按 `Ctrl-B`、再按 `D` 退出 `tmux`，然后继续下面的步骤。

另开终端检查：

```bash
ss -lntp | grep 6030
```

看到 `6030` 处于 `LISTEN` 后，再继续下一步。

### 2. 编译 Core

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core
export TDENGINE_ROOT="$HOME/sfkg/tdengine"
export LD_LIBRARY_PATH="$TDENGINE_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
cmake -S . -B build-taos \
  -DSFKG_WITH_TAOS=ON \
  -DSFKG_BUILD_GRPC=ON \
  -DSFKG_TAOS_ROOT="$TDENGINE_ROOT"
cmake --build build-taos -j2
```

### 3. 准备演示数据并启动 Core

先导入 ETTh1 数据：

```bash
cd /home/yumiduo/attempt/暑期项目
export TDENGINE_ROOT="$HOME/sfkg/tdengine"
export LD_LIBRARY_PATH="$TDENGINE_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export SFKG_TAOS_DB=sfkg_etth1_demo
./sfkg-timeseries-core/build-taos/etth1-taos-demo ETTh1.csv
```

看到 `ETTh1 demo passed` 后，在同一台服务器启动 Core：

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core
export TDENGINE_ROOT="$HOME/sfkg/tdengine"
export LD_LIBRARY_PATH="$TDENGINE_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export SFKG_TAOS_DB=sfkg_etth1_demo
./build-taos/sfkg-timeseries-core-server 0.0.0.0:50052
```

另开终端检查：

```bash
ss -lntp | grep 50052
```

看到 `LISTEN` 后，Core 已经启动。不要在同一端口重复启动第二个 Core。

### 4. 启动客户端或异常检测模块

在 `222.29.156.142` 上先确认能连接 Core：

```bash
nc -vz 222.29.156.141 50052
```

然后启动客户端或异常检测模块，并将 Core 地址填写为：

```text
222.29.156.141:50052
```

历史查询演示可以这样运行：

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core
cmake -S . -B build-grpc \
  -DSFKG_WITH_TAOS=OFF \
  -DSFKG_BUILD_GRPC=ON
cmake --build build-grpc --target etth1-grpc-history-demo -j2
./build-grpc/etth1-grpc-history-demo 222.29.156.141:50052
```

停止时按相反顺序操作：先停止客户端，再在 Core 终端按 `Ctrl-C`，最后按需
停止 TDengine。系统服务使用 `sudo systemctl stop taosd`；`tmux` 中启动的
TDengine 重新进入会话后按 `Ctrl-C`。

## 两台服务器联调

本次约定：

```text
Core 服务器：222.29.156.141
客户端服务器：222.29.156.142
临时端口：50052
```

### 1. 在 Core 服务器上准备数据

```bash
cd /home/yumiduo/attempt/暑期项目
export LD_LIBRARY_PATH="$HOME/sfkg/tdengine/lib"
export SFKG_TAOS_DB=sfkg_etth1_demo
./sfkg-timeseries-core/build-taos/etth1-taos-demo ETTh1.csv
```

看到 `ETTh1 demo passed` 后，说明 ETTh1 数据已经写入 TDengine。

### 2. 在 Core 服务器上启动服务

```bash
cd /home/yumiduo/attempt/暑期项目/sfkg-timeseries-core
export LD_LIBRARY_PATH="$HOME/sfkg/tdengine/lib"
export SFKG_TAOS_DB=sfkg_etth1_demo
./build-taos/sfkg-timeseries-core-server 0.0.0.0:50052
```

保持这个终端运行。另开终端检查：

```bash
ss -lntp | grep 50052
```

看到 `LISTEN` 后，服务已经启动。

如果希望退出终端后服务继续运行，可以使用下面的方式替代上面的直接启动命令，
不要同时启动两个 Core：

```bash
tmux new -s sfkg-core
./build-taos/sfkg-timeseries-core-server 0.0.0.0:50052
```

按 `Ctrl-B`、再按 `D` 退出 tmux；重新进入使用：

```bash
tmux attach -t sfkg-core
```

### 3. 在客户端服务器上检查端口

在 `222.29.156.142` 上执行：

```bash
nc -vz 222.29.156.141 50052
```

如果检查失败，请联系服务器管理员开通 `222.29.156.141` 的 TCP 50052 端口。

### 4. 运行历史查询演示

在客户端服务器准备同一份代码后执行：

```bash
cmake -S . -B build-grpc \
  -DSFKG_WITH_TAOS=OFF \
  -DSFKG_BUILD_GRPC=ON
cmake --build build-grpc --target etth1-grpc-history-demo -j2
./build-grpc/etth1-grpc-history-demo 222.29.156.141:50052
```

演示程序会先同步 `ETTh1_*` 序列，再调用：

```text
queryHistoryOverview
queryHistoryData
```

异常检测模块使用的服务地址也是：

```text
222.29.156.141:50052
```

如果返回 `NOT_FOUND`，先确认序列配置已经同步；如果返回
`UNAVAILABLE`，先检查 Core 进程和端口。

联调结束后，在 Core 服务器的服务终端按 `Ctrl-C` 停止服务。

## ETT 临时导入演示

`etth1-taos-demo` 是一个临时验证工具：它直接读取上一级项目目录下的
`ETTh1.csv`，绕过 gRPC `WriteRawData`，直接调用 `TaosClient` 批量写入独立的
演示数据库；随后在本地注册 7 个 ETT 序列，并调用
`HistoryQueryService::queryHistoryOverview` 和
`HistoryQueryService::queryHistoryData`，打印摘要和查询结果样例。

sequence ID 使用“文件名_变量名”格式，例如 `ETTh1_HUFL`、`ETTm2_OT`。

在外层项目目录下运行：

```bash
export LD_LIBRARY_PATH="$HOME/sfkg/tdengine/lib"
./sfkg-timeseries-core/build-taos/etth1-taos-demo ETTh1.csv
```

程序预期打印 `121940` 个写入点、7 个序列，并以 `ETTh1 demo passed` 结束。
临时演示库会自动设置较长的保留期，以覆盖 ETTh1 的 2016 年时间戳。

### 跨进程 gRPC 历史查询演示

先启动 ETTh1 临时 Core 服务。它启动时会在本地运行时注册表中预置 7 个
ETTh1 变量，因此后续可以直接查询单个变量：

```bash
cd sfkg-timeseries-core
SFKG_TAOS_DB=sfkg_etth1_demo \
  LD_LIBRARY_PATH="$HOME/sfkg/tdengine/lib" \
  ./build-taos/etth1-temp-core-server 127.0.0.1:50052
```

另开终端运行交互式 gRPC 客户端：

```bash
cd "/home/yumiduo/attempt/暑期项目"
./sfkg-timeseries-core/build-taos/etth1-grpc-history-demo 127.0.0.1:50052
```

客户端会先通过 gRPC 同步 `ETTh1_*` sequence，然后显示菜单：

```text
1. queryHistoryOverview
2. query first hour
3. query latest points
4. query custom [start, end)
q. quit
```

## Git 节点管理

修改应在一个可独立解释和验证的节点完成后提交。推荐节点为：协议或设计
基线、领域结构、通信适配、可运行入口、测试闭环，以及单项真实业务实现。
提交前至少执行适用的编译或测试，并保持工作区不混入生成文件和构建产物。
