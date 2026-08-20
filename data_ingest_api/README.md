# 数据写入接口 — 独立运行版

## 这是什么？

一个**完整独立的小软件包**：别人拿到这个文件夹后，双击一个文件就能启动服务，
之后任何人都能通过网址把「实体 + 关系」数据写入 gStore 图数据库（也能读库）。

- 启动服务、建数据库、写数据、读数据 —— 全部封装在这个文件夹里
- **不依赖 gbuilder2.0 或其他任何文件夹**，拷到哪台电脑都能跑
- 对方不需要懂 SPARQL，会发 HTTP 请求即可（任何语言都可以）

## 文件夹里有什么？

```
数据写入接口/
├── 启动.bat      ← 别人双击这个就能启动（Windows）
├── startup.py    ← 一键启动流程：装依赖 → 连库 → 建库 → 启服务
├── config.ini    ← 配置文件（最重要，用记事本改）
├── api.py        ← HTTP 接口：/insert 写入、/query 读库、/health 健康检查
├── service.py    ← 核心逻辑：连接 gStore、建库、写入、查询
├── models.py     ← 数据格式定义与自动校验
├── dialog.py     ← 弹窗小工具（写入时问库名）
└── README.md     ← 本说明
```

## 一、发给别人之前，你要做的事

**只需要确认一件事：gStore 数据库能被别人的电脑访问到。**

打开 `config.ini`，改 `[gstore]` 段的地址：

```ini
[gstore]
; 数据库在自己电脑上（别人也在同一局域网）→ 填你这台电脑的 IP，如：
host = 192.168.1.100
port = 9999

; 数据库在服务器上 → 填服务器的 IP
```

注意：

- 如果 gStore 跑在你自己的电脑上，**你的电脑要一直开着**，防火墙要放行 9999 端口
- 如果 gStore 跑在服务器上，直接填服务器 IP 即可

## 二、别人拿到文件夹后，怎么用？

1. **装 Python**（只需一次）：去 python.org 下载安装，安装时勾选 "Add Python to PATH"
2. **双击 `启动.bat`**，会看到：
   - 首次运行自动安装三个小依赖库（flask、pydantic、requests）
   - 自动连接 config.ini 里的 gStore
   - 自动检查/创建默认库（如果配置了库名）
   - 服务启动，出现"服务已就绪"
3. 把网址告诉要写数据的人：
   - 写入：`POST http://<这台电脑的IP>:8006/insert`
   - 读库：`POST http://<这台电脑的IP>:8006/query`

关闭黑色窗口即停止服务。

## 三、写入数据（别人怎么用 /insert）

### 数据格式（实体 + 关系 JSON）

```json
{
  "db_name": "my_graph",
  "entities": [
    {
      "name": "主发电机",              // 必填
      "type": "equipment",            // 可选，默认 concept
      "description": "船舶主电源设备",   // 可选
      "properties": {                 // 可选，自定义属性
        "额定功率": "500kW"
      }
    }
  ],
  "relations": [
    {
      "source": "主发电机",            // 必填：起点实体名
      "type": "depends_on",           // 必填：关系类型
      "target": "燃油系统",            // 必填：终点实体名
      "description": "需要燃油供给"      // 可选
    }
  ]
}
```

### 库名怎么定（弹窗逻辑）

- 请求里**带了 `db_name`** → 直接写入那个库，不弹窗（程序自动调用走这条）
- 请求里**没带 `db_name`** → **运行接口的电脑上弹出窗口**：
  - 窗口里列出 gStore 已有的库名做参考
  - 输入已有库名 → 数据直接存入那个库
  - 输入新名字 → 自动创建新库再存入
  - 点取消 → 本次不写入，接口返回提示

### 调用示例

命令行（curl）：

```bash
curl -X POST http://192.168.1.100:8006/insert -H "Content-Type: application/json" -d "{\"db_name\":\"my_graph\",\"entities\":[{\"name\":\"主发电机\",\"type\":\"equipment\"}],\"relations\":[]}"
```

Python（requests）：

```python
import requests
resp = requests.post("http://192.168.1.100:8006/insert", json={
    "db_name": "my_graph",
    "entities": [{"name": "主发电机", "type": "equipment"}],
    "relations": [{"source": "主发电机", "type": "depends_on", "target": "燃油系统"}],
})
print(resp.json())
```

### 返回结果

成功（200）：`{"success": true, "db_name": "...", "entities": 1, "relations": 1, "triples": 7, "message": "写入成功..."}`

失败会返回 `"success": false` 加一句中文原因（400 = 格式不对/弹窗被取消，502 = 连不上 gStore 或写入失败）。

## 四、读取数据（/query）

```python
import requests
resp = requests.post("http://192.168.1.100:8006/query", json={
    "db_name": "my_graph",   # 可选，留空用默认库
    "sparql": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
})
print(resp.json())  # 数据在 results.bindings 里
```

## 五、读取时序服务业务记录（/records）

时序服务通过 `/records` 按表名读取之前通过 `/insert` 写入的实体，接口会把
gStore 的 SPARQL 绑定结果转换为 JSON 记录，调用方不需要自己拼接 SPARQL。

请求：

```json
{
  "db_name": "ett_system_project-a",
  "table_name": "timeseries_category"
}
```

返回：

```json
{
  "success": true,
  "db_name": "ett_system_project-a",
  "table_name": "timeseries_category",
  "records": [
    {
      "business_key": "cat001",
      "record": {
        "categoryId": "cat001",
        "categoryName": "Temperature"
      }
    }
  ]
}
```

`record` 中不包含顶层 `projectId`。时序服务根据当前查询的项目目录记录，在恢复内存时补回该字段。
Java 时序服务写入的元字段名是 `tableName`、`businessKey`、`recordJson`；
为了兼容早期 Python 测试数据，接口也支持读取 `field_tableName`、
`field_businessKey`、`field_recordJson`。

## 六、写入后的数据长什么样（约定）

与 gbuilder2.0 管线一致，读数据的代码能直接读：

| 内容 | 写成 |
|------|------|
| 实体 | `<http://gbuilder.org/knowledge/entity/主发电机>` |
| 类型 | `... has_type "equipment"` |
| 描述 | `... has_description "..."` |
| 自定义属性 | `... prop/额定功率 "500kW"` |
| 关系 | `... relation/depends_on ...` |

## 七、常见问题

1. **启动时提示"连不上 gStore"** → 用记事本打开 config.ini，检查 host/port 是否填对；
   gStore 在别的电脑上时，确认那台电脑开着、防火墙放行。
2. **双击 bat 提示没有 Python** → 装 Python 时没勾 "Add Python to PATH"，重装勾上即可
   （本 bat 会自动尝试 py 启动器，一般能自动找到 Python）。
3. **提示"Python 是微软商店的占位版本"** → 从 python.org 官网装正式版即可（商店版跑不了）。
4. **依赖自动安装失败** → 程序会给出完整的手动安装命令（已默认用清华镜像源，国内网络快），
   复制到命令行运行即可；网络不通时换个能上网的环境装好再拷回来。
5. **重复提交同一批数据** → 图数据库自动去重，放心重发。
6. **8006 端口被占用** → 改 config.ini 里 [service] 段的 port，再重新启动。
