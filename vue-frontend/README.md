# Timeseries 管理系统前端（Vue 3）

sfkg-timeseries-service 的管理前端，覆盖**全部配置输入**与请求发送，通过 HTTP 调用 Java 后端 REST 接口。

## 技术栈

- Vue 3 + Vite 5 + vue-router 4（hash 路由）
- axios（后端统一返回 `ApiResult<T> = { success, message, data }`）

页面顶部提供项目列表和创建项目入口。选择当前项目后，CRUD 列表、引用下拉和自定义数据/任务请求都会自动携带该项目的 `projectId`，切换项目会清空并重新加载当前页面数据。

## 启动

```bash
cd vue-frontend
npm install
npm run dev
```

浏览器访问 http://localhost:5173

开发模式下 Vite 将 `/api` 代理转发到 Java 后端 `http://localhost:8080`（可用环境变量 `BACKEND_URL` 覆盖）：

```bash
BACKEND_URL=http://192.168.1.10:8080 npm run dev
```

## 页面与后端接口对照

| 页面 | 路由 | 后端接口 |
|---|---|---|
| 实例配置 | `/instances` | `/api/timeseries/instances` GET/POST/PUT + `/query` |
| 语义类别 | `/categories` | `/api/timeseries/semantic/categories` GET/POST/PUT + `/query` + PATCH `/status` |
| 语义约束 | `/constraints` | `/api/timeseries/semantic/constraints` GET/POST/PUT + `/query` + PATCH `/status` |
| 语义关系 | `/relations` | `/api/timeseries/semantic/relations` GET/POST/PUT + `/query` + PATCH `/status` |
| 数据管理 | `/data` | `/data/ingest`、`/data/history/query`、`/data/history/overview`、`/data/window/query` |
| 窗口配置 | `/window-config` | `/api/timeseries/window-config` POST |
| 派生序列 | `/derived-series` | `/api/timeseries/derived-series` POST（线性组合 / 表达式 / 原始 JSON） |
| 异常检测任务 | `/anomaly-tasks` | `/api/timeseries/anomaly-tasks` GET/POST/PUT + `/query` + PATCH `/status` |
| 预测任务 | `/forecast-tasks` | `/api/timeseries/forecast-tasks` GET/POST/PUT + `/query` + PATCH `/status` |
| 事件管理 | `/events` | `/api/timeseries/events` GET/POST/PUT + `/query` + GET `/{eventId}` |
| 结果查询 | `/results` | `/anomaly-results`、`/forecast-results` GET + `/query` |
| 相关性统计 | `/statistics` | `/api/timeseries/statistics/compute` POST + GET |
| 决策辅助 | `/decision` | `/decision/diagnosis`、`/decision/suggestion` POST + PATCH `/feedback` |
| 缓存管理 | `/cache` | `/cache/warm-up`、`/cache/tables/{tableName}/refresh` POST |

## 通用 CRUD 页面能力

实例/类别/约束/关系/任务/事件等页面由 `src/config/pages.js` 声明式驱动 `CrudPage.vue`：

- 表单创建（POST）/ 更新（PUT），点击表格行回填表单
- 条件查询（POST `/query`）与查询全部（GET）
- 状态更新（PATCH `/status`，如任务启停、约束确认状态）
- 任意页面可切换 **JSON 模式**：直接粘贴原始 JSON 发送创建/更新请求

## 字段说明

- `projectId` 每个表单第一位，留空 = 默认项目（多租户支持）
- 列表字段（`sequenceIds`、`methods`、`constraintIds` 等）用逗号/空格分隔输入
- JSON 字段（`variableMapping`、`terms`、points 等）粘贴 JSON
- 时间字段为 `datetime-local`，自动补秒以匹配后端 `LocalDateTime`
