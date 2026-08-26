import http from './http'

// 所有接口封装，返回 Promise<ApiResult<T>>
export const api = {
  // ── 登录鉴权 ─────────────────────────────────────────────
  login: (d) => http.post('/api/auth/login', d),
  currentUser: () => http.get('/api/auth/me'),
  csrfToken: () => http.get('/api/auth/csrf'),
  logout: () => http.post('/api/auth/logout'),

  // ── 项目 ─────────────────────────────────────────────────
  createProject: (d) => http.post('/api/timeseries/projects', d),
  listProjects: () => http.get('/api/timeseries/projects'),

  // ── 实例配置 ──────────────────────────────────────────────
  createInstance: (d) => http.post('/api/timeseries/instances', d),
  updateInstance: (d) => http.put('/api/timeseries/instances', d),
  listInstances: () => http.get('/api/timeseries/instances'),
  queryInstances: (d) => http.post('/api/timeseries/instances/query', d),

  // ── 语义类别 ──────────────────────────────────────────────
  listCategories: () => http.get('/api/timeseries/semantic/categories'),
  queryCategories: (d) => http.post('/api/timeseries/semantic/categories/query', d),
  createCategory: (d) => http.post('/api/timeseries/semantic/categories', d),
  updateCategory: (d) => http.put('/api/timeseries/semantic/categories', d),
  updateCategoryStatus: (d) => http.patch('/api/timeseries/semantic/categories/status', d),

  // ── 语义约束 ──────────────────────────────────────────────
  listConstraints: () => http.get('/api/timeseries/semantic/constraints'),
  queryConstraints: (d) => http.post('/api/timeseries/semantic/constraints/query', d),
  createConstraint: (d) => http.post('/api/timeseries/semantic/constraints', d),
  updateConstraint: (d) => http.put('/api/timeseries/semantic/constraints', d),
  updateConstraintStatus: (d) => http.patch('/api/timeseries/semantic/constraints/status', d),

  // ── 语义关系 ──────────────────────────────────────────────
  listRelations: () => http.get('/api/timeseries/semantic/relations'),
  queryRelations: (d) => http.post('/api/timeseries/semantic/relations/query', d),
  createRelation: (d) => http.post('/api/timeseries/semantic/relations', d),
  updateRelation: (d) => http.put('/api/timeseries/semantic/relations', d),
  updateRelationStatus: (d) => http.patch('/api/timeseries/semantic/relations/status', d),

  // ── 数据管理 ──────────────────────────────────────────────
  ingestData: (d) => http.post('/api/timeseries/data/ingest', d),
  savePoints: (d) => http.post('/api/timeseries/data/points', d),
  queryHistory: (d) => http.post('/api/timeseries/data/history/query', d),
  queryHistoryOverview: (d) => http.post('/api/timeseries/data/history/overview', d),
  queryWindow: (d) => http.post('/api/timeseries/data/window/query', d),

  // ── 窗口配置 ──────────────────────────────────────────────
  syncWindowConfig: (d) => http.post('/api/timeseries/window-config', d),

  // ── 派生序列 ──────────────────────────────────────────────
  syncDerivedSeries: (d) => http.post('/api/timeseries/derived-series', d),

  // ── 异常检测任务 ──────────────────────────────────────────
  createAnomalyTask: (d) => http.post('/api/timeseries/anomaly-tasks', d),
  updateAnomalyTask: (d) => http.put('/api/timeseries/anomaly-tasks', d),
  listAnomalyTasks: () => http.get('/api/timeseries/anomaly-tasks'),
  queryAnomalyTasks: (d) => http.post('/api/timeseries/anomaly-tasks/query', d),
  updateAnomalyTaskStatus: (d) => http.patch('/api/timeseries/anomaly-tasks/status', d),

  // ── 预测任务 ──────────────────────────────────────────────
  createForecastTask: (d) => http.post('/api/timeseries/forecast-tasks', d),
  updateForecastTask: (d) => http.put('/api/timeseries/forecast-tasks', d),
  listForecastTasks: () => http.get('/api/timeseries/forecast-tasks'),
  queryForecastTasks: (d) => http.post('/api/timeseries/forecast-tasks/query', d),
  updateForecastTaskStatus: (d) => http.patch('/api/timeseries/forecast-tasks/status', d),

  // ── 事件管理 ──────────────────────────────────────────────
  listEvents: () => http.get('/api/timeseries/events'),
  queryEvents: (d) => http.post('/api/timeseries/events/query', d),
  getEventDetail: (eventId) => http.get(`/api/timeseries/events/${encodeURIComponent(eventId)}`),
  postEventDetail: (d) => http.post('/api/timeseries/events/detail', d),
  createEvent: (d) => http.post('/api/timeseries/events', d),
  updateEvent: (d) => http.put('/api/timeseries/events', d),

  // ── 结果查询 ──────────────────────────────────────────────
  listAnomalyResults: (projectId) => http.get('/api/timeseries/anomaly-results', { params: { projectId } }),
  queryAnomalyResults: (d) => http.post('/api/timeseries/anomaly-results/query', d),
  listForecastResults: (projectId) => http.get('/api/timeseries/forecast-results', { params: { projectId } }),
  queryForecastResults: (d) => http.post('/api/timeseries/forecast-results/query', d),

  // ── 决策辅助 ──────────────────────────────────────────────
  diagnosis: (d) => http.post('/api/timeseries/decision/diagnosis', d),
  suggestion: (d) => http.post('/api/timeseries/decision/suggestion', d),
  feedback: (d) => http.patch('/api/timeseries/decision/feedback', d),

  // ── 相关性统计 ────────────────────────────────────────────
  computeStatistics: (d) => http.post('/api/timeseries/statistics/compute', d),
  listStatistics: (projectId) => http.get('/api/timeseries/statistics', { params: { projectId } }),

  // ── 缓存管理 ──────────────────────────────────────────────
  cacheWarmUp: () => http.post('/api/timeseries/cache/warm-up'),
  cacheRefreshTable: (tableName) =>
    http.post(`/api/timeseries/cache/tables/${encodeURIComponent(tableName)}/refresh`),
}
