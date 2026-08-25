<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import ResultViewer from '../components/ResultViewer.vue'
import { toastError } from '../composables/toast'
import { projectContext, withCurrentProject } from '../stores/project'

const tab = ref('anomaly')
const loading = ref(false)
const response = ref(null)
const error = ref('')
const rows = ref([])

const anomaly = reactive({
  projectId: projectContext.currentProjectId.value,
  taskId: '',
  sequenceId: '',
  startTime: '',
  endTime: '',
  eventLevel: '',
})

const forecast = reactive({
  projectId: projectContext.currentProjectId.value,
  taskId: '',
  sequenceId: '',
  startTime: '',
  endTime: '',
})

function dt(v) {
  return v && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(v) ? v + ':00' : v || undefined
}

function buildPayload(src, extra) {
  const payload = withCurrentProject({ projectId: src.projectId || undefined })
  if (!payload) return null
  if (src.taskId) payload.taskId = src.taskId
  if (src.sequenceId) payload.sequenceId = src.sequenceId
  const s = dt(src.startTime)
  const e = dt(src.endTime)
  if (s) payload.startTime = s
  if (e) payload.endTime = e
  Object.assign(payload, extra)
  return payload
}

async function run(promise, collect) {
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await promise
    response.value = res
    if (res && res.success === false) error.value = res.message || '请求失败'
    if (collect && res && res.data) rows.value = res.data
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

async function queryAnomaly() {
  const extra = {}
  if (anomaly.eventLevel) extra.eventLevel = anomaly.eventLevel
  const payload = buildPayload(anomaly, extra)
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  await run(api.queryAnomalyResults(payload), true)
}

async function queryForecast() {
  const payload = buildPayload(forecast, {})
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  await run(api.queryForecastResults(payload), true)
}

async function listAll() {
  rows.value = []
  const projectId = projectContext.currentProjectId.value
  if (!projectId) return
  if (tab.value === 'anomaly') await run(api.listAnomalyResults(projectId), true)
  else await run(api.listForecastResults(projectId), true)
}

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    anomaly.projectId = projectId
    forecast.projectId = projectId
    rows.value = []
  },
)
</script>

<template>
  <div>
    <div class="page-title">
      <h2>结果查询</h2>
      <button class="primary" :disabled="loading" @click="listAll">查询全部</button>
    </div>

    <div class="card">
      <div class="actions" style="margin-top: 0; margin-bottom: 14px">
        <button :class="{ primary: tab === 'anomaly' }" @click="tab = 'anomaly'">异常检测结果</button>
        <button :class="{ primary: tab === 'forecast' }" @click="tab = 'forecast'">预测结果</button>
      </div>

      <template v-if="tab === 'anomaly'">
        <h3>异常结果查询 (POST /api/timeseries/anomaly-results/query)</h3>
        <div class="grid">
          <div class="field"><label>项目ID</label><input v-model="anomaly.projectId" /></div>
          <div class="field"><label>任务ID</label><input v-model="anomaly.taskId" /></div>
          <div class="field"><label>序列ID</label><input v-model="anomaly.sequenceId" /></div>
          <div class="field"><label>开始时间</label><input type="datetime-local" v-model="anomaly.startTime" /></div>
          <div class="field"><label>结束时间</label><input type="datetime-local" v-model="anomaly.endTime" /></div>
          <div class="field">
            <label>事件级别</label>
            <select v-model="anomaly.eventLevel">
              <option value=""></option>
              <option>LOW</option><option>MEDIUM</option><option>HIGH</option>
            </select>
          </div>
        </div>
        <div class="actions"><button class="primary" :disabled="loading" @click="queryAnomaly">查询</button></div>

        <div v-if="rows.length" class="card" style="box-shadow: none; margin-bottom: 0">
          <h3>异常结果（{{ rows.length }} 条）</h3>
          <table>
            <thead>
              <tr>
                <th>结果ID</th><th>项目ID</th><th>任务ID</th><th>序列</th>
                <th>级别</th><th>事件类型</th><th>事件时间</th><th>来源</th><th>约束</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(r, i) in rows" :key="i">
                <td>{{ r.resultId }}</td><td>{{ r.projectId }}</td><td>{{ r.taskId }}</td>
                <td>{{ Array.isArray(r.sequenceIds) ? r.sequenceIds.join(', ') : r.sequenceId }}</td>
                <td>{{ r.anomalyLevel }}</td><td>{{ r.eventType }}</td><td>{{ r.eventTime }}</td>
                <td>{{ r.source }}</td><td>{{ Array.isArray(r.constraintIds) ? r.constraintIds.join(', ') : '' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>

      <template v-else>
        <h3>预测结果查询 (POST /api/timeseries/forecast-results/query)</h3>
        <div class="grid">
          <div class="field"><label>项目ID</label><input v-model="forecast.projectId" /></div>
          <div class="field"><label>任务ID</label><input v-model="forecast.taskId" /></div>
          <div class="field"><label>序列ID</label><input v-model="forecast.sequenceId" /></div>
          <div class="field"><label>开始时间</label><input type="datetime-local" v-model="forecast.startTime" /></div>
          <div class="field"><label>结束时间</label><input type="datetime-local" v-model="forecast.endTime" /></div>
        </div>
        <div class="actions"><button class="primary" :disabled="loading" @click="queryForecast">查询</button></div>

        <div v-if="rows.length" class="card" style="box-shadow: none; margin-bottom: 0">
          <h3>预测结果（{{ rows.length }} 条）</h3>
          <table>
            <thead>
              <tr>
                <th>结果ID</th><th>项目ID</th><th>任务ID</th><th>序列</th><th>预警级别</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(r, i) in rows" :key="i">
                <td>{{ r.resultId }}</td><td>{{ r.projectId }}</td><td>{{ r.taskId }}</td>
                <td>{{ r.sequenceId }}</td><td>{{ r.warningLevel }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
