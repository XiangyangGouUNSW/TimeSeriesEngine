<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import RefInput from '../components/RefInput.vue'
import { toastError } from '../composables/toast'
import { useRefOptions } from '../composables/refOptions'
import ResultViewer from '../components/ResultViewer.vue'
import { projectContext, withCurrentProject } from '../stores/project'

const { refOptions, loadTypes } = useRefOptions()
loadTypes(['instance'])

const seqSingleField = { name: 'sequenceId', placeholder: '按子串匹配选择实例' }
const seqMultiField = { name: 'sequenceIds', placeholder: '输入后回车/逗号或选择候选，可多选' }

const loading = ref(false)
const response = ref(null)
const error = ref('')

async function run(promise) {
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await promise
    response.value = res
    if (res && res.success === false) error.value = res.message || '请求失败'
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

function parseIds(str) {
  return String(str || '')
    .split(/[\s,，;]+/)
    .map((s) => s.trim())
    .filter(Boolean)
}

function dt(v) {
  return v && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(v) ? v + ':00' : v || undefined
}

// ── 数据写入 ──────────────────────────────────────────────
const ingest = reactive({
  projectId: projectContext.currentProjectId.value,
  returnResolvedData: 'true',
  pointsJson: '[\n  {\n    "sequenceId": "ETTh1_HUFL",\n    "time": "2024-01-01T00:00:00",\n    "doubleValue": 5.827\n  }\n]',
})

async function doIngest() {
  let points
  try {
    points = JSON.parse(ingest.pointsJson)
  } catch (e) {
    toastError('points JSON 解析失败：' + e.message)
    return
  }
  const payload = withCurrentProject({ points, projectId: ingest.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  if (ingest.returnResolvedData) payload.returnResolvedData = ingest.returnResolvedData === 'true'
  await run(api.ingestData(payload))
}

// ── 历史数据查询 ──────────────────────────────────────────
const history = reactive({
  projectId: projectContext.currentProjectId.value,
  sequenceId: '',
  sequenceIds: '',
  startTime: '',
  endTime: '',
  granularity: '',
})

async function doHistoryQuery() {
  const payload = withCurrentProject({ projectId: history.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  if (history.sequenceId) payload.sequenceId = history.sequenceId
  const ids = parseIds(history.sequenceIds)
  if (ids.length) payload.sequenceIds = ids
  const s = dt(history.startTime)
  const e = dt(history.endTime)
  if (s) payload.startTime = s
  if (e) payload.endTime = e
  if (history.granularity) payload.granularity = history.granularity
  await run(api.queryHistory(payload))
}

// ── 历史数据概览 ──────────────────────────────────────────
const overview = reactive({ projectId: projectContext.currentProjectId.value, sequenceIds: '', startTime: '', endTime: '' })

async function doOverview() {
  const payload = withCurrentProject({ projectId: overview.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  const ids = parseIds(overview.sequenceIds)
  if (ids.length) payload.sequenceIds = ids
  const s = dt(overview.startTime)
  const e = dt(overview.endTime)
  if (s) payload.startTime = s
  if (e) payload.endTime = e
  await run(api.queryHistoryOverview(payload))
}

// ── 窗口数据查询 ──────────────────────────────────────────
const windowQ = reactive({ projectId: projectContext.currentProjectId.value, sequenceIds: '', startTime: '', endTime: '' })

async function doWindowQuery() {
  const payload = withCurrentProject({ projectId: windowQ.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  const ids = parseIds(windowQ.sequenceIds)
  if (ids.length) payload.sequenceIds = ids
  const s = dt(windowQ.startTime)
  const e = dt(windowQ.endTime)
  if (s) payload.startTime = s
  if (e) payload.endTime = e
  await run(api.queryWindow(payload))
}

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    ingest.projectId = projectId
    history.projectId = projectId
    overview.projectId = projectId
    windowQ.projectId = projectId
    response.value = null
    loadTypes(['instance'])
  },
)
</script>

<template>
  <div>
    <div class="page-title">
      <h2>数据管理</h2>
      <button :disabled="loading" @click="loadTypes(['instance'])">刷新下拉选项</button>
    </div>

    <div class="card">
      <h3>数据写入 (POST /api/timeseries/data/ingest)</h3>
      <div class="grid">
        <div class="field">
          <label>项目ID</label>
          <input v-model="ingest.projectId" placeholder="留空 = 默认项目" />
        </div>
        <div class="field">
          <label>返回解析数据</label>
          <select v-model="ingest.returnResolvedData">
            <option value="true">true</option>
            <option value="false">false</option>
          </select>
        </div>
        <div class="field full">
          <label>points（JSON 数组，time 为 ISO 时间，值字段 doubleValue/int64Value/boolValue/stringValue）</label>
          <textarea v-model="ingest.pointsJson" rows="10"></textarea>
        </div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doIngest">写入数据</button>
      </div>
    </div>

    <div class="card">
      <h3>历史数据查询 (POST /api/timeseries/data/history/query)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="history.projectId" /></div>
        <div class="field">
          <label>序列ID（单个）</label>
          <RefInput :field="seqSingleField" :options="refOptions.instance || []" v-model="history.sequenceId" />
        </div>
        <div class="field full">
          <label>序列ID列表（与单个二选一）</label>
          <RefInput :field="seqMultiField" :options="refOptions.instance || []" multiple v-model="history.sequenceIds" />
        </div>
        <div class="field"><label>开始时间</label><input type="datetime-local" v-model="history.startTime" /></div>
        <div class="field"><label>结束时间</label><input type="datetime-local" v-model="history.endTime" /></div>
        <div class="field"><label>粒度 granularity</label><input v-model="history.granularity" placeholder="如 1m / 1h / 1d" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doHistoryQuery">查询历史数据</button>
      </div>
    </div>

    <div class="card">
      <h3>历史数据概览 (POST /api/timeseries/data/history/overview)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="overview.projectId" /></div>
        <div class="field full">
          <label>序列ID（可多选）</label>
          <RefInput :field="seqMultiField" :options="refOptions.instance || []" multiple v-model="overview.sequenceIds" />
        </div>
        <div class="field"><label>开始时间</label><input type="datetime-local" v-model="overview.startTime" /></div>
        <div class="field"><label>结束时间</label><input type="datetime-local" v-model="overview.endTime" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doOverview">查询概览</button>
      </div>
    </div>

    <div class="card">
      <h3>窗口数据查询 (POST /api/timeseries/data/window/query)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="windowQ.projectId" /></div>
        <div class="field full">
          <label>序列ID（可多选）</label>
          <RefInput :field="seqMultiField" :options="refOptions.instance || []" multiple v-model="windowQ.sequenceIds" />
        </div>
        <div class="field"><label>开始时间</label><input type="datetime-local" v-model="windowQ.startTime" /></div>
        <div class="field"><label>结束时间</label><input type="datetime-local" v-model="windowQ.endTime" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doWindowQuery">查询窗口数据</button>
      </div>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
