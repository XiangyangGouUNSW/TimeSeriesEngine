<script setup>
import { reactive, ref, watch } from 'vue'
import * as XLSX from 'xlsx'
import { api } from '../api/timeseries'
import RefInput from '../components/RefInput.vue'
import { toastError, toastSuccess } from '../composables/toast'
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
    return res
  } catch (e) {
    error.value = e.message || String(e)
    return null
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
  const res = await run(api.ingestData(payload))
  if (res && res.success !== false) toastSuccess((res && res.message) || '数据写入成功')
}

// ── 文件导入（CSV / XLSX） ─────────────────────────────────
const importOpts = reactive({
  prefix: 'ETTh1_',
  timeColumn: 0,
})
const importPreview = ref(null)
const importedPoints = ref([])

async function onFileSelected(e) {
  const file = e.target.files && e.target.files[0]
  e.target.value = ''
  if (!file) return
  try {
    const rows = await parseImportFile(file)
    const result = buildImportPoints(rows, file.name)
    importedPoints.value = result.points
    importPreview.value = result
    if (!result.points.length) {
      toastError('未解析出任何点位，请检查时间列与表头列名')
    }
  } catch (err) {
    importedPoints.value = []
    importPreview.value = null
    toastError('文件解析失败：' + (err && err.message ? err.message : err))
  }
}

function parseImportFile(file) {
  const ext = (file.name.split('.').pop() || '').toLowerCase()
  if (ext === 'csv') {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      reader.onload = () => {
        const text = String(reader.result || '')
        const lines = text.split(/\r?\n/).filter((line) => line.trim() !== '')
        resolve(lines.map(parseCsvLine))
      }
      reader.onerror = () => reject(reader.error || new Error('读取失败'))
      reader.readAsText(file, 'utf-8')
    })
  }
  if (ext === 'xlsx' || ext === 'xls') {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      reader.onload = () => {
        try {
          const workbook = XLSX.read(new Uint8Array(reader.result), { type: 'array' })
          const sheet = workbook.Sheets[workbook.SheetNames[0]]
          resolve(XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' }))
        } catch (err) {
          reject(err)
        }
      }
      reader.onerror = () => reject(reader.error || new Error('读取失败'))
      reader.readAsArrayBuffer(file)
    })
  }
  throw new Error('仅支持 .csv / .xlsx / .xls 文件')
}

function parseCsvLine(line) {
  const cells = []
  let current = ''
  let inQuotes = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"'
        i++
      } else {
        inQuotes = !inQuotes
      }
    } else if (ch === ',' && !inQuotes) {
      cells.push(current)
      current = ''
    } else {
      current += ch
    }
  }
  cells.push(current)
  return cells
}

function parseTimeMs(raw) {
  const s = String(raw ?? '').trim()
  if (!s) return null
  if (/^\d+$/.test(s)) {
    const n = Number(s)
    return s.length <= 10 ? n * 1000 : n // 秒级时间戳 → 毫秒
  }
  const ms = new Date(s.replace(' ', 'T')).getTime()
  return Number.isNaN(ms) ? null : ms
}

function buildImportPoints(rows, fileName) {
  if (!rows || rows.length < 2) {
    throw new Error('至少需要表头 + 一行数据')
  }
  const timeIdx = Math.max(0, Number(importOpts.timeColumn) || 0)
  const prefix = importOpts.prefix.trim()
  const headers = rows[0].map((h) => String(h ?? '').trim())
  const valueCols = headers
    .map((header, index) => ({ header, index }))
    .filter(({ header, index }) => index !== timeIdx && header !== '')
  if (!valueCols.length) {
    throw new Error('未找到序列列：请确认第一行为表头且时间列之外还有序列列')
  }
  const points = []
  let skipped = 0
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r]
    if (!row) continue
    const timeMs = parseTimeMs(row[timeIdx])
    if (timeMs === null) {
      skipped++
      continue
    }
    for (const { header, index } of valueCols) {
      const raw = row[index]
      if (raw === '' || raw === undefined || raw === null) continue
      const point = { sequenceId: prefix + header, dataSourceId: fileName, time: timeMs }
      const text = String(raw).trim()
      const num = Number(text)
      if (text !== '' && !Number.isNaN(num)) point.doubleValue = num
      else point.stringValue = text
      points.push(point)
    }
  }
  return {
    fileName,
    valueCols: valueCols.map((c) => prefix + c.header),
    dataRows: rows.length - 1,
    points,
    skipped,
  }
}

async function doImport() {
  if (!importedPoints.value.length) {
    toastError('请先选择并解析文件')
    return
  }
  const payload = withCurrentProject({ points: importedPoints.value })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  const res = await run(api.ingestData(payload))
  if (res && res.success !== false) toastSuccess((res && res.message) || '文件数据导入成功')
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
        <div class="field full">
          <label>从文件导入（.csv / .xlsx / .xls）</label>
          <div class="import-row">
            <input type="file" accept=".csv,.xlsx,.xls" @change="onFileSelected" />
            <input v-model="importOpts.prefix" placeholder="序列ID前缀，如 ETTh1_" title="序列ID = 前缀 + 列名" />
            <label class="import-time-col">时间列
              <input v-model.number="importOpts.timeColumn" type="number" min="0" />
            </label>
          </div>
          <small class="t-hint">
            第一行为表头；时间列默认第 0 列（支持 yyyy-MM-dd HH:mm:ss 或秒/毫秒时间戳）；
            其余列按「序列ID = 前缀 + 列名」生成点位，数值写 doubleValue，非数值写 stringValue。
          </small>
          <div v-if="importPreview" class="import-preview">
            <span class="badge ok">已解析 {{ importPreview.fileName }}</span>
            <span class="import-meta">
              {{ importPreview.valueCols.length }} 个序列列 ｜ {{ importPreview.dataRows }} 行数据 →
              {{ importPreview.points.length }} 条点位（跳过 {{ importPreview.skipped }} 行）
            </span>
          </div>
        </div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doIngest">写入数据</button>
        <button :disabled="loading || !importedPoints.length" @click="doImport">
          导入文件数据（{{ importedPoints.length }} 点）
        </button>
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
