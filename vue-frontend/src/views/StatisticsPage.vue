<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import RefInput from '../components/RefInput.vue'
import ResultViewer from '../components/ResultViewer.vue'
import { toastError } from '../composables/toast'
import { useRefOptions } from '../composables/refOptions'
import { projectContext, withCurrentProject } from '../stores/project'

const { refOptions, loadTypes } = useRefOptions()
loadTypes(['instance', 'relation'])

const seqField = { name: 'sequenceIds', placeholder: '输入后回车/逗号或选择候选，可多选' }

const loading = ref(false)
const response = ref(null)
const error = ref('')
const rows = ref([])

const form = reactive({
  projectId: projectContext.currentProjectId.value,
  sequenceIds: '',
  dependentSequenceId: '',
  relationIds: '',
  startTime: '',
  endTime: '',
})

function parseIds(str) {
  return String(str || '')
    .split(/[\s,，;]+/)
    .map((s) => s.trim())
    .filter(Boolean)
}

function dt(v) {
  return v && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(v) ? v + ':00' : v || undefined
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

async function compute() {
  const payload = withCurrentProject({ projectId: form.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  const seqIds = parseIds(form.sequenceIds)
  if (seqIds.length) payload.sequenceIds = seqIds
  if (form.dependentSequenceId) payload.dependentSequenceId = form.dependentSequenceId
  const relIds = parseIds(form.relationIds)
  if (relIds.length) payload.relationIds = relIds
  const s = dt(form.startTime)
  const e = dt(form.endTime)
  if (s) payload.startTime = s
  if (e) payload.endTime = e
  await run(api.computeStatistics(payload), false)
}

async function loadAll() {
  const projectId = projectContext.currentProjectId.value
  if (!projectId) {
    rows.value = []
    return
  }
  await run(api.listStatistics(projectId), true)
}

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    form.projectId = projectId
    rows.value = []
    loadTypes(['instance', 'relation'])
  },
)
</script>

<template>
  <div>
    <div class="page-title">
      <h2>相关性统计</h2>
      <div>
        <button :disabled="loading" @click="loadTypes(['instance', 'relation'])">刷新下拉选项</button>
        <button class="primary" style="margin-left: 8px" :disabled="loading" @click="loadAll">查询全部统计结果</button>
      </div>
    </div>

    <div class="card">
      <h3>计算统计 (POST /api/timeseries/statistics/compute)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="form.projectId" placeholder="留空 = 默认项目" /></div>
        <div class="field full">
          <label>序列ID（需与依赖序列同设备以触发相关性展开）</label>
          <RefInput :field="seqField" :options="refOptions.instance || []" multiple v-model="form.sequenceIds" />
        </div>
        <div class="field">
          <label>依赖序列ID</label>
          <RefInput :field="{ name: 'dependentSequenceId', placeholder: '按子串匹配选择实例' }" :options="refOptions.instance || []" v-model="form.dependentSequenceId" />
        </div>
        <div class="field full">
          <label>关系ID（用于相关性计算，可留空）</label>
          <RefInput :field="{ name: 'relationIds', placeholder: '输入后回车/逗号或选择候选，可多选' }" :options="refOptions.relation || []" multiple v-model="form.relationIds" />
        </div>
        <div class="field"><label>开始时间</label><input type="datetime-local" v-model="form.startTime" /></div>
        <div class="field"><label>结束时间</label><input type="datetime-local" v-model="form.endTime" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="compute">计算</button>
      </div>
    </div>

    <div v-if="rows.length" class="card">
      <h3>统计结果（共 {{ rows.length }} 条）</h3>
      <table>
        <thead>
          <tr>
            <th>统计ID</th><th>项目ID</th><th>序列</th><th>依赖序列</th>
            <th>关系ID</th><th>开始时间</th><th>结束时间</th><th>指标</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(r, i) in rows" :key="i">
            <td>{{ r.statisticsId }}</td>
            <td>{{ r.projectId }}</td>
            <td>{{ Array.isArray(r.sequenceIds) ? r.sequenceIds.join(', ') : r.sequenceId }}</td>
            <td>{{ r.dependentSequenceId }}</td>
            <td>{{ Array.isArray(r.relationIds) ? r.relationIds.join(', ') : r.relationId }}</td>
            <td>{{ r.startTime }}</td>
            <td>{{ r.endTime }}</td>
            <td>{{ typeof r.metrics === 'object' ? JSON.stringify(r.metrics) : r.metrics }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
