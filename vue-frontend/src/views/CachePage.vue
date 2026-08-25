<script setup>
import { ref } from 'vue'
import { api } from '../api/timeseries'
import { toastError } from '../composables/toast'
import ResultViewer from '../components/ResultViewer.vue'

const loading = ref(false)
const response = ref(null)
const error = ref('')
const tableName = ref('')

const TABLES = [
  'instance_config',
  'category',
  'constraint',
  'relation',
  'anomaly_task',
  'forecast_task',
  'event',
  'window_config',
  'derived_series',
  'statistics',
]

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

async function warmUp() {
  await run(api.cacheWarmUp())
}

async function refreshTable() {
  if (!tableName.value) {
    toastError('请先输入表名')
    return
  }
  await run(api.cacheRefreshTable(tableName.value))
}
</script>

<template>
  <div>
    <div class="page-title"><h2>缓存管理</h2></div>

    <div class="card">
      <h3>全量预热 (POST /api/timeseries/cache/warm-up)</h3>
      <p style="color: #7b8794">从本地 JSON 文件重新加载全部表到内存缓存，并同步配置到 Core。</p>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="warmUp">执行预热</button>
      </div>
    </div>

    <div class="card">
      <h3>单表刷新 (POST /api/timeseries/cache/tables/{tableName}/refresh)</h3>
      <div class="grid">
        <div class="field">
          <label>表名</label>
          <select v-model="tableName">
            <option value=""></option>
            <option v-for="t in TABLES" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="refreshTable">刷新表</button>
      </div>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
