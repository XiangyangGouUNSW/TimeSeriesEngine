<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import { toastError, toastSuccess } from '../composables/toast'
import ResultViewer from '../components/ResultViewer.vue'
import { projectContext, withCurrentProject } from '../stores/project'

const form = reactive({ projectId: projectContext.currentProjectId.value, windowSizeMs: '' })
const loading = ref(false)
const response = ref(null)
const error = ref('')

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    form.projectId = projectId
    response.value = null
  },
)

async function submit() {
  if (form.windowSizeMs === '' || form.windowSizeMs === null) {
    toastError('请填写必填字段：窗口大小 windowSizeMs（毫秒）')
    return
  }
  const payload = withCurrentProject({ projectId: form.projectId || undefined })
  if (!payload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  if (form.windowSizeMs !== '' && form.windowSizeMs !== null) {
    payload.windowSizeMs = Number(form.windowSizeMs)
    if (Number.isNaN(payload.windowSizeMs)) {
      toastError('windowSizeMs 必须为数字（毫秒）')
      return
    }
  }
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await api.syncWindowConfig(payload)
    response.value = res
    if (res && res.success === false) error.value = res.message || '请求失败'
    else if (res) toastSuccess((res && res.message) || '窗口配置保存成功')
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div>
    <div class="page-title"><h2>窗口配置</h2></div>
    <div class="card">
      <h3>同步窗口配置 (POST /api/timeseries/window-config)</h3>
      <div class="grid">
        <div class="field">
          <label>项目ID</label>
          <input v-model="form.projectId" placeholder="留空 = 默认项目" />
        </div>
        <div class="field">
          <label>窗口大小 windowSizeMs（毫秒）<span class="req">*</span></label>
          <input v-model="form.windowSizeMs" type="number" placeholder="如 60000" />
        </div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="submit">同步到 Core</button>
      </div>
    </div>
    <ResultViewer :response="response" :error="error" />
  </div>
</template>
