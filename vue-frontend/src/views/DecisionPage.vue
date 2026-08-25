<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import { toastError } from '../composables/toast'
import ResultViewer from '../components/ResultViewer.vue'
import { projectContext, withCurrentProject } from '../stores/project'

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

const diagnosis = reactive({ projectId: projectContext.currentProjectId.value, eventId: '' })
const suggestion = reactive({ projectId: projectContext.currentProjectId.value, eventId: '' })
const feedback = reactive({
  projectId: projectContext.currentProjectId.value,
  eventId: '',
  disposalResult: '',
  handleStatus: '',
})

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    diagnosis.projectId = projectId
    suggestion.projectId = projectId
    feedback.projectId = projectId
    response.value = null
  },
)

function eventPayload(src) {
  const payload = withCurrentProject({ projectId: src.projectId || undefined })
  if (!payload) return null
  if (src.eventId) payload.eventId = src.eventId
  return payload
}

async function doDiagnosis() {
  const payload = eventPayload(diagnosis)
  if (!payload) return
  await run(api.diagnosis(payload))
}

async function doSuggestion() {
  const payload = eventPayload(suggestion)
  if (!payload) return
  await run(api.suggestion(payload))
}

async function doFeedback() {
  if (!feedback.eventId) {
    toastError('请填写事件ID')
    return
  }
  const payload = eventPayload(feedback)
  if (!payload) return
  if (feedback.disposalResult) payload.disposalResult = feedback.disposalResult
  if (feedback.handleStatus) payload.handleStatus = feedback.handleStatus
  await run(api.feedback(payload))
}
</script>

<template>
  <div>
    <div class="page-title"><h2>决策辅助</h2></div>

    <div class="card">
      <h3>事件诊断 (POST /api/timeseries/decision/diagnosis)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="diagnosis.projectId" placeholder="留空 = 默认项目" /></div>
        <div class="field"><label>事件ID *</label><input v-model="diagnosis.eventId" placeholder="如 event_xxx" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doDiagnosis">诊断</button>
      </div>
    </div>

    <div class="card">
      <h3>处置建议 (POST /api/timeseries/decision/suggestion)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="suggestion.projectId" placeholder="留空 = 默认项目" /></div>
        <div class="field"><label>事件ID *</label><input v-model="suggestion.eventId" /></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doSuggestion">获取建议</button>
      </div>
    </div>

    <div class="card">
      <h3>处置反馈 (PATCH /api/timeseries/decision/feedback)</h3>
      <div class="grid">
        <div class="field"><label>项目ID</label><input v-model="feedback.projectId" placeholder="留空 = 默认项目" /></div>
        <div class="field"><label>事件ID *</label><input v-model="feedback.eventId" /></div>
        <div class="field">
          <label>处理状态</label>
          <select v-model="feedback.handleStatus">
            <option value=""></option>
            <option>UNHANDLED</option>
            <option>PROCESSING</option>
            <option>HANDLED</option>
          </select>
        </div>
        <div class="field full"><label>处置结果</label><textarea v-model="feedback.disposalResult" rows="3"></textarea></div>
      </div>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="doFeedback">提交反馈</button>
      </div>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
