<script setup>
import { reactive, ref } from 'vue'
import { api } from '../api/timeseries'
import { toastError } from '../composables/toast'
import ResultViewer from '../components/ResultViewer.vue'

const mode = ref('linear') // linear | expression | raw
const loading = ref(false)
const response = ref(null)
const error = ref('')

// 线性组合模式
const linear = reactive({
  projectId: '',
  derivedSequenceId: '',
  enabled: 'true',
  termsJson: '[\n  { "sequenceId": "ETTh1_HUFL", "coefficient": 1.0 }\n]',
  bias: '',
})

// 表达式模式
const expr = reactive({
  projectId: '',
  derivedSequenceId: '',
  enabled: 'true',
  expressionJson:
    '{\n  "binary": {\n    "operator": "ADD",\n    "left": { "sequenceId": "ETTh1_HUFL" },\n    "right": { "constant": 1.0 }\n  }\n}',
})

// 原始 JSON 模式
const rawJson = ref(
  '{\n  "projectId": "",\n  "items": [\n    {\n      "derivedSequenceId": "ETTh1_HUFL_d",\n      "enabled": true,\n      "linearCombination": {\n        "terms": [ { "sequenceId": "ETTh1_HUFL", "coefficient": 1.0 } ],\n        "bias": 0.0\n      }\n    }\n  ]\n}'
)

async function send(payload) {
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await api.syncDerivedSeries(payload)
    response.value = res
    if (res && res.success === false) error.value = res.message || '请求失败'
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

async function submitLinear() {
  if (!linear.derivedSequenceId.trim()) {
    toastError('请填写必填字段：派生序列ID')
    return
  }
  let terms
  try {
    terms = JSON.parse(linear.termsJson)
  } catch (e) {
    toastError('terms JSON 解析失败：' + e.message)
    return
  }
  const item = {
    derivedSequenceId: linear.derivedSequenceId,
    enabled: linear.enabled === 'true',
    linearCombination: { terms },
  }
  if (linear.bias !== '' && linear.bias !== null) {
    item.linearCombination.bias = Number(linear.bias)
    if (Number.isNaN(item.linearCombination.bias)) {
      toastError('bias 必须为数字')
      return
    }
  }
  const payload = { items: [item] }
  if (linear.projectId) payload.projectId = linear.projectId
  await send(payload)
}

async function submitExpression() {
  if (!expr.derivedSequenceId.trim()) {
    toastError('请填写必填字段：派生序列ID')
    return
  }
  let expression
  try {
    expression = JSON.parse(expr.expressionJson)
  } catch (e) {
    toastError('expression JSON 解析失败：' + e.message)
    return
  }
  const item = {
    derivedSequenceId: expr.derivedSequenceId,
    enabled: expr.enabled === 'true',
    expression,
  }
  const payload = { items: [item] }
  if (expr.projectId) payload.projectId = expr.projectId
  await send(payload)
}

async function submitRaw() {
  let payload
  try {
    payload = JSON.parse(rawJson.value)
  } catch (e) {
    toastError('JSON 解析失败：' + e.message)
    return
  }
  await send(payload)
}
</script>

<template>
  <div>
    <div class="page-title">
      <h2>派生序列配置</h2>
      <div>
        <button @click="mode = 'linear'" :class="{ primary: mode === 'linear' }">线性组合</button>
        <button @click="mode = 'expression'" :class="{ primary: mode === 'expression' }" style="margin-left: 8px">表达式</button>
        <button @click="mode = 'raw'" :class="{ primary: mode === 'raw' }" style="margin-left: 8px">原始 JSON</button>
      </div>
    </div>

    <div class="card">
      <h3>同步派生序列配置 (POST /api/timeseries/derived-series)</h3>

      <template v-if="mode === 'linear'">
        <div class="grid">
          <div class="field"><label>项目ID</label><input v-model="linear.projectId" placeholder="留空 = 默认项目" /></div>
          <div class="field"><label>派生序列ID *</label><input v-model="linear.derivedSequenceId" placeholder="如 ETTh1_HUFL_d" /></div>
          <div class="field">
            <label>启用</label>
            <select v-model="linear.enabled"><option value="true">true</option><option value="false">false</option></select>
          </div>
          <div class="field full">
            <label>terms（JSON 数组：[{sequenceId, coefficient}]）</label>
            <textarea v-model="linear.termsJson" rows="5"></textarea>
          </div>
          <div class="field"><label>bias（偏置，可选）</label><input v-model="linear.bias" type="number" /></div>
        </div>
        <div class="actions"><button class="primary" :disabled="loading" @click="submitLinear">同步（线性组合）</button></div>
      </template>

      <template v-else-if="mode === 'expression'">
        <div class="grid">
          <div class="field"><label>项目ID</label><input v-model="expr.projectId" placeholder="留空 = 默认项目" /></div>
          <div class="field"><label>派生序列ID *</label><input v-model="expr.derivedSequenceId" /></div>
          <div class="field">
            <label>启用</label>
            <select v-model="expr.enabled"><option value="true">true</option><option value="false">false</option></select>
          </div>
          <div class="field full">
            <label>expression（JSON：{sequenceId} / {constant} / {binary:{operator,left,right}}）</label>
            <textarea v-model="expr.expressionJson" rows="9"></textarea>
          </div>
        </div>
        <div class="actions"><button class="primary" :disabled="loading" @click="submitExpression">同步（表达式）</button></div>
      </template>

      <template v-else>
        <textarea v-model="rawJson" rows="14"></textarea>
        <div class="actions"><button class="primary" :disabled="loading" @click="submitRaw">发送原始 JSON</button></div>
      </template>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
