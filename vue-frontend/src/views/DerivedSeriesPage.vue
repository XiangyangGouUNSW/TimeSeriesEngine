<script setup>
import { reactive, ref, watch } from 'vue'
import { api } from '../api/timeseries'
import RefInput from '../components/RefInput.vue'
import DerivedExprInput from '../components/DerivedExprInput.vue'
import { toastError, toastSuccess } from '../composables/toast'
import { useRefOptions } from '../composables/refOptions'
import ResultViewer from '../components/ResultViewer.vue'
import { projectContext, withCurrentProject } from '../stores/project'

const mode = ref('linear') // linear | expression | raw
const loading = ref(false)
const response = ref(null)
const error = ref('')

const { refOptions, loadTypes } = useRefOptions()
loadTypes(['instance'])

const seqField = { name: 'sequenceId', placeholder: '按子串匹配选择实例' }

// 线性组合模式：逐行配置 序列 + 系数
const linear = reactive({
  projectId: projectContext.currentProjectId.value,
  derivedSequenceId: '',
  enabled: 'true',
  terms: [{ sequenceId: '', coefficient: 1.0 }],
  bias: '',
})

function addTerm() {
  linear.terms.push({ sequenceId: '', coefficient: 1.0 })
}

function removeTerm(i) {
  linear.terms.splice(i, 1)
}

// 表达式模式：结构化递归编辑
const expr = reactive({
  projectId: projectContext.currentProjectId.value,
  derivedSequenceId: '',
  enabled: 'true',
  expression: {
    binary: {
      operator: 'ADD',
      left: { sequenceId: '' },
      right: { constant: 1.0 },
    },
  },
})

// 原始 JSON 模式
const rawJson = ref(
  '{\n  "projectId": "",\n  "items": [\n    {\n      "derivedSequenceId": "ETTh1_HUFL_d",\n      "enabled": true,\n      "linearCombination": {\n        "terms": [ { "sequenceId": "ETTh1_HUFL", "coefficient": 1.0 } ],\n        "bias": 0.0\n      }\n    }\n  ]\n}'
)

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    linear.projectId = projectId
    expr.projectId = projectId
    response.value = null
    loadTypes(['instance'])
  },
)

async function send(payload) {
  const scopedPayload = withCurrentProject(payload)
  if (!scopedPayload) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await api.syncDerivedSeries(scopedPayload)
    response.value = res
    if (res && res.success === false) error.value = res.message || '请求失败'
    else if (res) toastSuccess((res && res.message) || '派生序列同步成功')
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
  const terms = linear.terms
    .filter((t) => t.sequenceId && String(t.sequenceId).trim())
    .map((t) => {
      const item = { sequenceId: String(t.sequenceId).trim() }
      if (t.coefficient !== '' && t.coefficient !== null && t.coefficient !== undefined) {
        item.coefficient = Number(t.coefficient)
        if (Number.isNaN(item.coefficient)) {
          toastError('系数必须为数字')
          return null
        }
      }
      return item
    })
  if (terms.some((t) => t === null)) return
  if (!terms.length) {
    toastError('请至少添加一个序列项')
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
  await send({ items: [item] })
}

function expressionFilled(node) {
  if (!node) return false
  if (node.sequenceId !== undefined && node.sequenceId !== null && String(node.sequenceId).trim() !== '') {
    return true
  }
  if (node.constant !== undefined && node.constant !== null && node.constant !== '') {
    return true
  }
  if (node.binary) {
    return expressionFilled(node.binary.left) && expressionFilled(node.binary.right)
  }
  return false
}

async function submitExpression() {
  if (!expr.derivedSequenceId.trim()) {
    toastError('请填写必填字段：派生序列ID')
    return
  }
  if (!expressionFilled(expr.expression)) {
    toastError('表达式不完整：请填写序列或常数')
    return
  }
  await send({
    items: [
      {
        derivedSequenceId: expr.derivedSequenceId,
        enabled: expr.enabled === 'true',
        expression: expr.expression,
      },
    ],
  })
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
            <label>线性组合项（Σ 系数 × 序列 + 偏置）</label>
            <div class="linear-terms">
              <div v-if="linear.terms.length" class="term-head">
                <span class="t-seq">序列ID</span>
                <span class="t-coef">系数</span>
                <span class="t-del-slot"></span>
              </div>
              <div v-for="(t, i) in linear.terms" :key="i" class="term-row">
                <div class="t-seq">
                  <RefInput :field="seqField" :options="refOptions.instance || []" v-model="t.sequenceId" />
                </div>
                <input class="t-coef" type="number" step="any" v-model="t.coefficient" placeholder="系数（如 1）" />
                <button type="button" class="t-del" @click="removeTerm(i)">删除</button>
              </div>
              <button type="button" class="t-add" @click="addTerm">＋ 添加序列项</button>
              <small class="t-hint">
                派生序列 = Σ(系数 × 序列) + 偏置；序列从已有实例下拉选择（按子串匹配），可自由输入自定义序列；系数留空则忽略该项。
              </small>
            </div>
          </div>
          <div class="field"><label>bias（偏置，可选，与序列同单位）</label><input v-model="linear.bias" type="number" step="any" /></div>
        </div>
        <div class="actions">
          <button :disabled="loading" @click="loadTypes(['instance'])">刷新下拉选项</button>
          <button class="primary" :disabled="loading" @click="submitLinear">同步（线性组合）</button>
        </div>
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
            <label>表达式（序列 / 常数 / 二元运算 自由组合）</label>
            <div class="expr-editor">
              <DerivedExprInput v-model="expr.expression" :options="refOptions.instance || []" />
            </div>
          </div>
        </div>
        <div class="actions">
          <button :disabled="loading" @click="loadTypes(['instance'])">刷新下拉选项</button>
          <button class="primary" :disabled="loading" @click="submitExpression">同步（表达式）</button>
        </div>
      </template>

      <template v-else>
        <textarea v-model="rawJson" rows="14"></textarea>
        <div class="actions"><button class="primary" :disabled="loading" @click="submitRaw">发送原始 JSON</button></div>
      </template>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
