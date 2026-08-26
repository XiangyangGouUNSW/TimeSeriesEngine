<script setup>
import { computed, reactive, ref, watch } from 'vue'
import http from '../api/http'
import { api } from '../api/timeseries'
import { toastError } from '../composables/toast'
import { useRefOptions } from '../composables/refOptions'
import FieldInput from './FieldInput.vue'
import RefInput from './RefInput.vue'
import ResultViewer from './ResultViewer.vue'
import { projectContext, withCurrentProject } from '../stores/project'

// 配置驱动的通用 CRUD 页面：
//   config.base         基础路径，如 /api/timeseries/instances
//   config.fields       表单字段（创建/更新）
//   config.queryFields  条件查询字段（POST {base}/query）
//   config.columns      表格列
//   config.statusUpdate { idField, path, fields }  可选，状态更新 PATCH
//   config.detailGet    (row) => url               可选，行详情 GET
const props = defineProps({
  config: { type: Object, required: true },
})

const base = computed(() => props.config.base)

const form = reactive({})
const query = reactive({})
const statusForm = reactive({})
const rows = ref([])
const loading = ref(false)
const response = ref(null)
const error = ref('')
const jsonMode = ref(false)
const jsonText = ref('')
const editing = ref(false)
const detailModalVisible = ref(false)
const detailData = ref(null)

// 初始化默认值
function initFormDefaults() {
  for (const f of props.config.fields || []) {
    if (f.default !== undefined) form[f.name] = f.default
  }
}
initFormDefaults()

// ── 引用型字段（ref/refs）的选项：从后端已有实体列表加载 ─────────────
const REF_SOURCES = {
  category: { loader: (projectId) => api.queryCategories({ projectId }), idKey: 'categoryId', labelKey: 'categoryName' },
  instance: { loader: (projectId) => api.queryInstances({ projectId }), idKey: 'sequenceId', labelKey: 'instanceName' },
  constraint: { loader: (projectId) => api.queryConstraints({ projectId }), idKey: 'constraintId', labelKey: 'constraintName' },
  relation: { loader: (projectId) => api.queryRelations({ projectId }), idKey: 'relationId', labelKey: 'relationName' },
  event: { loader: (projectId) => api.queryEvents({ projectId }), idKey: 'eventId', labelKey: 'eventName' },
  anomalyTask: { loader: (projectId) => api.queryAnomalyTasks({ projectId }), idKey: 'taskId', labelKey: 'taskName' },
  forecastTask: { loader: (projectId) => api.queryForecastTasks({ projectId }), idKey: 'taskId', labelKey: 'taskName' },
  task: { loader: null, idKey: 'taskId', labelKey: 'taskName' }, // 异常+预测任务合并
}
const refOptions = reactive({})

function refTypesNeeded() {
  const types = new Set()
  for (const f of [...(props.config.fields || []), ...(props.config.queryFields || [])]) {
    if (f.refType) types.add(f.refType)
    if (f.categoryRefType) types.add(f.categoryRefType)
  }
  const su = props.config.statusUpdate
  if (su && su.refType) types.add(su.refType)
  return types
}

async function loadRefOptions() {
  for (const type of refTypesNeeded()) {
    try {
      let list = []
      if (type === 'task') {
        const projectId = projectContext.currentProjectId.value
        if (!projectId) {
          refOptions[type] = []
          continue
        }
        const [a, b] = await Promise.all([
          api.queryAnomalyTasks({ projectId }),
          api.queryForecastTasks({ projectId }),
        ])
        const seen = new Set()
        for (const t of [...(a.data || []), ...(b.data || [])]) {
          if (!t || seen.has(t.taskId)) continue
          seen.add(t.taskId)
          list.push(t)
        }
      } else {
        const projectId = projectContext.currentProjectId.value
        if (!projectId) {
          refOptions[type] = []
          continue
        }
        const res = await REF_SOURCES[type].loader(projectId)
        list = res.data || []
      }
      const { idKey, labelKey } = REF_SOURCES[type]
      refOptions[type] = list
        .filter((it) => it && it[idKey])
        .map((it) => ({
          value: it[idKey],
          label: it[labelKey] || it[idKey],
        }))
    } catch (e) {
      refOptions[type] = []
    }
  }
}
loadRefOptions()

// 配置变化时重置页面状态，防止复用组件实例时残留上一个页面的数据
watch(
  () => props.config,
  () => {
    for (const k of Object.keys(form)) delete form[k]
    for (const k of Object.keys(query)) delete query[k]
    for (const k of Object.keys(statusForm)) delete statusForm[k]
    rows.value = []
    response.value = null
    error.value = ''
    jsonMode.value = false
    jsonText.value = ''
    editing.value = false
    initFormDefaults()
    const projectId = projectContext.currentProjectId.value
    if ((props.config.fields || []).some((field) => field.name === 'projectId')) {
      form.projectId = projectId
    }
    if ((props.config.queryFields || []).some((field) => field.name === 'projectId')) {
      query.projectId = projectId
    }
    if (props.config.statusUpdate) statusForm.projectId = projectId
    loadRefOptions()
  }
)

async function run(method, url, body) {
  loading.value = true
  response.value = null
  error.value = ''
  try {
    const res = await http({ method, url, data: body })
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

// 表单 → 请求体（按字段类型转换，跳过空值）
function buildPayload(fields, src) {
  const payload = {}
  for (const f of fields) {
    let v = src[f.name]
    if (v === '' || v === undefined || v === null) continue
    if (f.type === 'array' || f.type === 'refs') {
      v = String(v).split(/[\s,，;]+/).map((s) => s.trim()).filter(Boolean)
      if (!v.length) continue
    } else if (f.type === 'number') {
      v = Number(v)
      if (Number.isNaN(v)) continue
    } else if (f.type === 'datetime') {
      // datetime-local 值为 YYYY-MM-DDTHH:mm，补 :00 匹配后端 LocalDateTime
      v = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(v) ? v + ':00' : v
    } else if (f.type === 'terms') {
      const rows = (Array.isArray(v) ? v : []).filter(
        (t) => t && typeof t === 'object' && (t.variable || t.mappingId || t.sequenceId)
      )
      if (!rows.length) continue
      // terms 数组：仅包含填了变量名的行，空系数/偏移不提交
      payload.terms = rows
        .filter((t) => t.variable)
        .map((t) => {
          const item = { variable: t.variable }
          if (t.coefficient !== undefined && t.coefficient !== null && t.coefficient !== '') {
            item.coefficient = Number(t.coefficient)
          }
          if (t.sampleOffset !== undefined && t.sampleOffset !== null && t.sampleOffset !== '') {
            item.sampleOffset = Number(t.sampleOffset)
          }
          return item
        })
      // 由「变量名 → 映射ID（实例或类别）」自动生成 variableMapping
      const mapping = {}
      for (const t of rows) {
        const id = t.mappingId || t.sequenceId
        if (t.variable && id) mapping[t.variable] = id
      }
      if (Object.keys(mapping).length) payload.variableMapping = mapping
    } else if (f.type === 'json') {
      try {
        v = JSON.parse(v)
      } catch (e) {
        toastError(`字段「${f.label}」JSON 解析失败：${e.message}`)
        return null
      }
    }
    payload[f.name] = v
  }
  return payload
}

// 必填字段校验（与后端一致），缺失时直接拦截
function checkRequired(fields, src) {
  const missing = []
  for (const f of fields || []) {
    if (!f.required) continue
    const v = src[f.name]
    if (v === undefined || v === null || v === '' || (Array.isArray(v) && !v.length)) {
      missing.push(f.label)
    }
  }
  if (missing.length) {
    toastError('请填写必填字段：' + missing.join('、'))
    return false
  }
  return true
}

async function create() {
  if (!checkRequired(props.config.fields, form)) return
  const payload = scopePayload(buildPayload(props.config.fields, form))
  if (payload === null) return
  const res = await run('post', base.value, payload)
  if (res && res.success !== false) {
    editing.value = true
    await loadAll()
    loadRefOptions()
  }
}

async function update() {
  if (!checkRequired(props.config.fields, form)) return
  const payload = scopePayload(buildPayload(props.config.fields, form))
  if (payload === null) return
  const res = await run('put', base.value, payload)
  if (res && res.success !== false) {
    await loadAll()
    loadRefOptions()
  }
}

async function loadAll() {
  const projectId = projectContext.currentProjectId.value
  if (!projectId) {
    rows.value = []
    return
  }
  const res = await run('post', `${base.value}/query`, { projectId })
  rows.value = (res && res.data) || []
}

async function doQuery() {
  const payload = scopePayload(buildPayload(props.config.queryFields || [], query))
  if (payload === null) return
  const res = await run('post', `${base.value}/query`, payload)
  rows.value = (res && res.data) || []
}

function scopePayload(payload) {
  if (payload === null) return null
  const scoped = withCurrentProject(payload)
  if (scoped === null) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
  }
  return scoped
}

async function doStatus() {
  const su = props.config.statusUpdate
  if (!statusForm.id) {
    toastError(`请先填写 ${su.idField}`)
    return
  }
  const payload = { [su.idField]: statusForm.id }
  if (statusForm.projectId) payload.projectId = statusForm.projectId
  for (const f of su.fields) {
    const v = statusForm[f.name]
    if (v !== '' && v !== undefined && v !== null) payload[f.name] = v
  }
  const scopedPayload = scopePayload(payload)
  if (scopedPayload === null) return
  const res = await run('patch', `${base.value}${su.path}`, scopedPayload)
  if (res && res.success !== false) await loadAll()
}

async function sendRaw(method) {
  let payload
  try {
    payload = jsonText.value.trim() ? JSON.parse(jsonText.value) : {}
  } catch (e) {
    toastError('JSON 解析失败：' + e.message)
    return
  }
  const scopedPayload = scopePayload(payload)
  if (scopedPayload === null) return
  const res = await run(method, base.value, scopedPayload)
  if (res && res.success !== false) await loadAll()
}

async function showDetail(row) {
  if (!props.config.detailGet) return
  const res = await run('get', props.config.detailGet(row))
  if (res && res.data) {
    detailData.value = res.data
    detailModalVisible.value = true
  }
}

function formatDetailValue(v) {
  if (v === undefined || v === null || v === '') return '—'
  if (Array.isArray(v)) return v.join(', ')
  if (typeof v === 'object') return JSON.stringify(v)
  return v
}

// 详情展示行：顶层字段平铺，嵌套对象展开为 key.subKey
function detailRows() {
  const rows = []
  for (const [key, value] of Object.entries(detailData.value || {})) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      for (const [k2, v2] of Object.entries(value)) {
        rows.push([`${key}.${k2}`, formatDetailValue(v2)])
      }
    } else {
      rows.push([key, formatDetailValue(value)])
    }
  }
  return rows
}

function fillFromRow(row) {
  for (const k of Object.keys(form)) delete form[k]
  for (const f of props.config.fields || []) {
    const v = row[f.name]
    if (v === undefined || v === null) continue
    if (f.type === 'array' || f.type === 'refs') form[f.name] = Array.isArray(v) ? v.join(', ') : v
    else if (f.type === 'terms') {
      // 回填：既有 terms 行，也把 variableMapping 合并成行（避免丢失旧数据的映射）
      const list = Array.isArray(v) ? v.map((t) => ({ ...t })) : []
      for (const t of list) {
        if (t.mappingId === undefined && t.sequenceId !== undefined) {
          t.mappingId = t.sequenceId
          delete t.sequenceId
        }
        if (!t.mappingType) t.mappingType = 'sequence'
      }
      const mapping = row.variableMapping
      if (mapping && typeof mapping === 'object') {
        const categoryIds = new Set((refOptions.category || []).map((o) => o.value))
        for (const [variable, id] of Object.entries(mapping)) {
          const t = list.find((x) => x.variable === variable)
          if (t) {
            if (!t.mappingId) t.mappingId = id
          } else {
            list.push({
              variable,
              mappingId: id,
              mappingType: categoryIds.has(id) ? 'category' : 'sequence',
              coefficient: 1.0,
              sampleOffset: 0,
            })
          }
        }
      }
      form[f.name] = list
    }
    else if (f.type === 'json') form[f.name] = JSON.stringify(v, null, 2)
    else if (f.type === 'datetime') form[f.name] = String(v).slice(0, 16)
    else form[f.name] = v
  }
  editing.value = true
  window.scrollTo({ top: 0, behavior: 'smooth' })
}

function clearForm() {
  for (const k of Object.keys(form)) delete form[k]
  editing.value = false
}

watch(
  () => projectContext.currentProjectId.value,
  (projectId) => {
    if ((props.config.fields || []).some((field) => field.name === 'projectId')) {
      form.projectId = projectId
    }
    if ((props.config.queryFields || []).some((field) => field.name === 'projectId')) {
      query.projectId = projectId
    }
    if (props.config.statusUpdate) statusForm.projectId = projectId
    rows.value = []
    loadAll()
    loadRefOptions()
  },
  { immediate: true },
)

function cellText(row, col) {
  const v = col.accessor ? col.accessor(row) : row[col.key]
  if (v === undefined || v === null) return ''
  if (Array.isArray(v)) return v.join(', ')
  if (typeof v === 'object') return JSON.stringify(v)
  return v
}
</script>

<template>
  <div>
    <div class="page-title">
      <h2>{{ config.title }}</h2>
      <div>
        <button @click="jsonMode = !jsonMode">
          {{ jsonMode ? '表单模式' : 'JSON 模式' }}
        </button>
        <button class="primary" style="margin-left: 8px" @click="loadAll">刷新列表</button>
      </div>
    </div>

    <!-- JSON 原始模式 -->
    <div v-if="jsonMode" class="card">
      <h3>原始 JSON 请求（发送到 {{ base }}）</h3>
      <textarea v-model="jsonText" rows="12" placeholder='{"projectId": "", ...}'></textarea>
      <div class="actions">
        <button class="primary" :disabled="loading" @click="sendRaw('post')">发送创建 (POST)</button>
        <button :disabled="loading" @click="sendRaw('put')">发送更新 (PUT)</button>
      </div>
      <ResultViewer :response="response" :error="error" />
    </div>

    <!-- 表单模式 -->
    <template v-else>
      <div class="card">
        <h3>{{ editing ? '编辑' : '新增' }} {{ config.title }}</h3>
        <div class="grid">
          <FieldInput
            v-for="f in config.fields"
            :key="f.name"
            :field="f"
            :options="f.refType ? refOptions[f.refType] || [] : []"
            :category-options="f.categoryRefType ? refOptions[f.categoryRefType] || [] : []"
            v-model="form[f.name]"
          />
        </div>
        <div class="actions">
          <button class="primary" :disabled="loading" @click="create">创建 (POST)</button>
          <button :disabled="loading" @click="update">更新 (PUT)</button>
          <button :disabled="loading" @click="clearForm">清空表单</button>
          <button :disabled="loading" @click="loadRefOptions">刷新下拉选项</button>
        </div>
        <small style="color: #9aa5b1">点击表格行可将该记录填入表单进行更新</small>
      </div>

      <div v-if="config.statusUpdate" class="card">
        <h3>状态更新 (PATCH {{ base }}{{ config.statusUpdate.path }})</h3>
        <div class="grid">
          <div class="field">
            <label>projectId</label>
            <input v-model="statusForm.projectId" placeholder="留空 = 默认项目" />
          </div>
          <div class="field">
            <label>{{ config.statusUpdate.idField }} *</label>
            <RefInput
              :field="{ name: 'statusId', placeholder: '按子串匹配，从已有实体中选择' }"
              :options="refOptions[config.statusUpdate.refType] || []"
              :model-value="statusForm.id"
              @update:model-value="(v) => (statusForm.id = v)"
            />
          </div>
          <FieldInput
            v-for="f in config.statusUpdate.fields"
            :key="f.name"
            :field="f"
            v-model="statusForm[f.name]"
          />
        </div>
        <div class="actions">
          <button class="primary" :disabled="loading" @click="doStatus">更新状态</button>
        </div>
      </div>

      <div class="card">
        <h3>条件查询 (POST {{ base }}/query)</h3>
        <div class="grid">
          <FieldInput
            v-for="f in config.queryFields || []"
            :key="f.name"
            :field="f"
            :options="f.refType ? refOptions[f.refType] || [] : []"
            v-model="query[f.name]"
          />
        </div>
        <div class="actions">
          <button class="primary" :disabled="loading" @click="doQuery">查询</button>
          <button :disabled="loading" @click="loadAll">查询全部</button>
        </div>
      </div>

      <div class="card">
        <h3>列表（共 {{ rows.length }} 条）</h3>
        <div v-if="!rows.length" class="empty">暂无数据</div>
        <table v-else>
          <thead>
            <tr>
              <th v-for="c in config.columns" :key="c.key">{{ c.label }}</th>
              <th v-if="config.detailGet" style="width: 60px">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(row, i) in rows"
              :key="i"
              :class="{ clickable: config.fields.length }"
              @click="fillFromRow(row)"
            >
              <td v-for="c in config.columns" :key="c.key">{{ cellText(row, c) }}</td>
              <td v-if="config.detailGet" @click.stop>
                <button @click="showDetail(row)">详情</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <!-- 详情弹窗 -->
    <div v-if="detailModalVisible" class="modal-mask" @click.self="detailModalVisible = false">
      <div class="modal-card">
        <div class="modal-head">
          <h3>详情</h3>
          <button class="modal-close" @click="detailModalVisible = false">×</button>
        </div>
        <div class="modal-body">
          <table v-if="detailRows().length">
            <tbody>
              <tr v-for="(r, i) in detailRows()" :key="i">
                <th>{{ r[0] }}</th>
                <td>{{ r[1] }}</td>
              </tr>
            </tbody>
          </table>
          <div v-else class="empty">暂无详情数据</div>
          <details>
            <summary>原始 JSON</summary>
            <pre>{{ JSON.stringify(detailData, null, 2) }}</pre>
          </details>
        </div>
      </div>
    </div>

    <ResultViewer :response="response" :error="error" />
  </div>
</template>
