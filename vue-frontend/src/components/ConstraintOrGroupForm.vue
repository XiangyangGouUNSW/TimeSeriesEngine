<script setup>
import { ref } from 'vue'
import http from '../api/http'
import { toastError, toastSuccess } from '../composables/toast'
import { withCurrentProject } from '../stores/project'
import TermsEditor from './TermsEditor.vue'

// OR 组批量创建表单：
//   一次定义多条约束（共享同一 orGroupId），提交时统一 POST /constraints/batch，
//   后端整体校验、整体落库后一次性同步 Core —— 组确定好之前不会单独发到 Core。
const props = defineProps({
  instanceOptions: { type: Array, default: () => [] },
  categoryOptions: { type: Array, default: () => [] },
})
const emit = defineEmits(['created'])

const groupId = ref('')
const members = ref([])
const submitting = ref(false)

function newMember() {
  return {
    constraintId: '',
    constraintName: '',
    constraintExpression: '',
    lowerBound: undefined,
    upperBound: undefined,
    constraintDescription: '',
    effectiveStatus: 'ENABLE',
    confirmStatus: 'CONFIRMED',
    terms: [
      {
        variable: '',
        mappingType: 'sequence',
        mappingId: '',
        coefficient: 1.0,
        sampleOffset: 0,
        aggregation: 'SAMPLE',
      },
    ],
  }
}

function addMember() {
  members.value.push(newMember())
}

function removeMember(i) {
  members.value.splice(i, 1)
}

function setMemberField(i, key, value) {
  const next = members.value.map((m) => ({ ...m }))
  next[i] = { ...next[i], [key]: value }
  members.value = next
}

function setMemberTerms(i, terms) {
  const next = members.value.map((m) => ({ ...m }))
  next[i] = { ...next[i], terms }
  members.value = next
}

function buildMember(m) {
  const member = {}
  if (m.constraintId) member.constraintId = m.constraintId
  if (m.constraintName) member.constraintName = m.constraintName
  if (m.constraintExpression) member.constraintExpression = m.constraintExpression
  if (m.lowerBound !== undefined && m.lowerBound !== null && m.lowerBound !== '') {
    member.lowerBound = Number(m.lowerBound)
  }
  if (m.upperBound !== undefined && m.upperBound !== null && m.upperBound !== '') {
    member.upperBound = Number(m.upperBound)
  }
  if (m.constraintDescription) member.constraintDescription = m.constraintDescription
  member.effectiveStatus = m.effectiveStatus || 'ENABLE'
  member.confirmStatus = m.confirmStatus || 'CONFIRMED'

  const rows = (Array.isArray(m.terms) ? m.terms : []).filter((t) => t && t.variable)
  member.terms = rows.map((t) => {
    const item = { variable: t.variable }
    if (t.coefficient !== undefined && t.coefficient !== null && t.coefficient !== '') {
      item.coefficient = Number(t.coefficient)
    }
    if (t.sampleOffset !== undefined && t.sampleOffset !== null && t.sampleOffset !== '') {
      item.sampleOffset = Number(t.sampleOffset)
    }
    if (t.aggregation && t.aggregation !== 'SAMPLE') {
      item.aggregation = t.aggregation
    }
    return item
  })

  const mapping = {}
  for (const t of rows) {
    const id = t.mappingId || t.sequenceId
    if (t.variable && id) mapping[t.variable] = id
  }
  if (Object.keys(mapping).length) member.variableMapping = mapping
  return member
}

async function submit() {
  const gid = groupId.value.trim()
  if (!gid) {
    toastError('请填写 OR 组 ID')
    return
  }
  if (!members.value.length) {
    toastError('请至少添加一个组成员')
    return
  }
  const constraints = []
  for (const m of members.value) {
    const c = buildMember(m)
    if (!c.constraintName) {
      toastError('组成员缺少「约束名称」')
      return
    }
    if (!c.constraintExpression) {
      toastError(`组成员「${c.constraintName}」缺少「约束表达式」`)
      return
    }
    if (!c.terms || !c.terms.length) {
      toastError(`组成员「${c.constraintName}」缺少约束项`)
      return
    }
    if (!c.variableMapping || !Object.keys(c.variableMapping).length) {
      toastError(`组成员「${c.constraintName}」缺少变量映射（请绑定映射目标）`)
      return
    }
    constraints.push(c)
  }
  const payload = { orGroupId: gid, constraints }
  const scoped = withCurrentProject(payload)
  if (scoped === null) {
    toastError('请先选择当前项目，且请求项目必须与当前项目一致')
    return
  }
  submitting.value = true
  try {
    const res = await http({
      method: 'post',
      url: '/api/timeseries/semantic/constraints/batch',
      data: scoped,
    })
    if (res && res.success === false) {
      toastError(res.message || 'OR 组创建失败')
      return
    }
    toastSuccess((res && res.message) || `OR 组「${gid}」创建成功，已统一同步 Core`)
    members.value = []
    groupId.value = ''
    emit('created')
  } catch (e) {
    toastError(e.message || String(e))
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="card or-batch-card">
    <h3>OR 组批量创建（一次提交多条约束，统一同步 Core）</h3>
    <p class="or-batch-hint">
      先在这里定义好所有组成员，点「提交 OR 组」时才会统一发送到后端并一次性同步 Core；
      在此之前不会单独把约束发出去。同组内「任一满足即组满足」，不同组间 AND。
    </p>
    <div class="or-batch-head">
      <div class="field">
        <label>OR 组 ID *</label>
        <input v-model="groupId" placeholder="如 ot-band（同组约束共享此 ID）" />
      </div>
      <button type="button" class="primary" :disabled="submitting" @click="submit">提交 OR 组 (POST /constraints/batch)</button>
    </div>

    <div v-for="(m, i) in members" :key="i" class="or-member">
      <div class="or-member-head">
        <span class="or-member-title">组成员 #{{ i + 1 }}</span>
        <button type="button" @click="removeMember(i)">删除该成员</button>
      </div>
      <div class="grid">
        <div class="field">
          <label>约束ID（可选，留空自动生成）</label>
          <input :value="m.constraintId" placeholder="如 ott-upper-40" @input="setMemberField(i, 'constraintId', $event.target.value)" />
        </div>
        <div class="field">
          <label>约束名称 *</label>
          <input :value="m.constraintName" placeholder="如 OT 低于 40" @input="setMemberField(i, 'constraintName', $event.target.value)" />
        </div>
        <div class="field">
          <label>约束表达式 *</label>
          <input :value="m.constraintExpression" placeholder="如 x < 40" @input="setMemberField(i, 'constraintExpression', $event.target.value)" />
        </div>
        <div class="field">
          <label>下界（留空 = 无）</label>
          <input type="number" step="any" :value="m.lowerBound" @input="setMemberField(i, 'lowerBound', $event.target.value === '' ? undefined : Number($event.target.value))" />
        </div>
        <div class="field">
          <label>上界（留空 = 无）</label>
          <input type="number" step="any" :value="m.upperBound" @input="setMemberField(i, 'upperBound', $event.target.value === '' ? undefined : Number($event.target.value))" />
        </div>
        <div class="field">
          <label>生效状态</label>
          <select :value="m.effectiveStatus" @change="setMemberField(i, 'effectiveStatus', $event.target.value)">
            <option value="ENABLE">ENABLE</option>
            <option value="DISABLE">DISABLE</option>
          </select>
        </div>
        <div class="field">
          <label>确认状态</label>
          <select :value="m.confirmStatus" @change="setMemberField(i, 'confirmStatus', $event.target.value)">
            <option value="CONFIRMED">CONFIRMED</option>
            <option value="PENDING">PENDING</option>
          </select>
        </div>
      </div>
      <div class="field">
        <label>约束描述</label>
        <input :value="m.constraintDescription" placeholder="可选" @input="setMemberField(i, 'constraintDescription', $event.target.value)" />
      </div>
      <TermsEditor
        :model-value="m.terms || []"
        :options="instanceOptions"
        :category-options="categoryOptions"
        @update:model-value="(v) => setMemberTerms(i, v)"
      />
    </div>

    <button type="button" class="t-add" @click="addMember">＋ 添加组成员</button>
  </div>
</template>
