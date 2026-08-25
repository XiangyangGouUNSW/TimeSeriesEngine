<script setup>
import RefInput from './RefInput.vue'

// 约束项（terms）编辑器：逐项配置 变量名 / 映射序列 / 系数 / 采样偏移
// 变量名 → 映射序列 的对应关系会自动汇总为 variableMapping 随请求提交
const props = defineProps({
  modelValue: { type: Array, default: () => [] },
  options: { type: Array, default: () => [] }, // 映射序列下拉选项（已有实例列表）
})
const emit = defineEmits(['update:modelValue'])

const seqField = { name: 'sequenceId', placeholder: '按子串匹配选择已有实例' }

function addTerm() {
  emit('update:modelValue', [
    ...(props.modelValue || []),
    { variable: '', sequenceId: '', coefficient: 1.0, sampleOffset: 0 },
  ])
}

function remove(i) {
  const next = [...(props.modelValue || [])]
  next.splice(i, 1)
  emit('update:modelValue', next)
}

function setField(i, key, value) {
  const next = (props.modelValue || []).map((t) => ({ ...t }))
  next[i] = { ...next[i], [key]: value }
  emit('update:modelValue', next)
}
</script>

<template>
  <div class="terms-editor">
    <div v-if="modelValue.length" class="term-head">
      <span class="t-var">变量名</span>
      <span class="t-seq">映射序列ID</span>
      <span class="t-coef">系数（无量纲）</span>
      <span class="t-offset">采样偏移（步）</span>
      <span class="t-del-slot"></span>
    </div>

    <div v-for="(t, i) in modelValue" :key="i" class="term-row">
      <input
        class="t-var"
        type="text"
        :value="t.variable"
        placeholder="如 x（表达式中的变量名）"
        @input="setField(i, 'variable', $event.target.value)"
      />
      <div class="t-seq">
        <RefInput
          :field="seqField"
          :options="options"
          :model-value="t.sequenceId || ''"
          @update:model-value="(v) => setField(i, 'sequenceId', v)"
        />
      </div>
      <input
        class="t-coef"
        type="number"
        step="any"
        :value="t.coefficient"
        placeholder="系数（如 1）"
        @input="setField(i, 'coefficient', $event.target.value === '' ? undefined : Number($event.target.value))"
      />
      <input
        class="t-offset"
        type="number"
        step="any"
        :value="t.sampleOffset"
        placeholder="采样偏移（可选）"
        @input="setField(i, 'sampleOffset', $event.target.value === '' ? undefined : Number($event.target.value))"
      />
      <button type="button" class="t-del" @click="remove(i)">删除</button>
    </div>

    <button type="button" class="t-add" @click="addTerm">＋ 添加约束项</button>
    <small class="t-hint">
      说明：变量名需与「约束表达式」中的变量一致（如表达式 x &lt; 40 中的 x）；
      映射序列为改变量绑定的实例序列（按子串匹配的下拉框选择）；
      每个约束项构成 系数 × 变量(t − 采样偏移) 的线性项，系数与采样偏移留空则忽略该项；
      所有「变量名 → 映射序列」会自动生成 variableMapping 随请求提交，无需单独填写。
    </small>
  </div>
</template>
