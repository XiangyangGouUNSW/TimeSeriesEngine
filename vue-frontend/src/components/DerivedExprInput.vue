<script setup>
import { computed } from 'vue'
import RefInput from './RefInput.vue'

// 派生序列表达式递归编辑节点：
//   序列 {sequenceId} ｜ 常数 {constant} ｜ 二元运算 {binary:{operator,left,right}}
const props = defineProps({
  modelValue: { type: Object, default: () => ({ sequenceId: '' }) },
  options: { type: Array, default: () => [] },
  depth: { type: Number, default: 0 }, // 限制嵌套深度
})
const emit = defineEmits(['update:modelValue'])

const OPERATORS = [
  { value: 'ADD', label: '加 +' },
  { value: 'SUBTRACT', label: '减 −' },
  { value: 'MULTIPLY', label: '乘 ×' },
  { value: 'DIVIDE', label: '除 ÷' },
]

const kind = computed(() => {
  const v = props.modelValue || {}
  if (v.binary) return 'binary'
  if (v.sequenceId !== undefined && v.sequenceId !== null) return 'sequence'
  if (v.constant !== undefined && v.constant !== null) return 'constant'
  return 'sequence'
})

function setKind(k) {
  if (k === 'sequence') emit('update:modelValue', { sequenceId: '' })
  else if (k === 'constant') emit('update:modelValue', { constant: 1.0 })
  else {
    emit('update:modelValue', {
      binary: { operator: 'ADD', left: { sequenceId: '' }, right: { constant: 1.0 } },
    })
  }
}

function setSequence(v) {
  emit('update:modelValue', { sequenceId: v })
}

function setConstant(v) {
  emit('update:modelValue', { constant: v === '' ? undefined : Number(v) })
}

function setOperator(v) {
  const node = props.modelValue || {}
  emit('update:modelValue', { ...node, binary: { ...(node.binary || {}), operator: v } })
}

function setChild(side, child) {
  const node = props.modelValue || {}
  emit('update:modelValue', { ...node, binary: { ...(node.binary || {}), [side]: child } })
}
</script>

<template>
  <div class="expr-node">
    <select :value="kind" @change="setKind($event.target.value)">
      <option value="sequence">序列</option>
      <option value="constant">常数</option>
      <option value="binary" :disabled="depth >= 3">二元运算</option>
    </select>

    <template v-if="kind === 'sequence'">
      <div class="expr-seq">
        <RefInput
          :field="{ name: 'seq', placeholder: '按子串匹配选择实例' }"
          :options="options"
          :model-value="(modelValue || {}).sequenceId || ''"
          @update:model-value="setSequence"
        />
      </div>
    </template>

    <template v-else-if="kind === 'constant'">
      <input
        class="expr-const"
        type="number"
        step="any"
        :value="(modelValue || {}).constant"
        placeholder="常数"
        @input="setConstant($event.target.value)"
      />
    </template>

    <template v-else>
      <select
        class="expr-op"
        :value="(modelValue || {}).binary?.operator || 'ADD'"
        @change="setOperator($event.target.value)"
      >
        <option v-for="o in OPERATORS" :key="o.value" :value="o.value">{{ o.label }}</option>
      </select>
      <div class="expr-children">
        <DerivedExprInput
          :model-value="(modelValue || {}).binary?.left || {}"
          :options="options"
          :depth="depth + 1"
          @update:model-value="(v) => setChild('left', v)"
        />
        <DerivedExprInput
          :model-value="(modelValue || {}).binary?.right || {}"
          :options="options"
          :depth="depth + 1"
          @update:model-value="(v) => setChild('right', v)"
        />
      </div>
    </template>
  </div>
</template>
