<script setup>
import { ref } from 'vue'
import RefInput from './RefInput.vue'

// 约束项（terms）编辑器：逐项配置 变量名 / 映射目标（实例或类别） / 系数 / 采样偏移
// 「变量名 → 映射ID」自动汇总为 variableMapping 随请求提交；
// 约束级别按钮：实例级（映射到具体序列）/ 类别级（映射到类别，后端自动展开为该类别下所有实例）
const props = defineProps({
  modelValue: { type: Array, default: () => [] },
  options: { type: Array, default: () => [] }, // 实例选项
  categoryOptions: { type: Array, default: () => [] }, // 类别选项
})
const emit = defineEmits(['update:modelValue'])

const level = ref('sequence') // sequence=实例级 | category=类别级

function setLevel(type) {
  level.value = type
  // 切换级别时，统一改变所有已有行的映射类型
  emit('update:modelValue', (props.modelValue || []).map((t) => ({ ...t, mappingType: type })))
}

function addTerm() {
  emit('update:modelValue', [
    ...(props.modelValue || []),
    { variable: '', mappingType: level.value, mappingId: '', coefficient: 1.0, sampleOffset: 0 },
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
    <div class="terms-level">
      <span class="terms-level-label">约束级别</span>
      <button type="button" :class="{ active: level === 'sequence' }" @click="setLevel('sequence')">
        实例级（映射到具体序列）
      </button>
      <button type="button" :class="{ active: level === 'category' }" @click="setLevel('category')">
        类别级（映射到类别，自动展开该类别下所有实例）
      </button>
    </div>

    <div v-if="modelValue.length" class="term-head">
      <span class="t-var">变量名</span>
      <span class="t-type">类型</span>
      <span class="t-seq">映射ID</span>
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
      <select
        class="t-type"
        :value="t.mappingType || 'sequence'"
        @change="setField(i, 'mappingType', $event.target.value)"
      >
        <option value="sequence">实例</option>
        <option value="category">类别</option>
      </select>
      <div class="t-seq">
        <RefInput
          :field="{
            name: 'mappingId',
            placeholder: (t.mappingType || 'sequence') === 'category' ? '按子串匹配选择已有类别' : '按子串匹配选择已有实例',
          }"
          :options="(t.mappingType || 'sequence') === 'category' ? categoryOptions : options"
          :model-value="t.mappingId || ''"
          @update:model-value="(v) => setField(i, 'mappingId', v)"
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
      映射目标可选「实例」（具体序列 ID）或「类别」（类别 ID，后端自动展开为该类别下所有实例）；
      每个约束项构成 系数 × 变量(t − 采样偏移) 的线性项，系数与采样偏移留空则忽略该项；
      所有「变量名 → 映射ID」会自动生成 variableMapping 随请求提交，无需单独填写。
    </small>
  </div>
</template>
