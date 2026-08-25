<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

// 引用型可搜索下拉框：
// - 选项来自后端已有实体列表，输入时按子串匹配过滤
// - 自绘下拉列表：仅渲染匹配项，紧挨输入框下方显示，无空位
// - single：单选（可输入自定义值）；multiple：多选（回车/逗号添加，标签可删除）
let counter = 0
const instanceId = `ref_${++counter}_${Math.random().toString(36).slice(2, 8)}`

const props = defineProps({
  field: { type: Object, required: true },
  modelValue: { type: [String, Number], default: '' },
  multiple: { type: Boolean, default: false },
  options: { type: Array, default: () => [] },
})
const emit = defineEmits(['update:modelValue'])

const root = ref(null)
const open = ref(false)
const text = ref('') // 多选：待确认输入；单选：过滤关键字
const activeIndex = ref(-1)

const selected = computed(() =>
  String(props.modelValue || '')
    .split(/[\s,，;]+/)
    .map((s) => s.trim())
    .filter(Boolean)
)

// 字串匹配（包含即命中）：仅返回匹配项，未匹配的不渲染（紧挨显示，无空位）
const suggestions = computed(() => {
  const t = text.value.trim().toLowerCase()
  if (!t) return props.options
  return props.options.filter(
    (o) =>
      String(o.value || '').toLowerCase().includes(t) ||
      String(o.label || '').toLowerCase().includes(t)
  )
})

function select(opt) {
  if (props.multiple) {
    if (!selected.value.includes(opt.value)) {
      emit('update:modelValue', [...selected.value, opt.value].join(', '))
    }
    text.value = ''
  } else {
    emit('update:modelValue', opt.value)
    text.value = ''
  }
  open.value = false
  activeIndex.value = -1
}

function commitText() {
  const v = text.value.trim()
  if (!v) return
  if (!selected.value.includes(v)) {
    emit('update:modelValue', [...selected.value, v].join(', '))
  }
  text.value = ''
}

function onInput(e) {
  text.value = e.target.value
  if (!props.multiple) emit('update:modelValue', e.target.value)
  open.value = true
  activeIndex.value = -1
}

function onFocus() {
  text.value = ''
  open.value = true
  activeIndex.value = -1
}

function onKeydown(e) {
  if (e.key === 'ArrowDown') {
    e.preventDefault()
    open.value = true
    activeIndex.value = Math.min(activeIndex.value + 1, suggestions.value.length - 1)
  } else if (e.key === 'ArrowUp') {
    e.preventDefault()
    activeIndex.value = Math.max(activeIndex.value - 1, -1)
  } else if (e.key === 'Enter') {
    e.preventDefault()
    if (open.value && activeIndex.value >= 0 && suggestions.value[activeIndex.value]) {
      select(suggestions.value[activeIndex.value])
    } else if (props.multiple) {
      commitText()
    }
  } else if (e.key === 'Escape') {
    open.value = false
  } else if (e.key === ',' && props.multiple) {
    e.preventDefault()
    commitText()
  }
}

function onBlur() {
  // 延迟关闭，让点击选项先触发 select
  setTimeout(() => {
    open.value = false
    activeIndex.value = -1
  }, 150)
}

function onDocClick(e) {
  if (root.value && !root.value.contains(e.target)) open.value = false
}

onMounted(() => document.addEventListener('click', onDocClick))
onBeforeUnmount(() => document.removeEventListener('click', onDocClick))

function remove(item) {
  emit('update:modelValue', selected.value.filter((s) => s !== item).join(', '))
}
</script>

<template>
  <div ref="root" class="ref-input">
    <div v-if="multiple" class="chips">
      <span v-for="item in selected" :key="item" class="chip">
        {{ item }}
        <button type="button" class="chip-x" title="移除" @click="remove(item)">×</button>
      </span>
    </div>
    <input
      :value="multiple ? text : modelValue"
      :placeholder="multiple ? field.placeholder || '输入后回车/逗号或选择候选，可多选' : field.placeholder"
      @input="onInput"
      @focus="onFocus"
      @keydown="onKeydown"
      @blur="onBlur"
    />
    <ul v-if="open && suggestions.length" :id="instanceId" class="ref-dropdown">
      <li
        v-for="(o, i) in suggestions"
        :key="o.value"
        :class="{ active: i === activeIndex }"
        @mousedown.prevent="select(o)"
        @mouseenter="activeIndex = i"
      >
        <span class="opt-value">{{ o.value }}</span>
        <span v-if="o.label && o.label !== o.value" class="opt-label">{{ o.label }}</span>
      </li>
    </ul>
  </div>
</template>
