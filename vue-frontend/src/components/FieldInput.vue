<script setup>
import RefInput from './RefInput.vue'
import TermsEditor from './TermsEditor.vue'

const props = defineProps({
  field: { type: Object, required: true },
  modelValue: { type: [String, Number, Boolean, Array, Object], default: '' },
  options: { type: Array, default: () => [] },
})
const emit = defineEmits(['update:modelValue'])

const jsonPlaceholder = 'JSON 对象，例如 {"a": 1}'

function onInput(e) {
  emit('update:modelValue', e.target.value)
}
</script>

<template>
  <div class="field" :class="{ full: field.full }">
    <label>{{ field.label }}<span v-if="field.unit" class="unit">（{{ field.unit }}）</span><span v-if="field.required" class="req">*</span></label>

    <select
      v-if="field.type === 'select'"
      :value="modelValue"
      @change="onInput"
    >
      <option value=""></option>
      <option v-for="o in field.options" :key="typeof o === 'object' ? o.value : o" :value="typeof o === 'object' ? o.value : o">
        {{ typeof o === 'object' ? o.label : o }}
      </option>
    </select>

    <RefInput
      v-else-if="field.type === 'ref' || field.type === 'refs'"
      :field="field"
      :options="options"
      :multiple="field.type === 'refs'"
      :model-value="modelValue"
      @update:model-value="(v) => $emit('update:modelValue', v)"
    />

    <TermsEditor
      v-else-if="field.type === 'terms'"
      :options="options"
      :model-value="modelValue"
      @update:model-value="(v) => $emit('update:modelValue', v)"
    />

    <input
      v-else-if="field.type === 'number'"
      type="number"
      :value="modelValue"
      :placeholder="field.placeholder"
      @input="onInput"
    />

    <input
      v-else-if="field.type === 'datetime'"
      type="datetime-local"
      :value="modelValue"
      @input="onInput"
    />

    <textarea
      v-else-if="field.type === 'json'"
      :value="modelValue"
      rows="5"
      :placeholder="field.placeholder || jsonPlaceholder"
      @input="onInput"
    ></textarea>

    <textarea
      v-else-if="field.type === 'textarea'"
      :value="modelValue"
      rows="3"
      :placeholder="field.placeholder"
      @input="onInput"
    ></textarea>

    <input
      v-else
      type="text"
      :value="modelValue"
      :placeholder="field.placeholder"
      @input="onInput"
    />

    <small v-if="field.hint">{{ field.hint }}</small>
  </div>
</template>
