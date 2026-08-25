<script setup>
import { watch } from 'vue'
import { toastError } from '../composables/toast'

// 报错信息改为顶部弹出提示（Toast），不再显示在页面底部；
// 底部仅展示成功/失败请求的响应数据 JSON。
const props = defineProps({
  response: { type: Object, default: null },
  error: { type: String, default: '' },
})

watch(
  () => props.error,
  (v) => {
    if (v) toastError(v)
  }
)
</script>

<template>
  <div v-if="response" class="result">
    <div class="result-line">
      <span class="badge" :class="response.success ? 'ok' : 'fail'">
        {{ response.success ? '成功' : '失败' }}
      </span>
      <span class="msg">{{ response.message }}</span>
    </div>
    <pre class="json">{{ JSON.stringify(response, null, 2) }}</pre>
  </div>
</template>
