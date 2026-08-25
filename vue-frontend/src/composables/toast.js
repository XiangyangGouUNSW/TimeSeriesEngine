import { reactive } from 'vue'

// 全局 Toast 消息队列
export const toasts = reactive([])

let seq = 0

export function toast(type, message, duration) {
  const id = ++seq
  toasts.push({ id, type, message })
  setTimeout(() => dismiss(id), duration)
}

export function dismiss(id) {
  const i = toasts.findIndex((t) => t.id === id)
  if (i >= 0) toasts.splice(i, 1)
}

export function toastError(message) {
  toast('error', message, 6000)
}

export function toastSuccess(message) {
  toast('success', message, 2500)
}
