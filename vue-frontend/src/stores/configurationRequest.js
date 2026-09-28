import { computed, ref } from 'vue'

const pendingCount = ref(0)
const activeLabel = ref('正在提交配置，等待服务端确认…')

export const configurationRequest = {
  locked: computed(() => pendingCount.value > 0),
  label: computed(() => activeLabel.value),
}

export function isConfigurationWrite(config = {}) {
  if (config.meta?.lockPage === false) return false
  if (config.meta?.lockPage === true) return true

  const method = String(config.method || 'get').toLowerCase()
  if (!['post', 'put', 'patch'].includes(method)) return false

  const url = String(config.url || '')
  if (url.includes('/query') || url.includes('/history/') || url.includes('/window/query')) {
    return false
  }

  return [
    '/api/timeseries/projects',
    '/api/timeseries/instances',
    '/api/timeseries/semantic/',
    '/api/timeseries/window-config',
    '/api/timeseries/derived-series',
    '/api/timeseries/anomaly-tasks',
    '/api/timeseries/forecast-tasks',
  ].some((path) => url.startsWith(path))
}

export function beginConfigurationRequest(config) {
  if (!isConfigurationWrite(config)) return
  config.__configurationLock = true
  activeLabel.value = '正在提交配置，等待服务端确认…'
  pendingCount.value += 1
}

export function endConfigurationRequest(config) {
  if (!config?.__configurationLock) return
  config.__configurationLock = false
  pendingCount.value = Math.max(0, pendingCount.value - 1)
}
