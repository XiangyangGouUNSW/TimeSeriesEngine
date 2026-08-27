import { reactive } from 'vue'
import { api } from '../api/timeseries'
import { setCsrfToken } from '../api/http'

const state = reactive({
  user: null,
  initialized: false,
  loading: false,
  loadPromise: null,
})

async function initializeCsrfCookie() {
  const csrf = await api.csrfToken()
  if (!csrf.success || !csrf.data?.token) {
    throw new Error('CSRF token query failed')
  }
  setCsrfToken(csrf.data.token)
}

export const authStore = {
  get user() {
    return state.user
  },

  get isAuthenticated() {
    return Boolean(state.user)
  },

  get permissions() {
    return state.user?.permissions || []
  },

  hasPermission(permission) {
    return this.permissions.includes(permission)
  },

  async ensureLoaded() {
    if (state.initialized) return Boolean(state.user)
    if (state.loadPromise) return state.loadPromise

    state.loading = true
    state.loadPromise = api.currentUser()
      .then(async (response) => {
        state.user = response.success ? response.data : null
        if (state.user) {
          await initializeCsrfCookie()
        }
        return Boolean(state.user)
      })
      .catch(() => {
        state.user = null
        return false
      })
      .finally(() => {
        state.loading = false
        state.initialized = true
        state.loadPromise = null
      })
    return state.loadPromise
  },

  async login(credentials) {
    const response = await api.login(credentials)
    if (!response.success) {
      throw new Error(response.message || '登录失败')
    }
    state.user = response.data
    state.initialized = true
    try {
      await initializeCsrfCookie()
    } catch (error) {
      state.user = null
      throw error
    }
    return state.user
  },

  async logout() {
    try {
      await api.logout()
    } finally {
      state.user = null
      state.initialized = true
      setCsrfToken(null)
    }
  },

  defaultPath() {
    if (this.hasPermission('HISTORY_DATA')) return '/data'
    if (this.hasPermission('CONFIG_INFO')) return '/instances'
    if (this.hasPermission('TASK_INFO')) return '/anomaly-tasks'
    return '/login'
  },
}
