import axios from 'axios'

let csrfToken = ''

export function setCsrfToken(token) {
  csrfToken = token || ''
}

export function clearCsrfToken() {
  csrfToken = ''
}

// 所有请求走相对路径，开发模式由 vite proxy 转发到 Java 后端（:8080）
const http = axios.create({
  baseURL: '',
  timeout: 60000,
  withCredentials: true,
  xsrfCookieName: 'XSRF-TOKEN',
  xsrfHeaderName: 'X-XSRF-TOKEN',
})

http.interceptors.request.use((config) => {
  config.withCredentials = true
  const method = (config.method || 'get').toLowerCase()
  if (csrfToken && !['get', 'head', 'options'].includes(method)) {
    if (config.headers?.set) {
      config.headers.set('X-XSRF-TOKEN', csrfToken)
    } else {
      config.headers = { ...config.headers, 'X-XSRF-TOKEN': csrfToken }
    }
  }
  return config
})

// 后端统一返回 ApiResult<T> = { success, message, data }
http.interceptors.response.use(
  (response) => response.data,
  (error) => {
    if (error.response?.status === 401 && window.location.hash !== '#/login') {
      window.location.hash = '#/login'
    }
    const detail =
      error.response && error.response.data
        ? error.response.data.message || JSON.stringify(error.response.data)
        : error.message || '网络请求失败'
    return Promise.reject(new Error(detail))
  }
)

export default http
