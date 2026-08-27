import axios from 'axios'

// 所有请求走相对路径，开发模式由 vite proxy 转发到 Java 后端（:8080）
const http = axios.create({
  baseURL: '',
  timeout: 60000,
  withCredentials: true,
  // axios 1.19 默认对同源请求自动附加 XSRF-TOKEN Cookie 的原始值并覆盖拦截器设置，
  // 而 Spring Security 7 只接受 /api/auth/csrf 返回的编码令牌，必须显式关闭。
  withXSRFToken: false,
})

// Spring Security 7 使用 XOR(BREACH) 编码的 CSRF 令牌：
// XSRF-TOKEN Cookie 里是原始值，服务端只接受 /api/auth/csrf 返回的编码值，
// 因此不能直接复制 Cookie，必须用登录/初始化时拿到的编码令牌放进请求头。
let csrfToken = null
export function setCsrfToken(token) {
  csrfToken = token || null
}

http.interceptors.request.use((config) => {
  config.withCredentials = true
  if (csrfToken) {
    config.headers = {
      ...config.headers,
      'X-XSRF-TOKEN': csrfToken,
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
