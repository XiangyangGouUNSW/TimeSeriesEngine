import axios from 'axios'

// 所有请求走相对路径，开发模式由 vite proxy 转发到 Java 后端（:8080）
const http = axios.create({
  baseURL: '',
  timeout: 60000,
})

// 后端统一返回 ApiResult<T> = { success, message, data }
http.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const detail =
      error.response && error.response.data
        ? error.response.data.message || JSON.stringify(error.response.data)
        : error.message || '网络请求失败'
    return Promise.reject(new Error(detail))
  }
)

export default http
