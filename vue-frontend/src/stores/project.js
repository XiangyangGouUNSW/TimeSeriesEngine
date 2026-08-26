import { ref } from 'vue'
import { api } from '../api/timeseries'

const STORAGE_KEY = 'timeseries.currentProjectId'

export const projectContext = {
  projects: ref([]),
  currentProjectId: ref(localStorage.getItem(STORAGE_KEY) || ''),
  loading: ref(false),
}

export async function loadProjects() {
  projectContext.loading.value = true
  try {
    const response = await api.listProjects()
    if (response?.success === false) {
      throw new Error(response.message || '项目列表查询失败')
    }
    const projects = response?.data || []
    const current = projectContext.currentProjectId.value
    const exists = projects.some((project) => project.projectId === current)
    if (!exists && current) {
      // 后端项目目录可能滞后（DataIngest 关闭时新项目未自动注册）：
      // 保留本地选择并补进下拉选项，避免被静默切到其他项目导致查询无数据。
      projects.push({ projectId: current, databaseName: '', status: 'ACTIVE' })
    } else if (!current && projects.length) {
      projectContext.currentProjectId.value = projects[0].projectId
      persistCurrentProject()
    }
    projectContext.projects.value = projects
    return response
  } finally {
    projectContext.loading.value = false
  }
}

export function selectProject(projectId) {
  projectContext.currentProjectId.value = projectId || ''
  persistCurrentProject()
}

export async function createProject(projectId) {
  const response = await api.createProject({ projectId: String(projectId || '').trim() })
  if (response?.success === false) {
    throw new Error(response.message || '项目创建失败')
  }
  await loadProjects()
  selectProject(response?.data?.projectId || projectId)
  return response
}

export function persistCurrentProject() {
  if (projectContext.currentProjectId.value) {
    localStorage.setItem(STORAGE_KEY, projectContext.currentProjectId.value)
  } else {
    localStorage.removeItem(STORAGE_KEY)
  }
}

// 所有业务请求都通过当前项目限定范围，避免页面忘记填写 projectId。
export function withCurrentProject(payload = {}) {
  const projectId = projectContext.currentProjectId.value
  if (!projectId) return null
  if (payload.projectId && payload.projectId !== projectId) return null
  return { ...payload, projectId }
}
