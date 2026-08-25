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
    projectContext.projects.value = response?.data || []
    const exists = projectContext.projects.value.some(
      (project) => project.projectId === projectContext.currentProjectId.value,
    )
    if (!exists) {
      projectContext.currentProjectId.value = projectContext.projects.value[0]?.projectId || ''
      persistCurrentProject()
    }
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
