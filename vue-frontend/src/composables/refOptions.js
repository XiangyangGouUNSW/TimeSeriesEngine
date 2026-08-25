import { reactive } from 'vue'
import { api } from '../api/timeseries'
import { projectContext } from '../stores/project'

// 引用型下拉选项来源：按当前项目（projectContext）从后端查询已有实体列表
export const REF_SOURCES = {
  category: { loader: (projectId) => api.queryCategories({ projectId }), idKey: 'categoryId', labelKey: 'categoryName' },
  instance: { loader: (projectId) => api.queryInstances({ projectId }), idKey: 'sequenceId', labelKey: 'instanceName' },
  constraint: { loader: (projectId) => api.queryConstraints({ projectId }), idKey: 'constraintId', labelKey: 'constraintName' },
  relation: { loader: (projectId) => api.queryRelations({ projectId }), idKey: 'relationId', labelKey: 'relationName' },
  event: { loader: (projectId) => api.queryEvents({ projectId }), idKey: 'eventId', labelKey: 'eventName' },
  anomalyTask: { loader: (projectId) => api.queryAnomalyTasks({ projectId }), idKey: 'taskId', labelKey: 'taskName' },
  forecastTask: { loader: (projectId) => api.queryForecastTasks({ projectId }), idKey: 'taskId', labelKey: 'taskName' },
  task: { loader: null, idKey: 'taskId', labelKey: 'taskName' }, // 异常+预测任务合并
}

export function useRefOptions() {
  const refOptions = reactive({})

  async function loadTypes(types) {
    const projectId = projectContext.currentProjectId.value
    for (const type of types || []) {
      if (!projectId) {
        refOptions[type] = []
        continue
      }
      try {
        let list = []
        if (type === 'task') {
          const [a, b] = await Promise.all([
            api.queryAnomalyTasks({ projectId }),
            api.queryForecastTasks({ projectId }),
          ])
          const seen = new Set()
          for (const t of [...(a.data || []), ...(b.data || [])]) {
            if (!t || seen.has(t.taskId)) continue
            seen.add(t.taskId)
            list.push(t)
          }
        } else {
          const res = await REF_SOURCES[type].loader(projectId)
          list = res.data || []
        }
        const { idKey, labelKey } = REF_SOURCES[type]
        refOptions[type] = list
          .filter((it) => it && it[idKey])
          .map((it) => ({ value: it[idKey], label: it[labelKey] || it[idKey] }))
      } catch (e) {
        refOptions[type] = []
      }
    }
  }

  return { refOptions, loadTypes }
}
