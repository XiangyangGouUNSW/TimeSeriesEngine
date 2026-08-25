<script setup>
import { onMounted, ref } from 'vue'
import { toastError } from '../composables/toast'
import { createProject, loadProjects, projectContext, selectProject } from '../stores/project'

const newProjectId = ref('')
const error = ref('')

onMounted(async () => {
  try {
    await loadProjects()
  } catch (e) {
    error.value = e.message || String(e)
  }
})

function changeProject(event) {
  selectProject(event.target.value)
}

async function submitProject() {
  const projectId = newProjectId.value.trim()
  if (!projectId) {
    toastError('请输入项目ID')
    return
  }
  error.value = ''
  try {
    await createProject(projectId)
    newProjectId.value = ''
  } catch (e) {
    error.value = e.message || String(e)
  }
}
</script>

<template>
  <header class="project-bar">
    <div class="project-current">
      <label for="current-project">当前项目</label>
      <select id="current-project" :value="projectContext.currentProjectId.value" @change="changeProject">
        <option value="">请选择项目</option>
        <option v-for="project in projectContext.projects.value" :key="project.projectId" :value="project.projectId">
          {{ project.projectId }}
        </option>
      </select>
    </div>
    <form class="project-create" @submit.prevent="submitProject">
      <label for="new-project">创建项目</label>
      <input id="new-project" v-model="newProjectId" placeholder="项目ID" maxlength="128" />
      <button class="primary" type="submit" :disabled="projectContext.loading.value">创建</button>
    </form>
    <span v-if="error" class="project-error">{{ error }}</span>
  </header>
</template>
