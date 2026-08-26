<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ProjectBar from '../components/ProjectBar.vue'
import { authStore } from '../stores/auth'

const route = useRoute()
const router = useRouter()

const nav = [
  { path: '/instances', label: '实例配置', permission: 'CONFIG_INFO' },
  { path: '/categories', label: '语义类别', permission: 'CONFIG_INFO' },
  { path: '/constraints', label: '语义约束', permission: 'CONFIG_INFO' },
  { path: '/relations', label: '语义关系', permission: 'CONFIG_INFO' },
  { path: '/data', label: '数据管理', permission: 'HISTORY_DATA' },
  { path: '/window-config', label: '窗口配置', permission: 'CONFIG_INFO' },
  { path: '/derived-series', label: '派生序列', permission: 'CONFIG_INFO' },
  { path: '/anomaly-tasks', label: '异常检测任务', permission: 'TASK_INFO' },
  { path: '/forecast-tasks', label: '预测任务', permission: 'TASK_INFO' },
  { path: '/events', label: '事件管理', permission: 'TASK_INFO' },
  { path: '/results', label: '结果查询', permission: 'TASK_INFO' },
  { path: '/statistics', label: '相关性统计', permission: 'HISTORY_DATA' },
  { path: '/decision', label: '决策辅助', permission: 'TASK_INFO' },
]

const visibleNav = computed(() => nav.filter((item) => authStore.hasPermission(item.permission)))

async function logout() {
  await authStore.logout()
  await router.replace('/login')
}
</script>

<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">⏱ Timeseries</div>
      <div class="account-bar">
        <span>{{ authStore.user?.username }}</span>
        <button type="button" title="退出登录" @click="logout">退出</button>
      </div>
      <nav>
        <router-link
          v-for="item in visibleNav"
          :key="item.path"
          :to="item.path"
          :class="{ active: route.path === item.path }"
        >
          {{ item.label }}
        </router-link>
      </nav>
    </aside>
    <main class="content">
      <ProjectBar />
      <router-view :key="route.path" />
    </main>
  </div>
</template>

<style scoped>
.layout {
  display: flex;
  min-height: 100vh;
}

.sidebar {
  width: 210px;
  flex-shrink: 0;
  background: var(--sidebar-bg);
  color: var(--sidebar-text);
  padding: 18px 0;
  position: sticky;
  top: 0;
  height: 100vh;
  overflow-y: auto;
}

.brand {
  color: #fff;
  font-size: 17px;
  font-weight: 700;
  padding: 0 18px 18px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  margin-bottom: 10px;
}

.account-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 18px 14px;
  color: var(--sidebar-text);
  font-size: 12px;
}

.account-bar button {
  padding: 4px 7px;
  color: var(--sidebar-text);
  background: transparent;
  border-color: rgba(255, 255, 255, 0.22);
  font-size: 11px;
}

nav {
  display: flex;
  flex-direction: column;
}

nav a {
  display: block;
  padding: 9px 18px;
  color: var(--sidebar-text);
  text-decoration: none;
  font-size: 13.5px;
  border-left: 3px solid transparent;
}

nav a:hover {
  color: #fff;
  background: rgba(255, 255, 255, 0.05);
}

nav a.active {
  color: #fff;
  background: rgba(47, 111, 237, 0.28);
  border-left-color: #5b8df5;
  font-weight: 600;
}

.content {
  flex: 1;
  padding: 20px 24px;
  max-width: 1400px;
}
</style>
