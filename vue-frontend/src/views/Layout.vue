<script setup>
import { useRoute } from 'vue-router'
import ProjectBar from '../components/ProjectBar.vue'

const route = useRoute()

const nav = [
  { path: '/instances', label: '实例配置' },
  { path: '/categories', label: '语义类别' },
  { path: '/constraints', label: '语义约束' },
  { path: '/relations', label: '语义关系' },
  { path: '/data', label: '数据管理' },
  { path: '/window-config', label: '窗口配置' },
  { path: '/derived-series', label: '派生序列' },
  { path: '/anomaly-tasks', label: '异常检测任务' },
  { path: '/forecast-tasks', label: '预测任务' },
  { path: '/events', label: '事件管理' },
  { path: '/results', label: '结果查询' },
  { path: '/statistics', label: '相关性统计' },
  { path: '/decision', label: '决策辅助' },
  { path: '/cache', label: '缓存管理' },
]
</script>

<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">⏱ Timeseries</div>
      <nav>
        <router-link
          v-for="item in nav"
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
