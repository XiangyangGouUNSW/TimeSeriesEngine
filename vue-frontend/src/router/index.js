import { createRouter, createWebHashHistory } from 'vue-router'
import Layout from '../views/Layout.vue'
import CrudPage from '../components/CrudPage.vue'
import { pages } from '../config/pages'
import DataPage from '../views/DataPage.vue'
import WindowConfigPage from '../views/WindowConfigPage.vue'
import DerivedSeriesPage from '../views/DerivedSeriesPage.vue'
import StatisticsPage from '../views/StatisticsPage.vue'
import DecisionPage from '../views/DecisionPage.vue'
import ResultsPage from '../views/ResultsPage.vue'
import LoginPage from '../views/LoginPage.vue'
import { authStore } from '../stores/auth'

const routes = [
  { path: '/login', component: LoginPage },
  {
    path: '/',
    component: Layout,
    redirect: '/instances',
    meta: { requiresAuth: true },
    children: [
      { path: 'instances', component: CrudPage, meta: { permission: 'CONFIG_INFO' }, props: { config: pages.instances } },
      { path: 'categories', component: CrudPage, meta: { permission: 'CONFIG_INFO' }, props: { config: pages.categories } },
      { path: 'constraints', component: CrudPage, meta: { permission: 'CONFIG_INFO' }, props: { config: pages.constraints } },
      { path: 'relations', component: CrudPage, meta: { permission: 'CONFIG_INFO' }, props: { config: pages.relations } },
      { path: 'anomaly-tasks', component: CrudPage, meta: { permission: 'TASK_INFO' }, props: { config: pages.anomalyTasks } },
      { path: 'forecast-tasks', component: CrudPage, meta: { permission: 'TASK_INFO' }, props: { config: pages.forecastTasks } },
      { path: 'events', component: CrudPage, meta: { permission: 'TASK_INFO' }, props: { config: pages.events } },
      { path: 'data', component: DataPage, meta: { permission: 'HISTORY_DATA' } },
      { path: 'window-config', component: WindowConfigPage, meta: { permission: 'CONFIG_INFO' } },
      { path: 'derived-series', component: DerivedSeriesPage, meta: { permission: 'CONFIG_INFO' } },
      { path: 'statistics', component: StatisticsPage, meta: { permission: 'HISTORY_DATA' } },
      { path: 'decision', component: DecisionPage, meta: { permission: 'TASK_INFO' } },
      { path: 'results', component: ResultsPage, meta: { permission: 'TASK_INFO' } },
    ],
  },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
})

router.beforeEach(async (to) => {
  const requiresAuth = to.matched.some((record) => record.meta.requiresAuth)
  if (!requiresAuth && to.path === '/login') {
    const authenticated = await authStore.ensureLoaded()
    if (authenticated) return authStore.defaultPath()
    return true
  }

  if (requiresAuth) {
    const authenticated = await authStore.ensureLoaded()
    if (!authenticated) {
      return { path: '/login', query: { redirect: to.fullPath } }
    }
    const permission = to.meta.permission
    if (permission && !authStore.hasPermission(permission)) {
      return authStore.defaultPath()
    }
  }
  return true
})

export default router
