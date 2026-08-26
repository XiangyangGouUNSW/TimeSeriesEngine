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

const routes = [
  {
    path: '/',
    component: Layout,
    redirect: '/instances',
    children: [
      { path: 'instances', component: CrudPage, props: { config: pages.instances } },
      { path: 'categories', component: CrudPage, props: { config: pages.categories } },
      { path: 'constraints', component: CrudPage, props: { config: pages.constraints } },
      { path: 'relations', component: CrudPage, props: { config: pages.relations } },
      { path: 'anomaly-tasks', component: CrudPage, props: { config: pages.anomalyTasks } },
      { path: 'forecast-tasks', component: CrudPage, props: { config: pages.forecastTasks } },
      { path: 'events', component: CrudPage, props: { config: pages.events } },
      { path: 'data', component: DataPage },
      { path: 'window-config', component: WindowConfigPage },
      { path: 'derived-series', component: DerivedSeriesPage },
      { path: 'statistics', component: StatisticsPage },
      { path: 'decision', component: DecisionPage },
      { path: 'results', component: ResultsPage },
    ],
  },
]

export default createRouter({
  history: createWebHashHistory(),
  routes,
})
