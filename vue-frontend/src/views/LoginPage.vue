<script setup>
import { reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { authStore } from '../stores/auth'

const router = useRouter()
const route = useRoute()
const form = reactive({ username: 'admin', password: 'admin123' })
const errorMessage = ref('')
const submitting = ref(false)

async function submit() {
  errorMessage.value = ''
  submitting.value = true
  try {
    await authStore.login(form)
    const redirect = typeof route.query.redirect === 'string'
      ? route.query.redirect
      : authStore.defaultPath()
    await router.replace(redirect)
  } catch (error) {
    errorMessage.value = error.message || '登录失败'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <main class="login-page">
    <section class="login-panel">
      <div class="login-mark">⏱</div>
      <h1>时序数据管理系统</h1>
      <p class="login-subtitle">登录后访问项目数据和分析功能</p>

      <form @submit.prevent="submit">
        <label>
          用户名
          <input v-model.trim="form.username" autocomplete="username" required />
        </label>
        <label>
          密码
          <input
            v-model="form.password"
            type="password"
            autocomplete="current-password"
            required
          />
        </label>
        <p v-if="errorMessage" class="login-error">{{ errorMessage }}</p>
        <button class="primary login-button" type="submit" :disabled="submitting">
          {{ submitting ? '登录中...' : '登录' }}
        </button>
      </form>
    </section>
  </main>
</template>

<style scoped>
.login-page {
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 24px;
  background: var(--bg);
}

.login-panel {
  width: min(100%, 390px);
  padding: 36px;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: 0 16px 45px rgba(31, 45, 61, 0.08);
}

.login-mark {
  width: 44px;
  height: 44px;
  display: grid;
  place-items: center;
  margin-bottom: 18px;
  border-radius: 10px;
  color: #fff;
  background: var(--primary);
  font-size: 22px;
}

h1 {
  margin-bottom: 8px;
  font-size: 24px;
}

.login-subtitle {
  margin: 0 0 26px;
  color: var(--muted);
}

label {
  display: block;
  margin-bottom: 16px;
  color: var(--text);
  font-size: 13px;
  font-weight: 600;
}

label input {
  margin-top: 7px;
}

.login-error {
  margin: 0 0 14px;
  color: var(--fail);
}

.login-button {
  width: 100%;
  margin-top: 4px;
  padding: 10px 14px;
}
</style>
