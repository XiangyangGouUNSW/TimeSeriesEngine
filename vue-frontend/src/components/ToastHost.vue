<script setup>
import { toasts, dismiss } from '../composables/toast'
</script>

<template>
  <div class="toast-host">
    <TransitionGroup name="toast">
      <div
        v-for="t in toasts"
        :key="t.id"
        class="toast"
        :class="t.type"
        role="alert"
        @click="dismiss(t.id)"
      >
        <span class="toast-icon">{{ t.type === 'error' ? '⚠' : '✓' }}</span>
        <span class="toast-msg">{{ t.message }}</span>
        <span class="toast-close" title="关闭">×</span>
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.toast-host {
  position: fixed;
  top: 16px;
  right: 16px;
  z-index: 999;
  display: flex;
  flex-direction: column;
  gap: 10px;
  max-width: 440px;
}

.toast {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 12px 14px;
  border-radius: 10px;
  font-size: 13.5px;
  line-height: 1.5;
  cursor: pointer;
  box-shadow: 0 8px 24px rgba(16, 24, 40, 0.16);
  border: 1px solid transparent;
}

.toast.error {
  background: #fdeaea;
  border-color: #f5c6c7;
  color: #b3261e;
}

.toast.success {
  background: #e3f6ee;
  border-color: #b9e7d1;
  color: #0b7a4b;
}

.toast-icon {
  font-weight: 700;
  flex-shrink: 0;
}

.toast-msg {
  flex: 1;
  word-break: break-all;
}

.toast-close {
  color: inherit;
  opacity: 0.5;
  flex-shrink: 0;
}

.toast:hover .toast-close {
  opacity: 1;
}

.toast-enter-active,
.toast-leave-active {
  transition: all 0.25s ease;
}

.toast-enter-from {
  opacity: 0;
  transform: translateX(24px);
}

.toast-leave-to {
  opacity: 0;
  transform: translateX(24px);
}
</style>
