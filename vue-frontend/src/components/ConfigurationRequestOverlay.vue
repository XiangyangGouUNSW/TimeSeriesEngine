<script setup>
import { configurationRequest } from '../stores/configurationRequest'
</script>

<template>
  <Teleport to="body">
    <div v-if="configurationRequest.locked.value" class="configuration-lock" role="alert" aria-live="assertive">
      <div class="configuration-lock-panel" aria-busy="true">
        <span class="configuration-spinner" aria-hidden="true"></span>
        <div>
          <strong>配置提交中</strong>
          <p>{{ configurationRequest.label.value }}</p>
        </div>
      </div>
    </div>
  </Teleport>
</template>
