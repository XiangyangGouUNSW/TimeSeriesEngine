<script setup>
import ToastHost from './components/ToastHost.vue'
import ConfigurationRequestOverlay from './components/ConfigurationRequestOverlay.vue'
</script>

<template>
  <router-view />
  <ConfigurationRequestOverlay />
  <ToastHost />
</template>
