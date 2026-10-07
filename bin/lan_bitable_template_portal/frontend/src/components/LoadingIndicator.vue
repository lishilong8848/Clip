<template>
  <span class="loading-indicator" role="status" aria-live="polite"><LoaderCircle :size="18" class="loading-spinner" aria-hidden="true" /><slot /></span>
</template>

<script setup lang="ts">
import { LoaderCircle } from "lucide-vue-next";
</script>

<style scoped>
.loading-indicator { display: inline-flex; align-items: center; gap: 7px; color: inherit; vertical-align: middle; }
.loading-spinner { flex: 0 0 auto; animation: loading-turn 1s linear infinite; }
@keyframes loading-turn { to { transform: rotate(360deg); } }
</style>
