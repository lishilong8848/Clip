<template>
  <Teleport to="body">
    <button class="assistant-entry-fallback" type="button" :disabled="!error"
      :title="error ? '助手加载失败，刷新页面重试' : '正在载入灯塔助手'"
      :aria-label="error ? '助手加载失败，刷新页面重试' : '正在载入灯塔助手'" @click="reload">
      <Bot v-if="error" :size="26" /><Loader2 v-else :size="26" class="spin" /><span>{{ error ? '刷新重试' : '加载中' }}</span>
    </button>
  </Teleport>
</template>

<script setup lang="ts">
import { Bot, Loader2 } from 'lucide-vue-next';
defineProps<{ error?: Error }>();
function reload(): void { window.location.reload(); }
</script>

<style scoped>
.assistant-entry-fallback { position: fixed; right: 24px; bottom: 24px; z-index: 10200; width: 76px; height: 76px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 3px; border: 1px solid #0f766e; border-radius: 50%; background: #0f766e; color: #fff; font: 12px sans-serif; cursor: pointer; box-shadow: 0 4px 18px #122c302e; }
.assistant-entry-fallback:disabled { cursor: progress; opacity: 1; }
.assistant-entry-fallback:focus-visible { outline: 3px solid #14b8a6; outline-offset: 3px; }
.spin { animation: loading-spin 1s linear infinite; }
@keyframes loading-spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .spin { animation: none; } }
@media print { .assistant-entry-fallback { display: none; } }
</style>
