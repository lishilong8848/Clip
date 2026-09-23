<template>
  <section class="submission-status" role="status" aria-live="polite">
    <div class="submission-main">
      <Clock3 :size="17" aria-hidden="true" />
      <span>{{ text }}</span>
      <button type="button" :disabled="checking" @click="$emit('check')"><RefreshCw :size="15" />{{ checking ? '确认中' : '刷新状态' }}</button>
    </div>
  </section>
</template>

<script setup lang="ts">
import { Clock3, RefreshCw } from "lucide-vue-next";
defineProps<{ text: string; checking: boolean }>();
defineEmits<{ check: [] }>();
</script>

<style scoped>
.submission-status{border-left:3px solid #c38a17;background:#fff8e8;color:#644708;padding:8px 12px;margin:8px 0;font-size:13px;overflow-wrap:anywhere}
.submission-main{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.submission-main span{flex:1;min-width:200px}
button{display:inline-flex;align-items:center;gap:5px;border:1px solid #d9c8a2;border-radius:4px;background:white;padding:5px 9px;cursor:pointer;color:inherit}
button:disabled{cursor:wait;opacity:.65}
</style>
