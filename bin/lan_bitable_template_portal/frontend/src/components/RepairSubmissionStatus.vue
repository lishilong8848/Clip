<template>
  <section class="submission-status" role="status" aria-live="polite">
    <div class="submission-main">
      <Clock3 :size="17" aria-hidden="true" />
      <span>{{ failed ? '本次未写入，请保留填写后重新提交。' : text }}</span>
      <button type="button" :disabled="checking" @click="$emit('check')"><RefreshCw :size="15" />{{ checking ? '核验中' : '核验结果' }}</button>
      <button type="button" title="复制原提交内容" @click="$emit('copy')"><Copy :size="15" />保留填写</button>
      <button v-if="failed" type="button" @click="$emit('dismiss')">返回修改</button>
    </div>
    <details v-if="detail"><summary>处理详情</summary><p>{{ detail }}</p></details>
  </section>
</template>

<script setup lang="ts">
import { Clock3, Copy, RefreshCw } from "lucide-vue-next";
defineProps<{ text: string; detail: string; checking: boolean; failed: boolean }>();
defineEmits<{ check: []; copy: []; dismiss: [] }>();
</script>

<style scoped>
.submission-status{border-left:3px solid #c38a17;background:#fff8e8;color:#644708;padding:10px 12px;margin:8px 0;font-size:13px;overflow-wrap:anywhere}
.submission-main{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.submission-main span{flex:1;min-width:200px}
button{display:inline-flex;align-items:center;gap:5px;border:1px solid #d9c8a2;border-radius:4px;background:white;padding:6px 10px;cursor:pointer;color:inherit}
button:disabled{cursor:wait;opacity:.65}summary{cursor:pointer;margin-top:8px}p{margin:6px 0;white-space:pre-wrap}
</style>
