<template>
  <section v-if="data || error" class="notice-tags" aria-live="polite">
    <h4>通告推荐标签（仅供现场核对，不代表已给告警打标）</h4>
    <p v-if="error">{{ error }}</p>
    <LoadingIndicator v-else-if="data?.status === 'pending'">正在后台生成推荐标签…</LoadingIndicator>
    <template v-else-if="data?.status === 'ready'">
      <p>{{ data.title ? data.title + ' · ' : '' }}{{ data.action === 'start' ? '开始' : '更新' }}</p>
      <div v-for="(tag, index) in data.tags" :key="index">
        <strong>【{{ tag.label }}】{{ tag.content }}</strong>
      </div>
    </template>
    <p v-else>{{ data?.error || '推荐标签获取失败，通告业务不受影响。' }}</p>
    <p v-if="data?.message_warning">{{ data.message_warning }}</p>
  </section>
</template>

<script setup lang="ts">
import { onActivated, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from 'vue';
import { requestJson, type Dict } from '../api/client';
const props = defineProps<{ recordId: string; workType: string }>();
const data = ref<Dict | null>(null), error = ref('');
let timer = 0, sequence = 0, controller: AbortController | null = null;
let active = true;
function stop(): void { sequence++; window.clearTimeout(timer); controller?.abort(); controller = null; }
async function read(): Promise<void> {
  stop();
  if (!active || !props.recordId || document.hidden) return;
  const current = sequence;
  controller = new AbortController();
  try {
    const value = await requestJson('/api/notice-alert-tags?' + new URLSearchParams({ target_record_id: props.recordId, work_type: props.workType }),
      { signal: controller.signal, timeoutMs: 8000 });
    if (current !== sequence) return;
    data.value = value?.status ? value : null;
    error.value = '';
    if (data.value?.status === 'pending') timer = window.setTimeout(read, 5000);
    else if (data.value?.status === 'failed') timer = window.setTimeout(read,
      Math.max(10000, Math.min(60000, (Number(data.value.retry_after || 0) * 1000) - Date.now())));
  } catch {
    if (current === sequence) error.value = '推荐标签暂不可用，通告业务不受影响。';
  }
}
watch(() => [props.recordId, props.workType], () => { data.value = null; error.value = ''; void read(); }, { immediate: true });
function visibility(): void { if (document.hidden) stop(); else void read(); }
onMounted(() => document.addEventListener('visibilitychange', visibility));
onActivated(() => { active = true; void read(); });
onDeactivated(() => { active = false; stop(); });
onBeforeUnmount(() => { stop(); document.removeEventListener('visibilitychange', visibility); });
</script>

<style scoped>
.notice-tags { border-top: 1px solid #dce5ef; padding-top: 12px; margin-top: 14px; font-size: 13px; line-height: 1.6; overflow-wrap: anywhere; }
h4 { margin: 0 0 10px; font-size: 14px; }
p { margin: 5px 0 10px; white-space: pre-wrap; color: #425269; }
</style>
