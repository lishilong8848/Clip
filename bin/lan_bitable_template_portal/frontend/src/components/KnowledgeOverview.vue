<template>
  <section class="knowledge-overview" aria-label="知识库概况" :aria-busy="loading">
    <header>
      <strong>公司知识库</strong>
      <button type="button" title="刷新知识库状态" aria-label="刷新知识库状态" :disabled="loading" @click="load">
        <RefreshCw :size="14" :class="{ spin: loading }" />
      </button>
    </header>
    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-else-if="!data && loading" role="status"><Loader2 :size="14" class="spin" />正在读取知识库…</p>
    <template v-else-if="data">
      <div class="metrics"><span><b>{{ data.total }}</b> 份文档</span><span><b>{{ data.settings?.vectors || 0 }}</b> 个索引片段</span></div>
      <p class="engine"><Database :size="13" />BGE small zh · FAISS<small>内存预算 2 GiB</small></p>
      <p v-if="data.settings?.warning && data.total" class="warning" role="status">{{ data.settings.warning }}</p>
      <p v-else-if="!data.total" class="empty">暂无公司资料，请进入知识库上传文档。</p>
      <ul v-if="showDocuments && data.items?.length">
        <li v-for="doc in data.items.slice(0, 4)" :key="doc.id">
          <button type="button" @click="openDocument(doc.id)">
            <FileText :size="14" /><span>{{ doc.name }}</span><small :class="{ error: ['failed', 'blocked'].includes(doc.status) }">{{ statusLabel(doc.status) }}</small>
          </button>
        </li>
      </ul>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue';
import { Database, FileText, Loader2, RefreshCw } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { navigate } from '../navigation';

const props = withDefaults(defineProps<{ active?: boolean; showDocuments?: boolean }>(), { active: true, showDocuments: true });
const data = ref<Dict | null>(null), error = ref(''), loading = ref(false);
let controller: AbortController | undefined, timer: ReturnType<typeof setTimeout> | undefined, disposed = false;
function openDocument(id: string): void { navigate('/knowledge-base?' + new URLSearchParams({ document: id })); }
function statusLabel(status: string): string {
  return ({ ready: '可检索', queued: '待索引', indexing: '索引中', failed: '索引失败', blocked: '需脱敏', deleted: '已删除' } as Record<string, string>)[status] || '待处理';
}
async function load(): Promise<void> {
  if (!props.active || disposed || loading.value) return;
  clearTimeout(timer);
  const current = new AbortController(); controller = current; loading.value = true; error.value = '';
  try {
    const result = await requestJson('/api/assistant/knowledge?page=1', { signal: current.signal, cache: 'no-store', timeoutMs: 15000 });
    if (!Array.isArray(result.items) || !Number.isInteger(result.total)) throw new Error('知识库数据未完整返回，请刷新重试。');
    if (!current.signal.aborted && !disposed) data.value = result;
  } catch (cause) {
    if (!current.signal.aborted && !disposed) error.value = cause instanceof Error ? cause.message : '知识库暂时无法读取，请刷新重试。';
  } finally {
    if (controller === current) loading.value = false;
    if (!disposed && props.active && !current.signal.aborted && data.value?.items?.some((doc: Dict) => ['queued', 'indexing'].includes(doc.status))) timer = setTimeout(load, 10000);
  }
}
watch(() => props.active, active => {
  clearTimeout(timer);
  if (active) void load();
  else { controller?.abort(); controller = undefined; loading.value = false; }
}, { immediate: true });
onBeforeUnmount(() => { disposed = true; clearTimeout(timer); controller?.abort(); });
</script>

<style scoped>
.knowledge-overview{padding:4px 2px 10px;border-bottom:1px solid var(--lh-border,#dae2e8);font-size:12px;line-height:1.5}
header,.metrics,.engine,li button{display:flex;align-items:center;gap:8px}header{justify-content:space-between}header strong{font-size:13px}
button{font:inherit;color:inherit;cursor:pointer;border:0;background:transparent;padding:3px;border-radius:4px}button:hover{background:var(--lh-surface-hover,#edf4fa)}button:focus-visible{outline:2px solid var(--lh-accent,#2878b8);outline-offset:2px}button:disabled{cursor:wait}
.metrics{gap:20px;margin:9px 0}.metrics b{font-size:17px}p{margin:6px 0;display:flex;gap:6px;align-items:center}.engine{flex-wrap:wrap;color:var(--lh-muted,#536b7c)}small{font-size:11px;color:var(--lh-muted,#536b7c)}.engine small{margin-left:auto}
.warning{color:var(--lh-warning,#986a1f)}.error{color:var(--lh-error,#b74842)}.empty{color:var(--lh-muted,#536b7c)}ul{list-style:none;margin:9px 0 0;padding:0}li button{width:100%;text-align:left;padding:6px 2px}li span{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}li small,svg{flex:none}.spin{animation:kb-overview-spin 1s linear infinite}@keyframes kb-overview-spin{to{transform:rotate(360deg)}}
</style>
