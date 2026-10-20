<template>
  <aside class="artifact-preview" aria-label="文件预览">
    <header>
      <FileText :size="18" /><strong :title="file.name">{{ file.name }}</strong>
      <a :href="url" download :aria-label="'下载 ' + file.name" title="下载文件"><Download :size="18" /></a>
      <button type="button" aria-label="关闭文件预览" title="关闭预览" @click="$emit('close')"><X :size="20" /></button>
    </header>
    <div v-if="loading" class="preview-state" role="status"><Loader2 class="spin" :size="22" />正在读取文件</div>
    <div v-else-if="error" class="preview-state" role="alert"><AlertCircle :size="22" /><p>{{ error }}</p><button type="button" @click="load">重试</button></div>
    <template v-else>
      <div v-if="['html', 'markdown'].includes(data.kind)" class="preview-modes" role="group" aria-label="文件查看方式">
        <button type="button" :aria-pressed="!source" @click="source = false">预览</button>
        <button type="button" :aria-pressed="source" @click="source = true">{{ data.kind === 'markdown' ? '原文' : '源码' }}</button>
      </div>
      <div v-if="data.truncated" class="preview-warning">当前显示部分内容，下载文件可查看完整内容。</div>
      <div v-if="data.kind === 'spreadsheet'" class="sheet-tabs" role="tablist" aria-label="工作表">
        <button v-for="(sheet, index) in data.sheets" :key="sheet.name" type="button" role="tab" :aria-selected="sheetIndex === index" @click="sheetIndex = index">{{ sheet.name }}</button>
      </div>
      <div class="preview-body" tabindex="0">
        <LighthouseReply v-if="data.kind === 'markdown' && !source" :text="data.text" />
        <iframe v-else-if="data.kind === 'html' && !source" title="网页文件预览（禁止脚本及外部请求）" sandbox="" referrerpolicy="no-referrer" :srcdoc="html" />
        <iframe v-else-if="data.kind === 'pdf'" title="PDF文件预览" :src="url + '?inline=1'" />
        <img v-else-if="data.kind === 'image'" :src="url" :alt="file.name" />
        <table v-else-if="data.kind === 'spreadsheet'"><tbody><tr v-for="(row, index) in data.sheets?.[sheetIndex]?.rows || []" :key="index"><component :is="index === 0 ? 'th' : 'td'" v-for="(cell, col) in row" :key="col">{{ cell }}</component></tr></tbody></table>
        <section v-else-if="data.kind === 'slides'" class="slides"><article v-for="slide in data.slides" :key="slide.title"><h3>{{ slide.title }}</h3><pre>{{ slide.text }}</pre></article></section>
        <pre v-else-if="data.text !== undefined" class="source-text">{{ data.text }}</pre>
        <p v-else>此文件暂不支持在线预览，可下载后查看。</p>
      </div>
    </template>
  </aside>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import DOMPurify from 'dompurify';
import { AlertCircle, Download, FileText, Loader2, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import LighthouseReply from './LighthouseReply.vue';

const props = defineProps<{ file: Dict }>();
defineEmits<{ close: [] }>();
const data = ref<Dict>({}), error = ref(''), loading = ref(false), source = ref(false), sheetIndex = ref(0);
const url = computed(() => /^[a-f0-9]{32}$/.test(props.file.id) ? '/api/assistant/files/' + props.file.id : '');
let controller: AbortController | null = null, serial = 0;
const html = computed(() => {
  if (data.value.kind !== 'html') return '';
  const body = DOMPurify.sanitize(data.value.text || '', { WHOLE_DOCUMENT: true,
    FORBID_TAGS: ['script', 'iframe', 'object', 'embed', 'form', 'input', 'button', 'meta', 'link', 'base', 'audio', 'video'],
    FORBID_ATTR: ['href', 'srcset', 'action', 'formaction', 'target', 'download'] });
  return '<!doctype html><html><head><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src data:; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'"></head><body>' + body + '</body></html>';
});
async function load(): Promise<void> {
  const current = ++serial;
  controller?.abort(); controller = new AbortController();
  data.value = {}; error.value = ''; loading.value = true; source.value = false; sheetIndex.value = 0;
  try {
    if (!url.value) throw new Error('文件编号无效');
    const result = await requestJson(url.value + '?preview=1', { signal: controller.signal, timeoutMs: 30000 });
    if (current === serial) data.value = result;
  } catch (e) {
    if (current === serial) error.value = e instanceof Error ? e.message : '文件暂时无法读取';
  } finally { if (current === serial) loading.value = false; }
}
watch(() => props.file.id, load, { immediate: true });
onBeforeUnmount(() => { serial++; controller?.abort(); });
</script>

<style scoped>
.artifact-preview { display:flex; flex-direction:column; flex:none; width:var(--artifact-panel-width, 480px); min-width:0; height:100%; border-left:1px solid var(--lh-border); background:var(--lh-surface-subtle); color:var(--lh-charcoal); }
header { display:flex; align-items:center; gap:8px; min-height:56px; padding:10px 12px; border-bottom:1px solid var(--lh-border); }
header strong { flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:13px; }
button, a { display:inline-flex; align-items:center; justify-content:center; min-width:32px; min-height:32px; padding:5px 8px; border:1px solid transparent; border-radius:6px; color:inherit; background:transparent; cursor:pointer; text-decoration:none; }
button:hover, a:hover { background:var(--lh-surface-hover); }
button:focus-visible, a:focus-visible, .preview-body:focus-visible { outline:2px solid var(--lh-accent); outline-offset:-2px; }
.preview-state { display:flex; gap:10px; align-items:center; flex-direction:column; padding:40px 20px; font-size:13px; }
.preview-state p { margin:0; }
.preview-body { flex:1; min-height:0; overflow:auto; padding:16px; scrollbar-gutter:stable; }
.preview-body:has(iframe) { padding:0; overflow:hidden; }
iframe { width:100%; height:100%; display:block; border:0; background:white; }
img { display:block; max-width:100%; height:auto; margin:auto; }
pre { margin:0; white-space:pre-wrap; overflow-wrap:anywhere; font:13px/1.7 Consolas,'Microsoft YaHei',monospace; }
.preview-modes,.sheet-tabs { display:flex; flex:none; gap:4px; padding:6px 10px; overflow-x:auto; border-bottom:1px solid var(--lh-border); }
.preview-modes button[aria-pressed=true],.sheet-tabs button[aria-selected=true] { background:var(--lh-accent-soft); color:var(--lh-accent-strong); }
.sheet-tabs button { flex:none; }
.preview-warning { padding:6px 12px; color:var(--lh-warn); font-size:12px; }
table { border-collapse:collapse; font-size:12px; }
td,th { border:1px solid var(--lh-border-strong); padding:7px 10px; min-width:90px; max-width:300px; overflow-wrap:anywhere; text-align:left; }
th { position:sticky; top:0; background:var(--lh-surface-hover); }
.slides { display:grid; gap:16px; }.slides article { padding:18px; border:1px solid var(--lh-border); border-radius:6px; }.slides h3 { margin:0 0 12px; font-size:15px; }
.spin { animation:artifact-spin 1s linear infinite; }@keyframes artifact-spin { to { transform:rotate(360deg); } }
@media(prefers-reduced-motion:reduce) { .spin { animation-duration:2s; } }
</style>
