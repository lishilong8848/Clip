<template>
  <section class="skill-manager" aria-label="技能管理">
    <header><button type="button" class="skill-icon" :aria-label="detail ? '返回技能列表' : '返回会话'" @click="detail ? detail = null : $emit('close')"><ArrowLeft :size="18" /></button><strong>{{ detail ? detail.label : '技能' }}</strong>
      <button v-if="!detail" type="button" class="install" :disabled="installing" @click="fileInput?.click()"><Loader2 v-if="installing" :size="15" class="spin" /><Upload v-else :size="15" />{{ installing ? '安装中' : '安装技能' }}</button>
      <input ref="fileInput" type="file" accept=".md,.zip" hidden @change="install" />
    </header>
    <div v-if="error" class="skill-error" role="alert">{{ error }}</div>
    <p v-if="notice" class="skill-notice" role="status">{{ notice }}</p>
    <div v-if="loading" class="skill-state" role="status"><Loader2 :size="18" class="spin" />读取中</div>
    <template v-else-if="detail">
      <div class="skill-detail">
        <p>{{ detail.description }}</p><p v-for="warning in detail.warnings || []" :key="warning" class="skill-notice">{{ warning }}</p>
        <div class="detail-actions"><button type="button" class="install" @click="use(detail)"><BookOpen :size="15" />用于本轮</button><button v-if="detail.removable" type="button" class="skill-icon danger" aria-label="删除共享技能" title="删除共享技能" @click="deleteTarget = detail"><Trash2 :size="17" /></button></div>
        <div v-if="reading" class="skill-state"><Loader2 :size="16" class="spin" />读取说明</div>
        <LighthouseReply v-else :text="guide" />
        <button v-if="nextOffset" type="button" class="install" :disabled="reading" @click="readGuide(detail, nextOffset, currentReference)">继续读取</button>
        <div v-if="references.length" class="skill-references"><button v-for="reference in references" :key="reference" type="button" @click="readGuide(detail, 0, reference)"><FileText :size="14" />{{ reference }}</button></div>
      </div>
    </template>
    <template v-else>
      <div class="skill-filters"><label><Search :size="16" /><input v-model="query" placeholder="搜索技能" aria-label="搜索技能" /></label><select v-model="source" aria-label="技能来源"><option value="">全部</option><option value="shared">共享</option><option value="builtin">内置</option></select><button type="button" class="skill-icon" title="刷新技能" aria-label="刷新技能" @click="load"><RefreshCw :size="16" /></button></div>
      <div class="skill-list">
        <button v-for="item in filtered" :key="item.name" type="button" @click="show(item)"><BookOpen :size="18" /><span><strong>{{ item.label }}</strong><small>{{ item.description }}</small></span><em>{{ item.source === 'shared' ? '共享' : '内置' }}</em><ChevronRight :size="15" /></button>
        <p v-if="!filtered.length" class="skill-state">{{ error ? '技能列表读取未完成' : '没有匹配的技能' }}</p>
      </div>
      <footer>{{ filtered.length }} 个技能<span>共享 {{ items.filter(item => item.source === 'shared').length }} / 20</span></footer>
    </template>
    <ConfirmDialog :open="!!deleteTarget" title="删除共享技能" :message="'删除“' + (deleteTarget?.label || '') + '”？所有账号将无法再选择此技能，已发送消息的记录保留。'" tone="danger" @resolve="remove" />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { ArrowLeft, BookOpen, ChevronRight, FileText, Loader2, RefreshCw, Search, Trash2, Upload } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import ConfirmDialog from './ConfirmDialog.vue';
import LighthouseReply from './LighthouseReply.vue';

const emit = defineEmits<{ close: []; use: [Dict]; changed: [] }>();
const items = ref<Dict[]>([]), query = ref(''), source = ref(''), loading = ref(false), installing = ref(false), reading = ref(false);
const error = ref(''), notice = ref(''), detail = ref<Dict | null>(null), deleteTarget = ref<Dict | null>(null);
const fileInput = ref<HTMLInputElement | null>(null), guide = ref(''), nextOffset = ref(0), references = ref<string[]>([]);
const currentReference = ref('');
const filtered = computed(() => items.value.filter(item => (!source.value || item.source === source.value) && (item.label + ' ' + item.description).toLowerCase().includes(query.value.trim().toLowerCase())));
let controller: AbortController | undefined, guideController: AbortController | undefined, installController: AbortController | undefined, disposed = false;
function label(item: Dict): string { return item.display_name || item.label || item.source_identity || item.name; }
async function load(): Promise<void> {
  controller?.abort(); const current = new AbortController(); controller = current;
  loading.value = true; error.value = '';
  try { const data = await requestJson('/api/assistant/skills', { signal: current.signal, timeoutMs: 20000 });
    if (controller === current && !disposed) items.value = (data.items || []).map((item: Dict) => ({ ...item, label: label(item) }));
  } catch (cause) { if (!current.signal.aborted && !disposed) error.value = cause instanceof Error ? cause.message : '技能读取失败'; }
  finally { if (controller === current) loading.value = false; }
}
function show(item: Dict): void { detail.value = item; guide.value = ''; references.value = []; void readGuide(item); }
async function readGuide(item: Dict, offset = 0, reference = ''): Promise<void> {
  guideController?.abort(); const current = new AbortController(); guideController = current; reading.value = true; error.value = '';
  try { const params = new URLSearchParams({ offset: String(offset), reference });
    const data = await requestJson('/api/assistant/skills/' + encodeURIComponent(item.name) + '?' + params, { signal: current.signal });
    if (guideController !== current || disposed) return;
    guide.value = (offset ? guide.value : '') + (data.content || ''); nextOffset.value = data.next_offset || 0; references.value = data.references || []; currentReference.value = reference;
  } catch (cause) { if (!current.signal.aborted && !disposed) error.value = cause instanceof Error ? cause.message : '技能说明读取失败'; }
  finally { if (guideController === current) reading.value = false; }
}
function use(item: Dict): void { emit('use', { kind: 'skill', id: item.name, label: item.label }); }
async function install(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement, file = input.files?.[0]; input.value = '';
  if (!file || installing.value) return;
  if (file.size > 10 * 1024 * 1024) { error.value = '技能文件不得超过10MiB'; return; }
  const current = new AbortController(); installController = current; installing.value = true; error.value = ''; notice.value = '';
  try { const body = new FormData(); body.append('file', file);
    const data = await requestJson('/api/assistant/skills/install', { method: 'POST', body, timeoutMs: 30000, signal: current.signal });
    if (disposed) return;
    notice.value = data.duplicate ? '该技能已安装，所有账号可用' : '共享技能已安装，所有账号可用';
    if (data.warnings?.length) notice.value += ' · ' + data.warnings.join('；');
    emit('changed'); await load();
  } catch (cause) { if (!disposed) error.value = cause instanceof Error ? cause.message : '技能安装失败'; }
  finally { installing.value = false; installController = undefined; }
}
async function remove(accepted: boolean): Promise<void> {
  const item = deleteTarget.value; deleteTarget.value = null; if (!accepted || !item) return;
  error.value = '';
  try { await requestJson('/api/assistant/skills/' + encodeURIComponent(item.name), { method: 'DELETE' });
    if (disposed) return; detail.value = null; notice.value = '共享技能已删除'; emit('changed'); await load();
  } catch (cause) { if (!disposed) error.value = cause instanceof Error ? cause.message : '技能删除失败'; }
}
onMounted(load);
onBeforeUnmount(() => { disposed = true; controller?.abort(); guideController?.abort(); installController?.abort(); });
</script>

<style scoped>
.skill-manager { flex: 1; min-height: 0; display: flex; flex-direction: column; color: var(--lh-charcoal); animation: skill-enter .16s ease-out; }
@keyframes skill-enter { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
@media (prefers-reduced-motion: reduce) { .skill-manager { animation: none; } }
header { display: flex; gap: 8px; align-items: center; padding: 9px 12px; border-bottom: 1px solid var(--lh-border); }
header > strong { flex: 1; min-width: 0; font-size: 14px; overflow-wrap: anywhere; }
button { font: inherit; cursor: pointer; }
.skill-icon { display: inline-flex; align-items: center; justify-content: center; padding: 5px; color: inherit; background: transparent; border: 0; border-radius: 5px; }
.skill-icon:hover { background: var(--lh-accent-soft); }
.install { display: inline-flex; align-items: center; gap: 5px; padding: 6px 9px; border: 1px solid var(--lh-border); border-radius: 5px; color: var(--lh-accent); background: var(--lh-surface); font-size: 12px; }
.skill-filters { display: flex; gap: 7px; padding: 9px 12px; }
.skill-filters label { flex: 1; min-width: 0; display: flex; align-items: center; gap: 6px; border: 1px solid var(--lh-border); border-radius: 5px; padding: 6px 8px; }
input { min-width: 0; width: 100%; font: inherit; font-size: 12px; border: 0; outline: 0; color: inherit; background: transparent; }
select { font: inherit; font-size: 12px; color: inherit; border: 1px solid var(--lh-border); border-radius: 5px; background: var(--lh-surface); }
.skill-list, .skill-detail { overflow: auto; overscroll-behavior: contain; flex: 1; min-height: 0; }
.skill-list { padding: 0 7px; }
.skill-list > button { display: flex; align-items: center; gap: 9px; width: 100%; text-align: left; padding: 10px 6px; color: inherit; background: transparent; border: 0; border-bottom: 1px solid var(--lh-border); }
.skill-list > button:hover { background: var(--lh-accent-soft); }
.skill-list > button > svg { flex-shrink: 0; color: var(--lh-muted); }
.skill-list span { min-width: 0; flex: 1; }
.skill-list strong { display: block; font-weight: 600; font-size: 13px; overflow-wrap: anywhere; }
.skill-list small { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; margin-top: 3px; font-size: 12px; line-height: 1.5; color: var(--lh-muted); overflow-wrap: anywhere; }
.skill-list em { font-size: 11px; font-style: normal; color: var(--lh-muted); }
footer { display: flex; justify-content: space-between; padding: 8px 12px; border-top: 1px solid var(--lh-border); color: var(--lh-muted); font-size: 12px; }
.skill-detail { padding: 12px; font-size: 13px; line-height: 1.6; }
.detail-actions { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
.skill-references { display: flex; flex-direction: column; gap: 5px; margin-top: 12px; }
.skill-references button { display: flex; align-items: center; gap: 5px; border: 0; padding: 5px 0; text-align: left; color: var(--lh-accent); background: transparent; overflow-wrap: anywhere; }
.skill-state { display: flex; align-items: center; justify-content: center; gap: 7px; padding: 20px; font-size: 13px; }
.skill-error, .skill-notice { font-size: 12px; padding: 7px 12px; margin: 0; overflow-wrap: anywhere; }
.skill-error, .danger { color: var(--lh-danger); }
.skill-notice { background: var(--lh-accent-soft); color: var(--lh-muted); }
.spin { animation: skill-spin 1s linear infinite; }
@keyframes skill-spin { to { transform: rotate(360deg); } }
</style>
