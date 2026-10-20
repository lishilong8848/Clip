<template>
  <UiTransition name="ui-overlay" appear>
    <div class="workspace-backdrop" data-assistant-control="true" data-v2-navigation @click.self="navigateAway(() => emit('close'))">
      <section ref="dialog" class="assistant-workspace" role="dialog" aria-modal="true" aria-labelledby="assistant-workspace-title" tabindex="-1">
        <header>
          <h2 id="assistant-workspace-title">助手工作区</h2>
          <button type="button" class="icon" title="关闭工作区" aria-label="关闭工作区" @click="navigateAway(() => emit('close'))"><X :size="20" /></button>
        </header>
        <nav class="workspace-tabs" aria-label="工作区分类">
          <button v-for="item in tabs" :key="item.id" type="button" :aria-pressed="tab === item.id" :disabled="busy" @click="navigateAway(() => changeTab(item.id))"><component :is="item.icon" :size="16" aria-hidden="true" />{{ item.label }}</button>
        </nav>
        <div class="workspace-toolbar">
          <button v-if="detail || creating" type="button" :disabled="busy" @click="navigateAway(back)"><ArrowLeft :size="16" />返回列表</button>
          <template v-else>
            <button type="button" :disabled="busy || !userId" @click="beginCreate"><Plus :size="16" />新建{{ tabLabel }}</button>
            <button type="button" class="icon" :disabled="busy" title="刷新列表" aria-label="刷新列表" @click="load"><RefreshCw :size="16" /></button>
          </template>
          <span v-if="detail && tab === 'document'" class="revision">版本 {{ detail.revision }}</span>
        </div>
        <p v-if="error" class="workspace-error" role="alert">{{ error }}</p>
        <p v-if="notice" class="workspace-notice" role="status">{{ notice }}</p>
        <p v-if="tab === 'memory'" class="privacy-note">仅当前账号可见。只保存个人偏好，请勿填写业务记录、客户信息或凭据。</p>
        <div class="workspace-body" :aria-busy="busy">
          <details v-if="voiceConfiguration" class="speech-settings">
            <summary>管理员语音服务</summary>
            <form @submit.prevent="saveSpeech">
              <label>允许发送音频和文字的 HTTPS 服务来源<textarea v-model="allowedOrigins" :disabled="busy" rows="2" placeholder="https://speech.example.com" /></label>
              <fieldset v-for="kind in (['stt', 'tts'] as const)" :key="kind" :disabled="busy">
                <legend>{{ kind === 'stt' ? '语音识别' : '语音播报' }}</legend>
                <label class="file-choice"><input v-model="speechProfiles[kind].enabled" type="checkbox" />启用</label>
                <label>完整 HTTPS 接口地址<input v-model="speechProfiles[kind].endpoint" type="url" /></label>
                <label>模型<input v-model="speechProfiles[kind].model" maxlength="200" /></label>
                <label v-if="kind === 'tts'">音色<input v-model="speechProfiles.tts.voice" maxlength="200" /></label>
                <label>API Key<input v-model="speechKeys[kind]" type="password" autocomplete="new-password" :placeholder="speechProfiles[kind].has_key ? '留空保留已配置凭据' : '尚未配置'" maxlength="4000" @copy.prevent @cut.prevent /></label>
              </fieldset>
              <button type="submit" :disabled="busy"><Save :size="16" />保存语音配置</button>
            </form>
          </details>
          <p v-if="busy" class="workspace-status" role="status"><LoadingIndicator>处理中</LoadingIndicator></p>
          <template v-if="!detail && !creating">
            <ul v-if="items.length" class="workspace-list">
              <li v-for="item in items" :key="item.id">
                <button type="button" :disabled="busy" @click="show(item)"><component :is="tabs.find(t => t.id === tab)?.icon" :size="18" aria-hidden="true" /><span>{{ item.title || item.name || item.content?.slice(0, 160) || item.id }}<small v-if="item.updated_at">{{ new Date(item.updated_at * 1000).toLocaleString() }}</small></span><ChevronRight :size="16" /></button>
              </li>
            </ul>
            <button v-if="nextOffset !== null" type="button" :disabled="busy" @click="loadMore">加载更多</button>
            <p v-if="!items.length && !busy && !error" class="workspace-status">暂无{{ tabLabel }}</p>
          </template>
          <form v-else class="workspace-editor" @submit.prevent="requestSave">
            <label v-if="tab !== 'memory'">名称<input v-model="name" :readonly="tab === 'document' && !creating" :disabled="busy" maxlength="200" required /></label>
            <label v-else>记忆类型<select v-model="memoryKind" :disabled="busy"><option value="preference">个人偏好</option><option value="writing_rule">写作规则</option></select></label>
            <template v-if="tab === 'document'">
              <div v-if="!creating" class="edit-modes" role="group" aria-label="文档编辑方式">
                <label><input v-model="editMode" type="radio" value="targeted" :disabled="busy" />局部修改</label>
                <label><input v-model="editMode" type="radio" value="full" :disabled="busy" />全文编辑</label>
              </div>
              <label>文档内容<textarea v-model="content" :readonly="!creating && editMode !== 'full'" :disabled="busy" rows="10" /></label>
              <template v-if="!creating && editMode === 'targeted'">
                <label>原文<textarea v-model="oldText" :disabled="busy" rows="3" /></label>
                <label>替换为<textarea v-model="newText" :disabled="busy" rows="3" /></label>
              </template>
            </template>
            <template v-else-if="tab === 'package'">
              <label>处理说明<textarea v-model="instructions" :disabled="busy" rows="7" /></label>
              <fieldset :disabled="busy"><legend>资料文件</legend>
                <label v-for="file in packageFiles" :key="file.id" class="file-choice"><input v-model="fileIds" type="checkbox" :value="file.id" /><span>{{ file.name || file.id }}</span></label>
                <p v-if="!packageFiles.length" class="workspace-status">暂无可用附件</p>
              </fieldset>
            </template>
            <label v-else>个人偏好<textarea v-model="content" :disabled="busy" rows="7" required /></label>
            <footer>
              <button type="button" class="primary" :disabled="busy || !canSave" @click="requestSave"><Save :size="16" />{{ tab === 'document' && !creating ? (editMode === 'full' ? '保存全文' : '应用局部修改') : '保存' }}</button>
              <button v-if="!creating" type="button" class="danger" :disabled="busy" @click="askDelete"><Trash2 :size="16" />删除</button>
            </footer>
          </form>
          <section v-if="detail && tab === 'document'" class="version-history" aria-label="文档历史">
            <div class="version-actions">
              <button type="button" :disabled="busy" @click="loadVersions"><History :size="16" />历史版本</button>
              <a v-if="exportUrl" :href="exportUrl" download>下载文档</a>
              <button v-else type="button" :disabled="busy" @click="exportDocument"><Download :size="16" />导出</button>
            </div>
            <template v-if="versions.length">
              <button v-if="versionsOffset !== null" type="button" :disabled="busy" @click="loadMoreVersions">更多历史版本</button>
              <label>历史版本<select v-model="targetRevision" aria-label="历史版本" :disabled="busy"><option value="" disabled>选择版本</option><option v-for="version in versions" :key="version.revision" :value="version.revision">{{ version.revision }}{{ version.created_at ? ' · ' + version.created_at : '' }}</option></select></label>
              <div class="version-actions">
                <button type="button" :disabled="busy || targetRevision === ''" @click="readVersion"><Eye :size="16" />查看</button>
                <button type="button" :disabled="busy || targetRevision === ''" @click="compareVersion"><GitCompareArrows :size="16" />对比当前版本</button>
                <button type="button" :disabled="busy || targetRevision === '' || targetRevision === detail.revision" @click="askRestore"><RotateCcw :size="16" />恢复</button>
              </div>
              <pre v-if="versionText !== null" class="version-text" tabindex="0">{{ versionText }}</pre>
            </template>
          </section>
        </div>
        <ConfirmDialog :open="!!confirmation" :title="confirmation?.title || ''" :message="confirmation?.message || ''" :tone="confirmation?.danger ? 'danger' : 'warning'" @resolve="resolveConfirmation" />
      </section>
    </div>
  </UiTransition>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ArrowLeft, Brain, ChevronRight, Download, Eye, FileText, FolderOpen, GitCompareArrows, History, Plus, RefreshCw, RotateCcw, Save, Trash2, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { acquireModal } from '../modalState';
import ConfirmDialog from './ConfirmDialog.vue';
import LoadingIndicator from './LoadingIndicator.vue';
import UiTransition from './UiTransition.vue';

type Tab = 'document' | 'package' | 'memory';
type Confirmation = { title: string; message: string; danger?: boolean; run: () => void };
const props = defineProps<{ userId: string; files: Dict[]; voiceConfiguration?: Dict | null }>();
const emit = defineEmits<{ close: []; voiceConfigured: [] }>();
const tabs = [{ id: 'document' as const, label: '文档', icon: FileText }, { id: 'package' as const, label: '资料包', icon: FolderOpen }, { id: 'memory' as const, label: '个人记忆', icon: Brain }];
const tab = ref<Tab>('document'), items = ref<Dict[]>([]), detail = ref<Dict | null>(null), creating = ref(false);
const name = ref(''), content = ref(''), instructions = ref(''), fileIds = ref<string[]>([]), oldText = ref(''), newText = ref('');
const editMode = ref<'targeted' | 'full'>('targeted'), baseline = ref('');
const memoryKind = ref('preference'), nextOffset = ref<number | null>(null);
const versionsOffset = ref<number | null>(null);
const allowedOrigins = ref(''), speechProfiles = ref<Dict>({ stt: {}, tts: {} }), speechKeys = ref({ stt: '', tts: '' });
watch(() => props.voiceConfiguration, value => {
  allowedOrigins.value = (value?.allowed_origins || []).join('\n');
  speechProfiles.value = { stt: { ...value?.stt }, tts: { ...value?.tts } };
  speechKeys.value = { stt: '', tts: '' };
}, { immediate: true });
const busy = ref(false), error = ref(''), notice = ref('');
const versions = ref<Dict[]>([]), targetRevision = ref<string | number>(''), versionText = ref<string | null>(null), exportUrl = ref('');
const confirmation = ref<Confirmation | null>(null), dialog = ref<HTMLElement | null>(null);
const tabLabel = computed(() => tabs.find(item => item.id === tab.value)!.label);
const packageFiles = computed(() => [...new Map([...props.files.filter(file => file.id && !file.uploading && !file.error), ...fileIds.value.map(id => ({ id, name: id }))].map(file => [file.id, file])).values()].map(file => props.files.find(item => item.id === file.id) || file));
const snapshot = () => JSON.stringify([name.value, content.value, instructions.value, fileIds.value, oldText.value, newText.value, memoryKind.value]);
const dirty = computed(() => (creating.value || !!detail.value) && snapshot() !== baseline.value);
const canSave = computed(() => tab.value === 'memory' ? !!content.value.trim() : !!name.value.trim() && (tab.value !== 'document' || creating.value || editMode.value === 'full' || !!oldText.value && oldText.value !== newText.value));
let controller: AbortController | undefined, disposed = false, modal: ReturnType<typeof acquireModal> | undefined;
let returnFocus: HTMLElement | null = null;

async function call(action: string, payload: Dict = {}): Promise<Dict> {
  const current = controller, owner = props.userId;
  if (disposed || !owner || !current || current.signal.aborted) throw new DOMException('Cancelled', 'AbortError');
  const data = await requestJson('/api/assistant/workspace', { method: 'POST', body: JSON.stringify({ action, ...payload, ...(['memory.save', 'memory.delete'].includes(action) ? { confirm_memory: true } : {}) }), signal: current.signal, timeoutMs: 45000 });
  if (disposed || current !== controller || current.signal.aborted || owner !== props.userId) throw new DOMException('Cancelled', 'AbortError');
  return data;
}
async function perform(work: () => Promise<void>): Promise<void> {
  if (busy.value || disposed || !props.userId) return;
  const current = new AbortController(), owner = props.userId;
  controller = current; busy.value = true; error.value = ''; notice.value = '';
  try { await work(); }
  catch (cause) { if (!disposed && !current.signal.aborted && owner === props.userId) error.value = cause instanceof Error ? cause.message : '操作未完成，请重试'; }
  finally { if (controller === current) { busy.value = false; controller = undefined; } }
}
async function list(offset = 0): Promise<void> {
  const data = await call(tab.value + '.list', { offset, limit: 20 });
  if (controller?.signal.aborted || disposed) return;
  if (!Array.isArray(data.items)) throw new Error('列表响应缺少 items，请检查工作区服务');
  items.value = offset ? [...items.value, ...data.items] : data.items;
  nextOffset.value = data.next_offset ?? null;
}
function load(): Promise<void> { return perform(() => list()); }
function loadMore(): Promise<void> { return perform(() => list(nextOffset.value!)); }
function resetEditor(): void {
  detail.value = null; creating.value = false; name.value = ''; content.value = ''; instructions.value = ''; fileIds.value = [];
  oldText.value = ''; newText.value = ''; memoryKind.value = 'preference'; editMode.value = 'targeted'; versions.value = []; versionsOffset.value = null; targetRevision.value = ''; versionText.value = null; exportUrl.value = '';
  baseline.value = snapshot();
}
function adopt(data: Dict): void {
  if (!data.id || tab.value === 'document' && (data.revision === undefined || typeof data.content !== 'string')) throw new Error('文档响应缺少内容或版本，请重新读取');
  detail.value = data; creating.value = false; name.value = String(data.title || data.name || (tab.value === 'memory' ? data.content?.slice(0, 80) : '') || data.id); content.value = String(data.content || ''); memoryKind.value = data.kind || 'preference';
  instructions.value = String(data.instructions || ''); fileIds.value = [...(data.file_ids || [])]; oldText.value = ''; newText.value = ''; exportUrl.value = '';
  baseline.value = snapshot();
}
function show(item: Dict): Promise<void> {
  return perform(async () => {
    const data = tab.value === 'memory' ? item : await call(tab.value + '.read', { id: item.id });
    if (!disposed && !controller?.signal.aborted) adopt(data);
  });
}
function beginCreate(): void { resetEditor(); creating.value = true; baseline.value = snapshot(); void nextTick(() => dialog.value?.querySelector('input')?.focus()); }
function back(): void { resetEditor(); error.value = ''; notice.value = ''; void load(); }
function changeTab(value: Tab): void { tab.value = value; items.value = []; nextOffset.value = null; back(); }
function navigateAway(run: () => void): void {
  if (busy.value) { confirmation.value = { title: '关闭工作区', message: '操作仍在处理中。关闭会取消等待，服务端可能已接受操作；再次打开后请刷新核对。', run }; return; }
  if (dirty.value) confirmation.value = { title: '放弃未保存的修改', message: '离开后将丢弃当前未保存的修改。', run };
  else run();
}
function resolveConfirmation(yes: boolean): void { const pending = confirmation.value; confirmation.value = null; if (yes) pending?.run(); }
function requestSave(): void {
  if (busy.value || !canSave.value) return;
  if (tab.value === 'memory') confirmation.value = { title: '保存个人记忆', message: '确认将以下内容保存到当前账号的个人记忆？请确认不含业务数据或凭据。\n' + name.value + '\n' + content.value, run: () => { void save(); } };
  else void save();
}
function save(): Promise<void> {
  return perform(async () => {
    const kind = tab.value, id = detail.value?.id;
    const existing = id ? { id, revision: detail.value!.revision } : {};
    const payload: Dict = kind === 'package' ? { ...existing, title: name.value.trim(), instructions: instructions.value, file_ids: [...fileIds.value] }
      : kind === 'memory' ? { ...existing, content: content.value, kind: memoryKind.value }
      : creating.value ? { title: name.value.trim(), content: content.value }
      : { ...existing, old_text: editMode.value === 'full' ? detail.value!.content : oldText.value, new_text: editMode.value === 'full' ? content.value : newText.value };
    const data = await call(kind + '.' + (kind === 'memory' ? 'save' : creating.value ? 'create' : kind === 'package' ? 'update' : 'edit'), payload);
    if (disposed || controller?.signal.aborted) return;
    // Read the canonical document after a write so the next edit uses its new revision.
    if (kind !== 'memory') {
      const result = await call(kind + '.read', { id: data.id || id });
      if (disposed || controller?.signal.aborted) return;
      adopt(result);
    } else { resetEditor(); await list(); }
    if (!disposed && !controller?.signal.aborted) notice.value = '已保存';
  });
}
function askDelete(): void {
  if (busy.value || !detail.value) return;
  confirmation.value = { title: '删除' + tabLabel.value, message: '确认删除“' + name.value + '”？', danger: true, run: () => { void perform(async () => {
    await call(tab.value + '.delete', { id: detail.value!.id, revision: detail.value!.revision });
    if (disposed || controller?.signal.aborted) return;
    resetEditor(); await list(); if (!disposed && !controller?.signal.aborted) notice.value = '已删除';
  }); } };
}
function loadVersions(): Promise<void> { return perform(async () => { const data = await call('document.versions', { id: detail.value!.id, offset: 0, limit: 20 }); versions.value = data.items || []; versionsOffset.value = data.next_offset ?? null; targetRevision.value = ''; versionText.value = null; if (!versions.value.length) notice.value = '暂无历史版本'; }); }
function loadMoreVersions(): Promise<void> { return perform(async () => { const data = await call('document.versions', { id: detail.value!.id, offset: versionsOffset.value, limit: 20 }); versions.value.push(...(data.items || [])); versionsOffset.value = data.next_offset ?? null; }); }
function readVersion(): Promise<void> { return perform(async () => { const data = await call('document.read', { id: detail.value!.id, version: targetRevision.value }); if (!disposed && !controller?.signal.aborted) versionText.value = String(data.content ?? ''); }); }
function compareVersion(): Promise<void> { return perform(async () => { const data = await call('document.diff', { id: detail.value!.id, from_version: targetRevision.value, to_version: detail.value!.revision }); if (!disposed && !controller?.signal.aborted) versionText.value = (typeof data.diff === 'string' ? data.diff || '两个版本内容相同' : JSON.stringify(data, null, 2)) + (data.truncated ? '\n差异内容较长，当前仅显示部分。' : ''); }); }
function askRestore(): void {
  if (busy.value || !detail.value || targetRevision.value === '') return;
  confirmation.value = { title: '恢复历史版本', message: '将当前文档恢复到版本 ' + targetRevision.value + (dirty.value ? '，并丢弃未保存的修改' : '') + '？', run: () => { void perform(async () => {
    const id = detail.value!.id;
    await call('document.restore', { id, revision: detail.value!.revision, version: targetRevision.value });
    if (disposed || controller?.signal.aborted) return;
    const data = await call('document.read', { id });
    if (!disposed && !controller?.signal.aborted) { adopt(data); versions.value = []; versionText.value = null; notice.value = '已恢复'; }
  }); } };
}
function exportDocument(): Promise<void> { return perform(async () => {
  const data = await call('document.export', { id: detail.value!.id, version: detail.value!.revision });
  if (disposed || controller?.signal.aborted) return;
  if (typeof data.url !== 'string' || !/^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(data.url)) throw new Error('导出服务未返回可下载的个人文件地址');
  exportUrl.value = data.url;
}); }
function saveSpeech(): void {
  if (busy.value || !props.voiceConfiguration) return;
  confirmation.value = { title: '保存语音服务配置', message: '录音和播报文字将发送到允许列表中的服务。确认这些 HTTPS 来源和服务已获授权？\n' + allowedOrigins.value, run: () => { void perform(async () => {
    const payload: Dict = { allowed_origins: allowedOrigins.value.split(/[,\r\n]+/).map(value => value.trim()).filter(Boolean) };
    for (const kind of ['stt', 'tts'] as const) {
      const profile = speechProfiles.value[kind];
      payload[kind] = { enabled: !!profile.enabled, endpoint: profile.endpoint || '', ...(profile.model ? { model: profile.model } : {}), ...(kind === 'tts' && profile.voice ? { voice: profile.voice } : {}), ...(speechKeys.value[kind] ? { api_key: speechKeys.value[kind] } : {}) };
    }
    speechKeys.value = { stt: '', tts: '' };
    const current = controller, owner = props.userId;
    await requestJson('/api/assistant/voice', { method: 'PUT', body: JSON.stringify(payload), signal: current?.signal, timeoutMs: 20000 });
    if (disposed || current !== controller || current?.signal.aborted || owner !== props.userId) return;
    notice.value = '语音配置已保存'; emit('voiceConfigured');
  }); } };
}
function keydown(event: KeyboardEvent): void {
  if (!modal?.isTop(event, dialog.value) || !dialog.value) return;
  if (event.key === 'Escape') { event.preventDefault(); event.stopImmediatePropagation(); navigateAway(() => emit('close')); return; }
  if (event.key !== 'Tab') return;
  const nodes = [...dialog.value.querySelectorAll<HTMLElement>('button:not(:disabled),a[href],input:not(:disabled),textarea:not(:disabled),select:not(:disabled),[tabindex="0"]')].filter(node => node.getClientRects().length);
  const first = nodes[0], last = nodes[nodes.length - 1];
  if (!dialog.value.contains(document.activeElement) || (event.shiftKey ? document.activeElement === first : document.activeElement === last)) { event.preventDefault(); (event.shiftKey ? last : first)?.focus(); }
}
watch(() => props.userId, () => { controller?.abort(); controller = undefined; busy.value = false; confirmation.value = null; items.value = []; speechKeys.value = { stt: '', tts: '' }; resetEditor(); error.value = ''; notice.value = ''; void load(); }, { flush: 'sync' });
onMounted(() => { returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null; modal = acquireModal(); window.addEventListener('keydown', keydown, true); dialog.value?.querySelector('button')?.focus(); void load(); });
onBeforeUnmount(() => { disposed = true; speechKeys.value = { stt: '', tts: '' }; controller?.abort(); modal?.release(); window.removeEventListener('keydown', keydown, true); const target = returnFocus; void nextTick(() => { if (target?.isConnected) target.focus(); }); });
</script>

<style scoped>
.workspace-backdrop { position: fixed; inset: 0; z-index: 10100; display: grid; place-items: center; padding: 12px; background: #10171488; pointer-events: auto; }
.assistant-workspace { width: min(700px, 100%); max-height: calc(100dvh - 24px); min-height: min(400px, calc(100dvh - 24px)); display: flex; flex-direction: column; overflow: hidden; border: 1px solid var(--lh-border-strong); border-radius: 8px; background: var(--lh-surface); color: var(--lh-charcoal); font-size: 14px; letter-spacing: 0; box-shadow: 0 18px 60px #0004; }
header, .workspace-toolbar, .workspace-tabs, footer, .version-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
header { padding: 10px 14px; justify-content: space-between; border-bottom: 1px solid var(--lh-border); }
h2 { margin: 0; font-size: 16px; } .workspace-tabs, .workspace-toolbar { padding: 8px 14px; } .workspace-tabs { border-bottom: 1px solid var(--lh-border); } .workspace-tabs button[aria-pressed="true"] { background: var(--lh-accent-soft); color: var(--lh-accent-strong); border-color: var(--lh-accent); }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 36px; padding: 6px 10px; border: 1px solid var(--lh-border); border-radius: 5px; background: var(--lh-surface-subtle); color: inherit; font: inherit; cursor: pointer; transition: background 150ms cubic-bezier(.4,0,.2,1); }
button:hover:not(:disabled) { background: var(--lh-surface-hover); } button:disabled { opacity: .5; cursor: not-allowed; } .icon { width: 36px; padding: 6px; flex-shrink: 0; } .primary { background: var(--lh-action); color: white; } .primary:hover:not(:disabled) { background: var(--lh-action-hover); } .danger, .workspace-error { color: var(--lh-danger); }
.workspace-body { overflow-y: auto; overscroll-behavior: contain; min-height: 0; padding: 0 14px 14px; } .workspace-list { list-style: none; padding: 0; margin: 0; } .workspace-list li { border-bottom: 1px solid var(--lh-border); } .workspace-list button { width: 100%; text-align: left; border: 0; background: transparent; padding: 12px 4px; } .workspace-list span { flex: 1; min-width: 0; overflow-wrap: anywhere; } small { display: block; color: var(--lh-muted); margin-top: 4px; }
.workspace-editor, .version-history { display: grid; gap: 14px; } label { display: grid; gap: 6px; min-width: 0; } input:not([type="checkbox"]):not([type="radio"]), textarea, select { box-sizing: border-box; width: 100%; min-width: 0; padding: 9px; border: 1px solid var(--lh-input-border); border-radius: 5px; background: var(--lh-surface-subtle); color: inherit; font: inherit; line-height: 1.5; } textarea { resize: vertical; max-height: 50dvh; } textarea[readonly] { color: var(--lh-muted); } fieldset { border: 1px solid var(--lh-border); border-radius: 5px; min-width: 0; } .file-choice, .edit-modes label { display: flex; align-items: center; gap: 8px; padding: 6px 0; } .file-choice span { overflow-wrap: anywhere; } input[type="checkbox"], input[type="radio"] { accent-color: var(--lh-action); } .edit-modes { display: flex; gap: 16px; flex-wrap: wrap; }
.workspace-status, .revision, .privacy-note { color: var(--lh-muted); } .workspace-error, .workspace-notice, .privacy-note { margin: 4px 14px 10px; overflow-wrap: anywhere; white-space: pre-wrap; } .privacy-note { font-size: 12px; line-height: 1.6; } .workspace-notice { color: var(--lh-accent); } .workspace-status { padding: 10px 0; margin: 0; } .revision { margin-left: auto; font-size: 12px; } .version-history { border-top: 1px solid var(--lh-border); margin-top: 16px; padding-top: 14px; } .version-text { margin: 0; max-height: 300px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font: inherit; padding: 10px; background: var(--lh-surface-subtle); } a { color: var(--lh-accent); } button svg { flex-shrink: 0; }
button:focus-visible, input:focus-visible, textarea:focus-visible, select:focus-visible, a:focus-visible, pre:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
@media (max-width: 480px) { .workspace-backdrop { padding: 6px; } .assistant-workspace { max-height: calc(100dvh - 12px); width: 100%; } button { min-height: 44px; } .icon { width: 44px; } .workspace-tabs { gap: 4px; padding: 6px 8px; } .workspace-tabs button { flex: 1; padding: 6px; } header, .workspace-toolbar { padding: 8px; } .workspace-body { padding: 0 10px max(12px, env(safe-area-inset-bottom)); } input, textarea, select { font-size: 16px !important; } }
@media (prefers-reduced-motion: reduce) { button { transition: none; } }
.speech-settings { border-bottom: 1px solid var(--lh-border); padding: 8px 0 14px; margin-bottom: 14px; }
.speech-settings summary { cursor: pointer; padding: 10px 0; }
.speech-settings form, .speech-settings fieldset { display: grid; gap: 10px; }
</style>
