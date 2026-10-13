<template>
  <section class="extension-manager" aria-label="工具与MCP">
    <div class="extension-toolbar">
      <label class="search"><Search :size="16" /><input v-model="query" aria-label="搜索工具" placeholder="搜索工具或MCP" /></label>
      <button class="icon" type="button" aria-label="刷新工具" title="刷新" :disabled="busy" @click="load"><RefreshCw :size="17" /></button>
      <button v-if="state.can_manage" class="primary" type="button" :disabled="busy" @click="add"><Plus :size="15" />添加MCP</button>
    </div>
    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-if="notice" class="notice" role="status">{{ notice }}</p>
    <div v-if="loading" class="loading" role="status"><Loader2 :size="18" class="spin" />正在读取工具</div>
    <div v-else class="extension-content">
      <form v-if="editing" class="editor" :aria-busy="busy" @submit.prevent="save">
        <div class="editor-title"><strong>{{ editing.id ? '编辑MCP' : '添加MCP' }}</strong><button class="icon" type="button" aria-label="取消编辑" :disabled="busy" @click="editing = null"><X :size="18" /></button></div>
        <label for="extension-name">名称</label><input id="extension-name" v-model="editing.name" required maxlength="80" />
        <label for="extension-transport">连接方式</label><select id="extension-transport" v-model="editing.transport"><option value="http">HTTPS MCP</option><option value="stdio">本机程序（stdio）</option></select>
        <template v-if="editing.transport === 'http'"><label for="extension-endpoint">MCP服务地址</label><input id="extension-endpoint" v-model="editing.endpoint" type="url" placeholder="https://example.com/mcp" required /></template>
        <template v-else><label for="extension-command">启动程序</label><input id="extension-command" v-model="editing.command" placeholder="python 或本机可执行文件的绝对路径" required /><label for="extension-args">启动参数（每行一个）</label><textarea id="extension-args" v-model="argumentLines" rows="3" /></template>
        <label for="extension-secret">认证密钥</label><input id="extension-secret" v-model="editing.secret" type="password" autocomplete="new-password" :placeholder="editing.has_secret ? '已保存；留空保留原密钥' : '无需认证可留空'" />
        <label class="check"><input v-model="editing.admin_only" type="checkbox" />仅管理员使用</label>
        <label v-if="editing.tested_at" class="check"><input v-model="editing.enabled" type="checkbox" />启用自动调用</label>
        <fieldset v-if="editing.tools?.length"><legend>允许自动调用的只读工具</legend><label v-for="tool in editing.tools" :key="tool.name" class="check"><input v-model="editing.allowed_tools" type="checkbox" :value="tool.name" :disabled="!tool.read_only" /><span>{{ tool.name }}<small>{{ tool.description }}{{ tool.read_only ? '' : ' · 未标明只读，不能启用' }}</small></span></label></fieldset>
        <div class="editor-actions"><button class="primary" type="submit" :disabled="busy"><Loader2 v-if="busy" :size="15" class="spin" /><Save v-else :size="15" />保存</button><button v-if="editing.id" type="button" :disabled="busy" @click="test(editing.id)"><Play :size="15" />测试已保存连接</button></div>
      </form>
      <h3>内置工具</h3>
      <div v-for="tool in filteredNative" :key="tool.name" class="native-row"><Wrench :size="16" /><span><strong>{{ tool.label }}</strong><small>{{ tool.description }}</small></span><span class="badge">内置</span></div>
      <h3>MCP服务</h3>
      <article v-for="server in filteredServers" :key="server.id" class="server-row">
        <div class="server-heading"><Plug :size="18" /><strong>{{ server.name }}</strong><span class="badge" :class="{ muted: !server.enabled }">{{ server.enabled ? '已启用' : '已停用' }}</span>
          <button v-if="state.can_manage" class="icon" type="button" :disabled="busy" :aria-label="'测试' + server.name" title="测试连接" @click="test(server.id)"><Loader2 v-if="busyId === server.id" :size="16" class="spin" /><Play v-else :size="16" /></button>
          <button v-if="state.can_manage && server.removable" class="icon" type="button" :disabled="busy" :aria-label="'编辑' + server.name" title="编辑" @click="edit(server)"><Pencil :size="16" /></button>
          <button v-if="state.can_manage && server.removable" class="icon danger" type="button" :disabled="busy" :aria-label="'删除' + server.name" title="删除" @click="deleteTarget = server"><Trash2 :size="16" /></button>
          <label v-if="state.can_manage && !server.removable" class="check"><input type="checkbox" :checked="server.enabled" :disabled="busy" :aria-label="'启用' + server.name" @change="toggleBuiltin(server, $event)" /></label>
        </div>
        <p>{{ server.transport === 'builtin' ? '随程序更新，无需单独安装' : server.transport === 'http' ? 'HTTPS连接' : '本机stdio连接' }} · {{ server.allowed_tools.length }} 个授权工具{{ server.admin_only ? ' · 管理员专用' : '' }}</p>
        <ul><li v-for="tool in server.tools" :key="tool.name"><span :class="{ disabled: !server.allowed_tools.includes(tool.name) }">{{ tool.description || tool.name }}</span></li></ul>
      </article>
      <template v-if="state.recent_calls?.length"><h3>最近调用</h3><div v-for="(call, index) in state.recent_calls" :key="index" class="call-row"><CheckCircle2 v-if="call.ok" :size="15" /><TriangleAlert v-else :size="15" /><span>{{ call.tool }}</span><small>{{ call.elapsed_ms }}ms · {{ call.at }}</small></div></template>
    </div>
    <ConfirmDialog :open="!!deleteTarget" title="删除MCP" :message="'删除“' + (deleteTarget?.name || '') + '”？已有会话记录保留，后续不再调用。'" tone="danger" @resolve="remove" />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { CheckCircle2, Loader2, Pencil, Play, Plug, Plus, RefreshCw, Save, Search, Trash2, TriangleAlert, Wrench, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import ConfirmDialog from './ConfirmDialog.vue';

const emit = defineEmits<{ changed: [] }>();
const state = ref<Dict>({ native_tools: [], servers: [], recent_calls: [] }), loading = ref(false), busy = ref(false), busyId = ref('');
const query = ref(''), error = ref(''), notice = ref(''), editing = ref<Dict | null>(null), argumentLines = ref(''), deleteTarget = ref<Dict | null>(null);
let disposed = false, controller: AbortController | undefined;
const matches = (value: unknown) => JSON.stringify(value).toLowerCase().includes(query.value.trim().toLowerCase());
const filteredNative = computed(() => state.value.native_tools.filter(matches));
const filteredServers = computed(() => state.value.servers.filter(matches));
async function load(): Promise<void> {
  controller?.abort(); const current = new AbortController(); controller = current; loading.value = true; error.value = '';
  try { const data = await requestJson('/api/assistant/extensions', { signal: current.signal }); if (!disposed && controller === current) state.value = data; }
  catch (cause) { if (!disposed && !current.signal.aborted) error.value = cause instanceof Error ? cause.message : '工具读取失败'; }
  finally { if (controller === current) loading.value = false; }
}
function add(): void { editing.value = { name: '', transport: 'http', endpoint: '', command: 'python', secret: '', enabled: false, admin_only: true, allowed_tools: [], tools: [] }; argumentLines.value = ''; error.value = ''; notice.value = ''; }
function edit(server: Dict): void { editing.value = { ...server, secret: '', allowed_tools: [...server.allowed_tools] }; argumentLines.value = (server.args || []).join('\n'); error.value = ''; }
async function persist(payload: Dict): Promise<void> { const data = await requestJson('/api/assistant/extensions', { method: 'PUT', body: JSON.stringify({ ...payload, revision: state.value.revision }) }); if (!disposed) { state.value = data; emit('changed'); } }
async function save(): Promise<void> {
  if (!editing.value || busy.value) return; busy.value = true; error.value = ''; notice.value = '';
  const item = editing.value, previousIds = new Set(state.value.servers.map((server: Dict) => server.id));
  try {
    await persist({ id: item.id, name: item.name, transport: item.transport, endpoint: item.endpoint, command: item.command,
      args: argumentLines.value.split('\n').filter(Boolean), secret: item.secret, enabled: item.enabled, admin_only: item.admin_only, allowed_tools: item.allowed_tools });
    if (disposed) return;
    const saved = state.value.servers.find((server: Dict) => item.id ? server.id === item.id : !previousIds.has(server.id));
    if (saved) edit(saved); notice.value = saved?.tested_at ? '工具配置已保存' : '连接已保存，请测试连接后选择工具并启用';
  } catch (cause) { if (!disposed) error.value = cause instanceof Error ? cause.message : '保存失败'; }
  finally { busy.value = false; }
}
async function test(identity: string): Promise<void> {
  if (busy.value) return; busy.value = true; busyId.value = identity; error.value = ''; notice.value = '';
  try { const data = await requestJson(`/api/assistant/extensions/${encodeURIComponent(identity)}/test`, { method: 'POST', body: '{}', timeoutMs: 30000 });
    if (disposed) return; await load(); const saved = state.value.servers.find((server: Dict) => server.id === identity); if (editing.value?.id === identity && saved) edit(saved);
    notice.value = `连接成功，发现 ${data.tools?.length || 0} 个工具`; }
  catch (cause) { if (!disposed) error.value = cause instanceof Error ? cause.message : '连接失败'; }
  finally { busy.value = false; busyId.value = ''; }
}
async function toggleBuiltin(server: Dict, event: Event): Promise<void> { busy.value = true; error.value = ''; const input = event.target as HTMLInputElement; try { await persist({ id: server.id, enabled: input.checked }); } catch (cause) { input.checked = server.enabled; error.value = cause instanceof Error ? cause.message : '保存失败'; } finally { busy.value = false; } }
async function remove(accepted: boolean): Promise<void> { const item = deleteTarget.value; deleteTarget.value = null; if (!accepted || !item) return; busy.value = true; error.value = ''; try { await persist({ id: item.id, delete: true }); if (editing.value?.id === item.id) editing.value = null; notice.value = 'MCP已删除'; } catch (cause) { error.value = cause instanceof Error ? cause.message : '删除失败'; } finally { busy.value = false; } }
onMounted(load);
onBeforeUnmount(() => { disposed = true; controller?.abort(); });
</script>

<style scoped>
.extension-manager { display: flex; flex-direction: column; flex: 1; min-height: 0; color: var(--lh-charcoal); }
.extension-toolbar { display: flex; align-items: center; gap: 8px; padding: 10px 12px; }
.search { display: flex; align-items: center; gap: 6px; flex: 1; min-width: 0; }
.extension-content { overflow: auto; overscroll-behavior: contain; padding: 0 12px 14px; }
h3 { font-size: 13px; margin: 14px 0 8px; }
button { display: inline-flex; align-items: center; justify-content: center; gap: 5px; font: inherit; font-size: 12px; min-height: 32px; padding: 5px 9px; border: 1px solid var(--lh-border); border-radius: 6px; background: var(--lh-surface); color: var(--lh-charcoal); cursor: pointer; }
button:hover { background: var(--lh-accent-soft); }
button:disabled { opacity: .55; cursor: default; }
button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
.primary { color: var(--lh-on-accent, #fff); background: var(--lh-accent); border-color: var(--lh-accent); }
.primary:hover { background: var(--lh-accent); filter: brightness(.95); }
.icon { width: 32px; padding: 5px; flex-shrink: 0; }
input:not([type=checkbox]), textarea, select { width: 100%; min-width: 0; box-sizing: border-box; border: 1px solid var(--lh-border); border-radius: 5px; color: inherit; background: var(--lh-surface); padding: 7px 8px; font: inherit; font-size: 13px; }
.editor { display: grid; gap: 7px; padding: 12px; border: 1px solid var(--lh-border); border-radius: 7px; }
.editor > label:not(.check) { font-size: 12px; }
.editor-title, .editor-actions, .server-heading, .native-row, .call-row { display: flex; align-items: center; gap: 8px; }
.editor-title strong, .server-heading strong { flex: 1; min-width: 0; overflow-wrap: anywhere; font-size: 13px; }
.check { display: flex; align-items: center; gap: 6px; font-size: 12px; cursor: pointer; }
.check input { accent-color: var(--lh-accent); }
fieldset { border: 0; padding: 8px 0; margin: 0; display: grid; gap: 8px; font-size: 12px; }
legend { padding: 0; font-weight: 600; }
small { display: block; color: var(--lh-muted); font-size: 12px; overflow-wrap: anywhere; }
.native-row { padding: 8px 0; border-bottom: 1px solid var(--lh-border); }
.native-row > span:first-of-type { flex: 1; }
.native-row strong { font-size: 13px; }
.server-row { border: 1px solid var(--lh-border); border-radius: 7px; padding: 10px; margin-bottom: 8px; }
.server-row p { font-size: 12px; color: var(--lh-muted); margin: 6px 0; }
.server-row ul { display: grid; gap: 5px; padding-left: 18px; margin: 8px 0 0; font-size: 12px; }
.badge { font-size: 11px; color: var(--lh-accent); white-space: nowrap; }
.muted, .disabled { color: var(--lh-muted); }
.call-row { font-size: 12px; padding: 7px 0; flex-wrap: wrap; border-bottom: 1px solid var(--lh-border); }
.call-row span { flex: 1; }
.error, .danger { color: var(--lh-danger); }
.error, .notice { margin: 0; padding: 8px 12px; font-size: 12px; overflow-wrap: anywhere; }
.notice { color: var(--lh-accent); }
.loading { display: flex; align-items: center; justify-content: center; gap: 8px; padding: 24px; font-size: 13px; }
.spin { animation: tools-spin 1s linear infinite; }
@keyframes tools-spin { to { transform: rotate(360deg); } }
@media (max-width: 520px) { .server-heading { flex-wrap: wrap; } .server-heading strong { flex-basis: 60%; } }
@media (prefers-reduced-motion: reduce) { .spin { animation-duration: 2s; } }
</style>
