<template>
  <section class="accounts">
    <header class="account-header">
      <strong>账号与权限</strong>
      <span class="muted">{{ loadedAt ? `人员同步于 ${formatTime(loadedAt)}` : '人员目录未同步' }}</span>
      <button type="button" :disabled="loading || saving" @click="load(true)"><RefreshCw :size="16" :class="{ spin: loading }" />同步人员</button>
    </header>
    <div class="metrics">
      <div><span>账号总数</span><b>{{ items.length }}</b></div>
      <div><span>已授权</span><b>{{ items.filter(x => x.enabled && x.scopes.length && !x.blocked_reason).length }}</b></div>
      <div><span>待审批</span><b>{{ pendingCount }}</b></div>
      <div><span>停用 / 待核对</span><b>{{ items.filter(x => !x.enabled || x.blocked_reason).length }}</b></div>
    </div>
    <nav class="tabs" aria-label="账号管理">
      <button v-for="option in tabs" :key="option.value" type="button" :class="{ active: tab === option.value }" :aria-pressed="tab === option.value" @click="switchTab(option.value)">{{ option.label }}</button>
    </nav>
    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-if="notice" class="success" role="status">{{ notice }}</p>
    <p v-if="warning" class="warning" role="status">{{ warning }}</p>
    <template v-if="tab === 'accounts'">
      <div class="filters">
        <label class="search"><Search :size="16" /><input v-model.trim="search" type="search" aria-label="搜索账号" placeholder="姓名、工号或账号名" /></label>
        <label><span>类型</span><select v-model="kind"><option value="">全部类型</option><option v-for="(label, value) in kinds" :key="value" :value="value">{{ label }}</option></select></label>
        <label><span>所属楼栋</span><select v-model="building"><option value="">全部楼栋</option><option v-for="scope in ownScopeOptions" :key="scope.value" :value="scope.value">{{ scope.label }}</option></select></label>
        <label><span>状态</span><select v-model="state"><option value="">全部状态</option><option value="ready">已授权</option><option value="pending">未授权</option><option value="disabled">已停用</option><option value="blocked">待核对</option><option value="admin">管理员</option></select></label>
      </div>
      <div v-if="selected.size" class="bulk">
        <b>已选 {{ selected.size }} 个账号</b>
        <button type="button" @click="openBulk('add')"><Plus :size="15" />增加楼栋权限</button>
        <button type="button" @click="openBulk('remove')"><Minus :size="15" />撤销楼栋权限</button>
        <button type="button" @click="selected.clear()">取消选择</button>
      </div>
      <LoadingIndicator v-if="loading && !items.length">正在读取账号</LoadingIndicator>
      <div v-else class="table-wrap">
        <table>
          <thead><tr><th><input type="checkbox" aria-label="选择本页账号" :checked="pageSelectable.length > 0 && pageSelectable.every(x => selected.has(x.id))" @change="togglePage(($event.target as HTMLInputElement).checked)" /></th><th>人员 / 账号</th><th>类型</th><th>所属楼栋</th><th>角色</th><th>可访问楼栋</th><th>状态</th><th>最近登录</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="item in pageItems" :key="item.id" :class="{ selected: selected.has(item.id) }">
              <td><input type="checkbox" :aria-label="`选择${item.name}`" :checked="selected.has(item.id)" :disabled="!canEdit(item) || item.role === 'admin'" @change="toggle(item.id, ($event.target as HTMLInputElement).checked)" /></td>
              <td><b>{{ item.name }}</b><small>{{ item.employee_no || (item.kind === 'duty' ? '固定值班账号' : '工号未填') }}</small></td>
              <td><span class="tag">{{ kinds[item.kind] }}</span></td><td>{{ scopesLabel(item.own_scopes) }}</td>
              <td>{{ item.locked ? '固定管理员' : item.role === 'admin' ? '管理员' : '普通用户' }}</td>
              <td class="scope-cell">{{ scopesLabel(item.scopes) }}</td>
              <td><span :class="['status', !item.enabled || item.blocked_reason ? 'warn' : item.scopes.length ? 'ok' : '']" :title="item.blocked_reason">{{ stateLabel(item) }}</span></td>
              <td class="muted">{{ formatTime(item.last_login) }}</td>
              <td><button type="button" class="link-button" :aria-label="`管理${item.name}`" @click="openAccount(item)"><Settings2 :size="15" />管理</button></td>
            </tr>
            <tr v-if="!filtered.length"><td colspan="9" class="empty">没有匹配的账号</td></tr>
          </tbody>
        </table>
      </div>
      <footer class="pagination"><span>共 {{ filtered.length }} 个账号</span><button type="button" :disabled="page <= 1" @click="page--"><ChevronLeft :size="16" />上一页</button><span>{{ page }} / {{ pageCount }}</span><button type="button" :disabled="page >= pageCount" @click="page++">下一页<ChevronRight :size="16" /></button></footer>
    </template>
    <slot v-else-if="tab === 'requests'" />
    <template v-else>
      <button type="button" :disabled="auditLoading" @click="loadAudit"><RefreshCw :size="16" :class="{ spin: auditLoading }" />刷新记录</button>
      <div class="table-wrap"><table><thead><tr><th>时间</th><th>操作人</th><th>操作</th><th>账号及变更</th><th>结果</th></tr></thead>
        <tbody><tr v-for="row in audits" :key="row.audit_id"><td>{{ formatTime(row.updated_at) }}</td><td>{{ row.actor_name || '管理员' }}</td><td>{{ row.action === 'password' ? '修改密码' : '修改权限' }}</td><td><template v-if="row.action === 'password'">{{ row.metadata?.name }}</template><div v-for="(change, i) in row.metadata?.changes || []" :key="i">{{ change.name }}：{{ scopesLabel(change.before?.scopes || []) }} → {{ scopesLabel(change.after?.scopes || []) }}；{{ change.after?.enabled === false ? '停用' : change.after?.role === 'admin' ? '管理员' : '普通用户' }}</div></td><td>{{ row.status === 'success' ? '成功' : row.status === 'failed' ? '未完成' : '处理中' }}</td></tr><tr v-if="!audits.length"><td colspan="5" class="empty">暂无变更记录</td></tr></tbody></table></div>
    </template>
    <Teleport to="body">
      <UiTransition name="ui-drawer">
        <div v-if="draft || bulkMode" class="account-overlay" @click.self="closeEditor">
          <section ref="editor" class="account-editor" role="dialog" aria-modal="true" aria-labelledby="account-editor-title" tabindex="-1">
            <header><div><h2 id="account-editor-title">{{ draft?.name || (bulkMode === 'add' ? '批量增加楼栋权限' : '批量撤销楼栋权限') }}</h2><span class="muted">{{ draft ? kinds[draft.kind] : `涉及 ${selected.size} 个账号` }}</span></div><button type="button" class="icon" aria-label="关闭账号详情" :disabled="saving" @click="closeEditor"><X :size="20" /></button></header>
            <p v-if="editorError" class="error" role="alert">{{ editorError }}</p>
            <p v-if="notice" class="success" role="status">{{ notice }}</p>
            <div v-if="draft" class="editor-body">
              <dl><dt>工号</dt><dd>{{ draft.employee_no || '未填写' }}</dd><dt>所属楼栋</dt><dd>{{ scopesLabel(draft.own_scopes) }}</dd><dt>登录方式</dt><dd>{{ draft.can_set_password ? '姓名密码登录' : '飞书登录' }}</dd><dt>OpenID</dt><dd class="identity">{{ draft.open_id || '未关联' }}</dd></dl>
              <p v-if="draft.blocked_reason" class="warning">{{ draft.blocked_reason }}</p>
              <fieldset :disabled="saving || !canEdit(draft)"><legend>账号权限</legend>
                <label class="form-row">角色<select v-model="draft.role"><option value="building">普通用户</option><option value="admin">管理员</option></select></label>
                <label class="check"><input v-model="draft.enabled" type="checkbox" />启用账号</label>
                <div class="scopes"><label v-for="scope in scopeOptions" :key="scope.value"><input v-model="draft.scopes" type="checkbox" :value="scope.value" :disabled="draft.role === 'admin'" />{{ scope.label }}</label></div>
                <p v-if="draft.role === 'admin'" class="warning">管理员可以访问全部楼栋和管理功能。</p>
                <p v-else-if="!draft.scopes.length" class="warning">未授权楼栋：登录后仅可申请权限。</p>
                <button type="button" class="primary" @click="saveAccount"><Save :size="16" />{{ saving ? '正在保存' : '保存权限' }}</button>
              </fieldset>
              <form v-if="draft.can_set_password" @submit.prevent="savePassword">
                <fieldset :disabled="saving"><legend>修改灯塔登录密码</legend><p class="muted">{{ draft.password_set ? '密码已设置' : '尚未设置正式密码' }} · 保存后该人员需重新登录</p>
                  <label for="admin-account-new-password">新密码</label><input id="admin-account-new-password" v-model="newPassword" type="password" autocomplete="new-password" minlength="8" maxlength="128" required />
                  <label for="admin-account-confirm-password">再次输入新密码</label><input id="admin-account-confirm-password" v-model="confirmPassword" type="password" autocomplete="new-password" minlength="8" maxlength="128" required />
                  <button type="submit" class="primary"><KeyRound :size="16" />{{ saving ? '正在保存' : '确认修改密码' }}</button>
                </fieldset>
              </form>
              <p v-else class="muted">{{ draft.kind === 'vnet' || draft.kind === 'duty' ? '飞书账号密码请在飞书中修改。' : '该账号未关联可使用密码登录的在岗人员。' }}</p>
            </div>
            <div v-else class="editor-body"><div class="scopes"><label v-for="scope in scopeOptions" :key="scope.value"><input v-model="bulkScopes" type="checkbox" :value="scope.value" :disabled="saving" />{{ scope.label }}</label></div><ul><li v-for="item in selectedItems" :key="item.id">{{ item.name }} · {{ scopesLabel(item.scopes) }}</li></ul><button type="button" class="primary" :disabled="saving || !bulkScopes.length" @click="saveBulk">{{ saving ? '正在保存' : `确认${bulkMode === 'add' ? '增加' : '撤销'}权限` }}</button></div>
          </section>
        </div>
      </UiTransition>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { ChevronLeft, ChevronRight, KeyRound, Minus, Plus, RefreshCw, Save, Search, Settings2, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { acquireModal } from '../modalState';
type Account = { id: string; record_id: string; open_id: string; name: string; employee_no: string; kind: string; own_scopes: string[]; scopes: string[]; role: string; enabled: boolean; authorized: boolean; locked: boolean; blocked_reason: string; can_set_password: boolean; password_set: boolean; revision: string; last_login?: number };
const props = defineProps<{ pendingCount: number; permissions: Dict[] }>();
const emit = defineEmits<{ changed: [] }>();
const kinds: Record<string, string> = { vnet: 'VNET人员', external: '非VNET人员', duty: '值班账号', legacy: '历史账号', unknown: '类型待核对' };
const tabs = [{ value: 'accounts', label: '账号列表' }, { value: 'requests', label: '权限申请' }, { value: 'audit', label: '变更记录' }];
const tab = ref('accounts'), items = ref<Account[]>([]), scopeOptions = ref<{value: string; label: string}[]>([]);
const search = ref(''), kind = ref(''), building = ref(''), state = ref(''), page = ref(1);
const loading = ref(false), saving = ref(false), error = ref(''), notice = ref(''), warning = ref(''), loadedAt = ref(0);
const selected = reactive(new Set<string>()), draft = ref<Account | null>(null), editor = ref<HTMLElement | null>(null);
const editorError = ref(''), newPassword = ref(''), confirmPassword = ref(''), bulkMode = ref(''), bulkScopes = ref<string[]>([]);
const audits = ref<Dict[]>([]), auditLoading = ref(false);
let modal: ReturnType<typeof acquireModal> | null = null;
const abort = new AbortController();
const ownScopeOptions = computed(() => scopeOptions.value.filter(x => !['ALL', 'CAMPUS'].includes(x.value)));
const filtered = computed(() => items.value.filter(x => (!search.value || `${x.name} ${x.employee_no}`.toLowerCase().includes(search.value.toLowerCase()))
  && (!kind.value || x.kind === kind.value) && (!building.value || x.own_scopes.includes(building.value))
  && (!state.value || (state.value === 'admin' ? x.role === 'admin' : state.value === 'blocked' ? !!x.blocked_reason : state.value === 'disabled' ? !x.enabled : state.value === 'pending' ? !x.scopes.length && x.enabled && !x.blocked_reason : x.enabled && !!x.scopes.length && !x.blocked_reason))));
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / 20)));
const pageItems = computed(() => filtered.value.slice((page.value - 1) * 20, page.value * 20));
const pageSelectable = computed(() => pageItems.value.filter(x => canEdit(x) && x.role !== 'admin'));
const selectedItems = computed(() => items.value.filter(x => selected.has(x.id)));
function canEdit(item: Account) { return !!item.open_id && !item.locked && !item.blocked_reason; }
function stateLabel(item: Account) { return item.blocked_reason ? '待核对' : !item.enabled ? '已停用' : item.scopes.length ? '已授权' : '未授权'; }
function scopesLabel(scopes: string[]) { return scopes.map(scope => scopeOptions.value.find(x => x.value === scope)?.label || scope).join('、') || '未配置'; }
function formatTime(timestamp?: number) { return timestamp ? new Date(timestamp * 1000).toLocaleString('zh-CN', { hour12: false }) : '暂无记录'; }
function toggle(id: string, checked: boolean) { checked ? selected.add(id) : selected.delete(id); }
function togglePage(checked: boolean) { pageSelectable.value.forEach(item => toggle(item.id, checked)); }
async function load(refresh = false) {
  if (loading.value) return;
  loading.value = true; error.value = '';
  try {
    const data = await requestJson(`/api/auth/accounts${refresh ? '?refresh=true' : ''}`, { signal: abort.signal });
    items.value = data.items || []; scopeOptions.value = data.scope_options || [];
    loadedAt.value = data.loaded_at || 0; warning.value = data.warning || '';
    for (const id of selected) if (!items.value.some(x => x.id === id && canEdit(x))) selected.delete(id);
  } catch (e: any) { if (!abort.signal.aborted) error.value = e.message || '账号加载失败'; }
  finally { loading.value = false; }
}
async function loadAudit() {
  auditLoading.value = true;
  try { audits.value = (await requestJson('/api/auth/accounts/audit', { signal: abort.signal })).items || []; }
  catch (e: any) { if (!abort.signal.aborted) error.value = e.message; }
  finally { auditLoading.value = false; }
}
function switchTab(value: string) { tab.value = value; if (value === 'audit') void loadAudit(); }
function openAccount(item: Account) { notice.value = ''; editorError.value = ''; newPassword.value = ''; confirmPassword.value = ''; draft.value = { ...item, scopes: [...item.scopes] }; }
function openBulk(mode: string) { editorError.value = ''; bulkMode.value = mode; bulkScopes.value = []; }
function closeEditor() { if (saving.value) return; draft.value = null; bulkMode.value = ''; newPassword.value = ''; confirmPassword.value = ''; editorError.value = ''; }
function permissionChange(item: Account) { return { id: item.id, revision: item.revision, role: item.role, scopes: item.scopes, enabled: item.enabled }; }
async function savePermissions(changes: ReturnType<typeof permissionChange>[]) {
  if (saving.value) return;
  saving.value = true; editorError.value = '';
  try {
    await requestJson('/api/auth/accounts/permissions', { method: 'POST', body: JSON.stringify({ changes }), signal: abort.signal });
    notice.value = `已保存 ${changes.length} 个账号的权限`; selected.clear();
    saving.value = false; closeEditor(); await load(); emit('changed');
  } catch (e: any) { editorError.value = e.message || '权限保存失败'; }
  finally { saving.value = false; }
}
function saveAccount() { if (draft.value) void savePermissions([permissionChange(draft.value)]); }
function saveBulk() {
  const changes = selectedItems.value.map(item => ({ ...permissionChange(item), scopes: bulkMode.value === 'add'
    ? [...new Set([...item.scopes, ...bulkScopes.value])] : item.scopes.filter(scope => !bulkScopes.value.includes(scope)) }));
  void savePermissions(changes);
}
async function savePassword() {
  if (!draft.value || saving.value) return;
  editorError.value = '';
  if (newPassword.value !== confirmPassword.value) { editorError.value = '两次输入的密码不一致'; return; }
  if (newPassword.value.trim().length < 8 || newPassword.value.length > 128) { editorError.value = '新密码须为8至128个字符'; return; }
  saving.value = true;
  try {
    await requestJson(`/api/auth/accounts/${encodeURIComponent(draft.value.record_id)}/password`, { method: 'POST', signal: abort.signal,
      body: JSON.stringify({ new_password: newPassword.value, confirm_password: confirmPassword.value }) });
    newPassword.value = ''; confirmPassword.value = ''; draft.value.password_set = true;
    notice.value = `已修改${draft.value.name}的灯塔登录密码，该人员需重新登录`; await load();
  } catch (e: any) { editorError.value = e.message || '密码修改未完成'; }
  finally { saving.value = false; }
}
function escape(event: KeyboardEvent) {
  if (!modal?.isTop(event, editor.value)) return;
  if (event.key === 'Escape') { event.stopImmediatePropagation(); closeEditor(); }
  if (event.key === 'Tab') {
    const controls = Array.from(editor.value?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled)') || []);
    const first = controls[0], last = controls[controls.length - 1];
    if (event.shiftKey && (document.activeElement === first || document.activeElement === editor.value)) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  }
}
watch(() => !!draft.value || !!bulkMode.value, async open => {
  if (open) { modal = acquireModal(); await nextTick(); editor.value?.focus(); }
  else { modal?.release(); modal = null; }
});
watch([search, kind, building, state], () => page.value = 1);
watch(pageCount, value => page.value = Math.min(page.value, value));
watch(() => JSON.stringify(props.permissions), () => { if (!saving.value) void load(); });
onMounted(() => { void load(); window.addEventListener('keydown', escape, true); });
onBeforeUnmount(() => { abort.abort(); newPassword.value = ''; confirmPassword.value = ''; modal?.release(); window.removeEventListener('keydown', escape, true); });
</script>

<style scoped>
.accounts { display: grid; gap: 16px; color: #203650; }
.account-header,.filters,.bulk,.pagination,.tabs { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; }
.account-header strong { font-size: 19px; margin-right: auto; }.muted,small { color: #63778e; font-size: 12px; }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; border: 1px solid #ccd8e6; background: #fff; color: #294662; border-radius: 7px; min-height: 36px; padding: 6px 11px; font: inherit; font-size: 13px; cursor: pointer; }
button:hover:not(:disabled) { background: #eef5ff; border-color: #9cbef0; }button:disabled { opacity: .5; cursor: not-allowed; }
button:focus-visible,input:focus-visible,select:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
.metrics { display: grid; grid-template-columns: repeat(4,1fr); border-block: 1px solid #dce6f0; padding: 16px 0; }.metrics div { display: grid; gap: 6px; padding-left: 16px; border-left: 2px solid #d6e6fd; }.metrics span { font-size: 12px; color: #63778e; }.metrics b { font-size: 24px; }
.tabs { border-bottom: 1px solid #dce6f0; gap: 20px; }.tabs button { border: 0; border-bottom: 3px solid transparent; border-radius: 0; background: transparent; padding: 9px 0; }.tabs .active { color: #155adb; border-bottom-color: #2563eb; font-weight: 700; }
.filters label { display: flex; align-items: center; gap: 7px; font-size: 12px; }.filters .search { flex: 1; min-width: 180px; border: 1px solid #ccd8e6; border-radius: 7px; padding-left: 9px; }.search input { border: 0; width: 100%; }
input:not([type=checkbox]),select { min-height: 36px; padding: 7px 9px; border: 1px solid #ccd8e6; border-radius: 7px; font: inherit; color: #203650; background: #fff; min-width: 0; }input[type=checkbox] { accent-color: #2563eb; width: 16px; height: 16px; }
.table-wrap { overflow: auto; }table { width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; }th { background: #f0f5fb; color: #506884; white-space: nowrap; font-weight: 600; }th,td { border-bottom: 1px solid #e0e8f1; padding: 11px 10px; }td small { display: block; margin-top: 4px; }td:first-child { width: 34px; }tr.selected { background: #eef5ff; }.scope-cell { max-width: 240px; line-height: 1.6; }.tag,.status { display: inline-block; padding: 3px 6px; background: #f0f4f9; border-radius: 4px; font-size: 12px; white-space: nowrap; }.ok { background: #e6f5ed; color: #186742; }.warn { background: #fff2df; color: #8b4c03; }.link-button { border: 0; color: #1d60c6; white-space: nowrap; }.empty { padding: 32px; text-align: center; color: #63778e; }
.pagination { justify-content: flex-end; font-size: 13px; }.pagination > span:first-child { margin-right: auto; }.bulk { padding: 10px; background: #edf5ff; border-radius: 7px; }
.error,.warning,.success { margin: 0; padding: 10px 12px; border-radius: 6px; font-size: 13px; line-height: 1.6; }.error { background: #fff0f0; color: #a92b2b; }.warning { background: #fff7e7; color: #805600; }.success { background: #eaf7ef; color: #206a44; }
.account-overlay { position: fixed; inset: 0; z-index: 4000; background: #142b4938; display: flex; justify-content: flex-end; }.account-editor { width: 500px; max-width: 100%; height: 100%; overflow: auto; background: #fff; color: #203650; box-shadow: -10px 0 36px #19355526; padding: 22px; outline: none; }.account-editor header { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; border-bottom: 1px solid #dce6f0; padding-bottom: 16px; }.account-editor h2 { margin: 0 0 6px; font-size: 19px; }.icon { width: 38px; padding: 0; }.editor-body { display: grid; gap: 22px; padding-top: 16px; }.account-editor dl { display: grid; grid-template-columns: 80px 1fr; margin: 0; gap: 10px; font-size: 13px; }.account-editor dt { color: #63778e; }.account-editor dd { margin: 0; }.identity { overflow-wrap: anywhere; }
fieldset { border: 0; border-top: 1px solid #dce6f0; padding: 15px 0 0; margin: 0; display: grid; gap: 12px; min-width: 0; }legend { font-weight: 700; padding-right: 10px; font-size: 14px; }.form-row,.check { display: flex; align-items: center; gap: 10px; font-size: 13px; }.scopes { display: flex; flex-wrap: wrap; gap: 12px 18px; }.scopes label { display: inline-flex; align-items: center; gap: 5px; font-size: 13px; }.primary { background: #1f62de; border-color: #1f62de; color: white; justify-self: start; }.primary:hover:not(:disabled) { background: #154cad; color: white; }form label { font-size: 13px; }.editor-body ul { padding-left: 20px; font-size: 13px; line-height: 1.9; }.spin { animation: account-spin 1s linear infinite; }@keyframes account-spin { to { transform: rotate(360deg); } }
@media(max-width:600px) { .metrics { grid-template-columns: repeat(2,1fr); gap: 14px; }.filters .search { flex-basis: 100%; }.account-editor { padding: 16px; } }
@media(prefers-reduced-motion:reduce) { .spin { animation: none; } }
</style>
