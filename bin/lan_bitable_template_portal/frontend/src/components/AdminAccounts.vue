<template>
  <section class="accounts">
    <header class="account-header">
      <strong>账号与权限</strong>
      <span class="muted">{{ loadedAt ? `人员同步于 ${formatTime(loadedAt)}` : '人员目录未同步' }}</span>
      <button type="button" :disabled="loading || saving" @click="load(true)"><RefreshCw :size="16" :class="{ spin: loading }" />同步人员</button>
    </header>
    <div v-if="myLevel !== 'L2'" class="metrics">
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
        <label class="search"><Search :size="16" /><input v-model.trim="search" type="search" aria-label="搜索账号" placeholder="姓名、工号、账号名或岗位" /></label>
        <label><span>类型</span><select v-model="kind"><option value="">全部类型</option><option v-for="(label, value) in kinds" :key="value" :value="value">{{ label }}</option></select></label>
        <label><span>所属楼栋</span><select v-model="building"><option value="">全部楼栋</option><option v-for="scope in ownScopeOptions" :key="scope.value" :value="scope.value">{{ scope.label }}</option></select></label>
        <label><span>状态</span><select v-model="state"><option value="">全部状态</option><option value="ready">已授权</option><option value="pending">未授权</option><option value="disabled">已停用</option><option value="blocked">待核对</option><option value="admin">管理员</option></select></label>
      </div>
      <nav class="category-tabs" aria-label="岗位分类筛选">
        <button v-for="option in categoryTabs" :key="option.value || 'all'" type="button" :class="{ active: category === option.value }" :aria-pressed="category === option.value" @click="category = option.value">{{ option.label }}<span>{{ option.count }}</span></button>
      </nav>
      <LoadingIndicator v-if="loading && !items.length">正在读取账号</LoadingIndicator>
      <div v-else class="table-wrap">
        <table>
          <thead><tr><th>人员 / 账号</th><th>岗位分类</th><th>类型</th><th>所属楼栋</th><th>级别</th><th>角色</th><th>可访问楼栋</th><th>状态</th><th v-if="myLevel !== 'L2'">最近登录</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="item in pageItems" :key="item.id">
              <td><b>{{ item.name }}</b><small v-if="myLevel !== 'L2'">{{ item.employee_no || (item.kind === 'duty' ? '固定值班账号' : '工号未填') }}</small></td>
              <td class="category-cell"><span class="tag">{{ categoryLabel(item.category) }}</span><small v-if="item.position" :title="item.position">原岗位：{{ item.position }}</small><small v-else>原岗位未填写</small></td>
              <td><span class="tag">{{ kinds[item.kind] }}</span></td><td>{{ scopesLabel(item.own_scopes) }}</td>
              <td><span class="tag level">{{ levelLabel(item.level) }}</span></td>
              <td>{{ item.locked ? '固定管理员' : item.role === 'admin' ? '管理员' : '普通用户' }}</td>
              <td class="scope-cell">{{ scopesLabel(item.scopes) }}</td>
              <td><span :class="['status', !item.enabled || item.blocked_reason ? 'warn' : item.scopes.length ? 'ok' : '']" :title="item.blocked_reason">{{ stateLabel(item) }}</span></td>
              <td v-if="myLevel !== 'L2'" class="muted">{{ formatTime(item.last_login) }}</td>
              <td class="account-actions"><button type="button" class="link-button" :disabled="saving" :aria-label="`管理${item.name}`" @click="openAccount(item)"><Settings2 :size="15" />管理</button><button v-if="item.can_delete" type="button" class="link-button danger" :disabled="saving" :aria-label="`撤销${item.name}的登录权限`" @click="deleteAccount(item)"><Trash2 :size="15" />撤销</button></td>
            </tr>
            <tr v-if="!filtered.length"><td :colspan="myLevel !== 'L2' ? 10 : 9" class="empty">没有匹配的账号</td></tr>
          </tbody>
        </table>
      </div>
      <footer class="pagination"><span>共 {{ filtered.length }} 个账号</span><button type="button" :disabled="page <= 1" @click="page--"><ChevronLeft :size="16" />上一页</button><span>{{ page }} / {{ pageCount }}</span><button type="button" :disabled="page >= pageCount" @click="page++">下一页<ChevronRight :size="16" /></button></footer>
    </template>
    <slot v-else-if="tab === 'requests'" />
    <template v-else>
      <button type="button" :disabled="auditLoading || saving" @click="loadAudit"><RefreshCw :size="16" :class="{ spin: auditLoading }" />刷新记录</button>
      <div class="table-wrap"><table><thead><tr><th>时间</th><th>操作人</th><th>操作</th><th>账号及变更</th><th>结果</th></tr></thead>
        <tbody><tr v-for="row in audits" :key="row.audit_id"><td>{{ formatTime(row.updated_at) }}</td><td>{{ row.actor_name || '管理员' }}</td><td>{{ actionLabel(row.action) }}</td><td><template v-if="row.action === 'password' || row.action === 'delete'">{{ row.metadata?.name }}</template><div v-for="(change, i) in row.metadata?.changes || []" :key="i">{{ change.name }}：{{ scopesLabel(change.before?.scopes || []) }} → {{ scopesLabel(change.after?.scopes || []) }}；{{ change.after?.enabled === false ? '停用' : change.after?.role === 'admin' ? '管理员' : '普通用户' }}</div></td><td>{{ statusLabel(row.status) }}</td></tr><tr v-if="!audits.length"><td colspan="5" class="empty">暂无变更记录</td></tr></tbody></table></div>
    </template>
    <Teleport to="body">
      <UiTransition name="ui-overlay">
        <div v-if="draft" class="account-overlay" @click.self="closeEditor">
          <section ref="editor" class="account-editor" role="dialog" aria-modal="true" aria-labelledby="account-editor-title" tabindex="-1">
            <header><div><h2 id="account-editor-title">{{ draft?.name }}</h2><span class="muted">{{ draft ? kinds[draft.kind] : '' }}</span></div><button type="button" class="icon" aria-label="关闭账号详情" :disabled="saving" @click="closeEditor"><X :size="20" /></button></header>
            <p v-if="editorError" class="error" role="alert">{{ editorError }}</p>
            <p v-if="notice" class="success" role="status">{{ notice }}</p>
            <div v-if="draft" class="editor-body" :class="{ 'access-readonly': !canEditAccess(draft) }">
              <dl><template v-if="myLevel !== 'L2'"><dt>工号</dt><dd>{{ draft.employee_no || '未填写' }}</dd></template><dt>所属楼栋</dt><dd>{{ scopesLabel(draft.own_scopes) }}</dd><dt>权限级别</dt><dd>{{ levelLabel(draft.level) }}</dd><dt>角色</dt><dd>{{ draft.role === 'admin' ? '管理员' : '普通用户' }}</dd><dt>可访问楼栋</dt><dd>{{ scopesLabel(draft.scopes) }}</dd><dt>可用模块</dt><dd :title="moduleLabel(draft.modules)">{{ (draft.modules || []).length }} 项</dd><dt>登录方式</dt><dd>{{ draft.can_set_password ? '姓名密码登录' : '飞书登录' }}</dd><dt>OpenID</dt><dd class="identity">{{ draft.open_id || '未关联' }}</dd></dl>
              <p v-if="draft.blocked_reason" class="warning">{{ draft.blocked_reason }}</p>
              <fieldset class="account-state" :disabled="saving || !canEdit(draft)"><legend>账号状态</legend>
                <label class="check"><input v-model="draft.enabled" type="checkbox" />启用登录</label>
                <p v-if="!canEdit(draft)" class="warning">当前级别只能查看该账号，不能修改。角色与可访问楼栋由HR统一管理。</p>
                <button type="button" class="primary" @click="saveEnabled"><Save :size="16" />{{ saving ? '正在保存' : '保存启用状态' }}</button>
              </fieldset>
              <fieldset v-if="canEditAccess(draft)" class="account-access" :disabled="saving"><legend>访问权限</legend>
                <p class="muted">此处设置的级别、岗位、楼栋与模块会覆盖HR默认授权。</p>
                <label><span>权限级别</span>
                  <select v-model="draft.level" aria-label="权限级别" :disabled="isFixedDuty" @change="changeLevel">
                    <option v-for="opt in accessLevelOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
                  </select>
                  <small v-if="isFixedDuty" class="muted">固定值班账号级别不可修改。</small>
                </label>
                <label><span>岗位分类</span>
                  <select v-model="draft.category" aria-label="岗位分类">
                    <option v-for="opt in accessCategoryOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
                  </select>
                </label>
                <fieldset class="check-group" :disabled="draft.level === 'L1'"><legend>可访问楼栋（多选）</legend>
                  <label class="check" v-for="scope in accessScopeOptions" :key="scope.value">
                    <input type="checkbox" :value="scope.value" v-model="draft.scopes" />{{ scope.label }}
                  </label>
                </fieldset>
                <div class="module-groups">
                  <fieldset v-for="group in moduleGroups" :key="group.label" class="module-group"><legend>{{ group.label }}</legend>
                    <div class="module-options"><label class="check" v-for="mod in group.options" :key="mod.value" :class="{ granted: draft.modules?.includes(mod.value) }">
                      <input type="checkbox" :value="mod.value" v-model="draft.modules" :disabled="mod.value === 'accounts' && draft.level === 'L4'" />{{ mod.label }}
                    </label></div>
                  </fieldset>
                </div>
              </fieldset>
              <form v-if="draft.can_set_password" class="account-password" @submit.prevent="savePassword">
                <fieldset :disabled="saving"><legend>修改灯塔登录密码</legend><p class="muted">{{ draft.password_set ? '密码已设置' : '尚未设置正式密码' }} · 保存后该人员需重新登录</p>
                  <label for="admin-account-new-password">新密码</label><input id="admin-account-new-password" v-model="newPassword" type="password" autocomplete="new-password" minlength="8" maxlength="128" required />
                  <label for="admin-account-confirm-password">再次输入新密码</label><input id="admin-account-confirm-password" v-model="confirmPassword" type="password" autocomplete="new-password" minlength="8" maxlength="128" required />
                  <button type="submit" class="primary"><KeyRound :size="16" />{{ saving ? '正在保存' : '确认修改密码' }}</button>
                </fieldset>
              </form>
              <p v-else class="muted">{{ draft.kind === 'vnet' || draft.kind === 'duty' ? '飞书账号密码请在飞书中修改。' : '该账号未关联可使用密码登录的在岗人员。' }}</p>
            </div>
            <footer class="editor-footer"><span class="muted">{{ (draft?.modules || []).length }} 项模块权限</span><button type="button" :disabled="saving" @click="closeEditor">关闭</button><button v-if="draft && canEditAccess(draft)" type="button" class="primary" :disabled="saving" @click="saveAccess"><Save :size="16" />{{ saving ? '正在保存' : '保存权限' }}</button></footer>
          </section>
        </div>
      </UiTransition>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ChevronLeft, ChevronRight, KeyRound, RefreshCw, Save, Search, Settings2, Trash2, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { acquireModal } from '../modalState';
type Account = { id: string; record_id: string; open_id: string; name: string; employee_no: string; kind: string; account_nature: string; position: string; category: string; own_scopes: string[]; scopes: string[]; role: string; enabled: boolean; authorized: boolean; locked: boolean; blocked_reason: string; can_set_password: boolean; password_set: boolean; revision: string; last_login?: number; level?: string; can_edit?: boolean; can_delete?: boolean; modules?: string[]; can_edit_access?: boolean; home_scope?: string };
const props = defineProps<{ pendingCount: number; permissions: Dict[] }>();
const emit = defineEmits<{ changed: [] }>();
const kinds: Record<string, string> = { vnet: 'VNET人员', external: '非VNET人员', duty: '值班账号', legacy: '历史账号', unknown: '类型待核对' };
const levelLabels: Record<string, string> = { L0: 'L0 超级管理员', L1: 'L1 跨楼超级管理员', L2: 'L2 工程师编辑', L3: 'L3 楼栋主管', L4: 'L4 普通账号' };
function levelLabel(level?: string) { return level ? levelLabels[level] || level : '未分级'; }
const policy = ref<Dict>({});
const myLevel = computed(() => String(policy.value.level || ''));
const tabs = computed(() => {
  const level = myLevel.value;
  if (level === 'L2') return [{ value: 'accounts', label: '账号列表' }];
  const list = [{ value: 'accounts', label: '账号列表' }];
  if (level !== 'L3') list.push({ value: 'requests', label: '权限申请' });
  list.push({ value: 'audit', label: '变更记录' });
  return list;
});
const tab = ref('accounts'), items = ref<Account[]>([]), scopeOptions = ref<{value: string; label: string}[]>([]), categoryOptions = ref<{value: string; label: string}[]>([]), moduleOptions = ref<{value: string; label: string; group?: string}[]>([]), levelOptions = ref<{value: string; label: string}[]>([]);
const search = ref(''), kind = ref(''), building = ref(''), state = ref(''), category = ref(''), page = ref(1);
const loading = ref(false), saving = ref(false), error = ref(''), notice = ref(''), warning = ref(''), loadedAt = ref(0);
const draft = ref<Account | null>(null), editor = ref<HTMLElement | null>(null);
const editorError = ref(''), newPassword = ref(''), confirmPassword = ref('');
const audits = ref<Dict[]>([]), auditLoading = ref(false);
let modal: ReturnType<typeof acquireModal> | null = null;
const abort = new AbortController();
const ownScopeOptions = computed(() => scopeOptions.value.filter(x => !['ALL', 'CAMPUS'].includes(x.value)));
const baseFiltered = computed(() => items.value.filter(x => (!search.value || `${x.name} ${x.employee_no} ${x.position}`.toLowerCase().includes(search.value.toLowerCase()))
  && (!kind.value || x.kind === kind.value) && (!building.value || x.own_scopes.includes(building.value))
  && (!state.value || (state.value === 'admin' ? x.role === 'admin' : state.value === 'blocked' ? !!x.blocked_reason : state.value === 'disabled' ? !x.enabled : state.value === 'pending' ? !x.scopes.length && x.enabled && !x.blocked_reason : x.enabled && !!x.scopes.length && !x.blocked_reason))));
const filtered = computed(() => category.value ? baseFiltered.value.filter(x => x.category === category.value) : baseFiltered.value);
const categoryTabs = computed(() => {
  const counts = new Map<string, number>();
  for (const item of baseFiltered.value) counts.set(item.category, (counts.get(item.category) || 0) + 1);
  const all = { value: '', label: '全部', count: baseFiltered.value.length };
  // Keep the server-declared category order stable: never filter out zero-count
  // categories and never reorder by count, so tabs do not move/hide while
  // searching and the selected category cannot disappear.
  const options = categoryOptions.value.map(option => ({ ...option, count: counts.get(option.value) || 0 }));
  return [all, ...options];
});
function categoryLabel(value: string) { return categoryOptions.value.find(x => x.value === value)?.label || value || '待分类'; }
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / 20)));
const pageItems = computed(() => filtered.value.slice((page.value - 1) * 20, page.value * 20));
function canEdit(item: Account) { return item.can_edit === true; }
// Access grants (level/category/scopes/modules) are governed by the backend's
// per-account `can_edit_access` flag, which already incorporates `can_edit`.
function canEditAccess(item: Account) { return item.can_edit_access === true; }
// H-building fixed duty accounts never have their level changed, regardless of
// anything else the access panel offers.
const isFixedDuty = computed(() => draft.value?.kind === 'duty' && (draft.value.home_scope === 'H' || draft.value.own_scopes?.includes('H')));
const actorModules = computed(() => Array.isArray(policy.value?.modules) ? policy.value.modules as string[] : []);
const isSuperAdmin = computed(() => ['L0', 'L1'].includes(myLevel.value));
const accessLevelOptions = computed(() => levelOptions.value);
const accessCategoryOptions = computed(() => categoryOptions.value);
const accessModuleOptions = computed(() => moduleOptions.value.filter(option => actorModules.value.includes(option.value)));
const moduleGroups = computed(() => ['业务管理', '通告办理', '协作与资料', '系统管理', '功能模块'].map(label => ({ label, options: accessModuleOptions.value.filter(option => (option.group || '功能模块') === label) })).filter(group => group.options.length));
const accessScopeOptions = computed(() => {
  const options = scopeOptions.value;
  if (!draft.value) return options;
  if (isSuperAdmin.value) return options;
  const own = new Set(draft.value.own_scopes || []);
  return options.filter(option => own.has(option.value) || option.value === draft.value?.home_scope);
});

function modulesForLevel(level?: string): string[] {
  return accessModuleOptions.value.filter(option => level !== 'L4' || option.value !== 'accounts').map(option => option.value);
}
function scopesForLevel(level?: string): string[] {
  const d = draft.value;
  if (!d) return [];
  if (level === 'L1') return accessScopeOptions.value.map(option => option.value);
  const own = new Set([...(d.own_scopes || []), ...(d.home_scope ? [d.home_scope] : [])]);
  return accessScopeOptions.value.filter(option => own.has(option.value)).map(option => option.value);
}
function roleForLevel(level?: string): string {
  return level === 'L1' ? 'admin' : 'building';
}

// Snapshot of the opened account's access fields so a rejected save can restore
// the old (server-reported) role/modules instead of leaving an incompatible draft.
let draftOriginal: Pick<Account, 'level' | 'category' | 'scopes' | 'modules' | 'role'> | null = null;

function moduleLabel(modules?: string[]) {
  const list = Array.isArray(modules) ? modules : [];
  if (!list.length) return '未配置';
  return list.map(value => moduleOptions.value.find(option => option.value === value)?.label || value).join('、');
}
function actionLabel(action: string) { return action === 'password' ? '修改密码' : action === 'delete' ? '撤销登录' : '修改权限'; }
function statusLabel(status: string) { return status === 'success' ? '成功' : status === 'failed' ? '未完成' : status === 'denied' ? '已拒绝' : '处理中'; }
function stateLabel(item: Account) { return item.blocked_reason ? '待核对' : !item.enabled ? '已停用' : item.scopes.length ? '已授权' : '未授权'; }
function scopesLabel(scopes: string[]) { return scopes.map(scope => scopeOptions.value.find(x => x.value === scope)?.label || scope).join('、') || '未配置'; }
function formatTime(timestamp?: number) { return timestamp ? new Date(timestamp * 1000).toLocaleString('zh-CN', { hour12: false }) : '暂无记录'; }
async function load(refresh = false) {
  if (loading.value) return;
  loading.value = true; error.value = '';
  try {
    const data = await requestJson(`/api/auth/accounts${refresh ? '?refresh=true' : ''}`, { signal: abort.signal });
    items.value = data.items || []; scopeOptions.value = data.scope_options || []; categoryOptions.value = data.category_options || [];
    moduleOptions.value = data.module_options || []; levelOptions.value = data.level_options || [];
    policy.value = data.policy || {};
    loadedAt.value = data.loaded_at || 0; warning.value = data.warning || '';
    // Permission reload downgrade guard: switch stale tabs back to accounts and close a
    // stale editor when the acting profile no longer has access to the open target.
    const available = tabs.value.map(option => option.value);
    if (!available.includes(tab.value)) tab.value = 'accounts';
    if (draft.value) {
      const currentDraft = draft.value;
      const fresh = items.value.find(item => item.id === currentDraft.id);
      if (!fresh || !canEdit(fresh)) closeEditor();
    }
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
function openAccount(item: Account) {
  notice.value = ''; editorError.value = ''; newPassword.value = ''; confirmPassword.value = '';
  draft.value = { ...item, scopes: [...(item.scopes || item.own_scopes || [])], modules: [...(item.modules || [])] };
  draftOriginal = {
    level: item.level,
    category: item.category,
    scopes: [...(item.scopes || item.own_scopes || [])],
    modules: [...(item.modules || [])],
    role: item.role,
  };
}
function closeEditor() { if (saving.value) return; draft.value = null; draftOriginal = null; newPassword.value = ''; confirmPassword.value = ''; editorError.value = ''; }
function permissionChange(item: Account) { return { id: item.id, revision: item.revision, role: item.role, scopes: item.scopes, enabled: item.enabled }; }
function accessChange(item: Account) { return { id: item.id, revision: item.revision, role: item.role, scopes: item.scopes, enabled: item.enabled, level: item.level, category: item.category, modules: item.modules }; }
function dispatchAuthChanged() { window.dispatchEvent(new Event('auth-account-changed')); }
async function savePermissions(changes: ReturnType<typeof permissionChange>[]) {
  if (saving.value) return;
  saving.value = true; editorError.value = '';
  try {
    await requestJson('/api/auth/accounts/permissions', { method: 'POST', body: JSON.stringify({ changes }), signal: abort.signal });
    notice.value = `已保存 ${changes.length} 个账号的权限`;
    saving.value = false; closeEditor(); await load(); emit('changed'); dispatchAuthChanged();
  } catch (e: any) {
    editorError.value = e.message || '权限保存失败';
    // A rejected save must keep the old (server-reported) role/modules rather than
    // leaving an incompatible level/provided combination on screen.
    if (draft.value && draftOriginal) {
      draft.value.level = draftOriginal.level;
      draft.value.category = draftOriginal.category;
      draft.value.scopes = [...draftOriginal.scopes];
      draft.value.modules = [...(draftOriginal.modules || [])];
      draft.value.role = draftOriginal.role;
    }
  }
  finally { saving.value = false; }
}
function saveEnabled() { if (draft.value) void savePermissions([{ ...permissionChange(draft.value), role: draftOriginal?.role || draft.value.role, scopes: [...(draftOriginal?.scopes || draft.value.scopes)] }]); }
function saveAccess() { if (draft.value) void savePermissions([accessChange(draft.value)]); }
async function deleteAccount(item: Account) {
  if (saving.value) return;
  const target = item.name;
  const confirmed = window.confirm(`确定撤销「${target}」的登录权限吗？\n撤销后该人员将无法登录系统，但其HR、学习与业务历史记录会完整保留。`);
  if (!confirmed) return;
  saving.value = true; error.value = '';
  try {
    await requestJson(`/api/auth/accounts/${encodeURIComponent(item.id)}`, { method: 'DELETE', signal: abort.signal, body: JSON.stringify({ revision: item.revision }) });
    notice.value = `已撤销${target}的登录权限（历史记录保留）`;
    await load(); emit('changed'); dispatchAuthChanged();
  } catch (e: any) { if (!abort.signal.aborted) error.value = e.message || '撤销登录权限未完成'; }
  finally { saving.value = false; }
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
watch(() => !!draft.value, async open => {
  if (open) { modal = acquireModal(); await nextTick(); editor.value?.focus(); }
  else { modal?.release(); modal = null; }
});
watch([search, kind, building, state, category], () => { page.value = 1; });
watch(pageCount, value => page.value = Math.min(page.value, value));
function changeLevel(): void {
  const d = draft.value;
  if (!d || !d.level) return;
  const level = d.level;
  d.role = roleForLevel(level);
  d.scopes = scopesForLevel(level);
  d.modules = modulesForLevel(level);
}
watch(() => JSON.stringify(props.permissions), () => { if (!saving.value) void load(); });
onMounted(() => { void load(); window.addEventListener('keydown', escape, true); });
onBeforeUnmount(() => { abort.abort(); newPassword.value = ''; confirmPassword.value = ''; modal?.release(); window.removeEventListener('keydown', escape, true); });
</script>

<style scoped>
.accounts { display: grid; gap: 16px; color: #203650; }
.account-header,.filters,.pagination,.tabs { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; }
.account-header strong { font-size: 19px; margin-right: auto; }.muted,small { color: #63778e; font-size: 12px; }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; border: 1px solid #ccd8e6; background: #fff; color: #294662; border-radius: 7px; min-height: 36px; padding: 6px 11px; font: inherit; font-size: 13px; cursor: pointer; }
button:hover:not(:disabled) { background: #eef5ff; border-color: #9cbef0; }button:disabled { opacity: .5; cursor: not-allowed; }
button:focus-visible,input:focus-visible,select:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
.metrics { display: grid; grid-template-columns: repeat(4,1fr); border-block: 1px solid #dce6f0; padding: 16px 0; }.metrics div { display: grid; gap: 6px; padding-left: 16px; border-left: 2px solid #d6e6fd; }.metrics span { font-size: 12px; color: #63778e; }.metrics b { font-size: 24px; }
.tabs { border-bottom: 1px solid #dce6f0; gap: 20px; }.tabs button { border: 0; border-bottom: 3px solid transparent; border-radius: 0; background: transparent; padding: 9px 0; }.tabs .active { color: #155adb; border-bottom-color: #2563eb; font-weight: 700; }
.filters label { display: flex; align-items: center; gap: 7px; font-size: 12px; }.filters .search { flex: 1; min-width: 180px; border: 1px solid #ccd8e6; border-radius: 7px; padding-left: 9px; }.search input { border: 0; width: 100%; }
.category-tabs { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; padding: 2px 0 4px; }.category-tabs button { border-radius: 7px; min-height: 30px; padding: 4px 10px; background: #f0f5fb; border-color: #e0e8f1; color: #294662; }.category-tabs button:hover:not(:disabled) { background: #e6efff; border-color: #9cbef0; }.category-tabs button.active { background: #e6efff; border-color: #9cbef0; color: #155adb; font-weight: 700; }.category-tabs button span { margin-left: 5px; font-size: 11px; color: #506884; }.category-tabs button.active span { color: #155adb; }
input:not([type=checkbox]),select { min-height: 36px; padding: 7px 9px; border: 1px solid #ccd8e6; border-radius: 7px; font: inherit; color: #203650; background: #fff; min-width: 0; }input[type=checkbox] { accent-color: #2563eb; width: 16px; height: 16px; }
.table-wrap { overflow: auto; }table { width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; }th { background: #f0f5fb; color: #506884; white-space: nowrap; font-weight: 600; }th,td { border-bottom: 1px solid #e0e8f1; padding: 11px 10px; }td small { display: block; margin-top: 4px; }.scope-cell { max-width: 240px; line-height: 1.6; }.category-cell { min-width: 130px; line-height: 1.6; }.category-cell small { max-width: 200px; overflow-wrap: anywhere; }.tag,.status { display: inline-block; padding: 3px 6px; background: #f0f4f9; border-radius: 4px; font-size: 12px; white-space: nowrap; }.tag.level { background: #e8f0fe; color: #1d5fbf; }.ok { background: #e6f5ed; color: #186742; }.warn { background: #fff2df; color: #8b4c03; }.link-button { border: 0; color: #1d60c6; white-space: nowrap; }.link-button.danger { color: #b3231e; }.account-actions { white-space: nowrap; }.empty { padding: 32px; text-align: center; color: #63778e; }
.pagination { justify-content: flex-end; font-size: 13px; }.pagination > span:first-child { margin-right: auto; }
.error,.warning,.success { margin: 0; padding: 10px 12px; border-radius: 6px; font-size: 13px; line-height: 1.6; }.error { background: #fff0f0; color: #a92b2b; }.warning { background: #fff7e7; color: #805600; }.success { background: #eaf7ef; color: #206a44; }
.account-overlay { position: fixed; inset: 0; z-index: 4000; background: #142b4959; display: flex; align-items: center; justify-content: center; padding: 24px; }.account-editor { width: min(1180px, 100%); max-height: 92vh; overflow: hidden; display: flex; flex-direction: column; background: #fff; border: 1px solid #dce6f0; border-radius: 8px; color: #203650; box-shadow: 0 20px 64px #19355538; outline: none; }.account-editor header { flex: none; display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; border-bottom: 1px solid #dce6f0; padding: 18px 24px; background: #f4f8fe; }.account-editor h2 { margin: 0 0 6px; font-size: 19px; }.icon { width: 38px; padding: 0; }.editor-body { min-height: 0; overflow: auto; display: grid; grid-template-columns: 270px minmax(0, 1fr); gap: 22px 28px; padding: 22px 24px; align-items: start; }.account-editor dl { grid-column: 1; display: grid; grid-template-columns: 80px minmax(0, 1fr); margin: 0; gap: 10px; font-size: 13px; }.account-editor dt { color: #63778e; }.account-editor dd { margin: 0; overflow-wrap: anywhere; }.identity { overflow-wrap: anywhere; }
.account-state, .account-password, .editor-body > p { grid-column: 1; }.editor-body .account-access { grid-column: 2; grid-row: 1 / span 5; border-top: 0; padding: 0 0 0 26px; border-left: 1px solid #dce6f0; }.account-access > label { display: grid; grid-template-columns: 100px 1fr; align-items: center; gap: 10px; }.module-groups { display: grid; gap: 18px; }.editor-body .module-group { padding-top: 10px; }.module-options { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 5px; }.module-options .check { min-height: 38px; padding: 6px 8px; line-height: 1.5; border-radius: 4px; cursor: pointer; }.module-options .check:hover { background: #f1f5fb; }.module-options .granted { background: #edf4ff; color: #1555b6; }.module-options input { flex: none; }.editor-footer { flex: none; display: flex; align-items: center; justify-content: flex-end; gap: 10px; padding: 14px 24px; border-top: 1px solid #dce6f0; background: #f8fafc; }.editor-footer > span { margin-right: auto; }.account-editor > .error, .account-editor > .success { margin: 10px 24px 0; }.editor-body.access-readonly { grid-template-columns: 1fr; }.account-editor:has(.access-readonly) { width: min(640px, 100%); }
fieldset { border: 0; border-top: 1px solid #dce6f0; padding: 15px 0 0; margin: 0; display: grid; gap: 12px; min-width: 0; }legend { font-weight: 700; padding-right: 10px; font-size: 14px; }.check { display: flex; align-items: center; gap: 10px; font-size: 13px; }.primary { background: #1f62de; border-color: #1f62de; color: white; justify-self: start; }.primary:hover:not(:disabled) { background: #154cad; color: white; }form label { font-size: 13px; }.spin { animation: account-spin 1s linear infinite; }@keyframes account-spin { to { transform: rotate(360deg); } }
.editor-body .check-group { border: 1px solid #dce6f0; border-radius: 7px; padding: 10px 12px 6px; background: #f8fafc; }.check-group legend { font-size: 12px; color: #506884; }.editor-body label { font-size: 13px; }.editor-body label > span { display: block; margin-bottom: 5px; color: #506884; }
.editor-body .check-group { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 14px; padding-bottom: 12px; }
.account-access .check-group { grid-template-columns: repeat(4, minmax(0, 1fr)); }
@media(max-width:600px) { .metrics { grid-template-columns: repeat(2,1fr); gap: 14px; }.filters .search { flex-basis: 100%; }.account-editor { padding: 16px; } }
@media(prefers-reduced-motion:reduce) { .spin { animation: none; } }
</style>
