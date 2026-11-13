<template>
  <section class="zhihang-page" aria-labelledby="zhihang-title">
    <header class="page-heading">
      <div>
        <h1 id="zhihang-title">智航数据拉取</h1>
        <div class="auth-line">
          <NTag size="small" :type="authReady ? 'success' : 'default'" :bordered="false">{{ authLabel }}</NTag>
          <span v-if="authReady && authName">{{ authName }}</span>
          <span v-if="dashboard?.auth.expires_at">有效期至 {{ dateText(dashboard.auth.expires_at) }}</span>
        </div>
      </div>
      <div class="actions">
        <NButton v-if="loginActive" :loading="authAction === 'cancel'" :disabled="!!authAction" @click="login('cancel')">取消登录</NButton>
        <NButton v-else :loading="authAction === 'start'" :disabled="!!authAction || !dashboard" @click="login('start')">登录智航</NButton>
        <NButton :loading="authAction === 'test'" :disabled="!!authAction || loginActive || !dashboard" @click="testConnection">测试连接</NButton>
        <NButton :loading="refreshing" @click="refresh()"><template #icon><RefreshCw :size="16" /></template>刷新</NButton>
      </div>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NAlert v-if="loginJob.message" :type="loginJob.status === 'failed' ? 'error' : 'info'" role="status">{{ loginJob.message }}</NAlert>
    <NAlert v-if="connection.message" :type="connection.ok ? 'success' : 'error'" role="status">{{ connection.message }}</NAlert>
    <NSpin :show="refreshing && !dashboard">
      <div v-if="dashboard" class="task-grid">
        <NCard v-for="task in dashboard.tasks" :key="task.kind" :title="task.label" size="small" class="task-card">
          <template #header-extra><NTag size="small" :type="task.enabled ? 'success' : 'default'" :bordered="false">{{ task.enabled ? '定时已启用' : '定时已关闭' }}</NTag></template>
          <dl class="facts">
            <dt>定时（北京时间）</dt><dd>{{ task.times.join('、') || '未设置' }}</dd>
            <dt>下次运行</dt><dd>{{ dateText(task.next_at) }}</dd>
            <dt>最近执行</dt>
            <dd>
              <NButton v-if="task.latest?.id" text type="primary" @click="openRun(task.latest.id)">{{ statusText(task.latest.status) }} · {{ dateText(task.latest.finished_at || task.latest.created_at) }}</NButton>
              <span v-else>暂无记录</span>
            </dd>
            <dt>最近同步成功</dt><dd>{{ dateText(task.last_sync?.finished_at) }}</dd>
            <template v-if="task.last_preview?.counts?.total != null">
              <dt>预览记录数</dt><dd>{{ task.last_preview.counts.total }}</dd>
            </template>
            <template v-if="task.targets.length">
              <dt>原飞书表</dt>
              <dd class="target-list">
                <template v-for="target in task.targets" :key="target.label">
                  <a v-if="safeUrl(target.url)" :href="safeUrl(target.url)" target="_blank" rel="noopener noreferrer">{{ target.label }}</a>
                  <span v-else>{{ target.label }}</span>
                </template>
              </dd>
            </template>
            <dt>接管</dt><dd>{{ task.confirmed ? '已确认' : '待确认' }}</dd>
            <dt>卡片通知</dt><dd>{{ task.notify ? '已开启' : '已关闭' }}</dd>
            <template v-if="task.notification_targets?.length">
              <dt>原通知对象</dt><dd>{{ targetText(task.notification_targets) }}</dd>
            </template>
          </dl>
          <NAlert v-if="taskErrors[task.kind]" type="error" role="alert">{{ taskErrors[task.kind] }}</NAlert>
          <template #footer>
            <div class="actions">
              <NButton size="small" :loading="previewBusy === task.kind" :disabled="!!previewBusy || !!syncBusy || loginActive" @click="preview(task)">拉取预览</NButton>
              <NButton v-if="previewId(task)" size="small" text type="primary" @click="openRun(previewId(task))">查看预览</NButton>
              <NButton size="small" secondary @click="editSettings(task)">定时设置</NButton>
            </div>
          </template>
        </NCard>
      </div>
    </NSpin>

    <NCard v-if="dashboard" title="执行记录" size="small">
      <div v-if="dashboard.history.length" class="table-scroll">
        <table>
          <thead><tr><th scope="col">任务</th><th scope="col">执行时间</th><th scope="col">状态</th><th scope="col">卡片通知</th><th scope="col">操作</th></tr></thead>
          <tbody>
            <tr v-for="item in dashboard.history" :key="item.id">
              <td>{{ taskLabel(item.kind) }} · {{ modeText(item.mode) }}</td>
              <td>{{ dateText(item.created_at) }}</td>
              <td><NTag size="small" :type="statusType(item.status)" :bordered="false">{{ statusText(item.status) }}</NTag></td>
              <td>{{ notificationText(item) }}</td>
              <td><NButton text type="primary" :aria-label="`查看${taskLabel(item.kind)}执行详情`" @click="openRun(item.id)">查看</NButton></td>
            </tr>
          </tbody>
        </table>
      </div>
      <NEmpty v-else description="暂无执行记录" />
    </NCard>

    <NModal :show="!!settings" preset="card" title="定时设置" class="zhihang-settings-dialog" :mask-closable="!saving" :close-on-esc="!saving" :closable="!saving" @update:show="closeSettings">
      <form v-if="settings" class="settings-form" @submit.prevent="saveSettings">
        <strong>{{ taskLabel(settings.kind) }}</strong>
        <label for="zhihang-times">运行时间（北京时间）</label>
        <NInput v-model:value="settings.times" placeholder="例如 08:30、13:30" :input-props="{ id: 'zhihang-times' }" :disabled="saving" />
        <NCheckbox v-model:checked="settings.confirmed" :disabled="saving" @update:checked="clearApproval">确认旧工具已停止写入，由灯塔同步原表</NCheckbox>
        <NCheckbox v-model:checked="settings.enabled" :disabled="saving || !settings.confirmed">启用定时同步</NCheckbox>
        <NCheckbox v-model:checked="settings.notify" :disabled="saving">同步成功后发送原有卡片通知</NCheckbox>
        <p v-if="settings.targets.length" class="settings-targets">原通知对象 {{ targetText(settings.targets) }}</p>
        <NAlert v-if="settingsError" type="error" role="alert">{{ settingsError }}</NAlert>
        <div class="actions dialog-actions"><NButton :disabled="saving" @click="closeSettings(false)">取消</NButton><NButton type="primary" attr-type="submit" :loading="saving" :disabled="saving">保存设置</NButton></div>
      </form>
    </NModal>

    <NModal :show="!!selectedRunId" preset="card" :title="run ? `${taskLabel(run.kind)} · ${modeText(run.mode)}` : '执行详情'" class="zhihang-run-dialog" :mask-closable="!syncBusy" :close-on-esc="!syncBusy" :closable="!syncBusy" @update:show="closeRun">
      <NSpin :show="runLoading">
        <NAlert v-if="runError" type="error" role="alert">
          {{ runError }} <NButton text type="primary" @click="readRun(selectedRunId, run?.page || 1)">重读详情</NButton>
        </NAlert>
        <div v-if="run" class="run-details">
          <div class="run-status" role="status">
            <NTag :type="statusType(run.status)" :bordered="false">{{ statusText(run.status) }}</NTag>
            <span>{{ dateText(run.finished_at || run.created_at) }}</span>
          </div>
          <NAlert v-if="run.error" type="warning">{{ run.error }}</NAlert>
          <dl v-if="countEntries(run.counts).length" class="facts">
            <template v-for="[name, value] in countEntries(run.counts)" :key="name"><dt>{{ name }}</dt><dd>{{ value }}</dd></template>
          </dl>
          <dl v-if="run.mode !== 'preview' && notificationText(run) !== '—'" class="facts">
            <dt>卡片通知</dt><dd>{{ notificationText(run) }}</dd>
            <template v-if="run.notification?.targets?.length"><dt>通知对象</dt><dd>{{ targetText(run.notification.targets) }}</dd></template>
          </dl>
          <div v-if="columns.length" class="table-scroll preview-table" tabindex="0" aria-label="批次数据">
            <table>
              <thead><tr><th v-for="column in columns" :key="column" scope="col">{{ column }}</th></tr></thead>
              <tbody><tr v-for="(row, index) in rowFields" :key="index"><td v-for="column in columns" :key="column">{{ cellText(row[column]) }}</td></tr></tbody>
            </table>
          </div>
          <NEmpty v-else-if="!running(run.status)" description="本批暂无可展示的数据行" />
          <div v-if="run.total > 0" class="actions pagination">
            <span>共 {{ run.total }} 条 · 第 {{ run.page }} / {{ pageCount }} 页</span>
            <NButton size="small" :disabled="runLoading || run.page <= 1" @click="readRun(run.id, run.page - 1)">上一页</NButton>
            <NButton size="small" :disabled="runLoading || run.page >= pageCount" @click="readRun(run.id, run.page + 1)">下一页</NButton>
          </div>
        </div>
      </NSpin>
      <template v-if="run?.mode === 'preview' && succeeded(run.status)" #footer>
        <div class="actions dialog-actions">
          <span v-if="!runTask?.confirmed">请先在定时设置中确认接管。</span>
          <NButton type="primary" :loading="!!syncBusy" :disabled="!runTask?.confirmed || !!syncBusy || !!previewBusy || !!runError" @click="confirmSync">同步原表</NButton>
        </div>
      </template>
    </NModal>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import { NAlert, NButton, NCard, NCheckbox, NEmpty, NInput, NModal, NSpin, NTag, useDialog } from 'naive-ui';
import { RefreshCw } from 'lucide-vue-next';
import { requestJson } from './client';

type Stamp = number | string | null | undefined;
type Target = { label: string; url?: string };
type NotificationTarget = Target | string;
type Receipt = { status?: string; message?: string; error?: string; targets?: NotificationTarget[] };
type RunSummary = {
  id: string; kind: string; status: string; mode: string; created_at?: Stamp; finished_at?: Stamp;
  notification?: Receipt; notification_status?: string; notification_error?: string; counts?: Record<string, unknown>;
};
type Run = RunSummary & { error?: string; rows: Record<string, unknown>[]; total: number; page: number; page_size: number };
type Task = {
  kind: string; label: string; times: string[]; enabled: boolean; confirmed: boolean;
  notify?: boolean; targets: Target[]; latest?: RunSummary | null; next_at?: Stamp; notification_targets?: NotificationTarget[];
  last_preview?: RunSummary | null; last_sync?: RunSummary | null;
};
type Auth = { has_token?: boolean; expired?: boolean; expires_at?: Stamp; display_name?: string; login_name?: string; token_owner?: string };
type Dashboard = { auth: Auth; tasks: Task[]; history: RunSummary[] };
type LoginJob = { job_id?: string; status?: string; message?: string };
type Settings = { kind: string; times: string; confirmed: boolean; enabled: boolean; notify: boolean; targets: NotificationTarget[] };

const base = '/api/zhihang-data';
const dialog = useDialog();
const lifetime = new AbortController();
const dashboard = ref<Dashboard>();
const refreshing = ref(false);
const error = ref('');
const connection = reactive({ ok: false, message: '' });
const loginJob = ref<LoginJob>({});
const authAction = ref('');
const authReady = computed(() => !!dashboard.value?.auth.has_token && !dashboard.value.auth.expired);
const authLabel = computed(() => !dashboard.value ? '读取登录状态' : authReady.value ? '智航已登录' : dashboard.value.auth.expired ? '智航登录已过期' : '智航未登录');
const authName = computed(() => dashboard.value?.auth.display_name || dashboard.value?.auth.login_name || dashboard.value?.auth.token_owner);
const loginActive = computed(() => ['starting', 'waiting', 'verifying'].includes(loginJob.value.status || ''));
const previewBusy = ref('');
const syncBusy = ref('');
const previews = reactive<Record<string, string>>({});
const taskErrors = reactive<Record<string, string>>({});
const settings = ref<Settings>();
const settingsError = ref('');
const saving = ref(false);
const selectedRunId = ref('');
const run = ref<Run>();
const runError = ref('');
const runLoading = ref(false);
const runTask = computed(() => dashboard.value?.tasks.find(task => task.kind === run.value?.kind));
const pageCount = computed(() => Math.max(1, Math.ceil((run.value?.total || 0) / (run.value?.page_size || 1))));
const rowFields = computed(() => (run.value?.rows || []).map(row =>
  row.fields && typeof row.fields === 'object' && !Array.isArray(row.fields) ? row.fields as Record<string, unknown> : row));
const columns = computed(() => [...new Set(rowFields.value.flatMap(row => Object.keys(row)))]);
let runRead: AbortController | undefined;
let refreshTimer: ReturnType<typeof setInterval>;
let runTimer: ReturnType<typeof setTimeout> | undefined;
let loginTimer: ReturnType<typeof setTimeout> | undefined;
let loginRead: AbortController | undefined;
let overviewRead: AbortController | undefined;

function read(path: string, signal = lifetime.signal) {
  return requestJson(base + path, { fresh: true, cache: 'no-store', signal });
}
function write(path: string, method: 'POST' | 'PUT', payload: object = {}) {
  return requestJson(base + path, { method, body: JSON.stringify(payload), signal: lifetime.signal });
}
function message(cause: unknown) {
  return cause instanceof Error ? cause.message : '请求暂未完成，请稍后重试。';
}
function dateText(value: Stamp) {
  if (!value) return '—';
  const number = Number(value);
  const date = new Date(Number.isFinite(number) ? (number < 1e12 ? number * 1000 : number) : String(value));
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' });
}
function safeUrl(url?: string) {
  if (!url) return undefined;
  try { return new URL(url).protocol === 'https:' ? url : undefined; } catch { return undefined; }
}
function taskLabel(kind: string) {
  return dashboard.value?.tasks.find(task => task.kind === kind)?.label || kind;
}
function previewId(task: Task) {
  return previews[task.kind] || task.last_preview?.id || '';
}
const statuses: Record<string, string> = {
  pending: '等待执行', queued: '等待执行', running: '执行中', pulling: '拉取中', syncing: '同步中',
  verifying: '正在核验', pending_verify: '待核验', pending_verification: '待核验', verification_pending: '待核验', awaiting_verification: '待核验',
  success: '成功', succeeded: '成功', completed: '成功', ready: '预览就绪', preview_ready: '预览就绪',
  failed: '失败', error: '失败', cancelled: '已取消', interrupted: '已中断', blocked: '已暂停',
  paused: '已暂停', skipped: '已跳过', sent: '已发送', partial: '部分成功', disabled: '已关闭', not_requested: '未发送',
};
function statusText(status?: string) {
  return status ? statuses[status] || status : '—';
}
function succeeded(status: string) {
  return ['success', 'succeeded', 'completed', 'ready', 'preview_ready'].includes(status);
}
function running(status: string) {
  return ['queued', 'pending', 'running', 'pulling', 'syncing', 'verifying', 'pending_verify', 'pending_verification', 'verification_pending', 'awaiting_verification'].includes(status);
}
function statusType(status: string) {
  if (succeeded(status)) return 'success';
  if (['failed', 'error', 'interrupted'].includes(status)) return 'error';
  if (running(status) || ['blocked', 'paused'].includes(status)) return 'warning';
  return 'default';
}
function modeText(mode: string) {
  return ({ preview: '拉取预览', sync: '同步原表', scheduled: '定时同步', automatic: '定时同步' } as Record<string, string>)[mode] || mode;
}
function notificationText(item: RunSummary) {
  const receipt = item.notification;
  const status = receipt?.status || item.notification_status;
  return [status ? statusText(status) : '', receipt?.error || receipt?.message || item.notification_error].filter(Boolean).join(' · ') || '—';
}
function targetText(targets: NotificationTarget[]) {
  return targets.map(target => typeof target === 'string' ? target : target.label).join('、');
}
function cellText(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—';
  if (typeof value === 'boolean') return value ? '是' : '否';
  if (Array.isArray(value)) return value.map(cellText).join('、');
  if (typeof value === 'object') {
    const entry = value as Record<string, unknown>;
    return typeof entry.text === 'string' ? entry.text : JSON.stringify(value);
  }
  return String(value);
}
const countLabels: Record<string, string> = {
  pulled: '已拉取', fetched: '已拉取', rows: '记录数', total: '记录数', add: '新增', create: '新增', creates: '新增',
  created: '已新增', update: '更新', updates: '更新', updated: '已更新', delete: '清理', deletes: '清理',
  deleted: '已清理', unchanged: '无变更', retained: '保留', verified: '已核验', failed: '失败',
};
function countEntries(counts?: Record<string, unknown>) {
  return Object.entries(counts || {}).filter(([, value]) => typeof value === 'number')
    .map(([key, value]) => [countLabels[key] || key, value] as const);
}
async function refresh(quiet = false) {
  if (lifetime.signal.aborted || quiet && refreshing.value) return;
  overviewRead?.abort();
  const controller = new AbortController();
  overviewRead = controller;
  refreshing.value = true;
  if (!quiet) error.value = '';
  try {
    const data = await read('', controller.signal) as Dashboard;
    if (controller.signal.aborted) return;
    dashboard.value = data;
    error.value = '';
  } catch (cause) {
    if (!quiet && !controller.signal.aborted) error.value = message(cause);
  } finally {
    if (overviewRead === controller) refreshing.value = false;
  }
}

function applyLogin(job: LoginJob) {
  loginJob.value = job;
  clearTimeout(loginTimer);
  if (loginActive.value) loginTimer = setTimeout(() => void readLogin(), 1000);
  else if (job.status === 'success') void refresh();
}
async function readLogin() {
  if (lifetime.signal.aborted || authAction.value) return;
  loginRead?.abort();
  const controller = new AbortController();
  loginRead = controller;
  try {
    const job = await read('/login', controller.signal);
    if (!controller.signal.aborted) applyLogin(job);
  } catch (cause) {
    if (controller.signal.aborted) return;
    connection.ok = false;
    connection.message = message(cause);
    if (loginActive.value) loginTimer = setTimeout(() => void readLogin(), 3000);
  }
}
async function login(action: 'start' | 'cancel') {
  if (authAction.value) return;
  authAction.value = action;
  connection.message = '';
  clearTimeout(loginTimer);
  loginRead?.abort();
  try {
    if (action === 'start') applyLogin(await write('/login', 'POST'));
    else {
      await write('/login/cancel', 'POST', { job_id: loginJob.value.job_id });
      applyLogin(await read('/login'));
    }
  } catch (cause) {
    if (!lifetime.signal.aborted) {
      connection.ok = false;
      connection.message = message(cause);
      if (loginActive.value) loginTimer = setTimeout(() => void readLogin(), 1000);
    }
  } finally {
    authAction.value = '';
  }
}
async function testConnection() {
  if (authAction.value || loginActive.value) return;
  authAction.value = 'test';
  connection.message = '';
  try {
    const result = await write('/test', 'POST');
    connection.ok = result.ok !== false && result.success !== false && result.connected !== false;
    connection.message = result.message || (connection.ok ? '连接测试通过。' : '连接测试失败。');
  } catch (cause) {
    connection.ok = false;
    connection.message = message(cause);
  } finally {
    authAction.value = '';
    void refresh(true);
    void readLogin();
  }
}

function editSettings(task: Task) {
  settingsError.value = '';
  settings.value = { kind: task.kind, times: task.times.join('、'), confirmed: task.confirmed,
    enabled: task.enabled, notify: task.notify ?? false, targets: task.notification_targets || [] };
}
function clearApproval(checked: boolean) {
  if (!checked && settings.value) settings.value.enabled = false;
}
function closeSettings(show: boolean) {
  if (!show && !saving.value) settings.value = undefined;
}
async function saveSettings() {
  const draft = settings.value;
  if (!draft || saving.value) return;
  const times = [...new Set(draft.times.trim().split(/[、,，;；\s]+/).filter(Boolean))].sort();
  if (!times.length || times.length > 24 || times.some(time => !/^([01]\d|2[0-3]):[0-5]\d$/.test(time))) {
    settingsError.value = '请输入 1 至 24 个运行时间，格式为 HH:mm。';
    return;
  }
  saving.value = true;
  settingsError.value = '';
  const payload = { enabled: draft.enabled, times, confirmed: draft.confirmed, notify: draft.notify };
  try {
    await write('/' + encodeURIComponent(draft.kind) + '/settings', 'PUT', payload);
    const task = dashboard.value?.tasks.find(item => item.kind === draft.kind);
    if (task) Object.assign(task, payload);
    settings.value = undefined;
    void refresh();
  } catch (cause) {
    settingsError.value = message(cause);
  } finally {
    saving.value = false;
  }
}

function closeRun(show: boolean) {
  if (show || syncBusy.value) return;
  clearTimeout(runTimer);
  runRead?.abort();
  selectedRunId.value = '';
  runLoading.value = false;
  run.value = undefined;
}
function openRun(id: string) {
  if (selectedRunId.value !== id) run.value = undefined;
  selectedRunId.value = id;
  void readRun(id, 1);
}
async function readRun(id: string, page: number, quiet = false) {
  if (!id || selectedRunId.value !== id) return;
  clearTimeout(runTimer);
  runRead?.abort();
  const controller = new AbortController();
  runRead = controller;
  runLoading.value = !quiet;
  if (!quiet) runError.value = '';
  try {
    const result = await read('/runs/' + encodeURIComponent(id) + '?page=' + page, controller.signal) as Run;
    if (controller.signal.aborted) return;
    run.value = result;
    runError.value = '';
    if (running(result.status)) {
      runTimer = setTimeout(() => void readRun(id, result.page, true), 2500);
    } else void refresh(true);
  } catch (cause) {
    if (controller.signal.aborted) return;
    runError.value = message(cause);
    if (run.value && running(run.value.status)) runTimer = setTimeout(() => void readRun(id, page, true), 5000);
  } finally {
    if (runRead === controller) runLoading.value = false;
  }
}
async function preview(task: Task) {
  if (previewBusy.value || syncBusy.value) return;
  previewBusy.value = task.kind;
  taskErrors[task.kind] = '';
  try {
    const response = await write('/' + encodeURIComponent(task.kind) + '/preview', 'POST');
    const id = response.id || response.run_id || response.preview_id;
    if (!id) throw new Error('未收到预览编号，请在执行记录中核对。');
    previews[task.kind] = id;
    openRun(id);
    void refresh(true);
  } catch (cause) {
    taskErrors[task.kind] = message(cause);
  } finally {
    previewBusy.value = '';
  }
}
function confirmSync() {
  const item = run.value;
  const task = runTask.value;
  if (!item || !task?.confirmed || item.mode !== 'preview' || !succeeded(item.status)) return;
  dialog.warning({ title: '确认同步原表', content: `将按这次预览同步「${task.label}」到原飞书多维表。`,
    positiveText: '确认同步', negativeText: '取消', onPositiveClick: () => void sync(task, item.id) });
}
async function sync(task: Task, previewId: string) {
  if (syncBusy.value || previewBusy.value || selectedRunId.value !== previewId || !runTask.value?.confirmed) return;
  syncBusy.value = task.kind;
  runError.value = '';
  try {
    const response = await write('/' + encodeURIComponent(task.kind) + '/sync', 'POST', { preview_id: previewId });
    const id = response.id || response.run_id;
    if (!id) throw new Error('未收到同步任务编号，请刷新执行记录核对。');
    if (previews[task.kind] === previewId) delete previews[task.kind];
    openRun(id);
    void refresh(true);
  } catch (cause) {
    runError.value = message(cause);
    void refresh(true);
  } finally {
    syncBusy.value = '';
  }
}
onMounted(() => {
  void refresh();
  void readLogin();
  refreshTimer = setInterval(() => { if (!document.hidden) void refresh(true); }, 15000);
});
onBeforeUnmount(() => {
  lifetime.abort();
  overviewRead?.abort();
  runRead?.abort();
  loginRead?.abort();
  clearInterval(refreshTimer);
  clearTimeout(runTimer);
  clearTimeout(loginTimer);
});
</script>

<style scoped>
.zhihang-page { display: grid; gap: 16px; min-width: 0; }
.page-heading { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }
h1 { margin: 0 0 8px; font-size: 24px; }
.actions, .auth-line, .run-status, .target-list { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.auth-line, .run-status { font-size: 13px; }
.task-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 300px), 1fr)); gap: 12px; }
.task-card { min-width: 0; }
.facts { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 7px 16px; margin: 0; font-size: 13px; }
.facts dt { opacity: .78; }
.facts dd { margin: 0; overflow-wrap: anywhere; }
.task-card .n-alert { margin-top: 10px; }
.target-list a { color: var(--v2-primary, inherit); text-decoration: underline; text-underline-offset: 3px; }
.settings-form, .run-details { display: grid; gap: 12px; min-width: 0; }
.settings-targets { margin: 0; }
.dialog-actions, .pagination { justify-content: flex-end; }
.table-scroll { overflow: auto; min-width: 0; }
.preview-table { max-height: 46vh; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid color-mix(in srgb, currentColor 12%, transparent); vertical-align: top; }
th { white-space: nowrap; }
.preview-table td { min-width: 100px; max-width: 360px; overflow-wrap: anywhere; white-space: pre-wrap; }
@media (max-width: 760px) {
  h1 { font-size: 20px; }
  .facts { column-gap: 9px; }
}
</style>

<style>
.zhihang-settings-dialog { width: min(500px, calc(100vw - 24px)); }
.zhihang-run-dialog { width: min(1040px, calc(100vw - 24px)); }
</style>
