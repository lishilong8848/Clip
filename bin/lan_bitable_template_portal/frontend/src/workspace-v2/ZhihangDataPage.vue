<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import { NCheckbox } from 'naive-ui';
import {
  Bell, Cable, Check, ChevronRight, ClipboardCheck, Clock3, Database, Download, ExternalLink, Flag,
  GitPullRequest, History, Layers3, LogIn, RadioTower, ReceiptText, RefreshCw, ShieldCheck,
  ShieldAlert, SlidersHorizontal, Table2, Workflow,
} from 'lucide-vue-next';
import type { Component } from 'vue';
import ZhihangDialog from './ZhihangDialog.vue';
import ZhihangDigestList, { type ZhihangDigest as Digest } from './ZhihangDigestList.vue';
import { requestJson } from './client';

type Stamp = number | string | null | undefined;
type Target = { label: string; url?: string };
type NotificationTarget = (Target & { status?: string; message?: string; error?: string }) | string;
type Receipt = { status?: string; message?: string; error?: string; targets?: NotificationTarget[] };
type RunSummary = {
  id: string; kind: string; status: string; mode: string; created_at?: Stamp; finished_at?: Stamp; error?: string;
  notification?: Receipt; notification_status?: string; notification_error?: string; counts?: Record<string, unknown>;
};
type Run = RunSummary & {
  rows: Record<string, unknown>[]; total: number; page: number; page_size: number;
  stage?: string; slot?: Stamp; checks?: Record<string, Record<string, number | string>>;
};
type Task = {
  pull_scope?: string;
  kind: string; label: string; times: string[]; enabled: boolean; confirmed: boolean;
  notify?: boolean; targets: Target[]; latest?: RunSummary | null; next_at?: Stamp; notification_targets?: NotificationTarget[];
  last_preview?: RunSummary | null; last_sync?: RunSummary | null;
};
type Auth = { has_token?: boolean; expired?: boolean; expires_at?: Stamp; display_name?: string; login_name?: string; token_owner?: string };
type Dashboard = { auth: Auth; tasks: Task[]; history: RunSummary[];
  notification_digests?: Digest[];
  history_pagination?: { page: number; page_size: number; total: number } };
type LoginJob = { job_id?: string; status?: string; message?: string };
type Settings = { kind: string; times: string; confirmed: boolean; enabled: boolean; notify: boolean; targets: NotificationTarget[] };

const base = '/api/zhihang-data';
const lifetime = new AbortController();
const dashboard = ref<Dashboard>();
const chosenKind = ref('');
const selectedTask = computed(() => dashboard.value?.tasks.find(task => task.kind === chosenKind.value) || dashboard.value?.tasks[0]);
const activeDigest = computed(() => dashboard.value?.notification_digests?.find(digest => selectedTask.value && digest.modules.includes(selectedTask.value.kind)));
const taskIcons: Record<string, Component> = {
  risk: ShieldAlert, change: GitPullRequest, drill: Flag, event: RadioTower, inspection: ClipboardCheck,
};
function selectTask(kind: string) {
  if (selectedTask.value?.kind === kind) return;
  chosenKind.value = kind;
  previewSequence++;
}

const initialHistoryPageSize = 10;
const historyPage = ref(1);
const historyTotal = computed(() => dashboard.value?.history_pagination?.total ?? dashboard.value?.history.length ?? 0);
const historyPageCount = computed(() => Math.max(1, Math.ceil(historyTotal.value /
  (dashboard.value?.history_pagination?.page_size || initialHistoryPageSize))));
const historyRows = computed(() => {
  if (!dashboard.value) return [];
  if (dashboard.value.history_pagination) return dashboard.value.history;
  const start = (historyPage.value - 1) * initialHistoryPageSize;
  return dashboard.value.history.slice(start, start + initialHistoryPageSize);
});
const refreshing = ref(false);
const error = ref('');
const connection = reactive({ ok: false, message: '' });
const loginJob = ref<LoginJob>({});
const authAction = ref('');
const authReady = computed(() => !!dashboard.value?.auth.has_token && !dashboard.value.auth.expired);
const authLabel = computed(() => !dashboard.value ? '读取登录状态' : authReady.value ? '智航已登录' : dashboard.value.auth.expired ? '智航登录已过期' : '智航未登录');
const authName = computed(() => dashboard.value?.auth.display_name || dashboard.value?.auth.login_name || dashboard.value?.auth.token_owner);
const loginActive = computed(() => ['starting', 'waiting', 'verifying'].includes(loginJob.value.status || ''));
const operations = reactive<Record<string, 'preview' | 'sync' | undefined>>({});
let previewSequence = 0;
const previews = reactive<Record<string, string>>({});
const taskErrors = reactive<Record<string, string>>({});
const settings = ref<Settings>();
const settingsDigest = computed(() => dashboard.value?.notification_digests?.find(
  (digest) => !!settings.value && digest.modules.includes(settings.value.kind),
));

function digestSchedule(digest: Digest) {
  const [first, second] = digest.times;
  if (digest.kind === 'month_end') {
    return `每月 ${digest.start_day} 日起 ${first}${second ? `，月末最后一天加发 ${second}` : ''}`;
  }
  return `每日 ${digest.prepare_at} 准备，${first} 发送昨日汇总`;
}

function cardSettingLabel(kind: string) {
  if (!settingsDigest.value) return '同步成功后发送原有卡片通知';
  return kind === 'inspection' ? '参与日报并发送独立巡检异常提醒' : '参与定时汇总卡片';
}
const settingsError = ref('');
const saving = ref(false);
const selectedRunId = ref('');
const confirmationId = ref('');
const run = ref<Run>();
const requestedRunPage = ref(1);
const runError = ref('');
const runLoading = ref(false);
const runTask = computed(() => dashboard.value?.tasks.find(task => task.kind === run.value?.kind));
const syncBusy = computed(() => !!run.value && operations[run.value.kind] === 'sync');
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
const statuses: Record<string, string> = {
  pending: '等待恢复', queued: '等待恢复', retry_wait: '等待重试', running: '执行中', pulling: '拉取中', syncing: '同步中',
  verifying: '正在核验', pending_verify: '待核验', pending_verification: '待核验', verification_pending: '待核验', awaiting_verification: '待核验',
  success: '成功', succeeded: '成功', completed: '成功', ready: '预览就绪', preview_ready: '预览就绪',
  failed: '失败', error: '失败', cancelled: '已取消', interrupted: '已中断', blocked: '已暂停',
  prepared: '已准备', sending: '投递中', delivery_unknown: '投递待核对',
  paused: '已暂停', skipped: '已跳过', sent: '已发送', partial: '部分成功', disabled: '已关闭', not_requested: '未发送',
};
function statusText(status?: string) {
  return status ? statuses[status] || status : '—';
}
function succeeded(status: string) {
  return ['success', 'succeeded', 'completed', 'ready', 'preview_ready'].includes(status);
}
function running(status: string) {
  return ['queued', 'pending', 'retry_wait', 'running', 'pulling', 'syncing', 'verifying', 'pending_verify', 'pending_verification', 'verification_pending', 'awaiting_verification'].includes(status);
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
function deliveryStatus(status?: string) {
  return status === 'pending' ? '投递待确认' : statusText(status);
}
function notificationText(item: RunSummary) {
  const receipt = item.notification;
  const status = receipt?.status || item.notification_status;
  return [status ? deliveryStatus(status) : '', receipt?.error || receipt?.message || item.notification_error].filter(Boolean).join(' · ') || '—';
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
  unmanaged: '未接管', deleted: '已清理', unchanged: '无变更', retained: '保留', verified: '已核验', failed: '失败',
};
function countEntries(counts?: Record<string, unknown>) {
  return Object.entries(counts || {}).filter(([, value]) => typeof value === 'number')
    .map(([key, value]) => [countLabels[key] || key, value] as const);
}
async function refresh(quiet = false, page = historyPage.value) {
  if (lifetime.signal.aborted || quiet && refreshing.value) return;
  overviewRead?.abort();
  const controller = new AbortController();
  overviewRead = controller;
  refreshing.value = true;
  if (!quiet) error.value = '';
  try {
    const data = await read('?history_page=' + page, controller.signal) as Dashboard;
    if (controller.signal.aborted) return;
    dashboard.value = data;
    for (const task of data.tasks) {
      if (previews[task.kind] && previews[task.kind] === task.last_preview?.id) delete previews[task.kind];
    }
    historyPage.value = data.history_pagination?.page ?? Math.min(page, historyPageCount.value);
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
function setApproval(checked: boolean) {
  if (!settings.value) return;
  settings.value.confirmed = checked;
  if (!checked) settings.value.enabled = false;
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
  if (show) return;
  confirmationId.value = '';
  clearTimeout(runTimer);
  runRead?.abort();
  selectedRunId.value = '';
  runLoading.value = false;
  run.value = undefined;
}
function openRun(id: string) {
  confirmationId.value = '';
  if (selectedRunId.value !== id) run.value = undefined;
  selectedRunId.value = id;
  void readRun(id, 1);
}
async function readRun(id: string, page: number, quiet = false) {
  if (!id || selectedRunId.value !== id) return;
  requestedRunPage.value = page;
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
  if (operations[task.kind]) return;
  const sequence = ++previewSequence;
  const selection = selectedRunId.value;
  operations[task.kind] = 'preview';
  taskErrors[task.kind] = '';
  try {
    const response = await write('/' + encodeURIComponent(task.kind) + '/preview', 'POST');
    const id = response.id || response.run_id || response.preview_id;
    if (!id) throw new Error('未收到预览编号，请在执行记录中核对。');
    if (succeeded(response.status) || running(response.status)) previews[task.kind] = id;
    if (sequence === previewSequence && selectedRunId.value === selection) openRun(id);
    else if (response.status === 'failed') taskErrors[task.kind] = response.error || '拉取失败，请查看执行记录。';
    void refresh(true);
  } catch (cause) {
    taskErrors[task.kind] = message(cause);
  } finally {
    delete operations[task.kind];
  }
}
function confirmSync() {
  const item = run.value;
  const task = runTask.value;
  if (!item || !task?.confirmed || item.mode !== 'preview' || !succeeded(item.status)) return;
  confirmationId.value = item.id;
}
function submitConfirmedSync() {
  const id = confirmationId.value;
  confirmationId.value = '';
  if (id && run.value?.id === id && run.value.mode === 'preview' && succeeded(run.value.status) && runTask.value?.confirmed) {
    void sync(runTask.value, id);
  }
}
async function sync(task: Task, previewId: string) {
  if (operations[task.kind] || selectedRunId.value !== previewId || !runTask.value?.confirmed) return;
  operations[task.kind] = 'sync';
  taskErrors[task.kind] = '';
  runError.value = '';
  try {
    const response = await write('/' + encodeURIComponent(task.kind) + '/sync', 'POST', { preview_id: previewId });
    const id = response.id || response.run_id;
    if (!id) throw new Error('未收到同步任务编号，请刷新执行记录核对。');
    if (succeeded(response.status) && previews[task.kind] === previewId) delete previews[task.kind];
    if (selectedRunId.value === previewId) openRun(id);
    else if (response.status === 'failed') taskErrors[task.kind] = response.error || '同步失败，请查看执行记录。';
    void refresh(true);
  } catch (cause) {
    if (selectedRunId.value === previewId) runError.value = message(cause);
    else taskErrors[task.kind] = message(cause);
    void refresh(true);
  } finally {
    delete operations[task.kind];
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

<template>
  <section class="zh-console" aria-labelledby="zh-title">
    <header class="zh-mast">
      <div class="zh-topline">
        <div class="zh-brand">
          <span class="zh-sigil"><Workflow aria-hidden="true" :size="25" /></span>
          <h1 id="zh-title">智航数据拉取</h1>
        </div>
        <button class="zh-btn" :disabled="refreshing" @click="refresh()">
          <RefreshCw :size="15" aria-hidden="true" :class="{ rotating: refreshing }" />刷新
        </button>
      </div>
      <section class="zh-connection" aria-label="登录与连接">
        <div class="zh-session">
          <span class="zh-dot" :data-ready="authReady"></span>
          <strong>{{ authLabel }}</strong>
          <span v-if="authName">{{ authName }}</span>
          <span v-if="dashboard?.auth.expires_at">有效至 {{ dateText(dashboard.auth.expires_at) }}</span>
        </div>
        <div class="zh-actions">
          <button v-if="loginActive" class="zh-btn" :disabled="!!authAction" @click="login('cancel')">取消登录</button>
          <button v-else class="zh-btn" :disabled="!!authAction" @click="login('start')">
            <LogIn :size="14" aria-hidden="true" />{{ authReady ? '重新登录' : '登录智航' }}
          </button>
          <button class="zh-btn" :disabled="!!authAction || loginActive" @click="testConnection">
            <Cable :size="14" aria-hidden="true" />{{ authAction === 'test' ? '测试中…' : '测试连接' }}
          </button>
        </div>
        <div v-if="loginJob.message || connection.message" class="zh-feedback" aria-live="polite">
          <span v-if="loginJob.message">{{ loginJob.message }}</span>
          <span v-if="connection.message" :class="{ negative: !connection.ok }">{{ connection.message }}</span>
        </div>
      </section>
    </header>

    <p v-if="error" class="zh-alert" role="alert">{{ error }}</p>
    <p v-if="!dashboard && !error" class="zh-empty" role="status">正在读取智航任务与执行记录…</p>
    <template v-if="dashboard">
      <section class="zh-desk" aria-labelledby="zh-kinds">
        <header class="zh-section-head">
          <div class="zh-heading"><Layers3 aria-hidden="true" :size="17" /><h2 id="zh-kinds">拉取任务</h2></div>
          <span class="zh-muted">{{ dashboard.tasks.length }} 类任务 · {{ dashboard.tasks.filter(task => task.enabled).length }} 类已启用定时</span>
        </header>
        <div v-if="dashboard.tasks.length" class="zh-selector" role="group" aria-label="选择任务类型">
          <button v-for="task in dashboard.tasks" :key="task.kind" class="zh-choice" type="button"
            :aria-pressed="selectedTask?.kind === task.kind" aria-controls="zh-task-detail" @click="selectTask(task.kind)">
            <component :is="taskIcons[task.kind] || Database" class="zh-choice-icon" :size="19" aria-hidden="true" />
            <span>
              <strong>{{ task.label }}</strong>
              <small>{{ task.enabled ? '定时开' : '定时关' }} ·
                <span :class="{ 'zh-choice-error': taskErrors[task.kind] || statusType(task.latest?.status || '') === 'error' }">{{ taskErrors[task.kind] ? '操作失败' : task.latest ? statusText(task.latest.status) : '暂无执行' }}</span>
              </small>
            </span>
            <Check v-if="selectedTask?.kind === task.kind" class="zh-check" :size="15" aria-hidden="true" />
          </button>
        </div>

        <article v-if="selectedTask" id="zh-task-detail" class="zh-task-detail">
          <section class="zh-task-work">
            <div class="zh-work-head">
              <h3>{{ selectedTask.label }}</h3>
              <span class="zh-badge" :data-tone="selectedTask.confirmed ? 'success' : 'warning'">{{ selectedTask.confirmed ? '已确认接管原表' : '待确认接管原表' }}</span>
            </div>
            <dl class="zh-facts">
              <dt>拉取范围</dt><dd>{{ selectedTask.pull_scope || '接口未提供范围' }}</dd>
              <dt>写入原表</dt>
              <dd class="zh-links">
                <template v-for="target in selectedTask.targets" :key="target.url || target.label">
                  <a v-if="safeUrl(target.url)" :href="safeUrl(target.url)" target="_blank" rel="noopener noreferrer">{{ target.label }}<ExternalLink :size="12" aria-hidden="true" /></a>
                  <span v-else>{{ target.label }}</span>
                </template>
                <span v-if="!selectedTask.targets.length">未配置</span>
              </dd>
              <dt>运行时刻</dt><dd>{{ selectedTask.times.join(' / ') || '未设置' }} <small>北京时间</small></dd>
              <dt>下次定时</dt><dd>{{ selectedTask.enabled ? dateText(selectedTask.next_at) : '定时未启用' }}</dd>
              <dt>卡片通知</dt><dd>{{ selectedTask.notify ? '已启用' : '已关闭' }}<span v-if="selectedTask.notification_targets?.length"> · {{ targetText(selectedTask.notification_targets) }}</span><span v-if="activeDigest"> · {{ activeDigest.label }}</span></dd>
            </dl>
            <div class="zh-actions zh-task-actions">
              <button class="zh-btn primary" :disabled="!!operations[selectedTask.kind]" @click="preview(selectedTask)">
                <Download :size="15" aria-hidden="true" />{{ operations[selectedTask.kind] === 'preview' ? '正在拉取…' : '拉取预览' }}
              </button>
              <button class="zh-btn" @click="editSettings(selectedTask)"><SlidersHorizontal :size="15" aria-hidden="true" />定时设置</button>
            </div>
            <p v-if="taskErrors[selectedTask.kind]" class="zh-alert" role="alert">{{ taskErrors[selectedTask.kind] }}</p>
          </section>
          <section class="zh-receipt" aria-labelledby="zh-receipt-heading">
            <div class="zh-section-head">
              <div class="zh-heading"><ReceiptText :size="16" aria-hidden="true" /><h3 id="zh-receipt-heading">任务回执</h3></div>
              <span v-if="selectedTask.latest" class="zh-badge" :data-tone="statusType(selectedTask.latest.status)">{{ statusText(selectedTask.latest.status) }}</span>
            </div>
            <template v-if="selectedTask.latest">
              <button class="zh-latest" @click="openRun(selectedTask.latest.id)">
                <span><small>最近执行</small><strong>{{ modeText(selectedTask.latest.mode) }}</strong></span>
                <span>{{ dateText(selectedTask.latest.finished_at || selectedTask.latest.created_at) }} <ChevronRight :size="15" aria-hidden="true" /></span>
              </button>
              <p v-if="selectedTask.latest.error" class="zh-alert">{{ selectedTask.latest.error }}</p>
              <div v-if="countEntries(selectedTask.latest.counts).length" class="zh-mini-counts">
                <span v-for="([label, value], i) in countEntries(selectedTask.latest.counts)" :key="i">{{ label }} <b>{{ value }}</b></span>
              </div>
              <p v-if="selectedTask.latest.notification || selectedTask.latest.notification_status || selectedTask.latest.notification_error" class="zh-receipt-note">通知回执 · {{ notificationText(selectedTask.latest) }}</p>
            </template>
            <p v-else class="zh-receipt-note">此任务暂无执行记录</p>
            <div v-if="previews[selectedTask.kind] && previews[selectedTask.kind] !== selectedTask.last_preview?.id" class="zh-receipt-row">
              <small>本次拉取</small>
              <button class="zh-text-btn" @click="openRun(previews[selectedTask.kind])">查看本次结果</button>
            </div>
            <div class="zh-receipt-row">
              <div><small>最近成功预览</small><span>{{ dateText(selectedTask.last_preview?.created_at) }}</span></div>
              <button v-if="selectedTask.last_preview" class="zh-text-btn" @click="openRun(selectedTask.last_preview.id)">查看预览</button>
            </div>
            <div class="zh-receipt-row">
              <div><small>最近成功同步</small><span>{{ dateText(selectedTask.last_sync?.finished_at || selectedTask.last_sync?.created_at) }}</span></div>
              <button v-if="selectedTask.last_sync" class="zh-text-btn" @click="openRun(selectedTask.last_sync.id)">查看同步</button>
            </div>
          </section>
        </article>
        <p v-else class="zh-empty">暂无可用任务</p>
      </section>

      <ZhihangDigestList v-if="dashboard.notification_digests?.length" :digests="dashboard.notification_digests"
        :schedule="digestSchedule" :task-label="taskLabel" :date-text="dateText" :status-text="deliveryStatus" />

      <section class="zh-history" aria-labelledby="zh-history-heading">
        <div class="zh-section-head">
          <div class="zh-heading"><History :size="17" aria-hidden="true" /><h2 id="zh-history-heading">执行记录</h2></div>
          <span class="zh-muted">全部任务 · 按发起时间倒序</span>
        </div>
        <table v-if="historyRows.length" class="zh-log-table">
          <thead><tr><th scope="col">任务 / 操作</th><th scope="col">发起时间</th><th scope="col">执行结果</th><th scope="col">通知回执</th><th scope="col"><span class="zh-sr">详情</span></th></tr></thead>
          <tbody>
            <tr v-for="item in historyRows" :key="item.id">
              <td class="zh-log-task"><strong>{{ taskLabel(item.kind) }}</strong><small>{{ modeText(item.mode) }}</small></td>
              <td class="zh-date">{{ dateText(item.created_at) }}</td>
              <td class="zh-log-result"><span class="zh-badge" :data-tone="statusType(item.status)">{{ statusText(item.status) }}</span><p v-if="item.error" class="run-failure"><span>{{ item.error }}</span></p></td>
              <td class="zh-log-notice"><span class="zh-mobile-label">通知回执 · </span>{{ notificationText(item) }}</td>
              <td class="zh-log-open"><button class="zh-text-btn" :aria-label="'查看' + taskLabel(item.kind) + '执行详情'" @click="openRun(item.id)">详情<ChevronRight :size="14" aria-hidden="true" /></button></td>
            </tr>
          </tbody>
        </table>
        <p v-else class="zh-empty">暂无执行记录</p>
        <nav class="zh-pager" aria-label="执行记录分页">
          <span>共 {{ historyTotal }} 条 · 第 {{ historyPage }} / {{ historyPageCount }} 页</span>
          <div class="zh-actions">
            <button class="zh-btn" :disabled="refreshing || historyPage <= 1" @click="refresh(false, historyPage - 1)">上一页</button>
            <button class="zh-btn" :disabled="refreshing || historyPage >= historyPageCount" @click="refresh(false, historyPage + 1)">下一页</button>
          </div>
        </nav>
      </section>
    </template>

    <ZhihangDialog :show="!!settings" :title="(settings ? taskLabel(settings.kind) : '') + ' · 定时设置'"
      context="原表同步配置" :locked="saving" @dismiss="closeSettings(false)">
      <form v-if="settings" id="zh-settings-form" class="zh-form" @submit.prevent="saveSettings">
        <fieldset :disabled="saving">
          <legend><ShieldCheck :size="16" aria-hidden="true" />写入授权</legend>
          <NCheckbox :checked="settings.confirmed" :disabled="saving" @update:checked="setApproval">确认旧工具已停止写入，由灯塔同步原表</NCheckbox>
        </fieldset>
        <fieldset :disabled="saving">
          <legend><Clock3 :size="16" aria-hidden="true" />执行计划</legend>
          <NCheckbox v-model:checked="settings.enabled" :disabled="!settings.confirmed || saving">启用定时同步</NCheckbox>
          <label for="zh-times">运行时间（北京时间）</label>
          <input id="zh-times" v-model="settings.times" type="text" autocomplete="off" placeholder="07:00、11:00"
            aria-describedby="zh-times-hint" :aria-invalid="!!settingsError && !saving && settingsError.startsWith('请输入')" />
          <small id="zh-times-hint">按 24 小时制填写，多次运行用顿号分隔。</small>
        </fieldset>
        <fieldset :disabled="saving">
          <legend><Bell :size="16" aria-hidden="true" />通知投递</legend>
          <NCheckbox v-model:checked="settings.notify" :disabled="saving">{{ cardSettingLabel(settings.kind) }}</NCheckbox>
          <dl v-if="settingsDigest || settings.targets.length" class="zh-facts">
            <template v-if="settingsDigest">
              <dt>汇总卡片</dt><dd>{{ settingsDigest.label }}</dd>
              <dt>发送计划</dt><dd>{{ digestSchedule(settingsDigest) }}</dd>
              <dt>汇总去向</dt><dd>{{ settingsDigest.target.label }}</dd>
            </template>
            <template v-if="settings.targets.length"><dt>独立通知</dt><dd>{{ targetText(settings.targets) }}</dd></template>
          </dl>
        </fieldset>
        <p v-if="settingsError" class="zh-alert" role="alert"><span>{{ settingsError }}</span></p>
      </form>
      <template #actions>
        <button type="submit" form="zh-settings-form" class="zh-btn primary" :disabled="saving">{{ saving ? '正在保存…' : '保存设置' }}</button>
      </template>
    </ZhihangDialog>

    <ZhihangDialog :show="!!selectedRunId" :title="run ? taskLabel(run.kind) + ' · ' + modeText(run.mode) : '读取执行记录'"
      context="执行详情" wide @dismiss="closeRun(false)">
      <div class="zh-run-body" :aria-busy="runLoading">
        <div v-if="runError" class="zh-alert" role="alert">{{ runError }}
          <button class="zh-text-btn" @click="readRun(selectedRunId, requestedRunPage)">重新读取</button>
        </div>
        <p v-if="runLoading && !run" class="zh-empty" role="status">正在读取执行详情…</p>
        <template v-if="run">
          <div class="zh-run-meta">
            <span class="zh-badge" :data-tone="statusType(run.status)">{{ statusText(run.status) }}</span>
            <span>发起 <b>{{ dateText(run.created_at) }}</b></span>
            <span v-if="run.finished_at">结束 <b>{{ dateText(run.finished_at) }}</b></span>
            <span v-if="run.slot">定时时点 <b>{{ dateText(run.slot) }}</b></span>
            <span class="zh-id">编号 {{ run.id }}</span>
          </div>
          <p v-if="run.mode === 'preview' && succeeded(run.status) && !runTask?.confirmed" class="zh-receipt-note">在定时设置中确认接管原表后可同步。</p>
          <div v-if="countEntries(run.counts).length" class="zh-counts">
            <div v-for="([label, value], i) in countEntries(run.counts)" :key="i"><small>{{ label }}</small><strong>{{ value }}</strong></div>
          </div>
          <section v-if="run.error || run.stage || Object.keys(run.checks || {}).length" class="zh-run-block">
            <h3>{{ statusType(run.status) === 'error' ? '失败阶段' : '执行进度' }}</h3>
            <p v-if="run.stage" class="zh-stage">{{ run.stage }}</p>
            <p v-if="run.error" class="zh-alert">{{ run.error }}</p>
            <dl v-if="Object.keys(run.checks || {}).length" class="zh-checks">
              <div v-for="(facts, stage) in run.checks" :key="stage"><dt>{{ stage }}</dt><dd>{{ Object.entries(facts).map(([key, value]) => key + ' ' + cellText(value)).join(' · ') }}</dd></div>
            </dl>
          </section>
          <section v-if="run.notification || run.notification_status || run.notification_error" class="zh-run-block">
            <h3>通知回执</h3>
            <p>{{ notificationText(run) }}</p>
            <ul v-if="run.notification?.targets?.length" class="zh-deliveries">
              <li v-for="(target, i) in run.notification.targets" :key="i">
                <template v-if="typeof target === 'string'">{{ target }}</template>
                <template v-else><b>{{ target.label }}</b><span v-if="target.status">{{ deliveryStatus(target.status) }}</span><span v-if="target.message || target.error">{{ target.message || target.error }}</span></template>
              </li>
            </ul>
          </section>
          <section class="zh-data-block">
            <div class="zh-section-head">
              <div class="zh-heading"><Table2 :size="16" aria-hidden="true" /><h3>数据明细</h3></div>
              <span class="zh-muted">{{ run.total }} 条 · {{ columns.length }} 个本页字段</span>
            </div>
            <div v-if="rowFields.length" class="zh-data-scroll" role="region" aria-label="数据明细表，可横向滚动" tabindex="0">
              <table class="zh-data-table">
                <thead><tr><th scope="col">行</th><th v-for="key in columns" :key="key" scope="col">{{ key }}</th></tr></thead>
                <tbody><tr v-for="(row, i) in rowFields" :key="i"><th scope="row">{{ (run.page - 1) * run.page_size + i + 1 }}</th><td v-for="key in columns" :key="key">{{ cellText(row[key]) }}</td></tr></tbody>
              </table>
            </div>
            <p v-else class="zh-empty">此记录暂无明细</p>
            <nav class="zh-pager" aria-label="数据明细分页">
              <span>第 {{ run.page }} / {{ pageCount }} 页</span>
              <div class="zh-actions">
                <button class="zh-btn" :disabled="runLoading || run.page <= 1" @click="readRun(run.id, run.page - 1)">上一页</button>
                <button class="zh-btn" :disabled="runLoading || run.page >= pageCount" @click="readRun(run.id, run.page + 1)">下一页</button>
              </div>
            </nav>
          </section>
        </template>
      </div>
      <template #actions>
        <button v-if="run?.mode === 'preview' && succeeded(run.status)" class="zh-btn primary" :disabled="!runTask?.confirmed || syncBusy || runLoading" @click="confirmSync">{{ syncBusy ? '正在提交…' : '同步原表' }}</button>
      </template>
    </ZhihangDialog>
    <ZhihangDialog :show="!!confirmationId" title="确认同步原表" :context="runTask?.label || '智航数据拉取'" @dismiss="confirmationId = ''">
      <div v-if="run && runTask" class="zh-run-body">
        <dl class="zh-facts">
          <dt>写入原表</dt><dd>{{ runTask.targets.map(target => target.label).join('、') || '未提供目标名称' }}</dd>
          <dt>预览时间</dt><dd>{{ dateText(run.created_at) }}</dd>
          <dt>预览编号</dt><dd class="zh-id">{{ confirmationId }}</dd>
        </dl>
        <div v-if="countEntries(run.counts).length" class="zh-counts">
          <div v-for="([label, value], i) in countEntries(run.counts)" :key="i"><small>{{ label }}</small><strong>{{ value }}</strong></div>
        </div>
        <div class="zh-confirm-line"><p>将按本次预览写入「{{ runTask.label }}」原表。</p><div class="zh-actions">
          <button class="zh-btn" @click="confirmationId = ''">取消</button>
          <button class="zh-btn primary" :disabled="!runTask?.confirmed || syncBusy" @click="submitConfirmedSync">确认同步</button>
        </div></div>
      </div>
    </ZhihangDialog>
  </section>
</template>

<style scoped>
.zh-console {
  --zh-line: #d4e0e4;
  --zh-muted: #516977;
  --zh-green: #176f55;
  display: grid;
  align-content: start;
  gap: 12px;
  min-width: 0;
  width: 100%;
  container: zhtasks / inline-size;
  color: var(--v2-ink, #1b3443);
  font-family: "Inter", "Microsoft YaHei", system-ui, sans-serif;
  font-size: 14px;
  line-height: 1.5;
}
.zh-console, .zh-console *, .zh-run-body *, .zh-form * { box-sizing: border-box; }
.zh-console :is(h1,h2,h3,p,dl,dd), .zh-run-body :is(h3,p,dl,dd), .zh-form :is(dl,dd,p) { margin: 0; }
.zh-console h1 { font-size: clamp(21px, 2.2vw, 30px); letter-spacing: -.04em; font-weight: 720; line-height: 1.2; }
.zh-console h2 { font-size: 16px; line-height: 1.35; }
.zh-console h3, .zh-run-body h3 { font-size: 15px; line-height: 1.45; }
.zh-console small, .zh-run-body small, .zh-form small, .zh-sync-note { font-size: 12px; color: #516977; }
.zh-muted { font-size: 12px; color: var(--zh-muted, #516977); }
.zh-mast, .zh-desk, .zh-history {
  min-width: 0;
  border: 1px solid var(--zh-line);
  border-radius: 18px;
  background: var(--soft-face, #f2f5f6);
  box-shadow: var(--soft-raised, 3px 4px 12px #76909b20, -3px -3px 8px #ffffffbf);
}
.zh-mast {
  padding: 16px 19px 12px;
  background:
    radial-gradient(ellipse at 60% -55%, #f9ffffcf 5%, transparent 66%),
    linear-gradient(108deg,#d8eafa 0%,#e1eded 57%,#d6eee2 100%);
}
.zh-topline, .zh-brand, .zh-session, .zh-actions, .zh-heading, .zh-work-head, .zh-section-head, .zh-pager {
  display: flex; align-items: center; gap: 8px;
}
.zh-topline, .zh-section-head, .zh-pager { justify-content: space-between; flex-wrap: wrap; }
.zh-brand { gap: 12px; }
.zh-sigil {
  display: grid; place-items: center; width: 41px; height: 41px; border-radius: 13px;
  background: linear-gradient(150deg,#f9ffff,#d4e7e7);
  color: #22666f; border: 1px solid #f7ffff; box-shadow: 3px 3px 6px #74939e36;
}
.zh-connection {
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 6px 12px;
  margin-top: 12px; padding-top: 10px; border-top: 1px solid #abc6cd85;
}
.zh-session { flex-wrap: wrap; gap: 4px 11px; color: #3e5867; font-size: 12px; font-variant-numeric: tabular-nums; }
.zh-session strong { color: #233f4b; font-size: 14px; }
.zh-dot { width: 8px; height: 8px; border-radius: 50%; background: #976a34; box-shadow: 0 0 0 3px #fff9; }
.zh-dot[data-ready="true"] { background: #218365; }
.zh-actions { flex-wrap: wrap; }
.zh-btn, .zh-text-btn, .zh-choice, .zh-latest {
  font: inherit; cursor: pointer; line-height: 1.35; color: inherit;
}
.zh-btn, .zh-text-btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 33px;
  font-size: 13px; font-weight: 600;
}
.zh-btn {
  padding: 6px 12px; border: 1px solid #cbd8da; border-radius: 9px; background: linear-gradient(#fff,#edf2f3);
  box-shadow: 1px 2px 3px #6d89961a; color: #2c4857;
}
.zh-btn.primary { background: linear-gradient(125deg,#208166,#15694f); border-color: #1b7057; color: white; }
.zh-btn:hover:not(:disabled) { border-color: #8cabae; filter: brightness(.98); }
.zh-btn:disabled { opacity: .56; cursor: default; }
.zh-text-btn { padding: 4px 6px; background: transparent; border: 0; border-radius: 6px; color: #21668a; }
.zh-text-btn:hover, .zh-links a:hover { text-decoration: underline; }
:is(.zh-console,.zh-run-body,.zh-form) :is(button,a,input,[tabindex]):focus-visible, .zh-btn:focus-visible {
  outline: 2px solid #277ba2; outline-offset: 2px;
}
.zh-feedback { flex: 1 0 100%; display: flex; flex-wrap: wrap; gap: 3px 12px; color: #315d53; font-size: 13px; }
.zh-feedback .negative, .run-failure, .zh-choice-error { color: #8a3634; }
.zh-alert { padding: 8px 10px; border-left: 3px solid #bd6c57; background: #fff1ea; color: #823e32; border-radius: 4px 8px 8px 4px; font-size: 13px; overflow-wrap: anywhere; }
.zh-empty { padding: 16px 8px; color: #5c717b; text-align: center; font-size: 13px; }
.zh-desk, .zh-history { padding: 14px 16px; }
.zh-heading { gap: 7px; color: #254a59; }
.zh-desk > .zh-section-head, .zh-history > .zh-section-head { margin-bottom: 10px; }
.zh-selector {
  display: grid; gap: 6px; grid-template-columns: repeat(auto-fit, minmax(min(124px, 100%),1fr));
  background: var(--soft-field,#e9eef0); border: 1px solid #d8e1e4; padding: 5px; border-radius: 13px;
  box-shadow: var(--soft-inset, inset 1px 2px 4px #667e8c18);
}
.zh-choice {
  position: relative; display: flex; align-items: center; gap: 8px; min-width: 0; min-height: 53px;
  padding: 7px 10px; border: 1px solid transparent; border-radius: 9px; background: transparent; text-align: left;
}
.zh-choice strong, .zh-choice small, .zh-receipt-row small { display: block; }
.zh-choice small { margin-top: 2px; }
.zh-choice-icon { color: #587686; flex: none; }
.zh-choice:hover { background: #ffffff80; }
.zh-choice[aria-pressed="true"] { background: linear-gradient(115deg,#e2f1ff,#e1f2e9); border-color: #a5c7c5; box-shadow: 0 2px 4px #5b7e8a24,inset 0 1px #fffb; }
.zh-choice[aria-pressed="true"] .zh-choice-icon, .zh-check { color: #236b65; }
.zh-check { margin-left: auto; flex: none; }
.zh-task-detail { display: grid; grid-template-columns: minmax(0,1.13fr) minmax(0,.87fr); gap: 18px; margin-top: 14px; }
.zh-task-work, .zh-receipt { min-width: 0; }
.zh-work-head { flex-wrap: wrap; margin-bottom: 9px; }
.zh-work-head h3 { font-size: 19px; }
.zh-badge { display: inline-flex; align-items: center; width: fit-content; padding: 2px 7px; font-size: 12px; line-height: 1.5; font-weight: 600; background: #e7edef; color: #506372; border-radius: 6px; }
.zh-badge[data-tone="success"] { background: #dcefe5; color: #206148; }
.zh-badge[data-tone="warning"] { background: #f7eed9; color: #765220; }
.zh-badge[data-tone="error"] { background: #f9e5e1; color: #913e35; }
.zh-facts { display: grid; grid-template-columns: max-content minmax(0,1fr); column-gap: 14px; row-gap: 4px; align-items: baseline; font-size: 13px; }
.zh-facts dt, .zh-receipt-row small { color: #526874; }
.zh-facts dd, .zh-receipt, .zh-feedback, .zh-log-table td { overflow-wrap: anywhere; }
.zh-facts dd { font-variant-numeric: tabular-nums; }
.zh-links { display: flex; align-items: baseline; gap: 4px 10px; flex-wrap: wrap; }
.zh-links a { display: inline-flex; align-items: center; gap: 4px; color: #236688; text-decoration: none; }
.zh-task-actions { margin-top: 12px; }
.zh-task-work > .zh-alert { margin-top: 8px; }
.zh-receipt { align-self: start; background: var(--soft-field,#eaf0f1); padding: 10px 13px; border-radius: 13px; border: 1px solid #d5e1e1; }
.zh-receipt .zh-section-head { margin-bottom: 5px; }
.zh-latest, .zh-receipt-row { width: 100%; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.zh-latest { padding: 5px 0; text-align: left; border: 0; background: transparent; }
.zh-latest:hover { color: #186c77; }
.zh-latest span:last-child { display: inline-flex; align-items: center; gap: 2px; font-size: 12px; }
.zh-latest strong { display: block; font-size: 14px; }
.zh-mini-counts { display: flex; flex-wrap: wrap; gap: 2px 12px; padding: 5px 0; font-size: 12px; }
.zh-receipt-note { padding: 5px 0; font-size: 12px; color: #536b77; }
.zh-receipt-row { padding: 5px 0; border-top: 1px solid #cfdcde; font-size: 12px; font-variant-numeric: tabular-nums; }
.zh-receipt-row > div { display: flex; flex-wrap: wrap; gap: 3px 9px; }
.zh-log-table, .zh-data-table { width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; }
.zh-log-table thead, .zh-data-table thead { background: var(--soft-field,#e8eef1); color: #486170; }
.zh-log-table th, .zh-log-table td { padding: 7px 9px; border-bottom: 1px solid #d8e2e6; }
.zh-log-table th { font-size: 12px; font-weight: 600; }
.zh-log-table :is(th,td):first-child { padding-left: 8px; }
.zh-log-table td { vertical-align: top; }
.zh-log-table th:first-child { width: 16%; }
.zh-log-table th:nth-child(2) { width: 20%; }
.zh-log-table th:nth-child(3) { width: 34%; }
.zh-log-table tbody tr:hover { background: #f7fafb; }
.zh-log-task small { display: block; font-size: 12px; }
.zh-log-open { text-align: right; }
.zh-log-open button { white-space: nowrap; }
.zh-date { font-variant-numeric: tabular-nums; }
.zh-log-result .run-failure { margin-top: 3px; font-size: 12px; }
.zh-sr { position: absolute; width: 1px; height: 1px; padding: 0; overflow: hidden; clip-path: inset(50%); }
.zh-mobile-label { display: none; }
.zh-pager { gap: 4px 12px; padding-top: 9px; font-size: 12px; color: #546976; }
.zh-form, .zh-run-body { display: grid; min-width: 0; gap: 12px; color: var(--v2-ink,#1b3443); font-size: 14px; }
.zh-form fieldset, .zh-run-block, .zh-data-block {
  min-width: 0; margin: 0; padding: 10px 12px; border: 1px solid #d2dfe1; border-radius: 12px; background: #f7f9f9;
}
.zh-form fieldset { display: grid; gap: 7px; }
.zh-form legend { padding: 0 6px; color: #2d515e; font-weight: 650; }
.zh-form legend svg { vertical-align: -3px; margin-right: 5px; }
.zh-form label[for] { margin-top: 3px; font-size: 13px; font-weight: 600; }
.zh-form input { color: #203e4c; font: inherit; font-variant-numeric: tabular-nums; width: 100%; padding: 8px 10px; border: 1px solid #b8ced3; border-radius: 8px; background: #fff; }
.zh-form input[aria-invalid="true"] { border-color: #ae5b4a; }
.zh-run-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 14px; font-size: 12px; color: #536975; }
.zh-run-meta b { color: #304b59; font-weight: 550; font-variant-numeric: tabular-nums; }
.zh-id { margin-left: auto; overflow-wrap: anywhere; }
.zh-counts { display: grid; gap: 1px; grid-template-columns: repeat(auto-fit,minmax(80px,1fr)); padding: 1px; background: #d5e1e4; border-radius: 10px; overflow: hidden; }
.zh-counts > div { display: grid; padding: 6px 10px; background: #edf4f5; }
.zh-counts strong { color: #285f66; font-size: 21px; font-variant-numeric: tabular-nums; }
.zh-run-block { display: grid; gap: 6px; overflow-wrap: anywhere; }
.zh-stage { color: #3f6575; font-size: 13px; }
.zh-checks { display: grid; gap: 0; font-size: 12px; }
.zh-checks > div { display: flex; align-items: baseline; gap: 4px 10px; flex-wrap: wrap; padding: 4px 0; border-top: 1px solid #d8e2e3; }
.zh-checks dt { color: #496273; }
.zh-deliveries { list-style: none; padding: 0; margin: 0; display: grid; gap: 4px; font-size: 12px; }
.zh-deliveries li { display: flex; gap: 6px 10px; flex-wrap: wrap; }
.zh-data-block .zh-section-head { margin-bottom: 8px; }
.zh-data-scroll { max-height: min(40vh, 450px); overflow: auto; overscroll-behavior: contain; border: 1px solid #d5e1e4; border-radius: 7px; }
.zh-data-table { width: max-content; min-width: 100%; background: #fcfdfd; }
.zh-data-table th, .zh-data-table td { padding: 6px 9px; border-bottom: 1px solid #dbe4e6; vertical-align: top; }
.zh-data-table thead th { position: sticky; top: 0; z-index: 1; background: #e7eef1; white-space: nowrap; }
.zh-data-table td { min-width: 90px; max-width: 300px; overflow-wrap: anywhere; white-space: pre-wrap; }
.zh-data-table th[scope="row"] { color: #5a6f7a; font-weight: 500; }
.zh-confirm-line { display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:8px;font-size:13px; }
@keyframes zh-spin { to { transform: rotate(360deg); } }
.rotating { animation: zh-spin 1.4s linear infinite; }
@media (prefers-reduced-motion: reduce) { .rotating { animation: none; } }
@container zhtasks (max-width: 760px) {
  .zh-task-detail { grid-template-columns: minmax(0,1fr); gap: 12px; }
  .zh-selector { grid-template-columns: repeat(auto-fit,minmax(110px,1fr)); }
}
@container zhtasks (max-width: 560px) {
  .zh-mast, .zh-desk, .zh-history { padding: 12px; border-radius: 14px; }
  .zh-session { width: 100%; }
  .zh-topline { gap: 6px; }
  .zh-sigil { width: 33px; height: 33px; }
  .zh-brand { gap: 8px; }
  .zh-selector { grid-template-columns: repeat(2,minmax(0,1fr)); gap: 3px; }
  .zh-choice { min-height: 48px; padding: 5px 8px; }
  .zh-choice small { font-size: 11px; }
  .zh-log-table, .zh-log-table tbody { display: block; }
  .zh-log-table thead { position: absolute; clip-path: inset(50%); width: 1px; height: 1px; overflow: hidden; }
  .zh-log-table tr { display: grid; grid-template-columns: minmax(0,1fr) auto; gap: 3px 9px; padding: 8px 0; border-top: 1px solid #d6e1e3; }
  .zh-log-table :is(td,td:first-child) { padding: 0; border: 0; align-self: center; }
  .zh-log-task { grid-column: 1; grid-row: 1; }
  .zh-log-task small { display: inline; margin-left: 7px; }
  .zh-log-open { grid-column: 2; grid-row: 1 / 3; }
  .zh-log-result, .zh-log-notice { grid-column: 1 / -1; }
  .zh-date, .zh-log-notice { font-size: 12px; color: #516977; }
  .zh-mobile-label { display: inline; }
}
@media (max-width: 640px) {
  .zh-btn, .zh-text-btn { min-height: 44px; }
  .zh-id { width: 100%; margin-left: 0; }
  .zh-form input { font-size: 16px; }
}
</style>
