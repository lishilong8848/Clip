<template>
  <section class="h-drill" aria-labelledby="h-drill-title">
    <header class="h-header">
      <VnetBackButton @click="emit('back')" />
      <h1 id="h-drill-title">H 楼值班演练</h1>
      <NButton :loading="busy === 'refresh'" @click="refresh">刷新</NButton>
      <a v-if="status?.table_url" :href="status.table_url" target="_blank" rel="noopener noreferrer">打开飞书原表</a>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NAlert v-if="status?.account_error" type="warning">{{ status.account_error }}</NAlert>
    <div class="h-settings">
      <form class="h-actions" @submit.prevent="saveLogin">
        <label for="h-duty-login">H 楼值班登录名</label>
        <NInput v-model:value="login" placeholder="仅在会话没有显示账号名称时填写" :maxlength="80"
          :input-props="{ id: 'h-duty-login', autocomplete: 'off' }" :disabled="status?.enabled || running" />
        <NButton attr-type="submit" :loading="busy === 'settings'" :disabled="!status || status.enabled || running || login === status.login_name">保存</NButton>
      </form>
      <div class="h-actions">
        <NButton :disabled="!!busy || loggingIn" @click="startLogin">登录 H 楼智航账号</NButton>
        <span>每 30 分钟同步 · {{ status?.enabled ? '已开启' : '已关闭' }}</span>
        <NButton :disabled="!status || !!busy || (!status.enabled && (!status.account_ready || running))"
          @click="toggleSchedule">{{ status?.enabled ? '关闭定时同步' : '开启定时同步' }}</NButton>
        <NButton type="primary" :disabled="!status?.account_ready || !!busy || running" @click="startPreview">拉取当月预览</NButton>
        <NButton v-if="confirmedPreview" :disabled="!!busy || running || !status?.account_ready" @click="showSync = true">同步原表</NButton>
      </div>
      <span v-if="status?.login_job?.message" role="status">{{ status.login_job.message }}</span>
    </div>
    <div v-if="status?.latest" class="h-receipt" role="status">
      <strong>{{ runLabel(status.latest) }}</strong>
      <span>{{ stateLabel(status.latest.status) }} · {{ formatTime(status.latest.finished_at || status.latest.created_at) }}</span>
      <span v-if="status.latest.counts">拉取 {{ status.latest.counts.total }} 条 · 拟新增／新增 {{ status.latest.counts.create }} · 拟更新／更新 {{ status.latest.counts.update }}</span>
      <span v-if="status.latest.error">{{ status.latest.error }}</span>
    </div>
    <div class="h-actions">
      <NButton :type="showPreview ? 'default' : 'primary'" @click="selectView(false)">飞书记录</NButton>
      <NButton :type="showPreview ? 'primary' : 'default'" :disabled="!preview" @click="selectView(true)">拉取预览</NButton>
      <span v-if="showPreview && preview?.stage">{{ preview.stage }}</span>
    </div>
    <div v-if="displayRows.length" class="h-table" tabindex="0" :aria-label="showPreview ? '智航演练预览' : '飞书演练记录'">
      <table>
        <thead><tr><th v-for="[field, label] in columns" :key="field" scope="col">{{ label }}</th></tr></thead>
        <tbody><tr v-for="(row, index) in pageRows" :key="index">
          <td v-for="[field] in columns" :key="field">{{ row[field] || '—' }}</td>
        </tr></tbody>
      </table>
    </div>
    <p v-else class="h-receipt">{{ showPreview ? '本次预览暂无记录' : '原表暂无演练记录' }}</p>
    <div v-if="rowTotal > perPage" class="h-actions">
      <span>共 {{ rowTotal }} 条 · 第 {{ page }} / {{ pageCount }} 页</span>
      <NButton size="small" :disabled="page <= 1 || paging" @click="changePage(page - 1)">上一页</NButton>
      <NButton size="small" :disabled="page >= pageCount || paging" @click="changePage(page + 1)">下一页</NButton>
    </div>
    <NModal v-model:show="showSync" preset="dialog" title="确认同步 H 楼值班演练" positive-text="同步原表"
      negative-text="取消" :loading="busy === 'sync'" @positive-click="syncPreview">
      按演练编号新增或更新本次预览记录。保留历史和其他记录。
    </NModal>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { NAlert, NButton, NInput, NModal } from 'naive-ui';
import { requestJson } from '../api/client';
import VnetBackButton from './VnetBackButton.vue';

const emit = defineEmits<{ back: [] }>();
const base = '/api/drills/zhihang-h';
type Row = Record<string, string>;
type Run = { id: string; mode: string; status: string; created_at: string; finished_at?: string;
  error?: string; counts?: { total: number; create: number; update: number } };
type Detail = Run & { rows: Row[]; total: number; stage?: string };
type Summary = { enabled: boolean; account_ready: boolean; account_error: string;
  login_name: string; table_url: string; latest: Run | null; history: Run[];
  login_job?: { status: string; message: string } };
const columns = Object.entries({ 文本: '演练名称', 发生时间: '发生时间', 楼栋: '楼栋', 演练状态: '状态',
  演练编号: '演练编号', 班组: '班组', 专业: '专业', 响应时间: '响应时间', 确认时间: '确认时间',
  恢复时间: '恢复时间', 创建人: '创建人', 录制状态: '录制状态', 等级: '等级' });
const status = ref<Summary>();
const records = ref<Row[]>([]);
const preview = ref<Detail>();
const login = ref('');
const error = ref('');
const busy = ref('');
const paging = ref(false);
const page = ref(1);
const showPreview = ref(false);
const showSync = ref(false);
let timer: ReturnType<typeof setTimeout> | undefined;
let closed = false;
let loaded = false;
const controller = new AbortController();
const running = computed(() => ['queued', 'running', 'pending_verify', 'retry_wait'].includes(status.value?.latest?.status || ''));
const loggingIn = computed(() => ['starting', 'waiting', 'verifying'].includes(status.value?.login_job?.status || ''));
const confirmedPreview = computed(() => preview.value?.status === 'success' && !!preview.value.total
  && Date.now() - Date.parse(preview.value.created_at) < 60 * 60 * 1000 ? preview.value : undefined);
const displayRows = computed(() => showPreview.value ? preview.value?.rows || [] : records.value);
const rowTotal = computed(() => showPreview.value ? preview.value?.total || 0 : records.value.length);
const perPage = computed(() => showPreview.value ? 50 : 20);
const pageCount = computed(() => Math.max(1, Math.ceil(rowTotal.value / perPage.value)));
const pageRows = computed(() => showPreview.value ? displayRows.value :
  displayRows.value.slice((page.value - 1) * perPage.value, page.value * perPage.value));
function formatTime(value: string) {
  const time = new Date(value);
  return Number.isNaN(time.valueOf()) ? value : time.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
}
function stateLabel(value: string) {
  return ({ queued: '等待恢复', running: '执行中', success: '成功', failed: '失败', pending_verify: '等待核验', retry_wait: '等待重试' } as Record<string,string>)[value] || value;
}
function runLabel(run: Run) {
  return run.mode === 'preview' ? '拉取预览' : run.mode === 'scheduled' ? '定时同步' : '手动同步';
}
async function request<T>(path = '', options: RequestInit = {}) {
  return await requestJson(base + path, { ...options, signal: controller.signal, cache: 'reload' }) as T;
}
function showError(cause: unknown) {
  if (!closed) error.value = cause instanceof Error ? cause.message : '请求未完成。';
}
async function loadStatus() {
  const previous = status.value;
  const next = await request<Summary>();
  if (!loaded || login.value === previous?.login_name) login.value = next.login_name;
  status.value = next;
  loaded = true;
  const latestPreview = next.history.find(run => run.mode === 'preview' && (run.status === 'failed' || run.status === 'success'));
  if (latestPreview && (latestPreview.id !== preview.value?.id || latestPreview.status !== preview.value?.status)) {
    preview.value = await request<Detail>(`/runs/${latestPreview.id}?page=1`);
    if (showPreview.value) page.value = 1;
  }
  if (previous && next.latest?.mode !== 'preview' && next.latest?.status === 'success'
      && (next.latest.id !== previous.latest?.id || previous.latest?.status !== 'success')) await loadRecords();
}
async function loadRecords() {
  records.value = (await request<{ rows: Row[] }>('/records')).rows;
  if (!showPreview.value) page.value = Math.min(page.value, pageCount.value);
}
async function refresh() {
  if (busy.value) return;
  busy.value = 'refresh';
  error.value = '';
  try { await Promise.all([loadStatus(), loadRecords()]); } catch (cause) { showError(cause); }
  finally { busy.value = ''; schedulePoll(); }
}
function schedulePoll() {
  clearTimeout(timer);
  if (!closed) timer = setTimeout(async () => {
    try { if (!busy.value) await loadStatus(); } catch (cause) { showError(cause); }
    finally { schedulePoll(); }
  }, running.value || loggingIn.value ? 2000 : 15000);
}
async function submit(action: string, path: string, payload?: Record<string, unknown>) {
  if (busy.value) return false;
  busy.value = action;
  error.value = '';
  try {
    await request(path, { method: action === 'settings' ? 'PUT' : 'POST', body: payload ? JSON.stringify(payload) : undefined });
    await loadStatus();
    return true;
  } catch (cause) { showError(cause); return false; }
  finally { busy.value = ''; schedulePoll(); }
}
function saveLogin() { return submit('settings', '/settings', { login_name: login.value.trim() }); }
function startLogin() { return submit('login', '/login'); }
function toggleSchedule() { return submit('settings', '/settings', { enabled: !status.value?.enabled }); }
async function startPreview() {
  if (await submit('preview', '/preview')) selectView(true);
}
function syncPreview() { return confirmedPreview.value ? submit('sync', '/sync', { preview_id: confirmedPreview.value.id }) : false; }
function selectView(value: boolean) {
  if (showPreview.value === value) return;
  showPreview.value = value;
  page.value = 1;
  if (value && preview.value) void changePage(1);
}
async function changePage(next: number) {
  if (!showPreview.value || !preview.value) { page.value = next; return; }
  paging.value = true;
  try {
    preview.value = await request<Detail>(`/runs/${preview.value.id}?page=${next}`);
    page.value = next;
  } catch (cause) { showError(cause); }
  finally { paging.value = false; }
}
onMounted(refresh);
onBeforeUnmount(() => { closed = true; clearTimeout(timer); controller.abort(); });
</script>

<style scoped>
.h-drill { min-width: 0; display: grid; gap: 14px; padding: 16px; }
.h-header, .h-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 8px 12px; }
.h-header h1 { margin: 0 auto 0 0; font-size: 22px; }
.h-settings, .h-receipt { display: grid; gap: 10px; padding: 12px; border: 1px solid var(--vnet-border, #77809355); border-radius: 8px; }
.h-settings .n-input { width: min(100%, 270px); }
.h-receipt { grid-template-columns: repeat(auto-fit, minmax(160px, auto)); }
.h-table { overflow: auto; max-width: 100%; border: 1px solid var(--vnet-border, #77809355); border-radius: 8px; }
.h-table table { border-collapse: collapse; min-width: max-content; width: 100%; }
.h-table th, .h-table td { text-align: left; padding: 10px 12px; border-block-end: 1px solid var(--vnet-border, #77809355); }
.h-table td:first-child { max-width: 320px; white-space: normal; }
.h-table td { white-space: nowrap; }
@media (max-width: 600px) { .h-drill { padding: 10px; } .h-header h1 { font-size: 18px; } }
</style>
