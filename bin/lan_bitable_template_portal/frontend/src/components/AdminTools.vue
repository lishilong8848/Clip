<template>
  <section v-if="open" class="admin-shell">
    <div class="admin-card">
      <header>
        <div>
          <strong>管理员工具</strong>
          <p>查看后台状态、权限、交接班链接和 mock 压测，不触发真实飞书或多维写入。</p>
        </div>
        <button class="btn ghost" @click="$emit('close')">关闭</button>
      </header>

      <nav class="tabs">
        <button :class="{ active: tab === 'status' }" @click="tab = 'status'; loadStatus()">状态</button>
        <button :class="{ active: tab === 'permissions' }" @click="tab = 'permissions'; loadPermissions()">权限</button>
        <button :class="{ active: tab === 'handover' }" @click="tab = 'handover'; loadHandover()">交接班链接</button>
        <button :class="{ active: tab === 'pressure' }" @click="tab = 'pressure'">mock 压测</button>
      </nav>

      <section v-if="message" class="message">{{ message }}</section>

      <section v-if="tab === 'status'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadStatus">刷新状态</button>
          <button class="btn green" @click="openHistoryMemory">历史记忆导入</button>
          <button class="btn ghost" @click="runPreflight">真实联调预检</button>
          <button class="btn ghost" @click="cleanupJobs">清理终态任务</button>
        </div>
        <div class="metric-grid">
          <article>
            <span>消息队列</span>
            <strong>{{ stats.message_queue_size ?? "-" }}</strong>
          </article>
          <article>
            <span>Qt 队列</span>
            <strong>{{ stats.qt_queue_size ?? stats.qt_outbox_pending ?? "-" }}</strong>
          </article>
          <article>
            <span>上传等待</span>
            <strong>{{ stats.upload_wait_size ?? "-" }}</strong>
          </article>
          <article>
            <span>SSE 连接</span>
            <strong>{{ stats.sse_connections?.connections ?? "-" }}</strong>
          </article>
        </div>
        <div class="status-grid">
          <article :class="['status-card', qtBridgeTone]">
            <div>
              <span>Qt 连接</span>
              <strong>{{ stats.qt_bridge?.connected ? "已连接" : "未连接" }}</strong>
            </div>
            <p>心跳延迟 {{ formatSeconds(stats.qt_bridge?.age_seconds) }}</p>
          </article>
          <article :class="['status-card', sourceSnapshotTone]">
            <div>
              <span>源表快照</span>
              <strong>{{ stats.source_snapshot?.active?.status || "未知" }}</strong>
            </div>
            <p>最近成功 {{ formatTime(stats.source_snapshot?.active?.finished_at) }}</p>
            <p v-if="stats.source_snapshot?.last_failed?.error" class="danger-text">
              最近失败：{{ stats.source_snapshot.last_failed.error }}
            </p>
          </article>
          <article :class="['status-card', attachmentTone]">
            <div>
              <span>附件暂存</span>
              <strong>{{ formatBytes(stats.upload_attachments?.total_bytes) }}</strong>
            </div>
            <p>
              {{ stats.upload_attachments?.pending ?? 0 }} 个待使用 /
              上限 {{ formatBytes(stats.upload_attachments?.max_pending_bytes) }}
            </p>
            <div class="bar"><i :style="{ width: attachmentUsagePercent + '%' }"></i></div>
          </article>
          <article class="status-card">
            <div>
              <span>批量任务轮询</span>
              <strong>{{ stats.job_batch?.requests ?? 0 }}</strong>
            </div>
            <p>
              平均 {{ stats.job_batch?.avg_request_size ?? 0 }} 个/次，
              拒绝 {{ stats.job_batch?.denied_jobs ?? 0 }}，
              丢失 {{ stats.job_batch?.missing_jobs ?? 0 }}
            </p>
          </article>
        </div>
        <section class="diagnostic-section">
          <div class="section-title">
            <strong>后台维护</strong>
            <span>最近清理 {{ formatTime(stats.job_cleanup?.cleaned_at) }}</span>
          </div>
          <div class="source-type-grid">
            <article>
              <span>终态任务</span>
              <strong>{{ stats.job_cleanup?.deleted ?? 0 }}</strong>
            </article>
            <article>
              <span>运行队列</span>
              <strong>{{ stats.job_cleanup?.runtime_queue_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>Qt 事件</span>
              <strong>{{ stats.job_cleanup?.outbox_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>附件暂存</span>
              <strong>{{ stats.job_cleanup?.attachment_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>剪贴板候选</span>
              <strong>{{ stats.job_cleanup?.clipboard_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>弹窗会话</span>
              <strong>{{ stats.job_cleanup?.dialog_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>回退记录</span>
              <strong>{{ stats.job_cleanup?.undo_removed ?? 0 }}</strong>
            </article>
            <article>
              <span>事件日志</span>
              <strong>{{ stats.job_cleanup?.append_events_removed ?? 0 }}</strong>
            </article>
          </div>
          <p class="muted-line">
            SQLite 维护：{{ sqliteMaintenanceText }}
          </p>
        </section>
        <section class="diagnostic-section">
          <div class="section-title">
            <strong>源表分类</strong>
            <span>当前 active snapshot</span>
          </div>
          <div class="source-type-grid">
            <article v-for="item in sourceTypeCards" :key="item.key">
              <span>{{ item.label }}</span>
              <strong>{{ item.count }}</strong>
            </article>
          </div>
        </section>
        <section v-if="slowJobs.length" class="diagnostic-section">
          <div class="section-title">
            <strong>慢任务 TOP</strong>
            <span>按受理后耗时排序</span>
          </div>
          <div class="slow-job-list">
            <article v-for="job in slowJobs" :key="job.job_id">
              <div>
                <strong>{{ job.title || job.job_id }}</strong>
                <span>{{ workTypeLabel(job.work_type) }} · {{ actionLabel(job.action) }} · {{ job.phase }}</span>
              </div>
              <b>{{ formatSeconds(job.elapsed_seconds) }}</b>
            </article>
          </div>
        </section>
        <section v-if="recentJobItems.length" class="diagnostic-section">
          <div class="section-title">
            <strong>最近任务</strong>
            <span>卡住任务可先标记失败，再按需重试</span>
          </div>
          <div class="job-list">
            <article v-for="job in recentJobItems" :key="job.job_id" class="job-row">
              <div class="job-main">
                <strong>{{ workTypeLabel(job.work_type) }} · {{ actionLabel(job.action) }}</strong>
                <span>{{ shortId(job.job_id) }} · {{ job.phase || "未知" }} · {{ formatMaybeTime(job.updated_at) }}</span>
                <p v-if="job.error" class="danger-text">{{ job.error }}</p>
              </div>
              <div class="job-actions">
                <button
                  class="btn ghost"
                  :disabled="busy || !job.can_mark_stuck_failed"
                  @click="markStuckFailed(job)"
                >
                  标记卡住
                </button>
                <button class="btn blue" :disabled="busy || !job.can_retry" @click="retryJob(job)">
                  重试
                </button>
                <button class="btn danger" :disabled="busy || !job.can_clear" @click="clearJob(job)">
                  清理
                </button>
              </div>
            </article>
          </div>
        </section>
        <section v-if="statusWarnings.length" class="warning-list">
          <strong>需要关注</strong>
          <p v-for="item in statusWarnings" :key="item">{{ item }}</p>
        </section>
        <details class="raw-diagnostic">
          <summary>查看原始诊断数据</summary>
          <pre>{{ pretty({ stats, perf, queues, recentJobs }) }}</pre>
        </details>
      </section>

      <section v-else-if="tab === 'permissions'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadPermissions">刷新权限</button>
          <button class="btn green" @click="savePermissions">保存权限</button>
          <button class="btn ghost" @click="addPermissionUser">添加用户</button>
        </div>
        <div class="permission-list">
          <article v-for="user in permissions.users" :key="user.open_id" class="permission-row">
            <input v-model="user.name" placeholder="姓名" :disabled="user.locked" />
            <input v-model="user.open_id" placeholder="openid" :disabled="user.locked" />
            <select v-model="user.role" :disabled="user.locked">
              <option value="building">用户</option>
              <option value="admin">管理员</option>
            </select>
            <label><input v-model="user.enabled" type="checkbox" :disabled="user.locked" /> 启用</label>
            <button class="btn danger" :disabled="user.locked" @click="removePermissionUser(user)">删除</button>
            <div class="scope-checks">
              <label v-for="scope in scopeOptions" :key="scope.value">
                <input
                  type="checkbox"
                  :checked="user.scopes?.includes(scope.value)"
                  :disabled="user.locked || user.role === 'admin'"
                  @change="toggleUserScope(user, scope.value, ($event.target as HTMLInputElement).checked)"
                />
                {{ scope.label }}
              </label>
            </div>
          </article>
        </div>
      </section>

      <section v-else-if="tab === 'handover'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadHandover">刷新链接</button>
          <button class="btn green" @click="saveHandover">保存链接</button>
        </div>
        <label class="password-line">
          设置密码
          <input v-model="handoverPassword" type="password" placeholder="输入交接班链接设置密码" />
        </label>
        <div class="handover-grid">
          <label v-for="scope in buildingScopes" :key="scope.value">
            {{ scope.label }}
            <input v-model="handoverLinks[scope.value]" placeholder="https://..." />
          </label>
        </div>
      </section>

      <section v-else class="pane">
        <div class="pressure-form">
          <label>数量 <input v-model.number="pressure.count" type="number" min="1" max="60" /></label>
          <label>并发 <input v-model.number="pressure.concurrency" type="number" min="1" max="10" /></label>
          <label class="inline-check">
            <input v-model="pressure.include_site_photos" type="checkbox" />
            带现场照片
          </label>
          <label>每条照片数 <input v-model.number="pressure.site_photo_count" type="number" min="1" max="3" /></label>
          <label>照片大小 KB <input v-model.number="pressure.site_photo_kb" type="number" min="1" max="512" /></label>
          <label>平均提交阈值 ms <input v-model.number="pressure.max_submit_average_ms" type="number" min="1" max="10000" /></label>
          <label>总耗时阈值 s <input v-model.number="pressure.max_total_seconds" type="number" min="1" max="600" /></label>
          <label>允许失败数 <input v-model.number="pressure.max_failed" type="number" min="0" max="60" /></label>
          <label>
            场景
            <select v-model="pressure.scenario">
              <option value="accepted">accepted</option>
              <option value="mixed">mixed</option>
              <option value="failed-network">failed-network</option>
              <option value="failed-remote-missing">failed-remote-missing</option>
            </select>
          </label>
          <button class="btn blue" :disabled="busy" @click="runMockPressure">运行 mock 压测</button>
        </div>
        <section
          v-if="pressureResult.assessment"
          :class="['pressure-assessment', pressureResult.assessment.ok ? 'good' : 'bad']"
        >
          <div>
            <span>压测判定</span>
            <strong>{{ pressureResult.assessment.summary || (pressureResult.assessment.ok ? "达标" : "未达标") }}</strong>
          </div>
          <p>
            受理 {{ pressureResult.assessment.observed?.accepted ?? "-" }} /
            {{ pressureResult.assessment.observed?.count ?? "-" }}，
            失败 {{ pressureResult.assessment.observed?.failed ?? 0 }}，
            平均 {{ pressureResult.assessment.observed?.submit_average_ms ?? "-" }} ms，
            总耗时 {{ pressureResult.assessment.observed?.elapsed_seconds ?? "-" }} s
          </p>
          <ul v-if="pressureResult.assessment.failures?.length">
            <li v-for="item in pressureResult.assessment.failures" :key="item">{{ item }}</li>
          </ul>
        </section>
        <div v-if="pressureResult.site_photos" class="pressure-summary">
          <article>
            <span>照片上传</span>
            <strong>{{ pressureResult.site_photos.enabled ? "已启用" : "未启用" }}</strong>
          </article>
          <article>
            <span>预期附件数</span>
            <strong>{{ pressureResult.site_photos.expected_uploads ?? 0 }}</strong>
          </article>
          <article>
            <span>预期附件大小</span>
            <strong>{{ formatBytes(pressureResult.site_photos.expected_bytes) }}</strong>
          </article>
          <article>
            <span>提交平均耗时</span>
            <strong>{{ pressureResult.submit_average_ms ?? "-" }} ms</strong>
          </article>
        </div>
        <details class="raw-diagnostic" :open="Boolean(pressureResult.assessment)">
          <summary>查看原始压测结果</summary>
          <pre>{{ pretty(pressureResult) }}</pre>
        </details>
      </section>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { requestJson } from "../api/client";

type Dict = Record<string, any>;

const props = defineProps<{
  open: boolean;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

defineEmits<{
  close: [];
}>();

const buildingScopes = [
  { value: "110", label: "110站" },
  { value: "A", label: "A楼" },
  { value: "B", label: "B楼" },
  { value: "C", label: "C楼" },
  { value: "D", label: "D楼" },
  { value: "E", label: "E楼" },
  { value: "H", label: "H楼" },
];

const tab = ref<"status" | "permissions" | "handover" | "pressure">("status");
const busy = ref(false);
const message = ref("");
const stats = ref<Dict>({});
const perf = ref<Dict>({});
const queues = ref<Dict>({});
const recentJobs = ref<Dict>({});
const permissions = reactive<{ users: Dict[]; scope_options: Dict[] }>({ users: [], scope_options: [] });
const handoverLinks = reactive<Record<string, string>>({});
const handoverPassword = ref("");
const pressure = reactive({
  count: 60,
  concurrency: 10,
  scenario: "accepted",
  include_site_photos: false,
  site_photo_count: 1,
  site_photo_kb: 32,
  max_submit_average_ms: 300,
  max_total_seconds: 20,
  max_failed: 0,
});
const pressureResult = ref<Dict>({});

const attachmentUsagePercent = computed(() => {
  const used = Number(stats.value.upload_attachments?.total_bytes || 0);
  const limit = Number(stats.value.upload_attachments?.max_pending_bytes || 0);
  if (!limit) return 0;
  return Math.max(0, Math.min(100, Math.round((used / limit) * 100)));
});

const attachmentTone = computed(() => {
  if (attachmentUsagePercent.value >= 90) return "bad";
  if (attachmentUsagePercent.value >= 70) return "warn";
  return "good";
});

const qtBridgeTone = computed(() => {
  return stats.value.qt_bridge?.connected ? "good" : "warn";
});

const sourceSnapshotTone = computed(() => {
  const active = stats.value.source_snapshot?.active || {};
  if (!active.status) return "warn";
  if (String(active.status) !== "active") return "warn";
  if (stats.value.source_snapshot?.last_failed?.error) return "warn";
  return "good";
});

const statusWarnings = computed(() => {
  const result: string[] = [];
  for (const item of stats.value.capacity_warnings || []) {
    if (item) result.push(String(item));
  }
  if (!stats.value.qt_bridge?.connected) result.push("Qt 展示壳未连接，桌面界面不会实时同步。");
  if (attachmentUsagePercent.value >= 90) result.push("附件暂存空间接近上限，建议等待后台清理或减少现场照片并发。");
  const snapshotError = stats.value.source_snapshot?.last_failed?.error;
  if (snapshotError) result.push(`源表最近刷新失败：${snapshotError}`);
  return Array.from(new Set(result)).slice(0, 8);
});

const sourceTypeCards = computed(() => {
  const totals = stats.value.source_type_summary?.totals || {};
  const labels: Record<string, string> = {
    maintenance: "维保",
    change: "变更",
    repair: "检修",
    power: "上电",
    polling: "轮巡",
    adjust: "调整",
    unknown: "未知",
  };
  return Object.keys(labels).map((key) => ({
    key,
    label: labels[key],
    count: Number(totals[key] || 0),
  })).filter((item) => item.count > 0 || ["maintenance", "change", "repair"].includes(item.key));
});

const slowJobs = computed(() => {
  return Array.isArray(stats.value.slow_jobs) ? stats.value.slow_jobs.slice(0, 10) : [];
});

const recentJobItems = computed(() => {
  const items = recentJobs.value?.items;
  return Array.isArray(items) ? items.slice(0, 20) : [];
});

watch(() => props.open, (open) => {
  if (open) void loadStatus();
});

const api = requestJson;

function pretty(value: unknown): string {
  return JSON.stringify(value || {}, null, 2);
}

function formatBytes(value: unknown): string {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatTime(value: unknown): string {
  const ts = Number(value || 0);
  if (!ts) return "暂无";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "暂无";
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatMaybeTime(value: unknown): string {
  const text = String(value || "").trim();
  return text || "暂无时间";
}

function shortId(value: unknown): string {
  const text = String(value || "").trim();
  if (!text) return "-";
  return text.length > 12 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text;
}

function formatSeconds(value: unknown): string {
  const seconds = Number(value || 0);
  if (!Number.isFinite(seconds) || seconds <= 0) return "暂无";
  return `${Math.round(seconds)} 秒`;
}

function workTypeLabel(value: unknown): string {
  const labels: Record<string, string> = {
    maintenance: "维保",
    change: "变更",
    repair: "检修",
    power: "上电",
    polling: "轮巡",
    adjust: "调整",
  };
  return labels[String(value || "")] || String(value || "通告");
}

function actionLabel(value: unknown): string {
  const labels: Record<string, string> = {
    start: "开始",
    update: "更新",
    end: "结束",
    delete: "删除",
  };
  return labels[String(value || "")] || String(value || "任务");
}

const sqliteMaintenanceText = computed(() => {
  const item = stats.value.sqlite_maintenance || {};
  if (item.skipped_at) return `运行繁忙，已跳过 ${formatTime(item.skipped_at)}`;
  if (item.checked_at && item.checkpointed) return `已整理 ${formatTime(item.checked_at)}`;
  if (item.checked_at) return `无需整理 ${formatTime(item.checked_at)}`;
  return "暂无";
});

function cleanupRemovedTotal(value: Dict): number {
  const keys = [
    "deleted",
    "runtime_queue_removed",
    "outbox_removed",
    "append_events_removed",
    "undo_removed",
    "attachment_removed",
    "clipboard_removed",
    "dialog_removed",
  ];
  return keys.reduce((total, key) => total + Number(value?.[key] || 0), 0);
}

function openHistoryMemory(): void {
  window.location.href = "/admin/history-memory";
}

async function loadStatus(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const [statsData, perfData, queuesData, jobsData] = await Promise.all([
      api("/api/backend/stats"),
      api("/api/backend/perf"),
      api("/api/backend/queues"),
      api("/api/jobs/recent?limit=20"),
    ]);
    stats.value = statsData;
    perf.value = perfData;
    queues.value = queuesData;
    recentJobs.value = jobsData;
  } catch (error: any) {
    message.value = error?.message || "状态加载失败";
  } finally {
    busy.value = false;
  }
}

async function runPreflight(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/backend/preflight", { method: "POST", body: "{}" });
    message.value = "预检完成";
    stats.value = { ...stats.value, preflight: data };
  } catch (error: any) {
    message.value = error?.message || "预检失败";
  } finally {
    busy.value = false;
  }
}

async function cleanupJobs(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/backend/jobs/cleanup", { method: "POST", body: "{}" });
    message.value = `清理完成：${cleanupRemovedTotal(data)}`;
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "清理失败";
  } finally {
    busy.value = false;
  }
}

async function retryJob(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_retry) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/retry`, { method: "POST", body: "{}" });
    message.value = "任务已重新入队";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "任务重试失败";
  } finally {
    busy.value = false;
  }
}

async function clearJob(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_clear) return;
  if (!window.confirm(`确认清理任务 ${shortId(jobId)}？`)) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/clear`, { method: "POST", body: "{}" });
    message.value = "任务已清理";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "任务清理失败";
  } finally {
    busy.value = false;
  }
}

async function markStuckFailed(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_mark_stuck_failed) return;
  const ok = window.confirm(`确认将任务 ${shortId(jobId)} 标记为卡住失败？之后可按需点击重试。`);
  if (!ok) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/mark-stuck-failed`, {
      method: "POST",
      body: JSON.stringify({ reason: "管理员手动标记卡住任务，请核对后重试。" }),
    });
    message.value = "任务已标记为失败，可按需重试";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "标记任务失败";
  } finally {
    busy.value = false;
  }
}

async function loadPermissions(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const data = await api("/api/auth/permissions");
    permissions.users.splice(0, permissions.users.length, ...(data.users || []));
    permissions.scope_options.splice(0, permissions.scope_options.length, ...(data.scope_options || []));
  } catch (error: any) {
    message.value = error?.message || "权限加载失败";
  } finally {
    busy.value = false;
  }
}

function addPermissionUser(): void {
  permissions.users.push({ open_id: "", name: "", role: "building", scopes: [], enabled: true });
}

function removePermissionUser(user: Dict): void {
  if (user.locked) return;
  const label = String(user.name || user.open_id || "该用户");
  if (!window.confirm(`确认删除「${label}」的门户权限？保存后生效。`)) return;
  const index = permissions.users.indexOf(user);
  if (index >= 0) {
    permissions.users.splice(index, 1);
    message.value = "已从列表移除，点击保存权限后生效。";
  }
}

function toggleUserScope(user: Dict, scope: string, checked: boolean): void {
  const next = new Set((user.scopes || []) as string[]);
  if (checked) next.add(scope);
  else next.delete(scope);
  user.scopes = Array.from(next);
}

async function savePermissions(): Promise<void> {
  busy.value = true;
  try {
    await api("/api/auth/permissions", { method: "POST", body: JSON.stringify({ users: permissions.users }) });
    message.value = "权限已保存";
    await loadPermissions();
  } catch (error: any) {
    message.value = error?.message || "权限保存失败";
  } finally {
    busy.value = false;
  }
}

async function loadHandover(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const data = await api("/api/handover-links");
    for (const item of buildingScopes) handoverLinks[item.value] = String(data.links?.[item.value] || "");
  } catch (error: any) {
    message.value = error?.message || "交接班链接加载失败";
  } finally {
    busy.value = false;
  }
}

async function saveHandover(): Promise<void> {
  busy.value = true;
  try {
    await api("/api/handover-links", {
      method: "POST",
      body: JSON.stringify({ links: handoverLinks, password: handoverPassword.value }),
    });
    message.value = "交接班链接已保存";
  } catch (error: any) {
    message.value = error?.message || "交接班链接保存失败";
  } finally {
    busy.value = false;
  }
}

async function runMockPressure(): Promise<void> {
  busy.value = true;
  pressureResult.value = {};
  try {
    pressureResult.value = await api("/api/backend/mock-pressure", {
      method: "POST",
      body: JSON.stringify(pressure),
    });
    message.value = "mock 压测完成";
  } catch (error: any) {
    message.value = error?.message || "mock 压测失败";
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.admin-shell {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(15, 23, 42, 0.38);
}

.admin-card {
  width: min(1180px, 100%);
  max-height: calc(100vh - 48px);
  overflow: auto;
  display: grid;
  gap: 12px;
  padding: 18px;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.22);
}

header,
.actions,
.tabs,
.pressure-form {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.inline-check {
  display: inline-flex;
  grid-template-columns: none;
  align-items: center;
  gap: 6px;
  min-height: 35px;
}

.inline-check input {
  width: auto;
}

header {
  justify-content: space-between;
}

p {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
}

.btn,
button {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
}

.btn.blue,
.tabs button.active {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

.btn.green {
  border-color: #16a34a;
  background: #16a34a;
  color: #ffffff;
}

.btn.danger {
  border-color: #dc2626;
  background: #dc2626;
  color: #ffffff;
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.tabs {
  padding: 4px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
  background: #f8fafc;
}

.tabs button {
  border: 0;
  background: transparent;
}

.pane {
  display: grid;
  gap: 12px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 10px;
}

.metric-grid article {
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #f8fafc;
}

.metric-grid span {
  color: #64748b;
  font-size: 12px;
}

.metric-grid strong {
  display: block;
  margin-top: 4px;
  font-size: 20px;
}

.status-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  gap: 10px;
}

.status-card {
  display: grid;
  gap: 8px;
  min-height: 108px;
  padding: 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.status-card.good {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.status-card.warn {
  border-color: #fde68a;
  background: #fffbeb;
}

.status-card.bad {
  border-color: #fecaca;
  background: #fef2f2;
}

.status-card div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.status-card span {
  color: #64748b;
  font-size: 12px;
}

.status-card strong {
  color: #0f172a;
  font-size: 18px;
}

.status-card p {
  margin: 0;
}

.danger-text {
  color: #b91c1c;
}

.bar {
  overflow: hidden;
  height: 6px;
  border-radius: 999px;
  background: #e2e8f0;
}

.bar i {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: #2563eb;
}

.status-card.warn .bar i {
  background: #d97706;
}

.status-card.bad .bar i {
  background: #dc2626;
}

.warning-list {
  display: grid;
  gap: 4px;
  padding: 10px 12px;
  border: 1px solid #fde68a;
  border-radius: 8px;
  background: #fffbeb;
  color: #92400e;
}

.warning-list strong,
.warning-list p {
  margin: 0;
}

.diagnostic-section {
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.section-title {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.section-title span {
  color: #64748b;
  font-size: 12px;
}

.source-type-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(90px, 1fr));
  gap: 8px;
}

.source-type-grid article {
  padding: 8px;
  border-radius: 6px;
  background: #f8fafc;
}

.source-type-grid span {
  color: #64748b;
  font-size: 12px;
}

.source-type-grid strong {
  display: block;
  margin-top: 3px;
  font-size: 18px;
}

.slow-job-list {
  display: grid;
  gap: 6px;
}

.slow-job-list article {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 8px;
  border-radius: 6px;
  background: #f8fafc;
}

.slow-job-list strong,
.slow-job-list span {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.slow-job-list span {
  margin-top: 2px;
  color: #64748b;
  font-size: 12px;
}

.slow-job-list b {
  color: #0f172a;
  font-size: 14px;
}

.job-list {
  display: grid;
  gap: 8px;
}

.job-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.job-main {
  min-width: 0;
}

.job-main strong,
.job-main span {
  display: block;
}

.job-main span {
  margin-top: 4px;
  color: #64748b;
  font-size: 12px;
}

.job-main p {
  overflow-wrap: anywhere;
}

.job-actions {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.pressure-summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 8px;
}

.pressure-assessment {
  display: grid;
  gap: 8px;
  padding: 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #f8fafc;
}

.pressure-assessment.good {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.pressure-assessment.bad {
  border-color: #fecaca;
  background: #fef2f2;
}

.pressure-assessment div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.pressure-assessment span {
  color: #64748b;
  font-size: 12px;
}

.pressure-assessment strong {
  color: #0f172a;
  font-size: 20px;
}

.pressure-assessment p {
  margin: 0;
}

.pressure-assessment ul {
  margin: 0;
  padding-left: 18px;
  color: #991b1b;
}

.pressure-summary article {
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #f8fafc;
}

.pressure-summary span {
  color: #64748b;
  font-size: 12px;
}

.pressure-summary strong {
  display: block;
  margin-top: 4px;
  font-size: 18px;
}

pre {
  max-height: 320px;
  overflow: auto;
  padding: 12px;
  border-radius: 8px;
  background: #0f172a;
  color: #e2e8f0;
  font-size: 12px;
}

.raw-diagnostic {
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.raw-diagnostic summary {
  padding: 10px 12px;
  color: #334155;
  cursor: pointer;
  font-size: 13px;
  font-weight: 700;
}

.raw-diagnostic pre {
  margin: 0 10px 10px;
}

.permission-list,
.handover-grid {
  display: grid;
  gap: 10px;
}

.permission-row {
  display: grid;
  grid-template-columns: minmax(120px, 0.7fr) minmax(180px, 1.2fr) 110px 90px auto;
  gap: 8px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
}

.scope-checks {
  grid-column: 1 / -1;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
select {
  width: 100%;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

.handover-grid {
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}

.password-line {
  max-width: 360px;
}

.message {
  padding: 10px 12px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #eff6ff;
  color: #1d4ed8;
}

@media (max-width: 820px) {
  .permission-row {
    grid-template-columns: 1fr;
  }
}

/* VNET admin skin */
.admin-shell {
  background:
    linear-gradient(rgba(5, 20, 55, 0.42), rgba(5, 20, 55, 0.42)),
    radial-gradient(circle at 48% 0%, rgba(21, 116, 239, 0.32), transparent 40%);
  backdrop-filter: blur(6px);
}

.admin-card {
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background:
    linear-gradient(180deg, #ffffff 0, #f8fbff 100%);
  box-shadow: 0 30px 90px rgba(4, 43, 116, 0.28);
}

header {
  padding: 4px 2px 12px;
  border-bottom: 1px solid #e7f0fb;
}

header strong {
  color: #071634;
  font-size: 22px;
  font-weight: 900;
}

header strong::before {
  content: "";
  display: inline-block;
  width: 4px;
  height: 20px;
  margin-right: 8px;
  border-radius: 999px;
  vertical-align: -4px;
  background: linear-gradient(180deg, #0757d7, #21c6e7);
}

.tabs {
  width: fit-content;
  padding: 5px;
  border-color: #cde0f6;
  border-radius: 12px;
  background: #edf5ff;
}

.tabs button {
  min-height: 36px;
  border-radius: 9px;
  color: #23486f;
  font-weight: 800;
}

.btn,
button {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
}

.btn:hover:not(:disabled),
button:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.12);
  transform: translateY(-1px);
}

.btn.blue,
.tabs button.active {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.24);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #dc2626, #f05656);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.pressure-assessment,
.pressure-summary article,
.permission-row,
.warning-list,
.message {
  border-color: #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background-color 0.14s ease;
}

.metric-grid article:hover,
.status-card:hover,
.diagnostic-section:hover,
.pressure-assessment:hover,
.pressure-summary article:hover {
  border-color: #9cc7ff;
  background: #ffffff;
  box-shadow: 0 14px 32px rgba(22, 78, 151, 0.12);
}

.metric-grid strong,
.source-type-grid strong,
.pressure-summary strong,
.status-card strong {
  color: #0757d7;
}

.source-type-grid article,
.slow-job-list article,
.job-row {
  border: 1px solid #e1ecfa;
  border-radius: 12px;
  background: #f7fbff;
}

input,
select {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
}

input:focus,
select:focus {
  border-color: #1678ff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

pre {
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 14px;
  background: #071634;
}

.raw-diagnostic {
  border-color: #d8e7f8;
  border-radius: 14px;
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.06);
}

.raw-diagnostic summary {
  color: #0757d7;
}

.permission-list {
  grid-template-columns: 1fr;
}

.permission-row {
  grid-template-columns: minmax(160px, 0.8fr) minmax(220px, 1.2fr) 116px 92px auto;
  align-items: center;
  border-color: #d8e7f8;
  border-radius: 14px;
  background:
    linear-gradient(180deg, #ffffff, #f9fcff),
    radial-gradient(circle at 5% 0%, rgba(22, 120, 255, 0.08), transparent 28%);
  box-shadow: 0 10px 24px rgba(22, 78, 151, 0.08);
}

.permission-row:hover {
  border-color: #9cc7ff;
  box-shadow: 0 14px 30px rgba(22, 78, 151, 0.12);
}

/* Softer rounded and text polish */
.admin-card {
  border-radius: 24px;
}

.metric-grid article,
.status-card,
.diagnostic-section,
.pressure-assessment,
.pressure-summary article,
.permission-row,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row,
.raw-diagnostic,
pre {
  border-radius: 18px;
}

.tabs,
.btn,
button,
input,
select {
  border-radius: 12px;
}

header strong,
.metric-grid strong,
.source-type-grid strong,
.pressure-summary strong,
.status-card strong {
  font-weight: 820;
  letter-spacing: 0;
}

p,
.metric-grid span,
.status-card span {
  color: #5f7189;
}

.tabs button,
.btn,
button {
  font-weight: 720;
}

.permission-row > label {
  min-height: 36px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 1px solid #d8e7f8;
  border-radius: 999px;
  padding: 7px 11px;
  background: #f7fbff;
  color: #37536f;
  font-weight: 800;
}

.permission-row > label input[type="checkbox"],
.scope-checks input[type="checkbox"] {
  width: 15px;
  min-width: 15px;
  height: 15px;
  accent-color: #1678ff;
}

.scope-checks {
  padding-top: 2px;
}

.scope-checks label {
  min-height: 34px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 1px solid #c8dcf3;
  border-radius: 999px;
  padding: 7px 11px;
  background: #fbfdff;
  color: #37536f;
  font-size: 12px;
  font-weight: 750;
}

.scope-checks label:has(input:checked) {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 10px 20px rgba(20, 103, 226, 0.18);
}

.scope-checks label:has(input:disabled) {
  opacity: 0.62;
}

/* Panorama construction-management polish */
.admin-shell {
  background:
    linear-gradient(rgba(7, 22, 52, 0.38), rgba(7, 22, 52, 0.38)),
    radial-gradient(circle at 50% 0%, rgba(48, 128, 255, 0.22), transparent 42%);
  backdrop-filter: blur(6px);
}

.admin-card {
  border: 1px solid rgba(207, 224, 255, 0.96);
  border-radius: 28px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(250, 253, 255, 0.98));
  box-shadow: 0 30px 90px rgba(4, 43, 116, 0.22);
}

.tabs {
  border-color: #d8e7f8;
  border-radius: 18px;
  background: rgba(239, 246, 255, 0.86);
}

.tabs button.active,
.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 12px 24px rgba(21, 93, 252, 0.22);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.pressure-assessment,
.pressure-summary article,
.permission-row,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row,
.raw-diagnostic,
pre {
  border-color: rgba(216, 231, 248, 0.95);
  box-shadow: 0 10px 24px rgba(20, 70, 138, 0.07);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.pressure-assessment,
.pressure-summary article,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row,
.raw-diagnostic,
pre {
  background: rgba(255, 255, 255, 0.96);
}

.permission-row {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(249, 252, 255, 0.97)),
    radial-gradient(circle at 4% 0%, rgba(48, 128, 255, 0.08), transparent 32%);
}

.btn,
button,
input,
select {
  border-radius: 14px;
}

/* Panorama construction-management admin skin */
.admin-card {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 24px 64px rgba(0, 47, 135, 0.18);
}

.tabs {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.72);
}

.tabs button.active,
.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}

.btn.green {
  background: #059669;
}

.btn.danger {
  background: #e11d48;
}

.metric-grid article,
.status-card,
.diagnostic-section,
.pressure-assessment,
.pressure-summary article,
.permission-row,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row,
.raw-diagnostic,
pre {
  border-color: #d8e5f7;
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.06);
}

.permission-row {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.94), rgba(248, 251, 255, 0.88));
}

input,
select {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

input:focus,
select:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.scope-checks label:has(input:checked) {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

@media (max-width: 1120px) {
  .permission-row {
    grid-template-columns: 1fr 1fr;
  }

  .permission-row .scope-checks {
    grid-column: 1 / -1;
  }
}
</style>
