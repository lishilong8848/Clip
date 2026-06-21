<template>
  <section class="pane">
    <div class="actions">
      <button class="btn blue" @click="emit('refresh')">刷新状态</button>
      <button class="btn green" @click="emit('open-history-memory')">历史记忆导入</button>
      <button class="btn ghost" @click="emit('preflight')">真实联调预检</button>
      <button class="btn ghost" @click="emit('cleanup')">清理已完成任务</button>
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
          <span>任务状态刷新</span>
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
        <article v-for="item in cleanupCards" :key="item.key">
          <span>{{ item.label }}</span>
          <strong>{{ item.count }}</strong>
        </article>
      </div>
      <p class="muted-line">
        SQLite 维护：{{ sqliteMaintenanceText }}
      </p>
    </section>
    <section class="diagnostic-section">
      <div class="section-title">
        <strong>源表分类</strong>
        <span>当前生效快照</span>
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
        <strong>耗时较长任务</strong>
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
              @click="emit('mark-stuck-failed', job)"
            >
              标记卡住
            </button>
            <button class="btn blue" :disabled="busy || !job.can_retry" @click="emit('retry-job', job)">
              重试
            </button>
            <button class="btn danger" :disabled="busy || !job.can_clear" @click="emit('clear-job', job)">
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
      <summary>查看详细诊断数据</summary>
      <pre>{{ pretty({ stats, perf, queues, recentJobs }) }}</pre>
    </details>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";

type Dict = Record<string, any>;

const props = defineProps<{
  stats: Dict;
  perf: Dict;
  queues: Dict;
  recentJobs: Dict;
  busy?: boolean;
}>();

const emit = defineEmits<{
  refresh: [];
  "open-history-memory": [];
  preflight: [];
  cleanup: [];
  "mark-stuck-failed": [job: Dict];
  "retry-job": [job: Dict];
  "clear-job": [job: Dict];
}>();

const attachmentUsagePercent = computed(() => {
  const used = Number(props.stats.upload_attachments?.total_bytes || 0);
  const limit = Number(props.stats.upload_attachments?.max_pending_bytes || 0);
  if (!limit) return 0;
  return Math.max(0, Math.min(100, Math.round((used / limit) * 100)));
});

const attachmentTone = computed(() => {
  if (attachmentUsagePercent.value >= 90) return "bad";
  if (attachmentUsagePercent.value >= 70) return "warn";
  return "good";
});

const qtBridgeTone = computed(() => (props.stats.qt_bridge?.connected ? "good" : "warn"));

const sourceSnapshotTone = computed(() => {
  const active = props.stats.source_snapshot?.active || {};
  if (!active.status) return "warn";
  if (String(active.status) !== "active") return "warn";
  if (props.stats.source_snapshot?.last_failed?.error) return "warn";
  return "good";
});

const statusWarnings = computed(() => {
  const result: string[] = [];
  for (const item of props.stats.capacity_warnings || []) {
    if (item) result.push(String(item));
  }
  if (!props.stats.qt_bridge?.connected) result.push("Qt 展示壳未连接，桌面界面不会实时同步。");
  if (attachmentUsagePercent.value >= 90) result.push("附件暂存空间接近上限，建议等待后台清理或减少现场照片并发。");
  const snapshotError = props.stats.source_snapshot?.last_failed?.error;
  if (snapshotError) result.push(`源表最近刷新失败：${snapshotError}`);
  return Array.from(new Set(result)).slice(0, 8);
});

const sourceTypeCards = computed(() => {
  const totals = props.stats.source_type_summary?.totals || {};
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

const slowJobs = computed(() => (
  Array.isArray(props.stats.slow_jobs) ? props.stats.slow_jobs.slice(0, 10) : []
));

const recentJobItems = computed(() => {
  const items = props.recentJobs?.items;
  return Array.isArray(items) ? items.slice(0, 20) : [];
});

const sqliteMaintenanceText = computed(() => {
  const item = props.stats.sqlite_maintenance || {};
  if (item.skipped_at) return `运行繁忙，已跳过 ${formatTime(item.skipped_at)}`;
  if (item.checked_at && item.checkpointed) return `已整理 ${formatTime(item.checked_at)}`;
  if (item.checked_at) return `无需整理 ${formatTime(item.checked_at)}`;
  return "暂无";
});

const cleanupCards = computed(() => {
  const item = props.stats.job_cleanup || {};
  return [
    ["deleted", "终态任务"],
    ["runtime_queue_removed", "运行队列"],
    ["outbox_removed", "Qt 事件"],
    ["attachment_removed", "附件暂存"],
    ["clipboard_removed", "剪贴板候选"],
    ["dialog_removed", "弹窗会话"],
    ["undo_removed", "回退记录"],
    ["append_events_removed", "事件日志"],
  ].map(([key, label]) => ({ key, label, count: Number(item[key] || 0) }));
});

function formatTime(value: unknown): string {
  const number = Number(value || 0);
  if (!number) return "暂无";
  const ms = number > 1_000_000_000_000 ? number : number * 1000;
  return new Date(ms).toLocaleString("zh-CN", { hour12: false });
}

function formatMaybeTime(value: unknown): string {
  return value ? formatTime(value) : "暂无时间";
}

function formatBytes(value: unknown): string {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
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

function shortId(value: unknown): string {
  const text = String(value || "");
  return text.length > 10 ? `${text.slice(0, 6)}...${text.slice(-4)}` : text || "-";
}

function pretty(value: unknown): string {
  return JSON.stringify(value, null, 2);
}
</script>

<style scoped>
.pane {
  display: grid;
  gap: 16px;
}

.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
}

.btn {
  border: 1px solid rgba(148, 163, 184, 0.32);
  border-radius: 14px;
  padding: 9px 14px;
  background: rgba(255, 255, 255, 0.82);
  color: #1e293b;
  font-weight: 800;
  font-size: 13px;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
  transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
}

.btn:hover:not(:disabled) {
  transform: translateY(-1px);
  border-color: rgba(37, 99, 235, 0.32);
  box-shadow: 0 12px 24px rgba(37, 99, 235, 0.12);
}

.btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
  transform: none;
}

.btn.blue {
  border-color: transparent;
  color: #fff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.btn.green {
  border-color: transparent;
  color: #fff;
  background: linear-gradient(135deg, #10b981, #059669);
}

.btn.danger {
  border-color: transparent;
  color: #fff;
  background: linear-gradient(135deg, #ef4444, #dc2626);
}

.btn.ghost {
  color: #1d4ed8;
  background: rgba(255, 255, 255, 0.72);
}

.metric-grid,
.status-grid,
.source-type-grid {
  display: grid;
  gap: 12px;
}

.metric-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.status-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.source-type-grid {
  grid-template-columns: repeat(auto-fit, minmax(128px, 1fr));
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.raw-diagnostic,
.source-type-grid article,
.slow-job-list article,
.job-row {
  border: 1px solid rgba(191, 219, 254, 0.74);
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.94), rgba(248, 251, 255, 0.82)),
    #fff;
  box-shadow: 0 12px 30px rgba(30, 99, 255, 0.08);
}

.metric-grid article,
.source-type-grid article {
  display: grid;
  gap: 5px;
  padding: 14px;
}

.metric-grid span,
.status-card span,
.source-type-grid span {
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
}

.metric-grid strong,
.status-card strong,
.source-type-grid strong {
  color: #0f2f6f;
  font-size: 22px;
  line-height: 1;
}

.status-card {
  display: grid;
  gap: 10px;
  padding: 15px;
}

.status-card.good {
  border-color: rgba(16, 185, 129, 0.34);
}

.status-card.warn {
  border-color: rgba(245, 158, 11, 0.36);
  background: linear-gradient(135deg, rgba(255, 251, 235, 0.95), rgba(255, 255, 255, 0.86));
}

.status-card.bad {
  border-color: rgba(239, 68, 68, 0.34);
  background: linear-gradient(135deg, rgba(254, 242, 242, 0.95), rgba(255, 255, 255, 0.86));
}

.status-card div {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
}

.status-card p,
.muted-line {
  margin: 0;
  color: #64748b;
  font-size: 12px;
  line-height: 1.6;
}

.bar {
  height: 7px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(226, 232, 240, 0.9);
}

.bar i {
  display: block;
  height: 100%;
  max-width: 100%;
  border-radius: inherit;
  background: linear-gradient(90deg, #22c55e, #16a34a);
}

.status-card.warn .bar i {
  background: linear-gradient(90deg, #f59e0b, #d97706);
}

.status-card.bad .bar i {
  background: linear-gradient(90deg, #ef4444, #dc2626);
}

.danger-text {
  color: #b91c1c !important;
  font-weight: 700;
}

.diagnostic-section,
.warning-list,
.raw-diagnostic {
  padding: 16px;
}

.section-title {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 12px;
}

.section-title strong,
.warning-list strong {
  color: #0f172a;
  font-size: 15px;
}

.section-title span {
  color: #64748b;
  font-size: 12px;
}

.slow-job-list,
.job-list {
  display: grid;
  gap: 10px;
}

.slow-job-list article,
.job-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  padding: 12px;
  box-shadow: none;
}

.slow-job-list article div,
.job-main {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.slow-job-list strong,
.job-main strong {
  color: #0f172a;
  font-size: 13px;
}

.slow-job-list span,
.job-main span {
  color: #64748b;
  font-size: 12px;
}

.slow-job-list b {
  color: #1d4ed8;
  white-space: nowrap;
}

.job-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.warning-list {
  display: grid;
  gap: 8px;
  border-color: rgba(245, 158, 11, 0.36);
  background: linear-gradient(135deg, rgba(255, 251, 235, 0.96), rgba(255, 255, 255, 0.88));
}

.warning-list p {
  margin: 0;
  color: #92400e;
  font-size: 13px;
}

.raw-diagnostic summary {
  color: #1d4ed8;
  font-weight: 800;
  cursor: pointer;
}

.raw-diagnostic pre {
  max-height: 320px;
  overflow: auto;
  margin: 12px 0 0;
  padding: 12px;
  border-radius: 14px;
  background: #0f172a;
  color: #dbeafe;
  font-size: 12px;
}

@media (max-width: 900px) {
  .metric-grid,
  .status-grid {
    grid-template-columns: 1fr;
  }

  .job-row,
  .slow-job-list article {
    align-items: stretch;
    flex-direction: column;
  }

  .job-actions {
    justify-content: flex-start;
  }
}
</style>
