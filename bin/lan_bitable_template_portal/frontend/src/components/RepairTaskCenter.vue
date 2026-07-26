<template>
  <Teleport to="body">
    <div
      v-if="open"
      class="repair-task-overlay"
      role="presentation"
      @click.self="emit('close')"
    >
      <section
        class="repair-task-center"
        role="dialog"
        aria-modal="true"
        aria-labelledby="repair-task-title"
      >
        <header>
          <div>
            <span>后台状态</span>
            <h3 id="repair-task-title">维修任务中心</h3>
          </div>
          <button type="button" aria-label="关闭任务中心" @click="emit('close')">
            <X :size="19" aria-hidden="true" />
          </button>
        </header>
        <div class="repair-task-summary">
          <span><small>失败</small><b>{{ failedCount }}</b></span>
          <span><small>处理中</small><b>{{ pendingCount }}</b></span>
          <span><small>关联待核对</small><b>{{ integrityCount }}</b></span>
        </div>
        <MessageBanner v-if="message" :tone="messageTone" :text="message" />
        <div class="repair-task-list" :aria-busy="loading">
          <div v-if="loading" class="empty-state">正在读取任务...</div>
          <div v-else-if="!items.length" class="empty-state">当前没有失败或处理中任务</div>
          <article
            v-for="(item, index) in items"
            v-else
            :key="[
              item.operation_id || item.record_id || item.summary_record_id || 'task',
              item.operation_type || item.type || '',
              item.updated_at || '',
              index,
            ].join(':')"
          >
            <div>
              <b>{{ taskLabel(item) }}</b>
              <span :class="String(item.status || '')">{{ taskStatusLabel(item.status) }}</span>
            </div>
            <p v-if="item.last_error || item.error">{{ item.last_error || item.error }}</p>
            <small>{{ repairDisplayTime(item.updated_at) || "时间未记录" }}</small>
          </article>
        </div>
        <footer>
          <button
            v-if="failedCount"
            type="button"
            :disabled="retrying"
            @click="emit('retry')"
          >
            <RefreshCw :size="15" :class="{ spinning: retrying }" aria-hidden="true" />
            重试失败任务
          </button>
          <button
            v-if="canReconcile"
            type="button"
            :disabled="integrationChecking || reconciling"
            @click="emit('integration-check')"
          >
            校验飞书读取
          </button>
          <button
            v-if="canReconcile"
            type="button"
            class="primary"
            :disabled="integrationChecking || reconciling"
            @click="emit('reconcile')"
          >
            {{ reconciling ? "对账中" : "对账并同步本地" }}
          </button>
        </footer>
      </section>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { RefreshCw, X } from "lucide-vue-next";
import { repairDisplayTime } from "../repairManagementUtils";
import type { LooseDict } from "../types";
import MessageBanner from "./MessageBanner.vue";

withDefaults(defineProps<{
  open: boolean;
  loading?: boolean;
  items?: LooseDict[];
  failedCount?: number;
  pendingCount?: number;
  integrityCount?: number;
  canReconcile?: boolean;
  message?: string;
  messageTone?: "success" | "warning" | "failed";
  retrying?: boolean;
  integrationChecking?: boolean;
  reconciling?: boolean;
}>(), {
  loading: false,
  items: () => [],
  failedCount: 0,
  pendingCount: 0,
  integrityCount: 0,
  canReconcile: false,
  message: "",
  messageTone: "success",
  retrying: false,
  integrationChecking: false,
  reconciling: false,
});

const emit = defineEmits<{
  close: [];
  retry: [];
  "integration-check": [];
  reconcile: [];
}>();

function taskLabel(item: LooseDict): string {
  const labels: Record<string, string> = {
    followup_summary_sync: "跟进汇总同步",
    summary_followup_copy_sync: "维修单字段同步",
    notice_summary_sync: "检修通告同步",
    event_transfer_sync: "事件转检修同步",
    project_delete: "删除同步",
  };
  return String(item.label || labels[String(item.operation_type || item.type || "")] || "后台同步任务");
}

function taskStatusLabel(status: unknown): string {
  const labels: Record<string, string> = {
    failed: "失败",
    started: "等待处理",
    remote_written: "远端已写入",
    sync_pending: "等待同步",
    processing: "处理中",
  };
  return labels[String(status || "")] || "处理中";
}
</script>

<style scoped>
.repair-task-overlay {
  position: fixed;
  z-index: 260;
  inset: 0;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(8, 25, 52, 0.52);
  backdrop-filter: blur(3px);
}

.repair-task-center {
  width: min(760px, calc(100vw - 48px));
  max-height: min(720px, calc(100vh - 48px));
  overflow: hidden;
  display: grid;
  grid-template-rows: auto auto auto minmax(140px, 1fr) auto;
  gap: 12px;
  border: 1px solid #cbdcf1;
  border-radius: 14px;
  padding: 16px;
  background: #fff;
  box-shadow: 0 24px 70px rgba(8, 37, 82, 0.28);
}

.repair-task-center > header,
.repair-task-center > footer,
.repair-task-center > header button,
.repair-task-center > footer button {
  display: flex;
  align-items: center;
}

.repair-task-center > header {
  justify-content: space-between;
  gap: 12px;
  border-bottom: 1px solid #e2ebf6;
  padding-bottom: 10px;
}

.repair-task-center > header span {
  color: #67809b;
  font-size: 11px;
  font-weight: 700;
}

.repair-task-center > header h3 {
  margin: 2px 0 0;
  color: #10294a;
  font-size: 19px;
}

.repair-task-center > header button {
  width: 34px;
  height: 34px;
  justify-content: center;
  border: 1px solid #d4e1f1;
  border-radius: 9px;
  background: #f8fbff;
  color: #486683;
  cursor: pointer;
}

.repair-task-summary {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}

.repair-task-summary span {
  display: grid;
  gap: 3px;
  border: 1px solid #dce7f4;
  border-radius: 9px;
  padding: 9px 11px;
  background: #f7faff;
}

.repair-task-summary small {
  color: #6d829a;
  font-size: 11px;
}

.repair-task-summary b {
  color: #173657;
  font-size: 19px;
}

.repair-task-list {
  min-height: 140px;
  overflow-y: auto;
  overscroll-behavior: contain;
  display: grid;
  align-content: start;
  gap: 7px;
}

.repair-task-list article {
  display: grid;
  gap: 5px;
  border: 1px solid #dfe8f3;
  border-radius: 9px;
  padding: 9px 11px;
  background: #fbfdff;
}

.repair-task-list article > div {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.repair-task-list article span {
  border-radius: 999px;
  padding: 3px 8px;
  background: #eef4fb;
  color: #496581;
  font-size: 11px;
}

.repair-task-list article span.failed {
  background: #fff0f0;
  color: #b13232;
}

.repair-task-list article p {
  margin: 0;
  color: #9b3434;
  font-size: 12px;
  overflow-wrap: anywhere;
}

.repair-task-list article small {
  color: #74889e;
  font-size: 11px;
}

.repair-task-center > footer {
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 8px;
  border-top: 1px solid #e2ebf6;
  padding-top: 10px;
}

.repair-task-center > footer button {
  min-height: 34px;
  gap: 6px;
  border: 1px solid #cbd9eb;
  border-radius: 9px;
  padding: 0 12px;
  background: #fff;
  color: #31506f;
  font: inherit;
  font-size: 12px;
  font-weight: 750;
  cursor: pointer;
}

.repair-task-center > footer button.primary {
  border-color: #1e63ff;
  background: #1e63ff;
  color: #fff;
}

.repair-task-center > footer button:disabled {
  cursor: wait;
  opacity: 0.62;
}

.empty-state {
  display: grid;
  min-height: 140px;
  place-items: center;
  color: #7589a0;
  font-size: 13px;
}

.spinning {
  animation: spin 0.85s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
