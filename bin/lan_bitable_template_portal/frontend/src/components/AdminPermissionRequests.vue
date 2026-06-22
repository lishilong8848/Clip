<template>
  <section class="permission-review-panel">
    <div class="section-title">
      <div>
        <strong>权限申请审批</strong>
        <span>用户提交申请后，管理员在这里集中通过或拒绝</span>
      </div>
      <div class="section-title-stats" aria-label="权限申请数量">
        <b>{{ pendingCount }}</b>
        <small>待审批 / 共 {{ items.length }}</small>
      </div>
    </div>
    <div class="permission-review-toolbar">
      <div class="review-filter-main">
        <input v-model="searchModel" placeholder="搜索姓名、openid、楼栋、原因" />
        <select v-model="statusModel" @change="$emit('load')">
          <option value="pending">待审批</option>
          <option value="approved">已通过</option>
          <option value="rejected">已拒绝</option>
          <option value="all">全部</option>
        </select>
      </div>
      <div class="reject-reason-field">
        <span>拒绝原因</span>
        <input v-model="rejectReasonModel" placeholder="批量拒绝时使用，可不填" />
      </div>
      <div class="toolbar-actions">
        <button class="btn ghost" :disabled="busy" :title="busy ? '正在处理权限申请' : '重新读取申请列表'" @click="$emit('load')">刷新申请</button>
        <button class="btn blue" :disabled="busy || !selectedPendingCount" :title="batchActionTitle" @click="$emit('approve-selected')">
          批量通过{{ selectedPendingCount ? ` ${selectedPendingCount}` : "" }}
        </button>
        <button class="btn danger" :disabled="busy || !selectedPendingCount" :title="batchActionTitle" @click="$emit('reject-selected')">
          批量拒绝{{ selectedPendingCount ? ` ${selectedPendingCount}` : "" }}
        </button>
      </div>
    </div>
    <DisabledReason
      v-if="batchDisabledReason"
      :text="batchDisabledReason"
      tone="warning"
    />
    <div v-if="items.length" class="review-summary-strip">
      <span><b>{{ items.length }}</b> 条申请</span>
      <span><b>{{ pendingCount }}</b> 条待审批</span>
      <span :class="{ ready: selectedPendingCount > 0 }">
        <b>{{ selectedPendingCount }}</b> 条已选
      </span>
      <em>{{ selectedPendingCount ? "可直接批量通过或拒绝。" : "勾选待审批申请后可批量处理。" }}</em>
    </div>
    <label v-if="items.length" class="select-all-line">
      <input
        type="checkbox"
        :checked="allFilteredPendingSelected"
        :disabled="!pendingCount || busy"
        :title="!pendingCount ? '当前列表没有待审批申请' : busy ? '正在处理权限申请' : '全选当前待审批申请'"
        @change="$emit('toggle-all', ($event.target as HTMLInputElement).checked)"
      />
      全选当前待审批申请
      <span v-if="pendingCount">共 {{ pendingCount }} 条</span>
    </label>
    <div v-if="items.length" class="permission-request-list">
      <article
        v-for="item in items"
        :key="item.request_id"
        class="permission-request-row"
        :class="`status-${item.status || 'unknown'}`"
      >
        <label class="request-select">
          <input
            type="checkbox"
            :disabled="busy || item.status !== 'pending'"
            :title="item.status !== 'pending' ? '该申请已处理，不能再次勾选' : busy ? '正在处理权限申请' : '勾选后可批量处理'"
            :checked="selectedIds.has(String(item.request_id || ''))"
            @change="$emit('toggle-selection', String(item.request_id || ''), ($event.target as HTMLInputElement).checked)"
          />
        </label>
        <div class="request-main">
          <div class="request-title">
            <strong>{{ item.name || "飞书用户" }}</strong>
            <span>{{ item.open_id }}</span>
            <b class="request-status" :class="`status-${item.status || 'unknown'}`">{{ statusLabel(item.status) }}</b>
          </div>
          <p>申请范围：{{ requestScopeLabels(item) }}</p>
          <p>已有权限：{{ currentScopeLabels(item) }}</p>
          <p v-if="item.reason">申请原因：{{ item.reason }}</p>
          <p v-if="item.reject_reason" class="danger-text">拒绝原因：{{ item.reject_reason }}</p>
          <small>提交时间：{{ item.created_at || "未知" }}</small>
          <div v-if="item.status === 'pending'" class="scope-checks review-scopes">
            <label v-for="scope in scopeOptions" :key="scope.value">
              <input
                type="checkbox"
                :checked="reviewScopes(item).includes(scope.value)"
                @change="$emit('toggle-review-scope', item, scope.value, ($event.target as HTMLInputElement).checked)"
              />
              {{ scope.label }}
            </label>
          </div>
        </div>
        <div class="request-actions">
          <button class="btn blue" :disabled="busy || item.status !== 'pending'" :title="rowActionTitle(item)" @click="$emit('approve', item)">
            通过
          </button>
          <button class="btn danger" :disabled="busy || item.status !== 'pending'" :title="rowActionTitle(item)" @click="$emit('reject', item)">
            拒绝
          </button>
        </div>
      </article>
    </div>
    <div v-else class="empty-review">
      <strong>当前没有符合条件的权限申请</strong>
      <span>可切换状态筛选，或点击刷新申请重新读取。</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { Dict } from "../api/client";
import DisabledReason from "./DisabledReason.vue";

const props = defineProps<{
  items: Dict[];
  scopeOptions: Dict[];
  busy: boolean;
  selectedIds: Set<string>;
  allFilteredPendingSelected: boolean;
  selectedPendingCount: number;
  search: string;
  status: string;
  rejectReason: string;
  statusLabel: (value: unknown) => string;
  requestScopeLabels: (item: Dict) => string;
  currentScopeLabels: (item: Dict) => string;
  reviewScopes: (item: Dict) => string[];
}>();

const emit = defineEmits<{
  "update:search": [value: string];
  "update:status": [value: string];
  "update:rejectReason": [value: string];
  load: [];
  "approve-selected": [];
  "reject-selected": [];
  "toggle-all": [checked: boolean];
  "toggle-selection": [requestId: string, checked: boolean];
  "toggle-review-scope": [item: Dict, scope: string, checked: boolean];
  approve: [item: Dict];
  reject: [item: Dict];
}>();

const searchModel = computed({
  get: () => props.search,
  set: (value: string) => emit("update:search", value),
});

const statusModel = computed({
  get: () => props.status,
  set: (value: string) => emit("update:status", value),
});

const rejectReasonModel = computed({
  get: () => props.rejectReason,
  set: (value: string) => emit("update:rejectReason", value),
});

const pendingCount = computed(() => (
  props.items.filter((item) => String(item.status || "") === "pending").length
));

const batchActionTitle = computed(() => {
  if (props.busy) return "正在处理权限申请";
  if (!pendingCount.value) return "当前列表没有待审批申请";
  if (!props.selectedPendingCount) return "请先勾选待审批申请";
  return `将处理 ${props.selectedPendingCount} 条已选申请`;
});

const batchDisabledReason = computed(() => {
  if (props.busy) return "正在处理权限申请，请等待当前操作完成。";
  if (!pendingCount.value) return "当前列表没有待审批申请。";
  if (!props.selectedPendingCount) return "请先勾选待审批申请，再批量通过或拒绝。";
  return "";
});

function rowActionTitle(item: Dict): string {
  if (props.busy) return "正在处理权限申请";
  if (String(item.status || "") !== "pending") return "该申请已处理";
  return "审批当前申请";
}
</script>

<style scoped>
.permission-review-panel {
  display: grid;
  gap: 14px;
  padding: 16px;
  border: 1px solid #d8e7f8;
  border-radius: 22px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.97), rgba(248, 251, 255, 0.9)),
    #ffffff;
  box-shadow: 0 14px 34px rgba(20, 70, 138, 0.09);
}

.section-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.section-title > div:first-child {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.section-title strong {
  color: #09204a;
  font-size: 17px;
  font-weight: 950;
}

.section-title span,
.muted-line {
  color: #64748b;
  font-size: 12px;
}

.section-title-stats {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 6px 10px;
  background: #eff6ff;
  color: #0757d7;
}

.section-title-stats b {
  font-size: 18px;
  font-weight: 950;
  line-height: 1;
}

.section-title-stats small {
  color: #315a8a;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.permission-review-toolbar {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) minmax(220px, 0.7fr) auto;
  gap: 12px;
  align-items: stretch;
  border: 1px solid #dbe7f5;
  border-radius: 20px;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
  padding: 10px;
}

.review-filter-main {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 132px;
  gap: 8px;
  min-width: 0;
}

.reject-reason-field {
  display: grid;
  gap: 5px;
  min-width: 0;
}

.reject-reason-field span {
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
}

.permission-review-toolbar input,
.permission-review-toolbar select {
  width: 100%;
  min-height: 40px;
  border: 1px solid #c8dcf3;
  border-radius: 15px;
  background: #fbfdff;
  padding: 8px 11px;
  color: #0f172a;
  font-weight: 760;
}

.toolbar-actions {
  display: grid;
  grid-template-columns: repeat(3, max-content);
  justify-content: end;
  gap: 8px;
  align-content: end;
}

.review-summary-strip {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  padding: 9px 11px;
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.88), rgba(255, 255, 255, 0.94)),
    #ffffff;
}

.review-summary-strip span {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  min-height: 28px;
  border: 1px solid #dbe7f5;
  border-radius: 999px;
  padding: 4px 10px;
  background: #ffffff;
  color: #48627f;
  font-size: 12px;
  font-weight: 850;
}

.review-summary-strip span.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.review-summary-strip b {
  color: #0757d7;
  font-size: 13px;
  font-weight: 950;
}

.review-summary-strip em {
  color: #64748b;
  font-size: 12px;
  font-style: normal;
  font-weight: 800;
  line-height: 1.4;
}

.select-all-line {
  width: fit-content;
  min-height: 34px;
  border: 1px solid #d8e7f8;
  border-radius: 999px;
  background: #f8fbff;
  padding: 6px 11px;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #334155;
  font-size: 13px;
  font-weight: 800;
}

.select-all-line:has(input:disabled) {
  opacity: 0.62;
}

.select-all-line span {
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.permission-request-list {
  display: grid;
  gap: 10px;
}

.permission-request-row {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) auto;
  gap: 12px;
  align-items: start;
  padding: 13px;
  border: 1px solid #dbeafe;
  border-radius: 18px;
  background: #ffffff;
  box-shadow: 0 8px 20px rgba(15, 86, 228, 0.05);
}

.permission-request-row.status-approved {
  border-color: #bbf7d0;
  background: #f6fef9;
}

.permission-request-row.status-rejected {
  border-color: #fecaca;
  background: #fff8f8;
}

.request-select {
  padding-top: 5px;
}

.request-main {
  display: grid;
  gap: 6px;
  min-width: 0;
}

.request-title {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.request-title strong {
  color: #0f172a;
  font-size: 15px;
}

.request-title span {
  max-width: 320px;
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.request-title .request-status {
  padding: 4px 8px;
  border-radius: 999px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 12px;
}

.request-title .request-status.status-approved {
  background: #ecfdf5;
  color: #047857;
}

.request-title .request-status.status-rejected {
  background: #fef2f2;
  color: #b91c1c;
}

.request-title .request-status.status-pending {
  background: #fff7ed;
  color: #9a3412;
}

.request-main p,
.request-main small {
  margin: 0;
  color: #64748b;
  font-size: 12px;
}

.danger-text {
  color: #b91c1c;
}

.request-actions {
  display: grid;
  gap: 8px;
  min-width: 96px;
}

.scope-checks {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
}

.review-scopes {
  margin-top: 4px;
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

.btn {
  min-height: 36px;
  border: 1px solid #c5d9f2;
  border-radius: 15px;
  background: #ffffff;
  padding: 8px 12px;
  color: #09204a;
  font-weight: 850;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #dc2626, #f05656);
  color: #ffffff;
}

.empty-review {
  display: grid;
  gap: 5px;
  border: 1px dashed #cfe0ff;
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.86);
  padding: 18px;
  color: #64748b;
}

.empty-review strong {
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.empty-review span {
  font-size: 12px;
  font-weight: 780;
}

@media (max-width: 980px) {
  .permission-review-toolbar,
  .permission-request-row {
    grid-template-columns: 1fr;
  }

  .review-filter-main,
  .toolbar-actions {
    grid-template-columns: 1fr;
  }

  .request-actions {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .section-title {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
