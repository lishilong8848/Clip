<template>
  <section class="pane permission-pane">
    <div class="actions permission-command-bar">
      <div class="command-copy">
        <strong>权限管理</strong>
      </div>
      <button class="btn ghost" :disabled="busy" @click="$emit('load-permissions')">刷新权限</button>
      <button class="btn ghost" :disabled="busy" @click="$emit('add-user')">添加用户</button>
      <button class="btn green" :disabled="busy" @click="$emit('save')">保存权限</button>
    </div>

    <section class="permission-metrics" aria-label="权限概览">
      <article>
        <span>待审批</span>
        <strong>{{ pendingRequestCount }}</strong>
      </article>
      <article>
        <span>已启用用户</span>
        <strong>{{ enabledUserCount }}</strong>
      </article>
      <article>
        <span>管理员</span>
        <strong>{{ adminUserCount }}</strong>
      </article>
      <article>
        <span>当前筛选</span>
        <strong>{{ users.length }}</strong>
      </article>
    </section>

    <AdminPermissionRequests
      :search="requestSearch"
      :status="requestStatus"
      :reject-reason="rejectReason"
      :items="requestItems"
      :scope-options="scopeOptions"
      :busy="busy"
      :selected-ids="selectedRequestIds"
      :all-filtered-pending-selected="allFilteredPendingSelected"
      :selected-pending-count="selectedPendingCount"
      :status-label="requestStatusLabel"
      :request-scope-labels="requestScopeLabels"
      :current-scope-labels="currentScopeLabels"
      :review-scopes="reviewScopes"
      @update:search="$emit('update:requestSearch', $event)"
      @update:status="$emit('update:requestStatus', $event)"
      @update:reject-reason="$emit('update:rejectReason', $event)"
      @load="$emit('load-requests')"
      @approve-selected="$emit('approve-selected')"
      @reject-selected="$emit('reject-selected')"
      @toggle-all="$emit('toggle-all', $event)"
      @toggle-selection="(requestId, checked) => $emit('toggle-selection', requestId, checked)"
      @toggle-review-scope="(item, scope, checked) => $emit('toggle-review-scope', item, scope, checked)"
      @approve="$emit('approve', $event)"
      @reject="$emit('reject', $event)"
    />

    <AdminPermissionUsers
      :search="userSearch"
      :filter="userFilter"
      :users="users"
      :total="usersTotal"
      :scope-options="scopeOptions"
      :scope-summary="userScopeSummary"
      @update:search="$emit('update:userSearch', $event)"
      @update:filter="$emit('update:userFilter', $event)"
      @toggle-scope="(user, scope, checked) => $emit('toggle-user-scope', user, scope, checked)"
      @remove="$emit('remove-user', $event)"
    />
  </section>
</template>

<script setup lang="ts">
import type { LooseDict, ScopeOption } from "../types";
import AdminPermissionRequests from "./AdminPermissionRequests.vue";
import AdminPermissionUsers from "./AdminPermissionUsers.vue";

defineProps<{
  busy: boolean;
  pendingRequestCount: number;
  enabledUserCount: number;
  adminUserCount: number;
  requestSearch: string;
  requestStatus: string;
  rejectReason: string;
  requestItems: LooseDict[];
  scopeOptions: ScopeOption[];
  selectedRequestIds: Set<string>;
  allFilteredPendingSelected: boolean;
  selectedPendingCount: number;
  requestStatusLabel: (value: unknown) => string;
  requestScopeLabels: (item: LooseDict) => string;
  currentScopeLabels: (item: LooseDict) => string;
  reviewScopes: (item: LooseDict) => string[];
  userSearch: string;
  userFilter: string;
  users: LooseDict[];
  usersTotal: number;
  userScopeSummary: (user: LooseDict) => string;
}>();

defineEmits<{
  "update:requestSearch": [value: string];
  "update:requestStatus": [value: string];
  "update:rejectReason": [value: string];
  "update:userSearch": [value: string];
  "update:userFilter": [value: string];
  "load-permissions": [];
  "load-requests": [];
  "add-user": [];
  save: [];
  "approve-selected": [];
  "reject-selected": [];
  "toggle-all": [checked: boolean];
  "toggle-selection": [requestId: string, checked: boolean];
  "toggle-review-scope": [item: LooseDict, scope: string, checked: boolean];
  approve: [item: LooseDict];
  reject: [item: LooseDict];
  "toggle-user-scope": [user: LooseDict, scope: string, checked: boolean];
  "remove-user": [user: LooseDict];
}>();
</script>

<style scoped>
.permission-pane {
  display: grid;
  gap: 16px;
}

.actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.btn {
  min-height: 38px;
  border: 1px solid #cbd5e1;
  border-radius: 12px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
  font-weight: 850;
  padding: 0 14px;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.btn:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 18px rgba(27, 101, 213, 0.12);
  transform: translateY(-1px);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.62;
}

.btn.ghost {
  background: #f8fbff;
  color: #1d4ed8;
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
  color: #ffffff;
  box-shadow: 0 10px 22px rgba(22, 163, 109, 0.2);
}

.permission-command-bar {
  justify-content: space-between;
  padding: 12px;
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.86);
}

.command-copy {
  display: grid;
  gap: 2px;
  margin-right: auto;
}

.command-copy strong {
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
}

.permission-metrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.permission-metrics article {
  min-width: 0;
  display: grid;
  gap: 5px;
  padding: 14px 15px;
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.9);
}

.permission-metrics span {
  color: #60758f;
  font-size: 12px;
  font-weight: 850;
}

.permission-metrics strong {
  color: #0e5bd8;
  font-size: 24px;
  font-weight: 950;
  line-height: 1;
}

@media (max-width: 900px) {
  .permission-command-bar,
  .permission-metrics {
    grid-template-columns: 1fr;
  }

  .permission-command-bar {
    align-items: stretch;
    flex-direction: column;
  }

  .permission-command-bar .btn {
    width: 100%;
  }
}
</style>
