<template>
  <AdminAccounts :pending-count="pendingRequestCount" :permissions="users" @changed="$emit('load-permissions')">
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

  </AdminAccounts>
</template>

<script setup lang="ts">
import type { LooseDict, ScopeOption } from "../types";
import AdminAccounts from "./AdminAccounts.vue";
import AdminPermissionRequests from "./AdminPermissionRequests.vue";

defineProps<{
  busy: boolean;
  pendingRequestCount: number;
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
  users: LooseDict[];
}>();

defineEmits<{
  "update:requestSearch": [value: string];
  "update:requestStatus": [value: string];
  "update:rejectReason": [value: string];
  "load-permissions": [];
  "load-requests": [];
  "approve-selected": [];
  "reject-selected": [];
  "toggle-all": [checked: boolean];
  "toggle-selection": [requestId: string, checked: boolean];
  "toggle-review-scope": [item: LooseDict, scope: string, checked: boolean];
  approve: [item: LooseDict];
  reject: [item: LooseDict];
}>();
</script>
