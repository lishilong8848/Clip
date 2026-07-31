<template>
  <div class="signature-role-summary" role="group" aria-label="签名角色">
    <button type="button"
      v-for="item in items"
      :key="item.role"
      :aria-pressed="currentRole === item.role"
      :class="{
        active: currentRole === item.role,
        ready: item.totalCount > 0 && notReadyCount(item) === 0,
        pending: notReadyCount(item) > 0
      }"
      @click="$emit('select', item.role)"
    >
      <div class="role-title-row">
        <span>{{ item.label }}</span>
        <b>{{ item.totalCount ? `${readyCount(item)}/${item.totalCount}` : "未选" }}</b>
      </div>
      <div class="role-state-row">
        <strong :class="{ ok: item.totalCount > 0 && notReadyCount(item) === 0, warn: notReadyCount(item) > 0 }">
          {{ roleStateText(item) }}
        </strong>
        <small v-if="item.totalCount">公司 {{ item.companyCount }} · 临时 {{ item.temporaryCount }}</small>
      </div>
    </button>
  </div>
</template>

<script setup lang="ts">
export type MopSignatureRole = "implementer" | "auditor";

export type MopSignatureRoleSummaryItem = {
  role: MopSignatureRole;
  label: string;
  totalCount: number;
  companyCount: number;
  companyMissingSignature: number;
  companyPendingConfirmation: number;
  companyRejected: number;
  temporaryCount: number;
  temporaryUnsigned: number;
};

defineProps<{
  currentRole: MopSignatureRole;
  items: MopSignatureRoleSummaryItem[];
}>();

defineEmits<{
  select: [role: MopSignatureRole];
}>();

function missingSignatureCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.companyMissingSignature + item.temporaryUnsigned);
}

function pendingConfirmationCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.companyPendingConfirmation);
}

function rejectedCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.companyRejected);
}

function notReadyCount(item: MopSignatureRoleSummaryItem): number {
  return missingSignatureCount(item) + pendingConfirmationCount(item) + rejectedCount(item);
}

function readyCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.totalCount - notReadyCount(item));
}

function roleStateText(item: MopSignatureRoleSummaryItem): string {
  if (!item.totalCount) return "选择人员";
  const missing = missingSignatureCount(item);
  const pending = pendingConfirmationCount(item);
  const rejected = rejectedCount(item);
  if (!missing && !pending && !rejected) return "全部可用";
  const statuses: string[] = [];
  if (missing) statuses.push(`未签 ${missing}`);
  if (pending) statuses.push(`待确认 ${pending}`);
  if (rejected) statuses.push(`已拒绝 ${rejected}`);
  return statuses.join(" · ");
}
</script>

<style scoped>
.signature-role-summary {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}

.signature-role-summary button {
  min-width: 0;
  min-height: 50px;
  display: grid;
  gap: 5px;
  border: 1px solid #d8e5f7;
  border-radius: 10px;
  padding: 7px 9px;
  background: #ffffff;
  color: inherit;
  cursor: pointer;
  text-align: left;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, background 0.16s ease;
}

.signature-role-summary button:hover,
.signature-role-summary button.active {
  border-color: #7fb0ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.1);
}

.signature-role-summary button.active {
  background: #eff6ff;
  box-shadow: inset 3px 0 0 #1e63ff, 0 8px 18px rgba(30, 99, 255, 0.1);
}

.signature-role-summary button.ready {
  border-color: #a7e8c2;
}

.signature-role-summary button.active.ready {
  background: #f0fdf4;
  box-shadow: inset 3px 0 0 #059669, 0 8px 18px rgba(5, 150, 105, 0.1);
}

.signature-role-summary button.pending {
  border-color: #fed7aa;
}

.signature-role-summary button:focus-visible {
  outline: 3px solid rgba(30, 99, 255, 0.22);
  outline-offset: 2px;
}

.role-title-row,
.role-state-row {
  min-width: 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.role-title-row span {
  min-width: 0;
  overflow: hidden;
  color: #334155;
  font-size: 12px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.role-title-row b {
  flex: 0 0 auto;
  border-radius: 999px;
  padding: 2px 7px;
  background: #dbeafe;
  color: #075bd8;
  font-size: 11px;
  font-weight: 950;
}

.role-state-row strong {
  min-width: 0;
  overflow: hidden;
  color: #c2410c;
  font-size: 11px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.role-state-row strong.ok {
  color: #047857;
}

.role-state-row small {
  flex: 0 0 auto;
  color: #64748b;
  font-size: 10px;
  font-weight: 800;
  white-space: nowrap;
}

@media (max-width: 720px) {
  .signature-role-summary {
    grid-template-columns: 1fr;
  }
}
</style>
