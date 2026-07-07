<template>
  <div class="signature-role-summary">
    <button type="button"
      v-for="item in items"
      :key="item.role"
      :class="{
        active: currentRole === item.role,
        ready: item.totalCount > 0 && signedCount(item) >= item.totalCount,
        pending: item.totalCount > 0 && signedCount(item) < item.totalCount
      }"
      @click="$emit('select', item.role)"
    >
      <div class="role-title-row">
        <span>{{ item.label }}</span>
        <b>{{ item.totalCount ? `${signedCount(item)}/${item.totalCount}` : "未选" }}</b>
      </div>
      <strong class="role-state-line" :class="{ ok: item.totalCount > 0 && missingCount(item) === 0, warn: missingCount(item) > 0 }">
        {{ roleStateText(item) }}
      </strong>
      <div v-if="item.totalCount" class="role-chip-row" aria-label="签名人员构成">
        <em class="company" :class="{ ok: item.companyCount > 0 && item.companyUnsigned === 0, empty: item.companyCount === 0 }">
          <span>公司</span>
          <strong>{{ item.companyCount }}</strong>
          <small>{{ item.companyUnsigned ? `${item.companyUnsigned} 未签` : item.companyCount ? "已齐" : "未选" }}</small>
        </em>
        <em class="temporary" :class="{ ok: item.temporaryCount > 0 && item.temporaryUnsigned === 0, empty: item.temporaryCount === 0 }">
          <span>临时</span>
          <strong>{{ item.temporaryCount }}</strong>
          <small>{{ item.temporaryUnsigned ? `${item.temporaryUnsigned} 未签` : item.temporaryCount ? "已齐" : "未选" }}</small>
        </em>
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
  companyUnsigned: number;
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

function signedCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.totalCount - item.companyUnsigned - item.temporaryUnsigned);
}

function missingCount(item: MopSignatureRoleSummaryItem): number {
  return Math.max(0, item.companyUnsigned + item.temporaryUnsigned);
}

function roleStateText(item: MopSignatureRoleSummaryItem): string {
  if (!item.totalCount) return "选择人员";
  const missing = missingCount(item);
  if (!missing) return "签名已齐";
  return `待签 ${missing} 人`;
}
</script>

<style scoped>
.signature-role-summary {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}

.signature-role-summary button {
  display: grid;
  gap: 4px;
  min-width: 0;
  padding: 6px;
  border: 1px solid rgba(191, 219, 254, 0.8);
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(248, 251, 255, 0.86)),
    #fff;
  box-shadow: 0 6px 14px rgba(37, 99, 235, 0.055);
  color: inherit;
  cursor: pointer;
  text-align: left;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.signature-role-summary button:hover,
.signature-role-summary button.active {
  border-color: rgba(37, 99, 235, 0.46);
  box-shadow: 0 12px 24px rgba(37, 99, 235, 0.1);
  transform: translateY(-1px);
}

.signature-role-summary button.ready {
  border-color: #bbf7d0;
  background:
    linear-gradient(135deg, rgba(236, 253, 245, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.signature-role-summary button.pending {
  border-color: #fed7aa;
  background:
    linear-gradient(135deg, rgba(255, 247, 237, 0.98), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.signature-role-summary button.active.ready {
  box-shadow: inset 4px 0 0 #059669, 0 14px 28px rgba(5, 150, 105, 0.12);
}

.signature-role-summary button.active.pending {
  box-shadow: inset 4px 0 0 #f97316, 0 14px 28px rgba(249, 115, 22, 0.12);
}

.signature-role-summary button:focus-visible {
  outline: 3px solid rgba(30, 99, 255, 0.22);
  outline-offset: 2px;
}

.role-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  min-width: 0;
}

.signature-role-summary span {
  min-width: 0;
  color: #475569;
  font-size: 12px;
  font-weight: 900;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.role-title-row b {
  flex: 0 0 auto;
  border-radius: 999px;
  padding: 3px 6px;
  background: #eff6ff;
  color: #075bd8;
  font-size: 11px;
  font-weight: 950;
}

.role-state-line {
  display: inline-flex;
  align-items: center;
  width: fit-content;
  border-radius: 999px;
  padding: 3px 6px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 11px;
  font-weight: 950;
  line-height: 1;
}

.role-state-line.ok {
  background: #ecfdf5;
  color: #047857;
}

.role-state-line.warn {
  background: #fff7ed;
  color: #c2410c;
}

.signature-role-summary strong:not(.role-state-line) {
  color: #0f172a;
  font-size: 13px;
  font-weight: 900;
}

.role-chip-row {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 5px;
}

.role-chip-row em {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 2px 6px;
  min-width: 0;
  min-height: 30px;
  padding: 4px 6px;
  border: 1px solid transparent;
  border-radius: 12px;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
  line-height: 1.2;
}

.role-chip-row .company {
  border-color: rgba(147, 197, 253, 0.62);
  background: rgba(219, 234, 254, 0.9);
  color: #1d4ed8;
}

.role-chip-row .temporary {
  border-color: rgba(253, 186, 116, 0.72);
  background: rgba(254, 243, 199, 0.95);
  color: #92400e;
}

.role-chip-row em.ok {
  border-color: rgba(134, 239, 172, 0.74);
  background: rgba(220, 252, 231, 0.92);
  color: #047857;
}

.role-chip-row em.empty {
  border-color: rgba(203, 213, 225, 0.68);
  background: rgba(248, 250, 252, 0.92);
  color: #64748b;
}

.role-chip-row em span,
.role-chip-row em small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.role-chip-row em span {
  color: inherit;
  font-size: 11px;
  font-weight: 950;
}

.role-chip-row em strong {
  grid-row: 1 / 3;
  grid-column: 2;
  color: inherit;
  font-size: 14px;
  font-weight: 950;
  line-height: 1;
}

.role-chip-row em small {
  color: currentColor;
  font-size: 10px;
  font-weight: 850;
  opacity: 0.82;
}

@media (max-width: 720px) {
  .signature-role-summary {
    grid-template-columns: 1fr;
  }
}
</style>
