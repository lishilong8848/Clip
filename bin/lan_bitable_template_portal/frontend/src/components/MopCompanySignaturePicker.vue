<template>
  <section class="company-signature-picker" :class="{ compact }">
    <div class="company-signature-head">
      <strong>公司人员</strong>
      <small>{{ selectedIds.length ? `已选 ${selectedIds.length}` : "未选择" }}</small>
    </div>
    <label class="company-signature-search">
      <span>搜索</span>
      <div class="inline-search">
        <input
          :value="search"
          enterkeyhint="search"
          placeholder="姓名、工号、楼栋"
          @input="emit('update:search', ($event.target as HTMLInputElement).value)"
          @keyup.enter="emit('refresh')"
        />
        <button
          class="btn ghost signature-refresh"
          type="button"
          :disabled="loading"
          title="重新读取签名人员"
          @click="emit('refresh')"
        >
          {{ loading ? "读取中" : "刷新" }}
        </button>
      </div>
      <small class="search-inline-status">{{ statusText }}</small>
    </label>
    <div class="sign-person-list">
      <button
        v-for="person in people"
        :key="String(person.record_id || person.open_id || person.name || '')"
        type="button"
        class="sign-person"
        :class="{
          active: selectedIds.includes(String(person.record_id || '')),
          current: String(person.record_id || '') === activeRecordId
        }"
        :disabled="!person.record_id"
        :title="person.record_id ? '选择该人员签名' : '该人员缺少记录信息，无法选择'"
        @click="emit('select', String(person.record_id || ''))"
      >
        <b v-if="selectedIds.includes(String(person.record_id || ''))" class="selected-corner">已选</b>
        <span>{{ personInitial(person) }}</span>
        <strong>{{ person.name || "未命名人员" }}</strong>
        <small>
          <template v-if="person.employee_no">{{ person.employee_no }} · </template>
          <template v-if="person.building">{{ person.building }} · </template>
          {{ person.position || person.team || "签名人员" }}
        </small>
        <em :class="{ ok: personHasUsableSignature(person) }">
          {{ personHasUsableSignature(person) ? "已有签名" : "待签名" }}
        </em>
      </button>
      <div v-if="!loading && !people.length" class="empty-box compact">
        暂未找到签名人员。
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { mopPersonHasUsableSignature } from "../mopSignatureUtils";

type Dict = Record<string, any>;

withDefaults(defineProps<{
  search: string;
  loading: boolean;
  statusText: string;
  people: Dict[];
  selectedIds: string[];
  activeRecordId?: string;
  compact?: boolean;
}>(), {
  activeRecordId: "",
  compact: false,
});

const emit = defineEmits<{
  "update:search": [value: string];
  refresh: [];
  select: [recordId: string];
}>();

function personInitial(person: Dict): string {
  const name = String(person?.name || person?.employee_no || person?.position || person?.team || "?").trim();
  return name.slice(0, 1).toUpperCase() || "?";
}

function personHasUsableSignature(person: Dict | null | undefined): boolean {
  return mopPersonHasUsableSignature(person);
}
</script>

<style scoped>
.company-signature-picker {
  display: grid;
  align-content: start;
  gap: 7px;
  min-width: 0;
  border: 1px solid #b8d7ff;
  border-radius: 16px;
  background: linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.98));
  box-shadow: inset 4px 0 0 #1e63ff;
  padding: 8px;
}

.company-signature-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.company-signature-head strong {
  color: #0f172a;
  font-size: 13px;
  font-weight: 950;
}

.company-signature-head small {
  border-radius: 999px;
  padding: 3px 7px;
  color: #1d4ed8;
  background: #dbeafe;
  font-size: 11px;
  font-weight: 900;
  line-height: 1.4;
}

.company-signature-search {
  display: grid;
  gap: 4px;
  min-width: 0;
  color: #475569;
  font-size: 11px;
  font-weight: 900;
}

.inline-search {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 6px;
}

.inline-search input {
  min-width: 0;
}

.signature-refresh {
  min-width: 42px;
  min-height: 30px;
  border-radius: 999px;
  padding: 4px 9px;
  font-size: 11px;
  line-height: 1;
}

.search-inline-status {
  display: inline-flex;
  width: fit-content;
  max-width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 3px 7px;
  background: rgba(248, 251, 255, 0.92);
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
  line-height: 1.4;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sign-person-list {
  display: grid;
  gap: 6px;
  max-height: 260px;
  overflow: auto;
  padding-right: 4px;
}

.sign-person {
  position: relative;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr) auto;
  gap: 6px;
  align-items: center;
  border: 1px solid #dbe7f6;
  border-radius: 13px;
  background: #ffffff;
  padding: 6px 7px;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
  transition:
    border-color 0.16s ease,
    box-shadow 0.16s ease,
    background 0.16s ease;
}

.sign-person:hover,
.sign-person.active {
  border-color: #1e63ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.12);
}

.sign-person.active {
  border-color: #1e63ff;
  background:
    linear-gradient(135deg, rgba(219, 234, 254, 0.98), rgba(239, 246, 255, 0.96)),
    #eff6ff;
  box-shadow:
    0 12px 28px rgba(30, 99, 255, 0.18),
    inset 4px 0 0 #1e63ff,
    inset 0 0 0 1px rgba(30, 99, 255, 0.2);
}

.sign-person:disabled {
  cursor: not-allowed;
  opacity: 0.58;
  box-shadow: none;
}

.sign-person:disabled:hover {
  border-color: #dbe7f6;
  background: #ffffff;
}

.sign-person.current {
  background: linear-gradient(135deg, #f8fbff, #eef6ff);
}

.sign-person.active.current {
  border-color: #0f4ed8;
  background:
    linear-gradient(135deg, rgba(191, 219, 254, 0.98), rgba(239, 246, 255, 0.98)),
    #dbeafe;
  box-shadow:
    0 14px 30px rgba(30, 99, 255, 0.24),
    inset 4px 0 0 #0f4ed8,
    inset 0 0 0 2px rgba(15, 78, 216, 0.2);
}

.sign-person > span {
  grid-row: span 2;
  display: grid;
  width: 30px;
  height: 30px;
  place-items: center;
  border-radius: 11px;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #22c1dc);
  font-weight: 900;
}

.sign-person.active > span {
  background: linear-gradient(135deg, #0f4ed8, #00b7d7);
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.28);
}

.selected-corner {
  position: absolute;
  top: 4px;
  right: 6px;
  z-index: 2;
  border-radius: 999px;
  background: #1e63ff;
  padding: 2px 6px;
  color: #ffffff;
  font-size: 10px;
  font-weight: 950;
  line-height: 1.1;
  box-shadow: 0 6px 14px rgba(30, 99, 255, 0.24);
}

.sign-person strong,
.sign-person small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sign-person strong {
  align-self: end;
  font-size: 12px;
  line-height: 1.25;
}

.sign-person small {
  align-self: start;
  color: #64748b;
  font-size: 11px;
  line-height: 1.25;
}

.sign-person em {
  grid-column: 3;
  grid-row: 1 / 3;
  align-self: center;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 22px;
  border-radius: 999px;
  background: #eef2ff;
  padding: 4px 7px;
  color: #3156c9;
  font-size: 11px;
  font-style: normal;
  font-weight: 850;
  line-height: 1;
  white-space: nowrap;
}

.sign-person.active em {
  border: 1px solid rgba(30, 99, 255, 0.18);
  background: rgba(255, 255, 255, 0.86);
  color: #0f4ed8;
}

.sign-person em.ok {
  color: #047857;
  background: #ecfdf5;
}

.sign-person.active em.ok {
  border-color: rgba(16, 185, 129, 0.25);
  background: rgba(236, 253, 245, 0.94);
  color: #047857;
}

.company-signature-picker.compact .sign-person-list {
  max-height: 176px;
}

.company-signature-picker.compact {
  gap: 6px;
  padding: 7px;
}

.company-signature-picker.compact .company-signature-head small {
  display: none;
}

.company-signature-picker.compact .company-signature-search {
  gap: 5px;
}

.company-signature-picker.compact .search-inline-status {
  max-width: 100%;
  padding: 3px 7px;
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.company-signature-picker.compact .sign-person {
  min-height: 42px;
  grid-template-columns: 30px minmax(0, 1fr) auto;
  padding: 6px 7px;
}

.company-signature-picker.compact .sign-person > span {
  width: 30px;
  height: 30px;
  border-radius: 10px;
}

.company-signature-picker.compact .sign-person small,
.company-signature-picker.compact .sign-person em {
  font-size: 11px;
}

.company-signature-picker.compact .sign-person em {
  min-height: 22px;
  padding: 4px 7px;
}

.empty-box.compact {
  border: 1px dashed #cfe0ff;
  border-radius: 16px;
  padding: 14px;
  color: #64748b;
  background: rgba(255, 255, 255, 0.74);
  font-size: 13px;
  font-weight: 800;
  text-align: center;
}

@media (max-width: 720px) {
  .sign-person-list {
    max-height: 220px;
  }
}
</style>
