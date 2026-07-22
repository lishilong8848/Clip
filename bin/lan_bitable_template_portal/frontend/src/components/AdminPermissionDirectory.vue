<template>
  <section class="directory-card">
    <header class="directory-head">
      <div>
        <strong>人员表授权</strong>
        <span>{{ selectableCount }} 人可授权</span>
      </div>
      <button
        type="button"
        class="icon-button"
        :disabled="loading || granting"
        title="重新读取人员表"
        @click="$emit('refresh')"
      >
        <RefreshCw :size="17" :class="{ spinning: loading }" aria-hidden="true" />
      </button>
    </header>

    <div class="directory-toolbar">
      <label class="directory-search">
        <Search :size="16" aria-hidden="true" />
        <input v-model.trim="query" type="search" placeholder="搜索姓名、工号、岗位、楼栋或专业" />
      </label>
      <label class="select-page">
        <input
          type="checkbox"
          :checked="allPageSelected"
          :disabled="!pageSelectableItems.length || loading || granting"
          @change="togglePage(($event.target as HTMLInputElement).checked)"
        />
        选择本页
      </label>
      <button
        type="button"
        class="grant-button"
        :disabled="!selectedIds.size || !selectedScopes.size || loading || granting"
        :title="!selectedScopes.size ? '请先选择本次授权楼栋' : ''"
        @click="$emit('grant')"
      >
        <UserPlus :size="17" aria-hidden="true" />
        {{ granting ? "授权中" : `授权 ${selectedIds.size} 人` }}
      </button>
    </div>

    <div class="grant-scope-picker">
      <strong>本次授权楼栋</strong>
      <div>
        <label
          v-for="option in scopeOptions"
          :key="option.value"
          :class="{ selected: selectedScopes.has(option.value) }"
        >
          <input
            type="checkbox"
            :checked="selectedScopes.has(option.value)"
            :disabled="loading || granting"
            @change="$emit('toggle-scope', option.value, ($event.target as HTMLInputElement).checked)"
          />
          {{ option.label }}
        </label>
      </div>
    </div>

    <div v-if="error" class="directory-message error">{{ error }}</div>
    <div v-else-if="loading && !items.length" class="directory-message">正在读取人员表...</div>
    <div v-else-if="!filteredItems.length" class="directory-message">没有匹配的人员</div>

    <div v-else class="directory-table-wrap">
      <table class="directory-table">
        <thead>
          <tr>
            <th aria-label="选择"></th>
            <th>人员</th>
            <th>岗位</th>
            <th>机楼权限</th>
            <th>专业</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in pageItems"
            :key="String(item.record_id || '')"
            :class="{
              selected: selectedIds.has(String(item.record_id || '')),
              unavailable: !item.selectable,
            }"
          >
            <td>
              <input
                type="checkbox"
                :checked="selectedIds.has(String(item.record_id || ''))"
                :disabled="!isGrantable(item) || loading || granting"
                :aria-label="`选择 ${item.name || '人员'}`"
                @change="$emit('toggle', String(item.record_id || ''), ($event.target as HTMLInputElement).checked)"
              />
            </td>
            <td>
              <div class="person-cell">
                <b>{{ item.name || "未命名人员" }}</b>
                <span>{{ item.employee_no || "工号未填" }}</span>
              </div>
            </td>
            <td><span class="position-value">{{ item.position || "未配置" }}</span></td>
            <td>
              <div class="tag-list">
                <span v-for="label in item.scope_labels || []" :key="label" class="tag building">{{ label }}</span>
                <span v-if="!(item.scope_labels || []).length" class="muted">未配置</span>
              </div>
            </td>
            <td>
              <div class="tag-list">
                <span v-for="label in item.specialties || []" :key="label" class="tag specialty">{{ label }}</span>
                <span v-if="!(item.specialties || []).length" class="muted">未配置</span>
              </div>
            </td>
            <td>
              <span v-if="!item.selectable" class="status unavailable-status">{{ item.unavailable_reason }}</span>
              <span v-else-if="hasAllSelectedScopes(item)" class="status authorized">
                <ShieldCheck :size="14" aria-hidden="true" />已拥有所选权限
              </span>
              <span
                v-else-if="(item.authorized_scope_labels || []).length"
                class="status partial"
                :title="`已有权限：${(item.authorized_scope_labels || []).join('、')}`"
              >已有 {{ (item.authorized_scope_labels || []).length }} 楼</span>
              <span v-else class="status ready">未授权</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer v-if="filteredItems.length" class="directory-footer">
      <span>{{ loadedAt ? `更新于 ${loadedAt}` : `共 ${filteredItems.length} 人` }}</span>
      <nav aria-label="人员表分页">
        <button type="button" :disabled="page <= 1" @click="page -= 1">上一页</button>
        <b>{{ page }} / {{ pageCount }}</b>
        <button type="button" :disabled="page >= pageCount" @click="page += 1">下一页</button>
      </nav>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { RefreshCw, Search, ShieldCheck, UserPlus } from "lucide-vue-next";
import type { LooseDict } from "../types";

const PAGE_SIZE = 12;

const props = defineProps<{
  items: LooseDict[];
  selectedIds: Set<string>;
  scopeOptions: Array<{ value: string; label: string }>;
  selectedScopes: Set<string>;
  loading: boolean;
  granting: boolean;
  loadedAt: string;
  error: string;
}>();

const emit = defineEmits<{
  refresh: [];
  grant: [];
  toggle: [recordId: string, checked: boolean];
  "toggle-scope": [scope: string, checked: boolean];
}>();

const query = ref("");
const page = ref(1);
const filteredItems = computed(() => {
  const normalized = query.value.trim().toLowerCase();
  if (!normalized) return props.items;
  return props.items.filter((item) => [
    item.name,
    item.employee_no,
    item.position,
    ...(Array.isArray(item.assignments) ? item.assignments : []),
  ].join(" ").toLowerCase().includes(normalized));
});
const pageCount = computed(() => Math.max(1, Math.ceil(filteredItems.value.length / PAGE_SIZE)));
const pageItems = computed(() => {
  const start = (page.value - 1) * PAGE_SIZE;
  return filteredItems.value.slice(start, start + PAGE_SIZE);
});
function isGrantable(item: LooseDict): boolean {
  return Boolean(item.selectable);
}

function hasAllSelectedScopes(item: LooseDict): boolean {
  if (!props.selectedScopes.size) return false;
  const authorized = new Set(
    (Array.isArray(item.authorized_scopes) ? item.authorized_scopes : [])
      .map((scope: unknown) => String(scope || "").trim())
      .filter(Boolean),
  );
  return Array.from(props.selectedScopes).every((scope) => authorized.has(scope));
}

const pageSelectableItems = computed(() => pageItems.value.filter(isGrantable));
const allPageSelected = computed(() => (
  Boolean(pageSelectableItems.value.length)
  && pageSelectableItems.value.every((item) => props.selectedIds.has(String(item.record_id || "")))
));
const selectableCount = computed(() => props.items.filter(isGrantable).length);

function togglePage(checked: boolean): void {
  for (const item of pageSelectableItems.value) {
    emit("toggle", String(item.record_id || ""), checked);
  }
}

watch([query, () => props.items.length], () => {
  page.value = 1;
});
watch(pageCount, (count) => {
  if (page.value > count) page.value = count;
});
</script>

<style scoped>
.directory-card {
  display: grid;
  gap: 12px;
  padding: 15px;
  border: 1px solid #d8e7f8;
  border-radius: 16px;
  background: #fff;
}

.directory-head,
.directory-toolbar,
.directory-footer,
.directory-head > div,
.person-cell,
.tag-list,
.status,
.directory-footer nav {
  display: flex;
  align-items: center;
}

.directory-head {
  justify-content: space-between;
}

.directory-head > div {
  gap: 9px;
}

.directory-head strong {
  color: #071a39;
  font-size: 16px;
}

.directory-head span,
.directory-footer,
.person-cell span,
.muted {
  color: #71839a;
  font-size: 12px;
}

.icon-button,
.directory-footer button {
  display: inline-grid;
  place-items: center;
  min-width: 34px;
  height: 34px;
  border: 1px solid #cfe0f4;
  border-radius: 9px;
  background: #f8fbff;
  color: #1d63d6;
  cursor: pointer;
}

.directory-toolbar {
  gap: 10px;
}

.grant-scope-picker {
  display: flex;
  align-items: center;
  gap: 12px;
  min-height: 42px;
  padding: 7px 10px;
  border: 1px solid #d9e8f9;
  border-radius: 10px;
  background: #f7fbff;
}

.grant-scope-picker > strong {
  flex: 0 0 auto;
  color: #244767;
  font-size: 12px;
}

.grant-scope-picker > div {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.grant-scope-picker label {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-height: 28px;
  padding: 0 9px;
  border: 1px solid #cfdef0;
  border-radius: 999px;
  background: #fff;
  color: #526c88;
  font-size: 12px;
  cursor: pointer;
}

.grant-scope-picker label.selected {
  border-color: #3783ee;
  background: #eaf3ff;
  color: #145fc8;
  font-weight: 750;
}

.directory-search {
  min-width: 240px;
  flex: 1;
  display: flex;
  align-items: center;
  gap: 8px;
  height: 36px;
  padding: 0 11px;
  border: 1px solid #cfe0f4;
  border-radius: 9px;
  color: #64809f;
}

.directory-search:focus-within {
  border-color: #4c91f2;
  box-shadow: 0 0 0 3px rgba(37, 117, 232, 0.1);
}

.directory-search input {
  width: 100%;
  min-width: 0;
  border: 0;
  outline: 0;
  background: transparent;
  color: #10233f;
  font: inherit;
  font-size: 13px;
}

.select-page {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #415b78;
  font-size: 13px;
  white-space: nowrap;
}

.grant-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  min-height: 36px;
  padding: 0 14px;
  border: 0;
  border-radius: 9px;
  background: #176ee4;
  color: #fff;
  font: inherit;
  font-size: 13px;
  font-weight: 800;
  cursor: pointer;
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.directory-table-wrap {
  overflow-x: auto;
  border: 1px solid #e0ebf7;
  border-radius: 12px;
}

.directory-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}

.directory-table th,
.directory-table td {
  padding: 9px 10px;
  border-bottom: 1px solid #edf3fa;
  text-align: left;
  vertical-align: middle;
}

.directory-table th {
  background: #f6f9fd;
  color: #5c718b;
  font-size: 12px;
}

.directory-table th:first-child,
.directory-table td:first-child { width: 38px; text-align: center; }
.directory-table th:nth-child(2) { width: 21%; }
.directory-table th:nth-child(3) { width: 15%; }
.directory-table th:nth-child(4) { width: 20%; }
.directory-table th:nth-child(5) { width: 18%; }
.directory-table th:nth-child(6) { width: 110px; }

.directory-table tr:last-child td { border-bottom: 0; }
.directory-table tr.selected td { background: #eef6ff; }
.directory-table tr.unavailable td { background: #fafbfd; }

.person-cell {
  min-width: 0;
  align-items: flex-start;
  flex-direction: column;
  gap: 2px;
}

.person-cell b {
  max-width: 100%;
  overflow: hidden;
  color: #10233f;
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tag-list {
  gap: 5px;
  flex-wrap: wrap;
}

.tag,
.status {
  min-height: 24px;
  padding: 0 8px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 750;
  white-space: nowrap;
}

.tag.building { background: #eaf3ff; color: #1764cc; }
.tag.specialty { background: #edf9f5; color: #15805f; }
.position-value { color: #405a77; font-size: 12px; }
.status { justify-content: center; gap: 4px; }
.status.ready { background: #eef6ff; color: #1764cc; }
.status.partial { background: #fff5df; color: #a35b00; }
.status.authorized { background: #eaf8f1; color: #16805f; }
.status.unavailable-status { background: #f2f4f7; color: #7d8998; }

.directory-message {
  padding: 18px;
  border-radius: 10px;
  background: #f7faff;
  color: #5d718a;
  text-align: center;
  font-size: 13px;
}

.directory-message.error { background: #fff2f2; color: #b33a3a; }

.directory-footer {
  justify-content: space-between;
}

.directory-footer nav { gap: 8px; }
.directory-footer button { width: auto; padding: 0 10px; font-size: 12px; }
.directory-footer b { color: #334d6b; font-size: 12px; }

.spinning { animation: directory-spin 900ms linear infinite; }
@keyframes directory-spin { to { transform: rotate(360deg); } }

@media (max-width: 900px) {
  .directory-toolbar { align-items: stretch; flex-direction: column; }
  .directory-search { min-width: 0; }
  .grant-button { width: 100%; }
  .directory-table { min-width: 760px; }
}
</style>
