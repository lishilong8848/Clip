<template>
  <Teleport to="body">
    <div
      v-if="open"
      class="record-picker-backdrop"
      role="presentation"
      @mousedown.self="emit('close')"
    >
      <section
        ref="dialogRef"
        class="record-picker-dialog"
        role="dialog"
        aria-modal="true"
        :aria-labelledby="titleId"
      >
        <header class="record-picker-head">
          <div>
            <span class="picker-kicker">{{ kicker }}</span>
            <h2 :id="titleId">{{ title }}</h2>
          </div>
          <button
            type="button"
            class="picker-close"
            aria-label="关闭"
            title="关闭"
            @click="emit('close')"
          >
            <X :size="18" aria-hidden="true" />
          </button>
        </header>

        <form class="record-picker-toolbar" @submit.prevent="runSearchNow">
          <label>
            <Search :size="16" class="search-icon" aria-hidden="true" />
            <input
              ref="searchInput"
              v-model.trim="queryModel"
              type="search"
              :placeholder="searchPlaceholder"
              autofocus
            />
          </label>
          <button
            type="submit"
            class="picker-search icon-only"
            :disabled="loading"
            :aria-label="loading ? '正在刷新记录' : '刷新记录'"
            :title="loading ? '读取中' : '刷新记录'"
          >
            <RefreshCw :size="17" :class="{ spinning: loading }" aria-hidden="true" />
          </button>
          <div
            v-if="$slots['toolbar-actions']"
            class="record-picker-toolbar-actions"
          >
            <slot name="toolbar-actions" />
          </div>
        </form>

        <div
          class="record-picker-status"
          :class="[`is-${statusTone}`, { 'is-empty': !statusMessage }]"
          :role="statusTone === 'error' ? 'alert' : 'status'"
          :aria-hidden="statusMessage ? undefined : 'true'"
          aria-live="polite"
        >
          {{ statusMessage }}
        </div>

        <div class="record-picker-table-wrap">
          <table class="record-picker-table">
            <thead>
              <tr>
                <th class="select-column" scope="col">
                  <span class="sr-only">选择</span>
                </th>
                <th
                  v-for="column in columns"
                  :key="column.key"
                  scope="col"
                  :class="{ 'wrap-column': column.wrap }"
                  :style="column.width ? { width: column.width, minWidth: column.width } : undefined"
                >
                  {{ column.label }}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="loading && !records.length">
                <td :colspan="columns.length + 1" class="picker-empty">正在读取多维记录...</td>
              </tr>
              <tr v-else-if="!records.length">
                <td :colspan="columns.length + 1" class="picker-empty">没有找到可选择的记录</td>
              </tr>
              <tr
                v-for="record in pagedRecords"
                v-else
                :key="recordId(record)"
                :class="{ selected: isSelected(record), unavailable: !recordSelectable(record) }"
                :aria-selected="isSelected(record)"
                :aria-disabled="!recordSelectable(record)"
                :tabindex="recordSelectable(record) ? 0 : -1"
                @click="toggle(record)"
                @dblclick="confirmSingle(record)"
                @keydown.enter.prevent="toggle(record)"
                @keydown.space.prevent="toggle(record)"
              >
                <td class="select-column">
                  <input
                    :type="multiple ? 'checkbox' : 'radio'"
                    :name="multiple ? undefined : titleId"
                    :checked="isSelected(record)"
                    :disabled="!recordSelectable(record)"
                    :aria-label="`选择${cellText(record, columns[0]?.key || 'title')}`"
                    @click.stop="toggle(record)"
                  />
                </td>
                <td
                  v-for="column in columns"
                  :key="column.key"
                  :class="{ 'wrap-column': column.wrap }"
                  :title="cellText(record, column.key)"
                >
                  <span>{{ cellText(record, column.key) || "-" }}</span>
                  <small
                    v-if="column.key === 'title' && record.recommended"
                    class="recommended-mark"
                  >
                    推荐 {{ record.score || "" }}
                  </small>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <footer class="record-picker-footer">
          <div class="picker-result-summary">
            <span>已选：<b>{{ draftSelection.length }}</b> 条记录</span>
            <small v-if="resultNote">{{ resultNote }}</small>
            <button
              v-if="hasMore"
              type="button"
              class="picker-load-more"
              :disabled="loading"
              @click="emit('load-more')"
            >
              {{ loading ? "加载中" : "加载更多" }}
            </button>
          </div>
          <nav v-if="pageCount > 1" class="picker-pager" aria-label="候选记录分页">
            <button type="button" :disabled="page <= 1" @click="page -= 1">上一页</button>
            <span>{{ page }} / {{ pageCount }}</span>
            <button type="button" :disabled="page >= pageCount" @click="page += 1">下一页</button>
          </nav>
          <div>
            <button type="button" class="picker-cancel" @click="emit('close')">取消</button>
            <button
              type="button"
              class="picker-confirm"
              :disabled="!allowEmpty && !draftSelection.length"
              @click="emit('confirm', draftSelection.slice())"
            >
              {{ allowEmpty && !draftSelection.length ? "清空关联" : "确认" }}
            </button>
          </div>
        </footer>
      </section>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { RefreshCw, Search, X } from "lucide-vue-next";
import { repairFieldValueToText } from "../repairManagementUtils";
import type { LooseDict } from "../types";

type RecordPickerColumn = {
  key: string;
  label: string;
  width?: string;
  wrap?: boolean;
};

const props = withDefaults(defineProps<{
  open: boolean;
  title: string;
  kicker?: string;
  records: LooseDict[];
  columns: RecordPickerColumn[];
  selectedIds?: string[];
  multiple?: boolean;
  allowEmpty?: boolean;
  loading?: boolean;
  hasMore?: boolean;
  resultNote?: string;
  statusMessage?: string;
  statusTone?: "info" | "success" | "warning" | "error";
  query?: string;
  searchPlaceholder?: string;
}>(), {
  kicker: "多维记录选择",
  selectedIds: () => [],
  multiple: true,
  allowEmpty: false,
  loading: false,
  hasMore: false,
  resultNote: "",
  statusMessage: "",
  statusTone: "info",
  query: "",
  searchPlaceholder: "输入关键词搜索",
});

const emit = defineEmits<{
  close: [];
  confirm: [recordIds: string[]];
  search: [];
  "load-more": [];
  "update:query": [value: string];
}>();

const titleId = `record-picker-${Math.random().toString(36).slice(2, 9)}`;
const draftSelection = ref<string[]>([]);
const page = ref(1);
const dialogRef = ref<HTMLElement | null>(null);
const searchInput = ref<HTMLInputElement | null>(null);
let previousBodyOverflow = "";
let searchTimer: ReturnType<typeof setTimeout> | undefined;
let returnFocusElement: HTMLElement | null = null;
const PAGE_SIZE = 30;

const queryModel = computed({
  get: () => props.query,
  set: (value: string) => emit("update:query", value),
});
const pageCount = computed(() => Math.max(1, Math.ceil(props.records.length / PAGE_SIZE)));
const selectedIdsKey = computed(() => props.selectedIds
  .map((item) => String(item || "").trim())
  .filter(Boolean)
  .join("\u001f"));
const pagedRecords = computed(() => {
  const start = (page.value - 1) * PAGE_SIZE;
  return props.records.slice(start, start + PAGE_SIZE);
});

function recordId(record: LooseDict): string {
  return String(record.record_id || record.source_record_id || "").trim();
}

function cellText(record: LooseDict, key: string): string {
  return repairFieldValueToText(record[key]).trim();
}

function isSelected(record: LooseDict): boolean {
  return draftSelection.value.includes(recordId(record));
}

function recordSelectable(record: LooseDict): boolean {
  return record.selectable !== false;
}

function toggle(record: LooseDict): void {
  if (!recordSelectable(record)) return;
  const id = recordId(record);
  if (!id) return;
  if (!props.multiple) {
    draftSelection.value = [id];
    return;
  }
  draftSelection.value = draftSelection.value.includes(id)
    ? draftSelection.value.filter((item) => item !== id)
    : [...draftSelection.value, id];
}

function confirmSingle(record: LooseDict): void {
  if (props.multiple || !recordSelectable(record)) return;
  const id = recordId(record);
  if (id) emit("confirm", [id]);
}

function runSearchNow(): void {
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = undefined;
  page.value = 1;
  emit("search");
}

function handleKeydown(event: KeyboardEvent): void {
  if (!props.open) return;
  if (event.key === "Escape") {
    emit("close");
    return;
  }
  if (event.key !== "Tab" || !dialogRef.value) return;
  const focusable = Array.from(dialogRef.value.querySelectorAll<HTMLElement>(
    'button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])',
  )).filter((element) => element.offsetParent !== null);
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

function syncSelectionFromProps(): void {
  const selected = props.selectedIds.map((item) => String(item || "").trim()).filter(Boolean);
  draftSelection.value = props.multiple ? selected : selected.slice(0, 1);
}

watch(selectedIdsKey, () => {
  if (props.open) syncSelectionFromProps();
});

watch(
  () => props.open,
  (open) => {
    if (open) {
      syncSelectionFromProps();
      page.value = 1;
      returnFocusElement = document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
      previousBodyOverflow = document.body.style.overflow;
      document.body.style.overflow = "hidden";
      window.addEventListener("keydown", handleKeydown);
      void nextTick(() => searchInput.value?.focus());
    } else {
      document.body.style.overflow = previousBodyOverflow;
      window.removeEventListener("keydown", handleKeydown);
      const returnFocus = returnFocusElement;
      returnFocusElement = null;
      void nextTick(() => {
        if (returnFocus?.isConnected) returnFocus.focus();
      });
    }
  },
  { immediate: true },
);

watch(
  () => props.query,
  () => {
    if (!props.open) return;
    page.value = 1;
    if (searchTimer) clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      if (props.open) emit("search");
    }, 300);
  },
);

watch(
  () => props.records.length,
  () => {
    page.value = Math.min(page.value, pageCount.value);
  },
);

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer);
  returnFocusElement = null;
  document.body.style.overflow = previousBodyOverflow;
  window.removeEventListener("keydown", handleKeydown);
});
</script>

<style scoped>
.record-picker-backdrop {
  position: fixed;
  inset: 0;
  z-index: 2400;
  display: grid;
  align-items: start;
  padding: 14px;
  background: rgba(7, 24, 55, 0.36);
  backdrop-filter: blur(4px);
}

.record-picker-dialog {
  width: min(1880px, calc(100vw - 28px));
  height: min(900px, calc(100vh - 28px));
  margin: 0 auto;
  display: grid;
  grid-template-rows: auto auto auto minmax(0, 1fr) auto;
  overflow: hidden;
  border: 1px solid #cdddf1;
  border-radius: 16px;
  background: #fff;
  box-shadow: 0 30px 90px rgba(5, 33, 78, 0.28);
}

.record-picker-head,
.record-picker-toolbar,
.record-picker-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 20px;
}

.record-picker-head {
  border-bottom: 1px solid #e1e9f4;
}

.record-picker-head h2 {
  margin: 3px 0 0;
  color: #0a1c39;
  font-size: 20px;
}

.picker-kicker {
  color: #55708f;
  font-size: 12px;
  font-weight: 800;
}

.picker-close {
  width: 36px;
  height: 36px;
  border: 1px solid #d8e3f0;
  border-radius: 50%;
  background: #f7faff;
  color: #35526f;
  font-size: 24px;
  line-height: 1;
  cursor: pointer;
}

.record-picker-toolbar {
  justify-content: flex-start;
  padding-block: 10px;
  background: #f8fbff;
  border-bottom: 1px solid #e4ecf6;
}

.record-picker-toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-left: auto;
}

.record-picker-status {
  padding: 9px 20px;
  border-bottom: 1px solid transparent;
  font-size: 13px;
  font-weight: 700;
  line-height: 1.45;
}

.record-picker-status.is-empty {
  min-height: 0;
  overflow: hidden;
  border: 0;
  padding: 0;
}

.record-picker-status.is-info {
  border-color: #cfe0f5;
  background: #eef6ff;
  color: #245c96;
}

.record-picker-status.is-success {
  border-color: #bfe7d5;
  background: #edf9f4;
  color: #087555;
}

.record-picker-status.is-warning {
  border-color: #f0d6a8;
  background: #fff8eb;
  color: #8a5a16;
}

.record-picker-status.is-error {
  border-color: #efc0c7;
  background: #fff1f3;
  color: #a22a3b;
}

.record-picker-toolbar label {
  position: relative;
  flex: 1;
}

.record-picker-toolbar input {
  width: 100%;
  height: 38px;
  padding: 0 14px 0 38px;
  border: 1px solid #cddaea;
  border-radius: 10px;
  background: #fff;
  color: #122846;
  font: inherit;
}

.record-picker-toolbar input:focus {
  border-color: #2775e8;
  outline: 3px solid rgba(39, 117, 232, 0.16);
}

.search-icon {
  position: absolute;
  left: 14px;
  top: 50%;
  transform: translateY(-55%);
  color: #748aa3;
  pointer-events: none;
}

.picker-search,
.picker-load-more,
.picker-confirm,
.picker-cancel {
  min-height: 38px;
  padding: 0 20px;
  border-radius: 10px;
  font-weight: 800;
  cursor: pointer;
}

.picker-search.icon-only {
  width: 38px;
  flex: 0 0 38px;
  padding: 0;
}

.picker-search,
.picker-confirm {
  border: 1px solid #1265dc;
  background: #1265dc;
  color: #fff;
}

.picker-load-more {
  border: 1px solid #b9cce4;
  background: #f5f9ff;
  color: #245b96;
}

.picker-load-more:hover:not(:disabled) {
  border-color: #6ba2e9;
  background: #eaf3ff;
}

.picker-cancel {
  border: 1px solid #d4deeb;
  background: #fff;
  color: #38516e;
}

.picker-search:disabled,
.picker-load-more:disabled,
.picker-confirm:disabled {
  opacity: 0.48;
  cursor: not-allowed;
}

.picker-result-summary {
  display: flex;
  align-items: center;
  gap: 10px;
}

.picker-result-summary small {
  max-width: 360px;
  overflow: hidden;
  color: #8a5a16;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-picker-table-wrap {
  min-height: 0;
  overflow: auto;
  overscroll-behavior: contain;
}

.record-picker-table {
  width: 100%;
  min-width: 1180px;
  border-collapse: separate;
  border-spacing: 0;
  table-layout: fixed;
  color: #253b57;
  font-size: 13px;
}

.record-picker-table th,
.record-picker-table td {
  height: 42px;
  padding: 8px 12px;
  border-right: 1px solid #e2e9f2;
  border-bottom: 1px solid #e2e9f2;
  text-align: left;
  vertical-align: middle;
}

.record-picker-table th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #f3f7fc;
  color: #405a78;
  font-weight: 850;
}

.record-picker-table tbody tr {
  cursor: pointer;
  transition: background 120ms ease;
}

.record-picker-table tbody tr:hover {
  background: #f4f9ff;
}

.record-picker-table tbody tr.selected {
  background: #e9f3ff;
  box-shadow: inset 3px 0 0 #176de0;
}

.record-picker-table tbody tr.unavailable {
  background: #f6f8fb;
  color: #738398;
  cursor: not-allowed;
}

.record-picker-table tbody tr.unavailable:hover {
  background: #f6f8fb;
}

.record-picker-table tbody tr.unavailable td {
  opacity: 0.78;
}

.record-picker-table td span {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-picker-table td.wrap-column {
  height: auto;
  min-height: 42px;
  vertical-align: top;
}

.record-picker-table td.wrap-column span {
  overflow: visible;
  text-overflow: clip;
  white-space: normal;
  overflow-wrap: anywhere;
  line-height: 1.45;
}

.select-column {
  width: 48px;
  min-width: 48px;
  text-align: center !important;
}

.select-column input {
  width: 17px;
  height: 17px;
  accent-color: #176de0;
}

.recommended-mark {
  display: inline-flex;
  margin-top: 3px;
  padding: 1px 7px;
  border-radius: 999px;
  background: #e4f7ef;
  color: #087c58;
  font-size: 11px;
  font-weight: 800;
}

.picker-empty {
  height: 180px !important;
  color: #7488a1;
  text-align: center !important;
}

.record-picker-footer {
  border-top: 1px solid #dce6f2;
  background: #fff;
}

.record-picker-footer > span {
  color: #627690;
}

.record-picker-footer > span b {
  color: #1265dc;
  font-size: 17px;
}

.record-picker-footer > div {
  display: flex;
  gap: 10px;
}

.picker-pager {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.picker-pager button {
  min-height: 32px;
  border: 1px solid #d4deeb;
  border-radius: 8px;
  padding: 0 10px;
  background: #f7faff;
  color: #24527e;
  font: inherit;
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;
}

.picker-pager button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
}

.picker-pager span {
  color: #627690;
  font-size: 12px;
  font-weight: 700;
  white-space: nowrap;
}

.spinning {
  animation: record-picker-spin 0.9s linear infinite;
}

@keyframes record-picker-spin {
  to { transform: rotate(360deg); }
}

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

@media (max-width: 900px) {
  .record-picker-backdrop {
    padding: 0;
  }

  .record-picker-dialog {
    width: 100vw;
    height: 100vh;
    border: 0;
    border-radius: 0;
  }

  .record-picker-footer {
    flex-wrap: wrap;
  }

  .picker-pager {
    order: 3;
    width: 100%;
  }
}

@media (prefers-reduced-motion: reduce) {
  .spinning {
    animation: none;
  }
}
</style>
