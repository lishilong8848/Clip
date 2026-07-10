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
            ×
          </button>
        </header>

        <form class="record-picker-toolbar" @submit.prevent="emit('search')">
          <label>
            <span class="search-icon" aria-hidden="true"></span>
            <input
              ref="searchInput"
              v-model.trim="queryModel"
              type="search"
              :placeholder="searchPlaceholder"
              autofocus
            />
          </label>
          <button type="submit" class="picker-search" :disabled="loading">
            {{ loading ? "读取中" : "搜索" }}
          </button>
        </form>

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
                  :style="column.width ? { minWidth: column.width } : undefined"
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
                v-for="record in records"
                v-else
                :key="recordId(record)"
                :class="{ selected: isSelected(record) }"
                :aria-selected="isSelected(record)"
                tabindex="0"
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
                    :aria-label="`选择${cellText(record, columns[0]?.key || 'title')}`"
                    @click.stop="toggle(record)"
                  />
                </td>
                <td
                  v-for="column in columns"
                  :key="column.key"
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
          <span>已选：<b>{{ draftSelection.length }}</b> 条记录</span>
          <div>
            <button type="button" class="picker-cancel" @click="emit('close')">取消</button>
            <button
              type="button"
              class="picker-confirm"
              :disabled="!draftSelection.length"
              @click="emit('confirm', draftSelection.slice())"
            >
              确认
            </button>
          </div>
        </footer>
      </section>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import type { LooseDict } from "../types";

type RecordPickerColumn = {
  key: string;
  label: string;
  width?: string;
};

const props = withDefaults(defineProps<{
  open: boolean;
  title: string;
  kicker?: string;
  records: LooseDict[];
  columns: RecordPickerColumn[];
  selectedIds?: string[];
  multiple?: boolean;
  loading?: boolean;
  query?: string;
  searchPlaceholder?: string;
}>(), {
  kicker: "多维记录选择",
  selectedIds: () => [],
  multiple: true,
  loading: false,
  query: "",
  searchPlaceholder: "输入关键词搜索",
});

const emit = defineEmits<{
  close: [];
  confirm: [recordIds: string[]];
  search: [];
  "update:query": [value: string];
}>();

const titleId = `record-picker-${Math.random().toString(36).slice(2, 9)}`;
const draftSelection = ref<string[]>([]);
const dialogRef = ref<HTMLElement | null>(null);
const searchInput = ref<HTMLInputElement | null>(null);
let previousBodyOverflow = "";
let searchTimer: ReturnType<typeof setTimeout> | undefined;

const queryModel = computed({
  get: () => props.query,
  set: (value: string) => emit("update:query", value),
});

function recordId(record: LooseDict): string {
  return String(record.record_id || record.source_record_id || "").trim();
}

function cellText(record: LooseDict, key: string): string {
  const value = record[key];
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map((item) => String(item || "")).filter(Boolean).join("、");
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value).trim();
}

function isSelected(record: LooseDict): boolean {
  return draftSelection.value.includes(recordId(record));
}

function toggle(record: LooseDict): void {
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
  if (props.multiple) return;
  const id = recordId(record);
  if (id) emit("confirm", [id]);
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

watch(
  () => [props.open, props.selectedIds] as const,
  ([open]) => {
    if (!open) return;
    const selected = props.selectedIds.map((item) => String(item || "").trim()).filter(Boolean);
    draftSelection.value = props.multiple ? selected : selected.slice(0, 1);
  },
  { immediate: true, deep: true },
);

watch(
  () => props.open,
  (open) => {
    if (open) {
      previousBodyOverflow = document.body.style.overflow;
      document.body.style.overflow = "hidden";
      window.addEventListener("keydown", handleKeydown);
      void nextTick(() => searchInput.value?.focus());
    } else {
      document.body.style.overflow = previousBodyOverflow;
      window.removeEventListener("keydown", handleKeydown);
    }
  },
  { immediate: true },
);

watch(
  () => props.query,
  () => {
    if (!props.open) return;
    if (searchTimer) clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      if (props.open) emit("search");
    }, 300);
  },
);

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer);
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
  grid-template-rows: auto auto minmax(0, 1fr) auto;
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
  width: 13px;
  height: 13px;
  transform: translateY(-55%);
  border: 2px solid #748aa3;
  border-radius: 50%;
}

.search-icon::after {
  content: "";
  position: absolute;
  right: -5px;
  bottom: -3px;
  width: 6px;
  height: 2px;
  transform: rotate(45deg);
  border-radius: 2px;
  background: #748aa3;
}

.picker-search,
.picker-confirm,
.picker-cancel {
  min-height: 38px;
  padding: 0 20px;
  border-radius: 10px;
  font-weight: 800;
  cursor: pointer;
}

.picker-search,
.picker-confirm {
  border: 1px solid #1265dc;
  background: #1265dc;
  color: #fff;
}

.picker-cancel {
  border: 1px solid #d4deeb;
  background: #fff;
  color: #38516e;
}

.picker-search:disabled,
.picker-confirm:disabled {
  opacity: 0.48;
  cursor: not-allowed;
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

.record-picker-table td span {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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
}
</style>
