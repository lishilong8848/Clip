<template>
  <MopTableFillToolbar
    v-if="activeSheet && !activeSheet.is_cover"
    :bulk-count="bulkCount"
    :filled-count="filledCount"
    @mark-all-normal="emit('mark-all-normal')"
  />
  <table v-if="activeSheet">
    <thead>
      <tr>
        <th class="corner-cell"></th>
        <th
          v-for="colIndex in columnIndexes"
          :key="`head:${colIndex}`"
          class="column-head"
        >
          {{ columnLabel(colIndex) }}
        </th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="(row, rowIndex) in renderedRows" :key="rowIndex">
        <th class="row-head">{{ rowIndex + 1 }}</th>
        <template
          v-for="cell in rowCells(row, rowIndex)"
          :key="cell.key"
        >
          <td
            v-if="!cell.hidden"
            :data-mop-cell-key="cell.key"
            :rowspan="cell.rowspan"
            :colspan="cell.colspan"
            :class="{
              merged: cell.rowspan > 1 || cell.colspan > 1,
              fillable: Boolean(cell.checkbox),
              'field-fillable': cell.fieldFillable,
              'raw-editable': cell.rawEditable,
              'cell-modified': cell.modified,
              'selected-cell': cell.selected,
              'active-cell': cell.active,
              'signature-cell': Boolean(cell.signatureRole),
              'signature-cell-empty': Boolean(cell.signatureRole) && !cell.signatures.length,
              normal: cell.checkboxLabel.includes('正常'),
              abnormal: cell.checkboxLabel.includes('异常')
            }"
            @mousedown.left.stop.prevent="emit('cell-mousedown', rowIndex, cell.colIndex, $event)"
            @mouseenter="emit('cell-enter', rowIndex, cell.colIndex, $event)"
          >
            <div
              v-if="cell.signatureRole"
              class="sheet-cell-signatures"
              :style="signatureCellStyle(rowIndex)"
            >
              <img
                v-for="person in cell.signatures.slice(0, 3)"
                :key="`${rowIndex}:${cell.colIndex}:${person.record_id}`"
                :src="person.signature_preview_url"
                :alt="person.name || '签名'"
                :style="signatureImageStyle(rowIndex)"
                loading="lazy"
                decoding="async"
              />
              <em
                v-if="cell.signatures.length > 3"
                class="signature-more-count"
                :style="signatureMoreStyle(rowIndex)"
              >
                +{{ cell.signatures.length - 3 }}
              </em>
              <span v-if="!cell.signatures.length">
                点击添加{{ cell.signatureRole === 'auditor' ? '审核人' : '实施人' }}签名
              </span>
            </div>
            <template v-else>{{ cell.value }}</template>
          </td>
        </template>
      </tr>
      <tr
        v-if="hasMoreRows"
        ref="loadMoreRowRef"
        class="mop-load-more-row"
      >
        <td :colspan="columnIndexes.length + 1">
          <button type="button" @click="loadNextRows">继续加载表格</button>
        </td>
      </tr>
    </tbody>
  </table>
  <MopCellPopover
    v-if="!signatureManagerOpen && activeMopCellPosition"
    :date-time="dateTime"
    :mode="popoverMode"
    :style="overlayStyle"
    :label="popoverLabel"
    :checkbox-options="checkboxOptions"
    :checkbox-value="checkboxValue"
    :raw-value="rawValue"
    :selected-count="selectedCount"
    @select-checkbox="emit('select-checkbox', $event)"
    @fill-date="emit('fill-date')"
    @fill-completion="emit('fill-completion', $event)"
    @update:date-time="emit('update:dateTime', $event)"
    @update:raw-value="emit('update:rawValue', $event)"
    @copy="emit('copy')"
    @paste="emit('paste')"
    @restore="emit('restore')"
    @cancel="emit('cancel')"
  />
  <div v-if="!activeSheet" class="empty-box">该附件没有可显示的 Sheet。</div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import MopCellPopover, { type MopCellPopoverMode } from "./MopCellPopover.vue";
import MopTableFillToolbar from "./MopTableFillToolbar.vue";

type Dict = Record<string, any>;

const props = defineProps<{
  activeSheet: Dict | null;
  columnIndexes: number[];
  bulkCount: number;
  filledCount: number;
  signatureManagerOpen: boolean;
  activeMopCellPosition: { row: number; col: number } | null;
  activeMopCellKey: string;
  dateTime: string;
  popoverMode: MopCellPopoverMode;
  overlayStyle: Record<string, string>;
  popoverLabel: string;
  checkboxOptions: Array<{ label: string; value: string }>;
  checkboxValue: string;
  rawValue: string;
  selectedCount: number;
  columnLabel: (colIndex: number) => string;
  cellMergeSpan: (rowIndex: number, colIndex: number) => Dict;
  mopCellKey: (rowIndex: number, colIndex: number) => string;
  checkboxCellAt: (rowIndex: number, colIndex: number) => Dict | null;
  maintenanceFieldAt: (rowIndex: number, colIndex: number) => Dict | null;
  editableCellAt: (rowIndex: number, colIndex: number) => boolean;
  mopCellHasOverride: (rowIndex: number, colIndex: number) => boolean;
  isMopCellSelected: (rowIndex: number, colIndex: number) => boolean;
  signatureRoleAtCell: (rowIndex: number, colIndex: number) => string;
  cellSignatures: (rowIndex: number, colIndex: number) => Dict[];
  signatureCellStyle: (rowIndex: number) => Record<string, string>;
  signatureImageStyle: (rowIndex: number) => Record<string, string>;
  signatureMoreStyle: (rowIndex: number) => Record<string, string>;
  checkboxStateLabel: (cell: Dict) => string;
  cellOverrideValue: (rowIndex: number, colIndex: number) => string;
}>();

const initialRenderRows = 80;
const renderStepRows = 120;
const renderedRowLimit = ref(initialRenderRows);
const loadMoreRowRef = ref<HTMLElement | null>(null);
let loadMoreObserver: IntersectionObserver | null = null;
let loadMoreFrame = 0;

const activeRows = computed<unknown[][]>(() => (
  Array.isArray(props.activeSheet?.rows) ? props.activeSheet.rows : []
));
const renderedRows = computed(() => activeRows.value.slice(0, renderedRowLimit.value));
const hasMoreRows = computed(() => renderedRowLimit.value < activeRows.value.length);

function cellView(row: unknown[], rowIndex: number, colIndex: number) {
  const merge = props.cellMergeSpan(rowIndex, colIndex) || {};
  const checkbox = props.checkboxCellAt(rowIndex, colIndex);
  const signatureRole = props.signatureRoleAtCell(rowIndex, colIndex);
  const signatures = signatureRole ? props.cellSignatures(rowIndex, colIndex) : [];
  const overrideValue = props.cellOverrideValue(rowIndex, colIndex);
  const modified = props.mopCellHasOverride(rowIndex, colIndex);
  const key = props.mopCellKey(rowIndex, colIndex);
  return {
    colIndex,
    key,
    hidden: Boolean(merge.hidden),
    rowspan: Number(merge.rowspan || 1),
    colspan: Number(merge.colspan || 1),
    checkbox,
    checkboxLabel: checkbox ? props.checkboxStateLabel(checkbox) : "",
    fieldFillable: Boolean(props.maintenanceFieldAt(rowIndex, colIndex)),
    rawEditable: Boolean(props.editableCellAt(rowIndex, colIndex)),
    modified,
    selected: props.isMopCellSelected(rowIndex, colIndex),
    active: props.activeMopCellKey === key,
    signatureRole,
    signatures,
    value: modified ? overrideValue : (row[colIndex] ?? ""),
  };
}

function rowCells(row: unknown[], rowIndex: number) {
  return props.columnIndexes.map((colIndex) => cellView(row, rowIndex, colIndex));
}

function cancelLoadMoreFrame(): void {
  if (loadMoreFrame) {
    window.cancelAnimationFrame(loadMoreFrame);
    loadMoreFrame = 0;
  }
}

function loadNextRows(): void {
  if (!hasMoreRows.value || loadMoreFrame) return;
  loadMoreFrame = window.requestAnimationFrame(() => {
    loadMoreFrame = 0;
    renderedRowLimit.value = Math.min(
      activeRows.value.length,
      renderedRowLimit.value + renderStepRows,
    );
    void nextTick(observeLoadMoreRow);
  });
}

function observeLoadMoreRow(): void {
  loadMoreObserver?.disconnect();
  if (!hasMoreRows.value || !loadMoreRowRef.value || !loadMoreObserver) return;
  loadMoreObserver.observe(loadMoreRowRef.value);
}

watch(
  () => `${props.activeSheet?.name || ""}:${activeRows.value.length}`,
  () => {
    renderedRowLimit.value = Math.min(initialRenderRows, activeRows.value.length || initialRenderRows);
    void nextTick(observeLoadMoreRow);
  },
  { immediate: true },
);

onMounted(() => {
  if (typeof IntersectionObserver === "undefined") return;
  loadMoreObserver = new IntersectionObserver(
    (entries) => {
      if (entries.some((entry) => entry.isIntersecting)) loadNextRows();
    },
    { rootMargin: "900px 0px" },
  );
  observeLoadMoreRow();
});

onBeforeUnmount(() => {
  cancelLoadMoreFrame();
  loadMoreObserver?.disconnect();
  loadMoreObserver = null;
});

const emit = defineEmits<{
  "mark-all-normal": [];
  "cell-mousedown": [rowIndex: number, colIndex: number, event: MouseEvent];
  "cell-enter": [rowIndex: number, colIndex: number, event: MouseEvent];
  "select-checkbox": [value: string];
  "update:dateTime": [value: string];
  "update:rawValue": [value: string];
  "fill-date": [];
  "fill-completion": [value: string];
  copy: [];
  paste: [];
  restore: [];
  cancel: [];
}>();
</script>

<style scoped>
table {
  border-collapse: collapse;
  min-width: 100%;
  background: #ffffff;
  user-select: none;
  -webkit-user-select: none;
}

th,
td {
  box-sizing: border-box;
  min-width: 96px;
  max-width: 360px;
  border: 1px solid #d8e5f7;
  padding: 6px 8px;
  color: #0f172a;
  background: #ffffff;
  font-size: 12px;
  line-height: 1.45;
  vertical-align: middle;
  white-space: pre-wrap;
}

th {
  position: sticky;
  z-index: 2;
  background: linear-gradient(180deg, #f8fbff, #edf5ff);
  color: #3156c9;
  font-weight: 900;
}

.column-head {
  top: 0;
  box-shadow: 0 1px 0 rgba(216, 229, 247, 0.95);
}

.corner-cell {
  top: 0;
  left: 0;
  z-index: 5;
  min-width: 44px;
  width: 44px;
}

.row-head {
  left: 0;
  z-index: 4;
  min-width: 44px;
  width: 44px;
  color: #64748b;
  text-align: center;
  background: #f8fbff;
  box-shadow: 1px 0 0 rgba(216, 229, 247, 0.95);
}

tbody tr:hover td:not(.active-cell) {
  background-image: linear-gradient(0deg, rgba(30, 99, 255, 0.035), rgba(30, 99, 255, 0.035));
}

td.merged {
  background: #ffffff;
}

td.fillable,
td.field-fillable,
td.raw-editable {
  cursor: pointer;
  transition: background 0.12s ease, box-shadow 0.12s ease, outline-color 0.12s ease;
}

td.fillable,
td.field-fillable {
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.68), rgba(255, 255, 255, 0.8)),
    #f8fbff;
  box-shadow: inset 0 0 0 1px rgba(147, 197, 253, 0.16);
}

td.raw-editable {
  background:
    linear-gradient(135deg, rgba(255, 251, 235, 0.74), rgba(255, 255, 255, 0.84)),
    #fffdf7;
  box-shadow: inset 0 0 0 1px rgba(251, 191, 36, 0.12);
}

td.fillable.normal {
  background: #ecfdf5;
}

td.fillable.abnormal {
  background: #fff1f2;
}

td.cell-modified {
  background: linear-gradient(135deg, #ecfeff, #eff6ff);
  box-shadow:
    inset 0 0 0 1px rgba(14, 165, 233, 0.28),
    inset 3px 0 0 rgba(14, 165, 233, 0.5);
}

td.signature-cell {
  position: relative;
  min-width: 136px;
}

td.signature-cell-empty {
  color: #c2410c;
  background: #fff7ed;
}

td.active-cell {
  position: relative;
  z-index: 3;
  outline: 2px solid rgba(30, 99, 255, 0.5);
  outline-offset: -2px;
  box-shadow:
    inset 0 0 0 1px rgba(30, 99, 255, 0.4),
    0 0 0 2px rgba(30, 99, 255, 0.12),
    0 8px 18px rgba(30, 99, 255, 0.1);
}

td.selected-cell {
  position: relative;
  background: #eaf3ff;
  outline: 1px solid rgba(30, 99, 255, 0.38);
  outline-offset: -1px;
}

td.selected-cell::after {
  content: "";
  position: absolute;
  inset: 2px;
  pointer-events: none;
  border-radius: 3px;
  background: rgba(30, 99, 255, 0.045);
}

td.selected-cell.cell-modified {
  background: linear-gradient(135deg, #dff7ff, #eaf2ff);
}

td.selected-cell.active-cell {
  outline: 2px solid rgba(30, 99, 255, 0.5);
}

.sheet-cell-signatures {
  display: flex;
  align-items: center;
  gap: 6px;
  min-height: 26px;
  overflow: visible;
}

.sheet-cell-signatures img {
  display: block;
  flex: 0 1 auto;
  object-fit: contain;
}

.sheet-cell-signatures span {
  color: #c2410c;
  font-size: 12px;
  font-weight: 850;
}

.signature-more-count {
  flex: 0 0 auto;
  display: inline-grid;
  place-items: center;
  min-width: 24px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  padding: 0 6px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 11px;
  font-style: normal;
  font-weight: 950;
  line-height: 1;
}

.empty-box {
  border: 1px dashed #cfe0ff;
  border-radius: 18px;
  padding: 22px;
  color: #64748b;
  background: rgba(255, 255, 255, 0.78);
  font-size: 13px;
  font-weight: 850;
  text-align: center;
}

.mop-load-more-row td {
  height: 44px;
  padding: 6px;
  text-align: center;
  background: #f8fbff;
}

.mop-load-more-row button {
  min-height: 30px;
  border: 1px solid #bfd4f4;
  border-radius: 8px;
  padding: 0 14px;
  background: #ffffff;
  color: #1d5bbf;
  font: inherit;
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
}
</style>
