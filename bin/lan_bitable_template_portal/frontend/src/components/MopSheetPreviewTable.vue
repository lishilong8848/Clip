<template>
  <div v-if="mobileLayout && activeSheet && mobileStep === 'checks'" class="mobile-sheet-mode">
    <strong>{{ showOriginal ? '原表' : '快捷填写' }}</strong>
    <button type="button" data-v2-navigation @click="showOriginal = !showOriginal; emit('cancel')">
      <Table2 v-if="!showOriginal" :size="17" /><PenLine v-else :size="17" />{{ showOriginal ? '返回填写' : '查看原表' }}
    </button>
  </div>
  <MopMobileFillForm
    v-if="mobileLayout && !showOriginal && activeSheet"
    :sheet="activeSheet"
    :step="mobileStep"
    :bulk-count="bulkCount"
    :cell-value="displayValue"
    :editable-cell-at="editableCellAt"
    :cell-merge-span="cellMergeSpan"
    :checkbox-state-label="checkboxStateLabel"
    :maintenance-field-at="maintenanceFieldAt"
    @mark-all-normal="emit('mark-all-normal')"
    @edit-raw="(row, col, value) => emit('edit-raw', row, col, value)"
    @edit-checkbox="(row, col, value) => emit('edit-checkbox', row, col, value)"
    @edit-field="(field, value) => emit('edit-field', field, value)"
  />
  <MopTableFillToolbar
    v-if="activeSheet && !activeSheet.is_cover && (!mobileLayout || showOriginal)"
    :bulk-count="bulkCount"
    :filled-count="filledCount"
    @mark-all-normal="emit('mark-all-normal')"
  />
  <table v-if="activeSheet && (!mobileLayout || showOriginal)" ref="tableRef" :class="{ 'mobile-sheet': mobileLayout }">
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
            :tabindex="mobileLayout && cell.canEdit ? 0 : undefined"
            @mousedown.left="startCell(rowIndex, cell.colIndex, $event)"
            @click="mobileLayout && emit('cell-activate', rowIndex, cell.colIndex)"
            @keydown.enter.stop.prevent="mobileLayout && emit('cell-activate', rowIndex, cell.colIndex)"
            @keydown.space.stop.prevent="mobileLayout && emit('cell-activate', rowIndex, cell.colIndex)"
            @mouseenter="emit('cell-enter', rowIndex, cell.colIndex, $event)"
          >
            <div
              v-if="cell.signatureRole"
              class="sheet-cell-signatures"
              :style="signatureCellStyle(rowIndex)"
            >
              <span
                v-for="person in cell.signatures.slice(0, 3)"
                :key="`${rowIndex}:${cell.colIndex}:${person.record_id}`"
                class="sheet-cell-signature-token"
              >
                {{ person.name || person.display_name || "已确认签名" }}
              </span>
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
  <Teleport to="body" :disabled="!mobileLayout">
  <UiTransition name="ui-popover" appear>
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
    :mobile="mobileLayout"
    :context="activeRowContext"
    :has-previous="activeEditIndex > 0"
    :has-next="activeEditIndex >= 0 && activeEditIndex < editablePositions.length - 1"
    @previous="moveMobileCell(-1)"
    @next="moveMobileCell(1)"
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
  </UiTransition>
  </Teleport>
  <div v-if="!activeSheet" class="empty-box">该附件没有可显示的 Sheet。</div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { PenLine, Table2 } from 'lucide-vue-next';
import MopMobileFillForm from './MopMobileFillForm.vue';
import MopCellPopover, { type MopCellPopoverMode } from "./MopCellPopover.vue";
import MopTableFillToolbar from "./MopTableFillToolbar.vue";

type Dict = Record<string, any>;

const props = defineProps<{
  activeSheet: Dict | null;
  mobileStep: 'details' | 'checks';
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
const tableRef = ref<HTMLTableElement | null>(null);
const mobileQuery = window.matchMedia('(max-width: 720px)');
const mobileLayout = ref(mobileQuery.matches);
const showOriginal = ref(false);
watch(() => props.mobileStep, () => { showOriginal.value = false; });
function updateMobileLayout(): void { mobileLayout.value = mobileQuery.matches; }

const activeRows = computed<unknown[][]>(() => (
  Array.isArray(props.activeSheet?.rows) ? props.activeSheet.rows : []
));
const renderedRows = computed(() => activeRows.value.slice(0, renderedRowLimit.value));
const hasMoreRows = computed(() => renderedRowLimit.value < activeRows.value.length);
function displayValue(row: number, col: number): string {
  return props.mopCellHasOverride(row, col) ? props.cellOverrideValue(row, col) : String(activeRows.value[row]?.[col] ?? '');
}
const editablePositions = computed(() => {
  const positions: Array<{ row: number; col: number }> = [];
  if (!mobileLayout.value) return positions;
  activeRows.value.forEach((_row, row) => {
    for (const col of props.columnIndexes) {
      if (props.cellMergeSpan(row, col)?.hidden || props.signatureRoleAtCell(row, col)) continue;
      if (props.checkboxCellAt(row, col) || props.maintenanceFieldAt(row, col) || props.editableCellAt(row, col)) {
        positions.push({ row, col });
      }
    }
  });
  return positions;
});
const activeEditIndex = computed(() => editablePositions.value.findIndex(position => (
  position.row === props.activeMopCellPosition?.row && position.col === props.activeMopCellPosition?.col
)));
const activeRowContext = computed(() => {
  const position = props.activeMopCellPosition;
  if (!position) return '';
  const row = activeRows.value[position.row] || [];
  const labels = row.slice(0, position.col).map((value, col) => String(
    props.mopCellHasOverride(position.row, col) ? props.cellOverrideValue(position.row, col) : value ?? '',
  ).trim()).filter(Boolean);
  return [...new Set(labels)].slice(-3).join(' · ');
});

function startCell(row: number, col: number, event: MouseEvent): void {
  if (mobileLayout.value) return;
  event.stopPropagation();
  event.preventDefault();
  emit('cell-mousedown', row, col, event);
}

async function moveMobileCell(direction: number): Promise<void> {
  if (activeEditIndex.value < 0) return;
  const position = editablePositions.value[activeEditIndex.value + direction];
  if (!position) return;
  renderedRowLimit.value = Math.max(renderedRowLimit.value, position.row + 1);
  emit('cell-activate', position.row, position.col);
  await nextTick();
  const key = props.mopCellKey(position.row, position.col);
  const cell = tableRef.value?.querySelector<HTMLElement>(`[data-mop-cell-key="${CSS.escape(key)}"]`);
  cell?.scrollIntoView({ block: 'nearest', inline: 'nearest', behavior: 'auto' });
  observeLoadMoreRow();
}

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
    canEdit: Boolean(checkbox || signatureRole || props.maintenanceFieldAt(rowIndex, colIndex) || props.editableCellAt(rowIndex, colIndex)),
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

watch(loadMoreRowRef, observeLoadMoreRow, { flush: 'post' });

watch(
  () => `${props.activeSheet?.name || ""}:${activeRows.value.length}`,
  () => {
    renderedRowLimit.value = Math.min(initialRenderRows, activeRows.value.length || initialRenderRows);
    void nextTick(observeLoadMoreRow);
  },
  { immediate: true },
);

onMounted(() => {
  mobileQuery.addEventListener('change', updateMobileLayout);
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
  mobileQuery.removeEventListener('change', updateMobileLayout);
  cancelLoadMoreFrame();
  loadMoreObserver?.disconnect();
  loadMoreObserver = null;
});

const emit = defineEmits<{
  "mark-all-normal": [];
  "cell-mousedown": [rowIndex: number, colIndex: number, event: MouseEvent];
  "cell-activate": [rowIndex: number, colIndex: number];
  "edit-raw": [row: number, col: number, value: string];
  "edit-checkbox": [row: number, col: number, value: string];
  "edit-field": [field: Dict, value: string];
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

.sheet-cell-signatures span {
  color: #c2410c;
  font-size: 12px;
  font-weight: 850;
}

.sheet-cell-signatures .sheet-cell-signature-token {
  max-width: 112px;
  overflow: hidden;
  border: 1px solid #bfdbfe;
  border-radius: 4px;
  padding: 2px 6px;
  background: #eff6ff;
  color: #1d4ed8;
  text-overflow: ellipsis;
  white-space: nowrap;
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

@media (max-width: 720px) {
  .mobile-sheet-mode { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 8px 12px; position: sticky; left: 0; box-sizing: border-box; width: 100%; background: #fff; border-bottom: 1px solid #e2e8f0; }
  .mobile-sheet-mode strong { font-size: 15px; font-weight: 650; color: #1e293b; }
  .mobile-sheet-mode button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 44px; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 6px; background: #fff; color: #1d4ed8; font: inherit; font-size: 13px; cursor: pointer; }
  .mobile-sheet-mode button:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
  .mobile-sheet th, .mobile-sheet td { min-width: 112px; padding: 10px; font-size: 14px; line-height: 1.5; }
  .mobile-sheet td { height: 46px; touch-action: pan-x pan-y; }
  .mobile-sheet .row-head, .mobile-sheet .corner-cell { min-width: 36px; width: 36px; padding: 6px; font-size: 12px; }
  .mobile-sheet td:focus-visible { outline: 2px solid #1e63ff; outline-offset: -2px; }
  .mobile-sheet .mop-load-more-row button { min-height: 44px; }
}
</style>
