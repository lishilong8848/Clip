<template>
  <section class="followup-panel" :class="{ embedded }">
    <header class="followup-head">
      <div class="followup-title">
        <strong>{{ summaryTitle || "尚未选择维修项目" }}</strong>
      </div>
      <div class="followup-head-actions">
        <b class="followup-count">{{ total }} 条</b>
        <button type="button" class="followup-button quiet compact" :disabled="loading" @click="loadRecords(true)">
          刷新
        </button>
      </div>
    </header>

    <div v-if="!summaryRecordId" class="followup-empty">未选择维修项目</div>
    <div v-else class="followup-layout">
      <aside class="followup-list">
        <div class="followup-list-head">
          <strong>全部跟进</strong>
          <span>{{ total }} 条</span>
        </div>
        <button
          v-for="record in records"
          :key="record.record_id"
          type="button"
          :class="{ active: editingRecordId === record.record_id }"
          @click="requestSelectRecord(record)"
        >
          <strong>{{ record.title || "未命名跟进记录" }}</strong>
          <span>{{ progressLabel(record.progress) }} · {{ record.created_time || "时间未填" }}</span>
        </button>
        <div v-if="loading" class="followup-empty">读取中...</div>
        <div v-else-if="!records.length" class="followup-empty">暂无维修跟进</div>
        <nav v-if="pageCount > 1" class="followup-pager" aria-label="跟进记录分页">
          <button type="button" :disabled="loading || page <= 1" @click="requestChangePage(-1)">上一页</button>
          <span>{{ page }} / {{ pageCount }}</span>
          <button type="button" :disabled="loading || page >= pageCount" @click="requestChangePage(1)">下一页</button>
        </nav>
      </aside>

      <main class="followup-editor">
        <div class="followup-editor-head">
          <strong>{{ editingRecordId ? selectedRecord?.title || "编辑跟进" : "新建跟进" }}</strong>
          <div>
            <button
              v-if="editingRecordId"
              type="button"
              class="followup-button danger"
              :disabled="saving"
              @click="requestDeleteRecord"
            >
              删除
            </button>
            <button type="button" class="followup-button primary" :disabled="primaryActionDisabled" @click="handlePrimaryAction">
              {{ primaryActionLabel }}
            </button>
          </div>
        </div>

        <div class="cmdb-line">
          <div>
            <b>CMDB 设备</b>
            <span>{{ selectedCmdbLabel }}</span>
          </div>
          <button type="button" class="followup-button quiet" @click="openCmdbPicker">
            {{ cmdbRecordId ? "重新选择" : "选择设备" }}
          </button>
        </div>

        <div class="followup-sections">
          <section v-for="group in groupedFields" :key="group.key" class="followup-field-section">
            <header>
              <strong>{{ group.label }}</strong>
            </header>
            <div class="followup-field-grid">
              <label
                v-for="field in group.fields"
                :key="field.field_name"
                :class="{ wide: usesTextarea(field.field_name) }"
              >
                <span>{{ fieldLabel(field.field_name) }}</span>
                <select v-if="field.options?.length" v-model="draft[field.field_name]" @change="markDirty">
                  <option value="">请选择</option>
                  <option
                    v-if="draft[field.field_name] && !field.options.includes(draft[field.field_name])"
                    :value="draft[field.field_name]"
                    disabled
                  >
                    {{ draft[field.field_name] }}（不在当前选项中）
                  </option>
                  <option v-for="option in field.options" :key="option" :value="option">{{ option }}</option>
                </select>
                <input
                  v-else-if="isDateField(field)"
                  v-model="draft[field.field_name]"
                  type="datetime-local"
                  @input="markDirty"
                />
                <input
                  v-else-if="isNumberField(field)"
                  v-model="draft[field.field_name]"
                  type="number"
                  :min="isProgressField(field) ? 0 : undefined"
                  :max="isProgressField(field) ? 1 : undefined"
                  :step="isProgressField(field) ? 0.01 : 'any'"
                  @input="markDirty"
                />
                <textarea
                  v-else-if="usesTextarea(field.field_name)"
                  v-model="draft[field.field_name]"
                  rows="2"
                  @input="markDirty"
                />
                <input v-else v-model="draft[field.field_name]" type="text" @input="markDirty" />
              </label>
            </div>
          </section>
        </div>
      </main>
    </div>

    <MessageBanner v-if="message" :tone="messageTone" :text="message" />

    <ConfirmDialog
      :open="deleteDialogOpen"
      tone="danger"
      kicker="删除跟进记录"
      title="删除当前跟进？"
      message="删除后无法恢复，只会删除当前这一条跟进记录。"
      confirm-label="确认删除"
      @resolve="resolveDeleteConfirmation"
    />
    <ConfirmDialog
      :open="discardDialogOpen"
      tone="warning"
      kicker="未保存修改"
      title="放弃当前跟进修改？"
      message="继续后，当前跟进表单中的修改会丢失。"
      confirm-label="放弃并继续"
      @resolve="resolveDiscardConfirmation"
    />

    <RecordPickerDialog
      :open="cmdbPickerOpen"
      title="选择 CMDB 设备"
      :records="cmdbCandidates"
      :columns="cmdbColumns"
      :selected-ids="cmdbRecordId ? [cmdbRecordId] : []"
      :multiple="false"
      :loading="cmdbLoading"
      :query="cmdbQuery"
      search-placeholder="搜索设备名称、唯一ID、分类或位置"
      @update:query="cmdbQuery = $event"
      @search="loadCmdbCandidates"
      @close="cmdbPickerOpen = false"
      @confirm="confirmCmdb"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { requestJson } from "../api/client";
import {
  parseRepairDraftValue,
  repairDraftInputValue,
  repairFieldUsesTextarea,
} from "../repairManagementUtils";
import type { LooseDict } from "../types";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
import RecordPickerDialog from "./RecordPickerDialog.vue";

const props = withDefaults(defineProps<{
  scope: string;
  summaryRecordId: string;
  summaryTitle: string;
  embedded?: boolean;
}>(), {
  embedded: false,
});

const emit = defineEmits<{
  changed: [];
  "dirty-changed": [value: boolean];
}>();

const cmdbColumns = [
  { key: "title", label: "设备名称", width: "320px" },
  { key: "unique_id", label: "智航唯一ID", width: "190px" },
  { key: "category", label: "分类名称", width: "180px" },
  { key: "building", label: "楼栋", width: "120px" },
  { key: "location", label: "位置", width: "420px" },
];

const hiddenFieldNames = new Set(["是否本维修单第一次提交跟进记录"]);
const fieldLabels: Record<string, string> = {
  "设备名称-1": "设备名称",
  "设备编号-1": "设备编号",
  "设备品牌 -1": "设备品牌",
  "设备型号 -1": "设备型号",
  "维修方 -1": "维修方",
  "供应商名称 -1": "供应商名称",
  "供应商维修人员-1": "供应商维修人员",
  "跟进项（如有）": "跟进项",
  "后续整改措施（如有）": "后续整改措施",
};
const groupFields: Array<{ key: string; label: string; fields: string[] }> = [
  {
    key: "equipment",
    label: "设备信息",
    fields: [
      "设备名称-1", "设备编号-1", "设备品牌 -1", "设备型号 -1",
      "设备生产日期", "设备使用年限", "设备容量KW/AH", "是否质保期内",
    ],
  },
  {
    key: "execution",
    label: "维修执行",
    fields: [
      "维修方 -1", "供应商名称 -1", "供应商维修人员-1",
      "更换备件名称", "更换备件数量", "故障维修总费用",
    ],
  },
  {
    key: "progress",
    label: "进展记录",
    fields: ["维修进展描述", "维修进度", "跟进项（如有）", "后续整改措施（如有）", "超链接"],
  },
];

const loading = ref(false);
const saving = ref(false);
const records = ref<LooseDict[]>([]);
const fields = ref<LooseDict[]>([]);
const editingRecordId = ref("");
const selectedRecord = ref<LooseDict | null>(null);
const draft = reactive<Record<string, string>>({});
const message = ref("");
const messageTone = ref<"success" | "warning" | "failed">("success");
const cmdbPickerOpen = ref(false);
const cmdbLoading = ref(false);
const cmdbQuery = ref("");
const cmdbCandidates = ref<LooseDict[]>([]);
const cmdbRecordId = ref("");
const total = ref(0);
const page = ref(1);
const deleteDialogOpen = ref(false);
const creatingNewFollowup = ref(false);
const followupDirty = ref(false);
const discardDialogOpen = ref(false);
const PAGE_SIZE = 20;
let pendingDiscardAction: null | (() => void) = null;

const editableFields = computed(() => fields.value.filter((field) => (
  field.editable
  && !hiddenFieldNames.has(String(field.field_name || ""))
  && ![11, 17, 18, 21].includes(Number(field.field_type || 0))
)));
const groupedFields = computed(() => {
  const byName = new Map(
    editableFields.value.map((field) => [String(field.field_name || ""), field]),
  );
  const groups = groupFields.map((group) => ({
    key: group.key,
    label: group.label,
    fields: group.fields.map((name) => byName.get(name)).filter(Boolean) as LooseDict[],
  })).filter((group) => group.fields.length);
  const assigned = new Set(groupFields.flatMap((group) => group.fields));
  const otherFields = editableFields.value.filter(
    (field) => !assigned.has(String(field.field_name || "")),
  );
  if (otherFields.length) groups.push({ key: "other", label: "其他信息", fields: otherFields });
  return groups;
});

const selectedCmdbLabel = computed(() => {
  if (!cmdbRecordId.value) return "未选择";
  const matched = cmdbCandidates.value.find(
    (item) => String(item.record_id || "") === cmdbRecordId.value,
  );
  return String(matched?.title || matched?.unique_id || "已选择设备");
});
const pageCount = computed(() => Math.max(1, Math.ceil(total.value / PAGE_SIZE)));
const hasDraftContent = computed(() => Boolean(
  cmdbRecordId.value
  || editableFields.value.some((field) => String(draft[String(field.field_name || "")] || "").trim()),
));
const primaryActionLabel = computed(() => {
  if (saving.value) return "保存中";
  if (editingRecordId.value && !followupDirty.value) return "新增跟进";
  return editingRecordId.value ? "保存修改" : "保存跟进";
});
const primaryActionDisabled = computed(() => {
  if (saving.value || !props.summaryRecordId) return true;
  if (editingRecordId.value && !followupDirty.value) return false;
  return !hasDraftContent.value;
});

function showMessage(text: string, tone: "success" | "warning" | "failed" = "success"): void {
  message.value = text;
  messageTone.value = tone;
}

function isDateField(field: LooseDict): boolean {
  return Number(field.field_type || 0) === 5 || String(field.ui_type || "").toLowerCase().includes("datetime");
}

function isNumberField(field: LooseDict): boolean {
  return Number(field.field_type || 0) === 2 || String(field.ui_type || "").toLowerCase() === "number";
}

function isProgressField(field: LooseDict): boolean {
  return String(field.field_name || "") === "维修进度"
    || String(field.ui_type || "").toLowerCase() === "progress";
}

function fieldLabel(fieldName: unknown): string {
  const name = String(fieldName || "");
  return fieldLabels[name] || name;
}

function usesTextarea(fieldName: unknown): boolean {
  return repairFieldUsesTextarea(fieldName);
}

function progressLabel(value: unknown): string {
  const number = Number(value);
  if (Number.isFinite(number)) {
    const percent = number <= 1 ? number * 100 : number;
    return `${Math.round(percent)}%`;
  }
  return String(value || "进度未填");
}

function clearDraft(): void {
  for (const key of Object.keys(draft)) delete draft[key];
  for (const field of editableFields.value) draft[String(field.field_name || "")] = "";
}

function setDirty(value: boolean): void {
  if (followupDirty.value === value) return;
  followupDirty.value = value;
  emit("dirty-changed", value);
}

function markDirty(): void {
  setDirty(true);
}

function runWithDirtyGuard(action: () => void): void {
  if (!followupDirty.value) {
    action();
    return;
  }
  pendingDiscardAction = action;
  discardDialogOpen.value = true;
}

function resolveDiscardConfirmation(confirmed: boolean): void {
  const action = pendingDiscardAction;
  pendingDiscardAction = null;
  discardDialogOpen.value = false;
  if (confirmed && action) action();
}

function startCreate(): void {
  creatingNewFollowup.value = true;
  editingRecordId.value = "";
  selectedRecord.value = null;
  cmdbRecordId.value = "";
  clearDraft();
  setDirty(false);
}

function requestStartCreate(): void {
  if (creatingNewFollowup.value && !editingRecordId.value) return;
  runWithDirtyGuard(startCreate);
}

function selectRecord(record: LooseDict): void {
  creatingNewFollowup.value = false;
  editingRecordId.value = String(record.record_id || "");
  selectedRecord.value = record;
  cmdbRecordId.value = String(record.cmdb_record_id || "");
  clearDraft();
  const raw = record.raw_fields && typeof record.raw_fields === "object" ? record.raw_fields : {};
  const display = record.display_fields && typeof record.display_fields === "object" ? record.display_fields : {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const value = Object.prototype.hasOwnProperty.call(raw, name) ? raw[name] : display[name];
    draft[name] = repairDraftInputValue(field, value);
  }
  setDirty(false);
}

function requestSelectRecord(record: LooseDict): void {
  if (String(record.record_id || "") === editingRecordId.value) return;
  runWithDirtyGuard(() => selectRecord(record));
}

function resetForParent(): void {
  creatingNewFollowup.value = false;
  editingRecordId.value = "";
  selectedRecord.value = null;
  cmdbRecordId.value = "";
  clearDraft();
  setDirty(false);
}

function changePage(delta: number): void {
  const nextPage = Math.min(pageCount.value, Math.max(1, page.value + delta));
  if (nextPage === page.value) return;
  page.value = nextPage;
  void loadRecords(false);
}

function requestChangePage(delta: number): void {
  runWithDirtyGuard(() => {
    setDirty(false);
    changePage(delta);
  });
}

async function loadRecords(announce = false): Promise<void> {
  if (!props.summaryRecordId) {
    records.value = [];
    fields.value = [];
    total.value = 0;
    return;
  }
  loading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      summary_record_id: props.summaryRecordId,
      limit: String(PAGE_SIZE),
      offset: String((page.value - 1) * PAGE_SIZE),
    });
    const payload = await requestJson(`/api/repair-management/followups?${params.toString()}`);
    records.value = Array.isArray(payload.records) ? payload.records : [];
    fields.value = Array.isArray(payload.fields) ? payload.fields : [];
    total.value = Number(payload.total || records.value.length || 0);
    const maxPage = Math.max(1, Math.ceil(total.value / PAGE_SIZE));
    if (page.value > maxPage) {
      page.value = maxPage;
      loading.value = false;
      await loadRecords(announce);
      return;
    }
    if (editingRecordId.value) {
      const current = records.value.find((item) => String(item.record_id || "") === editingRecordId.value);
      if (current) selectRecord(current);
      else if (records.value.length) selectRecord(records.value[0]);
      else resetForParent();
    } else if (!creatingNewFollowup.value && records.value.length) {
      selectRecord(records.value[0]);
    } else {
      clearDraft();
    }
    if (announce) showMessage(records.value.length ? "跟进记录已刷新。" : "暂无跟进记录。", records.value.length ? "success" : "warning");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修跟进读取失败。", "failed");
  } finally {
    loading.value = false;
  }
}

function buildFields(): LooseDict {
  const payload: LooseDict = {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const value = String(draft[name] ?? "");
    if (!value.trim() && !editingRecordId.value) continue;
    payload[name] = parseRepairDraftValue(value);
  }
  return payload;
}

function handlePrimaryAction(): void {
  if (editingRecordId.value && !followupDirty.value) {
    requestStartCreate();
    return;
  }
  void saveRecord();
}

async function saveRecord(): Promise<void> {
  if (!props.summaryRecordId) return;
  if (!hasDraftContent.value) {
    showMessage("请至少填写一项跟进内容。", "warning");
    return;
  }
  saving.value = true;
  try {
    const body = JSON.stringify({
      scope: props.scope || "ALL",
      summary_record_id: props.summaryRecordId,
      cmdb_record_id: cmdbRecordId.value,
      fields: buildFields(),
    });
    const wasEditing = Boolean(editingRecordId.value);
    const payload = wasEditing
      ? await requestJson(`/api/repair-management/followups/${encodeURIComponent(editingRecordId.value)}`, {
          method: "PUT",
          body,
        })
      : await requestJson("/api/repair-management/followups", { method: "POST", body });
    editingRecordId.value = String(payload.record_id || editingRecordId.value || "");
    setDirty(false);
    if (!wasEditing) page.value = 1;
    const warnings = Array.isArray(payload.warnings)
      ? payload.warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
      : [];
    const successText = wasEditing ? "维修跟进已更新。" : "维修跟进已创建。";
    showMessage(
      warnings.length ? `${successText}${warnings.join("；")}` : successText,
      warnings.length ? "warning" : "success",
    );
    await loadRecords(false);
    emit("changed");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修跟进保存失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function requestDeleteRecord(): void {
  if (!editingRecordId.value || !props.summaryRecordId) return;
  deleteDialogOpen.value = true;
}

async function resolveDeleteConfirmation(confirmed: boolean): Promise<void> {
  deleteDialogOpen.value = false;
  if (confirmed) await deleteRecordNow();
}

async function deleteRecordNow(): Promise<void> {
  if (!editingRecordId.value || !props.summaryRecordId) return;
  saving.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      summary_record_id: props.summaryRecordId,
    });
    await requestJson(
      `/api/repair-management/followups/${encodeURIComponent(editingRecordId.value)}?${params.toString()}`,
      { method: "DELETE" },
    );
    resetForParent();
    await loadRecords(false);
    emit("changed");
    showMessage("维修跟进已删除。", "success");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修跟进删除失败。", "failed");
  } finally {
    saving.value = false;
  }
}

async function loadCmdbCandidates(): Promise<void> {
  cmdbLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      q: cmdbQuery.value,
      limit: "200",
    });
    const payload = await requestJson(`/api/repair-management/cmdb-candidates?${params.toString()}`);
    cmdbCandidates.value = Array.isArray(payload.records) ? payload.records : [];
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "CMDB 设备读取失败。", "failed");
  } finally {
    cmdbLoading.value = false;
  }
}

async function openCmdbPicker(): Promise<void> {
  cmdbPickerOpen.value = true;
  await loadCmdbCandidates();
}

function confirmCmdb(recordIds: string[]): void {
  cmdbRecordId.value = String(recordIds[0] || "").trim();
  cmdbPickerOpen.value = false;
  const matched = cmdbCandidates.value.find(
    (item) => String(item.record_id || "") === cmdbRecordId.value,
  );
  if (matched) {
    if (!String(draft["设备名称-1"] || "").trim()) {
      const category = String(matched.category || "").trim();
      const deviceNameField = fields.value.find(
        (field) => String(field.field_name || "") === "设备名称-1",
      );
      const options = Array.isArray(deviceNameField?.options) ? deviceNameField.options : [];
      if (!options.length || options.includes(category)) {
        draft["设备名称-1"] = category;
      } else if (category) {
        showMessage(`设备分类“${category}”不是跟进表可选项，请手动选择设备名称。`, "warning");
      }
    }
    if (!String(draft["设备编号-1"] || "").trim()) {
      draft["设备编号-1"] = String(matched.title || "");
    }
  }
  markDirty();
}

watch(
  () => [props.summaryRecordId, props.scope] as const,
  () => {
    page.value = 1;
    resetForParent();
    void loadRecords(false);
  },
  { immediate: true },
);
</script>

<style scoped>
.followup-panel {
  display: grid;
  gap: 12px;
  padding: 14px 16px 16px;
  border: 1px solid #cfe5e2;
  border-top: 4px solid #0e9f8c;
  border-radius: 16px;
  background: #fff;
  box-shadow: 0 14px 34px rgba(16, 91, 94, 0.08);
}

.followup-panel.embedded {
  padding: 2px 0 0;
  border: 0;
  border-radius: 0;
  background: transparent;
  box-shadow: none;
}

.followup-head,
.followup-editor-head,
.cmdb-line {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.followup-title {
  min-width: 0;
}

.followup-title strong {
  overflow: hidden;
  color: #102848;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.followup-head-actions,
.followup-editor-head > div:last-child {
  display: flex;
  align-items: center;
  gap: 8px;
}

.followup-count {
  min-height: 28px;
  display: inline-flex;
  align-items: center;
  padding: 0 10px;
  border-radius: 999px;
  background: #eef8f7;
  color: #087a69;
  font-size: 12px;
  white-space: nowrap;
}

.followup-layout {
  min-height: 320px;
  display: grid;
  grid-template-columns: minmax(230px, 0.62fr) minmax(0, 2.38fr);
  gap: 16px;
  padding-top: 12px;
  border-top: 1px solid #e2ecef;
}

.followup-list {
  display: grid;
  align-content: start;
  gap: 8px;
  max-height: 560px;
  overflow: auto;
  padding-right: 8px;
  border-right: 1px solid #e2ecef;
}

.followup-pager {
  min-height: 38px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-top: auto;
  padding: 8px 5px 2px;
  border-top: 1px solid #dbeae8;
}

.followup-pager button {
  min-height: 28px;
  border: 1px solid #cfe2df;
  border-radius: 9px;
  padding: 0 9px;
  background: #f5fbfa;
  color: #087f70;
  font: inherit;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}

.followup-pager button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
}

.followup-pager span {
  color: #64817e;
  font-size: 12px;
  font-weight: 850;
}

.followup-list-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 2px 2px 5px;
}

.followup-list-head strong {
  color: #17304f;
  font-size: 13px;
}

.followup-list-head span,
.followup-list > button span,
.cmdb-line span {
  color: #647b97;
  font-size: 12px;
}

.followup-list > button {
  display: grid;
  gap: 5px;
  padding: 11px 12px;
  border: 1px solid #dce9e8;
  border-radius: 10px;
  background: #f8fcfb;
  text-align: left;
  cursor: pointer;
}

.followup-list > button.active {
  border-color: #0e9f8c;
  background: #eaf8f5;
  box-shadow: inset 3px 0 0 #0e9f8c;
}

.followup-list > button strong {
  overflow: hidden;
  color: #17304f;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.followup-editor {
  min-width: 0;
  display: grid;
  align-content: start;
  gap: 12px;
}

.followup-editor-head > div:first-child {
  display: grid;
  gap: 2px;
}

.followup-editor-head > div:first-child span {
  color: #6a8098;
  font-size: 11px;
  font-weight: 850;
}

.followup-editor-head strong {
  color: #102848;
}

.cmdb-line {
  padding: 9px 11px;
  border: 1px solid #cfe5e2;
  border-radius: 10px;
  background: #f1faf8;
}

.cmdb-line > div {
  display: grid;
  gap: 2px;
}

.followup-sections {
  display: grid;
  gap: 8px;
}

.followup-field-section {
  display: grid;
  gap: 9px;
  padding: 4px 0 12px;
  border-bottom: 1px solid #e2ecef;
}

.followup-field-section:last-child {
  border-bottom: 0;
}

.followup-field-section > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  color: #17304f;
}

.followup-field-section > header span {
  color: #758aa1;
  font-size: 12px;
}

.followup-field-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.followup-field-grid label {
  min-width: 0;
  display: grid;
  align-content: start;
  gap: 5px;
}

.followup-field-grid label.wide {
  grid-column: span 3;
}

.followup-field-grid label > span {
  color: #4a6380;
  font-size: 12px;
  font-weight: 800;
}

.followup-field-grid input,
.followup-field-grid select,
.followup-field-grid textarea {
  width: 100%;
  min-height: 36px;
  padding: 7px 10px;
  border: 1px solid #d1deed;
  border-radius: 9px;
  background: #fff;
  color: #152d4b;
  font: inherit;
}

.followup-field-grid textarea {
  resize: vertical;
}

.followup-button {
  min-height: 34px;
  padding: 0 14px;
  border-radius: 9px;
  font-weight: 800;
  cursor: pointer;
}

.followup-button.primary {
  border: 1px solid #0e8f7f;
  background: #0e8f7f;
  color: #fff;
}

.followup-button.quiet {
  border: 1px solid #cfdceb;
  background: #fff;
  color: #31506f;
}

.followup-button.danger {
  border: 1px solid #efc5c9;
  background: #fff5f5;
  color: #bd2935;
}

.followup-button.compact {
  min-height: 30px;
  padding-inline: 10px;
  font-size: 12px;
}

.followup-button:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.followup-empty {
  padding: 18px 16px;
  border: 1px dashed #cfe5e2;
  border-radius: 10px;
  background: #f7fbfb;
  color: #6f8298;
  text-align: center;
}

@media (max-width: 1180px) {
  .followup-layout,
  .followup-field-grid {
    grid-template-columns: 1fr;
  }

  .followup-field-grid label.wide {
    grid-column: auto;
  }

  .followup-list {
    max-height: 220px;
    padding-right: 0;
    padding-bottom: 10px;
    border-right: 0;
    border-bottom: 1px solid #e2ecef;
  }
}
</style>
