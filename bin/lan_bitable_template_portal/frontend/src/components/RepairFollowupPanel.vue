<template>
  <section class="followup-panel" :class="{ embedded }">
    <header class="followup-head">
      <div class="followup-title">
        <strong>{{ summaryTitle || "尚未选择维修项目" }}</strong>
      </div>
      <div class="followup-head-actions">
        <b class="followup-count">{{ total }} 条</b>
      </div>
    </header>

    <MessageBanner v-if="message" :tone="messageTone" :text="message" />

    <div v-if="!summaryRecordId" class="followup-empty">未选择维修项目</div>
    <div v-else-if="loading && !fields.length" class="followup-empty" aria-live="polite">
      正在读取当前维修项目的跟进记录...
    </div>
    <div v-else class="followup-workspace">
      <aside class="followup-timeline" aria-label="跟进记录时间线">
        <header class="followup-timeline-head">
          <strong>跟进记录</strong>
          <span>{{ total }} 条</span>
        </header>
        <div class="followup-timeline-tools">
          <label class="followup-timeline-search">
            <Search :size="15" aria-hidden="true" />
            <input v-model.trim="followupQuery" type="search" placeholder="搜索跟进内容" />
          </label>
          <button
            type="button"
            class="followup-button quiet icon-button"
            :disabled="loading"
            title="刷新跟进记录"
            aria-label="刷新跟进记录"
            @click="requestRefresh"
          >
            <RefreshCw :size="16" :class="{ spinning: loading }" aria-hidden="true" />
          </button>
        </div>
        <div class="followup-timeline-list" role="listbox" aria-label="选择跟进记录">
          <div v-if="loading && !timelineRecords.length" class="followup-empty">正在读取...</div>
          <div v-else-if="!timelineRecords.length" class="followup-empty">暂无匹配的跟进记录</div>
          <button
            v-for="record in timelineRecords"
            v-else
            :key="record.record_id"
            type="button"
            role="option"
            :aria-selected="editingRecordId === record.record_id"
            :class="{
              active: editingRecordId === record.record_id,
              'outside-filter': isOutsideFilterRecord(record),
            }"
            @click="requestSelectRecord(record)"
          >
            <span class="timeline-marker">
              <Check v-if="editingRecordId === record.record_id" :size="13" aria-hidden="true" />
            </span>
            <span class="timeline-copy">
              <strong>{{ record.title || "未命名跟进记录" }}</strong>
              <small v-if="isOutsideFilterRecord(record)" class="timeline-filter-note">
                当前编辑 · 筛选外
              </small>
              <small v-else>{{ repairDisplayTime(record.created_time) || "时间未填" }}</small>
            </span>
            <b>{{ progressLabel(record.progress) }}</b>
          </button>
        </div>
        <nav v-if="pageCount > 1" class="followup-pager" aria-label="跟进记录分页">
          <button type="button" :disabled="loading || page <= 1" @click="requestChangePage(-1)">上一页</button>
          <span>{{ page }} / {{ pageCount }}</span>
          <button type="button" :disabled="loading || page >= pageCount" @click="requestChangePage(1)">下一页</button>
        </nav>
      </aside>

      <main class="followup-editor">
        <div class="followup-editor-head">
          <strong>{{ editingRecordId ? selectedRecord?.title || "编辑跟进" : "填写新跟进" }}</strong>
        </div>

        <div class="cmdb-line">
          <div>
            <b>CMDB 设备</b>
            <span>{{ selectedCmdbLabel }}</span>
          </div>
          <button type="button" class="followup-button quiet" @click="openCmdbPicker">
            {{ cmdbRecordIds.length ? "重新选择" : "选择设备" }}
          </button>
        </div>

        <div class="followup-sections">
          <section v-for="group in groupedFields" :key="group.key" class="followup-field-section">
            <header>
              <strong>{{ group.label }}</strong>
            </header>
            <div class="followup-field-grid">
              <template v-for="field in group.fields" :key="field.field_name">
                <RepairPeoplePicker
                  v-if="isWorkerField(field)"
                  :scope="scope"
                  :input-id="followupFieldInputId(field)"
                  :model-value="workerPeople"
                  @update:model-value="updateWorkerPeople"
                />
                <RepairFieldControl
                  v-else
                  :field="field"
                  :input-id="followupFieldInputId(field)"
                  :label="fieldLabel(field.field_name)"
                  :model-value="draft[field.field_name]"
                  :wide="usesTextarea(field.field_name)"
                  :percentage="isProgressField(field)"
                  :select-options="selectOptionsForField(field)"
                  :allow-custom-select="isModelField(field)"
                  :disabled="isFollowupFieldDisabled(field)"
                  :placeholder="fieldPlaceholder(field)"
                  :number-min="isProgressField(field) ? 0 : undefined"
                  :number-max="isProgressField(field) ? 1 : undefined"
                  :number-step="isProgressField(field) ? 0.01 : 'any'"
                  compact
                  @update:model-value="updateFollowupField(field, $event)"
                />
              </template>
            </div>
          </section>
        </div>

        <footer class="followup-action-bar">
          <div class="followup-save-state" :class="followupSaveStateTone">
            <component :is="followupSaveStateIcon" :size="17" aria-hidden="true" />
            <span>{{ followupSaveStateText }}</span>
          </div>
          <div>
            <button
              v-if="editingRecordId"
              type="button"
              class="followup-button danger"
              :disabled="saving"
              @click="requestDeleteRecord"
            >
              <Trash2 :size="16" aria-hidden="true" />
              <span>删除</span>
            </button>
            <button
              type="button"
              class="followup-button primary"
              :disabled="primaryActionDisabled"
              :title="primaryActionDisabledReason"
              @click="handlePrimaryAction"
            >
              <Plus v-if="primaryActionMode === 'create'" :size="16" aria-hidden="true" />
              <Save v-else :size="16" aria-hidden="true" />
              <span>{{ primaryActionLabel }}</span>
            </button>
          </div>
        </footer>
      </main>
    </div>

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
      :selected-ids="cmdbRecordIds"
      :multiple="true"
      :allow-empty="true"
      :loading="cmdbLoading"
      :has-more="cmdbCandidatesHaveMore"
      :result-note="cmdbCandidateNote"
      :query="cmdbQuery"
      search-placeholder="搜索设备名称、唯一ID、分类或位置"
      @update:query="cmdbQuery = $event"
      @search="loadCmdbCandidates(true)"
      @load-more="loadCmdbCandidates(false)"
      @close="cmdbPickerOpen = false"
      @confirm="confirmCmdb"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import {
  AlertCircle,
  Check,
  CheckCircle2,
  LoaderCircle,
  Plus,
  RefreshCw,
  Save,
  Search,
  Trash2,
} from "lucide-vue-next";
import { requestJson } from "../api/client";
import {
  createRepairOperationId,
  parseRepairDraftValue,
  repairDraftInputValue,
  repairDisplayTime,
  repairFieldUsesTextarea,
} from "../repairManagementUtils";
import {
  clearRepairFollowupCache,
  getRepairFollowupCache,
  setRepairFollowupCache,
} from "../repairFollowupCache";
import type { LooseDict } from "../types";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
import RecordPickerDialog from "./RecordPickerDialog.vue";
import RepairFieldControl from "./RepairFieldControl.vue";
import RepairPeoplePicker from "./RepairPeoplePicker.vue";

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
  "count-changed": [value: number];
}>();

const cmdbColumns = [
  { key: "title", label: "设备名称", width: "280px" },
  { key: "location", label: "位置", width: "380px", wrap: true },
  { key: "unique_id", label: "智航唯一ID", width: "180px" },
  { key: "category", label: "分类名称", width: "160px" },
  { key: "building", label: "楼栋", width: "100px" },
];

const DEVICE_NAME_FIELD_NAME = "设备名称";
const DEVICE_NUMBER_FIELD_NAME = "设备编号";
const BRAND_FIELD_NAME = "设备品牌";
const MODEL_FIELD_NAME = "设备型号";
const REPAIR_PARTY_FIELD_NAME = "维修方";
const SUPPLIER_FIELD_NAMES = new Set(["供应商名称", "供应商维修人员"]);
const WORKER_FIELD_NAME = "随工人员（我方维修人员）";
const fieldLabels: Record<string, string> = {
  "跟进项（如有）": "跟进项",
  "后续整改措施（如有）": "后续整改措施",
};
const groupFields: Array<{ key: string; label: string; fields: string[] }> = [
  {
    key: "equipment",
    label: "设备信息",
    fields: [
      DEVICE_NAME_FIELD_NAME, DEVICE_NUMBER_FIELD_NAME, BRAND_FIELD_NAME, MODEL_FIELD_NAME,
      "设备生产日期", "设备使用年限", "设备容量KW/AH", "是否质保期内",
    ],
  },
  {
    key: "execution",
    label: "维修执行",
    fields: [
      "维修方", "供应商名称", "供应商维修人员",
      WORKER_FIELD_NAME,
      "更换备件名称", "更换备件数量", "故障维修总费用",
    ],
  },
  {
    key: "progress",
    label: "进展记录",
    fields: ["维修进展描述", "维修进度", "跟进项（如有）", "后续整改措施（如有）"],
  },
];
const visibleFollowupFieldNames = new Set(
  groupFields.flatMap((group) => group.fields),
);
const CMDB_CANDIDATE_BATCH_SIZE = 200;
const CMDB_CANDIDATE_MAX_LIMIT = 1_000;

const loading = ref(false);
const saving = ref(false);
const records = ref<LooseDict[]>([]);
const fields = ref<LooseDict[]>([]);
const editingRecordId = ref("");
const selectedRecord = ref<LooseDict | null>(null);
const draft = reactive<Record<string, string>>({});
const dirtyFieldNames = new Set<string>();
const message = ref("");
const messageTone = ref<"success" | "warning" | "failed">("success");
const cmdbPickerOpen = ref(false);
const cmdbLoading = ref(false);
const cmdbQuery = ref("");
const cmdbCandidates = ref<LooseDict[]>([]);
const cmdbCandidateLimit = ref(CMDB_CANDIDATE_BATCH_SIZE);
const cmdbCandidatesHaveMore = ref(false);
const cmdbCandidateNote = ref("");
const cmdbRecordIds = ref<string[]>([]);
const workerPeople = ref<LooseDict[]>([]);
const brandModelOptions = ref<Record<string, string[]>>({});
const total = ref(0);
const page = ref(1);
const deleteDialogOpen = ref(false);
const creatingNewFollowup = ref(false);
const createOperationId = ref("");
const followupFocusRecordId = ref("");
const followupDirty = ref(false);
const discardDialogOpen = ref(false);
const followupQuery = ref("");
const PAGE_SIZE = 20;
let pendingDiscardAction: null | (() => void) = null;
let recordsRequestVersion = 0;
let cmdbRequestVersion = 0;
let queryTimer: ReturnType<typeof setTimeout> | undefined;
let skipNextQueryReload = false;
let recordsAbortController: AbortController | null = null;
let cmdbAbortController: AbortController | null = null;

const editableFields = computed(() => fields.value.filter((field) => (
  field.editable
  && visibleFollowupFieldNames.has(String(field.field_name || ""))
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
  return groups;
});

const selectedCmdbLabel = computed(() => {
  const ids = cmdbRecordIds.value;
  if (!ids.length) return "未选择";
  const labels = ids.map((recordId) => {
    const matched = cmdbCandidates.value.find(
      (item) => String(item.record_id || "") === recordId,
    );
    return String(matched?.title || matched?.unique_id || "").trim();
  }).filter(Boolean);
  if (ids.length === 1) return labels[0] || "已选择 1 台设备";
  const summary = labels.slice(0, 2).join("、");
  return summary ? `${summary}${ids.length > 2 ? ` 等 ${ids.length} 台` : ""}` : `已选择 ${ids.length} 台设备`;
});
const pageCount = computed(() => Math.max(1, Math.ceil(total.value / PAGE_SIZE)));
const editingRecordOutsideResults = computed(() => Boolean(
  editingRecordId.value
  && selectedRecord.value
  && !records.value.some((item) => String(item.record_id || "") === editingRecordId.value),
));
const timelineRecords = computed(() => {
  if (!editingRecordOutsideResults.value || !selectedRecord.value) return records.value;
  return [selectedRecord.value, ...records.value];
});
const selectedBrand = computed(() => String(draft[BRAND_FIELD_NAME] || "").trim());
const isInternalRepairParty = computed(() => (
  String(draft[REPAIR_PARTY_FIELD_NAME] || "").trim() === "我方"
));
const selectedModelOptions = computed(() => {
  if (!selectedBrand.value) return [];
  const values = brandModelOptions.value[selectedBrand.value];
  return Array.isArray(values) ? values : [];
});
const hasDraftContent = computed(() => Boolean(
  cmdbRecordIds.value.length
  || workerPeople.value.length
  || editableFields.value.some((field) => String(draft[String(field.field_name || "")] || "").trim()),
));
const primaryActionMode = computed<"create" | "save">(() => (
  editingRecordId.value && !followupDirty.value ? "create" : "save"
));
const primaryActionLabel = computed(() => {
  if (saving.value) return "保存中";
  if (primaryActionMode.value === "create") return "新增跟进记录";
  return editingRecordId.value ? "更新跟进记录" : "新增跟进记录";
});
const primaryActionDisabled = computed(() => {
  if (saving.value || !props.summaryRecordId) return true;
  if (primaryActionMode.value === "create") return false;
  if (editingRecordId.value) return !followupDirty.value;
  return !hasDraftContent.value;
});
const primaryActionDisabledReason = computed(() => {
  if (saving.value) return "正在保存";
  if (!props.summaryRecordId) return "请先选择维修项目";
  if (primaryActionMode.value === "create") return "新增一条跟进记录";
  if (!primaryActionDisabled.value) return primaryActionLabel.value;
  return "请至少填写一项跟进内容";
});
const followupSaveStateText = computed(() => {
  if (saving.value) return "保存中";
  if (followupDirty.value) return "有未保存修改";
  if (editingRecordId.value) return "已保存";
  return hasDraftContent.value ? "等待保存" : "等待填写";
});
const followupSaveStateTone = computed(() => {
  if (saving.value) return "saving";
  if (followupDirty.value || hasDraftContent.value && !editingRecordId.value) return "dirty";
  return editingRecordId.value ? "saved" : "idle";
});
const followupSaveStateIcon = computed(() => {
  if (saving.value) return LoaderCircle;
  if (followupDirty.value || hasDraftContent.value && !editingRecordId.value) return AlertCircle;
  return CheckCircle2;
});

function showMessage(text: string, tone: "success" | "warning" | "failed" = "success"): void {
  message.value = text;
  messageTone.value = tone;
}

function isOutsideFilterRecord(record: LooseDict): boolean {
  return editingRecordOutsideResults.value
    && String(record.record_id || "") === editingRecordId.value;
}

function candidateHasMore(
  payload: LooseDict,
  recordCount: number,
  requestedLimit: number,
): boolean {
  const totalCount = Number(payload.total ?? payload.total_count);
  if (Number.isFinite(totalCount) && totalCount > recordCount) return true;
  return payload.has_more === true
    || payload.truncated === true
    || recordCount >= requestedLimit;
}

function isProgressField(field: LooseDict): boolean {
  return String(field.field_name || "") === "维修进度"
    || String(field.ui_type || "").toLowerCase() === "progress";
}

function isModelField(field: LooseDict): boolean {
  return String(field.field_name || "") === MODEL_FIELD_NAME;
}

function isWorkerField(field: LooseDict): boolean {
  return String(field.field_name || "") === WORKER_FIELD_NAME;
}

function isSupplierField(field: LooseDict): boolean {
  return SUPPLIER_FIELD_NAMES.has(String(field.field_name || ""));
}

function isFollowupFieldDisabled(field: LooseDict): boolean {
  if (isModelField(field) && !selectedBrand.value) return true;
  return isSupplierField(field) && isInternalRepairParty.value;
}

function normalizeWorkerPeople(value: unknown): LooseDict[] {
  let source: unknown[] = [];
  if (Array.isArray(value)) {
    source = value;
  } else if (value && typeof value === "object") {
    const payload = value as LooseDict;
    source = Array.isArray(payload.users)
      ? payload.users
      : Array.isArray(payload.value) ? payload.value : [payload];
  }
  const result: LooseDict[] = [];
  const seen = new Set<string>();
  for (const item of source) {
    if (!item || typeof item !== "object") continue;
    const person = item as LooseDict;
    const userId = String(person.user_id || person.open_id || person.id || "").trim();
    if (!userId || seen.has(userId)) continue;
    seen.add(userId);
    result.push({
      user_id: userId,
      name: String(person.name || person.text || "已选人员").trim() || "已选人员",
    });
  }
  return result;
}

function selectOptionsForField(field: LooseDict): string[] | null {
  return isModelField(field) ? selectedModelOptions.value : null;
}

function fieldPlaceholder(field: LooseDict): string {
  if (isSupplierField(field) && isInternalRepairParty.value) {
    return "维修方为我方，无需填写";
  }
  if (!isModelField(field)) return "";
  if (!selectedBrand.value) return "请先选择设备品牌";
  return "选择或输入设备型号";
}

function fieldLabel(fieldName: unknown): string {
  const name = String(fieldName || "");
  return fieldLabels[name] || name;
}

function usesTextarea(fieldName: unknown): boolean {
  return repairFieldUsesTextarea(fieldName);
}

function followupFieldInputId(field: LooseDict): string {
  return `repair-followup-field-${encodeURIComponent(String(field.field_name || "field"))}`;
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
  dirtyFieldNames.clear();
  workerPeople.value = [];
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

function updateFollowupField(field: LooseDict, value: string): void {
  const fieldName = String(field.field_name || "");
  draft[fieldName] = value;
  dirtyFieldNames.add(fieldName);
  if (
    fieldName === DEVICE_NAME_FIELD_NAME
    && !String(draft[DEVICE_NUMBER_FIELD_NAME] || "").trim()
  ) {
    draft[DEVICE_NUMBER_FIELD_NAME] = String(value || "").trim();
    dirtyFieldNames.add(DEVICE_NUMBER_FIELD_NAME);
  }
  if (fieldName === BRAND_FIELD_NAME) {
    const allowedModels = brandModelOptions.value[value] || [];
    const currentModel = String(draft[MODEL_FIELD_NAME] || "").trim();
    if (currentModel && !allowedModels.includes(currentModel)) {
      draft[MODEL_FIELD_NAME] = "";
      dirtyFieldNames.add(MODEL_FIELD_NAME);
    }
  }
  if (fieldName === REPAIR_PARTY_FIELD_NAME && String(value || "").trim() === "我方") {
    for (const supplierFieldName of SUPPLIER_FIELD_NAMES) {
      draft[supplierFieldName] = "";
      dirtyFieldNames.add(supplierFieldName);
    }
  }
  markDirty();
}

function updateWorkerPeople(value: LooseDict[]): void {
  workerPeople.value = value;
  dirtyFieldNames.add(WORKER_FIELD_NAME);
  markDirty();
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
  createOperationId.value = "";
  followupFocusRecordId.value = "";
  creatingNewFollowup.value = true;
  editingRecordId.value = "";
  selectedRecord.value = null;
  cmdbRecordIds.value = [];
  clearDraft();
  setDirty(false);
}

function requestStartCreate(): void {
  if (creatingNewFollowup.value && !editingRecordId.value) return;
  runWithDirtyGuard(startCreate);
}

function selectRecord(record: LooseDict): void {
  createOperationId.value = "";
  followupFocusRecordId.value = "";
  creatingNewFollowup.value = false;
  editingRecordId.value = String(record.record_id || "");
  selectedRecord.value = record;
  cmdbRecordIds.value = Array.isArray(record.cmdb_record_ids)
    ? Array.from(new Set(record.cmdb_record_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean)))
    : [];
  clearDraft();
  const raw = record.raw_fields && typeof record.raw_fields === "object" ? record.raw_fields : {};
  const display = record.display_fields && typeof record.display_fields === "object" ? record.display_fields : {};
  workerPeople.value = normalizeWorkerPeople(
    Object.prototype.hasOwnProperty.call(raw, WORKER_FIELD_NAME)
      ? raw[WORKER_FIELD_NAME]
      : display[WORKER_FIELD_NAME],
  );
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    if (name === WORKER_FIELD_NAME) continue;
    const fieldType = Number(field.field_type || 0);
    const prefersRaw = [2, 5, 15].includes(fieldType);
    const value = prefersRaw && Object.prototype.hasOwnProperty.call(raw, name)
      ? raw[name]
      : Object.prototype.hasOwnProperty.call(display, name) ? display[name] : raw[name];
    draft[name] = repairDraftInputValue(field, value);
  }
  if (String(draft[REPAIR_PARTY_FIELD_NAME] || "").trim() === "我方") {
    for (const supplierFieldName of SUPPLIER_FIELD_NAMES) {
      draft[supplierFieldName] = "";
    }
  }
  setDirty(false);
}

function requestSelectRecord(record: LooseDict): void {
  if (String(record.record_id || "") === editingRecordId.value) return;
  runWithDirtyGuard(() => {
    selectRecord(record);
  });
}

function resetForParent(): void {
  recordsRequestVersion += 1;
  cmdbRequestVersion += 1;
  recordsAbortController?.abort();
  recordsAbortController = null;
  cmdbAbortController?.abort();
  cmdbAbortController = null;
  if (queryTimer) {
    clearTimeout(queryTimer);
    queryTimer = undefined;
  }
  loading.value = false;
  cmdbLoading.value = false;
  records.value = [];
  fields.value = [];
  brandModelOptions.value = {};
  total.value = 0;
  cmdbPickerOpen.value = false;
  deleteDialogOpen.value = false;
  discardDialogOpen.value = false;
  pendingDiscardAction = null;
  creatingNewFollowup.value = false;
  editingRecordId.value = "";
  followupFocusRecordId.value = "";
  selectedRecord.value = null;
  cmdbRecordIds.value = [];
  message.value = "";
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

function requestRefresh(): void {
  runWithDirtyGuard(() => {
    setDirty(false);
    void loadRecords(true);
  });
}

async function loadRecords(announce = false): Promise<void> {
  if (!props.summaryRecordId) {
    records.value = [];
    fields.value = [];
    total.value = 0;
    emit("count-changed", 0);
    return;
  }
  const requestVersion = ++recordsRequestVersion;
  const summaryRecordId = props.summaryRecordId;
  const scope = props.scope || "ALL";
  recordsAbortController?.abort();
  const abortController = new AbortController();
  recordsAbortController = abortController;
  loading.value = true;
  try {
    const params = new URLSearchParams({
      scope,
      summary_record_id: summaryRecordId,
      q: followupQuery.value,
      limit: String(PAGE_SIZE),
      offset: String((page.value - 1) * PAGE_SIZE),
    });
    if (followupFocusRecordId.value) {
      params.set("focus_record_id", followupFocusRecordId.value);
    }
    if (announce) params.set("refresh", "1");
    const requestUrl = `/api/repair-management/followups?${params.toString()}`;
    const cacheKey = requestUrl.replace(/([?&])refresh=1(?:&|$)/, "$1").replace(/[?&]$/, "");
    const cached = getRepairFollowupCache(cacheKey);
    let payload: LooseDict;
    if (!announce && cached) {
      payload = cached;
    } else {
      payload = await requestJson(requestUrl, { signal: abortController.signal });
      setRepairFollowupCache(cacheKey, payload);
    }
    if (
      requestVersion !== recordsRequestVersion
      || summaryRecordId !== props.summaryRecordId
      || scope !== (props.scope || "ALL")
    ) return;
    records.value = Array.isArray(payload.records) ? payload.records : [];
    fields.value = Array.isArray(payload.fields) ? payload.fields : [];
    brandModelOptions.value = payload.brand_model_options && typeof payload.brand_model_options === "object"
      ? payload.brand_model_options as Record<string, string[]>
      : {};
    total.value = Number(payload.total || records.value.length || 0);
    emit("count-changed", total.value);
    if (followupFocusRecordId.value) {
      page.value = Math.floor(Number(payload.offset || 0) / PAGE_SIZE) + 1;
    }
    const maxPage = Math.max(1, Math.ceil(total.value / PAGE_SIZE));
    if (page.value > maxPage) {
      page.value = maxPage;
      loading.value = false;
      await loadRecords(announce);
      return;
    }
    if (editingRecordId.value) {
      const current = records.value.find((item) => String(item.record_id || "") === editingRecordId.value);
      if (current && !followupDirty.value) {
        selectRecord(current);
      } else if (!current) {
        if (followupFocusRecordId.value) followupFocusRecordId.value = "";
        if (!followupDirty.value) {
          if (records.value.length) selectRecord(records.value[0]);
          else startCreate();
        }
      }
    } else if (!creatingNewFollowup.value && records.value.length) {
      selectRecord(records.value[0]);
    } else if (!records.value.length) {
      creatingNewFollowup.value = true;
      editingRecordId.value = "";
      selectedRecord.value = null;
      clearDraft();
      setDirty(false);
    } else {
      clearDraft();
    }
    if (announce) showMessage(records.value.length ? "跟进记录已刷新。" : "暂无跟进记录。", records.value.length ? "success" : "warning");
  } catch (error: unknown) {
    if (
      requestVersion !== recordsRequestVersion
      || summaryRecordId !== props.summaryRecordId
      || scope !== (props.scope || "ALL")
    ) return;
    showMessage(error instanceof Error ? error.message : "维修跟进读取失败。", "failed");
  } finally {
    if (recordsAbortController === abortController) recordsAbortController = null;
    if (requestVersion === recordsRequestVersion) loading.value = false;
  }
}

function buildFields(): LooseDict {
  const payload: LooseDict = {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    if (editingRecordId.value && !dirtyFieldNames.has(name)) continue;
    if (name === WORKER_FIELD_NAME) {
      if (workerPeople.value.length || editingRecordId.value) {
        payload[name] = workerPeople.value.map((person) => ({
          id: String(person.user_id || person.id || person.open_id || "").trim(),
        })).filter((person) => person.id);
      }
      continue;
    }
    const value = String(draft[name] ?? "");
    if (!value.trim() && !editingRecordId.value) continue;
    payload[name] = parseRepairDraftValue(value, field);
  }
  return payload;
}

function handlePrimaryAction(): void {
  if (primaryActionMode.value === "create") {
    requestStartCreate();
    return;
  }
  void saveRecord();
}

async function saveRecord(): Promise<void> {
  if (saving.value || !props.summaryRecordId) return;
  const wasEditing = Boolean(editingRecordId.value);
  if (wasEditing && !followupDirty.value) {
    showMessage("当前跟进没有需要保存的修改。", "warning");
    return;
  }
  if (!wasEditing && !hasDraftContent.value) {
    showMessage("请至少填写一项跟进内容。", "warning");
    return;
  }
  saving.value = true;
  try {
    if (!editingRecordId.value && !createOperationId.value) {
      createOperationId.value = createRepairOperationId("repair-followup");
    }
    const body = JSON.stringify({
      operation_id: editingRecordId.value ? "" : createOperationId.value,
      scope: props.scope || "ALL",
      summary_record_id: props.summaryRecordId,
      cmdb_record_ids: cmdbRecordIds.value,
      fields: buildFields(),
    });
    const payload = wasEditing
      ? await requestJson(`/api/repair-management/followups/${encodeURIComponent(editingRecordId.value)}`, {
          method: "PUT",
          body,
        })
      : await requestJson("/api/repair-management/followups", { method: "POST", body });
    editingRecordId.value = String(payload.record_id || editingRecordId.value || "");
    if (!wasEditing) {
      createOperationId.value = "";
      followupFocusRecordId.value = editingRecordId.value;
    }
    setDirty(false);
    if (!wasEditing) page.value = 1;
    const warnings = Array.isArray(payload.warnings)
      ? payload.warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
      : [];
    const successText = wasEditing ? "维修跟进记录已更新。" : "维修跟进记录已新增。";
    showMessage(
      warnings.length ? `${successText}${warnings.join("；")}` : successText,
      warnings.length ? "warning" : "success",
    );
    clearRepairFollowupCache(props.summaryRecordId);
    await loadRecords(false);
    emit("changed");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修跟进保存失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function requestDeleteRecord(): void {
  if (saving.value || !editingRecordId.value || !props.summaryRecordId) return;
  deleteDialogOpen.value = true;
}

async function resolveDeleteConfirmation(confirmed: boolean): Promise<void> {
  deleteDialogOpen.value = false;
  if (confirmed) await deleteRecordNow();
}

async function deleteRecordNow(): Promise<void> {
  if (saving.value || !editingRecordId.value || !props.summaryRecordId) return;
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
    clearRepairFollowupCache(props.summaryRecordId);
    await loadRecords(false);
    emit("changed");
    showMessage("维修跟进已删除。", "success");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修跟进删除失败。", "failed");
  } finally {
    saving.value = false;
  }
}

async function loadCmdbCandidates(resetLimit = true): Promise<void> {
  const previousCount = cmdbCandidates.value.length;
  if (resetLimit) {
    cmdbCandidateLimit.value = CMDB_CANDIDATE_BATCH_SIZE;
    cmdbCandidateNote.value = "";
  } else if (cmdbCandidateLimit.value < CMDB_CANDIDATE_MAX_LIMIT) {
    cmdbCandidateLimit.value = Math.min(
      CMDB_CANDIDATE_MAX_LIMIT,
      cmdbCandidateLimit.value + CMDB_CANDIDATE_BATCH_SIZE,
    );
  } else {
    cmdbCandidatesHaveMore.value = false;
    return;
  }
  const requestedLimit = cmdbCandidateLimit.value;
  const requestVersion = ++cmdbRequestVersion;
  cmdbAbortController?.abort();
  const abortController = new AbortController();
  cmdbAbortController = abortController;
  cmdbLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      q: cmdbQuery.value,
      limit: String(requestedLimit),
    });
    const payload = await requestJson(
      `/api/repair-management/cmdb-candidates?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (requestVersion !== cmdbRequestVersion) return;
    cmdbCandidates.value = Array.isArray(payload.records) ? payload.records : [];
    const loadedMore = cmdbCandidates.value.length > previousCount;
    const moreAvailable = candidateHasMore(
      payload,
      cmdbCandidates.value.length,
      requestedLimit,
    );
    const reachedVisibleLimit = moreAvailable
      && (requestedLimit >= CMDB_CANDIDATE_MAX_LIMIT || (!resetLimit && !loadedMore));
    cmdbCandidatesHaveMore.value = moreAvailable && !reachedVisibleLimit;
    cmdbCandidateNote.value = reachedVisibleLimit
      ? "候选较多，当前已到显示上限，请输入关键词缩小范围。"
      : "";
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (requestVersion !== cmdbRequestVersion) return;
    showMessage(error instanceof Error ? error.message : "CMDB 设备读取失败。", "failed");
  } finally {
    if (cmdbAbortController === abortController) cmdbAbortController = null;
    if (requestVersion === cmdbRequestVersion) cmdbLoading.value = false;
  }
}

async function openCmdbPicker(): Promise<void> {
  cmdbPickerOpen.value = true;
  await loadCmdbCandidates(true);
}

function confirmCmdb(recordIds: string[]): void {
  cmdbRecordIds.value = Array.from(new Set(
    recordIds.map((item) => String(item || "").trim()).filter(Boolean),
  ));
  cmdbPickerOpen.value = false;
  const selectedRecords = cmdbRecordIds.value.map((recordId) => (
    cmdbCandidates.value.find((item) => String(item.record_id || "") === recordId)
  )).filter(Boolean) as LooseDict[];
  const categories = Array.from(new Set(
    selectedRecords.map((item) => String(item.category || "").trim()).filter(Boolean),
  ));
  const deviceNames = Array.from(new Set(
    selectedRecords.map((item) => String(item.title || "").trim()).filter(Boolean),
  ));
  if (!String(draft[DEVICE_NAME_FIELD_NAME] || "").trim() && categories.length === 1) {
    const category = categories[0];
    const deviceNameField = fields.value.find(
      (field) => String(field.field_name || "") === DEVICE_NAME_FIELD_NAME,
    );
    const options = Array.isArray(deviceNameField?.options) ? deviceNameField.options : [];
    if (!options.length || options.includes(category)) {
      draft[DEVICE_NAME_FIELD_NAME] = category;
      dirtyFieldNames.add(DEVICE_NAME_FIELD_NAME);
    } else {
      showMessage(`设备分类“${category}”不是跟进表可选项，请手动选择设备名称。`, "warning");
    }
  } else if (!String(draft[DEVICE_NAME_FIELD_NAME] || "").trim() && categories.length > 1) {
    showMessage("已选择不同分类的设备，请手动确认设备名称。", "warning");
  }
  if (!String(draft[DEVICE_NUMBER_FIELD_NAME] || "").trim() && deviceNames.length) {
    draft[DEVICE_NUMBER_FIELD_NAME] = deviceNames.join("、");
    dirtyFieldNames.add(DEVICE_NUMBER_FIELD_NAME);
  }
  if (!selectedRecords.length && cmdbRecordIds.value.length) {
    showMessage(
      `已选择 ${cmdbRecordIds.value.length} 台设备，保存时由后端读取完整设备信息。`,
      "success",
    );
  }
  markDirty();
}

watch(
  () => [props.summaryRecordId, props.scope] as const,
  () => {
    if (queryTimer) {
      clearTimeout(queryTimer);
      queryTimer = undefined;
    }
    page.value = 1;
    if (followupQuery.value) {
      skipNextQueryReload = true;
      followupQuery.value = "";
    }
    resetForParent();
    void loadRecords(false);
  },
  { immediate: true, flush: "sync" },
);

watch(followupQuery, () => {
  if (skipNextQueryReload) {
    skipNextQueryReload = false;
    return;
  }
  if (queryTimer) clearTimeout(queryTimer);
  queryTimer = setTimeout(() => {
    page.value = 1;
    void loadRecords(false);
  }, 300);
}, { flush: "sync" });

onBeforeUnmount(() => {
  recordsRequestVersion += 1;
  cmdbRequestVersion += 1;
  recordsAbortController?.abort();
  recordsAbortController = null;
  cmdbAbortController?.abort();
  cmdbAbortController = null;
  if (queryTimer) clearTimeout(queryTimer);
});
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

.cmdb-line span {
  color: #647b97;
  font-size: 12px;
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

/* Compact VNET workbench layout. */
.followup-panel {
  gap: 10px;
  padding: 12px 14px 16px;
  border: 1px solid #d8e5f7;
  border-top: 1px solid #d8e5f7;
  border-radius: 12px;
  box-shadow: 0 10px 26px rgba(20, 75, 150, 0.08);
}

.followup-panel.embedded {
  padding: 0;
}

.followup-head {
  min-height: 34px;
}

.followup-title strong {
  font-size: 14px;
  font-weight: 700;
}

.followup-count {
  min-height: 24px;
  background: #edf4ff;
  color: #1658b5;
  font-weight: 700;
}

.followup-workspace {
  min-width: 0;
  display: grid;
  grid-template-columns: 300px minmax(0, 1fr);
  align-content: start;
  gap: 12px;
  padding-top: 7px;
  border-top: 1px solid #e1eaf5;
}

.followup-timeline {
  min-width: 0;
  max-height: min(620px, calc(100vh - 250px));
  position: sticky;
  top: 12px;
  align-self: start;
  display: grid;
  grid-template-rows: auto auto minmax(140px, 1fr) auto;
  overflow: hidden;
  border: 1px solid #d7e3f2;
  border-radius: 10px;
  background: #f8fbff;
}

.followup-timeline-head {
  min-height: 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  border-bottom: 1px solid #e0e9f4;
  padding: 0 10px;
  background: #fff;
}

.followup-timeline-head strong {
  color: #17314f;
  font-size: 13px;
}

.followup-timeline-head span {
  color: #68809a;
  font-size: 11px;
  font-weight: 700;
}

.followup-timeline-tools {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 36px;
  gap: 7px;
  border-bottom: 1px solid #e3ebf5;
  padding: 8px;
}

.followup-timeline-search {
  min-width: 0;
  min-height: 36px;
  display: flex;
  align-items: center;
  gap: 6px;
  border: 1px solid #d1deed;
  border-radius: 8px;
  padding: 0 8px;
  background: #fff;
  color: #5e7692;
}

.followup-timeline-search:focus-within {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.1);
}

.followup-timeline-search input {
  min-width: 0;
  width: 100%;
  border: 0;
  outline: 0;
  background: transparent;
  color: #17314f;
  font: inherit;
  font-size: 12px;
}

.followup-timeline-tools .icon-button {
  width: 36px;
  min-height: 36px;
}

.followup-timeline-list {
  min-height: 140px;
  overflow-y: auto;
  overscroll-behavior: contain;
  display: grid;
  align-content: start;
  gap: 4px;
  padding: 7px;
}

.followup-timeline-list > button {
  width: 100%;
  min-height: 52px;
  display: grid;
  grid-template-columns: 22px minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  border: 1px solid transparent;
  border-radius: 8px;
  padding: 6px 8px;
  background: #fff;
  color: #17314f;
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.followup-timeline-list > button:hover,
.followup-timeline-list > button.active,
.followup-timeline-list > button:focus-visible {
  border-color: #b7cdf0;
  outline: 0;
  background: #edf4ff;
}

.followup-timeline-list > button.outside-filter {
  border-color: #efc47d;
  background: #fff8eb;
}

.timeline-marker {
  width: 20px;
  height: 20px;
  display: grid;
  place-items: center;
  border: 2px solid #b8c9db;
  border-radius: 50%;
  background: #fff;
  color: #fff;
}

.followup-timeline-list > button.active .timeline-marker {
  border-color: #1464e7;
  background: #1464e7;
}

.timeline-copy {
  min-width: 0;
  display: grid;
  gap: 3px;
}

.timeline-copy strong,
.timeline-copy small {
  overflow: hidden;
  text-overflow: ellipsis;
}

.timeline-copy strong {
  display: -webkit-box;
  font-size: 13px;
  font-weight: 700;
  line-height: 1.35;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.timeline-copy small {
  white-space: nowrap;
  color: #70839a;
  font-size: 11px;
}

.timeline-copy .timeline-filter-note {
  color: #a05d14;
  font-weight: 700;
}

.followup-timeline-list > button > b {
  color: #1658b5;
  font-size: 11px;
  white-space: nowrap;
}

.followup-pager {
  margin: 0;
  border-top: 1px solid #e1eaf5;
  padding: 7px;
}

.followup-pager button {
  border-color: #d4e0ee;
  border-radius: 7px;
  background: #f7faff;
  color: #1758b5;
}

.followup-editor {
  gap: 7px;
  scroll-padding-bottom: 84px;
}

.followup-editor-head {
  min-height: 24px;
}

.followup-editor-head strong {
  font-size: 14px;
  font-weight: 750;
}

.cmdb-line {
  padding: 6px 9px;
  border-color: #d6e3f2;
  border-radius: 9px;
  background: #f7faff;
}

.followup-field-section {
  gap: 5px;
  padding: 0 0 7px;
  border-bottom-color: #e2ebf5;
}

.followup-field-section > header strong {
  color: #17314f;
  font-size: 13px;
  font-weight: 750;
}

.followup-field-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 7px 10px;
}

.followup-field-section:last-child {
  scroll-margin-bottom: 84px;
}

.followup-action-bar {
  position: sticky;
  z-index: 20;
  bottom: 8px;
  min-height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  border: 1px solid #d5e2f2;
  border-radius: 10px;
  padding: 5px 8px 5px 10px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 10px 26px rgba(21, 67, 128, 0.13);
  backdrop-filter: blur(10px);
}

.followup-action-bar > div,
.followup-save-state,
.followup-button {
  display: inline-flex;
  align-items: center;
}

.followup-action-bar > div:last-child {
  gap: 7px;
}

.followup-save-state {
  gap: 7px;
  color: #657a93;
  font-size: 12px;
  font-weight: 700;
}

.followup-save-state.saving { color: #155ec2; }
.followup-save-state.dirty { color: #a65314; }
.followup-save-state.saved { color: #16805f; }
.followup-save-state.saving svg { animation: followup-spin 900ms linear infinite; }

.followup-button {
  min-height: 36px;
  justify-content: center;
  gap: 6px;
  border-radius: 8px;
  padding: 0 12px;
  font-size: 13px;
  font-weight: 700;
}

.followup-button.primary {
  border-color: #1464e7;
  background: #1464e7;
}

.followup-button.quiet {
  border-color: #d3dfed;
  color: #1e538f;
}

.followup-button.icon-button {
  width: 40px;
  padding: 0;
}

.spinning {
  animation: followup-spin 900ms linear infinite;
}

@keyframes followup-spin {
  to { transform: rotate(360deg); }
}

@media (max-height: 900px) {
  .followup-action-bar {
    position: static;
  }
}

@media (max-width: 1279px) and (min-width: 1024px) {
  .followup-workspace {
    grid-template-columns: 260px minmax(0, 1fr);
  }

  .followup-field-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 1023px) and (min-width: 761px) {
  .followup-workspace {
    grid-template-columns: 1fr;
  }

  .followup-timeline {
    position: static;
    max-height: 300px;
  }

  .followup-field-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .followup-action-bar {
    align-items: stretch;
    flex-direction: column;
  }

  .followup-action-bar > div:last-child {
    justify-content: flex-start;
  }

  .followup-workspace {
    grid-template-columns: 1fr;
  }

  .followup-timeline {
    position: static;
    max-height: 300px;
  }

  .followup-field-grid {
    grid-template-columns: 1fr;
  }
}
</style>
