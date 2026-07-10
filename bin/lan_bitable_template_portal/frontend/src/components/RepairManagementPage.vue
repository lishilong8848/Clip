<template>
  <section class="repair-management-page">
    <div class="page-back-row">
      <VnetBackButton @click="requestBack" />
    </div>

    <div class="repair-hero">
      <div>
        <h2>维修项目与跟进</h2>
      </div>
      <div class="hero-actions">
        <button type="button" class="btn secondary" :disabled="loading" @click="loadRecords(true)">
          {{ loading ? "读取中" : "刷新" }}
        </button>
        <button type="button" class="btn primary" @click="requestStartCreate">
          清空表单
        </button>
      </div>
    </div>

    <MessageBanner v-if="messageText" :tone="messageTone" :text="messageText" />

    <div class="repair-layout">
      <aside class="record-panel">
        <div class="panel-head">
          <div>
            <strong>维修项目列表</strong>
            <span>{{ total }} 条</span>
          </div>
          <div class="record-search">
            <input v-model.trim="searchText" type="search" placeholder="搜索维修项目" />
            <button v-if="searchText" type="button" class="search-clear" @click="clearSearch">清空</button>
          </div>
        </div>
        <div class="record-list" :aria-busy="loading">
          <div v-if="loading" class="empty-state">正在读取维修项目...</div>
          <div v-else-if="!records.length" class="empty-state">暂无维修项目</div>
          <button
            v-for="record in records"
            v-else
            :key="record.record_id"
            type="button"
            class="record-row"
            :class="{ active: selectedRecord?.record_id === record.record_id }"
            :aria-current="selectedRecord?.record_id === record.record_id ? 'true' : undefined"
            @click="requestSelectRecord(record)"
          >
            <span class="record-title">{{ record.title || "未命名维修项目" }}</span>
            <span class="record-meta">
              <b>{{ recordBuildingLabel(record) }}</b>
              <b>{{ recordSpecialtyLabel(record) }}</b>
              <b>{{ recordTimeLabel(record) }}</b>
            </span>
          </button>
        </div>
        <nav v-if="recordPageCount > 1" class="pager" aria-label="维修项目分页">
          <button type="button" :disabled="loading || recordPage <= 1" @click="changeRecordPage(-1)">上一页</button>
          <span>{{ recordPage }} / {{ recordPageCount }}</span>
          <button type="button" :disabled="loading || recordPage >= recordPageCount" @click="changeRecordPage(1)">下一页</button>
        </nav>
      </aside>

      <main class="editor-panel">
        <header class="workspace-tabs-row">
          <nav class="workspace-tabs" aria-label="维修项目工作区">
            <button
              type="button"
              :class="{ active: activeWorkspaceTab === 'project' }"
              @click="activeWorkspaceTab = 'project'"
            >
              项目信息
              <span v-if="hasUnsavedChanges" class="unsaved-dot">未保存</span>
            </button>
            <button
              type="button"
              :class="{ active: activeWorkspaceTab === 'followups' }"
              :disabled="!editingRecordId"
              @click="activeWorkspaceTab = 'followups'"
            >
              跟进记录
              <span v-if="followupHasUnsavedChanges" class="unsaved-dot">未保存</span>
            </button>
          </nav>
        </header>

        <div v-show="activeWorkspaceTab === 'project'" class="project-workspace">
          <section class="source-relation-panel" :class="{ collapsed: !sourceExpanded }">
            <header class="source-relation-head">
              <h3>来源关系</h3>
              <div class="source-head-actions">
                <button type="button" class="btn quiet compact" @click="sourceExpanded = !sourceExpanded">
                  {{ sourceExpanded ? "收起" : "更改来源" }}
                </button>
              </div>
            </header>
            <div v-if="sourceExpanded" class="source-relation-grid">
              <div class="source-relation-item">
                <span class="relation-order">1</span>
                <div><b>关联事件单</b><small>{{ selectedEventSummary }}</small></div>
                <button type="button" class="btn secondary" @click="openSourcePicker('event')">
                  {{ sourceEventId ? "重新选择" : "选择事件" }}
                </button>
              </div>
              <div class="source-relation-item">
                <span class="relation-order">2</span>
                <div><b>设备检修关联</b><small>{{ selectedRepairSummary }}</small></div>
                <button type="button" class="btn secondary" @click="openSourcePicker('repair')">
                  {{ selectedRepairIds.length ? "重新选择" : "选择检修通告" }}
                </button>
              </div>
            </div>
            <div v-else class="source-summary-row">
              <span><b>事件</b>{{ selectedEventSummary }}</span>
              <span><b>检修</b>{{ selectedRepairSummary }}</span>
            </div>
            <div v-if="prefillWarnings.length" class="prefill-warning">{{ prefillWarnings.join("；") }}</div>
          </section>

          <header class="editor-head">
            <div>
              <h3>{{ editingRecordId ? selectedRecordTitle : "填写维修项目" }}</h3>
              <p v-if="eventTitle">来自事件：{{ eventTitle }}</p>
            </div>
            <div class="editor-actions">
              <button v-if="editingRecordId" type="button" class="btn secondary" @click="requestOpenRepairNoticeWorkbench">
                检修通告
              </button>
              <button type="button" class="btn quiet" :disabled="saving" @click="requestResetDraft">重置</button>
              <button v-if="editingRecordId" type="button" class="btn danger" :disabled="saving" @click="requestDeleteRecord">
                删除
              </button>
              <button type="button" class="btn primary" :disabled="saving || !canSaveRecord" @click="saveRecord">
                {{ saving ? "保存中" : editingRecordId ? "保存修改" : "保存维修单" }}
              </button>
            </div>
          </header>

          <div v-if="missingRequiredEditableFields.length" class="form-warning">
            还缺 {{ missingRequiredEditableFields.length }} 项：{{ missingRequiredEditableFields.join("、") }}
          </div>

          <div v-if="!fields.length && loading" class="empty-state">正在读取字段...</div>
          <div v-else-if="!editableFields.length" class="empty-state">暂无可填写字段</div>
          <div v-else class="field-grid">
            <label
              v-for="field in editableFields"
              :key="field.field_name"
              class="field-card"
              :class="{ required: isRequiredField(field.field_name) && !editingRecordId }"
            >
              <span><b>{{ field.field_name }}</b><small v-if="fieldBadge(field)">{{ fieldBadge(field) }}</small></span>
              <select
                v-if="field.options?.length"
                v-model="fieldDraft[field.field_name]"
                :required="!editingRecordId && isRequiredField(field.field_name)"
                @change="markFieldDirty(field.field_name)"
              >
                <option value="">请选择{{ field.field_name }}</option>
                <option
                  v-if="fieldDraft[field.field_name] && !field.options.includes(fieldDraft[field.field_name])"
                  :value="fieldDraft[field.field_name]"
                >{{ fieldDraft[field.field_name] }}</option>
                <option v-for="option in field.options" :key="option" :value="option">{{ option }}</option>
              </select>
              <input
                v-else-if="isDateField(field)"
                v-model="fieldDraft[field.field_name]"
                type="datetime-local"
                :required="!editingRecordId && isRequiredField(field.field_name)"
                @input="markFieldDirty(field.field_name)"
              />
              <input
                v-else-if="isNumberField(field)"
                v-model="fieldDraft[field.field_name]"
                type="number"
                step="any"
                :required="!editingRecordId && isRequiredField(field.field_name)"
                @input="markFieldDirty(field.field_name)"
              />
              <textarea
                v-else-if="usesTextarea(field.field_name)"
                v-model="fieldDraft[field.field_name]"
                placeholder="填写字段内容"
                :required="!editingRecordId && isRequiredField(field.field_name)"
                rows="2"
                @input="markFieldDirty(field.field_name)"
              />
              <input
                v-else
                v-model="fieldDraft[field.field_name]"
                type="text"
                placeholder="填写字段内容"
                :required="!editingRecordId && isRequiredField(field.field_name)"
                @input="markFieldDirty(field.field_name)"
              />
            </label>
          </div>
          <details v-if="readonlyPreviewFields.length" class="readonly-summary" :open="!editingRecordId">
            <summary>自动填写字段（{{ visibleReadonlyFields.length }} 项）</summary>
            <div class="readonly-grid">
              <div v-for="field in readonlyPreviewFields" :key="field.field_name" class="readonly-line">
                <b>{{ field.field_name }}</b>
                <span>{{ displayReadonlyValue(field.field_name) || "未填写" }}</span>
              </div>
            </div>
          </details>
        </div>

        <RepairFollowupPanel
          v-if="editingRecordId"
          v-show="activeWorkspaceTab === 'followups'"
          embedded
          :scope="scope"
          :summary-record-id="editingRecordId"
          :summary-title="selectedRecordTitle"
          @changed="handleFollowupChanged"
          @dirty-changed="followupHasUnsavedChanges = $event"
        />
      </main>
    </div>

    <ConfirmDialog
      :open="confirmDialog.open"
      :tone="confirmDialog.tone"
      :kicker="confirmDialog.kicker"
      :title="confirmDialog.title"
      :message="confirmDialog.message"
      :details="confirmDialog.details"
      :confirm-label="confirmDialog.confirmLabel"
      @resolve="resolveConfirmation"
    />

    <RecordPickerDialog
      :open="activePicker === 'event'"
      title="选择关联事件单"
      :records="eventCandidates"
      :columns="eventPickerColumns"
      :selected-ids="sourceEventId ? [sourceEventId] : []"
      :multiple="false"
      :loading="eventLoading"
      :query="eventSearchText"
      search-placeholder="搜索事件标题、楼栋、专业、来源"
      @update:query="eventSearchText = $event"
      @search="loadEventCandidates"
      @close="activePicker = ''"
      @confirm="confirmEventSelection"
    />
    <RecordPickerDialog
      :open="activePicker === 'repair'"
      title="选择设备检修通告"
      :records="repairCandidates"
      :columns="repairPickerColumns"
      :selected-ids="repairPickerSelectedIds"
      :multiple="false"
      :loading="repairCandidateLoading"
      :query="repairSearchText"
      search-placeholder="搜索检修标题、设备、故障、位置"
      @update:query="repairSearchText = $event"
      @search="loadRepairCandidates"
      @close="activePicker = ''"
      @confirm="confirmRepairSelection"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { requestJson } from "../api/client";
import { navigate, navigateHard } from "../navigation";
import {
  REPAIR_REQUIRED_FIELD_GROUPS,
  isRequiredRepairField as isRequiredField,
  parseRepairDraftValue as parseDraftValue,
  repairDraftInputValue,
  repairEventRecordId as eventRecordId,
  repairFieldUsesTextarea,
  repairFieldValueToText as fieldValueToText,
  repairRecordBuildingLabel as recordBuildingLabel,
  repairRecordSpecialtyLabel as recordSpecialtyLabel,
  repairRecordTimeLabel as recordTimeLabel,
  sortedRepairFields as sortedFields,
} from "../repairManagementUtils";
import type { LooseDict, ScopeOption } from "../types";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
import RepairFollowupPanel from "./RepairFollowupPanel.vue";
import RecordPickerDialog from "./RecordPickerDialog.vue";
import VnetBackButton from "./VnetBackButton.vue";

type PickerColumn = { key: string; label: string; width?: string };
type SourcePicker = "" | "event" | "repair";
type WorkspaceTab = "project" | "followups";
type ConfirmTone = "warning" | "danger" | "primary";

const RECORD_PAGE_SIZE = 30;

const ASSOCIATION_FIELD_NAMES = new Set(["关联事件单", "设备检修关联", "维修跟进记录"]);
const eventPickerColumns: PickerColumn[] = [
  { key: "title", label: "事件标题", width: "360px" },
  { key: "building", label: "楼栋", width: "100px" },
  { key: "specialty", label: "专业", width: "100px" },
  { key: "level", label: "等级", width: "90px" },
  { key: "source", label: "来源", width: "120px" },
  { key: "status", label: "状态", width: "120px" },
  { key: "occurrence_time", label: "事件发生时间", width: "170px" },
];
const repairPickerColumns: PickerColumn[] = [
  { key: "title", label: "检修概述", width: "420px" },
  { key: "specialty", label: "专业", width: "100px" },
  { key: "building", label: "楼栋", width: "110px" },
  { key: "status", label: "检修状态", width: "110px" },
  { key: "location", label: "位置", width: "180px" },
  { key: "urgency", label: "紧急程度", width: "100px" },
  { key: "repair_device", label: "维修设备", width: "240px" },
  { key: "actual_start_time", label: "实际开始时间", width: "170px" },
];

const props = defineProps<{
  scope: string;
  scopeOptions: ScopeOption[];
}>();

const loading = ref(false);
const saving = ref(false);
const searchText = ref("");
const messageText = ref("");
const messageTone = ref<"success" | "warning" | "failed">("success");
const fields = ref<LooseDict[]>([]);
const records = ref<LooseDict[]>([]);
const total = ref(0);
const selectedRecord = ref<LooseDict | null>(null);
const editingRecordId = ref("");
const fieldDraft = reactive<Record<string, string>>({});
const eventLoading = ref(false);
const eventSearchText = ref("");
const eventCandidates = ref<LooseDict[]>([]);
const selectedEvent = ref<LooseDict | null>(null);
const routeEventPrefillApplied = ref(false);
const repairCandidateLoading = ref(false);
const repairSearchText = ref("");
const repairCandidates = ref<LooseDict[]>([]);
const selectedRepairIds = ref<string[]>([]);
const repairRecommendedIds = ref<string[]>([]);
const activePicker = ref<SourcePicker>("");
const prefillLoading = ref(false);
const prefillWarnings = ref<string[]>([]);
const prefillPreview = reactive<Record<string, unknown>>({});
const dirtyFieldNames = new Set<string>();
const activeWorkspaceTab = ref<WorkspaceTab>("project");
const sourceExpanded = ref(true);
const hasUnsavedChanges = ref(false);
const followupHasUnsavedChanges = ref(false);
const creatingNewProject = ref(false);
const recordPage = ref(1);
const confirmDialog = reactive({
  open: false,
  tone: "warning" as ConfirmTone,
  kicker: "请确认",
  title: "",
  message: "",
  details: [] as string[],
  confirmLabel: "确认",
});
let pendingConfirmAction: null | (() => void | Promise<void>) = null;
let searchTimer: ReturnType<typeof setTimeout> | undefined;

const routeParams = new URLSearchParams(window.location.search);
const routeEventTitle = String(routeParams.get("event_title") || "").trim();
const routeEventId = String(routeParams.get("from_event_record_id") || "").trim();
const eventTitle = ref(routeEventTitle);
const sourceEventId = ref(routeEventId);

const editableFields = computed(() => sortedFields(fields.value.filter((field) => field.editable)));
const readonlyFields = computed(() => sortedFields(fields.value.filter((field) => !field.editable)));
const visibleReadonlyFields = computed(() => readonlyFields.value.filter(
  (field) => !ASSOCIATION_FIELD_NAMES.has(String(field.field_name || "")),
));
const readonlyPreviewFields = computed(() => {
  return visibleReadonlyFields.value;
});
const hasSelectedSources = computed(() => Boolean(
  sourceEventId.value || selectedRepairIds.value.length,
));
const repairPickerSelectedIds = computed(() => (
  selectedRepairIds.value.length ? selectedRepairIds.value.slice(0, 1) : repairRecommendedIds.value.slice(0, 1)
));
const selectedEventSummary = computed(() => {
  if (!sourceEventId.value) return "未选择";
  return String(selectedEvent.value?.title || eventTitle.value || "已关联");
});
const selectedRepairSummary = computed(() => selectedSourceSummary(
  selectedRepairIds.value,
  repairCandidates.value,
  "title",
));

const hasWritableDraft = computed(() => {
  return fields.value.some((field) => {
    if (!field.editable) return false;
    return String(fieldDraft[field.field_name] ?? "").trim() !== "";
  });
});
const missingRequiredEditableFields = computed(() => {
  if (editingRecordId.value) return [];
  const editableNames = new Set(editableFields.value.map((field) => String(field.field_name || "")));
  const missing: string[] = [];
  for (const group of REPAIR_REQUIRED_FIELD_GROUPS) {
    const available = group.filter((name) => editableNames.has(name));
    if (!available.length) continue;
    if (!available.some((name) => String(fieldDraft[name] ?? "").trim())) {
      missing.push(available[0]);
    }
  }
  return missing;
});
const canSaveRecord = computed(() => {
  if (saving.value || !hasWritableDraft.value) return false;
  return missingRequiredEditableFields.value.length === 0;
});

const selectedRecordTitle = computed(() => selectedRecord.value?.title || "未命名维修项目");
const recordPageCount = computed(() => Math.max(1, Math.ceil(total.value / RECORD_PAGE_SIZE)));

function fieldBadge(field: LooseDict): string {
  const fieldName = String(field.field_name || "");
  return !editingRecordId.value && isRequiredField(fieldName) ? "必填" : "";
}

function isDateField(field: LooseDict): boolean {
  return String(field.ui_type || "").toLowerCase().includes("datetime");
}

function isNumberField(field: LooseDict): boolean {
  return String(field.ui_type || "").toLowerCase() === "number";
}

function usesTextarea(fieldName: unknown): boolean {
  return repairFieldUsesTextarea(fieldName);
}

function selectedSourceSummary(ids: string[], candidates: LooseDict[], preferredKey: string): string {
  if (!ids.length) return "未选择";
  const idSet = new Set(ids);
  const labels = candidates
    .filter((item) => idSet.has(String(item.record_id || "")))
    .map((item) => String(item[preferredKey] || item.title || "").trim())
    .filter(Boolean);
  if (!labels.length) return "已关联";
  const preview = labels.slice(0, 2).join("；");
  return labels.length > 2 ? `${preview} 等 ${labels.length} 条` : preview;
}

function markFieldDirty(fieldName: unknown): void {
  const name = String(fieldName || "").trim();
  if (!name) return;
  dirtyFieldNames.add(name);
  hasUnsavedChanges.value = true;
}

function scopedQuery(): string {
  return new URLSearchParams({
    scope: props.scope || "ALL",
    q: searchText.value,
    limit: String(RECORD_PAGE_SIZE),
    offset: String((recordPage.value - 1) * RECORD_PAGE_SIZE),
  }).toString();
}

function currentMonthKey(): string {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function showMessage(text: string, tone: "success" | "warning" | "failed" = "success"): void {
  messageText.value = text;
  messageTone.value = tone;
}

function openConfirmation(
  options: {
    tone?: ConfirmTone;
    kicker?: string;
    title: string;
    message: string;
    details?: string[];
    confirmLabel?: string;
  },
  action: () => void | Promise<void>,
): void {
  confirmDialog.tone = options.tone || "warning";
  confirmDialog.kicker = options.kicker || "请确认";
  confirmDialog.title = options.title;
  confirmDialog.message = options.message;
  confirmDialog.details = options.details || [];
  confirmDialog.confirmLabel = options.confirmLabel || "确认";
  pendingConfirmAction = action;
  confirmDialog.open = true;
}

async function resolveConfirmation(confirmed: boolean): Promise<void> {
  const action = pendingConfirmAction;
  confirmDialog.open = false;
  pendingConfirmAction = null;
  if (confirmed && action) await action();
}

function runWithUnsavedGuard(action: () => void | Promise<void>): void {
  if (!hasUnsavedChanges.value && !followupHasUnsavedChanges.value) {
    void action();
    return;
  }
  openConfirmation(
    {
      title: "放弃未保存修改？",
      message: "当前页面存在未保存内容，继续后这些修改会丢失。",
      details: ["已保存的维修项目和跟进记录不会受影响。"],
      confirmLabel: "放弃并继续",
    },
    action,
  );
}

function requestBack(): void {
  runWithUnsavedGuard(() => {
    dirtyFieldNames.clear();
    hasUnsavedChanges.value = false;
    followupHasUnsavedChanges.value = false;
    navigate("/");
  });
}

function requestOpenRepairNoticeWorkbench(): void {
  runWithUnsavedGuard(() => {
    dirtyFieldNames.clear();
    hasUnsavedChanges.value = false;
    followupHasUnsavedChanges.value = false;
    openRepairNoticeWorkbench();
  });
}

function requestResetDraft(): void {
  if (!hasUnsavedChanges.value) {
    resetDraft();
    return;
  }
  openConfirmation(
    {
      title: "重置当前修改？",
      message: "表单会恢复为最近一次保存的内容。",
      confirmLabel: "确认重置",
    },
    resetDraft,
  );
}

function handleBeforeUnload(event: BeforeUnloadEvent): void {
  if (!hasUnsavedChanges.value && !followupHasUnsavedChanges.value) return;
  event.preventDefault();
  event.returnValue = "";
}

function resetDraft(): void {
  dirtyFieldNames.clear();
  hasUnsavedChanges.value = false;
  if (editingRecordId.value && selectedRecord.value) {
    const savedDisplayFields = selectedRecord.value.display_fields || {};
    eventTitle.value = String(savedDisplayFields["事件描述"] || savedDisplayFields["故障维修原因"] || "");
    sourceEventId.value = String(selectedRecord.value.source_event_id || "").trim();
    selectedRepairIds.value = Array.isArray(selectedRecord.value.source_repair_ids)
      ? selectedRecord.value.source_repair_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
      : [];
    selectedEvent.value = sourceEventId.value
      ? { record_id: sourceEventId.value, title: eventTitle.value }
      : null;
  }
  for (const key of Object.keys(fieldDraft)) delete fieldDraft[key];
  for (const key of Object.keys(prefillPreview)) delete prefillPreview[key];
  const rawFields = selectedRecord.value?.raw_fields || {};
  const displayFields = selectedRecord.value?.display_fields || {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const rawValue = Object.prototype.hasOwnProperty.call(rawFields, name) ? rawFields[name] : displayFields[name];
    fieldDraft[name] = repairDraftInputValue(field, rawValue);
  }
  if (!editingRecordId.value && eventTitle.value) prefillEventTitle();
}

function applyPrefillFields(fieldsPayload: LooseDict, preserveDirty = true): void {
  if (!fieldsPayload || typeof fieldsPayload !== "object") return;
  for (const [name, value] of Object.entries(fieldsPayload)) {
    prefillPreview[name] = value;
    const field = fields.value.find((item) => String(item.field_name || "") === name);
    if (!field?.editable || (preserveDirty && dirtyFieldNames.has(name))) continue;
    fieldDraft[name] = repairDraftInputValue(field, value);
  }
}

function replacePrefillFields(fieldsPayload: LooseDict, preserveDirty = true): void {
  const previousNames = Object.keys(prefillPreview);
  for (const name of previousNames) {
    const field = fields.value.find((item) => String(item.field_name || "") === name);
    if (field?.editable && (!preserveDirty || !dirtyFieldNames.has(name))) {
      fieldDraft[name] = "";
    }
    delete prefillPreview[name];
  }
  applyPrefillFields(fieldsPayload, preserveDirty);
}

function prefillEventTitle(): void {
  const titleField = fields.value.find((field) => {
    const name = String(field.field_name || "");
    return field.editable && name === "维修名称";
  });
  if (titleField && !String(fieldDraft[titleField.field_name] || "").trim()) {
    fieldDraft[titleField.field_name] = eventTitle.value;
  }
}

async function loadEventCandidates(): Promise<void> {
  eventLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      q: eventSearchText.value,
      limit: "120",
    });
    const payload = await requestJson(`/api/repair-management/event-candidates?${params.toString()}`);
    eventCandidates.value = Array.isArray(payload.records) ? payload.records : [];
    if (!eventCandidates.value.length) {
      showMessage("当前条件下没有可选择的事件。", "warning");
    }
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "事件候选读取失败。", "failed");
  } finally {
    eventLoading.value = false;
  }
}

async function applyEventPrefill(recordId: string, quiet = false): Promise<void> {
  const cleanRecordId = String(recordId || "").trim();
  if (!cleanRecordId) return;
  try {
    await confirmEventSelection([cleanRecordId], quiet);
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "事件预填失败。", "failed");
  }
}

async function openSourcePicker(picker: Exclude<SourcePicker, "">): Promise<void> {
  activePicker.value = picker;
  if (picker === "event") await loadEventCandidates();
  if (picker === "repair") await loadRepairCandidates();
}

async function confirmEventSelection(recordIds: string[], quiet = false): Promise<void> {
  const recordId = String(recordIds[0] || "").trim();
  if (!recordId) return;
  const changed = recordId !== sourceEventId.value;
  const event = eventCandidates.value.find((item) => eventRecordId(item) === recordId) || {
    record_id: recordId,
    title: eventTitle.value,
  };
  sourceEventId.value = recordId;
  selectedEvent.value = event;
  eventTitle.value = String(event.title || eventTitle.value || "");
  activePicker.value = "";
  dirtyFieldNames.clear();
  prefillWarnings.value = [];
  if (changed) {
    selectedRepairIds.value = [];
    repairRecommendedIds.value = [];
  }
  await applyCombinedPrefill(true);
  hasUnsavedChanges.value = true;
  await loadRepairCandidates();
  if (!quiet) showMessage("事件已关联，可继续选择检修通告。", "success");
}

async function loadRepairCandidates(): Promise<void> {
  repairCandidateLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      event_record_id: sourceEventId.value,
      q: repairSearchText.value,
      limit: "80",
    });
    const payload = await requestJson(`/api/repair-management/repair-candidates?${params.toString()}`);
    repairCandidates.value = Array.isArray(payload.records) ? payload.records : [];
    repairRecommendedIds.value = Array.isArray(payload.auto_selected_ids)
      ? payload.auto_selected_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
      : [];
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "设备检修候选读取失败。", "failed");
  } finally {
    repairCandidateLoading.value = false;
  }
}

async function confirmRepairSelection(recordIds: string[]): Promise<void> {
  selectedRepairIds.value = recordIds.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 1);
  repairRecommendedIds.value = [];
  activePicker.value = "";
  await applyCombinedPrefill(true);
  hasUnsavedChanges.value = true;
  showMessage(`已关联 ${selectedRepairIds.value.length} 条设备检修通告。`, "success");
}

async function handleFollowupChanged(): Promise<void> {
  await loadRecords(false);
  const current = records.value.find(
    (item) => String(item.record_id || "") === editingRecordId.value,
  );
  if (current) selectedRecord.value = current;
}

async function applyCombinedPrefill(quiet = false): Promise<void> {
  if (!hasSelectedSources.value) return;
  prefillLoading.value = true;
  try {
    const payload = await requestJson("/api/repair-management/prefill", {
      method: "POST",
      body: JSON.stringify({
        scope: props.scope || "ALL",
        source_event_id: sourceEventId.value,
        source_repair_ids: selectedRepairIds.value,
        source_month: currentMonthKey(),
      }),
    });
    if (payload.event && typeof payload.event === "object") {
      const event = payload.event as LooseDict;
      selectedEvent.value = event;
      sourceEventId.value = eventRecordId(event) || sourceEventId.value;
      eventTitle.value = String(event.title || event.alarm_desc || eventTitle.value || "");
    }
    replacePrefillFields(
      payload.fields && typeof payload.fields === "object" ? payload.fields : {},
      true,
    );
    prefillWarnings.value = Array.isArray(payload.warnings)
      ? payload.warnings.map((item: unknown) => String(item || "")).filter(Boolean)
      : [];
    if (!quiet) {
      hasUnsavedChanges.value = true;
      showMessage("已按所选关联记录重新填入。", "success");
    }
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "自动填入失败。", "failed");
  } finally {
    prefillLoading.value = false;
  }
}

function selectRecordNow(record: LooseDict): void {
  followupHasUnsavedChanges.value = false;
  creatingNewProject.value = false;
  selectedRecord.value = record;
  editingRecordId.value = String(record.record_id || "");
  sourceEventId.value = String(record.source_event_id || "").trim();
  selectedRepairIds.value = Array.isArray(record.source_repair_ids)
    ? record.source_repair_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
    : [];
  repairRecommendedIds.value = [];
  const displayFields = record.display_fields || {};
  eventTitle.value = String(displayFields["事件描述"] || displayFields["故障维修原因"] || "");
  selectedEvent.value = sourceEventId.value
    ? { record_id: sourceEventId.value, title: eventTitle.value }
    : null;
  resetDraft();
  sourceExpanded.value = false;
  activeWorkspaceTab.value = "project";
}

function requestSelectRecord(record: LooseDict): void {
  if (String(record.record_id || "") === editingRecordId.value) return;
  runWithUnsavedGuard(() => selectRecordNow(record));
}

function startCreateNow(): void {
  followupHasUnsavedChanges.value = false;
  creatingNewProject.value = true;
  selectedRecord.value = null;
  editingRecordId.value = "";
  sourceEventId.value = "";
  eventTitle.value = "";
  selectedEvent.value = null;
  selectedRepairIds.value = [];
  repairRecommendedIds.value = [];
  repairCandidates.value = [];
  prefillWarnings.value = [];
  resetDraft();
  sourceExpanded.value = true;
  activeWorkspaceTab.value = "project";
  showMessage("请填写维修项目。", "warning");
}

function requestStartCreate(): void {
  runWithUnsavedGuard(startCreateNow);
}

function clearSearch(): void {
  searchText.value = "";
  recordPage.value = 1;
  void loadRecords(false);
}

function changeRecordPage(delta: number): void {
  const nextPage = Math.min(recordPageCount.value, Math.max(1, recordPage.value + delta));
  if (nextPage === recordPage.value) return;
  runWithUnsavedGuard(() => {
    recordPage.value = nextPage;
    followupHasUnsavedChanges.value = false;
    creatingNewProject.value = false;
    selectedRecord.value = null;
    editingRecordId.value = "";
    sourceEventId.value = "";
    eventTitle.value = "";
    selectedEvent.value = null;
    selectedRepairIds.value = [];
    repairRecommendedIds.value = [];
    resetDraft();
    void loadRecords(false);
  });
}

async function loadRecords(announce = false): Promise<void> {
  loading.value = true;
  try {
    const payload = await requestJson(`/api/repair-management/records?${scopedQuery()}`);
    fields.value = Array.isArray(payload.fields) ? payload.fields : [];
    records.value = Array.isArray(payload.records) ? payload.records : [];
    total.value = Number(payload.total || records.value.length || 0);
    const maxPage = Math.max(1, Math.ceil(total.value / RECORD_PAGE_SIZE));
    if (recordPage.value > maxPage) {
      recordPage.value = maxPage;
      loading.value = false;
      await loadRecords(announce);
      return;
    }
    if (editingRecordId.value) {
      const current = records.value.find((item) => String(item.record_id || "") === editingRecordId.value);
      if (current) {
        selectedRecord.value = current;
      } else if (!hasUnsavedChanges.value && records.value.length) {
        selectRecordNow(records.value[0]);
      }
    } else if (!creatingNewProject.value && records.value.length) {
      selectRecordNow(records.value[0]);
    } else if (!selectedRecord.value) {
      resetDraft();
    }
    if (sourceEventId.value && !editingRecordId.value && !routeEventPrefillApplied.value) {
      routeEventPrefillApplied.value = true;
      await applyEventPrefill(sourceEventId.value, true);
    }
    if (announce) {
      showMessage(records.value.length ? "维修项目已刷新。" : "当前筛选下没有维修项目。", records.value.length ? "success" : "warning");
    }
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "维修项目读取失败。", "failed");
  } finally {
    loading.value = false;
  }
}

function writablePayload(): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const value = String(fieldDraft[name] ?? "");
    if (value.trim() === "" && !editingRecordId.value) continue;
    result[name] = parseDraftValue(value);
  }
  return result;
}

async function saveRecord(): Promise<void> {
  if (missingRequiredEditableFields.value.length) {
    showMessage(`请先填写：${missingRequiredEditableFields.value.join("、")}。`, "warning");
    return;
  }
  saving.value = true;
  try {
    const body = JSON.stringify({
      scope: props.scope || "ALL",
      source_event_id: sourceEventId.value,
      source_repair_ids: selectedRepairIds.value,
      source_month: currentMonthKey(),
      fields: writablePayload(),
    });
    if (editingRecordId.value) {
      const updated = await requestJson(`/api/repair-management/records/${encodeURIComponent(editingRecordId.value)}`, {
        method: "PUT",
        body,
      });
      const warnings = Array.isArray(updated.warnings) ? updated.warnings.filter(Boolean) : [];
      showMessage(
        warnings.length ? `维修项目已保存；${warnings.join("；")}` : "维修项目已保存。",
        warnings.length ? "warning" : "success",
      );
    } else {
      const created = await requestJson("/api/repair-management/records", {
        method: "POST",
        body,
      });
      editingRecordId.value = String(created.record_id || "");
      const warnings = Array.isArray(created.warnings) ? created.warnings.filter(Boolean) : [];
      showMessage(
        warnings.length ? `维修项目已创建；${warnings.join("；")}` : "维修项目已创建。",
        warnings.length ? "warning" : "success",
      );
    }
    dirtyFieldNames.clear();
    hasUnsavedChanges.value = false;
    sourceExpanded.value = false;
    recordPage.value = 1;
    await loadRecords(false);
    const current = records.value.find((item) => String(item.record_id || "") === editingRecordId.value);
    if (current) {
      selectedRecord.value = current;
      resetDraft();
    }
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "保存失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function requestDeleteRecord(): void {
  if (!editingRecordId.value) return;
  openConfirmation(
    {
      tone: "danger",
      kicker: "删除维修项目",
      title: `删除“${selectedRecordTitle.value}”？`,
      message: "删除后无法恢复；如果该项目已有跟进记录，后端会阻止删除。",
      details: ["只删除当前维修项目，不会误删其他记录。"],
      confirmLabel: "确认删除",
    },
    deleteRecordNow,
  );
}

async function deleteRecordNow(): Promise<void> {
  if (!editingRecordId.value) return;
  saving.value = true;
  try {
    const params = new URLSearchParams({ scope: props.scope || "ALL" });
    await requestJson(`/api/repair-management/records/${encodeURIComponent(editingRecordId.value)}?${params.toString()}`, {
      method: "DELETE",
    });
    selectedRecord.value = null;
    editingRecordId.value = "";
    followupHasUnsavedChanges.value = false;
    creatingNewProject.value = false;
    sourceEventId.value = "";
    eventTitle.value = "";
    selectedEvent.value = null;
    selectedRepairIds.value = [];
    repairRecommendedIds.value = [];
    resetDraft();
    activeWorkspaceTab.value = "project";
    sourceExpanded.value = true;
    showMessage("维修项目已删除。");
    await loadRecords(false);
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "删除失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function displayReadonlyValue(fieldName: string): string {
  if (Object.prototype.hasOwnProperty.call(prefillPreview, fieldName)) {
    const field = fields.value.find((item) => String(item.field_name || "") === fieldName) || {};
    return repairDraftInputValue(field, prefillPreview[fieldName]).replace("T", " ");
  }
  if (!editingRecordId.value) {
    if (fieldName === "维修名称" && eventTitle.value) {
      return eventTitle.value;
    }
    if (fieldName === "关联事件单" && sourceEventId.value) {
      return sourceEventId.value;
    }
  }
  const displayFields = selectedRecord.value?.display_fields || {};
  return fieldValueToText(displayFields[fieldName]);
}

function openRepairNoticeWorkbench(): void {
  const url = new URL("/workbench-lite", window.location.origin);
  url.searchParams.set("scope", props.scope || "ALL");
  url.searchParams.set("work_type", "repair");
  if (editingRecordId.value) {
    url.searchParams.set("repair_management_record_id", editingRecordId.value);
  }
  navigateHard(url);
}

watch(
  () => props.scope,
  () => {
    recordPage.value = 1;
    followupHasUnsavedChanges.value = false;
    eventCandidates.value = [];
    selectedEvent.value = null;
    repairCandidates.value = [];
    selectedRepairIds.value = [];
    repairRecommendedIds.value = [];
    activePicker.value = "";
    prefillWarnings.value = [];
    routeEventPrefillApplied.value = false;
    void loadRecords(false);
    void loadEventCandidates();
  },
);

watch(searchText, () => {
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    recordPage.value = 1;
    void loadRecords(false);
  }, 300);
});

onMounted(() => {
  if (routeParams.get("mode") === "create") {
    startCreateNow();
    sourceEventId.value = routeEventId;
    eventTitle.value = routeEventTitle;
  }
  window.addEventListener("beforeunload", handleBeforeUnload);
  void loadRecords(false);
  void loadEventCandidates();
});

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer);
  window.removeEventListener("beforeunload", handleBeforeUnload);
});
</script>

<style scoped>
.repair-management-page {
  width: min(1720px, 100%);
  margin: 0 auto;
  padding: 18px 32px 40px;
  display: grid;
  gap: 14px;
}

.page-back-row {
  display: flex;
  align-items: center;
  justify-content: flex-start;
}

.page-back-btn {
  min-height: 36px;
  padding: 0 13px;
  border-radius: 999px;
}

.page-back-btn span {
  font-size: 19px;
  line-height: 1;
}

.repair-hero,
.repair-workflow,
.event-link-panel,
.repair-source-panel,
.source-relation-panel,
.record-panel,
.editor-panel {
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 22px 58px rgba(20, 75, 150, 0.12);
}

.source-relation-panel {
  padding: 16px 18px;
}

.source-relation-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  margin-bottom: 12px;
}

.source-relation-head h3 {
  margin: 5px 0 0;
  color: #0a1d3b;
  font-size: 18px;
}

.source-relation-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  border: 1px solid #dce7f4;
  border-radius: 12px;
  overflow: hidden;
  background: #f9fbfe;
}

.source-relation-item {
  min-width: 0;
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 12px;
  border-right: 1px solid #dce7f4;
}

.source-relation-item:last-child {
  border-right: 0;
}

.source-relation-item > div {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.source-relation-item b {
  color: #17314f;
  font-size: 14px;
}

.source-relation-item small {
  overflow: hidden;
  color: #617792;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.source-relation-item .btn {
  min-height: 34px;
  padding-inline: 12px;
  white-space: nowrap;
}

.relation-order {
  width: 26px;
  height: 26px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  background: #e8f2ff;
  color: #1164d8;
  font-size: 12px;
  font-weight: 900;
}

.repair-hero {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: center;
  padding: 14px 18px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(244, 249, 255, 0.96)),
    radial-gradient(circle at 92% 18%, rgba(42, 119, 255, 0.12), transparent 30%);
}

.repair-hero h2,
.editor-head h3 {
  margin: 0;
  color: #071a39;
  font-size: 24px;
  font-weight: 950;
}

.repair-hero p,
.editor-head p,
.record-panel small,
.field-card small,
.empty-state {
  color: #62758f;
}

.editor-head .source-event-note {
  color: #0e5bd8;
  font-weight: 850;
}

.section-kicker,
.source-pill {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0 13px;
  border: 1px solid #d8e7f8;
  border-radius: 999px;
  background: #eff6ff;
  color: #0e5bd8;
  font-size: 12px;
  font-weight: 950;
}

.source-pill {
  background: #f8fbff;
  color: #57708f;
}

.hero-actions,
.editor-actions,
.record-actions {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.repair-workflow {
  min-height: 68px;
  display: grid;
  grid-template-columns: minmax(130px, 0.65fr) minmax(180px, 1fr) 18px minmax(190px, 1fr) 18px minmax(190px, 1fr);
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  box-shadow: 0 12px 32px rgba(20, 75, 150, 0.08);
}

.workflow-scope {
  min-width: 0;
  display: grid;
  gap: 2px;
  padding: 3px 14px 3px 2px;
  border-right: 1px solid #dce7f4;
}

.workflow-scope span,
.workflow-step small {
  color: #647b97;
  font-size: 12px;
  font-weight: 800;
}

.workflow-scope strong {
  overflow: hidden;
  color: #0b3f9f;
  font-size: 18px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.workflow-step {
  min-width: 0;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr);
  align-items: center;
  gap: 9px;
}

.workflow-step > span {
  width: 30px;
  height: 30px;
  display: grid;
  place-items: center;
  border: 1px solid #cddced;
  border-radius: 9px;
  background: #f2f6fb;
  color: #60758f;
  font-size: 12px;
  font-weight: 950;
}

.workflow-step.complete > span {
  border-color: #1e63ff;
  background: #1e63ff;
  color: #fff;
}

.workflow-step > div {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.workflow-step b,
.workflow-step small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.workflow-step b {
  color: #17314f;
  font-size: 13px;
  font-weight: 950;
}

.workflow-step.complete small {
  color: #087a66;
}

.repair-workflow > i {
  color: #9ab0c8;
  font-size: 25px;
  font-style: normal;
  text-align: center;
}

.event-link-panel {
  padding: 16px;
  display: grid;
  gap: 12px;
}

.repair-source-panel {
  padding: 16px;
  display: grid;
  gap: 12px;
}

.repair-source-head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
}

.repair-source-head h3 {
  margin: 7px 0 0;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

.repair-search {
  min-width: min(520px, 100%);
  display: grid;
  grid-template-columns: minmax(220px, 1fr) auto;
  gap: 10px;
  align-items: center;
}

.selected-source-strip {
  min-height: 42px;
  display: flex;
  align-items: center;
  gap: 9px;
  flex-wrap: wrap;
  padding: 7px 9px;
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background: #f7fbff;
}

.selected-source-strip span {
  min-height: 28px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 0 10px;
  border-radius: 999px;
  background: #eaf3ff;
  color: #315273;
  font-size: 12px;
  font-weight: 850;
}

.selected-source-strip .btn {
  margin-left: auto;
}

.repair-candidates {
  max-height: 286px;
  overflow-y: auto;
  overscroll-behavior: contain;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 9px;
  padding: 2px 4px 2px 2px;
}

.repair-candidate {
  min-width: 0;
  min-height: 74px;
  display: grid;
  grid-template-columns: 24px minmax(0, 1fr) auto;
  gap: 9px;
  align-items: center;
  padding: 10px;
  border: 1px solid #dfe9f7;
  border-radius: 14px;
  background: #fbfdff;
  color: #10203b;
  text-align: left;
  cursor: pointer;
  transition: border-color 180ms ease, background 180ms ease, box-shadow 180ms ease;
}

.repair-candidate:hover,
.repair-candidate.recommended {
  border-color: #9bbcff;
  background: #f5f9ff;
}

.repair-candidate.active {
  border-color: #1e63ff;
  background: #eaf3ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.13);
}

.candidate-check {
  width: 22px;
  height: 22px;
  display: grid;
  place-items: center;
  border: 1px solid #aac3e8;
  border-radius: 7px;
  background: #fff;
  color: #fff;
  font-size: 14px;
  font-weight: 950;
}

.repair-candidate.active .candidate-check {
  border-color: #1e63ff;
  background: #1e63ff;
}

.candidate-body {
  min-width: 0;
  display: grid;
  gap: 5px;
}

.candidate-body b,
.candidate-body small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.candidate-body b {
  color: #071a39;
  font-size: 13px;
  font-weight: 950;
}

.candidate-body small {
  color: #62758f;
  font-size: 12px;
  font-weight: 760;
}

.candidate-score {
  min-height: 24px;
  padding: 3px 8px;
  border-radius: 999px;
  background: #e8f2ff;
  color: #0e5bd8;
  font-size: 11px;
  font-weight: 900;
  white-space: nowrap;
}

.prefill-warning {
  padding: 9px 11px;
  border: 1px solid #fed7aa;
  border-radius: 12px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 12px;
  font-weight: 820;
}

.event-link-head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
}

.event-link-head h3 {
  margin: 7px 0 4px;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

.event-link-head p {
  margin: 0;
  color: #62758f;
  font-size: 13px;
  font-weight: 760;
}

.event-search {
  min-width: min(520px, 100%);
  display: grid;
  grid-template-columns: minmax(220px, 1fr) auto;
  gap: 10px;
  align-items: center;
}

.selected-event {
  display: flex;
  align-items: center;
  gap: 10px;
  min-height: 42px;
  padding: 9px 12px;
  border: 1px solid #bfdbfe;
  border-radius: 14px;
  background: #eff6ff;
  color: #17335e;
  font-weight: 850;
}

.selected-event span {
  min-width: 0;
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selected-event small {
  color: #5c7391;
  font-weight: 760;
}

.event-candidates {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.event-candidate {
  min-width: 0;
  min-height: 72px;
  display: grid;
  gap: 6px;
  text-align: left;
  border: 1px solid #e1ebf8;
  border-radius: 14px;
  background: #fbfdff;
  padding: 10px 12px;
  color: #10203b;
  cursor: pointer;
}

.event-candidate:hover,
.event-candidate.active {
  border-color: #1e63ff;
  background: #eff6ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.event-candidate span,
.event-candidate small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.event-candidate span {
  color: #071a39;
  font-weight: 950;
}

.event-candidate small,
.event-empty {
  color: #62758f;
  font-size: 12px;
  font-weight: 780;
}

.event-empty {
  padding: 12px;
  border: 1px dashed #d8e5f7;
  border-radius: 14px;
  background: #f8fbff;
}

.repair-layout {
  display: grid;
  grid-template-columns: minmax(320px, 0.34fr) minmax(0, 1fr);
  gap: 16px;
}

.record-panel,
.editor-panel {
  min-width: 0;
  padding: 16px;
}

.record-panel {
  align-self: start;
  position: sticky;
  top: 14px;
  max-height: calc(100vh - 28px);
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  overflow: hidden;
}

.record-list {
  min-height: 220px;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 2px 4px 4px 0;
}

.record-search {
  position: relative;
}

.record-search input {
  padding-right: 56px;
}

.search-clear {
  position: absolute;
  right: 7px;
  top: 50%;
  min-height: 28px;
  border: 0;
  border-radius: 9px;
  padding: 0 8px;
  background: #edf4ff;
  color: #0e5bd8;
  font: inherit;
  font-size: 12px;
  font-weight: 900;
  transform: translateY(-50%);
  cursor: pointer;
}

.pager {
  min-height: 42px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding-top: 10px;
  border-top: 1px solid #e4edf8;
}

.pager button {
  min-height: 30px;
  border: 1px solid #d5e2f4;
  border-radius: 10px;
  padding: 0 10px;
  background: #f8fbff;
  color: #1555b3;
  font: inherit;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}

.pager button:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.pager span {
  color: #60758f;
  font-size: 12px;
  font-weight: 850;
}

.editor-panel {
  align-self: start;
}

.workspace-tabs-row {
  min-height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin: -4px 0 14px;
  border-bottom: 1px solid #dfe9f6;
}

.workspace-tabs {
  align-self: stretch;
  display: flex;
  align-items: stretch;
  gap: 4px;
}

.workspace-tabs button {
  position: relative;
  min-width: 112px;
  border: 0;
  border-radius: 10px 10px 0 0;
  padding: 0 14px;
  background: transparent;
  color: #617792;
  font: inherit;
  font-weight: 900;
  cursor: pointer;
}

.workspace-tabs button::after {
  content: "";
  position: absolute;
  right: 12px;
  bottom: -1px;
  left: 12px;
  height: 3px;
  border-radius: 999px 999px 0 0;
  background: transparent;
}

.workspace-tabs button.active {
  background: #f4f8ff;
  color: #0a55c6;
}

.workspace-tabs button.active::after {
  background: #1e63ff;
}

.workspace-tabs button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
}

.unsaved-dot {
  margin-left: 5px;
  border-radius: 999px;
  padding: 2px 6px;
  background: #fff3db;
  color: #a94f00;
  font-size: 10px;
}

.workspace-state {
  color: #69809c;
  font-size: 12px;
  font-weight: 850;
}

.project-workspace {
  display: grid;
  gap: 14px;
}

.project-workspace .source-relation-panel {
  padding: 2px 0 12px;
  border: 0;
  border-bottom: 1px solid #dfe9f6;
  border-radius: 0;
  box-shadow: none;
  background: transparent;
}

.project-workspace .source-relation-panel.collapsed {
  padding-block: 2px 10px;
}

.project-workspace .source-relation-head h3 {
  margin: 0;
}

.source-head-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.source-summary-row {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.source-summary-row span {
  min-width: 0;
  overflow: hidden;
  border: 1px solid #dce8f6;
  border-radius: 10px;
  padding: 8px 10px;
  background: #fff;
  color: #5d7390;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.source-summary-row b {
  margin-right: 7px;
  color: #17314f;
}

.panel-head {
  display: grid;
  gap: 10px;
}

.panel-head > div {
  display: flex;
  justify-content: space-between;
  gap: 12px;
}

.panel-head strong {
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
}

input,
select,
textarea {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  background: #fff;
  color: #10203b;
  font: inherit;
  font-weight: 780;
  padding: 9px 12px;
}

input:focus,
select:focus,
textarea:focus {
  outline: 3px solid rgba(30, 99, 255, 0.16);
  border-color: #1e63ff;
}

select {
  min-height: 40px;
  appearance: none;
  background-image: linear-gradient(45deg, transparent 50%, #1d5edb 50%), linear-gradient(135deg, #1d5edb 50%, transparent 50%);
  background-position: calc(100% - 18px) 17px, calc(100% - 12px) 17px;
  background-size: 6px 6px, 6px 6px;
  background-repeat: no-repeat;
  padding-right: 34px;
}

textarea {
  min-height: 42px;
  resize: vertical;
  line-height: 1.45;
}

.record-actions {
  margin: 10px 0;
  justify-content: flex-start;
}

.record-row {
  width: 100%;
  margin-top: 8px;
  padding: 12px;
  display: grid;
  gap: 8px;
  text-align: left;
  border: 1px solid #e0e9f7;
  border-radius: 14px;
  background: #f8fbff;
  cursor: pointer;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.record-row:hover {
  border-color: #97b8ff;
  background: #f2f7ff;
}

.record-row.active {
  border-color: #1e63ff;
  background: #eff6ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.record-title {
  color: #071a39;
  font-weight: 950;
  line-height: 1.35;
}

.record-meta {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.record-meta b {
  max-width: 100%;
  min-height: 24px;
  padding: 3px 8px;
  border-radius: 999px;
  background: #edf5ff;
  color: #315273;
  font-size: 12px;
  font-weight: 850;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.editor-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 14px;
  margin-bottom: 14px;
}

.form-warning {
  margin-bottom: 12px;
  padding: 9px 12px;
  border: 1px solid #fed7aa;
  border-radius: 12px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 13px;
  font-weight: 850;
}

.next-step-panel {
  margin-bottom: 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 12px;
  border: 1px solid #bfdbfe;
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.96)),
    radial-gradient(circle at 92% 20%, rgba(30, 99, 255, 0.12), transparent 28%);
}

.next-step-panel > div:first-child {
  min-width: 0;
  display: grid;
  gap: 5px;
}

.next-step-panel strong {
  color: #071a39;
  font-size: 15px;
  font-weight: 950;
}

.next-step-panel p {
  margin: 0;
  color: #4f6684;
  font-size: 12px;
  font-weight: 820;
  line-height: 1.45;
}

.next-step-actions {
  flex: 0 0 auto;
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.field-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.field-card {
  min-width: 0;
  display: grid;
  gap: 8px;
  padding: 11px;
  border: 1px solid #e1ebf8;
  border-radius: 14px;
  background: #fbfdff;
}

.field-card.required {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.field-card > span {
  display: flex;
  justify-content: space-between;
  gap: 10px;
}

.field-card b {
  color: #0f2142;
  font-weight: 950;
}

.field-card output {
  min-height: 32px;
  white-space: pre-wrap;
  color: #4d627e;
}

.readonly-summary {
  margin-top: 14px;
  border: 1px solid #e4ecf8;
  border-radius: 14px;
  background: #f7fbff;
  padding: 10px 12px;
}

.readonly-summary summary {
  cursor: pointer;
  color: #315273;
  font-weight: 850;
}

.readonly-hint {
  margin: 8px 0 0;
  padding: 8px 10px;
  border-radius: 12px;
  background: #eef6ff;
  color: #506987;
  font-size: 12px;
  font-weight: 780;
}

.readonly-grid {
  margin-top: 10px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.readonly-line {
  min-width: 0;
  padding: 8px 10px;
  border-radius: 12px;
  background: #ffffff;
  border: 1px solid #edf3fb;
  display: grid;
  gap: 4px;
}

.readonly-line b,
.readonly-line span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.readonly-line b {
  color: #425a78;
  font-size: 12px;
}

.readonly-line span {
  color: #71839b;
  font-size: 12px;
}

.btn {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 0 15px;
  font: inherit;
  font-weight: 950;
  cursor: pointer;
  transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.btn:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 10px 20px rgba(20, 75, 150, 0.12);
}

.btn.primary {
  border-color: #1e63ff;
  background: linear-gradient(135deg, #2a77ff, #0050d9);
  color: #fff;
}

.btn.secondary,
.btn.quiet {
  background: #f8fbff;
  color: #0e4fb2;
}

.btn.ghost {
  background: rgba(255, 255, 255, 0.72);
  color: #0e4fb2;
}

.btn.danger {
  border-color: #fecdd3;
  background: #fff1f2;
  color: #be123c;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.62;
}

.empty-state {
  padding: 18px;
  border: 1px dashed #d8e5f7;
  border-radius: 16px;
  background: #f8fbff;
  font-weight: 850;
}

@media (max-width: 1180px) {
  .repair-layout,
  .field-grid,
  .source-relation-grid,
  .event-candidates {
    grid-template-columns: 1fr;
  }

  .source-relation-item {
    border-right: 0;
    border-bottom: 1px solid #dce7f4;
  }

  .source-relation-item:last-child {
    border-bottom: 0;
  }

  .record-panel {
    position: static;
    max-height: none;
  }

  .record-list {
    max-height: 420px;
  }

  .source-summary-row {
    grid-template-columns: 1fr;
  }

  .event-link-head,
  .event-search,
  .repair-source-head,
  .repair-search {
    grid-template-columns: 1fr;
    flex-direction: column;
  }

  .repair-candidates {
    grid-template-columns: 1fr;
  }

  .repair-workflow {
    grid-template-columns: 1fr;
  }

  .workflow-scope {
    border-right: 0;
    border-bottom: 1px solid #dce7f4;
    padding: 2px 2px 9px;
  }

  .repair-workflow > i {
    display: none;
  }
}

.btn.compact {
  min-height: 32px;
  padding-inline: 10px;
  font-size: 12px;
}
</style>
