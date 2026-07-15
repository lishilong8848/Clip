<template>
  <section class="repair-management-page">
    <div class="repair-hero">
      <div class="repair-hero-title">
        <VnetBackButton @click="requestBack" />
        <h2>维修项目与跟进</h2>
      </div>
      <div class="hero-actions">
        <button type="button" class="btn secondary" @click="openRepairStatus">
          <Activity :size="16" aria-hidden="true" />
          <span>检修状态</span>
        </button>
        <button type="button" class="btn secondary" :disabled="loading" @click="loadRecords(true)">
          <RefreshCw :size="16" :class="{ spinning: loading }" aria-hidden="true" />
          <span>{{ loading && !records.length ? "读取中" : "刷新" }}</span>
        </button>
        <button type="button" class="btn primary" @click="requestStartCreate">
          <FilePlus2 :size="16" aria-hidden="true" />
          <span>新建维修单</span>
        </button>
      </div>
    </div>

    <MessageBanner v-if="messageText" :tone="messageTone" :text="messageText" />
    <div v-if="hasAnyUnsavedChanges" class="page-unsaved-notice" role="status" aria-live="polite">
      <AlertCircle :size="16" aria-hidden="true" />
      <span>{{ unsavedNoticeText }}</span>
    </div>

    <div class="repair-layout">
      <aside class="record-panel">
        <div class="panel-head">
          <div>
            <strong>维修项目列表</strong>
            <span aria-live="polite">{{ loading && records.length ? "更新中" : `${total} 条` }}</span>
          </div>
          <div class="record-search">
            <input v-model.trim="searchText" type="search" placeholder="搜索维修项目" />
            <button v-if="searchText" type="button" class="search-clear" @click="clearSearch">清空</button>
          </div>
        </div>
        <div class="record-list" :aria-busy="loading">
          <div v-if="loading && !records.length" class="empty-state">正在读取维修项目...</div>
          <div v-else-if="!records.length" class="empty-state">{{ recordEmptyText }}</div>
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
            <span class="record-title-line">
              <span class="record-title" :title="record.title || '未命名维修项目'">
                {{ record.title || "未命名维修项目" }}
              </span>
              <CheckCircle2
                v-if="selectedRecord?.record_id === record.record_id"
                :size="16"
                aria-hidden="true"
              />
            </span>
            <span class="record-meta">
              <b v-if="record.workflow" class="workflow">{{ record.workflow }}</b>
              <b>{{ recordBuildingLabel(record) }}</b>
              <b>{{ recordSpecialtyLabel(record) }}</b>
            </span>
            <span class="record-time" :title="recordTimeLabel(record)">
              <Clock3 :size="13" aria-hidden="true" />
              <span>{{ recordTimeLabel(record) }}</span>
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
              维修单信息
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
                <span v-if="prefillLoading" class="source-loading">填入中</span>
                <button type="button" class="btn quiet compact" @click="sourceExpanded = !sourceExpanded">
                  {{ sourceExpanded ? "收起" : "更改事件检修关联" }}
                </button>
              </div>
            </header>
            <div v-if="sourceExpanded" class="source-relation-grid">
              <div class="source-relation-item">
                <span class="relation-order">1</span>
                <div><b>关联事件单</b><small>{{ selectedEventSummary }}</small></div>
                <div class="relation-actions">
                  <button type="button" class="btn secondary" :disabled="prefillLoading" @click="openSourcePicker('event')">
                    {{ sourceEventId ? "重新选择" : "选择事件" }}
                  </button>
                  <button v-if="sourceEventId" type="button" class="btn quiet" :disabled="prefillLoading" @click="clearEventSelection">
                    清除
                  </button>
                </div>
              </div>
              <div class="source-relation-item">
                <span class="relation-order">2</span>
                <div><b>设备检修关联</b><small>{{ selectedRepairSummary }}</small></div>
                <div class="relation-actions">
                  <button type="button" class="btn secondary" :disabled="prefillLoading" @click="openSourcePicker('repair')">
                    {{ selectedRepairIds.length ? "重新选择" : "选择检修通告" }}
                  </button>
                  <button v-if="selectedRepairIds.length" type="button" class="btn quiet" :disabled="prefillLoading" @click="clearRepairSelection">
                    清除
                  </button>
                </div>
              </div>
            </div>
            <div v-else class="source-summary-row">
              <span><b>关联事件</b>{{ selectedEventSummary }}</span>
              <span><b>关联检修通告</b>{{ selectedRepairSummary }}</span>
            </div>
            <div v-if="prefillWarnings.length" class="prefill-warning">{{ prefillWarnings.join("；") }}</div>
          </section>

          <header class="editor-head">
            <div>
              <button
                v-if="editingRecordId"
                type="button"
                class="editor-title-button"
                title="查看完整维修单"
                @click="detailDialogOpen = true"
              >
                <span>{{ selectedRecordTitle }}</span>
                <Eye :size="15" aria-hidden="true" />
              </button>
              <h3 v-else>填写维修项目</h3>
              <p v-if="sourceEventId && eventTitle">关联事件：{{ eventTitle }}</p>
            </div>
          </header>

          <div v-if="validationAttempted && missingRequiredEditableFields.length" class="form-warning">
            还缺 {{ missingRequiredEditableFields.length }} 项：{{ missingRequiredEditableFields.join("、") }}
          </div>

          <div v-if="!fields.length && loading" class="empty-state">正在读取字段...</div>
          <div v-else-if="!editableFields.length" class="empty-state">暂无可填写字段</div>
          <div v-else class="project-form-sections">
            <section v-for="group in projectFieldGroups" :key="group.key" class="project-field-section">
              <header v-if="group.label">
                <h4>{{ group.label }}</h4>
                <span>{{ group.fields.length }} 项</span>
              </header>
              <div class="project-field-grid">
                <RepairFieldControl
                  v-for="field in group.fields"
                  :key="field.field_name"
                  :field="field"
                  :input-id="fieldInputId(field)"
                  :label="projectFieldLabel(field.field_name)"
                  :model-value="fieldDraft[field.field_name]"
                  :required="isRequiredField(field.field_name)"
                  :error="fieldValidationError(field.field_name)"
                  :wide="usesTextarea(field.field_name)"
                  compact
                  @update:model-value="fieldDraft[field.field_name] = $event"
                  @edited="markFieldDirty(field.field_name)"
                />
              </div>
            </section>
          </div>

          <footer class="editor-action-bar">
            <div class="save-state" :class="projectSaveStateTone">
              <component :is="projectSaveStateIcon" :size="17" aria-hidden="true" />
              <span>{{ projectSaveStateText }}</span>
            </div>
            <div class="editor-action-buttons">
              <button
                v-if="editingRecordId"
                type="button"
                class="btn secondary"
                @click="requestOpenRepairNoticeWorkbench"
              >
                <Wrench :size="16" aria-hidden="true" />
                <span>检修通告</span>
              </button>
              <button type="button" class="btn quiet" :disabled="saving" @click="requestResetDraft">
                <RotateCcw :size="16" aria-hidden="true" />
                <span>重置</span>
              </button>
              <button
                v-if="editingRecordId"
                type="button"
                class="btn danger"
                :disabled="saving"
                @click="requestDeleteRecord"
              >
                <Trash2 :size="16" aria-hidden="true" />
                <span>删除</span>
              </button>
              <button
                type="button"
                class="btn primary"
                :disabled="saving || !canSaveRecord"
                :title="saveDisabledReason"
                @click="saveRecord"
              >
                <Save :size="16" aria-hidden="true" />
                <span>{{ saving ? "保存中" : editingRecordId ? "保存修改" : "保存维修单" }}</span>
              </button>
            </div>
          </footer>
        </div>

        <RepairFollowupPanel
          v-if="editingRecordId"
          :key="`${scope}:${editingRecordId}`"
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

    <Teleport to="body">
      <div
        v-if="detailDialogOpen"
        class="repair-detail-overlay"
        role="presentation"
        @click.self="detailDialogOpen = false"
        @keydown.esc="detailDialogOpen = false"
      >
        <section
          class="repair-detail-dialog"
          role="dialog"
          aria-modal="true"
          aria-labelledby="repair-detail-title"
        >
          <header class="repair-detail-head">
            <div>
              <span>维修单详情</span>
              <h3 id="repair-detail-title">{{ selectedRecordTitle }}</h3>
            </div>
            <button type="button" aria-label="关闭维修单详情" @click="detailDialogOpen = false">
              <X :size="19" aria-hidden="true" />
            </button>
          </header>
          <div class="repair-detail-body">
            <div v-if="hasSelectedSources" class="repair-detail-context">
              <span><b>关联事件</b>{{ selectedEventSummary }}</span>
              <span><b>关联检修通告</b>{{ selectedRepairSummary }}</span>
            </div>
            <section v-if="detailPreviewFields.length" class="repair-detail-section">
              <div class="repair-detail-readonly-grid">
                <div
                  v-for="field in detailPreviewFields"
                  :key="`detail-${field.field_name}`"
                  :class="{ wide: field.wide }"
                >
                  <b>{{ field.label }}</b>
                  <span :title="field.value">{{ field.value }}</span>
                </div>
              </div>
            </section>
            <div v-else class="empty-state">暂无可展示内容</div>
          </div>
          <footer class="repair-detail-footer">
            <button type="button" class="btn quiet" @click="detailDialogOpen = false">关闭</button>
          </footer>
        </section>
      </div>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onActivated, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock3,
  Eye,
  FilePlus2,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  Save,
  Trash2,
  Wrench,
  X,
} from "lucide-vue-next";
import { requestJson } from "../api/client";
import { navigate, navigateHard } from "../navigation";
import { invalidateRepairStatus } from "../repairStatusState";
import {
  REPAIR_REQUIRED_FIELD_GROUPS,
  createRepairOperationId,
  isRequiredRepairField as isRequiredField,
  parseRepairDraftValue as parseDraftValue,
  repairDraftInputValue,
  repairEventRecordId as eventRecordId,
  repairFieldUsesTextarea,
  repairRecordBuildingLabel as recordBuildingLabel,
  repairRecordSpecialtyLabel as recordSpecialtyLabel,
  repairRecordTimeLabel as recordTimeLabel,
  sortedRepairFields as sortedFields,
} from "../repairManagementUtils";
import type { LooseDict, ScopeOption } from "../types";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
import RepairFieldControl from "./RepairFieldControl.vue";
import RepairFollowupPanel from "./RepairFollowupPanel.vue";
import RecordPickerDialog from "./RecordPickerDialog.vue";
import VnetBackButton from "./VnetBackButton.vue";

type PickerColumn = { key: string; label: string; width?: string };
type SourcePicker = "" | "event" | "repair";
type WorkspaceTab = "project" | "followups";
type ConfirmTone = "warning" | "danger" | "primary";
type ProjectFieldGroupKey = "basic" | "execution" | "other";
type PrefillSelection = { eventRecordId: string; repairRecordIds: string[] };

const RECORD_PAGE_SIZE = 30;
const RECORD_RESPONSE_CACHE_TTL_MS = 15_000;
const recordResponseCache = new Map<string, { expiresAt: number; payload: LooseDict }>();

function clearRecordResponseCache(): void {
  recordResponseCache.clear();
}

const PROJECT_FIELD_GROUPS: Array<{ key: ProjectFieldGroupKey; label: string }> = [
  { key: "basic", label: "" },
  { key: "execution", label: "维修执行" },
  { key: "other", label: "其他信息" },
];
const BASIC_FIELD_NAMES = new Set([
  "维修名称",
  "故障发生时间",
  "所属专业",
  "专业",
  "专业（推送消息用）",
  "楼栋",
  "所属数据中心/楼栋-使用",
  "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）",
]);
const PROJECT_FIELD_LABELS: Record<string, string> = {
  "故障维修原因": "故障原因",
  "故障发生现象描述": "故障现象",
  "所属专业": "专业",
  "所属数据中心/楼栋-使用": "楼栋",
};

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
  focusRecordId?: string;
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
const selectedRepairRecords = ref<LooseDict[]>([]);
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
const validationAttempted = ref(false);
const creatingNewProject = ref(false);
const detailDialogOpen = ref(false);
const createOperationId = ref("");
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
let skipNextSearchReload = false;
let recordsRequestVersion = 0;
let eventRequestVersion = 0;
let repairRequestVersion = 0;
let prefillRequestVersion = 0;
let recordDetailRequestVersion = 0;
let activationCount = 0;
let recordsAbortController: AbortController | null = null;
let eventAbortController: AbortController | null = null;
let repairAbortController: AbortController | null = null;
let prefillAbortController: AbortController | null = null;
let recordDetailAbortController: AbortController | null = null;

const routeParams = new URLSearchParams(window.location.search);
const routeEventTitle = String(routeParams.get("event_title") || "").trim();
const routeEventId = String(routeParams.get("from_event_record_id") || "").trim();
const appliedFocusRecordId = ref("");
const eventTitle = ref(routeEventTitle);
const sourceEventId = ref(routeEventId);

const editableFields = computed(() => sortedFields(fields.value.filter((field) => field.editable)));
const readonlyFields = computed(() => sortedFields(fields.value.filter((field) => !field.editable)));
const visibleReadonlyFields = computed(() => readonlyFields.value.filter(
  (field) => !ASSOCIATION_FIELD_NAMES.has(String(field.field_name || "")),
));
const readonlyPreviewFields = computed(() => visibleReadonlyFields.value.filter(
  (field) => Boolean(displayReadonlyValue(field.field_name)),
));
const detailPreviewFields = computed(() => [
  ...editableFields.value.map((field) => {
    const fieldName = String(field.field_name || "").trim();
    return {
      field_name: fieldName,
      label: projectFieldLabel(fieldName),
      value: String(fieldDraft[fieldName] ?? "").trim().replace("T", " "),
      wide: usesTextarea(fieldName),
    };
  }),
  ...readonlyPreviewFields.value.map((field) => {
    const fieldName = String(field.field_name || "").trim();
    return {
      field_name: fieldName,
      label: projectFieldLabel(fieldName),
      value: displayReadonlyValue(fieldName),
      wide: usesTextarea(fieldName),
    };
  }),
].filter((field) => field.field_name && field.value));
const projectFieldGroups = computed(() => {
  const grouped = new Map<ProjectFieldGroupKey, LooseDict[]>(
    PROJECT_FIELD_GROUPS.map((group) => [group.key, []]),
  );
  for (const field of editableFields.value) {
    grouped.get(projectFieldGroup(field.field_name))?.push(field);
  }
  return PROJECT_FIELD_GROUPS.map((group) => ({
    ...group,
    fields: grouped.get(group.key) || [],
  })).filter((group) => group.fields.length);
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
  [...selectedRepairRecords.value, ...repairCandidates.value],
  "title",
));

const hasWritableDraft = computed(() => {
  return fields.value.some((field) => {
    if (!field.editable) return false;
    return String(fieldDraft[field.field_name] ?? "").trim() !== "";
  });
});
const missingRequiredEditableFields = computed(() => {
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
  return !saving.value
    && !prefillLoading.value
    && hasWritableDraft.value
    && (!editingRecordId.value || hasUnsavedChanges.value);
});
const hasAnyUnsavedChanges = computed(() => (
  hasUnsavedChanges.value || followupHasUnsavedChanges.value
));
const unsavedNoticeText = computed(() => {
  if (hasUnsavedChanges.value && followupHasUnsavedChanges.value) {
    return "维修单信息和跟进记录有未保存修改";
  }
  return followupHasUnsavedChanges.value
    ? "跟进记录有未保存修改"
    : "维修单信息有未保存修改";
});
const projectSaveStateText = computed(() => {
  if (saving.value) return "保存中";
  if (missingRequiredEditableFields.value.length) return `缺 ${missingRequiredEditableFields.value.length} 项`;
  if (hasUnsavedChanges.value) return "有未保存修改";
  return editingRecordId.value ? "已保存" : "等待填写";
});
const projectSaveStateTone = computed(() => {
  if (saving.value) return "saving";
  if (missingRequiredEditableFields.value.length) return "warning";
  if (hasUnsavedChanges.value) return "dirty";
  return "saved";
});
const projectSaveStateIcon = computed(() => {
  if (saving.value) return LoaderCircle;
  if (missingRequiredEditableFields.value.length || hasUnsavedChanges.value) return AlertCircle;
  return CheckCircle2;
});
const saveDisabledReason = computed(() => {
  if (saving.value) return "正在保存";
  if (prefillLoading.value) return "关联字段正在填入";
  if (!hasWritableDraft.value) return "请先填写维修项目";
  if (editingRecordId.value && !hasUnsavedChanges.value) return "没有需要保存的修改";
  if (missingRequiredEditableFields.value.length) {
    return `请先填写：${missingRequiredEditableFields.value.join("、")}`;
  }
  return "保存维修项目";
});

const selectedRecordTitle = computed(() => selectedRecord.value?.title || "未命名维修项目");
const recordPageCount = computed(() => Math.max(1, Math.ceil(total.value / RECORD_PAGE_SIZE)));
const recordEmptyText = computed(() => (
  searchText.value ? "没有匹配的维修项目" : "暂无进行中的维修项目"
));

function usesTextarea(fieldName: unknown): boolean {
  return repairFieldUsesTextarea(fieldName);
}

function projectFieldGroup(fieldName: unknown): ProjectFieldGroupKey {
  const name = String(fieldName || "").trim();
  if (BASIC_FIELD_NAMES.has(name)) return "basic";
  if (/(故障|设备|位置|地点|机房|CMDB|唯一\s*id|编号|品牌|型号|容量|生产日期|使用年限)/i.test(name)) {
    return "basic";
  }
  if (/(维修方|维修人员|供应商|方案|措施|备件|费用|质保|开始时间|结束时间|维修进度|跟进|附件|推送群组|负责人|审批人)/.test(name)) {
    return "execution";
  }
  return "other";
}

function fieldInputId(field: LooseDict): string {
  return `repair-project-field-${encodeURIComponent(String(field.field_name || "field"))}`;
}

function projectFieldLabel(fieldName: unknown): string {
  const name = String(fieldName || "").trim();
  return PROJECT_FIELD_LABELS[name] || name;
}

function fieldValidationError(fieldName: unknown): string {
  const name = String(fieldName || "").trim();
  if (!validationAttempted.value || !missingRequiredEditableFields.value.includes(name)) return "";
  return `请填写${name}`;
}

async function focusFirstMissingField(): Promise<void> {
  const firstName = missingRequiredEditableFields.value[0];
  if (!firstName) return;
  const field = editableFields.value.find((item) => String(item.field_name || "") === firstName);
  if (!field) return;
  await nextTick();
  const control = document.getElementById(fieldInputId(field));
  control?.closest(".repair-field-control")?.scrollIntoView({ behavior: "smooth", block: "center" });
  window.setTimeout(() => control?.focus(), 180);
}

function selectedSourceSummary(ids: string[], candidates: LooseDict[], preferredKey: string): string {
  if (!ids.length) return "未选择";
  const idSet = new Set(ids);
  const seenIds = new Set<string>();
  const labels: string[] = [];
  for (const item of candidates) {
    const recordId = String(item.record_id || "").trim();
    if (!idSet.has(recordId) || seenIds.has(recordId)) continue;
    seenIds.add(recordId);
    const label = String(item[preferredKey] || item.title || "").trim();
    if (label) labels.push(label);
  }
  if (!labels.length) return "已关联";
  const preview = labels.slice(0, 2).join("；");
  return labels.length > 2 ? `${preview} 等 ${labels.length} 条` : preview;
}

function relationDisplayText(value: unknown): string {
  if (typeof value === "string" || typeof value === "number") return String(value).trim();
  if (Array.isArray(value)) {
    return value
      .map((item) => relationDisplayText(item))
      .filter(Boolean)
      .join("；");
  }
  if (!value || typeof value !== "object") return "";
  const item = value as LooseDict;
  for (const key of ["text", "name", "title", "value"]) {
    const label = relationDisplayText(item[key]);
    if (label) return label;
  }
  return "";
}

function businessRelationLabel(value: unknown): string {
  const label = relationDisplayText(value);
  if (!label) return "";
  return /(?:^|[^a-z0-9])(?:rec|localid)[a-z0-9_-]+(?:$|[^a-z0-9])/i.test(label)
    ? ""
    : label;
}

function seedSelectedSourceLabels(displayFields: LooseDict): void {
  const eventLabel = relationDisplayText(
    displayFields["事件描述"]
    || displayFields["故障发生现象描述"]
    || displayFields["故障维修原因"],
  ) || businessRelationLabel(
    displayFields["关联事件单"],
  );
  if (sourceEventId.value && eventLabel) {
    selectedEvent.value = { record_id: sourceEventId.value, title: eventLabel };
    eventTitle.value = eventLabel;
  }
  const repairLabel = relationDisplayText(
    displayFields["检修通告名称"]
    || displayFields["维修设备"],
  ) || businessRelationLabel(
    displayFields["设备检修关联"],
  );
  if (selectedRepairIds.value.length && repairLabel) {
    selectedRepairRecords.value = [{
      record_id: selectedRepairIds.value[0],
      title: repairLabel,
    }];
  }
}

function markFieldDirty(fieldName: unknown): void {
  const name = String(fieldName || "").trim();
  if (!name) return;
  dirtyFieldNames.add(name);
  hasUnsavedChanges.value = true;
}

function scopedQuery(): string {
  const params = new URLSearchParams({
    scope: props.scope || "ALL",
    q: searchText.value,
    limit: String(RECORD_PAGE_SIZE),
    offset: String((recordPage.value - 1) * RECORD_PAGE_SIZE),
  });
  const focusRecordId = String(props.focusRecordId || "").trim();
  if (focusRecordId && appliedFocusRecordId.value !== focusRecordId) {
    params.set("focus_record_id", focusRecordId);
  }
  return params.toString();
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

function openRepairStatus(): void {
  runWithUnsavedGuard(() => {
    const url = new URL("/repair-status", window.location.origin);
    url.searchParams.set("scope", props.scope || "ALL");
    navigate(url);
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

function resetDraft(): void {
  dirtyFieldNames.clear();
  hasUnsavedChanges.value = false;
  validationAttempted.value = false;
  if (editingRecordId.value && selectedRecord.value) {
    const savedDisplayFields = selectedRecord.value.display_fields || {};
    eventTitle.value = String(
      savedDisplayFields["事件描述"]
      || savedDisplayFields["故障发生现象描述"]
      || savedDisplayFields["故障维修原因"]
      || businessRelationLabel(savedDisplayFields["关联事件单"])
      || "",
    );
    sourceEventId.value = String(selectedRecord.value.source_event_id || "").trim();
    selectedRepairIds.value = Array.isArray(selectedRecord.value.source_repair_ids)
      ? selectedRecord.value.source_repair_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
      : [];
    selectedEvent.value = sourceEventId.value
      ? { record_id: sourceEventId.value, title: eventTitle.value }
      : null;
    selectedRepairRecords.value = [];
    seedSelectedSourceLabels(savedDisplayFields);
  }
  for (const key of Object.keys(fieldDraft)) delete fieldDraft[key];
  for (const key of Object.keys(prefillPreview)) delete prefillPreview[key];
  const rawFields = selectedRecord.value?.raw_fields || {};
  const displayFields = selectedRecord.value?.display_fields || {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const fieldType = Number(field.field_type || 0);
    const prefersRaw = [2, 5, 15].includes(fieldType);
    const value = prefersRaw && Object.prototype.hasOwnProperty.call(rawFields, name)
      ? rawFields[name]
      : Object.prototype.hasOwnProperty.call(displayFields, name) ? displayFields[name] : rawFields[name];
    fieldDraft[name] = repairDraftInputValue(field, value);
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
  const requestVersion = ++eventRequestVersion;
  eventAbortController?.abort();
  const abortController = new AbortController();
  eventAbortController = abortController;
  eventLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      q: eventSearchText.value,
      limit: "120",
    });
    const payload = await requestJson(
      `/api/repair-management/event-candidates?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (requestVersion !== eventRequestVersion) return;
    eventCandidates.value = Array.isArray(payload.records) ? payload.records : [];
    const selected = eventCandidates.value.find(
      (item) => eventRecordId(item) === sourceEventId.value,
    );
    if (selected) {
      selectedEvent.value = selected;
      eventTitle.value = String(selected.title || selected.alarm_desc || eventTitle.value || "");
    }
    if (!eventCandidates.value.length) {
      showMessage("当前条件下没有可选择的事件。", "warning");
    }
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (requestVersion !== eventRequestVersion) return;
    showMessage(error instanceof Error ? error.message : "事件候选读取失败。", "failed");
  } finally {
    if (eventAbortController === abortController) eventAbortController = null;
    if (requestVersion === eventRequestVersion) eventLoading.value = false;
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
  const nextRepairIds = changed ? [] : selectedRepairIds.value.slice(0, 1);
  const applied = await applyCombinedPrefill(true, {
    eventRecordId: recordId,
    repairRecordIds: nextRepairIds,
  });
  if (!applied) return;
  sourceEventId.value = recordId;
  selectedEvent.value = event;
  eventTitle.value = String(event.title || eventTitle.value || "");
  activePicker.value = "";
  if (changed) {
    selectedRepairIds.value = nextRepairIds;
    selectedRepairRecords.value = [];
    repairRecommendedIds.value = [];
  }
  hasUnsavedChanges.value = true;
  if (!quiet) showMessage("事件已关联。", "success");
}

async function loadRepairCandidates(): Promise<void> {
  const requestVersion = ++repairRequestVersion;
  repairAbortController?.abort();
  const abortController = new AbortController();
  repairAbortController = abortController;
  repairCandidateLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      event_record_id: sourceEventId.value,
      q: repairSearchText.value,
      limit: "80",
    });
    const payload = await requestJson(
      `/api/repair-management/repair-candidates?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (requestVersion !== repairRequestVersion) return;
    repairCandidates.value = Array.isArray(payload.records) ? payload.records : [];
    const selectedIdSet = new Set(selectedRepairIds.value);
    const matchedSelected = repairCandidates.value.filter(
      (item) => selectedIdSet.has(String(item.record_id || "").trim()),
    );
    if (matchedSelected.length) selectedRepairRecords.value = matchedSelected;
    repairRecommendedIds.value = Array.isArray(payload.auto_selected_ids)
      ? payload.auto_selected_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
      : [];
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (requestVersion !== repairRequestVersion) return;
    showMessage(error instanceof Error ? error.message : "设备检修候选读取失败。", "failed");
  } finally {
    if (repairAbortController === abortController) repairAbortController = null;
    if (requestVersion === repairRequestVersion) repairCandidateLoading.value = false;
  }
}

async function clearEventSelection(): Promise<void> {
  const nextRepairIds = selectedRepairIds.value.slice(0, 1);
  if (nextRepairIds.length) {
    const applied = await applyCombinedPrefill(true, {
      eventRecordId: "",
      repairRecordIds: nextRepairIds,
    });
    if (!applied) return;
  } else {
    prefillRequestVersion += 1;
    replacePrefillFields({}, true);
    prefillWarnings.value = [];
  }
  sourceEventId.value = "";
  selectedEvent.value = null;
  eventTitle.value = "";
  repairRecommendedIds.value = [];
  hasUnsavedChanges.value = true;
  showMessage("事件关联已清除。", "success");
}

async function clearRepairSelection(): Promise<void> {
  if (sourceEventId.value) {
    const applied = await applyCombinedPrefill(true, {
      eventRecordId: sourceEventId.value,
      repairRecordIds: [],
    });
    if (!applied) return;
  } else {
    prefillRequestVersion += 1;
    replacePrefillFields({}, true);
    prefillWarnings.value = [];
  }
  selectedRepairIds.value = [];
  selectedRepairRecords.value = [];
  repairRecommendedIds.value = [];
  hasUnsavedChanges.value = true;
  showMessage("检修通告关联已清除。", "success");
}

async function confirmRepairSelection(recordIds: string[]): Promise<void> {
  const nextRepairIds = recordIds.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 1);
  const applied = await applyCombinedPrefill(true, {
    eventRecordId: sourceEventId.value,
    repairRecordIds: nextRepairIds,
  });
  if (!applied) return;
  selectedRepairIds.value = nextRepairIds;
  const selectedIdSet = new Set(nextRepairIds);
  selectedRepairRecords.value = repairCandidates.value.filter(
    (item) => selectedIdSet.has(String(item.record_id || "").trim()),
  );
  repairRecommendedIds.value = [];
  activePicker.value = "";
  hasUnsavedChanges.value = true;
  showMessage(`已关联 ${selectedRepairIds.value.length} 条设备检修通告。`, "success");
}

async function handleFollowupChanged(): Promise<void> {
  invalidateRepairStatus();
  clearRecordResponseCache();
  const recordId = editingRecordId.value;
  if (!recordId) return;
  const requestVersion = ++recordDetailRequestVersion;
  recordDetailAbortController?.abort();
  const abortController = new AbortController();
  recordDetailAbortController = abortController;
  try {
    const params = new URLSearchParams({ scope: props.scope || "ALL" });
    const payload = await requestJson(
      `/api/repair-management/records/${encodeURIComponent(recordId)}?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (requestVersion !== recordDetailRequestVersion) return;
    if (recordId !== editingRecordId.value) return;
    const current = payload.record && typeof payload.record === "object"
      ? payload.record as LooseDict
      : null;
    if (!current) return;
    const index = records.value.findIndex(
      (item) => String(item.record_id || "") === recordId,
    );
    if (index >= 0) {
      const nextRecords = records.value.slice();
      nextRecords[index] = current;
      records.value = nextRecords;
    }
    selectedRecord.value = current;
    if (!hasUnsavedChanges.value) resetDraft();
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (requestVersion !== recordDetailRequestVersion) return;
    showMessage(
      `跟进记录已保存，维修项目信息稍后刷新：${error instanceof Error ? error.message : "读取失败"}`,
      "warning",
    );
  } finally {
    if (recordDetailAbortController === abortController) recordDetailAbortController = null;
  }
}

async function applyCombinedPrefill(
  quiet = false,
  selection?: PrefillSelection,
): Promise<boolean> {
  const eventRecordId = String(selection?.eventRecordId ?? sourceEventId.value).trim();
  const repairRecordIds = (selection?.repairRecordIds ?? selectedRepairIds.value)
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .slice(0, 1);
  if (!eventRecordId && !repairRecordIds.length) return true;
  const requestVersion = ++prefillRequestVersion;
  prefillAbortController?.abort();
  const abortController = new AbortController();
  prefillAbortController = abortController;
  prefillLoading.value = true;
  try {
    const payload = await requestJson("/api/repair-management/prefill", {
      method: "POST",
      body: JSON.stringify({
        scope: props.scope || "ALL",
        source_event_id: eventRecordId,
        source_repair_ids: repairRecordIds,
        source_month: currentMonthKey(),
      }),
      signal: abortController.signal,
    });
    if (requestVersion !== prefillRequestVersion) return false;
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
    return true;
  } catch (error: unknown) {
    if (abortController.signal.aborted) return false;
    if (requestVersion !== prefillRequestVersion) return false;
    showMessage(error instanceof Error ? error.message : "自动填入失败。", "failed");
    return false;
  } finally {
    if (prefillAbortController === abortController) prefillAbortController = null;
    if (requestVersion === prefillRequestVersion) prefillLoading.value = false;
  }
}

function selectRecordNow(record: LooseDict): void {
  recordDetailRequestVersion += 1;
  recordDetailAbortController?.abort();
  recordDetailAbortController = null;
  detailDialogOpen.value = false;
  createOperationId.value = "";
  followupHasUnsavedChanges.value = false;
  creatingNewProject.value = false;
  selectedRecord.value = record;
  editingRecordId.value = String(record.record_id || "");
  sourceEventId.value = String(record.source_event_id || "").trim();
  selectedRepairIds.value = Array.isArray(record.source_repair_ids)
    ? record.source_repair_ids.map((item: unknown) => String(item || "").trim()).filter(Boolean).slice(0, 1)
    : [];
  selectedRepairRecords.value = [];
  repairRecommendedIds.value = [];
  const displayFields = record.display_fields || {};
  eventTitle.value = String(
    displayFields["事件描述"]
    || displayFields["故障发生现象描述"]
    || displayFields["故障维修原因"]
    || businessRelationLabel(displayFields["关联事件单"])
    || "",
  );
  const matchedEvent = eventCandidates.value.find(
    (item) => eventRecordId(item) === sourceEventId.value,
  );
  selectedEvent.value = sourceEventId.value
    ? matchedEvent || { record_id: sourceEventId.value, title: eventTitle.value }
    : null;
  seedSelectedSourceLabels(displayFields);
  resetDraft();
  sourceExpanded.value = false;
  activeWorkspaceTab.value = "project";
}

function requestSelectRecord(record: LooseDict): void {
  if (String(record.record_id || "") === editingRecordId.value) return;
  runWithUnsavedGuard(() => selectRecordNow(record));
}

function startCreateNow(): void {
  recordDetailRequestVersion += 1;
  recordDetailAbortController?.abort();
  recordDetailAbortController = null;
  detailDialogOpen.value = false;
  createOperationId.value = "";
  followupHasUnsavedChanges.value = false;
  creatingNewProject.value = true;
  selectedRecord.value = null;
  editingRecordId.value = "";
  sourceEventId.value = "";
  eventTitle.value = "";
  selectedEvent.value = null;
  selectedRepairIds.value = [];
  selectedRepairRecords.value = [];
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
  if (searchTimer) clearTimeout(searchTimer);
  skipNextSearchReload = true;
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
    selectedRepairRecords.value = [];
    repairRecommendedIds.value = [];
    resetDraft();
    void loadRecords(false);
  });
}

async function loadRecords(announce = false): Promise<void> {
  const requestVersion = ++recordsRequestVersion;
  recordsAbortController?.abort();
  const abortController = new AbortController();
  recordsAbortController = abortController;
  loading.value = true;
  try {
    const query = new URLSearchParams(scopedQuery());
    if (announce) query.set("refresh", "1");
    const requestUrl = `/api/repair-management/records?${query.toString()}`;
    const cacheKey = requestUrl.replace(/([?&])refresh=1(?:&|$)/, "$1").replace(/[?&]$/, "");
    const cached = recordResponseCache.get(cacheKey);
    let payload: LooseDict;
    if (!announce && cached && cached.expiresAt > Date.now()) {
      payload = cached.payload;
    } else {
      payload = await requestJson(requestUrl, { signal: abortController.signal });
      recordResponseCache.set(cacheKey, {
        expiresAt: Date.now() + RECORD_RESPONSE_CACHE_TTL_MS,
        payload,
      });
      if (recordResponseCache.size > 24) {
        recordResponseCache.delete(recordResponseCache.keys().next().value || "");
      }
    }
    if (requestVersion !== recordsRequestVersion) return;
    fields.value = Array.isArray(payload.fields) ? payload.fields : [];
    records.value = Array.isArray(payload.records) ? payload.records : [];
    total.value = Number(payload.total || records.value.length || 0);
    const focusRecordId = String(props.focusRecordId || "").trim();
    if (focusRecordId && appliedFocusRecordId.value !== focusRecordId) {
      const focused = records.value.find(
        (item) => String(item.record_id || "").trim() === focusRecordId,
      );
      appliedFocusRecordId.value = focusRecordId;
      if (focused) {
        recordPage.value = Math.floor(Number(payload.offset || 0) / RECORD_PAGE_SIZE) + 1;
        selectRecordNow(focused);
      }
    }
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
      } else if (
        !hasUnsavedChanges.value
        && !followupHasUnsavedChanges.value
        && records.value.length
      ) {
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
    const schemaWarnings = Array.isArray(payload.schema_warnings)
      ? payload.schema_warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
      : [];
    if (schemaWarnings.length) {
      showMessage(schemaWarnings.join("；"), "warning");
    } else if (announce) {
      showMessage(records.value.length ? "维修项目已刷新。" : "当前筛选下没有维修项目。", records.value.length ? "success" : "warning");
    }
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (requestVersion !== recordsRequestVersion) return;
    showMessage(error instanceof Error ? error.message : "维修项目读取失败。", "failed");
  } finally {
    if (recordsAbortController === abortController) recordsAbortController = null;
    if (requestVersion === recordsRequestVersion) loading.value = false;
  }
}

function writablePayload(): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    if (editingRecordId.value && !dirtyFieldNames.has(name)) continue;
    const value = String(fieldDraft[name] ?? "");
    if (value.trim() === "" && !editingRecordId.value) continue;
    result[name] = parseDraftValue(value, field);
  }
  return result;
}

async function saveRecord(): Promise<void> {
  validationAttempted.value = true;
  if (prefillLoading.value) {
    showMessage("关联字段正在填入，请稍后保存。", "warning");
    return;
  }
  if (missingRequiredEditableFields.value.length) {
    showMessage(`请先填写：${missingRequiredEditableFields.value.join("、")}。`, "warning");
    await focusFirstMissingField();
    return;
  }
  saving.value = true;
  try {
    if (!editingRecordId.value && !createOperationId.value) {
      createOperationId.value = createRepairOperationId("repair-project");
    }
    const body = JSON.stringify({
      operation_id: editingRecordId.value ? "" : createOperationId.value,
      scope: props.scope || "ALL",
      source_event_id: sourceEventId.value,
      source_repair_ids: selectedRepairIds.value,
      replace_source_relations: true,
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
      createOperationId.value = "";
      const warnings = Array.isArray(created.warnings) ? created.warnings.filter(Boolean) : [];
      showMessage(
        warnings.length ? `维修项目已创建；${warnings.join("；")}` : "维修项目已创建。",
        warnings.length ? "warning" : "success",
      );
    }
    dirtyFieldNames.clear();
    hasUnsavedChanges.value = false;
    validationAttempted.value = false;
    detailDialogOpen.value = false;
    sourceExpanded.value = false;
    recordPage.value = 1;
    clearRecordResponseCache();
    await loadRecords(false);
    invalidateRepairStatus();
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
    selectedRepairRecords.value = [];
    repairRecommendedIds.value = [];
    resetDraft();
    activeWorkspaceTab.value = "project";
    sourceExpanded.value = true;
    showMessage("维修项目已删除。");
    invalidateRepairStatus();
    clearRecordResponseCache();
    await loadRecords(false);
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "删除失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function displayReadonlyValue(fieldName: string): string {
  const field = fields.value.find((item) => String(item.field_name || "") === fieldName) || {};
  const formatValue = (value: unknown): string => {
    const uiType = String(field.ui_type || "").toLowerCase();
    if (uiType === "progress" || fieldName === "当前维修进度") {
      const raw = String(value ?? "").trim().replace("％", "%");
      const parsed = Number.parseFloat(raw.replace("%", ""));
      if (Number.isFinite(parsed)) {
        const percent = raw.includes("%") || Math.abs(parsed) > 1 ? parsed : parsed * 100;
        return `${Math.max(0, Math.min(100, Math.round(percent)))}%`;
      }
    }
    return repairDraftInputValue(field, value).replace("T", " ");
  };
  if (Object.prototype.hasOwnProperty.call(prefillPreview, fieldName)) {
    return formatValue(prefillPreview[fieldName]);
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
  return formatValue(displayFields[fieldName]);
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
    selectedRepairRecords.value = [];
    repairRecommendedIds.value = [];
    activePicker.value = "";
    prefillWarnings.value = [];
    routeEventPrefillApplied.value = false;
    appliedFocusRecordId.value = "";
    void loadRecords(false);
  },
);

watch(
  () => String(props.focusRecordId || "").trim(),
  (focusRecordId) => {
    if (!focusRecordId || appliedFocusRecordId.value === focusRecordId) return;
    const focused = records.value.find(
      (item) => String(item.record_id || "").trim() === focusRecordId,
    );
    if (focused) {
      appliedFocusRecordId.value = focusRecordId;
      runWithUnsavedGuard(() => selectRecordNow(focused));
      return;
    }
    if (!loading.value) void loadRecords(false);
  },
);

onActivated(() => {
  activationCount += 1;
  if (activationCount === 1) return;
  const focusRecordId = String(props.focusRecordId || "").trim();
  if (!focusRecordId) return;
  const focused = records.value.find(
    (item) => String(item.record_id || "").trim() === focusRecordId,
  );
  if (focused) {
    appliedFocusRecordId.value = focusRecordId;
    if (editingRecordId.value !== focusRecordId) {
      runWithUnsavedGuard(() => selectRecordNow(focused));
    }
    return;
  }
  appliedFocusRecordId.value = "";
  if (!loading.value) void loadRecords(false);
});

watch(searchText, () => {
  if (skipNextSearchReload) {
    skipNextSearchReload = false;
    return;
  }
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
  void loadRecords(false);
});

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer);
  recordsAbortController?.abort();
  eventAbortController?.abort();
  repairAbortController?.abort();
  prefillAbortController?.abort();
  recordDetailAbortController?.abort();
});
</script>

<style scoped>
.repair-management-page {
  width: min(1720px, 100%);
  margin: 0 auto;
  padding: 16px 28px 32px;
  display: grid;
  gap: 12px;
  color: #142b49;
}

.page-unsaved-notice {
  min-height: 38px;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border: 1px solid #f2c78f;
  border-radius: 10px;
  background: #fff8ed;
  color: #96500f;
  font-size: 13px;
  font-weight: 700;
}

.page-unsaved-notice svg {
  flex: 0 0 auto;
}

.repair-hero-title {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 12px;
}

.repair-hero-title h2 {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repair-hero-title :deep(.vnet-back-button) {
  min-height: 36px;
  flex: 0 0 auto;
  border-radius: 9px;
  padding-inline: 11px;
  box-shadow: none;
}

.repair-hero,
.repair-workflow,
.event-link-panel,
.repair-source-panel,
.source-relation-panel,
.record-panel,
.editor-panel {
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  background: #fff;
  box-shadow: 0 10px 26px rgba(20, 75, 150, 0.08);
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

.source-relation-item > .relation-actions {
  display: flex;
  align-items: center;
  gap: 6px;
}

.source-loading {
  color: #176de0;
  font-size: 12px;
  font-weight: 850;
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
  padding: 12px 16px;
  background: #fff;
}

.repair-hero h2,
.editor-head h3 {
  margin: 0;
  color: #071a39;
  font-size: 20px;
  font-weight: 750;
}

.repair-hero p,
.editor-head p,
.record-panel small,
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
  grid-template-columns: minmax(300px, 320px) minmax(0, 1fr);
  align-items: stretch;
  gap: 12px;
}

.record-panel,
.editor-panel {
  min-width: 0;
  padding: 14px;
}

.record-panel {
  align-self: stretch;
  position: relative;
  min-height: 0;
  height: 100%;
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  overflow: hidden;
}

.record-list {
  min-height: 220px;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 2px 3px 4px 0;
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
  align-self: stretch;
  overflow: visible;
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
  font-size: 16px;
  font-weight: 750;
}

.record-search input {
  width: 100%;
  min-height: 36px;
  border: 1px solid #cbd8e8;
  border-radius: 8px;
  background: #fff;
  color: #10203b;
  font: inherit;
  font-size: 13px;
  font-weight: 500;
  padding: 7px 10px;
}

.record-search input:focus {
  outline: 0;
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.14);
}

.record-actions {
  margin: 10px 0;
  justify-content: flex-start;
}

.record-row {
  width: 100%;
  margin-top: 6px;
  min-height: 104px;
  padding: 10px 11px;
  display: grid;
  align-content: start;
  gap: 7px;
  text-align: left;
  border: 1px solid #e0e9f7;
  border-radius: 9px;
  background: #fbfdff;
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
  box-shadow: inset 3px 0 0 #1e63ff;
}

.record-title-line {
  min-width: 0;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
}

.record-title-line svg {
  flex: 0 0 auto;
  color: #1662d4;
}

.record-title {
  min-width: 0;
  display: block;
  color: #071a39;
  font-size: 13px;
  font-weight: 750;
  line-height: 1.45;
  overflow-wrap: anywhere;
  white-space: normal;
}

.record-meta {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.record-meta b {
  max-width: 100%;
  min-height: 21px;
  padding: 2px 7px;
  border-radius: 999px;
  background: #edf5ff;
  color: #315273;
  font-size: 11px;
  font-weight: 650;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.editor-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-bottom: 2px;
}

.record-meta b.workflow {
  border: 1px solid #c8e7df;
  background: #edf9f5;
  color: #08745f;
}

.record-time {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 5px;
  color: #6a7f99;
  font-size: 11px;
  font-weight: 650;
}

.record-time svg {
  flex: 0 0 auto;
  color: #7e94ad;
}

.record-time span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.editor-head > div {
  min-width: 0;
}

.editor-head p {
  margin: 4px 0 0;
  overflow: hidden;
  color: #60758f;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.editor-title-button {
  min-width: 0;
  max-width: 100%;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 0;
  border-radius: 7px;
  padding: 2px 4px;
  background: transparent;
  color: #10294a;
  font: inherit;
  font-size: 17px;
  font-weight: 800;
  cursor: pointer;
}

.editor-title-button span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.editor-title-button svg {
  flex: 0 0 auto;
  color: #1e63ff;
}

.editor-title-button:hover,
.editor-title-button:focus-visible {
  outline: 0;
  background: #edf5ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
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

.project-form-sections {
  display: grid;
  gap: 7px;
}

.project-field-section {
  display: grid;
  gap: 5px;
  padding: 0 0 7px;
  border-bottom: 1px solid #e4ecf6;
}

.project-field-section:last-child {
  border-bottom: 0;
}

.project-field-section > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.project-field-section h4 {
  margin: 0;
  color: #17314f;
  font-size: 13px;
  font-weight: 750;
}

.project-field-section > header span {
  color: #7a8da5;
  font-size: 11px;
  font-weight: 600;
}

.project-field-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 7px 10px;
}

.readonly-summary {
  margin-top: 2px;
  border: 1px solid #e4ecf8;
  border-radius: 10px;
  background: #f7fbff;
  padding: 8px 10px;
}

.readonly-summary summary {
  cursor: pointer;
  color: #315273;
  font-size: 12px;
  font-weight: 700;
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
  padding: 7px 9px;
  border-radius: 8px;
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
  font-size: 11px;
}

.readonly-line span {
  color: #71839b;
  font-size: 12px;
}

.editor-action-bar {
  position: sticky;
  z-index: 20;
  bottom: 8px;
  min-height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  margin-top: 4px;
  border: 1px solid #d5e2f2;
  border-radius: 10px;
  padding: 5px 8px 5px 10px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 10px 26px rgba(21, 67, 128, 0.13);
  backdrop-filter: blur(10px);
}

.project-workspace {
  scroll-padding-bottom: 84px;
}

.project-field-section:last-child,
.readonly-summary {
  scroll-margin-bottom: 84px;
}

.save-state,
.editor-action-buttons,
.btn {
  display: inline-flex;
  align-items: center;
}

.save-state {
  min-width: 0;
  gap: 7px;
  color: #536b87;
  font-size: 12px;
  font-weight: 700;
}

.save-state.saving { color: #155ec2; }
.save-state.warning,
.save-state.dirty { color: #a65314; }
.save-state.saved { color: #16805f; }

.save-state.saving svg {
  animation: repair-spin 900ms linear infinite;
}

.editor-action-buttons {
  flex: 0 0 auto;
  justify-content: flex-end;
  gap: 7px;
}

.btn {
  min-height: 36px;
  justify-content: center;
  gap: 6px;
  border: 1px solid #d8e5f7;
  border-radius: 8px;
  padding: 0 12px;
  font: inherit;
  font-size: 13px;
  font-weight: 700;
  cursor: pointer;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.btn:hover:not(:disabled) {
  border-color: #9bb9e2;
  box-shadow: 0 5px 14px rgba(20, 75, 150, 0.1);
}

.btn.primary {
  border-color: #1e63ff;
  background: #1464e7;
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

.spinning {
  animation: repair-spin 900ms linear infinite;
}

@keyframes repair-spin {
  to { transform: rotate(360deg); }
}

.empty-state {
  padding: 18px;
  border: 1px dashed #d8e5f7;
  border-radius: 16px;
  background: #f8fbff;
  font-weight: 850;
}

.repair-detail-overlay {
  position: fixed;
  z-index: 120;
  inset: 0;
  display: flex;
  justify-content: flex-end;
  background: rgba(8, 25, 52, 0.48);
}

.repair-detail-dialog {
  width: min(680px, 96vw);
  height: 100%;
  overflow: hidden;
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  border-left: 1px solid #cbdcf1;
  border-radius: 12px 0 0 12px;
  background: #fff;
  box-shadow: 0 24px 64px rgba(8, 37, 82, 0.24);
}

.repair-detail-head,
.repair-detail-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 13px 16px;
}

.repair-detail-head {
  border-bottom: 1px solid #dce7f4;
}

.repair-detail-head > div {
  min-width: 0;
}

.repair-detail-head span {
  color: #54708f;
  font-size: 11px;
  font-weight: 700;
}

.repair-detail-head h3 {
  overflow: hidden;
  margin: 2px 0 0;
  color: #10294a;
  font-size: 18px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repair-detail-head > button {
  width: 36px;
  height: 36px;
  flex: 0 0 auto;
  display: grid;
  place-items: center;
  border: 1px solid #d4e1f1;
  border-radius: 9px;
  background: #f8fbff;
  color: #486683;
  cursor: pointer;
}

.repair-detail-head > button:hover,
.repair-detail-head > button:focus-visible {
  border-color: #1e63ff;
  outline: 0;
  color: #155bc6;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.repair-detail-body {
  overflow: auto;
  padding: 14px 16px 18px;
}

.repair-detail-context {
  display: grid;
  gap: 7px;
  margin-bottom: 12px;
  border: 1px solid #d9e6f5;
  border-radius: 9px;
  padding: 9px 10px;
  background: #f7faff;
}

.repair-detail-context span {
  min-width: 0;
  overflow: hidden;
  color: #5f7690;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repair-detail-context b {
  margin-right: 8px;
  color: #214a73;
}

.repair-detail-section + .repair-detail-section {
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid #e1eaf5;
}

.repair-detail-section h4 {
  margin: 0 0 9px;
  color: #17314f;
  font-size: 13px;
}

.repair-detail-readonly-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 10px;
}

.repair-detail-readonly-grid > div {
  min-width: 0;
  display: grid;
  gap: 3px;
  border: 1px solid #e4edf7;
  border-radius: 8px;
  padding: 7px 9px;
  background: #f8fbff;
}

.repair-detail-readonly-grid > div.wide {
  grid-column: 1 / -1;
}

.repair-detail-readonly-grid b,
.repair-detail-readonly-grid span {
  overflow: hidden;
  text-overflow: ellipsis;
}

.repair-detail-readonly-grid b {
  color: #405d7c;
  font-size: 11px;
}

.repair-detail-readonly-grid span {
  color: #6b8098;
  font-size: 12px;
  line-height: 1.55;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.repair-detail-footer {
  justify-content: flex-end;
  border-top: 1px solid #dce7f4;
  background: #fbfdff;
}

@media (max-width: 1279px) and (min-width: 1024px) {
  .repair-layout {
    grid-template-columns: 280px minmax(0, 1fr);
  }

  .project-field-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-height: 900px) {
  .editor-action-bar {
    position: static;
  }
}

@media (max-width: 1023px) {
  .repair-layout,
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
    height: auto;
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

  .project-field-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .repair-detail-readonly-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .repair-management-page {
    padding-inline: 14px;
  }

  .repair-hero,
  .editor-head,
  .editor-action-bar {
    align-items: stretch;
    flex-direction: column;
  }

  .project-field-grid,
  .readonly-grid,
    .repair-detail-readonly-grid {
    grid-template-columns: 1fr;
  }

  .repair-detail-overlay {
    align-items: stretch;
  }

  .repair-detail-dialog {
    width: 100%;
    border-radius: 0;
  }

  .editor-action-buttons {
    width: 100%;
    flex-wrap: wrap;
    justify-content: flex-start;
  }
}

.btn.compact {
  min-height: 32px;
  padding-inline: 10px;
  font-size: 12px;
}
</style>
