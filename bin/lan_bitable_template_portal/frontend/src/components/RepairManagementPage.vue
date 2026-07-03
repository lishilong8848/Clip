<template>
  <section class="repair-management-page">
    <div class="page-back-row">
      <VnetBackButton to="/" />
    </div>
    <div class="repair-hero">
      <div>
        <span class="section-kicker">检修管理</span>
        <h2>检修记录管理</h2>
      </div>
      <div class="hero-actions">
        <button class="btn secondary" type="button" :disabled="loading" @click="loadRecords">
          {{ loading ? "读取中" : "刷新" }}
        </button>
        <button class="btn primary" type="button" @click="startCreate">
          新增检修记录
        </button>
      </div>
    </div>

    <MessageBanner v-if="messageText" :tone="messageTone" :text="messageText" />

    <div class="repair-stats" aria-label="检修记录状态">
      <div class="stat-card">
        <span>当前楼栋</span>
        <strong>{{ currentScopeLabel }}</strong>
      </div>
      <div class="stat-card">
        <span>检修记录</span>
        <strong>{{ records.length }} / {{ total }}</strong>
      </div>
      <div class="stat-card">
        <span>可填写字段</span>
        <strong>{{ editableFields.length }}</strong>
      </div>
      <div class="stat-card">
        <span>当前模式</span>
        <strong>{{ editingRecordId ? "编辑" : "新增" }}</strong>
      </div>
    </div>

    <div class="event-link-panel">
      <div class="event-link-head">
        <div>
          <span class="section-kicker">来源事件</span>
          <h3>选择事件后填写检修单</h3>
        </div>
        <div class="event-search">
          <input
            v-model.trim="eventSearchText"
            type="search"
            placeholder="搜索事件标题、来源、专业"
            @keydown.enter.prevent="loadEventCandidates"
          />
          <button class="btn quiet" type="button" :disabled="eventLoading" @click="loadEventCandidates">
            {{ eventLoading ? "读取中" : "查找事件" }}
          </button>
        </div>
      </div>
      <div v-if="selectedEvent" class="selected-event">
        <b>已选事件</b>
        <span>{{ selectedEvent.title || "未命名事件" }}</span>
        <small>{{ eventMeta(selectedEvent) }}</small>
      </div>
      <div v-if="eventCandidates.length" class="event-candidates">
        <button
          v-for="item in eventCandidates"
          :key="eventRecordId(item)"
          class="event-candidate"
          :class="{ active: eventRecordId(item) === sourceEventId }"
          type="button"
          @click="applyEventPrefill(eventRecordId(item))"
        >
          <span>{{ item.title || "未命名事件" }}</span>
          <small>{{ eventMeta(item) }}</small>
        </button>
      </div>
      <div v-else class="event-empty">
        {{ eventLoading ? "正在读取事件..." : "未显示事件候选，可输入关键字后查找。" }}
      </div>
    </div>

    <div class="repair-layout">
      <aside class="record-panel">
        <div class="panel-head">
          <div>
            <strong>检修记录</strong>
            <span>{{ records.length }} / {{ total }} 条</span>
          </div>
          <input
            v-model.trim="searchText"
            type="search"
            placeholder="搜索标题、楼栋、字段内容"
            @keydown.enter.prevent="loadRecords"
          />
        </div>
        <div class="record-actions">
          <button class="btn quiet" type="button" :disabled="loading" @click="loadRecords">搜索</button>
          <button class="btn quiet" type="button" :disabled="loading || !searchText" @click="clearSearch">清空</button>
        </div>
        <div v-if="loading" class="empty-state">正在读取检修表...</div>
        <div v-else-if="!records.length" class="empty-state">暂无检修记录</div>
        <button
          v-for="record in records"
          v-else
          :key="record.record_id"
          class="record-row"
          :class="{ active: selectedRecord?.record_id === record.record_id }"
          type="button"
          @click="selectRecord(record)"
        >
          <span class="record-title">{{ record.title || "未命名检修记录" }}</span>
          <span class="record-meta">
            <b>{{ recordBuildingLabel(record) }}</b>
            <b>{{ recordSpecialtyLabel(record) }}</b>
            <b>{{ recordTimeLabel(record) }}</b>
          </span>
        </button>
      </aside>

      <main class="editor-panel">
        <header class="editor-head">
          <div>
            <span class="section-kicker">{{ editingRecordId ? "编辑记录" : "新增记录" }}</span>
            <h3>{{ editingRecordId ? selectedRecordTitle : "填写检修单" }}</h3>
            <p v-if="eventTitle">来自事件：{{ eventTitle }}</p>
          </div>
          <div class="editor-actions">
            <button class="btn quiet" type="button" :disabled="saving" @click="resetDraft">重置</button>
            <button
              v-if="editingRecordId"
              class="btn danger"
              type="button"
              :disabled="saving"
              @click="deleteRecord"
            >
              {{ deleteConfirmRecordId === editingRecordId ? "确认删除" : "删除" }}
            </button>
            <button class="btn primary" type="button" :disabled="saving || !canSaveRecord" @click="saveRecord">
              {{ saving ? "保存中" : editingRecordId ? "保存修改" : "创建记录" }}
            </button>
          </div>
        </header>
        <div v-if="missingRequiredEditableFields.length" class="form-warning">
          还缺 {{ missingRequiredEditableFields.length }} 项：{{ missingRequiredEditableFields.join("、") }}
        </div>
        <section v-if="editingRecordId" class="next-step-panel">
          <div>
            <span class="section-kicker">下一步</span>
            <strong>检修记录已就绪</strong>
          </div>
          <div class="next-step-actions">
            <button class="btn quiet" type="button" :disabled="repairNoticeOpening" @click="openRepairNoticeWorkbench(true)">
              {{ repairNoticeOpening ? "刷新中" : "刷新检修后查看" }}
            </button>
            <button class="btn primary" type="button" @click="openRepairNoticeWorkbench(false)">
              去发检修通告
            </button>
          </div>
        </section>

        <div v-if="!fields.length && loading" class="empty-state">正在读取字段...</div>
        <div v-else-if="!editableFields.length" class="empty-state">暂无可填写字段</div>
        <div v-else class="field-grid">
          <label
            v-for="field in editableFields"
            :key="field.field_name"
            class="field-card"
            :class="{ required: isRequiredField(field.field_name) && !editingRecordId }"
          >
            <span>
              <b>{{ field.field_name }}</b>
              <small>{{ fieldBadge(field) }}</small>
            </span>
            <select
              v-if="field.options?.length"
              v-model="fieldDraft[field.field_name]"
              :required="!editingRecordId && isRequiredField(field.field_name)"
            >
              <option value="">请选择{{ field.field_name }}</option>
              <option v-for="option in field.options" :key="option" :value="option">
                {{ option }}
              </option>
            </select>
            <textarea
              v-else
              v-model="fieldDraft[field.field_name]"
              placeholder="填写字段内容"
              :required="!editingRecordId && isRequiredField(field.field_name)"
              rows="2"
            />
          </label>
        </div>
        <details v-if="readonlyPreviewFields.length" class="readonly-summary" :open="!editingRecordId">
          <summary>
            {{ editingRecordId ? "查看只读字段" : "只读核心字段" }}（{{ readonlyFields.length }} 项）
          </summary>
          <div class="readonly-grid">
            <div v-for="field in readonlyPreviewFields" :key="field.field_name" class="readonly-line">
              <b>{{ field.field_name }}</b>
              <span>{{ displayReadonlyValue(field.field_name) || "未填写" }}</span>
            </div>
          </div>
        </details>
      </main>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { requestJson } from "../api/client";
import { navigateHard } from "../navigation";
import {
  REPAIR_REQUIRED_FIELD_GROUPS,
  isRequiredRepairField as isRequiredField,
  parseRepairDraftValue as parseDraftValue,
  repairCurrentScopeLabel,
  repairEventMeta as eventMeta,
  repairEventRecordId as eventRecordId,
  repairFieldBadge,
  repairFieldValueToText as fieldValueToText,
  repairRecordBuildingLabel as recordBuildingLabel,
  repairRecordSpecialtyLabel as recordSpecialtyLabel,
  repairRecordTimeLabel as recordTimeLabel,
  sortedRepairFields as sortedFields,
} from "../repairManagementUtils";
import type { LooseDict, ScopeOption } from "../types";
import MessageBanner from "./MessageBanner.vue";
import VnetBackButton from "./VnetBackButton.vue";

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
const deleteConfirmRecordId = ref("");
const fieldDraft = reactive<Record<string, string>>({});
const eventLoading = ref(false);
const eventSearchText = ref("");
const eventCandidates = ref<LooseDict[]>([]);
const selectedEvent = ref<LooseDict | null>(null);
const routeEventPrefillApplied = ref(false);
const repairNoticeOpening = ref(false);

const routeParams = new URLSearchParams(window.location.search);
const eventTitle = ref(String(routeParams.get("event_title") || "").trim());
const sourceEventId = ref(String(routeParams.get("from_event_record_id") || "").trim());

const editableFields = computed(() => sortedFields(fields.value.filter((field) => field.editable)));
const readonlyFields = computed(() => sortedFields(fields.value.filter((field) => !field.editable)));
const readonlyPreviewFields = computed(() => {
  if (editingRecordId.value) return readonlyFields.value.slice(0, 12);
  const core = readonlyFields.value.filter((field) => isRequiredField(field.field_name));
  return (core.length ? core : readonlyFields.value).slice(0, 16);
});

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

const selectedRecordTitle = computed(() => selectedRecord.value?.title || "未命名检修记录");
const currentScopeLabel = computed(() => repairCurrentScopeLabel(props.scope, props.scopeOptions));

function fieldBadge(field: LooseDict): string {
  return repairFieldBadge(field, editingRecordId.value);
}

function scopedQuery(): string {
  return new URLSearchParams({ scope: props.scope || "ALL", q: searchText.value, limit: "250" }).toString();
}

function currentMonthKey(): string {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function showMessage(text: string, tone: "success" | "warning" | "failed" = "success"): void {
  messageText.value = text;
  messageTone.value = tone;
}

function resetDraft(): void {
  for (const key of Object.keys(fieldDraft)) delete fieldDraft[key];
  const rawFields = selectedRecord.value?.raw_fields || {};
  const displayFields = selectedRecord.value?.display_fields || {};
  for (const field of editableFields.value) {
    const name = String(field.field_name || "");
    const rawValue = Object.prototype.hasOwnProperty.call(rawFields, name) ? rawFields[name] : displayFields[name];
    fieldDraft[name] = fieldValueToText(rawValue);
  }
  if (!editingRecordId.value && eventTitle.value) prefillEventTitle();
}

function applyPrefillFields(fieldsPayload: LooseDict): void {
  if (!fieldsPayload || typeof fieldsPayload !== "object") return;
  const editableNames = new Set(editableFields.value.map((field) => String(field.field_name || "")));
  for (const [name, value] of Object.entries(fieldsPayload)) {
    if (!editableNames.has(name)) continue;
    fieldDraft[name] = fieldValueToText(value);
  }
}

function prefillEventTitle(): void {
  const titleField = fields.value.find((field) => {
    const name = String(field.field_name || "");
    return field.editable && ["检修通告名称", "维修名称", "标题", "名称"].includes(name);
  });
  if (titleField && !String(fieldDraft[titleField.field_name] || "").trim()) {
    fieldDraft[titleField.field_name] = eventTitle.value;
  }
  if (sourceEventId.value) {
    const sourceField = fields.value.find((field) => {
      const name = String(field.field_name || "");
      return field.editable && ["来源事件记录ID", "事件记录ID", "事件多维记录ID", "关联事件ID", "来源事件", "关联事件"].includes(name);
    });
    if (sourceField && !String(fieldDraft[sourceField.field_name] || "").trim()) {
      fieldDraft[sourceField.field_name] = sourceEventId.value;
    }
  }
}

async function loadEventCandidates(): Promise<void> {
  eventLoading.value = true;
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      q: eventSearchText.value,
      limit: "30",
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
  if (editingRecordId.value) {
    selectedRecord.value = null;
    editingRecordId.value = "";
    deleteConfirmRecordId.value = "";
    resetDraft();
  }
  try {
    const params = new URLSearchParams({
      scope: props.scope || "ALL",
      month: currentMonthKey(),
      record_id: cleanRecordId,
    });
    const payload = await requestJson(`/api/repair-management/event-prefill?${params.toString()}`);
    const event = payload.event && typeof payload.event === "object" ? payload.event : {};
    selectedEvent.value = event;
    sourceEventId.value = String(event.record_id || cleanRecordId);
    eventTitle.value = String(event.title || eventTitle.value || "");
    applyPrefillFields(payload.fields && typeof payload.fields === "object" ? payload.fields : {});
    if (!quiet) {
      showMessage("已按来源事件预填检修单，请检查并补齐剩余字段。", "success");
    }
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "事件预填失败。", "failed");
  }
}

function selectRecord(record: LooseDict): void {
  selectedRecord.value = record;
  editingRecordId.value = String(record.record_id || "");
  deleteConfirmRecordId.value = "";
  resetDraft();
}

function startCreate(): void {
  selectedRecord.value = null;
  editingRecordId.value = "";
  deleteConfirmRecordId.value = "";
  resetDraft();
  showMessage("请填写右侧字段后创建检修记录。", "warning");
}

function clearSearch(): void {
  searchText.value = "";
  void loadRecords();
}

async function loadRecords(): Promise<void> {
  loading.value = true;
  messageText.value = "";
  try {
    const payload = await requestJson(`/api/repair-management/records?${scopedQuery()}`);
    fields.value = Array.isArray(payload.fields) ? payload.fields : [];
    records.value = Array.isArray(payload.records) ? payload.records : [];
    total.value = Number(payload.total || records.value.length || 0);
    if (editingRecordId.value) {
      selectedRecord.value = records.value.find((item) => String(item.record_id || "") === editingRecordId.value) || selectedRecord.value;
    }
    if (!editingRecordId.value && !selectedRecord.value) resetDraft();
    if (sourceEventId.value && !editingRecordId.value && !routeEventPrefillApplied.value) {
      routeEventPrefillApplied.value = true;
      await applyEventPrefill(sourceEventId.value, true);
    }
    showMessage(records.value.length ? "检修记录已读取。" : "当前筛选下没有检修记录。");
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "检修记录读取失败。", "failed");
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
  deleteConfirmRecordId.value = "";
  try {
    const body = JSON.stringify({
      scope: props.scope || "ALL",
      source_event_id: sourceEventId.value,
      fields: writablePayload(),
    });
    if (editingRecordId.value) {
      await requestJson(`/api/repair-management/records/${encodeURIComponent(editingRecordId.value)}`, {
        method: "PUT",
        body,
      });
      showMessage("检修记录已保存。");
    } else {
      const created = await requestJson("/api/repair-management/records", {
        method: "POST",
        body,
      });
      editingRecordId.value = String(created.record_id || "");
      showMessage("检修记录已创建。");
    }
    await loadRecords();
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "保存失败。", "failed");
  } finally {
    saving.value = false;
  }
}

async function deleteRecord(): Promise<void> {
  if (!editingRecordId.value) return;
  if (deleteConfirmRecordId.value !== editingRecordId.value) {
    deleteConfirmRecordId.value = editingRecordId.value;
    showMessage(`将删除检修记录「${selectedRecordTitle.value}」，请再次点击确认删除。`, "warning");
    return;
  }
  saving.value = true;
  try {
    const params = new URLSearchParams({ scope: props.scope || "ALL" });
    await requestJson(`/api/repair-management/records/${encodeURIComponent(editingRecordId.value)}?${params.toString()}`, {
      method: "DELETE",
    });
    selectedRecord.value = null;
    editingRecordId.value = "";
    deleteConfirmRecordId.value = "";
    resetDraft();
    showMessage("检修记录已删除。");
    await loadRecords();
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "删除失败。", "failed");
  } finally {
    saving.value = false;
  }
}

function displayReadonlyValue(fieldName: string): string {
  if (!editingRecordId.value) {
    if (["检修通告名称", "维修名称", "标题", "名称"].includes(fieldName) && eventTitle.value) {
      return eventTitle.value;
    }
    if (["来源事件记录ID", "事件记录ID", "事件多维记录ID", "关联事件ID", "来源事件", "关联事件", "关联事件单"].includes(fieldName) && sourceEventId.value) {
      return sourceEventId.value;
    }
  }
  const displayFields = selectedRecord.value?.display_fields || {};
  return fieldValueToText(displayFields[fieldName]);
}

async function openRepairNoticeWorkbench(refreshFirst = false): Promise<void> {
  const url = new URL("/workbench-lite", window.location.origin);
  url.searchParams.set("scope", props.scope || "ALL");
  url.searchParams.set("work_type", "repair");
  if (editingRecordId.value) url.searchParams.set("record_id", editingRecordId.value);
  if (!refreshFirst) {
    navigateHard(url);
    return;
  }
  repairNoticeOpening.value = true;
  try {
    const params = new URLSearchParams({ scope: props.scope || "ALL" });
    await requestJson(`/api/repair-refresh?${params.toString()}`);
    navigateHard(url);
  } catch (error: unknown) {
    showMessage(error instanceof Error ? error.message : "刷新检修失败，请稍后再试。", "failed");
  } finally {
    repairNoticeOpening.value = false;
  }
}

watch(
  () => props.scope,
  () => {
    deleteConfirmRecordId.value = "";
    eventCandidates.value = [];
    selectedEvent.value = null;
    routeEventPrefillApplied.value = false;
    void loadRecords();
    void loadEventCandidates();
  },
);

onMounted(() => {
  if (routeParams.get("mode") === "create") startCreate();
  void loadRecords();
  void loadEventCandidates();
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
.stat-card,
.event-link-panel,
.record-panel,
.editor-panel {
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 22px 58px rgba(20, 75, 150, 0.12);
}

.repair-hero {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  padding: 20px 24px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(244, 249, 255, 0.96)),
    radial-gradient(circle at 92% 18%, rgba(42, 119, 255, 0.12), transparent 30%);
}

.repair-hero h2,
.editor-head h3 {
  margin: 7px 0 4px;
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

.repair-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.stat-card {
  min-height: 72px;
  padding: 13px 16px;
  display: grid;
  align-content: center;
  gap: 4px;
}

.stat-card span {
  color: #62758f;
  font-size: 12px;
  font-weight: 850;
}

.stat-card strong {
  color: #0b3f9f;
  font-size: 22px;
  font-weight: 950;
}

.event-link-panel {
  padding: 16px;
  display: grid;
  gap: 12px;
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
  .repair-stats,
  .repair-layout,
  .field-grid,
  .event-candidates {
    grid-template-columns: 1fr;
  }

  .event-link-head,
  .event-search {
    grid-template-columns: 1fr;
    flex-direction: column;
  }
}
</style>
