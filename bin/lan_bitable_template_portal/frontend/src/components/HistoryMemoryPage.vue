<template>
  <section class="history-memory">
    <div class="page-back-row">
      <VnetBackButton to="/" />
    </div>
    <header class="page-head">
      <div>
        <strong>历史通告记忆导入</strong>
      </div>
    </header>

    <div v-if="checking" class="notice-box">正在校验管理员身份...</div>
    <div v-else-if="!loggedIn" class="notice-box">
      请先登录飞书后再使用历史记忆导入。
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </div>
    <div v-else-if="!isAdmin" class="notice-box danger">仅管理员可使用历史通告记忆导入。</div>

    <template v-else>
      <HistoryMemorySteps :steps="historySteps" :active-key="historyStep" />
      <section class="scan-bar">
        <div class="scan-fields">
          <label>
            范围
            <select v-model.number="months">
              <option :value="3">近 3 个月</option>
              <option :value="6">近 6 个月</option>
            </select>
          </label>
          <label v-for="type in workTypes" :key="type.value" class="check">
            <input v-model="selectedWorkTypes" type="checkbox" :value="type.value" />
            {{ type.label }}
          </label>
        </div>
        <div class="scan-actions">
          <button class="btn blue" :disabled="Boolean(scanDisabledReason)" :title="scanDisabledReason" @click="scanHistory">
            {{ busy ? "扫描中" : "扫描历史通告" }}
          </button>
          <DisabledReason v-if="scanDisabledReason && !busy" :text="scanDisabledReason" />
        </div>
      </section>

      <MessageBanner
        v-if="message"
        :tone="messageType === 'failed' ? 'failed' : messageType === 'success' ? 'success' : 'info'"
        :text="message"
      />
      <MessageBanner
        v-if="warnings.length"
        tone="warning"
        title="需要注意"
        :items="warnings"
      />

      <section class="filters">
        <div class="filters-head">
          <div>
            <strong>筛选当前月事项</strong>
            <span>{{ filteredSources.length }} / {{ sourceItems.length }} 条</span>
          </div>
          <button v-if="historyFilterCount" class="btn ghost compact-btn" type="button" @click="clearHistoryFilters">
            清空筛选 {{ historyFilterCount }}
          </button>
        </div>
        <div class="filters-fields">
          <label>
            类型
            <select v-model="filterType">
              <option value="">全部</option>
              <option v-for="type in workTypes" :key="type.value" :value="type.value">{{ type.label }}</option>
            </select>
          </label>
          <label>
            楼栋
            <select v-model="filterBuilding">
              <option value="">全部</option>
              <option v-for="building in buildingOptions" :key="building" :value="building">{{ building }}</option>
            </select>
          </label>
          <label>
            匹配
            <select v-model="filterMatch">
              <option value="">全部</option>
              <option value="selected">已勾选保存</option>
              <option value="matched">有候选</option>
              <option value="unmatched">未匹配</option>
            </select>
          </label>
          <input v-model="query" placeholder="搜索标题、楼栋、专业" />
        </div>
      </section>

      <HistoryMemorySummaryGrid
        :source-count="sourceItems.length"
        :candidate-count="candidates.length"
        :selected-count="selectedCount"
        :recommended-count="recommendedMatchCount"
        :overwrite-hint="overwriteHint"
      />

      <section class="match-layout">
        <aside class="panel source-panel">
          <div class="panel-head">
            <h2>当前月事项</h2>
            <span>{{ filteredSources.length }}</span>
          </div>
          <div class="list">
            <article
              v-for="item in filteredSources"
              :key="item.id"
              class="source-row"
              :class="{ active: item.id === activeSourceId, selected: isSelected(item.id), unmatched: !candidateIdFor(item.id), remembered: sourceHasMemory(item) }"
              @click="selectSource(item)"
            >
              <div>
                <span>{{ workTypeLabel(item.work_type) }} · {{ item.building || "-" }}</span>
                <strong>{{ item.title || item.memory_name }}</strong>
                <small>
                  {{ item.specialty || "-" }}{{ item.maintenance_cycle ? ` · ${item.maintenance_cycle}` : "" }}
                  <template v-if="item.source_status"> · {{ item.source_status }}</template>
                  · {{ sourceFieldOriginLabel(item.id) }}
                </small>
              </div>
              <label @click.stop>
                <input v-model="selectedMap[item.id]" type="checkbox" />
                保存
              </label>
            </article>
            <div v-if="!filteredSources.length" class="panel-empty">
              暂无事项
            </div>
          </div>
        </aside>

        <section class="panel detail-panel">
          <div class="panel-head">
            <h2>字段确认</h2>
            <span>{{ activeSource?.title || "未选择" }}</span>
          </div>
          <div v-if="!activeSource" class="empty">请选择左侧事项。</div>
          <template v-else>
            <div class="match-note">
              <strong>{{ activeFieldOriginLabel }}</strong>
              <span>{{ activeCandidateHint }}</span>
            </div>
            <div class="field-scroll">
              <div class="field-summary">
                <span>核心字段 {{ primaryFieldDefs.length }} 项</span>
                <button
                  v-if="extraFieldDefs.length"
                  class="field-toggle"
                  type="button"
                  @click="showExtraFields = !showExtraFields"
                >
                  {{ showExtraFields ? "收起扩展字段" : `展开扩展字段 ${extraFieldDefs.length} 项` }}
                </button>
              </div>

              <div class="form-grid compact">
                <label
                  v-for="field in primaryFieldDefs"
                  :key="field.key"
                  :class="{ wide: field.multi }"
                >
                  {{ field.label }}
                  <textarea
                    v-if="field.multi"
                    v-model="activeFields[field.key]"
                    :placeholder="field.label"
                    rows="2"
                    @input="touchFields"
                  ></textarea>
                  <input
                    v-else
                    v-model="activeFields[field.key]"
                    :placeholder="field.label"
                    @input="touchFields"
                  />
                </label>
              </div>

              <section v-if="extraFieldDefs.length" class="extra-field-section" :class="{ open: showExtraFields }">
                <div v-show="showExtraFields" class="form-grid compact extra-grid">
                  <label
                    v-for="field in extraFieldDefs"
                    :key="field.key"
                    :class="{ wide: field.multi }"
                  >
                    {{ field.label }}
                    <textarea
                      v-if="field.multi"
                      v-model="activeFields[field.key]"
                      :placeholder="field.label"
                      rows="2"
                      @input="touchFields"
                    ></textarea>
                    <input
                      v-else
                      v-model="activeFields[field.key]"
                      :placeholder="field.label"
                      @input="touchFields"
                    />
                  </label>
                </div>
              </section>
            </div>
          </template>
        </section>

        <aside class="panel candidate-panel">
          <div class="panel-head">
            <h2>历史候选</h2>
            <span>{{ candidateOptions.length }}</span>
          </div>
          <div class="list">
            <article
              v-for="candidate in candidateOptions"
              :key="candidate.id"
              class="candidate-row"
              :class="{ active: candidate.id === candidateIdFor(activeSourceId) }"
              @click="chooseCandidate(candidate)"
            >
              <div>
                <span>{{ workTypeLabel(candidate.work_type) }} · {{ candidate.building || "-" }}</span>
                <strong>{{ candidate.title }}</strong>
                <small>{{ candidate.business_time || "-" }}{{ candidate.maintenance_cycle ? ` · ${candidate.maintenance_cycle}` : "" }}</small>
              </div>
            </article>
            <div v-if="!candidateOptions.length" class="panel-empty">
              暂无候选
            </div>
          </div>
        </aside>
      </section>

      <section v-if="sourceItems.length || candidates.length" class="history-save-footer">
        <div class="footer-summary">
          <strong>保存历史记忆</strong>
          <span>已勾选 {{ selectedCount }} 条 · 推荐可填充 {{ recommendedMatchCount }} 条 · 覆盖 {{ overwriteHint }}</span>
        </div>
        <div class="footer-actions">
          <button class="btn green" :disabled="Boolean(fillRecommendedDisabledReason)" :title="fillRecommendedDisabledReason" @click="fillCandidatesAndConfirmSave">
            填充推荐并保存 {{ recommendedMatchCount }}
          </button>
          <button class="btn blue" :disabled="Boolean(saveSelectedDisabledReason)" :title="saveSelectedDisabledReason" @click="saveSelected">
            保存已勾选 {{ selectedCount }}
          </button>
          <DisabledReason v-if="historySaveDisabledReason && !busy" :text="historySaveDisabledReason" />
        </div>
      </section>

      <ConfirmDialog
        :open="saveConfirmOpen"
        tone="primary"
        kicker="历史记忆保存"
        :title="saveConfirmTitle"
        :message="saveConfirmSubtitle"
        :details="saveConfirmDetails"
        :confirm-label="busy ? '保存中' : '确认批量保存'"
        cancel-label="取消"
        confirm-class="green"
        @resolve="resolveSaveConfirm"
      />
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { requestJson, type Dict } from "../api/client";
import {
  HISTORY_MEMORY_EDITABLE_FIELD_DEFS as editableFieldDefs,
  HISTORY_MEMORY_PRIMARY_FIELD_KEYS as primaryFieldKeys,
  HISTORY_MEMORY_WORK_TYPES as workTypes,
  buildHistoryMemorySavePayload,
  historyMemoryFieldVisible,
  historyMemoryOriginLabel,
  historyMemorySourceHasMemory as sourceHasMemory,
  historyMemorySourceInitialFields as sourceInitialFields,
  historyMemorySourceInitialOrigin as sourceInitialOrigin,
  historyMemoryWorkTypeLabel as workTypeLabel,
} from "../historyMemoryUtils";
import ConfirmDialog from "./ConfirmDialog.vue";
import DisabledReason from "./DisabledReason.vue";
import HistoryMemorySteps from "./HistoryMemorySteps.vue";
import HistoryMemorySummaryGrid from "./HistoryMemorySummaryGrid.vue";
import MessageBanner from "./MessageBanner.vue";
import VnetBackButton from "./VnetBackButton.vue";

defineProps<{
  checking: boolean;
  loggedIn: boolean;
  isAdmin: boolean;
  loginUrl: string;
}>();

const busy = ref(false);
const months = ref(3);
const selectedWorkTypes = ref(["maintenance", "change", "repair"]);
const sourceItems = ref<Dict[]>([]);
const candidates = ref<Dict[]>([]);
const matches = ref<Dict[]>([]);
const warnings = ref<string[]>([]);
const message = ref("");
const messageType = ref("");
const activeSourceId = ref("");
const showExtraFields = ref(false);
const filterType = ref("");
const filterBuilding = ref("");
const filterMatch = ref("");
const query = ref("");
const selectedMap = reactive<Record<string, boolean>>({});
const candidateMap = reactive<Record<string, string>>({});
const fieldEdits = reactive<Record<string, Dict>>({});
const fieldOriginMap = reactive<Record<string, string>>({});
const saveConfirmOpen = ref(false);
const pendingSavePayload = ref<Dict[]>([]);
const saveConfirmMode = ref<"selected" | "filled">("selected");

const buildingOptions = computed(() => {
  const values = new Set<string>();
  for (const item of sourceItems.value) if (item.building) values.add(String(item.building));
  return Array.from(values).sort();
});
const matchBySource = computed(() => {
  const map = new Map<string, Dict>();
  for (const item of matches.value) map.set(String(item.source_id || ""), item);
  return map;
});
const sourceById = computed(() => {
  const map = new Map<string, Dict>();
  for (const item of sourceItems.value) if (item.id) map.set(String(item.id), item);
  return map;
});
const candidateById = computed(() => {
  const map = new Map<string, Dict>();
  for (const item of candidates.value) if (item.id) map.set(String(item.id), item);
  return map;
});
const activeSource = computed(() => sourceItems.value.find((item) => item.id === activeSourceId.value));
const activeMatch = computed(() => matchBySource.value.get(activeSourceId.value));
const activeCandidate = computed(() => candidates.value.find((item) => item.id === candidateIdFor(activeSourceId.value)));
const activeFieldOriginLabel = computed(() => sourceFieldOriginLabel(activeSourceId.value));
const activeCandidateHint = computed(() => {
  if (fieldOriginMap[activeSourceId.value] === "candidate" && activeCandidate.value) {
    return `已套用历史候选：${activeCandidate.value.title || "未命名候选"}`;
  }
  if (activeCandidate.value) {
    return `推荐候选：${activeCandidate.value.title || "未命名候选"}`;
  }
  return activeMatch.value?.reason || "";
});
const primaryFieldDefs = computed(() => editableFieldDefs.filter((field) => fieldVisible(field.key) && primaryFieldKeys.has(field.key)));
const extraFieldDefs = computed(() => editableFieldDefs.filter((field) => fieldVisible(field.key) && !primaryFieldKeys.has(field.key)));
const activeFields = computed(() => {
  if (!activeSourceId.value) return {};
  if (!fieldEdits[activeSourceId.value]) fieldEdits[activeSourceId.value] = {};
  return fieldEdits[activeSourceId.value];
});
const selectedCount = computed(() => Object.keys(selectedMap).filter((key) => selectedMap[key]).length);
const recommendedMatches = computed(() => {
  return matches.value
    .filter((match) => match.selected !== false)
    .map((match) => {
      const sourceId = String(match.source_id || "");
      const candidateId = String(match.candidate_id || "");
      const source = sourceById.value.get(sourceId);
      const candidate = candidateById.value.get(candidateId);
      return source && candidate ? { match, source, candidate } : null;
    })
    .filter(Boolean) as Array<{ match: Dict; source: Dict; candidate: Dict }>;
});
const recommendedMatchCount = computed(() => recommendedMatches.value.length);
const overwriteHint = computed(() => selectedCount.value ? `${selectedOverwriteCount.value} 条` : "-");
const selectedOverwriteCount = computed(() => sourceItems.value.filter((source) => isSelected(source.id) && sourceHasMemory(source)).length);
const scanDisabledReason = computed(() => {
  if (busy.value) return "正在处理历史记忆，请等待当前操作完成。";
  if (!selectedWorkTypes.value.length) return "请至少选择一种通告类型后再扫描。";
  return "";
});
const fillRecommendedDisabledReason = computed(() => {
  if (busy.value) return "正在处理历史记忆，请等待当前操作完成。";
  if (!recommendedMatchCount.value) return "没有推荐候选。";
  return "";
});
const saveSelectedDisabledReason = computed(() => {
  if (busy.value) return "正在处理历史记忆，请等待当前操作完成。";
  if (!selectedCount.value) return "请先勾选至少一条要保存的当前月事项。";
  return "";
});
const historySaveDisabledReason = computed(() => {
  if (selectedCount.value || recommendedMatchCount.value) return "";
  return "暂无可保存内容。";
});
const saveSummary = computed(() => {
  const payload = pendingSavePayload.value;
  return {
    total: payload.length,
    candidate: payload.filter((item) => item.field_origin === "candidate").length,
    overwrite: payload.filter((item) => sourceHasMemory(item.source_item)).length,
  };
});
const saveConfirmTitle = computed(() => saveConfirmMode.value === "filled" ? "确认一键填充并保存" : "确认保存已勾选记忆");
const saveConfirmSubtitle = computed(() => {
  if (saveConfirmMode.value === "filled") {
    return "保存匹配记忆。";
  }
  return "保存已勾选记忆。";
});
const savePreviewItems = computed(() => pendingSavePayload.value.slice(0, 6).map((item, index) => {
  const source = item.source_item || {};
  return {
    key: String(source.id || source.source_record_id || item.candidate_id || `preview-${index}`),
    type: workTypeLabel(source.work_type || "maintenance"),
    building: source.building || "",
    title: source.title || source.memory_name || "未命名事项",
    origin: item.field_origin === "candidate" ? "使用历史候选字段" : sourceFieldOriginLabel(source.id || ""),
  };
}));
const saveConfirmDetails = computed(() => {
  const summary = saveSummary.value;
  const rows = [
    `保存数量：${summary.total}`,
    `历史候选填充：${summary.candidate}`,
    `会覆盖旧记忆：${summary.overwrite}`,
  ];
  for (const item of savePreviewItems.value) {
    rows.push(`${item.type} · ${item.building || "-"} · ${item.title}（${item.origin}）`);
  }
  const hiddenCount = summary.total - savePreviewItems.value.length;
  if (hiddenCount > 0) rows.push(`未展示 ${hiddenCount} 条`);
  return rows;
});
const historyStep = computed(() => {
  if (!sourceItems.value.length && !candidates.value.length) return "scan";
  if (selectedCount.value > 0 || recommendedMatchCount.value > 0) return "confirm";
  return "review";
});
const historySteps = computed(() => [
  {
    key: "scan",
    index: "1",
    title: "扫描历史",
    text: sourceItems.value.length ? `已识别 ${sourceItems.value.length}` : "待扫描",
    done: sourceItems.value.length > 0 || candidates.value.length > 0,
  },
  {
    key: "review",
    index: "2",
    title: "确认匹配",
    text: recommendedMatchCount.value ? `推荐 ${recommendedMatchCount.value}` : "待选择",
    done: selectedCount.value > 0,
  },
  {
    key: "confirm",
    index: "3",
    title: "保存记忆",
    text: selectedCount.value ? `准备保存 ${selectedCount.value}` : "待保存",
    done: false,
  },
]);
const filteredSources = computed(() => {
  const q = query.value.trim().toLowerCase();
  return sourceItems.value.filter((item) => {
    if (filterType.value && item.work_type !== filterType.value) return false;
    if (filterBuilding.value && item.building !== filterBuilding.value) return false;
    const hasCandidate = Boolean(candidateIdFor(item.id));
    if (filterMatch.value === "selected" && !isSelected(item.id)) return false;
    if (filterMatch.value === "matched" && !hasCandidate) return false;
    if (filterMatch.value === "unmatched" && hasCandidate) return false;
    if (!q) return true;
    return [item.title, item.memory_name, item.building, item.specialty].join(" ").toLowerCase().includes(q);
  });
});
const candidateOptions = computed(() => {
  const source = activeSource.value;
  if (!source) return candidates.value;
  const sourceCodes = new Set(Array.isArray(source.building_codes) ? source.building_codes : []);
  return candidates.value.filter((candidate) => {
    if (candidate.work_type !== source.work_type) return false;
    const candidateCodes = new Set(Array.isArray(candidate.building_codes) ? candidate.building_codes : []);
    if (!sourceCodes.size || !candidateCodes.size) return true;
    for (const code of sourceCodes) if (candidateCodes.has(code)) return true;
    return false;
  });
});
const historyFilterCount = computed(() => {
  return [filterType.value, filterBuilding.value, filterMatch.value, query.value]
    .filter((value) => String(value || "").trim()).length;
});

watch(filteredSources, (items) => {
  if (!activeSourceId.value || !items.some((item) => item.id === activeSourceId.value)) {
    activeSourceId.value = items[0]?.id || "";
  }
});

watch(activeSourceId, () => {
  showExtraFields.value = false;
});

const api = requestJson;

function candidateIdFor(sourceId: string): string {
  return candidateMap[sourceId] || "";
}

function isSelected(sourceId: string): boolean {
  return Boolean(selectedMap[sourceId]);
}

function sourceFieldOriginLabel(sourceId: string): string {
  return historyMemoryOriginLabel(fieldOriginMap[sourceId] || "");
}

function selectSource(item: Dict): void {
  activeSourceId.value = item.id;
  if (!fieldEdits[item.id]) {
    fieldEdits[item.id] = sourceInitialFields(item);
    fieldOriginMap[item.id] = sourceInitialOrigin(item);
  }
}

function fieldVisible(key: string): boolean {
  return historyMemoryFieldVisible(key, activeSource.value?.work_type || "maintenance");
}

function applyScanPayload(data: Dict): void {
  sourceItems.value = Array.isArray(data.source_items) ? data.source_items : [];
  candidates.value = Array.isArray(data.candidates) ? data.candidates : [];
  matches.value = Array.isArray(data.matches) ? data.matches : [];
  warnings.value = Array.isArray(data.warnings) ? data.warnings : [];
  for (const key of Object.keys(selectedMap)) delete selectedMap[key];
  for (const key of Object.keys(candidateMap)) delete candidateMap[key];
  for (const key of Object.keys(fieldEdits)) delete fieldEdits[key];
  for (const key of Object.keys(fieldOriginMap)) delete fieldOriginMap[key];
  for (const source of sourceItems.value) {
    if (!source?.id) continue;
    selectedMap[source.id] = false;
    fieldEdits[source.id] = sourceInitialFields(source);
    fieldOriginMap[source.id] = sourceInitialOrigin(source);
  }
  for (const match of matches.value) {
    const sourceId = String(match.source_id || "");
    if (!sourceId) continue;
    candidateMap[sourceId] = String(match.candidate_id || "");
  }
  activeSourceId.value = sourceItems.value[0]?.id || "";
}

function clearHistoryFilters(): void {
  filterType.value = "";
  filterBuilding.value = "";
  filterMatch.value = "";
  query.value = "";
}

async function scanHistory(): Promise<void> {
  busy.value = true;
  message.value = "扫描中...";
  messageType.value = "";
  try {
    const data = await api("/api/admin/notice-memory/history-scan", {
      method: "POST",
      body: JSON.stringify({ months: months.value, work_types: selectedWorkTypes.value }),
    });
    applyScanPayload(data);
    const counts = data.counts || {};
    message.value = `扫描完成：当前月事项 ${counts.source || 0}，历史候选 ${counts.candidates || 0}，推荐 ${counts.recommended || 0}。`;
    messageType.value = "success";
  } catch (error: any) {
    message.value = error?.message || "扫描失败";
    messageType.value = "failed";
  } finally {
    busy.value = false;
  }
}

function chooseCandidate(candidate: Dict): void {
  if (!activeSourceId.value) return;
  applyCandidateToSource(activeSourceId.value, candidate);
}

function applyCandidateToSource(sourceId: string, candidate: Dict): void {
  if (!sourceId || !candidate?.id) return;
  candidateMap[sourceId] = String(candidate.id || "");
  selectedMap[sourceId] = true;
  fieldEdits[sourceId] = { ...(candidate.fields || {}) };
  fieldOriginMap[sourceId] = "candidate";
}

function touchFields(): void {
  if (activeSourceId.value) {
    selectedMap[activeSourceId.value] = true;
  }
}

function buildSavePayloadForSources(sources: Dict[]): Dict[] {
  return buildHistoryMemorySavePayload({
    sources,
    selectedMap,
    candidateMap,
    fieldEdits,
    fieldOriginMap,
  });
}

function buildSavePayload(): Dict[] {
  return buildSavePayloadForSources(sourceItems.value);
}

function openSaveConfirm(mode: "selected" | "filled", explicitPayload: Dict[] | null = null): void {
  const payload = explicitPayload || buildSavePayload();
  if (!payload.length) return;
  pendingSavePayload.value = payload;
  saveConfirmMode.value = mode;
  saveConfirmOpen.value = true;
}

function saveSelected(): void {
  openSaveConfirm("selected");
}

function fillCandidatesAndConfirmSave(): void {
  if (!recommendedMatches.value.length) {
    message.value = "没有推荐候选。";
    messageType.value = "failed";
    return;
  }
  const filledSources: Dict[] = [];
  for (const item of recommendedMatches.value) {
    applyCandidateToSource(String(item.source.id || ""), item.candidate);
    filledSources.push(item.source);
  }
  activeSourceId.value = recommendedMatches.value[0]?.source?.id || activeSourceId.value;
  const payload = buildSavePayloadForSources(filledSources);
  message.value = `已填充 ${payload.length} 条推荐历史候选。`;
  messageType.value = "success";
  openSaveConfirm("filled", payload);
}

function cancelSaveConfirm(): void {
  if (busy.value) return;
  saveConfirmOpen.value = false;
  pendingSavePayload.value = [];
}

function resolveSaveConfirm(confirmed: boolean): void {
  if (!confirmed) {
    cancelSaveConfirm();
    return;
  }
  void confirmSaveSelected();
}

async function confirmSaveSelected(): Promise<void> {
  if (busy.value) return;
  const payload = pendingSavePayload.value;
  if (!payload.length) return;
  busy.value = true;
  message.value = "正在保存历史记忆...";
  messageType.value = "";
  try {
    const data = await api("/api/admin/notice-memory/history-save", {
      method: "POST",
      body: JSON.stringify({ matches: payload }),
    });
    message.value = `保存完成：成功 ${data.saved_count || 0}，覆盖 ${data.overwritten_count || 0}，跳过 ${data.skipped_count || 0}。`;
    messageType.value = "success";
    saveConfirmOpen.value = false;
    pendingSavePayload.value = [];
  } catch (error: any) {
    message.value = error?.message || "保存失败";
    messageType.value = "failed";
  } finally {
    busy.value = false;
  }
}

</script>

<style scoped>
.history-memory {
  display: grid;
  gap: 14px;
  padding: 18px;
  padding-bottom: 118px;
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

.page-head,
.scan-bar,
.filters,
.match-layout,
.panel {
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.page-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 16px;
}

.page-head strong {
  font-size: 20px;
}

.page-head p,
.panel-head span,
.source-row small,
.candidate-row small,
.match-note span {
  color: #64748b;
}

.head-actions,
.scan-bar,
.filters-fields {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
}

.scan-bar,
.filters {
  padding: 12px;
}

.filters {
  display: grid;
  gap: 10px;
}

.filters-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
}

.filters-head strong,
.footer-summary strong {
  display: block;
  color: #09204a;
  font-size: 14px;
  font-weight: 900;
}

.filters-head span,
.footer-summary span {
  display: block;
  margin-top: 2px;
  color: #64748b;
  font-size: 12px;
  font-weight: 780;
}

.filters-fields {
  min-width: 0;
}

.scan-fields,
.scan-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
}

.scan-fields {
  flex: 1 1 420px;
  min-width: 0;
}

.scan-actions {
  flex: 0 1 auto;
  justify-content: flex-end;
  margin-left: auto;
}

.scan-actions :deep(.disabled-reason) {
  margin: 0;
  max-width: 360px;
}

.scan-bar label,
.filters-fields label {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #334155;
}

.check {
  padding: 7px 9px;
  border: 1px solid #dbe3ee;
  border-radius: 6px;
  background: #f8fafc;
}

select,
input,
textarea {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
}

textarea {
  min-height: 74px;
  resize: vertical;
}

.message,
.notice-box,
.warning-list {
  padding: 12px;
  border-radius: 8px;
  background: #f8fafc;
  color: #334155;
}

.message.success {
  background: #f0fdf4;
  color: #15803d;
}

.message.failed,
.notice-box.danger {
  background: #fef2f2;
  color: #b91c1c;
}

.warning-list {
  display: grid;
  gap: 4px;
  background: #fff7ed;
  color: #c2410c;
}

.match-layout {
  min-height: 620px;
  display: grid;
  grid-template-columns: minmax(260px, 0.9fr) minmax(360px, 1.2fr) minmax(280px, 1fr);
  gap: 0;
  overflow: hidden;
}

.panel {
  min-width: 0;
  border: 0;
  border-right: 1px solid #e2e8f0;
  border-radius: 0;
  display: grid;
  grid-template-rows: auto 1fr;
}

.panel:last-child {
  border-right: 0;
}

.panel-head {
  padding: 12px;
  border-bottom: 1px solid #e2e8f0;
  display: flex;
  justify-content: space-between;
  gap: 10px;
}

.panel-head h2 {
  margin: 0;
  font-size: 16px;
}

.list {
  overflow: auto;
  padding: 8px;
  display: grid;
  align-content: start;
  gap: 8px;
}

.source-row,
.candidate-row {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  cursor: pointer;
}

.source-row.active,
.candidate-row.active {
  border-color: #2563eb;
  background: #eff6ff;
}

.source-row.selected {
  box-shadow: inset 3px 0 0 #16a34a;
}

.source-row.remembered {
  background: #f8fafc;
}

.source-row.unmatched {
  opacity: 0.78;
}

.source-row div,
.candidate-row div {
  min-width: 0;
  display: grid;
  gap: 3px;
}

.source-row strong,
.candidate-row strong {
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
}

.detail-panel {
  grid-template-rows: auto auto 1fr;
}

.empty {
  padding: 24px;
  color: #64748b;
}

.panel-empty {
  border: 1px dashed #cfe0ff;
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.84);
  padding: 16px;
  color: #64748b;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.55;
}

.match-note {
  display: grid;
  gap: 4px;
  padding: 12px;
  border-bottom: 1px solid #e2e8f0;
  background: #f8fafc;
}

.field-scroll {
  min-height: 0;
  overflow: auto;
}

.field-summary {
  position: sticky;
  top: 0;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 9px 12px;
  border-bottom: 1px solid #e2e8f0;
  background: #ffffff;
  color: #64748b;
  font-size: 12px;
}

.field-toggle {
  min-height: 28px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #f8fafc;
  color: #0f172a;
  cursor: pointer;
}

.form-grid {
  padding: 12px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.form-grid.compact {
  padding-top: 10px;
}

.form-grid label {
  display: grid;
  gap: 5px;
  color: #334155;
  font-size: 13px;
}

.form-grid label.wide {
  grid-column: span 2;
}

.form-grid.compact textarea {
  min-height: 52px;
  max-height: 120px;
}

.extra-field-section {
  border-top: 1px dashed #cbd5e1;
  background: #fbfdff;
}

.extra-field-section.open {
  background: #ffffff;
}

.extra-field-hint {
  padding: 8px 12px 0;
  color: #64748b;
  font-size: 12px;
}

.extra-grid {
  padding-top: 8px;
}

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 34px;
  padding: 7px 12px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #ffffff;
  color: #0f172a;
  text-decoration: none;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.btn.blue {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

.btn.green {
  border-color: #16a34a;
  background: #16a34a;
  color: #ffffff;
}

.btn.ghost {
  background: #f8fafc;
}

.compact-btn {
  min-height: 32px;
  padding: 5px 10px;
  font-size: 12px;
}

.history-save-footer {
  position: sticky;
  bottom: 18px;
  z-index: 8;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  width: min(980px, calc(100% - 28px));
  margin: 0 auto;
  border: 1px solid rgba(191, 219, 254, 0.9);
  border-radius: 20px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.94)),
    #ffffff;
  box-shadow: 0 20px 48px rgba(15, 73, 153, 0.16);
  padding: 12px 14px;
  backdrop-filter: blur(14px);
}

.footer-summary {
  min-width: 0;
}

.footer-summary span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.footer-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.footer-actions :deep(.disabled-reason) {
  flex: 1 1 100%;
  justify-content: flex-start;
  margin: 0;
}

@media (max-width: 1100px) {
  .match-layout {
    grid-template-columns: 1fr;
  }
  .panel {
    min-height: 360px;
    border-right: 0;
    border-bottom: 1px solid #e2e8f0;
  }
}

/* VNET history memory skin */
.history-memory {
  padding: 30px 36px;
  background:
    linear-gradient(180deg, #f4f9ff 0, #f8fbff 260px, #eef6ff 100%);
}

.page-head,
.scan-bar,
.filters,
.match-layout,
.notice-box,
.message,
.warning-list {
  border-color: #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 16px 38px rgba(22, 78, 151, 0.1);
}

.page-head {
  padding: 22px 24px;
}

.page-head strong {
  color: #071634;
  font-size: 24px;
  font-weight: 900;
}

.scan-bar,
.filters {
  padding: 14px;
}

.match-layout {
  border-radius: 16px;
  grid-template-columns: minmax(260px, 0.85fr) minmax(520px, 1.55fr) minmax(240px, 0.85fr);
}

.panel-head {
  background: linear-gradient(180deg, #ffffff, #f8fbff);
}

.panel-head h2 {
  color: #09204a;
  font-weight: 900;
}

.panel-head h2::before {
  content: "";
  display: inline-block;
  width: 4px;
  height: 17px;
  margin-right: 8px;
  border-radius: 999px;
  vertical-align: -3px;
  background: linear-gradient(180deg, #0757d7, #21c6e7);
}

.source-row,
.candidate-row {
  border-color: #d8e7f8;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 8px 18px rgba(22, 78, 151, 0.06);
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.source-row:hover,
.candidate-row:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 12px 26px rgba(22, 78, 151, 0.1);
}

.source-row.active,
.candidate-row.active {
  border-color: #1678ff;
  background: #edf6ff;
  box-shadow: inset 4px 0 0 #1678ff, 0 12px 26px rgba(22, 120, 255, 0.12);
}

.source-row.selected {
  box-shadow: inset 4px 0 0 #22b66b, 0 8px 18px rgba(22, 78, 151, 0.06);
}

.match-note,
.field-summary,
.extra-field-section,
.check {
  border-color: #d8e7f8;
  background: #f7fbff;
}

.field-toggle,
.btn {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
}

.btn:hover:not(:disabled),
.field-toggle:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.12);
  transform: translateY(-1px);
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.24);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
}

select,
input,
textarea {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
}

select:focus,
input:focus,
textarea:focus {
  border-color: #1678ff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

.field-scroll {
  max-height: calc(100vh - 330px);
}

.form-grid.compact {
  grid-template-columns: repeat(2, minmax(0, 1fr));
  align-content: start;
}

.form-grid.compact label.wide {
  grid-column: auto;
}

.form-grid.compact textarea {
  min-height: 44px;
  max-height: 76px;
  overflow: auto;
  line-height: 1.45;
}

/* Softer rounded and text polish */
.page-head,
.scan-bar,
.filters,
.match-layout,
.notice-box,
.message,
.warning-list {
  border-radius: 20px;
}

.panel,
.source-row,
.candidate-row,
.match-note,
.field-summary,
.extra-field-section,
.check {
  border-radius: 18px;
}

.field-toggle,
.btn,
select,
input,
textarea {
  border-radius: 12px;
}

.page-head strong,
.panel-head h2,
.source-row strong,
.candidate-row strong {
  font-weight: 820;
  letter-spacing: 0;
}

.source-row span,
.candidate-row span,
.source-row small,
.candidate-row small,
.page-head p {
  color: #5f7189;
}

@media (max-width: 1180px) {
  .match-layout {
    grid-template-columns: 1fr;
  }

  .panel {
    border-right: 0;
    border-bottom: 1px solid #e2e8f0;
  }

  .panel:last-child {
    border-bottom: 0;
  }

  .field-scroll {
    max-height: none;
  }

.form-grid.compact label.wide {
  grid-column: span 2;
}
}

/* Panorama construction-management polish */
.history-memory {
  padding: 28px 34px 42px;
  background:
    linear-gradient(180deg, #f7faff 0, #f9fbfd 280px, #eef5fc 100%),
    radial-gradient(circle at 10% 12%, rgba(48, 128, 255, 0.1), transparent 30%);
}

.page-head,
.scan-bar,
.filters,
.match-layout,
.notice-box,
.message,
.warning-list {
  border-color: rgba(207, 224, 255, 0.94);
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 14px 34px rgba(20, 70, 138, 0.08);
}

.panel,
.source-row,
.candidate-row,
.match-note,
.field-summary,
.extra-field-section,
.check {
  border-color: rgba(216, 231, 248, 0.95);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.96);
}

.source-row.active,
.candidate-row.active {
  border-color: #3080ff;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #3080ff, 0 12px 28px rgba(21, 93, 252, 0.12);
}

.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 12px 24px rgba(21, 93, 252, 0.22);
}

.btn,
.field-toggle,
select,
input,
textarea {
  border-radius: 14px;
}

/* Panorama construction-management history-memory skin */
.history-memory {
  background: linear-gradient(180deg, #eef4ff 0, #f8fbff 44%, #eef5ff 100%);
}

.scan-bar {
  justify-content: space-between;
  border-radius: 20px;
  padding: 12px 14px;
}

.scan-fields {
  border-right: 1px solid rgba(216, 229, 247, 0.9);
  padding-right: 14px;
}

.scan-actions .btn {
  min-height: 40px;
  border-radius: 15px;
}

@media (max-width: 980px) {
  .scan-fields {
    flex-basis: 100%;
    border-right: 0;
    border-bottom: 1px solid rgba(216, 229, 247, 0.9);
    padding-right: 0;
    padding-bottom: 10px;
  }

  .scan-actions {
    width: 100%;
    justify-content: flex-start;
    margin-left: 0;
  }

  .history-save-footer {
    align-items: stretch;
    flex-direction: column;
    width: calc(100% - 24px);
    bottom: 12px;
  }

  .footer-actions {
    justify-content: stretch;
  }

  .footer-actions .btn {
    flex: 1 1 180px;
  }
}

.page-head,
.scan-bar,
.filters,
.match-layout,
.notice-box,
.message,
.warning-list,
.panel,
.source-row,
.candidate-row,
.match-note,
.field-summary,
.extra-field-section,
.check {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.source-row.active,
.candidate-row.active {
  border-color: #005bff;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #005bff, 0 12px 24px rgba(0, 91, 255, 0.12);
}

.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}

.btn.green {
  background: #059669;
}

select,
input,
textarea {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

select:focus,
input:focus,
textarea:focus {
  border-color: #005bff;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.history-memory {
  padding-bottom: 136px;
}

@media (max-width: 980px) {
  .history-memory {
    padding-bottom: 190px;
  }
}
</style>
