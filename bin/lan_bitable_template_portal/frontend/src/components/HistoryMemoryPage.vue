<template>
  <section class="history-memory">
    <header class="page-head">
      <div>
        <strong>历史通告记忆导入</strong>
        <p>扫描近 3 个月目标多维表，匹配当前月源表事项；确认后只写入本地记忆。</p>
      </div>
      <div class="head-actions">
        <button class="btn ghost" @click="goHome">返回工作台</button>
        <a class="btn ghost" href="/">功能选择</a>
      </div>
    </header>

    <div v-if="checking" class="notice-box">正在校验管理员身份...</div>
    <div v-else-if="!loggedIn" class="notice-box">
      请先登录飞书后再使用历史记忆导入。
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </div>
    <div v-else-if="!isAdmin" class="notice-box danger">仅管理员可使用历史通告记忆导入。</div>

    <template v-else>
      <section class="scan-bar">
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
        <button class="btn blue" :disabled="busy || selectedWorkTypes.length === 0" @click="scanHistory">
          {{ busy ? "扫描中" : "扫描历史通告" }}
        </button>
        <button class="btn green" :disabled="busy || recommendedMatchCount === 0" @click="fillCandidatesAndConfirmSave">
          一键填充推荐并保存 {{ recommendedMatchCount }}
        </button>
        <button class="btn ghost" :disabled="busy || selectedCount === 0" @click="saveSelected">
          保存已勾选 {{ selectedCount }}
        </button>
      </section>

      <section v-if="message" class="message" :class="{ failed: messageType === 'failed', success: messageType === 'success' }">
        {{ message }}
      </section>
      <section v-if="warnings.length" class="warning-list">
        <span v-for="warning in warnings" :key="warning">{{ warning }}</span>
      </section>

      <section class="filters">
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
      </section>

      <section class="summary-grid">
        <article><span>当前源表事项</span><strong>{{ sourceItems.length }}</strong></article>
        <article><span>历史候选</span><strong>{{ candidates.length }}</strong></article>
        <article><span>已勾选</span><strong>{{ selectedCount }}</strong></article>
        <article><span>可一键填充</span><strong>{{ recommendedMatchCount }}</strong></article>
        <article><span>可保存覆盖</span><strong>{{ overwriteHint }}</strong></article>
      </section>

      <section class="match-layout">
        <aside class="panel source-panel">
          <div class="panel-head">
            <h2>当前月源表事项</h2>
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
                <div class="extra-field-hint">
                  检修、智航等扩展字段默认收起，展开后仍可完整编辑。
                </div>
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
          </div>
        </aside>
      </section>

      <div v-if="saveConfirmOpen" class="modal-backdrop" @click.self="cancelSaveConfirm">
        <section class="confirm-modal" role="dialog" aria-modal="true" aria-labelledby="history-save-title">
          <header>
            <div>
              <strong id="history-save-title">{{ saveConfirmTitle }}</strong>
              <p>{{ saveConfirmSubtitle }}</p>
            </div>
            <button class="icon-btn" type="button" aria-label="关闭" @click="cancelSaveConfirm">×</button>
          </header>

          <div class="confirm-metrics">
            <article><span>保存数量</span><strong>{{ saveSummary.total }}</strong></article>
            <article><span>历史候选填充</span><strong>{{ saveSummary.candidate }}</strong></article>
            <article><span>会覆盖旧记忆</span><strong>{{ saveSummary.overwrite }}</strong></article>
          </div>

          <div class="confirm-detail">
            <strong>即将保存的事项</strong>
            <ul>
              <li v-for="item in savePreviewItems" :key="item.key">
                <span>{{ item.type }} · {{ item.building || "-" }}</span>
                <strong>{{ item.title }}</strong>
                <small>{{ item.origin }}</small>
              </li>
            </ul>
            <p v-if="saveSummary.total > savePreviewItems.length">
              还有 {{ saveSummary.total - savePreviewItems.length }} 条未展示，确认后会一起保存。
            </p>
          </div>

          <footer>
            <button class="btn ghost" type="button" :disabled="busy" @click="cancelSaveConfirm">取消</button>
            <button class="btn green" type="button" :disabled="busy || pendingSavePayload.length === 0" @click="confirmSaveSelected">
              {{ busy ? "保存中" : "确认批量保存" }}
            </button>
          </footer>
        </section>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";

type Dict = Record<string, any>;

defineProps<{
  checking: boolean;
  loggedIn: boolean;
  isAdmin: boolean;
  loginUrl: string;
}>();

const workTypes = [
  { value: "maintenance", label: "维保" },
  { value: "change", label: "变更" },
  { value: "repair", label: "检修" },
];

const editableFieldDefs = [
  { key: "location", label: "位置/地点" },
  { key: "content", label: "内容", multi: true },
  { key: "reason", label: "原因/故障原因", multi: true },
  { key: "impact", label: "影响/影响范围", multi: true },
  { key: "progress", label: "进度/完成情况", multi: true },
  { key: "specialty", label: "专业" },
  { key: "level", label: "等级/紧急程度" },
  { key: "repair_device", label: "维修设备" },
  { key: "repair_fault", label: "维修故障" },
  { key: "fault_type", label: "故障类型" },
  { key: "repair_mode", label: "维修方式" },
  { key: "discovery", label: "故障发现方式" },
  { key: "symptom", label: "故障现象", multi: true },
  { key: "solution", label: "解决方案", multi: true },
  { key: "zhihang_title", label: "智航关联标题" },
  { key: "zhihang_record_id", label: "智航记录 ID" },
  { key: "zhihang_progress", label: "智航进展" },
] as Array<{ key: string; label: string; multi?: boolean }>;

const busy = ref(false);
const months = ref(3);
const selectedWorkTypes = ref(["maintenance", "change", "repair"]);
const sourceItems = ref<Dict[]>([]);
const candidates = ref<Dict[]>([]);
const matches = ref<Dict[]>([]);
const warnings = ref<string[]>([]);
const message = ref("点击扫描历史通告开始匹配。");
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
    return `右侧有推荐候选：${activeCandidate.value.title || "未命名候选"}；点击候选后会覆盖当前字段。`;
  }
  return activeMatch.value?.reason || "可在右侧选择历史候选。";
});
const primaryFieldKeys = new Set(["location", "content", "reason", "impact", "progress", "specialty", "level"]);
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
    return "系统会把所有已匹配历史候选的字段填入当前月源表事项，并批量写入本地记忆。";
  }
  return "系统会保存当前已勾选事项的字段内容，同键旧记忆会被覆盖。";
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

watch(filteredSources, (items) => {
  if (!activeSourceId.value || !items.some((item) => item.id === activeSourceId.value)) {
    activeSourceId.value = items[0]?.id || "";
  }
});

watch(activeSourceId, () => {
  showExtraFields.value = false;
});

async function api(path: string, options: RequestInit = {}): Promise<Dict> {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload.data || payload;
}

function workTypeLabel(value: string): string {
  return workTypes.find((item) => item.value === value)?.label || "维保";
}

function candidateIdFor(sourceId: string): string {
  return candidateMap[sourceId] || "";
}

function isSelected(sourceId: string): boolean {
  return Boolean(selectedMap[sourceId]);
}

function hasMeaningfulFields(fields: Dict | undefined): boolean {
  const payload = fields || {};
  return Object.entries(payload).some(([key, value]) => {
    if (["updated_at", "history_imported_at", "imported_by", "imported_from"].includes(key)) return false;
    return String(value ?? "").trim() !== "";
  });
}

function sourceHasMemory(source: Dict | undefined): boolean {
  return hasMeaningfulFields(source?.memory);
}

function sourceInitialFields(source: Dict): Dict {
  if (sourceHasMemory(source)) return { ...(source.memory || {}) };
  return { ...(source.current_fields || {}) };
}

function sourceInitialOrigin(source: Dict): string {
  return sourceHasMemory(source) ? "memory" : "current";
}

function sourceFieldOriginLabel(sourceId: string): string {
  const origin = fieldOriginMap[sourceId] || "";
  if (origin === "candidate") return "当前显示：历史候选";
  if (origin === "memory") return "当前显示：已有记忆";
  if (origin === "current") return "当前显示：当前源表";
  return "当前显示：未初始化";
}

function selectSource(item: Dict): void {
  activeSourceId.value = item.id;
  if (!fieldEdits[item.id]) {
    fieldEdits[item.id] = sourceInitialFields(item);
    fieldOriginMap[item.id] = sourceInitialOrigin(item);
  }
}

function fieldVisible(key: string): boolean {
  const type = activeSource.value?.work_type || "maintenance";
  if (key.startsWith("repair_") || ["fault_type", "repair_mode", "discovery", "symptom", "solution"].includes(key)) {
    return type === "repair";
  }
  if (key.startsWith("zhihang_")) return type === "change";
  if (key === "level") return type !== "maintenance";
  return true;
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

async function scanHistory(): Promise<void> {
  busy.value = true;
  message.value = "正在扫描历史多维记录...";
  messageType.value = "";
  try {
    const data = await api("/api/admin/notice-memory/history-scan", {
      method: "POST",
      body: JSON.stringify({ months: months.value, work_types: selectedWorkTypes.value }),
    });
    applyScanPayload(data);
    const counts = data.counts || {};
    message.value = `扫描完成：源表事项 ${counts.source || 0}，历史候选 ${counts.candidates || 0}，推荐 ${counts.recommended || 0}。`;
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
  return sources
    .filter((source) => isSelected(source.id))
    .map((source) => ({
      selected: true,
      source_item: source,
      candidate_id: candidateMap[source.id],
      fields: fieldEdits[source.id] || {},
      field_origin: fieldOriginMap[source.id] || sourceInitialOrigin(source),
    }));
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
    message.value = "当前没有可一键填充的推荐历史候选。";
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
  message.value = `已填充 ${payload.length} 条推荐历史候选，确认后会批量保存。`;
  messageType.value = "success";
  openSaveConfirm("filled", payload);
}

function cancelSaveConfirm(): void {
  if (busy.value) return;
  saveConfirmOpen.value = false;
  pendingSavePayload.value = [];
}

async function confirmSaveSelected(): Promise<void> {
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

function goHome(): void {
  window.location.href = "/";
}
</script>

<style scoped>
.history-memory {
  display: grid;
  gap: 14px;
  padding: 18px;
}

.page-head,
.scan-bar,
.filters,
.summary-grid,
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
.filters {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
}

.scan-bar,
.filters {
  padding: 12px;
}

.scan-bar label,
.filters label {
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

.summary-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.summary-grid article {
  padding: 12px;
  border-right: 1px solid #e2e8f0;
}

.summary-grid article:last-child {
  border-right: 0;
}

.summary-grid span {
  display: block;
  color: #64748b;
  font-size: 12px;
}

.summary-grid strong {
  font-size: 22px;
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

.icon-btn {
  width: 34px;
  height: 34px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #ffffff;
  color: #334155;
  cursor: pointer;
  font-size: 20px;
  line-height: 1;
}

.modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 50;
  display: grid;
  place-items: center;
  padding: 18px;
  background: rgba(15, 23, 42, 0.32);
}

.confirm-modal {
  width: min(720px, 100%);
  max-height: calc(100vh - 36px);
  overflow: auto;
  border: 1px solid #dbe3ee;
  border-radius: 10px;
  background: #ffffff;
  box-shadow: 0 24px 70px rgba(15, 23, 42, 0.18);
}

.confirm-modal header,
.confirm-modal footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px;
}

.confirm-modal header {
  border-bottom: 1px solid #e2e8f0;
}

.confirm-modal header strong {
  font-size: 18px;
}

.confirm-modal header p,
.confirm-detail p,
.confirm-detail small {
  color: #64748b;
}

.confirm-modal footer {
  border-top: 1px solid #e2e8f0;
  justify-content: flex-end;
}

.confirm-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  border-bottom: 1px solid #e2e8f0;
}

.confirm-metrics article {
  padding: 12px 16px;
  border-right: 1px solid #e2e8f0;
}

.confirm-metrics article:last-child {
  border-right: 0;
}

.confirm-metrics span {
  display: block;
  color: #64748b;
  font-size: 12px;
}

.confirm-metrics strong {
  display: block;
  margin-top: 4px;
  font-size: 24px;
}

.confirm-detail {
  display: grid;
  gap: 10px;
  padding: 14px 16px;
}

.confirm-detail ul {
  display: grid;
  gap: 8px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.confirm-detail li {
  display: grid;
  gap: 3px;
  padding: 10px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: #f8fafc;
}

.confirm-detail li span {
  color: #64748b;
  font-size: 12px;
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
  .summary-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .confirm-metrics {
    grid-template-columns: 1fr;
  }
  .confirm-metrics article {
    border-right: 0;
    border-bottom: 1px solid #e2e8f0;
  }
  .confirm-metrics article:last-child {
    border-bottom: 0;
  }
}
</style>
