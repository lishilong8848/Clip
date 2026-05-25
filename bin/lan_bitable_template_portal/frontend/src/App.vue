<template>
  <main class="app-shell">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark">21V</div>
        <div>
          <h1>南通基地-运维灯塔工作台</h1>
          <p>{{ scopeLabel(currentScope) }} · {{ syncText }}</p>
        </div>
      </div>
      <div class="topbar-actions">
        <span v-if="auth.loggedIn" class="user-chip">{{ auth.user?.name || auth.user?.open_id || "已登录" }}</span>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="loading" @click="loadWorkbench()">
          刷新本页
        </button>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="repairRefreshing" @click="refreshRepair">
          {{ repairRefreshing ? "检修刷新中" : "刷新检修" }}
        </button>
        <button v-if="isAdmin" class="btn ghost" @click="showAdminTools = true">管理/诊断</button>
        <a class="btn ghost" href="/legacy-index.html">兼容页面</a>
        <button v-if="auth.loggedIn" class="btn danger-text" @click="logout">退出</button>
      </div>
    </header>

    <AdminTools
      :open="showAdminTools"
      :scope-options="requestableScopes"
      @close="showAdminTools = false"
    />

    <AuthPanels
      v-if="authChecking || !auth.loggedIn || (auth.loggedIn && !auth.scopeOptions.length)"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :user="auth.user"
      :login-url="auth.loginUrl"
      :busy="permissionBusy"
      :request="permissionRequest"
      :requestable-scopes="requestableScopes"
      @update-request="updatePermissionRequest"
      @submit="submitPermissionRequest"
      @confirm="confirmPermissionRequest"
    />

    <ScopeHome
      v-else-if="!isWorkbench"
      :scope-options="visibleScopeOptions"
      :overview="scopeOverview"
      :handover-links="handoverLinks"
      @enter="enterScope"
    />

    <section v-else class="workbench">
      <div class="summary-strip">
        <article>
          <span>已发起</span>
          <strong>{{ dailyStats.started || 0 }}</strong>
        </article>
        <article>
          <span>有更新</span>
          <strong>{{ dailyStats.updated || 0 }}</strong>
        </article>
        <article>
          <span>已结束</span>
          <strong>{{ dailyStats.ended || 0 }}</strong>
        </article>
        <article>
          <span>进行中</span>
          <strong>{{ ongoing.length }}</strong>
        </article>
      </div>

      <div class="toolbar">
        <div class="segmented">
          <button
            v-for="type in workTypes"
            :key="type.value"
            :class="{ active: workType === type.value }"
            @click="selectWorkType(type.value)"
          >
            {{ type.label }} {{ recordTypeCounts[type.value] || 0 }}
          </button>
        </div>
        <input v-model="searchText" class="search" placeholder="搜索标题、楼栋、专业" />
        <button class="btn ghost" @click="addManualDraft">纯手填</button>
        <button class="btn ghost" @click="showPasteParser = !showPasteParser">解析粘贴通告</button>
      </div>

      <section v-if="showPasteParser" class="paste-panel">
        <textarea v-model="pasteText" placeholder="粘贴完整维保、变更或检修通告文本"></textarea>
        <button class="btn blue" @click="parsePastedNotice">解析到待发起通告</button>
      </section>

      <section class="workspace">
        <aside class="panel records-panel">
          <div class="panel-head">
            <h2>待发起事项</h2>
            <span>{{ filteredRows.length }}</span>
          </div>
          <VirtualNoticeList
            :rows="filteredRows"
            :selected-id="activeDraftKey"
            empty-text="当前筛选下没有待发起事项"
            @select="toggleRecordSelection"
          />
        </aside>

        <section class="panel drafts-panel">
          <div class="panel-head">
            <h2>待发起通告</h2>
            <span>{{ selectedDraftRows.length }}</span>
          </div>
          <div v-if="selectedDraftRows.length === 0" class="empty-block">
            从左侧选择事项，或使用纯手填、解析粘贴通告。
          </div>
          <div v-else class="draft-stack">
            <article
              v-for="row in selectedDraftRows"
              :key="row.key"
              class="draft-card"
              :class="{ active: row.key === activeDraftKey }"
              @click="activeDraftKey = row.key"
            >
              <div class="card-title">
                <strong>{{ row.title }}</strong>
                <span>{{ workTypeLabel(row.record.work_type) }}</span>
              </div>
              <div class="form-grid">
                <label>
                  标题
                  <input v-model="row.draft.title" placeholder="通告标题" @input="saveDrafts" />
                </label>
                <label>
                  专业
                  <input v-model="row.draft.specialty" placeholder="专业" @input="saveDrafts" />
                </label>
                <label v-if="row.record.work_type === 'maintenance'">
                  维保周期
                  <select v-model="row.draft.maintenance_cycle" @change="saveDrafts">
                    <option value="">请选择</option>
                    <option v-for="item in maintenanceCycleOptions" :key="item" :value="item">{{ item }}</option>
                  </select>
                </label>
                <label v-if="row.record.work_type !== 'maintenance'">
                  等级
                  <input v-model="row.draft.level" placeholder="等级" @input="saveDrafts" />
                </label>
                <label>
                  开始 / 期望完成时间
                  <input v-model="row.draft.start_time" type="datetime-local" @input="saveDrafts" />
                </label>
                <label>
                  结束 / 故障发生时间
                  <input v-model="row.draft.end_time" type="datetime-local" @input="saveDrafts" />
                </label>
                <label class="span-2">
                  地点
                  <input v-model="row.draft.location" placeholder="地点" @input="saveDrafts" />
                </label>
                <label class="span-2">
                  内容 / 标题
                  <textarea v-model="row.draft.content" placeholder="内容" @input="saveDrafts"></textarea>
                </label>
                <label>
                  原因
                  <textarea v-model="row.draft.reason" placeholder="原因" @input="saveDrafts"></textarea>
                </label>
                <label>
                  影响
                  <textarea v-model="row.draft.impact" placeholder="影响" @input="saveDrafts"></textarea>
                </label>
                <label class="span-2">
                  进度 / 完成情况
                  <textarea v-model="row.draft.progress" placeholder="进度" @input="saveDrafts"></textarea>
                </label>
              </div>
              <details v-if="row.record.work_type === 'repair'" class="repair-fields">
                <summary>检修字段</summary>
                <div class="form-grid">
                  <label><span>维修设备</span><input v-model="row.draft.repair_device" @input="saveDrafts" /></label>
                  <label><span>维修故障</span><input v-model="row.draft.repair_fault" @input="saveDrafts" /></label>
                  <label><span>故障类型</span><input v-model="row.draft.fault_type" @input="saveDrafts" /></label>
                  <label><span>维修方式</span><input v-model="row.draft.repair_mode" @input="saveDrafts" /></label>
                  <label><span>故障发现方式</span><input v-model="row.draft.discovery" @input="saveDrafts" /></label>
                  <label><span>故障现象</span><input v-model="row.draft.symptom" @input="saveDrafts" /></label>
                  <label class="span-2"><span>解决方案</span><textarea v-model="row.draft.solution" @input="saveDrafts"></textarea></label>
                </div>
              </details>
              <div v-if="row.record.work_type === 'change'" class="zhihang-line">
                <label>
                  <input v-model="row.draft.zhihang_involved" type="checkbox" @change="saveDrafts" />
                  涉及智航
                </label>
                <select v-if="row.draft.zhihang_involved" v-model="row.draft.zhihang_record_id" @change="bindZhihang(row.draft)">
                  <option value="">选择智航变更</option>
                  <option v-for="item in zhihangRecords" :key="item.record_id" :value="item.record_id">
                    {{ item.title || item.record_id }}
                  </option>
                </select>
              </div>
              <div class="card-actions">
                <span class="job-line" :class="jobClass(row.key)">{{ jobText(row.key) }}</span>
                <button class="btn blue" :disabled="isLineBusy(row.key)" @click.stop="sendStart(row.key)">
                  发送{{ sourceActionLabel(row.record) }}
                </button>
                <button class="btn ghost" :disabled="isLineBusy(row.key)" @click.stop="removeDraft(row.key)">移除</button>
              </div>
            </article>
          </div>
        </section>

        <aside class="panel ongoing-panel">
          <div class="panel-head">
            <h2>已开始未结束</h2>
            <span>{{ ongoing.length }}</span>
          </div>
          <div v-if="ongoing.length === 0" class="empty-block">当前没有进行中通告</div>
          <div v-else class="ongoing-list">
            <article v-for="item in ongoing" :key="item.active_item_id || item.record_id" class="ongoing-card">
              <div class="card-title">
                <strong>{{ ongoingTitle(item) }}</strong>
                <span>{{ workTypeLabel(item.work_type) }}</span>
              </div>
              <p>{{ ongoingMeta(item) }}</p>
              <div class="form-grid">
                <label class="span-2">
                  进度 / 完成情况
                  <textarea
                    :value="ongoingDraft(item).progress"
                    @input="setOngoingEdit(item, 'progress', ($event.target as HTMLTextAreaElement).value)"
                  ></textarea>
                </label>
              </div>
              <div class="card-actions">
                <span class="job-line" :class="jobClass(item.active_item_id || item.record_id)">
                  {{ jobText(item.active_item_id || item.record_id) }}
                </span>
                <button class="btn blue" :disabled="isLineBusy(item.active_item_id || item.record_id)" @click="sendOngoing(item, 'update')">更新</button>
                <button class="btn green" :disabled="isLineBusy(item.active_item_id || item.record_id)" @click="sendOngoing(item, 'end')">结束</button>
                <button class="btn danger" :disabled="isLineBusy(item.active_item_id || item.record_id)" @click="deleteOngoing(item)">删除</button>
              </div>
            </article>
          </div>
        </aside>
      </section>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import AdminTools from "./components/AdminTools.vue";
import AuthPanels from "./components/AuthPanels.vue";
import ScopeHome from "./components/ScopeHome.vue";
import VirtualNoticeList, { type NoticeRow } from "./components/VirtualNoticeList.vue";

type Dict = Record<string, any>;
type ScopeOption = { value: string; label: string };

const workTypes = [
  { value: "maintenance", label: "维保" },
  { value: "change", label: "变更" },
  { value: "repair", label: "检修" },
];
const maintenanceCycleOptions = ["每月", "每季", "每年", "半年", "每两年", "每三年", "每五年", "冬季保温每日", "/"];
const requestableScopes: ScopeOption[] = [
  { value: "110", label: "110站" },
  { value: "A", label: "A楼" },
  { value: "B", label: "B楼" },
  { value: "C", label: "C楼" },
  { value: "D", label: "D楼" },
  { value: "E", label: "E楼" },
  { value: "H", label: "H楼" },
  { value: "CAMPUS", label: "园区" },
];

const authChecking = ref(true);
const loading = ref(false);
const repairRefreshing = ref(false);
const isWorkbench = ref(false);
const currentScope = ref(new URLSearchParams(window.location.search).get("scope") || "");
const syncText = ref("准备中");
const workType = ref("maintenance");
const userSelectedWorkType = ref(false);
const searchText = ref("");
const activeDraftKey = ref("");
const showPasteParser = ref(false);
const showAdminTools = ref(false);
const pasteText = ref("");
const eventSource = ref<EventSource | null>(null);

const auth = reactive({
  loggedIn: false,
  user: {} as Dict,
  scopeOptions: [] as ScopeOption[],
  loginUrl: "/api/auth/login",
});
const permissionRequest = reactive({
  scopes: [] as string[],
  reason: "",
  code: "",
  requestId: "",
  message: "",
});
const permissionBusy = ref(false);

const records = ref<Dict[]>([]);
const ongoing = ref<Dict[]>([]);
const zhihangRecords = ref<Dict[]>([]);
const dailySummary = ref<Dict>({ date: "", items: [], stats: {} });
const scopeOverview = ref<Record<string, Dict>>({});
const handoverLinks = ref<Record<string, string>>({});
const selectedKeys = reactive(new Set<string>());
const drafts = reactive(new Map<string, Dict>());
const ongoingEdits = reactive(new Map<string, Dict>());
const jobStates = reactive(new Map<string, Dict>());
const defaults = reactive({ impact: "无", progress: "" });

const visibleScopeOptions = computed(() => auth.scopeOptions.length ? auth.scopeOptions : requestableScopes);
const isAdmin = computed(() => String(auth.user?.role || "").toLowerCase() === "admin");
const dailyStats = computed(() => dailySummary.value?.stats || {});
const recordTypeCounts = computed(() => {
  const counts: Record<string, number> = { maintenance: 0, change: 0, repair: 0 };
  for (const record of records.value) {
    const type = record.work_type || "maintenance";
    if (Object.prototype.hasOwnProperty.call(counts, type)) counts[type] += 1;
  }
  return counts;
});
const filteredRecords = computed(() => {
  const query = searchText.value.trim().toLowerCase();
  return records.value.filter((record) => {
    if ((record.work_type || "maintenance") !== workType.value) return false;
    if (!query) return true;
    return [recordCardTitle(record), buildingForRecord(record), specialtyForRecord(record), sourceProgressForRecord(record)]
      .join(" ")
      .toLowerCase()
      .includes(query);
  });
});
const filteredRows = computed<NoticeRow[]>(() => filteredRecords.value.map((record) => ({
  id: recordKey(record),
  title: recordCardTitle(record),
  type: workTypeLabel(record.work_type),
  meta: [buildingForRecord(record), specialtyForRecord(record), sourceProgressForRecord(record)].filter(Boolean).join(" · "),
  status: isRecordOngoing(record) ? "右侧处理" : sourceActionLabel(record),
  raw: record,
})));
const selectedDraftRows = computed(() => Array.from(selectedKeys).map((key) => {
  const record = draftRecordForKey(key);
  if (!record) return null;
  return {
    key,
    record,
    draft: getDraft(record),
    title: recordCardTitle(record),
  };
}).filter(Boolean) as Array<{ key: string; record: Dict; draft: Dict; title: string }>);

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function scopeLabel(value: string): string {
  const normalized = normalizeScopeValue(value, "ALL");
  const found = [...visibleScopeOptions.value, { value: "ALL", label: "全部" }].find((item) => normalizeScopeValue(item.value, "") === normalized);
  return found?.label || normalized;
}

function workTypeLabel(value: string): string {
  return workTypes.find((item) => item.value === value)?.label || "维保";
}

function fieldsOf(record: Dict | undefined): Dict {
  return record?.display_fields || {};
}

function todayInput(hour: number, minute: number): string {
  const d = new Date();
  d.setHours(hour, minute, 0, 0);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function toDatetimeLocal(value: string): string {
  const text = String(value || "").trim();
  const m = text.match(/(\d{4})[/-](\d{1,2})[/-](\d{1,2})\D+(\d{1,2})[:点](\d{1,2})?/);
  if (!m) return text.includes("T") ? text.slice(0, 16) : "";
  return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}T${m[4].padStart(2, "0")}:${(m[5] || "00").padStart(2, "0")}`;
}

function firstRepairField(record: Dict, names: string[]): string {
  const fields = fieldsOf(record);
  for (const name of names) {
    const value = String(fields[name] || "").trim();
    if (value && !["-", "--", "—", "——", "/", "无", "暂无"].includes(value)) return value;
  }
  return "";
}

function repairLevelFromEventLevel(value: string): string {
  const text = String(value || "").trim().toUpperCase();
  if (/(^|[^A-Z0-9])I3([^A-Z0-9]|$)/.test(text) || text === "低") return "低";
  if (/(^|[^A-Z0-9])I2([^A-Z0-9]|$)/.test(text) || text === "中") return "中";
  return "";
}

function repairDeviceText(record: Dict): string {
  const f = fieldsOf(record);
  const no = String(f["设备编号"] || "").trim();
  const name = String(f["设备名称"] || "").trim();
  return no && name ? `${no}${name}` : no || name || firstRepairField(record, ["维修设备", "资产名称", "设备"]);
}

function titleForRecord(record: Dict): string {
  if (record.manual) return fieldsOf(record)["手动标题"] || record.title || `手动${workTypeLabel(record.work_type)}通告`;
  const f = fieldsOf(record);
  const type = record.work_type || "maintenance";
  if (type === "change") return f["变更简述"] || record.title || record.record_id;
  if (type === "repair") return record.title || f["检修通告名称"] || f["维修名称"] || record.record_id;
  return `EA118机房${f["楼栋"] || ""}${f["维护总项"] || ""}`;
}

function recordCardTitle(record: Dict): string {
  const title = titleForRecord(record);
  if ((record.work_type || "maintenance") !== "maintenance") return title;
  const period = String(fieldsOf(record)["维护周期"] || record.maintenance_cycle || "").trim();
  return period ? `${title}-${period}` : title;
}

function specialtyForRecord(record: Dict): string {
  const f = fieldsOf(record);
  if (record.manual) return f["专业类别"] || f["专业"] || f["所属专业"] || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return f["专业"] || "";
  if (type === "repair") return f["所属专业"] || f["专业（推送消息用）"] || "";
  return f["专业类别"] || "";
}

function buildingForRecord(record: Dict): string {
  const f = fieldsOf(record);
  if (record.manual) return f["楼栋"] || f["变更楼栋"] || f["所属数据中心/楼栋-使用"] || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return f["变更楼栋"] || "";
  if (type === "repair") return f["所属数据中心/楼栋-使用"] || f["所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"] || "";
  return f["楼栋"] || "";
}

function levelForRecord(record: Dict): string {
  if ((record.work_type || "maintenance") === "repair") return repairLevelFromEventLevel(firstRepairField(record, ["对应事件等级"]));
  return fieldsOf(record)["变更等级（阿里）"] || "";
}

function sourceProgressForRecord(record: Dict): string {
  if (record.manual) return "未开始";
  const type = record.work_type || "maintenance";
  if (record.source_progress || record.source_status) return record.source_progress || record.source_status;
  if (type === "change") return fieldsOf(record)["变更进度"] || "";
  if (type === "repair") return firstRepairField(record, ["维修开始时间"]) ? "进行中" : "未开始";
  return fieldsOf(record)["维护实施状态"] || "";
}

function sourceActionForRecord(record: Dict): string {
  return sourceProgressForRecord(record) === "未开始" ? "start" : "update";
}

function sourceActionLabel(record: Dict): string {
  return sourceActionForRecord(record) === "start" ? "开始" : "更新";
}

function targetRecordIdForRecord(record: Dict): string {
  const summary = record?.work_summary || {};
  return String(summary.target_record_id || summary.feishu_record_id || summary.record_id || record?.target_record_id || "").trim();
}

function isRecordOngoing(record: Dict): boolean {
  const title = titleForRecord(record).replace(/\s+/g, "");
  const sourceId = record.source_record_id || record.record_id;
  return ongoing.value.some((item) => {
    if ((item.work_type || "maintenance") !== (record.work_type || "maintenance")) return false;
    if (sourceId && item.source_record_id && item.source_record_id === sourceId) return true;
    return String(item.title || "").replace(/\s+/g, "") === title;
  });
}

function isManualKey(key: string): boolean {
  return key.startsWith("manual:");
}

function recordKey(record: Dict): string {
  return record?.manual_key || `${record.work_type || "maintenance"}:${record.record_id}`;
}

function draftRecordForKey(key: string): Dict | null {
  const record = records.value.find((item) => recordKey(item) === key);
  if (record) return record;
  const draft = drafts.get(key);
  if (isManualKey(key) && draft) return manualRecordFromDraft(key, draft);
  return null;
}

function manualDraftDefaults(type: string): Dict {
  return {
    manual: true,
    work_type: workTypes.some((item) => item.value === type) ? type : "maintenance",
    title: "",
    building: "",
    specialty: "",
    level: "",
    maintenance_cycle: "",
    start_time: "",
    end_time: "",
    location: "",
    content: "",
    reason: "",
    impact: "",
    progress: "",
    zhihang_involved: false,
    zhihang_record_id: "",
    zhihang_title: "",
    zhihang_progress: "",
    repair_device: "",
    repair_fault: "",
    fault_type: "",
    repair_mode: "",
    discovery: "",
    symptom: "",
    solution: "",
  };
}

function manualRecordFromDraft(key: string, draft: Dict): Dict {
  const type = draft.work_type || "maintenance";
  return {
    manual: true,
    manual_key: key,
    record_id: key,
    source_record_id: "",
    work_type: type,
    notice_type: type === "change" ? "设备变更" : type === "repair" ? "设备检修" : "维保通告",
    title: draft.title || draft.content || `手动${workTypeLabel(type)}通告`,
    display_fields: {
      "手动标题": draft.title || draft.content || "",
      "楼栋": draft.building || "",
      "变更楼栋": draft.building || "",
      "所属数据中心/楼栋-使用": draft.building || "",
      "专业类别": draft.specialty || "",
      "专业": draft.specialty || "",
      "所属专业": draft.specialty || "",
      "维护周期": draft.maintenance_cycle || "",
    },
  };
}

function repairDraftDefaults(record: Dict): Dict {
  return {
    start_time: todayInput(23, 50),
    end_time: toDatetimeLocal(firstRepairField(record, ["故障发生时间", "发现故障时间"])) || "",
    location: "",
    content: titleForRecord(record),
    level: levelForRecord(record),
    specialty: specialtyForRecord(record),
    reason: firstRepairField(record, ["故障原因", "故障维修原因"]),
    impact: "",
    progress: "",
    repair_device: repairDeviceText(record),
    repair_fault: firstRepairField(record, ["维修故障", "故障维修原因"]),
    fault_type: firstRepairField(record, ["故障类型"]) || "设备故障",
    repair_mode: firstRepairField(record, ["维修方式", "维修方", "供应商名称"]),
    discovery: firstRepairField(record, ["对应来源"]),
    symptom: firstRepairField(record, ["故障发生现象描述", "故障现象"]),
    solution: firstRepairField(record, ["解决方案", "维修方案", "后续整改措施"]),
  };
}

function getDraft(record: Dict): Dict {
  const key = recordKey(record);
  if (!drafts.has(key)) {
    if (record.manual) {
      drafts.set(key, manualDraftDefaults(record.work_type));
    } else if ((record.work_type || "maintenance") === "repair") {
      drafts.set(key, repairDraftDefaults(record));
    } else {
      const f = fieldsOf(record);
      const memory = record.memory || {};
      const isChange = (record.work_type || "maintenance") === "change";
      drafts.set(key, {
        title: titleForRecord(record),
        specialty: specialtyForRecord(record),
        level: levelForRecord(record),
        maintenance_cycle: f["维护周期"] || "",
        start_time: isChange ? (toDatetimeLocal(f["变更开始日期（阿里）"]) || todayInput(9, 30)) : todayInput(9, 30),
        end_time: isChange ? (toDatetimeLocal(f["变更结束日期（阿里）"]) || todayInput(18, 30)) : todayInput(18, 30),
        location: memory.location || "",
        content: isChange ? titleForRecord(record) : (memory.content || ""),
        reason: memory.reason || "",
        impact: memory.impact || defaults.impact,
        progress: defaults.progress,
        zhihang_involved: false,
        zhihang_record_id: "",
        zhihang_title: "",
        zhihang_progress: "",
      });
    }
    saveDrafts();
  }
  return drafts.get(key) || {};
}

function storageKey(): string {
  return `clipflow-vue-workbench:${currentScope.value || "ALL"}`;
}

function loadDrafts(): void {
  try {
    const payload = JSON.parse(localStorage.getItem(storageKey()) || "{}");
    selectedKeys.clear();
    for (const key of payload.selected || []) selectedKeys.add(String(key));
    drafts.clear();
    for (const [key, value] of Object.entries(payload.drafts || {})) drafts.set(key, value as Dict);
  } catch {
    selectedKeys.clear();
    drafts.clear();
  }
}

function saveDrafts(): void {
  const payload: Dict = { selected: Array.from(selectedKeys), drafts: {} };
  for (const [key, value] of drafts.entries()) payload.drafts[key] = value;
  localStorage.setItem(storageKey(), JSON.stringify(payload));
}

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

async function loadAuthStatus(): Promise<void> {
  authChecking.value = true;
  try {
    const data = await api(`/api/auth/status?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    auth.loggedIn = Boolean(data.logged_in);
    auth.user = data.user || {};
    auth.scopeOptions = data.scope_options || [];
    auth.loginUrl = data.login_url || "/api/auth/login";
    if (!currentScope.value && data.default_scope) currentScope.value = data.default_scope;
    if (!currentScope.value && auth.scopeOptions[0]?.value) currentScope.value = auth.scopeOptions[0].value;
  } finally {
    authChecking.value = false;
  }
}

async function loadOverview(): Promise<void> {
  try {
    const data = await api("/api/scope-overview");
    scopeOverview.value = data.scopes || data.items || {};
  } catch {
    scopeOverview.value = {};
  }
}

async function loadCurrentPermissionRequest(): Promise<void> {
  try {
    const data = await api("/api/auth/permission-requests/current");
    const request = data.request || {};
    permissionRequest.requestId = request.request_id || "";
    permissionRequest.scopes = Array.isArray(request.requested_scopes) ? request.requested_scopes : permissionRequest.scopes;
    if (permissionRequest.requestId) {
      permissionRequest.message = "已恢复待确认申请，请输入管理员提供的验证码。";
    }
  } catch {
    // No permission request to restore.
  }
}

async function loadHandoverLinks(): Promise<void> {
  try {
    const data = await api("/api/handover-links");
    handoverLinks.value = data.links || {};
  } catch {
    handoverLinks.value = {};
  }
}

async function loadWorkbench(): Promise<void> {
  if (!currentScope.value) return;
  loading.value = true;
  try {
    const data = await api(`/api/workbench?scope=${encodeURIComponent(currentScope.value)}`);
    records.value = data.records || [];
    ongoing.value = data.ongoing || [];
    zhihangRecords.value = data.zhihang_change_records || [];
    dailySummary.value = data.daily_summary || { date: "", items: [], stats: {} };
    defaults.impact = data.defaults?.impact || defaults.impact;
    defaults.progress = data.defaults?.progress || defaults.progress;
    if (!userSelectedWorkType.value) {
      workType.value = resolveInitialWorkType(data.default_work_type || workType.value);
    }
    syncText.value = data.source_snapshot_ready === false ? "后台正在准备数据" : `数据 ${data.last_loaded_at || "已就绪"}`;
    pruneSelection();
  } catch (error: any) {
    syncText.value = error?.message || "加载失败";
  } finally {
    loading.value = false;
  }
}

function resolveInitialWorkType(preferred: string): string {
  const preferredType = workTypes.some((item) => item.value === preferred) ? preferred : "maintenance";
  if (recordTypeCounts.value[preferredType] > 0) return preferredType;
  const fallback = workTypes.find((item) => recordTypeCounts.value[item.value] > 0);
  return fallback?.value || preferredType;
}

function selectWorkType(value: string): void {
  workType.value = value;
  userSelectedWorkType.value = true;
}

function pruneSelection(): void {
  const valid = new Set(records.value.map(recordKey));
  for (const key of Array.from(selectedKeys)) {
    if (!valid.has(key) && !isManualKey(key)) selectedKeys.delete(key);
  }
  saveDrafts();
}

function enterScope(scope: string): void {
  currentScope.value = normalizeScopeValue(scope, "ALL");
  isWorkbench.value = true;
  userSelectedWorkType.value = false;
  const url = new URL(window.location.href);
  url.searchParams.set("scope", currentScope.value);
  window.history.replaceState({}, "", url);
  loadDrafts();
  loadWorkbench();
}

function toggleRecordSelection(row: NoticeRow | undefined): void {
  const key = row?.id || "";
  if (!key) return;
  if (selectedKeys.has(key)) selectedKeys.delete(key);
  else selectedKeys.add(key);
  activeDraftKey.value = key;
  const record = draftRecordForKey(key);
  if (record) getDraft(record);
  saveDrafts();
}

function addManualDraft(): void {
  const key = `manual:${workType.value}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  drafts.set(key, manualDraftDefaults(workType.value));
  selectedKeys.add(key);
  activeDraftKey.value = key;
  saveDrafts();
}

function removeDraft(key: string): void {
  selectedKeys.delete(key);
  if (isManualKey(key)) drafts.delete(key);
  if (activeDraftKey.value === key) activeDraftKey.value = "";
  saveDrafts();
}

function normalizeNoticeLabel(label: string): string {
  return String(label || "").replace(/\s+/g, "").replace(/[：:]/g, "");
}

function parseSections(text: string): Dict {
  const sections: Dict = {};
  const re = /【([^】]+)】([\s\S]*?)(?=(?:\n\s*)*【[^】]+】|$)/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(text))) sections[normalizeNoticeLabel(match[1])] = String(match[2] || "").trim();
  return sections;
}

function sectionValue(sections: Dict, names: string[], fallback = ""): string {
  for (const name of names) {
    const value = sections[normalizeNoticeLabel(name)];
    if (String(value || "").trim()) return String(value).trim();
  }
  return fallback;
}

function parsePastedNotice(): void {
  const text = pasteText.value.trim();
  if (!text) return;
  const sections = parseSections(text);
  const type = /设备检修|检修通告/.test(text) ? "repair" : /设备变更|变更通告/.test(text) ? "change" : "maintenance";
  const key = `manual:${type}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const draft = manualDraftDefaults(type);
  draft.title = sectionValue(sections, ["标题"]);
  draft.location = sectionValue(sections, ["地点", "位置"]);
  draft.specialty = sectionValue(sections, ["专业", "专业类别"]);
  draft.reason = sectionValue(sections, ["原因", "故障原因"]);
  draft.impact = sectionValue(sections, ["影响", "影响范围"]);
  draft.progress = sectionValue(sections, ["进度", "完成情况"]);
  draft.maintenance_cycle = sectionValue(sections, ["维保周期", "维护周期"]);
  draft.level = sectionValue(sections, ["等级", "变更等级", "紧急程度"]);
  draft.content = type === "repair" ? "" : sectionValue(sections, ["内容"], draft.title);
  draft.repair_device = sectionValue(sections, ["维修设备"]);
  draft.repair_fault = sectionValue(sections, ["维修故障"]);
  draft.fault_type = sectionValue(sections, ["故障类型"]);
  draft.repair_mode = sectionValue(sections, ["维修方式"]);
  draft.discovery = sectionValue(sections, ["故障发现方式"]);
  draft.symptom = sectionValue(sections, ["故障现象"]);
  draft.solution = sectionValue(sections, ["解决方案"]);
  drafts.set(key, draft);
  selectedKeys.add(key);
  activeDraftKey.value = key;
  workType.value = type;
  pasteText.value = "";
  showPasteParser.value = false;
  saveDrafts();
}

function bindZhihang(draft: Dict): void {
  const item = zhihangRecords.value.find((record) => record.record_id === draft.zhihang_record_id);
  draft.zhihang_title = item?.title || "";
  draft.zhihang_progress = item?.progress || "";
  saveDrafts();
}

function opId(key: string): string {
  return `${key}:${Date.now()}`;
}

function buildStartPayload(key: string): Dict | null {
  const record = draftRecordForKey(key);
  const draft = drafts.get(key);
  if (!record || !draft) return null;
  const type = record.work_type || "maintenance";
  const action = record.manual ? "start" : sourceActionForRecord(record);
  const targetRecordId = targetRecordIdForRecord(record);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: type,
    notice_type: record.notice_type || "",
    manual: Boolean(record.manual),
    manual_id: record.manual ? key : "",
    source_app_token: record.manual ? "" : (record.source_app_token || ""),
    source_table_id: record.manual ? "" : (record.source_table_id || ""),
    maintenance_cycle: record.manual ? (draft.maintenance_cycle || "") : (fieldsOf(record)["维护周期"] || ""),
    specialty: draft.specialty || specialtyForRecord(record),
    record_id: action === "start" ? record.record_id : targetRecordId,
    source_record_id: record.manual ? "" : record.record_id,
    target_record_id: action === "update" ? targetRecordId : "",
    source_progress: sourceProgressForRecord(record),
    building_codes: record.manual ? [] : (record.building_codes || []),
    building: record.manual ? (draft.building || "") : buildingForRecord(record),
    title: record.manual ? (draft.title || "") : (type === "repair" ? draft.content : titleForRecord(record)),
    level: record.manual ? (draft.level || "") : (type === "repair" ? (draft.level || "") : (type === "change" ? levelForRecord(record) : "")),
    start_time: draft.start_time,
    end_time: draft.end_time,
    location: draft.location,
    content: draft.content,
    reason: draft.reason,
    impact: draft.impact,
    progress: draft.progress,
    zhihang_involved: type === "change" ? Boolean(draft.zhihang_involved) : false,
    zhihang_record_id: type === "change" ? (draft.zhihang_record_id || "") : "",
    zhihang_title: type === "change" ? (draft.zhihang_title || "") : "",
    zhihang_progress: type === "change" ? (draft.zhihang_progress || "") : "",
    repair_device: type === "repair" ? (draft.repair_device || "") : "",
    repair_fault: type === "repair" ? (draft.repair_fault || "") : "",
    fault_type: type === "repair" ? (draft.fault_type || "") : "",
    repair_mode: type === "repair" ? (draft.repair_mode || "") : "",
    discovery: type === "repair" ? (draft.discovery || "") : "",
    symptom: type === "repair" ? (draft.symptom || "") : "",
    solution: type === "repair" ? (draft.solution || "") : "",
    fault_time: type === "repair" ? (draft.end_time || "") : "",
    expected_time: type === "repair" ? (draft.start_time || "") : "",
    operation_id: draft.operation_id || (draft.operation_id = opId(`${key}:${action}`)),
  };
}

async function sendStart(key: string): Promise<void> {
  const payload = buildStartPayload(key);
  if (!payload) return;
  if (payload.work_type === "maintenance" && payload.manual && !payload.maintenance_cycle) {
    rememberJob(key, { text: "纯手填维保必须选择维保周期", status: "failed", phase: "failed" });
    return;
  }
  await sendAction(payload, key);
  saveDrafts();
}

function ongoingDraft(item: Dict): Dict {
  const id = item.active_item_id || item.record_id || "";
  if (!ongoingEdits.has(id)) ongoingEdits.set(id, { progress: item.progress || item.content || "" });
  return ongoingEdits.get(id) || {};
}

function setOngoingEdit(item: Dict, key: string, value: string): void {
  const id = item.active_item_id || item.record_id || "";
  const current = ongoingDraft(item);
  current[key] = value;
  ongoingEdits.set(id, current);
}

function buildOngoingPayload(item: Dict, action: string): Dict {
  const edit = ongoingDraft(item);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: item.work_type || "maintenance",
    notice_type: item.notice_type || "",
    record_id: item.record_id || item.target_record_id || "",
    target_record_id: item.record_id || item.target_record_id || "",
    active_item_id: item.active_item_id || "",
    source_record_id: item.source_record_id || "",
    title: item.title || item.content || "",
    specialty: item.specialty || "",
    building: item.building || "",
    location: edit.location || item.location || "",
    content: edit.content || item.content || "",
    reason: edit.reason || item.reason || "",
    impact: edit.impact || item.impact || "",
    progress: edit.progress || item.progress || "",
    operation_id: opId(`${item.active_item_id || item.record_id}:${action}`),
  };
}

async function sendOngoing(item: Dict, action: string): Promise<void> {
  const key = item.active_item_id || item.record_id || "";
  await sendAction(buildOngoingPayload(item, action), key);
}

async function sendAction(payload: Dict, lineKey: string): Promise<void> {
  try {
    rememberJob(lineKey, { text: "已受理，正在进入后台任务", status: "busy", phase: "accepted" });
    const data = await api("/api/workbench-actions", { method: "POST", body: JSON.stringify(payload) });
    rememberJob(lineKey, {
      job_id: data.job_id,
      payload,
      text: payload.work_type === "maintenance" ? "已受理，正在发送个人消息" : "已受理，等待主界面显示",
      status: "busy",
      phase: data.initial_phase || "accepted",
    });
    pollJob(data.job_id, lineKey);
  } catch (error: any) {
    rememberJob(lineKey, { text: error?.message || "提交失败", status: "failed", phase: "failed" });
  }
}

function rememberJob(key: string, patch: Dict): void {
  jobStates.set(key, { ...(jobStates.get(key) || {}), ...patch, updated_at: new Date().toISOString() });
}

function jobText(key: string): string {
  return jobStates.get(key)?.text || "";
}

function jobClass(key: string): string {
  return jobStates.get(key)?.status || "";
}

function isLineBusy(key: string): boolean {
  const phase = jobStates.get(key)?.phase || "";
  return Boolean(phase && !["success", "failed"].includes(phase));
}

async function pollJob(jobId: string, lineKey: string): Promise<void> {
  for (let i = 0; i < 120; i += 1) {
    try {
      const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
      const phase = data.phase || data.status || "";
      const text = data.message || data.upload_message || phase || "处理中";
      rememberJob(lineKey, {
        phase,
        status: phase === "success" ? "success" : phase === "failed" ? "failed" : "busy",
        text: phase === "success" ? "成功" : phase === "failed" ? (data.error || text || "失败") : text,
      });
      if (["success", "failed"].includes(phase)) {
        if (phase === "success") await loadWorkbench();
        return;
      }
    } catch {
      rememberJob(lineKey, { text: "后台处理中，等待状态同步", status: "busy" });
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

function startJobSse(): void {
  if (eventSource.value) eventSource.value.close();
  try {
    const source = new EventSource("/api/jobs/stream");
    const handleJobEvent = (event: MessageEvent) => {
      let payload: Dict;
      try {
        payload = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      const job = payload.job || payload;
      if (!job?.job_id) return;
      for (const [key, value] of jobStates.entries()) {
        if (value.job_id === job.job_id) {
          rememberJob(key, {
            phase: job.phase,
            status: job.phase === "success" ? "success" : job.phase === "failed" ? "failed" : "busy",
            text: job.phase === "success" ? "成功" : job.error || job.message || job.phase,
          });
        }
      }
    };
    source.onmessage = handleJobEvent;
    source.addEventListener("job", handleJobEvent);
    source.onerror = () => {
      if (eventSource.value === source && source.readyState === EventSource.CLOSED) {
        eventSource.value = null;
        window.setTimeout(startJobSse, 5000);
      }
    };
    eventSource.value = source;
  } catch {
    eventSource.value = null;
  }
}

async function deleteOngoing(item: Dict): Promise<void> {
  const key = item.active_item_id || item.record_id || "";
  if (!window.confirm(`确认删除「${ongoingTitle(item)}」？将同步删除 Qt 条目和多维记录。`)) return;
  try {
    rememberJob(key, { text: "删除中", status: "busy", phase: "deleting" });
    await api("/api/ongoing-items/delete", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        work_type: item.work_type || "maintenance",
        notice_type: item.notice_type || "",
        active_item_id: item.active_item_id || "",
        record_id: item.record_id || "",
        target_record_id: item.target_record_id || item.record_id || "",
        source_record_id: item.source_record_id || "",
      }),
    });
    rememberJob(key, { text: "已删除", status: "success", phase: "success" });
    await loadWorkbench();
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "删除失败", status: "failed", phase: "failed" });
  }
}

function ongoingTitle(item: Dict): string {
  if ((item.work_type || "maintenance") === "repair") {
    const source = records.value.find((record) => record.record_id === item.source_record_id);
    if (source) return titleForRecord(source);
  }
  return item.title || item.content || "未命名通告";
}

function ongoingMeta(item: Dict): string {
  return [item.building, item.specialty, item.maintenance_cycle, item.time_str || item.start_time].filter(Boolean).join(" · ");
}

async function refreshRepair(): Promise<void> {
  repairRefreshing.value = true;
  try {
    await api(`/api/repair-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
  } finally {
    repairRefreshing.value = false;
  }
}

async function logout(): Promise<void> {
  await api("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
  window.location.href = "/";
}

async function submitPermissionRequest(): Promise<void> {
  permissionBusy.value = true;
  try {
    const data = await api("/api/auth/permission-requests", {
      method: "POST",
      body: JSON.stringify({ scopes: permissionRequest.scopes, reason: permissionRequest.reason }),
    });
    permissionRequest.requestId = data.request_id || data.request?.request_id || "";
    permissionRequest.message = "申请已发送给管理员，请输入验证码。";
  } catch (error: any) {
    permissionRequest.message = error?.message || "提交失败";
  } finally {
    permissionBusy.value = false;
  }
}

function updatePermissionRequest(patch: Partial<typeof permissionRequest>): void {
  Object.assign(permissionRequest, patch);
}

async function confirmPermissionRequest(): Promise<void> {
  permissionBusy.value = true;
  try {
    await api("/api/auth/permission-requests/confirm", {
      method: "POST",
      body: JSON.stringify({ request_id: permissionRequest.requestId, code: permissionRequest.code }),
    });
    await loadAuthStatus();
    if (auth.scopeOptions[0]?.value) enterScope(auth.scopeOptions[0].value);
  } catch (error: any) {
    permissionRequest.message = error?.message || "验证码错误";
  } finally {
    permissionBusy.value = false;
  }
}

watch(workType, () => {
  activeDraftKey.value = "";
});

onMounted(async () => {
  await loadAuthStatus();
  if (auth.loggedIn && !auth.scopeOptions.length) {
    await loadCurrentPermissionRequest();
  }
  if (auth.loggedIn && auth.scopeOptions.length) {
    await Promise.all([loadOverview(), loadHandoverLinks()]);
    if (currentScope.value) {
      isWorkbench.value = true;
      loadDrafts();
      await loadWorkbench();
    }
    startJobSse();
  }
});

onBeforeUnmount(() => {
  if (eventSource.value) eventSource.value.close();
});
</script>

<style scoped>
:global(*) {
  box-sizing: border-box;
}

.app-shell {
  min-height: 100vh;
  background: #eef3f8;
  color: #0f172a;
  font-family: "Microsoft YaHei", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.topbar {
  position: sticky;
  top: 0;
  z-index: 10;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 14px 20px;
  border-bottom: 1px solid #dbe3ee;
  background: rgba(255, 255, 255, 0.94);
  backdrop-filter: blur(10px);
}

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.brand-mark {
  display: grid;
  place-items: center;
  width: 46px;
  height: 46px;
  border-radius: 6px;
  background: #0f62fe;
  color: #ffffff;
  font-weight: 800;
}

h1,
h2,
p {
  margin: 0;
}

h1 {
  font-size: 22px;
  line-height: 1.25;
}

.brand p,
.hint {
  color: #64748b;
  font-size: 13px;
}

.topbar-actions,
.row-actions,
.card-actions,
.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.btn,
button,
a.btn {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  text-decoration: none;
  cursor: pointer;
}

.btn:disabled,
button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
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

.btn.danger {
  border-color: #dc2626;
  background: #dc2626;
  color: #ffffff;
}

.danger-text {
  color: #b91c1c;
}

.user-chip {
  padding: 7px 10px;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  background: #f8fafc;
  color: #334155;
  font-size: 13px;
}

.center-state {
  width: min(720px, calc(100vw - 32px));
  margin: 80px auto;
  display: grid;
  gap: 14px;
  justify-items: start;
  padding: 28px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.spinner {
  width: 28px;
  height: 28px;
  border: 3px solid #dbeafe;
  border-top-color: #2563eb;
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.home-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 14px;
  padding: 24px;
}

.scope-card {
  min-height: 112px;
  display: grid;
  align-content: center;
  gap: 10px;
  padding: 18px;
  text-align: left;
  border: 1px solid #dbe3ee;
  background: #ffffff;
}

.scope-card strong {
  font-size: 22px;
}

.scope-card span {
  color: #64748b;
  line-height: 1.5;
}

.workbench {
  padding: 16px;
}

.summary-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}

.summary-strip article,
.panel,
.paste-panel {
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.summary-strip article {
  padding: 12px 14px;
}

.summary-strip span {
  color: #64748b;
  font-size: 13px;
}

.summary-strip strong {
  display: block;
  margin-top: 4px;
  font-size: 22px;
}

.toolbar,
.paste-panel {
  margin-bottom: 12px;
  padding: 10px;
}

.segmented {
  display: inline-flex;
  gap: 4px;
  padding: 4px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
  background: #f8fafc;
}

.segmented button {
  border: 0;
  background: transparent;
}

.segmented button.active {
  background: #2563eb;
  color: #ffffff;
}

.search {
  flex: 1 1 260px;
  min-width: 180px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 9px 11px;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(280px, 0.88fr) minmax(430px, 1.35fr) minmax(320px, 0.95fr);
  gap: 12px;
  min-height: calc(100vh - 230px);
}

.panel {
  min-width: 0;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.panel-head,
.card-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.panel-head h2 {
  font-size: 17px;
}

.panel-head span,
.card-title span {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}

.draft-stack,
.ongoing-list {
  overflow: auto;
  display: grid;
  gap: 10px;
}

.draft-card,
.ongoing-card {
  padding: 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.draft-card.active {
  border-color: #2563eb;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-top: 10px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
select,
textarea {
  width: 100%;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

textarea {
  min-height: 72px;
  resize: vertical;
}

.span-2 {
  grid-column: 1 / -1;
}

.repair-fields,
.zhihang-line,
.card-actions {
  margin-top: 10px;
}

.zhihang-line {
  display: grid;
  gap: 8px;
}

.job-line {
  flex: 1 1 auto;
  color: #64748b;
  font-size: 13px;
}

.job-line.busy {
  color: #1d4ed8;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}

.empty-block {
  display: grid;
  place-items: center;
  min-height: 180px;
  padding: 18px;
  color: #64748b;
  text-align: center;
  line-height: 1.7;
  background: #f8fafc;
  border-radius: 6px;
}

.paste-panel textarea,
.request-panel textarea {
  min-height: 100px;
}

.scope-checks {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.verify-box {
  display: grid;
  gap: 8px;
}

@media (max-width: 1120px) {
  .workspace {
    grid-template-columns: 1fr;
  }

  .panel {
    min-height: 360px;
  }
}

@media (max-width: 720px) {
  .topbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .summary-strip,
  .form-grid {
    grid-template-columns: 1fr;
  }
}
</style>
