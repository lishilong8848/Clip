<template>
  <main class="app-shell" :class="{ 'signature-link-shell': signatureLinkMode }">
    <AppTopbar
      v-if="!signatureLinkMode"
      :brand-logo-src="brandLogoSrc"
      :header-subtitle="headerSubtitle"
      :auth="auth"
      :is-workbench="isWorkbench"
      :is-engineer-mop-page="isEngineerMopPage"
      :is-event-page="isEventPage"
      :visible-scope-options="normalizedVisibleScopeOptions"
      :current-scope="currentScope"
      :loading="loading"
      :refresh-menu-open="refreshMenuOpen"
      :repair-refreshing="repairRefreshing"
      :change-refreshing="changeRefreshing"
      :event-refreshing="eventRefreshing"
      :refresh-cooldown="refreshCooldown"
      :workbench-refresh-title="refreshButtonTitle('workbench')"
      :repair-refresh-title="refreshButtonTitle('repair')"
      :change-refresh-title="refreshButtonTitle('change')"
      :event-refresh-title="refreshButtonTitle('event')"
      :is-admin="isAdmin"
      @return-home="returnToHome"
      @switch-scope="switchScope"
      @update:refresh-menu-open="refreshMenuOpen = $event"
      @refresh-workbench="manualRefreshWorkbench"
      @refresh-repair="refreshRepair"
      @refresh-change="refreshChange"
      @refresh-event="refreshEvent"
      @open-admin="showAdminTools = true"
      @logout="logout"
    />

    <AppStatusNotices
      v-if="!signatureLinkMode || pageStatusText"
      :connection-notice="signatureLinkMode ? null : connectionNotice"
      :page-status-text="pageStatusText"
    />

    <AdminTools
      :open="showAdminTools"
      :scope-options="requestableScopes"
      @close="showAdminTools = false"
    />

    <HistoryMemoryPage
      v-if="isHistoryMemoryPage"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :is-admin="isAdmin"
      :login-url="auth.loginUrl"
    />

    <SignaturePage
      v-else-if="isSignaturePage"
      :default-scope="currentScope"
    />

    <EngineerMopPage
      v-else-if="isEngineerMopPage"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :login-url="auth.loginUrl"
      :scope-options="visibleScopeOptions"
    />

    <AuthPanels
      v-else-if="showPermissionRequestPanel || authChecking || !auth.loggedIn || (auth.loggedIn && !auth.scopeOptions.length)"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :user="auth.user"
      :login-url="auth.loginUrl"
      :busy="permissionBusy"
      :request="permissionRequest"
      :requestable-scopes="permissionPanelRequestableScopes"
      :title="permissionPanelTitle"
      :description="permissionPanelDescription"
      :empty-text="permissionPanelEmptyText"
      :show-back="showPermissionRequestPanel && auth.scopeOptions.length > 0"
      @update-request="updatePermissionRequest"
      @submit="submitPermissionRequest"
      @confirm="confirmPermissionRequest"
      @back="closePermissionRequestPanel"
    />

    <EventManagementPage
      v-else-if="isEventPage"
      :scope="currentScope"
      :scope-options="visibleScopeOptions"
      :refresh-nonce="eventRefreshNonce"
      :is-admin="isAdmin"
      @refreshing="eventRefreshing = $event"
      @status="syncText = $event"
    />

    <ScopeHome
      v-else-if="!isWorkbench"
      :scope-options="visibleScopeOptions"
      :overview="scopeOverview"
      :handover-links="handoverLinks"
      :can-request-more-scopes="additionalRequestableScopes.length > 0"
      @enter="enterScope"
      @event="enterEventManagement"
      @engineer="enterEngineerMop"
      @request-permission="openAdditionalPermissionRequest"
    />

    <section v-else class="workbench">
      <div v-if="loading" class="loading-line">
        正在加载 {{ scopeLabel(currentScope) }} 数据...
      </div>
      <WorkbenchSummaryStrip
        :started="liveDailyStats.started || 0"
        :updated="liveDailyStats.updated || 0"
        :ended="liveDailyStats.ended || 0"
        :ongoing="liveOngoingCount"
      />

      <WorkbenchToolbar
        v-model:search-text="searchText"
        v-model:specialty-filter="specialtyFilter"
        v-model:manual-picker-open="showManualTypePicker"
        :work-types="workTypes"
        :work-type="workType"
        :record-type-counts="recordTypeCounts"
        :specialty-filter-options="specialtyFilterOptions"
        :manual-recent-types="manualRecentTypeOptions"
        :manual-prefill-types="manualPrefillTypeValues"
        :is-admin="isAdmin"
        :draft-save-text="draftSaveText"
        :draft-save-failed="draftSaveFailed"
        @select-work-type="selectWorkType"
        @manual-select="addManualDraft"
        @toggle-paste="showPasteParser = !showPasteParser"
        @toggle-memory="showMemoryImporter = !showMemoryImporter"
        @clear-filters="clearWorkbenchFilters"
      />

      <WorkbenchPastePanel
        v-if="showPasteParser"
        v-model:paste-text="pasteText"
        v-model:change-target-search-text="changeTargetSearchText"
        v-model:change-source-search-text="changeSourceSearchText"
        v-model:selected-change-source-id="selectedChangeSourceId"
        :paste-parse-status="pasteParseStatus"
        :paste-parse-line="pasteParseLine"
        :paste-parse-busy="pasteParseBusy"
        :pending-change-target-selection="pendingChangeTargetSelection"
        :filtered-change-target-candidates="filteredChangeTargetCandidates"
        :change-source-candidates="changeSourceCandidates"
        :filtered-change-source-candidates="filteredChangeSourceCandidates"
        :visible-active-change-target-candidate="visibleActiveChangeTargetCandidate"
        :selected-change-target-id="selectedChangeTargetId"
        :change-target-confirming="changeTargetConfirming"
        :selected-target-visible="selectedChangeTargetVisible"
        :selected-source-visible="selectedChangeSourceVisible"
        :work-type-label="workTypeLabel"
        :target-candidate-id="changeTargetCandidateId"
        :source-candidate-id="changeSourceCandidateId"
        :detail-rows-for="changeTargetDetailRows"
        @parse="parsePastedNotice"
        @preview-target="previewChangeTarget"
        @select-target="selectChangeTarget"
        @confirm="confirmPastedChangeTarget"
      />

      <WorkbenchMemoryImportPanel
        v-if="showMemoryImporter && isAdmin"
        v-model:memory-import-text="memoryImportText"
        :memory-import-line="memoryImportLine"
        :memory-import-line-type="memoryImportLineType"
        :memory-import-busy="memoryImportBusy"
        @import="importHistoricalMemory"
      />

      <section class="workbench-flow-strip" aria-label="通告处理流程">
        <article>
          <b>1</b>
          <span>
            <strong>选事项</strong>
            <small>{{ filteredRows.length }} 条可选</small>
          </span>
        </article>
        <article :class="{ active: selectedDraftRows.length > 0 }">
          <b>2</b>
          <span>
            <strong>核对发送</strong>
            <small>{{ selectedDraftRows.length ? `${selectedDraftRows.length} 条草稿` : "先选左侧事项" }}</small>
          </span>
        </article>
        <article :class="{ active: filteredOngoing.length > 0 }">
          <b>3</b>
          <span>
            <strong>处理进行中</strong>
            <small>{{ filteredOngoing.length ? `${filteredOngoing.length} 条进行中` : "发送成功后出现" }}</small>
          </span>
        </article>
      </section>

      <section class="workspace">
        <WorkbenchRecordsPanel
          :rows="filteredRows"
          :selected-id="activeDraftKey"
          @select="toggleRecordSelection"
        />

        <WorkbenchDraftsPanel
          ref="draftStackRef"
          :rows="selectedDraftRows"
          :active-draft-key="activeDraftKey"
          :specialty-filter="specialtyFilter"
          :requestable-scopes="requestableScopes"
          :maintenance-cycle-options="maintenanceCycleOptions"
          :zhihang-records="zhihangRecords"
          :preview-draft-key="previewDraftKey"
          :type-override-busy-key="typeOverrideBusyKey"
          :is-line-busy="isLineBusy"
          :draft-card-meta="draftCardMeta"
          :draft-summary="draftSummary"
          :draft-type-conflict-text="draftTypeConflictText"
          :draft-missing-text="draftMissingText"
          :draft-work-type="draftWorkType"
          :draft-upload-preview-rows="draftUploadPreviewRows"
          :notice-preview-text="noticePreviewText"
          :can-toggle-work-type-override="canToggleWorkTypeOverride"
          :work-type-override-button-label="workTypeOverrideButtonLabel"
          :is-converted-maintenance-change="isConvertedMaintenanceChange"
          :send-draft-button-label="sendDraftButtonLabel"
          :draft-field-class="draftFieldClass"
          :job-text="jobText"
          :job-class="jobClass"
          :job-copy-text="jobCopyText"
          @activate="activeDraftKey = $event"
          @pin="pinDraftInMiddlePanel"
          @remove="removeDraft"
          @set-draft="setDraftField"
          @manual-type-change="onManualDraftTypeChange"
          @building-change="onDraftBuildingChange"
          @bind-zhihang="bindZhihang"
          @toggle-preview="toggleDraftPreview"
          @copy-notice="copyJobNoticeText"
          @send="sendStart"
          @toggle-work-type-override="toggleWorkTypeOverride"
        />

        <WorkbenchOngoingPanel
          v-model:ongoing-type-filter="ongoingTypeFilter"
          v-model:undo-filter="undoFilter"
          :filtered-ongoing="filteredOngoing"
          :ongoing-count-label="ongoingCountLabel"
          :ongoing-empty-text="ongoingEmptyText"
          :recent-undo-items="recentUndoItems"
          :closed-summary-items="closedSummaryItems"
          :maintenance-cycle-options="maintenanceCycleOptions"
          :zhihang-records="zhihangRecords"
          :ongoing-line-key="ongoingLineKey"
          :undo-line-key="undoLineKey"
          :closed-line-key="closedLineKey"
          :ongoing-draft="ongoingDraft"
          :ongoing-title="ongoingTitle"
          :ongoing-meta="ongoingMeta"
          :ongoing-compact-summary="ongoingCompactSummary"
          :is-ongoing-expanded="isOngoingExpanded"
          :is-line-busy="isLineBusy"
          :ongoing-needs-binding="ongoingNeedsBinding"
          :ongoing-photo-count="ongoingPhotoCount"
          :ongoing-end-requires-site-photo="ongoingEndRequiresSitePhoto"
          :source-work-type-for-record="sourceWorkTypeForRecord"
          :ongoing-notice-preview-text="ongoingNoticePreviewText"
          :job-copy-text="jobCopyText"
          :job-text="jobText"
          :job-class="jobClass"
          @expand="expandOngoingCard"
          @toggle="toggleOngoingCard"
          @set-edit="setOngoingEdit"
          @bind-zhihang="bindOngoingZhihang"
          @photo-input="handleOngoingPhotoInput"
          @photo-paste="handleOngoingPhotoPaste"
          @remove-photo="removeOngoingPhoto"
          @send="sendOngoing"
          @copy-notice="copyOngoingNoticeText"
          @delete="deleteOngoing"
          @bind-target="bindOngoingTarget"
          @apply-undo="applyUndo"
        />
      </section>
    </section>

    <TargetRecordSelectionModal
      :selection="ongoingBindSelection"
      :candidates="ongoingBindCandidates"
      :selected-id="selectedOngoingBindId"
      :active-candidate="activeOngoingBindCandidate"
      :candidate-id="changeTargetCandidateId"
      :detail-rows-for="changeTargetDetailRows"
      @cancel="cancelOngoingBindSelection"
      @confirm="confirmOngoingBindSelection"
      @preview="previewOngoingBindCandidate"
      @select="selectOngoingBindCandidate"
    />
    <ConfirmDialog
      :open="actionConfirm.open"
      :tone="actionConfirm.tone"
      :kicker="actionConfirm.kicker"
      :title="actionConfirm.title"
      :message="actionConfirm.message"
      :details="actionConfirm.details"
      :confirm-label="actionConfirm.confirmLabel"
      :cancel-label="actionConfirm.cancelLabel"
      :confirm-class="actionConfirm.confirmClass"
      @resolve="resolveActionConfirm"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import AppStatusNotices from "./components/AppStatusNotices.vue";
import AppTopbar from "./components/AppTopbar.vue";
import AsyncPageState from "./components/AsyncPageState.vue";
import AuthPanels from "./components/AuthPanels.vue";
import ConfirmDialog from "./components/ConfirmDialog.vue";
import ScopeHome from "./components/ScopeHome.vue";
import TargetRecordSelectionModal from "./components/TargetRecordSelectionModal.vue";
import type { NoticeRow } from "./components/VirtualNoticeList.vue";
import WorkbenchDraftsPanel from "./components/WorkbenchDraftsPanel.vue";
import WorkbenchMemoryImportPanel from "./components/WorkbenchMemoryImportPanel.vue";
import WorkbenchOngoingPanel from "./components/WorkbenchOngoingPanel.vue";
import WorkbenchPastePanel from "./components/WorkbenchPastePanel.vue";
import WorkbenchRecordsPanel from "./components/WorkbenchRecordsPanel.vue";
import WorkbenchSummaryStrip from "./components/WorkbenchSummaryStrip.vue";
import WorkbenchToolbar from "./components/WorkbenchToolbar.vue";
import { AUTH_EXPIRED_EVENT, requestBinaryJson, requestJson } from "./api/client";
import { filterCandidatesBySearch } from "./candidateSearch";
import {
  loadDraftStorage,
  loadManualRecentTypesFromStorage,
  loadManualTemplateMemoryFromStorage,
  saveDraftStorage,
  saveManualRecentTypeToStorage,
  saveManualTemplateMemoryToStorage,
} from "./draftStorage";
import { backendJobStatusPatch, friendlyFailureText, terminalPhase } from "./jobStatus";
import {
  isKnownWorkType,
  manualPrefillWorkTypes,
  normalizeWorkTypeFilter,
  normalizeWorkType,
  noticeDurationError,
  noticeFieldLabel,
  noticeTemplate,
  noticeTypeKeywordRules,
  workTypeLabel,
  workTypes,
} from "./noticeTemplates";
import {
  inferBuildingText,
  parseSections,
  parsedActionFromStatus,
  parsedActionLabel,
  pastedNoticeStatus,
  pastedNoticeWorkType,
  rawSectionValue,
  sectionValue,
  splitNoticeTimeRange,
  toDatetimeLocal,
} from "./noticeParsing";
import { createCrossTabStreamCoordinator, type CrossTabStreamCoordinator } from "./streamCoordinator";
import type { LooseDict, ScopeOption, WorkTypeFilterValue, WorkTypeOption, WorkTypeValue } from "./types";

function asyncPage(loader: () => Promise<unknown>) {
  return defineAsyncComponent({
    loader: loader as () => Promise<never>,
    loadingComponent: AsyncPageState,
    errorComponent: AsyncPageState,
    delay: 120,
    timeout: 30000,
  });
}

const AdminTools = asyncPage(() => import("./components/AdminTools.vue"));
const EngineerMopPage = asyncPage(() => import("./components/EngineerMopPage.vue"));
const EventManagementPage = asyncPage(() => import("./components/EventManagementPage.vue"));
const HistoryMemoryPage = asyncPage(() => import("./components/HistoryMemoryPage.vue"));
const SignaturePage = asyncPage(() => import("./components/SignaturePage.vue"));

type Dict = LooseDict;
type DraftsPanelExpose = { scrollToTop: () => void };
type ActionConfirmTone = "danger" | "warning" | "primary";
type ActionConfirmState = {
  open: boolean;
  tone: ActionConfirmTone;
  kicker: string;
  title: string;
  message: string;
  details: string[];
  confirmLabel: string;
  cancelLabel: string;
  confirmClass: string;
  resolve?: (confirmed: boolean) => void;
};
const brandLogoSrc = "/assets/vnet-logo.png";
const buildingScopeCodes = ["110", "A", "B", "C", "D", "E", "H"];
const maintenanceCycleOptions = ["/", "每月", "每季", "每年", "半年", "每两年", "每三年", "每五年", "冬季保温每日"];
const nonPlanTitleSuffix = "（非计划性）";
const authKeepaliveMs = 30 * 60 * 1000;
const authKeepaliveRetryMs = 3 * 60 * 1000;
const manualRefreshCooldownMs = 30 * 1000;
const workbenchRetryDelaysMs = [3000, 8000, 20000];
const streamLeaderHeartbeatMs = 5000;
const streamLeaderTtlMs = 15000;
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

const placeholderFieldValues = new Set(["", "-", "--", "—", "——", "/", "无", "暂无"]);
const repairSpecialtyOptionFallback: Record<string, string> = {
  optAssNaw3: "电气",
  opt3jdJJb7: "暖通",
  opt509Mxgr: "弱电",
  optyLRPdQS: "消防",
};

const authChecking = ref(true);
const loading = ref(false);
const isHistoryMemoryPage = ref(window.location.pathname.replace(/\/$/, "") === "/admin/history-memory");
const isEngineerMopPage = ref(window.location.pathname.replace(/\/$/, "") === "/engineer/mop");
const isSignaturePage = ref(window.location.pathname.replace(/\/$/, "") === "/signature");
const isEventPage = ref(
  window.location.pathname.replace(/\/$/, "") === "/events"
  || new URLSearchParams(window.location.search).get("mode") === "events",
);
const signatureLinkMode = computed(() => (
  isSignaturePage.value && (() => {
    const params = new URLSearchParams(window.location.search);
    return Boolean(params.get("record_id") || params.get("temporary_id"));
  })()
));
const repairRefreshing = ref(false);
const changeRefreshing = ref(false);
const eventRefreshing = ref(false);
const eventRefreshNonce = ref(0);
const refreshMenuOpen = ref(false);
const isWorkbench = ref(false);
const initialUrlParams = new URLSearchParams(window.location.search);
const initialWorkType = normalizeWorkTypeFilter(initialUrlParams.get("work_type") || "");
const currentScope = ref(normalizeScopeValue(initialUrlParams.get("scope") || "", ""));
const syncText = ref("准备中");
const workType = ref<WorkTypeFilterValue>(initialWorkType);
const userSelectedWorkType = ref(Boolean(initialUrlParams.get("work_type")));
const searchText = ref("");
const specialtyFilter = ref("");
const activeDraftKey = ref("");
const activeOngoingKey = ref("");
const ongoingTypeFilter = ref<"all" | "current">("all");
const showPasteParser = ref(false);
const showManualTypePicker = ref(false);
const showMemoryImporter = ref(false);
const showAdminTools = ref(false);
const showPermissionRequestPanel = ref(false);
const undoFilter = ref("all");
const pasteText = ref("");
const pasteParseBusy = ref(false);
const pasteParseLine = ref("粘贴通告后解析。");
const pasteParseStatus = ref("");
const pendingChangeTargetSelection = ref<Dict | null>(null);
const selectedChangeTargetId = ref("");
const hoveredChangeTargetId = ref("");
const selectedChangeSourceId = ref("");
const changeTargetSearchText = ref("");
const changeSourceSearchText = ref("");
const changeTargetConfirming = ref(false);
const ongoingBindSelection = ref<Dict | null>(null);
const selectedOngoingBindId = ref("");
const hoveredOngoingBindId = ref("");
const typeOverrideBusyKey = ref("");
const memoryImportText = ref("");
const memoryImportBusy = ref(false);
const memoryImportLine = ref("粘贴历史通告后导入。");
const memoryImportLineType = ref("");
const eventSource = ref<EventSource | null>(null);
const sseConnected = ref(false);
const sharedJobStreamAvailable = ref(false);
const activeItemsEventSource = ref<EventSource | null>(null);
const activeItemsConnected = ref(false);
const sharedActiveItemsStreamAvailable = ref(false);
const activeItemsUpdatePending = ref(false);
const realtimeWarningVisible = ref(false);
const pageVisible = ref(typeof document === "undefined" ? true : !document.hidden);
const pendingHiddenRefresh = ref(false);
const draftSavedAt = ref(0);
const draftSaveFailed = ref(false);

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
  status: "",
  rejectReason: "",
});
const permissionBusy = ref(false);
const refreshCooldown = reactive<Record<string, boolean>>({
  workbench: false,
  repair: false,
  change: false,
  event: false,
});
const backendStatus = reactive({
  offline: false,
  message: "",
  lastErrorAt: 0,
});

const records = ref<Dict[]>([]);
const ongoing = ref<Dict[]>([]);
const zhihangRecords = ref<Dict[]>([]);
const dailySummary = ref<Dict>({ date: "", items: [], stats: {} });
const availableUndoItems = ref<Dict[]>([]);
const scopeOverview = ref<Record<string, Dict>>({});
const handoverLinks = ref<Record<string, string>>({});
const selectedKeys = reactive(new Set<string>());
const drafts = reactive(new Map<string, Dict>());
const ongoingEdits = reactive(new Map<string, Dict>());
const jobStates = reactive(new Map<string, Dict>());
const defaults = reactive({ impact: "无", progress: "" });
const localSummaryAdjustments = reactive({ started: 0, updated: 0, ended: 0, ongoing: 0 });
const actionConfirm = reactive<ActionConfirmState>({
  open: false,
  tone: "primary",
  kicker: "操作确认",
  title: "",
  message: "",
  details: [],
  confirmLabel: "确认",
  cancelLabel: "取消",
  confirmClass: "blue",
});
const draftStackRef = ref<DraftsPanelExpose | null>(null);
const fallbackPollTimers = new Map<string, number>();
const pollingJobs = new Map<string, string>();
let batchPollTimer: number | null = null;
let batchPollActive = false;
const MAX_SITE_PHOTO_COUNT = 6;
const MAX_SITE_PHOTO_BYTES = 8 * 1024 * 1024;
const recentUndoSeconds = 3 * 24 * 60 * 60;
const staleSourceSnapshotSeconds = 2 * 60 * 60;
const manualRecentTypes = ref<string[]>([]);
const previewDraftKey = ref("");
const pendingFocusNotice = ref<Dict | null>(null);
let workbenchLoadSeq = 0;
let workbenchRefreshTimer: number | null = null;
let sseReconnectTimer: number | null = null;
let activeItemsReconnectTimer: number | null = null;
let authKeepaliveTimer: number | null = null;
let workbenchRetryTimer: number | null = null;
let workbenchRetryAttempt = 0;
const refreshCooldownTimers = new Map<string, number>();
let lastActiveItemsSignature = "";
let activeItemsStreamScope = "";
let appDisposed = false;
let resolveOngoingBindSelection: ((candidate: Dict | null) => void) | null = null;
let jobStreamCoordinator: CrossTabStreamCoordinator<Dict> | null = null;
let activeItemsStreamCoordinator: CrossTabStreamCoordinator<Dict> | null = null;
let authRedirectInProgress = false;
let sseAuthCheckTimer: number | null = null;
let realtimeWarningTimer: number | null = null;

const visibleScopeOptions = computed(() => auth.scopeOptions.length ? auth.scopeOptions : requestableScopes);
const normalizedVisibleScopeOptions = computed<ScopeOption[]>(() => (
  visibleScopeOptions.value.map((item) => ({
    ...item,
    value: normalizeScopeValue(item.value),
  }))
));
const isAdmin = computed(() => String(auth.user?.role || "").toLowerCase() === "admin");
const ownedScopeValues = computed(() => new Set(auth.scopeOptions.map((item) => normalizeScopeValue(item.value))));
const additionalRequestableScopes = computed(() => {
  if (!auth.loggedIn || !auth.scopeOptions.length) return requestableScopes;
  const owned = ownedScopeValues.value;
  return requestableScopes.filter((item) => !owned.has(normalizeScopeValue(item.value)));
});
const permissionPanelRequestableScopes = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? additionalRequestableScopes.value
    : requestableScopes
));
const permissionPanelTitle = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "申请其他楼栋权限"
    : "当前账号暂无门户权限"
));
const permissionPanelDescription = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "选择还需要访问的楼栋或园区，管理员审批后会追加到当前账号。"
    : "请选择需要访问的楼栋或园区，提交后由管理员在门户审批。"
));
const permissionPanelEmptyText = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "当前账号已经拥有全部可申请入口。"
    : "当前没有可申请的楼栋权限。"
));
const headerSubtitle = computed(() => {
  if (isHistoryMemoryPage.value) return "管理工具 · 历史通告记忆导入";
  if (isEngineerMopPage.value) return `${scopeLabel(currentScope.value)} · 工程师 MOP 填写`;
  if (isSignaturePage.value) return "线上签名 · 手机手写保存";
  if (isEventPage.value) return `${scopeLabel(currentScope.value)} · 事件管理`;
  if (authChecking.value) return "功能选择 · 正在检查登录";
  if (!auth.loggedIn) return "功能选择 · 请先登录";
  if (!auth.scopeOptions.length) return "功能选择 · 申请访问权限";
  if (isWorkbench.value) return `${scopeLabel(currentScope.value)} · 通告工作台`;
  return "功能选择 · 请选择功能";
});
const pageStatusText = computed(() => {
  if (signatureLinkMode.value) return "";
  const text = String(syncText.value || "").trim();
  if (!text || ["准备中", "请选择功能", "切换中"].includes(text)) return "";
  if (/^HTTP\s+\d+/i.test(text)) return "服务暂未就绪，页面会在连接恢复后自动刷新。";
  return text;
});
const hasActiveJobs = computed(() => {
  for (const state of jobStates.values()) {
    const phase = String(state?.phase || "");
    if (phase && !terminalPhase(phase)) return true;
  }
  return false;
});
const jobRealtimeUnavailable = computed(() => (
  isWorkbench.value
  && hasActiveJobs.value
  && !(sseConnected.value || sharedJobStreamAvailable.value)
));
const connectionNotice = computed(() => {
  if (!auth.loggedIn) return null;
  if (backendStatus.offline) {
    return {
      tone: "failed",
      text: backendStatus.message || "服务连接异常，页面会保留当前数据。",
      actionLabel: "重新连接",
      action: retryFrontendConnections,
    };
  }
  if (pendingHiddenRefresh.value) {
    return {
      tone: "info",
      text: "有新数据，页面恢复可见后会自动刷新。",
      actionLabel: "立即刷新",
      action: flushPendingHiddenRefresh,
    };
  }
  if (jobRealtimeUnavailable.value && realtimeWarningVisible.value) {
    return {
      tone: "warning",
      text: "任务实时状态正在重连，当前会自动轮询查询。",
      actionLabel: "重连",
      action: retryFrontendConnections,
    };
  }
  return null;
});

watch(jobRealtimeUnavailable, (unavailable) => {
  if (realtimeWarningTimer !== null) {
    window.clearTimeout(realtimeWarningTimer);
    realtimeWarningTimer = null;
  }
  if (!unavailable) {
    realtimeWarningVisible.value = false;
    return;
  }
  realtimeWarningTimer = window.setTimeout(() => {
    realtimeWarningTimer = null;
    if (jobRealtimeUnavailable.value) {
      realtimeWarningVisible.value = true;
    }
  }, 8000);
}, { immediate: true });

const dailyStats = computed(() => dailySummary.value?.stats || {});
const closedSummaryItems = computed(() => {
  const items = Array.isArray(dailySummary.value?.items) ? dailySummary.value.items : [];
  return items.filter((item: Dict) => String(item?.status || "") === "已结束" || Boolean(item?.ended_at));
});
const recentUndoItems = computed(() => [...availableUndoItems.value].sort((a, b) => Number(b.undo_created_at || 0) - Number(a.undo_created_at || 0)));
const liveDailyStats = computed(() => ({
  ...dailyStats.value,
  started: Math.max(0, Number(dailyStats.value.started || 0) + localSummaryAdjustments.started),
  updated: Math.max(0, Number(dailyStats.value.updated || 0) + localSummaryAdjustments.updated),
  ended: Math.max(0, Number(dailyStats.value.ended || 0) + localSummaryAdjustments.ended),
}));
const liveOngoingCount = computed(() => Math.max(0, ongoing.value.length + localSummaryAdjustments.ongoing));
const scopedRecords = computed(() => records.value.filter((record) => recordMatchesCurrentScope(record)));
const manualRecentTypeOptions = computed(() => manualRecentTypes.value
  .map((value) => workTypes.find((item) => item.value === value))
  .filter(Boolean)
  .slice(0, 2) as WorkTypeOption[]);
const manualPrefillTypeValues = computed(() => Array.from(manualPrefillWorkTypes));
const draftSaveText = computed(() => {
  if (!selectedKeys.size && !drafts.size) return "";
  if (draftSaveFailed.value) return "草稿保存失败";
  if (!draftSavedAt.value) return "草稿自动保存";
  return `草稿已保存 ${formatTimeOfDay(draftSavedAt.value)}`;
});
const recordTypeCounts = computed(() => {
  const counts: Record<string, number> = Object.fromEntries([["", 0], ...workTypes.map((item) => [item.value, 0])]);
  counts[""] = scopedRecords.value.length;
  for (const record of scopedRecords.value) {
    const type = record.work_type || "maintenance";
    if (Object.prototype.hasOwnProperty.call(counts, type)) counts[type] += 1;
  }
  return counts;
});
const filteredRecords = computed(() => {
  const query = searchText.value.trim().toLowerCase();
  return scopedRecords.value.filter((record) => {
    if (workType.value && (record.work_type || "maintenance") !== workType.value) return false;
    if (!matchesSpecialtyFilter(specialtyForRecord(record))) return false;
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
  status: recordStatusLabel(record),
  statusTone: recordStatusTone(record),
  selected: selectedKeys.has(recordKey(record)),
  disabled: isRecordOngoing(record),
  disabledReason: "已在进行中，请在右侧更新、结束或删除",
  raw: record,
})));
const selectedDraftRowsAll = computed(() => {
  const keys = Array.from(selectedKeys);
  const pinned = activeDraftKey.value;
  if (pinned && keys.includes(pinned)) {
    keys.splice(keys.indexOf(pinned), 1);
    keys.unshift(pinned);
  }
  return keys.map((key) => {
    const record = draftRecordForKey(key);
    if (!record) return null;
    return {
      key,
      record,
      draft: getDraft(record),
      title: recordCardTitle(record),
    };
  }).filter(Boolean) as Array<{ key: string; record: Dict; draft: Dict; title: string }>;
});
const selectedDraftRows = computed(() => selectedDraftRowsAll.value.filter((row) => matchesSpecialtyFilter(draftSpecialtyForRow(row))));
const typeFilteredOngoing = computed(() => {
  if (ongoingTypeFilter.value !== "current") return ongoing.value;
  if (!workType.value) return ongoing.value;
  return ongoing.value.filter((item) => String(item.work_type || "maintenance") === workType.value);
});
const filteredOngoing = computed(() => (
  typeFilteredOngoing.value.filter((item) => matchesSpecialtyFilter(ongoingSpecialtyForItem(item)))
));
const ongoingCountLabel = computed(() => {
  const baseCount = typeFilteredOngoing.value.length;
  if (filteredOngoing.value.length !== ongoing.value.length || ongoingTypeFilter.value === "current") {
    return `${filteredOngoing.value.length} / ${ongoing.value.length}`;
  }
  return String(baseCount);
});
const ongoingEmptyText = computed(() => {
  if (specialtyFilter.value) {
    return `当前专业下没有进行中通告，共 ${ongoing.value.length} 条`;
  }
  if (ongoingTypeFilter.value === "current" && ongoing.value.length) {
    return `当前类型没有进行中通告，全部类型共 ${ongoing.value.length} 条`;
  }
  return "当前没有进行中通告";
});
const specialtyFilterOptions = computed(() => {
  const values = new Set<string>();
  for (const record of scopedRecords.value) {
    const value = normalizeSpecialtyValue(specialtyForRecord(record));
    if (value) values.add(value);
  }
  for (const row of selectedDraftRowsAll.value) {
    const value = normalizeSpecialtyValue(draftSpecialtyForRow(row));
    if (value) values.add(value);
  }
  for (const item of ongoing.value) {
    const value = normalizeSpecialtyValue(ongoingSpecialtyForItem(item));
    if (value) values.add(value);
  }
  return Array.from(values).sort((a, b) => a.localeCompare(b, "zh-CN"));
});

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function normalizedRecordBuildingCodes(record: Dict): string[] {
  const raw = Array.isArray(record?.building_codes) ? record.building_codes : [];
  const codes: string[] = [];
  for (const item of raw) {
    const code = String(item || "").trim().toUpperCase();
    if (buildingScopeCodes.includes(code) && !codes.includes(code)) codes.push(code);
  }
  return buildingScopeCodes.filter((code) => codes.includes(code));
}

function recordMatchesCurrentScope(record: Dict): boolean {
  const scope = normalizeScopeValue(currentScope.value || "ALL");
  if (scope === "ALL") return true;
  const codes = normalizedRecordBuildingCodes(record);
  if (!codes.length) return true;
  if (scope === "CAMPUS") return codes.length >= 2;
  return codes.length === 1 && codes[0] === scope;
}

function scopeLabel(value: string): string {
  const normalized = normalizeScopeValue(value, "ALL");
  const found = [...visibleScopeOptions.value, { value: "ALL", label: "全部" }].find((item) => normalizeScopeValue(item.value, "") === normalized);
  return found?.label || normalized;
}

function defaultBuildingForCurrentScope(): string {
  const scope = normalizeScopeValue(currentScope.value || "", "");
  if (!scope || scope === "ALL") return "";
  return scopeLabel(scope);
}

function buildingCodesFromText(value: string): string[] {
  const code = normalizeScopeValue(value, "");
  if (buildingScopeCodes.includes(code)) return [code];
  if (code === "CAMPUS") return ["A", "B", "C"];
  return [];
}

function onDraftBuildingChange(draft: Dict): void {
  draft.building_codes = buildingCodesFromText(draft.building || "");
  saveDrafts();
}

function fieldsOf(record: Dict | undefined): Dict {
  return record?.display_fields || {};
}

function cleanDisplayText(value: any): string {
  const text = String(value ?? "").trim();
  if (placeholderFieldValues.has(text)) return "";
  if (/^opt[A-Za-z0-9]{6,}$/.test(text)) return "";
  return text;
}

function normalizeSpecialtyValue(value: any): string {
  return cleanDisplayText(value).replace(/\s+/g, "");
}

function matchesSpecialtyFilter(value: any): boolean {
  const expected = normalizeSpecialtyValue(specialtyFilter.value);
  if (!expected) return true;
  return normalizeSpecialtyValue(value) === expected;
}

function draftSpecialtyForRow(row: { record: Dict; draft: Dict }): string {
  return cleanDisplayText(row.draft?.specialty) || specialtyForRecord(row.record);
}

function ongoingSpecialtyForItem(item: Dict): string {
  const edit = ongoingEdits.get(ongoingLineKey(item));
  return cleanDisplayText(edit?.specialty) || cleanDisplayText(item.specialty);
}

function repairSpecialtyForRecord(record: Dict): string {
  const fields = fieldsOf(record);
  const direct = cleanDisplayText(fields["所属专业"]);
  if (direct) return direct;
  const pushedRaw = String(fields["专业（推送消息用）"] ?? "").trim();
  return repairSpecialtyOptionFallback[pushedRaw] || cleanDisplayText(pushedRaw);
}

function todayInput(hour: number, minute: number): string {
  const d = new Date();
  d.setHours(hour, minute, 0, 0);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function firstRepairField(record: Dict, names: string[]): string {
  const fields = fieldsOf(record);
  for (const name of names) {
    const value = cleanDisplayText(fields[name]);
    if (value) return value;
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
  if (record.manual) {
    return normalize110StationNoticeTitle(
      fieldsOf(record)["手动标题"] || record.title || `手动${workTypeLabel(record.work_type)}通告`,
      buildingForRecord(record),
      record.building_codes || [],
    );
  }
  const f = fieldsOf(record);
  const type = record.work_type || "maintenance";
  const rawTitle = type === "change"
    ? (f["变更简述"] || record.title || "未命名变更事项")
    : type === "repair"
      ? (record.title || f["检修通告名称"] || f["维修名称"] || "未命名检修事项")
      : `EA118机房${f["楼栋"] || ""}${f["维护总项"] || ""}`;
  return normalize110StationNoticeTitle(rawTitle, buildingForRecord(record), record.building_codes || []);
}

function recordCardTitle(record: Dict): string {
  const title = titleForRecord(record);
  if ((record.work_type || "maintenance") !== "maintenance") return title;
  const period = String(fieldsOf(record)["维护周期"] || record.maintenance_cycle || "").trim();
  return period ? `${title}-${period}` : title;
}

function specialtyForRecord(record: Dict): string {
  const f = fieldsOf(record);
  if (record.manual) return cleanDisplayText(f["专业类别"]) || cleanDisplayText(f["专业"]) || cleanDisplayText(f["所属专业"]) || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return cleanDisplayText(f["专业"]);
  if (type === "repair") return repairSpecialtyForRecord(record);
  return cleanDisplayText(f["专业类别"]);
}

function stripNonPlanTitleSuffix(title: string): string {
  return String(title || "").trim().replace(/（非计划性）$|（非计划）$/u, "").trim();
}

function appendNonPlanTitleSuffix(title: string, enabled: boolean): string {
  const value = String(title || "").trim();
  if (!enabled || !value) return value;
  return `${stripNonPlanTitleSuffix(value)}${nonPlanTitleSuffix}`;
}

function manualDraftTitle(draft: Dict, type: string): string {
  const baseTitle = String(draft.title || "").trim();
  const supplement = String(draft.content || "").trim();
  const title = type === "repair"
    ? combineTitleSupplement(baseTitle, supplement)
    : (baseTitle || supplement);
  const titled = type === "maintenance" ? appendNonPlanTitleSuffix(title, Boolean(draft.non_plan)) : title;
  return normalize110StationNoticeTitle(titled, draft.building || "", draft.building_codes || []);
}

function normalize110StationNoticeTitle(title: string, building = "", buildingCodes: unknown[] = []): string {
  const value = String(title || "").trim();
  if (!value) return value;
  const targetPrefix = "EA118-110KV阿里中天变";
  if (value.startsWith(targetPrefix)) return value;
  const codes = new Set((Array.isArray(buildingCodes) ? buildingCodes : []).map((code) => String(code || "").trim().toUpperCase()).filter(Boolean));
  const looksLike110 = codes.has("110")
    || String(building || "").includes("110站")
    || /^EA118\s*(?:机房)?\s*[-－]?\s*110\s*(?:站|KV)?/i.test(value);
  if (!looksLike110) return value;
  return value.replace(/^EA118\s*(?:机房)?\s*[-－]?\s*110\s*(?:站|KV)?\s*/i, targetPrefix).trim() || value;
}

function combineTitleSupplement(title: string, supplement: string): string {
  const base = String(title || "").trim();
  const extra = String(supplement || "").trim();
  if (!base) return extra;
  if (!extra || base.includes(extra)) return base;
  return `${base}${extra}`;
}

function payloadTitleForDraft(record: Dict, draft: Dict, type: string): string {
  if (type === "repair") {
    const baseTitle = String(draft.title || "").trim();
    if (baseTitle) return baseTitle;
    if (!record.manual) return titleForRecord(record);
    return String(draft.content || "").trim();
  }
  return record.manual ? manualDraftTitle(draft, type) : titleForRecord(record);
}

function inferManualNoticeWorkType(draft: Dict, fallback = "maintenance"): string {
  return normalizeWorkType(fallback);
}

function draftWorkType(record: Dict, draft: Dict): string {
  return record.manual
    ? inferManualNoticeWorkType(draft, draft.work_type || record.work_type || "maintenance")
    : String(record.work_type || "maintenance");
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

function draftActionForRecord(record: Dict, draft: Dict): string {
  if (record?.manual) {
    const action = String(draft?.parsed_action || "").toLowerCase();
    if (["start", "update", "end"].includes(action)) return action;
  }
  return sourceActionForRecord(record);
}

function draftActionLabel(record: Dict, draft: Dict): string {
  const action = draftActionForRecord(record, draft);
  if (action === "end") return "结束";
  if (action === "update") return "更新";
  return "开始";
}

function recordStatusLabel(record: Dict): string {
  const key = recordKey(record);
  const job = jobStates.get(key);
  if (job?.phase && !terminalPhase(job.phase)) return "提交中";
  if (job?.phase === "failed") return "提交失败";
  if (job?.phase === "success") return "已提交";
  if (selectedKeys.has(key)) return "已加入待发起";
  if (isRecordOngoing(record)) return "已在右侧进行中，请在右侧处理";
  const progress = sourceProgressForRecord(record);
  if (!progress || progress === "未开始") return "未开始，可发起";
  return `${progress}，可更新`;
}

function recordStatusTone(record: Dict): string {
  const key = recordKey(record);
  const job = jobStates.get(key);
  if (job?.phase && !terminalPhase(job.phase)) return "ongoing";
  if (job?.phase === "failed") return "failed";
  if (job?.phase === "success") return "queued";
  if (selectedKeys.has(key)) return "queued";
  if (isRecordOngoing(record)) return "ongoing";
  const progress = sourceProgressForRecord(record);
  if (!progress || progress === "未开始") return "pending";
  return "update";
}

function sourceWorkTypeForRecord(record: Dict): string {
  return String(record.source_work_type || record.converted_from_work_type || record.original_work_type || record.work_type || "maintenance").trim();
}

function canToggleWorkTypeOverride(record: Dict): boolean {
  if (!record || record.manual) return false;
  const sourceType = sourceWorkTypeForRecord(record);
  const displayType = String(record.work_type || "maintenance");
  return sourceType === "maintenance" && (displayType === "maintenance" || displayType === "change");
}

function isConvertedMaintenanceChange(record: Dict, draft?: Dict): boolean {
  const displayType = draft ? draftWorkType(record, draft) : String(record?.work_type || "maintenance");
  return sourceWorkTypeForRecord(record) === "maintenance" && displayType === "change";
}

function syncMaintenanceTargetValue(record: Dict, draft: Dict): boolean {
  if (!isConvertedMaintenanceChange(record, draft)) return false;
  if (Object.prototype.hasOwnProperty.call(draft, "sync_maintenance_target")) {
    return draft.sync_maintenance_target !== false;
  }
  return true;
}

function targetOverrideWorkType(record: Dict): WorkTypeValue {
  return String(record.work_type || "maintenance") === "change" && sourceWorkTypeForRecord(record) === "maintenance"
    ? "maintenance"
    : "change";
}

function workTypeOverrideButtonLabel(record: Dict): string {
  return targetOverrideWorkType(record) === "change" ? "转为变更" : "转为维保";
}

function targetRecordIdForRecord(record: Dict): string {
  const summary = record?.work_summary || {};
  return String(summary.target_record_id || summary.feishu_record_id || summary.raw_record_id || record?.target_record_id || record?.feishu_record_id || record?.raw_record_id || "").trim();
}

function targetRecordIdForOngoing(item: Dict): string {
  return String(item.target_record_id || item.feishu_record_id || item.raw_record_id || "").trim();
}

function ongoingLineKey(item: Dict): string {
  return String(item.active_item_id || item.identity_key || item.target_record_id || item.feishu_record_id || item.raw_record_id || item.source_record_id || item.record_id || "").trim();
}

function isOngoingExpanded(item: Dict): boolean {
  const key = ongoingLineKey(item);
  return Boolean(key && activeOngoingKey.value === key);
}

function expandOngoingCard(item: Dict): void {
  const key = ongoingLineKey(item);
  if (key) activeOngoingKey.value = key;
}

function toggleOngoingCard(item: Dict): void {
  const key = ongoingLineKey(item);
  if (!key) return;
  activeOngoingKey.value = activeOngoingKey.value === key ? "" : key;
}

function closedLineKey(item: Dict): string {
  return `closed:${item.key || item.active_item_id || item.target_record_id || item.feishu_record_id || item.title || ""}`;
}

function undoLineKey(item: Dict): string {
  return `undo:${item.undo_id || item.active_item_id || item.target_record_id || item.record_id || item.key || item.title || ""}`;
}

function sourceRecordIdForOngoing(item: Dict, targetRecordId = ""): string {
  const source = String(item.source_record_id || "").trim();
  if (source && source !== targetRecordId) return source;
  return "";
}

function ongoingNeedsBinding(item: Dict): boolean {
  const edit = ongoingEdits.get(ongoingLineKey(item));
  const editedTarget = String(edit?.target_record_id || "").trim();
  if (editedTarget) return false;
  return String(item?.binding_status || "") === "needs_binding" || !targetRecordIdForOngoing(item);
}

function ongoingTimeRange(item: Dict): { start: string; end: string } {
  const timeText = String(item.time_str || item.time || "").trim();
  const parts = timeText.split(/~|至|到/).map((part) => part.trim()).filter(Boolean);
  const isRepair = (item.work_type || "maintenance") === "repair";
  const start =
    toDatetimeLocal(isRepair ? (item.expected_time || item.start_time) : item.start_time) ||
    toDatetimeLocal(parts[0] || "") ||
    todayInput(isRepair ? 23 : 9, isRepair ? 50 : 30);
  const end =
    toDatetimeLocal(isRepair ? (item.fault_time || item.end_time) : item.end_time) ||
    toDatetimeLocal(parts[1] || "") ||
    todayInput(isRepair ? 0 : 18, isRepair ? 0 : 30);
  return { start, end };
}

function maintenanceCycleForRecord(record: Dict): string {
  if ((record.work_type || "maintenance") !== "maintenance") return "";
  return normalizeDraftSignatureText(String(fieldsOf(record)["维护周期"] || record.maintenance_cycle || ""));
}

function maintenanceCycleForOngoing(item: Dict): string {
  if ((item.work_type || "maintenance") !== "maintenance") return "";
  return normalizeDraftSignatureText(String(item.maintenance_cycle || fieldsOf(item)["维护周期"] || ""));
}

function isRecordOngoing(record: Dict): boolean {
  const titleCandidates = [
    titleForRecord(record),
    recordCardTitle(record),
  ].map((value) => normalizeDraftSignatureText(value)).filter(Boolean);
  const sourceId = record.source_record_id || record.record_id;
  const targetId = targetRecordIdForRecord(record);
  const recordCycle = maintenanceCycleForRecord(record);
  return ongoing.value.some((item) => {
    if ((item.work_type || "maintenance") !== (record.work_type || "maintenance")) return false;
    const itemSource = String(item.source_record_id || "").trim();
    const itemTarget = targetRecordIdForOngoing(item);
    const itemRecordId = String(item.record_id || "").trim();
    if (sourceId && (itemSource === sourceId || itemRecordId === sourceId)) return true;
    if (targetId && (itemTarget === targetId || itemRecordId === targetId)) return true;
    const ongoingTitles = [
      ongoingTitle(item),
      item.title || "",
      item.content || "",
    ].map((value) => normalizeDraftSignatureText(value)).filter(Boolean);
    const itemCycle = maintenanceCycleForOngoing(item);
    if ((record.work_type || "maintenance") === "maintenance" && (recordCycle || itemCycle) && recordCycle !== itemCycle) {
      return false;
    }
    return titleCandidates.some((title) => ongoingTitles.includes(title));
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
  const building = defaultBuildingForCurrentScope();
  const normalizedType = normalizeWorkType(type);
  return {
    manual: true,
    work_type: normalizedType,
    notice_type: normalizedType === "power" ? "上电通告" : "",
    title: "",
    building,
    building_codes: buildingCodesFromText(building),
    specialty: "",
    level: normalizedType === "change" ? "I3" : "",
    maintenance_cycle: "",
    non_plan: false,
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
    spare_parts: "",
    device: "",
    cabinet: "",
    quantity: "",
  };
}

function manualRecordFromDraft(key: string, draft: Dict): Dict {
  const type = inferManualNoticeWorkType(draft, draft.work_type || "maintenance");
  if (draft.work_type !== type) draft.work_type = type;
  const title = manualDraftTitle(draft, type);
  const noticeTypeMap: Record<string, string> = {
    maintenance: "维保通告",
    change: "设备变更",
    repair: "设备检修",
    power: draft.notice_type === "下电通告" ? "下电通告" : "上电通告",
    polling: "设备轮巡",
    adjust: "设备调整",
  };
  const buildingCodes = Array.isArray(draft.building_codes) && draft.building_codes.length
    ? draft.building_codes
    : buildingCodesFromText(draft.building || "");
  return {
    manual: true,
    manual_key: key,
    record_id: key,
    source_record_id: draft.source_record_id || "",
    work_type: type,
    notice_type: noticeTypeMap[type] || "维保通告",
    title: title || `手动${workTypeLabel(type)}通告`,
    display_fields: {
      "手动标题": title,
      "楼栋": draft.building || "",
      "变更楼栋": draft.building || "",
      "所属数据中心/楼栋-使用": draft.building || "",
      "专业类别": draft.specialty || "",
      "专业": draft.specialty || "",
      "所属专业": draft.specialty || "",
      "维护周期": draft.maintenance_cycle || "",
      "非计划性": draft.non_plan ? "是" : "",
      "设备": draft.device || "",
      "柜号": draft.cabinet || "",
      "数量": draft.quantity || "",
      "备件更换情况": draft.spare_parts || "",
    },
    target_record_id: draft.target_record_id || draft.feishu_record_id || draft.raw_record_id || "",
    building_codes: buildingCodes,
  };
}

function onManualDraftTypeChange(draft: Dict): void {
  const type = normalizeWorkType(draft.work_type);
  draft.work_type = type;
  if (type === "change" && !draft.level) draft.level = "I3";
  if (type === "power" && !["上电通告", "下电通告"].includes(String(draft.notice_type || ""))) {
    draft.notice_type = "上电通告";
  }
  if (type !== "maintenance") draft.non_plan = false;
  saveDrafts();
}

function repairDraftDefaults(record: Dict): Dict {
  const memory = record.memory || {};
  return {
    start_time: todayInput(23, 50),
    end_time: toDatetimeLocal(firstRepairField(record, ["故障发生时间", "发现故障时间"])) || "",
    title: memory.title || titleForRecord(record),
    location: memory.location || "",
    content: memory.content || firstRepairField(record, ["标题/补充内容", "标题补充内容"]),
    level: memory.level || levelForRecord(record),
    specialty: cleanDisplayText(memory.specialty) || specialtyForRecord(record),
    reason: memory.reason || firstRepairField(record, ["故障原因", "故障维修原因"]),
    impact: memory.impact || "",
    progress: memory.progress || "",
    repair_device: memory.repair_device || repairDeviceText(record),
    repair_fault: memory.repair_fault || firstRepairField(record, ["维修故障", "故障维修原因"]),
    fault_type: memory.fault_type || firstRepairField(record, ["故障类型"]) || "设备故障",
    repair_mode: memory.repair_mode || firstRepairField(record, ["维修方式", "维修方", "供应商名称"]),
    discovery: cleanDisplayText(memory.discovery) || firstRepairField(record, ["对应来源"]),
    symptom: memory.symptom || firstRepairField(record, ["故障发生现象描述", "故障现象"]),
    solution: memory.solution || firstRepairField(record, ["解决方案", "维修方案", "后续整改措施"]),
    spare_parts: memory.spare_parts || firstRepairField(record, ["备件更换情况", "备件使用情况"]),
  };
}

function rememberedZhihang(memory: Dict): Dict {
  const rememberedId = String(memory.zhihang_record_id || "").trim();
  if (!rememberedId) return {};
  const item = zhihangRecords.value.find((record) => record.record_id === rememberedId);
  if (!item) return {};
  return {
    zhihang_involved: true,
    zhihang_record_id: rememberedId,
    zhihang_title: item.title || memory.zhihang_title || "",
    zhihang_progress: item.progress || memory.zhihang_progress || "",
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
      const zhihangMemory = isChange ? rememberedZhihang(memory) : {};
      drafts.set(key, {
        title: titleForRecord(record),
        specialty: cleanDisplayText(memory.specialty) || specialtyForRecord(record),
        level: memory.level || levelForRecord(record) || (isChange ? "I3" : ""),
        maintenance_cycle: f["维护周期"] || "",
        start_time: isChange
          ? (toDatetimeLocal(f["变更开始日期（阿里）"] || f["计划开始日期（阿里）"] || f["计划开始"] || f["计划开始时间"] || f["计划延迟开始日期"]) || todayInput(9, 30))
          : todayInput(9, 30),
        end_time: isChange
          ? (toDatetimeLocal(f["变更结束日期（阿里）"] || f["计划结束日期（阿里）"] || f["计划结束"] || f["计划结束时间"] || f["计划延迟结束日期"]) || todayInput(18, 30))
          : todayInput(18, 30),
        location: memory.location || "",
        content: isChange ? (memory.content || titleForRecord(record)) : (memory.content || ""),
        reason: memory.reason || "",
        impact: memory.impact || defaults.impact,
        progress: memory.progress || defaults.progress,
        sync_maintenance_target: sourceWorkTypeForRecord(record) === "maintenance" && isChange,
        paired_maintenance_target_record_id: record.paired_maintenance_target_record_id || "",
        paired_maintenance_original_title: record.paired_maintenance_original_title || titleForRecord(record),
        paired_maintenance_actual_start_time: record.paired_maintenance_actual_start_time || "",
        zhihang_involved: Boolean(zhihangMemory.zhihang_involved),
        zhihang_record_id: zhihangMemory.zhihang_record_id || "",
        zhihang_title: zhihangMemory.zhihang_title || "",
        zhihang_progress: zhihangMemory.zhihang_progress || "",
      });
    }
    saveDrafts();
  }
  return drafts.get(key) || {};
}

function currentOpenId(): string {
  return String(auth.user?.open_id || auth.user?.openid || "anonymous").trim() || "anonymous";
}

function currentDraftScope(): string {
  return currentScope.value || "ALL";
}

function loadManualRecentTypes(): void {
  manualRecentTypes.value = loadManualRecentTypesFromStorage(currentOpenId(), currentDraftScope());
}

function saveManualRecentType(type: string): void {
  manualRecentTypes.value = saveManualRecentTypeToStorage(
    currentOpenId(),
    currentDraftScope(),
    type,
    manualRecentTypes.value,
  );
}

function loadManualTemplateMemory(type: string): Dict | null {
  return loadManualTemplateMemoryFromStorage(currentOpenId(), currentDraftScope(), type);
}

function saveManualTemplateMemory(type: string, draft: Dict): void {
  saveManualTemplateMemoryToStorage(currentOpenId(), currentDraftScope(), type, draft);
}

function loadDrafts(): void {
  loadManualRecentTypes();
  const payload = loadDraftStorage(currentOpenId(), currentDraftScope());
  selectedKeys.clear();
  for (const key of payload.selected) selectedKeys.add(key);
  drafts.clear();
  for (const [key, value] of Object.entries(payload.drafts)) drafts.set(key, value);
}

function saveDrafts(): void {
  const payload: Record<string, Dict> = {};
  for (const [key, value] of drafts.entries()) payload[key] = value;
  if (saveDraftStorage(currentOpenId(), currentDraftScope(), Array.from(selectedKeys), payload)) {
    draftSaveFailed.value = false;
    draftSavedAt.value = Date.now();
  } else {
    draftSaveFailed.value = true;
    syncText.value = "草稿保存失败，请减少待发起通告数量后重试";
  }
}

function setDraftField(draft: Dict, field: string, value: unknown): void {
  draft[field] = value;
  saveDrafts();
}

function formatTimeOfDay(timestamp: number): string {
  if (!timestamp) return "";
  const date = new Date(timestamp);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function startRefreshCooldown(key: keyof typeof refreshCooldown): void {
  refreshCooldown[key] = true;
  const existing = refreshCooldownTimers.get(key);
  if (existing) window.clearTimeout(existing);
  const timer = window.setTimeout(() => {
    refreshCooldown[key] = false;
    refreshCooldownTimers.delete(key);
  }, manualRefreshCooldownMs);
  refreshCooldownTimers.set(key, timer);
}

function refreshButtonTitle(key: keyof typeof refreshCooldown): string {
  if (refreshCooldown[key]) return "刚刷新过，稍后再试，避免重复读取数据。";
  if (key === "workbench" && loading.value) return "正在刷新当前页面。";
  if (key === "repair" && repairRefreshing.value) return "正在读取最新检修数据，完成后全楼可见。";
  if (key === "change" && changeRefreshing.value) return "正在读取最新变更数据，完成后全楼可见。";
  if (key === "event" && eventRefreshing.value) return "正在读取最新事件数据，完成后全楼可见。";
  if (key === "workbench") return "重新读取当前楼栋页面数据。";
  if (key === "repair") return "读取最新检修数据，完成后全楼可见。";
  if (key === "change") return "读取最新阿里变更和智航变更数据，完成后全楼可见。";
  return "读取最新事件数据，刷新当前月事件列表。";
}

function clearWorkbenchRetry(): void {
  if (workbenchRetryTimer !== null) {
    window.clearTimeout(workbenchRetryTimer);
    workbenchRetryTimer = null;
  }
  workbenchRetryAttempt = 0;
}

function scheduleWorkbenchRetry(reason = "加载失败"): void {
  if (appDisposed || !auth.loggedIn || !isWorkbench.value || !currentScope.value) return;
  if (workbenchRetryTimer !== null) return;
  if (!pageVisible.value) {
    pendingHiddenRefresh.value = true;
    syncText.value = "加载失败，页面恢复可见后自动重试";
    return;
  }
  const index = Math.min(workbenchRetryAttempt, workbenchRetryDelaysMs.length - 1);
  const delay = workbenchRetryDelaysMs[index];
  workbenchRetryAttempt += 1;
  syncText.value = `${reason}，${Math.round(delay / 1000)}秒后重试`;
  workbenchRetryTimer = window.setTimeout(() => {
    workbenchRetryTimer = null;
    if (appDisposed || !auth.loggedIn || !isWorkbench.value) return;
    void loadWorkbench();
  }, delay);
}

function clearAuthKeepalive(): void {
  if (authKeepaliveTimer !== null) {
    window.clearTimeout(authKeepaliveTimer);
    authKeepaliveTimer = null;
  }
}

function closeDirectJobEventSource(): void {
  if (eventSource.value) eventSource.value.close();
  eventSource.value = null;
  if (sseReconnectTimer !== null) {
    window.clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }
}

function stopJobSse(): void {
  if (jobStreamCoordinator) {
    jobStreamCoordinator.stop();
    jobStreamCoordinator = null;
  }
  closeDirectJobEventSource();
  sseConnected.value = false;
  sharedJobStreamAvailable.value = false;
}

function stopRealtimeConnections(): void {
  stopJobSse();
  stopActiveItemsSse();
  if (sseAuthCheckTimer !== null) {
    window.clearTimeout(sseAuthCheckTimer);
    sseAuthCheckTimer = null;
  }
}

function pauseJobStatusChecksForHiddenPage(): void {
  for (const timer of fallbackPollTimers.values()) window.clearTimeout(timer);
  fallbackPollTimers.clear();
  clearOngoingEdits();
  if (batchPollTimer !== null) {
    window.clearTimeout(batchPollTimer);
    batchPollTimer = null;
  }
}

function resumePendingJobStatusChecks(): void {
  for (const [key, state] of jobStates.entries()) {
    const jobId = String(state.job_id || "").trim();
    if (jobId && !terminalPhase(state.phase)) {
      watchJob(jobId, key);
    }
  }
  if (pollingJobs.size > 0) scheduleBatchJobPoll(0);
}

function markAuthExpired(message = "登录已过期，请重新扫码登录。"): void {
  auth.loggedIn = false;
  auth.user = {};
  auth.scopeOptions = [];
  clearAuthKeepalive();
  clearWorkbenchRetry();
  stopRealtimeConnections();
  syncText.value = message;
}

function currentLoginUrl(): string {
  const next = `${window.location.pathname}${window.location.search}`;
  return `/api/auth/login?next=${encodeURIComponent(next || "/")}`;
}

function shouldSuppressAuthRedirect(): boolean {
  if (!isSignaturePage.value) return false;
  const params = new URLSearchParams(window.location.search);
  return Boolean(params.get("record_id") || params.get("temporary_id"));
}

function currentRouteNeedsAuth(): boolean {
  if (shouldSuppressAuthRedirect()) return false;
  const path = window.location.pathname.replace(/\/$/, "") || "/";
  const params = new URLSearchParams(window.location.search);
  if (path !== "/") return true;
  return Boolean(params.get("scope") || params.get("mode") || params.get("work_type"));
}

function redirectToLogin(loginUrl = ""): void {
  if (authRedirectInProgress || shouldSuppressAuthRedirect()) return;
  authRedirectInProgress = true;
  const target = String(loginUrl || auth.loginUrl || currentLoginUrl()).trim() || currentLoginUrl();
  window.location.assign(target);
}

function handleSseAuthError(event: Event): boolean {
  const data = String((event as MessageEvent).data || "").trim();
  if (!data) return false;
  try {
    const payload = JSON.parse(data || "{}");
    if (!payload?.auth_required) return false;
    markAuthExpired(String(payload.error || "登录已过期，请重新扫码登录。"));
    redirectToLogin(String(payload.login_url || payload.loginUrl || ""));
    return true;
  } catch {
    return false;
  }
}

function scheduleSseAuthCheck(): void {
  if (appDisposed || authRedirectInProgress || shouldSuppressAuthRedirect()) return;
  if (sseAuthCheckTimer !== null) return;
  sseAuthCheckTimer = window.setTimeout(() => {
    sseAuthCheckTimer = null;
    if (appDisposed || authRedirectInProgress) return;
    void loadAuthStatus({ silent: true }).catch(() => null);
  }, 250);
}

function handleGlobalAuthExpired(event: Event): void {
  const detail = (event as CustomEvent<{ message?: string; login_url?: string; loginUrl?: string }>).detail || {};
  markAuthExpired(detail.message || "登录已过期，请重新扫码登录。");
  redirectToLogin(String(detail.login_url || detail.loginUrl || ""));
}

const api = (path: string, options: RequestInit = {}): Promise<Dict> =>
  requestJson(path, options, {
    onOnline: () => {
      backendStatus.offline = false;
      backendStatus.message = "";
    },
    onOffline: (message) => {
      backendStatus.offline = true;
      backendStatus.lastErrorAt = Date.now();
      backendStatus.message = message;
    },
    onAuthExpired: (message) => {
      markAuthExpired(message);
      redirectToLogin();
    },
    onServerError: (message) => {
      backendStatus.offline = true;
      backendStatus.lastErrorAt = Date.now();
      backendStatus.message = message;
    },
  });

function scheduleAuthKeepalive(delayMs = authKeepaliveMs): void {
  clearAuthKeepalive();
  if (!auth.loggedIn || appDisposed) return;
  authKeepaliveTimer = window.setTimeout(async () => {
    authKeepaliveTimer = null;
    if (appDisposed || !auth.loggedIn) return;
    try {
      await loadAuthStatus({ silent: true });
    } catch {
      if (!appDisposed && auth.loggedIn) {
        authKeepaliveTimer = window.setTimeout(() => {
          authKeepaliveTimer = null;
          void loadAuthStatus({ silent: true }).catch(() => scheduleAuthKeepalive(authKeepaliveRetryMs));
        }, authKeepaliveRetryMs);
      }
    }
  }, delayMs);
}

async function loadAuthStatus(options: { silent?: boolean } = {}): Promise<void> {
  if (!options.silent) authChecking.value = true;
  const wasLoggedIn = auth.loggedIn;
  try {
    const data = await api(`/api/auth/status?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    const nextLoggedIn = Boolean(data.logged_in);
    auth.loggedIn = nextLoggedIn;
    auth.user = data.user || {};
    auth.scopeOptions = data.scope_options || [];
    auth.loginUrl = data.login_url || "/api/auth/login";
    if (nextLoggedIn) {
      scheduleAuthKeepalive();
    } else {
      clearAuthKeepalive();
      if ((wasLoggedIn || currentRouteNeedsAuth()) && !authRedirectInProgress && !shouldSuppressAuthRedirect()) {
        redirectToLogin(String(auth.loginUrl || ""));
      }
    }
  } catch (error: any) {
    if (!options.silent) {
      syncText.value = error?.message || "登录状态检查失败";
    }
    throw error;
  } finally {
    if (!options.silent) authChecking.value = false;
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
    permissionRequest.status = request.status || "";
    permissionRequest.rejectReason = request.reject_reason || "";
    if (permissionRequest.requestId) {
      permissionRequest.message = permissionRequest.status === "rejected"
        ? "最近一次申请未通过，可调整后重新提交。"
        : "已恢复待审批申请，请等待管理员处理。";
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
  const requestSeq = ++workbenchLoadSeq;
  const requestScope = currentScope.value;
  loading.value = true;
  try {
    const data = await api(`/api/workbench?scope=${encodeURIComponent(requestScope)}`);
    if (requestSeq !== workbenchLoadSeq || requestScope !== currentScope.value) return;
    records.value = data.records || [];
    ongoing.value = Array.isArray(data.ongoing) ? data.ongoing : [];
    pruneOngoingExpansion();
    zhihangRecords.value = data.zhihang_change_records || [];
    dailySummary.value = data.daily_summary || { date: "", items: [], stats: {} };
    resetLocalSummaryAdjustments();
    defaults.impact = data.defaults?.impact || defaults.impact;
    defaults.progress = data.defaults?.progress || defaults.progress;
    if (!userSelectedWorkType.value && workType.value) {
      workType.value = resolveInitialWorkType(data.default_work_type || workType.value);
    }
    syncText.value = workbenchSyncText(data);
    clearWorkbenchRetry();
    await loadAvailableUndos(requestScope, requestSeq);
    focusPendingNoticeResult();
    pruneSelection();
  } catch (error: any) {
    if (requestSeq !== workbenchLoadSeq || requestScope !== currentScope.value) return;
    const message = error?.message || "加载失败";
    syncText.value = message;
    scheduleWorkbenchRetry(message);
  } finally {
    if (requestSeq === workbenchLoadSeq && requestScope === currentScope.value) {
      loading.value = false;
    }
  }
}

function workbenchSyncText(data: Dict): string {
  if (data.source_snapshot_ready === false) return "正在准备数据";
  const ts = Number(data.last_loaded_ts || 0);
  if (ts > 0) {
    const ageSeconds = Math.max(0, Date.now() / 1000 - ts);
    if (ageSeconds >= staleSourceSnapshotSeconds) {
      const loadedAt = String(data.last_loaded_at || "").trim();
      return loadedAt ? `显示上次成功数据：${loadedAt}` : "显示上次成功数据";
    }
  }
  return "数据已就绪";
}

async function manualRefreshWorkbench(): Promise<void> {
  if (loading.value || refreshCooldown.workbench) return;
  refreshMenuOpen.value = false;
  startRefreshCooldown("workbench");
  clearWorkbenchRetry();
  syncText.value = "正在刷新本页";
  await loadWorkbench();
  if (workbenchRetryTimer === null) {
    syncText.value = "已刷新，页面已更新";
  }
}

async function loadAvailableUndos(scope = currentScope.value, requestSeq = workbenchLoadSeq): Promise<void> {
  if (!scope) {
    availableUndoItems.value = [];
    return;
  }
  try {
    const data = await api(`/api/notice-undo/available?scope=${encodeURIComponent(scope)}&since_seconds=${recentUndoSeconds}`);
    if (requestSeq !== workbenchLoadSeq || scope !== currentScope.value) return;
    availableUndoItems.value = Array.isArray(data.items) ? data.items : [];
  } catch {
    if (requestSeq === workbenchLoadSeq && scope === currentScope.value) {
      availableUndoItems.value = [];
    }
  }
}

function resolveInitialWorkType(preferred: string): WorkTypeFilterValue {
  const preferredType = normalizeWorkTypeFilter(preferred);
  if (!preferredType) return "";
  if (recordTypeCounts.value[preferredType] > 0) return preferredType;
  const fallback = workTypes.find((item) => recordTypeCounts.value[item.value] > 0);
  return fallback?.value || preferredType;
}

function selectWorkType(value: string): void {
  workType.value = normalizeWorkTypeFilter(value);
  userSelectedWorkType.value = true;
  ongoingTypeFilter.value = "all";
  updateWorkbenchUrl(workType.value);
}

function clearWorkbenchFilters(): void {
  searchText.value = "";
  specialtyFilter.value = "";
}

async function toggleWorkTypeOverride(record: Dict): Promise<void> {
  if (!canToggleWorkTypeOverride(record)) return;
  const oldKey = recordKey(record);
  if (typeOverrideBusyKey.value) return;
  const targetType = targetOverrideWorkType(record);
  typeOverrideBusyKey.value = oldKey;
  try {
    await api("/api/notice-work-type-override", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        record_id: record.record_id || record.source_record_id || "",
        source_work_type: sourceWorkTypeForRecord(record),
        target_work_type: targetType,
      }),
    });
    selectedKeys.delete(oldKey);
    drafts.delete(oldKey);
    activeDraftKey.value = "";
    workType.value = targetType;
    userSelectedWorkType.value = true;
    syncText.value = targetType === "change" ? "已转为变更通告" : "已转回维保通告";
    await loadWorkbench();
    const nextRecord = records.value.find((item) => {
      return String(item.record_id || "") === String(record.record_id || "")
        && String(item.work_type || "maintenance") === targetType;
    });
    if (nextRecord) {
      const nextKey = recordKey(nextRecord);
      selectedKeys.add(nextKey);
      activeDraftKey.value = nextKey;
      nextTick(() => {
        document.querySelector(".draft-card.active")?.scrollIntoView({ block: "center", behavior: "smooth" });
      });
    }
  } catch (error: any) {
    syncText.value = error?.message || "通告类型切换失败";
  } finally {
    if (typeOverrideBusyKey.value === oldKey) typeOverrideBusyKey.value = "";
  }
}

function focusPendingNoticeResult(): void {
  const pending = pendingFocusNotice.value;
  if (!pending) return;
  const pendingTitle = normalizeDraftSignatureText(String(pending.title || ""));
  const found = ongoing.value.find((item) => {
    if (pending.work_type && (item.work_type || "maintenance") !== pending.work_type) return false;
    const target = String(item.target_record_id || item.feishu_record_id || item.raw_record_id || item.record_id || "").trim();
    const source = String(item.source_record_id || "").trim();
    if (pending.target_record_id && target && target === pending.target_record_id) return true;
    if (pending.source_record_id && source && source === pending.source_record_id) return true;
    return Boolean(pendingTitle && normalizeDraftSignatureText(ongoingTitle(item)) === pendingTitle);
  });
  if (!found) return;
  const key = ongoingLineKey(found);
  if (!key) return;
  activeOngoingKey.value = key;
  pendingFocusNotice.value = null;
  syncText.value = "已定位到右侧进行中通告";
  nextTick(() => {
    document.querySelector(".ongoing-card.active")?.scrollIntoView({ block: "center", behavior: "smooth" });
  });
}

function pruneSelection(): void {
  const valid = new Set(records.value.map(recordKey));
  for (const key of Array.from(selectedKeys)) {
    if (!valid.has(key) && !isManualKey(key)) {
      selectedKeys.delete(key);
      continue;
    }
    const record = records.value.find((item) => recordKey(item) === key);
    if (record && isRecordOngoing(record)) selectedKeys.delete(key);
  }
  saveDrafts();
  pruneRuntimeState();
}

function resetLocalSummaryAdjustments(): void {
  localSummaryAdjustments.started = 0;
  localSummaryAdjustments.updated = 0;
  localSummaryAdjustments.ended = 0;
  localSummaryAdjustments.ongoing = 0;
}

function bumpLocalSummary(field: keyof typeof localSummaryAdjustments, delta = 1): void {
  localSummaryAdjustments[field] += delta;
}

function removeOngoingLine(key: string): boolean {
  if (!key) return false;
  const before = ongoing.value.length;
  ongoing.value = ongoing.value.filter((item) => ongoingLineKey(item) !== key);
  deleteOngoingEdit(key);
  if (activeOngoingKey.value === key) activeOngoingKey.value = "";
  return ongoing.value.length !== before;
}

function pruneOngoingExpansion(): void {
  if (!activeOngoingKey.value) return;
  const exists = ongoing.value.some((item) => ongoingLineKey(item) === activeOngoingKey.value);
  if (!exists) activeOngoingKey.value = "";
}

function activeLineKeys(): Set<string> {
  const keys = new Set<string>(Array.from(selectedKeys));
  for (const item of ongoing.value) {
    const key = ongoingLineKey(item);
    if (key) keys.add(key);
  }
  for (const item of closedSummaryItems.value) {
    if (item.undo_available) keys.add(undoLineKey(item));
  }
  for (const item of recentUndoItems.value) {
    keys.add(undoLineKey(item));
  }
  return keys;
}

function clearFallbackPoll(key: string): void {
  const timer = fallbackPollTimers.get(key);
  if (timer) window.clearTimeout(timer);
  fallbackPollTimers.delete(key);
  pollingJobs.delete(key);
}

function scheduleWorkbenchReload(delay = 350): void {
  if (appDisposed) return;
  if (workbenchRefreshTimer !== null) return;
  workbenchRefreshTimer = window.setTimeout(() => {
    workbenchRefreshTimer = null;
    if (appDisposed) return;
    if (!pageVisible.value) {
      pendingHiddenRefresh.value = true;
      activeItemsUpdatePending.value = true;
      return;
    }
    if (isUserEditing()) {
      activeItemsUpdatePending.value = true;
      syncText.value = "有更新，完成输入后自动刷新";
      scheduleWorkbenchReload(3000);
      return;
    }
    activeItemsUpdatePending.value = false;
    void loadWorkbench();
  }, delay);
}

function isUserEditing(): boolean {
  const element = document.activeElement as HTMLElement | null;
  if (!element) return false;
  const tag = element.tagName.toLowerCase();
  if (["input", "textarea", "select"].includes(tag)) return true;
  return Boolean(element.isContentEditable);
}

function flushPendingHiddenRefresh(): void {
  if (!isWorkbench.value || !currentScope.value) {
    pendingHiddenRefresh.value = false;
    activeItemsUpdatePending.value = false;
    return;
  }
  pendingHiddenRefresh.value = false;
  activeItemsUpdatePending.value = false;
  void loadWorkbench();
}

function retryFrontendConnections(): void {
  backendStatus.offline = false;
  backendStatus.message = "";
  clearWorkbenchRetry();
  if (auth.loggedIn) {
    scheduleAuthKeepalive(1000);
    startJobSse();
    if (isWorkbench.value) startActiveItemsSse();
  }
  if (isWorkbench.value && currentScope.value) {
    void loadWorkbench();
  } else {
    void loadAuthStatus({ silent: true }).catch(() => null);
  }
}

function handleVisibilityChange(): void {
  pageVisible.value = !document.hidden;
  if (!pageVisible.value) {
    clearAuthKeepalive();
    pauseJobStatusChecksForHiddenPage();
    stopRealtimeConnections();
    return;
  }
  if (auth.loggedIn) {
    scheduleAuthKeepalive(1000);
    if (!isJobStreamStarted()) startJobSse();
    if (isWorkbench.value && !isActiveItemsStreamStarted()) startActiveItemsSse();
    resumePendingJobStatusChecks();
  }
  if (pendingHiddenRefresh.value) {
    flushPendingHiddenRefresh();
  }
}

function pruneRuntimeState(): void {
  const visibleKeys = activeLineKeys();
  const now = Date.now();
  const staleBefore = now - 30 * 60 * 1000;
  for (const key of Array.from(ongoingEdits.keys())) {
    if (!visibleKeys.has(key)) deleteOngoingEdit(key);
  }
  for (const [key, state] of Array.from(jobStates.entries())) {
    const updatedAt = Date.parse(String(state.updated_at || ""));
    const stale = Number.isFinite(updatedAt) && updatedAt < staleBefore;
    if (!visibleKeys.has(key) && (terminalPhase(state.phase) || stale)) {
      jobStates.delete(key);
      clearFallbackPoll(key);
    }
  }
}

function enterScope(scope: string, nextWorkType = ""): void {
  switchScope(scope, nextWorkType);
}

function enterEventManagement(scope: string): void {
  const nextScope = normalizeScopeValue(scope, "ALL");
  if (!nextScope) return;
  stopActiveItemsSse();
  clearWorkbenchRetry();
  if (currentScope.value) saveDrafts();
  currentScope.value = nextScope;
  isWorkbench.value = false;
  isEventPage.value = true;
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  selectedKeys.clear();
  clearOngoingEdits();
  records.value = [];
  ongoing.value = [];
  zhihangRecords.value = [];
  dailySummary.value = { date: "", items: [], stats: {} };
  availableUndoItems.value = [];
  resetLocalSummaryAdjustments();
  syncText.value = "正在读取事件数据";
  const url = new URL(window.location.href);
  url.searchParams.set("scope", currentScope.value);
  url.searchParams.set("mode", "events");
  url.searchParams.delete("work_type");
  window.history.replaceState({}, "", url);
}

function enterEngineerMop(scope: string): void {
  const url = new URL("/engineer/mop", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope, "ALL"));
  window.location.href = url.toString();
}

function returnToHome(): void {
  if (isEngineerMopPage.value || isHistoryMemoryPage.value) {
    window.location.href = "/";
    return;
  }
  if (currentScope.value) saveDrafts();
  stopActiveItemsSse();
  clearWorkbenchRetry();
  isWorkbench.value = false;
  isEventPage.value = false;
  currentScope.value = "";
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  selectedKeys.clear();
  clearOngoingEdits();
  records.value = [];
  ongoing.value = [];
  zhihangRecords.value = [];
  dailySummary.value = { date: "", items: [], stats: {} };
  availableUndoItems.value = [];
  resetLocalSummaryAdjustments();
  syncText.value = "请选择功能";
  const url = new URL(window.location.href);
  url.searchParams.delete("scope");
  url.searchParams.delete("work_type");
  url.searchParams.delete("mode");
  window.history.replaceState({}, "", url);
}

function updateWorkbenchUrl(nextWorkType = workType.value): void {
  if (!currentScope.value) return;
  const url = new URL(window.location.href);
  url.searchParams.set("scope", currentScope.value);
  const normalizedType = normalizeWorkTypeFilter(nextWorkType);
  if (normalizedType) {
    url.searchParams.set("work_type", normalizedType);
  } else {
    url.searchParams.delete("work_type");
  }
  url.searchParams.delete("mode");
  window.history.replaceState({}, "", url);
}

function switchScope(scope: string, nextWorkType = ""): void {
  const nextScope = normalizeScopeValue(scope, "ALL");
  if (!nextScope) return;
  if (isEventPage.value) {
    currentScope.value = nextScope;
    syncText.value = "正在读取事件数据";
    const url = new URL(window.location.href);
    url.searchParams.set("scope", currentScope.value);
    url.searchParams.set("mode", "events");
    url.searchParams.delete("work_type");
    window.history.replaceState({}, "", url);
    return;
  }
  const nextSelectedWorkType = nextWorkType
    ? normalizeWorkTypeFilter(nextWorkType)
    : (isWorkbench.value ? normalizeWorkTypeFilter(workType.value) : "");
  if (nextScope === currentScope.value && isWorkbench.value) {
    if (nextSelectedWorkType && nextSelectedWorkType !== workType.value) {
      workType.value = nextSelectedWorkType;
      userSelectedWorkType.value = true;
      ongoingTypeFilter.value = "all";
      updateWorkbenchUrl(nextSelectedWorkType);
    }
    return;
  }
  clearWorkbenchRetry();
  if (currentScope.value && nextScope !== currentScope.value) {
    saveDrafts();
  }
  currentScope.value = nextScope;
  isWorkbench.value = true;
  isEventPage.value = false;
  ongoingTypeFilter.value = "all";
  if (nextSelectedWorkType) {
    workType.value = nextSelectedWorkType;
    userSelectedWorkType.value = true;
  } else {
    workType.value = "";
    userSelectedWorkType.value = false;
  }
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  clearOngoingEdits();
  records.value = [];
  ongoing.value = [];
  zhihangRecords.value = [];
  dailySummary.value = { date: "", items: [], stats: {} };
  availableUndoItems.value = [];
  resetLocalSummaryAdjustments();
  syncText.value = "切换中";
  const url = new URL(window.location.href);
  url.searchParams.set("scope", currentScope.value);
  if (nextSelectedWorkType) {
    url.searchParams.set("work_type", nextSelectedWorkType);
  } else {
    url.searchParams.delete("work_type");
  }
  url.searchParams.delete("mode");
  window.history.replaceState({}, "", url);
  loadDrafts();
  loadWorkbench();
  startActiveItemsSse();
}

function pinDraftInMiddlePanel(key: string): void {
  if (!key) return;
  activeDraftKey.value = key;
  nextTick(() => {
    draftStackRef.value?.scrollToTop();
  });
}

function toggleDraftPreview(key: string): void {
  previewDraftKey.value = previewDraftKey.value === key ? "" : key;
}

function draftSummary(record: Dict, draft: Dict): string {
  const timeRange = [draft.start_time, draft.end_time].filter(Boolean).join("~");
  return [
    draft.specialty || specialtyForRecord(record),
    draft.maintenance_cycle || fieldsOf(record)["维护周期"],
    draft.non_plan ? "非计划性" : "",
    draft.location,
    timeRange,
  ].filter(Boolean).join(" · ");
}

function draftOriginLabel(record: Dict, draft: Dict): string {
  if (!record.manual) return "计划事项";
  if (draft.manual_origin === "paste") return "解析粘贴";
  if (draft.prefilled_from_last) return "纯手填 · 已带入上次内容";
  return "纯手填";
}

function draftCardMeta(record: Dict, draft: Dict, editing: boolean): string {
  return [
    workTypeLabel(record.work_type),
    draftOriginLabel(record, draft),
    editing ? "正在编辑" : "点击编辑",
  ].filter(Boolean).join(" · ");
}

function requiredDraftFields(record: Dict, draft: Dict): string[] {
  const type = draftWorkType(record, draft);
  const fields = new Set<string>(["title", "start_time", "end_time", "progress"]);
  if (record.manual) fields.add("building");
  if (type === "maintenance") {
    ["location", "content", "reason", "impact"].forEach((field) => fields.add(field));
    if (record.manual) fields.add("maintenance_cycle");
  } else if (type === "change") {
    ["level", "location", "content", "reason", "impact"].forEach((field) => fields.add(field));
  } else if (type === "repair") {
    ["location", "level", "specialty", "repair_device", "repair_fault", "impact", "reason", "solution"].forEach((field) => fields.add(field));
  } else if (type === "power") {
    ["cabinet", "quantity"].forEach((field) => fields.add(field));
  } else if (type === "polling") {
    ["device", "content", "impact"].forEach((field) => fields.add(field));
  } else if (type === "adjust") {
    ["location", "content", "reason", "impact"].forEach((field) => fields.add(field));
  }
  requiredUploadFields(type, draft).forEach((field) => fields.add(field));
  return Array.from(fields);
}

function draftFieldValue(record: Dict, draft: Dict, field: string): string {
  if (field === "title") return manualDraftTitle(draft, draftWorkType(record, draft));
  return String(draft[field] ?? "").trim();
}

function requiredUploadFields(type: string, values: Dict): string[] {
  const result: string[] = [];
  noticeTemplate(type).uploadFields.forEach((field) => {
    if (field === "non_plan") return;
    if (field === "zhihang") {
      if (Boolean(values.zhihang_involved)) result.push("zhihang_record_id");
      return;
    }
    result.push(field);
  });
  return result;
}

function missingUploadFields(type: string, values: Dict): string[] {
  return requiredUploadFields(type, values).filter((field) => !String(values[field] ?? "").trim());
}

function uploadFieldsMissingText(type: string, missing: string[]): string {
  return missing.length ? `请补充归档字段：${missing.map((field) => noticeFieldLabel(type, field)).join("、")}` : "";
}

function draftMissingFields(record: Dict, draft: Dict): string[] {
  return requiredDraftFields(record, draft).filter((field) => !draftFieldValue(record, draft, field));
}

function draftTypeConflict(record: Dict, draft: Dict): { expectedType: string; actualType: string; keyword: string } | null {
  if (!record.manual) return null;
  const expectedType = draftWorkType(record, draft);
  const parsedType = String(draft.parsed_work_type || "").trim();
  if (parsedType && isKnownWorkType(parsedType) && parsedType !== expectedType) {
    return {
      expectedType,
      actualType: parsedType,
      keyword: `原粘贴通告识别为${workTypeLabel(parsedType)}`,
    };
  }
  const title = manualDraftTitle(draft, expectedType) || recordCardTitle(record);
  const normalizedTitle = String(title || "").replace(/\s+/g, "");
  if (!normalizedTitle) return null;
  for (const type of workTypes.map((item) => item.value)) {
    if (type === expectedType) continue;
    for (const pattern of noticeTypeKeywordRules[type] || []) {
      const match = normalizedTitle.match(pattern);
      if (match) {
        return {
          expectedType,
          actualType: type,
          keyword: match[0] || workTypeLabel(type),
        };
      }
    }
  }
  return null;
}

function draftTypeConflictText(record: Dict, draft: Dict): string {
  const conflict = draftTypeConflict(record, draft);
  if (!conflict) return "";
  return `当前选择的是${workTypeLabel(conflict.expectedType)}，但标题/名称包含“${conflict.keyword}”，像是${workTypeLabel(conflict.actualType)}通告。请改标题或重新选择通告类型。`;
}

function draftFieldInvalid(record: Dict, draft: Dict, field: string): boolean {
  if (!draft.validation_touched) return false;
  if (field === "title" && draftTypeConflict(record, draft)) return true;
  return Boolean(requiredDraftFields(record, draft).includes(field) && !draftFieldValue(record, draft, field));
}

function draftFieldClass(record: Dict, draft: Dict, field: string): Dict {
  return { "field-missing": draftFieldInvalid(record, draft, field) };
}

function draftMissingText(record: Dict, draft: Dict): string {
  if (!draft.validation_touched) return "";
  const conflict = draftTypeConflictText(record, draft);
  if (conflict) return conflict;
  const type = draftWorkType(record, draft);
  const missing = draftMissingFields(record, draft).map((field) => noticeFieldLabel(type, field));
  return missing.length ? `请补充：${missing.join("、")}` : "";
}

function noticePreviewText(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  const template = noticeTemplate(type);
  const actionLabel = draftActionLabel(record, draft);
  const lines = [`【${template.heading}】状态：${actionLabel}`];
  const append = (field: string, value: string) => {
    lines.push(`【${noticeFieldLabel(type, field)}】${String(value || "").trim()}`);
  };
  append("title", manualDraftTitle(draft, type));
  if (type === "repair") {
    for (const field of template.messageFields.filter((item) => item !== "title")) append(field, draft[field] || "");
  } else {
    if (type === "change") append("level", draft.level || "");
    lines.push(`【时间】${draft.start_time || ""}~${draft.end_time || ""}`);
    for (const field of template.messageFields.filter((item) => !["title", "level", "start_time", "end_time"].includes(item))) {
      append(field, draft[field] || "");
    }
  }
  return lines.join("\n");
}

function ongoingNoticePreviewText(item: Dict): string {
  const draft = ongoingDraft(item);
  const action = String(jobStates.get(ongoingLineKey(item))?.payload?.action || "update").toLowerCase();
  return noticePreviewText(
    {
      ...item,
      manual: true,
      work_type: item.work_type || "maintenance",
    },
    {
      ...draft,
      manual: true,
      work_type: item.work_type || "maintenance",
      parsed_action: action || "update",
    },
  );
}

function draftUploadPreviewRows(record: Dict, draft: Dict): Array<{ label: string; value: string }> {
  const type = draftWorkType(record, draft);
  const rows: Array<{ label: string; value: string }> = [
    { label: "通告类型", value: workTypeLabel(type) },
    { label: "楼栋/范围", value: record.manual ? draft.building : buildingForRecord(record) },
    { label: "专业", value: draft.specialty || specialtyForRecord(record) },
  ];
  if (type === "maintenance") {
    rows.push({ label: "维保周期", value: draft.maintenance_cycle || fieldsOf(record)["维护周期"] || "" });
    rows.push({ label: "非计划性", value: draft.non_plan ? "是" : "" });
  }
  if (type === "change") {
    rows.push({ label: "变更等级", value: draft.level || levelForRecord(record) || "I3" });
    rows.push({ label: "涉及智航", value: draft.zhihang_involved ? (draft.zhihang_title || "是") : "" });
    if (isConvertedMaintenanceChange(record, draft)) {
      rows.push({ label: "同时写入维保记录", value: syncMaintenanceTargetValue(record, draft) ? "是" : "否" });
      rows.push({ label: "维保原名称", value: draft.paired_maintenance_original_title || titleForRecord(record) });
    }
  }
  if (type === "repair") {
    rows.push({ label: "紧急程度", value: draft.level || "" });
    rows.push({ label: "维修设备", value: draft.repair_device || "" });
  }
  if (type === "power") {
    rows.push({ label: "柜号", value: draft.cabinet || "" });
    rows.push({ label: "数量", value: draft.quantity || "" });
  }
  if (type === "polling") {
    rows.push({ label: "设备", value: draft.device || "" });
  }
  return rows.filter((row) => String(row.value || "").trim());
}

function sendDraftButtonLabel(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  return `发送${workTypeLabel(type)}${draftActionLabel(record, draft)}`;
}

function payloadDurationError(payload: Dict): string {
  const workType = String(payload?.work_type || "maintenance");
  if (workType === "repair") {
    return noticeDurationError(
      workType,
      payload?.fault_time || payload?.end_time,
      payload?.expected_time || payload?.start_time,
    );
  }
  return noticeDurationError(workType, payload?.start_time, payload?.end_time);
}

function validatePayloadDuration(payload: Dict, lineKey: string): boolean {
  const error = payloadDurationError(payload);
  if (!error) return true;
  rememberJob(lineKey, {
    text: error,
    status: "failed",
    phase: "failed",
  });
  syncText.value = error;
  return false;
}

function normalizeDraftSignatureText(value: string): string {
  return String(value || "").replace(/\s+/g, "").replace(/[；;。,.，、：:（）()《》【】]/g, "").toLowerCase();
}

function draftSignature(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  const title = normalizeDraftSignatureText(manualDraftTitle(draft, type) || recordCardTitle(record));
  const start = String(draft.start_time || "").slice(0, 16);
  const end = String(draft.end_time || "").slice(0, 16);
  return [type, title, start, end].join("|");
}

function findDuplicateDraftKey(record: Dict, draft: Dict, excludeKey = ""): string {
  const signature = draftSignature(record, draft);
  if (!signature.split("|")[1]) return "";
  for (const key of selectedKeys) {
    if (key === excludeKey) continue;
    const otherRecord = draftRecordForKey(key);
    const otherDraft = drafts.get(key);
    if (!otherRecord || !otherDraft) continue;
    if (draftSignature(otherRecord, otherDraft) === signature) return key;
  }
  return "";
}

function toggleRecordSelection(row: NoticeRow | undefined): void {
  const key = row?.id || "";
  if (!key) return;
  const record = draftRecordForKey(key);
  if (row?.disabled || (record && isRecordOngoing(record))) {
    syncText.value = "该事项已在进行中，请在右侧卡片更新、结束或删除";
    return;
  }
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  if (record) getDraft(record);
  saveDrafts();
}

function addManualDraft(type = workType.value): void {
  const normalizedType = normalizeWorkType(type);
  const key = `manual:${normalizedType}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const draft = manualDraftDefaults(normalizedType);
  const memory = loadManualTemplateMemory(normalizedType);
  if (memory) {
    Object.assign(draft, memory, {
      manual: true,
      work_type: normalizedType,
      prefilled_from_last: true,
    });
    if (!draft.building) draft.building = defaultBuildingForCurrentScope();
    if (!Array.isArray(draft.building_codes) || !draft.building_codes.length) {
      draft.building_codes = buildingCodesFromText(draft.building || "");
    }
  }
  draft.manual_origin = "manual";
  drafts.set(key, draft);
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  showManualTypePicker.value = false;
  workType.value = normalizedType;
  saveManualRecentType(normalizedType);
  saveDrafts();
}

function removeDraft(key: string): void {
  selectedKeys.delete(key);
  if (isManualKey(key)) drafts.delete(key);
  jobStates.delete(key);
  clearFallbackPoll(key);
  if (activeDraftKey.value === key) activeDraftKey.value = "";
  saveDrafts();
}

function changeTargetCandidateId(item: Dict): string {
  return String(item?.record_id || item?.target_record_id || "").trim();
}

function changeSourceCandidateId(item: Dict): string {
  return String(item?.source_record_id || item?.record_id || "").trim();
}

const changeTargetCandidates = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  return Array.isArray(pending?.candidates) ? pending.candidates : [];
});

const filteredChangeTargetCandidates = computed(() => {
  return filterCandidatesBySearch(changeTargetCandidates.value, changeTargetSearchText.value);
});

const selectedChangeTargetVisible = computed(() => {
  const selected = selectedChangeTargetId.value;
  if (!selected) return false;
  return changeTargetCandidates.value.some((item: Dict) => changeTargetCandidateId(item) === selected);
});

const visibleActiveChangeTargetCandidate = computed(() => {
  const detailId = hoveredChangeTargetId.value || selectedChangeTargetId.value;
  if (detailId) {
    const visible =
      changeTargetCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === detailId) ||
      filteredChangeTargetCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === detailId);
    if (visible) return visible;
  }
  return filteredChangeTargetCandidates.value[0] || null;
});

const changeSourceCandidates = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  return Array.isArray(pending?.sourceCandidates) ? pending.sourceCandidates : [];
});

const filteredChangeSourceCandidates = computed(() => {
  return filterCandidatesBySearch(changeSourceCandidates.value, changeSourceSearchText.value);
});

const selectedChangeSourceVisible = computed(() => {
  const selected = selectedChangeSourceId.value;
  if (!selected) return false;
  return changeSourceCandidates.value.some((item: Dict) => changeSourceCandidateId(item) === selected);
});

const selectedChangeSourceCandidate = computed(() => {
  const id = selectedChangeSourceId.value;
  if (!id) return null;
  return changeSourceCandidates.value.find((item: Dict) => changeSourceCandidateId(item) === id) || null;
});

const ongoingBindCandidates = computed(() => {
  const pending = ongoingBindSelection.value;
  return Array.isArray(pending?.candidates) ? pending.candidates : [];
});

const activeOngoingBindCandidate = computed(() => {
  const candidates = ongoingBindCandidates.value;
  if (!candidates.length) return null;
  const detailId = hoveredOngoingBindId.value || selectedOngoingBindId.value;
  return candidates.find((item: Dict) => changeTargetCandidateId(item) === detailId) || candidates[0];
});

function changeTargetDetailRows(item: Dict | null): Array<{ label: string; value: string }> {
  if (!item) return [];
  const source = Array.isArray(item.field_items)
    ? item.field_items
    : Object.entries(item.fields || {}).map(([label, value]) => ({ label, value }));
  const rows = source
    .map((row: Dict) => ({
      label: String(row.label || "").trim(),
      value: String(row.value ?? "").trim(),
    }))
    .filter((row: { label: string; value: string }) => row.label && row.value);
  if (rows.length) return rows;
  return [
    { label: "名称", value: String(item.title || "") },
    { label: "楼栋", value: String(item.building || "") },
    { label: "状态", value: String(item.status || "") },
    { label: "开始时间", value: String(item.start_time || "") },
    { label: "结束时间", value: String(item.end_time || "") },
  ].filter((row) => row.value);
}

function previewChangeTarget(item: Dict): void {
  hoveredChangeTargetId.value = changeTargetCandidateId(item);
}

function selectChangeTarget(item: Dict): void {
  const id = changeTargetCandidateId(item);
  selectedChangeTargetId.value = id;
  hoveredChangeTargetId.value = id;
}

function previewOngoingBindCandidate(item: Dict): void {
  hoveredOngoingBindId.value = changeTargetCandidateId(item);
}

function selectOngoingBindCandidate(item: Dict): void {
  const id = changeTargetCandidateId(item);
  selectedOngoingBindId.value = id;
  hoveredOngoingBindId.value = id;
}

function closeOngoingBindSelection(candidate: Dict | null): void {
  const resolver = resolveOngoingBindSelection;
  resolveOngoingBindSelection = null;
  ongoingBindSelection.value = null;
  selectedOngoingBindId.value = "";
  hoveredOngoingBindId.value = "";
  if (resolver) resolver(candidate);
}

function cancelOngoingBindSelection(): void {
  closeOngoingBindSelection(null);
}

function confirmOngoingBindSelection(): void {
  const selected =
    ongoingBindCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === selectedOngoingBindId.value) ||
    activeOngoingBindCandidate.value;
  closeOngoingBindSelection(selected || null);
}

function firstCandidateField(fields: Dict, names: string[]): string {
  for (const name of names) {
    const value = fields?.[name];
    const text = cleanDisplayText(value);
    if (text) return text;
  }
  return "";
}

function fillDraftBlank(draft: Dict, key: string, value: string): void {
  const text = String(value || "").trim();
  if (!text) return;
  if (String(draft[key] ?? "").trim()) return;
  draft[key] = text;
}

function fillDraftBlankDatetime(draft: Dict, key: string, value: string): void {
  const normalized = toDatetimeLocal(value);
  fillDraftBlank(draft, key, normalized || value);
}

function applyChangeTargetCandidateDefaults(draft: Dict, candidate: Dict): Dict {
  const fields = candidate?.fields || {};
  const next = { ...draft };
  fillDraftBlank(next, "title", firstCandidateField(fields, ["名称", "标题", "变更简述"]) || candidate.title || "");
  fillDraftBlank(next, "building", firstCandidateField(fields, ["楼栋", "变更楼栋"]) || candidate.building || "");
  fillDraftBlank(next, "specialty", firstCandidateField(fields, ["专业", "专业类别"]));
  fillDraftBlank(next, "maintenance_cycle", firstCandidateField(fields, ["维保周期", "维护周期"]));
  fillDraftBlank(next, "level", firstCandidateField(fields, ["阿里-变更等级", "智航-变更等级", "变更等级", "变更等级（阿里）", "紧急程度", "等级"]));
  fillDraftBlankDatetime(next, "start_time", firstCandidateField(fields, ["变更开始时间", "计划开始时间", "计划开始", "开始时间", "期望完成时间"]) || candidate.start_time || "");
  fillDraftBlankDatetime(next, "end_time", firstCandidateField(fields, ["计划结束时间", "计划结束", "结束时间", "发生故障时间", "故障发生时间"]) || candidate.end_time || "");
  fillDraftBlank(next, "location", firstCandidateField(fields, ["位置", "地点"]));
  fillDraftBlank(next, "content", firstCandidateField(fields, ["内容", "变更内容", "变更简述"]));
  fillDraftBlank(next, "reason", firstCandidateField(fields, ["原因", "变更原因"]));
  fillDraftBlank(next, "impact", firstCandidateField(fields, ["影响", "影响范围"]));
  fillDraftBlank(next, "progress", firstCandidateField(fields, ["进度", "完成情况"]));
  fillDraftBlank(next, "repair_device", firstCandidateField(fields, ["维修设备"]));
  fillDraftBlank(next, "repair_fault", firstCandidateField(fields, ["维修故障"]));
  fillDraftBlank(next, "fault_type", firstCandidateField(fields, ["故障类型"]));
  fillDraftBlank(next, "repair_mode", firstCandidateField(fields, ["维修方式"]));
  fillDraftBlank(next, "discovery", firstCandidateField(fields, ["故障发现方式（来源）", "故障发现方式"]));
  fillDraftBlank(next, "symptom", firstCandidateField(fields, ["故障现象"]));
  fillDraftBlank(next, "solution", firstCandidateField(fields, ["解决方案"]));
  fillDraftBlank(next, "spare_parts", firstCandidateField(fields, ["备件更换情况", "备件使用情况"]));
  fillDraftBlank(next, "device", firstCandidateField(fields, ["设备", "维修设备"]));
  fillDraftBlank(next, "cabinet", firstCandidateField(fields, ["柜号"]));
  fillDraftBlank(next, "quantity", firstCandidateField(fields, ["数量（个）", "数量"]));
  return next;
}

function completeParsedNoticeDraft(type: string, draft: Dict, options: Dict = {}): void {
  const normalizedType = normalizeWorkType(type);
  const key = `manual:${normalizedType}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  Object.assign(draft, options, { manual_origin: "paste" });
  const candidateRecord = manualRecordFromDraft(key, draft);
  const duplicateKey = findDuplicateDraftKey(candidateRecord, draft, key);
  if (duplicateKey) {
    activeDraftKey.value = duplicateKey;
    pasteParseLine.value = "待发起通告中已有相同类型、标题和时间的草稿。";
    pasteParseStatus.value = "failed";
    return;
  }
  drafts.set(key, draft);
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  workType.value = normalizedType;
  pasteText.value = "";
  pendingChangeTargetSelection.value = null;
  selectedChangeTargetId.value = "";
  hoveredChangeTargetId.value = "";
  selectedChangeSourceId.value = "";
  changeTargetSearchText.value = "";
  changeSourceSearchText.value = "";
  showPasteParser.value = false;
  pasteParseLine.value = `已解析为${workTypeLabel(normalizedType)}${parsedActionLabel(draft.parsed_action || "start")}通告。`;
  pasteParseStatus.value = "success";
  saveDrafts();
}

async function parsePastedNotice(): Promise<void> {
  const text = pasteText.value.trim();
  if (!text) return;
  pasteParseBusy.value = true;
  pasteParseLine.value = "正在解析通告...";
  pasteParseStatus.value = "";
  pendingChangeTargetSelection.value = null;
  selectedChangeTargetId.value = "";
  hoveredChangeTargetId.value = "";
  selectedChangeSourceId.value = "";
  changeTargetSearchText.value = "";
  changeSourceSearchText.value = "";
  try {
    const sections = parseSections(text);
    if (/事件通告/.test(text)) {
      throw new Error("前端暂不支持事件通告纯手填或解析，请在 Qt 主界面处理事件通告。");
    }
    const type = pastedNoticeWorkType(text, sections);
    const draft = manualDraftDefaults(type);
    draft.parsed_work_type = type;
    if (type === "power") {
      draft.notice_type = /【\s*下电通告\s*】|下电通告/.test(text) ? "下电通告" : "上电通告";
    }
    const status = pastedNoticeStatus(text);
    const action = parsedActionFromStatus(status);
    const timeRange = splitNoticeTimeRange(sectionValue(sections, ["时间"]));
    draft.parsed_action = action;
    draft.title = type === "change"
      ? sectionValue(sections, ["名称", "标题"])
      : sectionValue(sections, ["标题", "名称", "维修名称"]);
    draft.non_plan = /（非计划性）|（非计划）/.test(draft.title);
    draft.location = sectionValue(sections, ["地点", "位置"]) || rawSectionValue(text, ["地点", "位置"]);
    draft.specialty = sectionValue(sections, ["专业", "专业类别"]) || rawSectionValue(text, ["专业", "专业类别"]);
    draft.reason = sectionValue(sections, ["原因", "故障原因"]) || rawSectionValue(text, ["原因", "故障原因"]);
    draft.impact = sectionValue(sections, ["影响", "影响范围"]) || rawSectionValue(text, ["影响", "影响范围"]);
    draft.progress = sectionValue(sections, ["进度", "完成情况"]) || rawSectionValue(text, ["进度", "完成情况"]);
    draft.maintenance_cycle = sectionValue(sections, ["维保周期", "维护周期"]) || rawSectionValue(text, ["维保周期", "维护周期"]);
    draft.level = sectionValue(sections, ["等级", "变更等级", "紧急程度"]) || rawSectionValue(text, ["等级", "变更等级", "紧急程度"]) || (type === "change" ? "I3" : "");
    draft.start_time = timeRange.start || draft.start_time;
    draft.end_time = timeRange.end || draft.end_time;
    draft.building = sectionValue(sections, ["楼栋", "变更楼栋", "所属楼栋"])
      || inferBuildingText(draft.title, draft.location, text)
      || defaultBuildingForCurrentScope();
    draft.building_codes = buildingCodesFromText(draft.building);
    draft.content = type === "repair"
      ? sectionValue(sections, ["标题/补充内容", "标题补充内容", "补充内容", "内容"])
      : sectionValue(sections, ["内容"], draft.title);
    draft.repair_device = sectionValue(sections, ["维修设备"]) || rawSectionValue(text, ["维修设备"]);
    draft.repair_fault = sectionValue(sections, ["维修故障"]) || rawSectionValue(text, ["维修故障"]);
    draft.fault_type = sectionValue(sections, ["故障类型"]) || rawSectionValue(text, ["故障类型"]);
    draft.repair_mode = sectionValue(sections, ["维修方式"]) || rawSectionValue(text, ["维修方式"]);
    draft.discovery = sectionValue(sections, ["故障发现方式"]) || rawSectionValue(text, ["故障发现方式"]);
    draft.symptom = sectionValue(sections, ["故障现象"]) || rawSectionValue(text, ["故障现象"]);
    draft.solution = sectionValue(sections, ["解决方案"]) || rawSectionValue(text, ["解决方案"]);
    draft.spare_parts = sectionValue(sections, ["备件更换情况", "备件使用情况"]) || rawSectionValue(text, ["备件更换情况", "备件使用情况"]);
    draft.device = sectionValue(sections, ["设备"]) || rawSectionValue(text, ["设备"]);
    draft.cabinet = sectionValue(sections, ["柜号"]) || rawSectionValue(text, ["柜号"]);
    draft.quantity = sectionValue(sections, ["数量"]) || rawSectionValue(text, ["数量"]);
    if (type === "repair") {
      const expectedTime = sectionValue(sections, ["期望完成时间"]);
      const faultTime = sectionValue(sections, ["发现故障时间", "故障发生时间", "发生故障时间"]);
      draft.start_time = toDatetimeLocal(expectedTime) || timeRange.end || timeRange.start || draft.start_time;
      draft.end_time = toDatetimeLocal(faultTime) || timeRange.start || draft.end_time;
      draft.reason = sectionValue(sections, ["故障原因", "原因"], draft.reason);
      draft.impact = sectionValue(sections, ["影响范围", "影响"], draft.impact);
      draft.progress = sectionValue(sections, ["完成情况", "进度"], draft.progress);
    }
    if (action !== "start") {
      if (!draft.title) {
        throw new Error(`${workTypeLabel(type)}更新/结束通告必须包含【名称】或【标题】。`);
      }
      const data = await api(type === "change" ? "/api/change-target-candidates" : "/api/notice-target-candidates", {
        method: "POST",
        body: JSON.stringify({
          work_type: type,
          scope: currentScope.value || "ALL",
          title: draft.title,
          start_time: draft.start_time,
          end_time: draft.end_time,
          action,
        }),
      });
      const candidates = Array.isArray(data.candidates) ? data.candidates : [];
      const sourceCandidates = Array.isArray(data.source_candidates) ? data.source_candidates : [];
      if (candidates.length > 0 || sourceCandidates.length > 0) {
        pendingChangeTargetSelection.value = {
          type,
          draft,
          action,
          actionLabel: parsedActionLabel(action),
          candidates,
          sourceCandidates,
          returnedCount: Number(data.returned_count ?? data.count ?? candidates.length),
          totalMatched: Number(data.total_matched ?? candidates.length),
          limit: Number(data.limit ?? candidates.length),
          limited: Boolean(data.limited),
        };
        changeTargetSearchText.value = "";
        changeSourceSearchText.value = "";
        selectedChangeTargetId.value = changeTargetCandidateId(candidates[0]);
        hoveredChangeTargetId.value = selectedChangeTargetId.value;
        selectedChangeSourceId.value = sourceCandidates.length ? changeSourceCandidateId(sourceCandidates[0]) : "";
        pasteParseLine.value = candidates.length
          ? `找到 ${Number(data.total_matched ?? candidates.length)} 条同名${workTypeLabel(type)}已上传通告，当前显示 ${candidates.length} 条，请确认要${parsedActionLabel(action)}的通告。`
          : `未找到同名已上传通告，但找到 ${sourceCandidates.length} 条原始事项；可先选择原始事项后继续提交。`;
        pasteParseStatus.value = "";
        return;
      }
      throw new Error(`未找到同名${workTypeLabel(type)}通告，不能作为更新/结束通告发送。`);
    }
    completeParsedNoticeDraft(type, draft);
  } catch (error: any) {
    pasteParseLine.value = error?.message || "解析失败";
    pasteParseStatus.value = "failed";
  } finally {
    pasteParseBusy.value = false;
  }
}

function choosePastedChangeTarget(candidate: Dict): void {
  const pending = pendingChangeTargetSelection.value;
  if (!pending) return;
  const source = selectedChangeSourceCandidate.value || {};
  const targetId = candidate.record_id || candidate.target_record_id || "";
  const draft = applyChangeTargetCandidateDefaults(
    applyChangeTargetCandidateDefaults({ ...(pending.draft || {}) }, candidate),
    source,
  );
  completeParsedNoticeDraft(String(pending.type || "change"), draft, {
    target_record_id: targetId,
    record_id: targetId || source.source_record_id || source.record_id || "",
    source_record_id: source.source_record_id || source.record_id || "",
    source_app_token: source.source_app_token || "",
    source_table_id: source.source_table_id || "",
    building: draft.building || candidate.building || "",
    building_codes: source.building_codes || candidate.building_codes || [],
  });
}

async function confirmPastedChangeTarget(): Promise<void> {
  const pending = pendingChangeTargetSelection.value;
  if (!pending || changeTargetConfirming.value) return;
  const candidate =
    changeTargetCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === selectedChangeTargetId.value) ||
    visibleActiveChangeTargetCandidate.value;
  const sourceOnly = !candidate && selectedChangeSourceVisible.value && selectedChangeSourceCandidate.value;
  if (!candidate && !sourceOnly) {
    pasteParseLine.value = "请先选择一条已上传通告或原始事项。";
    pasteParseStatus.value = "failed";
    return;
  }
  changeTargetConfirming.value = true;
  pasteParseLine.value = "正在确认对应通告...";
  pasteParseStatus.value = "";
  try {
    let selected = candidate || {};
    if (candidate && String(pending.type || "") === "change" && String(pending.action || "") === "update") {
      const data = await api("/api/change-target-candidates/confirm", {
        method: "POST",
        body: JSON.stringify({
          scope: currentScope.value || "ALL",
          title: pending.draft?.title || candidate.title || "",
          start_time: pending.draft?.start_time || "",
          end_time: pending.draft?.end_time || "",
          action: pending.action || "update",
          record_id: changeTargetCandidateId(candidate || {}),
        }),
      });
      selected = data.candidate || candidate;
      if (Array.isArray(data.source_candidates)) {
        pending.sourceCandidates = data.source_candidates;
        if (
          pending.sourceCandidates.length &&
          !pending.sourceCandidates.some((item: Dict) => changeSourceCandidateId(item) === selectedChangeSourceId.value)
        ) {
          selectedChangeSourceId.value = changeSourceCandidateId(pending.sourceCandidates[0]);
        }
      }
      const cleared = data.clear_actual_end?.cleared;
      pasteParseLine.value = cleared ? "已确认对应通告，并清空原实际结束时间。" : "已确认对应通告。";
    }
    choosePastedChangeTarget(selected || {});
  } catch (error: any) {
    pasteParseLine.value = error?.message || "确认对应通告失败";
    pasteParseStatus.value = "failed";
  } finally {
    changeTargetConfirming.value = false;
  }
}

async function importHistoricalMemory(): Promise<void> {
  const text = memoryImportText.value.trim();
  if (!text) {
    memoryImportLine.value = "请先粘贴历史通告。";
    memoryImportLineType.value = "failed";
    return;
  }
  memoryImportBusy.value = true;
  memoryImportLine.value = "正在导入历史记忆...";
  memoryImportLineType.value = "";
  try {
    const data = await api("/api/notice-memory/import", {
      method: "POST",
      body: JSON.stringify({ scope: currentScope.value || "ALL", text }),
    });
    const imported = Number(data.imported_count || 0);
    const skipped = Number(data.skipped_count || 0);
    memoryImportLine.value = `已导入 ${imported} 条记忆${skipped ? `，跳过 ${skipped} 条` : ""}。`;
    memoryImportLineType.value = imported > 0 ? "success" : "failed";
    if (imported > 0) {
      memoryImportText.value = "";
      showMemoryImporter.value = false;
      await loadWorkbench();
    }
  } catch (error: any) {
    memoryImportLine.value = error?.message || "导入失败";
    memoryImportLineType.value = "failed";
  } finally {
    memoryImportBusy.value = false;
  }
}

function bindZhihang(draft: Dict): void {
  const item = zhihangRecords.value.find((record) => record.record_id === draft.zhihang_record_id);
  draft.zhihang_title = item?.title || "";
  draft.zhihang_progress = item?.progress || "";
  saveDrafts();
}

function bindOngoingZhihang(item: Dict, recordId: string): void {
  const change = zhihangRecords.value.find((record) => record.record_id === recordId);
  setOngoingEdit(item, "zhihang_record_id", recordId);
  setOngoingEdit(item, "zhihang_title", change?.title || "");
  setOngoingEdit(item, "zhihang_progress", change?.progress || "");
}

function opId(key: string): string {
  return `${key}:${Date.now()}`;
}

function payloadSendsPersonalMessage(payload: Dict): boolean {
  const type = String(payload?.work_type || "maintenance");
  return ["maintenance", "change", "repair", "power", "polling", "adjust"].includes(type);
}

function buildStartPayload(key: string): Dict | null {
  const record = draftRecordForKey(key);
  const draft = drafts.get(key);
  if (!record || !draft) return null;
  const type = draftWorkType(record, draft);
  if (record.manual && draft.work_type !== type) draft.work_type = type;
  const action = record.manual ? draftActionForRecord(record, draft) : sourceActionForRecord(record);
  const targetRecordId = record.manual ? String(draft.target_record_id || draft.feishu_record_id || draft.raw_record_id || "").trim() : targetRecordIdForRecord(record);
  const syncMaintenanceTarget = syncMaintenanceTargetValue(record, draft);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: type,
    notice_type: record.notice_type || "",
    manual: Boolean(record.manual),
    manual_id: record.manual ? key : "",
    source_app_token: record.manual ? (draft.source_app_token || "") : (record.source_app_token || ""),
    source_table_id: record.manual ? (draft.source_table_id || "") : (record.source_table_id || ""),
    maintenance_cycle: record.manual ? (draft.maintenance_cycle || "") : (fieldsOf(record)["维护周期"] || ""),
    specialty: cleanDisplayText(draft.specialty) || specialtyForRecord(record),
    record_id: action === "start" ? record.record_id : targetRecordId,
    source_record_id: record.manual ? (draft.source_record_id || "") : record.record_id,
    source_work_type: record.manual ? (draft.source_work_type || type) : sourceWorkTypeForRecord(record),
    converted_from_work_type: record.manual ? (draft.converted_from_work_type || "") : (record.converted_from_work_type || ""),
    sync_maintenance_target: syncMaintenanceTarget,
    paired_maintenance_target_record_id: syncMaintenanceTarget ? (draft.paired_maintenance_target_record_id || record.paired_maintenance_target_record_id || "") : "",
    paired_maintenance_original_title: syncMaintenanceTarget ? (draft.paired_maintenance_original_title || titleForRecord(record)) : "",
    paired_maintenance_actual_start_time: syncMaintenanceTarget ? (draft.paired_maintenance_actual_start_time || record.paired_maintenance_actual_start_time || "") : "",
    target_record_id: action !== "start" ? targetRecordId : "",
    source_progress: sourceProgressForRecord(record),
    building_codes: record.manual ? (draft.building_codes || []) : (record.building_codes || []),
    building: record.manual ? (draft.building || "") : buildingForRecord(record),
    title: payloadTitleForDraft(record, draft, type),
    non_plan: record.manual && type === "maintenance" ? Boolean(draft.non_plan) : false,
    level: record.manual ? (draft.level || (type === "change" ? "I3" : "")) : (type === "repair" ? (draft.level || "") : (type === "change" ? (levelForRecord(record) || "I3") : "")),
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
    spare_parts: type === "repair" ? (draft.spare_parts || "") : "",
    device: type === "polling" ? (draft.device || "") : "",
    cabinet: type === "power" ? (draft.cabinet || "") : "",
    quantity: type === "power" ? (draft.quantity || "") : "",
    fault_time: type === "repair" ? (draft.end_time || "") : "",
    expected_time: type === "repair" ? (draft.start_time || "") : "",
    operation_id: draft.operation_id || (draft.operation_id = opId(`${key}:${action}`)),
  };
}

async function sendStart(key: string): Promise<void> {
  const payload = buildStartPayload(key);
  if (!payload) return;
  const draft = drafts.get(key);
  const record = draftRecordForKey(key);
  if (record && draft) {
    draft.validation_touched = true;
    const typeConflict = draftTypeConflictText(record, draft);
    if (typeConflict) {
      rememberJob(key, {
        text: typeConflict,
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
    const missing = draftMissingFields(record, draft);
    if (missing.length) {
      rememberJob(key, {
        text: draftMissingText(record, draft) || "请补充必填字段",
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
    const duplicateKey = findDuplicateDraftKey(record, draft, key);
    if (duplicateKey) {
      activeDraftKey.value = duplicateKey;
      rememberJob(key, {
        text: "待发起通告中已有相同类型、标题和时间的草稿，请核对后再发送",
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
  }
  if (record && !recordMatchesCurrentScope(record)) {
    rememberJob(key, { text: "当前入口与通告楼栋不匹配，请切换到对应楼栋或园区后再发送", status: "failed", phase: "failed" });
    return;
  }
  if (payload.work_type === "maintenance" && payload.manual && !payload.maintenance_cycle) {
    rememberJob(key, { text: "纯手填维保必须选择维保周期", status: "failed", phase: "failed" });
    return;
  }
  if (!validatePayloadDuration(payload, key)) {
    saveDrafts();
    return;
  }
  if (payload.manual && draft?.manual_origin === "manual") {
    saveManualTemplateMemory(String(payload.work_type || ""), draft);
    saveManualRecentType(String(payload.work_type || ""));
  }
  await sendAction(payload, key);
  saveDrafts();
}

function ongoingDraft(item: Dict): Dict {
  const id = ongoingLineKey(item);
  if (!ongoingEdits.has(id)) {
    const timeRange = ongoingTimeRange(item);
    const workType = item.work_type || "maintenance";
    let title = item.title || item.content || "";
    const content = workType === "repair" && item.content === title ? "" : item.content || "";
    if (workType === "repair" && content && title.endsWith(content)) {
      title = title.slice(0, -content.length).trim() || title;
    }
    ongoingEdits.set(id, {
      title,
      specialty: cleanDisplayText(item.specialty),
      maintenance_cycle: item.maintenance_cycle || "",
      level: item.level || "",
      start_time: timeRange.start,
      end_time: timeRange.end,
      location: item.location || "",
      content,
      reason: item.reason || "",
      impact: item.impact || "",
      progress: item.progress || item.content || "",
      zhihang_involved: Boolean(item.zhihang_involved || item.zhihang_record_id),
      zhihang_record_id: item.zhihang_record_id || "",
      zhihang_title: item.zhihang_title || "",
      zhihang_progress: item.zhihang_progress || "",
      repair_device: item.repair_device || "",
      repair_fault: item.repair_fault || "",
      fault_type: item.fault_type || "",
      repair_mode: item.repair_mode || "",
      discovery: cleanDisplayText(item.discovery),
      symptom: item.symptom || "",
      solution: item.solution || "",
      spare_parts: item.spare_parts || "",
      device: item.device || "",
      cabinet: item.cabinet || "",
      quantity: item.quantity || "",
      extra_images: Array.isArray(item.extra_images) ? [...item.extra_images] : [],
      target_record_id: targetRecordIdForOngoing(item),
      source_record_id: sourceRecordIdForOngoing(item, targetRecordIdForOngoing(item)),
      sync_maintenance_target: Boolean(item.sync_maintenance_target),
      paired_maintenance_target_record_id: item.paired_maintenance_target_record_id || "",
      paired_maintenance_original_title: item.paired_maintenance_original_title || item.title || "",
      paired_maintenance_actual_start_time: item.paired_maintenance_actual_start_time || "",
    });
  }
  return ongoingEdits.get(id) || {};
}

function setOngoingEdit(item: Dict, key: string, value: any): void {
  const id = ongoingLineKey(item);
  const current = ongoingDraft(item);
  current[key] = value;
  ongoingEdits.set(id, current);
}

function draftValue(edit: Dict, key: string, fallback = ""): string {
  if (Object.prototype.hasOwnProperty.call(edit, key)) return String(edit[key] ?? "");
  return String(fallback ?? "");
}

function ongoingExtraImages(item: Dict): Dict[] {
  const edit = ongoingDraft(item);
  return Array.isArray(edit.extra_images) ? edit.extra_images : [];
}

function ongoingPhotoCount(item: Dict): number {
  return ongoingExtraImages(item).filter((photo) => String(photo?.upload_id || photo?.bytes_b64 || photo?.file_token || "").trim()).length;
}

function ongoingEndRequiresSitePhoto(item: Dict): boolean {
  return ["maintenance", "change", "repair"].includes(String(item?.work_type || "maintenance"));
}

async function uploadSitePhotoFile(file: File): Promise<Dict> {
  const fileName = file.name || `site_photo_${Date.now()}.png`;
  try {
    const data = await requestBinaryJson(
      `/api/notice-attachments?file_name=${encodeURIComponent(fileName)}`,
      file,
      {
        headers: { "Content-Type": file.type || "image/png" },
      },
      {
        onOnline: () => {
          backendStatus.offline = false;
          backendStatus.message = "";
        },
        onOffline: (message) => {
          backendStatus.offline = true;
          backendStatus.lastErrorAt = Date.now();
          backendStatus.message = message;
        },
        onAuthExpired: (message) => {
          markAuthExpired(message);
        },
        onServerError: (message) => {
          backendStatus.offline = true;
          backendStatus.lastErrorAt = Date.now();
          backendStatus.message = message;
        },
      },
    );
    return {
      upload_id: data.upload_id,
      file_name: data.file_name || fileName,
      mime_type: data.mime_type || file.type || "image/png",
      size: data.size || file.size || 0,
      expires_at: data.expires_at || 0,
      preview_url: URL.createObjectURL(file),
    };
  } catch (error: any) {
    throw new Error(error?.message || "现场照片上传失败");
  }
}

function isValidSitePhotoFile(file: File): boolean {
  const type = String(file.type || "");
  if (type && !type.startsWith("image/")) return false;
  if (file.size > MAX_SITE_PHOTO_BYTES) return false;
  return true;
}

function pastedImageFiles(event: ClipboardEvent): File[] {
  const clipboard = event.clipboardData;
  if (!clipboard) return [];
  const files: File[] = [];
  Array.from(clipboard.items || []).forEach((item) => {
    if (!item.type.startsWith("image/")) return;
    const file = item.getAsFile();
    if (file) files.push(file);
  });
  if (files.length) return files;
  return Array.from(clipboard.files || []).filter((file) => {
    const type = String(file.type || "");
    return !type || type.startsWith("image/");
  });
}

async function addOngoingPhotoFiles(item: Dict, files: File[], invalidMessage: string): Promise<void> {
  if (!files.length) return;
  const key = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const existing = ongoingExtraImages(item);
  const availableSlots = Math.max(0, MAX_SITE_PHOTO_COUNT - existing.length);
  if (availableSlots <= 0) {
    rememberJob(key, { text: `现场照片最多添加 ${MAX_SITE_PHOTO_COUNT} 张`, status: "failed", phase: "failed" });
    return;
  }
  const validFiles = files
    .filter(isValidSitePhotoFile)
    .slice(0, availableSlots);
  if (!validFiles.length) {
    rememberJob(key, { text: invalidMessage, status: "failed", phase: "failed" });
    return;
  }
  try {
    rememberJob(key, { text: "正在上传现场照片", status: "busy", phase: "uploading_attachment" });
    const photos = await Promise.all(validFiles.map(uploadSitePhotoFile));
    edit.extra_images = [...existing, ...photos];
    ongoingEdits.set(key, edit);
    rememberJob(key, { text: `已添加现场照片 ${edit.extra_images.length} 张`, status: "", phase: "" });
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "现场照片读取失败", status: "failed", phase: "failed" });
  }
}

async function handleOngoingPhotoInput(item: Dict, event: Event): Promise<void> {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = "";
  await addOngoingPhotoFiles(item, files, "请选择不超过 8MB 的图片文件");
}

async function handleOngoingPhotoPaste(item: Dict, event: ClipboardEvent): Promise<void> {
  const files = pastedImageFiles(event);
  if (!files.length) {
    rememberJob(ongoingLineKey(item), { text: "剪贴板中没有图片，请先复制图片后再粘贴", status: "failed", phase: "failed" });
    return;
  }
  event.preventDefault();
  await addOngoingPhotoFiles(item, files, "剪贴板图片不是有效图片或超过 8MB");
}

function removeOngoingPhoto(item: Dict, index: number): void {
  const key = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const images = ongoingExtraImages(item).slice();
  revokePhotoPreview(images[index]);
  images.splice(index, 1);
  edit.extra_images = images;
  ongoingEdits.set(key, edit);
}

function revokePhotoPreview(photo: Dict | undefined): void {
  const url = String(photo?.preview_url || "");
  if (url.startsWith("blob:")) {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // Best effort only; failing to revoke should not block user actions.
    }
  }
}

function revokeOngoingEditPreviews(edit: Dict | undefined): void {
  const images = Array.isArray(edit?.extra_images) ? edit.extra_images : [];
  for (const photo of images) revokePhotoPreview(photo);
}

function deleteOngoingEdit(key: string): void {
  if (!key) return;
  revokeOngoingEditPreviews(ongoingEdits.get(key));
  ongoingEdits.delete(key);
}

function clearOngoingEdits(): void {
  for (const edit of ongoingEdits.values()) revokeOngoingEditPreviews(edit);
  ongoingEdits.clear();
}

function ongoingExtraImagesForUpload(item: Dict): Dict[] {
  return ongoingExtraImages(item).map((photo) => {
    const { preview_url: _previewUrl, data_url: _dataUrl, ...rest } = photo || {};
    return rest;
  });
}

function buildOngoingPayload(item: Dict, action: string): Dict {
  const edit = ongoingDraft(item);
  const targetRecordId = draftValue(edit, "target_record_id", targetRecordIdForOngoing(item));
  const sourceRecordId = draftValue(edit, "source_record_id", sourceRecordIdForOngoing(item, targetRecordId));
  const workType = item.work_type || "maintenance";
  const simpleManual = ["power", "polling", "adjust"].includes(workType);
  const startTime = draftValue(edit, "start_time", ongoingTimeRange(item).start);
  const endTime = draftValue(edit, "end_time", ongoingTimeRange(item).end);
  const syncMaintenanceTarget = sourceWorkTypeForRecord(item) === "maintenance"
    && workType === "change"
    && edit.sync_maintenance_target !== false;
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: workType,
    notice_type: item.notice_type || "",
    manual: simpleManual ? true : Boolean(item.manual),
    manual_id: simpleManual ? String(item.manual_id || item.active_item_id || targetRecordId || sourceRecordId || ongoingLineKey(item)) : "",
    record_id: targetRecordId,
    target_record_id: targetRecordId,
    active_item_id: item.active_item_id || "",
    source_record_id: sourceRecordId,
    source_work_type: String(item.source_work_type || item.converted_from_work_type || workType),
    converted_from_work_type: String(item.converted_from_work_type || ""),
    sync_maintenance_target: syncMaintenanceTarget,
    paired_maintenance_target_record_id: syncMaintenanceTarget ? draftValue(edit, "paired_maintenance_target_record_id", item.paired_maintenance_target_record_id || "") : "",
    paired_maintenance_original_title: syncMaintenanceTarget ? draftValue(edit, "paired_maintenance_original_title", item.paired_maintenance_original_title || item.title || "") : "",
    paired_maintenance_actual_start_time: syncMaintenanceTarget ? draftValue(edit, "paired_maintenance_actual_start_time", item.paired_maintenance_actual_start_time || "") : "",
    title: draftValue(edit, "title", item.title || item.content || ""),
    specialty: cleanDisplayText(draftValue(edit, "specialty", item.specialty || "")),
    building: item.building || "",
    building_codes: Array.isArray(item.building_codes) ? item.building_codes : [],
    maintenance_cycle: draftValue(edit, "maintenance_cycle", item.maintenance_cycle || ""),
    level: draftValue(edit, "level", item.level || ""),
    start_time: startTime,
    end_time: endTime,
    location: draftValue(edit, "location", item.location || ""),
    content: draftValue(edit, "content", item.content || ""),
    reason: draftValue(edit, "reason", item.reason || ""),
    impact: draftValue(edit, "impact", item.impact || ""),
    progress: draftValue(edit, "progress", item.progress || ""),
    zhihang_involved: workType === "change" ? Boolean(edit.zhihang_involved) : false,
    zhihang_record_id: workType === "change" ? draftValue(edit, "zhihang_record_id", item.zhihang_record_id || "") : "",
    zhihang_title: workType === "change" ? draftValue(edit, "zhihang_title", item.zhihang_title || "") : "",
    zhihang_progress: workType === "change" ? draftValue(edit, "zhihang_progress", item.zhihang_progress || "") : "",
    repair_device: workType === "repair" ? draftValue(edit, "repair_device", item.repair_device || "") : "",
    repair_fault: workType === "repair" ? draftValue(edit, "repair_fault", item.repair_fault || "") : "",
    fault_type: workType === "repair" ? draftValue(edit, "fault_type", item.fault_type || "") : "",
    repair_mode: workType === "repair" ? draftValue(edit, "repair_mode", item.repair_mode || "") : "",
    discovery: workType === "repair" ? draftValue(edit, "discovery", item.discovery || "") : "",
    symptom: workType === "repair" ? draftValue(edit, "symptom", item.symptom || "") : "",
    solution: workType === "repair" ? draftValue(edit, "solution", item.solution || "") : "",
    spare_parts: workType === "repair" ? draftValue(edit, "spare_parts", item.spare_parts || "") : "",
    device: workType === "polling" ? draftValue(edit, "device", item.device || "") : "",
    cabinet: workType === "power" ? draftValue(edit, "cabinet", item.cabinet || "") : "",
    quantity: workType === "power" ? draftValue(edit, "quantity", item.quantity || "") : "",
    fault_time: workType === "repair" ? endTime : "",
    expected_time: workType === "repair" ? startTime : "",
    extra_images: ongoingExtraImagesForUpload(item),
    operation_id: opId(`${item.active_item_id || targetRecordId || sourceRecordId}:${action}`),
  };
}

function chooseCandidateInModal(candidates: Dict[], label: string, title: string): Promise<Dict | null> {
  if (!candidates.length) return Promise.resolve(null);
  if (candidates.length === 1) return Promise.resolve(candidates[0]);
  if (resolveOngoingBindSelection) closeOngoingBindSelection(null);
  const visibleCandidates = candidates.slice(0, 50);
  const firstId = changeTargetCandidateId(visibleCandidates[0]);
  ongoingBindSelection.value = {
    label,
    title,
    candidates: visibleCandidates,
  };
  selectedOngoingBindId.value = firstId;
  hoveredOngoingBindId.value = firstId;
  return new Promise((resolve) => {
    resolveOngoingBindSelection = resolve;
  });
}

async function bindOngoingTarget(item: Dict): Promise<void> {
  const lineKey = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const workType = String(item.work_type || "maintenance");
  const title = draftValue(edit, "title", item.title || item.content || "");
  if (!title) {
    rememberJob(lineKey, { text: "缺少标题，无法查找对应通告", status: "failed", phase: "failed" });
    return;
  }
  rememberJob(lineKey, { text: "正在查找对应通告", status: "busy", phase: "binding" });
  try {
    const data = await api(workType === "change" ? "/api/change-target-candidates" : "/api/notice-target-candidates", {
      method: "POST",
      body: JSON.stringify({
        work_type: workType,
        scope: currentScope.value || "ALL",
        title,
        start_time: draftValue(edit, "start_time", ongoingTimeRange(item).start),
        end_time: draftValue(edit, "end_time", ongoingTimeRange(item).end),
        action: "update",
      }),
    });
    const candidates = Array.isArray(data.candidates) ? data.candidates : [];
    const sourceCandidates = Array.isArray(data.source_candidates) ? data.source_candidates : [];
    const selected = await chooseCandidateInModal(
      candidates,
      workTypeLabel(workType),
      `找到 ${Number(data.total_matched ?? candidates.length)} 条同名已上传通告，当前显示 ${candidates.length} 条，请选择要继续处理的一条。`,
    );
    if (!selected) {
      rememberJob(lineKey, {
        text: candidates.length ? "已取消对应通告选择" : "未找到同名已上传通告",
        status: candidates.length ? "" : "failed",
        phase: candidates.length ? "" : "failed",
      });
      return;
    }
    let candidate = selected;
    if (workType === "change") {
      const confirmed = await api("/api/change-target-candidates/confirm", {
        method: "POST",
        body: JSON.stringify({
          scope: currentScope.value || "ALL",
          title,
          start_time: draftValue(edit, "start_time", ongoingTimeRange(item).start),
          end_time: draftValue(edit, "end_time", ongoingTimeRange(item).end),
          action: "update",
          record_id: changeTargetCandidateId(selected),
        }),
      });
      candidate = confirmed.candidate || selected;
    }
    const source = sourceCandidates[0] || {};
    const next = applyChangeTargetCandidateDefaults({ ...edit }, candidate);
    next.target_record_id = candidate.record_id || candidate.target_record_id || "";
    if (source.source_record_id || source.record_id) {
      next.source_record_id = source.source_record_id || source.record_id;
    }
    ongoingEdits.set(lineKey, next);
    rememberJob(lineKey, { text: "已选择对应通告，可继续更新或结束", status: "success", phase: "bound" });
  } catch (error: any) {
    rememberJob(lineKey, { text: error?.message || "选择对应通告失败", status: "failed", phase: "failed" });
  }
}

async function sendOngoing(item: Dict, action: string): Promise<void> {
  const key = ongoingLineKey(item);
  const payload = buildOngoingPayload(item, action);
  const missingUpload = missingUploadFields(String(payload.work_type || item.work_type || ""), payload);
  if (missingUpload.length) {
    rememberJob(key, { text: uploadFieldsMissingText(String(payload.work_type || item.work_type || ""), missingUpload), status: "failed", phase: "failed" });
    return;
  }
  if (!validatePayloadDuration(payload, key)) return;
  if (action === "end" && ongoingEndRequiresSitePhoto(item) && !ongoingPhotoCount(item)) {
    rememberJob(key, { text: "结束通告前必须添加至少一张现场照片", status: "failed", phase: "failed" });
    return;
  }
  await sendAction(payload, key);
}

async function sendAction(payload: Dict, lineKey: string): Promise<void> {
  try {
    rememberJob(lineKey, { text: "已受理，正在处理", status: "busy", phase: "accepted" });
    const data = await api("/api/workbench-actions", { method: "POST", body: JSON.stringify(payload) });
    rememberJob(lineKey, {
      job_id: data.job_id,
      payload,
      text: payloadSendsPersonalMessage(payload) ? "已受理，正在发送飞书消息" : "已受理，等待主界面显示",
      status: "busy",
      phase: data.initial_phase || "accepted",
    });
    watchJob(data.job_id, lineKey);
  } catch (error: any) {
    rememberJob(lineKey, { text: friendlyFailureText(error, "提交失败"), status: "failed", phase: "failed" });
  }
}

function rememberJob(key: string, patch: Dict): void {
  jobStates.set(key, { ...(jobStates.get(key) || {}), ...patch, updated_at: new Date().toISOString() });
}

function applySuccessfulJobState(lineKey: string): void {
  const state = jobStates.get(lineKey) || {};
  if (state.local_applied) return;
  const payload = state.payload || {};
  const action = String(payload.action || "").toLowerCase();
  if (selectedKeys.has(lineKey)) {
    selectedKeys.delete(lineKey);
    if (isManualKey(lineKey)) drafts.delete(lineKey);
    if (activeDraftKey.value === lineKey) activeDraftKey.value = "";
    saveDrafts();
  }
  if (action === "start") {
    bumpLocalSummary("started", 1);
    bumpLocalSummary("ongoing", 1);
    pendingFocusNotice.value = {
      action,
      work_type: payload.work_type || "",
      target_record_id: payload.target_record_id || payload.record_id || "",
      source_record_id: payload.source_record_id || "",
      title: payload.title || "",
    };
  } else if (action === "update") {
    bumpLocalSummary("updated", 1);
    pendingFocusNotice.value = {
      action,
      work_type: payload.work_type || "",
      target_record_id: payload.target_record_id || payload.record_id || "",
      source_record_id: payload.source_record_id || "",
      title: payload.title || "",
    };
  } else if (action === "end") {
    bumpLocalSummary("ended", 1);
    if (removeOngoingLine(lineKey)) {
      // The row was already removed locally; no additional ongoing delta needed.
    } else {
      bumpLocalSummary("ongoing", -1);
    }
    syncText.value = "已结束，可在今日结束通告或近三天可回退中查看";
  }
  rememberJob(lineKey, { local_applied: true });
}

function handleTerminalJob(lineKey: string, phase: string): void {
  if (phase === "success") {
    applySuccessfulJobState(lineKey);
    scheduleWorkbenchReload(0);
  }
}

function jobText(key: string): string {
  return jobStates.get(key)?.text || "";
}

function jobClass(key: string): string {
  return jobStates.get(key)?.status || "";
}

function jobCopyText(key: string, fallback = ""): string {
  const state = jobStates.get(key) || {};
  return String(state.notice_text || state.copy_text || fallback || "").trim();
}

async function copyTextToClipboard(text: string): Promise<void> {
  const value = String(text || "").trim();
  if (!value) throw new Error("没有可复制的通告文本");
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "readonly");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  const ok = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!ok) throw new Error("复制失败，请手动选中文本复制");
}

async function copyJobNoticeText(key: string, fallback = ""): Promise<void> {
  try {
    await copyTextToClipboard(jobCopyText(key, fallback));
    syncText.value = "通告文本已复制";
  } catch (error: any) {
    syncText.value = error?.message || "复制失败";
  }
}

function copyOngoingNoticeText(item: Dict): void {
  copyJobNoticeText(ongoingLineKey(item), ongoingNoticePreviewText(item));
}

function isLineBusy(key: string): boolean {
  const phase = jobStates.get(key)?.phase || "";
  return Boolean(phase && !terminalPhase(phase));
}

function applyJobStatusToLine(lineKey: string, job: Dict): void {
  const { phase, status, text } = backendJobStatusPatch(job);
  const noticeText = String(job?.prepared?.text || job?.notice_text || job?.copy_text || "").trim();
  rememberJob(lineKey, {
    phase,
    status,
    text,
    notice_text: noticeText || jobStates.get(lineKey)?.notice_text || "",
  });
  if (terminalPhase(phase)) {
    clearFallbackPoll(lineKey);
    handleTerminalJob(lineKey, phase);
  }
}

function isJobRealtimeAvailable(): boolean {
  return sseConnected.value
    || Boolean(eventSource.value && eventSource.value.readyState !== EventSource.CLOSED);
}

function isJobStreamStarted(): boolean {
  return Boolean(jobStreamCoordinator || eventSource.value);
}

function isActiveItemsStreamStarted(): boolean {
  return Boolean(activeItemsStreamCoordinator || activeItemsEventSource.value);
}

function watchJob(jobId: string, lineKey: string): void {
  clearFallbackPoll(lineKey);
  if (isJobRealtimeAvailable()) {
    const delay = sseConnected.value ? 15000 : 6000;
    const timer = window.setTimeout(() => {
      fallbackPollTimers.delete(lineKey);
      if (!isLineBusy(lineKey)) return;
      queueJobPoll(jobId, lineKey);
    }, delay);
    fallbackPollTimers.set(lineKey, timer);
    return;
  }
  queueJobPoll(jobId, lineKey);
}

function queueJobPoll(jobId: string, lineKey: string): void {
  if (!jobId || !lineKey || appDisposed) return;
  pollingJobs.set(lineKey, jobId);
  scheduleBatchJobPoll(0);
}

function scheduleBatchJobPoll(delayMs = 2000): void {
  if (appDisposed || batchPollTimer !== null || pollingJobs.size === 0) return;
  batchPollTimer = window.setTimeout(() => {
    batchPollTimer = null;
    void pollJobsBatch();
  }, Math.max(0, delayMs));
}

async function pollJobsBatch(): Promise<void> {
  if (batchPollActive || appDisposed || pollingJobs.size === 0) return;
  batchPollActive = true;
  try {
    const entries = Array.from(pollingJobs.entries());
    const ids = Array.from(new Set(entries.map(([, jobId]) => jobId).filter(Boolean))).slice(0, 100);
    if (!ids.length) return;
    try {
      const data = await api(`/api/jobs/batch?ids=${encodeURIComponent(ids.join(","))}`);
      const byId = new Map<string, Dict>();
      for (const item of Array.isArray(data.items) ? data.items : []) {
        if (item?.job_id) byId.set(String(item.job_id), item);
      }
      const missing = new Set<string>(Array.isArray(data.missing) ? data.missing.map(String) : []);
      const denied = new Set<string>(Array.isArray(data.denied) ? data.denied.map(String) : []);
      for (const [lineKey, jobId] of entries) {
        const job = byId.get(jobId);
        if (job) {
          applyJobStatusToLine(lineKey, job);
          continue;
        }
        if (missing.has(jobId) || denied.has(jobId)) {
          rememberJob(lineKey, {
            phase: "failed",
            status: "failed",
            text: denied.has(jobId) ? "无权查看该任务状态" : "任务状态已丢失，请刷新核对后重试",
          });
          clearFallbackPoll(lineKey);
        }
      }
    } catch {
      for (const key of pollingJobs.keys()) {
        rememberJob(key, { text: "任务处理中，等待状态更新", status: "busy" });
      }
    }
  } finally {
    batchPollActive = false;
    if (!appDisposed && pollingJobs.size > 0) scheduleBatchJobPoll(2000);
  }
}

function applyJobStreamPayload(job: Dict): void {
  if (!job?.job_id) return;
  for (const [key, value] of jobStates.entries()) {
    if (value.job_id === job.job_id) {
      applyJobStatusToLine(key, job);
    }
  }
}

function openDirectJobEventSource(coordinator: CrossTabStreamCoordinator<Dict> | null): void {
  closeDirectJobEventSource();
  if (appDisposed) return;
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
      applyJobStreamPayload(job);
      coordinator?.broadcast({ kind: "job", job });
    };
    source.onopen = () => {
      sseConnected.value = true;
      coordinator?.broadcast({ kind: "status", connected: true });
    };
    source.onmessage = handleJobEvent;
    source.addEventListener("job", handleJobEvent);
    source.addEventListener("error", (event: Event) => {
      if (handleSseAuthError(event)) return;
      scheduleSseAuthCheck();
      sseConnected.value = false;
      coordinator?.broadcast({ kind: "status", connected: false });
      if (eventSource.value === source && source.readyState === EventSource.CLOSED) {
        eventSource.value = null;
        if (!appDisposed && sseReconnectTimer === null && (!coordinator || coordinator.isLeader())) {
          sseReconnectTimer = window.setTimeout(() => {
            sseReconnectTimer = null;
            openDirectJobEventSource(coordinator);
          }, 5000);
        }
      }
    });
    eventSource.value = source;
  } catch {
    eventSource.value = null;
    sseConnected.value = false;
    coordinator?.broadcast({ kind: "status", connected: false });
  }
}

function startJobSse(): void {
  if (appDisposed || jobStreamCoordinator) return;
  const coordinator = createCrossTabStreamCoordinator<Dict>({
    channelName: "clipflow-jobs-stream-v1",
    leaderKey: "clipflow:jobs-stream-leader",
    heartbeatMs: streamLeaderHeartbeatMs,
    leaderTtlMs: streamLeaderTtlMs,
  });
  jobStreamCoordinator = coordinator;
  coordinator.start(
    (message) => {
      if (message.kind === "job") {
        applyJobStreamPayload(message.job || {});
      } else if (message.kind === "status") {
        sharedJobStreamAvailable.value = Boolean(message.connected);
      }
    },
    (leader) => {
      if (leader) {
        sharedJobStreamAvailable.value = false;
        openDirectJobEventSource(coordinator);
      } else {
        closeDirectJobEventSource();
        sseConnected.value = false;
        sharedJobStreamAvailable.value = coordinator.supported;
      }
    },
  );
}

function closeDirectActiveItemsEventSource(): void {
  if (activeItemsEventSource.value) activeItemsEventSource.value.close();
  activeItemsEventSource.value = null;
  if (activeItemsReconnectTimer !== null) {
    window.clearTimeout(activeItemsReconnectTimer);
    activeItemsReconnectTimer = null;
  }
}

function applyActiveItemsStreamPayload(payload: Dict, scope: string): void {
  if (String(payload.scope || "") !== scope) return;
  const signature = String(payload.display_signature || payload.scope_signature || "");
  if (!signature) return;
  if (!lastActiveItemsSignature) {
    lastActiveItemsSignature = signature;
    return;
  }
  if (signature !== lastActiveItemsSignature) {
    lastActiveItemsSignature = signature;
    scheduleWorkbenchReload(isUserEditing() ? 3000 : 250);
  }
}

function openDirectActiveItemsEventSource(
  scope: string,
  coordinator: CrossTabStreamCoordinator<Dict> | null,
): void {
  closeDirectActiveItemsEventSource();
  if (appDisposed || !isWorkbench.value || !scope) return;
  try {
    const source = new EventSource(`/api/qt-active-items/stream?scope=${encodeURIComponent(scope)}`);
    source.onopen = () => {
      activeItemsConnected.value = true;
      coordinator?.broadcast({ kind: "status", connected: true, scope });
    };
    source.addEventListener("qt_active_items", (event: MessageEvent) => {
      let payload: Dict;
      try {
        payload = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      applyActiveItemsStreamPayload(payload, scope);
      coordinator?.broadcast({ kind: "qt_active_items", payload, scope });
    });
    source.addEventListener("error", (event: Event) => {
      if (handleSseAuthError(event)) return;
      scheduleSseAuthCheck();
      activeItemsConnected.value = false;
      coordinator?.broadcast({ kind: "status", connected: false, scope });
      if (activeItemsEventSource.value === source && source.readyState === EventSource.CLOSED) {
        activeItemsEventSource.value = null;
        if (
          !appDisposed
          && isWorkbench.value
          && activeItemsReconnectTimer === null
          && (!coordinator || coordinator.isLeader())
        ) {
          activeItemsReconnectTimer = window.setTimeout(() => {
            activeItemsReconnectTimer = null;
            openDirectActiveItemsEventSource(scope, coordinator);
          }, 5000);
        }
      }
    });
    activeItemsEventSource.value = source;
  } catch {
    activeItemsEventSource.value = null;
    activeItemsConnected.value = false;
    coordinator?.broadcast({ kind: "status", connected: false, scope });
  }
}

function startActiveItemsSse(): void {
  if (appDisposed || !isWorkbench.value || !currentScope.value) return;
  const scope = currentScope.value || "ALL";
  if (activeItemsStreamCoordinator && activeItemsStreamScope === scope) return;
  stopActiveItemsSse();
  lastActiveItemsSignature = "";
  activeItemsStreamScope = scope;
  const coordinator = createCrossTabStreamCoordinator<Dict>({
    channelName: `clipflow-active-items-stream-v1:${scope}`,
    leaderKey: `clipflow:active-items-stream-leader:${scope}`,
    heartbeatMs: streamLeaderHeartbeatMs,
    leaderTtlMs: streamLeaderTtlMs,
  });
  activeItemsStreamCoordinator = coordinator;
  coordinator.start(
    (message) => {
      if (String(message.scope || scope) !== scope) return;
      if (message.kind === "qt_active_items") {
        applyActiveItemsStreamPayload(message.payload || {}, scope);
      } else if (message.kind === "status") {
        sharedActiveItemsStreamAvailable.value = Boolean(message.connected);
      }
    },
    (leader) => {
      if (leader) {
        sharedActiveItemsStreamAvailable.value = false;
        openDirectActiveItemsEventSource(scope, coordinator);
      } else {
        closeDirectActiveItemsEventSource();
        activeItemsConnected.value = false;
        sharedActiveItemsStreamAvailable.value = coordinator.supported;
      }
    },
  );
}

function stopActiveItemsSse(): void {
  if (activeItemsStreamCoordinator) {
    activeItemsStreamCoordinator.stop();
    activeItemsStreamCoordinator = null;
  }
  closeDirectActiveItemsEventSource();
  activeItemsConnected.value = false;
  sharedActiveItemsStreamAvailable.value = false;
  activeItemsUpdatePending.value = false;
  lastActiveItemsSignature = "";
  activeItemsStreamScope = "";
}

function requestActionConfirm(options: {
  tone?: ActionConfirmTone;
  kicker?: string;
  title: string;
  message: string;
  details?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
}): Promise<boolean> {
  return new Promise((resolve) => {
    const tone = options.tone || "primary";
    Object.assign(actionConfirm, {
      open: true,
      tone,
      kicker: options.kicker || "操作确认",
      title: options.title,
      message: options.message,
      details: Array.isArray(options.details) ? options.details : [],
      confirmLabel: options.confirmLabel || "确认",
      cancelLabel: options.cancelLabel || "取消",
      confirmClass: tone === "danger" ? "danger" : tone === "warning" ? "green" : "blue",
      resolve,
    });
  });
}

function resolveActionConfirm(confirmed: boolean): void {
  const resolver = actionConfirm.resolve;
  Object.assign(actionConfirm, {
    open: false,
    title: "",
    message: "",
    details: [],
    resolve: undefined,
  });
  resolver?.(confirmed);
}

async function deleteOngoing(item: Dict): Promise<void> {
  const key = ongoingLineKey(item);
  const targetRecordId = targetRecordIdForOngoing(item);
  const sourceRecordId = sourceRecordIdForOngoing(item, targetRecordId);
  const confirmed = await requestActionConfirm({
    tone: "danger",
    kicker: "删除进行中通告",
    title: ongoingTitle(item),
    message: "确认删除这条进行中通告？",
    details: [
      "会同步移除前端和 Qt 界面的进行中条目。",
      targetRecordId ? "会同步删除对应已上传记录。" : "该条暂未找到已上传记录，只移除页面显示。",
      "删除后可在近三天可回退中恢复。",
    ],
    confirmLabel: "确认删除",
  });
  if (!confirmed) return;
  try {
    rememberJob(key, { text: "删除中", status: "busy", phase: "deleting" });
    await api("/api/ongoing-items/delete", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        work_type: item.work_type || "maintenance",
        notice_type: item.notice_type || "",
        active_item_id: item.active_item_id || "",
        record_id: targetRecordId,
        target_record_id: targetRecordId,
        source_record_id: sourceRecordId,
        title: item.title || item.content || "",
        building: item.building || "",
        building_codes: Array.isArray(item.building_codes) ? item.building_codes : [],
      }),
    });
    removeOngoingLine(key);
    rememberJob(key, { text: "已删除", status: "success", phase: "success" });
    await loadWorkbench();
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "删除失败", status: "failed", phase: "failed" });
  }
}

async function applyUndo(item: Dict): Promise<void> {
  const undoId = String(item.undo_id || "").trim();
  if (!undoId) return;
  const key = undoLineKey(item);
  if (isLineBusy(key)) return;
  const title = String(item.title || ongoingTitle(item) || "该通告").trim();
  const label = String(item.undo_label || "回退上一步").trim();
  const confirmed = await requestActionConfirm({
    tone: "warning",
    kicker: "回退通告操作",
    title,
    message: `确认执行${label}？`,
    details: [
      "会同步恢复页面和桌面显示。",
      "会刷新前端和 Qt 展示。",
      "会按回退记录恢复或重建对应已上传记录。",
    ],
    confirmLabel: label,
  });
  if (!confirmed) return;
  try {
    rememberJob(key, { text: "已受理，正在回退", status: "busy", phase: "undo_queued" });
    const data = await api(`/api/notice-undo/${encodeURIComponent(undoId)}/apply`, {
      method: "POST",
      body: JSON.stringify({ scope: currentScope.value || "ALL" }),
    });
    rememberJob(key, {
      job_id: data.job_id,
      payload: { action: "undo", undo_id: undoId },
      text: "已受理，正在回退",
      status: "busy",
      phase: data.initial_phase || "undo_queued",
    });
    watchJob(data.job_id, key);
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "回退失败", status: "failed", phase: "failed" });
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
  const boundText = item.binding_status === "needs_binding" ? "待选择对应通告" : "";
  return [
    item.building,
    item.specialty,
    item.maintenance_cycle,
    todayProgressTextForOngoing(item),
    item.time_str || item.start_time,
    boundText,
  ].filter(Boolean).join(" · ");
}

function todayProgressTextForOngoing(item: Dict): string {
  const noticeType = String(item.notice_type || "").trim();
  const work = String(item.work_type || "").trim();
  if (work !== "change" && !["设备变更", "变更通告"].includes(noticeType)) return "";
  const value = String(item.today_in_progress_state || item["今日是否进行"] || "").trim().toLowerCase();
  if (["yes", "是", "在进行"].includes(value)) return "今日：在进行";
  if (["no", "否", "未进行"].includes(value)) return "今日：未进行";
  return "今日：未确认";
}

function ongoingCompactSummary(item: Dict): string {
  const edit = ongoingEdits.get(ongoingLineKey(item)) || {};
  const progress = draftValue(edit, "progress", item.progress || "");
  const location = draftValue(edit, "location", item.location || "");
  return [location, progress].filter(Boolean).join(" · ");
}

async function refreshRepair(): Promise<void> {
  if (repairRefreshing.value || refreshCooldown.repair) return;
  refreshMenuOpen.value = false;
  startRefreshCooldown("repair");
  repairRefreshing.value = true;
  syncText.value = "正在读取最新检修数据，完成后全楼可见";
  try {
    await api(`/api/repair-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
    syncText.value = "检修已刷新，页面已更新";
  } catch (error: any) {
    syncText.value = `刷新检修失败，当前显示上次成功数据：${error?.message || "请稍后再试"}`;
  } finally {
    repairRefreshing.value = false;
  }
}

async function refreshChange(): Promise<void> {
  if (changeRefreshing.value || refreshCooldown.change) return;
  refreshMenuOpen.value = false;
  startRefreshCooldown("change");
  changeRefreshing.value = true;
  syncText.value = "正在读取最新变更数据，完成后全楼可见";
  try {
    await api(`/api/change-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
    syncText.value = "变更已刷新，页面已更新";
  } catch (error: any) {
    syncText.value = `刷新变更失败，当前显示上次成功数据：${error?.message || "请稍后再试"}`;
  } finally {
    changeRefreshing.value = false;
  }
}

function refreshEvent(): void {
  if (eventRefreshing.value || refreshCooldown.event) return;
  refreshMenuOpen.value = false;
  startRefreshCooldown("event");
  eventRefreshNonce.value += 1;
}

async function logout(): Promise<void> {
  clearAuthKeepalive();
  stopRealtimeConnections();
  await api("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
  window.location.href = "/";
}

async function submitPermissionRequest(): Promise<void> {
  if (!permissionPanelRequestableScopes.value.length) {
    permissionRequest.message = "当前没有可申请的楼栋权限。";
    return;
  }
  permissionBusy.value = true;
  try {
    const data = await api("/api/auth/permission-requests", {
      method: "POST",
      body: JSON.stringify({ scopes: permissionRequest.scopes, reason: permissionRequest.reason }),
    });
    permissionRequest.requestId = data.request_id || data.request?.request_id || "";
    permissionRequest.status = data.status || data.request?.status || "pending";
    permissionRequest.rejectReason = data.reject_reason || data.request?.reject_reason || "";
    permissionRequest.message = "申请已提交，等待管理员审批。";
  } catch (error: any) {
    permissionRequest.message = error?.message || "提交失败";
  } finally {
    permissionBusy.value = false;
  }
}

function updatePermissionRequest(patch: Partial<typeof permissionRequest>): void {
  Object.assign(permissionRequest, patch);
}

async function openAdditionalPermissionRequest(): Promise<void> {
  Object.assign(permissionRequest, {
    scopes: [],
    reason: "",
    code: "",
    requestId: "",
    message: "",
    status: "",
    rejectReason: "",
  });
  showPermissionRequestPanel.value = true;
  await loadCurrentPermissionRequest();
}

function closePermissionRequestPanel(): void {
  showPermissionRequestPanel.value = false;
  Object.assign(permissionRequest, {
    scopes: [],
    reason: "",
    code: "",
    requestId: "",
    message: "",
    status: "",
    rejectReason: "",
  });
  syncText.value = "请选择功能";
}

async function confirmPermissionRequest(): Promise<void> {
  permissionBusy.value = true;
  try {
    await api("/api/auth/permission-requests/confirm", {
      method: "POST",
      body: JSON.stringify({ request_id: permissionRequest.requestId, code: permissionRequest.code }),
    });
    await loadAuthStatus();
    if (auth.scopeOptions.length) {
      await Promise.all([loadOverview(), loadHandoverLinks()]);
      isWorkbench.value = false;
      showPermissionRequestPanel.value = false;
      Object.assign(permissionRequest, { scopes: [], reason: "", code: "", requestId: "", message: "", status: "", rejectReason: "" });
      syncText.value = "请选择功能";
      if (!isJobStreamStarted()) startJobSse();
    }
  } catch (error: any) {
    permissionRequest.message = error?.message || "授权确认失败";
  } finally {
    permissionBusy.value = false;
  }
}

watch(workType, () => {
  activeDraftKey.value = "";
  ongoingTypeFilter.value = "all";
});

onMounted(async () => {
  appDisposed = false;
  pageVisible.value = !document.hidden;
  document.addEventListener("visibilitychange", handleVisibilityChange);
  window.addEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  if (isSignaturePage.value) {
    authChecking.value = false;
    syncText.value = "";
    void loadAuthStatus({ silent: true }).catch(() => null);
    return;
  }
  await loadAuthStatus();
  if (auth.loggedIn && !auth.scopeOptions.length) {
    await loadCurrentPermissionRequest();
  }
  if (auth.loggedIn && auth.scopeOptions.length) {
    await Promise.all([loadOverview(), loadHandoverLinks()]);
    if (isEventPage.value && !currentScope.value) {
      currentScope.value = normalizeScopeValue(visibleScopeOptions.value[0]?.value || "ALL", "ALL");
      const url = new URL(window.location.href);
      url.searchParams.set("scope", currentScope.value);
      url.searchParams.set("mode", "events");
      window.history.replaceState({}, "", url);
    }
    if (currentScope.value && isEventPage.value) {
      isWorkbench.value = false;
      syncText.value = "正在读取事件数据";
    } else if (currentScope.value && !isEngineerMopPage.value && !isHistoryMemoryPage.value) {
      isWorkbench.value = true;
      loadDrafts();
      await loadWorkbench();
      startActiveItemsSse();
    } else {
      syncText.value = "请选择功能";
    }
    startJobSse();
  }
});

onBeforeUnmount(() => {
  appDisposed = true;
  document.removeEventListener("visibilitychange", handleVisibilityChange);
  window.removeEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  clearAuthKeepalive();
  clearWorkbenchRetry();
  stopRealtimeConnections();
  if (workbenchRefreshTimer !== null) window.clearTimeout(workbenchRefreshTimer);
  for (const timer of refreshCooldownTimers.values()) window.clearTimeout(timer);
  refreshCooldownTimers.clear();
  for (const timer of fallbackPollTimers.values()) window.clearTimeout(timer);
  fallbackPollTimers.clear();
  if (batchPollTimer !== null) {
    window.clearTimeout(batchPollTimer);
    batchPollTimer = null;
  }
  if (realtimeWarningTimer !== null) {
    window.clearTimeout(realtimeWarningTimer);
    realtimeWarningTimer = null;
  }
  pollingJobs.clear();
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

.app-shell.signature-link-shell {
  min-height: 100dvh;
  overflow: hidden;
}

h2,
p {
  margin: 0;
}

.row-actions,
.card-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
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

.workbench {
  padding: 16px;
}

.loading-line {
  margin-bottom: 12px;
  padding: 9px 12px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 13px;
}

.panel,
.paste-panel {
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.paste-panel {
  margin-bottom: 12px;
  padding: 10px;
}

.draft-save-status {
  margin-left: auto;
  padding: 6px 9px;
  border: 1px solid #dbe3ee;
  border-radius: 999px;
  background: #f8fafc;
  color: #64748b;
  font-size: 12px;
  white-space: nowrap;
}

.draft-save-status.failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #991b1b;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(280px, 0.88fr) minmax(430px, 1.35fr) minmax(320px, 0.95fr);
  gap: 12px;
  min-height: calc(100vh - 230px);
}

.workbench-flow-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 6px;
  padding: 6px;
  border: 1px solid rgba(216, 229, 247, 0.88);
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.56);
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.04);
}

.workbench-flow-strip article {
  min-width: 0;
  display: grid;
  grid-template-columns: 24px minmax(0, 1fr);
  align-items: center;
  gap: 7px;
  padding: 6px 9px;
  border: 1px solid rgba(216, 229, 247, 0.78);
  border-radius: 13px;
  background: rgba(248, 251, 255, 0.82);
  color: #64748b;
}

.workbench-flow-strip article.active {
  border-color: #9cc7ff;
  background: linear-gradient(135deg, #ffffff, #eef6ff);
  box-shadow: inset 0 0 0 1px rgba(30, 99, 255, 0.12);
}

.workbench-flow-strip b {
  width: 24px;
  height: 24px;
  display: grid;
  place-items: center;
  border-radius: 9px;
  background: #eaf3ff;
  color: #1d4ed8;
  font-size: 11px;
  font-weight: 950;
}

.workbench-flow-strip article.active b {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
}

.workbench-flow-strip span {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.workbench-flow-strip strong,
.workbench-flow-strip small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.workbench-flow-strip strong {
  color: #0b1f3f;
  font-size: 12px;
  font-weight: 950;
}

.workbench-flow-strip small {
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.panel {
  min-width: 0;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.panel-head {
  position: sticky;
  top: 0;
  z-index: 2;
  margin: -12px -12px 0;
  padding: 12px 12px 8px;
  border-bottom: 1px solid #eef2f7;
  background: rgba(255, 255, 255, 0.96);
  backdrop-filter: blur(6px);
}

.panel-head h2 {
  font-size: 17px;
}

.panel-head.compact {
  position: static;
  margin: 4px 0 0;
  padding: 10px 0 6px;
  backdrop-filter: none;
}

.panel-head.compact h3 {
  font-size: 14px;
}

.panel-head span {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}

.panel-head-actions {
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
  min-width: 0;
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
  padding: 7px 9px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

textarea {
  min-height: 58px;
  resize: vertical;
}

.card-actions {
  margin-top: 10px;
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
  min-height: 140px;
  padding: 18px;
  color: #64748b;
  text-align: center;
  line-height: 1.7;
  background: #f8fafc;
  border-radius: 6px;
}

.paste-panel textarea {
  min-height: 100px;
}

@media (max-width: 1120px) {
  .workbench-flow-strip {
    grid-template-columns: 1fr;
  }

  .workspace {
    grid-template-columns: 1fr;
  }

  .panel {
    min-height: 360px;
  }
}

.center-state,
.panel,
.paste-panel {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--panel);
  box-shadow: var(--shadow);
}

.center-state {
  margin-top: 48px;
  padding: 34px;
}

.paste-panel {
  margin-bottom: 16px;
  padding: 14px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 32px rgba(22, 78, 151, 0.08);
}

.workspace {
  gap: 18px;
}

.panel {
  position: relative;
  overflow: hidden;
  padding: 14px;
}

.panel-head {
  margin: -14px -14px 0;
  padding: 14px 16px 10px;
  border-bottom-color: #e7f0fb;
  border-radius: 14px 14px 0 0;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.96));
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.04);
}

.panel-head h2 {
  color: #09204a;
  font-size: 18px;
  font-weight: 800;
}

.panel-head h2::before {
  content: "";
  display: inline-block;
  width: 4px;
  height: 17px;
  margin-right: 8px;
  border-radius: 999px;
  vertical-align: -3px;
  background: linear-gradient(180deg, var(--brand-blue), var(--brand-cyan));
  box-shadow: 0 6px 14px rgba(22, 120, 255, 0.18);
}

.panel-head span,
.card-title span {
  background: #eaf3ff;
  color: #0757d7;
  font-weight: 700;
}

.empty-block,
.virtual-list {
  background: #f7fbff;
}

/* Rounded VNET polish pass */
.center-state,
.panel,
.paste-panel,
.empty-block {
  border-radius: var(--radius-panel);
}

.panel-head {
  border-radius: var(--radius-panel) var(--radius-panel) 0 0;
}

/* Softer text integration for graphic surfaces */
.panel-head span,
.card-title span {
  letter-spacing: 0;
}

.panel-head h2 {
  font-weight: 820;
  letter-spacing: 0;
}

.panel-head span,
.card-title span {
  border: 1px solid #d8e7f8;
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-weight: 720;
}

.paste-panel {
  border-color: rgba(207, 224, 255, 0.92);
  background: rgba(255, 255, 255, 0.9);
  box-shadow: var(--shadow);
  backdrop-filter: blur(10px);
}

.panel {
  border-color: rgba(207, 224, 255, 0.92);
  box-shadow: var(--shadow);
}

.panel-head {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.98)),
    linear-gradient(90deg, rgba(48, 128, 255, 0.06), transparent 42%);
}

.panel-head h2 {
  font-size: 17px;
}

.panel-head span,
.card-title span {
  border-color: #d8e7f8;
  border-radius: 999px;
  background: rgba(239, 246, 255, 0.9);
  color: #155dfc;
}

.empty-block,
.virtual-list {
  background: rgba(248, 251, 255, 0.94);
}

/* VNET blue-white production skin */
/* Panorama construction-management command-center skin */
.app-shell {
  --brand-blue: #1e63ff;
  --brand-blue-2: #005bff;
  --brand-blue-dark: #012a7d;
  --brand-cyan: #00b7d7;
  --ink: #0f172a;
  --muted: #64748b;
  --line: #d8e5f7;
  --panel: rgba(255, 255, 255, 0.86);
  --panel-soft: rgba(255, 255, 255, 0.72);
  --shadow: 0 14px 34px rgba(0, 47, 135, 0.1);
  --shadow-strong: 0 24px 64px rgba(0, 47, 135, 0.18);
  --radius-panel: 22px;
  --radius-card: 18px;
  --radius-control: 14px;
  background: linear-gradient(180deg, #eef4ff 0, #f8fbff 44%, #eef5ff 100%);
}

.workbench {
  width: min(1800px, 100%);
  margin: 0 auto;
  padding: 28px 32px 42px;
}

.panel,
.paste-panel {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.panel-head {
  background: rgba(255, 255, 255, 0.86);
}

.panel-head h2 {
  font-size: 16px;
  font-weight: 650;
}

.panel-head h2::before {
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
}

.panel-head span,
.card-title span,
.draft-save-status {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
}

/* Final VNET rounded controls: keep all operational controls visually consistent. */
.btn,
button,
a.btn {
  min-height: 42px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.94);
  color: #0f4fb8;
  font-weight: 850;
  letter-spacing: 0;
  box-shadow: 0 8px 18px rgba(15, 86, 228, 0.06);
  transition:
    transform 0.16s ease,
    box-shadow 0.16s ease,
    border-color 0.16s ease,
    background 0.16s ease;
}

.btn:hover:not(:disabled),
button:hover:not(:disabled),
a.btn:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  color: #075bd8;
  transform: translateY(-1px);
  box-shadow: 0 12px 26px rgba(15, 86, 228, 0.1);
}

.btn:disabled,
button:disabled,
.btn[aria-disabled="true"] {
  cursor: not-allowed;
  color: #8aa0ba;
  background: #f3f7fc;
  box-shadow: none;
  transform: none;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(30, 99, 255, 0.24);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #12b981, #059669);
  color: #ffffff;
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #f43f5e, #e11d48);
  color: #ffffff;
}

input,
select,
textarea {
  min-height: 42px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.96);
  color: #0f172a;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.92);
}

textarea {
  min-height: 88px;
}

select {
  padding-right: 34px;
  cursor: pointer;
}

.paste-panel,
.panel,
.empty-block {
  border-radius: 22px;
}

.panel-head {
  border-radius: 22px 22px 0 0;
}

</style>
