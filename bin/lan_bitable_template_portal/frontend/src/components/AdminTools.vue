<template>
  <UiTransition name="ui-page" appear>
  <section v-if="open" class="admin-shell">
    <div class="admin-card">
      <header>
        <div>
          <h1>{{ systemAdmin ? '管理员设置' : '账号与权限' }}</h1>
        </div>
        <button type="button" class="btn ghost" @click="$emit('close')"><ArrowLeft :size="17" />返回首页</button>
      </header>

      <div class="admin-settings-layout" :class="{ 'accounts-only': !systemAdmin }">
      <AdminWorkspaceChrome
        v-if="systemAdmin"
        :tabs="adminTabs"
        :active-tab="tab"
        :advanced-visible="advancedDiagnosticsVisible"
        @select-tab="selectAdminTab"
        @toggle-advanced="toggleAdvancedDiagnostics"
      />

      <div class="admin-settings-content">
      <MessageBanner v-if="message" :text="message" />
      <ConfirmDialog
        :open="confirmDialog.open"
        :tone="confirmDialog.tone"
        :kicker="confirmDialog.kicker"
        :title="confirmDialog.title"
        :message="confirmDialog.message"
        :details="confirmDialog.details"
        :confirm-label="confirmDialog.confirmLabel"
        :cancel-label="confirmDialog.cancelLabel"
        :confirm-class="confirmDialog.confirmClass"
        @resolve="resolveConfirm"
      />

      <AdminAccounts v-if="!systemAdmin" :pending-count="0" :permissions="[]" />

      <AdminStatusPane
        v-else-if="tab === 'status'"
        :stats="stats"
        :perf="perf"
        :queues="queues"
        :consistency="consistency"
        v-model:notice-diagnostic-query="noticeDiagnosticQuery"
        :notice-diagnostic="noticeDiagnostic"
        :notice-diagnostic-loading="noticeDiagnosticLoading"
        :recent-jobs="recentJobs"
        :operation-audits="operationAudits"
        :busy="busy"
        @refresh="loadStatus"
        @run-notice-diagnostic="runNoticeDiagnostic"
        @repair-notice-projection="repairNoticeProjection"
        @open-history-memory="openHistoryMemory"
        @preflight="runPreflight"
        @cleanup="cleanupJobs"
        @mark-stuck-failed="markStuckFailed"
        @retry-job="retryJob"
        @clear-job="clearJob"
      />

      <AdminPermissionPane
        v-else-if="tab === 'permissions'"
        v-model:request-search="permissionRequestSearch"
        v-model:request-status="permissionRequestStatus"
        v-model:reject-reason="permissionRejectReason"
        :busy="busy"
        :pending-request-count="pendingPermissionRequestCount"
        :request-items="filteredPermissionRequests"
        :scope-options="scopeOptions"
        :selected-request-ids="selectedRequestIds"
        :all-filtered-pending-selected="allFilteredPendingSelected"
        :selected-pending-count="selectedPendingRequestIds.length"
        :request-status-label="permissionRequestStatusLabel"
        :request-scope-labels="requestScopeLabels"
        :current-scope-labels="currentScopeLabels"
        :review-scopes="reviewScopes"
        :users="permissions.users"
        @load-permissions="loadPermissions"
        @load-requests="loadPermissionRequests"
        @approve-selected="approveSelectedPermissionRequests"
        @reject-selected="rejectSelectedPermissionRequests"
        @toggle-all="toggleAllFilteredPermissionRequests"
        @toggle-selection="togglePermissionRequestSelection"
        @toggle-review-scope="toggleReviewScope"
        @approve="approvePermissionRequest"
        @reject="rejectPermissionRequest"
      />

      <AdminHandoverPane
        v-else-if="tab === 'handover'"
        :building-scopes="buildingScopes"
        :links="handoverLinks"
        :password="handoverPassword"
        :busy="busy"
        @refresh="loadHandover"
        @save="saveHandover"
        @update-link="updateHandoverLink"
        @update-password="updateHandoverPassword"
      />

      <AdminMopSettingsPane
        v-else-if="tab === 'mop'"
        :settings="mopSettings"
        :busy="busy"
        @refresh="loadMopSettings"
        @save="saveMopSettings"
        @update-field="updateMopSettingField"
      />

      <FeishuAssistantSettings v-else-if="tab === 'feishu'" />
      <ReleaseJournalPage v-else-if="tab === 'development'" />

      <AdminPressurePane
        v-else
        :pressure="pressure"
        :pressure-result="pressureResult"
        :busy="busy"
        @update-pressure="updatePressureField"
        @run="runMockPressure"
      />
      </div>
      </div>
    </div>
  </section>
  </UiTransition>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, reactive, ref, watch } from "vue";
import { ArrowLeft } from 'lucide-vue-next';
import { requestJson } from "../api/client";
import {
  adminBuildingScopes,
  cleanupRemovedTotal,
  scopeOptionLabel as resolveScopeOptionLabel,
  shortId,
  type AdminTabKey,
} from "../adminToolsUtils";
import { navigate } from "../navigation";
import AdminHandoverPane from "./AdminHandoverPane.vue";
import AdminMopSettingsPane from "./AdminMopSettingsPane.vue";
import FeishuAssistantSettings from "./FeishuAssistantSettings.vue";
import AdminPermissionPane from "./AdminPermissionPane.vue";
import AdminAccounts from "./AdminAccounts.vue";
import AdminPressurePane from "./AdminPressurePane.vue";
import AdminStatusPane from "./AdminStatusPane.vue";
import AdminWorkspaceChrome from "./AdminWorkspaceChrome.vue";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
const ReleaseJournalPage = defineAsyncComponent(() => import('./ReleaseJournalPage.vue'));

type Dict = Record<string, any>;
type MopSettingsKey = "mop_app_token" | "mop_table_id" | "mop_view_id" | "mop_title_field" | "mop_attachment_field";

const props = defineProps<{
  open: boolean;
  systemAdmin?: boolean;
  initialTab?: AdminTabKey;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

const emit = defineEmits<{
  close: [];
  'select-tab': [tab: AdminTabKey];
}>();

const buildingScopes = adminBuildingScopes;

const tab = ref<AdminTabKey>("permissions");
const busy = ref(false);
const message = ref("");
const stats = ref<Dict>({});
const perf = ref<Dict>({});
const queues = ref<Dict>({});
const consistency = ref<Dict>({});
const noticeDiagnosticQuery = ref("");
const noticeDiagnostic = ref<Dict>({});
const noticeDiagnosticLoading = ref(false);
const recentJobs = ref<Dict>({});
const operationAudits = ref<Dict>({});
const permissions = reactive<{ users: Dict[]; scope_options: Dict[] }>({ users: [], scope_options: [] });
const permissionRequests = ref<Dict[]>([]);
const permissionRequestStatus = ref("pending");
const permissionRequestSearch = ref("");
const permissionRejectReason = ref("");
const selectedRequestIds = reactive(new Set<string>());
const advancedDiagnosticsVisible = ref(false);
const handoverLinks = reactive<Record<string, string>>({});
const handoverPassword = ref("");
const mopSettings = reactive({
  mop_app_token: "",
  mop_table_id: "",
  mop_view_id: "",
  mop_title_field: "文件名",
  mop_attachment_field: "文件",
});
const mopSettingsLoaded = ref(false);
const pressure = reactive({
  count: 60,
  concurrency: 10,
  scenario: "accepted",
  include_site_photos: false,
  site_photo_count: 1,
  site_photo_kb: 32,
  max_submit_average_ms: 300,
  max_total_seconds: 20,
  max_failed: 0,
});
const pressureResult = ref<Dict>({});
const confirmDialog = reactive({
  open: false,
  tone: "danger" as "danger" | "warning" | "primary",
  kicker: "",
  title: "",
  message: "",
  details: [] as string[],
  confirmLabel: "确认",
  cancelLabel: "取消",
  confirmClass: "danger",
  resolve: undefined as undefined | ((confirmed: boolean) => void),
});

const filteredPermissionRequests = computed(() => {
  const query = permissionRequestSearch.value.trim().toLowerCase();
  const items = permissionRequests.value || [];
  if (!query) return items;
  return items.filter((item) => {
    const haystack = [
      item.name,
      item.open_id,
      item.reason,
      item.status,
      ...(Array.isArray(item.requested_scope_labels) ? item.requested_scope_labels : []),
      ...(Array.isArray(item.current_scope_labels) ? item.current_scope_labels : []),
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });
});
const pendingPermissionRequestCount = computed(() => (
  permissionRequests.value.filter((item) => item.status === "pending").length
));
const backendQueueTotal = computed(() => {
  const directKeys = ["message_queue_length", "qt_queue_length", "upload_queue_length", "source_refresh_queue_length"];
  let total = directKeys.reduce((sum, key) => sum + Number(queues.value?.[key] || 0), 0);
  const nested = queues.value?.queues;
  if (nested && typeof nested === "object") {
    total += Object.values(nested).reduce((sum: number, value: any) => {
      if (typeof value === "number") return sum + value;
      if (value && typeof value === "object") {
        return sum + Number(value.length || value.pending || value.ready || value.waiting || 0);
      }
      return sum;
    }, 0);
  }
  return total;
});
const mopConfigured = computed(() => Boolean(
  String(mopSettings.mop_app_token || "").trim()
  && String(mopSettings.mop_table_id || "").trim()
));
const adminTabs = computed(() => {
  const tabs = [
    {
      key: "status" as AdminTabKey,
      label: "后台状态",
      description: "",
      badge: backendQueueTotal.value ? `${backendQueueTotal.value}` : "",
    },
    {
      key: "permissions" as AdminTabKey,
      label: "账号与权限",
      description: "",
      badge: pendingPermissionRequestCount.value ? `${pendingPermissionRequestCount.value}` : "",
    },
    {
      key: "handover" as AdminTabKey,
      label: "交接班链接",
      description: "",
      badge: "",
    },
    {
      key: "mop" as AdminTabKey,
      label: "MOP 配置",
      description: "",
      badge: mopSettingsLoaded.value && !mopConfigured.value ? "待配置" : "",
    },
    { key: "feishu" as AdminTabKey, label: "飞书智能体", description: "", badge: "" },
    { key: "development" as AdminTabKey, label: "开发问题与更新记录", description: "", badge: "" },
  ];
  if (advancedDiagnosticsVisible.value) {
    tabs.push({
      key: "pressure" as AdminTabKey,
      label: "高级诊断",
      description: "",
      badge: "",
    });
  }
  return tabs;
});
const selectedPendingRequestIds = computed(() => (
  Array.from(selectedRequestIds).filter((id) => {
    const item = permissionRequests.value.find((row) => row.request_id === id);
    return item?.status === "pending";
  })
));
const allFilteredPendingSelected = computed(() => {
  const pending = filteredPermissionRequests.value.filter((item) => item.status === "pending");
  return pending.length > 0 && pending.every((item) => selectedRequestIds.has(String(item.request_id || "")));
});
const api = requestJson;

let initializedTab = false;
watch([() => props.open, () => props.initialTab], ([open]) => {
  if (open) selectAdminTab(props.initialTab || tab.value);
}, { immediate: true });

function selectAdminTab(next: AdminTabKey): void {
  if (props.systemAdmin === false) { tab.value = 'permissions'; return; }
  if (initializedTab && tab.value === next) return;
  initializedTab = true;
  if (next === "pressure" && !advancedDiagnosticsVisible.value) {
    advancedDiagnosticsVisible.value = true;
  }
  tab.value = next;
  emit('select-tab', next);
  if (next === "status") void loadStatus();
  else if (next === "permissions") {
    void loadPermissions();
  }
  else if (next === "handover") void loadHandover();
  else if (next === "mop") void loadMopSettings();
}

function toggleAdvancedDiagnostics(): void {
  advancedDiagnosticsVisible.value = !advancedDiagnosticsVisible.value;
  if (!advancedDiagnosticsVisible.value && tab.value === "pressure") {
    selectAdminTab("status");
  }
}

function scopeOptionLabel(value: unknown): string {
  return resolveScopeOptionLabel(props.scopeOptions, value);
}

function openHistoryMemory(): void {
  emit("close");
  navigate("/admin/history-memory");
}

async function loadStatus(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    let auditLoadWarning = "";
    const auditRequest = api("/api/backend/operation-audits?limit=30").catch(
      (error: any) => {
        auditLoadWarning = error?.message || "业务操作审计加载失败";
        return { items: [] };
      },
    );
    const [statsData, perfData, queuesData, consistencyData, jobsData, auditsData] = await Promise.all([
      api("/api/backend/stats"),
      api("/api/backend/perf"),
      api("/api/backend/queues"),
      api("/api/backend/consistency?scope=ALL"),
      api("/api/jobs/recent?limit=20"),
      auditRequest,
    ]);
    stats.value = statsData;
    perf.value = perfData;
    queues.value = queuesData;
    consistency.value = consistencyData;
    recentJobs.value = jobsData;
    operationAudits.value = auditsData;
    if (auditLoadWarning) message.value = auditLoadWarning;
  } catch (error: any) {
    message.value = error?.message || "状态加载失败";
  } finally {
    busy.value = false;
  }
}

async function runNoticeDiagnostic(): Promise<void> {
  const query = noticeDiagnosticQuery.value.trim();
  if (!query) {
    message.value = "请输入通告标题、active_item_id、source_record_id 或 target_record_id";
    return;
  }
  noticeDiagnosticLoading.value = true;
  try {
    const params = new URLSearchParams({ scope: "ALL", query });
    noticeDiagnostic.value = await api(`/api/backend/notice-diagnostic?${params.toString()}`);
  } catch (error: any) {
    message.value = error?.message || "通告链路自检失败";
  } finally {
    noticeDiagnosticLoading.value = false;
  }
}

async function repairNoticeProjection(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/backend/notice-projection-repair?scope=ALL", {
      method: "POST",
      body: "{}",
    });
    const repairedIdentities = Number(data.repaired_identities || 0);
    const repairedQtItems = Number(data.repaired_qt_items || 0);
    consistency.value = data.consistency || consistency.value;
    await loadStatus();
    message.value = `本地映射修复完成：identity ${repairedIdentities}，Qt投影 ${repairedQtItems}`;
  } catch (error: any) {
    message.value = error?.message || "本地映射修复失败";
  } finally {
    busy.value = false;
  }
}

async function runPreflight(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/backend/preflight", { method: "POST", body: "{}" });
    message.value = "预检完成";
    stats.value = { ...stats.value, preflight: data };
  } catch (error: any) {
    message.value = error?.message || "预检失败";
  } finally {
    busy.value = false;
  }
}

async function cleanupJobs(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/backend/jobs/cleanup", { method: "POST", body: "{}" });
    const removedTotal = cleanupRemovedTotal(data);
    await loadStatus();
    message.value = `清理完成：${removedTotal}`;
  } catch (error: any) {
    message.value = error?.message || "清理失败";
  } finally {
    busy.value = false;
  }
}

async function retryJob(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_retry) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/retry`, { method: "POST", body: "{}" });
    message.value = "任务已重新入队";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "任务重试失败";
  } finally {
    busy.value = false;
  }
}

async function clearJob(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_clear) return;
  const confirmed = await requestConfirm({
    tone: "danger",
    kicker: "任务清理",
    title: `清理任务 ${shortId(jobId)}`,
    message: "清理后该终态任务会从最近任务列表移除，不影响已经写入的通告记录。",
    details: ["仅清理后台任务记录", "不会删除 Qt 条目、多维记录或权限配置"],
    confirmLabel: "确认清理",
  });
  if (!confirmed) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/clear`, { method: "POST", body: "{}" });
    message.value = "任务已清理";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "任务清理失败";
  } finally {
    busy.value = false;
  }
}

async function markStuckFailed(job: Dict): Promise<void> {
  const jobId = String(job?.job_id || "").trim();
  if (!jobId || !job.can_mark_stuck_failed) return;
  const ok = await requestConfirm({
    tone: "warning",
    kicker: "卡住任务处理",
    title: `标记任务 ${shortId(jobId)} 为失败`,
    message: "该操作用于释放卡住的后台任务。标记失败后，可在最近任务里按需重试。",
    details: ["不会重复发送已成功的个人消息", "重试仍会走后端队列和幂等检查"],
    confirmLabel: "标记失败",
    confirmClass: "blue",
  });
  if (!ok) return;
  busy.value = true;
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/mark-stuck-failed`, {
      method: "POST",
      body: JSON.stringify({ reason: "管理员手动标记卡住任务，请核对后重试。" }),
    });
    message.value = "任务已标记为失败，可按需重试";
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "标记任务失败";
  } finally {
    busy.value = false;
  }
}

async function loadPermissions(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const [data] = await Promise.all([
      api("/api/auth/permissions"),
      loadPermissionRequests(false),
    ]);
    permissions.users.splice(0, permissions.users.length, ...(data.users || []));
    permissions.scope_options.splice(0, permissions.scope_options.length, ...(data.scope_options || []));
  } catch (error: any) {
    message.value = error?.message || "权限加载失败";
  } finally {
    busy.value = false;
  }
}

async function loadPermissionRequests(showBusy = true): Promise<void> {
  if (showBusy) {
    message.value = "";
    busy.value = true;
  }
  try {
    const data = await api(
      `/api/auth/permission-requests/admin?status=${encodeURIComponent(permissionRequestStatus.value)}&limit=200`
    );
    const items = (Array.isArray(data.items) ? data.items : []).map((item: Dict) => ({
      ...item,
      review_scopes: Array.isArray(item.approved_scopes)
        ? [...item.approved_scopes]
        : (Array.isArray(item.requested_scopes) ? [...item.requested_scopes] : []),
    }));
    permissionRequests.value = items;
    for (const id of Array.from(selectedRequestIds)) {
      if (!items.some((item: Dict) => item.request_id === id && item.status === "pending")) {
        selectedRequestIds.delete(id);
      }
    }
  } catch (error: any) {
    message.value = error?.message || "权限申请加载失败";
  } finally {
    if (showBusy) busy.value = false;
  }
}

function permissionRequestStatusLabel(value: unknown): string {
  const labels: Record<string, string> = {
    pending: "待审批",
    approved: "已通过",
    rejected: "已拒绝",
    superseded: "已替换",
    expired: "已过期",
    failed: "已失败",
    notify_failed: "通知失败",
  };
  return labels[String(value || "")] || String(value || "未知");
}

function requestScopeLabels(item: Dict): string {
  const labels = Array.isArray(item.requested_scope_labels) ? item.requested_scope_labels : [];
  return labels.length ? labels.join("、") : "未选择";
}

function currentScopeLabels(item: Dict): string {
  const labels = Array.isArray(item.current_scope_labels) ? item.current_scope_labels : [];
  return labels.length ? labels.join("、") : "暂无";
}

function reviewScopes(item: Dict): string[] {
  if (!Array.isArray(item.review_scopes)) {
    item.review_scopes = Array.isArray(item.requested_scopes) ? [...item.requested_scopes] : [];
  }
  return item.review_scopes;
}

function toggleReviewScope(item: Dict, scope: string, checked: boolean): void {
  const next = new Set(reviewScopes(item));
  if (checked) next.add(scope);
  else next.delete(scope);
  item.review_scopes = Array.from(next);
}

function togglePermissionRequestSelection(requestId: string, checked: boolean): void {
  const id = String(requestId || "").trim();
  if (!id) return;
  if (checked) selectedRequestIds.add(id);
  else selectedRequestIds.delete(id);
}

function toggleAllFilteredPermissionRequests(checked: boolean): void {
  for (const item of filteredPermissionRequests.value) {
    const id = String(item.request_id || "").trim();
    if (!id || item.status !== "pending") continue;
    if (checked) selectedRequestIds.add(id);
    else selectedRequestIds.delete(id);
  }
}

function scopesByRequestIds(ids: string[]): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  for (const id of ids) {
    const item = permissionRequests.value.find((row) => row.request_id === id);
    result[id] = item ? reviewScopes(item) : [];
  }
  return result;
}

function notificationMessage(notification: Dict | undefined): string {
  if (!notification || notification.ok !== false) return "";
  return `，但通知用户失败：${notification.message || "未知原因"}`;
}

async function approvePermissionRequest(item: Dict): Promise<void> {
  const requestId = String(item.request_id || "").trim();
  if (!requestId || item.status !== "pending") return;
  if (!reviewScopes(item).length) {
    message.value = "请至少选择一个审批楼栋。";
    return;
  }
  busy.value = true;
  try {
    const data = await api(`/api/auth/permission-requests/${encodeURIComponent(requestId)}/approve`, {
      method: "POST",
      body: JSON.stringify({ scopes: reviewScopes(item) }),
    });
    message.value = `权限申请已通过${notificationMessage(data.notification)}`;
    await loadPermissions();
  } catch (error: any) {
    message.value = error?.message || "审批失败";
  } finally {
    busy.value = false;
  }
}

async function rejectPermissionRequest(item: Dict): Promise<void> {
  const requestId = String(item.request_id || "").trim();
  if (!requestId || item.status !== "pending") return;
  const confirmed = await requestConfirm({
    tone: "warning",
    kicker: "权限申请拒绝",
    title: `拒绝「${item.name || item.open_id || "该用户"}」的申请`,
    message: permissionRejectReason.value || "将拒绝该权限申请，申请人不会获得新楼栋权限。",
    confirmLabel: "确认拒绝",
    confirmClass: "danger",
  });
  if (!confirmed) return;
  busy.value = true;
  try {
    const data = await api(`/api/auth/permission-requests/${encodeURIComponent(requestId)}/reject`, {
      method: "POST",
      body: JSON.stringify({ reason: permissionRejectReason.value }),
    });
    message.value = `权限申请已拒绝${notificationMessage(data.notification)}`;
    await loadPermissionRequests(false);
  } catch (error: any) {
    message.value = error?.message || "拒绝失败";
  } finally {
    busy.value = false;
  }
}

async function approveSelectedPermissionRequests(): Promise<void> {
  const ids = selectedPendingRequestIds.value;
  if (!ids.length) return;
  busy.value = true;
  try {
    const data = await api("/api/auth/permission-requests/bulk-approve", {
      method: "POST",
      body: JSON.stringify({ request_ids: ids, scopes_by_request_id: scopesByRequestIds(ids) }),
    });
    const okCount = Array.isArray(data.items) ? data.items.length : 0;
    const failCount = Array.isArray(data.failed) ? data.failed.length : 0;
    message.value = `批量通过完成：成功 ${okCount} 条，失败 ${failCount} 条`;
    selectedRequestIds.clear();
    await loadPermissions();
  } catch (error: any) {
    message.value = error?.message || "批量通过失败";
  } finally {
    busy.value = false;
  }
}

async function rejectSelectedPermissionRequests(): Promise<void> {
  const ids = selectedPendingRequestIds.value;
  if (!ids.length) return;
  const confirmed = await requestConfirm({
    tone: "warning",
    kicker: "批量拒绝",
    title: `拒绝 ${ids.length} 条权限申请`,
    message: permissionRejectReason.value || "将拒绝选中的权限申请，申请人不会获得新楼栋权限。",
    confirmLabel: "确认拒绝",
    confirmClass: "danger",
  });
  if (!confirmed) return;
  busy.value = true;
  try {
    const data = await api("/api/auth/permission-requests/bulk-reject", {
      method: "POST",
      body: JSON.stringify({ request_ids: ids, reason: permissionRejectReason.value }),
    });
    const okCount = Array.isArray(data.items) ? data.items.length : 0;
    const failCount = Array.isArray(data.failed) ? data.failed.length : 0;
    message.value = `批量拒绝完成：成功 ${okCount} 条，失败 ${failCount} 条`;
    selectedRequestIds.clear();
    await loadPermissionRequests(false);
  } catch (error: any) {
    message.value = error?.message || "批量拒绝失败";
  } finally {
    busy.value = false;
  }
}

function requestConfirm(options: {
  tone?: "danger" | "warning" | "primary";
  kicker: string;
  title: string;
  message: string;
  details?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
  confirmClass?: string;
}): Promise<boolean> {
  confirmDialog.open = true;
  confirmDialog.tone = options.tone || "danger";
  confirmDialog.kicker = options.kicker;
  confirmDialog.title = options.title;
  confirmDialog.message = options.message;
  confirmDialog.details = options.details || [];
  confirmDialog.confirmLabel = options.confirmLabel || "确认";
  confirmDialog.cancelLabel = options.cancelLabel || "取消";
  confirmDialog.confirmClass = options.confirmClass || (confirmDialog.tone === "danger" ? "danger" : "blue");
  return new Promise((resolve) => {
    confirmDialog.resolve = resolve;
  });
}

function resolveConfirm(confirmed: boolean): void {
  const resolver = confirmDialog.resolve;
  confirmDialog.open = false;
  confirmDialog.resolve = undefined;
  if (resolver) resolver(confirmed);
}

async function loadHandover(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const data = await api("/api/handover-links");
    for (const item of buildingScopes) handoverLinks[item.value] = String(data.links?.[item.value] || "");
  } catch (error: any) {
    message.value = error?.message || "交接班链接加载失败";
  } finally {
    busy.value = false;
  }
}

async function saveHandover(): Promise<void> {
  busy.value = true;
  try {
    await api("/api/handover-links", {
      method: "POST",
      body: JSON.stringify({ links: handoverLinks, password: handoverPassword.value }),
    });
    message.value = "交接班链接已保存";
  } catch (error: any) {
    message.value = error?.message || "交接班链接保存失败";
  } finally {
    busy.value = false;
  }
}

function updateHandoverLink(scope: string, value: string): void {
  handoverLinks[scope] = value;
}

function updateHandoverPassword(value: string): void {
  handoverPassword.value = value;
}

async function loadMopSettings(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const data = await api("/api/admin/mop-settings");
    mopSettings.mop_app_token = String(data.mop_app_token || "");
    mopSettings.mop_table_id = String(data.mop_table_id || "");
    mopSettings.mop_view_id = String(data.mop_view_id || "");
    mopSettings.mop_title_field = String(data.mop_title_field || "文件名");
    mopSettings.mop_attachment_field = String(data.mop_attachment_field || "文件");
    mopSettingsLoaded.value = true;
  } catch (error: any) {
    message.value = error?.message || "MOP 配置加载失败";
  } finally {
    busy.value = false;
  }
}

function updateMopSettingField(key: MopSettingsKey, value: string): void {
  mopSettings[key] = value;
}

async function saveMopSettings(): Promise<void> {
  busy.value = true;
  try {
    const data = await api("/api/admin/mop-settings", {
      method: "POST",
      body: JSON.stringify(mopSettings),
    });
    mopSettings.mop_app_token = String(data.mop_app_token || "");
    mopSettings.mop_table_id = String(data.mop_table_id || "");
    mopSettings.mop_view_id = String(data.mop_view_id || "");
    mopSettings.mop_title_field = String(data.mop_title_field || "文件名");
    mopSettings.mop_attachment_field = String(data.mop_attachment_field || "文件");
    mopSettingsLoaded.value = true;
    message.value = "MOP 配置已保存";
  } catch (error: any) {
    message.value = error?.message || "MOP 配置保存失败";
  } finally {
    busy.value = false;
  }
}

function updatePressureField(key: keyof typeof pressure, value: number | string | boolean): void {
  (pressure[key] as number | string | boolean) = value;
}

async function runMockPressure(): Promise<void> {
  busy.value = true;
  pressureResult.value = {};
  try {
    pressureResult.value = await api("/api/backend/mock-pressure", {
      method: "POST",
      body: JSON.stringify(pressure),
    });
    message.value = "离线压测完成";
  } catch (error: any) {
    message.value = error?.message || "离线压测失败";
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.admin-shell { width: 100%; padding: 20px 24px 32px; min-height: calc(100vh - 110px); color: #203650; }
.admin-card { width: 100%; margin: 0 auto; }
.admin-card > header { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 0 0 20px; border-bottom: 1px solid #d8e5f0; }
h1 { font-size: 23px; font-weight: 650; margin: 0; letter-spacing: 0; }
.btn { display: inline-flex; align-items: center; justify-content: center; gap: 7px; min-height: 38px; padding: 7px 12px; border: 1px solid #ccd8e6; border-radius: 7px; background: #fff; color: #294662; font: inherit; cursor: pointer; transition: background 160ms ease, border-color 160ms ease; }
.btn:hover { border-color: #9cbef0; background: #eef5ff; }
.btn:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
.admin-settings-layout { display: grid; grid-template-columns: 198px minmax(0, 1fr); gap: 24px; padding-top: 22px; align-items: start; }
.admin-settings-layout.accounts-only { grid-template-columns: minmax(0, 1fr); }
.admin-settings-content { min-width: 0; display: grid; gap: 16px; background: #fff; padding: 20px 24px; border-left: 1px solid #dce6f0; }
@media(max-width: 960px) {
  .admin-shell { padding: 16px; }
  .admin-settings-layout { grid-template-columns: 165px minmax(0,1fr); gap: 12px; }
  .admin-settings-content { padding: 16px; }
}
@media(max-width: 640px) {
  .admin-settings-layout { grid-template-columns: minmax(0,1fr); }
  .admin-settings-content { border-left: 0; padding: 16px 0; }
}
@media(prefers-reduced-motion: reduce) { .btn { transition: none; } }
</style>
