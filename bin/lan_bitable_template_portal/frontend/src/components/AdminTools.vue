<template>
  <section v-if="open" class="admin-shell">
    <div class="admin-card">
      <header>
        <div>
          <strong>管理员工具</strong>
        </div>
        <button class="btn ghost" @click="$emit('close')">关闭</button>
      </header>

      <AdminWorkspaceChrome
        :overview-items="adminOverviewItems"
        :tabs="adminTabs"
        :active-tab="tab"
        :active-guide="activeAdminGuide"
        :advanced-visible="advancedDiagnosticsVisible"
        @select-tab="selectAdminTab"
        @toggle-advanced="toggleAdvancedDiagnostics"
      />

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

      <AdminStatusPane
        v-if="tab === 'status'"
        :stats="stats"
        :perf="perf"
        :queues="queues"
        :consistency="consistency"
        v-model:notice-diagnostic-query="noticeDiagnosticQuery"
        :notice-diagnostic="noticeDiagnostic"
        :notice-diagnostic-loading="noticeDiagnosticLoading"
        :recent-jobs="recentJobs"
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
        v-model:user-search="permissionUserSearch"
        v-model:user-filter="permissionUserFilter"
        :busy="busy"
        :pending-request-count="pendingPermissionRequestCount"
        :enabled-user-count="enabledPermissionUserCount"
        :admin-user-count="adminPermissionUserCount"
        :request-items="filteredPermissionRequests"
        :scope-options="scopeOptions"
        :selected-request-ids="selectedRequestIds"
        :all-filtered-pending-selected="allFilteredPendingSelected"
        :selected-pending-count="selectedPendingRequestIds.length"
        :request-status-label="permissionRequestStatusLabel"
        :request-scope-labels="requestScopeLabels"
        :current-scope-labels="currentScopeLabels"
        :review-scopes="reviewScopes"
        :users="filteredPermissionUsers"
        :users-total="permissions.users.length"
        :user-scope-summary="permissionUserScopeSummary"
        @load-permissions="loadPermissions"
        @load-requests="loadPermissionRequests"
        @add-user="addPermissionUser"
        @save="savePermissions"
        @approve-selected="approveSelectedPermissionRequests"
        @reject-selected="rejectSelectedPermissionRequests"
        @toggle-all="toggleAllFilteredPermissionRequests"
        @toggle-selection="togglePermissionRequestSelection"
        @toggle-review-scope="toggleReviewScope"
        @approve="approvePermissionRequest"
        @reject="rejectPermissionRequest"
        @toggle-user-scope="toggleUserScope"
        @remove-user="removePermissionUser"
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

      <AdminPressurePane
        v-else
        :pressure="pressure"
        :pressure-result="pressureResult"
        :busy="busy"
        @update-pressure="updatePressureField"
        @run="runMockPressure"
      />
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
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
import AdminPermissionPane from "./AdminPermissionPane.vue";
import AdminPressurePane from "./AdminPressurePane.vue";
import AdminStatusPane from "./AdminStatusPane.vue";
import AdminWorkspaceChrome from "./AdminWorkspaceChrome.vue";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";

type Dict = Record<string, any>;
type MopSettingsKey = "mop_app_token" | "mop_table_id" | "mop_view_id" | "mop_title_field" | "mop_attachment_field";

const props = defineProps<{
  open: boolean;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

const emit = defineEmits<{
  close: [];
}>();

const buildingScopes = adminBuildingScopes;

const tab = ref<AdminTabKey>("status");
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
const permissions = reactive<{ users: Dict[]; scope_options: Dict[] }>({ users: [], scope_options: [] });
const permissionRequests = ref<Dict[]>([]);
const permissionRequestStatus = ref("pending");
const permissionRequestSearch = ref("");
const permissionRejectReason = ref("");
const permissionUserSearch = ref("");
const permissionUserFilter = ref("all");
const selectedRequestIds = reactive(new Set<string>());
const permissionOriginalRoles = new Map<string, string>();
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
const enabledPermissionUserCount = computed(() => (
  permissions.users.filter((user) => user.enabled !== false).length
));
const adminPermissionUserCount = computed(() => (
  permissions.users.filter((user) => String(user.role || "building") === "admin").length
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
const adminOverviewItems = computed(() => [
  {
    key: "requests",
    label: "权限申请",
    value: `${pendingPermissionRequestCount.value} 待审批`,
    hint: pendingPermissionRequestCount.value ? "需要管理员处理" : "暂无待处理申请",
    tone: pendingPermissionRequestCount.value ? "warn" : "good",
    target: "permissions" as AdminTabKey,
    targetLabel: "权限管理",
    ariaLabel: "打开申请审批",
  },
  {
    key: "users",
    label: "授权用户",
    value: `${enabledPermissionUserCount.value} 人`,
    hint: `管理员 ${adminPermissionUserCount.value} 人`,
    tone: "blue",
    target: "permissions" as AdminTabKey,
    targetLabel: "权限管理",
    ariaLabel: "打开用户授权列表",
  },
  {
    key: "queues",
    label: "后台队列",
    value: `${backendQueueTotal.value} 条`,
    hint: backendQueueTotal.value ? "有任务正在排队" : "队列空闲",
    tone: backendQueueTotal.value ? "warn" : "good",
    target: "status" as AdminTabKey,
    targetLabel: "后台状态",
    ariaLabel: "打开后台队列概览",
  },
  {
    key: "mop",
    label: "MOP 配置",
    value: !mopSettingsLoaded.value ? "未读取" : mopConfigured.value ? "已配置" : "待配置",
    hint: !mopSettingsLoaded.value ? "进入配置页后读取" : mopConfigured.value ? "维护单页面可用" : "需填写 app_token/table_id",
    tone: !mopSettingsLoaded.value ? "blue" : mopConfigured.value ? "good" : "warn",
    target: "mop" as AdminTabKey,
    targetLabel: "MOP 配置",
    ariaLabel: "打开维护单配置",
  },
]);
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
      label: "权限管理",
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
const activeAdminGuide = computed(() => {
  const guides: Record<AdminTabKey, { title: string; text: string; badge: string }> = {
    status: {
      title: "后台状态",
      text: "",
      badge: backendQueueTotal.value ? `${backendQueueTotal.value} 条排队` : "队列空闲",
    },
    permissions: {
      title: "权限管理",
      text: "",
      badge: pendingPermissionRequestCount.value ? `${pendingPermissionRequestCount.value} 待审批` : "暂无申请",
    },
    handover: {
      title: "交接班链接",
      text: "",
      badge: "楼栋链接",
    },
    mop: {
      title: "MOP 配置",
      text: "",
      badge: mopConfigured.value ? "已配置" : "待配置",
    },
    pressure: {
      title: "高级诊断",
      text: "",
      badge: "离线测试",
    },
  };
  return guides[tab.value];
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
const filteredPermissionUsers = computed(() => {
  const query = permissionUserSearch.value.trim().toLowerCase();
  return permissions.users.filter((user) => {
    const role = String(user.role || "building");
    const enabled = user.enabled !== false;
    if (permissionUserFilter.value === "admin" && role !== "admin") return false;
    if (permissionUserFilter.value === "building" && role === "admin") return false;
    if (permissionUserFilter.value === "disabled" && enabled) return false;
    if (permissionUserFilter.value === "locked" && !user.locked) return false;
    if (!query) return true;
    const scopeText = (Array.isArray(user.scopes) ? user.scopes : [])
      .map((scope) => scopeOptionLabel(scope))
      .join(" ");
    const haystack = [
      user.name,
      user.open_id,
      role === "admin" ? "管理员" : "普通用户",
      enabled ? "启用" : "禁用",
      scopeText,
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });
});

watch(() => props.open, (open) => {
  if (open) {
    void loadStatus();
    void loadPermissionRequests(false);
  }
});

const api = requestJson;

function selectAdminTab(next: AdminTabKey): void {
  if (next === "pressure" && !advancedDiagnosticsVisible.value) {
    advancedDiagnosticsVisible.value = true;
  }
  tab.value = next;
  if (next === "status") void loadStatus();
  else if (next === "permissions") void loadPermissions();
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
    const [statsData, perfData, queuesData, consistencyData, jobsData] = await Promise.all([
      api("/api/backend/stats"),
      api("/api/backend/perf"),
      api("/api/backend/queues"),
      api("/api/backend/consistency?scope=ALL"),
      api("/api/jobs/recent?limit=20"),
    ]);
    stats.value = statsData;
    perf.value = perfData;
    queues.value = queuesData;
    consistency.value = consistencyData;
    recentJobs.value = jobsData;
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
    permissionOriginalRoles.clear();
    for (const user of permissions.users) {
      const openId = String(user.open_id || "").trim();
      if (openId) permissionOriginalRoles.set(openId, String(user.role || "building"));
    }
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

function addPermissionUser(): void {
  permissions.users.push({ open_id: "", name: "", role: "building", scopes: [], enabled: true });
}

async function removePermissionUser(user: Dict): Promise<void> {
  if (user.locked) return;
  const label = String(user.name || user.open_id || "该用户");
  const confirmed = await requestConfirm({
    tone: "danger",
    kicker: "权限删除",
    title: `删除「${label}」`,
    message: "该人员会先从当前编辑列表移除，点击保存权限后才真正生效。",
    details: ["固定管理员不会被删除", "删除后该 openid 将无法进入已授权楼栋"],
    confirmLabel: "移除人员",
  });
  if (!confirmed) return;
  const index = permissions.users.indexOf(user);
  if (index >= 0) {
    permissions.users.splice(index, 1);
    message.value = "已从列表移除，点击保存权限后生效。";
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

function toggleUserScope(user: Dict, scope: string, checked: boolean): void {
  const next = new Set((user.scopes || []) as string[]);
  if (checked) next.add(scope);
  else next.delete(scope);
  user.scopes = Array.from(next);
}

function permissionUserScopeSummary(user: Dict): string {
  if (String(user.role || "") === "admin") return "管理员 · 全部楼栋";
  const values = Array.isArray(user.scopes) ? user.scopes.map((item: unknown) => String(item || "").trim()).filter(Boolean) : [];
  if (!values.length) return "未选择楼栋";
  const labels = values.map(scopeOptionLabel).filter(Boolean);
  if (labels.length <= 3) return labels.join("、");
  return `${labels.slice(0, 3).join("、")} 等 ${labels.length} 个`;
}

async function savePermissions(): Promise<void> {
  const roleChanges = permissions.users
    .filter((user) => {
      const openId = String(user.open_id || "").trim();
      const oldRole = permissionOriginalRoles.get(openId) || "building";
      const nextRole = String(user.role || "building");
      return openId && oldRole !== nextRole && (oldRole === "admin" || nextRole === "admin");
    })
    .map((user) => `${user.name || user.open_id}: ${permissionOriginalRoles.get(String(user.open_id || "").trim()) || "building"} -> ${user.role || "building"}`);
  if (roleChanges.length) {
    const confirmed = await requestConfirm({
      tone: "warning",
      kicker: "管理员身份变更",
      title: "确认修改管理员身份",
      message: "管理员拥有全部楼栋和管理权限，请确认这些角色变更是预期操作。",
      details: roleChanges.slice(0, 8),
      confirmLabel: "确认保存",
      confirmClass: "blue",
    });
    if (!confirmed) return;
  }
  busy.value = true;
  try {
    await api("/api/auth/permissions", { method: "POST", body: JSON.stringify({ users: permissions.users }) });
    message.value = "权限已保存";
    await loadPermissions();
  } catch (error: any) {
    message.value = error?.message || "权限保存失败";
  } finally {
    busy.value = false;
  }
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
.admin-shell {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-modal-backdrop, 800);
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(15, 23, 42, 0.38);
}

.admin-card {
  width: min(1180px, 100%);
  max-height: calc(100vh - 48px);
  overflow: auto;
  display: grid;
  gap: 14px;
  padding: 18px;
  border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-panel, 22px);
  background:
    linear-gradient(180deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.98) 130px),
    var(--cf-surface, #ffffff);
  box-shadow: var(--cf-shadow-popover, 0 20px 50px rgba(15, 23, 42, 0.22));
}

header,
.actions,
.tabs {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.inline-check {
  display: inline-flex;
  grid-template-columns: none;
  align-items: center;
  gap: 6px;
  min-height: 35px;
}

.inline-check input {
  width: auto;
}

header {
  justify-content: space-between;
  padding: 12px 14px;
  border: 1px solid rgba(191, 219, 254, 0.82);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.82);
}

header strong {
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

p {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
}

.btn,
button {
  border: 1px solid #cbd5e1;
  border-radius: 14px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
}

.btn.blue,
.tabs button.active {
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

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.pane {
  display: grid;
  gap: 12px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 10px;
}

.metric-grid article {
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #f8fafc;
}

.metric-grid span {
  color: #64748b;
  font-size: 12px;
}

.metric-grid strong {
  display: block;
  margin-top: 4px;
  font-size: 20px;
}

.status-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  gap: 10px;
}

.status-card {
  display: grid;
  gap: 8px;
  min-height: 108px;
  padding: 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.status-card.good {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.status-card.warn {
  border-color: #fde68a;
  background: #fffbeb;
}

.status-card.bad {
  border-color: #fecaca;
  background: #fef2f2;
}

.status-card div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.status-card span {
  color: #64748b;
  font-size: 12px;
}

.status-card strong {
  color: #0f172a;
  font-size: 18px;
}

.status-card p {
  margin: 0;
}

.bar {
  overflow: hidden;
  height: 6px;
  border-radius: 999px;
  background: #e2e8f0;
}

.bar i {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: #2563eb;
}

.status-card.warn .bar i {
  background: #d97706;
}

.status-card.bad .bar i {
  background: #dc2626;
}

.warning-list {
  display: grid;
  gap: 4px;
  padding: 10px 12px;
  border: 1px solid #fde68a;
  border-radius: 8px;
  background: #fffbeb;
  color: #92400e;
}

.warning-list strong,
.warning-list p {
  margin: 0;
}

.diagnostic-section {
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.section-title {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.section-title span {
  color: #64748b;
  font-size: 12px;
}

.source-type-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(90px, 1fr));
  gap: 8px;
}

.source-type-grid article {
  padding: 8px;
  border-radius: 6px;
  background: #f8fafc;
}

.source-type-grid span {
  color: #64748b;
  font-size: 12px;
}

.source-type-grid strong {
  display: block;
  margin-top: 3px;
  font-size: 18px;
}

.slow-job-list {
  display: grid;
  gap: 6px;
}

.slow-job-list article {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 8px;
  border-radius: 6px;
  background: #f8fafc;
}

.slow-job-list strong,
.slow-job-list span {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.slow-job-list span {
  margin-top: 2px;
  color: #64748b;
  font-size: 12px;
}

.slow-job-list b {
  color: #0f172a;
  font-size: 14px;
}

.job-list {
  display: grid;
  gap: 8px;
}

.job-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.job-main {
  min-width: 0;
}

.job-main strong,
.job-main span {
  display: block;
}

.job-main span {
  margin-top: 4px;
  color: #64748b;
  font-size: 12px;
}

.job-main p {
  overflow-wrap: anywhere;
}

.job-actions {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.handover-grid,
.mop-settings-grid {
  display: grid;
  gap: 10px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
select {
  width: 100%;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

.handover-grid,
.mop-settings-grid {
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}

.password-line {
  max-width: 360px;
}

.message {
  padding: 10px 12px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #eff6ff;
  color: #1d4ed8;
}

/* VNET admin skin */
.admin-shell {
  background:
    linear-gradient(rgba(5, 20, 55, 0.42), rgba(5, 20, 55, 0.42)),
    radial-gradient(circle at 48% 0%, rgba(21, 116, 239, 0.32), transparent 40%);
  backdrop-filter: blur(6px);
}

.admin-card {
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background:
    linear-gradient(180deg, #ffffff 0, #f8fbff 100%);
  box-shadow: 0 30px 90px rgba(4, 43, 116, 0.28);
}

header {
  padding: 4px 2px 12px;
  border-bottom: 1px solid #e7f0fb;
}

header strong {
  color: #071634;
  font-size: 22px;
  font-weight: 900;
}

header strong::before {
  content: "";
  display: inline-block;
  width: 4px;
  height: 20px;
  margin-right: 8px;
  border-radius: 999px;
  vertical-align: -4px;
  background: linear-gradient(180deg, #0757d7, #21c6e7);
}

.tabs {
  width: fit-content;
  padding: 5px;
  border-color: #cde0f6;
  border-radius: 12px;
  background: #edf5ff;
}

.tabs button {
  min-height: 36px;
  border-radius: 9px;
  color: #23486f;
  font-weight: 800;
}

.btn,
button {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
}

.btn:hover:not(:disabled),
button:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.12);
  transform: translateY(-1px);
}

.btn.blue,
.tabs button.active {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.24);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #dc2626, #f05656);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.message {
  border-color: #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background-color 0.14s ease;
}

.metric-grid article:hover,
.status-card:hover,
.diagnostic-section:hover {
  border-color: #9cc7ff;
  background: #ffffff;
  box-shadow: 0 14px 32px rgba(22, 78, 151, 0.12);
}

.metric-grid strong,
.source-type-grid strong,
.status-card strong {
  color: #0757d7;
}

.source-type-grid article,
.slow-job-list article,
.job-row {
  border: 1px solid #e1ecfa;
  border-radius: 12px;
  background: #f7fbff;
}

input,
select {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
}

input:focus,
select:focus {
  border-color: #1678ff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

/* Softer rounded and text polish */
.admin-card {
  border-radius: 24px;
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row {
  border-radius: 18px;
}

.tabs,
.btn,
button,
input,
select {
  border-radius: 12px;
}

header strong,
.metric-grid strong,
.source-type-grid strong,
.status-card strong {
  font-weight: 820;
  letter-spacing: 0;
}

p,
.metric-grid span,
.status-card span {
  color: #5f7189;
}

.tabs button,
.btn,
button {
  font-weight: 720;
}

/* Panorama construction-management polish */
.admin-shell {
  background:
    linear-gradient(rgba(7, 22, 52, 0.38), rgba(7, 22, 52, 0.38)),
    radial-gradient(circle at 50% 0%, rgba(48, 128, 255, 0.22), transparent 42%);
  backdrop-filter: blur(6px);
}

.admin-card {
  border: 1px solid rgba(207, 224, 255, 0.96);
  border-radius: 28px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(250, 253, 255, 0.98));
  box-shadow: 0 30px 90px rgba(4, 43, 116, 0.22);
}

.tabs {
  border-color: #d8e7f8;
  border-radius: 18px;
  background: rgba(239, 246, 255, 0.86);
}

.tabs button.active,
.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 12px 24px rgba(21, 93, 252, 0.22);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row {
  border-color: rgba(216, 231, 248, 0.95);
  box-shadow: 0 10px 24px rgba(20, 70, 138, 0.07);
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row {
  background: rgba(255, 255, 255, 0.96);
}

.btn,
button,
input,
select {
  border-radius: 14px;
}

/* Panorama construction-management admin skin */
.admin-card {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 24px 64px rgba(0, 47, 135, 0.18);
}

.tabs {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.72);
}

.tabs button.active,
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

.btn.danger {
  background: #e11d48;
}

.metric-grid article,
.status-card,
.diagnostic-section,
.warning-list,
.message,
.source-type-grid article,
.slow-job-list article,
.job-row {
  border-color: #d8e5f7;
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.06);
}

input,
select {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

input:focus,
select:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

/* Final admin usability pass: compact command area and bounded long lists */
.admin-card {
  grid-template-rows: auto auto auto minmax(0, 1fr);
  overscroll-behavior: contain;
}

.admin-card > header {
  position: sticky;
  top: 0;
  z-index: 3;
  margin: -18px -18px 0;
  padding: 16px 18px 14px;
  border-bottom: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 28px 28px 0 0;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.94)),
    #ffffff;
  backdrop-filter: blur(12px);
}

.tabs {
  position: sticky;
  top: 72px;
  z-index: 2;
  width: 100%;
  box-sizing: border-box;
  justify-content: flex-start;
  overflow-x: auto;
  padding: 6px;
  overscroll-behavior-x: contain;
}

.tabs button {
  flex: 0 0 auto;
  min-width: 88px;
  min-height: 38px;
  white-space: nowrap;
}

.pane > .actions {
  position: sticky;
  top: 126px;
  z-index: 1;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 18px;
  padding: 9px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.9)),
    #ffffff;
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.mop-settings-grid,
.handover-grid {
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 20px;
  padding: 12px;
  background: rgba(248, 251, 255, 0.76);
}

/* Final density pass: keep admin controls readable without taking over the modal. */
.admin-card > header p {
  max-width: 720px;
}

.pane > .actions {
  gap: 7px;
  min-height: 50px;
}

.pane > .actions .btn {
  min-height: 34px;
  padding: 7px 12px;
  border-radius: 999px;
  font-size: 13px;
}

@media (max-width: 820px) {
  .admin-shell {
    padding: 12px;
  }

  .admin-card {
    max-height: calc(100vh - 24px);
    padding: 14px;
  }

  .admin-card > header {
    margin: -14px -14px 0;
    padding: 14px;
  }

  .tabs,
  .pane > .actions {
    position: static;
  }

}

/* Final admin workspace navigation pass */
.admin-card .btn,
.admin-card button {
  min-height: 38px;
  border-radius: 16px;
  font-weight: 900;
}

.admin-card .btn.ghost,
.admin-card button:not(.blue):not(.green):not(.danger):not(.active) {
  border-color: #cfe0ff;
  background: rgba(255, 255, 255, 0.88);
  color: #0f2f6a;
}

.admin-card .btn:hover:not(:disabled),
.admin-card button:hover:not(:disabled) {
  transform: translateY(-1px);
}
</style>
