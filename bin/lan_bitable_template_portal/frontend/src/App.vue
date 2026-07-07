<template>
  <main class="app-shell" :class="{ 'signature-link-shell': signatureLinkMode }">
    <AppTopbar
      v-if="!signatureLinkMode"
      :brand-logo-src="brandLogoSrc"
      :header-subtitle="headerSubtitle"
      :auth="auth"
      :is-engineer-mop-page="isEngineerMopPage"
      :is-event-page="isEventPage"
      :visible-scope-options="normalizedVisibleScopeOptions"
      :current-scope="currentScope"
      :loading="loading"
      :refresh-menu-open="refreshMenuOpen"
      :event-refreshing="eventRefreshing"
      :refresh-cooldown="refreshCooldown"
      :event-refresh-title="refreshButtonTitle('event')"
      :is-admin="isAdmin"
      @switch-scope="switchScope"
      @update:refresh-menu-open="refreshMenuOpen = $event"
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

    <RepairManagementPage
      v-else-if="isRepairManagementPage"
      :scope="currentScope"
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
      @switch-scope="enterEventManagement"
    />

    <ScopeHome
      v-else
      :scope-options="visibleScopeOptions"
      :overview="scopeOverview"
      :handover-links="handoverLinks"
      :can-request-more-scopes="additionalRequestableScopes.length > 0"
      @enter="enterScope"
      @event="enterEventManagement"
      @engineer="enterEngineerMop"
      @repair-management="enterRepairManagement"
      @request-permission="openAdditionalPermissionRequest"
    />

  </main>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import AppStatusNotices from "./components/AppStatusNotices.vue";
import AppTopbar from "./components/AppTopbar.vue";
import AsyncPageState from "./components/AsyncPageState.vue";
import { AUTH_EXPIRED_EVENT, requestJson } from "./api/client";
import { navigate, navigateHard } from "./navigation";
import type { LooseDict, ScopeOption } from "./types";

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
const AuthPanels = asyncPage(() => import("./components/AuthPanels.vue"));
const EngineerMopPage = asyncPage(() => import("./components/EngineerMopPage.vue"));
const EventManagementPage = asyncPage(() => import("./components/EventManagementPage.vue"));
const HistoryMemoryPage = asyncPage(() => import("./components/HistoryMemoryPage.vue"));
const RepairManagementPage = asyncPage(() => import("./components/RepairManagementPage.vue"));
const SignaturePage = asyncPage(() => import("./components/SignaturePage.vue"));
const ScopeHome = asyncPage(() => import("./components/ScopeHome.vue"));

type Dict = LooseDict;

const brandLogoSrc = "/assets/vnet-logo.png";
const authKeepaliveMs = 30 * 60 * 1000;
const authKeepaliveRetryMs = 3 * 60 * 1000;
const manualRefreshCooldownMs = 30 * 1000;
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

const routePath = ref(normalizedPath());
const routeParams = ref(new URLSearchParams(window.location.search));
const currentScope = ref(normalizeScopeValue(routeParams.value.get("scope") || ""));
const authChecking = ref(true);
const loading = ref(false);
const showAdminTools = ref(false);
const showPermissionRequestPanel = ref(false);
const refreshMenuOpen = ref(false);
const eventRefreshing = ref(false);
const eventRefreshNonce = ref(0);
const syncText = ref("准备中");
const scopeOverview = ref<Record<string, Dict>>({});
const handoverLinks = ref<Record<string, string>>({});
const refreshCooldown = reactive<Record<string, boolean>>({
  event: false,
});
const permissionBusy = ref(false);
const permissionRequest = reactive({
  scopes: [] as string[],
  reason: "",
  code: "",
  requestId: "",
  message: "",
  status: "",
  rejectReason: "",
});
const auth = reactive({
  loggedIn: false,
  user: {} as Dict,
  scopeOptions: [] as ScopeOption[],
  loginUrl: "/api/auth/login",
});

let authKeepaliveTimer: number | null = null;
let authRedirectInProgress = false;
let appDisposed = false;
const refreshCooldownTimers = new Map<string, number>();

const isHistoryMemoryPage = computed(() => routePath.value === "/admin/history-memory");
const isEngineerMopPage = computed(() => routePath.value === "/engineer/mop");
const isRepairManagementPage = computed(() => routePath.value === "/repair-management");
const isSignaturePage = computed(() => routePath.value === "/signature");
const isEventPage = computed(() => routeParams.value.get("mode") === "events");
const signatureLinkMode = computed(() => isSignaturePage.value && Boolean(routeParams.value.get("record_id") || routeParams.value.get("temporary_id")));
const isAdmin = computed(() => String(auth.user?.role || "").toLowerCase() === "admin");
const visibleScopeOptions = computed(() => auth.scopeOptions.length ? auth.scopeOptions : requestableScopes);
const normalizedVisibleScopeOptions = computed<ScopeOption[]>(() => visibleScopeOptions.value.map((item) => ({
  ...item,
  value: normalizeScopeValue(item.value),
})));
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
const permissionPanelEmptyText = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "当前账号已经拥有全部可申请入口。"
    : "当前没有可申请的楼栋权限。"
));
const headerSubtitle = computed(() => {
  if (isHistoryMemoryPage.value) return "管理工具 · 历史通告记忆导入";
  if (isEngineerMopPage.value) return `${scopeLabel(currentScope.value)} · 工程师 MOP 填写`;
  if (isRepairManagementPage.value) return `${scopeLabel(currentScope.value)} · 检修管理`;
  if (isSignaturePage.value) return "线上签名 · 手机手写保存";
  if (isEventPage.value) return `${scopeLabel(currentScope.value)} · 事件管理`;
  if (authChecking.value) return "功能选择 · 正在检查登录";
  if (!auth.loggedIn) return "功能选择 · 请先登录";
  if (!auth.scopeOptions.length) return "功能选择 · 申请访问权限";
  return "功能选择 · 请选择功能";
});
const pageStatusText = computed(() => {
  if (signatureLinkMode.value) return "";
  const text = String(syncText.value || "").trim();
  if (!text || ["准备中", "请选择功能", "切换中"].includes(text)) return "";
  if (isRoutinePageStatus(text)) return "";
  if (/^HTTP\s+\d+/i.test(text)) return "服务暂未就绪，页面会在连接恢复后自动刷新。";
  return text;
});
const connectionNotice = computed(() => null as null | { tone?: string; text: string; actionLabel?: string; action?: () => void });

function normalizedPath(): string {
  return window.location.pathname.replace(/\/$/, "") || "/";
}

function normalizeScopeValue(value: string | null | undefined, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  if (text.includes("园区") || text.includes("PARK")) return "CAMPUS";
  if (text.includes("110")) return "110";
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function scopeLabel(value: string): string {
  const normalized = normalizeScopeValue(value, "ALL");
  const found = [...visibleScopeOptions.value, { value: "ALL", label: "全部" }].find((item) => normalizeScopeValue(item.value, "") === normalized);
  return found?.label || normalized;
}

function isRoutinePageStatus(text: string): boolean {
  const needsAttention = /(失败|异常|过期|离线|未配置|不可用|错误|无法|请先|登录|超时|拒绝|缺少|未找到|无权限|服务暂未就绪)/.test(text);
  if (needsAttention) return false;
  return /(数据已就绪|本月暂无事件|正在读取事件数据|已刷新|已更新|已保存|已自动绑定)/.test(text);
}

function currentLoginUrl(): string {
  const next = `${window.location.pathname}${window.location.search}`;
  return `/api/auth/login?next=${encodeURIComponent(next || "/")}`;
}

function shouldSuppressAuthRedirect(): boolean {
  if (!isSignaturePage.value) return false;
  return Boolean(routeParams.value.get("record_id") || routeParams.value.get("temporary_id"));
}

function currentRouteNeedsAuth(): boolean {
  if (shouldSuppressAuthRedirect()) return false;
  if (routePath.value !== "/") return true;
  return Boolean(routeParams.value.get("scope") || routeParams.value.get("mode") || routeParams.value.get("work_type"));
}

function redirectToLogin(loginUrl = ""): void {
  if (authRedirectInProgress || shouldSuppressAuthRedirect()) return;
  authRedirectInProgress = true;
  window.location.assign(String(loginUrl || auth.loginUrl || currentLoginUrl()).trim() || currentLoginUrl());
}

function clearAuthKeepalive(): void {
  if (authKeepaliveTimer !== null) {
    window.clearTimeout(authKeepaliveTimer);
    authKeepaliveTimer = null;
  }
}

function scheduleAuthKeepalive(delayMs = authKeepaliveMs): void {
  clearAuthKeepalive();
  if (appDisposed || !auth.loggedIn) return;
  authKeepaliveTimer = window.setTimeout(async () => {
    authKeepaliveTimer = null;
    if (appDisposed || !auth.loggedIn) return;
    try {
      await loadAuthStatus({ silent: true });
    } catch {
      if (!appDisposed && auth.loggedIn) scheduleAuthKeepalive(authKeepaliveRetryMs);
    }
  }, delayMs);
}

function markAuthExpired(message = "登录已过期，请重新扫码登录。"): void {
  auth.loggedIn = false;
  auth.user = {};
  auth.scopeOptions = [];
  clearAuthKeepalive();
  syncText.value = message;
}

function handleGlobalAuthExpired(event: Event): void {
  const detail = (event as CustomEvent<{ message?: string; login_url?: string; loginUrl?: string }>).detail || {};
  markAuthExpired(detail.message || "登录已过期，请重新扫码登录。");
  redirectToLogin(String(detail.login_url || detail.loginUrl || ""));
}

async function portalRequest(path: string, options: RequestInit = {}): Promise<Dict> {
  return requestJson(path, options, {
    onOffline(message) {
      syncText.value = message;
    },
    onAuthExpired(message, _response, payload) {
      markAuthExpired(message);
      redirectToLogin(String(payload.login_url || payload.loginUrl || ""));
    },
    onServerError(message) {
      syncText.value = message;
    },
  });
}

async function loadAuthStatus(options: { silent?: boolean } = {}): Promise<void> {
  if (!options.silent) authChecking.value = true;
  const wasLoggedIn = auth.loggedIn;
  try {
    const data = await portalRequest(`/api/auth/status?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    const nextLoggedIn = Boolean(data.logged_in);
    auth.loggedIn = nextLoggedIn;
    auth.user = data.user || {};
    auth.scopeOptions = Array.isArray(data.scope_options) ? data.scope_options : [];
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
    if (!options.silent) syncText.value = error?.message || "登录状态检查失败";
    throw error;
  } finally {
    if (!options.silent) authChecking.value = false;
  }
}

async function loadOverview(): Promise<void> {
  try {
    const data = await portalRequest("/api/scope-overview");
    scopeOverview.value = data.scopes || data.items || {};
  } catch {
    scopeOverview.value = {};
  }
}

async function loadHandoverLinks(): Promise<void> {
  try {
    const data = await portalRequest("/api/handover-links");
    handoverLinks.value = data.links || {};
  } catch {
    handoverLinks.value = {};
  }
}

async function loadCurrentPermissionRequest(): Promise<void> {
  try {
    const data = await portalRequest("/api/auth/permission-requests/current");
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
  if (key === "event" && eventRefreshing.value) return "正在读取最新事件数据，完成后全楼可见。";
  return "读取最新事件数据，刷新当前月事件列表。";
}

function updateLocationRefs(): void {
  routePath.value = normalizedPath();
  routeParams.value = new URLSearchParams(window.location.search);
  currentScope.value = normalizeScopeValue(routeParams.value.get("scope") || currentScope.value || "");
}

function enterScope(scope: string, workType = "maintenance"): void {
  const url = new URL("/workbench-lite", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope));
  url.searchParams.set("work_type", workType || "maintenance");
  navigateHard(url);
}

function enterEventManagement(scope: string, detail = false): void {
  const url = new URL("/", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope));
  url.searchParams.set("mode", "events");
  if (detail) url.searchParams.set("detail", "1");
  navigate(url);
}

function enterEngineerMop(scope: string): void {
  const url = new URL("/engineer/mop", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope));
  navigate(url);
}

function enterRepairManagement(scope: string): void {
  const url = new URL("/repair-management", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope));
  navigate(url);
}

function switchScope(scope: string): void {
  if (isEventPage.value) {
    enterEventManagement(scope, true);
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
  await portalRequest("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
  navigateHard("/");
}

function updatePermissionRequest(patch: Partial<typeof permissionRequest>): void {
  Object.assign(permissionRequest, patch);
}

async function submitPermissionRequest(): Promise<void> {
  if (!permissionPanelRequestableScopes.value.length) {
    permissionRequest.message = "当前没有可申请的楼栋权限。";
    return;
  }
  permissionBusy.value = true;
  try {
    const data = await portalRequest("/api/auth/permission-requests", {
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
  await submitPermissionRequest();
}

onMounted(async () => {
  appDisposed = false;
  window.addEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  window.addEventListener("popstate", updateLocationRefs);
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
      enterEventManagement(visibleScopeOptions.value[0]?.value || "ALL");
      return;
    }
    syncText.value = isEventPage.value ? "正在读取事件数据" : "请选择功能";
  }
});

onBeforeUnmount(() => {
  appDisposed = true;
  window.removeEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  window.removeEventListener("popstate", updateLocationRefs);
  clearAuthKeepalive();
  for (const timer of refreshCooldownTimers.values()) window.clearTimeout(timer);
  refreshCooldownTimers.clear();
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
  min-height: 42px;
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
