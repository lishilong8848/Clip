<template>
  <section v-if="checking" class="center-state">
    <div class="spinner"></div>
    <strong>正在确认登录状态</strong>
  </section>

  <section v-else-if="!loggedIn" class="center-state login-panel">
    <div class="login-head">
      <span class="login-shield"><ShieldCheck :size="28" aria-hidden="true" /></span>
      <strong>登录灯塔</strong>
    </div>

    <p v-if="authCancelled" class="login-notice" role="status">已取消飞书授权，可重新选择登录方式。</p>

    <div class="login-tabs" role="tablist" aria-label="登录方式" @keydown="onTabKeydown">
      <button
        v-for="tab in tabs"
        :id="`login-tab-${tab.value}`"
        :key="tab.value"
        type="button"
        role="tab"
        :aria-selected="activeTab === tab.value ? 'true' : 'false'"
        :aria-controls="`login-panel-${tab.value}`"
        :tabindex="activeTab === tab.value ? 0 : -1"
        :class="['login-tab', { active: activeTab === tab.value }]"
        @click="selectTab(tab.value)"
      >
        <component :is="tab.icon" :size="16" aria-hidden="true" />
        <span>{{ tab.label }}</span>
      </button>
    </div>

    <div
      v-if="activeTab === 'feishu'"
      id="login-panel-feishu"
      role="tabpanel"
      class="login-panel-body"
      aria-labelledby="login-tab-feishu"
    >
      <div class="feishu-authorize">
        <div class="feishu-qr" aria-hidden="true"><QrCode :size="64" /></div>
        <strong>使用本人飞书账号授权</strong>
        <p>可以验证身份后申请灯塔访问权限。</p>
        <a class="btn blue feishu-btn" :href="feishuLoginUrl" @click="handleFeishuLogin">飞书扫码登录</a>
      </div>
    </div>

    <div
      v-else-if="activeTab === 'password'"
      id="login-panel-password"
      role="tabpanel"
      class="login-panel-body password-panel"
      aria-labelledby="login-tab-password"
    >
      <PersonnelPasswordLogin />
    </div>

    <div
      v-else
      id="login-panel-guest"
      role="tabpanel"
      class="login-panel-body guest-panel"
      aria-labelledby="login-tab-guest"
    >
      <p class="guest-copy">可查看机柜数据并单独导出楼栋文件；不可修改数据或一键上传。</p>
      <form action="/api/auth/guest" method="post">
        <button class="btn blue guest-btn" type="submit">访客查看机柜</button>
      </form>
    </div>
  </section>

  <section v-else-if="loggedIn && passwordSettings" class="center-state request-panel">
    <VnetBackButton title="返回首页" @click="$emit('back')" />
    <strong>修改密码</strong>
    <PersonnelPasswordLogin
      :key="changePersonId"
      initialMode="change"
      :personId="changePersonId"
      :userName="changeUserName"
    />
  </section>

  <section v-else class="center-state request-panel">
    <VnetBackButton v-if="showBack" @click="$emit('back')" />
    <strong>{{ title || "当前账号暂无门户权限" }}</strong>
    <p class="user-line" :title="userOpenId ? `飞书身份：${userOpenId}` : ''">{{ userLineText }}</p>
    <a v-if="!showBack && userOpenId.startsWith('ou_')" class="btn blue" href="/learning?view=practice">进入我的答题</a>
    <div class="request-steps" aria-label="权限申请流程">
      <span class="active">
        <b>1</b>
        <strong>选择楼栋</strong>
        <small>{{ request.scopes.length ? `已选 ${request.scopes.length} 个` : "至少选择 1 个" }}</small>
      </span>
      <span :class="{ active: request.scopes.length > 0 }">
        <b>2</b>
        <strong>提交申请</strong>
        <small>管理员门户审批</small>
      </span>
      <span :class="{ active: Boolean(request.requestId), rejected: request.status === 'rejected' }">
        <b>3</b>
        <strong>{{ request.status === "rejected" ? "调整重提" : "等待生效" }}</strong>
        <small>{{ request.requestId ? requestStatusTitle : "待审批" }}</small>
      </span>
    </div>
    <div v-if="requestableScopes.length" class="scope-checks">
      <div class="scope-checks-head">
        <strong>选择申请楼栋</strong>
        <span>{{ request.scopes.length ? `已选 ${request.scopes.length} 个` : "至少选择 1 个" }}</span>
      </div>
      <label
        v-for="scope in requestableScopes"
        :key="scope.value"
        class="scope-pill"
        :class="{ selected: request.scopes.includes(scope.value) }"
      >
        <input
          :checked="request.scopes.includes(scope.value)"
          type="checkbox"
          :value="scope.value"
          @change="toggleScope(scope.value, ($event.target as HTMLInputElement).checked)"
        />
        {{ scope.label }}
      </label>
    </div>
    <p v-else class="hint">{{ emptyText || "当前没有可继续申请的楼栋权限。" }}</p>
    <textarea
      :value="request.reason"
      placeholder="申请原因（可选，便于管理员判断）"
      :disabled="busy || !requestableScopes.length"
      @input="$emit('update-request', { reason: ($event.target as HTMLTextAreaElement).value })"
    ></textarea>
    <div class="row-actions">
      <button type="button" class="btn blue" :disabled="!canSubmit" :title="submitTitle" @click="$emit('submit')">提交给管理员</button>
      <span v-if="!canSubmit" class="submit-hint">{{ submitTitle }}</span>
    </div>
    <div v-if="request.requestId" class="verify-box" :class="`status-${request.status || 'pending'}`">
      <strong>{{ requestStatusTitle }}</strong>
      <span>{{ requestStatusDescription }}</span>
    </div>
    <p v-if="request.message" class="hint">{{ request.message }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { QrCode, UserRound, Eye, ShieldCheck } from "lucide-vue-next";
import VnetBackButton from "./VnetBackButton.vue";
import PersonnelPasswordLogin from "./PersonnelPasswordLogin.vue";
import { rememberLoginMethod } from "../api/client";

type Dict = Record<string, any>;

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  user?: Dict;
  loginUrl?: string;
  busy: boolean;
  request: {
    scopes: string[];
    reason: string;
    code: string;
    requestId: string;
    message: string;
    status?: string;
    rejectReason?: string;
  };
  requestableScopes: Array<{ value: string; label: string }>;
  title?: string;
  emptyText?: string;
  showBack?: boolean;
  passwordSettings?: boolean;
}>();

const emit = defineEmits<{
  submit: [];
  confirm: [];
  back: [];
  "update-request": [patch: Partial<typeof props.request>];
}>();

// --- Login panel tabs (only used by the !loggedIn branch) ---
const tabs = [
  { value: "feishu", label: "飞书扫码登录", icon: QrCode },
  { value: "password", label: "账号密码登录", icon: UserRound },
  { value: "guest", label: "访客查看机柜", icon: Eye },
] as const;

type TabValue = (typeof tabs)[number]["value"];

const activeTab = ref<TabValue>(initialLoginTab());

function initialLoginTab(): TabValue {
  if (typeof window === "undefined") return "feishu";
  const login = new URLSearchParams(window.location.search).get("login");
  if (login === "password" || login === "guest") return login;
  return "feishu";
}

const authCancelled = computed(() => {
  if (typeof window === "undefined") return false;
  return new URLSearchParams(window.location.search).get("auth_notice") === "cancelled";
});

function selectTab(value: TabValue): void {
  activeTab.value = value;
}

function onTabKeydown(event: KeyboardEvent): void {
  const current = tabs.findIndex((tab) => tab.value === activeTab.value);
  let next = current;
  switch (event.key) {
    case "ArrowRight":
      next = (current + 1) % tabs.length;
      break;
    case "ArrowLeft":
      next = (current - 1 + tabs.length) % tabs.length;
      break;
    case "Home":
      next = 0;
      break;
    case "End":
      next = tabs.length - 1;
      break;
    default:
      return;
  }
  event.preventDefault();
  activeTab.value = tabs[next].value;
  const tabEl = document.getElementById(`login-tab-${tabs[next].value}`);
  tabEl?.focus();
}

const LOGIN_ENTRY_VALUES = ["password", "feishu", "guest"] as const;

function sanitizeCleanPath(value: string): string {
  const text = String(value || "").trim();
  if (
    !text.startsWith("/")
    || text.startsWith("//")
    || text.includes("\\")
    || text.startsWith("/api/")
    || /[\u0000-\u001F\u007F]/.test(text)
  ) return "";
  let url: URL;
  try {
    url = new URL(text, window.location.origin);
  } catch {
    return "";
  }
  if (url.origin !== window.location.origin) return "";
  return url.pathname + url.search + url.hash;
}

function unwrapLoginEntry(value: string): string {
  let current = value;
  for (let depth = 0; depth < 5; depth++) {
    const href = sanitizeCleanPath(current);
    if (!href) return "";
    let url: URL;
    try {
      url = new URL(href, window.location.origin);
    } catch {
      return "";
    }
    const login = url.searchParams.get("login");
    if (login && (LOGIN_ENTRY_VALUES as readonly string[]).includes(login)) {
      const nested = url.searchParams.get("next");
      if (nested === null || nested === "") return "";
      current = nested;
      continue;
    }
    return href;
  }
  return "";
}

const feishuLoginUrl = computed(() => {
  const base = new URL("/api/auth/login", window.location.origin);
  let candidateNext = "";
  if (typeof window !== "undefined") {
    const params = new URLSearchParams(window.location.search);
    const explicitLogin = params.get("login");
    const explicitNext = params.get("next");
    if (explicitLogin && explicitNext !== null && explicitNext !== undefined) {
      candidateNext = unwrapLoginEntry(explicitNext);
    } else {
      let propNext = "";
      try {
        propNext = new URL(String(props.loginUrl || ""), window.location.origin).searchParams.get("next") || "";
      } catch {
        /* malformed props.loginUrl: ignore, still fall back to /api/auth/login */
      }
      if (propNext) candidateNext = unwrapLoginEntry(propNext);
    }
    if (candidateNext) base.searchParams.set("next", candidateNext);
  }
  return base.pathname + base.search;
});

function handleFeishuLogin(): void {
  rememberLoginMethod("feishu");
}

function toggleScope(scope: string, checked: boolean): void {
  const next = new Set(props.request.scopes);
  if (checked) next.add(scope);
  else next.delete(scope);
  emit("update-request", { scopes: Array.from(next) });
}

const requestStatusTitle = computed(() => {
  if (props.request.status === "rejected") return "申请未通过";
  return "申请已提交，等待管理员审批";
});

const requestStatusDescription = computed(() => {
  if (props.request.status === "rejected") {
    const reason = String(props.request.rejectReason || "").trim();
    return reason ? `拒绝原因：${reason}。可调整楼栋或原因后重新提交。` : "管理员未通过该申请，可调整楼栋或原因后重新提交。";
  }
  return "等待管理员审批。";
});

const submitTitle = computed(() => {
  if (props.busy) return "正在提交申请";
  if (!props.requestableScopes.length) return props.emptyText || "当前没有可继续申请的楼栋权限";
  if (!props.request.scopes.length) return "请先选择要申请的楼栋";
  return "提交权限申请，等待管理员审批";
});

const canSubmit = computed(() => {
  return !props.busy && props.requestableScopes.length > 0 && props.request.scopes.length > 0;
});

const userOpenId = computed(() => String(props.user?.open_id || "").trim());

const changePersonId = computed(() => String(props.user?.personnel_record_id ?? "").trim());

const changeUserName = computed(() => String(props.user?.name || "").trim());

const userLineText = computed(() => {
  const name = String(props.user?.name || "").trim();
  return `当前登录：${name || "已完成飞书登录"}`;
});
</script>

<style scoped>
.center-state {
  position: relative;
  z-index: 0;
  overflow: hidden;
  width: min(660px, calc(100vw - 32px));
  margin: 44px auto;
  display: grid;
  gap: 12px;
  justify-items: start;
  padding: 26px;
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.94)),
    #ffffff;
  box-shadow: 0 18px 42px rgba(0, 47, 135, 0.12);
  backdrop-filter: blur(10px);
}

.center-state::after {
  content: "";
  position: absolute;
  right: -80px;
  bottom: -108px;
  z-index: -1;
  width: 230px;
  height: 160px;
  pointer-events: none;
  opacity: 0.5;
  background:
    repeating-linear-gradient(0deg, rgba(22, 120, 255, 0.1) 0 1px, transparent 1px 18px),
    repeating-linear-gradient(90deg, rgba(22, 120, 255, 0.08) 0 1px, transparent 1px 18px);
  transform: rotate(-12deg);
}

.center-state:not(:has(.spinner))::before {
  content: "";
  width: 50px;
  height: 50px;
  border-radius: 16px;
  background:
    linear-gradient(#ffffff, #ffffff) 15px 13px / 20px 4px no-repeat,
    linear-gradient(#ffffff, #ffffff) 15px 23px / 20px 4px no-repeat,
    linear-gradient(#ffffff, #ffffff) 15px 33px / 20px 4px no-repeat,
    linear-gradient(135deg, #0757d7, #1681ff);
  box-shadow: 0 14px 30px rgba(20, 103, 226, 0.26);
}

.center-state > strong {
  color: #071a39;
  font-size: 22px;
  line-height: 1.25;
  font-weight: 950;
  letter-spacing: 0;
}

.spinner {
  width: 34px;
  height: 34px;
  border: 3px solid #dbeafe;
  border-top-color: #1678ff;
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.btn,
a.btn {
  min-height: 38px;
  border: 1px solid #c8dcf3;
  border-radius: 15px;
  padding: 9px 15px;
  background: #ffffff;
  color: #09204a;
  font-size: 14px;
  font-weight: 900;
  line-height: 1;
  text-decoration: none;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 14px 28px rgba(21, 93, 252, 0.24);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}

p,
.hint {
  margin: 0;
  color: #5f7189;
  font-size: 13px;
}

.user-line {
  display: inline-flex;
  max-width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #f8fbff;
  padding: 7px 11px;
  color: #31445f;
  font-size: 12px;
  font-weight: 850;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.login-panel {
  width: min(600px, calc(100vw - 32px));
  margin-top: 36px;
  padding: 30px;
  display: grid;
  gap: 16px;
  justify-items: stretch;
  align-content: start;
  transition: opacity 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
}

/* Disable the shared document badge / decorative pseudo backgrounds for the
   login panel only. This must come after the shared .center-state rules so it
   wins over their higher-specificity selectors. */
.center-state.login-panel::before,
.center-state.login-panel::after {
  display: none;
}

.center-state.login-panel {
  background: #ffffff;
  backdrop-filter: none;
}

.login-head {
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 12px;
  padding: 0;
}

.login-shield {
  display: grid;
  place-items: center;
  width: 52px;
  height: 52px;
  border-radius: 16px;
  background: linear-gradient(135deg, #eaf3ff, #f5faff);
  color: #1678ff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.16);
}

.login-head > strong {
  color: #071a39;
  font-size: 21px;
  line-height: 1.25;
  font-weight: 950;
  letter-spacing: 0;
}

.login-notice {
  width: 100%;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin: 0;
  border: 1px solid #fecaca;
  border-radius: 14px;
  padding: 9px 11px;
  background: #fff1f2;
  color: #b91c1c;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.5;
  text-align: center;
}

.login-tabs {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 4px;
  padding: 4px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.92);
}

.login-tab {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  min-width: 0;
  min-height: 40px;
  padding: 9px 6px;
  border: 1px solid transparent;
  border-radius: 12px;
  background: transparent;
  color: #5f7189;
  font-size: 12px;
  font-weight: 900;
  letter-spacing: 0;
  line-height: 1.25;
  cursor: pointer;
  transition: background-color 0.16s ease, color 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}

.login-tab span {
  min-width: 0;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.login-tab:hover {
  color: #0f4fb8;
}

.login-tab:focus-visible {
  outline: 3px solid rgba(0, 91, 255, 0.22);
  outline-offset: 1px;
}

.login-tab.active {
  background: #eaf3ff;
  border-color: #a8c9f5;
  color: #0757d7;
  box-shadow: 0 1px 2px rgba(21, 93, 252, 0.06);
}

.login-panel-body {
  display: grid;
  gap: 12px;
  justify-items: center;
  text-align: center;
  padding: 4px 2px 2px;
  animation: login-panel-in 0.18s ease;
}

/* Password panel: keep label/form content left-aligned and stretch the
   PersonnelPasswordLogin child across the full panel width. */
.login-panel-body.password-panel {
  justify-items: start;
  text-align: left;
  width: 100%;
}

.login-panel-body.password-panel :deep(.pwd-form),
.login-panel-body.password-panel :deep(.app-link),
.login-panel-body.password-panel :deep(.load-state) {
  width: 100%;
  max-width: none;
  box-sizing: border-box;
}

@keyframes login-panel-in {
  from {
    opacity: 0;
    transform: translateY(3px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.feishu-authorize {
  display: grid;
  justify-items: center;
  gap: 10px;
  width: 100%;
}

.feishu-qr {
  display: grid;
  place-items: center;
  width: 108px;
  height: 108px;
  border: 1px dashed #bcd7fe;
  border-radius: 22px;
  background: rgba(245, 250, 255, 0.9);
  color: #1678ff;
}

.feishu-authorize > strong {
  color: #0f2f6a;
  font-size: 15px;
  font-weight: 950;
  letter-spacing: 0;
}

.feishu-authorize > p {
  max-width: 260px;
  margin: 0;
  color: #5f7189;
  font-size: 12px;
  line-height: 1.55;
}

.feishu-btn {
  width: 100%;
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin-top: 2px;
  border-radius: 15px;
  font-size: 14px;
  font-weight: 950;
}

.guest-panel {
  gap: 12px;
}

.guest-copy {
  margin: 0;
  color: #5f7189;
  font-size: 13px;
  line-height: 1.6;
  text-align: center;
}

.guest-panel form {
  width: 100%;
}

.guest-btn {
  width: 100%;
  min-height: 42px;
  border-radius: 15px;
  font-size: 14px;
  font-weight: 950;
}

@media (prefers-reduced-motion: reduce) {
  .login-panel,
  .login-panel-body,
  .login-tab {
    animation: none;
    transition: none;
  }
}

.request-steps {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}

.request-steps span {
  min-width: 0;
  display: grid;
  grid-template-columns: 24px minmax(0, 1fr);
  grid-template-areas:
    "num title"
    "num hint";
  align-items: center;
  column-gap: 7px;
  min-height: 46px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 7px 9px;
  background: rgba(248, 251, 255, 0.86);
  color: #64748b;
}

.request-steps b {
  grid-area: num;
  display: grid;
  place-items: center;
  width: 24px;
  height: 24px;
  border-radius: 9px;
  background: #e2e8f0;
  color: #52657f;
  font-size: 12px;
  font-weight: 950;
}

.request-steps strong,
.request-steps small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.request-steps strong {
  grid-area: title;
  color: #31445f;
  font-size: 12px;
  font-weight: 950;
}

.request-steps small {
  grid-area: hint;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}

.request-steps span.active {
  border-color: #bdd7ff;
  background: #eff6ff;
}

.request-steps span.active b {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
}

.request-steps span.active strong {
  color: #0f2f6a;
}

.request-steps span.rejected {
  border-color: #fecaca;
  background: #fff1f2;
}

.request-steps span.rejected b {
  background: #e11d48;
}

.scope-checks,
.row-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.scope-checks {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.82);
  padding: 10px;
}

.scope-checks-head {
  flex: 1 0 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 0 2px 2px;
}

.scope-checks-head strong {
  color: #0f2f6a;
  font-size: 13px;
  font-weight: 950;
}

.scope-checks-head span,
.submit-hint {
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.submit-hint {
  align-self: center;
}

.scope-pill {
  min-height: 32px;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.9);
  color: #37536f;
  font-size: 13px;
  font-weight: 850;
  cursor: pointer;
  transition: border-color 0.14s ease, background-color 0.14s ease, color 0.14s ease, box-shadow 0.14s ease;
}

.scope-pill:hover {
  border-color: #8dbbfb;
  background: #f5faff;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
}

/* VNET auth access skin */
.scope-pill.selected {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.2);
}

.scope-pill input,
.scope-pill input[type="checkbox"] {
  width: 15px;
  min-width: 15px;
  height: 15px;
  flex: 0 0 auto;
  accent-color: #1678ff;
  padding: 0;
  box-shadow: none;
}

.scope-pill.selected input {
  accent-color: #ffffff;
}

input,
textarea {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.9);
  color: #071634;
  font: inherit;
}

input:focus,
textarea:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

textarea {
  min-height: 54px;
  line-height: 1.55;
  resize: vertical;
}

.verify-box {
  width: 100%;
  display: grid;
  gap: 6px;
  border: 1px solid #bfdbfe;
  border-radius: 16px;
  padding: 10px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: inset 4px 0 0 #1e63ff, 0 10px 22px rgba(15, 86, 228, 0.07);
}

.row-actions {
  width: 100%;
  justify-content: flex-start;
}

.row-actions .btn {
  min-width: 136px;
  min-height: 38px;
  border-radius: 15px;
  font-weight: 950;
}

.verify-box strong {
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.verify-box span {
  color: #516a88;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.55;
}

.verify-box.status-rejected {
  border-color: #fecaca;
  background:
    linear-gradient(135deg, rgba(254, 242, 242, 0.97), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: inset 4px 0 0 #e11d48, 0 10px 22px rgba(225, 29, 72, 0.07);
}

.center-state > * {
  position: relative;
  z-index: 1;
}

@media (max-width: 640px) {
  .center-state {
    margin-block: 24px;
    padding: 22px;
  }

  .login-panel {
    margin-block: 20px;
    padding: 18px 16px 20px;
    width: min(420px, calc(100vw - 20px));
  }

  .login-tab {
    gap: 4px;
    font-size: 11px;
    padding: 9px 4px;
  }

  .feishu-qr {
    width: 92px;
    height: 92px;
  }

  .scope-pill {
    flex: 1 1 136px;
    justify-content: center;
  }

  .request-steps {
    grid-template-columns: 1fr;
  }
}
</style>
