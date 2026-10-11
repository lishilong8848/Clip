<template>
  <a
    v-if="mode === 'login'"
    class="app-link"
    href="https://applink.feishu.cn/T9aqNt3i8fXN"
    target="_blank"
    rel="noopener noreferrer"
  >
    <ExternalLink :size="15" aria-hidden="true" />
    <span>在登录前需先点击此链接并申请使用飞书应用，如已经申请就无需重复申请。</span>
  </a>

  <div v-if="peopleNeeded && loadingPeople" class="load-state" role="status">
    <div class="spinner" aria-label="正在加载人员列表"></div>
    <strong>正在加载人员列表</strong>
  </div>

  <div v-else-if="peopleNeeded && peopleError" class="load-state error" role="alert">
    <AlertCircle :size="28" aria-hidden="true" />
    <strong>人员列表加载失败</strong>
    <p>{{ peopleError }}</p>
    <button type="button" class="btn blue" :disabled="busy" @click="fetchPeople">
      <RefreshCw :size="15" aria-hidden="true" />
      重试
    </button>
  </div>

  <template v-else>
    <!-- 姓名密码登录 -->
    <form v-if="mode === 'login'" class="pwd-form" novalidate @submit.prevent="submitLogin">
      <label class="field" for="personnel-name-select">
        <span class="field-label"><User :size="15" aria-hidden="true" /> 姓名</span>
        <VnetSelect input-id="personnel-name-select" :model-value="personSearch" :options="personOptions"
          allow-custom placeholder="输入姓名或工号查找" label="姓名" :disabled="busy" required
          @update:model-value="selectPerson" />
      </label>

      <p v-if="selectedPerson && selectedPerson.needs_setup && !requiresPasswordChange" class="hint first-hint" role="status">
        <KeyRound :size="14" aria-hidden="true" />
        首次登录密码为本人身份证号后6位，验证后须立即修改
      </p>

      <label class="field" for="personnel-password">
        <span class="field-label"><Lock :size="15" aria-hidden="true" /> 密码</span>
        <input
          id="personnel-password"
          v-model="password"
          type="password"
          autocomplete="current-password"
          maxlength="128"
          placeholder="请输入登录密码"
          :disabled="busy || !selectedPerson"
          required
        />
      </label>

      <template v-if="requiresPasswordChange">
        <p class="change-note">首次登录需设置新密码，新密码不能与初始密码相同。</p>
        <label class="field" for="personnel-new-password">
          <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 新密码</span>
          <input
            id="personnel-new-password"
            v-model="newPassword"
            type="password"
            autocomplete="new-password"
            maxlength="128"
            placeholder="8-128 位新密码"
            :disabled="busy"
            required
          />
        </label>
        <label class="field" for="personnel-confirm-password">
          <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 确认新密码</span>
          <input
            id="personnel-confirm-password"
            v-model="confirmPassword"
            type="password"
            autocomplete="new-password"
            maxlength="128"
            placeholder="再次输入新密码"
            :disabled="busy"
            required
          />
        </label>
      </template>

      <p v-if="errorMessage" class="hint error-message" role="alert">{{ errorMessage }}</p>

      <div class="form-actions">
        <button type="submit" class="btn blue submit" :disabled="busy">
          <Loader2 v-if="busy" class="spin" :size="16" aria-hidden="true" />
          <template v-else>
            <LogIn v-if="!requiresPasswordChange" :size="16" aria-hidden="true" />
            <ShieldCheck v-else :size="16" aria-hidden="true" />
          </template>
          {{ busy ? loginBusyLabel : loginSubmitLabel }}
        </button>
        <button type="button" class="link-btn modest" :disabled="busy" @click="enterResetMode">忘记密码</button>
      </div>
    </form>

    <!-- 忘记密码（自动身份核验） -->
    <form v-else-if="mode === 'reset'" class="pwd-form" novalidate @submit.prevent="submitReset">
      <p class="reset-note">验证本人身份后即可设置新密码</p>

      <label class="field" for="personnel-reset-name">
        <span class="field-label"><User :size="15" aria-hidden="true" /> 姓名</span>
        <VnetSelect input-id="personnel-reset-name" :model-value="personSearch" :options="personOptions"
          allow-custom placeholder="查找外部人员姓名或工号" label="姓名" :disabled="busy" required
          @update:model-value="selectPerson" />
      </label>

      <label class="field" for="personnel-identity-number">
        <span class="field-label"><Fingerprint :size="15" aria-hidden="true" /> 身份证号</span>
        <input
          id="personnel-identity-number"
          v-model="identityNumber"
          type="password"
          autocomplete="off"
          maxlength="18"
          placeholder="请输入完整身份证号"
          :disabled="busy || !selectedPerson"
          required
          @input="onIdentityInput"
        />
      </label>

      <p v-if="resetVerified === 'checking'" class="verify-status checking" role="status" data-verify-status="checking">
        <Loader2 class="spin" :size="14" aria-hidden="true" />身份核验中…
      </p>
      <p v-else-if="resetVerified === 'passed'" class="verify-status passed" role="status" data-verify-status="passed">身份核验通过，可设置新密码</p>
      <p v-else-if="resetVerified === 'failed'" class="verify-status failed" role="status" data-verify-status="failed">{{ verifyMessage }}</p>
      <button v-if="resetVerified === 'failed'" type="button" class="link-btn" :disabled="busy" @click="scheduleIdentityVerify">重新核验</button>

      <label class="field" for="personnel-reset-new-password">
        <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 新密码</span>
        <input
          id="personnel-reset-new-password"
          v-model="newPassword"
          type="password"
          autocomplete="new-password"
          maxlength="128"
          placeholder="8-128 位新密码"
          :disabled="busy || resetVerified !== 'passed'"
          required
        />
      </label>

      <label class="field" for="personnel-reset-confirm-password">
        <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 确认新密码</span>
        <input
          id="personnel-reset-confirm-password"
          v-model="confirmPassword"
          type="password"
          autocomplete="new-password"
          maxlength="128"
          placeholder="再次输入新密码"
          :disabled="busy || resetVerified !== 'passed'"
          required
        />
      </label>

      <p v-if="errorMessage" class="hint error-message" role="alert">{{ errorMessage }}</p>

      <div class="form-actions">
        <button type="submit" class="btn blue submit" :disabled="!canSubmitReset">
          <Loader2 v-if="busy" class="spin" :size="16" aria-hidden="true" />
          <KeyRound v-else :size="16" aria-hidden="true" />
          {{ resetSubmitLabel }}
        </button>
        <button type="button" class="link-btn modest" :disabled="busy" @click="exitResetMode">{{ cameFromChange ? "返回修改密码" : "返回密码登录" }}</button>
      </div>
    </form>

    <!-- 修改密码（已登录） -->
    <form v-else class="pwd-form" novalidate @submit.prevent="submitChange">
      <div class="current-account">
        <span class="field-label"><User :size="15" aria-hidden="true" /> 当前账号</span>
        <strong>{{ changeUserName || "已登录" }}</strong>
      </div>

      <label class="field" for="personnel-current-password">
        <span class="field-label"><Lock :size="15" aria-hidden="true" /> 原密码</span>
        <input
          id="personnel-current-password"
          v-model="currentPassword"
          type="password"
          autocomplete="current-password"
          maxlength="128"
          placeholder="请输入原密码"
          :disabled="busy"
          required
        />
      </label>

      <label class="field" for="personnel-change-new-password">
        <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 新密码</span>
        <input
          id="personnel-change-new-password"
          v-model="newPassword"
          type="password"
          autocomplete="new-password"
          maxlength="128"
          placeholder="8-128 位新密码"
          :disabled="busy"
          required
        />
      </label>

      <label class="field" for="personnel-change-confirm-password">
        <span class="field-label"><ShieldCheck :size="15" aria-hidden="true" /> 确认新密码</span>
        <input
          id="personnel-change-confirm-password"
          v-model="confirmPassword"
          type="password"
          autocomplete="new-password"
          maxlength="128"
          placeholder="再次输入新密码"
          :disabled="busy"
          required
        />
      </label>

      <p v-if="errorMessage" class="hint error-message" role="alert">{{ errorMessage }}</p>

      <div class="form-actions">
        <button type="submit" class="btn blue submit" :disabled="!canSubmitChange">
          <Loader2 v-if="busy" class="spin" :size="16" aria-hidden="true" />
          <KeyRound v-else :size="16" aria-hidden="true" />
          {{ changeSubmitLabel }}
        </button>
        <button type="button" class="link-btn modest" :disabled="busy" @click="enterResetMode">忘记密码</button>
      </div>
    </form>
  </template>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import {
  AlertCircle,
  ExternalLink,
  Fingerprint,
  KeyRound,
  Loader2,
  Lock,
  LogIn,
  RefreshCw,
  ShieldCheck,
  User,
} from "lucide-vue-next";
import { ApiError, rememberLoginMethod, requestJson, type Dict } from "../api/client";
import { navigateHard } from "../navigation";
import VnetSelect from './VnetSelect.vue';

type Person = {
  id: string;
  name: string;
  employee_no?: string;
  building?: string;
  selectable?: boolean;
  disabled_reason?: string;
  needs_setup?: boolean;
  account_nature?: string;
};

type VerifyStatus = "idle" | "checking" | "passed" | "failed";

const props = defineProps<{
  initialMode?: "login" | "change";
  personId?: string;
  userName?: string;
}>();

const people = ref<Person[]>([]);
const loadingPeople = ref(false);
const peopleLoaded = ref(false);
const peopleError = ref("");
const selectedId = ref("");
const personSearch = ref("");
let peopleRefreshTimer: number | undefined;
let peopleFetching = false;
const mode = ref<"login" | "reset" | "change">("login");
const cameFromChange = ref(false);
const password = ref("");
const currentPassword = ref("");
const newPassword = ref("");
const confirmPassword = ref("");
const requiresPasswordChange = ref(false);
const busy = ref(false);
const errorMessage = ref("");
const identityNumber = ref("");
const resetVerified = ref<VerifyStatus>("idle");
const verifyMessage = ref("");

const disposed = { current: false };
const peopleController = new AbortController();
const submitController = new AbortController();

// Debounced automatic identity verification (forgot / reset flow).
const verifyTimer = ref<number | null>(null);
const verifyController = ref<AbortController | null>(null);
let verifySeq = 0;

const peopleNeeded = computed(() => mode.value !== "change");

const selectedPerson = computed<Person | null>(() =>
  people.value.find(item => item.id === selectedId.value) ?? null,
);

const changeUserName = computed(() => String(props.userName || "").trim());
const changePersonId = computed(() => String(props.personId || "").trim());

const loginSubmitLabel = computed(() =>
  requiresPasswordChange.value ? "设置新密码并登录" : "姓名密码登录",
);
const loginBusyLabel = computed(() =>
  requiresPasswordChange.value ? "正在保存…" : "正在登录…",
);
const resetSubmitLabel = computed(() => (busy.value ? "正在保存…" : "确认重置密码"));
const changeSubmitLabel = computed(() => (busy.value ? "正在保存…" : "确认修改密码"));

const canSubmitReset = computed(() =>
  mode.value === "reset"
    && resetVerified.value === "passed"
    && Boolean(newPassword.value)
    && newPassword.value === confirmPassword.value
    && strongPassword(newPassword.value)
    && !busy.value,
);

const canSubmitChange = computed(() =>
  mode.value === "change"
    && Boolean(currentPassword.value)
    && Boolean(newPassword.value)
    && newPassword.value === confirmPassword.value
    && strongPassword(newPassword.value)
    && !busy.value,
);

function optionLabel(item: Person): string {
  const label = [item.name, item.building, item.employee_no]
    .filter((part): part is string => Boolean(part && String(part).trim()))
    .join(" · ");
  return item.selectable === false ? `${label}（${item.disabled_reason || "无权限"}）` : label;
}

const availablePeople = computed(() => people.value.filter(item => item.selectable !== false
  && (mode.value !== 'reset' || item.account_nature === '外部账号')));
const personChoices = computed(() => availablePeople.value.map((item, index, all) => {
  const label = optionLabel(item);
  return { id: item.id, label: all.filter(other => optionLabel(other) === label).length > 1 ? `${label}（同名 ${index + 1}）` : label };
}));
const personOptions = computed(() => personChoices.value.map(item => item.label));

function selectPerson(value: string): void {
  personSearch.value = value;
  const id = personChoices.value.find(item => item.label === value)?.id || '';
  if (id === selectedId.value) return;
  selectedId.value = id;
  onSelectionChange();
  if (mode.value === 'login' && selectedPerson.value?.account_nature?.toUpperCase() === 'VNET') {
    // Keep password as the return preference so browser Back does not restart OAuth.
    rememberLoginMethod('password');
    navigateHard(`/api/auth/login?next=${encodeURIComponent(currentUrlPath())}`);
  }
}

function currentUrlPath(): string {
  if (typeof window === "undefined") return "/";
  return `${window.location.pathname}${window.location.search}`;
}

function errorMessageOf(error: unknown): string {
  return error instanceof Error && error.message ? error.message : "请求失败，请稍后重试。";
}

function isSafeLocalRedirect(value: string): boolean {
  let url: URL;
  try {
    url = new URL(value, window.location.origin);
  } catch {
    return false;
  }
  return (
    url.origin === window.location.origin &&
    url.pathname.startsWith("/") &&
    !url.pathname.startsWith("//")
  );
}

function handleLoginResult(payload: Dict): void {
  const redirect = String(payload.redirect_url || "").trim();
  if (!redirect || !isSafeLocalRedirect(redirect)) {
    throw new Error("登录成功但跳转地址无效，请刷新后重试。");
  }
  navigateHard(redirect);
}

function strongPassword(pwd: string): boolean {
  const trimmed = pwd.trim();
  return Boolean(trimmed && trimmed.length >= 8 && pwd.length <= 128);
}

function identityFormatValid(id: string): boolean {
  const cleaned = id.trim().toUpperCase();
  return /^[0-9]{15}$/.test(cleaned) || /^[0-9]{17}[0-9X]$/.test(cleaned);
}

function clearAllSecrets(): void {
  password.value = "";
  currentPassword.value = "";
  newPassword.value = "";
  confirmPassword.value = "";
  requiresPasswordChange.value = false;
  identityNumber.value = "";
  resetVerified.value = "idle";
  errorMessage.value = "";
}

function clearVerify(): void {
  if (verifyTimer.value !== null) {
    window.clearTimeout(verifyTimer.value);
    verifyTimer.value = null;
  }
  verifyController.value?.abort();
  verifyController.value = null;
  verifySeq++;
  resetVerified.value = "idle";
  verifyMessage.value = "";
  newPassword.value = "";
  confirmPassword.value = "";
}

function markSetupFalse(): void {
  const idx = people.value.findIndex(p => p.id === selectedId.value);
  if (idx !== -1 && people.value[idx]) {
    people.value[idx] = { ...people.value[idx], needs_setup: false };
  }
}

function onSelectionChange(): void {
  if (mode.value === "reset") {
    identityNumber.value = "";
    scheduleIdentityVerify();
  } else {
    clearAllSecrets();
  }
}

function onIdentityInput(): void {
  if (mode.value === "reset") {
    scheduleIdentityVerify();
  }
}

function scheduleIdentityVerify(): void {
  clearVerify();
  errorMessage.value = "";
  const personId = selectedId.value;
  const identity = identityNumber.value.trim().toUpperCase();
  const person = selectedPerson.value;
  if (
    mode.value !== "reset"
    || !personId
    || !person
    || person.selectable === false
    || !identityFormatValid(identity)
  ) {
    return;
  }
  resetVerified.value = "checking";
  const seq = ++verifySeq;
  verifyTimer.value = window.setTimeout(async () => {
    verifyTimer.value = null;
    const controller = new AbortController();
    verifyController.value = controller;
    try {
      const result = await requestJson("/api/auth/password/reset", {
        method: "POST",
        body: JSON.stringify({ person_id: personId, identity_number: identity }),
        signal: controller.signal,
      });
      if (disposed.current || seq !== verifySeq || controller.signal.aborted) return;
      if (result.requires_password_change !== true) throw new Error("身份核验未完成，请重试。");
      if (resetVerified.value === "checking") resetVerified.value = "passed";
    } catch (error) {
      if (disposed.current || seq !== verifySeq || controller.signal.aborted) return;
      resetVerified.value = "failed";
      verifyMessage.value = errorMessageOf(error);
    }
  }, 500);
}

function enterResetMode(): void {
  if (busy.value || mode.value === "reset") return;
  cameFromChange.value = mode.value === "change";
  clearVerify();
  clearAllSecrets();
  mode.value = "reset";
  if (selectedPerson.value?.account_nature !== '外部账号') { selectedId.value = ''; personSearch.value = ''; }
  if (!peopleLoaded.value) void fetchPeople();
}

function exitResetMode(): void {
  if (busy.value || mode.value !== "reset") return;
  clearVerify();
  clearAllSecrets();
  mode.value = cameFromChange.value ? "change" : "login";
}

async function fetchPeople(): Promise<void> {
  if (peopleFetching || busy.value) return;
  peopleFetching = true;
  loadingPeople.value = !peopleLoaded.value;
  peopleError.value = "";
  try {
    const data = await requestJson("/api/auth/password/people", {
      cache: "no-store",
      signal: peopleController.signal,
    });
    if (disposed.current) return;
    people.value = Array.isArray(data.items) ? (data.items as Person[]).slice() : [];
    peopleLoaded.value = true;
    if (selectedId.value) {
      const selected = personChoices.value.find(item => item.id === selectedId.value);
      if (!selected || selectedPerson.value?.account_nature?.toUpperCase() === 'VNET') {
        selectedId.value = ''; personSearch.value = ''; clearVerify(); clearAllSecrets();
      } else personSearch.value = selected.label;
    }
  } catch (error) {
    if (disposed.current) return;
    if (!peopleLoaded.value) peopleError.value = errorMessageOf(error);
  } finally {
    if (!disposed.current) loadingPeople.value = false;
    peopleFetching = false;
  }
}

function validatePerson(): boolean {
  if (!selectedPerson.value || selectedPerson.value.selectable === false) {
    errorMessage.value = "请先选择姓名。";
    return false;
  }
  return true;
}

async function submitLogin(): Promise<void> {
  if (busy.value || disposed.current) return;
  if (!validatePerson()) return;
  if (!password.value) {
    errorMessage.value = "请输入密码。";
    return;
  }
  if (requiresPasswordChange.value) {
    if (!newPassword.value || !confirmPassword.value) {
      errorMessage.value = "请填写并确认新密码。";
      return;
    }
    if (newPassword.value !== confirmPassword.value) {
      errorMessage.value = "两次输入的新密码不一致，请重新确认。";
      return;
    }
    if (!strongPassword(newPassword.value)) {
      errorMessage.value = "新密码长度需为 8-128 位。";
      return;
    }
  }

  rememberLoginMethod("password");
  busy.value = true;
  errorMessage.value = "";
  try {
    const body: Dict = {
      person_id: selectedPerson.value!.id,
      password: password.value,
      next: currentUrlPath(),
    };
    if (requiresPasswordChange.value) body.new_password = newPassword.value;
    const payload = await requestJson("/api/auth/password/login", {
      method: "POST",
      body: JSON.stringify(body),
      signal: submitController.signal,
    });
    if (disposed.current) return;
    if (payload.requires_password_change === true) {
      requiresPasswordChange.value = true;
      newPassword.value = "";
      confirmPassword.value = "";
      errorMessage.value = "";
      return;
    }
    handleLoginResult(payload);
  } catch (error) {
    if (disposed.current) return;
    errorMessage.value = errorMessageOf(error);
  } finally {
    busy.value = false;
  }
}

async function submitReset(): Promise<void> {
  if (busy.value || disposed.current) return;
  if (mode.value !== "reset") return;
  if (!validatePerson()) return;
  if (resetVerified.value !== "passed") {
    errorMessage.value = "请先完成身份核验。";
    return;
  }
  const personId = selectedPerson.value!.id;
  const identity = identityNumber.value.trim().toUpperCase();
  if (!identityFormatValid(identity)) {
    errorMessage.value = "请填写完整的身份证号。";
    return;
  }
  if (!newPassword.value || !confirmPassword.value) {
    errorMessage.value = "请填写并确认新密码。";
    return;
  }
  if (newPassword.value !== confirmPassword.value) {
    errorMessage.value = "两次输入的新密码不一致，请重新确认。";
    return;
  }
  if (!strongPassword(newPassword.value)) {
    errorMessage.value = "新密码长度需为 8-128 位。";
    return;
  }

  busy.value = true;
  errorMessage.value = "";
  try {
    const payload = await requestJson("/api/auth/password/reset", {
      method: "POST",
      body: JSON.stringify({
        person_id: personId,
        identity_number: identity,
        new_password: newPassword.value,
      }),
      signal: submitController.signal,
    });
    if (disposed.current) return;
    if (payload.password_changed === true) {
      markSetupFalse();
      clearAllSecrets();
      rememberLoginMethod("password");
      navigateHard("/?login=password");
      return;
    }
    errorMessage.value = errorMessageOf(new Error(String(payload.error || "请求未能完成，请重试。")));
  } catch (error) {
    if (disposed.current) return;
    errorMessage.value = errorMessageOf(error);
    if (error instanceof ApiError && error.status === 401) {
      clearVerify();
      resetVerified.value = "failed";
      verifyMessage.value = errorMessageOf(error);
    }
  } finally {
    busy.value = false;
  }
}

async function submitChange(): Promise<void> {
  if (busy.value || disposed.current) return;
  if (mode.value !== "change") return;
  if (!currentPassword.value) {
    errorMessage.value = "请输入原密码。";
    return;
  }
  if (!newPassword.value || !confirmPassword.value) {
    errorMessage.value = "请填写并确认新密码。";
    return;
  }
  if (newPassword.value !== confirmPassword.value) {
    errorMessage.value = "两次输入的新密码不一致，请重新确认。";
    return;
  }
  if (!strongPassword(newPassword.value)) {
    errorMessage.value = "新密码长度需为 8-128 位。";
    return;
  }

  busy.value = true;
  errorMessage.value = "";
  try {
    const payload = await requestJson("/api/auth/password/change", {
      method: "POST",
      body: JSON.stringify({
        current_password: currentPassword.value,
        new_password: newPassword.value,
      }),
      signal: submitController.signal,
    });
    if (disposed.current) return;
    if (payload.password_changed === true) {
      clearAllSecrets();
      rememberLoginMethod("password");
      navigateHard("/?login=password");
      return;
    }
    errorMessage.value = errorMessageOf(new Error(String(payload.error || "请求未能完成，请重试。")));
  } catch (error) {
    if (disposed.current) return;
    errorMessage.value = errorMessageOf(error);
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  mode.value = props.initialMode === "change" ? "change" : "login";
  if (mode.value === "change") selectedId.value = changePersonId.value;
  if (mode.value !== "change") void fetchPeople();
  peopleRefreshTimer = window.setInterval(() => {
    if (!document.hidden && peopleNeeded.value && !busy.value && resetVerified.value !== 'checking') void fetchPeople();
  }, 60000);
});

onBeforeUnmount(() => {
  disposed.current = true;
  window.clearInterval(peopleRefreshTimer);
  if (verifyTimer.value !== null) window.clearTimeout(verifyTimer.value);
  verifyController.value?.abort();
  peopleController.abort();
  submitController.abort();
  clearAllSecrets();
  selectedId.value = "";
});
</script>

<style scoped>
.app-link {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  max-width: 100%;
  border: 1px solid #cfe0ff;
  border-radius: 14px;
  padding: 10px 12px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.5;
  text-decoration: none;
}

.app-link:hover {
  border-color: #8dbbfb;
  background: #f5faff;
}

.app-link svg {
  flex: 0 0 auto;
  margin-top: 2px;
}

.pwd-form {
  width: 100%;
  margin: 0;
  display: grid;
  gap: 12px;
}

.load-state {
  display: grid;
  justify-items: center;
  gap: 10px;
  color: #5f7189;
}

.load-state strong {
  color: #071a39;
  font-size: 15px;
  font-weight: 900;
}

.load-state.error p {
  margin: 0;
  color: #b91c1c;
  font-size: 13px;
  font-weight: 800;
  text-align: center;
}

.spinner {
  width: 30px;
  height: 30px;
  border: 3px solid #dbeafe;
  border-top-color: #1678ff;
  border-radius: 50%;
  animation: pwd-spin 0.9s linear infinite;
}

@keyframes pwd-spin {
  to { transform: rotate(360deg); }
}

.field {
  display: grid;
  gap: 7px;
}

.field-label {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  color: #31445f;
  font-size: 13px;
  font-weight: 900;
}

.field-label svg {
  color: #1678ff;
}

input,
select {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.9);
  color: #071634;
  font: inherit;
  font-size: 14px;
}

input:focus,
select:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

input:disabled,
select:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}

select {
  min-height: 42px;
  cursor: pointer;
}

.hint {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  margin: 0;
  color: #5f7189;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.5;
}

.first-hint {
  width: fit-content;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 6px 10px;
  background: #eff6ff;
  color: #0757d7;
}

.change-note {
  margin: 0;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
}

.reset-note {
  margin: 0;
  color: #31445f;
  font-size: 13px;
  font-weight: 900;
}

.error-message {
  width: 100%;
  border: 1px solid #fecaca;
  border-radius: 14px;
  padding: 9px 11px;
  background: #fff1f2;
  color: #b91c1c;
  font-weight: 900;
}

.verify-status {
  width: fit-content;
  margin: 0;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 12px;
  font-weight: 900;
}

.verify-status.checking {
  border: 1px solid #bfdbfe;
  background: #eff6ff;
  color: #0757d7;
}

.verify-status.passed {
  border: 1px solid #bbf7d0;
  background: #f0fdf4;
  color: #15803d;
}

.verify-status.failed {
  border: 1px solid #fecaca;
  background: #fff1f2;
  color: #b91c1c;
}

.current-account {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.86);
  padding: 10px 12px;
}

.current-account .field-label {
  font-weight: 900;
}

.current-account strong {
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.form-actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
}

.submit {
  justify-self: start;
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-width: 136px;
  border-radius: 15px;
  padding: 9px 15px;
  font-size: 14px;
  font-weight: 950;
  line-height: 1;
  cursor: pointer;
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

.submit:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.link-btn {
  border: 0;
  background: transparent;
  color: #1678ff;
  font-size: 13px;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
  padding: 2px 4px;
}

.link-btn.modest {
  text-decoration: underline;
  text-underline-offset: 3px;
}

.link-btn:hover:not(:disabled) {
  color: #0757d7;
}

.link-btn:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.spin {
  animation: pwd-spin 0.9s linear infinite;
}
</style>
