<template>
  <section v-if="checking" class="center-state">
    <div class="spinner"></div>
    <strong>正在确认登录状态</strong>
  </section>

  <section v-else-if="!loggedIn" class="center-state">
    <strong>请先使用飞书登录</strong>
    <a class="btn blue" :href="loginUrl || '/api/auth/login'">飞书扫码登录</a>
  </section>

  <section v-else class="center-state request-panel">
    <button type="button" v-if="showBack" class="back-link" @click="$emit('back')">
      <span aria-hidden="true">‹</span>
      返回
    </button>
    <strong>{{ title || "当前账号暂无门户权限" }}</strong>
    <p class="user-line" :title="userOpenId ? `飞书身份：${userOpenId}` : ''">{{ userLineText }}</p>
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
import { computed } from "vue";

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
}>();

const emit = defineEmits<{
  submit: [];
  confirm: [];
  back: [];
  "update-request": [patch: Partial<typeof props.request>];
}>();

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

.back-link {
  min-height: 36px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  border: 1px solid #d4e3f7;
  border-radius: 999px;
  padding: 0 13px;
  background: rgba(255, 255, 255, 0.9);
  color: #0757d7;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
}

.back-link span {
  font-size: 19px;
  line-height: 1;
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

.login-hint {
  display: inline-flex;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 6px 10px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 12px;
  font-weight: 900;
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

  .scope-pill {
    flex: 1 1 136px;
    justify-content: center;
  }

  .request-steps {
    grid-template-columns: 1fr;
  }
}
</style>
