<template>
  <section v-if="checking" class="center-state">
    <div class="spinner"></div>
    <strong>正在确认登录状态</strong>
  </section>

  <section v-else-if="!loggedIn" class="center-state">
    <strong>请先使用飞书登录</strong>
    <p>登录后会根据 openid 显示可访问楼栋。</p>
    <a class="btn blue" :href="loginUrl || '/api/auth/login'">飞书扫码登录</a>
  </section>

  <section v-else class="center-state request-panel">
    <strong>当前账号暂无门户权限</strong>
    <p>{{ user?.name || "" }} {{ user?.open_id || "" }}</p>
    <div class="scope-checks">
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
    <textarea
      :value="request.reason"
      placeholder="申请原因"
      @input="$emit('update-request', { reason: ($event.target as HTMLTextAreaElement).value })"
    ></textarea>
    <div class="row-actions">
      <button class="btn blue" :disabled="busy" @click="$emit('submit')">提交申请</button>
    </div>
    <div v-if="request.requestId" class="verify-box">
      <span>申请已发送给管理员，请输入验证码。</span>
      <input
        :value="request.code"
        placeholder="6位验证码"
        @input="$emit('update-request', { code: ($event.target as HTMLInputElement).value })"
      />
      <button class="btn green" :disabled="busy" @click="$emit('confirm')">确认授权</button>
    </div>
    <p v-if="request.message" class="hint">{{ request.message }}</p>
  </section>
</template>

<script setup lang="ts">
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
  };
  requestableScopes: Array<{ value: string; label: string }>;
}>();

const emit = defineEmits<{
  submit: [];
  confirm: [];
  "update-request": [patch: Partial<typeof props.request>];
}>();

function toggleScope(scope: string, checked: boolean): void {
  const next = new Set(props.request.scopes);
  if (checked) next.add(scope);
  else next.delete(scope);
  emit("update-request", { scopes: Array.from(next) });
}
</script>

<style scoped>
.center-state {
  width: min(720px, calc(100vw - 32px));
  margin: 58px auto;
  display: grid;
  gap: 16px;
  justify-items: start;
  padding: 36px;
  border: 1px solid #d8e7f8;
  border-radius: 16px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.96)),
    radial-gradient(circle at 12% 16%, rgba(28, 126, 255, 0.12), transparent 30%);
  box-shadow: 0 24px 62px rgba(22, 78, 151, 0.14);
}

.center-state > strong {
  color: #071634;
  font-size: 24px;
  font-weight: 900;
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
  min-height: 40px;
  border: 1px solid #c5d9f2;
  border-radius: 10px;
  padding: 10px 16px;
  background: #ffffff;
  color: #09204a;
  font-size: 14px;
  font-weight: 800;
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
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 14px 30px rgba(20, 103, 226, 0.26);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
  color: #ffffff;
}

p,
.hint {
  margin: 0;
  color: #64748b;
  font-size: 13px;
}

.scope-checks,
.row-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.scope-checks {
  width: 100%;
}

.scope-pill {
  min-height: 38px;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border: 1px solid #c8dcf3;
  border-radius: 999px;
  background: #fbfdff;
  color: #37536f;
  font-size: 13px;
  font-weight: 700;
  cursor: pointer;
  transition: border-color 0.14s ease, background-color 0.14s ease, color 0.14s ease, box-shadow 0.14s ease;
}

.scope-pill:hover {
  border-color: #8dbbfb;
  background: #f5faff;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
}

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
  border: 1px solid #c8dcf3;
  border-radius: 10px;
  padding: 10px 12px;
  background: #fbfdff;
  color: #071634;
  font: inherit;
}

input:focus,
textarea:focus {
  border-color: #1678ff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

textarea {
  min-height: 100px;
  resize: vertical;
}

.scope-pill input[type="checkbox"] {
  width: 15px;
  min-width: 15px;
  height: 15px;
  border: 0;
  padding: 0;
  background: transparent;
  box-shadow: none;
}

.verify-box {
  display: grid;
  gap: 8px;
}

/* VNET auth access skin */
.center-state {
  position: relative;
  overflow: hidden;
}

/* Panorama construction-management polish */
.center-state {
  border-color: rgba(207, 224, 255, 0.94);
  border-radius: 26px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(250, 253, 255, 0.97)),
    radial-gradient(circle at 12% 16%, rgba(48, 128, 255, 0.09), transparent 30%);
  box-shadow: 0 18px 42px rgba(20, 70, 138, 0.11);
}

.center-state > strong {
  font-weight: 820;
  letter-spacing: 0;
}

.scope-pill,
input,
textarea,
.btn,
a.btn {
  border-radius: 15px;
}

.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 14px 28px rgba(21, 93, 252, 0.24);
}

/* Panorama construction-management auth skin */
.center-state {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 18px 42px rgba(0, 47, 135, 0.12);
  backdrop-filter: blur(10px);
}

.scope-pill,
input,
textarea {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

.scope-pill.selected,
.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}

.btn.green {
  background: #059669;
}

input:focus,
textarea:focus {
  border-color: #005bff;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.center-state::after {
  content: "";
  position: absolute;
  right: -70px;
  bottom: -96px;
  width: 260px;
  height: 180px;
  pointer-events: none;
  opacity: 0.52;
  background:
    repeating-linear-gradient(0deg, rgba(22, 120, 255, 0.1) 0 1px, transparent 1px 18px),
    repeating-linear-gradient(90deg, rgba(22, 120, 255, 0.08) 0 1px, transparent 1px 18px);
  transform: rotate(-12deg);
}

.center-state:not(:has(.spinner))::before {
  content: "";
  width: 58px;
  height: 58px;
  border-radius: 14px;
  background:
    linear-gradient(#ffffff, #ffffff) 18px 15px / 22px 5px no-repeat,
    linear-gradient(#ffffff, #ffffff) 18px 26px / 22px 5px no-repeat,
    linear-gradient(#ffffff, #ffffff) 18px 37px / 22px 5px no-repeat,
    linear-gradient(135deg, #0757d7, #1681ff);
  box-shadow: 0 14px 30px rgba(20, 103, 226, 0.26);
}

.center-state > * {
  position: relative;
  z-index: 1;
}

.verify-box {
  width: 100%;
  border: 1px solid #d8e7f8;
  border-radius: 12px;
  padding: 12px;
  background: #f7fbff;
  box-shadow: inset 4px 0 0 #1678ff;
}

/* Softer rounded VNET auth polish */
.center-state {
  border-radius: 24px;
}

.center-state:not(:has(.spinner))::before {
  border-radius: 18px;
}

.btn,
a.btn,
input,
textarea {
  border-radius: 13px;
}

.verify-box {
  border-radius: 18px;
}

.center-state > strong {
  font-weight: 820;
  letter-spacing: 0;
}

p,
.hint {
  color: #5f7189;
}

.scope-pill,
.btn,
a.btn {
  font-weight: 720;
}
</style>
