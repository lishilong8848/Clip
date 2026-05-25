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
      <label v-for="scope in requestableScopes" :key="scope.value">
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

.btn,
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

.btn:disabled {
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

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
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
  min-height: 100px;
  resize: vertical;
}

.verify-box {
  display: grid;
  gap: 8px;
}
</style>
