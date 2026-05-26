<template>
  <section v-if="open" class="admin-shell">
    <div class="admin-card">
      <header>
        <div>
          <strong>管理员工具</strong>
          <p>查看后台状态、权限、交接班链接和 mock 压测，不触发真实飞书或多维写入。</p>
        </div>
        <button class="btn ghost" @click="$emit('close')">关闭</button>
      </header>

      <nav class="tabs">
        <button :class="{ active: tab === 'status' }" @click="tab = 'status'; loadStatus()">状态</button>
        <button :class="{ active: tab === 'permissions' }" @click="tab = 'permissions'; loadPermissions()">权限</button>
        <button :class="{ active: tab === 'handover' }" @click="tab = 'handover'; loadHandover()">交接班链接</button>
        <button :class="{ active: tab === 'pressure' }" @click="tab = 'pressure'">mock 压测</button>
      </nav>

      <section v-if="message" class="message">{{ message }}</section>

      <section v-if="tab === 'status'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadStatus">刷新状态</button>
          <button class="btn ghost" @click="runPreflight">真实联调预检</button>
          <button class="btn ghost" @click="cleanupJobs">清理终态任务</button>
        </div>
        <div class="metric-grid">
          <article>
            <span>消息队列</span>
            <strong>{{ stats.message_queue_size ?? "-" }}</strong>
          </article>
          <article>
            <span>Qt 队列</span>
            <strong>{{ stats.qt_queue_size ?? stats.qt_outbox_pending ?? "-" }}</strong>
          </article>
          <article>
            <span>上传等待</span>
            <strong>{{ stats.upload_wait_size ?? "-" }}</strong>
          </article>
          <article>
            <span>SSE 连接</span>
            <strong>{{ stats.sse_connections?.connections ?? "-" }}</strong>
          </article>
        </div>
        <pre>{{ pretty({ stats, perf, queues, recentJobs }) }}</pre>
      </section>

      <section v-else-if="tab === 'permissions'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadPermissions">刷新权限</button>
          <button class="btn green" @click="savePermissions">保存权限</button>
          <button class="btn ghost" @click="addPermissionUser">添加用户</button>
        </div>
        <div class="permission-list">
          <article v-for="user in permissions.users" :key="user.open_id" class="permission-row">
            <input v-model="user.name" placeholder="姓名" :disabled="user.locked" />
            <input v-model="user.open_id" placeholder="openid" :disabled="user.locked" />
            <select v-model="user.role" :disabled="user.locked">
              <option value="building">用户</option>
              <option value="admin">管理员</option>
            </select>
            <label><input v-model="user.enabled" type="checkbox" :disabled="user.locked" /> 启用</label>
            <button class="btn danger" :disabled="user.locked" @click="removePermissionUser(user)">删除</button>
            <div class="scope-checks">
              <label v-for="scope in scopeOptions" :key="scope.value">
                <input
                  type="checkbox"
                  :checked="user.scopes?.includes(scope.value)"
                  :disabled="user.locked || user.role === 'admin'"
                  @change="toggleUserScope(user, scope.value, ($event.target as HTMLInputElement).checked)"
                />
                {{ scope.label }}
              </label>
            </div>
          </article>
        </div>
      </section>

      <section v-else-if="tab === 'handover'" class="pane">
        <div class="actions">
          <button class="btn blue" @click="loadHandover">刷新链接</button>
          <button class="btn green" @click="saveHandover">保存链接</button>
        </div>
        <label class="password-line">
          设置密码
          <input v-model="handoverPassword" type="password" placeholder="输入交接班链接设置密码" />
        </label>
        <div class="handover-grid">
          <label v-for="scope in buildingScopes" :key="scope.value">
            {{ scope.label }}
            <input v-model="handoverLinks[scope.value]" placeholder="https://..." />
          </label>
        </div>
      </section>

      <section v-else class="pane">
        <div class="pressure-form">
          <label>数量 <input v-model.number="pressure.count" type="number" min="1" max="60" /></label>
          <label>并发 <input v-model.number="pressure.concurrency" type="number" min="1" max="10" /></label>
          <label>
            场景
            <select v-model="pressure.scenario">
              <option value="accepted">accepted</option>
              <option value="mixed">mixed</option>
              <option value="failed-network">failed-network</option>
              <option value="failed-remote-missing">failed-remote-missing</option>
            </select>
          </label>
          <button class="btn blue" :disabled="busy" @click="runMockPressure">运行 mock 压测</button>
        </div>
        <pre>{{ pretty(pressureResult) }}</pre>
      </section>
    </div>
  </section>
</template>

<script setup lang="ts">
import { reactive, ref, watch } from "vue";

type Dict = Record<string, any>;

const props = defineProps<{
  open: boolean;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

defineEmits<{
  close: [];
}>();

const buildingScopes = [
  { value: "110", label: "110站" },
  { value: "A", label: "A楼" },
  { value: "B", label: "B楼" },
  { value: "C", label: "C楼" },
  { value: "D", label: "D楼" },
  { value: "E", label: "E楼" },
  { value: "H", label: "H楼" },
];

const tab = ref<"status" | "permissions" | "handover" | "pressure">("status");
const busy = ref(false);
const message = ref("");
const stats = ref<Dict>({});
const perf = ref<Dict>({});
const queues = ref<Dict>({});
const recentJobs = ref<Dict>({});
const permissions = reactive<{ users: Dict[]; scope_options: Dict[] }>({ users: [], scope_options: [] });
const handoverLinks = reactive<Record<string, string>>({});
const handoverPassword = ref("");
const pressure = reactive({ count: 60, concurrency: 10, scenario: "accepted" });
const pressureResult = ref<Dict>({});

watch(() => props.open, (open) => {
  if (open) void loadStatus();
});

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

function pretty(value: unknown): string {
  return JSON.stringify(value || {}, null, 2);
}

async function loadStatus(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const [statsData, perfData, queuesData, jobsData] = await Promise.all([
      api("/api/backend/stats"),
      api("/api/backend/perf"),
      api("/api/backend/queues"),
      api("/api/jobs/recent?limit=20"),
    ]);
    stats.value = statsData;
    perf.value = perfData;
    queues.value = queuesData;
    recentJobs.value = jobsData;
  } catch (error: any) {
    message.value = error?.message || "状态加载失败";
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
    message.value = `清理完成：${data.deleted || 0}`;
    await loadStatus();
  } catch (error: any) {
    message.value = error?.message || "清理失败";
  } finally {
    busy.value = false;
  }
}

async function loadPermissions(): Promise<void> {
  message.value = "";
  busy.value = true;
  try {
    const data = await api("/api/auth/permissions");
    permissions.users.splice(0, permissions.users.length, ...(data.users || []));
    permissions.scope_options.splice(0, permissions.scope_options.length, ...(data.scope_options || []));
  } catch (error: any) {
    message.value = error?.message || "权限加载失败";
  } finally {
    busy.value = false;
  }
}

function addPermissionUser(): void {
  permissions.users.push({ open_id: "", name: "", role: "building", scopes: [], enabled: true });
}

function removePermissionUser(user: Dict): void {
  if (user.locked) return;
  const label = String(user.name || user.open_id || "该用户");
  if (!window.confirm(`确认删除「${label}」的门户权限？保存后生效。`)) return;
  const index = permissions.users.indexOf(user);
  if (index >= 0) {
    permissions.users.splice(index, 1);
    message.value = "已从列表移除，点击保存权限后生效。";
  }
}

function toggleUserScope(user: Dict, scope: string, checked: boolean): void {
  const next = new Set((user.scopes || []) as string[]);
  if (checked) next.add(scope);
  else next.delete(scope);
  user.scopes = Array.from(next);
}

async function savePermissions(): Promise<void> {
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

async function runMockPressure(): Promise<void> {
  busy.value = true;
  pressureResult.value = {};
  try {
    pressureResult.value = await api("/api/backend/mock-pressure", {
      method: "POST",
      body: JSON.stringify(pressure),
    });
    message.value = "mock 压测完成";
  } catch (error: any) {
    message.value = error?.message || "mock 压测失败";
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.admin-shell {
  position: fixed;
  inset: 0;
  z-index: 100;
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
  gap: 12px;
  padding: 18px;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.22);
}

header,
.actions,
.tabs,
.pressure-form {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

header {
  justify-content: space-between;
}

p {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
}

.btn,
button {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
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

.tabs {
  padding: 4px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
  background: #f8fafc;
}

.tabs button {
  border: 0;
  background: transparent;
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

pre {
  max-height: 320px;
  overflow: auto;
  padding: 12px;
  border-radius: 8px;
  background: #0f172a;
  color: #e2e8f0;
  font-size: 12px;
}

.permission-list,
.handover-grid {
  display: grid;
  gap: 10px;
}

.permission-row {
  display: grid;
  grid-template-columns: minmax(120px, 0.7fr) minmax(180px, 1.2fr) 110px 90px auto;
  gap: 8px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
}

.scope-checks {
  grid-column: 1 / -1;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
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

.handover-grid {
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

@media (max-width: 820px) {
  .permission-row {
    grid-template-columns: 1fr;
  }
}
</style>
