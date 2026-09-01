<template>
  <main v-if="printMode" class="morning-print-page">
    <div v-if="morningPrintLoading" class="morning-print-state" role="status">
      正在准备晨会表格…
    </div>
    <div v-else-if="morningPrintError" class="morning-print-state error" role="alert">
      {{ morningPrintError }}
    </div>
    <MorningMeetingSheet v-else :model="morningPrintModel" />
  </main>
  <section v-else class="daily-page">
    <header class="daily-header">
      <div class="daily-header__left">
        <VnetBackButton @click="navigate('/')" />
        <div>
          <span>每日任务清单</span>
          <h1>{{ scopeLabel }}今日工作</h1>
        </div>
      </div>
      <div class="daily-header__actions">
        <button
          v-if="scopeCode === 'H'"
          type="button"
          class="btn secondary"
          :disabled="loading || selectedDate !== today || morningLoading"
          title="生成当天A-E、H楼及110站晨会表格"
          @click="openMorningMeeting"
        >
          <FileSpreadsheet :size="17" aria-hidden="true" />
          生成晨会表格
        </button>
        <button
          type="button"
          class="btn primary"
          :disabled="loading || selectedDate !== today"
          :title="selectedDate === today ? '选择人员并发送今日工作汇总' : '仅支持发送今天的工作汇总'"
          @click="openSendDialog"
        >
          <Send :size="17" aria-hidden="true" />
          发送今日工作至飞书
        </button>
        <VnetSelect
          v-if="availableScopes.length > 1"
          :model-value="scopeCode"
          :options="availableScopes.map((item) => item.value)"
          input-id="daily-scope-switch"
          label="楼栋"
          @update:model-value="switchScope"
        />
        <button
          type="button"
          class="btn secondary"
          :disabled="loading"
          @click="loadTasks"
        >
          <RefreshCw :size="17" :class="{ spinning: loading }" aria-hidden="true" />
          {{ loading ? "读取中" : "刷新" }}
        </button>
      </div>
    </header>

    <div class="date-toolbar" aria-label="任务日期">
      <button
        type="button"
        class="icon-button"
        aria-label="查看前一天"
        @click="moveDate(-1)"
      >
        <ChevronLeft :size="19" aria-hidden="true" />
      </button>
      <label class="date-input">
        <CalendarDays :size="17" aria-hidden="true" />
        <input
          v-model="selectedDate"
          type="date"
          :max="today"
          aria-label="选择任务日期"
          @change="changeDate"
        />
      </label>
      <button
        type="button"
        class="today-button"
        :disabled="selectedDate === today"
        @click="setToday"
      >
        今天
      </button>
      <button
        type="button"
        class="icon-button"
        aria-label="查看后一天"
        :disabled="selectedDate >= today"
        @click="moveDate(1)"
      >
        <ChevronRight :size="19" aria-hidden="true" />
      </button>
      <span class="generated-time">{{ generatedText }}</span>
    </div>

    <div v-if="errorText" class="message error" role="alert">
      <CircleAlert :size="18" aria-hidden="true" />
      <span>{{ errorText }}</span>
      <button type="button" @click="loadTasks">重试</button>
    </div>
    <div v-else-if="warnings.length" class="message warning" role="status">
      <CircleAlert :size="18" aria-hidden="true" />
      <span>{{ warnings.join("；") }}</span>
    </div>

    <section class="summary-grid" aria-label="每日任务统计">
      <article>
        <span class="summary-icon total"><ClipboardList :size="20" /></span>
        <div><small>今日任务</small><strong>{{ stats.total }}</strong></div>
      </article>
      <article>
        <span class="summary-icon ongoing"><Clock3 :size="20" /></span>
        <div><small>进行中</small><strong>{{ stats.ongoing }}</strong></div>
      </article>
      <article>
        <span class="summary-icon completed"><CheckCircle2 :size="20" /></span>
        <div><small>已完成</small><strong>{{ stats.completed }}</strong></div>
      </article>
      <article>
        <span class="summary-icon attention"><CircleAlert :size="20" /></span>
        <div><small>需关注</small><strong>{{ stats.attention }}</strong></div>
      </article>
    </section>

    <nav class="category-tabs" aria-label="任务分类">
      <button
        type="button"
        :class="{ active: selectedCategory === 'all' }"
        @click="selectedCategory = 'all'"
      >
        全部
        <b>{{ stats.total }}</b>
      </button>
      <button
        v-for="category in categories"
        :key="category.key"
        type="button"
        :class="[category.key, { active: selectedCategory === category.key }]"
        @click="selectedCategory = category.key"
      >
        {{ category.label }}
        <b>{{ category.count }}</b>
      </button>
    </nav>

    <main class="task-content" :aria-busy="loading">
      <div v-if="loading && !hasStableData" class="loading-state" role="status">
        <span class="spinner" aria-hidden="true"></span>
        <strong>正在整理每日任务</strong>
      </div>
      <div v-else-if="!visibleTaskCount" class="empty-state">
        <ClipboardCheck :size="38" aria-hidden="true" />
        <strong>{{ selectedDate === today ? "今天暂无任务记录" : "这一天暂无任务记录" }}</strong>
      </div>
      <template v-else>
        <section
          v-for="category in visibleCategories"
          :key="category.key"
          class="task-group"
          :class="category.key"
        >
          <header>
            <div>
              <span class="group-icon" aria-hidden="true">
                <Megaphone v-if="category.key === 'notice'" :size="18" />
                <Siren v-else-if="category.key === 'event'" :size="18" />
                <Wrench v-else-if="category.key === 'repair'" :size="18" />
                <FileCheck2 v-else-if="category.key === 'mop'" :size="18" />
                <Droplets v-else :size="18" />
              </span>
              <h2>{{ category.label }}</h2>
            </div>
            <b>{{ category.count }} 项</b>
          </header>

          <div class="task-list">
            <article
              v-for="task in category.tasks"
              :key="task.task_id"
              class="task-row"
            >
              <div class="task-main">
                <div class="task-title-line">
                  <span class="type-badge">{{ task.type_label }}</span>
                  <span v-if="task.level" class="level-badge">{{ task.level }}</span>
                  <strong>{{ task.title }}</strong>
                </div>
                <div class="task-meta">
                  <span v-if="task.building">{{ task.building }}</span>
                  <span v-if="task.specialty">{{ task.specialty }}</span>
                  <span v-if="task.action_summary">{{ task.action_summary }}</span>
                </div>
                <div
                  v-if="typeof task.progress_percent === 'number'"
                  class="progress-line"
                  :aria-label="`当前进度 ${task.progress_percent}%`"
                >
                  <span><i :style="{ width: `${clampProgress(task.progress_percent)}%` }"></i></span>
                  <b>{{ clampProgress(task.progress_percent) }}%</b>
                </div>
              </div>
              <div class="task-state">
                <span class="status-pill" :class="task.status_tone">{{ task.status }}</span>
                <time>{{ task.time || "已记录" }}</time>
              </div>
            </article>
          </div>
        </section>
      </template>
      <div v-if="loading && hasStableData" class="updating-mask" role="status">
        正在更新
      </div>
    </main>

    <div v-if="sendDialogOpen" class="send-dialog-backdrop" @click.self="closeSendDialog">
      <section class="send-dialog" role="dialog" aria-modal="true" aria-labelledby="daily-send-title">
        <header>
          <div>
            <span>飞书发送</span>
            <h2 id="daily-send-title">发送{{ scopeLabel }}今日工作</h2>
          </div>
          <button type="button" class="dialog-close" :disabled="sendBusy" @click="closeSendDialog">关闭</button>
        </header>
        <RepairPeoplePicker
          v-model="sendRecipients"
          :scope="scopeCode"
          input-id="daily-report-recipients"
          label="选择接收人员"
          :disabled="sendBusy"
        />
        <div v-if="sendError" class="dialog-message error" role="alert">{{ sendError }}</div>
        <div v-else-if="sendSuccess" class="dialog-message success" role="status">{{ sendSuccess }}</div>
        <footer>
          <span>已选择 {{ sendRecipients.length }} 人</span>
          <div>
            <button type="button" class="btn secondary" :disabled="sendBusy" @click="closeSendDialog">取消</button>
            <button
              type="button"
              class="btn primary"
              :disabled="sendBusy || !sendRecipients.length"
              @click="sendTodayReport"
            >
              <Send :size="16" aria-hidden="true" />
              {{ sendBusy ? "发送中" : "确认发送" }}
            </button>
          </div>
        </footer>
      </section>
    </div>

    <div v-if="morningDialogOpen" class="send-dialog-backdrop" @click.self="closeMorningMeeting">
      <section class="morning-dialog" role="dialog" aria-modal="true" aria-labelledby="morning-dialog-title">
        <header>
          <div>
            <span>H楼晨会</span>
            <h2 id="morning-dialog-title">生成当天晨会表格</h2>
          </div>
          <button type="button" class="dialog-close" :disabled="morningBusy" @click="closeMorningMeeting">关闭</button>
        </header>
        <div v-if="morningLoading" class="morning-inline-state" role="status">
          <span class="spinner" aria-hidden="true"></span> 正在汇总当天通告
        </div>
        <template v-else-if="morningModel">
          <div class="morning-weather-fields">
            <label><span>天气</span><input v-model="morningModel.weather_condition" maxlength="40" /></label>
            <label><span>干球温度（℃）</span><input v-model="morningModel.dry_bulb_temperature" type="number" min="-50" max="80" step="0.1" /></label>
            <label><span>湿球温度（℃）</span><input v-model="morningModel.wet_bulb_temperature" type="number" min="-50" max="80" step="0.1" /></label>
          </div>
          <div v-if="morningWarnings.length" class="dialog-message warning" role="status">
            {{ morningWarnings.join('；') }}
          </div>
          <div class="morning-preview-wrap">
            <MorningMeetingSheet :model="morningModel" />
          </div>
          <div v-if="morningError" class="dialog-message error" role="alert">{{ morningError }}</div>
          <div v-else-if="morningSuccess" class="dialog-message success" role="status">{{ morningSuccess }}</div>
        </template>
        <div v-else-if="morningError" class="dialog-message error" role="alert">{{ morningError }}</div>
        <footer>
          <span>{{ morningGeneratedAt ? `生成于 ${morningGeneratedAt}` : '生成后可下载或打印' }}</span>
          <div>
            <button type="button" class="btn secondary" :disabled="morningBusy || !morningDownloadUrl" @click="downloadMorningMeeting">
              <Download :size="16" /> 下载Excel
            </button>
            <button type="button" class="btn secondary" :disabled="morningBusy || !morningPrintUrl" @click="printMorningMeeting">
              <Printer :size="16" /> 打印
            </button>
            <button type="button" class="btn primary" :disabled="morningBusy || !morningModel" @click="generateMorningMeeting">
              <FileSpreadsheet :size="16" /> {{ morningBusy ? '生成中' : morningDownloadUrl ? '重新生成' : '生成表格' }}
            </button>
          </div>
        </footer>
      </section>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  CalendarDays,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  ClipboardCheck,
  ClipboardList,
  Clock3,
  Download,
  Droplets,
  FileCheck2,
  FileSpreadsheet,
  Megaphone,
  RefreshCw,
  Printer,
  Send,
  Siren,
  Wrench,
} from "lucide-vue-next";
import { requestJson } from "../api/client";
import { navigate } from "../navigation";
import VnetBackButton from "./VnetBackButton.vue";
import VnetSelect from "./VnetSelect.vue";
import RepairPeoplePicker from "./RepairPeoplePicker.vue";
import MorningMeetingSheet from "./MorningMeetingSheet.vue";

type Dict = Record<string, any>;
type DailyTask = {
  task_id: string;
  category: string;
  category_label: string;
  type_key: string;
  type_label: string;
  title: string;
  status: string;
  status_tone: "ongoing" | "completed" | "warning" | "error" | "neutral";
  time: string;
  sort_time: number;
  action_summary: string;
  building: string;
  specialty: string;
  level: string;
  progress_percent: number | null;
};
type DailyCategory = {
  key: string;
  label: string;
  count: number;
  tasks: DailyTask[];
};

const props = withDefaults(defineProps<{
  scope: string;
  scopeOptions: Array<{ value: string; label: string }>;
  printMode?: boolean;
}>(), {
  printMode: false,
});

const emit = defineEmits<{
  "switch-scope": [scope: string];
  status: [text: string];
}>();

const today = localDateKey(new Date());
const selectedDate = ref(validDateParam(new URLSearchParams(window.location.search).get("date")) || today);
const selectedCategory = ref("all");
const loading = ref(false);
const errorText = ref("");
const payload = ref<Dict>({});
const sendDialogOpen = ref(false);
const sendRecipients = ref<Dict[]>([]);
const sendBusy = ref(false);
const sendError = ref("");
const sendSuccess = ref("");
const morningDialogOpen = ref(false);
const morningLoading = ref(false);
const morningBusy = ref(false);
const morningError = ref("");
const morningSuccess = ref("");
const morningModel = ref<Dict | null>(null);
const morningDownloadUrl = ref("");
const morningPrintUrl = ref("");
const morningGeneratedAt = ref("");
const morningOperationId = ref("");
const morningPrintLoading = ref(false);
const morningPrintError = ref("");
const morningPrintModel = ref<Dict>({});
let morningPrintStyle: HTMLStyleElement | null = null;
let requestController: AbortController | null = null;
let requestGeneration = 0;

const scopeCode = computed(() => normalizeScope(props.scope));
const availableScopes = computed(() => {
  const seen = new Set<string>();
  return props.scopeOptions
    .map((item) => ({
      value: normalizeScope(item.value),
      label: String(item.label || "").trim(),
    }))
    .filter((item) => item.value && !seen.has(item.value) && seen.add(item.value));
});
const scopeLabel = computed(() => {
  const found = availableScopes.value.find((item) => item.value === scopeCode.value);
  if (found?.label) return found.label;
  if (scopeCode.value === "ALL") return "全部楼栋";
  if (scopeCode.value === "CAMPUS") return "园区";
  if (scopeCode.value === "110") return "110站";
  return `${scopeCode.value}楼`;
});
const stats = computed(() => ({
  total: Number(payload.value.stats?.total || 0),
  ongoing: Number(payload.value.stats?.ongoing || 0),
  completed: Number(payload.value.stats?.completed || 0),
  attention: Number(payload.value.stats?.attention || 0),
}));
const categories = computed<DailyCategory[]>(() => (
  Array.isArray(payload.value.categories)
    ? payload.value.categories.map((item: Dict) => ({
        key: String(item.key || ""),
        label: String(item.label || ""),
        count: Number(item.count || 0),
        tasks: Array.isArray(item.tasks) ? item.tasks as DailyTask[] : [],
      }))
    : []
));
const visibleCategories = computed(() => {
  if (selectedCategory.value === "all") {
    return categories.value.filter((item) => item.count > 0);
  }
  return categories.value.filter((item) => item.key === selectedCategory.value);
});
const visibleTaskCount = computed(() => (
  visibleCategories.value.reduce((total, item) => total + item.tasks.length, 0)
));
const warnings = computed(() => (
  Array.isArray(payload.value.warnings)
    ? payload.value.warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
    : []
));
const hasStableData = computed(() => Boolean(payload.value.date));
const generatedText = computed(() => {
  const value = String(payload.value.generated_at || "").trim();
  if (!value) return "";
  return `更新于 ${value.slice(11, 16)}`;
});
const morningWarnings = computed(() => (
  Array.isArray(morningModel.value?.warnings)
    ? morningModel.value?.warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
    : []
));

function normalizeScope(value: string): string {
  const text = String(value || "").trim().toUpperCase();
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : "ALL";
}

function localDateKey(value: Date): string {
  return [
    value.getFullYear(),
    String(value.getMonth() + 1).padStart(2, "0"),
    String(value.getDate()).padStart(2, "0"),
  ].join("-");
}

function validDateParam(value: string | null): string {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
}

function clampProgress(value: number): number {
  return Math.max(0, Math.min(100, Math.round(Number(value || 0))));
}

function syncDateRoute(): void {
  const url = new URL(window.location.href);
  if (selectedDate.value === today) url.searchParams.delete("date");
  else url.searchParams.set("date", selectedDate.value);
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

function switchScope(value: string): void {
  emit("switch-scope", normalizeScope(value));
}

function setToday(): void {
  selectedDate.value = today;
  changeDate();
}

function moveDate(delta: number): void {
  const base = new Date(`${selectedDate.value}T12:00:00`);
  if (Number.isNaN(base.getTime())) return;
  base.setDate(base.getDate() + delta);
  const next = localDateKey(base);
  selectedDate.value = next > today ? today : next;
  changeDate();
}

function changeDate(): void {
  if (!validDateParam(selectedDate.value)) {
    selectedDate.value = today;
  }
  if (selectedDate.value > today) selectedDate.value = today;
  syncDateRoute();
  closeSendDialog();
  closeMorningMeeting();
  void loadTasks();
}

function openSendDialog(): void {
  if (loading.value || selectedDate.value !== today) return;
  sendRecipients.value = [];
  sendError.value = "";
  sendSuccess.value = "";
  sendDialogOpen.value = true;
}

function closeSendDialog(): void {
  if (sendBusy.value) return;
  sendDialogOpen.value = false;
  sendError.value = "";
  sendSuccess.value = "";
}

async function openMorningMeeting(): Promise<void> {
  if (scopeCode.value !== "H" || selectedDate.value !== today || morningLoading.value) return;
  morningDialogOpen.value = true;
  morningLoading.value = true;
  morningError.value = "";
  morningSuccess.value = "";
  try {
    const data = await requestJson(
      `/api/daily-tasks/morning-meeting/preview?date=${encodeURIComponent(selectedDate.value)}`,
      { cache: "no-store", timeoutMs: 60_000 },
    );
    morningModel.value = data;
    morningDownloadUrl.value = String(data.download_url || "");
    morningPrintUrl.value = String(data.print_url || "");
    morningGeneratedAt.value = String(data.generated_at || "");
  } catch (error: any) {
    morningModel.value = null;
    morningError.value = error?.message || "晨会数据读取失败。";
  } finally {
    morningLoading.value = false;
  }
}

function closeMorningMeeting(): void {
  if (morningBusy.value) return;
  morningDialogOpen.value = false;
  morningError.value = "";
  morningSuccess.value = "";
}

async function generateMorningMeeting(): Promise<void> {
  if (morningBusy.value || !morningModel.value) return;
  morningBusy.value = true;
  morningError.value = "";
  morningSuccess.value = "";
  if (!morningOperationId.value) morningOperationId.value = newOperationId();
  try {
    const data = await requestJson("/api/daily-tasks/morning-meeting/generate", {
      method: "POST",
      timeoutMs: 120_000,
      body: JSON.stringify({
        date: selectedDate.value,
        weather_condition: morningModel.value.weather_condition || "",
        dry_bulb_temperature: morningModel.value.dry_bulb_temperature ?? null,
        wet_bulb_temperature: morningModel.value.wet_bulb_temperature ?? null,
        operation_id: morningOperationId.value,
      }),
    });
    morningModel.value = data.model || morningModel.value;
    morningDownloadUrl.value = String(data.download_url || "");
    morningPrintUrl.value = String(data.print_url || "");
    morningGeneratedAt.value = String(data.generated_at || "");
    morningSuccess.value = "晨会表格已生成，可下载或打印。";
    morningOperationId.value = "";
  } catch (error: any) {
    morningError.value = error?.message || "晨会表格生成失败。";
  } finally {
    morningBusy.value = false;
  }
}

function downloadMorningMeeting(): void {
  if (!morningDownloadUrl.value) return;
  const anchor = document.createElement("a");
  anchor.href = morningDownloadUrl.value;
  anchor.download = "";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
}

function printMorningMeeting(): void {
  if (!morningPrintUrl.value) return;
  window.open(morningPrintUrl.value, "_blank", "noopener,noreferrer");
}

async function loadMorningMeetingPrint(): Promise<void> {
  const date = validDateParam(new URLSearchParams(window.location.search).get("date"));
  if (!date) {
    morningPrintError.value = "打印日期缺失。";
    return;
  }
  morningPrintLoading.value = true;
  morningPrintError.value = "";
  try {
    const data = await requestJson(
      `/api/daily-tasks/morning-meeting/print-model?date=${encodeURIComponent(date)}`,
      { cache: "no-store", timeoutMs: 60_000 },
    );
    morningPrintModel.value = data;
    morningPrintStyle?.remove();
    morningPrintStyle = document.createElement("style");
    morningPrintStyle.textContent = "@page { size: A4 landscape; margin: 5mm; }";
    document.head.appendChild(morningPrintStyle);
    await nextTick();
    window.setTimeout(() => window.print(), 120);
  } catch (error: any) {
    morningPrintError.value = error?.message || "晨会表格打印内容读取失败。";
  } finally {
    morningPrintLoading.value = false;
  }
}

function newOperationId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `daily-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function sendTodayReport(): Promise<void> {
  if (sendBusy.value || !sendRecipients.value.length) return;
  sendBusy.value = true;
  sendError.value = "";
  sendSuccess.value = "";
  try {
    const data = await requestJson("/api/daily-tasks/send", {
      method: "POST",
      body: JSON.stringify({
        scope: scopeCode.value,
        date: selectedDate.value,
        recipient_open_ids: sendRecipients.value
          .map((person) => String(person.user_id || person.open_id || "").trim())
          .filter(Boolean),
        operation_id: newOperationId(),
      }),
    });
    const sent = Number(data.sent_count || 0);
    const failed = Number(data.failed_count || 0);
    sendSuccess.value = failed
      ? `已发送 ${sent} 人，${failed} 人发送失败。`
      : `已成功发送给 ${sent} 人。`;
  } catch (error: any) {
    sendError.value = error?.message || "今日工作汇总发送失败。";
  } finally {
    sendBusy.value = false;
  }
}

async function loadTasks(): Promise<void> {
  const generation = ++requestGeneration;
  requestController?.abort();
  requestController = new AbortController();
  loading.value = true;
  errorText.value = "";
  emit("status", "");
  try {
    const query = new URLSearchParams({
      scope: scopeCode.value,
      date: selectedDate.value,
    });
    const data = await requestJson(
      `/api/daily-tasks?${query.toString()}`,
      { cache: "no-store", signal: requestController.signal },
    );
    if (generation !== requestGeneration) return;
    payload.value = data;
    if (
      selectedCategory.value !== "all"
      && !categories.value.some((item) => item.key === selectedCategory.value)
    ) {
      selectedCategory.value = "all";
    }
  } catch (error: any) {
    if (generation !== requestGeneration || error?.message === "请求已取消。") return;
    errorText.value = error?.message || "每日任务读取失败。";
  } finally {
    if (generation === requestGeneration) loading.value = false;
  }
}

watch(
  () => props.scope,
  () => {
    if (props.printMode) return;
    selectedCategory.value = "all";
    closeSendDialog();
    closeMorningMeeting();
    void loadTasks();
  },
);

onMounted(() => {
  if (props.printMode) void loadMorningMeetingPrint();
  else void loadTasks();
});
onBeforeUnmount(() => {
  requestGeneration += 1;
  requestController?.abort();
  morningPrintStyle?.remove();
});
</script>

<style scoped>
.daily-page {
  width: min(1560px, calc(100% - 36px));
  margin: 0 auto;
  padding: 18px 0 32px;
  color: #0b1f3a;
}

.daily-header,
.date-toolbar,
.summary-grid,
.category-tabs,
.task-group {
  border: 1px solid #d9e6f6;
  background: rgba(255, 255, 255, 0.96);
}

.daily-header {
  min-height: 76px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  padding: 14px 18px;
  border-radius: 12px;
  box-shadow: 0 14px 34px rgba(22, 73, 137, 0.09);
}

.daily-header__left,
.daily-header__actions,
.daily-header__left > div,
.date-toolbar,
.summary-grid article,
.task-group > header,
.task-group > header > div,
.task-title-line,
.task-meta,
.task-state {
  display: flex;
  align-items: center;
}

.daily-header__left {
  min-width: 0;
  gap: 14px;
}

.daily-header__left > div {
  min-width: 0;
  align-items: flex-start;
  flex-direction: column;
  gap: 2px;
}

.daily-header__left span {
  color: #5d7290;
  font-size: 12px;
  font-weight: 850;
}

.daily-header h1 {
  max-width: 680px;
  overflow: hidden;
  margin: 0;
  color: #071a39;
  font-size: 21px;
  line-height: 1.25;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.daily-header__actions {
  justify-content: flex-end;
  gap: 9px;
}

.btn,
.icon-button,
.today-button,
.category-tabs button,
.message button {
  border: 0;
  font: inherit;
  cursor: pointer;
}

.btn {
  min-height: 38px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  padding: 0 14px;
  border-radius: 9px;
  font-size: 13px;
  font-weight: 900;
}

.btn.secondary {
  border: 1px solid #cfe0f7;
  background: #fff;
  color: #1558b7;
}

.btn.primary {
  background: #1763d7;
  color: #fff;
  box-shadow: 0 8px 18px rgba(23, 99, 215, 0.2);
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.54;
}

.date-toolbar {
  min-height: 52px;
  gap: 8px;
  margin-top: 10px;
  padding: 8px 12px;
  border-radius: 10px;
}

.icon-button,
.today-button {
  min-width: 36px;
  height: 36px;
  display: inline-grid;
  place-items: center;
  border: 1px solid #d5e3f4;
  border-radius: 9px;
  background: #fff;
  color: #1759b9;
}

.today-button {
  min-width: 58px;
  padding: 0 12px;
  font-size: 13px;
  font-weight: 900;
}

.date-input {
  height: 36px;
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 0 10px;
  border: 1px solid #cfe0f7;
  border-radius: 9px;
  background: #f8fbff;
  color: #1763d7;
}

.date-input input {
  width: 132px;
  border: 0;
  outline: 0;
  background: transparent;
  color: #17375f;
  font: inherit;
  font-size: 13px;
  font-weight: 850;
}

.generated-time {
  margin-left: auto;
  color: #74869d;
  font-size: 12px;
  font-weight: 750;
}

.message {
  min-height: 42px;
  display: flex;
  align-items: center;
  gap: 9px;
  margin-top: 10px;
  padding: 8px 12px;
  border: 1px solid;
  border-radius: 9px;
  font-size: 13px;
  font-weight: 800;
}

.message span {
  min-width: 0;
  flex: 1;
}

.message.error {
  border-color: #f1c3c3;
  background: #fff6f6;
  color: #a43232;
}

.message.warning {
  border-color: #f0d49d;
  background: #fffaf0;
  color: #8b5b09;
}

.message button {
  padding: 6px 12px;
  border-radius: 7px;
  background: #fff;
  color: inherit;
  font-weight: 900;
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 1px;
  margin-top: 10px;
  overflow: hidden;
  border-radius: 10px;
}

.summary-grid article {
  min-width: 0;
  gap: 12px;
  padding: 13px 16px;
  background: #fff;
}

.summary-grid article + article {
  border-left: 1px solid #e5edf7;
}

.summary-icon {
  width: 40px;
  height: 40px;
  flex: 0 0 auto;
  display: grid;
  place-items: center;
  border-radius: 9px;
}

.summary-icon.total {
  background: #eaf2ff;
  color: #1763d7;
}

.summary-icon.ongoing {
  background: #e8f8ff;
  color: #0787b6;
}

.summary-icon.completed {
  background: #e9f9f1;
  color: #138a5f;
}

.summary-icon.attention {
  background: #fff2df;
  color: #c76d08;
}

.summary-grid small {
  display: block;
  color: #6c7e95;
  font-size: 12px;
  font-weight: 800;
}

.summary-grid strong {
  display: block;
  margin-top: 2px;
  color: #081d3e;
  font-size: 23px;
  line-height: 1;
  font-weight: 950;
}

.category-tabs {
  display: flex;
  align-items: center;
  gap: 5px;
  margin-top: 10px;
  padding: 7px;
  overflow-x: auto;
  border-radius: 10px;
}

.category-tabs button {
  min-height: 34px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  padding: 0 12px;
  border-radius: 8px;
  background: transparent;
  color: #4c6380;
  font-size: 13px;
  font-weight: 900;
  white-space: nowrap;
}

.category-tabs button b {
  min-width: 22px;
  padding: 2px 6px;
  border-radius: 999px;
  background: #edf3fa;
  color: #57708f;
  font-size: 11px;
}

.category-tabs button.active {
  background: #1763d7;
  color: #fff;
  box-shadow: 0 8px 18px rgba(23, 99, 215, 0.2);
}

.category-tabs button.active b {
  background: rgba(255, 255, 255, 0.2);
  color: #fff;
}

.task-content {
  position: relative;
  display: grid;
  gap: 10px;
  margin-top: 10px;
}

.task-group {
  overflow: hidden;
  border-radius: 10px;
}

.task-group > header {
  min-height: 44px;
  justify-content: space-between;
  gap: 16px;
  padding: 8px 13px;
  border-bottom: 1px solid #e5edf7;
  background: #f8fbff;
}

.task-group > header > div {
  gap: 9px;
}

.task-group h2 {
  margin: 0;
  color: #0b2347;
  font-size: 15px;
  font-weight: 950;
}

.task-group > header > b {
  color: #657b97;
  font-size: 12px;
}

.group-icon {
  width: 30px;
  height: 30px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  background: #e7f0ff;
  color: #1763d7;
}

.task-group.event .group-icon {
  background: #fff0e2;
  color: #d36a14;
}

.task-group.repair .group-icon {
  background: #f0ebff;
  color: #7553d5;
}

.task-group.mop .group-icon {
  background: #e8f8ef;
  color: #168659;
}

.task-group.water .group-icon {
  background: #e3f8fb;
  color: #068ca2;
}

.task-list {
  display: grid;
}

.task-row {
  min-height: 68px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 18px;
  padding: 10px 14px;
  background: #fff;
}

.task-row + .task-row {
  border-top: 1px solid #edf2f8;
}

.task-row:hover {
  background: #f9fbfe;
}

.task-main {
  min-width: 0;
}

.task-title-line {
  min-width: 0;
  gap: 7px;
}

.task-title-line strong {
  min-width: 0;
  overflow: hidden;
  color: #0a2144;
  font-size: 14px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.type-badge,
.level-badge,
.status-pill {
  flex: 0 0 auto;
  padding: 4px 7px;
  border-radius: 6px;
  font-size: 11px;
  line-height: 1;
  font-weight: 900;
}

.type-badge {
  background: #eaf2ff;
  color: #1763d7;
}

.level-badge {
  background: #fff0df;
  color: #bb6209;
}

.task-meta {
  min-width: 0;
  gap: 7px;
  margin-top: 6px;
  color: #657991;
  font-size: 12px;
  font-weight: 750;
}

.task-meta span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-meta span + span::before {
  content: "·";
  margin-right: 7px;
  color: #b0bfd0;
}

.task-state {
  min-width: 88px;
  justify-content: flex-end;
  gap: 10px;
}

.task-state time {
  min-width: 40px;
  color: #74859a;
  font-size: 12px;
  font-weight: 800;
  text-align: right;
}

.status-pill.ongoing {
  background: #e7f3ff;
  color: #1261c5;
}

.status-pill.completed {
  background: #e8f8ef;
  color: #107d54;
}

.status-pill.warning {
  background: #fff2df;
  color: #ad610d;
}

.status-pill.error {
  background: #fff0f0;
  color: #b83838;
}

.status-pill.neutral {
  background: #eef2f6;
  color: #5f7085;
}

.progress-line {
  max-width: 320px;
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 7px;
}

.progress-line > span {
  height: 5px;
  flex: 1;
  overflow: hidden;
  border-radius: 999px;
  background: #e8eef6;
}

.progress-line i {
  height: 100%;
  display: block;
  border-radius: inherit;
  background: #397cec;
}

.progress-line b {
  min-width: 34px;
  color: #3f5f87;
  font-size: 11px;
  text-align: right;
}

.loading-state,
.empty-state {
  min-height: 260px;
  display: grid;
  place-items: center;
  align-content: center;
  gap: 12px;
  border: 1px dashed #c8d9ee;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.92);
  color: #647991;
}

.loading-state strong,
.empty-state strong {
  font-size: 14px;
  font-weight: 900;
}

.spinner {
  width: 28px;
  height: 28px;
  border: 3px solid #dce8f7;
  border-top-color: #1763d7;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

.updating-mask {
  position: absolute;
  top: 8px;
  right: 8px;
  z-index: 2;
  padding: 5px 9px;
  border: 1px solid #cfe0f7;
  border-radius: 7px;
  background: rgba(255, 255, 255, 0.96);
  color: #1763d7;
  font-size: 11px;
  font-weight: 900;
  box-shadow: 0 8px 18px rgba(18, 72, 137, 0.1);
}

.send-dialog-backdrop {
  position: fixed;
  inset: 0;
  z-index: 920;
  display: grid;
  place-items: center;
  padding: 20px;
  background: rgba(5, 20, 44, 0.54);
  backdrop-filter: blur(4px);
}

.send-dialog {
  width: min(620px, 100%);
  max-height: calc(100vh - 40px);
  overflow: visible;
  display: grid;
  gap: 16px;
  padding: 20px;
  border: 1px solid #d5e3f5;
  border-radius: 16px;
  background: #f8fbff;
  box-shadow: 0 28px 80px rgba(7, 37, 86, 0.28);
}

.morning-dialog {
  width: min(1240px, 100%);
  max-height: calc(100vh - 28px);
  overflow: auto;
  display: grid;
  gap: 14px;
  padding: 18px;
  border: 1px solid #d5e3f5;
  border-radius: 16px;
  background: #f8fbff;
  box-shadow: 0 28px 80px rgba(7, 37, 86, 0.28);
}

.send-dialog > header,
.send-dialog > footer,
.send-dialog > footer > div,
.morning-dialog > header,
.morning-dialog > footer,
.morning-dialog > footer > div {
  display: flex;
  align-items: center;
}

.send-dialog > header,
.send-dialog > footer,
.morning-dialog > header,
.morning-dialog > footer {
  justify-content: space-between;
  gap: 14px;
}

.send-dialog > header span,
.send-dialog > footer > span,
.morning-dialog > header span,
.morning-dialog > footer > span {
  color: #647991;
  font-size: 12px;
  font-weight: 800;
}

.send-dialog h2 {
  margin: 3px 0 0;
  color: #071a39;
  font-size: 20px;
}

.morning-dialog h2 {
  margin: 3px 0 0;
  color: #071a39;
  font-size: 20px;
}

.send-dialog > footer {
  padding-top: 14px;
  border-top: 1px solid #dce7f5;
}

.morning-dialog > footer {
  position: sticky;
  bottom: -18px;
  z-index: 3;
  padding: 12px 0 0;
  border-top: 1px solid #dce7f5;
  background: #f8fbff;
}

.morning-dialog > footer > div {
  gap: 8px;
}

.morning-weather-fields {
  display: grid;
  grid-template-columns: 1fr 180px 180px;
  gap: 10px;
}

.morning-weather-fields label {
  display: grid;
  gap: 5px;
  color: #526a87;
  font-size: 12px;
  font-weight: 850;
}

.morning-weather-fields input {
  min-height: 38px;
  border: 1px solid #cfe0f7;
  border-radius: 9px;
  padding: 0 10px;
  background: #fff;
  color: #102d56;
  font: inherit;
}

.morning-preview-wrap {
  overflow: auto;
  border: 1px solid #d3dfed;
  border-radius: 10px;
  padding: 10px;
  background: #dfe6ee;
}

.morning-preview-wrap > * {
  margin: 0 auto;
  box-shadow: 0 10px 28px rgba(7, 37, 86, 0.16);
}

.morning-inline-state,
.morning-print-state {
  min-height: 180px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  color: #607792;
  font-weight: 850;
}

.morning-print-page {
  min-height: 100vh;
  display: grid;
  place-items: start center;
  overflow: auto;
  background: #fff;
}

.morning-print-state.error {
  color: #b4232f;
}

.send-dialog > footer > div {
  gap: 8px;
}

.dialog-close {
  min-height: 34px;
  padding: 0 12px;
  border: 1px solid #cfe0f7;
  border-radius: 9px;
  background: #fff;
  color: #1558b7;
  font: inherit;
  font-weight: 900;
  cursor: pointer;
}

.dialog-message {
  padding: 10px 12px;
  border: 1px solid;
  border-radius: 9px;
  font-size: 13px;
  font-weight: 800;
}

.dialog-message.error {
  border-color: #fecaca;
  background: #fff6f6;
  color: #a43232;
}

.dialog-message.success {
  border-color: #a7f3d0;
  background: #ecfdf5;
  color: #047857;
}

.dialog-message.warning {
  border-color: #f0d49d;
  background: #fffaf0;
  color: #8b5b09;
}

.spinning {
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 1100px) {
  .daily-page {
    width: min(100% - 24px, 1560px);
  }

  .summary-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .summary-grid article:nth-child(3) {
    border-left: 0;
    border-top: 1px solid #e5edf7;
  }

  .summary-grid article:nth-child(4) {
    border-top: 1px solid #e5edf7;
  }

  .daily-header,
  .daily-header__actions,
  .morning-dialog > footer {
    align-items: stretch;
    flex-direction: column;
  }

  .morning-weather-fields {
    grid-template-columns: 1fr;
  }
}

@media print {
  .morning-print-page {
    width: 277mm;
    height: 190mm;
    min-height: 0;
    overflow: hidden;
  }

  .morning-print-state {
    display: none !important;
  }
}
</style>
