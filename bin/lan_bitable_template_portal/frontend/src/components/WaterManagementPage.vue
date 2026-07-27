<template>
  <section class="water-page">
    <header class="water-page__header">
      <div class="header-left">
        <VnetBackButton @click="navigate('/')" />
        <div>
          <span>容量管理</span>
          <h1>{{ buildingLabel }}水耗管理</h1>
        </div>
      </div>
      <div class="header-actions">
        <VnetSelect
          v-if="availableScopes.length > 1"
          :model-value="scopeCode"
          :options="availableScopes.map((item) => item.value)"
          input-id="water-scope-switch"
          label="楼栋"
          @update:model-value="switchScope"
        />
        <span class="refresh-time">{{ snapshotText }}</span>
        <button
          type="button"
          class="btn secondary icon-label"
          :disabled="refreshing"
          @click="refreshData"
        >
          <RefreshCw :size="17" :class="{ spinning: refreshing }" aria-hidden="true" />
          {{ refreshing ? "刷新中" : "刷新数据" }}
        </button>
        <button type="button" class="btn primary icon-label" @click="openCreate">
          <Plus :size="18" aria-hidden="true" />
          新增录入
        </button>
      </div>
    </header>

    <div v-if="message.text" class="water-message" :class="message.tone" role="status">
      <span>{{ message.text }}</span>
      <button v-if="message.retry" type="button" @click="message.retry">重试</button>
    </div>

    <section class="water-stats" aria-label="水耗统计">
      <article>
        <span class="stat-icon blue"><ClipboardList :size="20" /></span>
        <div><small>本月记录</small><strong>{{ formatNumber(summary.record_count) }}</strong></div>
      </article>
      <article>
        <span class="stat-icon cyan"><Droplets :size="20" /></span>
        <div><small>本月耗水量</small><strong>{{ formatNumber(summary.total_usage, 2) }} <b>t</b></strong></div>
      </article>
      <article>
        <span class="stat-icon green"><Camera :size="20" /></span>
        <div><small>有照片记录</small><strong>{{ formatNumber(summary.photo_records) }}</strong></div>
      </article>
      <article>
        <span class="stat-icon violet"><Clock3 :size="20" /></span>
        <div><small>最近统计</small><strong class="date-value">{{ formatDateMs(summary.latest_date_ms) }}</strong></div>
      </article>
    </section>

    <section class="water-workspace">
      <div class="filter-bar">
        <VnetSelect
          v-model="filters.range"
          :options="rangeOptions"
          input-id="water-range"
          label="日期范围"
        />
        <label v-if="filters.range === '本月'" class="month-control">
          <span class="sr-only">月份</span>
          <input v-model="filters.month" type="month" />
        </label>
        <template v-if="filters.range === '自定义日期'">
          <label class="date-control">
            <span>从</span>
            <input v-model="filters.startDate" type="date" />
          </label>
          <label class="date-control">
            <span>至</span>
            <input v-model="filters.endDate" type="date" />
          </label>
        </template>
        <VnetSelect
          v-model="filters.meter"
          :options="meterFilterOptions"
          input-id="water-filter-meter"
          label="水表"
        />
        <VnetSelect
          v-model="filters.frequency"
          :options="frequencyFilterOptions"
          input-id="water-filter-frequency"
          label="统计频次"
        />
        <VnetSelect
          v-model="filters.shift"
          :options="shiftFilterOptions"
          input-id="water-filter-shift"
          label="班次"
        />
        <label class="search-control">
          <Search :size="17" aria-hidden="true" />
          <input v-model.trim="filters.query" type="search" placeholder="搜索水表、日期或描述" />
        </label>
      </div>

      <div class="table-shell" :aria-busy="recordsLoading">
        <table>
          <thead>
            <tr>
              <th>统计日期</th>
              <th>水表</th>
              <th>频次</th>
              <th>班次</th>
              <th class="number-cell">水表数值</th>
              <th class="number-cell">修正耗水量</th>
              <th class="number-cell">公式耗水量</th>
              <th class="number-cell">同比</th>
              <th>水表照片</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="recordsLoading && !records.length">
              <td colspan="9" class="table-state">正在读取水耗记录</td>
            </tr>
            <tr v-else-if="!records.length">
              <td colspan="9" class="table-state">当前条件下暂无水耗记录</td>
            </tr>
            <tr
              v-for="record in records"
              :key="record.record_id"
              class="record-row"
              role="button"
              tabindex="0"
              :aria-label="`打开${record.statistic_date || ''}${record.meter || ''}水耗记录`"
              @click="openEdit(record.record_id)"
              @keydown.enter.prevent="openEdit(record.record_id)"
              @keydown.space.prevent="openEdit(record.record_id)"
            >
              <td>{{ record.statistic_date || "未填写" }}</td>
              <td><strong>{{ record.meter || "未填写" }}</strong></td>
              <td><span class="data-tag">{{ record.frequency || "未填" }}</span></td>
              <td>{{ record.shift || "未填" }}</td>
              <td class="number-cell">{{ formatNullableNumber(record.meter_value) }}</td>
              <td class="number-cell">{{ formatNullableNumber(record.corrected_usage) }}</td>
              <td class="number-cell">
                <span v-if="record.computed_usage == null" class="calculating">计算中</span>
                <template v-else>{{ formatNullableNumber(record.computed_usage) }}</template>
              </td>
              <td class="number-cell">{{ formatRatio(record.yoy_ratio) }}</td>
              <td>
                <span
                  class="photo-status"
                  :class="{ 'has-photo': photoCount(record) > 0 }"
                >
                  {{ photoCount(record) > 0 ? "有照片" : "无照片" }}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="recordsLoading && records.length" class="table-loading-overlay">正在更新</div>
      </div>

      <footer class="pagination">
        <span>共 {{ total }} 条，第 {{ page }} / {{ totalPages }} 页</span>
        <div>
          <button type="button" :disabled="page <= 1 || recordsLoading" @click="changePage(page - 1)">
            <ChevronLeft :size="17" />
            上一页
          </button>
          <button type="button" :disabled="page >= totalPages || recordsLoading" @click="changePage(page + 1)">
            下一页
            <ChevronRight :size="17" />
          </button>
        </div>
      </footer>
    </section>

    <div v-if="drawerOpen" class="drawer-backdrop" @click.self="requestCloseDrawer">
      <aside class="record-drawer" role="dialog" aria-modal="true" aria-labelledby="water-drawer-title">
        <header>
          <div>
            <span>{{ editingRecordId ? "修改记录" : "新增录入" }}</span>
            <h2 id="water-drawer-title">{{ buildingLabel }}水耗记录</h2>
          </div>
          <button type="button" class="icon-button" aria-label="关闭" @click="requestCloseDrawer">
            <X :size="20" />
          </button>
        </header>

        <div v-if="drawerLoading" class="drawer-state">正在读取记录详情</div>
        <form
          v-else
          class="record-form"
          :class="{ saving }"
          :aria-busy="saving"
          @submit.prevent="saveRecord"
        >
          <div class="form-grid">
            <label>
              <span>楼栋</span>
              <input :value="buildingLabel" type="text" disabled />
            </label>
            <label>
              <span>统计日期 <b>*</b></span>
              <input v-model="form.statisticDate" type="date" required @input="markDrawerDirty" />
            </label>
            <label>
              <span>水表 <b>*</b></span>
              <VnetSelect
                v-model="form.meter"
                :options="bootstrap.options?.meters || []"
                input-id="water-form-meter"
                label="水表"
                required
                @change="markDrawerDirty"
              />
            </label>
            <label>
              <span>统计频次 <b>*</b></span>
              <VnetSelect
                v-model="form.frequency"
                :options="bootstrap.options?.frequencies || []"
                input-id="water-form-frequency"
                label="统计频次"
                required
                @change="markDrawerDirty"
              />
            </label>
            <label>
              <span>班次 <b>*</b></span>
              <VnetSelect
                v-model="form.shift"
                :options="bootstrap.options?.shifts || []"
                input-id="water-form-shift"
                label="班次"
                required
                @change="markDrawerDirty"
              />
            </label>
            <label>
              <span>水表数值 <b>*</b></span>
              <input
                v-model="form.meterValue"
                type="number"
                min="0"
                step="any"
                placeholder="请输入非负数"
                required
                @input="markDrawerDirty"
              />
            </label>
            <label class="wide-field">
              <span>当期耗水量（修正）</span>
              <input
                v-model="form.correctedUsage"
                type="number"
                step="any"
                placeholder="选填，留空使用公式结果"
                @input="markDrawerDirty"
              />
            </label>
          </div>

          <section v-if="editingRecordId" class="readonly-grid">
            <article><small>变更编码&描述</small><strong>{{ detail.title || "暂无" }}</strong></article>
            <article><small>自增编号</small><strong>{{ detail.auto_number || "暂无" }}</strong></article>
            <article><small>上期日期</small><strong>{{ detail.previous_date_text || "暂无" }}</strong></article>
            <article><small>上期数值</small><strong>{{ detail.previous_value_text || "暂无" }}</strong></article>
            <article><small>公式耗水量</small><strong>{{ detail.computed_usage == null ? "计算中" : formatNullableNumber(detail.computed_usage) }}</strong></article>
            <article><small>上期耗水量</small><strong>{{ formatNullableNumber(detail.previous_usage) }}</strong></article>
            <article><small>耗水量同比</small><strong>{{ formatRatio(detail.yoy_ratio) }}</strong></article>
            <article><small>创建时间</small><strong>{{ detail.created_time || "暂无" }}</strong></article>
          </section>

          <section class="photo-editor">
            <div class="photo-editor__head">
              <div>
                <strong>水表照片 <b>*</b></strong>
                <span>已保留 {{ retainedPhotos.length }} 张，待上传 {{ stagedPhotos.length }} 张</span>
              </div>
              <button
                type="button"
                class="compact-button"
                :disabled="photoUploading || saving"
                @click="openFilePicker"
              >
                <Plus :size="16" />
                添加照片
              </button>
            </div>
            <input
              ref="fileInput"
              class="sr-only"
              type="file"
              accept="image/*"
              multiple
              @change="handleFileInput"
            />
            <div
              class="photo-dropzone"
              :class="{ dragging: photoDragging, disabled: photoUploading || saving }"
              tabindex="0"
              :aria-disabled="photoUploading || saving"
              @click="openFilePicker"
              @dragenter.prevent="photoDragging = true"
              @dragover.prevent="photoDragging = true"
              @dragleave.prevent="photoDragging = false"
              @drop.prevent="handleDrop"
              @paste="handlePaste"
              @keydown.enter.prevent="openFilePicker"
              @keydown.space.prevent="openFilePicker"
            >
              <Upload :size="24" aria-hidden="true" />
              <strong>点击、拖入或 Ctrl+V 粘贴水表照片</strong>
              <span>支持多图，单张不超过 8MB</span>
            </div>
            <div v-if="retainedPhotos.length || stagedPhotos.length" class="photo-editor__grid">
              <article v-for="(photo, index) in retainedPhotos" :key="photo.image_id">
                <img
                  class="clickable-photo"
                  :src="imageUrl(photo.image_id, 'thumb')"
                  :alt="displayPhotoName(photo, index)"
                  role="button"
                  tabindex="0"
                  @click="openLightbox(retainedPhotos, index)"
                  @keydown.enter.prevent="openLightbox(retainedPhotos, index)"
                  @keydown.space.prevent="openLightbox(retainedPhotos, index)"
                />
                <span>{{ displayPhotoName(photo, index) }}</span>
                <button type="button" aria-label="移除照片" @click="askRemoveRetainedPhoto(photo)">
                  <X :size="15" />
                </button>
              </article>
              <article v-for="(photo, index) in stagedPhotos" :key="photo.localId" :class="{ uploading: photo.uploading }">
                <img
                  class="clickable-photo"
                  :src="photo.previewUrl"
                  :alt="photo.name"
                  role="button"
                  tabindex="0"
                  @click="openLightbox(stagedPhotos, index)"
                  @keydown.enter.prevent="openLightbox(stagedPhotos, index)"
                  @keydown.space.prevent="openLightbox(stagedPhotos, index)"
                />
                <span>{{ photo.uploading ? "上传中" : displayPhotoName(photo, index) }}</span>
                <button type="button" aria-label="移除照片" :disabled="photo.uploading" @click="removeStagedPhoto(photo.localId)">
                  <X :size="15" />
                </button>
              </article>
            </div>
          </section>

          <p v-if="formError" class="form-error">{{ formError }}</p>

          <footer class="drawer-actions">
            <span>{{ drawerDirty ? "有未保存修改" : editingRecordId ? "记录已加载" : "请填写必填项" }}</span>
            <div>
              <button type="button" class="btn secondary" :disabled="saving" @click="requestCloseDrawer">取消</button>
              <button type="submit" class="btn primary icon-label" :disabled="saving || photoUploading">
                <Save :size="17" />
                {{ saving ? "保存中" : editingRecordId ? "保存修改" : "新增记录" }}
              </button>
            </div>
          </footer>
        </form>
      </aside>
    </div>

    <div v-if="lightboxPhotos.length" class="lightbox" role="dialog" aria-modal="true" @click.self="closeLightbox">
      <button type="button" class="lightbox-close" aria-label="关闭图片预览" @click="closeLightbox">
        <X :size="24" />
      </button>
      <button
        v-if="lightboxPhotos.length > 1"
        type="button"
        class="lightbox-nav prev"
        aria-label="上一张"
        @click="moveLightbox(-1)"
      >
        <ChevronLeft :size="30" />
      </button>
      <figure>
        <img :src="lightboxPhotoUrl(activeLightboxPhoto, 'original')" :alt="activeLightboxPhoto.name || '水表照片'" />
        <figcaption>
          <span>{{ lightboxIndex + 1 }} / {{ lightboxPhotos.length }} · {{ activeLightboxPhoto.name || "水表照片" }}</span>
          <a :href="lightboxPhotoUrl(activeLightboxPhoto, 'original')" :download="activeLightboxPhoto.name || '水表照片'">
            <Download :size="17" />
            下载
          </a>
        </figcaption>
      </figure>
      <button
        v-if="lightboxPhotos.length > 1"
        type="button"
        class="lightbox-nav next"
        aria-label="下一张"
        @click="moveLightbox(1)"
      >
        <ChevronRight :size="30" />
      </button>
    </div>

    <ConfirmDialog
      :open="confirmState.open"
      :tone="confirmState.tone"
      :title="confirmState.title"
      :message="confirmState.message"
      :confirm-label="confirmState.confirmLabel"
      @resolve="resolveConfirmation"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import {
  Camera,
  ChevronLeft,
  ChevronRight,
  ClipboardList,
  Clock3,
  Download,
  Droplets,
  Plus,
  RefreshCw,
  Save,
  Search,
  Upload,
  X,
} from "lucide-vue-next";
import { requestBinaryJson, requestJson } from "../api/client";
import { navigate } from "../navigation";
import ConfirmDialog from "./ConfirmDialog.vue";
import VnetBackButton from "./VnetBackButton.vue";
import VnetSelect from "./VnetSelect.vue";

type Dict = Record<string, any>;
type ScopeOption = { value: string; label: string };
type StagedPhoto = {
  localId: string;
  uploadId: string;
  name: string;
  previewUrl: string;
  uploading: boolean;
};
type LightboxPhoto = {
  image_id?: string;
  name?: string;
  previewUrl?: string;
};

const props = defineProps<{
  scope: string;
  scopeOptions: ScopeOption[];
}>();

const emit = defineEmits<{
  status: [message: string];
  "switch-scope": [scope: string];
}>();

const WATER_SCOPES = new Set(["A", "B", "C", "D", "E", "H"]);
const rangeOptions = ["近3天", "本月", "全部时间", "自定义日期"];
const bootstrap = ref<Dict>({});
const records = ref<Dict[]>([]);
const summary = computed(() => bootstrap.value.summary || {});
const snapshot = computed(() => bootstrap.value.snapshot || {});
const total = ref(0);
const page = ref(1);
const pageSize = 50;
const recordsLoading = ref(false);
const bootstrapLoading = ref(false);
const refreshing = ref(false);
const message = reactive<{ text: string; tone: "info" | "success" | "warning" | "danger"; retry: null | (() => void) }>({
  text: "",
  tone: "info",
  retry: null,
});
const drawerOpen = ref(false);
const drawerLoading = ref(false);
const drawerDirty = ref(false);
const saving = ref(false);
const pendingOperationId = ref("");
const editingRecordId = ref("");
const detail = ref<Dict>({});
const formError = ref("");
const retainedPhotos = ref<Dict[]>([]);
const stagedPhotos = ref<StagedPhoto[]>([]);
const photoDragging = ref(false);
const uploadBatchRunning = ref(false);
const photoUploading = computed(
  () => uploadBatchRunning.value || stagedPhotos.value.some((item) => item.uploading),
);
const fileInput = ref<HTMLInputElement | null>(null);
const lightboxPhotos = ref<LightboxPhoto[]>([]);
const lightboxIndex = ref(0);
const confirmState = reactive({
  open: false,
  tone: "warning" as "danger" | "warning" | "primary",
  title: "",
  message: "",
  confirmLabel: "确认",
  action: "" as "" | "close" | "remove-photo",
  targetId: "",
});
const filters = reactive({
  range: "近3天",
  month: currentMonth(),
  startDate: recentStartDate(),
  endDate: todayText(),
  meter: "全部水表",
  frequency: "全部频次",
  shift: "全部班次",
  query: "",
});
const form = reactive({
  meter: "",
  frequency: "",
  shift: "",
  statisticDate: todayText(),
  meterValue: "",
  correctedUsage: "",
  expectedVersion: "",
});

let recordsController: AbortController | null = null;
let recordsGeneration = 0;
let bootstrapController: AbortController | null = null;
let bootstrapGeneration = 0;
let detailController: AbortController | null = null;
let detailGeneration = 0;
let filterTimer: number | null = null;
let snapshotPollTimer: number | null = null;
let refreshBaselineVersion = 0;

const scopeCode = computed(() => normalizeScope(props.scope));
const buildingLabel = computed(() => `${scopeCode.value}楼`);
const availableScopes = computed(() => props.scopeOptions
  .map((item) => ({ ...item, value: normalizeScope(item.value) }))
  .filter((item) => WATER_SCOPES.has(item.value)));
const totalPages = computed(() => Math.max(1, Math.ceil(total.value / pageSize)));
const meterFilterOptions = computed(() => ["全部水表", ...(bootstrap.value.options?.meters || [])]);
const frequencyFilterOptions = computed(() => ["全部频次", ...(bootstrap.value.options?.frequencies || [])]);
const shiftFilterOptions = computed(() => ["全部班次", ...(bootstrap.value.options?.shifts || [])]);
const snapshotText = computed(() => {
  if (refreshing.value || snapshot.value.refreshing) return "数据刷新中";
  const value = Number(snapshot.value.refreshed_at || 0);
  if (!value) return "等待首次同步";
  return `更新于 ${formatDateTime(value * 1000)}`;
});
const activeLightboxPhoto = computed(() => lightboxPhotos.value[lightboxIndex.value] || {});

function normalizeScope(value: unknown): string {
  const text = String(value || "").trim().toUpperCase();
  const match = text.match(/[ABCDEH]/);
  return match?.[0] || "";
}

function currentMonth(): string {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function todayText(): string {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
}

function recentStartDate(): string {
  const date = new Date();
  date.setDate(date.getDate() - 2);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function formatNumber(value: unknown, digits = 0): string {
  const number = Number(value || 0);
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: digits }).format(Number.isFinite(number) ? number : 0);
}

function formatNullableNumber(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  return Number.isFinite(number)
    ? new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 3 }).format(number)
    : String(value);
}

function formatRatio(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return `${new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1 }).format(number * 100)}%`;
}

function photoCount(record: Dict): number {
  if (Array.isArray(record.photos)) return record.photos.length;
  const count = Number(record.photo_count || 0);
  return Number.isFinite(count) && count > 0 ? Math.floor(count) : 0;
}

function formatDateMs(value: unknown): string {
  const numeric = Number(value || 0);
  return numeric ? formatDateTime(numeric, true) : "暂无";
}

function formatDateTime(value: number, dateOnly = false): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "暂无";
  const day = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
  if (dateOnly) return day;
  return `${day} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function setMessage(text: string, tone: typeof message.tone = "info", retry: null | (() => void) = null): void {
  Object.assign(message, { text, tone, retry });
  emit("status", tone === "danger" ? text : "");
}

function buildDateParams(params: URLSearchParams): void {
  if (filters.range === "近3天") {
    params.set("start_date", recentStartDate());
    params.set("end_date", todayText());
  } else if (filters.range === "本月" && filters.month) {
    params.set("month", filters.month);
  } else if (filters.range === "自定义日期") {
    if (filters.startDate) params.set("start_date", filters.startDate);
    if (filters.endDate) params.set("end_date", filters.endDate);
  }
}

function buildRecordsUrl(): string {
  const params = new URLSearchParams({
    scope: scopeCode.value,
    page: String(page.value),
    page_size: String(pageSize),
  });
  buildDateParams(params);
  if (filters.meter !== "全部水表") params.set("meter", filters.meter);
  if (filters.frequency !== "全部频次") params.set("frequency", filters.frequency);
  if (filters.shift !== "全部班次") params.set("shift", filters.shift);
  if (filters.query) params.set("q", filters.query);
  return `/api/capacity/water/records?${params.toString()}`;
}

async function loadBootstrap(): Promise<void> {
  if (!scopeCode.value) return;
  bootstrapController?.abort();
  const controller = new AbortController();
  bootstrapController = controller;
  const generation = ++bootstrapGeneration;
  const requestedScope = scopeCode.value;
  bootstrapLoading.value = true;
  try {
    const data = await requestJson(`/api/capacity/water/bootstrap?scope=${encodeURIComponent(requestedScope)}`, {
      cache: "no-store",
      signal: controller.signal,
    });
    if (generation !== bootstrapGeneration || requestedScope !== scopeCode.value) return;
    bootstrap.value = data;
    refreshing.value = Boolean(data.snapshot?.refreshing);
    scheduleSnapshotPoll();
  } catch (error: any) {
    if (generation !== bootstrapGeneration || error?.message === "请求已取消。") return;
    setMessage(error?.message || "水耗基础数据读取失败。", "danger", () => void loadBootstrap());
  } finally {
    if (generation === bootstrapGeneration) bootstrapLoading.value = false;
  }
}

async function loadRecords(): Promise<void> {
  if (!scopeCode.value) return;
  if (filters.range === "自定义日期") {
    if (!filters.startDate || !filters.endDate) {
      records.value = [];
      total.value = 0;
      recordsLoading.value = false;
      setMessage("请选择完整的开始日期和结束日期。", "warning");
      return;
    }
    if (filters.startDate > filters.endDate) {
      records.value = [];
      total.value = 0;
      recordsLoading.value = false;
      setMessage("开始日期不能晚于结束日期。", "warning");
      return;
    }
  }
  recordsController?.abort();
  const controller = new AbortController();
  recordsController = controller;
  const generation = ++recordsGeneration;
  recordsLoading.value = true;
  try {
    const data = await requestJson(buildRecordsUrl(), {
      cache: "no-store",
      signal: controller.signal,
    });
    if (generation !== recordsGeneration) return;
    total.value = Number(data.total || 0);
    if (page.value > totalPages.value) {
      page.value = totalPages.value;
      void loadRecords();
      return;
    }
    records.value = Array.isArray(data.records) ? data.records : [];
  } catch (error: any) {
    if (generation !== recordsGeneration || error?.message === "请求已取消。") return;
    setMessage(error?.message || "水耗记录读取失败。", "danger", () => void loadRecords());
  } finally {
    if (generation === recordsGeneration) recordsLoading.value = false;
  }
}

function scheduleSnapshotPoll(): void {
  clearSnapshotPoll();
  if (!snapshot.value.refreshing && snapshot.value.status === "failed") {
    refreshing.value = false;
    refreshBaselineVersion = 0;
    setMessage(
      `${snapshot.value.error || "水耗数据刷新失败"}，当前仍显示上一次成功数据。`,
      "warning",
      () => void refreshData(),
    );
    return;
  }
  if (!snapshot.value.refreshing && snapshot.value.exists) {
    if (refreshBaselineVersion && Number(snapshot.value.snapshot_version || 0) > refreshBaselineVersion) {
      refreshBaselineVersion = 0;
      refreshing.value = false;
      setMessage("水耗数据已刷新。", "success");
      void loadRecords();
    }
    return;
  }
  snapshotPollTimer = window.setTimeout(async () => {
    snapshotPollTimer = null;
    await loadBootstrap();
    if (snapshot.value.exists) await loadRecords();
  }, 1600);
}

function clearSnapshotPoll(): void {
  if (snapshotPollTimer !== null) {
    window.clearTimeout(snapshotPollTimer);
    snapshotPollTimer = null;
  }
}

async function refreshData(): Promise<void> {
  if (refreshing.value) return;
  refreshing.value = true;
  refreshBaselineVersion = Number(snapshot.value.snapshot_version || 0);
  setMessage("", "info");
  try {
    await requestJson(`/api/capacity/water/refresh?scope=${encodeURIComponent(scopeCode.value)}`, {
      method: "POST",
      body: "{}",
    });
    await loadBootstrap();
    scheduleSnapshotPoll();
  } catch (error: any) {
    refreshing.value = false;
    setMessage(`${error?.message || "刷新失败"}，当前仍显示上一次成功数据。`, "warning", () => void refreshData());
  }
}

function scheduleFilterLoad(): void {
  if (filterTimer !== null) window.clearTimeout(filterTimer);
  filterTimer = window.setTimeout(() => {
    filterTimer = null;
    page.value = 1;
    void loadRecords();
  }, 280);
}

function changePage(nextPage: number): void {
  page.value = Math.max(1, Math.min(nextPage, totalPages.value));
  void loadRecords();
}

function switchScope(scope: string): void {
  const normalized = normalizeScope(scope);
  if (!WATER_SCOPES.has(normalized) || normalized === scopeCode.value) return;
  emit("switch-scope", normalized);
}

function resetForm(): void {
  pendingOperationId.value = "";
  Object.assign(form, {
    meter: "",
    frequency: bootstrap.value.options?.frequencies?.[0] || "日",
    shift: bootstrap.value.options?.shifts?.[0] || "白",
    statisticDate: todayText(),
    meterValue: "",
    correctedUsage: "",
    expectedVersion: "",
  });
  detail.value = {};
  retainedPhotos.value = [];
  clearStagedPhotos();
  formError.value = "";
  drawerDirty.value = false;
}

function openCreate(): void {
  detailController?.abort();
  detailController = null;
  detailGeneration += 1;
  editingRecordId.value = "";
  resetForm();
  drawerOpen.value = true;
}

async function openEdit(recordId: string): Promise<void> {
  const normalizedRecordId = String(recordId || "").trim();
  if (!normalizedRecordId) return;
  detailController?.abort();
  const controller = new AbortController();
  detailController = controller;
  const generation = ++detailGeneration;
  const requestedScope = scopeCode.value;
  editingRecordId.value = normalizedRecordId;
  drawerOpen.value = true;
  drawerLoading.value = true;
  resetForm();
  try {
    const record = await requestJson(
      `/api/capacity/water/records/${encodeURIComponent(normalizedRecordId)}?scope=${encodeURIComponent(requestedScope)}`,
      { cache: "no-store", signal: controller.signal },
    );
    if (
      generation !== detailGeneration
      || editingRecordId.value !== normalizedRecordId
      || requestedScope !== scopeCode.value
    ) return;
    detail.value = record;
    Object.assign(form, {
      meter: String(record.meter || ""),
      frequency: String(record.frequency || ""),
      shift: String(record.shift || ""),
      statisticDate: String(record.statistic_date || record.statistic_date_key || ""),
      meterValue: record.meter_value == null ? "" : String(record.meter_value),
      correctedUsage: record.corrected_usage == null ? "" : String(record.corrected_usage),
      expectedVersion: String(record.version || ""),
    });
    retainedPhotos.value = Array.isArray(record.photos) ? [...record.photos] : [];
    drawerDirty.value = false;
  } catch (error: any) {
    if (generation !== detailGeneration || error?.message === "请求已取消。") return;
    formError.value = error?.message || "记录详情读取失败。";
  } finally {
    if (generation === detailGeneration) drawerLoading.value = false;
  }
}

function markDrawerDirty(): void {
  if (!saving.value) pendingOperationId.value = "";
  drawerDirty.value = true;
}

function requestCloseDrawer(): void {
  if (saving.value) return;
  if (photoUploading.value) {
    setMessage("照片正在上传，请稍候。", "warning");
    return;
  }
  if (drawerDirty.value) {
    Object.assign(confirmState, {
      open: true,
      tone: "warning",
      title: "放弃未保存修改？",
      message: "关闭后，本次填写和刚上传但尚未保存的照片不会写入水耗记录。",
      confirmLabel: "放弃并关闭",
      action: "close",
      targetId: "",
    });
    return;
  }
  closeDrawer();
}

function closeDrawer(): void {
  detailController?.abort();
  detailController = null;
  detailGeneration += 1;
  drawerOpen.value = false;
  drawerLoading.value = false;
  editingRecordId.value = "";
  resetForm();
}

function validateForm(): string {
  if (!form.meter) return "请选择水表。";
  if (!form.frequency) return "请选择统计频次。";
  if (!form.shift) return "请选择班次。";
  if (!form.statisticDate) return "请选择统计日期。";
  const meterValue = Number(form.meterValue);
  if (form.meterValue === "" || !Number.isFinite(meterValue) || meterValue < 0) {
    return "水表数值必须是大于或等于 0 的数字。";
  }
  if (photoUploading.value) return "照片仍在上传，请稍候。";
  if (!retainedPhotos.value.length && !stagedPhotos.value.some((item) => item.uploadId)) {
    return "请至少上传一张水表照片。";
  }
  return "";
}

function operationId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `water_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

async function saveRecord(): Promise<void> {
  if (saving.value) return;
  const error = validateForm();
  if (error) {
    formError.value = error;
    return;
  }
  saving.value = true;
  formError.value = "";
  if (!pendingOperationId.value) pendingOperationId.value = operationId();
  const payload = {
    operation_id: pendingOperationId.value,
    expected_version: form.expectedVersion,
    scope: scopeCode.value,
    meter: form.meter,
    frequency: form.frequency,
    shift: form.shift,
    statistic_date: form.statisticDate,
    meter_value: form.meterValue,
    corrected_usage: form.correctedUsage === "" ? null : form.correctedUsage,
    upload_ids: stagedPhotos.value.map((item) => item.uploadId).filter(Boolean),
    retained_image_ids: retainedPhotos.value.map((item) => item.image_id).filter(Boolean),
  };
  try {
    const path = editingRecordId.value
      ? `/api/capacity/water/records/${encodeURIComponent(editingRecordId.value)}`
      : "/api/capacity/water/records";
    const result = await requestJson(path, {
      method: editingRecordId.value ? "PATCH" : "POST",
      body: JSON.stringify(payload),
    });
    pendingOperationId.value = "";
    drawerDirty.value = false;
    closeDrawer();
    const warnings = Array.isArray(result.warnings) ? result.warnings.filter(Boolean) : [];
    setMessage(
      warnings.length ? `水耗记录已保存。${warnings.join("；")}` : "水耗记录已保存。",
      warnings.length ? "warning" : "success",
    );
    await Promise.all([loadBootstrap(), loadRecords()]);
  } catch (saveError: any) {
    formError.value = saveError?.message || "水耗记录保存失败。";
  } finally {
    saving.value = false;
  }
}

async function uploadFiles(files: File[]): Promise<void> {
  if (saving.value || uploadBatchRunning.value) {
    setMessage("照片正在处理，请稍候。", "warning");
    return;
  }
  const validFiles = files
    .map((file) => ({ file, mimeType: imageMimeType(file) }))
    .filter(({ file, mimeType }) => {
      if (!mimeType) {
        setMessage(`${file.name} 不是图片，已忽略。`, "warning");
        return false;
      }
      if (file.size > 8 * 1024 * 1024) {
        setMessage(`${file.name} 超过 8MB，已忽略。`, "warning");
        return false;
      }
      return true;
    });
  if (!validFiles.length) return;
  uploadBatchRunning.value = true;
  try {
    for (const { file, mimeType } of validFiles) {
      if (!saving.value) pendingOperationId.value = "";
      const localId = operationId();
      const photo: StagedPhoto = {
        localId,
        uploadId: "",
        name: file.name,
        previewUrl: URL.createObjectURL(file),
        uploading: true,
      };
      stagedPhotos.value.push(photo);
      drawerDirty.value = true;
      try {
        const params = new URLSearchParams({
          scope: scopeCode.value,
          file_name: file.name,
        });
        const data = await requestBinaryJson(
          `/api/capacity/water/uploads?${params.toString()}`,
          file,
          { headers: { "Content-Type": mimeType } },
        );
        photo.uploadId = String(data.upload_id || "");
        if (!photo.uploadId) throw new Error("照片上传后未返回 upload_id。");
      } catch (error: any) {
        removeStagedPhoto(localId);
        setMessage(error?.message || `${file.name} 上传失败。`, "danger");
      } finally {
        photo.uploading = false;
      }
    }
  } finally {
    uploadBatchRunning.value = false;
  }
}

function imageMimeType(file: File): string {
  const declared = String(file.type || "").toLowerCase();
  if (declared.startsWith("image/")) return declared;
  const extension = String(file.name || "").split(".").pop()?.toLowerCase() || "";
  const mimeTypes: Record<string, string> = {
    jpg: "image/jpeg",
    jpeg: "image/jpeg",
    png: "image/png",
    webp: "image/webp",
    gif: "image/gif",
    bmp: "image/bmp",
    tif: "image/tiff",
    tiff: "image/tiff",
  };
  return mimeTypes[extension] || "";
}

function openFilePicker(): void {
  if (saving.value || photoUploading.value) return;
  fileInput.value?.click();
}

function handleFileInput(event: Event): void {
  const input = event.target as HTMLInputElement;
  void uploadFiles(Array.from(input.files || []));
  input.value = "";
}

function handleDrop(event: DragEvent): void {
  photoDragging.value = false;
  void uploadFiles(Array.from(event.dataTransfer?.files || []));
}

function handlePaste(event: ClipboardEvent): void {
  const files = Array.from(event.clipboardData?.files || []);
  if (!files.length) {
    setMessage("剪贴板中没有图片文件。", "warning");
    return;
  }
  event.preventDefault();
  void uploadFiles(files);
}

function removeStagedPhoto(localId: string): void {
  const index = stagedPhotos.value.findIndex((item) => item.localId === localId);
  if (index < 0) return;
  URL.revokeObjectURL(stagedPhotos.value[index].previewUrl);
  stagedPhotos.value.splice(index, 1);
  if (!saving.value) pendingOperationId.value = "";
  drawerDirty.value = true;
}

function clearStagedPhotos(): void {
  for (const photo of stagedPhotos.value) URL.revokeObjectURL(photo.previewUrl);
  stagedPhotos.value = [];
}

function askRemoveRetainedPhoto(photo: Dict): void {
  Object.assign(confirmState, {
    open: true,
    tone: "warning",
    title: "移除这张水表照片？",
    message: "保存修改后，这张照片将不再保留在该条水耗记录中。",
    confirmLabel: "移除照片",
    action: "remove-photo",
    targetId: String(photo.image_id || ""),
  });
}

function resolveConfirmation(confirmed: boolean): void {
  const action = confirmState.action;
  const targetId = confirmState.targetId;
  confirmState.open = false;
  confirmState.action = "";
  confirmState.targetId = "";
  if (!confirmed) return;
  if (action === "close") {
    drawerDirty.value = false;
    closeDrawer();
  } else if (action === "remove-photo") {
    retainedPhotos.value = retainedPhotos.value.filter((item) => item.image_id !== targetId);
    if (!saving.value) pendingOperationId.value = "";
    drawerDirty.value = true;
  }
}

function imageUrl(imageId: string, variant: "thumb" | "original"): string {
  const params = new URLSearchParams({ scope: scopeCode.value, variant });
  return `/api/capacity/water/images/${encodeURIComponent(imageId)}?${params.toString()}`;
}

function displayPhotoName(photo: Dict | StagedPhoto, index: number): string {
  const raw = String(photo.name || "水表照片").split(/[\\/]/).pop() || "";
  const match = raw.match(/^(.*?)(\.[^.]+)?$/);
  const stem = String(match?.[1] || "");
  const suffix = String(match?.[2] || ".jpg").toLowerCase();
  if (!stem || /^[0-9a-f]{24,64}$/i.test(stem)) return `水表照片 ${index + 1}${suffix}`;
  return raw;
}

function openLightbox(photos: Array<Dict | StagedPhoto>, index: number): void {
  lightboxPhotos.value = (Array.isArray(photos) ? photos : []).map((photo, photoIndex) => ({
    image_id: String((photo as Dict).image_id || ""),
    name: displayPhotoName(photo, photoIndex),
    previewUrl: String((photo as StagedPhoto).previewUrl || ""),
  }));
  lightboxIndex.value = Math.max(0, Math.min(index, lightboxPhotos.value.length - 1));
}

function lightboxPhotoUrl(photo: LightboxPhoto, variant: "thumb" | "original"): string {
  return photo.previewUrl || imageUrl(String(photo.image_id || ""), variant);
}

function closeLightbox(): void {
  lightboxPhotos.value = [];
  lightboxIndex.value = 0;
}

function moveLightbox(step: number): void {
  const count = lightboxPhotos.value.length;
  if (!count) return;
  lightboxIndex.value = (lightboxIndex.value + step + count) % count;
}

function handleGlobalKeydown(event: KeyboardEvent): void {
  if (event.key !== "Escape") return;
  if (lightboxPhotos.value.length) {
    closeLightbox();
    return;
  }
  if (drawerOpen.value) requestCloseDrawer();
}

watch(
  () => [filters.range, filters.month, filters.startDate, filters.endDate, filters.meter, filters.frequency, filters.shift, filters.query],
  scheduleFilterLoad,
);

watch(
  () => props.scope,
  async () => {
    bootstrapController?.abort();
    recordsController?.abort();
    detailController?.abort();
    page.value = 1;
    bootstrap.value = {};
    records.value = [];
    total.value = 0;
    closeLightbox();
    if (drawerOpen.value) {
      drawerDirty.value = false;
      closeDrawer();
    }
    await loadBootstrap();
    await loadRecords();
  },
);

onMounted(async () => {
  window.addEventListener("keydown", handleGlobalKeydown);
  if (!WATER_SCOPES.has(scopeCode.value)) {
    setMessage("水耗管理仅支持 A、B、C、D、E、H 楼。", "danger");
    return;
  }
  await loadBootstrap();
  await loadRecords();
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", handleGlobalKeydown);
  bootstrapController?.abort();
  recordsController?.abort();
  detailController?.abort();
  if (filterTimer !== null) window.clearTimeout(filterTimer);
  clearSnapshotPoll();
  clearStagedPhotos();
});
</script>

<style scoped>
.water-page {
  display: grid;
  gap: 14px;
  padding: 16px 22px 28px;
  color: #0f172a;
}

.water-page__header,
.water-workspace,
.water-stats article {
  border: 1px solid #d8e5f7;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
}

.water-page__header {
  display: flex;
  min-height: 68px;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 16px;
  border-radius: 18px;
}

.header-left,
.header-actions,
.icon-label,
.photo-editor__head,
.drawer-actions,
.pagination,
.pagination > div {
  display: flex;
  align-items: center;
}

.header-left {
  gap: 12px;
}

.header-left span,
.record-drawer header span {
  color: #1e63ff;
  font-size: 12px;
  font-weight: 850;
}

.header-left h1 {
  margin: 3px 0 0;
  font-size: 20px;
  letter-spacing: 0;
}

.header-actions {
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.header-actions :deep(.vnet-select) {
  width: 126px;
}

.refresh-time {
  color: #64748b;
  font-size: 12px;
  white-space: nowrap;
}

.btn,
.compact-button,
.pagination button,
.icon-button {
  border: 1px solid #cfe0f5;
  background: #fff;
  color: #1554b8;
  cursor: pointer;
  font: inherit;
  font-weight: 850;
}

.btn {
  min-height: 38px;
  padding: 0 14px;
  border-radius: 10px;
}

.btn.primary {
  border-color: #1e63ff;
  background: #1e63ff;
  color: #fff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.2);
}

.btn:disabled,
.compact-button:disabled,
.pagination button:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.icon-label {
  gap: 7px;
}

.spinning {
  animation: spin 1s linear infinite;
}

.water-message {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  min-height: 40px;
  padding: 8px 13px;
  border: 1px solid #bfdbfe;
  border-radius: 10px;
  background: #eff6ff;
  color: #1e40af;
  font-size: 13px;
  font-weight: 750;
}

.water-message.warning {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #9a3412;
}

.water-message.danger {
  border-color: #fecaca;
  background: #fef2f2;
  color: #991b1b;
}

.water-message.success {
  border-color: #a7f3d0;
  background: #ecfdf5;
  color: #047857;
}

.water-message button {
  border: 0;
  background: transparent;
  color: inherit;
  font-weight: 900;
  cursor: pointer;
}

.water-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.water-stats article {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 12px;
  padding: 13px 15px;
  border-radius: 14px;
}

.stat-icon {
  display: grid;
  width: 42px;
  height: 42px;
  flex: 0 0 auto;
  place-items: center;
  border-radius: 12px;
}

.stat-icon.blue { background: #e8f1ff; color: #1e63ff; }
.stat-icon.cyan { background: #e6faff; color: #0891b2; }
.stat-icon.green { background: #e8fbf3; color: #059669; }
.stat-icon.violet { background: #f1edff; color: #7c3aed; }

.water-stats small {
  display: block;
  color: #64748b;
  font-size: 12px;
}

.water-stats strong {
  display: block;
  margin-top: 3px;
  color: #0b2856;
  font-size: 22px;
}

.water-stats strong b {
  font-size: 12px;
}

.water-stats .date-value {
  font-size: 16px;
}

.water-workspace {
  min-width: 0;
  overflow: hidden;
  border-radius: 16px;
}

.filter-bar {
  display: grid;
  grid-template-columns: 132px 142px 156px 132px 112px minmax(210px, 1fr);
  gap: 8px;
  align-items: center;
  padding: 11px 12px;
  border-bottom: 1px solid #e3ebf7;
  background: #f8fbff;
}

.filter-bar :deep(.vnet-select-trigger),
.filter-bar input {
  min-height: 36px;
  border-radius: 9px;
}

.search-control,
.date-control {
  display: flex;
  align-items: center;
  gap: 7px;
  min-width: 0;
  padding: 0 10px;
  border: 1px solid #d8e5f7;
  border-radius: 9px;
  background: #fff;
  color: #6b7f99;
}

.search-control input,
.date-control input {
  width: 100%;
  border: 0;
  padding: 0;
  box-shadow: none !important;
}

.date-control span {
  flex: 0 0 auto;
  font-size: 12px;
  font-weight: 850;
}

.month-control input {
  width: 100%;
  min-height: 36px;
  padding: 0 9px;
  border: 1px solid #d8e5f7;
}

.table-shell {
  position: relative;
  min-height: 300px;
  overflow-x: auto;
}

table {
  width: 100%;
  min-width: 980px;
  border-collapse: collapse;
  table-layout: fixed;
}

th,
td {
  padding: 10px 11px;
  border-bottom: 1px solid #e7eef8;
  text-align: left;
  font-size: 13px;
  vertical-align: middle;
}

th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #f2f7ff;
  color: #47617e;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.record-row {
  cursor: pointer;
  transition: background-color 140ms ease, box-shadow 140ms ease;
}

.record-row:hover {
  background: #f8fbff;
}

.record-row:focus {
  outline: none;
}

.record-row:focus-visible {
  background: #f2f7ff;
  box-shadow: inset 4px 0 #1e63ff, inset 0 0 0 2px rgba(30, 99, 255, 0.22);
}

.number-cell {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.data-tag,
.calculating {
  display: inline-flex;
  padding: 4px 8px;
  border-radius: 999px;
  background: #edf5ff;
  color: #1d5eba;
  font-size: 12px;
  font-weight: 850;
}

.calculating {
  background: #fff7e8;
  color: #a16207;
}

.muted {
  color: #94a3b8;
}

.photo-status {
  display: inline-flex;
  min-width: 58px;
  justify-content: center;
  padding: 4px 9px;
  border: 1px solid #dbe5f1;
  border-radius: 999px;
  background: #f8fafc;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.photo-status.has-photo {
  border-color: #b7ead7;
  background: #ecfdf5;
  color: #087a55;
}

.table-state {
  height: 250px;
  text-align: center;
  color: #64748b;
}

.table-loading-overlay {
  position: absolute;
  top: 8px;
  right: 12px;
  padding: 5px 9px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: #eff6ff;
  color: #1e40af;
  font-size: 12px;
  font-weight: 850;
}

.pagination {
  justify-content: space-between;
  gap: 12px;
  padding: 10px 13px;
  color: #64748b;
  font-size: 12px;
}

.pagination > div {
  gap: 7px;
}

.pagination button {
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  gap: 4px;
  padding: 0 10px;
  border-radius: 8px;
}

.drawer-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-drawer-backdrop, 780);
  background: rgba(8, 31, 72, 0.34);
  backdrop-filter: blur(4px);
}

.record-drawer {
  position: absolute;
  top: 0;
  right: 0;
  width: min(620px, 88vw);
  height: 100%;
  overflow-y: auto;
  border-left: 1px solid #cfdef2;
  background: #f8fbff;
  box-shadow: -24px 0 60px rgba(7, 37, 86, 0.2);
}

.record-drawer > header {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 16px 18px;
  border-bottom: 1px solid #dce7f5;
  background: rgba(255, 255, 255, 0.96);
  backdrop-filter: blur(8px);
}

.record-drawer h2 {
  margin: 3px 0 0;
  font-size: 20px;
}

.icon-button {
  display: grid;
  width: 36px;
  height: 36px;
  place-items: center;
  border-radius: 999px;
}

.drawer-state {
  display: grid;
  min-height: 300px;
  place-items: center;
  color: #64748b;
}

.record-form {
  display: grid;
  gap: 14px;
  padding: 16px 18px 0;
}

.record-form.saving > :not(.drawer-actions) {
  pointer-events: none;
  opacity: 0.72;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 11px 12px;
}

.form-grid label {
  display: grid;
  min-width: 0;
  gap: 5px;
}

.form-grid label > span,
.photo-editor__head strong {
  color: #334155;
  font-size: 12px;
  font-weight: 900;
}

.form-grid b,
.photo-editor b {
  color: #dc2626;
}

.form-grid input {
  width: 100%;
  min-height: 36px;
  padding: 0 10px;
  border: 1px solid #d8e5f7;
  border-radius: 9px;
}

.wide-field {
  grid-column: 1 / -1;
}

.readonly-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  padding: 11px;
  border: 1px solid #dbe7f6;
  border-radius: 12px;
  background: #fff;
}

.readonly-grid article {
  min-width: 0;
  padding: 7px 8px;
  border-radius: 8px;
  background: #f6f9fd;
}

.readonly-grid small,
.readonly-grid strong {
  display: block;
}

.readonly-grid small {
  color: #64748b;
  font-size: 11px;
}

.readonly-grid strong {
  margin-top: 3px;
  overflow-wrap: anywhere;
  font-size: 12px;
}

.photo-editor {
  display: grid;
  gap: 9px;
}

.photo-editor__head {
  justify-content: space-between;
  gap: 12px;
}

.photo-editor__head strong,
.photo-editor__head span {
  display: block;
}

.photo-editor__head span {
  margin-top: 3px;
  color: #64748b;
  font-size: 11px;
}

.compact-button {
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  gap: 5px;
  padding: 0 10px;
  border-radius: 8px;
}

.photo-dropzone {
  display: grid;
  min-height: 108px;
  place-items: center;
  align-content: center;
  gap: 5px;
  border: 1.5px dashed #93b9ed;
  border-radius: 12px;
  background: #f0f7ff;
  color: #1e63b7;
  cursor: pointer;
  text-align: center;
}

.photo-dropzone.dragging {
  border-color: #1e63ff;
  background: #e5f0ff;
}

.photo-dropzone.disabled {
  cursor: wait;
  opacity: 0.68;
  pointer-events: none;
}

.photo-dropzone strong {
  font-size: 13px;
}

.photo-dropzone span {
  color: #6b7f99;
  font-size: 11px;
}

.photo-editor__grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.photo-editor__grid article {
  position: relative;
  min-width: 0;
  overflow: hidden;
  border: 1px solid #dce7f5;
  border-radius: 10px;
  background: #fff;
}

.photo-editor__grid article.uploading {
  opacity: 0.58;
}

.photo-editor__grid img {
  width: 100%;
  aspect-ratio: 4 / 3;
  object-fit: cover;
}

.photo-editor__grid .clickable-photo {
  cursor: zoom-in;
}

.photo-editor__grid .clickable-photo:focus-visible {
  outline: 3px solid rgba(30, 99, 255, 0.48);
  outline-offset: -3px;
}

.photo-editor__grid article > span {
  display: block;
  overflow: hidden;
  padding: 5px 7px;
  color: #52667f;
  font-size: 10px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.photo-editor__grid article > button {
  position: absolute;
  top: 5px;
  right: 5px;
  display: grid;
  width: 25px;
  height: 25px;
  place-items: center;
  border: 0;
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.72);
  color: #fff;
  cursor: pointer;
}

.form-error {
  margin: 0;
  padding: 9px 11px;
  border: 1px solid #fecaca;
  border-radius: 9px;
  background: #fef2f2;
  color: #991b1b;
  font-size: 12px;
  font-weight: 750;
}

.drawer-actions {
  position: sticky;
  bottom: 0;
  z-index: 4;
  justify-content: space-between;
  gap: 12px;
  margin: 0 -18px;
  padding: 12px 18px;
  border-top: 1px solid #dce7f5;
  background: rgba(255, 255, 255, 0.97);
  backdrop-filter: blur(8px);
}

.drawer-actions > span {
  color: #64748b;
  font-size: 11px;
}

.drawer-actions > div {
  display: flex;
  gap: 8px;
}

.lightbox {
  position: fixed;
  inset: 0;
  z-index: 940;
  display: grid;
  place-items: center;
  padding: 44px 72px;
  background: rgba(5, 15, 35, 0.9);
}

.lightbox figure {
  display: grid;
  max-width: min(1200px, 86vw);
  max-height: 88vh;
  gap: 10px;
  margin: 0;
}

.lightbox figure img {
  max-width: 100%;
  max-height: calc(88vh - 52px);
  border-radius: 8px;
  object-fit: contain;
}

.lightbox figcaption {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  color: #e2e8f0;
  font-size: 13px;
}

.lightbox figcaption a {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: #bfdbfe;
  font-weight: 850;
  text-decoration: none;
}

.lightbox-close,
.lightbox-nav {
  position: fixed;
  z-index: 2;
  display: grid;
  place-items: center;
  border: 1px solid rgba(255, 255, 255, 0.22);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.1);
  color: #fff;
  cursor: pointer;
}

.lightbox-close {
  top: 20px;
  right: 22px;
  width: 42px;
  height: 42px;
}

.lightbox-nav {
  top: 50%;
  width: 48px;
  height: 48px;
  transform: translateY(-50%);
}

.lightbox-nav.prev { left: 18px; }
.lightbox-nav.next { right: 18px; }

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

@media (max-width: 1280px) {
  .filter-bar {
    grid-template-columns: repeat(3, minmax(150px, 1fr));
  }

  .search-control {
    grid-column: span 2;
  }
}

@media (max-width: 900px) {
  .water-page {
    padding: 12px;
  }

  .water-page__header,
  .header-actions {
    align-items: stretch;
  }

  .water-page__header {
    flex-direction: column;
  }

  .water-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .filter-bar,
  .form-grid,
  .readonly-grid {
    grid-template-columns: 1fr;
  }

  .search-control,
  .wide-field {
    grid-column: auto;
  }

  .record-drawer {
    width: min(680px, 96vw);
  }
}
</style>
