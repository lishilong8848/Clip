<template>
  <section ref="pageElement" class="kb-page" @dragover="onDragOver" @dragleave="onDragLeave" @drop="onDrop">
    <VnetBackButton to="/" :disabled="uploadBusy" />
    <header class="kb-head">
      <div class="kb-title">
        <Database :size="22" aria-hidden="true" />
        <h1>共享知识库</h1>
        <span v-if="isAdmin" class="kb-badge" :class="engineBadgeClass">{{ engineBadge }}</span>
      </div>
      <div class="kb-actions">
        <button type="button" class="primary" :disabled="uploadBusy" @click="openUpload()">
          <Upload :size="16" />上传文件
        </button>
      </div>
    </header>

    <div class="kb-toolbar">
      <label class="kb-search">
        <Search :size="16" aria-hidden="true" />
        <input v-model="search" type="search" placeholder="搜索文件名" aria-label="搜索知识库" spellcheck="false" />
        <Loader2 v-if="loadingList" :size="15" class="spin" aria-hidden="true" />
      </label>
      <div class="kb-toggle" role="group" aria-label="列表范围">
        <button :class="{ active: !recycle }" :aria-pressed="!recycle" @click="switchBin(false)"><FileText :size="15" />可用文档</button>
        <button :class="{ active: recycle }" :aria-pressed="recycle" @click="switchBin(true)"><Trash2 :size="15" />回收站</button>
      </div>
      <button type="button" class="icon-button" title="刷新" aria-label="刷新列表" :disabled="loadingList" @click="refresh">
        <RefreshCw :size="17" :class="{ spin: loadingList }" />
      </button>
    </div>

    <div v-if="error" class="alert error" role="alert">
      <AlertCircle :size="18" aria-hidden="true" /><span>{{ error }}</span>
      <button type="button" class="icon-button" aria-label="关闭错误提示" @click="error = ''"><X :size="16" /></button>
    </div>
    <div v-if="notice" class="alert success" role="status">
      <CheckCircle2 :size="18" aria-hidden="true" /><span>{{ notice }}</span>
    </div>

    <div v-if="uploadPanel || dragging" class="kb-upload" :class="{ dragging }">
      <div class="kb-upload-head">
        <strong>{{ uploadTarget ? '替换文档' : '新增共享文档' }}</strong>
        <button type="button" class="icon-button" aria-label="关闭上传" :disabled="uploadBusy || scanningFolder" @click="uploadPanel = false"><X :size="16" /></button>
      </div>
      <div class="kb-toggle" role="group" aria-label="上传方式">
        <button type="button" :class="{ active: uploadMode === 'files' }" :aria-pressed="uploadMode === 'files'" :disabled="uploadBusy" @click="uploadMode = 'files'"><Upload :size="15" />文件</button>
        <button type="button" :class="{ active: uploadMode === 'text' }" :aria-pressed="uploadMode === 'text'" :disabled="uploadBusy" @click="uploadMode = 'text'"><FileText :size="15" />粘贴文本</button>
      </div>
      <button v-if="uploadMode === 'files'" ref="dropzone" type="button" class="kb-dropzone" :disabled="uploadBusy || scanningFolder" title="拖入文件或按 Ctrl+V 粘贴文件、文本" @click="fileInput?.click()">
        <Upload :size="24" aria-hidden="true" />
        <strong>{{ scanningFolder ? '正在读取文件夹…' : dragging ? '松开添加文件' : '选择文件或拖入文件夹' }}</strong>
        <span>PDF · Word（DOCX）· Excel（XLSX / XLSM）· MD · TXT · CSV · 图片</span>
      </button>
      <input ref="fileInput" type="file" hidden aria-label="选择知识库文件" :accept="FILE_ACCEPT" :multiple="!uploadTarget" :disabled="uploadBusy" @change="onUploadPick" />
      <button v-if="uploadMode === 'files' && !uploadTarget" type="button" class="folder-picker" :disabled="uploadBusy || scanningFolder" @click="folderInput?.click()"><FolderOpen :size="16" />选择文件夹</button>
      <input ref="folderInput" type="file" hidden webkitdirectory multiple aria-label="选择知识库文件夹" :disabled="uploadBusy || scanningFolder" @change="onFolderPick" />
      <div v-if="uploadMode === 'text'" class="kb-text-entry">
        <label>文档名称<input v-model="textName" aria-label="文档名称" placeholder="未填写时自动命名" maxlength="120" :disabled="uploadBusy" /></label>
        <label>文档内容<textarea v-model="textContent" aria-label="文档内容" rows="6" :disabled="uploadBusy" /></label>
        <button type="button" class="primary" :disabled="!textContent.trim() || uploadBusy" @click="addText"><FileText :size="15" />加入待上传</button>
      </div>
      <ul v-if="pendingFiles.length" class="kb-upload-list" aria-label="待上传文件">
        <li v-for="(file, index) in pendingFiles.slice(0, 100)" :key="index">
          <FileText :size="14" aria-hidden="true" /><span>{{ file.name }}</span><em>{{ sizeText(file.size) }}</em>
          <button type="button" class="icon-button" :aria-label="`移除${file.name}`" title="移除文件" :disabled="uploadBusy" @click="pendingFiles.splice(index, 1)"><X :size="15" /></button>
        </li>
      </ul>
      <p v-if="pendingFiles.length > 100" class="muted">共 {{ pendingFiles.length }} 个文件，列表展示前100个</p>
      <p v-if="uploadError" class="alert error" role="alert"><AlertCircle :size="15" aria-hidden="true" /><span>{{ uploadError }}</span></p>
      <p v-if="uploadTarget" class="kb-upload-replace">上传后将为「{{ uploadTarget.name }}」建立新索引版本，原共享版本继续保留。</p>
      <div class="kb-upload-actions">
        <span class="muted">{{ pendingFiles.length }} 个文件 · 单个 ≤100MiB</span>
        <button type="button" class="primary" :disabled="(!pendingFiles.length && !textContent.trim()) || uploadBusy || scanningFolder" @click="requestUpload">
          <Loader2 v-if="uploadBusy" :size="15" class="spin" aria-hidden="true" />{{ uploadBusy ? '上传中…' : '确认上传' }}
        </button>
      </div>
      <div v-if="uploadBusy" class="kb-upload-progress" role="progressbar" :aria-valuenow="uploadProgress" aria-valuemin="0" aria-valuemax="100">
        <span :style="{ width: uploadProgress + '%' }"></span><em>{{ Math.round(uploadProgress) }}%</em>
      </div>
    </div>

    <div class="kb-layout" :class="{ 'has-detail': detail }">
      <div class="kb-pane">
        <div v-if="loadingList && !quietLoading" class="empty"><Loader2 :size="24" class="spin" aria-hidden="true" />正在读取文档…</div>
        <div v-else-if="!items.length" class="empty">
          {{ recycle ? '回收站是空的' : search.trim() ? '没有匹配的文档' : '尚未上传文档' }}
        </div>
        <ul v-else class="kb-list" aria-label="文档列表">
          <li v-for="item in items" :key="item.id" class="kb-row" :class="{ selected: detail?.id === item.id }">
            <button type="button" class="kb-row-main" @click="openDetail(item)">
              <span class="kb-file-icon"><FileText :size="17" aria-hidden="true" /></span>
              <span class="kb-row-text">
                <strong>{{ item.name }}</strong>
                <small>{{ item.owner_name || '—' }} · {{ sizeText(item.size) }} · {{ item.chunks || 0 }} 片段 · {{ timeText(item.updated_at) }}</small>
              </span>
              <em class="kb-status" :class="statusClass(item.status)">{{ statusText(item.status) }}</em>
            </button>
            <div v-if="!recycle" class="kb-row-actions">
              <button v-if="item.status === 'ready'" type="button" class="icon-button" title="下载原文件" aria-label="下载原文件" :disabled="actionBusy" @click="downloadActive(item)"><Download :size="16" /></button>
              <button v-if="item.can_edit" type="button" class="icon-button" title="替换文件" aria-label="替换文件" :disabled="actionBusy || uploadBusy || pendingFiles.length > 0 || !!textContent" @click="pickReplace(item)"><RefreshCw :size="16" /></button>
              <button v-if="item.status === 'failed' && item.can_edit" type="button" class="icon-button" title="重新索引" aria-label="重新索引" :disabled="actionBusy" @click="retryTarget = item"><RotateCcw :size="16" /></button>
              <button v-if="item.can_edit" type="button" class="icon-button danger" title="移入回收站" aria-label="移入回收站" :disabled="actionBusy" @click="deleteTarget = item"><Trash2 :size="16" /></button>
            </div>
            <div v-else class="kb-row-actions">
              <button v-if="item.can_edit" type="button" class="icon-button" title="恢复" aria-label="恢复" :disabled="actionBusy" @click="restoreTarget = item"><RotateCcw :size="16" /></button>
            </div>
            <p v-if="item.error && !recycle" class="kb-row-error" role="alert">{{ item.error }}</p>
          </li>
        </ul>
        <footer v-if="pages > 1" class="kb-pager">
          <span>共 {{ total }} 项</span>
          <button class="icon-button" aria-label="上一页" :disabled="page <= 1 || loadingList" @click="changePage(page - 1)"><ChevronLeft :size="16" /></button>
          <span>{{ page }} / {{ pages }}</span>
          <button class="icon-button" aria-label="下一页" :disabled="page >= pages || loadingList" @click="changePage(page + 1)"><ChevronRight :size="16" /></button>
        </footer>
      </div>

      <aside v-if="detail" class="kb-detail">
        <header class="kb-detail-head">
          <div>
            <strong>{{ detail.name }}</strong>
            <small>{{ detail.owner_name || '—' }} · 预览版本 {{ detail.view_version || detail.active_version || detail.version }}{{ detail.historical ? '（历史版本）' : '' }}</small>
          </div>
          <button type="button" class="icon-button" aria-label="关闭预览" @click="closeDetail"><X :size="17" /></button>
        </header>
        <div v-if="detailLoading" class="empty"><Loader2 :size="20" class="spin" aria-hidden="true" />正在读取原文…</div>
        <template v-else>
          <div class="kb-detail-actions">
            <button type="button" class="primary" @click="downloadActive(detail)"><Download :size="15" />下载原文件</button>
            <span class="muted">共享版本 {{ detail.active_version || '—' }} · 状态 {{ statusText(detail.status) }}</span>
          </div>
          <p v-if="detail.status !== 'ready' && sections.length" class="kb-stale" role="status"><AlertCircle :size="14" aria-hidden="true" /><span>正在预览当前版本{{ detail.view_version ? `（${detail.view_version}）` : '' }}，新版本{{ detail.status === 'failed' ? '索引失败，可重试' : '仍在索引中' }}。</span></p>
          <div class="kb-sections">
            <article v-for="section in sections" :key="section.ordinal" class="kb-section" :class="{ current: focusedOrdinal === section.ordinal }">
              <div class="kb-section-meta"><span>片段 {{ section.ordinal }}</span><em>{{ section.location || '—' }}</em></div>
              <p>{{ section.text }}</p>
            </article>
            <div v-if="!sections.length" class="empty">{{ previewEmptyText }}</div>
          </div>
          <footer v-if="detailPages > 1" class="kb-pager">
            <button class="icon-button" aria-label="上一页片段" :disabled="detailPage <= 1 || detailLoading" @click="detailPage--; reloadDetail()"><ChevronLeft :size="16" /></button>
            <span>{{ detailPage }} / {{ detailPages }}</span>
            <button class="icon-button" aria-label="下一页片段" :disabled="detailPage >= detailPages || detailLoading" @click="detailPage++; reloadDetail()"><ChevronRight :size="16" /></button>
          </footer>
        </template>
      </aside>
    </div>

    <ConfirmDialog :open="!!uploadConfirm" title="确认上传" :message="uploadConfirmMessage" tone="warning" @resolve="resolveUpload" />
    <ConfirmDialog :open="!!deleteTarget" title="移入回收站" :message="deleteTarget ? '将「' + deleteTarget.name + '」移入回收站？历史共享版本保留，之后可恢复。' : ''" tone="danger" @resolve="resolveDelete" />
    <ConfirmDialog :open="!!restoreTarget" title="恢复文档" :message="restoreTarget ? '恢复「' + restoreTarget.name + '」？恢复后需要重新索引才会再次共享。' : ''" tone="primary" @resolve="resolveRestore" />
    <ConfirmDialog :open="!!retryTarget" title="重新索引" :message="retryTarget ? '对「' + retryTarget.name + '」重新执行索引？' : ''" tone="primary" @resolve="resolveRetry" />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  AlertCircle, CheckCircle2, ChevronLeft, ChevronRight, Database, Download, FileText,
  Loader2, RefreshCw, RotateCcw, Search, Trash2, Upload, X, FolderOpen,
} from "lucide-vue-next";
import { requestJson, downloadFile, uploadJson } from "../api/client";
import ConfirmDialog from "./ConfirmDialog.vue";
import VnetBackButton from "./VnetBackButton.vue";
import { isAssistantEvent } from "../modalState";
import { KNOWLEDGE_FILE_ACCEPT, supportedKnowledgeFile, folderFile, droppedKnowledgeFiles, knowledgeUploadBatches } from '../knowledgeFiles';

const BASE = "/api/assistant/knowledge";

interface DocumentItem {
  id: string;
  name: string;
  owner_name: string;
  can_edit: boolean;
  version: string | number;
  active_version?: string | number | null;
  view_version?: string | number | null;
  historical?: boolean;
  status: string;
  updated_at: unknown;
  size: number;
  chunks: number;
  error?: string;
}

interface Section {
  text: string;
  location: string;
  ordinal: number;
}

const search = ref("");
const page = ref(1);
const total = ref(0);
const recycle = ref(false);
const items = ref<DocumentItem[]>([]);
const isAdmin = ref(false);
const settings = ref<Record<string, any>>({});
const loadingList = ref(false);
const quietLoading = ref(false);
const error = ref("");
const notice = ref("");

const detail = ref<DocumentItem | null>(null);
const sections = ref<Section[]>([]);
const detailLoading = ref(false);
const detailPage = ref(1);
const detailTotal = ref(0);
const detailPageSize = ref(20);
const focusedOrdinal = ref<number | null>(null);
let detailRequest = 0;
const detailViewVersion = ref<string | number>("");

const previewEmptyText = computed(() => {
  const current = detail.value;
  if (!current) return "";
  if (current.status !== "ready") {
    return current.error || "文档尚未完成索引，暂无片段可预览。";
  }
  return current.historical ? "该版本尚无片段可预览。" : "没有可预览的片段";
});

const uploadPanel = ref(false);
const uploadTarget = ref<DocumentItem | null>(null);
const pendingFiles = ref<File[]>([]);
const uploadBusy = ref(false);
const uploadProgress = ref(0);
const uploadError = ref("");
const uploadConfirm = ref(false);
const pageElement = ref<HTMLElement | null>(null);
const fileInput = ref<HTMLInputElement | null>(null);
const folderInput = ref<HTMLInputElement | null>(null);
const scanningFolder = ref(false);
let folderDisposed = false;
const dropzone = ref<HTMLButtonElement | null>(null);
const uploadMode = ref<'files' | 'text'>('files');
const textName = ref("");
const textContent = ref("");
const dragging = ref(false);
const FILE_ACCEPT = KNOWLEDGE_FILE_ACCEPT;

const deleteTarget = ref<DocumentItem | null>(null);
const restoreTarget = ref<DocumentItem | null>(null);
const retryTarget = ref<DocumentItem | null>(null);
const actionBusy = ref(false);

const pages = computed(() => Math.max(1, Math.ceil(total.value / 20)));
const detailPages = computed(() => Math.max(1, Math.ceil(detailTotal.value / (detailPageSize.value || 20))));
const uploadConfirmMessage = computed(() => {
  if (uploadTarget.value) return "上传后将替换当前文档并建立新索引版本，原共享版本保留。确认继续？";
  const count = pendingFiles.value.length;
  const names = pendingFiles.value.slice(0, 3).map(f => f.name).join("、");
  return `将上传 ${count} 个文件（${names}${count > 3 ? "…" : ""}）至共享知识库。上传并完成索引后所有员工均可检索，请确认这些文件不包含私密聊天内容。`;
});

let searchTimer = 0;
let listController: AbortController | undefined;
let pollTimer: number | undefined;

function engineStatus(): string {
  if (settings.value?.runtime?.loading || settings.value?.queued > 0) return 'preparing';
  if (settings.value?.warning && !settings.value?.indexready) return 'pending';
  return String(settings.value?.engine_status || settings.value?.status || "");
}

const engineBadge = computed(() => {
  const mode = String(settings.value?.mode || "");
  const status = engineStatus();
  const preparing = status === "preparing";
  if (mode === "local_faiss") {
    return preparing ? "本地向量引擎准备中…" : "BGE small zh + FAISS 本地向量";
  }
  if (mode === "keyword") {
    return preparing ? "本地全文检索（关键词）准备中…" : "本地全文检索（关键词回退）";
  }
  if (mode === "hybrid") {
    return preparing ? "向量引擎准备中…" : "全文＋向量检索";
  }
  return preparing ? "检索引擎准备中…" : "本地全文检索";
});

const engineBadgeClass = computed(() => {
  const status = engineStatus();
  return { preparing: status === "preparing", ok: status !== "preparing" };
});

function statusText(status: string): string {
  const map: Record<string, string> = {
    queued: "排队中", indexing: "索引中", ready: "就绪", failed: "失败", blocked: "已屏蔽", deleted: "已删除",
  };
  return map[status] || status || "—";
}
function statusClass(status: string): string {
  const key = String(status || "").toLowerCase();
  if (["queued", "indexing"].includes(key)) return key;
  if (key === "ready") return "ready";
  if (key === "failed") return "failed";
  if (key === "blocked") return "blocked";
  return "deleted";
}
function sizeText(value: unknown): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / 1024 / 1024).toFixed(1) + " MB";
}
function timeText(value: unknown): string {
  if (!value) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  const ms = n < 1e12 ? n * 1000 : n;
  return new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(ms));
}

function clearNotice(): void { notice.value = ""; }

function updatePolling(): void {
  const hasPending = items.value.some(item => ["queued", "indexing"].includes(item.status));
  if (hasPending && pollTimer === undefined) {
    pollTimer = window.setInterval(() => {
      if (document.hidden) return;
      void loadList(false, false);
    }, 8000);
  } else if (!hasPending && pollTimer !== undefined) {
    window.clearInterval(pollTimer);
    pollTimer = undefined;
  }
}

async function loadList(spinner = true, clearError = true): Promise<void> {
  listController?.abort();
  const controller = new AbortController();
  listController = controller;
  if (spinner) { loadingList.value = true; quietLoading.value = false; }
  else { quietLoading.value = true; }
  if (clearError) error.value = "";
  try {
    const params = new URLSearchParams({
      page: String(page.value),
      deleted: recycle.value ? "1" : "0",
    });
    if (search.value.trim()) params.set("q", search.value.trim());
    const data = await requestJson(`${BASE}?${params}`, { signal: controller.signal, cache: "no-store" });
    if (listController !== controller || controller.signal.aborted) return;
    items.value = data.items || [];
    total.value = Number(data.total) || 0;
    page.value = Number(data.page) || page.value;
    isAdmin.value = Boolean(data.is_admin);
    settings.value = (data.settings && typeof data.settings === "object") ? data.settings : {};
    updatePolling();
  } catch (cause) {
    if (listController === controller && !controller.signal.aborted) {
      if (clearError) error.value = cause instanceof Error ? cause.message : "文档读取失败";
    }
  } finally {
    if (listController === controller) { loadingList.value = false; quietLoading.value = false; }
  }
}

function onSearchChange(): void {
  window.clearTimeout(searchTimer);
  listController?.abort();
  searchTimer = window.setTimeout(() => {
    if (page.value !== 1) page.value = 1;
    void loadList();
  }, 450);
}
watch(search, onSearchChange);

function switchBin(next: boolean): void {
  if (recycle.value === next) return;
  recycle.value = next;
  page.value = 1;
  void loadList();
}
function refresh(): void { void loadList(true, true); }
function changePage(next: number): void {
  page.value = next;
  void loadList(true, true);
}

function openDetail(item: DocumentItem): void {
  if (detailLoading.value) return;
  focusedOrdinal.value = null;
  // Active preview always shows the current shared (active) version, even while a replacement indexes.
  detailViewVersion.value = item.active_version != null && item.active_version !== "" ? item.active_version : "";
  void loadDetail(item, undefined, 1, "", detailViewVersion.value);
}

async function loadDetail(item: DocumentItem, chunk: number | undefined, targetPage = 1, preserveError = "", viewVersion: string | number = ""): Promise<void> {
  const id = String(item.id);
  detailRequest += 1;
  const requestSeq = detailRequest;
  detailLoading.value = true;
  if (preserveError) error.value = preserveError;
  try {
    const params = new URLSearchParams({ page: String(targetPage) });
    // Revision (item.version) is only valid for mutations. For preview, use the version being viewed.
    if (viewVersion != null && viewVersion !== "") params.set("version", String(viewVersion));
    // chunk is only sent on an explicit citation jump; plain pagination must not send chunk.
    if (chunk != null) params.set("chunk", String(chunk));
    const data = await requestJson(`${BASE}/documents/${encodeURIComponent(id)}?${params}`, { cache: "no-store" });
    if (requestSeq !== detailRequest) return;
    detail.value = (data.document || { ...item }) as DocumentItem;
    sections.value = data.sections || [];
    detailTotal.value = Number(data.total) || sections.value.length;
    detailPage.value = Number(data.page) || targetPage;
    detailPageSize.value = Number(data.page_size) || 20;
    focusedOrdinal.value = chunk ?? focusedOrdinal.value;
    if (data.document && (data.document.view_version != null)) {
      detailViewVersion.value = data.document.view_version;
    }
  } catch (cause) {
    if (requestSeq === detailRequest) error.value = cause instanceof Error ? cause.message : "原文读取失败";
  } finally {
    if (requestSeq === detailRequest) detailLoading.value = false;
  }
}

async function reloadDetail(): Promise<void> {
  if (!detail.value) return;
  // Pagination keeps the same viewed version; chunk is intentionally omitted (no citation jump).
  await loadDetail(detail.value, undefined, detailPage.value, error.value, detailViewVersion.value);
}

function closeDetail(): void {
  detailRequest += 1;
  detail.value = null;
  sections.value = [];
  detailViewVersion.value = "";
  focusedOrdinal.value = null;
  detailPage.value = 1;
}

async function downloadActive(item: DocumentItem): Promise<void> {
  if (actionBusy.value) return;
  // Download matches the version being previewed (active for list actions, view_version for citation preview).
  const version = item.view_version || item.active_version || item.version;
  actionBusy.value = true;
  clearNotice();
  try {
    await downloadFile(`${BASE}/documents/${encodeURIComponent(item.id)}/file?version=${encodeURIComponent(String(version))}`);
    notice.value = "已开始下载原文件。";
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "文件下载失败";
  } finally {
    actionBusy.value = false;
  }
}

function openUpload(): void {
  if (!pendingFiles.value.length && !textContent.value) {
    uploadTarget.value = null;
    uploadError.value = "";
  }
  uploadPanel.value = true;
  void nextTick(() => dropzone.value?.focus());
}

const MAX_SINGLE = 100 * 1024 * 1024;

function onUploadPick(event: Event): void {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = "";
  addFiles(files);
}

function onFolderPick(event: Event): void {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = '';
  const supported = files.filter(supportedKnowledgeFile).map(file => folderFile(file));
  if (!supported.length) { uploadError.value = '文件夹中没有支持的文件'; return; }
  if (addFiles(supported) && supported.length < files.length) notice.value = `已跳过 ${files.length - supported.length} 个不支持的文件。`;
}

function addFiles(files: File[]): boolean {
  if (!files.length || uploadBusy.value) return false;
  uploadPanel.value = true;
  const unsupported = files.find(file => !supportedKnowledgeFile(file));
  if (unsupported) {
    uploadError.value = `不支持「${unsupported.name}」的文件格式；旧版 Word / Excel 请另存为 DOCX / XLSX。`;
    return false;
  }
  const all = [...pendingFiles.value, ...files];
  if (uploadTarget.value && all.length > 1) {
    uploadError.value = "替换文档只能上传1个文件，请先移除已选文件";
    return false;
  }
  if (all.some(file => !file.size || file.size > MAX_SINGLE)) { uploadError.value = "单个文件须非空且不超过100MiB"; return false; }
  if (new Set(all.map(file => file.name)).size !== all.length) {
    uploadError.value = "待上传列表中已有同名文件，请先移除或重命名后添加";
    return false;
  }
  pendingFiles.value = all;
  uploadError.value = "";
  return true;
}

function addText(): boolean {
  if (!textContent.value.trim()) return false;
  let name = textName.value.trim().replace(/[\\/:*?"<>|\x00-\x1f]/g, "_");
  if (!name) name = `粘贴文本-${Date.now()}`;
  if (!name.toLowerCase().endsWith('.txt')) name += '.txt';
  if (addFiles([new File([textContent.value], name, { type: "text/plain;charset=utf-8" })])) {
    textName.value = "";
    textContent.value = "";
    return true;
  }
  return false;
}

function inputBlocked(): boolean {
  return uploadBusy.value || scanningFolder.value || uploadConfirm.value || !!deleteTarget.value || !!restoreTarget.value || !!retryTarget.value;
}

function onPaste(event: ClipboardEvent): void {
  if (event.defaultPrevented || isAssistantEvent(event) || inputBlocked()) return;
  const target = event.target;
  if (!(target instanceof Element) || (target !== document.body && !pageElement.value?.contains(target))) return;
  if (target.closest('input, textarea, [contenteditable]:not([contenteditable="false"])')) return;
  const data = event.clipboardData;
  if (!data) return;
  const files = Array.from(data.files);
  if (!files.length) {
    for (const item of Array.from(data.items)) {
      if (item.kind === 'file') {
        const file = item.getAsFile();
        if (file) files.push(file);
      }
    }
  }
  if (files.length) {
    event.preventDefault();
    addFiles(files);
  } else if (data.getData('text/plain').trim()) {
    event.preventDefault();
    uploadPanel.value = true;
    uploadMode.value = 'text';
    const content = data.getData('text/plain');
    textContent.value += (textContent.value ? '\n' : '') + content;
  }
}

function onDragOver(event: DragEvent): void {
  if (!event.dataTransfer?.types.includes('Files') || isAssistantEvent(event)) return;
  event.preventDefault();
  event.dataTransfer.dropEffect = inputBlocked() ? 'none' : 'copy';
  if (!inputBlocked()) dragging.value = true;
}

function onDragLeave(event: DragEvent): void {
  if (!(event.relatedTarget instanceof Node) || !pageElement.value?.contains(event.relatedTarget)) dragging.value = false;
}

async function onDrop(event: DragEvent): Promise<void> {
  dragging.value = false;
  if (!event.dataTransfer?.types.includes('Files') || isAssistantEvent(event)) return;
  event.preventDefault();
  if (inputBlocked()) return;
  const entries = Array.from(event.dataTransfer.items).map(item => item.webkitGetAsEntry?.()).filter((item): item is FileSystemEntry => !!item);
  if (entries.some(item => item.isDirectory)) {
    uploadPanel.value = true;
    if (uploadTarget.value) { uploadError.value = '替换文档请选择单个文件'; return; }
    scanningFolder.value = true;
    try {
      const files = await droppedKnowledgeFiles(entries, () => folderDisposed);
      if (!folderDisposed && !addFiles(files)) uploadError.value ||= '文件夹中没有支持的文件';
    } catch { if (!folderDisposed) uploadError.value = '文件夹读取失败，请检查文件访问权限后重试。'; }
    finally { scanningFolder.value = false; }
    return;
  }
  addFiles(Array.from(event.dataTransfer.files));
}

function pickReplace(item: DocumentItem): void {
  uploadTarget.value = item;
  pendingFiles.value = [];
  uploadError.value = "";
  uploadMode.value = 'files';
  uploadPanel.value = true;
  void nextTick(() => {
    dropzone.value?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    dropzone.value?.focus({ preventScroll: true });
  });
}

function requestUpload(): void {
  if (textContent.value.trim() && !addText()) return;
  if (!pendingFiles.value.length || uploadBusy.value) return;
  uploadConfirm.value = true;
}

async function doUpload(): Promise<void> {
  if (!pendingFiles.value.length || uploadBusy.value) return;
  const target = uploadTarget.value;
  const submitted = [...pendingFiles.value];
  uploadBusy.value = true;
  uploadProgress.value = 0;
  clearNotice();
  const confirmed = new Set<File>();
  const failures: string[] = [];
  const totalBytes = submitted.reduce((sum, file) => sum + file.size, 0);
  let completedBytes = 0;
  try {
    let url = `${BASE}/files`;
    if (target) {
      const params = new URLSearchParams({ document_id: target.id, version: String(target.version) });
      url += `?${params}`;
    }
    for (const batch of knowledgeUploadBatches(submitted)) {
      if (folderDisposed) break;
      const form = new FormData();
      for (const file of batch) form.append('files', file);
      const batchBytes = batch.reduce((sum, file) => sum + file.size, 0);
      const data = await uploadJson(url, form, {
        timeoutMs: 600_000,
        onProgress: ({ loaded, total }) => {
          uploadProgress.value = 100 * (completedBytes + (total > 0 ? Math.min(1, loaded / total) * batchBytes : 0)) / totalBytes;
        },
      });
      if (!Array.isArray(data?.items) || !Array.isArray(data?.errors) || data.items.length + data.errors.length !== batch.length) {
        throw new Error('上传结果不完整，未确认的文件已保留，请刷新列表核对后重试。');
      }
      const errors = data.errors;
      const failedNames = new Set<string>(errors.map(err => String(err?.name || err?.filename || err?.file || '')));
      // An unidentifiable failure cannot authorize discarding any file from this batch.
      if ([...failedNames].some(name => !batch.some(file => file.name === name)) || failedNames.size !== errors.length) {
        throw new Error('部分文件的上传结果无法对应，未确认的文件已保留。');
      }
      for (const file of batch) if (!failedNames.has(file.name)) confirmed.add(file);
      pendingFiles.value = submitted.filter(file => !confirmed.has(file));
      completedBytes += batchBytes;
      failures.push(...errors.map(err => err?.error || err?.message || '上传失败'));
    }
    uploadError.value = failures.join('；');
    const succeeded = confirmed.size;
    if (succeeded > 0) {
      if (pendingFiles.value.length === 0) {
        uploadPanel.value = false;
        uploadTarget.value = null;
      }
      notice.value = target
        ? "文件已上传，将建立新索引版本。"
        : `已上传 ${succeeded} 个文件，完成索引后将进入共享知识库。`;
      await loadList(true, true);
    } else {
      // All files failed: keep the upload panel open with the file list preserved for retry.
      uploadPanel.value = true;
    }
  } catch (cause) {
    uploadError.value = [...failures, cause instanceof Error ? cause.message : '上传失败，请重试。'].join('；');
    if (confirmed.size) { notice.value = `已上传 ${confirmed.size} 个文件，其余文件已保留。`; await loadList(false, false); }
  } finally {
    uploadBusy.value = false;
    uploadConfirm.value = false;
    uploadProgress.value = 0;
  }
}

async function resolveUpload(accepted: boolean): Promise<void> {
  uploadConfirm.value = false;
  if (!accepted) return;
  await doUpload();
}

async function resolveDelete(accepted: boolean): Promise<void> {
  const item = deleteTarget.value;
  deleteTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}`, { method: "DELETE", body: JSON.stringify({ version: item.version }) });
    if (detail.value?.id === item.id) closeDetail();
    notice.value = "已移入回收站。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "删除失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

async function resolveRestore(accepted: boolean): Promise<void> {
  const item = restoreTarget.value;
  restoreTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}/restore`, { method: "POST", body: JSON.stringify({ version: item.version }) });
    notice.value = "已恢复文档。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "恢复失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

async function resolveRetry(accepted: boolean): Promise<void> {
  const item = retryTarget.value;
  retryTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}/retry`, { method: "POST", body: JSON.stringify({ version: item.version }) });
    notice.value = "已重新提交索引。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "操作失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

onMounted(() => {
  document.addEventListener('paste', onPaste);
  void loadList(true, true);
  const params = new URLSearchParams(window.location.search);
  const docId = params.get("document");
  const version = params.get("version");
  const chunk = params.get("chunk");
  if (docId) {
    const item: DocumentItem = {
      id: docId,
      name: params.get("name") || "文档",
      owner_name: "",
      can_edit: false,
      version: version || "",
      status: "ready",
      updated_at: null,
      size: 0,
      chunks: 0,
    };
    focusedOrdinal.value = chunk !== null && Number.isInteger(Number(chunk)) ? Math.max(0, Number(chunk)) : null;
    const citationChunk = focusedOrdinal.value ?? undefined;
    detailViewVersion.value = version || "";
    void loadDetail(item, citationChunk, 1, "", version || "");
  }
});

onBeforeUnmount(() => {
  folderDisposed = true;
  document.removeEventListener('paste', onPaste);
  window.clearTimeout(searchTimer);
  window.clearInterval(pollTimer);
  pollTimer = undefined;
  listController?.abort();
  detailRequest += 1;
});
</script>

<style scoped>
.kb-page {
  display: grid;
  gap: 12px;
  width: min(1200px, 100%);
  margin: 0 auto;
  padding: 20px 22px 40px;
  box-sizing: border-box;
  color: var(--cf-text, #0f172a);
}

.kb-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.kb-title { display: flex; align-items: center; gap: 10px; min-width: 0; }
.kb-title h1 { margin: 0; font-size: 22px; line-height: 1.2; color: #0c2d63; }
.kb-title > svg { color: var(--cf-brand-blue, #1e63ff); }
.kb-badge { display: inline-flex; align-items: center; min-height: 22px; padding: 0 9px; border-radius: 999px; font-size: 12px; font-weight: 850; }
.kb-badge.ok { color: #047857; background: #ecfdf5; border: 1px solid #a7f3d0; }
.kb-badge.preparing { color: #1e40af; background: #e0ecff; border: 1px solid #bfdbfe; }
.kb-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }

.kb-toolbar {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px;
  border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-panel, 22px);
  background: rgba(255, 255, 255, 0.9);
  box-shadow: var(--cf-shadow-card, 0 12px 30px rgba(0, 47, 135, 0.08));
}
.kb-search {
  flex: 1 1 240px;
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-control, 16px);
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.96);
}
.kb-search > svg { flex: 0 0 auto; color: var(--cf-muted, #64748b); }
.kb-search input { flex: 1 1 auto; min-width: 0; border: 0; outline: 0; background: transparent; color: var(--cf-text, #0f172a); font: inherit; }
.kb-search .spin { flex: 0 0 auto; color: var(--cf-brand-blue, #1e63ff); }
.kb-toggle { display: flex; gap: 4px; padding: 4px; border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px; background: #f4f8ff; }
.kb-toggle button {
  display: inline-flex; align-items: center; gap: 5px; min-height: 30px; padding: 0 12px;
  border: 0; border-radius: 999px; background: transparent; color: #48627f; font: inherit; font-size: 13px; font-weight: 800; cursor: pointer;
}
.kb-toggle button.active { background: #fff; color: var(--cf-brand-blue, #1e63ff); box-shadow: 0 3px 10px rgba(30, 99, 255, 0.14); }

.alert {
  display: flex; align-items: center; gap: 8px; min-width: 0; padding: 9px 12px;
  border-radius: 14px; font-size: 13px; line-height: 1.4;
}
.alert > span { min-width: 0; overflow-wrap: anywhere; flex: 1 1 auto; }
.alert.error { border: 1px solid #fecaca; background: #fef2f2; color: var(--cf-danger, #991b1b); }
.alert.success { border: 1px solid #a7f3d0; background: #ecfdf5; color: var(--cf-success, #047857); }
.alert .icon-button { flex: 0 0 auto; }

.kb-upload {
  display: grid; gap: 10px; padding: 14px; border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-card, 18px); background: rgba(248, 251, 255, 0.86);
}
.kb-upload-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.kb-upload-head strong { color: #0c2d63; font-size: 15px; }
.kb-dropzone {
  display: grid; justify-items: center; gap: 7px; width: 100%; min-height: 124px;
  padding: 16px; border: 1px dashed #91b2dd; border-radius: 8px;
  background: #fff; color: #33526f; font: inherit; cursor: pointer;
  transition: background 160ms ease, border-color 160ms ease;
}
.kb-dropzone > svg { color: var(--cf-brand-blue, #1e63ff); }
.kb-dropzone strong { font-size: 14px; }
.kb-dropzone span { font-size: 12px; color: var(--cf-muted, #64748b); overflow-wrap: anywhere; }
.kb-dropzone:hover:not(:disabled), .kb-upload.dragging .kb-dropzone { background: #edf5ff; border-color: #1e63ff; }
.kb-dropzone:focus-visible { outline: 2px solid #1e63ff; outline-offset: 3px; }
.kb-dropzone:disabled { opacity: 0.6; cursor: not-allowed; }
.kb-text-entry { display: grid; gap: 10px; }
.kb-text-entry label { display: grid; gap: 5px; min-width: 0; font-size: 13px; }
.kb-text-entry input, .kb-text-entry textarea {
  box-sizing: border-box; width: 100%; min-width: 0; padding: 8px 10px;
  border: 1px solid var(--cf-border, #d8e5f7); border-radius: 8px; background: #fff; color: inherit; font: inherit;
}
.kb-text-entry textarea { resize: vertical; min-height: 108px; max-height: 360px; line-height: 1.6; }
.kb-text-entry input:focus-visible, .kb-text-entry textarea:focus-visible { outline: 2px solid #1e63ff; outline-offset: 1px; }
.kb-text-entry .primary { justify-self: end; }
.kb-upload-list { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; max-height: 320px; overflow-y: auto; }
.folder-picker { justify-self: start; display: inline-flex; align-items: center; gap: 6px; min-height: 34px; padding: 6px 12px; border: 1px solid var(--cf-border-strong, #cfe0ff); border-radius: 8px; color: var(--cf-brand-blue, #1e63ff); background: #fff; font: inherit; font-size: 13px; cursor: pointer; }
.folder-picker:hover:not(:disabled) { background: #edf4ff; }
.folder-picker:disabled { opacity: .55; cursor: not-allowed; }
.folder-picker:focus-visible { outline: 2px solid var(--cf-brand-blue, #1e63ff); outline-offset: 2px; }
.kb-upload-list li { display: flex; align-items: center; gap: 8px; min-width: 0; font-size: 13px; }
.kb-upload-list li span { flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.kb-upload-list li em { flex: 0 0 auto; color: var(--cf-muted, #64748b); font-style: normal; }
.kb-upload-list li > svg { flex-shrink: 0; }
.kb-upload-replace { margin: 0; color: var(--cf-warning, #92400e); font-size: 12px; overflow-wrap: anywhere; }
.kb-upload-actions { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
.kb-upload-progress { display: flex; align-items: center; gap: 8px; height: 18px; border-radius: 999px; background: #eef3fb; overflow: hidden; }
.kb-upload-progress > span { flex: 1 1 auto; align-self: stretch; background: linear-gradient(135deg, #1e63ff, #1554df); transition: width 120ms ease; border-radius: 999px; min-width: 6px; }
.kb-upload-progress > em { flex: 0 0 auto; min-width: 34px; padding: 0 6px; color: var(--cf-muted, #64748b); font-size: 11px; font-style: normal; text-align: right; }

.kb-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 14px;
  min-width: 0;
}
@media (min-width: 920px) {
  .kb-layout.has-detail { grid-template-columns: minmax(0, 1.05fr) minmax(0, 1fr); align-items: start; }
}
.kb-pane {
  min-width: 0; padding: 10px 0; border-top: 1px solid var(--cf-border, #d8e5f7);
}
.kb-list { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
.kb-row {
  display: grid; gap: 4px; padding: 8px; border: 1px solid transparent; border-radius: 8px;
}
.kb-row.selected { border-color: var(--cf-border-strong, #cfe0ff); background: #f2f7ff; }
.kb-row-main {
  display: flex; align-items: center; gap: 10px; width: 100%; min-width: 0; text-align: left;
  border: 0; background: transparent; color: inherit; font: inherit; cursor: pointer; padding: 4px 2px;
}
.kb-file-icon { flex: 0 0 auto; display: inline-grid; place-items: center; width: 34px; height: 34px; border-radius: 12px; background: #eef4ff; color: var(--cf-brand-blue, #1e63ff); }
.kb-row-text { flex: 1 1 auto; min-width: 0; }
.kb-row-text strong { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 14px; }
.kb-row-text small { display: block; margin-top: 2px; color: var(--cf-muted, #64748b); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.kb-status { flex: 0 0 auto; display: inline-flex; align-items: center; min-height: 20px; padding: 0 8px; border-radius: 999px; font-size: 11px; font-style: normal; font-weight: 850; }
.kb-status.ready { color: #047857; background: #ecfdf5; }
.kb-status.queued, .kb-status.indexing { color: #1e40af; background: #e0ecff; }
.kb-status.failed { color: #991b1b; background: #fef2f2; }
.kb-status.blocked { color: #92400e; background: #fffbeb; }
.kb-status.deleted { color: #64748b; background: #f1f5f9; }
.kb-row-actions { display: flex; align-items: center; gap: 4px; padding-left: 44px; flex-wrap: wrap; }
.kb-row-error { margin: 0; padding-left: 44px; color: var(--cf-danger, #991b1b); font-size: 12px; overflow-wrap: anywhere; }

.icon-button {
  display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; flex: 0 0 auto;
  border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px; background: #fff; color: #33526f; cursor: pointer; padding: 0;
}
.icon-button:hover:not(:disabled) { border-color: var(--cf-border-strong, #cfe0ff); background: #f4f8ff; }
.icon-button.danger { color: var(--cf-danger, #991b1b); }
.icon-button:disabled { opacity: 0.5; cursor: not-allowed; }

.primary {
  display: inline-flex; align-items: center; gap: 6px; min-height: 36px; padding: 0 14px;
  border: 0; border-radius: var(--cf-radius-control, 16px); background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff; font: inherit; font-size: 13px; font-weight: 900; cursor: pointer;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.24);
}
.primary:hover:not(:disabled) { filter: brightness(1.04); }
.primary:disabled { opacity: 0.55; cursor: not-allowed; }

.kb-pager { display: flex; align-items: center; justify-content: flex-end; gap: 6px; padding: 10px 4px 2px; color: var(--cf-muted, #64748b); font-size: 12px; }

.kb-detail {
  min-width: 0; padding: 12px; border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-panel, 22px); background: rgba(248, 251, 255, 0.9);
  box-shadow: var(--cf-shadow-card, 0 12px 30px rgba(0, 47, 135, 0.08));
  position: sticky; top: 14px;
}
.kb-detail-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; padding: 2px 2px 10px; border-bottom: 1px solid #e5edf8; }
.kb-detail-head > div { min-width: 0; }
.kb-detail-head strong { display: block; overflow-wrap: anywhere; color: #0c2d63; font-size: 15px; }
.kb-detail-head small { display: block; margin-top: 3px; color: var(--cf-muted, #64748b); font-size: 12px; overflow-wrap: anywhere; }
.kb-detail-actions { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 10px 2px; flex-wrap: wrap; }
.kb-stale { display: flex; align-items: flex-start; gap: 6px; margin: 0 0 8px; padding: 8px 10px; border: 1px solid #fde68a; border-radius: 10px; background: #fffbeb; color: var(--cf-warning, #92400e); font-size: 12px; line-height: 1.5; overflow-wrap: anywhere; }
.kb-stale svg { flex: 0 0 auto; margin-top: 1px; }
.kb-sections { display: grid; gap: 8px; max-height: 60vh; overflow: auto; padding-right: 2px; }
.kb-section { padding: 10px; border-bottom: 1px solid var(--cf-border, #d8e5f7); background: #fff; }
.kb-section.current { border-color: #a8c7ff; box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.1); }
.kb-section-meta { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; font-size: 12px; }
.kb-section-meta span { color: var(--cf-brand-blue, #1e63ff); font-weight: 850; }
.kb-section-meta em { color: var(--cf-muted, #64748b); font-style: normal; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 60%; }
.kb-section p { margin: 0; color: #334155; font-size: 13px; line-height: 1.6; white-space: pre-wrap; overflow-wrap: anywhere; }

.empty { display: grid; place-items: center; gap: 8px; min-height: 120px; padding: 20px; color: var(--cf-muted, #64748b); font-size: 13px; text-align: center; }

.muted { color: var(--cf-muted, #64748b); font-size: 12px; }
.spin { animation: kb-spin 1s linear infinite; }
@keyframes kb-spin { to { transform: rotate(360deg); } }

@media (max-width: 720px) {
  .kb-page { padding: 14px 12px 32px; }
  .kb-detail { position: static; }
}
</style>
