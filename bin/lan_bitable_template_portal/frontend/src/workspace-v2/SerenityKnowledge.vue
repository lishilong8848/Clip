<script lang="ts">
import type { GroupItem } from './knowledgeFolders';

export interface LibraryDocument {
  id: string; name: string; owner_name: string; can_edit: boolean; version: string | number;
  active_version?: string | number | null; view_version?: string | number | null; historical?: boolean;
  status: string; updated_at: unknown; size: number; chunks: number; error?: string; group_name?: string | null;
}
export interface LibraryController {
  search: string; recycle: boolean; items: LibraryDocument[]; groups: GroupItem[]; groupFilter: string;
  groupOptions: {value: string; label: string}[]; groupBusy: boolean; assignGroupId: string;
  page: number; pages: number; pageNumbers: number[]; total: number; loadingList: boolean; quietLoading: boolean;
  selectedCount: number; allSelection: object | null; allPageSelected: boolean; selectionDisabled: boolean;
  editableOnPage: LibraryDocument[]; error: string; notice: string; actionBusy: boolean;
  engineBadge: string; settings: { warning?: string }; pageElement: HTMLElement | null;
  detail: LibraryDocument | null; detailPage: number; detailPages: number; detailTotal: number;
  detailLoading: boolean; readerError: string; previewEmptyText: string; focusedOrdinal: number | null;
  sections: {text: string; location: string; ordinal: number}[];
  uploadPanel: boolean; uploadTarget: LibraryDocument | null; pendingFiles: File[]; uploadMode: 'files' | 'text';
  textName: string; textContent: string; uploadBusy: boolean; scanningFolder: boolean; uploadProgress: number;
  uploadError: string; uploadConfirm: boolean; uploadConfirmMessage: string; dragging: boolean; FILE_ACCEPT: string;
  dropzone: HTMLButtonElement | null; fileInput: HTMLInputElement | null; folderInput: HTMLInputElement | null;
  deleteTarget: LibraryDocument | null; retryTarget: LibraryDocument | null; restoreTarget: LibraryDocument | null;
  bulkDeleteTarget: {count: number} | null; deleteGroupTarget: GroupItem | null;
  refresh(): void; switchBin(value: boolean): void; switchGroupFilter(id: string): void; changePage(page: number): void;
  createFolder(name: string, parent?: string): void; moveRenameFolder(id: string, name: string, parent?: string): void;
  isSelected(doc: LibraryDocument): boolean; toggleSelect(id: string): void; toggleSelectAll(): void;
  selectAllMatching(): void; clearSelection(): void; askBulkDelete(): void; assignSelection(): void; unassignSelection(): void;
  openUpload(): void; onUploadPick(event: Event): void; onFolderPick(event: Event): void;
  onDragOver(event: DragEvent): void; onDragLeave(event: DragEvent): void; onDrop(event: DragEvent): void;
  addText(): boolean; requestUpload(): void; pickReplace(doc: LibraryDocument): void;
  statusText(status: string): string; sizeText(bytes: number): string; timeText(value: unknown): string;
  openDetail(doc: LibraryDocument): void; closeDetail(): void; readPage(page: number): void; retryRead(): void;
  downloadActive(doc: LibraryDocument): void;
  resolveUpload(ok: boolean): void; resolveDelete(ok: boolean): void; resolveRestore(ok: boolean): void;
  resolveRetry(ok: boolean): void; resolveBulkDelete(ok: boolean): void; resolveDeleteGroup(ok: boolean): void;
}
</script>
<script setup lang="ts">
import {computed, ref, toRef} from 'vue';
import {ArchiveRestore, BookOpen, ChevronLeft, ChevronRight, FileText, Folders, RefreshCw, Search, Upload} from 'lucide-vue-next';
import KnowledgeFolders from './KnowledgeFolders.vue';
import KnowledgeGroupPicker from '../components/KnowledgeGroupPicker.vue';
import ConfirmDialog from '../components/ConfirmDialog.vue';
import ModuleBackButton from './ModuleBackButton.vue';
import SerenityKnowledgeReader from './SerenityKnowledgeReader.vue';
import SerenityKnowledgeUpload from './SerenityKnowledgeUpload.vue';

const props = defineProps<{model: LibraryController}>();
const model = props.model;
const pageElement = toRef(model, 'pageElement');
const foldersOpen = ref(false);
const folderName = computed(() => model.groupOptions.find(g => g.value === model.groupFilter)?.label
  || (model.groupFilter === 'ungrouped' ? '未分组' : '全部文件'));
function chooseFolder(id: string) {
  model.switchGroupFilter(id);
  foldersOpen.value = false;
}
function deleteFolder(ok: boolean) {
  if (ok) model.resolveDeleteGroup(true);
  else model.deleteGroupTarget = null;
}
</script>

<template>
  <section ref="pageElement" class="sk-library" aria-label="知识库" @dragover="model.onDragOver" @dragleave="model.onDragLeave" @drop="model.onDrop">
    <header class="sk-cover">
      <ModuleBackButton to="/" />
      <div class="sk-heading"><span class="sk-symbol"><BookOpen :size="23" /></span>
        <div><h1>知识库</h1><small>{{ model.engineBadge }}</small></div>
      </div>
      <div class="sk-commands">
        <button :disabled="model.loadingList || model.groupBusy" @click="model.refresh"><RefreshCw :size="15" />刷新</button>
        <button class="sk-primary" :disabled="model.uploadBusy" @click="model.openUpload"><Upload :size="16" />上传文件</button>
      </div>
    </header>

    <p v-if="model.settings.warning" class="sk-alert" role="status">{{ model.settings.warning }}</p>
    <p v-if="model.error" class="sk-alert" role="alert">{{ model.error }}</p>
    <p v-if="model.notice" class="sk-notice" role="status">{{ model.notice }}</p>
    <SerenityKnowledgeUpload v-if="model.uploadPanel" :model="model" />

    <section class="sk-shelf" aria-label="查找文档">
      <div class="sk-findbar">
        <div class="sk-switch" aria-label="文档范围">
          <button :aria-pressed="!model.recycle" :disabled="model.groupBusy" @click="model.switchBin(false)">共享文档</button>
          <button :aria-pressed="model.recycle" :disabled="model.groupBusy" @click="model.switchBin(true)">回收站</button>
        </div>
        <label class="sk-search"><Search :size="17"/><input v-model="model.search" type="search" aria-label="搜索知识库" placeholder="搜索文档名或上传者" maxlength="200" /></label>
      </div>
      <div class="sk-context">
        <button v-if="!model.recycle" :aria-expanded="foldersOpen" :disabled="model.groupBusy" @click="foldersOpen = !foldersOpen"><Folders :size="16" />{{ folderName }}</button>
        <span role="status">{{ model.loadingList ? '正在读取…' : model.total + ' 个文件' }}<template v-if="model.quietLoading"> · 同步中</template></span>
        <template v-if="!model.recycle">
          <label class="sk-page-check kb-select-all"><input type="checkbox" :checked="model.allPageSelected" :disabled="model.selectionDisabled || !model.editableOnPage.length" @change="model.toggleSelectAll" />全选本页</label>
          <button :disabled="model.selectionDisabled || !model.total" @click="model.selectAllMatching">全选全部</button>
        </template>
      </div>
      <section v-if="foldersOpen && !model.recycle" class="sk-folders" aria-label="文件夹目录">
        <button :disabled="model.groupBusy" @click="chooseFolder('all')">全部文件</button>
        <button :disabled="model.groupBusy" @click="chooseFolder('ungrouped')">未分组</button>
        <KnowledgeFolders :groups="model.groups" :busy="model.groupBusy" :selected-id="model.groupFilter"
          @select="chooseFolder" @create="model.createFolder" @rename="model.moveRenameFolder" @delete="model.deleteGroupTarget = $event" />
      </section>
      <div v-if="(model.selectedCount || model.allSelection) && !model.recycle" class="sk-batch" role="toolbar" aria-label="批量分组">
        <strong>已选 {{ model.selectedCount }} 个文件{{ model.allSelection ? '（全部搜索结果）' : '' }}</strong>
        <KnowledgeGroupPicker v-model="model.assignGroupId" :groups="model.groups" :disabled="model.selectionDisabled"/>
        <button class="sk-primary" :disabled="model.selectionDisabled || !model.assignGroupId" @click="model.assignSelection">应用分组</button>
        <button :disabled="model.selectionDisabled" @click="model.unassignSelection">移出分组</button>
        <button :disabled="model.selectionDisabled" @click="model.askBulkDelete">批量删除</button>
        <button :disabled="model.selectionDisabled" @click="model.clearSelection">取消选择</button>
      </div>
      <div v-if="model.items.length" class="sk-records kb-list" :aria-busy="model.loadingList">
        <article v-for="doc in model.items" :key="doc.id" class="sk-record kb-row" :class="{selected: !model.recycle && model.isSelected(doc)}">
          <input v-if="!model.recycle" type="checkbox" :aria-label="(model.isSelected(doc) ? '取消选择' : '选择') + doc.name" :checked="model.isSelected(doc)" :disabled="!doc.can_edit || model.selectionDisabled" @change="model.toggleSelect(doc.id)" />
          <div class="sk-doc">
            <button v-if="!model.recycle" class="sk-name kb-row-main" :title="doc.name" @click="model.openDetail(doc)"><FileText :size="16"/>{{ doc.name }}</button>
            <strong v-else>{{ doc.name }}</strong>
            <div class="sk-meta"><span>{{ doc.group_name || '未分组' }}</span><span>{{ doc.owner_name }}</span><span>{{ model.sizeText(doc.size) }}</span><span>{{ model.timeText(doc.updated_at) }}</span></div>
            <small v-if="doc.error" class="sk-issue">{{ doc.error }}</small>
          </div>
          <div class="sk-state"><span :data-state="doc.status">{{ model.statusText(doc.status) }}</span>
            <small v-if="doc.active_version">共享 v{{ doc.active_version }}</small>
          </div>
          <details v-if="!model.recycle" class="sk-actions"><summary :aria-label="'文件操作 '+doc.name">操作</summary>
            <div><button :disabled="model.actionBusy" @click="model.downloadActive(doc)">下载原文件</button>
              <template v-if="doc.can_edit">
                <button :disabled="model.uploadBusy || model.actionBusy" @click="model.pickReplace(doc)">替换文件</button>
                <button v-if="doc.status==='failed'" :disabled="model.actionBusy" @click="model.retryTarget=doc">重新索引</button>
                <button :disabled="model.actionBusy" @click="model.deleteTarget=doc">移入回收站</button>
              </template></div>
          </details>
          <button v-if="model.recycle && doc.can_edit" :disabled="model.actionBusy" @click="model.restoreTarget = doc"><ArchiveRestore :size="14"/>恢复</button>
        </article>
      </div>
      <p v-else-if="!model.loadingList && !model.error" class="sk-empty">
        {{ model.search ? '没有找到匹配的文档' : model.recycle ? '回收站为空' : '当前范围内还没有文档' }}
      </p>
      <nav v-if="model.pages > 1" class="sk-pages" aria-label="文档分页">
        <button aria-label="首页" :disabled="model.selectionDisabled || model.page===1" @click="model.changePage(1)">首页</button>
        <button aria-label="上一页" :disabled="model.selectionDisabled || model.page === 1" @click="model.changePage(model.page-1)"><ChevronLeft :size="16"/></button>
        <template v-for="(number,index) in model.pageNumbers" :key="number">
          <span v-if="index && number > model.pageNumbers[index-1]+1">…</span>
          <button :aria-current="model.page===number?'page':undefined" :disabled="model.selectionDisabled" @click="model.changePage(number)">{{ number }}</button>
        </template>
        <button aria-label="下一页" :disabled="model.selectionDisabled || model.page === model.pages" @click="model.changePage(model.page+1)"><ChevronRight :size="16"/></button>
        <button aria-label="末页" :disabled="model.selectionDisabled || model.page===model.pages" @click="model.changePage(model.pages)">末页</button>
      </nav>
    </section>
    <SerenityKnowledgeReader :model="model"/>
    <ConfirmDialog :open="model.uploadConfirm" title="确认上传" :message="model.uploadConfirmMessage" @resolve="model.resolveUpload"/>
    <ConfirmDialog :open="!!model.deleteTarget" title="移入回收站" tone="danger" :message="'将「'+(model.deleteTarget?.name || '')+'」移入回收站？'" @resolve="model.resolveDelete"/>
    <ConfirmDialog :open="!!model.bulkDeleteTarget" title="批量移入回收站" confirm-label="确认移入回收站" tone="danger" :message="'将选中的 '+(model.bulkDeleteTarget?.count || 0)+' 个文件移入回收站？'" @resolve="model.resolveBulkDelete"/>
    <ConfirmDialog :open="!!model.restoreTarget" title="恢复文档" :message="'恢复「'+(model.restoreTarget?.name || '')+'」并重新建立索引？'" @resolve="model.resolveRestore"/>
    <ConfirmDialog :open="!!model.retryTarget" title="重新索引" :message="'重新提交「'+(model.retryTarget?.name || '')+'」的索引任务？'" @resolve="model.resolveRetry"/>
    <ConfirmDialog :open="!!model.deleteGroupTarget" title="删除分组" tone="danger" :message="'删除「'+(model.deleteGroupTarget?.name || '')+'」分组？组内文件将退回未分组。'" @resolve="deleteFolder"/>
  </section>
</template>

<style scoped>
.sk-library { --sk-ink:var(--v2-ink,#203b43); --sk-muted:color-mix(in srgb,var(--sk-ink) 78%,var(--sk-face)); --sk-edge:color-mix(in srgb,var(--sk-ink) 16%,transparent); --sk-face:var(--soft-face,#f0f4f4); --sk-field:var(--soft-field,#eaf0f0); display:grid; align-content:start; gap:12px; min-width:0; padding:clamp(10px,2vw,24px); color:var(--sk-ink); }
.sk-cover,.sk-shelf,.sk-library :deep(.sk-upload) { min-width:0; border:1px solid var(--sk-edge); border-radius:var(--cf-radius-panel); background:var(--sk-face); box-shadow:var(--soft-raised,0 3px 9px #263d480e); }
.sk-cover {display:flex; align-items:center; flex-wrap:wrap; gap:12px; padding:14px 18px; background:var(--soft-sky);}
.sk-heading,.sk-commands,.sk-switch,.sk-search,.sk-context,.sk-batch,.sk-pages,.sk-meta {display:flex; align-items:center; gap:8px;}
.sk-heading {flex:1 1 180px;} h1 {font-size:clamp(20px,2vw,28px); margin:0; letter-spacing:.02em; line-height:1.2;}
.sk-symbol {display:grid; place-items:center; width:42px; height:42px; border-radius:14px; background:var(--sk-face); color:var(--v2-blue,#25668b);}
.sk-library small,.sk-meta,.sk-context>span {color:var(--sk-muted); font-size:12px;}
.sk-library button,.sk-library :deep(.sk-upload button) {font:inherit; font-size:13px; font-weight:600; cursor:pointer; border:1px solid var(--sk-edge); background:var(--sk-face); color:var(--sk-ink); padding:7px 11px; border-radius:9px; display:inline-flex; justify-content:center; align-items:center; gap:6px; min-height:34px; overflow-wrap:anywhere;}
.sk-library button:hover:not(:disabled) {border-color:#549381;}
.sk-library button:disabled,.sk-library :deep(.sk-upload button:disabled) {cursor:default; opacity:.55;}
.sk-library button.sk-primary,.sk-library :deep(.sk-upload .sk-primary) {background:var(--v2-palette-accent); color:var(--v2-palette-on-accent); border-color:var(--v2-palette-accent);}
.sk-library :focus-visible {outline:3px solid var(--v2-blue,#3287b0); outline-offset:2px;}
.sk-shelf {padding:clamp(10px,1.6vw,18px);}
.sk-findbar {display:flex; flex-wrap:wrap; justify-content:space-between; gap:12px;}
.sk-switch,.sk-search {border-radius:11px; background:var(--sk-field); padding:4px; box-shadow:var(--soft-inset,inset 1px 2px 4px #24425214);}
.sk-switch button {border-color:transparent; background:transparent;}
.sk-switch [aria-pressed=true],.sk-pages [aria-current=page] {background:var(--sk-face); border-color:var(--sk-edge); color:var(--v2-blue,#235b78);}
.sk-search {flex:1 1 190px; max-width:34rem; padding:6px 12px;}
.sk-search input {width:100%; min-width:0; border:0; background:transparent; color:inherit; font:inherit;}
.sk-context,.sk-batch,.sk-pages,.sk-meta {flex-wrap:wrap;}
.sk-context {padding:12px 0 9px;}
.sk-page-check {margin-inline-start:auto; display:flex; align-items:center; gap:5px; font-size:13px;}
.sk-library input[type=checkbox] {width:17px;height:17px;accent-color:#187653;}
.sk-folders,.sk-batch {background:var(--sk-field); border-radius:11px; padding:10px; margin-bottom:10px;}
.sk-folders {max-height:55vh; overflow:auto;} .sk-folders>button {margin:0 6px 8px 0;}
.sk-record {display:flex; gap:12px; align-items:center; border-top:1px solid var(--sk-edge); padding:11px 4px;}
.sk-record.selected {background:color-mix(in srgb,var(--sk-face) 87%,#89cbb0);}
.sk-doc {min-width:0;flex:1;overflow-wrap:anywhere;}
.sk-library .sk-name {background:transparent; border:0; min-height:28px; padding:0; justify-content:flex-start; text-align:left; font-size:15px;}
.sk-name svg {flex:none;color:var(--v2-blue,#236282);}
.sk-meta {gap:2px 12px;} .sk-state {display:grid; justify-items:end; font-size:13px; gap:2px;}
.sk-state [data-state=ready] {color:#247252;} .sk-state [data-state=failed],.sk-issue {color:#a14035;}
.sk-actions {position:relative;} .sk-actions summary {cursor:pointer;list-style:none;font-size:13px;padding:8px;color:var(--v2-blue,#246586);}
.sk-actions[open]>div {position:absolute;right:0;top:100%;min-width:8rem;z-index:2;display:grid;gap:3px;padding:6px;border:1px solid var(--sk-edge);background:var(--sk-face);border-radius:9px;box-shadow:0 6px 18px #203c461d;}
.sk-pages {justify-content:center; padding-top:12px;}
.sk-notice,.sk-alert,.sk-empty {padding:10px 13px; margin:0; border-radius:10px;}
.sk-alert {background:#fff2e0;color:#713d1c;} .sk-notice {background:#e0f2e9;color:#225b43;}
.sk-empty {padding:35px;text-align:center;color:var(--sk-muted);}
@media(max-width:620px) { .sk-library {padding:8px;gap:8px;} .sk-cover {padding:12px;} .sk-heading {flex-basis:130px;} .sk-search {max-width:none; flex-basis:100%;} .sk-record {gap:7px;} .sk-page-check {margin-left:0;} .sk-library button {min-height:40px;} .sk-state {font-size:12px;} }
</style>

