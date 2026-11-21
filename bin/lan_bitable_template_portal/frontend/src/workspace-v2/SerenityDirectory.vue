<script lang="ts">
export interface DirectoryEntry {
  id: string | number; name: string; url: string; purpose: string; category: string; sort: number;
}
export interface LibraryController {
  items: DirectoryEntry[]; sortedFiltered: DirectoryEntry[]; pageItems: DirectoryEntry[]; PAGE_SIZE: number;
  categories: string[]; search: string; categoryFilter: string; kindFilter: '' | 'bitable' | 'webpage';
  page: number; pageCount: number; canEdit: boolean; loading: boolean; saving: boolean;
  error: string; staleWarning: string; orderMessage: string; orderFailed: boolean;
  pageElement: HTMLElement | null; modalElement: HTMLElement | null; modalTitleId: string;
  modalOpen: boolean; modalError: string; modalDirty: boolean; editor: DirectoryEntry;
  confirmOpen: boolean; confirmMessage: string; dragName: string; dragPosition: {left: string; top: string};
  draggedId: string; movedId: string; dropTarget: string; dropPlacement: 'before' | 'after'; hoverDirection: number;
  reload(): void; openCreate(): void; openEdit(entry: DirectoryEntry): void; askDelete(entry: DirectoryEntry): void;
  resolveDelete(ok: boolean): void; saveEntry(): void; closeModal(): void;
  startDrag(event: PointerEvent, entry: DirectoryEntry): void; pointerMove(event: PointerEvent): void;
  pointerDrop(event: PointerEvent): void; endDrag(): void; moveByKeyboard(entry: DirectoryEntry, delta: number): void;
  isTrustedUrl(url: string): boolean; linkKindLabel(url: string): string;
}
</script>
<script setup lang="ts">
import {computed, toRef} from 'vue';
import {ArrowDown, ArrowUp, BookMarked, ChevronLeft, ChevronRight, ExternalLink, Globe, GripVertical, Pencil, Plus, RefreshCw, Search, Table2, Trash2} from 'lucide-vue-next';
import ConfirmDialog from '../components/ConfirmDialog.vue';
import ModuleBackButton from './ModuleBackButton.vue';
import SerenityDirectoryEditor from './SerenityDirectoryEditor.vue';

const props = defineProps<{model: LibraryController}>();
const model = props.model;
const pageElement = toRef(model, 'pageElement');
const firstIndex = computed(() => (model.page - 1) * model.PAGE_SIZE);
const kinds = [{id:'', name:'全部'}, {id:'bitable', name:'多维表'}, {id:'webpage', name:'网页导航'}] as const;
function clearFilters() {
  model.search = '';
  model.categoryFilter = '';
  model.kindFilter = '';
}
</script>
<template>
  <section ref="pageElement" class="sd-directory" aria-label="多维表导航" @pointermove="model.pointerMove" @pointerup="model.pointerDrop" @pointercancel="model.endDrag">
    <header class="sd-heading">
      <ModuleBackButton to="/"/>
      <div class="sd-title"><BookMarked :size="23"/><div><h1>多维表导航</h1><small>{{ model.items.length }} 个入口 · {{ model.categories.length }} 个分类</small></div></div>
      <button aria-label="刷新链接列表" :disabled="model.saving || model.loading" @click="model.reload"><RefreshCw :size="15"/>刷新</button>
      <button v-if="model.canEdit" class="primary" :disabled="model.saving || model.loading" @click="model.openCreate"><Plus :size="16"/>新增链接</button>
    </header>
    <section class="sd-catalogue" aria-label="查找入口">
      <div class="sd-search">
        <label><Search :size="17"/><input v-model="model.search" type="search" aria-label="按名称、用途或分类搜索" placeholder="搜索名称、用途或分类"/></label>
        <select v-model="model.categoryFilter" aria-label="按分类筛选"><option value="">全部分类</option><option v-for="category in model.categories" :key="category">{{ category }}</option></select>
      </div>
      <div class="sd-scope"><div class="sd-kinds" aria-label="链接类型"><button v-for="kind in kinds" :key="kind.id" :aria-pressed="model.kindFilter===kind.id" @click="model.kindFilter=kind.id">{{ kind.name }}</button></div>
        <span role="status">{{ model.loading ? '正在读取…' : model.sortedFiltered.length + ' 个结果' }}</span>
      </div>
      <p v-if="model.staleWarning" class="sd-warning" role="status">{{ model.staleWarning }}</p>
      <p v-if="model.error" class="sd-warning" role="alert">{{ model.error }} <button aria-label="刷新链接列表" :disabled="model.saving || model.loading" @click="model.reload">重试</button></p>
      <p v-if="model.orderMessage" :class="model.orderFailed?'sd-warning':'sd-feedback'" :role="model.orderFailed?'alert':'status'">{{ model.orderMessage }}</p>
      <table v-if="model.pageItems.length" class="sd-entries link-table" aria-label="导航链接">
        <thead><tr><th scope="col">序号</th><th scope="col">入口与用途</th><th scope="col">分类</th><th v-if="model.canEdit" scope="col">管理</th></tr></thead>
        <tbody><tr v-for="(entry,index) in model.pageItems" :key="entry.id" :data-link-id="entry.id"
          :class="{moved: model.movedId===String(entry.id),dragging: model.draggedId===String(entry.id)}" :data-placement="model.dropTarget===String(entry.id)?model.dropPlacement:undefined">
          <td class="sd-seq"><div><button v-if="model.canEdit" class="drag-handle" :disabled="model.saving || model.loading" :aria-label="'调整顺序 '+entry.name" title="拖动排序，或用上下方向键移动"
            @pointerdown="model.startDrag($event,entry)" @keydown.up.prevent="model.moveByKeyboard(entry,-1)" @keydown.down.prevent="model.moveByKeyboard(entry,1)"><GripVertical :size="15"/></button><span class="sort-cell">{{ firstIndex+index+1 }}</span></div></td>
          <td class="sd-entry"><div class="sd-entry-main"><component :is="model.linkKindLabel(entry.url)==='多维表'?Table2:Globe" :size="19"/>
            <div><a v-if="model.isTrustedUrl(entry.url)" :href="entry.url" target="_blank" rel="noopener noreferrer">{{ entry.name }} <ExternalLink :size="13"/></a>
              <strong v-else>{{ entry.name }} <small>链接无效</small></strong>
              <p v-if="entry.purpose">{{ entry.purpose }}</p>
            </div></div></td>
          <td class="sd-category">{{ entry.category || '未分类' }}<small>{{ model.linkKindLabel(entry.url) }}</small></td>
          <td v-if="model.canEdit" class="sd-actions"><div>
            <button :disabled="model.saving" aria-label="编辑链接" :title="'编辑 '+entry.name" @click="model.openEdit(entry)"><Pencil :size="15"/></button>
            <button :disabled="model.saving" aria-label="删除链接" :title="'删除 '+entry.name" @click="model.askDelete(entry)"><Trash2 :size="15"/></button>
            <details class="v2-link-menu"><summary aria-label="调整链接顺序">排序</summary>
              <span><button :disabled="model.loading || model.saving || firstIndex+index===0" @click="model.moveByKeyboard(entry,-1)"><ArrowUp :size="14"/>上移</button>
              <button :disabled="model.loading || model.saving || firstIndex+index+1===model.sortedFiltered.length" @click="model.moveByKeyboard(entry,1)"><ArrowDown :size="14"/>下移</button></span></details>
          </div></td>
        </tr></tbody>
      </table>
      <div v-else-if="!model.loading && !model.error" class="sd-empty">
        <p>{{ model.items.length ? '没有匹配的链接' : '目录中暂无入口' }}</p><button v-if="model.search || model.kindFilter || model.categoryFilter" @click="clearFilters">清除筛选</button>
      </div>
      <nav v-if="model.pageCount>1" class="sd-pages" aria-label="目录分页"><small v-if="model.hoverDirection">停留后翻页</small>
        <button data-page-direction="-1" aria-label="上一页" :disabled="model.page<=1 || model.loading || model.saving" @click="model.page--"><ChevronLeft :size="16"/></button>
        <span>{{ model.page }} / {{ model.pageCount }}</span>
        <button data-page-direction="1" aria-label="下一页" :disabled="model.page>=model.pageCount || model.loading || model.saving" @click="model.page++"><ChevronRight :size="16"/></button>
      </nav>
    </section>
    <SerenityDirectoryEditor v-if="model.modalOpen" :model="model"/>
    <ConfirmDialog :open="model.confirmOpen" tone="danger" title="删除导航入口" :message="model.confirmMessage" confirm-label="删除入口" @resolve="(ok: boolean) => model.resolveDelete(ok && model.canEdit)"/>
    <Teleport to="body"><div v-if="model.draggedId" class="sd-ghost" :style="model.dragPosition">{{ model.dragName }}</div></Teleport>
  </section>
</template>
<style scoped>
.sd-directory {--panel:var(--soft-face,#f1f4f4);--field:var(--soft-field,#e9eeee);--ink:var(--v2-ink,#243e47);--line:color-mix(in srgb,var(--ink) 15%,transparent);--muted:color-mix(in srgb,var(--ink) 76%,var(--panel));display:grid;align-content:start;gap:12px;padding:clamp(10px,2vw,24px);min-width:0;color:var(--ink);}
.sd-heading,.sd-catalogue {border:1px solid var(--line);border-radius:17px;box-shadow:var(--soft-raised,0 3px 9px #263d480e);padding:clamp(12px,1.6vw,18px);background:var(--panel);min-width:0;}
.sd-heading,.sd-title,.sd-search,.sd-scope,.sd-kinds,.sd-pages,.sd-actions>div,.sd-seq>div,.sd-entry-main {display:flex;align-items:center;gap:8px;}
.sd-heading {flex-wrap:wrap;background:linear-gradient(110deg,color-mix(in srgb,var(--panel) 65%,#a3d4f0),color-mix(in srgb,var(--panel) 78%,#a1dec0));}
.sd-title {flex:1 1 180px;gap:11px;} .sd-title>svg {width:42px;height:42px;padding:10px;border-radius:13px;background:var(--panel);color:var(--v2-blue,#25678c);box-sizing:border-box;}
h1 {font-size:clamp(20px,2vw,28px);line-height:1.2;margin:0;} small,.sd-scope>span,.sd-category,.sd-entry p {color:var(--muted);font-size:12px;}
button,summary,select {cursor:pointer;border:1px solid var(--line);background:var(--panel);border-radius:8px;color:inherit;font:inherit;font-size:13px;padding:7px 10px;}
button {display:inline-flex;align-items:center;justify-content:center;gap:5px;min-height:34px;} button.primary {background:var(--v2-palette-accent);color:var(--v2-palette-on-accent);border-color:transparent;} button:disabled {opacity:.5;cursor:default;}
:focus-visible {outline:3px solid var(--v2-blue,#3387b0);outline-offset:2px;}
.sd-search {flex-wrap:wrap;} .sd-search label {display:flex;align-items:center;gap:8px;flex:1 1 200px;background:var(--field);padding:9px 12px;border-radius:10px;box-shadow:var(--soft-inset,inset 1px 2px 4px #263d4814);}
.sd-search input {min-width:0;flex:1;border:0;background:transparent;color:inherit;font:inherit;} .sd-search select {max-width:100%;}
.sd-scope {justify-content:space-between;flex-wrap:wrap;margin:10px 0;}.sd-kinds {gap:4px;}.sd-kinds button[aria-pressed=true] {background:var(--v2-palette-selection,#d8eee6);color:var(--ink);border-color:var(--ink);}
table.sd-entries {width:100%;border-collapse:collapse;table-layout:auto;color:inherit;font-size:14px;}
.sd-entries th,.sd-entries td {text-align:start;padding:9px 6px;border-top:1px solid var(--line);background:transparent;vertical-align:middle;}
.sd-entries th {font-size:12px;color:var(--muted);}.sd-entries .sd-seq,.sd-entries .sd-actions {width:1%;white-space:nowrap;}
.sd-entry-main {align-items:flex-start;gap:9px;}.sd-entry-main>svg {flex:none;color:var(--v2-blue,#286785);}
.sd-entry,.sd-category {overflow-wrap:anywhere;} .sd-entry a {color:var(--ink);font-weight:650;text-decoration:none;}.sd-entry a:hover {text-decoration:underline;}
.sd-entry p {margin:3px 0 0;white-space:pre-wrap;}.sd-category small {display:block;}
.sd-directory .drag-handle {touch-action:none;cursor:grab;padding:4px;}
.sd-entries tr[data-placement=before] td {box-shadow:inset 0 2px var(--v2-blue,#459dc4);}.sd-entries tr[data-placement=after] td {box-shadow:inset 0 -2px var(--v2-blue,#459dc4);}
.sd-entries tr.moved,.sd-entries tr.dragging {background:var(--v2-palette-selection,#d8eee6);}
.sd-directory .v2-link-menu {display:none;}.sd-pages {justify-content:flex-end;padding-top:10px;font-size:13px;}
.sd-warning,.sd-feedback,.sd-empty {border-radius:9px;padding:8px 12px;}.sd-warning {background:#fff2df;color:#734019;}.sd-feedback {background:#ddf0e5;color:#245b41;}.sd-empty {text-align:center;}
.sd-ghost {position:fixed;pointer-events:none;z-index:3000;width:max-content;max-width:min(300px,90vw);overflow-wrap:anywhere;padding:8px 12px;border-radius:8px;background:#245d51;color:white;}
@media(max-width:760px){.sd-directory {padding:8px;}.sd-search select {flex:1 1 140px;}.sd-title {flex-basis:130px;}table.sd-entries,.sd-entries tbody {display:block;width:auto;}.sd-entries thead {position:absolute;clip-path:inset(50%);width:1px;height:1px;overflow:hidden;}.sd-directory .sd-entries tr {display:grid;grid-template-columns:auto minmax(0,1fr);gap:6px 8px;border-top:1px solid var(--line);padding:10px 0;}
.sd-directory .sd-entries td {display:block;border:0;padding:0;width:auto;}.sd-category,.sd-actions {grid-column:2;}.sd-category small {display:inline;margin-left:8px;}.sd-actions>div {flex-wrap:wrap;}.sd-directory .drag-handle {display:none;}.sd-seq {align-self:start;}.sd-directory .v2-link-menu {display:block;}.v2-link-menu[open]>span {display:inline-flex;gap:6px;margin-top:5px;}button,summary {min-height:38px;}}
</style>
