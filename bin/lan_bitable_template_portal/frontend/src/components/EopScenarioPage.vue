<template>
  <section ref="root" class="eop-page" aria-label="EOP 场景维护" data-v2-inline-feedback>
    <header class="eop-header">
      <NButton v-if="!embedded || record || opening" quaternary :aria-label="record || opening ? '返回场景列表' : '返回'"
        :disabled="saving || uploading" @click="headerBack"><ArrowLeft :size="19" /></NButton>
      <div class="eop-heading">
        <small>{{ opening ? 'EOP 场景 · 读取中' : record ? editing ? 'EOP 场景 · 编辑中' : 'EOP 场景 · 查看' : '专项功能' }}</small>
        <h1 ref="heading" tabindex="-1">{{ (record || opening)?.fields['场景名称'] || 'EOP 场景维护' }}</h1>
        <div v-if="record && !editing" class="eop-summary" aria-label="场景信息">
          <span v-for="field in selectFields" :key="field" class="eop-tag">{{ field }} · {{ record.fields[field] || '未设置' }}</span>
        </div>
      </div>
      <div class="eop-header-actions">
        <NButton v-if="record?.editable && !editing" type="primary" :disabled="busy" @click="editing = true"><Pencil :size="15" />编辑</NButton>
        <NButton :loading="loading" :disabled="busy" aria-label="刷新" title="刷新" @click="leave(refresh)"><RefreshCw :size="16" /><span v-if="!record">刷新</span></NButton>
      </div>
    </header>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false" role="status">{{ message }}</NAlert>

    <section v-if="opening" class="eop-panel eop-opening" aria-busy="true" role="status">正在读取场景正文和步骤…</section>
    <section v-else-if="!record" class="eop-panel" aria-label="场景列表">
      <div class="eop-toolbar" aria-label="场景范围"><NButton :type="listMode==='p0'?'primary':'default'" :disabled="busy" @click="changeListMode('p0')">首页 · P0 高优先级</NButton><NButton :type="listMode==='other'?'primary':'default'" :disabled="busy" @click="changeListMode('other')">其他等级场景</NButton></div>
      <div class="eop-stats" aria-label="场景统计">
        <button type="button" :class="{selected:!specialty&&!missing}" @click="specialty='';missing=''">{{ listMode==='p0'?'全部 P0':'全部其他等级' }} <strong>{{ rows.length }}</strong></button>
        <button v-for="item in specialtyCounts" :key="item.name" type="button" :class="{selected:specialty===item.name}" @click="specialty=specialty===item.name?'':item.name;missing=''">{{ item.name }} <strong>{{ item.count }}</strong></button>
        <button v-for="item in missingCounts" :key="item.key" type="button" :class="{selected:missing===item.key}" @click="missing=missing===item.key?'':item.key;specialty=''">{{ item.label }}未填写 <strong>{{ item.count }}</strong></button>
      </div>
      <p v-if="updatedAt" class="eop-cache-note">{{ listMode==='p0'?'仅显示 P0-高':'显示非 P0 及未设置等级的场景' }} · 每 5 分钟同步多维表 · 最近同步 {{ new Date(updatedAt*1000).toLocaleString('zh-CN') }}<strong v-if="Date.now()/1000-updatedAt>360"> · 同步可能延迟，请刷新核对</strong></p>
      <div class="eop-toolbar">
        <NInput v-model:value="search" class="eop-search" clearable placeholder="搜索场景名称、专业或设备"
          :input-props="{ 'aria-label': '搜索场景' }"><template #prefix><Search :size="16" /></template></NInput>
        <NSelect v-if="listMode==='other'" v-model:value="priority" class="eop-filter" :options="priorities" aria-label="筛选等级" />
        <NSelect v-model:value="specialty" class="eop-filter" :options="specialties" aria-label="筛选专业" />
        <NSelect v-model:value="missing" class="eop-filter" :options="missingOptions" aria-label="筛选缺失内容" />
        <span class="eop-muted">共 {{ filtered.length }} 条</span>
      </div>
      <div v-if="filtered.length" class="eop-list">
        <div class="eop-list-head" aria-hidden="true"><span>场景名称</span><span>专业</span><span>系统／设备</span><span>资料完整情况</span></div>
        <button v-for="item in shown" :key="item.id" type="button" class="eop-list-row" :disabled="busy" @click="openFromList(item.id, $event)">
          <strong>{{ item.fields['场景名称'] || '未命名场景' }}<small v-if="listMode==='other'" class="eop-priority">{{ item.fields['原优先级'] || '等级未设置' }}</small></strong>
          <span class="eop-row-tags">
            <span v-for="field in selectFields.slice(0,2)" :key="field"><span class="eop-tag">{{ item.fields[field] || '未设置' }}</span></span>
            <span class="eop-presence"><span :class="{missing:!item.has_steps}">步骤{{ item.has_steps?'✓':'缺' }}</span><span :class="{missing:!item.has_template}">模板{{ item.has_template?'✓':'缺' }}</span><span :class="{missing:!item.has_images}">图片{{ item.has_images?'✓':'缺' }}</span></span>
          </span>
          <ChevronRight :size="16" />
        </button>
      </div>
      <NEmpty v-else-if="!loading" class="eop-empty" description="暂无符合条件的场景" />
      <div v-if="filtered.length > pageSize" class="eop-page-controls">
        <NPagination v-model:page="page" :item-count="filtered.length" :page-size="pageSize" simple />
      </div>
    </section>

    <template v-else>
      <section v-if="editing" class="eop-panel eop-meta" aria-label="编辑场景信息">
        <label class="eop-name">场景名称 <input v-model="record.fields['场景名称']" maxlength="200" :disabled="busy" /></label>
        <label v-for="field in selectFields" :key="field">{{ field }}
          <select v-model="record.fields[field]" :disabled="busy">
            <option value="">未填写</option>
            <option v-for="option in choices(field)" :key="option" :value="option">{{ option }}</option>
          </select>
        </label>
      </section>

      <div class="eop-workspace" :class="{ 'with-pool': unassigned.length || editing }">
      <section class="eop-panel eop-work" aria-label="步骤与图片" @paste="pastePictures($event,'')">
        <header class="eop-section-head">
          <div><h2>原通告重点项</h2><small>{{ record.steps.length }} 步 · {{ record.images.length - unassigned.length }} 张已关联</small></div>
          <div class="eop-step-tools">
            <select v-if="record.steps.length > 3" aria-label="跳到步骤" :value="''" @change="jumpToStep($event)">
              <option value="">跳到步骤</option>
              <option v-for="(step, index) in record.steps" :key="step.key" :value="step.key">第 {{ index + 1 }} 步 · {{ step.text.slice(0,28) || '待填写' }}</option>
            </select>
            <NButton v-if="unassigned.length" size="small" @click="focusSection(poolHeading)">待分配 {{ unassigned.length }} 张</NButton>
            <NButton v-if="editing" type="primary" secondary :disabled="busy || record.steps.length >= 200" @click="addStep"><Plus :size="16" />添加步骤</NButton>
          </div>
        </header>
        <NEmpty v-if="!record.steps.length" class="eop-empty" description="原记录暂无步骤" />
        <article v-for="(step,index) in record.steps" :key="step.key" :id="'eop-step-'+step.key" class="eop-step"
          :class="{ 'has-images': step.images.length, 'eop-drop-ready': draggedPhoto, 'eop-drop-active': dropStep===step.key }"
          @paste.stop="pastePictures($event,step.key)" @dragover="overStep(step.key,$event)" @dragleave="leaveStep(step.key,$event)" @drop.stop="dropPicture(step.key,$event)">
          <div class="eop-step-body">
            <div class="eop-step-title">
              <h3 tabindex="-1">第 {{ index + 1 }} 步</h3>
              <div v-if="editing" class="eop-step-actions">
                <NButton quaternary size="small" :disabled="busy || index === 0" :aria-label="`上移第 ${index+1} 步`" title="上移" @click="moveStep(index,-1)"><ArrowUp :size="16" /></NButton>
                <NButton quaternary size="small" :disabled="busy || index === record.steps.length-1" :aria-label="`下移第 ${index+1} 步`" title="下移" @click="moveStep(index,1)"><ArrowDown :size="16" /></NButton>
                <NButton quaternary size="small" type="error" class="eop-delete-step" :disabled="busy" :aria-label="`删除第 ${index+1} 步`" title="删除步骤，图片移回待分配" @click="record.steps.splice(index,1)"><Trash2 :size="16" /></NButton>
              </div>
            </div>
            <textarea v-if="editing" v-model="step.text" rows="3" maxlength="16000" :aria-label="`第 ${index+1} 步内容`" placeholder="填写这一项需要执行或通告的内容" :disabled="busy" />
            <p v-else class="eop-step-text">{{ step.text }}</p>
            <NButton v-if="editing && !step.images.length" class="eop-add-image" size="small" :disabled="busy || record.images.length >= 100" :aria-label="`为第 ${index+1} 步添加图片`" @click="choosePictures(step.key)"><ImagePlus :size="16" />添加图片</NButton>
          </div>
          <div v-if="step.images.length" class="eop-step-media">
            <div class="eop-media-head"><strong>对应图片 <small>{{ step.images.length }} 张</small></strong>
              <NButton v-if="editing" size="small" :disabled="busy || record.images.length >= 100" :aria-label="`为第 ${index+1} 步添加图片`" @click="choosePictures(step.key)"><ImagePlus :size="16" />添加图片</NButton>
            </div>
            <div class="eop-gallery">
              <figure v-for="photo in stepPictures(step)" :key="photo.token">
                <button type="button" class="eop-thumb" :aria-label="'预览 '+photo.name" @click="openPreview(photo, stepPictures(step), `第 ${index+1} 步`, $event)">
                  <img v-if="!broken.has(photo.token)" :src="photo.url" alt="" loading="lazy" @error="broken.add(photo.token)" />
                  <span v-else>打开原图</span>
                </button>
                <figcaption><input v-if="editing" v-model="photo.name" maxlength="180" aria-label="图片名称" :disabled="busy" /><span v-else :title="photo.name">{{ photo.name }}</span>
                  <NButton v-if="editing" text size="tiny" :disabled="busy" @click="step.images = step.images.filter(token => token !== photo.token)">移回待分配</NButton>
                </figcaption>
              </figure>
            </div>
          </div>
        </article>
      </section>

      <section v-if="unassigned.length || editing" class="eop-panel eop-pool" aria-label="待分配图片" @paste="pastePictures($event,'')" @dragover="overFiles" @drop="dropFiles($event,'')">
        <header class="eop-section-head">
          <div><h2 ref="poolHeading" tabindex="-1">待分配图片</h2><small>{{ unassigned.length }} 张<span v-if="editing && unassigned.length && record.steps.length"> · 拖动图片到步骤，或选择步骤</span></small></div>
          <NButton v-if="editing" size="small" :disabled="busy || record.images.length >= 100" @click="choosePictures('')"><Upload :size="16" />上传到待分配</NButton>
        </header>
        <p v-if="editing" class="eop-muted">可点击上传、粘贴图片或拖入此区域。</p>
        <div v-if="editing && assignedPhoto" class="eop-assigned">
          <span role="status">已关联「{{ assignedPhoto.name }}」到第 {{ assignedPhoto.index }} 步</span>
          <NButton text type="primary" @click="focusStep(assignedPhoto.key)">查看步骤</NButton>
        </div>
        <div v-if="unassigned.length" class="eop-gallery eop-pool-grid">
          <figure v-for="photo in unassigned" :key="photo.token">
            <button type="button" class="eop-thumb" :aria-label="'预览 '+photo.name"
              :draggable="editing && !busy && record.steps.length>0"
              @dragstart="startPictureDrag(photo.token,$event)" @dragend="clearPictureDrag"
              @click="openPreview(photo, unassigned, '待分配图片', $event)">
              <img v-if="!broken.has(photo.token)" :src="photo.url" alt="" loading="lazy" decoding="async" draggable="false" @error="broken.add(photo.token)" />
              <span v-else>打开原图</span>
            </button>
            <figcaption><input v-if="editing" v-model="photo.name" maxlength="180" aria-label="图片名称" :disabled="busy" /><span v-else :title="photo.name">{{ photo.name }}</span>
              <select v-if="editing && record.steps.length" aria-label="分配到步骤" :disabled="busy" @change="assign(photo.token, $event)">
                <option value="">分配到步骤</option>
                <option v-for="(step,index) in record.steps" :key="step.key" :value="step.key">第 {{ index + 1 }} 步 · {{ step.text.slice(0,28) || '待填写' }}</option>
              </select>
              <NButton v-if="editing" text type="error" size="tiny" :disabled="busy" @click="removePicture(photo)">从记录移除</NButton>
            </figcaption>
          </figure>
        </div>
        <p v-else-if="editing" class="eop-pool-empty">当前没有待分配图片，可上传图片后关联到步骤。</p>
      </section>
      </div>

      <section class="eop-panel eop-source" aria-label="原文资料">
        <header class="eop-section-head"><h2>原文资料</h2></header>
        <details v-for="field in longFields" :key="field">
          <summary>{{ field }} <small v-if="!record.fields[field]">未填写</small></summary>
          <textarea v-if="editing" v-model="record.fields[field]" :aria-label="field" :disabled="busy" rows="7" maxlength="100000" />
          <p v-else class="eop-step-text">{{ record.fields[field] || '—' }}</p>
        </details>
      </section>
      <footer v-if="editing" class="eop-save">
        <span class="eop-muted" role="status">{{ saving || uploading ? progress : loading ? '正在读取记录' : error || (dirty ? '有未保存的更改' : '当前记录已同步') }}</span>
        <div class="eop-save-actions">
          <NButton :disabled="busy" @click="leave(finishEditing)">{{ dirty ? '取消编辑' : '完成编辑' }}</NButton>
          <NButton type="primary" :disabled="!dirty || busy" :loading="saving" @click="save">保存记录</NButton>
        </div>
      </footer>
      <input ref="fileInput" hidden type="file" multiple accept="image/png,image/jpeg,image/gif,image/webp" @change="uploadPictures" />
    </template>
    <NModal :show="!!preview" preset="card" :title="previewTitle" class="eop-preview" style="width:min(94vw,1000px)" @update:show="show => { if (!show) closePreview(); }" @after-leave="restorePreviewFocus">
      <div v-if="preview" @keydown.left.prevent="changePreview(-1)" @keydown.right.prevent="changePreview(1)">
        <img :key="preview.token" :src="preview.ticket ? preview.url : preview.url + '?original=1'" :alt="preview.name" class="eop-original" @error="previewError = true" />
        <NAlert v-if="previewError" type="warning" :bordered="false">图片暂时无法读取，可关闭后重试。</NAlert>
        <div class="eop-preview-actions">
          <span>{{ preview.name }}</span>
          <div v-if="previewPhotos.length > 1">
            <NButton :disabled="previewIndex === 0" @click="changePreview(-1)">上一张</NButton>
            <NButton :disabled="previewIndex === previewPhotos.length - 1" @click="changePreview(1)">下一张</NButton>
          </div>
        </div>
      </div>
    </NModal>
    <NModal :show="!!discardAction" preset="dialog" title="有未保存的更改" content="离开当前记录会丢失本次更改。" positive-text="放弃更改" negative-text="继续编辑"
      @positive-click="discard" @update:show="show => { if (!show) discardAction = undefined; }" />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NEmpty, NInput, NModal, NPagination, NSelect } from 'naive-ui';
import { ArrowDown, ArrowLeft, ArrowUp, ChevronRight, ImagePlus, Pencil, Plus, RefreshCw, Search, Trash2, Upload } from 'lucide-vue-next';
import { requestBinaryJson, requestJson } from '../api/client';
import { registerNavigationGuard } from '../navigation';

defineOptions({ inheritAttrs: false });
defineProps<{embedded?:boolean}>();
const emit=defineEmits<{close:[]}>();

interface Row { id: string; fields: Record<string,string>; has_steps?:boolean; has_template?:boolean; has_images?:boolean }
interface Photo { token: string; name: string; url: string; ticket?: string }
interface Step { key: string; text: string; images: string[] }
interface Detail extends Row { revision: string; editable: boolean; options: Record<string,string[]>; steps: Step[]; images: Photo[] }
const api = '/api/eop-scenarios';
const selectFields = ['专业', '系统/设备', '原优先级'];
const longFields = ['处置正文（原文）', '原通告模板', '原演练场景', '原EOP精炼（参考）'];
const rows = ref<Row[]>([]);
const listMode=ref<'p0'|'other'>('p0'),priority=ref('');
const priorities=computed(()=>[{label:'全部等级',value:''},...Array.from(new Set(rows.value.map(r=>r.fields['原优先级']||'等级未设置'))).sort().map(value=>({label:value,value}))]);
const record = ref<Detail>();
const opening = shallowRef<Row>();
const editing = ref(false);
const search = ref('');
const specialty = ref('');
const missing = ref('');
const updatedAt = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);
const saving = ref(false);
const uploading = ref(false);
const busy = computed(() => saving.value || uploading.value || loading.value);
const progress = ref('');
const error = ref('');
const message = ref('');
const saved = ref('');
const removedImages = new Set<string>();
const previewPhotos = shallowRef<Photo[]>([]);
const previewIndex = ref(0);
const previewGroup = ref('');
const preview = computed(() => previewPhotos.value[previewIndex.value]);
const previewTitle = computed(() => preview.value ? `${previewGroup.value} · 第 ${previewIndex.value+1}/${previewPhotos.value.length} 张` : '场景图片');
const previewError = ref(false);
const broken = ref(new Set<string>());
const root = ref<HTMLElement>();
const heading = ref<HTMLElement>();
const poolHeading = ref<HTMLElement>();
const fileInput = ref<HTMLInputElement>();
const discardAction = shallowRef<() => void>();
const lastAssignment = ref<{key: string; token: string}>();
const draggedPhoto = ref('');
const dropStep = ref('');
let previewTrigger: HTMLElement | undefined;
let listPosition: {scroller: Element; top: number; id: string} | undefined;
let pictureTarget: {record: Detail; stepKey: string} | undefined;
let stepKey = 0;
let activeRead: AbortController | undefined;
let disposed = false;
const specialties = computed(() => [{ label: '全部专业', value: '' }, ...Array.from(new Set(rows.value.map(row => row.fields['专业']).filter(Boolean))).sort().map(value => ({ label: value, value }))]);
const specialtyCounts = computed(() => specialties.value.slice(1).map(item=>({name:item.value,count:rows.value.filter(row=>row.fields['专业']===item.value).length})));
const missingOptions = [{label:'全部资料',value:''},{label:'缺少步骤',value:'has_steps'},{label:'缺少模板',value:'has_template'},{label:'缺少图片',value:'has_images'}];
const missingCounts = computed(() => [{key:'has_steps',label:'步骤',count:rows.value.filter(row=>!row.has_steps).length},{key:'has_template',label:'模板',count:rows.value.filter(row=>!row.has_template).length},{key:'has_images',label:'图片',count:rows.value.filter(row=>!row.has_images).length}]);
const filtered = computed(() => {
  const query = search.value.trim().toLowerCase();
  return rows.value.filter(row => (!specialty.value || row.fields['专业'] === specialty.value) && (!priority.value || (row.fields['原优先级']||'等级未设置')===priority.value)
    && (!missing.value || !row[missing.value as 'has_steps'|'has_template'|'has_images'])
    && Object.values(row.fields).some(value => value.toLowerCase().includes(query)));
});
const shown = computed(() => filtered.value.slice((page.value-1)*pageSize,page.value*pageSize));
const payload = computed(() => record.value && ({
  fields: record.value.fields, steps: record.value.steps.map(({text,images}) => ({text,images})),
  images: record.value.images.map(({token,ticket,name}) => ({token,ticket,name})),
}));
const dirty = computed(() => !!record.value?.editable && JSON.stringify(payload.value) !== saved.value);
const photos = computed(() => new Map(record.value?.images.map(photo => [photo.token,photo])));
const unassigned = computed(() => {
  const assigned = new Set(record.value?.steps.flatMap(step => step.images));
  return (record.value?.images || []).filter(photo => !assigned.has(photo.token));
});
const assignedPhoto = computed(() => {
  const assignment = lastAssignment.value;
  if (!assignment || !record.value) return undefined;
  const index = record.value.steps.findIndex(step => step.key === assignment.key && step.images.includes(assignment.token));
  const photo = photos.value.get(assignment.token);
  return index >= 0 && photo ? {key: assignment.key, name: photo.name, index: index+1} : undefined;
});
watch([search,specialty,missing,priority], () => { page.value = 1; });
watch(() => filtered.value.length, length => { page.value = Math.min(page.value, Math.max(1, Math.ceil(length/pageSize))); });
watch(preview, () => { previewError.value = false; });
const choices = (field: string) => [...new Set([...(record.value?.options[field] || []),record.value?.fields[field] || ''])].filter(Boolean);
const stepPictures = (step: Step) => step.images.flatMap(token => photos.value.get(token) || []);
function focusSection(section: HTMLElement | null | undefined, anchor: HTMLElement | null | undefined = section) {
  anchor?.scrollIntoView({block:'start'});
  section?.focus({preventScroll:true});
}
function focusRecordHeading() {
  focusSection(heading.value, root.value?.querySelector<HTMLElement>('.eop-header'));
}
function focusStep(key: string) {
  focusSection(root.value?.querySelector<HTMLElement>('#eop-step-'+key+' h3'));
}
function jumpToStep(event: Event) {
  const select = event.target as HTMLSelectElement;
  if (select.value) focusStep(select.value);
  select.value = '';
}
function openPreview(photo: Photo, group: Photo[], label: string, event: MouseEvent) {
  previewTrigger = event.currentTarget as HTMLElement;
  previewGroup.value = label;
  previewIndex.value = group.findIndex(item => item.token === photo.token);
  previewPhotos.value = group;
}
function changePreview(delta: number) {
  const next = previewIndex.value + delta;
  if (previewPhotos.value[next]) previewIndex.value = next;
}
function closePreview() { previewPhotos.value = []; }
function restorePreviewFocus() { previewTrigger?.focus({preventScroll:true}); previewTrigger = undefined; }
const fail = (value: unknown) => { error.value = value instanceof Error ? value.message : '请求未完成，请重试。'; };
function revokePictures() { for (const photo of record.value?.images || []) if (photo.ticket) URL.revokeObjectURL(photo.url); }
async function read<T>(path: string, apply: (data:T) => void) {
  activeRead?.abort();
  const request = activeRead = new AbortController();
  loading.value = true;
  error.value = '';
  try {
    const data = await requestJson(path,{signal:request.signal,cache:'no-store'}) as T;
    if (!disposed && activeRead === request && !request.signal.aborted) apply(data);
  } catch (value) { if (!request.signal.aborted) fail(value); }
  finally {
    if (activeRead === request) {
      loading.value = false;
      opening.value = undefined;
    }
  }
}
function loadList(refresh=false) { return read<{items:Row[];updated_at?:number}>(api+'?priority_scope='+listMode.value+(refresh?'&refresh=1':''), data => { rows.value = data.items; updatedAt.value=data.updated_at || 0; }); }
function changeListMode(mode:'p0'|'other'){if(busy.value||mode===listMode.value)return;listMode.value=mode;priority.value='';specialty.value='';missing.value='';search.value='';rows.value=[];page.value=1;void loadList();}
function applyRecord(data: Detail) {
  revokePictures();
  clearPictureDrag();
  pictureTarget = undefined;
  record.value = { ...data, steps: data.steps.map(step => ({...step,key:String(++stepKey)})) };
  broken.value = new Set();
  lastAssignment.value = undefined;
  editing.value = editing.value && data.editable;
  removedImages.clear();
  saved.value = JSON.stringify(payload.value);
}
function openFromList(id: string, event: MouseEvent) {
  const button = event.currentTarget as HTMLElement;
  let scroller = button.parentElement;
  while (scroller && !/auto|scroll/.test(getComputedStyle(scroller).overflowY)) scroller = scroller.parentElement;
  const container = scroller ?? document.scrollingElement;
  if (container) listPosition = {scroller:container, top:container.scrollTop, id};
  openRecord(id);
}
function openRecord(id: string) {
  message.value = '';
  if (!record.value) {
    opening.value = rows.value.find(row => row.id === id);
    void nextTick(focusRecordHeading);
  }
  void read<Detail>(api+'/'+id, data => {
    applyRecord(data);
    void nextTick(focusRecordHeading);
  });
}
function closeRecord() {
  activeRead?.abort();
  opening.value = undefined;
  clearPictureDrag();
  revokePictures();
  pictureTarget = undefined;
  record.value = undefined;
  editing.value = false;
  lastAssignment.value = undefined;
  loading.value = false;
  error.value = message.value = '';
  const position = listPosition;
  listPosition = undefined;
  void nextTick(() => {
    if (!position) return;
    position.scroller.scrollTop = position.top;
    const index = shown.value.findIndex(row => row.id === position.id);
    if (index >= 0) root.value?.querySelectorAll<HTMLElement>('.eop-list-row')[index]?.focus({preventScroll:true});
  });
}
function back() { emit('close'); }
function headerBack() {
  if (saving.value || uploading.value) return;
  if (opening.value) { closeRecord(); return; }
  if (loading.value) {
    activeRead?.abort();
    loading.value = false;
  }
  leave(record.value ? closeRecord : back);
}
function refresh() {
  if (!record.value) { void loadList(true); return; }
  const id=record.value.id;
  void read<{items:Row[];updated_at?:number}>(api+'?refresh=1', data=>{rows.value=data.items;updatedAt.value=data.updated_at || 0;openRecord(id);});
}
function finishEditing() {
  const current = record.value;
  if (current && dirty.value) {
    void read<Detail>(api+'/'+current.id, data => {
      applyRecord(data);
      editing.value = false;
      void nextTick(focusRecordHeading);
    });
  } else {
    editing.value = false;
    void nextTick(focusRecordHeading);
  }
}
function addStep() {
  if (!record.value) return;
  record.value.steps.push({key:String(++stepKey),text:'',images:[]});
  void nextTick(() => root.value?.querySelector<HTMLTextAreaElement>('.eop-step:last-child textarea')?.focus());
}
function moveStep(index: number, delta: number) {
  const steps = record.value?.steps;
  if (!steps?.[index + delta]) return;
  const step = steps[index];
  [steps[index],steps[index+delta]] = [steps[index+delta],step];
  void nextTick(() => focusStep(step.key));
}
function choosePictures(stepKey: string) {
  if (!record.value?.editable || !editing.value || busy.value) return;
  pictureTarget = {record:record.value, stepKey};
  fileInput.value?.click();
}
async function uploadPictures(event: Event) {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = '';
  const target = pictureTarget;
  pictureTarget = undefined;
  if (!target) return;
  await uploadFiles(files,target);
}
async function uploadFiles(files:File[],target:{record:Detail;stepKey:string}) {
  if (target.record !== record.value || !editing.value || busy.value || !files.length) return;
  const current = target.record;
  const step = current.steps.find(item => item.key === target.stepKey);
  if (target.stepKey && !step) return;
  if (current.images.length + files.length > 100 || files.some(file => file.size > 18*1024*1024 || !file.size)) {
    error.value = '每条记录最多 100 张图片，单张图片须在 18 MB 以内。';
    return;
  }
  uploading.value = true;
  error.value = message.value = '';
  try {
    for (const [index,file] of files.entries()) {
      progress.value = `正在上传图片 ${index+1}/${files.length}`;
      const added = await requestBinaryJson(`${api}/${current.id}/image?name=${encodeURIComponent(file.name)}`,file) as Omit<Photo,'url'>;
      if (disposed || current !== record.value) break;
      if (!current.images.some(photo => photo.token === added.token)) {
        current.images.push({...added,url:URL.createObjectURL(file)});
        step?.images.push(added.token);
      }
    }
  } catch (value) { fail(value); }
  finally { uploading.value = false; }
}
function pastePictures(event:ClipboardEvent,stepKey:string) {
  if (!record.value?.editable || !editing.value || busy.value) return;
  const files=Array.from(event.clipboardData?.files || []).filter(file=>file.type.startsWith('image/'));
  if (!files.length) return;
  event.preventDefault();
  void uploadFiles(files,{record:record.value,stepKey});
}
function overFiles(event:DragEvent) {
  if (editing.value && !busy.value && event.dataTransfer?.types.includes('Files')) event.preventDefault();
}
function dropFiles(event:DragEvent,stepKey:string) {
  if (!record.value?.editable || !editing.value || busy.value) return;
  const files=Array.from(event.dataTransfer?.files || []).filter(file=>file.type.startsWith('image/'));
  if (!files.length) return;
  event.preventDefault();
  void uploadFiles(files,{record:record.value,stepKey});
}
function assign(token: string, event: Event) {
  const input = event.target as HTMLSelectElement;
  if (assignPicture(token,input.value)) {
    void nextTick(() => root.value?.querySelector<HTMLElement>('.eop-assigned button')?.focus({preventScroll:true}));
  }
  input.value = '';
}
function assignPicture(token: string, stepKey: string) {
  if (!record.value?.editable || !editing.value || busy.value || !unassigned.value.some(photo => photo.token===token)) return false;
  const step = record.value.steps.find(item => item.key === stepKey);
  if (!step) return false;
  step.images.push(token);
  lastAssignment.value = {key:step.key, token};
  return true;
}
function clearPictureDrag() {
  draggedPhoto.value = '';
  dropStep.value = '';
}
function startPictureDrag(token: string, event: DragEvent) {
  if (!event.dataTransfer || !record.value?.editable || !editing.value || busy.value || !record.value.steps.length) {
    event.preventDefault();
    return;
  }
  event.dataTransfer.setData('application/x-clipflow-eop-picture',token);
  event.dataTransfer.effectAllowed = 'move';
  draggedPhoto.value = token;
}
function overStep(key: string, event: DragEvent) {
  if (event.dataTransfer?.types.includes('Files')) { overFiles(event); return; }
  if (!draggedPhoto.value || busy.value || !event.dataTransfer?.types.includes('application/x-clipflow-eop-picture')) return;
  event.preventDefault();
  event.dataTransfer.dropEffect = 'move';
  dropStep.value = key;
}
function leaveStep(key: string, event: DragEvent) {
  if (dropStep.value===key && !(event.relatedTarget instanceof Node && (event.currentTarget as HTMLElement).contains(event.relatedTarget))) {
    dropStep.value = '';
  }
}
function dropPicture(key: string, event: DragEvent) {
  if (event.dataTransfer?.files.length) { dropFiles(event,key); return; }
  const token = event.dataTransfer?.getData('application/x-clipflow-eop-picture');
  if (!token || token!==draggedPhoto.value) return;
  event.preventDefault();
  clearPictureDrag();
  assignPicture(token,key);
}
function removePicture(photo: Photo) {
  if (!record.value) return;
  if (photo.ticket) URL.revokeObjectURL(photo.url);
  else removedImages.add(photo.token);
  record.value.images = record.value.images.filter(item => item.token !== photo.token);
}
async function save() {
  const current = record.value;
  if (!current?.editable || !editing.value || busy.value || !dirty.value) return;
  if (!current.fields['场景名称'].trim() || current.steps.some(step => !step.text.trim())) {
    error.value = '请填写场景名称和每个步骤的内容。';
    return;
  }
  saving.value = true;
  progress.value = '正在保存记录';
  error.value = message.value = '';
  try {
    const result = await requestJson(api+'/'+current.id,{
      method:'PUT',
      body:JSON.stringify({...payload.value,revision:current.revision,removed_images:[...removedImages]}),
    }) as {saved: boolean; record: Detail};
    if (disposed || record.value !== current) return;
    if (!result.saved || result.record?.id !== current.id) throw new Error('保存结果未确认，请重新打开核对。');
    applyRecord(result.record);
    message.value = '记录已保存到飞书。';
    const row = rows.value.find(item => item.id === current.id);
    if (row) {
      if ((result.record.fields['原优先级']==='P0-高')!==(listMode.value==='p0')) rows.value=rows.value.filter(item=>item.id!==row.id);
      else { row.fields = {...result.record.fields}; row.has_steps=!!result.record.steps.length; row.has_template=!!result.record.fields['原通告模板'].trim(); row.has_images=!!result.record.images.length; }
    }
  } catch (value) { if (!disposed && record.value === current) fail(value); }
  finally { saving.value = false; }
}
function leave(action: () => void) { if (!busy.value) { if (dirty.value) discardAction.value = action; else action(); } }
function discard() { const action = discardAction.value; discardAction.value = undefined; action?.(); }
const unregister = registerNavigationGuard((_target,proceed) => {
  if (busy.value) return false;
  if (!dirty.value) return true;
  discardAction.value = proceed;
  return false;
});
function beforeUnload(event: BeforeUnloadEvent) { if (busy.value || dirty.value) event.preventDefault(); }
let refreshTimer:ReturnType<typeof setInterval> | undefined;
onMounted(() => { void loadList(); refreshTimer=setInterval(()=>{if (!record.value && !opening.value && !loading.value) void loadList();},300000); window.addEventListener('beforeunload',beforeUnload); });
onBeforeUnmount(() => { disposed = true; if (refreshTimer) clearInterval(refreshTimer); activeRead?.abort(); unregister(); revokePictures(); window.removeEventListener('beforeunload',beforeUnload); });
</script>

<style scoped>
.eop-page {
  --ep-ink:var(--v2-ink,var(--cf-text,#17243a)); --ep-muted:var(--v2-muted,var(--cf-muted,#53657a));
  --ep-line:var(--v2-border,var(--cf-border,#d8e5f7)); --ep-surface:var(--cf-surface,#fff);
  --ep-accent:var(--v2-blue,var(--cf-brand-blue,#2563eb)); --ep-soft:color-mix(in srgb,var(--ep-surface),var(--ep-accent) 7%);
  display:grid; gap:12px; padding:12px 14px; margin:auto; color:var(--ep-ink); min-width:0; box-sizing:border-box; max-width:1500px;
}
.eop-header,.eop-panel,.eop-save { min-width:0; border:1px solid var(--ep-line); border-radius:10px; background:var(--ep-surface); }
.eop-opening { min-height:96px; display:grid; place-items:center; color:var(--ep-muted); }
.eop-pool .eop-thumb[draggable=true] { cursor:grab; }
.eop-work .eop-step.eop-drop-ready { outline:1px dashed var(--ep-accent); outline-offset:-2px; }
.eop-work .eop-step.eop-drop-active { outline:2px solid var(--ep-accent); background:var(--ep-soft); }
.eop-header { display:flex; align-items:center; gap:10px; padding:10px; background:linear-gradient(110deg,var(--ep-soft),var(--ep-surface)); }
.eop-heading { flex:1; min-width:0; }
.eop-header h1 { margin:0; font-size:20px; overflow-wrap:anywhere; }
.eop-header small,.eop-muted,.eop-section-head small,.eop-media-head small { color:var(--ep-muted); font-size:12px; }
.eop-toolbar,.eop-section-head,.eop-media-head,.eop-step-title,.eop-save { display:flex; align-items:center; gap:8px 12px; flex-wrap:wrap; justify-content:space-between; }
.eop-header-actions,.eop-step-tools,.eop-summary,.eop-save-actions { display:flex; align-items:center; flex-wrap:wrap; gap:6px; }
.eop-header-actions { flex:none; }
.eop-summary { margin-top:6px; }
.eop-tag { display:inline-block; padding:2px 7px; border-radius:5px; background:var(--ep-soft); color:var(--ep-ink); font-size:12px; overflow-wrap:anywhere; }
.eop-toolbar,.eop-meta,.eop-work,.eop-pool,.eop-source { padding:12px; }
.eop-workspace { display:grid; grid-template-columns:minmax(0,1fr); align-items:start; gap:12px; min-width:0; }
@media(min-width:1100px){
  .eop-workspace.with-pool { grid-template-columns:minmax(0,1fr) minmax(245px,320px); }
  .eop-workspace .eop-pool { position:sticky; top:12px; max-height:calc(100dvh - 32px); overflow:auto; overscroll-behavior:contain; }
  .eop-workspace .eop-pool-grid { grid-template-columns:repeat(auto-fill,minmax(min(100%,110px),1fr)); }
}
.eop-toolbar { justify-content:flex-start; }
.eop-stats { display:flex; flex-wrap:wrap; gap:6px; padding:12px 12px 0; }
.eop-stats button { display:flex; align-items:center; gap:6px; padding:6px 9px; border:1px solid var(--ep-line); border-radius:6px; background:var(--ep-surface); color:var(--ep-ink); cursor:pointer; font:inherit; font-size:13px; }
.eop-stats button.selected { border-color:var(--ep-accent); background:var(--ep-soft); }
.eop-cache-note { margin:8px 12px 0; color:var(--ep-muted); font-size:12px; }
.eop-presence { display:flex; flex-wrap:wrap; gap:4px; font-size:12px; }
.eop-presence span { color:#18805e; white-space:nowrap; }
.eop-presence .missing { color:#c3413b; font-weight:600; }
.eop-search { flex:1 1 210px; }
.eop-filter { width:128px; }
.eop-list-head,.eop-list-row { display:grid; grid-template-columns:minmax(180px,2.5fr) minmax(0,1fr) minmax(0,1.5fr) minmax(170px,1.4fr) 16px; gap:8px; align-items:center; padding:10px 12px; }
.eop-list-head { background:var(--ep-soft); color:var(--ep-muted); font-size:12px; font-weight:700; }
.eop-list-row { width:100%; border:0; border-top:1px solid var(--ep-line); background:transparent; text-align:left; cursor:pointer; font:inherit; color:inherit; }
.eop-list-row:hover { background:var(--ep-soft); }
.eop-list-row:focus-visible { outline:2px solid var(--ep-accent); outline-offset:-2px; }
.eop-list-row>* { min-width:0; overflow-wrap:anywhere; }
.eop-row-tags { display:contents; }
.eop-empty { padding:20px 0; }
.eop-page-controls { display:flex; justify-content:flex-end; padding:10px 12px; border-top:1px solid var(--ep-line); }
.eop-meta { display:grid; grid-template-columns:minmax(210px,2fr) repeat(3,minmax(0,1fr)); gap:8px 12px; }
.eop-meta label { display:grid; gap:5px; min-width:0; font-size:13px; }
.eop-page input,.eop-page select,.eop-page textarea { min-width:0; max-width:100%; box-sizing:border-box; padding:7px 9px; border:1px solid var(--ep-line); border-radius:6px; background:var(--ep-surface); color:inherit; font:inherit; }
.eop-page textarea { display:block; width:100%; line-height:1.6; resize:vertical; }
.eop-section-head h2 { margin:0; font-size:16px; }
.eop-step { display:grid; grid-template-columns:minmax(0,1fr); border:1px solid var(--ep-line); border-radius:9px; margin-top:10px; overflow:hidden; }
.eop-step.has-images { grid-template-columns:minmax(0,1.25fr) minmax(0,1fr); }
.eop-step-body,.eop-step-media { min-width:0; padding:10px; }
.eop-step-media { background:var(--ep-soft); border-left:1px solid var(--ep-line); }
.eop-step-title { margin-bottom:7px; }
.eop-step-title h3 { margin:0; padding:3px 7px; border-radius:5px; background:var(--ep-soft); color:var(--ep-ink); font-size:14px; }
.eop-step-actions { display:flex; gap:4px; }
.eop-delete-step { margin-inline-start:8px; }
.eop-add-image { margin-top:8px; }
.eop-step-tools select { max-width:min(215px,100%); }
.eop-header,.eop-step-title h3,.eop-pool h2 { scroll-margin-top:12px; }
.eop-step-text { margin:4px 0; line-height:1.6; white-space:pre-wrap; overflow-wrap:anywhere; }
.eop-gallery { display:grid; grid-template-columns:repeat(auto-fill,minmax(min(100%,118px),1fr)); gap:8px; margin-top:8px; }
.eop-pool-grid { grid-template-columns:repeat(auto-fill,minmax(min(100%,155px),1fr)); }
.eop-gallery figure { margin:0; min-width:0; padding:5px; border:1px solid var(--ep-line); border-radius:7px; background:var(--ep-surface); }
.eop-thumb { display:grid; place-items:center; width:100%; aspect-ratio:4/3; padding:0; overflow:hidden; background:var(--ep-soft); border:0; cursor:zoom-in; color:inherit; }
.eop-thumb img { width:100%; height:100%; min-height:0; object-fit:contain; }
.eop-gallery figcaption { display:grid; gap:4px; margin-top:4px; font-size:12px; }
.eop-gallery figcaption>span { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.eop-gallery figcaption .n-button { justify-self:start; }
.eop-pool-empty { margin:10px 0 0; color:var(--ep-muted); font-size:13px; line-height:1.6; }
.eop-assigned { display:flex; align-items:center; flex-wrap:wrap; gap:6px 12px; padding:7px 9px; margin-top:8px; background:var(--ep-soft); border-radius:6px; font-size:13px; overflow-wrap:anywhere; }
.eop-source details { border-top:1px solid var(--ep-line); }
.eop-source details:first-of-type { margin-top:8px; }
.eop-source summary { cursor:pointer; padding:9px 0; }
.eop-source summary small { margin-inline-start:8px; color:var(--ep-muted); }
.eop-save { position:sticky; bottom:0; padding:8px 12px; z-index:2; box-shadow:0 -3px 14px rgb(15 30 50 / 6%); }
.eop-save-actions { margin-inline-start:auto; }
:global(.workspace-v2 .eop-page .eop-save) { padding-inline-end:64px; }
.eop-original { display:block; max-width:100%; max-height:72vh; margin:auto; object-fit:contain; }
.eop-preview-actions { display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px; margin-top:10px; overflow-wrap:anywhere; }
.eop-preview-actions>div { display:flex; gap:6px; margin-inline-start:auto; }
@media(max-width:760px) {
  .eop-page { padding:6px 4px; gap:8px; }
  .eop-workspace { gap:8px; }
  .eop-meta { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .eop-name { grid-column:1/-1; }
  .eop-header { gap:6px; }
  .eop-header h1 { font-size:17px; }
  .eop-header-actions .n-button { padding-inline:8px; }
  .eop-step.has-images { grid-template-columns:minmax(0,1fr); }
  .eop-step-media { border-left:0; border-top:1px solid var(--ep-line); }
  .eop-list-head { display:none; }
  .eop-list-row { grid-template-columns:minmax(0,1fr) 16px; gap:4px 8px; }
  .eop-row-tags { display:flex; flex-wrap:wrap; gap:4px; grid-column:1; }
  .eop-list-row svg { grid-row:1/3; grid-column:2; }
  .eop-step-actions .n-button { min-width:44px; min-height:44px; padding:0; }
  .eop-media-head .n-button,.eop-add-image,.eop-save .n-button,.eop-preview-actions .n-button { min-height:40px; }
  :global(.v2-shell.is-mobile-workspace .eop-page .eop-save) { bottom:calc(62px + env(safe-area-inset-bottom,0px)); }
}
.eop-priority{display:block;font-size:12px;font-weight:400;margin-top:4px;color:var(--text-color-3,#64748b)}
</style>
