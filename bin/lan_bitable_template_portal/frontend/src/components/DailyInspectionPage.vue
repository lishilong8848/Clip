<template>
  <section class="inspection-page inspection-theme" :class="{'is-inspection-immersive':immersive}" aria-label="日常巡检" data-v2-inline-feedback>
    <header class="inspection-heading">
      <NButton v-if="!embedded || immersive" quaternary aria-label="返回" @click="back"><ArrowLeft :size="18" /></NButton>
      <div class="inspection-title"><h1>日常巡检</h1><p>{{ duty ? dutyLabel(duty) : '设备参数与测点读数' }}</p></div>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" role="status">{{ message }}</NAlert>
    <details v-if="recoveries.length" class="inspection-recovery" open>
      <summary>有 {{ recoveries.length }} 轮本机修改需要核对</summary>
      <p>本轮结束时仍有未确认的本机修改，请在历史巡检核对是否已归档。</p>
      <div v-for="recovery in recoveries" :key="recovery.key" class="inspection-recovery-item">
        <strong>{{ dutyLabel(recovery.duty) }}第 {{ recovery.round }} 次</strong>
        <div v-for="(draft,id) in recovery.drafts" :key="id">
          <b>{{ draft.label || id }}</b>
          <span v-for="(value,parameter) in draft.values" :key="parameter"> · {{ draft.parameterLabels?.[parameter] || parameter }}：{{ value || '空' }}</span>
          <span v-if="draft.result"> · {{ draft.result }}</span><span v-if="draft.note"> · {{ draft.note }}</span>
        </div>
        <NButton size="small" @click="recoveryRemoval=recovery">已核对，清除本机留存</NButton>
      </div>
    </details>
    <p v-if="storageError && hasDrafts && active && !active.pending" class="warning" role="status">浏览器暂存不可用，请保持页面打开，等待自动保存完成。</p>
    <details v-if="hasDrafts && autoError && active && !active.pending && !readOnly" class="inspection-recovery" open>
      <summary>本机旧版填写待处理</summary>
      <p>{{ autoError }}</p>
      <div v-for="id in conflicts" :key="id" class="inspection-recovery-item">
        <strong>{{ drafts[id]?.label || id }}</strong>
        <NButton size="small" @click="resolveConflict(id,false)">使用共享填写</NButton>
        <NButton v-if="drafts[id]?.base" size="small" @click="resolveConflict(id,true)">合并本机修改</NButton>
      </div>
      <p v-for="id in orphanedDrafts" :key="id">已找不到原测点 {{ drafts[id]?.label || id }}，本机内容仍保留。</p>
      <NButton v-if="!conflicts.size && !orphanedDrafts.length" size="small" @click="retryAutoSave">重试保存</NButton>
    </details>
    <p v-if="!state && busy" role="status">正在读取巡检状态…</p>
    <p v-if="pollError" class="warning" role="status">{{ pollError }}</p>
    <DailyInspectionWorkbook :immersive="immersive" ref="workbookPage" v-if="state?.history_scopes?.length" :scopes="state.history_scopes"
      :template-version="state.template_version" :archive-revision="state.archive_revision" :default-scope="state.view_scope || duty?.scope" :default-date="duty?.date"
      :default-shift="duty?.shift" :next-round="state.next_round" :active="active || undefined"
      :read-only="readOnly" :planned-groups="state.planned_sheet_groups" :parent-busy="!!busy || autoSaving || hasDrafts"
      @changed="load" @round-state="acceptRoundState" @scope="switchView" @exit="back">
      <template #start>
        <div v-if="duty && !readOnly && !active && canStartRound" class="inspection-start">
          <strong>{{ duty.scope }}楼 · {{ duty.shift }}班第 {{ nextRound }} 轮</strong>
          <label v-for="item in availableProfessions" :key="item.name"><input v-model="selectedProfessions" type="checkbox" :value="item.name"> {{ item.name }}<small v-if="preview"> · {{ item.count }} 台</small></label>
          <NButton v-if="!waitingForSlot" type="primary" :loading="busy==='start'" :disabled="!!busy || !selectedProfessions.length" @click="start">开始第 {{ nextRound }} 次巡检</NButton>
          <span v-else>第 {{ nextRound }} 轮 {{ time(state.opens_at || 0) }} 自动取数</span>
          <small v-if="preview">参考批次 {{ time(preview.batch) }}<template v-if="preview.cache_stale"> · 上次核验</template></small>
          <small v-else-if="previewBusy" role="status">正在读取设备清单…</small><span v-if="previewError" class="warning">{{ previewError }}</span>
        </div>
        <span v-else-if="duty && !active && !canStartRound">{{ duty.scope }}楼本班已归档 {{ archivedRounds }} / {{ state.round_limit }} 轮</span>
        <span v-else-if="duty && !active && readOnly">{{ duty.scope }}楼当前为查看模式</span>
      </template>
    </DailyInspectionWorkbook>
    <NModal :show="!!leaveAction" preset="dialog" type="warning" title="本机填写尚未保存" aria-label="本机填写尚未保存"
      content="浏览器暂存不可用，离开后本机尚未共享的填写会丢失。"
      positive-text="仍要离开" negative-text="继续填写"
      @positive-click="leaveWithoutDraft" @update:show="show=>{if(!show)leaveAction=undefined;}" />
    <NModal :show="!!recoveryRemoval" preset="dialog" type="warning" title="清除本机留存？" aria-label="清除本机留存？"
      content="请先确认历史巡检中的对应内容已核对。清除后无法从本机恢复这些修改。"
      positive-text="确认清除" negative-text="保留" @positive-click="clearRecovery"
      @update:show="show=>{if(!show)recoveryRemoval=undefined;}" />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NModal } from 'naive-ui';
import { ArrowLeft } from 'lucide-vue-next';
import './inspection.css';
import { ApiError, requestJson } from '../api/client';
import { registerNavigationGuard } from '../navigation';
import DailyInspectionWorkbook from './DailyInspectionWorkbook.vue';

defineProps<{embedded?:boolean;immersive?:boolean}>();
const emit = defineEmits<{close:[]}>();

type Value = string | number | boolean | null;
type Reading = {location:string; form:string; integrity:string; missing:string; note:string};
type Point = {id:string; point_id:string; name:string; sampled:number|null; integrity:string; location:string; path:string; note:string; template:string; sheet:string; cell:string; template_known:boolean;mapping_issue?:string};
type Parameter = {id:string; name:string; reference:Value; source_reference?:Value; value_type?:string; section?:number; point?:string; read_only?:boolean};
type Device = {id:string; name:string; scope?:string; profession:string; sections:Reading[]; points:Point[]; parameters:Parameter[];deferred_sections?:number[]};
type Batch = {batch:number; count:number; devices:Device[];form_devices?:Device[];cache_stale?:boolean;cache_refreshed_at?:number};
type Duty = {scope:string; date:string; shift:string};
type Review = {values:Record<string,string>; result:''|'正常'|'异常'; note:string; revision:number};
type Reviews = Record<string,Review>;
type DraftReview = Review & {base?:Review;pending?:Review;label?:string;parameterLabels?:Record<string,string>};
type Recovery = {key:string;duty:Duty;round:number;drafts:Record<string,DraftReview>};
type ProfessionConfirmation = {actor:string;confirmed:number;version:string};
type State = {template_version?:string;archive_revision?:string;duty:Duty|null; completed_rounds:number; archived_rounds?:number; round_limit:number; next_round?:number; scheduled?:boolean; opens_at?:number|null; history_scopes?:string[]; duty_warning?:string;
  view_scopes?:string[]; view_scope?:string; can_edit?:boolean;
  planned_sheet_groups?:Record<string,string>;
  active:null|{id:string;started_by?:string;started?:number;visited_sheets?:string[];presence?:{actor:string;sheet:string;seen_at:number}[];sheet_groups?:Record<string,string>;confirmation_invalidations?:Record<string,string>;worksheet_mode?:boolean;available_professions?:string[]; duty:Duty; round:number; pending:boolean; draft_key:string; reviews:Reviews; review_version:number; batch:Batch;
    professions?:string[]|null;profession_confirmations?:Record<string,ProfessionConfirmation>;profession_versions?:Record<string,string>;
    orders?:Record<string,string[]>};
  history:{id:string; duty:Duty; round:number; actor:string; completed:number; url:string;draft_key?:string}[]};

const api = '/api/daily-inspection';
const state = ref<State>();
const busy = ref('');
const error = ref('');
const message = ref('');
const inspectionProfessions = ['电气','暖通'];
const selectedProfessions = ref([...inspectionProfessions]);
const workbookPage=ref<InstanceType<typeof DailyInspectionWorkbook>>();
const pollError=ref('');
const drafts = ref<Record<string,DraftReview>>({});
const recoveries = ref<Recovery[]>([]);
const recoveryRemoval = ref<Recovery>();
const autoStatus = ref<'idle'|'waiting'|'saving'|'saved'|'error'>('idle');
const autoError = ref('');
const storageError = ref(false);
const leaveAction = shallowRef<() => void>();
const preview = ref<(Batch & {scope:string;orders?:Record<string,string[]>})>();
const previewBusy = ref(false);
const previewError = ref('');
const emptyReview:Review = {values:{},result:'',note:'',revision:0};

const active = computed(()=>state.value?.active);
const readOnly = computed(()=>state.value?.can_edit===false);
const duty = computed(()=>state.value?.duty);
const nextRound = computed(()=>state.value?.next_round ?? (state.value?.completed_rounds ?? 0)+1);
const canStartRound = computed(()=>nextRound.value<=(state.value?.round_limit ?? 2));
const archivedRounds = computed(()=>state.value?.archived_rounds ?? state.value?.completed_rounds ?? 0);
const waitingForSlot = computed(()=>(state.value?.opens_at ?? 0)>Date.now());
const batch = computed(()=>active.value?.batch);
const devices = computed(()=>batch.value?.devices || []);
const byId = computed(()=>new Map([...devices.value,...(batch.value?.form_devices || [])].map(device=>[device.id,device])));
const hasDrafts = computed(()=>Object.keys(drafts.value).length>0);
const orphanedDrafts = computed(()=>Object.keys(drafts.value).filter(id=>!byId.value.has(id)));
const autoSaving = computed(()=>autoStatus.value==='saving');
const conflicts = computed(()=>new Set(active.value?.pending || readOnly.value ? [] : Object.entries(drafts.value)
  .filter(([id,review])=>review.revision!==savedReview(id).revision).map(([id])=>id)));
const hasVisibleDevice=(device:Device)=>device.parameters.length>0 || device.sections.some((_,index)=>!device.deferred_sections?.includes(index)) || (!device.sections.length&&!device.points.length);
const professionName = (device:Device)=>device.profession?.trim() || '未分类';
const availableProfessions = computed(()=>{
  const counts=new Map<string,number>();
  for (const device of preview.value?.devices || []) {if(!hasVisibleDevice(device))continue;const name=professionName(device);counts.set(name,(counts.get(name) || 0)+1);}
  return inspectionProfessions.map(name=>({name,count:counts.get(name) ?? 0}));
});
const formatter = new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'medium',hour12:false});
const time = (value?:number|null)=>value ? formatter.format(value) : '—';
const dutyLabel = (value:Duty)=>value.scope+'楼 · '+value.date+' '+value.shift+'班';
const storageKey = (key:string)=>'clipflow:daily-inspection:'+key;
const savedReview = (id:string)=>active.value?.reviews?.[id] || emptyReview;
let controller:AbortController|undefined;
let statusController:AbortController|undefined;
let previewController:AbortController|undefined;
let previewTimer:number|undefined;
let previewScope='';
let previewChoiceScope='';
let autoController:AbortController|undefined;
let statusTimer:number|undefined;
let disposed=false;
let autoTimer:number|undefined;
let draftKey='';
function persistDrafts() {
  if (!draftKey) return;
  try {
    if (hasDrafts.value) localStorage.setItem(storageKey(draftKey),JSON.stringify(drafts.value));
    else localStorage.removeItem(storageKey(draftKey));
    storageError.value=false;
  } catch {storageError.value=true;}
}
function validReview(item?:Review) {
  return item && Number.isInteger(item.revision) && ['', '正常', '异常'].includes(item.result) &&
    typeof item.note==='string' && item.values && !Array.isArray(item.values) && typeof item.values==='object' &&
    Object.values(item.values).every(value=>typeof value==='string');
}
function restoreDrafts(key:string) {
  if (!key) return {};
  try {
    const saved=JSON.parse(localStorage.getItem(storageKey(key)) || '{}') as Record<string,DraftReview>;
    return Object.fromEntries(Object.entries(saved).filter(([,item])=>validReview(item))
      .map(([id,item])=>[id,{...item,base:validReview(item.base) ? item.base : undefined,
        pending:validReview(item.pending) ? item.pending : undefined}]));
  } catch {return {};}
}
function sameReview(left:Review,right:Review) {
  const values=Object.entries(left.values);
  return left.result===right.result && left.note.trim()===right.note &&
    values.length===Object.keys(right.values).length && values.every(([key,value])=>right.values[key]===value.trim());
}
function resolveConflict(id:string,keep:boolean) {
  const draft=drafts.value[id];
  if (!draft) return;
  if (!keep) {
    delete drafts.value[id];
  } else {
    const latest=savedReview(id), base=draft.base;
    if (!base) {
      draft.revision=latest.revision;
      draft.base=latest;
    } else {
      const values={...latest.values};
      for (const [key,value] of Object.entries(draft.values)) {
        if (!Object.prototype.hasOwnProperty.call(base.values,key) || value.trim()!==base.values[key].trim()) values[key]=value;
      }
      drafts.value[id]={
        ...latest, values, base:latest,
        label:draft.label,parameterLabels:draft.parameterLabels,
        result:draft.result===base.result ? latest.result : draft.result,
        note:draft.note.trim()===base.note.trim() ? latest.note : draft.note,
      };
    }
  }
  persistDrafts();
  if (!conflicts.value.size) {autoStatus.value='waiting';autoError.value='';}
  queueAutoSave();
}
function submissionReviews():Reviews {
  return Object.fromEntries(Object.entries(drafts.value).map(([id,{values,result,note,revision}])=>
    [id,{values,result,note,revision}]));
}
function refreshRecoveries(next:State) {
  recoveries.value=next.history.flatMap(row=>{
    if (!row.draft_key || row.id===next.active?.id) return [];
    const saved=restoreDrafts(row.draft_key);
    return Object.keys(saved).length ? [{key:row.draft_key,duty:row.duty,round:row.round,drafts:saved}] : [];
  });
}
function clearRecovery() {
  const target=recoveryRemoval.value;
  recoveryRemoval.value=undefined;
  if (!target) return;
  try {
    localStorage.removeItem(storageKey(target.key));
    recoveries.value=recoveries.value.filter(item=>item.key!==target.key);
  } catch {error.value='无法清除本机留存，请检查浏览器存储设置。';}
}
function queueAutoSave(delay=1400) {
  window.clearTimeout(autoTimer);
  if (disposed || !active.value || active.value.pending || readOnly.value || !hasDrafts.value) return;
  if (orphanedDrafts.value.length) {
    autoStatus.value='error';
    autoError.value='设备清单已变化，本机修改仍保留，请刷新后核对。';
    return;
  }
  if (conflicts.value.size) {
    autoStatus.value='error';
    autoError.value='共享数据已有修改，请先在对应设备处核对。';
    return;
  }
  if (autoController || busy.value) return;
  if (autoStatus.value!=='error') autoStatus.value='waiting';
  autoTimer=window.setTimeout(()=>void saveAutomatically(),delay);
}
function retryAutoSave() {
  autoStatus.value='waiting';
  autoError.value='';
  queueAutoSave(0);
}
async function saveAutomatically() {
  const current=active.value;
  if (disposed || !current || current.pending || readOnly.value || !hasDrafts.value || conflicts.value.size || orphanedDrafts.value.length || autoController || busy.value) return;
  const request=autoController=new AbortController();
  const scope=state.value?.view_scope;
  const suffix=scope ? '?'+new URLSearchParams({view_scope:scope}) : '';
  autoStatus.value='saving';
  autoError.value='';
  let retryable=true;
  try {
    if (Object.values(drafts.value).some(review=>review.pending)) {
      // Resolve a previously unconfirmed write before sending newer edits.
      const latest=await requestJson(api+suffix,{cache:'no-store',fresh:true,signal:request.signal,timeoutMs:30000}) as State;
      if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
      useState(latest);
      if (active.value?.id!==current.id || !hasDrafts.value) {autoStatus.value='saved';return;}
      if (conflicts.value.size) {
        autoStatus.value='error';
        autoError.value='共享数据已有修改，请先在对应设备处核对。';
        retryable=false;
        return;
      }
    }
    const submitted=JSON.parse(JSON.stringify(submissionReviews())) as Reviews;
    for (const [id,review] of Object.entries(submitted)) {
      if (drafts.value[id]) drafts.value[id].pending=review;
    }
    // Keep the exact in-flight revision so a restart can reconcile a lost response.
    persistDrafts();
    const next=await requestJson(api+'/draft'+suffix,{method:'POST',body:JSON.stringify({id:current.id,reviews:submitted}),
      signal:request.signal,timeoutMs:30000}) as State;
    if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
    if (next.active?.id!==current.id) throw new Error('本轮巡检已变化，请刷新核对。');
    useState(next);
    autoStatus.value=hasDrafts.value ? 'waiting' : 'saved';
  } catch (value) {
    if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
    autoStatus.value='error';
    autoError.value=value instanceof Error ? value.message : '自动保存未完成，本机内容已保留。';
    if (value instanceof ApiError && value.status>=400 && value.status<500 && ![408,429].includes(value.status)) retryable=false;
    if (value instanceof ApiError && value.status===409) {
      try {
        const latest=await requestJson(api+suffix,{cache:'no-store',fresh:true,signal:request.signal}) as State;
        if (!disposed && !request.signal.aborted && active.value?.id===current.id) useState(latest);
      } catch { /* The local draft remains available for the next retry. */ }
    }
  } finally {
    if (autoController===request) {
      autoController=undefined;
      if (!disposed && retryable && hasDrafts.value && !conflicts.value.size) queueAutoSave(autoStatus.value==='error'?15000:1400);
    }
  }
}
function useState(next:State,discardPrevious=false) {
  const nextKey=next.active?.draft_key || '';
  if (draftKey!==nextKey) {
    window.clearTimeout(autoTimer);
    if (autoController) {const previous=autoController;autoController=undefined;previous.abort();}
    if (discardPrevious) drafts.value={};
    persistDrafts();
    draftKey=nextKey;
    drafts.value=restoreDrafts(nextKey);
    autoStatus.value='idle';
    autoError.value='';
    previewChoiceScope='';
    selectedProfessions.value=[...inspectionProfessions];
  }
  state.value=next;
  for (const [id,review] of Object.entries(drafts.value)) {
    const saved=savedReview(id);
    if (review.pending && saved.revision>=review.pending.revision && sameReview(review.pending,saved)) {
      review.revision=saved.revision;
      review.base=saved;
      delete review.pending;
    }
    if (byId.value.has(id) && sameReview(review,savedReview(id))) delete drafts.value[id];
  }
  persistDrafts();
  refreshRecoveries(next);
  if (hasDrafts.value) queueAutoSave();
}
function acceptRoundState(next:State) {
  if(next.duty?.scope!==duty.value?.scope||next.duty?.date!==duty.value?.date||next.duty?.shift!==duty.value?.shift)return;
  statusController?.abort();
  useState({...state.value,...next});
}
async function fetchState(action:string, body?:object, selectedScope?:string) {
  if (busy.value) return false;
  statusController?.abort();
  const request=controller=new AbortController();
  const roundId=active.value?.id;
  busy.value=action;
  error.value='';
  message.value='';
  pollError.value='';
  try {
    const scope=selectedScope ?? state.value?.view_scope ?? '';
    const suffix=scope ? '?'+new URLSearchParams({view_scope:scope}) : '';
    const next=await requestJson(api+(body?'/'+action:'')+suffix,{
      ...(body?{method:'POST',body:JSON.stringify(body)}:{cache:'no-store',fresh:true}),
      signal:request.signal,timeoutMs:120000,
    }) as State;
    if (disposed || request.signal.aborted) return false;
    useState(next,action==='finish' || action==='confirm-profession' && !next.active);
    if (action==='finish') message.value='本轮巡检已归档。';
    if (action==='confirm-profession') message.value=next.active?.professions?.every(name=>next.active?.profession_confirmations?.[name]?.version===next.active?.profession_versions?.[name]) ? '所有专业已确认，可统一提交归档。' : '该专业已确认，等待其他已选专业。';
    if (action==='order') message.value='设备顺序已保存。';
    if (action==='draft') message.value='本轮草稿已共享给本楼当班人员。';
    return true;
  } catch (value) {
    if (disposed || request.signal.aborted) return false;
    error.value=value instanceof Error ? value.message : '巡检请求未完成。';
    if (value instanceof ApiError && [401,403].includes(value.status)) useState({duty:null,active:null,history:[],completed_rounds:0,round_limit:2});
    else if (action==='finish' || action==='confirm-profession' || (value instanceof ApiError && value.status===409)) {
      try {
        const viewScope=state.value?.view_scope;
        const latest=await requestJson(api+(viewScope ? '?'+new URLSearchParams({view_scope:viewScope}) : ''),
          {cache:'no-store',fresh:true,signal:request.signal}) as State;
        if (!disposed && !request.signal.aborted) {
          const archived=['finish','confirm-profession'].includes(action) && latest.history.some(row=>row.id===roundId);
          const submitted=body as {profession?:string;profession_version?:string}|undefined;
          const confirmed=action==='confirm-profession' && submitted?.profession && latest.active?.id===roundId &&
            latest.active?.profession_confirmations?.[submitted.profession]?.version===submitted.profession_version;
          useState(latest,archived);
          if (archived) {error.value='';message.value='本轮巡检已归档。';}
          else if (confirmed && latest.active?.pending) {error.value='所有专业已确认，但归档尚未完成，请重试归档。';return false;}
          else if (confirmed) {error.value='';message.value='该专业已确认，等待其他已选专业。';}
          if (archived || confirmed) return true;
        }
      } catch { /* Keep the original error and draft. */ }
    }
    return false;
  } finally {
    if (controller===request) {busy.value='';if (hasDrafts.value) queueAutoSave();}
  }
}
async function refreshSharedStatus() {
  const current=state.value;
  if (disposed || !current || (!current.duty && !current.history_scopes?.length)
      || document.visibilityState==='hidden' || busy.value || statusController || autoController) return;
  const request=statusController=new AbortController();
  const params=new URLSearchParams({date:current.duty?.date || '',shift:current.duty?.shift || ''});
  if(current.archive_revision)params.set('archive_revision',current.archive_revision);
  if (current.view_scope) params.set('view_scope',current.view_scope);
  if (current.active) {
    params.set('id',current.active.id);
    params.set('version',String(current.active.review_version));
    params.set('pending',current.active.pending?'1':'0');
  }
  try {
    const next=await requestJson(api+'/status?'+params,{cache:'no-store',fresh:true,signal:request.signal,timeoutMs:8000});
    if (disposed || request.signal.aborted || state.value!==current) return;
    pollError.value='';
    if (!next.unchanged) {
      useState(next as State);
    } else {
      state.value={...current,history_scopes:next.history_scopes??current.history_scopes,
        active:current.active ? {...current.active,visited_sheets:next.visited_sheets??current.active.visited_sheets,
          presence:next.presence??current.active.presence} : null};
    }
  } catch (value) {
    if (disposed || request.signal.aborted) return;
    if (value instanceof ApiError && [401,403].includes(value.status)) {
      useState({duty:null,active:null,history:[],completed_rounds:0,round_limit:2});
      error.value=value.message;
    } else pollError.value='共享状态暂未刷新，本机填写已保留。';
  } finally {
    if (statusController===request) statusController=undefined;
  }
}
async function refreshPreview() {
  const scope=state.value?.view_scope || duty.value?.scope;
  if (previewController && previewScope===scope && !active.value) return;
  previewController?.abort();
  if (!scope || active.value) {
    preview.value=undefined;
    previewBusy.value=false;
    previewError.value='';
    return;
  }
  const request=previewController=new AbortController();
  previewScope=scope;
  if (preview.value?.scope!==scope) preview.value=undefined;
  previewBusy.value=true;
  previewError.value='';
  try {
    const data=await requestJson(api+'/source?'+new URLSearchParams({view_scope:scope}),
      {cache:'no-store',fresh:true,signal:request.signal,timeoutMs:120000}) as Batch & {scope:string;orders?:Record<string,string[]>};
    if (!disposed && !request.signal.aborted && (state.value?.view_scope || duty.value?.scope)===scope && !active.value) {
      preview.value=data;
      if(previewChoiceScope!==scope) selectedProfessions.value=[...inspectionProfessions];
      previewChoiceScope=scope;
      window.clearTimeout(previewTimer);
      if (data.cache_stale) previewTimer=window.setTimeout(()=>void refreshPreview(),30000);
    }
  } catch (value) {
    if (!disposed && !request.signal.aborted) previewError.value=value instanceof Error ? value.message : '最新设备数据暂时无法读取。';
  } finally {
    if (previewController===request) {previewController=undefined;previewScope='';previewBusy.value=false;}
  }
}
watch([()=>state.value?.view_scope,()=>state.value?.duty?.scope,()=>state.value?.can_edit,()=>state.value?.active?.id],()=>void refreshPreview());
const load = async()=>{
  if (await fetchState('load') && !active.value) void refreshPreview();
};
function switchView(scope:string) {
  if (scope!==state.value?.view_scope) void fetchState('load',undefined,scope);
}
const start = ()=>fetchState('start',{duty:duty.value,professions:selectedProfessions.value,worksheet_mode:true});
function back() {
  if (workbookPage.value?.allowNavigation()!==false) emit('close');
}
function leaveWithoutDraft() {
  const proceed=leaveAction.value;
  leaveAction.value=undefined;
  proceed?.();
}
const unregister=registerNavigationGuard((_target,proceed)=>{
  if (busy.value && busy.value!=='load') {
    error.value='巡检操作正在处理，请稍后再离开。';
    return false;
  }
  if (hasDrafts.value) {
    persistDrafts();
    if (storageError.value) {
      leaveAction.value=proceed;
      return false;
    }
  }
  return true;
});
function beforeUnload(event:BeforeUnloadEvent) {
  if (hasDrafts.value) persistDrafts();
  if ((busy.value && busy.value!=='load') || (storageError.value && hasDrafts.value)) event.preventDefault();
}
onMounted(()=>{
  window.addEventListener('pagehide',persistDrafts);
  window.addEventListener('beforeunload',beforeUnload);
  document.addEventListener('visibilitychange',refreshSharedStatus);
  statusTimer=window.setInterval(refreshSharedStatus,8000);
  void load();
});
onBeforeUnmount(()=>{
  disposed=true;
  unregister();
  controller?.abort();
  statusController?.abort();
  previewController?.abort();
  window.clearTimeout(previewTimer);
  autoController?.abort();
  window.clearTimeout(autoTimer);
  window.clearInterval(statusTimer);
  document.removeEventListener('visibilitychange',refreshSharedStatus);
  persistDrafts();
  window.removeEventListener('pagehide',persistDrafts);
  window.removeEventListener('beforeunload',beforeUnload);
});
</script>

<style scoped>
.inspection-page{display:grid;gap:10px;max-width:1500px;margin:auto;padding:10px 12px;min-width:0;background:var(--inspection-surface);color:var(--inspection-ink)}
.inspection-heading,.inspection-start{display:flex;align-items:center;flex-wrap:wrap;gap:6px 12px}
.inspection-heading{padding-bottom:8px;border-bottom:1px solid var(--inspection-line)}
.inspection-title{min-width:136px;flex:1}
h1,p{margin:0;overflow-wrap:anywhere}
h1{font-size:20px}p,small{color:var(--inspection-muted);font-size:12px}
.inspection-start{padding:6px 8px;border:1px solid var(--inspection-line)}
.inspection-start label{white-space:nowrap}
.inspection-start input[type=checkbox]{accent-color:var(--inspection-accent)}
.inspection-recovery{padding:7px;border:1px solid var(--cf-warning);font-size:13px}
.inspection-recovery summary{cursor:pointer;color:var(--cf-warning);font-weight:600}
.inspection-recovery-item{padding:6px 0;border-top:1px solid var(--inspection-line);overflow-wrap:anywhere}
.warning{color:var(--cf-warning)}
@media(max-width:760px),(hover:none) and (max-width:1024px) and (max-height:500px){
  .inspection-page.is-inspection-immersive{padding:0 10px max(12px,env(safe-area-inset-bottom));gap:8px;max-width:none;animation:none!important;transform:none!important}
  .is-inspection-immersive .inspection-heading{display:none}
}
</style>
