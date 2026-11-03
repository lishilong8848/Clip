<template>
  <section class="inspection-page" aria-label="日常巡检" data-v2-inline-feedback>
    <header class="inspection-heading">
      <NButton quaternary aria-label="返回" @click="back"><ArrowLeft :size="18" /></NButton>
      <div><h1>日常巡检</h1><p>{{ duty ? dutyLabel(duty) : '设备参数与测点读数' }}</p></div>
      <NButton v-if="state?.history_scopes?.length" size="small" :disabled="!!busy"
        @click="historyOpen=!historyOpen">{{ historyOpen ? '返回本班' : '历史巡检' }}</NButton>
      <NButton v-if="!historyOpen" size="small" :disabled="!!busy" :loading="busy==='load'" @click="load">刷新状态</NButton>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" role="status">{{ message }}</NAlert>
    <p v-if="storageError && active && !active.pending" class="warning" role="status">浏览器暂存不可用，离开前可保存本轮草稿。</p>
    <p v-if="!state && busy">正在读取巡检状态…</p>
    <DailyInspectionHistory v-show="historyOpen" v-if="historyVisited && state?.history_scopes?.length"
      :scopes="state.history_scopes" :default-scope="duty?.scope" />
    <template v-if="state && !historyOpen">
      <p v-if="!duty">{{ state.duty_warning || '当前排班没有可巡检的 D 楼或 E 楼。如排班有变更，请刷新状态。' }}</p>
      <section v-else-if="!active" class="inspection-summary">
        <div class="inspection-actions">
          <h2>{{ duty.scope }}楼本班巡检</h2>
          <span>已完成 {{ state.completed_rounds }} / {{ state.round_limit }} 次</span>
          <NButton v-if="state.completed_rounds<state.round_limit" type="primary" :loading="busy==='start'"
            :disabled="!!busy" @click="start">开始第 {{ state.completed_rounds+1 }} 次巡检</NButton>
        </div>
        <p>{{ state.completed_rounds===state.round_limit ? '本楼本班巡检已全部完成。' : '本楼当班人员共用进度，开始后固定本轮参考数据。' }}</p>
      </section>
      <template v-else>
        <section class="inspection-summary">
          <div class="inspection-actions">
            <strong>{{ active.duty.scope }}楼 · {{ active.duty.shift }}班第 {{ active.round }} / {{ state.round_limit }} 次</strong>
            <span v-if="active.pending" class="warning">待归档</span>
            <span v-else>已填齐 {{ completedDevices }} / {{ devices.length }} 台</span>
            <span v-if="abnormalCount" class="warning">异常 {{ abnormalCount }} 台</span>
            <button type="button" :disabled="!!busy || active.pending" @click="openOrder">批量排序</button>
          </div>
          <p>参考批次 {{ time(batch?.batch) }} · {{ batch?.count }} 条源数据</p>
          <p v-if="active.duty.date!==duty.date || active.duty.shift!==duty.shift" class="warning">
            此轮始于 {{ dutyLabel(active.duty) }}，归档计入原班次。
          </p>
          <div class="inspection-filters">
            <input v-model="search" type="search" placeholder="搜索设备、位置或测点" aria-label="搜索设备">
            <select v-model="profession" aria-label="专业筛选" data-native-select>
              <option value="">全部专业</option><option v-for="item in professions" :key="item">{{ item }}</option>
            </select>
            <label><input v-model="onlyMissing" type="checkbox"> 只看待填写</label>
          </div>
          <p v-if="pollError" class="warning" role="status">{{ pollError }}</p>
          <p v-else-if="hasDrafts && !active.pending">本机修改尚未共享，可保存本轮草稿。</p>
        </section>
        <p v-if="active.pending" class="warning" role="status">本轮数据已锁定，可重试同一轮归档。</p>
        <section v-if="filtered.length" class="inspection-sheet" aria-label="设备参数核对">
          <div class="inspection-grid inspection-columns inspection-shared-head" aria-hidden="true">
            <span>参数 / 单位</span><span>参考值</span><span>现场读数</span><span>差值</span>
          </div>
          <div class="inspection-devices">
            <article v-for="item in filtered" :id="deviceAnchor(item.id)" :key="item.id" class="inspection-device"
              @focusin="focusedDevice=item.id" @focusout="leaveDevice(item.id,$event)">
              <header class="inspection-device-heading">
                <h2 tabindex="-1"><span class="sequence">{{ positions.get(item.id) }}</span>{{ item.name }}</h2>
                <span v-if="conflicts.has(item.id)" class="warning">待核对共享修改</span>
                <span v-else-if="!missingByDevice.get(item.id)" :class="reviewFor(item.id).result==='异常'?'warning':'done'">
                  {{ reviewFor(item.id).result==='异常' ? '异常' : '已填齐' }}
                </span>
                <span v-else class="warning">{{ missingByDevice.get(item.id) }}</span>
              </header>
              <p>{{ [item.profession,item.sections[0]?.location || item.points[0]?.location].filter(Boolean).join(' · ') }}</p>
              <div class="inspection-grid inspection-columns inspection-device-columns" aria-hidden="true">
                <span>参数 / 单位</span><span>参考值</span><span>现场读数</span><span>差值</span>
              </div>
              <p v-if="!item.parameters.length" class="muted">无可填参数，请填写现场结论。</p>
              <div v-for="row in item.parameters" :key="row.id" class="inspection-grid inspection-reading">
                <div>
                  <strong>{{ row.name }}</strong>
                  <small v-if="parameterLocation(item,row)">{{ parameterLocation(item,row) }}</small>
                </div>
                <span class="numeric">{{ shownValue(row.reference) }}</span>
                <span v-if="row.read_only" class="muted numeric">已停用</span>
                <input v-else :value="reviewFor(item.id).values[row.id] ?? ''" :disabled="inputsLocked"
                  :inputmode="numericValue(row.reference)===null ? 'text' : 'decimal'" maxlength="160"
                  class="numeric" :class="{'missing-reading':!reviewFor(item.id).values[row.id]?.trim()}"
                  placeholder="未填" :aria-label="row.name+'现场读数'"
                  @input="setReading(item.id,row.id,$event)">
                <span class="numeric muted">{{ difference(row.reference,reviewFor(item.id).values[row.id]) }}</span>
              </div>
              <details v-if="item.points.length || item.sections.some(row=>row.integrity || row.missing || row.note)" class="inspection-source">
                <summary>来源明细</summary>
                <p v-for="(row,index) in item.sections" v-show="row.integrity || row.missing || row.note" :key="index">
                  {{ [row.form || row.location,row.integrity,row.missing && '缺少 '+row.missing,row.note].filter(Boolean).join(' · ') }}
                </p>
                <p v-for="point in item.points" :key="point.id">
                  {{ [point.name,time(point.sampled),point.path,point.template,point.sheet,point.cell,
                    point.integrity,point.note,point.template && !point.template_known && '未找到模板附件'].filter(Boolean).join(' · ') }}
                </p>
              </details>
              <div class="inspection-actions inspection-entry">
                <select :value="reviewFor(item.id).result" :disabled="inputsLocked" aria-label="现场结论" data-native-select @change="setResult(item.id,$event)">
                  <option value="">选择结论</option><option value="正常">正常</option><option value="异常">异常</option>
                </select>
                <button type="button" :aria-expanded="showNote(item.id)" :aria-controls="deviceAnchor(item.id)+'-note'" @click="toggleNote(item.id)">备注</button>
                <textarea v-show="showNote(item.id)" :id="deviceAnchor(item.id)+'-note'" :value="reviewFor(item.id).note"
                  :disabled="inputsLocked" rows="2" maxlength="1000" aria-label="现场备注" placeholder="异常时填写现场情况"
                  @input="setNote(item.id,$event)" />
              </div>
              <div v-if="!active.pending && conflicts.has(item.id)" class="inspection-actions warning">
                <span>本台在其他页面有新填写，请选择要保留的版本。</span>
                <button type="button" :disabled="inputsLocked" @click="resolveConflict(item.id,false)">使用共享填写</button>
                <button type="button" :disabled="inputsLocked" @click="resolveConflict(item.id,true)">
                  {{drafts[item.id]?.base ? '合并我的改动' : '用本机旧草稿覆盖本台'}}
                </button>
              </div>
              <p v-else-if="blockedDevice===item.id && missingByDevice.get(item.id)" class="warning">
                {{ missingByDevice.get(item.id) }}
              </p>
            </article>
          </div>
        </section>
        <p v-else>没有匹配的设备。</p>
        <footer class="inspection-actions inspection-footer">
          <p v-if="error" class="warning">{{ error }}</p>
          <a :href="api+'/xlsx?id='+active.id" download>下载 DE 巡检表</a>
          <template v-if="!active.pending">
            <NButton size="small" :disabled="!!busy" @click="promptConfirmation('cancel')">取消本轮</NButton>
            <NButton size="small" :disabled="!!busy || !hasDrafts" :loading="busy==='draft'" @click="saveDraft">保存本轮草稿</NButton>
          </template>
          <NButton type="primary" :loading="busy==='finish'" :disabled="!!busy" @click="confirmFinish">
            {{ active.pending ? '重试归档' : '确认本轮并归档' }}
          </NButton>
        </footer>
      </template>
      <section v-if="state.history.length && !active" class="inspection-summary">
        <h2>最近归档</h2>
        <a v-for="row in state.history" :key="row.id" class="inspection-history" :href="row.url" target="_blank" rel="noopener noreferrer">
          <span>{{ dutyLabel(row.duty) }}第 {{ row.round }} 次 · {{ row.actor }} · {{ time(row.completed) }}</span><ChevronRight :size="17" />
        </a>
      </section>
    </template>
    <NModal v-model:show="orderOpen" preset="card" title="批量调整设备顺序" class="inspection-order">
      <p>顺序按账号和楼栋保存。可导入同楼人员分享的排序文件。</p>
      <div class="inspection-actions order-toolbar">
        <label>移动到第 <input v-model.number="position" type="number" inputmode="numeric" min="1" :max="lastPosition" :disabled="!!busy" aria-label="设备目标位置"> 位</label>
        <button type="button" :disabled="!!busy || !selected.length || !validPosition" @click="moveOrder">移动</button>
        <span>{{ selected.length }} 台已选</span>
      </div>
      <div class="inspection-actions order-toolbar">
        <input ref="orderFile" type="file" accept=".json,application/json" hidden @change="importOrder">
        <button type="button" :disabled="!!busy" @click="orderFile?.click()">导入顺序</button>
        <button type="button" :disabled="!order.length || !!busy" @click="exportOrder">导出顺序</button>
      </div>
      <p v-if="orderFeedback" :class="{warning:orderImportError}" role="status">{{ orderFeedback }}</p>
      <div class="order-list">
        <label v-for="(item,index) in orderedDevices" :key="item.id">
          <input v-model="selected" type="checkbox" :value="item.id" :disabled="!!busy"><span>{{ index+1 }}. {{ item.name }} <small>{{ item.profession }}</small></span>
        </label>
      </div>
      <template #footer>
        <div class="inspection-actions">
          <span v-if="busy==='order'" role="status">正在保存顺序</span>
          <span v-else-if="error" class="warning" role="alert">{{ error }}</span>
          <NButton :disabled="!!busy" @click="orderOpen=false">取消</NButton>
          <NButton type="primary" :loading="busy==='order'" :disabled="!!busy" @click="saveOrder">保存顺序</NButton>
        </div>
      </template>
    </NModal>
    <NModal :show="!!confirmation" preset="dialog" type="warning"
      :title="confirmation==='finish' ? '确认整轮巡检？' : '取消本轮巡检？'"
      :content="confirmation==='finish' ? '将把固定的参考数据、现场读数和核对结论归档到飞书「巡检记录」。' : '取消会清除本轮共享草稿与填写记录。'"
      :positive-text="confirmation==='finish' ? '确认并归档' : '取消本轮'" negative-text="返回填写"
      @positive-click="applyConfirmation" @update:show="show=>{if(!show)confirmation='';}" />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NModal } from 'naive-ui';
import { ArrowLeft, ChevronRight } from 'lucide-vue-next';
import { ApiError, requestJson } from '../api/client';
import { navigateBack } from '../navigation';
import DailyInspectionHistory from './DailyInspectionHistory.vue';

type Value = string | number | boolean | null;
type Reading = {location:string; form:string; integrity:string; missing:string; note:string};
type Point = {id:string; name:string; sampled:number|null; integrity:string; location:string; path:string; note:string; template:string; sheet:string; cell:string; template_known:boolean};
type Parameter = {id:string; name:string; reference:Value; section?:number; point?:string; read_only?:boolean};
type Device = {id:string; name:string; profession:string; sections:Reading[]; points:Point[]; parameters:Parameter[]};
type Batch = {batch:number; count:number; devices:Device[]};
type Duty = {scope:string; date:string; shift:string};
type Review = {values:Record<string,string>; result:''|'正常'|'异常'; note:string; revision:number};
type Reviews = Record<string,Review>;
type DraftReview = Review & {base?:Review};
type State = {duty:Duty|null; completed_rounds:number; round_limit:number; history_scopes?:string[]; duty_warning?:string;
  active:null|{id:string; duty:Duty; round:number; pending:boolean; draft_key:string; reviews:Reviews; review_version:number; batch:Batch};
  history:{id:string; duty:Duty; round:number; actor:string; completed:number; url:string}[]};

const api = '/api/daily-inspection';
const state = ref<State>();
const busy = ref('');
const error = ref('');
const message = ref('');
const search = ref('');
const profession = ref('');
const onlyMissing = ref(false);
const focusedDevice = ref('');
const historyOpen = ref(false);
const historyVisited = ref(false);
const pollError = ref('');
watch(historyOpen,open=>{if(open)historyVisited.value=true;});
const orderOpen = ref(false);
const order = ref<string[]>([]);
const orderFile = ref<HTMLInputElement>();
const orderFeedback = ref('');
const orderImportError = ref(false);
const orderFormat = 'clipflow-daily-inspection-order-v1';
const selected = ref<string[]>([]);
const position = ref<number|string>(1);
const confirmation = ref<''|'finish'|'cancel'>('');
const blockedDevice = ref('');
const drafts = ref<Record<string,DraftReview>>({});
const notes = ref<Record<string,boolean>>({});
const storageError = ref(false);
const emptyReview:Review = {values:{},result:'',note:'',revision:0};

const active = computed(()=>state.value?.active);
const duty = computed(()=>state.value?.duty);
const batch = computed(()=>active.value?.batch);
const devices = computed(()=>batch.value?.devices || []);
const byId = computed(()=>new Map(devices.value.map(device=>[device.id,device])));
const positions = computed(()=>new Map(devices.value.map((device,index)=>[device.id,index+1])));
const inputsLocked = computed(()=>!!busy.value || !!active.value?.pending);
const hasDrafts = computed(()=>Object.keys(drafts.value).length>0);
const conflicts = computed(()=>new Set(active.value?.pending ? [] : Object.entries(drafts.value)
  .filter(([id,review])=>review.revision!==savedReview(id).revision).map(([id])=>id)));
const missingByDevice = computed(()=>new Map(devices.value.map(item=>{
  const review=reviewFor(item.id);
  const missing=item.parameters.filter(row=>!row.read_only && !review.values[row.id]?.trim()).length;
  const issue=missing ? '未填 '+missing+' 项读数' : !review.result ? '待选结论'
    : review.result==='异常' && !review.note.trim() ? '请填写异常备注' : '';
  return [item.id,issue] as const;
})));
const completedDevices = computed(()=>devices.value.filter(item=>!missingByDevice.value.get(item.id)).length);
const abnormalCount = computed(()=>devices.value.filter(item=>reviewFor(item.id).result==='异常').length);
const professions = computed(()=>[...new Set(devices.value.map(item=>item.profession).filter(Boolean))]);
const filtered = computed(()=>{
  const q=search.value.trim().toLowerCase();
  return devices.value.filter(item=>(!profession.value || item.profession===profession.value) &&
    (!onlyMissing.value || focusedDevice.value===item.id || !!missingByDevice.value.get(item.id) || conflicts.value.has(item.id)) &&
    (!q || [item.name,item.profession,...item.sections.map(row=>row.location),...item.points.flatMap(row=>[row.name,row.path])]
      .some(value=>value.toLowerCase().includes(q))));
});
const orderedDevices = computed(()=>order.value.map(id=>byId.value.get(id)).filter((item):item is Device=>!!item));
const lastPosition = computed(()=>order.value.length-selected.value.length+1);
const validPosition = computed(()=>Number.isInteger(position.value) && Number(position.value)>=1 && Number(position.value)<=lastPosition.value);
const formatter = new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'medium',hour12:false});
const time = (value?:number|null)=>value ? formatter.format(value) : '—';
const dutyLabel = (value:Duty)=>value.scope+'楼 · '+value.date+' '+value.shift+'班';
const shownValue = (value:Value)=>value==null || value==='' ? '—' : typeof value==='boolean' ? (value?'是':'否') : value;
const deviceAnchor = (id:string)=>'inspection-'+encodeURIComponent(id);
const storageKey = (key:string)=>'clipflow:daily-inspection:'+key;
const savedReview = (id:string)=>active.value?.reviews?.[id] || emptyReview;
const reviewFor = (id:string)=>(!active.value?.pending && drafts.value[id]) || savedReview(id);
const showNote = (id:string)=>notes.value[id] ?? (reviewFor(id).result==='异常' || !!reviewFor(id).note);

let controller:AbortController|undefined;
let statusController:AbortController|undefined;
let statusTimer:number|undefined;
let confirmationTarget:{id:string;version:number}|undefined;
let disposed=false;
let saveTimer:number|undefined;
let draftKey='';
let orderImportRevision=0;

function numericValue(value:Value|undefined) {
  if (typeof value==='number') return Number.isFinite(value) ? value : null;
  return typeof value==='string' && /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value.trim()) && Number.isFinite(Number(value)) ? Number(value) : null;
}
function leaveDevice(id:string,event:FocusEvent) {
  if (focusedDevice.value===id && !(event.currentTarget as HTMLElement).contains(event.relatedTarget as Node | null)) {
    focusedDevice.value='';
  }
}
function difference(reference:Value,reading?:string) {
  const left=numericValue(reading), right=numericValue(reference);
  if (left===null || right===null) return '—';
  const value=Number((left-right).toPrecision(10));
  return (value>0?'+':'')+value;
}
function parameterLocation(device:Device,row:Parameter) {
  if (row.section!==undefined) return device.sections.length>1 ? device.sections[row.section]?.form || device.sections[row.section]?.location : '';
  return '测点';
}
function persistDrafts() {
  window.clearTimeout(saveTimer);
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
      .map(([id,item])=>[id,{...item,base:validReview(item.base) ? item.base : undefined}]));
  } catch {return {};}
}
function sameReview(left:Review,right:Review) {
  const values=Object.entries(left.values).filter(([,value])=>value.trim());
  return left.result===right.result && left.note.trim()===right.note &&
    values.length===Object.keys(right.values).length && values.every(([key,value])=>right.values[key]===value.trim());
}
function edit(id:string) {
  if (!drafts.value[id]) {
    const base=savedReview(id);
    drafts.value[id]={...base,values:{...base.values},base};
  }
  window.clearTimeout(saveTimer);
  saveTimer=window.setTimeout(persistDrafts,250);
  return drafts.value[id];
}
function setReading(id:string,parameter:string,event:Event) {edit(id).values[parameter]=(event.target as HTMLInputElement).value;}
function setResult(id:string,event:Event) {
  const review=edit(id);
  review.result=(event.target as HTMLSelectElement).value as Review['result'];
  if (review.result==='异常') notes.value[id]=true;
}
function setNote(id:string,event:Event) {edit(id).note=(event.target as HTMLTextAreaElement).value;}
function toggleNote(id:string) {notes.value[id]=!showNote(id);}
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
        if (value.trim()!==(base.values[key] || '').trim()) values[key]=value;
      }
      drafts.value[id]={
        ...latest, values, base:latest,
        result:draft.result===base.result ? latest.result : draft.result,
        note:draft.note.trim()===base.note.trim() ? latest.note : draft.note,
      };
    }
  }
  persistDrafts();
}
function submissionReviews():Reviews {
  return Object.fromEntries(Object.entries(drafts.value).map(([id,{values,result,note,revision}])=>
    [id,{values,result,note,revision}]));
}
function useState(next:State,discardPrevious=false) {
  const nextKey=next.active?.draft_key || '';
  if (draftKey!==nextKey) {
    if (discardPrevious || next.history.some(row=>row.id===active.value?.id)) drafts.value={};
    persistDrafts();
    draftKey=nextKey;
    drafts.value=restoreDrafts(nextKey);
    notes.value={};
    search.value='';
    profession.value='';
    onlyMissing.value=false;
    focusedDevice.value='';
    orderOpen.value=false;
    confirmation.value='';
    blockedDevice.value='';
  }
  state.value=next;
  if (!next.history_scopes?.length) historyOpen.value=false;
  for (const [id,review] of Object.entries(drafts.value)) {
    if (!byId.value.has(id) || sameReview(review,savedReview(id))) delete drafts.value[id];
  }
  persistDrafts();
}
async function fetchState(action:string, body?:object) {
  if (busy.value) return false;
  statusController?.abort();
  const request=controller=new AbortController();
  const roundId=active.value?.id;
  busy.value=action;
  error.value='';
  message.value='';
  pollError.value='';
  try {
    const next=await requestJson(api+(body?'/'+action:''),{
      ...(body?{method:'POST',body:JSON.stringify(body)}:{cache:'no-store',fresh:true}),
      signal:request.signal,timeoutMs:120000,
    }) as State;
    if (disposed || request.signal.aborted) return false;
    useState(next,action==='finish' || action==='cancel');
    if (action==='finish') message.value='本轮巡检已归档。';
    if (action==='order') message.value='设备顺序已保存。';
    if (action==='draft') message.value='本轮草稿已共享给本楼当班人员。';
    return true;
  } catch (value) {
    if (disposed || request.signal.aborted) return false;
    error.value=value instanceof Error ? value.message : '巡检请求未完成。';
    if (value instanceof ApiError && [401,403].includes(value.status)) useState({duty:null,active:null,history:[],completed_rounds:0,round_limit:2});
    else if (action==='finish' || (value instanceof ApiError && value.status===409)) {
      try {
        const latest=await requestJson(api,{cache:'no-store',fresh:true,signal:request.signal}) as State;
        if (!disposed && !request.signal.aborted) {
          const archived=action==='finish' && latest.history.some(row=>row.id===roundId);
          useState(latest,archived);
          if (archived) {error.value='';message.value='本轮巡检已归档。';}
        }
      } catch { /* Keep the original error and draft. */ }
    }
    return false;
  } finally {
    if (controller===request) busy.value='';
  }
}
async function refreshSharedStatus() {
  const current=state.value;
  if (disposed || !current || (!current.duty && !current.history_scopes?.length)
      || document.visibilityState==='hidden' || busy.value || statusController) return;
  const request=statusController=new AbortController();
  const params=new URLSearchParams({date:current.duty?.date || '',shift:current.duty?.shift || ''});
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
    } else if (Array.isArray(next.history_scopes) && next.history_scopes.join()!==current.history_scopes?.join()) {
      useState({...current,history_scopes:next.history_scopes});
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
const load = ()=>fetchState('load');
const start = ()=>fetchState('start',{duty:duty.value});
function saveDraft() {
  if (active.value) void fetchState('draft',{id:active.value.id,reviews:submissionReviews()});
}
function confirmFinish() {
  if (!active.value) return;
  if (active.value.pending) {void fetchState('finish',{id:active.value.id});return;}
  const missing=devices.value.find(item=>conflicts.value.has(item.id) || missingByDevice.value.get(item.id));
  if (missing) {
    blockedDevice.value=missing.id;
    const first=missing.parameters.find(row=>!row.read_only && !reviewFor(missing.id).values[row.id]?.trim());
    error.value=missing.name+' · '+(conflicts.value.has(missing.id)?'请先核对共享修改':
      first ? '缺少 '+first.name+' 的现场读数' : missingByDevice.value.get(missing.id));
    search.value='';
    profession.value='';
    onlyMissing.value=false;
    void nextTick(()=>{
      const element=document.getElementById(deviceAnchor(missing.id));
      element?.scrollIntoView({block:'start'});
      element?.querySelector('h2')?.focus({preventScroll:true});
    });
    return;
  }
  blockedDevice.value='';
  promptConfirmation('finish');
}
function promptConfirmation(action:'finish'|'cancel') {
  const current=active.value;
  if (!current) return;
  confirmationTarget={id:current.id,version:current.review_version};
  confirmation.value=action;
}
function applyConfirmation() {
  const action=confirmation.value;
  const current=active.value;
  const target=confirmationTarget;
  confirmation.value='';
  confirmationTarget=undefined;
  if (!current || !action || !target) return;
  if (current.id!==target.id || current.review_version!==target.version || current.pending) {
    error.value='本轮巡检已更新，请核对最新状态后再确认。';
    return;
  }
  const body={id:target.id,review_version:target.version,...(action==='finish' ? {reviews:submissionReviews()} : {})};
  void fetchState(action,body);
}
function openOrder() {
  orderImportRevision++;
  order.value=devices.value.map(item=>item.id);
  selected.value=[];
  position.value=1;
  orderFeedback.value='';
  orderImportError.value=false;
  error.value='';
  orderOpen.value=true;
}
function moveOrder() {
  if (!validPosition.value || !selected.value.length) return;
  const chosen=new Set(selected.value);
  const moving=order.value.filter(id=>chosen.has(id));
  const rest=order.value.filter(id=>!chosen.has(id));
  rest.splice(Number(position.value)-1,0,...moving);
  order.value=rest;
}
function exportOrder() {
  const scope=active.value?.duty.scope;
  if (!scope || !order.value.length) return;
  const blob=new Blob([JSON.stringify({format:orderFormat,scope,order:order.value},null,2)],{type:'application/json'});
  const link=document.createElement('a');
  link.href=URL.createObjectURL(blob);
  link.download=scope+'楼日常巡检设备顺序.json';
  link.click();
  window.setTimeout(()=>URL.revokeObjectURL(link.href),1000);
}
async function importOrder(event:Event) {
  const input=event.target as HTMLInputElement;
  const file=input.files?.[0];
  input.value='';
  const roundId=active.value?.id;
  const scope=active.value?.duty.scope;
  if (!file || !roundId || !scope || busy.value) return;
  const revision=++orderImportRevision;
  const current=()=>!disposed && orderOpen.value && active.value?.id===roundId && revision===orderImportRevision && !busy.value;
  orderFeedback.value='';
  orderImportError.value=false;
  try {
    if (file.size>1024*1024) throw new Error('排序文件超过 1 MB。');
    const data=JSON.parse(await file.text()) as {format?:unknown;scope?:unknown;order?:unknown}|null;
    if (!current()) return;
    if (data?.format!==orderFormat || !Array.isArray(data.order) || !data.order.every(id=>typeof id==='string') ||
        new Set(data.order).size!==data.order.length) throw new Error('请选择有效的日常巡检排序文件。');
    if (data.scope!==scope) throw new Error('排序文件不属于本楼，请选择 '+scope+' 楼的文件。');
    const matching=data.order.filter(id=>byId.value.has(id));
    if (!matching.length) throw new Error('文件中的设备与本楼当前设备没有匹配项。');
    const imported=new Set(matching);
    const remaining=order.value.filter(id=>!imported.has(id));
    order.value=[...matching,...remaining];
    selected.value=[];
    position.value=1;
    orderFeedback.value='已预览 '+matching.length+' 台；'+remaining.length+' 台当前设备保留在末尾。点击保存顺序后生效。';
  } catch (value) {
    if (!current()) return;
    orderImportError.value=true;
    orderFeedback.value=value instanceof SyntaxError ? '排序文件无法读取，请使用系统导出的 JSON 文件。'
      : value instanceof Error ? value.message : '导入顺序未完成。';
  }
}
async function saveOrder() {
  if (active.value && await fetchState('order',{id:active.value.id,order:order.value})) orderOpen.value=false;
}
function back() {navigateBack(document.querySelector('.v2-shell') ? '/?view=functions' : '/?entry=tools');}
onMounted(()=>{
  window.addEventListener('pagehide',persistDrafts);
  document.addEventListener('visibilitychange',refreshSharedStatus);
  statusTimer=window.setInterval(refreshSharedStatus,8000);
  void load();
});
onBeforeUnmount(()=>{
  disposed=true;
  controller?.abort();
  statusController?.abort();
  window.clearInterval(statusTimer);
  document.removeEventListener('visibilitychange',refreshSharedStatus);
  persistDrafts();
  window.removeEventListener('pagehide',persistDrafts);
});
</script>

<style scoped>
.inspection-page{--line:hsl(211 36% 88%);--ink:hsl(215 56% 18%);--muted:hsl(215 22% 40%);display:grid;gap:8px;max-width:1500px;margin:auto;padding:10px 12px;min-width:0;background:hsl(0 0% 100%);color:var(--ink);color-scheme:light}
.inspection-heading,.inspection-actions{display:flex;align-items:center;flex-wrap:wrap;gap:6px 10px}
.inspection-heading{flex-wrap:nowrap;padding-bottom:6px;border-bottom:1px solid var(--line)}
.inspection-heading>div{min-width:0;flex:1}
h1,h2,p{margin:0;overflow-wrap:anywhere}
h1{font-size:20px}h2{font-size:16px}p,small{color:var(--muted);font-size:12px}
.inspection-summary{display:grid;gap:4px}
.inspection-page input,.inspection-page select,.inspection-page textarea,.inspection-page button:not(.n-button),.order-toolbar input,.order-toolbar button{min-width:0;min-height:36px;padding:3px 7px;border:1px solid hsl(213 29% 75%);border-radius:0;color:var(--ink,hsl(215 56% 18%));background:hsl(0 0% 100%);font:inherit}
.inspection-page :is(input,select,textarea):disabled,.inspection-page button:not(.n-button):disabled,.order-toolbar button:disabled{opacity:.62}
.inspection-page :deep(.n-button){border-radius:0}
.inspection-filters{display:grid;grid-template-columns:minmax(0,1fr) minmax(90px,140px);gap:6px}
.inspection-filters label{display:flex;align-items:center;gap:5px;font-size:13px}
.inspection-filters label input{min-height:0;margin:0}
.inspection-devices{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-items:start;gap:10px 16px}
.inspection-device{min-width:0;border-top:1px solid var(--line);padding:6px 0;scroll-margin-top:30px}
.inspection-device-heading{display:flex;align-items:baseline;justify-content:space-between;gap:5px;font-size:12px}
.sequence{margin-right:7px;color:var(--muted);font-size:12px}
.inspection-grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(0,.76fr) minmax(0,1fr) minmax(0,.72fr);align-items:center;gap:5px;padding-inline:3px}
.inspection-columns{font-size:12px;color:var(--muted);background:hsl(214 40% 97%);padding-block:3px}
.inspection-columns span:not(:first-child),.numeric{text-align:right;font-variant-numeric:tabular-nums}
.inspection-shared-head{display:none}
.inspection-reading{min-height:38px;border-bottom:1px solid var(--line);font-size:14px}
.inspection-reading>*{min-width:0;overflow-wrap:anywhere}
.inspection-reading strong{font-size:13px}
.inspection-reading small{display:block}
.inspection-reading input{width:100%;min-height:32px}
.inspection-reading input.missing-reading{border-color:hsl(35 66% 65%);background:hsl(40 90% 97%)}
.inspection-source{padding-top:3px;color:var(--muted);font-size:12px}
.inspection-source summary{cursor:pointer}
.inspection-entry{padding:4px 0}
.inspection-entry textarea{width:100%;resize:vertical}
.inspection-footer{padding:7px 0;border-top:1px solid var(--line)}
.inspection-footer p{flex-basis:100%}
.inspection-history{display:flex;justify-content:space-between;align-items:center;gap:5px;padding:6px 0;border-bottom:1px solid var(--line)}
.inspection-order{width:min(510px,94vw)}
.order-list{max-height:48dvh;overflow:auto}
.order-list label{display:flex;align-items:center;gap:7px;min-height:36px;border-bottom:1px solid #d8e2ed;cursor:pointer}
.order-list input{width:18px;height:18px;accent-color:#1356be}
.order-toolbar{padding:5px 0}
.order-toolbar input{width:58px}
.muted{color:var(--muted)}.warning{color:#8d490b}.done{color:#087454}
@media(min-width:1260px){.inspection-devices{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:760px){
  .inspection-page{padding:6px 8px;gap:6px}
  .inspection-devices{grid-template-columns:minmax(0,1fr);gap:0}
  .inspection-shared-head{display:grid;position:sticky;top:0;z-index:1;border-bottom:1px solid var(--line)}
  .inspection-device-columns{display:none}
  .inspection-page input,.inspection-page select,.inspection-page textarea{font-size:16px}
  .inspection-columns{font-size:11px}
}
</style>
