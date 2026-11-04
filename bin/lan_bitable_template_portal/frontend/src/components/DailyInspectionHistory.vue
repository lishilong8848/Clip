<template>
  <section class="inspection-history-page" aria-label="历史巡检">
    <div v-if="!detail" class="history-tools">
      <h2>历史巡检</h2>
      <label>楼栋 <select v-model="scope" data-native-select><option v-for="item in scopes" :key="item" :value="item">{{ item }}楼</option></select></label>
      <label>日期 <input v-model="day" type="date" :disabled="showAll"></label>
      <NButton size="small" :type="showAll?'primary':'default'" :aria-pressed="showAll"
        :disabled="!!busy" @click="showAll=!showAll">全部日期</NButton>
      <NButton size="small" :disabled="!!busy || (!showAll && !day)" @click="loadList">刷新</NButton>
    </div>
    <p v-if="error" class="warning" role="alert">{{ error }}</p>
    <p v-if="message" class="done" role="status">{{ message }}</p>
    <template v-if="!detail">
      <p v-if="busy">正在读取历史巡检…</p>
      <p v-else-if="!rows.length && !error">{{ showAll ? '本楼暂无已归档巡检。' : '当天没有已归档巡检。' }}</p>
      <button v-for="row in rows" :key="row.id" type="button" class="history-row" :disabled="!!busy" @click="openRecord(row.id)">
        <strong>{{ row.date || '日期待核对' }} · {{ row.scope }}楼 · {{ row.group }} {{ row.shift }}第 {{ row.round }} 次</strong>
        <span>{{ row.actor }} · {{ clock(row.completed) }} · {{ row.confirmed ?? '—' }}/{{ row.devices ?? '—' }} 台已核对</span>
      </button>
    </template>
    <template v-else>
      <div class="history-tools">
        <NButton size="small" :disabled="!!busy" @click="returnToList">返回历史列表</NButton>
        <a :href="detail.url" target="_blank" rel="noopener noreferrer">飞书原记录</a>
        <NButton v-if="!editing && detail.can_edit!==false" size="small" type="primary" :disabled="!!busy" @click="beginEdit">修改记录</NButton>
        <span v-if="editing" :class="autoError || versionConflict || storageError ? 'warning' : 'done'" role="status">{{ autoLabel }}</span>
        <NButton v-if="editing && autoError && !versionConflict" size="small" :disabled="autoSaving" @click="retryAutoSave">重试保存</NButton>
      </div>
      <h2>{{ detail.date }} · {{ detail.scope }}楼 · {{ detail.group }} {{ detail.shift }}第 {{ detail.round }} 次</h2>
      <p>归档人 {{ detail.actor }} · {{ clock(detail.completed) }}<span v-if="detail.modified"> · 最后修改 {{ detail.editor }} {{ clock(detail.modified) }}</span></p>
      <div class="history-tools">
        <strong>数据齐全 {{ completed }} / {{ detail.devices_data.length }} 台</strong>
        <label><input v-model="onlyMissing" type="checkbox"> 只看待核对</label>
      </div>
      <div v-for="device in detail.devices_data" v-show="!onlyMissing || focusedDevice===device.id || !complete(device)"
        :key="device.id" class="history-device" @focusin="focusedDevice=device.id" @focusout="leaveDevice(device.id,$event)">
        <div class="history-tools">
          <strong>{{ device.order }}. {{ device.name }} · {{ device.profession }}</strong>
          <span :class="complete(device)?'done':'warning'">{{ complete(device) ? '数据齐全' : '待核对' }}</span>
        </div>
        <div class="history-readings">
          <div v-for="row in device.rows" v-show="row.name" :key="row.id" class="history-reading">
            <strong>{{ row.name }}</strong>
            <div class="history-value">
              <span v-if="row.read_only">{{ reading(device,row) || valueLabel(row.reference) }} · 已停用</span>
              <input v-else-if="editing" :value="reading(device,row)" :aria-label="device.name+' '+row.name+'巡检数据'"
                :disabled="!!busy" placeholder="未填" maxlength="160" @input="setValue(device,row.id,$event)">
              <span v-else :class="{'warning':!reading(device,row).trim()}">{{ reading(device,row) || '未填' }}</span>
              <small v-if="!row.read_only && reading(device,row)!==referenceText(row.reference)">原值 {{ valueLabel(row.reference) }}</small>
            </div>
          </div>
        </div>
      </div>
      <div v-if="editing" class="history-tools">
        <NButton size="small" :disabled="!!busy" @click="cancelEdit">结束编辑</NButton>
      </div>
    </template>
    <NModal :show="!!discardAction" preset="dialog" title="有未保存的历史巡检修改" aria-label="有未保存的历史巡检修改"
      content="可保留本机修改后离开，重新打开记录会继续保存；未确认的保存可能已经写入飞书。"
      @update:show="show=>{if(!show)discardAction=undefined;}">
    <template #action>
      <NButton @click="discardAction=undefined">继续编辑</NButton>
      <NButton @click="completeLeave(true)">保留并离开</NButton>
      <NButton type="error" @click="completeLeave(false)">放弃本机修改</NButton>
    </template>
  </NModal>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue';
import { NButton, NModal } from 'naive-ui';
import { ApiError, requestJson } from '../api/client';

type Row = {id:string;name:string;location:string;source:string;reference:string|number|boolean|null;value:string;read_only:boolean};
type Device = {id:string;order:number;name:string;profession:string;rows:Row[];result:string;note:string};
type Summary = {id:string;scope:string;date:string;shift:string;group:string;round:number;completed:number;actor:string;
  confirmed:number|null;devices:number|null;url:string;modified:number|null;editor:string};
type Detail = Summary & {version:string;devices_data:Device[];can_edit?:boolean;draft_key?:string};
type Change = {values:Record<string,string>;result:string;note:string};
type Pending = {id:string;version:string;changes:Record<string,Change>};
const props=defineProps<{scopes:string[];defaultScope?:string}>();
const emit=defineEmits<{close:[]}>();
const api='/api/daily-inspection/';
const scope=ref(props.defaultScope || props.scopes[0] || '');
const day=ref(new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date()));
const showAll=ref(false);
const rows=ref<Summary[]>([]);
const detail=ref<Detail>();
const changes=ref<Record<string,Change>>({});
const editing=ref(false);
const onlyMissing=ref(false);
const focusedDevice=ref('');
const busy=ref('');
const error=ref('');
const message=ref('');
const autoSaving=ref(false);
const hasSaved=ref(false);
const autoError=ref('');
const versionConflict=ref(false);
const storageError=ref(false);
const baseVersion=ref('');
const pending=ref<Pending>();
const discardAction=shallowRef<(keep:boolean) => void>();
const dirty=computed(()=>Object.keys(changes.value).length>0);
const autoLabel=computed(()=>autoSaving.value ? '正在自动保存到飞书…' : versionConflict.value ? '原记录已变化，本机修改待核对'
  : autoError.value ? autoError.value
  : dirty.value || pending.value ? storageError.value ? '本机暂存不可用，请保持页面打开' : '已暂存，等待自动保存'
  : hasSaved.value ? '已自动保存' : '修改后自动保存');
const clock=(value:number)=>value ? new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short',hour12:false}).format(value) : '—';
const valueLabel=(value:Row['reference'])=>value==null || value==='' ? '—' : typeof value==='boolean' ? (value?'是':'否') : value;
const referenceText=(value:Row['reference'])=>value==null ? '' : typeof value==='boolean' ? (value?'是':'否') : String(value);
const reading=(device:Device,row:Row)=>changes.value[device.id]?.values[row.id] ?? row.value;
const complete=(device:Device)=>device.rows.some(row=>!row.read_only) && device.rows.every(row=>row.read_only || reading(device,row).trim());
const completed=computed(()=>detail.value?.devices_data.filter(complete).length ?? 0);
let controller:AbortController|undefined;
let autoController:AbortController|undefined;
let autoTimer:number|undefined;
let disposed=false;

function validChanges(value:unknown):value is Record<string,Change> {
  if (!value || typeof value!=='object' || Array.isArray(value)) return false;
  return Object.values(value).every(item=>item && typeof item==='object' && !Array.isArray(item)
    && item.values && typeof item.values==='object' && !Array.isArray(item.values)
    && Object.values(item.values).every(text=>typeof text==='string')
    && ['', '正常', '异常'].includes(item.result) && typeof item.note==='string');
}
const draftStorageKey=(value:Detail)=>value.draft_key ? 'clipflow:daily-inspection-history:'+value.draft_key : '';
function persistDraft() {
  const current=detail.value, key=current && draftStorageKey(current);
  if (!key) {storageError.value=dirty.value;return;}
  try {
    if (dirty.value || pending.value) localStorage.setItem(key,JSON.stringify({version:baseVersion.value,changes:changes.value,pending:pending.value}));
    else localStorage.removeItem(key);
    storageError.value=false;
  } catch {storageError.value=true;}
}
function restoreDraft(value:Detail) {
  const key=draftStorageKey(value);
  if (!key || value.can_edit===false) return;
  try {
    const saved=JSON.parse(localStorage.getItem(key) || 'null');
    if (!saved || typeof saved.version!=='string' || !validChanges(saved.changes)) return;
    const old=saved.pending;
    pending.value=old && old.id===value.id && typeof old.version==='string' && validChanges(old.changes) ? old : undefined;
    changes.value=saved.changes;
    baseVersion.value=saved.version;
    editing.value=dirty.value || !!pending.value;
    versionConflict.value=baseVersion.value!==value.version && !pending.value;
    if (editing.value) queueAutoSave();
  } catch {storageError.value=true;}
}
function queueAutoSave(delay=4000) {
  window.clearTimeout(autoTimer);
  if (disposed || !detail.value || !editing.value || (!dirty.value && !pending.value) || autoController || busy.value || versionConflict.value) return;
  autoTimer=window.setTimeout(()=>void saveAutomatically(),delay);
}
function retryAutoSave() {autoError.value='';queueAutoSave(0);}
function remainingChanges(value:Detail) {
  const result:Record<string,Change>={};
  for (const [id,change] of Object.entries(changes.value)) {
    const device=value.devices_data.find(item=>item.id===id);
    if (!device) {result[id]=change;versionConflict.value=true;continue;}
    const values:Record<string,string>={};
    for (const [rowId,text] of Object.entries(change.values)) {
      const row=device.rows.find(item=>item.id===rowId);
      if (!row) {values[rowId]=text;versionConflict.value=true;}
      else if (text.trim()!==row.value.trim()) values[rowId]=text;
    }
    if (Object.keys(values).length || change.result!==device.result || change.note.trim()!==device.note.trim())
      result[id]={values,result:change.result,note:change.note};
  }
  return result;
}
async function saveAutomatically() {
  const current=detail.value;
  if (disposed || !current || !editing.value || (!dirty.value && !pending.value) || autoController || busy.value || versionConflict.value) return;
  if (!pending.value && baseVersion.value!==current.version) {
    versionConflict.value=true;
    autoError.value='原记录已变化，本机修改待核对。';
    persistDraft();
    return;
  }
  const operation=pending.value || {id:current.id,version:baseVersion.value,
    changes:JSON.parse(JSON.stringify(changes.value)) as Record<string,Change>};
  pending.value=operation;
  persistDraft();
  const request=autoController=new AbortController();
  autoSaving.value=true;
  autoError.value='';
  let retryable=true;
  try {
    const value=await requestJson(api+'history-edit?'+new URLSearchParams({scope:scope.value}),{
      method:'POST',body:JSON.stringify(operation),signal:request.signal,timeoutMs:120000}) as Detail;
    if (disposed || request.signal.aborted || detail.value?.id!==current.id || scope.value!==current.scope) return;
    const remaining=remainingChanges(value);
    detail.value=value;
    changes.value=remaining;
    pending.value=undefined;
    baseVersion.value=value.version;
    hasSaved.value=true;
    if (error.value==='当前操作正在处理，请稍后再离开。') error.value='';
    persistDraft();
  } catch (value) {
    if (disposed || request.signal.aborted || detail.value?.id!==current.id) return;
    autoError.value=value instanceof Error ? value.message : '自动保存未完成，本机内容已保留。';
    if (value instanceof ApiError && value.status>=400 && value.status<500 && ![408,429].includes(value.status)) {
      retryable=false;
      if (value.status===409) versionConflict.value=true;
      if (value.status===400 && /历史设备读数格式无效|请选择需要修改的历史设备|请核对.+结论及异常备注/.test(value.message)) pending.value=undefined;
    }
    persistDraft();
  } finally {
    if (autoController===request) {
      autoController=undefined;
      autoSaving.value=false;
      if (!disposed && retryable && (dirty.value || pending.value)) queueAutoSave(autoError.value?15000:4000);
    }
  }
}

async function run<T>(action:string,params:Record<string,string>,apply:(value:T)=>void,body?:object) {
  controller?.abort();
  const request=controller=new AbortController();
  busy.value=body?'save':action;
  error.value='';
  message.value='';
  try {
    const value=await requestJson(api+action+'?'+new URLSearchParams({scope:scope.value,...params}),{
      ...(body?{method:'POST',body:JSON.stringify(body)}:{cache:'no-store',fresh:true}),
      signal:request.signal,timeoutMs:120000,
    }) as T;
    if (!request.signal.aborted) apply(value);
  } catch (value) {
    if (!request.signal.aborted) {
      if (value instanceof ApiError && (value.status===401 || value.status===403)) {
        rows.value=[];
        detail.value=undefined;
        changes.value={};
        editing.value=false;
      }
      error.value=value instanceof Error ? value.message : '历史巡检操作未完成。';
    }
  } finally {
    if (controller===request) {busy.value='';if (dirty.value || pending.value) queueAutoSave();}
  }
}
function loadList() {
  rows.value=[];
  if ((!showAll.value && !day.value) || !scope.value) {
    controller?.abort();
    busy.value='';
    return;
  }
  void run<{history:Summary[]}>('history',showAll.value?{}:{date:day.value},value=>{rows.value=value.history;});
}
function openRecord(id:string) {
  void run<Detail>('history-record',{id},value=>{
    detail.value=value;
    changes.value={};
    pending.value=undefined;
    baseVersion.value=value.version;
    autoError.value='';
    hasSaved.value=false;
    versionConflict.value=false;
    editing.value=false;
    onlyMissing.value=false;
    focusedDevice.value='';
    restoreDraft(value);
  });
}
function leaveDevice(id:string,event:FocusEvent) {
  if (focusedDevice.value===id && !(event.currentTarget as HTMLElement).contains(event.relatedTarget as Node | null)) {
    focusedDevice.value='';
  }
}
function edit(device:Device) {
  return changes.value[device.id] ??= {values:{},result:device.result,note:device.note};
}
function beginEdit() {
  if (detail.value) restoreDraft(detail.value);
  editing.value=true;
}
const changed=()=>{
  if (!pending.value && !autoSaving.value && detail.value) changes.value=remainingChanges(detail.value);
  persistDraft();
  autoError.value='';
  queueAutoSave();
};
const setValue=(device:Device,id:string,event:Event)=>{edit(device).values[id]=(event.target as HTMLInputElement).value;changed();};
function resetEdit(keep=false) {
  window.clearTimeout(autoTimer);
  if (keep) persistDraft();
  changes.value={};
  pending.value=undefined;
  editing.value=false;
  error.value='';
  autoError.value='';
  versionConflict.value=false;
  hasSaved.value=false;
  if (!keep) persistDraft();
}
function requestDiscard(action:(keep:boolean)=>void) {
  if (busy.value==='save' || autoSaving.value) {
    error.value='当前操作正在处理，请稍后再离开。';
  } else if (dirty.value || pending.value) {
    discardAction.value=action;
  } else {
    if (busy.value) {
      controller?.abort();
      busy.value='';
    }
    action(false);
  }
}
function completeLeave(keep:boolean) {
  const action=discardAction.value;
  if (keep) {
    persistDraft();
    if (storageError.value) {error.value='本机暂存不可用，请保持页面打开或选择放弃修改。';return;}
  }
  discardAction.value=undefined;
  action?.(keep);
}
function cancelEdit() {
  requestDiscard(keep=>{
    resetEdit(keep);
    if (keep) {detail.value=undefined;loadList();}
  });
}
function returnToList() {
  requestDiscard(keep=>{
    resetEdit(keep);
    detail.value=undefined;
    loadList();
  });
}
function requestClose() {
  requestDiscard(keep=>{
    resetEdit(keep);
    detail.value=undefined;
    onlyMissing.value=false;
    emit('close');
  });
}
function requestBack() { if (detail.value) returnToList(); else requestClose(); }
function allowNavigation(proceed:()=>void) {
  if (busy.value==='save' || autoSaving.value) {
    error.value='当前操作正在处理，请稍后再离开。';
    return false;
  }
  if (!dirty.value && !pending.value) return true;
  discardAction.value=keep=>{if (keep) persistDraft(); else resetEdit();proceed();};
  return false;
}
function hasUnsavedChanges() { if (dirty.value || pending.value) persistDraft();return dirty.value || !!pending.value || autoSaving.value || busy.value==='save'; }
defineExpose({requestClose,requestBack,allowNavigation,hasUnsavedChanges});
watch([scope,day,showAll],loadList,{immediate:true});
watch(()=>props.scopes,allowed=>{
  if (allowed.includes(scope.value)) return;
  persistDraft();
  autoController?.abort();
  window.clearTimeout(autoTimer);
  detail.value=undefined;
  changes.value={};
  pending.value=undefined;
  editing.value=false;
  scope.value=allowed[0] || '';
});
onBeforeUnmount(()=>{
  disposed=true;
  persistDraft();
  controller?.abort();
  autoController?.abort();
  window.clearTimeout(autoTimer);
});
</script>

<style scoped>
.inspection-history-page{display:grid;gap:7px;min-width:0}
.inspection-history-page :is(.history-row,input:not([type=checkbox]),select,textarea){
  box-sizing:border-box;font:inherit;color:inherit;background:var(--inspection-surface);
  border:1px solid var(--inspection-line);border-radius:4px;padding:4px 8px;min-width:0;min-height:36px
}
.inspection-history-page select{appearance:auto}
.inspection-history-page a{color:var(--inspection-accent)}
.inspection-history-page .history-row:focus-visible{outline:2px solid var(--inspection-accent);outline-offset:2px}
.inspection-history-page .history-row:hover:not(:disabled){background:var(--inspection-soft);border-color:var(--inspection-accent)}
.inspection-history-page .history-row:disabled{opacity:.6}
.history-tools{display:flex;flex-wrap:wrap;align-items:center;gap:5px 9px}
.history-tools label{display:flex;align-items:center;gap:4px}
.history-row{display:flex;justify-content:space-between;align-items:center;gap:4px 10px;flex-wrap:wrap;text-align:left;cursor:pointer}
.history-row span{color:var(--inspection-muted);font-size:13px}
.history-device{border-top:1px solid var(--inspection-line);padding:7px 0;min-width:0}
.history-readings{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,155px),1fr));gap:0 8px;align-items:start}
.history-reading{display:grid;grid-template-columns:minmax(0,1fr) minmax(60px,42%);gap:4px;align-items:center;
  border-bottom:1px solid var(--inspection-line);padding:2px 0;min-width:0;min-height:40px;font-size:13px;overflow-wrap:anywhere}
.history-value{text-align:right;min-width:0;font-variant-numeric:tabular-nums}
.history-value small{display:block;font-size:11px;color:var(--inspection-muted)}
.history-value input{width:100%;text-align:right}
h2,p{margin:0}
h2{font-size:16px}
.warning{color:var(--cf-warning)}.done{color:var(--cf-success)}
@media(max-width:760px){
  .inspection-history-page :is(input,select,textarea){font-size:16px}
  .inspection-history-page .history-row{min-height:44px}
}
</style>
