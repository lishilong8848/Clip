<template>
  <section class="inspection-history-page" aria-label="历史巡检">
    <div v-if="!detail" class="history-tools">
      <h2>历史巡检</h2>
      <label>楼栋 <select v-model="scope" data-native-select><option v-for="item in scopes" :key="item" :value="item">{{ item }}楼</option></select></label>
      <label>日期 <input v-model="day" type="date"></label>
      <button type="button" :disabled="!!busy || !day" @click="loadList">刷新</button>
    </div>
    <p v-if="error" class="warning" role="alert">{{ error }}</p>
    <p v-if="message" class="done" role="status">{{ message }}</p>
    <template v-if="!detail">
      <p v-if="busy">正在读取历史巡检…</p>
      <p v-else-if="!rows.length && !error">当天没有已归档巡检。</p>
      <button v-for="row in rows" :key="row.id" type="button" class="history-row" :disabled="!!busy" @click="openRecord(row.id)">
        <strong>{{ row.scope }}楼 · {{ row.group }} {{ row.shift }}第 {{ row.round }} 次</strong>
        <span>{{ row.actor }} · {{ clock(row.completed) }} · {{ row.confirmed ?? '—' }}/{{ row.devices ?? '—' }} 台已填齐</span>
      </button>
    </template>
    <template v-else>
      <div class="history-tools">
        <button type="button" :disabled="!!busy" @click="returnToList">返回历史列表</button>
        <a :href="detail.url" target="_blank" rel="noopener noreferrer">飞书原记录</a>
        <button v-if="!editing && detail.can_edit!==false" type="button" :disabled="!!busy" @click="editing=true">修改记录</button>
      </div>
      <h2>{{ detail.date }} · {{ detail.scope }}楼 · {{ detail.group }} {{ detail.shift }}第 {{ detail.round }} 次</h2>
      <p>归档人 {{ detail.actor }} · {{ clock(detail.completed) }}<span v-if="detail.modified"> · 最后修改 {{ detail.editor }} {{ clock(detail.modified) }}</span></p>
      <div class="history-tools">
        <strong>已填齐 {{ completed }} / {{ detail.devices_data.length }} 台</strong>
        <label><input v-model="onlyMissing" type="checkbox"> 只看待填写</label>
      </div>
      <div v-for="device in detail.devices_data" v-show="!onlyMissing || focusedDevice===device.id || !complete(device)"
        :key="device.id" class="history-device" @focusin="focusedDevice=device.id" @focusout="leaveDevice(device.id,$event)">
        <div class="history-tools">
          <strong>{{ device.order }}. {{ device.name }} · {{ device.profession }}</strong>
          <span :class="complete(device)?'done':'warning'">{{ complete(device) ? '已填齐' : '待填写' }}</span>
        </div>
        <div class="history-grid history-head"><span>参数</span><span>参考值</span><span>现场读数</span></div>
        <div v-for="row in device.rows" v-show="row.name" :key="row.id" class="history-grid">
          <span>{{ row.name }} <small>{{ row.location || row.source }}</small></span>
          <span>{{ valueLabel(row.reference) }}</span>
          <span v-if="row.read_only">已停用</span>
          <input v-else-if="editing" :value="reading(device,row)" :aria-label="device.name+' '+row.name+'现场读数'"
            :disabled="!!busy" placeholder="未填" maxlength="160" @input="setValue(device,row.id,$event)">
          <span v-else :class="{'warning':!reading(device,row).trim()}">{{ reading(device,row) || '未填' }}</span>
        </div>
        <div class="history-tools">
          <label v-if="editing">结论 <select :value="result(device)" data-native-select :disabled="!!busy" @change="setResult(device,$event)">
            <option value="">未选</option><option>正常</option><option>异常</option>
          </select></label>
          <span v-else :class="{'warning':!result(device) || result(device)==='异常'}">{{ result(device) || '未选结论' }}</span>
          <span v-if="!editing && note(device)">{{ note(device) }}</span>
          <textarea v-if="editing" :value="note(device)" aria-label="现场备注" placeholder="备注，异常时必填" maxlength="1000"
            rows="1" :disabled="!!busy" @input="setNote(device,$event)" />
        </div>
      </div>
      <div v-if="editing" class="history-tools">
        <button type="button" :disabled="!!busy" @click="cancelEdit">取消编辑</button>
        <button type="button" class="save" :disabled="!!busy || !dirty" @click="save">保存到原记录</button>
        <span v-if="busy==='save'" role="status">正在保存到飞书…</span>
        <span v-else-if="error" class="warning">{{ error }}</span>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { ApiError, requestJson } from '../api/client';

type Row = {id:string;name:string;location:string;source:string;reference:string|number|boolean|null;value:string;read_only:boolean};
type Device = {id:string;order:number;name:string;profession:string;rows:Row[];result:string;note:string};
type Summary = {id:string;scope:string;date:string;shift:string;group:string;round:number;completed:number;actor:string;
  confirmed:number|null;devices:number|null;url:string;modified:number|null;editor:string};
type Detail = Summary & {version:string;devices_data:Device[];can_edit?:boolean};
type Change = {values:Record<string,string>;result:string;note:string};
const props=defineProps<{scopes:string[];defaultScope?:string}>();
const api='/api/daily-inspection/';
const scope=ref(props.defaultScope || props.scopes[0] || '');
const day=ref(new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date()));
const rows=ref<Summary[]>([]);
const detail=ref<Detail>();
const changes=ref<Record<string,Change>>({});
const editing=ref(false);
const onlyMissing=ref(false);
const focusedDevice=ref('');
const busy=ref('');
const error=ref('');
const message=ref('');
const dirty=computed(()=>Object.keys(changes.value).length>0);
const clock=(value:number)=>value ? new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short',hour12:false}).format(value) : '—';
const valueLabel=(value:Row['reference'])=>value==null || value==='' ? '—' : typeof value==='boolean' ? (value?'是':'否') : value;
const reading=(device:Device,row:Row)=>changes.value[device.id]?.values[row.id] ?? row.value;
const result=(device:Device)=>changes.value[device.id]?.result ?? device.result;
const note=(device:Device)=>changes.value[device.id]?.note ?? device.note;
const complete=(device:Device)=>['正常','异常'].includes(result(device)) && (result(device)!=='异常' || !!note(device).trim())
  && device.rows.every(row=>row.read_only || reading(device,row).trim());
const completed=computed(()=>detail.value?.devices_data.filter(complete).length ?? 0);
let controller:AbortController|undefined;

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
    if (controller===request) busy.value='';
  }
}
function loadList() {
  rows.value=[];
  if (!day.value || !scope.value) {
    controller?.abort();
    busy.value='';
    return;
  }
  void run<{history:Summary[]}>('history',{date:day.value},value=>{rows.value=value.history;});
}
function openRecord(id:string) {
  void run<Detail>('history-record',{id},value=>{
    detail.value=value;
    onlyMissing.value=false;
    focusedDevice.value='';
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
const setValue=(device:Device,id:string,event:Event)=>{edit(device).values[id]=(event.target as HTMLInputElement).value;};
const setResult=(device:Device,event:Event)=>{edit(device).result=(event.target as HTMLSelectElement).value;};
const setNote=(device:Device,event:Event)=>{edit(device).note=(event.target as HTMLTextAreaElement).value;};
function cancelEdit() {
  if (dirty.value && !window.confirm('放弃尚未保存的历史巡检修改？')) return false;
  changes.value={};
  editing.value=false;
  error.value='';
  return true;
}
function returnToList() {
  if (!cancelEdit()) return;
  detail.value=undefined;
  loadList();
}
function save() {
  const current=detail.value;
  if (!current || !dirty.value || busy.value) return;
  void run<Detail>('history-edit',{},value=>{
    detail.value=value;
    changes.value={};
    editing.value=false;
    message.value='已更新原飞书巡检记录。';
  },{id:current.id,version:current.version,changes:changes.value});
}
watch([scope,day],loadList,{immediate:true});
watch(()=>props.scopes,allowed=>{
  if (allowed.includes(scope.value)) return;
  detail.value=undefined;
  changes.value={};
  editing.value=false;
  scope.value=allowed[0] || '';
});
onBeforeUnmount(()=>controller?.abort());
</script>

<style scoped>
.inspection-history-page{display:grid;gap:7px;min-width:0}
.inspection-history-page :is(button,input,select,textarea){font:inherit;color:inherit;background:hsl(0 0% 100%);
  border:1px solid hsl(213 29% 75%);border-radius:0;padding:3px 6px;min-width:0;min-height:36px}
.inspection-history-page input[type="checkbox"]{min-height:0}
.inspection-history-page button:disabled{opacity:.6}
.history-tools{display:flex;flex-wrap:wrap;align-items:center;gap:5px 9px}
.history-tools label{display:flex;align-items:center;gap:4px}
.history-row{display:flex;justify-content:space-between;gap:4px 10px;flex-wrap:wrap;text-align:left}
.history-device{border-top:1px solid var(--line);padding:7px 0;min-width:0}
.history-grid{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(0,.7fr) minmax(0,1fr);gap:6px;align-items:center;
  border-bottom:1px solid var(--line);padding:3px 0;font-size:14px;overflow-wrap:anywhere}
.history-grid :is(input,span):not(:first-child){text-align:right}
.history-head{background:hsl(214 40% 97%);font-size:12px}
.history-grid small{display:block;font-size:11px}
.history-grid input{width:100%}
textarea{flex:1;min-width:130px}
h2,p{margin:0}
h2{font-size:16px}
.warning{color:#8d490b}.done{color:#087454}
.save{font-weight:700}
@media(max-width:760px){.inspection-history-page :is(input,select,textarea){font-size:16px}}
</style>
