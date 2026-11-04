<template>
  <section class="day-sheet" aria-label="巡检表查看和导出">
    <div class="sheet-tools">
      <h2>巡检表</h2>
      <label>楼栋 <select v-model="scope" data-native-select><option v-for="item in scopes" :key="item" :value="item">{{ item }}楼</option></select></label>
      <label>排班日期 <input v-model="day" type="date"></label>
      <NButton size="small" :disabled="!!busy || !day" @click="load">刷新</NButton>
      <NButton size="small" type="primary" :disabled="!!busy || !report?.rounds.length" :loading="busy==='export'" @click="download">导出白夜班表格</NButton>
      <NButton size="small" :disabled="!!busy" @click="$emit('close')">返回巡检</NButton>
    </div>
    <div v-if="active && !active.pending && !readOnly" class="sheet-tools" role="status">
      <span :class="{warning:saveError}">{{ saveLabel }}</span>
      <NButton v-if="saveError && canRetry" size="small" @click="$emit('retrySave')">重试保存</NButton>
      <NButton v-else-if="saveError" size="small" @click="$emit('close')">返回核对</NButton>
    </div>
    <div v-for="item in conflictDevices" :key="item.id" class="sheet-tools warning" role="alert">
      <span>{{ item.name }}：共享数据已有其他修改</span>
      <NButton size="small" @click="$emit('resolveConflict',item.id,true)">合并我的修改</NButton>
      <NButton size="small" @click="$emit('resolveConflict',item.id,false)">使用共享数据</NButton>
    </div>
    <p>按排班日期汇总白班和夜班；凌晨巡检归前一天夜班。冷机表按四轮分别导出，其余原表每天一份；未填写的位置留空。</p>
    <p v-if="busy" role="status">{{ busy==='export' ? '正在生成表格…' : '正在读取白夜班巡检表…' }}</p>
    <p v-if="error" class="warning" role="alert">{{ error }}</p>
    <div v-if="report" class="sheet-tools">
      <NButton size="small" :type="view==='original'?'primary':'default'" @click="view='original'">原巡检表</NButton>
      <NButton size="small" :type="view==='summary'?'primary':'default'" @click="view='summary'">数据明细</NButton>
      <label>文字大小 <select v-model.number="zoom" data-native-select aria-label="表格文字大小"><option :value="12">小</option><option :value="14">标准</option><option :value="18">放大</option><option :value="22">更大</option></select></label>
    </div>
    <DailyInspectionOriginalSheet v-if="report && view==='original' && report.original_templates?.length" :key="scope"
      :templates="report.original_templates" :rounds="report.rounds" :active="active" :drafts="drafts" :read-only="readOnly" :zoom="zoom"
      @update-reading="(device,parameter,value)=>$emit('updateReading',device,parameter,value)" />
    <p v-else-if="report && view==='original'">原表布局暂不可用，请稍后重试。</p>
    <p v-if="report && view==='summary' && !report.rounds.length && !busy && !error">该日期尚无可查看的巡检记录。</p>
    <template v-if="report?.rounds.length && view==='summary'">
      <div class="sheet-tools">
        <label>轮次 <select v-model="roundIndex" data-native-select>
          <option v-for="(item,index) in report.rounds" :key="item.round_id" :value="index">{{ item.shift }}班第{{ item.round }}次 · {{ item.status }}</option>
        </select></label>
        <label>工作表 <select v-model="sheetIndex" data-native-select>
          <option v-for="(item,index) in round?.sheets || []" :key="item.name" :value="index">{{ item.name }}</option>
        </select></label>
        <label v-if="isReview"><input v-model="onlyMissing" type="checkbox"> 只看数据缺失</label>
      </div>
      <p v-if="round?.local_draft" class="warning">包含本人尚未共享的填写；查看和导出不会提交巡检。</p>
      <p v-else>当前为读取时的记录快照，其他人员修改后可刷新。</p>
      <div v-if="sheet" class="sheet-scroll" tabindex="0" aria-label="巡检表格，可横向滑动查看更多列">
        <table :style="{fontSize:zoom+'px'}">
          <thead><tr><th v-for="(cell,index) in headers" :key="index" :title="headerLabel(cell)">{{ headerLabel(cell) }}</th></tr></thead>
          <tbody>
            <tr v-for="row in visibleRows" :key="row.index">
              <td v-for="(cell,index) in row.cells" :key="index" :title="cellText(cell,index,row.cells)"
                :class="cellClass(cell,index,row.cells)">{{ cellText(cell,index,row.cells) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="sheet-tools" v-if="filteredRows.length">
        <NButton size="small" :disabled="page<=1" @click="page--">上一页</NButton>
        <span>第 {{ page }} / {{ pageCount }} 页 · {{ filteredRows.length }} 行</span>
        <NButton size="small" :disabled="page>=pageCount" @click="page++">下一页</NButton>
      </div>
      <p v-else-if="sheet">没有符合条件的数据。</p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { NButton } from 'naive-ui';
import { downloadFile, requestJson } from '../api/client';
import DailyInspectionOriginalSheet from './DailyInspectionOriginalSheet.vue';

type Value=string|number|boolean|null;
type Sheet={name:string;rows:Value[][]};
type OriginalCell={template:string;sheet:string;cell:string;value:Value;device:string;point_id:string;parameter:string;read_only:boolean;kind:'point'|'section'|'paired';location?:string;form?:string;members?:OriginalCell[];conflict?:boolean};
type Round={round_id:string;shift:string;round:number;status:string;local_draft:boolean;sheets:Sheet[];original_cells:OriginalCell[]};
type OriginalTemplate={scope:string;profession:string;name:string;sheets:{name:string;rows:Value[][];merges:string[]}[]};
type Report={scope:string;date:string;rounds:Round[];original_templates:OriginalTemplate[]};
type Draft={values:Record<string,string>;result:string;note:string;revision:number};
const props=defineProps<{scopes:string[];defaultScope?:string;defaultDate?:string;
  active?:{id:string;duty:{scope:string;date:string};review_version:number;pending:boolean;batch:{devices:{id:string;sections:{location:string;form:string}[];points:{id:string;point_id:string}[];parameters:{id:string;reference:Value;read_only?:boolean}[]}[];form_devices?:{id:string;sections:{location:string;form:string}[];points:{id:string;point_id:string}[];parameters:{id:string;reference:Value;read_only?:boolean}[]}[]};reviews:Record<string,Draft>};drafts:Record<string,Draft>;readOnly:boolean;saveLabel:string;saveError:boolean;canRetry:boolean;conflictDevices:{id:string;name:string}[]}>();
defineEmits<{close:[];retrySave:[];resolveConflict:[device:string,keep:boolean];updateReading:[device:string,parameter:string,value:string]}>();
const today=()=>new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date());
const scope=ref(props.defaultScope || props.scopes[0] || '');
const day=ref(props.defaultDate || today());
const report=ref<Report>();
const busy=ref('');
const view=ref<'original'|'summary'>('original');
const error=ref('');
const roundIndex=ref(0),sheetIndex=ref(0),page=ref(1),zoom=ref(14),onlyMissing=ref(false);
const round=computed(()=>report.value?.rounds[roundIndex.value]);
const sheet=computed(()=>round.value?.sheets[sheetIndex.value]);
const isReview=computed(()=>sheet.value?.name==='现场核对');
// Put the values needed during a walk-through first; retain all other columns to the right.
const columnOrder=computed(()=>{
  const source=sheet.value?.rows[0] || [];
  if (!isReview.value) return source.map((_,index)=>index);
  const preferred=['设备','参数','参考值','现场读数','核对结论','现场备注','楼栋','顺序','专业','位置','来源','参数状态','填写人','设备标识'];
  const first=preferred.map(name=>source.indexOf(name)).filter(index=>index>=0);
  return [...first,...source.map((_,index)=>index).filter(index=>!first.includes(index))];
});
const headers=computed(()=>columnOrder.value.map(index=>sheet.value?.rows[0]?.[index] ?? ''));
const readingIndex=computed(()=>headers.value.indexOf('现场读数'));
const stateIndex=computed(()=>headers.value.indexOf('参数状态'));
const resultIndex=computed(()=>headers.value.indexOf('核对结论'));
const display=(value:Value|undefined)=>value==null ? '' : typeof value==='boolean' ? (value?'是':'否') : String(value);
const headerLabel=(value:Value)=>isReview.value && value==='现场读数' ? '巡检数据' : display(value);
const missing=(row:Value[])=>isReview.value && (display(row[stateIndex.value]).startsWith('源数据不完整') ||
  (stateIndex.value>=0 && !['配置停用','无可填参数'].includes(display(row[stateIndex.value]))
  && readingIndex.value>=0 && !display(row[readingIndex.value]).trim()));
const filteredRows=computed(()=>(sheet.value?.rows.slice(1) || []).map((row,index)=>({cells:columnOrder.value.map(column=>row[column] ?? null),index}))
  .filter(row=>!onlyMissing.value || !isReview.value || missing(row.cells)));
const pageCount=computed(()=>Math.max(1,Math.ceil(filteredRows.value.length/100)));
const visibleRows=computed(()=>filteredRows.value.slice((page.value-1)*100,page.value*100));
const cellText=(value:Value,index:number,row:Value[])=>isReview.value && index===readingIndex.value && !display(value).trim()
  ? (['配置停用','无可填参数'].includes(display(row[stateIndex.value])) ? '—' : '数据缺失')
  : isReview.value && index===resultIndex.value && !display(value).trim() ? '待统一确认' : display(value);
const cellClass=(value:Value,index:number,row:Value[])=>({
  missing:isReview.value && missing(row) && (index===stateIndex.value || index===readingIndex.value && !display(value).trim()),
  abnormal:isReview.value && index===resultIndex.value && value==='异常',
});
let controller:AbortController|undefined;
let disposed=false;
function payload() {
  const active=props.active;
  const local=active && !props.readOnly && active.duty.scope===scope.value && active.duty.date===day.value;
  return {scope:scope.value,date:day.value,active_id:local ? active.id : '',drafts:local ? props.drafts : {}};
}
async function load() {
  controller?.abort();
  const current=controller=new AbortController();
  const requestedScope=scope.value, requestedDay=day.value;
  if (report.value && (report.value.scope!==requestedScope || report.value.date!==requestedDay)) report.value=undefined;
  error.value='';
  if (!requestedScope || !requestedDay) {busy.value='';return;}
  busy.value='load';
  try {
    const value=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify(payload()),signal:current.signal,timeoutMs:180000}) as Report;
    if (!disposed && !current.signal.aborted && requestedScope===scope.value && requestedDay===day.value) {
      const previous=report.value;
      report.value=value;
      if (!previous || previous.scope!==value.scope || previous.date!==value.date) {
        roundIndex.value=0;
        sheetIndex.value=Math.max(0,value.rounds[0]?.sheets.findIndex(item=>item.name==='现场核对') ?? 0);
      }
    }
  } catch (value) {
    if (!disposed && !current.signal.aborted) error.value=value instanceof Error ? value.message : '巡检表读取失败，请重试。';
  } finally {if (controller===current) busy.value='';}
}
async function download() {
  if (busy.value || !report.value?.rounds.length) return;
  busy.value='export'; error.value='';
  try {await downloadFile('/api/daily-inspection/day-export',{method:'POST',body:JSON.stringify(payload()),timeoutMs:180000});}
  catch (value) {error.value=value instanceof Error ? value.message : '导出未完成，请重试。';}
  finally {if (!disposed) busy.value='';}
}
watch([scope,day],load,{immediate:true});
watch(roundIndex,()=>{sheetIndex.value=Math.max(0,round.value?.sheets.findIndex(item=>item.name==='现场核对') ?? 0);});
watch([sheetIndex,roundIndex,onlyMissing],()=>{page.value=1;});
watch(()=>props.scopes,allowed=>{if (!allowed.includes(scope.value)) scope.value=allowed[0] || '';});
watch([()=>props.active?.id,()=>props.active?.review_version,()=>props.active?.pending],()=>{
  const showingActive=report.value?.rounds.some(item=>item.status!=='已归档');
  if (busy.value!=='export' && (showingActive || props.active?.duty.scope===scope.value && props.active.duty.date===day.value)) void load();
});
onBeforeUnmount(()=>{disposed=true;controller?.abort();});
</script>

<style scoped>
.day-sheet{display:grid;gap:7px;min-width:0}
.sheet-tools{display:flex;flex-wrap:wrap;align-items:center;gap:5px 9px}
.sheet-tools label{display:flex;align-items:center;gap:4px}
.sheet-tools :is(select,input:not([type=checkbox])){min-height:34px;max-width:100%;padding:3px 6px;color:inherit;background:var(--inspection-surface);border:1px solid var(--inspection-line);font:inherit}
h2,p{margin:0} h2{font-size:16px}.warning{color:var(--cf-warning)}
.sheet-scroll{max-width:100%;max-height:65vh;overflow:auto;border:1px solid var(--inspection-line);overscroll-behavior:auto}
table{border-collapse:collapse;min-width:100%;white-space:nowrap;background:var(--inspection-surface)}
th,td{border:1px solid var(--inspection-line);padding:5px 7px;text-align:left;min-width:5em;max-width:32em;white-space:normal;overflow-wrap:anywhere}
th{position:sticky;top:0;background:var(--inspection-soft);z-index:1}td.missing{color:var(--cf-danger);background:color-mix(in srgb,var(--inspection-surface) 90%,var(--cf-danger));font-weight:600}
td.abnormal{color:var(--cf-danger);font-weight:600}
@media(max-width:760px){.sheet-tools :is(select,input:not([type=checkbox])){font-size:16px}.sheet-scroll{max-height:60vh}}
</style>
