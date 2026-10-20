<template>
  <section class="risk-feedback-table" aria-label="人员排查反馈表">
    <div class="risk-table-toolbar">
      <div class="risk-table-counts"><strong>人员反馈</strong><span>{{ peopleCount }} 人 · {{ rows.length }} 项</span><NTag size="small" :bordered="false" type="info">待审核 {{ count('submitted') }}</NTag><NTag size="small" :bordered="false" type="success">已通过 {{ count('approved') }}</NTag></div>
      <div class="risk-table-filters">
        <NInput v-model:value="search" clearable placeholder="搜索人员、检查项或反馈内容" :input-props="{'aria-label':'搜索排查反馈'}"><template #prefix><Search :size="16" /></template></NInput>
        <NSelect v-if="buildings.length>1" v-model:value="building" clearable :options="buildings" placeholder="全部楼栋" aria-label="反馈楼栋" />
        <NSelect v-model:value="status" clearable :options="statusOptions" placeholder="全部状态" aria-label="反馈状态" />
      </div>
    </div>
    <template v-if="mobile">
      <div class="risk-feedback-cards">
        <article v-for="row in mobileRows" :key="row.key" class="risk-feedback-card" :class="rowClass(row)" v-bind="rowProps(row)">
          <header><strong class="risk-feedback-title">{{row.item.title}}</strong><NTag size="small" :bordered="false" :type="row.item.status==='approved'?'success':row.item.status==='returned'?'warning':row.item.status==='submitted'?'info':'default'">{{statusNames[row.item.status]||row.item.status}}</NTag></header>
          <div class="risk-feedback-meta"><NButton text :disabled="busy" data-v2-navigation :aria-label="'查看 '+row.person.name+'的反馈 · '+row.item.title" @click="emit('open',row)">{{row.person.name}} · {{scopeLabel(row.item.scope||row.person.scope)}}</NButton><NTag v-if="overdue(row)" size="small" :bordered="false" type="error">已逾期</NTag><NButton size="small" secondary :disabled="busy" data-v2-navigation :aria-label="'处理 '+row.person.name+' · '+row.item.title" @click="emit('open',row)">{{actionLabel(row)}}<ArrowUpRight :size="14" /></NButton></div>
          <dl class="risk-feedback-summary"><div v-for="field in summaryFields" :key="field.key"><dt>{{field.field_name}}</dt><dd>{{fieldText(field,row)}}</dd></div></dl>
          <p v-if="row.item.reason" class="risk-feedback-reason">{{row.item.status==='revoked'?'撤销原因':'审核意见'}}：{{row.item.reason}}</p>
          <div v-if="attachments(row).length" class="risk-table-files"><a v-for="file in attachments(row).slice(0,2)" :key="file.id" :href="'/api/risk-inspections/files/'+encodeURIComponent(file.id)" target="_blank" rel="noopener noreferrer"><FileText :size="14" /><span>{{file.name}}</span></a><small v-if="attachments(row).length>2">另 {{attachments(row).length-2}} 个附件</small></div>
        </article>
        <NEmpty v-if="!filtered.length" description="暂无符合条件的反馈" />
      </div>
      <div v-if="filtered.length>20" class="risk-feedback-pagination"><NPagination :page="page" :page-size="20" :item-count="filtered.length" :page-slot="5" @update:page="value=>page=value" /></div>
    </template>
    <NDataTable v-else :columns="columns" :data="filtered" :row-key="rowKey" :row-class-name="rowClass" :row-props="rowProps" :scroll-x="tableWidth" :max-height="560" :pagination="pagination" size="small" :bordered="true" striped>
      <template #empty><NEmpty description="暂无符合条件的反馈" /></template>
    </NDataTable>
  </section>
</template>

<script setup lang="ts">
import {computed,h,onBeforeUnmount,ref,watch} from 'vue';
import {NButton,NDataTable,NEmpty,NInput,NPagination,NSelect,NTag,type DataTableColumns} from 'naive-ui';
import {ArrowUpRight,FileText,Search} from 'lucide-vue-next';
import type {Dict} from '../api/client';
import {activateCard} from '../workspace-v2/cardActivation';
import {riskFeedbackPriority,riskPresentationFields,riskSystemText} from './riskInspectionUi';

type Row={key:string;person:Dict;item:Dict};
const props=defineProps<{detail:Dict;busy:boolean}>();
const emit=defineEmits<{open:[row:Row]}>();
const search=ref(''),status=ref<string|null>(null),building=ref<string|null>(null),page=ref(1);
const mobileQuery=window.matchMedia('(max-width:700px)'),mobile=ref(mobileQuery.matches);
const resize=(event:MediaQueryListEvent)=>{mobile.value=event.matches;};
mobileQuery.addEventListener('change',resize);
onBeforeUnmount(()=>mobileQuery.removeEventListener('change',resize));
const statusNames:Record<string,string>={pending:'未提交',submitted:'待审核',approved:'审核通过',returned:'退回修改',revoked:'已撤销'};
const statusOptions=Object.entries(statusNames).map(([value,label])=>({value,label}));
const scopeLabel=(scope:string)=>scope==='110'?'110站':scope+'楼';
const rows=computed<Row[]>(()=>props.detail.people.flatMap((person:Dict)=>(person.items||[]).map((item:Dict)=>({key:person.id+':'+item.id,person,item}))));
const fields=computed<Dict[]>(()=>riskPresentationFields(props.detail.fields).filter(field=>field.role==='input'&&!field.is_primary));
const inputs=computed(()=>fields.value.filter(field=>field.role==='input'));
const summaryFields=computed(()=>inputs.value.filter(field=>field.type!==17).slice(0,2));
const peopleCount=computed(()=>props.detail.people.length);
const buildings=computed(()=>[...new Set<string>(rows.value.map(row=>row.item.scope||row.person.scope).filter(Boolean))].map(value=>({value,label:scopeLabel(value)})));
const count=(state:string)=>rows.value.filter(row=>row.item.status===state).length;
function readable(field:Dict,value:any):string{
  if(value===undefined||value===null||value===''||Array.isArray(value)&&!value.length)return '未填写';
  if(field.type===7)return value?'是':'否';
  if(field.type===5){const date=new Date(value);return Number.isFinite(date.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai'}).format(date):'日期无效';}
  return Array.isArray(value)?value.join('、'):String(value);
}
function fieldText(field:Dict,row:Row){return field.role==='input'?readable(field,row.item.values[field.key]):riskSystemText(field,row.item.system_values);}
function files(row:Row,field:Dict):Dict[]{return (row.item.values[field.key]||[]).map((id:string)=>({id,name:(row.item.files||[]).find((file:Dict)=>file.id===id)?.name||'排查附件'}));}
function attachments(row:Row){return inputs.value.filter(field=>field.type===17).flatMap(field=>files(row,field));}
function overdue(row:Row){return props.detail.status==='active'&&!row.person.revoked&&!['approved','revoked'].includes(row.item.status)&&props.detail.deadline<Date.now();}
function actionLabel(row:Row){return props.detail.status==='active'&&!row.person.revoked&&props.detail.self_id===row.person.id&&['pending','returned'].includes(row.item.status)?'填写':props.detail.is_reviewer&&row.item.status==='submitted'&&!row.person.revoked?'审核':'查看';}
const filtered=computed(()=>{
  const needle=search.value.trim().toLocaleLowerCase();
  return rows.value.filter(row=>(!building.value||(row.item.scope||row.person.scope)===building.value)&&(!status.value||row.item.status===status.value)&&(!needle||[
    row.person.name,scopeLabel(row.item.scope||row.person.scope),row.item.title,row.item.reason,
    ...fields.value.map(field=>field.role==='input'&&field.type===17?files(row,field).map(file=>file.name).join(' '):fieldText(field,row)),
  ].join(' ').toLocaleLowerCase().includes(needle))).sort((a,b)=>riskFeedbackPriority(a.item,props.detail.is_reviewer,a.person.revoked)-riskFeedbackPriority(b.item,props.detail.is_reviewer,b.person.revoked));
});
const mobileRows=computed(()=>filtered.value.slice((page.value-1)*20,page.value*20));
watch([search,status,building],()=>{page.value=1;});
watch(()=>filtered.value.length,total=>{page.value=Math.max(1,Math.min(page.value,Math.ceil(total/20)));});
const pagination=computed(()=>filtered.value.length>20?{page:page.value,pageSize:20,pageSlot:5,onChange:(value:number)=>{page.value=value;}}:false);
const rowKey=(row:Row)=>row.key;
function rowProps(row:Row){return {class:props.busy?'':'risk-clickable-row',onClick:(event:MouseEvent)=>{if(!props.busy&&activateCard(event))emit('open',row);}};}
const rowClass=(row:Row)=>row.item.status==='returned'?'risk-row-returned':row.item.status==='revoked'?'risk-row-revoked':'';
const fieldWidth=(field:Dict)=>field.type===1?240:field.type===17?190:150;
const tableWidth=computed(()=>(mobile.value?108:132)+220+112+188+104+fields.value.reduce((sum,field)=>sum+fieldWidth(field),0));
const columns=computed<DataTableColumns<Row>>(()=>[
  {key:'person',title:'排查人 / 楼栋',width:mobile.value?108:132,fixed:'left',render:row=>h('div',{class:'risk-table-person'},[h(NButton,{text:true,disabled:props.busy,onClick:()=>emit('open',row),'data-v2-navigation':true,'aria-label':'查看 '+row.person.name+'的反馈 · '+row.item.title},()=>row.person.name),h('small',scopeLabel(row.item.scope||row.person.scope))])},
  {key:'item',title:'检查项',width:220,render:row=>h('div',{class:'risk-table-text',title:row.item.title},row.item.title)},
  {key:'status',title:'状态',width:112,render:row=>h(NTag,{size:'small',bordered:false,type:row.item.status==='approved'?'success':row.item.status==='returned'?'warning':row.item.status==='submitted'?'info':'default'},()=>statusNames[row.item.status]||row.item.status)},
  ...fields.value.map(field=>({key:'field:'+field.key,title:field.field_name,width:fieldWidth(field),render:(row:Row)=>field.role==='input'&&field.type===17
    ?h('div',{class:'risk-table-files'},files(row,field).length?files(row,field).map(file=>h('a',{href:'/api/risk-inspections/files/'+encodeURIComponent(file.id),target:'_blank',rel:'noopener noreferrer'},[h(FileText,{size:14}),h('span',file.name)])):h('span',{class:'risk-table-empty'},'未上传'))
    :h('div',{class:'risk-table-text',title:fieldText(field,row)},fieldText(field,row))})),
  {key:'review',title:'审核意见',width:188,render:row=>h('div',{class:'risk-table-text'},[row.item.reason||'—',row.item.reviewed_by?h('small',row.item.reviewed_by):null])},
  {key:'action',title:'操作',width:104,fixed:mobile.value?undefined:'right',render:row=>h(NButton,{size:'small',disabled:props.busy,onClick:()=>emit('open',row),'data-v2-navigation':true,'aria-label':'处理 '+row.person.name+' · '+row.item.title},()=>[
    actionLabel(row),h(ArrowUpRight,{size:14}),
  ])},
]);
</script>

<style scoped>
.risk-feedback-table :deep(.risk-clickable-row){cursor:pointer}
.risk-feedback-cards{display:grid;gap:10px}
.risk-feedback-card{padding:12px;border:1px solid var(--border-color,#dce2e9);border-radius:8px;background:var(--card-color,#fff);min-width:0;overflow-wrap:anywhere}
.risk-feedback-card.risk-clickable-row{cursor:pointer}.risk-feedback-card:focus-within{outline:2px solid var(--primary-color,#2563eb);outline-offset:2px}
.risk-feedback-card>header,.risk-feedback-meta{display:flex;align-items:center;justify-content:space-between;gap:8px}
.risk-feedback-card>header{align-items:flex-start}.risk-feedback-title{min-width:0;flex:1;line-height:1.6;font-size:14px}.risk-feedback-card>header>.n-tag{flex:none}
.risk-feedback-meta{font-size:12px;color:var(--text-color-3,#667085);margin-top:4px}
.risk-feedback-meta :deep(.n-button){min-width:0;height:auto;font-size:12px}.risk-feedback-meta :deep(.n-button:first-child){flex:1;justify-content:flex-start}
.risk-feedback-meta :deep(.n-button__content){white-space:normal;text-align:left;overflow-wrap:anywhere}
.risk-feedback-summary{display:grid;gap:6px;margin:6px 0}.risk-feedback-summary>div{display:grid;grid-template-columns:minmax(0,80px) minmax(0,1fr);gap:8px;font-size:13px;line-height:1.6}
.risk-feedback-summary dt{color:var(--text-color-3,#667085)}.risk-feedback-summary dd{margin:0;white-space:pre-wrap;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.risk-feedback-reason{margin:8px 0;font-size:13px;line-height:1.6;color:var(--warning-color,#98631a);white-space:pre-wrap;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.risk-feedback-card .risk-table-files{display:grid;gap:8px;margin-top:8px}.risk-feedback-card .risk-table-files a{display:flex;align-items:center;gap:6px;color:var(--primary-color,#2563eb);min-height:44px}
.risk-feedback-card .risk-table-files svg{flex:none}.risk-feedback-card .risk-table-files small{font-size:12px;color:var(--text-color-3,#667085)}
.risk-feedback-card.risk-row-returned{border-color:var(--warning-color,#d99a2b)}.risk-feedback-card.risk-row-revoked{color:var(--text-color-3,#667085)}.risk-feedback-pagination{display:flex;justify-content:flex-end;margin-top:14px}
.risk-feedback-table :deep(.risk-table-person .n-button){height:auto;justify-content:flex-start;text-align:left;font-weight:600}.risk-feedback-table :deep(.risk-table-person .n-button__content){white-space:normal;overflow-wrap:anywhere}
.risk-feedback-table{min-width:0}.risk-table-toolbar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:12px;margin:16px 0 12px}.risk-table-counts,.risk-table-filters{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.risk-table-counts>span{font-size:12px;color:var(--text-color-3,#667085)}.risk-table-filters{min-width:0}.risk-table-filters>.n-input{width:240px}.risk-table-filters>.n-select{width:125px}.risk-feedback-table :deep(.n-data-table-th){font-size:12px;white-space:normal}.risk-feedback-table :deep(.n-data-table-td){vertical-align:top}.risk-feedback-table :deep(.risk-table-person){display:grid;gap:4px}.risk-feedback-table :deep(.risk-table-person small),.risk-feedback-table :deep(.risk-table-text small){display:block;color:var(--text-color-3,#667085);font-size:12px}.risk-feedback-table :deep(.risk-table-text){white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.65;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden}.risk-feedback-table :deep(.risk-table-empty){color:var(--text-color-3,#667085)}.risk-feedback-table :deep(.risk-table-files){display:grid;gap:6px}.risk-feedback-table :deep(.risk-table-files a){display:flex;align-items:flex-start;gap:5px;color:var(--primary-color,#2463d5);overflow-wrap:anywhere}.risk-feedback-table :deep(.risk-table-files svg){flex:none;margin-top:3px}.risk-feedback-table :deep(.risk-row-returned td){background:color-mix(in srgb,#e6a23c 8%,var(--card-color,#fff))}.risk-feedback-table :deep(.risk-row-revoked td){color:var(--text-color-3,#667085)}
@media(max-width:700px){.risk-table-toolbar{gap:8px}.risk-table-filters{width:100%}.risk-table-filters>.n-input{width:100%}.risk-table-filters>.n-select{flex:1;width:auto}.risk-feedback-table :deep(.n-button){min-height:44px}.risk-feedback-table :deep(.n-data-table-td){padding:10px 8px}}
</style>
