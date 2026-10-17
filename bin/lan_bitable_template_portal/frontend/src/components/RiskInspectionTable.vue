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
    <NDataTable :columns="columns" :data="filtered" :row-key="rowKey" :row-class-name="rowClass" :scroll-x="tableWidth" :max-height="560" :pagination="pagination" size="small" :bordered="true" striped>
      <template #empty><NEmpty description="暂无符合条件的反馈" /></template>
    </NDataTable>
  </section>
</template>

<script setup lang="ts">
import {computed,h,onBeforeUnmount,ref,watch} from 'vue';
import {NButton,NDataTable,NEmpty,NInput,NSelect,NTag,type DataTableColumns} from 'naive-ui';
import {ArrowUpRight,FileText,Search} from 'lucide-vue-next';
import type {Dict} from '../api/client';

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
const inputs=computed<Dict[]>(()=>props.detail.fields.filter((field:Dict)=>field.role==='input'));
const peopleCount=computed(()=>props.detail.people.length);
const buildings=computed(()=>[...new Set<string>(props.detail.people.map((p:Dict)=>p.scope))].map(value=>({value,label:scopeLabel(value)})));
const count=(state:string)=>rows.value.filter(row=>row.item.status===state).length;
function readable(field:Dict,value:any):string{
  if(value===undefined||value===null||value===''||Array.isArray(value)&&!value.length)return '未填写';
  if(field.type===7)return value?'是':'否';
  if(field.type===5){const date=new Date(value);return Number.isFinite(date.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai'}).format(date):'日期无效';}
  return Array.isArray(value)?value.join('、'):String(value);
}
function files(row:Row,field:Dict):Dict[]{return (row.item.values[field.key]||[]).map((id:string)=>({id,name:(row.item.files||[]).find((file:Dict)=>file.id===id)?.name||'排查附件'}));}
const filtered=computed(()=>{
  const needle=search.value.trim().toLocaleLowerCase();
  return rows.value.filter(row=>(!building.value||row.person.scope===building.value)&&(!status.value||row.item.status===status.value)&&(!needle||[
    row.person.name,scopeLabel(row.person.scope),row.item.title,row.item.reason,
    ...inputs.value.map(field=>field.type===17?files(row,field).map(file=>file.name).join(' '):readable(field,row.item.values[field.key])),
  ].join(' ').toLocaleLowerCase().includes(needle)));
});
watch([search,status,building],()=>{page.value=1;});
watch(()=>filtered.value.length,total=>{page.value=Math.max(1,Math.min(page.value,Math.ceil(total/20)));});
const pagination=computed(()=>filtered.value.length>20?{page:page.value,pageSize:20,pageSlot:5,onChange:(value:number)=>{page.value=value;}}:false);
const rowKey=(row:Row)=>row.key;
const rowClass=(row:Row)=>row.item.status==='returned'?'risk-row-returned':row.item.status==='revoked'?'risk-row-revoked':'';
const fieldWidth=(field:Dict)=>field.type===1?240:field.type===17?190:150;
const tableWidth=computed(()=>(mobile.value?108:132)+220+112+188+104+inputs.value.reduce((sum,field)=>sum+fieldWidth(field),0));
const columns=computed<DataTableColumns<Row>>(()=>[
  {key:'person',title:'排查人 / 楼栋',width:mobile.value?108:132,fixed:'left',render:row=>h('div',{class:'risk-table-person'},[h(NButton,{text:true,disabled:props.busy,onClick:()=>emit('open',row),'data-v2-navigation':true,'aria-label':'查看 '+row.person.name+'的反馈 · '+row.item.title},()=>row.person.name),h('small',scopeLabel(row.person.scope))])},
  {key:'item',title:'检查项',width:220,render:row=>h('div',{class:'risk-table-text',title:row.item.title},row.item.title)},
  {key:'status',title:'状态',width:112,render:row=>h(NTag,{size:'small',bordered:false,type:row.item.status==='approved'?'success':row.item.status==='returned'?'warning':row.item.status==='submitted'?'info':'default'},()=>statusNames[row.item.status]||row.item.status)},
  ...inputs.value.map(field=>({key:'field:'+field.key,title:field.field_name,width:fieldWidth(field),render:(row:Row)=>field.type===17
    ?h('div',{class:'risk-table-files'},files(row,field).length?files(row,field).map(file=>h('a',{href:'/api/risk-inspections/files/'+encodeURIComponent(file.id),target:'_blank',rel:'noopener noreferrer'},[h(FileText,{size:14}),h('span',file.name)])):h('span',{class:'risk-table-empty'},'未上传'))
    :h('div',{class:['risk-table-text',row.item.values[field.key]==null||row.item.values[field.key]===''?'risk-table-empty':''],title:readable(field,row.item.values[field.key])},readable(field,row.item.values[field.key]))})),
  {key:'review',title:'审核意见',width:188,render:row=>h('div',{class:'risk-table-text'},[row.item.reason||'—',row.item.reviewed_by?h('small',row.item.reviewed_by):null])},
  {key:'action',title:'操作',width:104,fixed:mobile.value?undefined:'right',render:row=>h(NButton,{size:'small',disabled:props.busy,onClick:()=>emit('open',row),'data-v2-navigation':true,'aria-label':'处理 '+row.person.name+' · '+row.item.title},()=>[
    props.detail.status==='active'&&!row.person.revoked&&props.detail.self_id===row.person.id&&['pending','returned'].includes(row.item.status)?'填写':props.detail.is_reviewer&&row.item.status==='submitted'&&!row.person.revoked?'审核':'查看',h(ArrowUpRight,{size:14}),
  ])},
]);
</script>

<style scoped>
.risk-feedback-table :deep(.risk-table-person .n-button){height:auto;justify-content:flex-start;text-align:left;font-weight:600}.risk-feedback-table :deep(.risk-table-person .n-button__content){white-space:normal;overflow-wrap:anywhere}
.risk-feedback-table{min-width:0}.risk-table-toolbar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:12px;margin:16px 0 12px}.risk-table-counts,.risk-table-filters{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.risk-table-counts>span{font-size:12px;color:var(--text-color-3,#667085)}.risk-table-filters{min-width:0}.risk-table-filters>.n-input{width:240px}.risk-table-filters>.n-select{width:125px}.risk-feedback-table :deep(.n-data-table-th){font-size:12px;white-space:normal}.risk-feedback-table :deep(.n-data-table-td){vertical-align:top}.risk-feedback-table :deep(.risk-table-person){display:grid;gap:4px}.risk-feedback-table :deep(.risk-table-person small),.risk-feedback-table :deep(.risk-table-text small){display:block;color:var(--text-color-3,#667085);font-size:12px}.risk-feedback-table :deep(.risk-table-text){white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.65;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden}.risk-feedback-table :deep(.risk-table-empty){color:var(--text-color-3,#667085)}.risk-feedback-table :deep(.risk-table-files){display:grid;gap:6px}.risk-feedback-table :deep(.risk-table-files a){display:flex;align-items:flex-start;gap:5px;color:var(--primary-color,#2463d5);overflow-wrap:anywhere}.risk-feedback-table :deep(.risk-table-files svg){flex:none;margin-top:3px}.risk-feedback-table :deep(.risk-row-returned td){background:color-mix(in srgb,#e6a23c 8%,var(--card-color,#fff))}.risk-feedback-table :deep(.risk-row-revoked td){color:var(--text-color-3,#667085)}
@media(max-width:700px){.risk-table-toolbar{gap:8px}.risk-table-filters{width:100%}.risk-table-filters>.n-input{width:100%}.risk-table-filters>.n-select{flex:1;width:auto}.risk-feedback-table :deep(.n-button){min-height:44px}.risk-feedback-table :deep(.n-data-table-td){padding:10px 8px}}
</style>
