<template>
  <NConfigProvider :locale="zhCN" :date-locale="dateZhCN">
  <section class="risk-page" aria-label="风险排查" data-v2-inline-feedback>
    <header class="risk-toolbar" data-v2-navigation>
      <NButton v-if="detail||editor" :disabled="busy" @click="back"><ArrowLeft :size="16" />排查列表</NButton>
      <strong>{{editor==='publish'?'发布风险排查':editor==='template'?'基础模板':editor==='history'?'历史模板库':detail?.name||'风险排查'}}</strong>
      <div class="risk-tools"><NButton v-if="editor!=='history'" :disabled="busy||unsaved" :loading="loading" title="刷新" aria-label="刷新" @click="reload"><RefreshCw :size="16" /></NButton><template v-if="isManager&&!detail&&!editor"><NButton :disabled="busy" @click="openEditor('template')"><Settings2 :size="16" />基础模板</NButton><NButton :disabled="busy" @click="openEditor('history')"><Library :size="16" />历史模板库</NButton><NButton type="primary" :disabled="busy" @click="openEditor('publish')"><Plus :size="16" />发布排查</NButton></template></div>
    </header>
    <NAlert v-if="error&&!confirmation&&!fieldEditor&&!feedbackOpen" type="error" :bordered="false" role="alert">{{error}} <NButton v-if="detail" text :disabled="busy" @click="readLatest">重新读取，保留填写</NButton></NAlert>
    <NAlert v-if="message&&!feedbackOpen" type="info" :bordered="false" role="status">{{message}}</NAlert>
    <NSpin :show="loading">
      <RiskInspectionTemplateLibrary v-if="editor==='history'" ref="historyLibrary" @reuse="reuseHistory" />
      <template v-else-if="editor==='template'&&template">
        <p class="risk-note">修改仅用于下一次发布，已发布任务保留原模板。系统字段自动填写；其它字段由排查人填写。</p>
        <div class="risk-toolbar"><span>{{template.fields.length}} 个字段</span><NButton v-if="canEditTemplate" :disabled="busy" @click="editField()"><Plus :size="16" />添加字段</NButton></div>
        <div class="risk-schema"><article v-for="field in template.fields" :key="field.key"><div><strong>{{field.field_name}}</strong><small>{{typeName(field.type)}} · {{field.role==='input'?(field.required?'必填':'选填'):'系统填写'}}{{field.is_primary?' · 主字段':''}}</small></div><NTag v-if="field.unsupported" type="warning">需调整类型</NTag><NButton v-if="canEditTemplate" size="small" :disabled="busy" @click="editField(field)"><Pencil :size="15" />修改</NButton><NButton v-if="canEditTemplate&&!field.is_primary" size="small" type="error" quaternary :disabled="busy" @click="ask('delete_field',field)"><Trash2 :size="15" /></NButton></article></div>
      </template>
      <form v-else-if="editor==='publish'&&template" class="risk-publish" @submit.prevent="ask('publish')">
        <div class="risk-toolbar"><NTag>{{publish.history_template_id?'历史模板：'+template.name+' · V'+template.revision:'从基础模板创建'}}</NTag><NButton :disabled="busy" @click="openEditor('history')"><Library :size="16" />选择历史模板</NButton></div>
        <NAlert v-if="template.legacy" type="warning" :bordered="false">原任务未保留原始预填内容，已仅带入字段和事项标题，请核对补充。</NAlert>
        <div class="risk-fields"><label><span>排查表名 *</span><NInput v-model:value="publish.name" :disabled="busy" :maxlength="120" placeholder="例如：10月电气设备专项排查" /></label><label><span>反馈截止时间 *</span><NDatePicker v-model:value="publish.deadline" :disabled="busy" type="datetime" clearable /></label><label><span>审核人 *</span><NSelect v-model:value="publish.reviewer_id" :disabled="busy" filterable :options="reviewerOptions" placeholder="选择有对应楼栋权限的审核人" /></label><label><span>接收人员 *</span><NSelect v-model:value="selectedPeople" :disabled="busy" multiple filterable :options="peopleOptions" placeholder="搜索姓名或工号" /></label></div>
        <div class="risk-recipient-editor"><div v-for="person in selectedRows" :key="person.id"><span>{{person.name}}</span><NSelect :disabled="busy" :value="publish.people.find((p:Dict)=>p.id===person.id)?.scope" :options="person.scopes.map((s:string)=>({value:s,label:scopeLabel(s)}))" @update:value="s=>setScope(person.id,s)" /></div></div>
        <div class="risk-toolbar"><strong>排查事项 · 每位接收人各一份</strong><NButton :disabled="busy" @click="publish.items.push({title:'',values:{}})"><Plus :size="16" />添加事项</NButton></div>
        <article v-for="(item,index) in publish.items" :key="index" class="risk-publish-item"><div class="risk-toolbar"><NInput v-model:value="item.title" :disabled="busy" :maxlength="300" :placeholder="'事项 '+(index+1)+'：设备、部件或需要检查的内容'" /><NButton v-if="publish.items.length>1" :disabled="busy" aria-label="移除此事项" @click="publish.items.splice(index,1)"><X :size="16" /></NButton></div><details><summary>预填内容</summary><RiskInspectionFields :fields="template.fields" v-model:values="item.values" prefill :disabled="busy" /></details></article>
        <p class="risk-note">发布后在模板所在多维表内新增数据表，表名附带本次发布编号以防重复。消息由飞书智能体发送，人员在灯塔填写并由指定审核人复核。</p>
        <footer class="risk-footer"><span>{{selectedPeople.length}} 人 · {{publish.items.length}} 项/人</span><NButton attr-type="submit" type="primary" :disabled="busy||!publishValid">确认发布</NButton></footer>
      </form>
      <template v-else-if="detail">
        <div class="risk-toolbar risk-summary"><span>{{detail.owner}}发布 · 审核人 {{detail.reviewer}} · 截止 {{date(detail.deadline)}}</span><div class="risk-tools"><NButton v-if="detail.can_manage_release&&detail.status==='active'" size="small" :disabled="busy" @click="openReviewer">更换审核人</NButton><a v-if="detail.url" :href="detail.url" target="_blank" rel="noopener noreferrer">查看结果表 <ExternalLink :size="14" /></a></div></div>
        <NAlert v-if="detail.status!=='active'" type="warning" :bordered="false">正在创建排查表并登记链接，完成后自动发送邀请。请勿重复发布。</NAlert>
        <div v-if="incompleteJobs.length" class="risk-sync"><span>{{failedJobs.length?'部分云端操作未完成':'云端处理中'}} · {{incompleteJobs.length}} 项</span><NButton v-if="detail.can_manage_release&&failedJobs.some((j:Dict)=>j.status==='failed')" size="small" :disabled="busy" @click="ask('retry')">重试未完成操作</NButton><details v-if="failedJobs.length"><summary>查看原因</summary><p v-for="job in failedJobs" :key="job.id">{{jobLabel(job.kind)}}：{{job.error}}</p></details></div>
        <RiskInspectionTable :key="detail.id" :detail="detail" :busy="busy" @open="openFeedback" />
        <NDrawer :show="feedbackOpen" placement="right" width="min(760px, 100vw)" :mask-closable="!busy" :close-on-esc="!busy" @update:show="value=>{if(!value)closeFeedback()}">
          <NDrawerContent :title="person?person.name+' · '+scopeLabel(person.scope):'排查反馈'" :closable="!busy" class="risk-feedback-drawer" data-v2-inline-feedback>
          <main v-if="person" class="risk-feedback">
            <NAlert v-if="error&&!confirmation" type="error" :bordered="false" role="alert">{{error}} <NButton text :disabled="busy" @click="readLatest">重新读取，保留填写</NButton></NAlert>
            <NAlert v-if="message" type="info" :bordered="false" role="status">{{message}}</NAlert>
            <header class="risk-toolbar"><NTag :bordered="false">{{personProgress(person)}}</NTag><NButton v-if="detail.is_manager&&!person.revoked" type="error" size="small" quaternary :disabled="busy" @click="ask('revoke_person')">撤销该人员</NButton></header>
            <article v-for="item in focusedItems" :key="item.id" class="risk-response" :class="{revoked:item.status==='revoked'}">
              <header><strong>{{item.title}}</strong><NTag :type="item.status==='approved'?'success':item.status==='returned'?'warning':'default'" size="small">{{statusName(item.status)}}</NTag><NButton v-if="detail.is_manager&&!person.revoked&&item.status!=='revoked'" size="small" quaternary type="error" :disabled="busy" @click="ask('revoke_item',item)">撤销事项</NButton></header>
              <p v-if="item.reason" class="risk-note">{{item.status==='revoked'?'撤销原因':item.reviewed_by||'管理员'}}：{{item.reason}}</p>
              <RiskInspectionFields v-if="item.status!=='revoked'" :fields="detail.fields" :values="editable(item)?drafts[item.id]||item.values:item.values" :files="item.files" :disabled="!editable(item)||busy" @update:values="v=>drafts[item.id]=v" @files="(field,files)=>uploadFiles(item,field,files)" />
              <details v-if="drafts[item.id]&&!editable(item)" class="risk-local-draft"><summary>本地未提交内容（当前事项已变更）</summary><RiskInspectionFields :fields="detail.fields" :values="drafts[item.id]" :files="item.files" disabled /><NButton size="small" :disabled="busy" @click="discardDraft(item.id)">放弃这份本地填写</NButton></details>
              <footer><small>{{item.submitted_at?'提交于 '+date(item.submitted_at*1000):'尚未提交'}}{{item.reviewed_at?' · 审核于 '+date(item.reviewed_at*1000):''}}</small><div class="risk-tools"><template v-if="editable(item)"><NButton :disabled="busy" @click="act('save',item)">保存草稿</NButton><NButton type="primary" :disabled="busy" @click="ask('submit',item)">提交审核</NButton></template><template v-if="detail.is_reviewer&&item.status==='submitted'&&!person.revoked"><NButton :disabled="busy" @click="ask('return',item)">退回修改</NButton><NButton type="primary" :disabled="busy" @click="ask('approve',item)">审核通过</NButton></template></div></footer>
            </article>
          </main>
          </NDrawerContent>
        </NDrawer>
        <details class="risk-audit"><summary>操作记录</summary><p v-for="(row,index) in detail.audit" :key="index">{{date(row.at*1000)}} · {{auditName(row)}} · {{actionName(row.action)}}</p></details>
      </template>
      <template v-else>
        <div class="risk-task-list"><NDataTable :columns="taskColumns" :data="list" :row-key="task=>task.id" :scroll-x="1050" size="small" :bordered="true" striped><template #empty><NEmpty description="暂无分配的风险排查" /></template></NDataTable></div>
        <footer class="risk-list-footer"><span>共 {{total}} 个排查任务</span><NPagination v-if="total>20" :page="page" :page-count="Math.ceil(total/20)" :page-slot="5" @update:page="changePage" /></footer>
      </template>
    </NSpin>
    <NModal :show="!!fieldEditor" preset="card" title="模板字段" class="risk-modal" :mask-closable="false" :closable="!busy" @update:show="v=>{if(!v&&!busy)fieldEditor=null}"><div v-if="fieldEditor" class="risk-fields"><label><span>字段名称</span><NInput v-model:value="fieldEditor.field_name" :maxlength="80" /></label><label><span>字段类型</span><NSelect v-model:value="fieldEditor.type" :options="typeOptions" :disabled="fieldEditor.is_primary" /></label><label><span>填写方式</span><NSelect v-model:value="fieldEditor.role" :options="roleOptions" /></label><NCheckbox v-model:checked="fieldEditor.required" :disabled="fieldEditor.role!=='input'">填写时必填</NCheckbox><label v-if="[3,4].includes(fieldEditor.type)" class="wide"><span>选项，每行一个</span><NInput v-model:value="fieldEditor.optionsText" type="textarea" :autosize="{minRows:3,maxRows:8}" /></label></div><template #footer><NButton type="primary" :disabled="busy||!fieldEditor?.field_name?.trim()" @click="ask('save_field')">保存字段</NButton></template></NModal>
    <NModal :show="!!confirmation" preset="card" :title="confirmTitle" class="risk-modal risk-confirm" :mask-closable="false" :close-on-esc="!busy" :closable="!busy" @update:show="v=>{if(!v&&!busy)confirmation=null}"><NAlert v-if="error" type="error" :bordered="false" role="alert">{{error}} <NButton v-if="detail" text :disabled="busy" @click="readLatest">重新读取，保留填写</NButton></NAlert><p>{{confirmText}}</p><NSelect v-if="confirmation?.action==='reviewer'" v-model:value="newReviewer" :options="replacementOptions" :disabled="busy" filterable placeholder="选择新审核人" aria-label="新审核人" /><NInput v-if="needsReason" v-model:value="reason" :disabled="busy" type="textarea" :maxlength="2000" :autosize="{minRows:2,maxRows:4}" placeholder="请填写原因" /><template #footer><div class="risk-tools"><NButton :disabled="busy" @click="confirmation=null">取消</NButton><NButton type="primary" :loading="busy" :disabled="busy||needsReason&&!reason.trim()||confirmation?.action==='reviewer'&&!newReviewer" @click="confirm">确认</NButton></div></template></NModal>
  </section>
  </NConfigProvider>
</template>
<script setup lang="ts">
import {computed,h,onBeforeUnmount,onMounted,reactive,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NConfigProvider,NDataTable,NDatePicker,NDrawer,NDrawerContent,NEmpty,NInput,NModal,NPagination,NSelect,NSpin,NTag,zhCN,dateZhCN,type DataTableColumns} from 'naive-ui';
import {ArrowLeft,ExternalLink,Library,Pencil,Plus,RefreshCw,Settings2,Trash2,X} from 'lucide-vue-next';
import {requestJson,type Dict} from '../api/client';
import {registerNavigationGuard} from '../navigation';
import RiskInspectionFields from './RiskInspectionFields.vue';
import RiskInspectionTemplateLibrary from './RiskInspectionTemplateLibrary.vue';
import RiskInspectionTable from './RiskInspectionTable.vue';
const props=defineProps<{releaseId?:string}>();
const root='/api/risk-inspections';
const list=ref<Dict[]>([]),detail=ref<Dict|null>(null),template=ref<Dict|null>(null),people=ref<Dict[]>([]);
const page=ref(1),total=ref(0),isManager=ref(false),canEditTemplate=ref(false),loading=ref(false),busy=ref(false),error=ref(''),message=ref(''),editor=ref(''),personId=ref('');
const feedbackOpen=ref(false),focusedItemId=ref('');
const drafts=ref<Record<string,Dict>>({}),fieldEditor=ref<Dict|null>(null),confirmation=ref<Dict|null>(null),reason=ref(''),selectedPeople=ref<string[]>([]);
const publish=reactive<Dict>({id:'',name:'',deadline:null,reviewer_id:null,people:[],items:[{title:'',values:{}}],history_template_id:''});
const historyLibrary=ref<InstanceType<typeof RiskInspectionTemplateLibrary>|null>(null);
const newReviewer=ref<string|null>(null);
const typeOptions=[{value:1,label:'文本'},{value:2,label:'数字'},{value:3,label:'单选'},{value:4,label:'多选'},{value:5,label:'日期'},{value:7,label:'勾选'},{value:13,label:'电话'},{value:15,label:'链接'},{value:17,label:'附件 / 照片'}];
const roleOptions=[{value:'input',label:'排查人填写'},...Object.entries({number:'任务编号',status:'排查状态',scope:'楼栋',person:'排查人',reviewer:'复核人',review:'审核意见',reviewed_at:'复核日期',sync:'落表状态'}).map(([value,label])=>({value,label:'系统填写 · '+label}))];
const peopleOptions=computed(()=>people.value.map(p=>({value:p.id,label:p.name+' · '+p.employee_no})));
const reviewerOptions=computed(()=>people.value.filter(p=>p.can_review).map(p=>({value:p.id,label:p.name+' · '+p.scopes.map(scopeLabel).join('、')})));
const replacementOptions=computed(()=>people.value.filter(p=>p.can_review&&p.id!==detail.value?.reviewer_id&&detail.value?.people.every((assigned:Dict)=>p.scopes.includes(assigned.scope)&&(assigned.revoked||assigned.id!==p.id))).map(p=>({value:p.id,label:p.name+' · '+p.scopes.map(scopeLabel).join('、')})));
const selectedRows=computed(()=>people.value.filter(p=>selectedPeople.value.includes(p.id)));
watch(selectedPeople,ids=>{publish.people=ids.map(id=>{const existing=publish.people.find((p:Dict)=>p.id===id),person=people.value.find(p=>p.id===id);return existing||{id,scope:person?.scopes.includes(person.home_scope)?person.home_scope:person?.scopes[0]};});});
function setScope(id:string,scope:string){const p=publish.people.find((p:Dict)=>p.id===id);if(p)p.scope=scope;}
const person=computed(()=>detail.value?.people.find((p:Dict)=>p.id===personId.value));
const focusedItems=computed(()=>(person.value?.items||[]).filter((item:Dict)=>item.id===focusedItemId.value));
const taskColumns:DataTableColumns<Dict>=[
  {key:'name',title:'排查任务',width:310,render:task=>h(NButton,{text:true,disabled:busy.value,onClick:()=>openTask(task.id),'data-v2-navigation':true,class:'risk-task-name'},()=>task.name)},
  {key:'status',title:'状态',width:115,render:task=>h(NTag,{size:'small',bordered:false,type:task.total>0&&task.approved===task.total?'success':'default'},()=>task.status==='publishing'?'发布处理中':!task.total?'已撤销':task.approved===task.total?'已完成':'进行中')},
  {key:'people_count',title:'人员',width:80,render:task=>task.people_count+' 人'},
  {key:'progress',title:'反馈 / 总项数',width:120,render:task=>task.submitted+' / '+task.total},
  {key:'approved',title:'已通过',width:80},
  {key:'reviewer',title:'审核人',width:115},
  {key:'deadline',title:'反馈截止时间',width:190,render:task=>h('span',{class:task.deadline<Date.now()&&task.approved<task.total?'overdue':''},date(task.deadline))},
  {key:'action',title:'操作',width:100,fixed:'right',render:task=>h(NButton,{size:'small',disabled:busy.value,onClick:()=>openTask(task.id),'data-v2-navigation':true},()=> '查看反馈')},
];
const incompleteJobs=computed(()=>(detail.value?.jobs||[]).filter((j:Dict)=>j.status!=='done'));
const failedJobs=computed(()=>incompleteJobs.value.filter((j:Dict)=>j.error));
const dirty=computed(()=>Object.keys(drafts.value).some(id=>!sameValues(drafts.value[id],person.value?.items.find((i:Dict)=>i.id===id)?.values)));
const unsaved=computed(()=>dirty.value||editor.value==='publish'&&!!(publish.name||publish.deadline||publish.reviewer_id||selectedPeople.value.length||publish.items.some((i:Dict)=>i.title||Object.keys(i.values).length)));
const publishValid=computed(()=>publish.name.trim()&&publish.deadline&&publish.reviewer_id&&publish.people.length&&publish.items.every((i:Dict)=>i.title.trim())&&!template.value?.fields.some((f:Dict)=>f.unsupported));
const needsReason=computed(()=>['return','revoke_item','revoke_person'].includes(confirmation.value?.action));
const confirmTitle=computed(()=>actionName(confirmation.value?.action||''));
const confirmText=computed(()=>(({publish:'确认后将创建排查表并通知所选人员。',submit:'提交后不能继续编辑，审核人退回后可修改。',approve:'确认反馈内容已核实，通过后同步最终结果。',return:'反馈将退回本人修改，原提交记录保留。',revoke_item:'该人员不再需要完成此事项，已有反馈和操作记录保留。',revoke_person:'该人员不再需要完成本次排查，已有反馈和操作记录保留。',delete_field:'此操作会删除发布模板的字段及该列数据；不会修改已经发布的任务。',save_field:'字段修改用于后续发布。更改字段类型可能转换或清空模板原有数据。',reviewer:'已提交的反馈和原审核记录保留，尚未审核的事项将交由新审核人处理。',retry:'请先核对未完成原因。确认后以你的管理权限重试已保存的反馈，不改动原操作人和已填写内容。'} as Dict)[confirmation.value?.action||'']||''));
let read:AbortController|undefined,timer=0,disposed=false;
function beginWrite(){read?.abort();read=undefined;loading.value=false;busy.value=true;}
function date(v:number){return new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(new Date(v));}
function scopeLabel(v:string){return v==='110'?'110站':v+'楼';}
function typeName(v:number){return typeOptions.find(x=>x.value===v)?.label||'暂不支持的类型';}
function statusName(v:string){return ({pending:'未提交',submitted:'待审核',approved:'审核通过',returned:'退回修改',revoked:'已撤销'} as Dict)[v]||v;}
function actionName(v:string){return ({save:'保存草稿',submit:'提交审核',approve:'审核通过',return:'退回修改',revoke_item:'撤销事项',revoke_person:'撤销人员',publish:'发布风险排查',delete_field:'删除模板字段',save_field:'保存模板字段',reviewer:'更换审核人',retry:'核对并重试同步'} as Dict)[v]||v;}
function jobLabel(v:string){return ({publish:'发布排查',notify:'消息通知',review_notice:'审核通知',sync:'结果落表',registry:'登记信息同步',template_sync:'历史模板备份'} as Dict)[v]||v;}
function auditName(row:Dict){try{return JSON.parse(row.data).name}catch{return '操作人'}}
function personProgress(p:Dict){const items=p.items.filter((i:Dict)=>i.status!=='revoked');return `反馈 ${items.filter((i:Dict)=>['submitted','approved'].includes(i.status)).length}/${items.length} · 通过 ${items.filter((i:Dict)=>i.status==='approved').length}`;}
function editable(item:Dict){return detail.value?.status==='active'&&!person.value?.revoked&&detail.value?.self_id===person.value?.id&&['pending','returned'].includes(item.status);}
function openFeedback(row:{person:Dict;item:Dict}){if(busy.value)return;if(dirty.value&&!window.confirm('有未保存内容，是否放弃并切换反馈？'))return;drafts.value={};personId.value=row.person.id;focusedItemId.value=row.item.id;feedbackOpen.value=true;error.value='';message.value='';}
function closeFeedback(){if(busy.value)return;if(dirty.value&&!window.confirm('有未保存内容，是否放弃并关闭？'))return;drafts.value={};feedbackOpen.value=false;}
function applyDetail(data:Dict){detail.value=data;if(!data.people.some((p:Dict)=>p.id===personId.value))personId.value=data.people.find((p:Dict)=>p.id===data.self_id)?.id||data.people[0]?.id;}
async function reload(){
 if(editor.value==='history')return;
 if(busy.value||dirty.value)return;read?.abort();const abort=new AbortController();read=abort;loading.value=true;error.value='';
 try{if(editor.value){const path=editor.value==='publish'&&publish.history_template_id?'/templates/'+publish.history_template_id:'/template';const data=await requestJson(root+path,{signal:abort.signal,cache:'no-store',timeoutMs:60000});if(!abort.signal.aborted)template.value=data;}
 else if(detail.value){const data=await requestJson(root+'/'+detail.value.id,{signal:abort.signal,cache:'no-store'});if(!abort.signal.aborted)applyDetail(data);}
 else {const data=await requestJson(root+'?page='+page.value,{signal:abort.signal,cache:'no-store'});if(!abort.signal.aborted){list.value=data.items;total.value=data.total;isManager.value=data.is_manager;canEditTemplate.value=data.can_edit_template;}}}
 catch(e:any){if(!abort.signal.aborted)error.value=e.message;}finally{if(read===abort){loading.value=false;read=undefined;}}
}
async function openTask(id:string){if(busy.value)return;read?.abort();const abort=new AbortController();read=abort;loading.value=true;error.value='';try{const d=await requestJson(root+'/'+encodeURIComponent(id),{signal:abort.signal,cache:'no-store'});if(!abort.signal.aborted)applyDetail(d);}catch(e:any){if(!abort.signal.aborted)error.value=e.message;}finally{if(read===abort){read=undefined;loading.value=false;}}}
function resetPublication(){selectedPeople.value=[];Object.assign(publish,{id:Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join(''),name:'',deadline:null,reviewer_id:null,people:[],items:[{title:'',values:{}}],history_template_id:''});}
async function openEditor(mode:string){if(busy.value||unsaved.value&&!window.confirm('放弃当前未发布内容并切换模板来源？'))return;resetPublication();editor.value=mode;message.value='';error.value='';if(mode==='history')return;await reload();if(mode==='publish'&&template.value){beginWrite();try{const data=await requestJson(root+'/people',{cache:'no-store'});people.value=data.items;}catch(e:any){error.value=e.message;}finally{busy.value=false;}}}
async function reuseHistory(data:Dict){beginWrite();error.value='';try{const directory=await requestJson(root+'/people',{cache:'no-store'});people.value=directory.items;resetPublication();template.value=data;Object.assign(publish,{name:data.name,items:structuredClone(data.items),history_template_id:data.id});editor.value='publish';message.value='已带入历史模板；请重新确认人员、审核人及截止时间。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
function back(){if(busy.value||editor.value==='history'&&historyLibrary.value&&!historyLibrary.value.leaveAllowed()||unsaved.value&&!window.confirm('放弃未保存的修改并返回列表？'))return;if(editor.value==='publish')resetPublication();feedbackOpen.value=false;detail.value=null;editor.value='';drafts.value={};message.value='';void reload();}
function changePage(value:number){page.value=value;void reload();}
function editField(field?:Dict){fieldEditor.value=field?{...field,optionsText:(field.property?.options||[]).map((v:Dict)=>v.name).join('\n')}:{field_name:'',type:1,role:'input',required:false,optionsText:''};}
function ask(action:string,item?:Dict){error.value='';reason.value='';confirmation.value={action,item,version:person.value?.version};}
async function openReviewer(){if(busy.value)return;beginWrite();error.value='';try{const data=await requestJson(root+'/people',{cache:'no-store'});people.value=data.items;newReviewer.value=null;confirmation.value={action:'reviewer',version:detail.value?.reviewer_version};}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
function discardDraft(id:string){if(window.confirm('确认放弃此项尚未提交的本地填写？'))delete drafts.value[id];}
function sameValues(a:Dict|undefined,b:Dict|undefined){const keys=Object.keys(a||{}),other=Object.keys(b||{});return keys.length===other.length&&keys.every(k=>other.includes(k)&&JSON.stringify(a?.[k])===JSON.stringify(b?.[k]));}
async function readLatest(){if(busy.value||!detail.value)return;beginWrite();try{const data=await requestJson(root+'/'+detail.value.id,{cache:'no-store'});const current=data.people.find((p:Dict)=>p.id===personId.value);for(const item of current?.items||[])if(sameValues(drafts.value[item.id],item.values))delete drafts.value[item.id];applyDetail(data);confirmation.value=null;error.value='';message.value='已读取最新状态，未提交的填写仍保留，请核对后继续。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
async function act(action:string,item?:Dict,version=person.value?.version){
 if(busy.value||!detail.value||!person.value)return;beginWrite();error.value='';message.value='';
 try{const data=await requestJson(`${root}/${detail.value.id}/people/${person.value.id}/actions`,{method:'POST',body:JSON.stringify({action,version,item_id:item?.id,values:item?(drafts.value[item.id]||item.values):undefined,reason:reason.value})});applyDetail(data);if(item)delete drafts.value[item.id];message.value=action==='save'?'草稿已保存。':'操作已保存，云端结果正在同步。';confirmation.value=null;}
 catch(e:any){error.value=e.message;}finally{busy.value=false;}
}
async function confirm(){
 const c=confirmation.value;if(!c||busy.value)return;
 if(c.action==='retry'){await retry();return;}
 if(c.action==='reviewer'){beginWrite();error.value='';try{applyDetail(await requestJson(root+'/'+detail.value!.id+'/reviewer',{method:'POST',body:JSON.stringify({reviewer_id:newReviewer.value,version:c.version})}));confirmation.value=null;message.value='审核人已更换，待审核事项将通知新审核人。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}return;}
 if(!['publish','save_field','delete_field'].includes(c.action)){await act(c.action,c.item,c.version);return;}
 beginWrite();error.value='';
 try{if(c.action==='publish'){const data=await requestJson(root,{method:'POST',body:JSON.stringify({...publish,template_version:template.value?.version}),timeoutMs:60000});editor.value='';applyDetail(data);message.value='发布已受理，创建排查表后自动发送通知。';selectedPeople.value=[];Object.assign(publish,{name:'',deadline:null,reviewer_id:null,people:[],items:[{title:'',values:{}}]});}
 else{const f=c.action==='delete_field'?c.item:fieldEditor.value;template.value=await requestJson(root+'/template',{method:'POST',body:JSON.stringify({action:c.action==='delete_field'?'delete':f.key?'update':'add',version:template.value?.version,field_id:f.key,field_name:f.field_name,type:f.type,role:f.role,required:f.required,options:(f.optionsText||'').split('\n').map((s:string)=>s.trim()).filter(Boolean),confirmed:true}),timeoutMs:60000});fieldEditor.value=null;message.value='模板已保存，已发布任务不受影响。';}
 confirmation.value=null;
 }catch(e:any){error.value=e.message;}finally{busy.value=false;}
}
async function uploadFiles(item:Dict,field:string,files:File[]){if(busy.value||!editable(item))return;beginWrite();error.value='';try{for(const file of files){if(file.size>20*1024*1024)throw new Error(file.name+'超过20MB');const q=new URLSearchParams({name:file.name,item_id:item.id,field_id:field});const saved=await requestJson(`${root}/${detail.value!.id}/files?${q}`,{method:'POST',body:file,headers:{'Content-Type':'application/octet-stream'},timeoutMs:120000});item.files.push(saved);const current=drafts.value[item.id]||item.values;drafts.value[item.id]={...current,[field]:[...(current[field]||[]),saved.id]};}}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
async function retry(){if(busy.value||!detail.value)return;beginWrite();error.value='';try{applyDetail(await requestJson(root+'/'+detail.value.id+'/retry',{method:'POST',body:'{}'}));confirmation.value=null;message.value='已安排重试，原操作人和反馈内容保持不变。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
function leaving(e:BeforeUnloadEvent){if(unsaved.value||busy.value){e.preventDefault();e.returnValue='';}}
const unregister=registerNavigationGuard(()=>!busy.value&&(!unsaved.value||window.confirm('有未保存内容，是否放弃并离开？')));
onMounted(async()=>{await reload();if(disposed)return;if(props.releaseId)await openTask(props.releaseId);if(disposed)return;window.addEventListener('beforeunload',leaving);timer=window.setInterval(()=>{const editing=document.activeElement?.closest('.risk-page input,.risk-page textarea,.risk-feedback input,.risk-feedback textarea,[role="listbox"]');if(!disposed&&!document.hidden&&!busy.value&&!dirty.value&&!editor.value&&!confirmation.value&&!fieldEditor.value&&!editing)void reload();},15000);});
onBeforeUnmount(()=>{disposed=true;read?.abort();unregister();window.clearInterval(timer);window.removeEventListener('beforeunload',leaving);});
</script>
<style scoped>
.risk-page{color:var(--text-color,#243244);display:grid;gap:14px;min-width:0;padding:16px;max-width:1600px;margin:auto}.risk-toolbar,.risk-tools{display:flex;align-items:center;gap:8px}.risk-toolbar{flex-wrap:wrap;justify-content:space-between}.risk-toolbar>strong{font-size:17px}.risk-toolbar>.risk-tools{margin-left:auto}.risk-page :deep(.n-alert){margin:0}.risk-note,.risk-summary,small{color:var(--text-color-3,#667085);font-size:13px}.risk-summary a{display:flex;gap:5px;align-items:center;color:var(--primary-color,#2563eb)}.risk-task-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,340px),1fr));gap:12px;margin-bottom:16px}.risk-task-list>button{border:1px solid var(--border-color,#dce2e9);background:var(--card-color,#fff);color:inherit;border-radius:8px;padding:16px;text-align:left;cursor:pointer}.risk-task-list>button>div{display:flex;justify-content:space-between;gap:8px}.risk-task-list strong{font-size:15px;overflow-wrap:anywhere}.risk-task-list p{margin:10px 0 6px}.overdue{color:#c94b42}.risk-detail-layout{display:grid;grid-template-columns:230px minmax(0,1fr);gap:18px;margin-top:14px}.risk-people{display:flex;flex-direction:column;gap:6px;align-self:start}.risk-people>button{text-align:left;border:1px solid transparent;background:transparent;color:inherit;padding:12px;border-radius:6px;cursor:pointer}.risk-people>button>div{display:flex;justify-content:space-between;gap:8px;margin-bottom:5px}.risk-people>button.selected{background:color-mix(in srgb,var(--primary-color,#2563eb) 10%,transparent);border-color:var(--primary-color,#2563eb)}.risk-feedback{min-width:0}.risk-response{border-top:1px solid var(--border-color,#dce2e9);padding:18px 0}.risk-response>header{display:flex;align-items:center;gap:8px;margin-bottom:14px;flex-wrap:wrap}.risk-response>header strong{flex:1}.risk-response>footer,.risk-footer{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:16px;flex-wrap:wrap}.risk-response.revoked{opacity:.65}.risk-sync{display:flex;align-items:center;flex-wrap:wrap;gap:8px;background:color-mix(in srgb,#dca426 10%,transparent);padding:10px 12px;margin:10px 0;border-radius:6px}.risk-sync details{flex-basis:100%}.risk-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.risk-fields label{display:flex;flex-direction:column;gap:6px;min-width:0}.risk-publish{display:grid;gap:16px}.risk-publish-item{padding:16px 0;border-top:1px solid var(--border-color,#dce2e9)}.risk-publish-item>.risk-toolbar{flex-wrap:nowrap}.risk-publish-item details{margin-top:12px}.risk-publish-item summary,.risk-audit summary{cursor:pointer;font-size:13px;margin-bottom:12px}.risk-recipient-editor{display:flex;flex-wrap:wrap;gap:10px}.risk-recipient-editor>div{display:flex;align-items:center;gap:8px}.risk-recipient-editor :deep(.n-select){width:115px}.risk-schema article{display:flex;align-items:center;gap:10px;padding:12px 0;border-bottom:1px solid var(--border-color,#dce2e9)}.risk-schema article>div{flex:1;display:grid;gap:3px}.risk-audit{border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.wide{grid-column:1/-1}.risk-modal{width:min(660px,calc(100vw - 24px));max-height:90dvh;overflow:auto}.risk-confirm{width:min(480px,calc(100vw - 24px))}.risk-modal :deep(.n-card__footer){display:flex;justify-content:flex-end}.risk-tools{flex-wrap:wrap}.risk-page button{transition:background-color .15s,border-color .15s}.risk-page button:focus-visible{outline:2px solid var(--primary-color,#2563eb);outline-offset:2px}@media(max-width:700px){.risk-page{padding:10px;gap:10px}.risk-detail-layout{grid-template-columns:1fr;gap:10px}.risk-people{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));max-height:210px;overflow:auto}.risk-people>:first-child{grid-column:1/-1}.risk-people>button{padding:8px}.risk-fields{grid-template-columns:1fr}.risk-toolbar>strong{font-size:15px}.risk-response{padding:14px 0}.risk-response>footer .risk-tools{width:100%;justify-content:flex-end}.risk-footer{position:sticky;bottom:0;background:var(--card-color,#fff);padding:12px 0}.risk-task-list{gap:8px}.risk-task-list>button{padding:12px}}@media(prefers-reduced-motion:reduce){.risk-page button{transition:none}}
</style>
<style scoped>
.risk-local-draft{margin-top:12px;border:1px solid var(--border-color,#dce2e9);border-radius:6px;padding:12px}.risk-local-draft summary{cursor:pointer;margin-bottom:10px}
:global(.risk-modal.n-modal){width:min(660px,calc(100vw - 24px));max-height:90dvh;overflow:auto}
:global(.risk-confirm.n-modal){width:min(480px,calc(100vw - 24px))}
.risk-page{--text-color:var(--v2-ink,#243244);--text-color-3:var(--v2-muted,#667085);--border-color:var(--v2-border,#dce2e9);--card-color:var(--cf-surface,#fff);font-size:14px}.risk-detail-layout.solo{grid-template-columns:1fr}.risk-detail-layout.solo>.risk-people{display:none}@media(max-width:700px){.risk-page :deep(.n-button){min-height:40px}.risk-page :deep(.n-button--primary-type){min-height:44px}}
</style>
<style scoped>
.risk-page{grid-template-columns:minmax(0,1fr);width:100%;box-sizing:border-box}.risk-page :deep(.n-spin-container),.risk-page :deep(.n-spin-content){min-width:0}
.risk-publish>.risk-fields{grid-template-columns:minmax(220px,1.5fr) minmax(180px,1fr) minmax(160px,1fr) minmax(200px,1.2fr)}
@media(max-width:1100px){.risk-publish>.risk-fields{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:700px){.risk-publish>.risk-fields{grid-template-columns:1fr}}
.risk-page{gap:10px}.risk-task-list{display:block;min-width:0;margin-bottom:10px}.risk-task-list :deep(.risk-task-name){height:auto;white-space:normal;text-align:left;line-height:1.6}.risk-task-list :deep(.risk-task-name .n-button__content){white-space:normal;overflow-wrap:anywhere;justify-content:flex-start}.risk-task-list :deep(.n-data-table-th){font-size:12px}.risk-list-footer{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:12px;color:var(--text-color-3,#667085)}.risk-summary{border-bottom:1px solid var(--border-color,#dce2e9);padding:4px 0 10px;gap:8px}.risk-feedback{display:grid;gap:12px;color:var(--v2-ink,#243244)}.risk-feedback .risk-response{padding:10px 0 0}.risk-feedback .risk-response>footer{position:sticky;bottom:0;background:var(--cf-surface,#fff);padding:12px 0 0;border-top:1px solid var(--v2-border,#dce2e9)}.risk-feedback .risk-toolbar{gap:8px}.risk-feedback .risk-response>header{margin-bottom:12px}.risk-feedback .risk-response>footer small{font-size:12px}.risk-feedback :deep(.n-button){min-height:36px}.risk-audit{font-size:12px}
@media(max-width:700px){.risk-page{padding:10px 8px}.risk-toolbar{gap:8px}.risk-summary>span{line-height:1.6;font-size:12px}.risk-toolbar>.risk-tools{gap:6px}.risk-toolbar>.risk-tools .n-button{padding-inline:10px}.risk-feedback :deep(.n-button){min-height:44px}.risk-feedback .risk-response>footer{gap:8px;padding-bottom:env(safe-area-inset-bottom)}.risk-feedback .risk-response>footer>.risk-tools{width:auto;margin-left:auto}.risk-list-footer{justify-content:flex-end}}
</style>
