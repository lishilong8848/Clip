<template>
  <NCard :bordered="false" class="v2-notice-editor v2-sop-manager is-page" title="SOP 与工单模板" data-v2-inline-feedback>
    <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }} <NButton text :disabled="busy" @click="leave(()=>{draft=null;cache.clear();load()})">重新读取列表</NButton></NAlert>
    <div class="v2-sop-filters"><NSelect :value="scope" :options="scopeChoices" aria-label="SOP楼栋" :disabled="busy" @update:value="v=>leave(()=>{scope=v;draft=null;load()})" /><NSelect :value="kind" :options="typeChoices" aria-label="SOP类型" :disabled="busy" @update:value="v=>leave(()=>{kind=v;draft=null;load()})" /><NInput v-model:value="search" clearable placeholder="搜索SOP名称" :input-props="{'aria-label':'查找SOP'}" /></div>
    <div class="v2-sop-layout">
      <aside>
        <div class="v2-notice-tools"><NButton :disabled="busy" data-v2-navigation @click="leave(create)"><Plus :size="16" />新增</NButton></div>
        <NSpin :show="loading"><NEmpty v-if="!visible.length&&!loading" :description="error?'SOP读取失败，请重试':'暂无SOP'" /><button v-for="item in visible" :key="item.sop_id" type="button" data-v2-navigation class="v2-sop-item" :class="{selected:draft?.sop_id===item.sop_id}" :disabled="busy" @click="leave(()=>select(item))"><strong>{{ item.name }}</strong><span>{{ scopeName(item.scope) }} · {{ typeName(item.work_type) }} · {{ item.steps?.length||0 }} 步 · {{ item.attachments?.length||0 }} 个附件</span></button></NSpin>
      </aside>
      <NForm v-if="draft" label-placement="top" :disabled="busy||!editing" class="v2-sop-content" :class="{'is-readonly':!editing}" @submit.prevent="save">
        <div class="v2-notice-field-grid"><NFormItem label="SOP名称" required><NInput v-model:value="draft.name" :disabled="busy||!editing" maxlength="160" :input-props="{'aria-label':'SOP名称'}" /></NFormItem><NFormItem label="所属楼栋" required><NSelect v-model:value="draft.scope" :options="scopes" :disabled="busy||!editing||!!draft.sop_id" placeholder="选择具体楼栋" aria-label="SOP所属楼栋" /></NFormItem><NFormItem label="适用类型"><NSelect v-model:value="draft.work_type" :disabled="busy||!editing" :options="types" aria-label="适用类型" /></NFormItem></div>
        <NAlert v-if="draft.sop_id&&draft.work_type!==originalType" type="warning" :bordered="false">保存将转换该 SOP 的适用类型，已有工单快照不变。</NAlert>
        <section v-for="(step,index) in draft.steps" :key="step.step_id" class="v2-sop-step">
          <header><h3>步骤 {{ index+1 }}</h3><NButton v-if="editing" quaternary circle :disabled="busy||draft.steps.length===1" :aria-label="'删除步骤'+(index+1)" @click="removeStep(index)"><Trash2 :size="16" /></NButton></header>
          <NInput v-model:value="step.content" :disabled="busy||!editing" type="textarea" :autosize="{minRows:2,maxRows:6}" :maxlength="5000" :placeholder="draft.work_type==='polling'?'操作内容，可使用 {{from}}、{{to}}、{{other}}':'操作内容'" :input-props="{'aria-label':'步骤'+(index+1)+'内容'}" />
          <div class="v2-sop-step-settings"><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.operator_required">操作人确认</NCheckbox><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.reviewer_required">审核人确认</NCheckbox><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.photo_required">操作照片</NCheckbox></div>
          <div class="v2-notice-field-grid"><NFormItem label="时限（秒，0为不限）"><NInputNumber v-model:value="step.time_limit_seconds" :disabled="busy||!editing" :min="0" :max="86400" :precision="0" :input-props="{'aria-label':'步骤'+(index+1)+'时限秒数'}" /></NFormItem><NFormItem label="完成后提醒（分钟，0为关闭）"><NInputNumber v-model:value="step.delay_reminder_minutes" :disabled="busy||!editing" :min="0" :max="1440" :precision="0" :input-props="{'aria-label':'步骤'+(index+1)+'延时提醒分钟'}" /></NFormItem></div>
          <div v-for="(rule,ri) in step.repeat_rules" :key="rule.rule_id" class="v2-sop-repeat"><NSelect v-model:value="rule.from_step_id" :disabled="busy||!editing" :options="stepOptions(index)" aria-label="循环起始步骤" /><span>至</span><NSelect v-model:value="rule.to_step_id" :disabled="busy||!editing" :options="stepOptions(index)" aria-label="循环结束步骤" /><NInputNumber v-model:value="rule.count" :disabled="busy||!editing" :min="1" :max="10" :precision="0" :input-props="{'aria-label':'总执行遍数（含首次）'}" /><NButton v-if="editing" :disabled="busy" quaternary circle :aria-label="'移除循环'+(ri+1)" @click="step.repeat_rules.splice(ri,1)"><X :size="15" /></NButton></div>
          <NButton v-if="editing" text :disabled="busy||step.repeat_rules.length>=10" @click="step.repeat_rules.push({rule_id:createRepairOperationId('rule'),from_step_id:step.step_id,to_step_id:step.step_id,count:1})">添加循环（总遍数含首次）</NButton>
        </section>
        <NButton v-if="editing" :disabled="busy||draft.steps.length>=200" @click="draft.steps.push(newStep())"><Plus :size="16" />添加步骤</NButton>
        <section class="v2-notice-field-section"><h2>SOP 附件</h2><NAlert v-if="!draft.attachments?.length" type="warning" :bordered="false">需先保存步骤，再上传附件，才能用于生成工单。</NAlert><div v-for="file in draft.attachments||[]" :key="file.attachment_id" class="v2-sop-file"><NButton text @click="downloadFile(attachmentPath(file)).catch(e=>error=e.message)">{{ file.file_name || file.name }}</NButton><NButton v-if="editing" :disabled="busy||dirty" quaternary circle aria-label="删除SOP附件" @click="deleteFile(file)"><Trash2 :size="16" /></NButton></div><label v-if="editing" class="v2-notice-upload"><Upload :size="18" /><span>{{ busy?(uploadStatus||'正在处理…'):'添加附件（单个及累计200MB以内）' }}</span><input type="file" multiple :disabled="busy||dirty||!draft.sop_id" aria-label="添加SOP附件" @change="upload" /></label></section>
      </NForm><NEmpty v-else description="选择或新建SOP" />
    </div>
    <template #header-extra><div class="v2-sop-actions"><NButton v-if="editing&&draft?.sop_id&&canDelete" type="error" quaternary circle :disabled="busy" title="删除SOP" aria-label="删除SOP" @click="remove"><Trash2 :size="17" /></NButton><NButton v-if="draft" :disabled="busy" data-v2-navigation @click="close"><template #icon><ArrowLeft :size="16" /></template>返回SOP列表</NButton><NButton v-if="draft&&!editing" type="primary" :disabled="busy||loading" @click="editing=true"><PencilLine :size="16" />修改</NButton><template v-if="editing"><NButton :disabled="busy" @click="cancelEdit">取消修改</NButton><NButton type="primary" :loading="busy" :disabled="!draft||!dirty||uncertain" @click="confirmSave"><template #icon><Save :size="16" /></template>保存SOP</NButton></template></div></template>
  </NCard>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NCard,NCheckbox,NEmpty,NForm,NFormItem,NInput,NInputNumber,NSelect,NSpin,useDialog,useMessage} from 'naive-ui';
import {ArrowLeft,PencilLine,Plus,Save,Trash2,Upload,X} from 'lucide-vue-next';
import {ApiError,requestJson,uploadJson,downloadFile,type Dict} from './client';
import {registerNavigationGuard} from './navigation';
import {createRepairOperationId} from '../repairManagementUtils';
import {trackButtonRequest} from './actionMotion';
const props=defineProps<{initialScope:string;initialType:string;scopes:{value:string;label:string}[];types:{value:string;label:string}[];canDelete:boolean}>();
const emit=defineEmits<{changed:[]}>(),dialog=useDialog(),message=useMessage();
const scope=ref('ALL'),kind=ref('all');
const scopeChoices=computed(()=>[{value:'ALL',label:'全部可访问楼栋'},...props.scopes]);
const typeChoices=computed(()=>[{value:'all',label:'全部通告类型'},...props.types]);
const scopeName=(value:string)=>props.scopes.find(s=>s.value===value)?.label||value;
const typeName=(value:string)=>props.types.find(t=>t.value===value)?.label||value;
const items=ref<Dict[]>([]),draft=ref<Dict|null>(null),baseline=ref(''),originalType=ref(''),loading=ref(false),busy=ref(false),error=ref(''),search=ref(''),uncertain=ref(false);
const editing=ref(false);
const uploadStatus=ref('');
const dirty=computed(()=>!!draft.value&&JSON.stringify(draft.value)!==baseline.value),visible=computed(()=>items.value.filter(s=>String(s.name).includes(search.value)));
let read:AbortController|undefined;
const cache=new Map<string,Dict[]>();
const newStep=()=>({step_id:createRepairOperationId('step'),content:'',operator_required:true,reviewer_required:true,photo_required:true,time_limit_seconds:0,delay_reminder_minutes:0,repeat_rules:[]});
function select(item:Dict,edit=false){editing.value=edit;draft.value=JSON.parse(JSON.stringify(item));for(const step of draft.value!.steps||[])step.repeat_rules??=[];baseline.value=JSON.stringify(draft.value);originalType.value=item.work_type;uncertain.value=false;if(item.sop_id){const key=item.scope+':'+item.work_type,rows=cache.get(key);if(rows)cache.set(key,rows.map(row=>row.sop_id===item.sop_id?item:row));items.value=items.value.map(row=>row.sop_id===item.sop_id?item:row);}}
function create(){select({sop_id:'',scope:scope.value==='ALL'?(props.scopes.length===1?props.scopes[0].value:''):scope.value,work_type:kind.value==='all'?(props.types.find(t=>t.value===props.initialType)?.value||props.types[0]?.value||'maintenance'):kind.value,name:'',version:0,steps:[newStep()],attachments:[]},true);baseline.value='';}
function leave(fn:()=>void){if(busy.value)return;if(dirty.value){dialog.warning({title:'放弃未保存的SOP修改？',positiveText:'放弃修改',negativeText:'继续编辑',onPositiveClick:fn});}else fn();}
function cancelEdit(){leave(()=>{if(draft.value?.sop_id)select(JSON.parse(baseline.value));else{draft.value=null;editing.value=false;}});}
function close(){leave(()=>{draft.value=null;editing.value=false;baseline.value='';});}
async function load(){
  if(busy.value||!scope.value)return;read?.abort();const c=new AbortController();read=c;loading.value=true;error.value='';items.value=[];
  const targets=(scope.value==='ALL'?props.scopes:props.scopes.filter(s=>s.value===scope.value)).flatMap(s=>(kind.value==='all'?props.types:props.types.filter(t=>t.value===kind.value)).map(t=>({scope:s.value,work_type:t.value})));
  try{
    const results=await Promise.all(targets.map(async target=>{
      const key=target.scope+':'+target.work_type;if(cache.has(key))return cache.get(key)!;
      const result=await requestJson('/api/polling-sops?'+new URLSearchParams(target),{signal:c.signal,cache:'no-store'});
      if(!Array.isArray(result.items))throw new Error('SOP列表读取不完整，请重试。');
      if(!c.signal.aborted)cache.set(key,result.items);return result.items as Dict[];
    }));
    if(!c.signal.aborted)items.value=results.flat().sort((a,b)=>String(a.name).localeCompare(String(b.name),'zh-CN'));
  }catch(e){if(!c.signal.aborted)error.value=(e as Error).message;}finally{if(read===c){loading.value=false;busy.value=false;}}
}
function stepOptions(index:number){return draft.value!.steps.slice(0,index+1).map((s:Dict,i:number)=>({label:'第'+(i+1)+'步',value:s.step_id}));}
function removeStep(index:number){if(!editing.value||busy.value)return;const id=draft.value!.steps[index].step_id;draft.value!.steps.splice(index,1);for(const s of draft.value!.steps)s.repeat_rules=s.repeat_rules.filter((r:Dict)=>r.from_step_id!==id&&r.to_step_id!==id);}
function confirmSave(){if(busy.value||uncertain.value)return;if(draft.value?.sop_id&&draft.value.work_type!==originalType.value){dialog.warning({title:'确认转换SOP类型',content:'此模板将归入所选类型，已有工单保持不变。',positiveText:'确认保存',negativeText:'返回修改',onPositiveClick:()=>save()});}else return save();}
function save(){if(!editing.value)return;return trackButtonRequest(saveNow());}
async function saveNow(){if(!editing.value||!draft.value||busy.value)return {ok:false};error.value='';if(!props.scopes.some(s=>s.value===draft.value!.scope)){error.value='请选择有权限的具体楼栋。';return {ok:false};}if(!draft.value.name?.trim()||!draft.value.steps.length||draft.value.steps.some((s:Dict)=>!s.content?.trim())){error.value='请填写SOP名称和每一步操作内容。';return {ok:false};}busy.value=true;try{const d=draft.value;const saved=await requestJson('/api/polling-sops'+(d.sop_id?'/'+encodeURIComponent(d.sop_id):''),{method:d.sop_id?'PUT':'POST',body:JSON.stringify({sop_id:d.sop_id,scope:d.scope,work_type:d.work_type,name:d.name,expected_version:d.version,steps:d.steps})});select(saved);cache.clear();emit('changed');message.success('SOP已保存');busy.value=false;void load();return saved;}catch(e){error.value=(e as Error).message;if(!(e instanceof ApiError)||!e.status||e.status>=500){uncertain.value=true;error.value+=' 保存结果尚未确认，请重新读取列表核验原SOP，不要重复新建。';}return {ok:false};}finally{busy.value=false;}}
const attachmentPath=(file:Dict)=>'/api/polling-sops/'+encodeURIComponent(draft.value!.sop_id)+'/attachments/'+encodeURIComponent(file.attachment_id);
async function upload(event:Event){
  const input=event.target as HTMLInputElement,files=[...input.files||[]];input.value='';
  if(!files.length||!editing.value||busy.value||dirty.value||!draft.value?.sop_id)return;
  const existing=draft.value.attachments||[],maxBytes=200*1024*1024;
  if(files.some(file=>!file.size||file.size>maxBytes)){error.value='请选择非空且不超过200MB的附件。';return;}
  if(existing.length+files.length>10){error.value='每个SOP最多上传10个附件。';return;}
  if([...existing,...files].reduce((sum,file)=>sum+Number(file.size||0),0)>maxBytes){error.value='SOP附件总大小不能超过200MB。';return;}
  busy.value=true;error.value='';
  try{
    for(const file of files){
      uploadStatus.value='正在上传 '+file.name+'…';
      const body=new FormData();body.append('file',file);body.append('expected_version',String(draft.value!.version));
      select(await uploadJson('/api/polling-sops/'+encodeURIComponent(draft.value!.sop_id)+'/attachments',body,{
        onProgress:({loaded,total})=>{uploadStatus.value=loaded>=total?'正在保存附件…':'正在上传 '+Math.round(loaded/total*100)+'%';}
      }),true);emit('changed');
    }
  }catch(e){error.value=(e as Error).message;}finally{uploadStatus.value='';busy.value=false;}
}
function deleteFile(file:Dict){if(!editing.value||busy.value)return;dialog.warning({title:'删除此附件？',content:file.file_name||file.name,positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{busy.value=true;try{select(await requestJson(attachmentPath(file)+'?expected_version='+draft.value!.version,{method:'DELETE'}),true);emit('changed');}catch(e){error.value=(e as Error).message;return false;}finally{busy.value=false;}}});}
function remove(){if(!editing.value||busy.value||!props.canDelete)return;const d=draft.value!;dialog.error({title:'删除SOP',content:d.name,positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{busy.value=true;try{await requestJson('/api/polling-sops/'+encodeURIComponent(d.sop_id)+'?expected_version='+d.version,{method:'DELETE'});draft.value=null;editing.value=false;cache.clear();emit('changed');busy.value=false;void load();}catch(e){error.value=(e as Error).message;return false;}finally{busy.value=false;}}});}
const stop=registerNavigationGuard((_url,proceed)=>{if(busy.value){message.warning('正在保存SOP，请等待结果');return false;}if(dirty.value){leave(proceed);return false;}return true;});
function unload(event:BeforeUnloadEvent){if(busy.value||dirty.value){event.preventDefault();event.returnValue='';}}
onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});onBeforeUnmount(()=>{read?.abort();stop();window.removeEventListener('beforeunload',unload);});
</script>
<style>
@media(max-width:760px){.v2-sop-manager.is-page>.n-card-header>.n-card-header__main{flex:1 0 100%;min-width:0;white-space:nowrap}.workspace-v2 .v2-sop-manager.is-page>.n-card-header{top:var(--head-height,56px)}}
.v2-sop-manager.is-page.n-card{width:100%;max-width:none;max-height:none;min-height:calc(100dvh - 140px);border:0;border-radius:0;background:transparent;box-shadow:none}.v2-sop-manager.is-page>.n-card-header{position:sticky;top:0;z-index:10;gap:12px;padding:12px 0 16px;background:var(--v2-bg,#eef2ef)}.v2-sop-manager.is-page>.n-card-content{padding:0;overflow:visible}.v2-sop-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px}.v2-sop-actions>.n-button{flex:none;min-height:36px}.v2-sop-manager.is-page .v2-sop-layout>aside{position:sticky;top:76px;max-height:calc(100dvh - 210px);overflow-y:auto}.v2-sop-manager.is-page .v2-sop-layout{grid-template-columns:minmax(220px,26%) minmax(0,1fr)}@media(max-width:760px){.v2-sop-manager.is-page .v2-sop-layout{grid-template-columns:1fr;gap:16px}.v2-sop-manager.is-page .v2-sop-layout>aside{position:static;max-height:none}.v2-sop-manager.is-page>.n-card-header{flex-wrap:wrap;padding:10px 0 12px;gap:8px}.v2-sop-manager.is-page>.n-card-header>.n-card-header__extra{margin-left:auto}.v2-sop-actions{gap:6px}.v2-sop-actions>.n-button{min-height:40px}}
.v2-sop-layout{display:grid;grid-template-columns:220px minmax(0,1fr);gap:24px}.v2-sop-layout>aside{display:flex;flex-direction:column;gap:10px;align-self:start}.v2-sop-item{width:100%;display:grid;gap:5px;padding:12px;border:0;border-bottom:1px solid var(--v2-border);background:transparent;text-align:left;color:var(--v2-ink);cursor:pointer}.v2-sop-item.selected{background:#eaf3ff}.v2-sop-item span{font-size:12px;color:var(--v2-muted)}.v2-sop-content{min-width:0}.v2-sop-step{padding:14px 0;border-top:1px solid var(--v2-border)}.v2-sop-step header,.v2-sop-file{display:flex;align-items:center;justify-content:space-between;gap:10px}.v2-sop-step h3{font-size:14px;margin:0 0 10px}.v2-sop-step-settings{display:flex;flex-wrap:wrap;gap:12px;margin:12px 0}.v2-sop-repeat{display:grid;grid-template-columns:minmax(0,1fr) auto minmax(0,1fr) 115px 30px;align-items:center;gap:6px;margin-bottom:10px}.v2-sop-file .n-button__content{white-space:normal;overflow-wrap:anywhere}
@media(max-width:760px){.v2-sop-layout{grid-template-columns:1fr}.v2-sop-layout>aside .n-spin-container{max-height:160px;overflow:auto}.v2-sop-repeat{grid-template-columns:minmax(0,1fr) auto minmax(0,1fr)}.v2-sop-repeat .n-input-number{grid-column:1/3}}
</style>
