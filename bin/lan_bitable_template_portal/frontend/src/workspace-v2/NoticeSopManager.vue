<template>
  <!-- v2-compact-adapted: PageCanvas mounts layout controls in this toolbar. -->
  <section class="sop-desk" data-v2-inline-feedback>
    <div class="sop-desk__toolbar">
      <h1 class="sop-desk__page-title">SOP 与工单模板</h1>
      <div class="sop-desk__selectors">
        <NSelect size="small" :value="scope" :options="scopeChoices" aria-label="SOP楼栋" :disabled="busy" @update:value="v=>leave(()=>{scope=v;draft=null;load()})" />
        <NSelect size="small" :value="kind" :options="typeChoices" aria-label="SOP类型" :disabled="busy" @update:value="v=>leave(()=>{kind=v;draft=null;load()})" />
      </div>
      <NInput size="small" class="sop-desk__search" v-model:value="search" clearable placeholder="搜索SOP名称" :input-props="{'aria-label':'查找SOP'}" />
      <NButton size="small" type="primary" :disabled="busy" data-v2-navigation @click="leave(create)"><template #icon><Plus :size="16" /></template>新增SOP</NButton>
      <span class="sop-desk__layout-tools" data-v2-layout-tools="sops"></span>
    </div>
    <NAlert v-if="error" class="sop-desk__error" role="alert" type="error" :bordered="false">{{ error }} <NButton text :disabled="busy" @click="leave(()=>{draft=null;cache.clear();load()})">重新读取列表</NButton></NAlert>
    <PageCanvas page="sops" label="SOP模板" :class="{'sop-catalog':!draft}" :blocks="[{id:'templates',title:'SOP列表',w:4,h:9,minW:4,minH:4},{id:'editor',title:'SOP步骤与附件',w:8,h:12,minW:4,minH:4}]">
      <template #templates>
        <section class="sop-desk__block" aria-label="SOP列表">
          <header class="sop-desk__block-head">
            <h2 class="sop-desk__block-title">SOP 列表 <span class="sop-desk__count">{{ visible.length }}</span></h2>
          </header>
          <div class="sop-desk__list">
            <NSpin :show="loading">
              <NEmpty v-if="!visible.length&&!loading" :description="error?'SOP读取失败，请重试':'暂无SOP'" />
              <div v-else class="sop-desk__cards">
                <button v-for="item in visible" :key="item.sop_id" type="button" data-v2-navigation class="sop-desk__card" :class="{selected:draft?.sop_id===item.sop_id}" :disabled="busy" :aria-current="draft?.sop_id===item.sop_id?'true':undefined" @click="leave(()=>select(item))">
                  <strong class="sop-desk__card-name">{{ item.name }}</strong>
                  <span class="sop-desk__card-meta">{{ scopeName(item.scope) }} · {{ typeName(item.work_type) }}</span>
                  <span class="sop-desk__card-stats"><span>{{ item.steps?.length||0 }} 步</span><span>{{ item.attachments?.length||0 }} 个附件</span></span>
                </button>
              </div>
            </NSpin>
          </div>
        </section>
      </template>
      <template #editor>
        <section class="sop-desk__block" aria-label="SOP步骤与附件">
          <header class="sop-desk__block-head">
            <div class="sop-desk__titles">
              <h2 class="sop-desk__block-title">{{ draft?(draft.sop_id?(draft.name||'未命名SOP'):'新建 SOP'):'SOP 编辑器' }}</h2>
              <p v-if="draft" class="sop-desk__sub">{{ scopeName(draft.scope) }} · {{ typeName(draft.work_type) }}<template v-if="draft.sop_id"> · 版本 {{ draft.version }}</template><template v-if="editing"> · {{ uncertain?'保存待核验':busy?'处理中':dirty?'有未保存修改':'编辑中' }}</template></p>
            </div>
            <div class="sop-desk__head-actions">
              <NButton v-if="!editing&&draft" type="primary" :disabled="busy||loading" data-v2-navigation @click="editing=true"><template #icon><PencilLine :size="16" /></template>修改</NButton>
              <NButton v-if="editing&&draft?.sop_id&&canDelete" type="error" quaternary circle :disabled="busy" title="删除SOP" aria-label="删除SOP" @click="remove"><Trash2 :size="17" /></NButton>
              <template v-if="editing"><NButton :disabled="busy" @click="cancelEdit">取消修改</NButton><NButton type="primary" :loading="busy" :disabled="!draft||!dirty||uncertain" @click="confirmSave"><template #icon><Save :size="16" /></template>保存SOP</NButton></template>
            </div>
          </header>
          <NForm v-if="draft" label-placement="top" :disabled="busy||!editing" class="sop-desk__form" :class="{'is-readonly':!editing}" @submit.prevent="confirmSave">
            <section class="sop-desk__section" aria-label="基本信息">
              <h3 class="sop-desk__section-title">基本信息</h3>
              <div class="sop-desk__grid sop-desk__grid--3">
                <NFormItem label="SOP名称" required><NInput v-model:value="draft.name" :disabled="busy||!editing" maxlength="160" :input-props="{'aria-label':'SOP名称'}" /></NFormItem>
                <NFormItem label="所属楼栋" required><NSelect v-model:value="draft.scope" :options="scopes" :disabled="busy||!editing||!!draft.sop_id" placeholder="选择具体楼栋" aria-label="SOP所属楼栋" /></NFormItem>
                <NFormItem label="适用类型"><NSelect v-model:value="draft.work_type" :disabled="busy||!editing" :options="types" aria-label="适用类型" /></NFormItem>
              </div>
              <NFormItem v-if="draft.work_type==='adjust'" label="调整 SOP 类型" class="sop-desk__inline-field"><NSelect :value="adjustSopType(draft)" :options="adjustTypeChoices" :disabled="busy||!editing" aria-label="调整SOP类型" @update:value="value=>{if(draft)draft.adjust_sop_type=value}" /></NFormItem>
              <NAlert v-if="draft.sop_id&&draft.work_type!==originalType" type="warning" :bordered="false">保存将转换该 SOP 的适用类型，已有工单快照不变。</NAlert>
            </section>
            <section class="sop-desk__section" aria-label="操作步骤">
              <div class="sop-desk__section-head">
                <h3 class="sop-desk__section-title">操作步骤 <span class="sop-desk__count">{{ draft.steps.length }}</span></h3>
                <NButton v-if="editing" size="small" :disabled="busy||draft.steps.length>=200" @click="appendStep"><template #icon><Plus :size="15" /></template>添加步骤</NButton>
              </div>
              <NEmpty v-if="!draft.steps.length" description="尚无步骤" />
              <div v-if="editing && draft.steps.length" class="sop-step-selector" role="group" aria-label="选择编辑步骤">
                <button v-for="(item,n) in draft.steps" type="button" :key="item.step_id" :disabled="busy"
                  :aria-pressed="activeIndex===n" :title="item.content" @click="selectedStep=item.step_id">
                  <b>{{n+1}}</b><span>{{item.content || '待填写操作内容'}}</span>
                </button>
              </div>
              <div v-if="draft.steps.length" class="sop-desk__steps">
                <template v-for="(step,index) in draft.steps" :key="step.step_id">
                <article v-if="!editing || index===activeIndex" class="sop-desk__step" :class="{'is-editing':editing}">
                  <header v-if="editing" class="sop-desk__step-head">
                    <strong>第 {{index+1}} 步 <small>共 {{draft.steps.length}} 步</small></strong>
                    <NButton text :disabled="busy||step.repeat_rules.length>=10" @click="step.repeat_rules.push({rule_id:createRepairOperationId('rule'),from_step_id:step.step_id,to_step_id:step.step_id,count:1})">添加循环</NButton>
                    <NButton quaternary circle size="small" :disabled="busy||draft.steps.length===1" :aria-label="'删除步骤'+(index+1)" @click="deleteActiveStep(index)"><Trash2 :size="16" /></NButton>
                  </header>
                  <div v-if="!editing" class="sop-desk__step-read">
                    <p class="sop-desk__step-content"><span class="sop-desk__step-no" aria-hidden="true">{{ index+1 }}</span>{{ step.content || '（未填写操作内容）' }}</p>
                    <ul class="sop-desk__rules" :aria-label="'第'+(index+1)+'步要求'">
                      <li class="sop-desk__rule-flag" :class="{on:step.operator_required}">{{ step.operator_required?'需操作人确认':'无需操作人确认' }}</li>
                      <li class="sop-desk__rule-flag" :class="{on:step.reviewer_required}">{{ step.reviewer_required?'需审核人确认':'无需审核人确认' }}</li>
                      <li class="sop-desk__rule-flag" :class="{on:step.photo_required}">{{ step.photo_required?'需操作照片':'无需操作照片' }}</li>
                      <li>时限：{{ step.time_limit_seconds>0?(step.time_limit_seconds+' 秒'):'不限' }}</li>
                      <li>完成后提醒：{{ step.delay_reminder_minutes>0?(step.delay_reminder_minutes+' 分钟'):'关闭' }}</li>
                      <li v-for="rule in step.repeat_rules||[]" :key="rule.rule_id">循环：{{ stepLabel(rule.from_step_id) }} 至 {{ stepLabel(rule.to_step_id) }} · 共 {{ rule.count }} 遍（含首次）</li>
                      <li v-if="!step.repeat_rules?.length">无循环</li>
                    </ul>
                  </div>
                  <div v-else class="sop-desk__step-edit">
                    <NInput v-model:value="step.content" :disabled="busy||!editing" type="textarea" :autosize="{minRows:2,maxRows:6}" :maxlength="5000" :placeholder="draft.work_type==='polling'?'操作内容，可使用 {{from}}、{{to}}、{{other}}':'操作内容'" :input-props="{'aria-label':'步骤'+(index+1)+'内容'}" />
                    <div class="sop-desk__step-settings"><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.operator_required">操作人确认</NCheckbox><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.reviewer_required">审核人确认</NCheckbox><NCheckbox :disabled="busy||!editing" :aria-disabled="busy||!editing" v-model:checked="step.photo_required">操作照片</NCheckbox></div>
                    <div class="sop-desk__grid sop-desk__grid--2">
                      <NFormItem label="时限（秒，0为不限）"><NInputNumber v-model:value="step.time_limit_seconds" :disabled="busy||!editing" :min="0" :max="86400" :precision="0" :input-props="{'aria-label':'步骤'+(index+1)+'时限秒数'}" /></NFormItem>
                      <NFormItem label="完成后提醒（分钟，0为关闭）"><NInputNumber v-model:value="step.delay_reminder_minutes" :disabled="busy||!editing" :min="0" :max="1440" :precision="0" :input-props="{'aria-label':'步骤'+(index+1)+'延时提醒分钟'}" /></NFormItem>
                    </div>
                    <div v-for="(rule,ri) in step.repeat_rules" :key="rule.rule_id" class="sop-desk__repeat">
                      <NSelect v-model:value="rule.from_step_id" :disabled="busy||!editing" :options="stepOptions(index)" aria-label="循环起始步骤" />
                      <span class="sop-desk__repeat-joiner">至</span>
                      <NSelect v-model:value="rule.to_step_id" :disabled="busy||!editing" :options="stepOptions(index)" aria-label="循环结束步骤" />
                      <label class="sop-repeat-count">总遍数（含首次）<NInputNumber v-model:value="rule.count" :disabled="busy||!editing" :min="1" :max="10" :precision="0" :input-props="{'aria-label':'总执行遍数（含首次）'}" /></label>
                      <NButton :disabled="busy" quaternary circle :aria-label="'移除循环'+(ri+1)" @click="step.repeat_rules.splice(ri,1)"><X :size="15" /></NButton>
                    </div>
                  </div>
                </article>
                </template>
              </div>
            </section>
            <section class="sop-desk__section" aria-label="SOP附件">
              <div class="sop-desk__section-head">
                <h3 class="sop-desk__section-title">SOP 附件 <span class="sop-desk__count">{{ draft.attachments?.length||0 }}</span></h3>
                <label v-if="editing" class="sop-desk__upload" :class="{blocked:busy||dirty||!draft.sop_id}"><Upload :size="16" /><span>{{ busy?(uploadStatus||'正在处理…'):'添加附件（单个及累计200MB以内）' }}</span><input type="file" multiple :disabled="busy||dirty||!draft.sop_id" aria-label="添加SOP附件" @change="upload" /></label>
              </div>
              <NAlert v-if="!draft.attachments?.length" type="warning" :bordered="false">需先保存步骤，再上传附件，才能用于生成工单。</NAlert>
              <div v-else class="sop-desk__files">
                <div v-for="file in draft.attachments||[]" :key="file.attachment_id" class="sop-desk__file">
                  <NButton text @click="downloadFile(attachmentPath(file)).catch(e=>error=e.message)">{{ file.file_name || file.name }}</NButton>
                  <NButton v-if="editing" :disabled="busy||dirty" quaternary circle aria-label="删除SOP附件" @click="deleteFile(file)"><Trash2 :size="16" /></NButton>
                </div>
              </div>
            </section>
          </NForm>
          <NEmpty v-else class="sop-desk__blank" description="选择或新建SOP" />
        </section>
      </template>
    </PageCanvas>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NCheckbox,NEmpty,NForm,NFormItem,NInput,NInputNumber,NSelect,NSpin,useDialog,useMessage} from 'naive-ui';
import {PencilLine,Plus,Save,Trash2,Upload,X} from 'lucide-vue-next';
import {ApiError,requestJson,uploadJson,downloadFile,type Dict} from './client';
import {registerNavigationGuard} from './navigation';
import {createRepairOperationId} from '../repairManagementUtils';
import {trackButtonRequest} from './actionMotion';
import {logicalUrl} from './version';
import PageCanvas from './PageCanvas.vue';
const props=defineProps<{initialScope:string;initialType:string;scopes:{value:string;label:string}[];types:{value:string;label:string}[];canDelete:boolean}>();
const emit=defineEmits<{changed:[]}>(),dialog=useDialog(),message=useMessage();
const fromNotice=props.types.some(item=>item.value===props.initialType);
const scope=ref((logicalUrl().searchParams.has('scope')||fromNotice)&&props.scopes.some(item=>item.value===props.initialScope)?props.initialScope:'ALL');
const kind=ref(fromNotice?props.initialType:'all');
const scopeChoices=computed(()=>[{value:'ALL',label:'全部可访问楼栋'},...props.scopes]);
const typeChoices=computed(()=>[{value:'all',label:'全部通告类型'},...props.types]);
const scopeName=(value:string)=>props.scopes.find(s=>s.value===value)?.label||value;
const typeName=(value:string)=>props.types.find(t=>t.value===value)?.label||value;
const adjustTypeChoices=[{value:'normal',label:'普通设备调整 SOP'},{value:'cooling',label:'制冷单元模式切换 SOP'}];
const sopTokens=(sop:Dict)=>['{{from}}','{{to}}','{{other}}'].filter(token=>(sop.steps||[]).some((step:Dict)=>String(step.content||'').includes(token)));
function adjustSopType(sop:Dict){if(['normal','cooling'].includes(sop.adjust_sop_type))return sop.adjust_sop_type;const tokens=sopTokens(sop);return tokens.length===1&&tokens[0]==='{{from}}'?'cooling':'normal';}
const items=ref<Dict[]>([]),draft=ref<Dict|null>(null),baseline=ref(''),originalType=ref(''),loading=ref(false),busy=ref(false),error=ref(''),search=ref(''),uncertain=ref(false);
const editing=ref(false);
const uploadStatus=ref('');
const dirty=computed(()=>!!draft.value&&JSON.stringify(draft.value)!==baseline.value),visible=computed(()=>items.value.filter(s=>String(s.name).includes(search.value)));
let read:AbortController|undefined;
const cache=new Map<string,Dict[]>();
const newStep=()=>({step_id:createRepairOperationId('step'),content:'',operator_required:true,reviewer_required:true,photo_required:true,time_limit_seconds:0,delay_reminder_minutes:0,repeat_rules:[]});
const selectedStep=ref('');
const activeIndex=computed(()=>Math.max(0,draft.value?.steps.findIndex((step:Dict)=>step.step_id===selectedStep.value)??0));
function appendStep(){
  if(!editing.value||busy.value||!draft.value||draft.value.steps.length>=200)return;
  const step=newStep();draft.value.steps.push(step);selectedStep.value=step.step_id;
}
function deleteActiveStep(index:number){
  if(!editing.value||busy.value||!draft.value||draft.value.steps.length<=1)return;
  removeStep(index);
  selectedStep.value=draft.value.steps[Math.min(index,draft.value.steps.length-1)].step_id;
}
function select(item:Dict,edit=false){selectedStep.value=item.steps?.[0]?.step_id||'';editing.value=edit;draft.value=JSON.parse(JSON.stringify(item));if(draft.value!.work_type==='adjust')draft.value!.adjust_sop_type=adjustSopType(draft.value!);for(const step of draft.value!.steps||[])step.repeat_rules??=[];baseline.value=JSON.stringify(draft.value);originalType.value=item.work_type;uncertain.value=false;if(item.sop_id){const key=item.scope+':'+item.work_type,rows=cache.get(key);if(rows)cache.set(key,rows.map(row=>row.sop_id===item.sop_id?item:row));items.value=items.value.map(row=>row.sop_id===item.sop_id?item:row);}}
function create(){select({sop_id:'',scope:scope.value==='ALL'?(props.scopes.length===1?props.scopes[0].value:''):scope.value,work_type:kind.value==='all'?(props.types.find(t=>t.value===props.initialType)?.value||props.types[0]?.value||'maintenance'):kind.value,name:'',version:0,steps:[newStep()],attachments:[]},true);baseline.value='';}
function leave(fn:()=>void){if(busy.value)return;if(dirty.value){dialog.warning({title:'放弃未保存的SOP修改？',positiveText:'放弃修改',negativeText:'继续编辑',onPositiveClick:fn});}else fn();}
function cancelEdit(){leave(()=>{if(draft.value?.sop_id)select(JSON.parse(baseline.value));else{draft.value=null;editing.value=false;}});}
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
function stepLabel(id:string){const index=draft.value!.steps.findIndex((step:Dict)=>step.step_id===id);return index<0?'步骤待核对':'第'+(index+1)+'步';}
function removeStep(index:number){if(!editing.value||busy.value)return;const id=draft.value!.steps[index].step_id;draft.value!.steps.splice(index,1);for(const s of draft.value!.steps)s.repeat_rules=s.repeat_rules.filter((r:Dict)=>r.from_step_id!==id&&r.to_step_id!==id);}
function confirmSave(){if(!editing.value||!draft.value||!dirty.value||busy.value||uncertain.value)return;if(draft.value?.sop_id&&draft.value.work_type!==originalType.value){dialog.warning({title:'确认转换SOP类型',content:'此模板将归入所选类型，已有工单保持不变。',positiveText:'确认保存',negativeText:'返回修改',onPositiveClick:()=>save()});}else return save();}
function save(){if(!editing.value)return;return trackButtonRequest(saveNow());}
async function saveNow(){if(!editing.value||!draft.value||busy.value)return {ok:false};error.value='';if(!props.scopes.some(s=>s.value===draft.value!.scope)){error.value='请选择有权限的具体楼栋。';return {ok:false};}if(!draft.value.name?.trim()||!draft.value.steps.length||draft.value.steps.some((s:Dict)=>!s.content?.trim())){error.value='请填写SOP名称和每一步操作内容。';return {ok:false};}if(draft.value.work_type==='adjust'&&sopTokens(draft.value).some(token=>adjustSopType(draft.value!)!=='cooling'||token!=='{{from}}')){error.value=adjustSopType(draft.value)==='cooling'?'制冷单元模式切换 SOP 只允许使用 {{from}}，也可以不使用占位符。':'普通设备调整 SOP 不能使用设备占位符。';return {ok:false};}busy.value=true;try{const d=draft.value;const saved=await requestJson('/api/polling-sops'+(d.sop_id?'/'+encodeURIComponent(d.sop_id):''),{method:d.sop_id?'PUT':'POST',body:JSON.stringify({sop_id:d.sop_id,scope:d.scope,work_type:d.work_type,...(d.work_type==='adjust'?{adjust_sop_type:adjustSopType(d)}:{}),name:d.name,expected_version:d.version,steps:d.steps})});select(saved);cache.clear();emit('changed');message.success('SOP已保存');busy.value=false;void load();return saved;}catch(e){error.value=(e as Error).message;if(!(e instanceof ApiError)||!e.status||e.status>=500){uncertain.value=true;error.value+=' 保存结果尚未确认，请重新读取列表核验原SOP，不要重复新建。';}return {ok:false};}finally{busy.value=false;}}
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
<style scoped>

.sop-desk{--sop-sky:#eaf4fb;--sop-mint:#e8f5ee;--sop-blue:#2563a8;--sop-sky-ink:#1d6fa8;--sop-green:#1f7a4d;--sop-green-soft:#dcf0e4;--sop-ink:var(--v2-ink,#1c2b24);--sop-muted:var(--v2-muted,#5d6f65);--sop-line:var(--v2-border,#d6e3da);--sop-face:var(--soft-face,#f7faf8);--sop-face-inset:var(--soft-field,#eef4f0);--sop-raised:var(--soft-raised,0 1px 2px rgba(28,43,36,.05),0 10px 24px -18px rgba(28,43,36,.35));--sop-inset:var(--soft-inset,inset 0 1px 2px rgba(28,43,36,.06),inset 0 -1px 0 rgba(255,255,255,.75));box-sizing:border-box;display:flex;flex-direction:column;gap:10px;min-width:0;width:100%;max-width:100%;color:var(--sop-ink)}
.sop-desk *,.sop-desk *::before,.sop-desk *::after{box-sizing:border-box}
.sop-desk__toolbar{display:flex;align-items:center;flex-wrap:wrap;gap:8px;min-width:0;padding:8px 10px;background:var(--sop-face);border:1px solid var(--sop-line);border-radius:12px;box-shadow:var(--sop-raised)}
.sop-desk__page-title{margin:0;flex:1 1 150px;min-width:0;font-size:16px;font-weight:650;line-height:1.3;color:var(--v2-ink,var(--sop-ink));overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-desk__selectors{display:flex;flex:0 1 auto;gap:6px;min-width:0}
.sop-desk__selectors>.n-select{min-width:118px;flex:0 1 168px}
.sop-desk__search{flex:1 1 140px;min-width:0;max-width:240px}
.sop-desk__layout-tools{display:inline-flex;flex:none;min-width:0}
.sop-desk__error{margin:0}
.sop-desk__block{box-sizing:border-box;display:flex;flex-direction:column;gap:10px;min-width:0;width:100%;max-width:100%;height:100%;padding:12px 14px;background:var(--sop-face);border:1px solid var(--sop-line);border-radius:12px;box-shadow:var(--sop-raised)}
.sop-desk__block-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap;min-width:0}
.sop-desk__titles{min-width:0;flex:1 1 140px;display:grid;gap:2px}
.sop-desk__block-title{margin:0;min-width:0;font-size:15px;font-weight:650;line-height:1.3;color:var(--sop-ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-desk__sub{margin:0;font-size:12px;line-height:1.4;color:var(--sop-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-desk__head-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex:none;flex-wrap:wrap}
.sop-desk__list{flex:1 1 auto;min-width:0;min-height:0;overflow:auto}
.sop-desk :deep([data-page-layout=sops]:not(.is-customizing) .grid-stack-item:is([data-widget-id=block_templates],[data-widget-id=block_editor])>.v2-widget>.v2-widget-head){display:none}
.sop-catalog :deep(.v2-flow-grid>[data-widget-id=block_templates]){grid-column:1/-1}
.sop-catalog :deep(.v2-flow-grid>[data-widget-id=block_editor]){display:none}
.sop-desk__cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,210px),1fr));gap:8px;align-items:start}
.sop-catalog :deep(.v2-flow-grid) .sop-desk__cards{grid-template-columns:repeat(auto-fill,minmax(min(100%,245px),1fr))}
.sop-desk__card{display:grid;gap:3px;min-width:0;padding:9px 11px;border:1px solid var(--sop-line);border-radius:10px;background:var(--sop-face);color:var(--sop-ink);text-align:left;cursor:pointer;transition:border-color .15s ease,box-shadow .15s ease,background-color .15s ease}
.sop-desk__card:hover:not(:disabled){border-color:#9fc9e4;background:var(--sop-sky)}
.sop-desk__card:focus-visible{outline:2px solid var(--sop-blue);outline-offset:2px}
.sop-desk__card.selected{border-color:var(--sop-blue);background:var(--sop-sky);box-shadow:inset 3px 0 0 var(--sop-blue)}
.sop-desk__card:disabled{opacity:.6;cursor:not-allowed}
.sop-desk__card-name{font-size:13px;font-weight:600;line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-desk__card-meta{font-size:11.5px;color:var(--sop-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-desk__card-stats{display:flex;flex-wrap:wrap;gap:6px;font-size:11px;color:var(--sop-sky-ink)}
.sop-desk__card-stats>span{padding:1px 7px;border-radius:999px;background:var(--sop-mint)}
.sop-desk__form{display:grid;gap:14px;min-width:0}
.sop-desk__form.is-readonly .n-form-item-label{color:var(--sop-muted)}
.sop-desk__section{display:grid;gap:10px;min-width:0;padding:12px 14px;background:var(--sop-face-inset);border-radius:12px;box-shadow:var(--sop-inset)}
.sop-desk__section-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;min-width:0}
.sop-desk__section-title{margin:0;min-width:0;font-size:13px;font-weight:650;letter-spacing:.01em;color:var(--sop-ink)}
.sop-desk__count{display:inline-block;min-width:20px;padding:0 6px;margin-left:4px;border-radius:999px;background:var(--sop-green-soft);color:var(--sop-green);font-size:11px;text-align:center}
.sop-desk__grid{display:grid;gap:10px;min-width:0}
.sop-desk__grid--3{grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr))}
.sop-desk__grid--2{grid-template-columns:repeat(auto-fit,minmax(min(100%,160px),1fr))}
.sop-desk__inline-field{max-width:360px}
.sop-desk__steps{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,230px),1fr));gap:10px;align-items:start;min-width:0}
.sop-desk__steps:has(.is-editing){grid-template-columns:minmax(0,1fr)}
.sop-step-selector{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(145px,100%),1fr));gap:5px;max-height:175px;overflow:auto;align-items:start}
.sop-step-selector button{min-width:0;display:flex;align-items:center;gap:7px;padding:6px 9px;text-align:left;color:var(--sop-ink);border:1px solid var(--sop-line);background:var(--sop-face);border-radius:7px;cursor:pointer}
.sop-step-selector span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sop-step-selector b{flex:none;font-variant-numeric:tabular-nums}
.sop-step-selector button[aria-pressed=true]{background:var(--sop-green-soft);border-color:var(--sop-green);font-weight:600}
.sop-desk__step-head small{font-size:12px;color:var(--sop-muted);font-weight:400}
.sop-desk__step{min-width:0;padding:10px 12px;border:1px solid var(--sop-line);border-radius:12px;background:var(--sop-face);box-shadow:var(--sop-raised)}
.sop-desk__step.is-editing{border-color:#bcd8ea;box-shadow:0 0 0 1px rgba(37,99,168,.06)}
.sop-desk__step-head>strong{margin-right:auto;font-size:13px}
.sop-desk__step-head{display:flex;align-items:center;justify-content:flex-end;gap:8px;min-width:0;margin-bottom:6px}
.sop-desk__step-read,.sop-desk__step-edit{display:grid;gap:8px;min-width:0}
.sop-desk__step-content{margin:0;font-size:13px;line-height:1.6;color:var(--sop-ink);white-space:pre-wrap;overflow-wrap:anywhere}
.sop-desk__step-no{display:inline-flex;align-items:center;justify-content:center;flex:none;min-width:20px;height:20px;margin-right:6px;padding:0 5px;border-radius:6px;background:var(--sop-sky);color:var(--sop-sky-ink);font-size:11px;font-weight:700;vertical-align:baseline}
.sop-desk__rules{display:flex;flex-wrap:wrap;gap:6px;margin:0;padding:0;list-style:none;min-width:0}
.sop-desk__rules>li{max-width:100%;font-size:11.5px;color:var(--sop-muted);padding:2px 8px;border-radius:6px;background:var(--sop-face-inset);overflow-wrap:anywhere}
.sop-desk__rule-flag{border:1px solid var(--sop-line)}
.sop-desk__rule-flag.on{color:var(--sop-green);background:var(--sop-green-soft);border-color:transparent;font-weight:600}
.sop-desk__step-settings{display:flex;flex-wrap:wrap;gap:6px 14px;min-width:0}
.sop-desk__repeat{display:grid;grid-template-columns:minmax(0,1fr) auto minmax(0,1fr) minmax(104px,140px) 30px;align-items:center;gap:6px;min-width:0}
.sop-repeat-count{min-width:0;display:grid;gap:3px;font-size:12px;color:var(--sop-muted)}
.sop-desk__repeat{align-items:end}.sop-desk__repeat-joiner{align-self:end;line-height:34px}
.sop-desk__repeat-joiner{font-size:12px;color:var(--sop-muted)}
.sop-desk__files{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:6px;min-width:0}
.sop-desk__file{display:flex;align-items:center;justify-content:space-between;gap:8px;min-width:0;padding:4px 6px 4px 10px;border:1px solid var(--sop-line);border-radius:9px;background:var(--sop-face)}
.sop-desk__file>.n-button{flex:1 1 auto;min-width:0;justify-content:flex-start}
.sop-desk__file .n-button__content{min-width:0;white-space:normal;overflow-wrap:anywhere;text-align:left}
.sop-desk__upload{display:inline-flex;align-items:center;gap:6px;min-width:0;max-width:100%;padding:4px 10px;border:1px dashed var(--sop-green);border-radius:9px;background:var(--sop-green-soft);color:var(--sop-green);font-size:12px;font-weight:600;cursor:pointer;overflow-wrap:anywhere}
.sop-desk__upload.blocked{opacity:.6;cursor:not-allowed}
.sop-desk__upload:focus-within{outline:2px solid var(--sop-green);outline-offset:2px}
.sop-desk__upload>input{position:absolute;width:1px;height:1px;opacity:0;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
.sop-desk__blank{padding:24px 0}
@media(max-width:520px){
  .sop-desk__toolbar{padding:8px}
  .sop-desk__page-title{flex:1 1 100%}
  .sop-desk__selectors{flex:1 1 100%}
  .sop-desk__selectors>.n-select{flex:1 1 0;min-width:0}
  .sop-desk__search{flex:1 1 100%;max-width:none}
  .sop-desk__block{padding:10px 12px}
  .sop-desk__repeat{grid-template-columns:minmax(0,1fr) auto minmax(0,1fr) 30px;row-gap:6px}
  .sop-desk__repeat>.sop-repeat-count{grid-column:1/4}
}
@media(prefers-reduced-motion:reduce){.sop-desk__card{transition:none}}
</style>
