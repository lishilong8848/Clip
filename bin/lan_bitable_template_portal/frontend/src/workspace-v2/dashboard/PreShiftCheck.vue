<template>
  <section class="v2-shift-check" aria-label="班前检查" :data-v2-layout-protected="dirty||busy||uploading||saving||editing?'true':undefined">
    <div class="v2-check-toolbar" data-v2-navigation>
      <NSelect v-if="state?.can_manage" :value="scope" :options="scopeOptions" :disabled="busy || uploading || dirty" aria-label="检查楼栋" @update:value="changeScope" />
      <span v-else>{{ scope === '110' ? '110站' : scope + '楼' }}</span>
      <div><NButton v-if="state?.can_manage" size="small" secondary :disabled="busy || dirty" @click="edit"><template #icon><Settings2 :size="15" /></template>管理清单</NButton><NButton quaternary circle :loading="loading" aria-label="刷新班前检查" @click="load(true)"><RefreshCw :size="16" /></NButton></div>
    </div>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NSkeleton v-if="!state && loading" text :repeat="4" />
    <template v-else-if="state">
      <div v-if="state.tasks.length" class="v2-check-shifts" data-v2-navigation><NButton v-for="row in state.tasks" :key="row.id" :type="task?.id===row.id?'primary':'default'" :secondary="task?.id===row.id" :disabled="busy || uploading || dirty && task?.id!==row.id" size="small" @click="selectedId=row.id">{{ row.day.slice(5) }} {{ row.shift }}班 {{timeLabel(row.due)}} <CheckCircle2 v-if="row.submitted" :size="14" /></NButton></div>
      <template v-if="task">
        <div class="v2-check-status" :class="{'is-complete':task.submitted}">
          <CheckCircle2 v-if="task.submitted" :size="20" /><ClipboardCheck v-else :size="20" />
          <div><strong>{{ task.submitted?'本班已完成':'本班待检查' }}</strong><small>{{ task.submitted ? task.actor_name + ' · ' + dateLabel(task.submitted) : task.people.map((p:Dict)=>p.name).join('、') + ' · 任意一人提交即可' }}</small></div>
          <span v-if="!task.submitted">{{ answered }} / {{ task.items.length }}</span>
        </div>
        <div v-if="task.submitted" class="v2-check-sync" role="status">{{ task.sync_status==='synced'?'已同步飞书':'本地已保存，飞书同步中' }}<span v-if="task.sync_error"> · {{ task.sync_error }}</span></div>
        <NAlert v-else-if="!task.can_submit" type="info" :bordered="false">{{ expired ? '本班次已结束，未提交。' : '仅本班排班人员或本楼值班账号可提交。' }}</NAlert>
        <form class="v2-check-form" @submit.prevent="submit">
          <div class="v2-check-items">
            <fieldset v-for="(item,index) in task.items" :key="item.id" :disabled="busy || !task.can_submit" class="v2-check-item">
              <legend><span>{{ index+1 }}</span>{{ item.text }}</legend>
              <NRadioGroup :value="answer(item.id).status" :name="task.id+'-'+item.id" :disabled="busy || !task.can_submit" :aria-label="item.text" @update:value="value=>setAnswer(item.id,String(value))">
                <NRadioButton value="normal">无异常</NRadioButton><NRadioButton value="abnormal">异常</NRadioButton>
              </NRadioGroup>
              <Transition name="v2-check-note"><div v-if="answer(item.id).status==='abnormal'" class="v2-check-note">
                <label :for="'check-note-'+item.id">异常备注 <span v-if="!task.submitted">*</span></label>
                <NInput :value="answer(item.id).note" type="textarea" :autosize="{minRows:2,maxRows:4}" :maxlength="500" :disabled="busy || !task.can_submit" :input-props="{id:'check-note-'+item.id,'aria-label':item.text+'异常备注'}" placeholder="填写异常情况" @update:value="value=>setNote(item.id,value)" />
              </div></Transition>
              <CheckAttachments :task-id="task.id" :item-id="item.id" kind="item" :files="(task.files||[]).filter((f:Dict)=>f.item_id===item.id)" :locked="busy||uploading||!task.can_submit" @busy="uploading=$event" @changed="load(true)" />
            </fieldset>
          </div>
          <footer v-if="task.can_submit"><span>{{ answered===task.items.length?'检查项已填完，请核对后提交':'逐项选择检查结果' }}</span><NButton type="primary" attr-type="submit" :loading="busy" :disabled="busy||uploading">提交检查</NButton></footer>
        </form>
        <section class="v2-check-handover"><h3>交接班照片 <small>{{task.handover_photo?'本班已上传':'本班共用一张'}}</small></h3><CheckAttachments :task-id="task.id" kind="handover" :files="task.handover_photo?[task.handover_photo]:[]" :locked="busy||uploading" :allow-upload="task.can_upload_handover===true" @busy="uploading=$event" @changed="load(true)" /></section>
      </template>
      <NEmpty v-else :description="!state.definition.items.length?'尚未配置班前检查清单':!state.roster_ready?'正在准备当天排班':'当前没有已发布的本楼班前检查'" size="small" />
      <ol v-if="!task && state.definition.items.length" class="v2-check-preview"><li v-for="item in state.definition.items" :key="item.id">{{ item.text }}<small> · {{frequencyLabel(item.frequency)}}</small></li></ol>
      <NAlert v-if="state.roster_error" type="warning" :bordered="false">{{ state.roster_error }}</NAlert>
    </template>
    <NModal v-model:show="editing" preset="card" title="班前检查清单" class="v2-check-editor" :mask-closable="false" :closable="!saving" :close-on-esc="!saving">
      <p class="v2-check-edit-note">{{ scope }}楼 · 保存后立即更新未提交的班次；已提交和历史清单保持原样。</p>
      <NAlert v-if="editError" type="error" :bordered="false">{{ editError }}</NAlert>
      <section v-for="(item,index) in items" :key="item.id" class="v2-check-definition"><div class="v2-check-edit-row" data-v2-navigation>
        <label :for="'check-edit-'+item.id">{{ index+1 }}</label><NInput v-model:value="item.text" :maxlength="200" :disabled="saving" :input-props="{id:'check-edit-'+item.id,'aria-label':'检查项'+(index+1)}" placeholder="检查内容" />
        <NButton quaternary circle :disabled="saving" :aria-label="'删除检查项'+(index+1)" @click="items.splice(index,1)"><Trash2 :size="17" /></NButton>
      </div><CheckFrequency v-model="item.frequency" :disabled="saving" /></section>
      <NButton data-v2-navigation dashed :disabled="saving || items.length>=50" @click="items.push({id:randomHexId(),text:'',frequency:defaultFrequency()})"><template #icon><Plus :size="16" /></template>添加检查项</NButton>
      <template #footer><div class="v2-check-editor-actions"><NButton :disabled="saving" @click="editing=false">取消</NButton><NButton type="primary" :loading="saving" :disabled="saving" @click="saveDefinition">保存清单</NButton></div></template>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NInput,NModal,NRadioButton,NRadioGroup,NSelect,NSkeleton,useDialog,useMessage } from 'naive-ui';
import { CheckCircle2,ClipboardCheck,Plus,RefreshCw,Settings2,Trash2 } from 'lucide-vue-next';
import { requestJson,type Dict } from '../client';
import { randomHexId } from '../../browserStorage';
import { registerNavigationGuard } from '../navigation';
import CheckFrequency from './CheckFrequency.vue';
import CheckAttachments from './CheckAttachments.vue';
const props=defineProps<{user:Dict}>();
const state=ref<Dict>(),scope=ref(props.user.home_scope||'A'),selectedId=ref(''),error=ref(''),loading=ref(false),busy=ref(false);
const editing=ref(false),saving=ref(false),uploading=ref(false),editError=ref(''),items=ref<{id:string;text:string;frequency:Dict}[]>([]),version=ref(0);
const drafts=ref<Record<string,Record<string,{status:string;note:string}>>>({});
const message=useMessage(),dialog=useDialog();let timer:ReturnType<typeof setInterval>|undefined,controller:AbortController|undefined,disposed=false;
const task=computed<Dict|undefined>(()=>state.value?.tasks.find((t:Dict)=>t.id===selectedId.value)||state.value?.tasks[0]);
const scopeOptions=computed(()=>state.value?.scopes.map((value:string)=>({value,label:value==='110'?'110站':value+'楼'}))||[]);
const dirty=computed(()=>Boolean(task.value&&!task.value.submitted&&Object.keys(drafts.value[task.value.id]||{}).length));
const expired=computed(()=>Boolean(task.value&&Date.now()/1000>=task.value.deadline));
function answer(id:string){return task.value?.submitted ? task.value.answers.find((a:Dict)=>a.id===id)||{status:'',note:''} : drafts.value[task.value?.id||'']?.[id]||{status:'',note:''};}
const answered=computed(()=>task.value?.items.filter((i:Dict)=>answer(i.id).status==='normal'||answer(i.id).status==='abnormal'&&answer(i.id).note.trim()).length||0);
function setAnswer(id:string,status:string){if(!task.value)return;error.value='';const draft=drafts.value[task.value.id]||={};draft[id]={...answer(id),status};}
function setNote(id:string,note:string){if(!task.value)return;error.value='';const draft=drafts.value[task.value.id]||={};draft[id]={...answer(id),note};}
function dateLabel(value:string){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});}
function timeLabel(value:number){return new Date(value*1000).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit'});}
function defaultFrequency(){return {kind:'shift',day_time:'08:50',night_time:'17:50',start_date:new Date().toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'})};}
function frequencyLabel(f:Dict=defaultFrequency()){const labels:Dict={daily:'每日',shift:'每日每班组',weekly:'每周'+['一','二','三','四','五','六','日'][f.weekday||0],month_start:'每月月初',month_end:'每月月底',interval:'每隔'+f.every_days+'天',temporary:'临时'+f.limit+(f.limit_type==='days'?'天':'次')};return (labels[f.kind]||'每日每班组')+' '+(f.time||[f.day_time,f.night_time].filter(Boolean).join(' / '));}
async function load(force=false){
  if(disposed||loading.value&&!force)return;controller?.abort();const read=new AbortController();controller=read;loading.value=true;
  try{const result=await requestJson('/api/pre-shift-checks?scope='+encodeURIComponent(scope.value),{signal:read.signal,cache:'no-store'});if(disposed||read.signal.aborted)return;if(!Array.isArray(result.tasks)||!Array.isArray(result.definition?.items)||!Array.isArray(result.scopes))throw new Error('检查数据不完整，请重试。');for(const id of Object.keys(drafts.value)){const next=result.tasks.find((t:Dict)=>t.id===id),previous=state.value?.tasks.find((t:Dict)=>t.id===id);if(!next||next.submitted||previous?.definition_version!==next.definition_version)delete drafts.value[id];}state.value=result;error.value='';if(!selectedId.value||!result.tasks.some((t:Dict)=>t.id===selectedId.value))selectedId.value=result.tasks[0]?.id||'';}
  catch(e){if(!disposed&&!read.signal.aborted)error.value=e instanceof Error?e.message:'班前检查读取失败，请重试。';}finally{if(controller===read)loading.value=false;}
}
function changeScope(value:string){if(dirty.value||busy.value||uploading.value)return;scope.value=value;selectedId.value='';state.value=undefined;void load(true);}
function edit(){if(uploading.value)return;items.value=(state.value?.definition.items||[]).map((item:Dict)=>({id:item.id,text:item.text,frequency:{...(item.frequency||defaultFrequency())}}));version.value=state.value?.definition.version||0;editError.value='';editing.value=true;}
async function saveDefinition(){
  if(items.value.some(item=>!item.text.trim())){editError.value='请填写检查内容，或删除空白项。';return;}
  for(const [index,item] of items.value.entries()){const f=item.frequency,shift=f.kind==='shift'||f.kind==='temporary'&&f.cadence==='shift';let issue='';if(!f.start_date)issue='请选择生效日期';else if(shift&&(!f.day_time||f.day_time<'08:00'||f.day_time>='18:00'))issue='白班时间须在08:00至18:00之间';else if(shift&&(!f.night_time||f.night_time>='08:00'&&f.night_time<'17:50'))issue='夜班时间须在17:50至次日08:00之间';else if(!shift&&!f.time)issue='请选择执行时间';else if(f.kind==='interval'&&!f.every_days||f.kind==='temporary'&&!f.limit)issue='请填写有效的次数或天数';if(issue){editError.value=`第${index+1}项：${issue}。`;return;}}
  saving.value=true;editError.value='';
  try{await requestJson('/api/pre-shift-checks/definition?scope='+encodeURIComponent(scope.value),{method:'PUT',body:JSON.stringify({version:version.value,items:items.value})});editing.value=false;message.success('检查清单已保存');window.dispatchEvent(new Event('clipflow-business-changed'));await load(true);}catch(e){editError.value=e instanceof Error?e.message:'保存失败，请重试。';}finally{saving.value=false;}
}
async function submit(){
  if(!task.value||!task.value.can_submit||busy.value||uploading.value)return;
  if(answered.value!==task.value.items.length){error.value='请逐项选择检查结果，并填写异常项备注。';return;}
  const row=task.value;busy.value=true;error.value='';
  try{const result=await requestJson('/api/pre-shift-checks/submit',{method:'POST',body:JSON.stringify({task_id:row.id,version:row.definition_version,answers:row.items.map((item:Dict)=>({id:item.id,...answer(item.id)}))})});delete drafts.value[row.id];message.success(result.already_completed?'本班已由'+result.actor_name+'提交，无需重复检查':'检查已提交，飞书后台同步');await load(true);}catch(e){error.value=e instanceof Error?e.message:'提交暂未确认，请重试；不会重复新增。';}finally{busy.value=false;}
}
const removeGuard=registerNavigationGuard((_target,proceed)=>{if(busy.value||saving.value||uploading.value){message.warning('正在保存，请稍候。');return false;}if(!dirty.value&&!editing.value)return true;dialog.warning({title:'检查尚未提交',content:'离开会放弃当前未保存的填写。',positiveText:'放弃并离开',negativeText:'继续填写',onPositiveClick:()=>{drafts.value={};editing.value=false;proceed();}});return false;});
function beforeUnload(event:BeforeUnloadEvent){if(dirty.value||busy.value||saving.value||editing.value||uploading.value){event.preventDefault();event.returnValue='';}}
function refresh(){if(!document.hidden&&!busy.value&&!saving.value&&!editing.value&&!uploading.value)void load();}
onMounted(()=>{void load();timer=setInterval(refresh,30000);window.addEventListener('clipflow-business-changed',refresh);window.addEventListener('beforeunload',beforeUnload);document.addEventListener('visibilitychange',refresh);});
watch(()=>props.user.open_id,()=>{drafts.value={};state.value=undefined;});
onBeforeUnmount(()=>{disposed=true;controller?.abort();if(timer)clearInterval(timer);removeGuard();window.removeEventListener('clipflow-business-changed',refresh);window.removeEventListener('beforeunload',beforeUnload);document.removeEventListener('visibilitychange',refresh);});
</script>
<style>
.v2-check-preview{margin:0;padding-left:24px;line-height:1.8;color:#53636e}
.v2-check-definition{border-bottom:1px solid var(--v2-border);padding:4px 0 16px;margin-bottom:12px}.v2-check-handover h3 small{font-size:12px;font-weight:400;color:var(--v2-muted);margin-left:8px}
.v2-shift-check{display:flex;flex-direction:column;gap:12px;min-height:0;font-size:14px}.v2-check-toolbar,.v2-check-toolbar>div,.v2-check-shifts,.v2-check-status,.v2-check-form>footer,.v2-check-editor-actions{display:flex;align-items:center;gap:8px}.v2-check-toolbar{justify-content:space-between}.v2-check-toolbar>.n-select{max-width:140px}.v2-check-shifts{flex-wrap:wrap}.v2-check-status{padding:10px 12px;background:#eef4ff;color:#2457a7;border-radius:8px}.v2-check-status.is-complete{background:#edf7f0;color:#267047}.v2-check-status>div{flex:1;min-width:0}.v2-check-status strong,.v2-check-status small{display:block}.v2-check-status small{margin-top:3px;color:#586577;font-size:12px;overflow-wrap:anywhere}.v2-check-sync,.v2-check-times{font-size:12px;color:#53636e;line-height:1.6}.v2-check-items{display:flex;flex-direction:column}.v2-check-item{border:0;border-bottom:1px solid #e8ecf0;margin:0;padding:12px 0;min-width:0;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center}.v2-check-item legend{float:left;display:block;padding:0;overflow-wrap:anywhere;line-height:1.6}.v2-check-item legend span{display:inline-flex;width:24px;height:24px;align-items:center;justify-content:center;margin-right:8px;border-radius:50%;background:#f0f3f6;color:#637184;font-size:12px}.v2-check-note{grid-column:1/-1}.v2-check-note label{display:block;margin-bottom:5px;font-size:12px;color:#98482c}.v2-check-form>footer{justify-content:space-between;padding:12px 0;background:#fff}.v2-check-form>footer>span{font-size:12px;color:#5a6a7b}.v2-check-editor.n-card{width:min(650px,calc(100vw - 24px));max-height:85dvh}.v2-check-editor .n-card-content{overflow-y:auto}.v2-check-edit-note{font-size:13px;color:#647487;line-height:1.6;margin:0 0 16px}.v2-check-edit-row{display:grid;grid-template-columns:20px minmax(0,1fr) 36px;gap:8px;align-items:center;margin:10px 0}.v2-check-editor-actions{justify-content:flex-end}.v2-check-note-enter-active,.v2-check-note-leave-active{transition:opacity 180ms ease-out,transform 180ms ease-out}.v2-check-note-enter-from,.v2-check-note-leave-to{opacity:0;transform:translateY(-5px)}@media(max-width:760px){.v2-check-item{grid-template-columns:1fr}.v2-check-item .n-radio-button{min-height:40px;display:inline-flex;align-items:center}.v2-check-item .n-radio-group{justify-self:end}.v2-check-toolbar .n-button{min-height:40px}}@media(prefers-reduced-motion:reduce){.v2-check-note-enter-active,.v2-check-note-leave-active{transition:opacity 100ms}}
</style>
