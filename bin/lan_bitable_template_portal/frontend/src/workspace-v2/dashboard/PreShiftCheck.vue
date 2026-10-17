<template>
  <section class="v2-shift-check" :class="{'has-pending-checks':needsAttention}" aria-label="班前检查" :data-v2-layout-protected="dirty||busy||uploading||saving||editing?'true':undefined">
    <div class="v2-check-toolbar" data-v2-navigation>
      <NSelect v-if="state?.can_manage&&!fixedScope" :value="scope" :options="scopeOptions" :disabled="busy || uploading || dirty" aria-label="检查楼栋" @update:value="changeScope" />
      <span v-else>{{ scope === '110' ? '110站' : scope + '楼' }}</span>
      <span v-if="needsAttention" class="v2-check-alert" role="status"><i aria-hidden="true" />待检查</span>
      <div class="v2-check-actions"><NButton size="small" secondary :disabled="busy||dirty||uploading||saving||editing" :aria-pressed="historyOpen" @click="historyOpen=!historyOpen"><template #icon><History :size="15" /></template>{{historyOpen?'返回检查':'历史记录'}}</NButton><NButton v-if="!historyOpen&&!fixedScope&&state?.can_manage&&state.overview?.rows" size="small" secondary :disabled="busy||dirty||uploading" :aria-pressed="overviewOpen" @click="overviewOpen=!overviewOpen">{{overviewOpen?'本楼检查':'完成总览'}}</NButton><NButton v-if="state?.can_manage" size="small" secondary :disabled="busy || dirty" @click="edit"><template #icon><Settings2 :size="15" /></template>管理清单</NButton><NButton v-if="!historyOpen" quaternary circle :loading="loading" aria-label="刷新班前检查" @click="load(true)"><RefreshCw :size="16" /></NButton></div>
    </div>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NSkeleton v-if="!state && loading" text :repeat="4" />
    <template v-else-if="state">
      <CheckHistory v-if="historyOpen" :key="String(user.open_id)" :scope="scope" />
      <CheckOverview v-else-if="overviewOpen&&state.can_manage" :rows="state.overview?.rows||[]" :disabled="busy||uploading||dirty" @select="openBuilding" />
      <div v-else class="v2-check-workspace">
      <div v-if="visibleTasks.length" class="v2-check-shifts" data-v2-navigation><button v-for="row in visibleTasks" :key="row.id" type="button" class="v2-check-choice" :class="{'is-selected':task?.id===row.id,'is-done':row.submitted}" :aria-pressed="task?.id===row.id" :disabled="busy || uploading || dirty && task?.id!==row.id" @click="selectedId=row.id"><span>{{ row.shift }}班<CheckCircle2 v-if="row.submitted" :size="14" /></span><small class="v2-check-window">{{compact?shortDate(row.due*1000):dateLabel(row.due*1000)}} 至 {{compact?shortDate(row.deadline*1000):dateLabel(row.deadline*1000)}}</small><small v-if="!compact">{{row.items.map((item:Dict)=>item.text).join('、')}}</small><small v-if="row.submitted">{{completionLabel(row)}} · {{row.actor_name}}</small><small v-else>{{row.due>serverNow?(compact?'可提前检查':'未到时间，可提前检查'):row.deadline<=serverNow?(compact?'逾期待补做':'已逾期，可补做'):'待检查'}} · {{row.items.length}} 项</small></button></div>
      <div class="v2-check-task">
      <template v-if="task">
        <div class="v2-check-status" :class="{'is-complete':task.submitted}">
          <CheckCircle2 v-if="task.submitted" :size="20" /><ClipboardCheck v-else :size="20" />
          <div><strong>{{ task.submitted?'本次已完成':task.due>serverNow?'可提前检查':task.deadline<=serverNow?'逾期待补做':'本次待检查' }}</strong><small>{{ task.submitted ? completionLabel(task) + ' · ' + task.actor_name + ' · ' + dateLabel(task.submitted) : (task.people.map((p:Dict)=>p.name).join('、') || '本楼值班人员') + ' · 任意一人提交即可' }}</small></div>
          <span v-if="!task.submitted">{{ answered }} / {{ task.items.length }}</span>
        </div>
        <div v-if="task.submitted" class="v2-check-sync" role="status">{{ task.sync_status==='synced'?'已同步飞书':'本地已保存，飞书同步中' }}<span v-if="task.sync_error"> · {{ task.sync_error }}</span></div>
        <NAlert v-else-if="!task.can_submit" type="info" :bordered="false">仅本楼值班人员或值班账号可提交。</NAlert>
        <form class="v2-check-form" @submit.prevent="submit">
          <div class="v2-check-items">
            <fieldset v-for="(item,index) in task.items" :key="item.id" class="v2-check-item">
              <legend><span>{{ index+1 }}</span>{{ item.text }}</legend>
              <div class="v2-check-result">
              <NRadioGroup :value="answer(item.id).status" :name="task.id+'-'+item.id" :disabled="busy || !task.can_submit" :aria-label="item.text" @update:value="value=>setAnswer(item.id,String(value))">
                <NRadioButton value="normal">无异常</NRadioButton><NRadioButton value="abnormal">异常</NRadioButton>
              </NRadioGroup>
              <NButton v-if="compact" quaternary circle :aria-label="item.text+'附件'" :aria-expanded="!!attachmentOpen[task.id+':'+item.id]" :title="'附件 '+(task.files||[]).filter((f:Dict)=>f.item_id===item.id).length+' 个'" :disabled="busy||uploading||!task.can_submit&&!(task.files||[]).some((f:Dict)=>f.item_id===item.id)" @click="attachmentOpen[task.id+':'+item.id]=!attachmentOpen[task.id+':'+item.id]"><Paperclip :size="16" /></NButton>
              </div>
              <Transition name="v2-check-note"><div v-if="answer(item.id).status==='abnormal'" class="v2-check-note">
                <label :for="'check-note-'+task.id+'-'+item.id">异常备注 <span v-if="!task.submitted">*</span></label>
                <NInput :value="answer(item.id).note" type="textarea" :autosize="{minRows:compact?1:2,maxRows:4}" :maxlength="500" :disabled="busy || !task.can_submit" :input-props="{id:'check-note-'+task.id+'-'+item.id,'aria-label':item.text+'异常备注'}" placeholder="填写异常情况" @update:value="value=>setNote(item.id,value)" />
              </div></Transition>
              <CheckAttachments v-if="!compact||attachmentOpen[task.id+':'+item.id]" :task-id="task.id" :item-id="item.id" kind="item" :files="(task.files||[]).filter((f:Dict)=>f.item_id===item.id)" :locked="busy||uploading||!task.can_submit" @busy="uploading=$event" @changed="load(true)" />
            </fieldset>
          </div>
          <footer v-if="task.can_submit"><span>{{ answered===task.items.length?'检查项已填完，请核对后提交':'逐项选择检查结果' }}</span><NButton type="primary" attr-type="submit" :loading="busy" :disabled="busy||uploading">提交检查</NButton></footer>
        </form>
        <section class="v2-check-handover"><h3>交接班照片 <small>{{task.handover_photo?'本班已上传':'本楼本班共用一张'}}</small></h3><CheckAttachments :task-id="task.id" kind="handover" :files="task.handover_photo?[task.handover_photo]:[]" :locked="busy||uploading" :allow-upload="task.can_upload_handover===true" @busy="uploading=$event" @changed="load(true)" /></section>
      </template>
      <NEmpty v-else :description="!state.definition.items.length?'尚未配置班前检查清单':!state.roster_ready?'正在准备当天排班':'当前没有已发布的本楼班前检查'" size="small" />
      <ol v-if="!task && state.definition.items.length" class="v2-check-preview"><li v-for="item in state.definition.items" :key="item.id">{{ item.text }}<small> · {{frequencyLabel(item.frequency)}}</small></li></ol>
      </div>
      </div>
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
      <template #footer><div class="v2-check-editor-actions"><NButton :disabled="saving" @click="editing=false">取消</NButton><NButton type="primary" :loading="saving" :disabled="saving" data-v2-submit @click="saveDefinition">保存清单</NButton></div></template>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NInput,NModal,NRadioButton,NRadioGroup,NSelect,NSkeleton,useDialog,useMessage } from 'naive-ui';
import { CheckCircle2,ClipboardCheck,History,Paperclip,Plus,RefreshCw,Settings2,Trash2 } from 'lucide-vue-next';
import { requestJson,type Dict } from '../client';
import { randomHexId } from '../../browserStorage';
import { registerNavigationGuard } from '../navigation';
import { preShiftSummary } from '../../preShiftUtils';
import CheckFrequency from './CheckFrequency.vue';
import CheckAttachments from './CheckAttachments.vue';
import CheckOverview from './CheckOverview.vue';
import CheckHistory from './CheckHistory.vue';
const props=defineProps<{user:Dict;fixedScope?:string;registerGuard?:(guard:(target:string,proceed:()=>void)=>boolean)=>()=>void}>();
const state=ref<Dict>(),scope=ref(props.fixedScope||props.user.home_scope||'A'),selectedId=ref(''),error=ref(''),loading=ref(false),busy=ref(false);
const editing=ref(false),saving=ref(false),uploading=ref(false),editError=ref(''),items=ref<{id:string;text:string;frequency:Dict}[]>([]),version=ref(0);
const overviewOpen=ref(false);let firstState=true;
const historyOpen=ref(false);
const compact=document.documentElement.classList.contains('workspace-v2');
const attachmentOpen=ref<Record<string,boolean>>({});
const drafts=ref<Record<string,Record<string,{status:string;note:string}>>>({});
const message=useMessage(),dialog=useDialog();let timer:ReturnType<typeof setInterval>|undefined,controller:AbortController|undefined,disposed=false;
const serverNow=computed(()=>Date.parse(state.value?.server_time||'')/1000);
const visibleTasks=computed<Dict[]>(()=>[...(state.value?.tasks||[])].filter((t:Dict)=>!t.submitted||t.deadline>serverNow.value).sort((a,b)=>a.due-b.due||String(a.id).localeCompare(String(b.id))));
const task=computed<Dict|undefined>(()=>visibleTasks.value.find((t:Dict)=>t.id===selectedId.value)||visibleTasks.value.find((t:Dict)=>!t.submitted)||visibleTasks.value[0]);
const needsAttention=computed(()=>Boolean(state.value&&(overviewOpen.value&&state.value.can_manage
  ?state.value.overview?.rows?.some((row:Dict)=>['待完成','逾期待补做'].includes(row.status))
  :preShiftSummary(state.value).pending)));
const scopeOptions=computed(()=>state.value?.scopes.map((value:string)=>({value,label:value==='110'?'110站':value+'楼'}))||[]);
const dirty=computed(()=>Boolean(task.value&&!task.value.submitted&&Object.keys(drafts.value[task.value.id]||{}).length));
function answer(id:string){return task.value?.submitted ? task.value.answers.find((a:Dict)=>a.id===id)||{status:'',note:''} : drafts.value[task.value?.id||'']?.[id]||{status:'',note:''};}
const answered=computed(()=>task.value?.items.filter((i:Dict)=>answer(i.id).status==='normal'||answer(i.id).status==='abnormal'&&answer(i.id).note.trim()).length||0);
function setAnswer(id:string,status:string){if(!task.value)return;error.value='';const draft=drafts.value[task.value.id]||={};draft[id]={...answer(id),status};}
function setNote(id:string,note:string){if(!task.value)return;error.value='';const draft=drafts.value[task.value.id]||={};draft[id]={...answer(id),note};}
function dateLabel(value:string|number){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});}
function shortDate(value:number){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});}
function completionLabel(row:Dict){const time=Date.parse(row.submitted)/1000;return time<row.due?'提前完成':time>=row.deadline?'补做完成':'已完成';}
function defaultFrequency(){return {kind:'shift',day_time:'08:50',night_time:'17:50',start_date:new Date().toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'})};}
function frequencyLabel(f:Dict=defaultFrequency()){const labels:Dict={daily:'每日',shift:'每日每班组',weekly:'每周'+['一','二','三','四','五','六','日'][f.weekday||0],month_start:'每月月初',month_end:'每月月底',interval:'每隔'+f.every_days+'天',temporary:'临时'+f.limit+(f.limit_type==='days'?'天':'次')};return (labels[f.kind]||'每日每班组')+' '+(f.time||[f.day_time,f.night_time].filter(Boolean).join(' / '));}
async function load(force=false){
  if(disposed||loading.value&&!force)return;controller?.abort();const read=new AbortController();controller=read;loading.value=true;
  try{const result=await requestJson('/api/pre-shift-checks?scope='+encodeURIComponent(scope.value),{signal:read.signal,cache:'no-store'});if(disposed||read.signal.aborted)return;if(!Array.isArray(result.tasks)||!Array.isArray(result.definition?.items)||!Array.isArray(result.scopes))throw new Error('检查数据不完整，请重试。');preShiftSummary(result);for(const id of Object.keys(drafts.value)){const next=result.tasks.find((t:Dict)=>t.id===id),previous=state.value?.tasks.find((t:Dict)=>t.id===id);if(!next||next.submitted||previous?.definition_version!==next.definition_version)delete drafts.value[id];}if(firstState){overviewOpen.value=!props.fixedScope&&!!result.can_manage&&result.scopes.length>1&&Array.isArray(result.overview?.rows);firstState=false;}state.value=result;error.value='';if(!visibleTasks.value.some((t:Dict)=>t.id===selectedId.value))selectedId.value=(visibleTasks.value.find((t:Dict)=>!t.submitted)||visibleTasks.value[0])?.id||'';}
  catch(e){if(!disposed&&!read.signal.aborted)error.value=e instanceof Error?e.message:'班前检查读取失败，请重试。';}finally{if(controller===read)loading.value=false;}
}
function changeScope(value:string){if(dirty.value||busy.value||uploading.value)return;scope.value=value;selectedId.value='';state.value=undefined;void load(true);}
function openBuilding(value:string){overviewOpen.value=false;changeScope(value);}
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
const removeGuard=(props.registerGuard||registerNavigationGuard)((_target,proceed)=>{if(busy.value||saving.value||uploading.value){message.warning('正在保存，请稍候。');return false;}if(!dirty.value&&!editing.value)return true;dialog.warning({title:'检查尚未提交',content:'离开会放弃当前未保存的填写。',positiveText:'放弃并离开',negativeText:'继续填写',onPositiveClick:()=>{drafts.value={};editing.value=false;proceed();}});return false;});
function beforeUnload(event:BeforeUnloadEvent){if(dirty.value||busy.value||saving.value||editing.value||uploading.value){event.preventDefault();event.returnValue='';}}
function refresh(){if(!document.hidden&&!busy.value&&!saving.value&&!editing.value&&!uploading.value)void load();}
onMounted(()=>{void load();timer=setInterval(refresh,30000);window.addEventListener('clipflow-business-changed',refresh);window.addEventListener('beforeunload',beforeUnload);document.addEventListener('visibilitychange',refresh);});
watch(()=>props.user.open_id,()=>{drafts.value={};state.value=undefined;});
onBeforeUnmount(()=>{disposed=true;controller?.abort();if(timer)clearInterval(timer);removeGuard();window.removeEventListener('clipflow-business-changed',refresh);window.removeEventListener('beforeunload',beforeUnload);document.removeEventListener('visibilitychange',refresh);});
</script>
<style>
.v2-check-workspace{display:grid;grid-template-columns:minmax(0,1fr);gap:12px}.v2-check-task{display:flex;flex-direction:column;gap:10px;min-width:0}.v2-check-result{display:flex;align-items:center;gap:6px;justify-content:flex-end}
.workspace-v2 .v2-shift-check{container-type:inline-size;container-name:shift-check;gap:10px}
.workspace-v2 .v2-check-toolbar{gap:8px;flex-wrap:wrap}.workspace-v2 .v2-check-actions{margin-left:auto;flex-wrap:wrap}
.workspace-v2 .v2-check-toolbar>.n-select{max-width:118px}.workspace-v2 .v2-check-status{padding:8px 10px;border-radius:6px}.workspace-v2 .v2-check-status strong{font-size:13px}
.workspace-v2 .v2-check-choice{padding:8px 10px;gap:3px}.workspace-v2 .v2-check-window{font-size:11px}
.workspace-v2 .v2-check-item{padding:10px 0;gap:8px 12px}.workspace-v2 .v2-check-item legend span{width:21px;height:21px;margin-right:6px}
.workspace-v2 .v2-check-note label{margin-bottom:4px}.workspace-v2 .v2-check-handover{padding:10px 0}.workspace-v2 .v2-check-handover h3{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:13px}
@container shift-check (min-width:650px){.workspace-v2 .v2-check-workspace{grid-template-columns:178px minmax(0,1fr);align-items:start;gap:16px}.workspace-v2 .v2-check-workspace>.v2-check-shifts{display:grid!important;grid-template-columns:minmax(0,1fr);align-content:start;gap:8px}.workspace-v2 .v2-check-item{grid-template-columns:minmax(0,1fr) auto}.workspace-v2 .v2-check-result{justify-self:end}}
@container shift-check (max-width:649px){.workspace-v2 .v2-check-item{grid-template-columns:minmax(0,1fr)}.workspace-v2 .v2-check-result{justify-self:end}}
.v2-shift-check{position:relative}.v2-shift-check.has-pending-checks::before{content:"";position:absolute;inset:-4px;pointer-events:none;border-radius:8px;box-shadow:0 0 0 1px #cf3548,0 0 10px #cf354826;animation:preShiftPulse 2.4s ease-in-out infinite}.v2-check-alert{display:inline-flex;align-items:center;gap:6px;margin-right:auto;color:#bb2b40;font-size:12px;font-weight:600}.v2-check-alert i{width:7px;height:7px;border-radius:50%;background:currentColor}@keyframes preShiftPulse{0%,100%{opacity:.3}50%{opacity:1}}@media(prefers-reduced-motion:reduce){.v2-shift-check.has-pending-checks::before{animation:none;opacity:.65}}
.workspace-v2 .v2-shift-check.has-pending-checks::before{background:#cf354812}
.workspace-v2 .v2-widget:has(.v2-shift-check.has-pending-checks)::after{content:"";position:absolute;inset:0;z-index:2;border-radius:inherit;background:#cf354812;box-shadow:inset 0 0 0 1px #cf3548;pointer-events:none;animation:preShiftPulse 2.4s ease-in-out infinite}
.workspace-v2 .v2-widget .v2-shift-check.has-pending-checks::before{display:none}
.workspace-v2 .grid-stack-item[data-widget-kind="pre_shift"] .v2-customize-button{height:28px;min-height:28px;padding-inline:7px;font-size:11px;margin-left:auto}
@media(prefers-reduced-motion:reduce){.workspace-v2 .v2-widget:has(.v2-shift-check.has-pending-checks)::after{animation:none;opacity:.4}}
.v2-check-preview{margin:0;padding-left:24px;line-height:1.8;color:#53636e}
.v2-check-shifts{display:grid!important;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr))}.v2-check-choice{display:flex;flex-direction:column;gap:5px;align-items:flex-start;text-align:left;padding:9px 10px;border:1px solid var(--v2-border);border-radius:6px;background:transparent;color:inherit;min-width:0;cursor:pointer;transition:border-color .18s,background .18s}.v2-check-choice>span{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600}.v2-check-choice small{font-size:12px;color:var(--v2-muted);overflow-wrap:anywhere;line-height:1.5}.v2-check-choice.is-selected{border-color:var(--v2-blue);background:color-mix(in srgb,var(--v2-blue) 6%,transparent)}.v2-check-choice.is-done>span{color:#267047}.v2-check-choice:disabled{cursor:not-allowed;opacity:.6}.v2-check-choice:focus-visible{outline:2px solid var(--v2-blue);outline-offset:2px}
.v2-check-definition{border-bottom:1px solid var(--v2-border);padding:4px 0 16px;margin-bottom:12px}.v2-check-handover h3 small{font-size:12px;font-weight:400;color:var(--v2-muted);margin-left:8px}
.v2-shift-check{display:flex;flex-direction:column;gap:12px;min-height:0;font-size:14px}.v2-check-toolbar,.v2-check-toolbar>div,.v2-check-shifts,.v2-check-status,.v2-check-form>footer,.v2-check-editor-actions{display:flex;align-items:center;gap:8px}.v2-check-toolbar{justify-content:space-between}.v2-check-toolbar>.n-select{max-width:140px}.v2-check-shifts{flex-wrap:wrap}.v2-check-status{padding:10px 12px;background:#eef4ff;color:#2457a7;border-radius:8px}.v2-check-status.is-complete{background:#edf7f0;color:#267047}.v2-check-status>div{flex:1;min-width:0}.v2-check-status strong,.v2-check-status small{display:block}.v2-check-status small{margin-top:3px;color:#586577;font-size:12px;overflow-wrap:anywhere}.v2-check-sync,.v2-check-times{font-size:12px;color:#53636e;line-height:1.6}.v2-check-items{display:flex;flex-direction:column}.v2-check-item{border:0;border-bottom:1px solid #e8ecf0;margin:0;padding:12px 0;min-width:0;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center}.v2-check-item legend{float:left;display:block;padding:0;overflow-wrap:anywhere;line-height:1.6}.v2-check-item legend span{display:inline-flex;width:24px;height:24px;align-items:center;justify-content:center;margin-right:8px;border-radius:50%;background:#f0f3f6;color:#637184;font-size:12px}.v2-check-note{grid-column:1/-1}.v2-check-note label{display:block;margin-bottom:5px;font-size:12px;color:#98482c}.v2-check-form>footer{justify-content:space-between;padding:12px 0;background:#fff}.v2-check-form>footer>span{font-size:12px;color:#5a6a7b}.v2-check-editor.n-card{width:min(650px,calc(100vw - 24px));max-height:85dvh}.v2-check-editor .n-card-content{overflow-y:auto}.v2-check-edit-note{font-size:13px;color:#647487;line-height:1.6;margin:0 0 16px}.v2-check-edit-row{display:grid;grid-template-columns:20px minmax(0,1fr) 36px;gap:8px;align-items:center;margin:10px 0}.v2-check-editor-actions{justify-content:flex-end}.v2-check-note-enter-active,.v2-check-note-leave-active{transition:opacity 180ms ease-out,transform 180ms ease-out}.v2-check-note-enter-from,.v2-check-note-leave-to{opacity:0;transform:translateY(-5px)}@media(max-width:760px){.v2-check-item{grid-template-columns:1fr}.v2-check-item .n-radio-button{min-height:40px;display:inline-flex;align-items:center}.v2-check-item .n-radio-group{justify-self:end}.v2-check-toolbar .n-button{min-height:40px}}@media(prefers-reduced-motion:reduce){.v2-check-note-enter-active,.v2-check-note-leave-active{transition:opacity 100ms}}
</style>
