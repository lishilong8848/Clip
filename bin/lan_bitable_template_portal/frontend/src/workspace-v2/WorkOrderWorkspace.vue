<template>
  <section class="serenity-desk order-desk" aria-label="工单办理">
    <header class="order-mast">
      <span class="order-emblem" aria-hidden="true"><ClipboardList :size="25"/></span>
      <div><p>{{ state?.role_label || '现场工单' }}<template v-if="state?.assigned_person?.name"> · {{ state.assigned_person.name }}</template></p>
        <h1>{{ state?.title || '工单办理' }}</h1></div>
      <div class="order-tools">
        <NButton v-if="stepsView" :disabled="busy" @click="back"><ArrowLeft :size="16"/>返回工单</NButton>
        <NButton :disabled="loading||busy" @click="load"><RefreshCw :size="16"/>刷新</NButton>
      </div>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NSkeleton v-if="!state && loading" text :repeat="3"/>
    <template v-if="state">
      <NAlert v-if="state.state!=='active'" :type="state.state==='completed'?'success':'info'">{{ stateLabel }}
        <NButton v-if="state.state==='upload_pending'" :disabled="busy" @click="retryUpload">重试上传工单表格</NButton>
      </NAlert>
      <section v-if="!stepsView" aria-label="工单列表">
        <header class="order-section"><h2>选择工单 <small>{{ state.work_orders?.length || 0 }} 张</small></h2>
          <span>{{ state.sop_name }}</span>
          <NButton v-if="state.can_release_selection" :disabled="busy" @click="act('release',{run_index:state.current_run_index})">取消当前选择</NButton></header>
        <div class="run-deck"><article v-for="order in state.work_orders" :key="order.run_index" class="run-card">
          <header><strong>{{ order.label }}</strong><span>{{ runStates[order.state] || order.state }}</span></header>
          <p v-if="order.from_unit || order.to_unit">{{ [order.from_unit,order.to_unit].filter(Boolean).join(' → ') }}</p>
          <div class="run-actions"><div class="order-meter"><progress :max="order.step_count || 1" :value="order.completed_steps" :aria-label="order.label+' 完成进度'"></progress>
            <span>{{ order.completed_steps }} / {{ order.step_count }} 步</span></div>
            <NButton v-if="order.selectable" :disabled="busy" type="primary" @click="activate(order)">进入步骤</NButton></div>
        </article></div>
      </section>
      <section v-else aria-label="工单步骤">
        <header class="order-section"><h2>{{ activeProgress?.label || state.sop_name || '执行步骤' }}</h2>
          <span v-if="activeProgress">已完成 {{ activeProgress.completed_steps }} / {{ activeProgress.step_count }} 步</span></header>
        <div class="order-timeline">
          <article v-for="step in state.steps" :key="step.step_key" :data-position="step.position" :aria-current="step.position==='current'?'step':undefined">
            <header class="step-heading"><span>{{ step.step_index }}</span>
              <div><small>{{ step.position==='current'?'当前步骤':step.position==='previous'?'上一步':'下一步' }}</small><h3>{{ step.content }}</h3></div></header>
            <div class="step-proof"><span v-for="role in roles" :key="role.key" :data-confirmed="!!step[role.key+'_confirmed']">
              <Check v-if="step[role.key+'_confirmed']" :size="13"/>{{ role.label }} · {{ !step[role.key+'_required']?'无需确认':step[role.key+'_confirmed']?'已确认':'待确认' }}</span></div>
            <div v-if="step.photos?.length" class="v2-order-photos">
              <template v-for="photo in step.photos" :key="photo.photo_id"><a v-if="photoUrl(photo)" :href="photoUrl(photo)" :download="isAttachment(photo)?photo.name||'操作附件':undefined" target="_blank" rel="noopener noreferrer">
                <span v-if="isAttachment(photo)">附件 · {{ photo.name || '下载文件' }}</span><img v-else :src="photoUrl(photo)" :alt="photo.name||'操作照片'" loading="lazy"/></a></template></div>
            <footer v-if="step.position==='current'&&state.state==='active'">
              <label v-if="!step.reviewer_confirmed&&(state.role==='reviewer'||!step.operator_confirmed)" class="v2-order-upload"><ImagePlus :size="17"/>添加操作照片
                <input type="file" accept="image/*,.heic,.heif,.avif" capture="environment" :disabled="busy" @change="upload(step,$event)"/></label>
              <NButton type="primary" :disabled="busy||!!blockReason(step)" @click="act('confirm',{step_key:step.step_key})">{{ blockReason(step)||'确认当前步骤（'+state.role_label+'）' }}</NButton>
              <NButton v-if="state.role==='reviewer'&&state.can_rollback_previous" secondary type="error" :disabled="busy" @click="rollbackStep=step.step_key">回退上一步</NButton>
            </footer>
          </article>
        </div>
      </section>
    </template>
    <ConfirmDialog :open="!!rollbackStep" tone="danger" title="回退上一步"
      message="上一步和当前步骤的确认记录、操作照片将被清空，需要重新执行并确认。"
      confirm-label="确认回退" @resolve="resolveRollback"/>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NSkeleton} from 'naive-ui';
import {ArrowLeft,Check,ClipboardList,ImagePlus,RefreshCw} from 'lucide-vue-next';
import ConfirmDialog from '../components/ConfirmDialog.vue';
import {ApiError,requestJson,requestBinaryJson,type Dict} from './client';
import {navigate,registerNavigationGuard} from './navigation';
const props=defineProps<{url:string}>();
const url=new URL(props.url,location.origin),token=url.searchParams.get('token')||'',stepsView=url.pathname.endsWith('/steps');
const state=ref<Dict|null>(null),error=ref(''),busy=ref(false),loading=ref(false),now=ref(Date.now()),rollbackStep=ref('');
const runStates: Record<string,string> = {completed:'已完成',active:'办理中',available:'可办理',locked:'等待其他工单'};
const activeProgress=computed(()=>state.value?.work_orders?.find((run:Dict)=>run.run_index===state.value?.current_run_index));
const roles=[{key:'operator',label:'操作人'},{key:'reviewer',label:'现场审核人'}];
let receivedAt=Date.now(),timer=0,clock=0,abort:AbortController|undefined,disposed=false,forbidden=false;
const overview='/polling-work-order?token='+encodeURIComponent(token);
const stateLabel=computed(()=>state.value?.state==='completed'?'全部步骤已完成，工单表格已上传。':state.value?.state==='upload_pending'?'全部步骤已完成，工单表格正在上传。'+(state.value.last_error||''):'工单已停止，不可继续执行。');
function apply(data:Dict){
  state.value=data;receivedAt=Date.now();now.value=receivedAt;
  if(stepsView&&!data.current_run_index)navigate(overview,{replace:true});
}
function blockReason(step:Dict){
  const role=state.value?.role;
  if(!role||!step[role+'_required'])return '本角色无需确认';
  if(step[role+'_confirmed'])return '已确认';
  if(role==='reviewer'&&step.operator_required&&!step.operator_confirmed)return '等待操作人确认';
  if(step.photo_required!==false&&!step.photos?.length)return '请先上传操作照片';
  const wait=Math.max(0,Math.ceil(Number(step.remaining_seconds||0)-(now.value-receivedAt)/1000));
  return wait ? '等待 '+wait+' 秒':'';
}
function isAttachment(photo:Dict){return photo.mime_type==='application/octet-stream';}
function photoUrl(photo:Dict){
  try{const link=new URL(photo.preview_url,location.origin);return link.origin===location.origin&&link.pathname.startsWith('/api/polling-work-orders/photos/')?link.pathname+link.search:'';}catch{return '';}
}
async function load(){
  window.clearTimeout(timer);
  if(busy.value||loading.value||disposed||document.hidden||forbidden)return;
  if(!token){error.value='工单链接缺少令牌。';return;}
  const c=new AbortController();abort=c;loading.value=true;
  try{const data=await requestJson('/api/polling-work-orders/session?token='+encodeURIComponent(token),{signal:c.signal,cache:'no-store'});if(!c.signal.aborted&&!busy.value){apply(data);error.value='';}}
  catch(e){if(!c.signal.aborted){error.value=(e as Error).message;forbidden=e instanceof ApiError&&[403,404].includes(e.status);}}
  finally{loading.value=false;if(!disposed&&!forbidden&&!document.hidden&&!['completed','cancelled','stopped'].includes(state.value?.state))timer=window.setTimeout(load,4000);}
}
async function act(action:string,fields:Dict):Promise<boolean>{
  if(!state.value||busy.value)return false;
  if(action==='confirm'){
    const step=state.value.steps?.find((s:Dict)=>s.step_key===fields.step_key&&s.position==='current');
    if(!step||blockReason(step)||state.value.state!=='active')return false;
  }
  busy.value=true;error.value='';abort?.abort();window.clearTimeout(timer);
  try{const data=await requestJson('/api/polling-work-orders/'+action,{method:'POST',body:JSON.stringify({token,...fields,expected_version:state.value.version})});apply(data.session||data);return true;}
  catch(e){error.value=(e as Error).message;return false;}
  finally{busy.value=false;if(!disposed)timer=window.setTimeout(load,4000);}
}
async function activate(order:Dict){if(order.selectable&&await act('activate',{run_index:order.run_index}))navigate('/polling-work-order/steps?'+new URLSearchParams({token,run_index:String(state.value?.current_run_index||order.run_index)}));}
async function back(){if(busy.value)return;if(state.value?.can_release_selection&&!await act('release',{run_index:state.value.current_run_index}))return;navigate(overview);}
function resolveRollback(confirmed:boolean){
  const stepKey=rollbackStep.value;
  rollbackStep.value='';
  if(!confirmed||state.value?.role!=='reviewer'||busy.value)return;
  if(!state.value.can_rollback_previous||!state.value.steps?.some((step:Dict)=>step.position==='current'&&step.step_key===stepKey)){
    error.value='工单步骤已变化，请核对当前步骤后再回退。';
    return;
  }
  void act('rollback',{step_key:stepKey});
}
async function upload(step:Dict,event:Event){
  const input=event.target as HTMLInputElement,file=input.files?.[0];input.value='';
  if(!file||!state.value||busy.value)return;
  if(file.size>20*1024*1024){error.value='单个照片或附件不能超过 20MB。';return;}
  busy.value=true;error.value='';abort?.abort();
  try{const data=await requestBinaryJson('/api/polling-work-orders/photo?'+new URLSearchParams({token,step_key:step.step_key,expected_version:String(state.value.version),file_name:file.name}),file,{headers:{'Content-Type':file.type||'application/octet-stream'}});apply(data);}
  catch(e){error.value=(e as Error).message;}
  finally{busy.value=false;if(!disposed)timer=window.setTimeout(load,4000);}
}
async function retryUpload(){
  if(!state.value||busy.value)return;busy.value=true;error.value='';
  try{const data=await requestJson('/api/polling-work-orders/'+encodeURIComponent(state.value.group_id)+'/retry-upload?token='+encodeURIComponent(token),{method:'POST'});if(!data.ok)error.value=data.error||'工单附件仍在等待上传，请稍后核验。';}
  catch(e){error.value=(e as Error).message;}finally{busy.value=false;timer=window.setTimeout(load,4000);}
}
function visibility(){window.clearTimeout(timer);window.clearInterval(clock);if(document.hidden)abort?.abort();else{void load();clock=window.setInterval(()=>now.value=Date.now(),1000);}}
const stopGuard=registerNavigationGuard(()=>!busy.value);
onMounted(()=>{visibility();document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;stopGuard();abort?.abort();window.clearTimeout(timer);window.clearInterval(clock);document.removeEventListener('visibilitychange',visibility);});
</script>
<style scoped>
.order-desk{max-width:1160px;margin-inline:auto;color:var(--v2-ink);padding:clamp(10px,2vw,25px);display:grid;gap:18px}
.order-mast,.order-section,.run-card header,.order-tools,.order-meter,.step-heading,.step-proof,.order-timeline footer,.v2-order-upload,.v2-order-photos{display:flex;align-items:center;gap:9px;flex-wrap:wrap}
.order-mast{padding-bottom:13px;border-bottom:1px solid var(--soft-edge)}.order-mast p,.run-card p{margin:0;color:var(--v2-muted)}.order-mast h1{font:700 clamp(21px,3vw,32px)/1.35 var(--soft-title);margin:3px 0 0}.order-tools{margin-left:auto}
.order-emblem,.step-heading>span{display:grid;place-items:center;width:43px;height:43px;border-radius:14px;background:var(--soft-mint);color:var(--v2-blue);box-shadow:var(--soft-key)}
.order-section{margin-bottom:12px}.order-section h2{font-size:18px;margin:0}.order-section small,.order-section>span,.step-heading small,.order-meter span{color:var(--v2-muted);font-size:12px;font-weight:500}
.run-deck,.order-timeline{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,235px),1fr));gap:12px;align-items:start}
.run-card,.order-timeline article{border-radius:16px;background:var(--soft-face);box-shadow:var(--soft-raised);padding:16px;display:grid;gap:12px;min-width:0}
.run-card{padding:12px;gap:8px}
.run-card header{justify-content:space-between}.run-card header>span,.step-proof span{font-size:12px;color:var(--v2-muted)}
.run-actions{display:flex;align-items:center;gap:8px}.order-meter{flex:1;min-width:0;gap:5px}.order-meter progress{accent-color:var(--v2-blue);height:7px;width:min(155px,55%)}.order-meter span{white-space:nowrap}.run-actions .n-button{flex:none}
.order-timeline article[aria-current=step]{border-top:3px solid var(--v2-blue);grid-column:span 2}
.step-heading{align-items:start;flex-wrap:nowrap}.step-heading>span{flex:none;width:31px;height:31px;border-radius:50%;font-weight:700}.step-heading h3{font-size:16px;font-weight:600;line-height:1.55;white-space:pre-wrap;overflow-wrap:anywhere;margin:3px 0}
.step-proof span{display:inline-flex;gap:3px;align-items:center}.step-proof [data-confirmed=true]{color:var(--v2-blue)}
.v2-order-upload{position:relative;cursor:pointer;border-radius:8px;background:var(--soft-field);padding:6px 9px;color:var(--v2-blue)}.v2-order-upload:focus-within{outline:2px solid currentColor}.v2-order-upload input{position:absolute;inset:0;width:100%;height:100%;opacity:0;cursor:pointer}
.v2-order-photos img{max-width:100px;height:70px;object-fit:contain;border-radius:9px}
@media(max-width:690px){.order-timeline{grid-template-columns:minmax(0,1fr)}.order-timeline article[aria-current=step]{grid-column:auto}.order-tools{margin-left:0}.order-mast{align-items:flex-start}.order-mast h1{font-size:21px}.order-mast>div:not(.order-tools){flex:1;min-width:150px}}
</style>
