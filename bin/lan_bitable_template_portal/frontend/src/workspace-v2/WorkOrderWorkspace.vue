<template>
  <section class="v2-work-order" aria-label="工单执行">
    <header><div><p>{{ state?.role_label }}<span v-if="state?.assigned_person?.name"> · {{ state.assigned_person.name }}</span></p><h1>{{ state?.title || '工单执行' }}</h1><p>{{ state?.sop_name }}</p></div><NButton v-if="stepsView" data-v2-navigation :disabled="busy" @click="back"><ArrowLeft :size="16" />返回工单</NButton><NButton :loading="loading" :disabled="busy" @click="load"><RefreshCw :size="16" />刷新</NButton></header>
    <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }}</NAlert>
    <NSkeleton v-if="!state&&loading" :repeat="3" height="100px" />
    <template v-if="state">
      <NAlert v-if="['completed','upload_pending','cancelled','stopped'].includes(state.state)" :type="state.state==='completed'?'success':'info'" :bordered="false">{{ stateLabel }}<NButton v-if="state.state==='upload_pending'" :disabled="busy" :loading="busy" @click="retryUpload">重试上传工单表格</NButton></NAlert>
      <template v-if="!stepsView">
        <NButton v-if="state.can_release_selection" :disabled="busy" @click="act('release',{run_index:state.current_run_index})">取消当前选择</NButton>
        <div class="v2-order-list"><NCard v-for="order in state.work_orders||[]" :key="order.run_index" :title="'工单 '+order.run_index" size="small"><p>{{ order.label }}</p><NProgress type="line" :percentage="order.step_count ? Math.round(order.completed_steps/order.step_count*100):0" /><p>已完成 {{ order.completed_steps }} / {{ order.step_count }} 步</p><NButton type="primary" :disabled="busy||!order.selectable" :loading="busy" @click="activate(order)">{{ order.state==='completed'?'已完成':order.state==='locked'?'其他工单执行中':'进入步骤' }}</NButton></NCard></div>
      </template>
      <div v-else class="v2-order-steps"><NCard v-for="step in state.steps||[]" :key="step.step_key" size="small" :class="{'is-current':step.position==='current'}" :title="'第 '+step.step_index+' 步'"><template #header-extra><NTag :type="step.position==='current'?'info':'default'" size="small">{{ step.position==='current'?'当前步骤':step.position==='previous'?'上一步':'下一步' }}</NTag></template><p class="v2-order-content">{{ step.content }}</p><div class="v2-order-checks"><NTag v-for="role in roles" :key="role.key" :type="step[role.key+'_confirmed']?'success':'default'" size="small">{{ role.label }}{{ !step[role.key+'_required']?'无需确认':step[role.key+'_confirmed']?'已确认':'待确认' }}</NTag><NTag size="small">{{ step.photos?.length ? step.photos.length+'张操作照片':step.photo_required===false?'无需照片':'待上传照片' }}</NTag></div><div class="v2-order-photos"><a v-for="photo in step.photos||[]" :key="photo.photo_id" :href="photoUrl(photo)" target="_blank" rel="noopener noreferrer"><img v-if="photoUrl(photo)" :src="photoUrl(photo)" :alt="photo.name||'操作照片'" loading="lazy" /></a></div>
          <template v-if="step.position==='current'&&state.state==='active'">
            <label v-if="!step.reviewer_confirmed&&(state.role==='reviewer'||!step.operator_confirmed)" class="v2-order-upload"><ImagePlus :size="18" />添加操作照片<input type="file" accept="image/*" capture="environment" :disabled="busy" @change="upload(step,$event)" /></label>
            <div class="v2-order-actions"><NButton type="primary" :disabled="busy||!!blockReason(step)" :loading="busy" @click="act('confirm',{step_key:step.step_key})">{{ blockReason(step)||'确认当前步骤（'+state.role_label+'）' }}</NButton><NButton v-if="state.role==='reviewer'&&state.can_rollback_previous" type="error" secondary :disabled="busy" @click="rollback(step)">回退上一步</NButton></div>
          </template>
        </NCard></div>
    </template>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NCard,NProgress,NSkeleton,NTag,useDialog} from 'naive-ui';
import {ArrowLeft,ImagePlus,RefreshCw} from 'lucide-vue-next';
import {ApiError,requestJson,requestBinaryJson,type Dict} from './client';
import {navigate,registerNavigationGuard} from './navigation';
const props=defineProps<{url:string}>(),dialog=useDialog();
const url=new URL(props.url,location.origin),token=url.searchParams.get('token')||'',stepsView=url.pathname.endsWith('/steps');
const state=ref<Dict|null>(null),error=ref(''),busy=ref(false),loading=ref(false),now=ref(Date.now());
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
function rollback(step:Dict){
  if(state.value?.role!=='reviewer'||!state.value.can_rollback_previous||busy.value)return;
  dialog.warning({title:'回退上一步',content:'上一步和当前步骤的确认记录、操作照片将被清空，需要重新执行并确认。',positiveText:'确认回退',negativeText:'取消',onPositiveClick:async()=>{return await act('rollback',{step_key:step.step_key});}});
}
async function upload(step:Dict,event:Event){
  const input=event.target as HTMLInputElement,file=input.files?.[0];input.value='';
  if(!file||!state.value||busy.value)return;
  if(!file.type.startsWith('image/')||file.size>8*1024*1024){error.value='请选择8MB以内的操作照片。';return;}
  busy.value=true;error.value='';abort?.abort();
  try{const data=await requestBinaryJson('/api/polling-work-orders/photo?'+new URLSearchParams({token,step_key:step.step_key,expected_version:String(state.value.version),file_name:file.name}),file,{headers:{'Content-Type':file.type}});apply(data);}
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
<style>
.v2-work-order{max-width:1080px;margin:0 auto;padding:24px 0}.v2-work-order>header{display:flex;align-items:center;gap:12px;margin-bottom:20px}.v2-work-order>header>div{flex:1;min-width:0}.v2-work-order h1{font-size:22px;line-height:1.5;margin:0;overflow-wrap:anywhere}.v2-work-order p{font-size:14px;line-height:1.8}.v2-work-order>header p{color:var(--v2-muted);margin:6px 0}.v2-order-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;margin-top:16px}.v2-order-steps{display:grid;gap:16px;margin-top:16px}.v2-order-steps .is-current{border-color:var(--v2-blue)}.v2-order-content{white-space:pre-wrap;overflow-wrap:anywhere}.v2-order-checks,.v2-order-actions,.v2-order-photos{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}.v2-order-photos img{width:100px;height:80px;object-fit:contain;border:1px solid var(--v2-border);border-radius:6px}.v2-order-upload{position:relative;display:flex;align-items:center;gap:8px;border:1px dashed #adc5df;border-radius:6px;padding:12px;margin-top:14px;cursor:pointer}.v2-order-upload input{position:absolute;inset:0;opacity:0;width:100%;cursor:pointer}.v2-work-order .n-button__content{gap:6px}.v2-work-order .n-alert{margin-bottom:16px}@media(max-width:760px){.v2-work-order{padding:12px 0}.v2-work-order>header{flex-wrap:wrap}.v2-work-order>header>div{flex-basis:100%}.v2-work-order h1{font-size:18px}.v2-order-actions .n-button{min-height:44px;white-space:normal}}
</style>
