<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,reactive,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NCheckboxGroup,NEmpty,NForm,NFormItem,NInput,NSelect,NSpin,NTag,useDialog,useMessage} from 'naive-ui';
import {ArrowLeft,RefreshCw,Send,ShieldCheck} from 'lucide-vue-next';
import {ApiError,requestJson,type Dict} from './client';
import {registerNavigationGuard} from './navigation';
const props=defineProps<{user:Dict}>();
const emit=defineEmits<{back:[];changed:[]}>();
const data=ref<Dict>({}),loading=ref(false),busy=ref(false),error=ref(''),request=ref<Dict>({});
const form=reactive({approver:'',level:'',category:'',scopes:[] as string[],modules:[] as string[],reason:''});
const dialog=useDialog(),message=useMessage(),baseline=ref(JSON.stringify(form)),uncertain=ref(false);
const dirty=computed(()=>JSON.stringify(form)!==baseline.value);
function matchesRequest(){
 const saved=request.value.requested_access;
 const same=(a:string[],b:string[])=>JSON.stringify([...a].sort())===JSON.stringify([...b].sort());
 return saved&&request.value.approver_open_id===form.approver&&request.value.reason===form.reason.trim()&&saved.level===form.level&&saved.category===form.category&&same(saved.scopes||[],form.scopes)&&same(saved.modules||[],form.modules);
}
const approver=computed<Dict|undefined>(()=>data.value.items?.find((row:Dict)=>row.open_id===form.approver));
const people=computed(()=> (data.value.items||[]).map((row:Dict)=>({value:row.open_id,label:row.name+' · '+row.level})));
const scopeOptions=computed(()=> (data.value.scope_options||[]).filter((row:Dict)=>approver.value?.scopes?.includes(row.value)&&(row.value!=='ALL'||['L0','L1'].includes(form.level))));
const moduleOptions=computed(()=> (data.value.module_options||[]).filter((row:Dict)=>approver.value?.modules?.includes(row.value)&&!(row.value==='accounts'&&form.level==='L4')));
const groups=computed(()=>[...new Set(moduleOptions.value.map((row:Dict)=>row.group||'功能模块'))].map(group=>({label:String(group),items:moduleOptions.value.filter((row:Dict)=>(row.group||'功能模块')===group)})));
let timer=0,disposed=false;
const abort=new AbortController();
const stop=registerNavigationGuard((_url,proceed)=>{if(busy.value){message.warning('正在提交申请，请等待结果');return false;}if(dirty.value){dialog.warning({title:'放弃未提交的权限申请？',positiveText:'放弃修改',negativeText:'继续填写',onPositiveClick:proceed});return false;}return true;});
watch(()=>form.approver,()=>{
  const own=data.value.current||{}, levels=approver.value?.level_options||[], categories=approver.value?.category_options||[];
  form.level=levels.some((row:Dict)=>row.value===own.level)?own.level:levels.at(-1)?.value||'';
  form.category=categories.some((row:Dict)=>row.value===own.category)?own.category:categories[0]?.value||'';
  form.scopes=(own.scope_rules||[]).filter((value:string)=>scopeOptions.value.some((row:Dict)=>row.value===value));
  form.modules=(own.modules||[]).filter((value:string)=>moduleOptions.value.some((row:Dict)=>row.value===value));
});
watch(()=>form.level,()=>{form.modules=form.modules.filter(value=>moduleOptions.value.some((row:Dict)=>row.value===value));form.scopes=['L0','L1'].includes(form.level)?scopeOptions.value.map((row:Dict)=>row.value):form.scopes.filter(value=>scopeOptions.value.some((row:Dict)=>row.value===value));});
async function load(){
  if(busy.value||loading.value)return;loading.value=true;error.value='';
  try{data.value=await requestJson('/api/auth/permission-approvers',{signal:abort.signal,cache:'no-store'});await status();
    if(!form.approver&&request.value.approver_open_id&&people.value.some((item:Dict)=>item.value===request.value.approver_open_id)){
      form.approver=request.value.approver_open_id;await nextTick();
      if(request.value.requested_access){form.level=request.value.requested_access.level;await nextTick();for(const key of ['category','scopes','modules'] as const)(form as Dict)[key]=request.value.requested_access[key];form.reason=request.value.reason||'';}
      else form.scopes=[...new Set([...form.scopes,...(request.value.requested_scopes||[])])];
      await nextTick();baseline.value=JSON.stringify(form);
    }
    if(matchesRequest())baseline.value=JSON.stringify(form);
    uncertain.value=false;
  }
  catch(e){if(!disposed)error.value=(e as Error).message;}finally{loading.value=false;}
}
async function status(){
  const id=request.value.request_id;
  const result=await requestJson('/api/auth/permission-requests/current'+(id?'?request_id='+encodeURIComponent(id):''),{signal:abort.signal,cache:'no-store'});
  if(disposed||id!==request.value.request_id)return;
  const previous=request.value.status;request.value=result.request||{};
  if(previous==='pending'&&request.value.status!=='pending'){
    emit('changed');
    if(request.value.status==='approved')data.value=await requestJson('/api/auth/permission-approvers',{signal:abort.signal,cache:'no-store'});
  }
  window.clearTimeout(timer);
  if(request.value.status==='pending')timer=window.setTimeout(poll,10000);
}
async function poll(){if(disposed)return;try{if(!document.hidden&&!busy.value)await status();}catch(e){if(!disposed)error.value=(e as Error).message;}finally{if(!disposed&&request.value.status==='pending'){window.clearTimeout(timer);timer=window.setTimeout(poll,10000);}}}
async function submit(){
  if(busy.value||uncertain.value||!approver.value||!form.level||!form.category||!form.scopes.length||!form.reason.trim()||(!dirty.value&&request.value.status==='pending'))return;
  busy.value=true;error.value='';
  try{const result=await requestJson('/api/auth/permission-requests',{method:'POST',body:JSON.stringify({scopes:form.scopes,reason:form.reason,approver_open_id:form.approver,requested_access:{level:form.level,category:form.category,scopes:form.scopes,modules:form.modules}})});request.value=result.request;baseline.value=JSON.stringify(form);await status();}
  catch(e){error.value=(e as Error).message;if(!(e instanceof ApiError)||!e.status||e.status>=500){uncertain.value=true;error.value+=' 提交结果尚未确认，请先刷新申请结果，勿重复提交。';}}finally{busy.value=false;}
}
async function retry(){busy.value=true;error.value='';try{await requestJson('/api/auth/permission-requests/'+encodeURIComponent(request.value.request_id)+'/notify',{method:'POST',body:'{}'});await status();}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
function unload(event:BeforeUnloadEvent){if(busy.value||dirty.value){event.preventDefault();event.returnValue='';}}
onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});onBeforeUnmount(()=>{disposed=true;abort.abort();window.clearTimeout(timer);stop();window.removeEventListener('beforeunload',unload);});
</script>
<template>
 <section class="v2-access-request" data-v2-inline-feedback>
  <header><div><ShieldCheck :size="22" /><h1>本人权限申请</h1></div><NButton :disabled="busy" data-v2-navigation @click="emit('back')"><ArrowLeft :size="16" />返回</NButton></header>
  <NAlert v-if="error" type="error" :bordered="false">{{error}}</NAlert>
  <NSpin :show="loading">
   <div class="v2-access-current"><strong>{{user.name}}</strong><NTag size="small">{{data.current?.level||user.level}}</NTag><span>{{(data.current?.scope_rules||[]).join('、')}} · {{data.current?.modules?.length||0}} 项权限</span><NButton quaternary circle aria-label="刷新申请结果" :disabled="busy" @click="load"><RefreshCw :size="17" /></NButton></div>
   <NAlert v-if="request.request_id" :type="request.status==='approved'?'success':request.status==='rejected'?'warning':'info'" :bordered="false" class="v2-access-state">
    {{({pending:'等待上级审批',approved:'已通过，权限已生效',rejected:'申请已拒绝',superseded:'已由新申请替代'} as Record<string,string>)[request.status]||'申请已失效'}} · {{request.approver_name}}
    <p v-if="request.reject_reason">{{request.reject_reason}}</p>
    <p v-if="request.notification_status==='failed'">{{request.notification_error}} <NButton text :disabled="busy" @click="retry">重试发送审批表单</NButton></p>
   </NAlert>
   <NEmpty v-if="!loading&&!people.length" :description="data.current?.is_fixed_superuser?'固定超级管理员已拥有全楼栋及全部功能权限':'暂无符合管理范围的审批人，请联系超级管理员'" />
   <NForm v-else label-placement="top" :disabled="busy" @submit.prevent="submit">
    <div class="v2-access-fields"><NFormItem label="上级审批人" required><NSelect v-model:value="form.approver" :options="people" filterable placeholder="选择上级审批人" aria-label="上级审批人" /></NFormItem><NFormItem label="目标级别" required><NSelect v-model:value="form.level" :options="approver?.level_options||[]" :disabled="!approver||busy" aria-label="目标级别" /></NFormItem><NFormItem label="岗位" required><NSelect v-model:value="form.category" :options="approver?.category_options||[]" :disabled="!approver||busy" aria-label="申请岗位" /></NFormItem></div>
    <NFormItem label="可访问楼栋" required><NCheckboxGroup v-model:value="form.scopes" :disabled="busy||['L0','L1'].includes(form.level)"><NCheckbox v-for="item in scopeOptions" :key="item.value" :value="item.value">{{item.label}}</NCheckbox></NCheckboxGroup></NFormItem>
    <fieldset v-for="group in groups" :key="group.label"><legend>{{group.label}}</legend><NCheckboxGroup v-model:value="form.modules" :disabled="busy"><NCheckbox v-for="item in group.items" :key="item.value" :value="item.value">{{item.label}}</NCheckbox></NCheckboxGroup></fieldset>
    <NFormItem label="申请原因" required><NInput v-model:value="form.reason" type="textarea" :autosize="{minRows:2,maxRows:5}" maxlength="500" :input-props="{'aria-label':'申请原因'}" /></NFormItem>
    <footer><NButton type="primary" attr-type="submit" :loading="busy" :disabled="busy||uncertain||(!dirty&&request.status==='pending')||!approver||!form.level||!form.category||!form.scopes.length||!form.reason.trim()"><Send :size="16" />提交并发送飞书审批</NButton></footer>
   </NForm>
  </NSpin>
 </section>
</template>
<style scoped>
.v2-access-request{max-width:1100px}.v2-access-request>header,.v2-access-request>header>div,.v2-access-current{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.v2-access-request>header{justify-content:space-between;margin-bottom:18px}h1{margin:0}.v2-access-current{padding:14px 0;border-bottom:1px solid var(--v2-border);margin-bottom:18px;font-size:13px}.v2-access-current>.n-button{margin-left:auto}.v2-access-state{margin-bottom:18px}.v2-access-fields{display:grid;grid-template-columns:2fr 1fr 1fr;gap:18px}fieldset{border:0;border-top:1px solid var(--v2-border);padding:14px 0;margin:0 0 18px}legend{font-size:13px;font-weight:600;padding-right:12px}.n-checkbox-group{display:flex;flex-wrap:wrap;gap:12px 20px}.n-button :deep(.n-button__content){gap:6px}footer{display:flex;justify-content:flex-end;padding:12px 0}
@media(max-width:760px){.v2-access-fields{grid-template-columns:1fr;gap:0}.n-checkbox-group{gap:10px}.n-checkbox{min-height:36px;align-items:center}footer>.n-button{width:100%}}
</style>
