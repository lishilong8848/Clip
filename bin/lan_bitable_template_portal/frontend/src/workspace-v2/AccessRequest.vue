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
  <section class="serenity-desk access-desk v2-access-request">
    <header class="serenity-heading"><div><h1>申请权限</h1><p>{{ user.name }} · {{ data.current?.level || user.level }}</p></div>
      <div class="serenity-actions"><NButton :loading="loading" :disabled="busy" aria-label="刷新申请结果" @click="load"><RefreshCw :size="15" />刷新</NButton><NButton @click="emit('back')">返回个人信息</NButton></div></header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <article v-if="request.request_id" class="serenity-panel access-receipt"><div><small>最近一次申请</small>
      <h2>{{ ({pending:'等待审批',approved:'权限已生效',rejected:'申请已驳回',superseded:'已提交新申请'} as Record<string,string>)[request.status] || '申请状态待核验' }}</h2></div>
      <div><strong>{{ request.approver_name }}</strong><p>{{ request.reject_reason || request.reason }}</p></div>
      <NButton v-if="request.notification_status==='failed'" :disabled="busy" @click="retry">重试审批提醒</NButton></article>
    <form class="access-grid" @submit.prevent="submit">
      <section class="serenity-panel access-settings"><h2>审批与岗位</h2>
        <label>审批人<NSelect v-model:value="form.approver" :options="people" filterable :disabled="busy" aria-label="审批人"/></label>
        <div class="access-pair"><label>权限等级<NSelect v-model:value="form.level" :options="approver?.level_options||[]" :disabled="!approver||busy" aria-label="权限等级"/></label>
        <label>岗位<NSelect v-model:value="form.category" :options="approver?.category_options||[]" :disabled="!approver||busy" aria-label="岗位"/></label></div>
        <label>申请原因<NInput v-model:value="form.reason" type="textarea" :disabled="busy" aria-label="申请原因"/></label>
      </section>
      <section class="serenity-panel access-grants"><header class="serenity-heading"><h2>授权范围</h2>
        <NButton type="primary" attr-type="submit" :loading="busy" :disabled="loading||uncertain||!approver||!form.level||!form.category||!form.scopes.length||!form.reason.trim()||(!dirty && request.status==='pending')"><Send :size="15" />提交申请</NButton></header>
        <p v-if="!approver">选择审批人后可查看申请范围。</p>
        <template v-else><h3>楼栋</h3><NCheckboxGroup v-model:value="form.scopes" :disabled="busy || ['L0','L1'].includes(form.level)">
          <NCheckbox v-for="option in scopeOptions" :key="option.value" :value="option.value">{{ option.label }}</NCheckbox></NCheckboxGroup>
          <h3>功能模块</h3><NCheckboxGroup v-model:value="form.modules" :disabled="busy"><div v-for="group in groups" :key="group.label" class="access-block">
            <strong>{{ group.label }}</strong><div><NCheckbox v-for="module in group.items" :key="module.value" :value="module.value">{{ module.label }}</NCheckbox></div></div></NCheckboxGroup></template>
      </section>
    </form>
  </section>
</template>
<style scoped>
.access-grid{display:grid;grid-template-columns:minmax(235px,.85fr) minmax(0,1.6fr);gap:12px;align-items:start}.access-settings{display:grid;gap:9px}.access-settings label{display:grid;gap:4px}.access-pair{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.access-desk h2{margin:0;font-size:17px}.access-desk h3{font-size:14px;margin:10px 0 5px}.access-block{display:grid;grid-template-columns:minmax(5em,.24fr) 1fr;align-items:baseline;gap:10px;padding:6px 0;border-top:1px solid var(--soft-edge)}.access-block .n-checkbox,.access-grants>.n-checkbox-group>.n-checkbox{margin:3px 8px 3px 0}
.access-receipt{display:flex;align-items:center;flex-wrap:wrap;gap:8px 18px}.access-receipt small,.access-receipt p{color:var(--v2-muted);margin:0}
@media(max-width:700px){.access-grid{grid-template-columns:1fr}.access-block{grid-template-columns:1fr;gap:3px}}
</style>
