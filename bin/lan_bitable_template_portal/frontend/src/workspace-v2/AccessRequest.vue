<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,reactive,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NCheckboxGroup,NInput,NSelect,NTag,useDialog,useMessage} from 'naive-ui';
import {ArrowLeft,LayoutGrid,RefreshCw,Send,ShieldCheck} from 'lucide-vue-next';
import {ApiError,requestJson,requestPeople,type Dict} from './client';
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
const emptyApprovers=computed(()=>data.value.empty_reason || (error.value ? '审批人读取失败，请刷新。' : '暂无可选审批人'));
const notificationLabel=computed(()=>({queued:'飞书审批卡片待发送',sent:'飞书审批卡片已发送',failed:'飞书审批卡片发送失败',expired:'飞书审批卡片已过期'} as Record<string,string>)[request.value.notification_status] || '');
const scopeOptions=computed(()=> (data.value.scope_options||[]).filter((row:Dict)=>approver.value?.scopes?.includes(row.value)&&(row.value!=='ALL'||['L0','L1'].includes(form.level))));
const moduleOptions=computed(()=> (data.value.module_options||[]).filter((row:Dict)=>approver.value?.modules?.includes(row.value)&&!(row.value==='accounts'&&form.level==='L4')&&(!row.admin_only||['L0','L1'].includes(form.level))));
const groups=computed(()=>[...new Set(moduleOptions.value.map((row:Dict)=>row.group||'功能模块'))].map(group=>({label:String(group),items:moduleOptions.value.filter((row:Dict)=>(row.group||'功能模块')===group)})));
const receiptLabel=computed(()=>({pending:'等待审批',approved:'权限已生效',rejected:'申请已驳回',superseded:'已提交新申请'} as Record<string,string>)[request.value.status]||'申请状态待核验');
const receiptTone=computed(()=>request.value.status==='approved'?'success':request.value.status==='rejected'?'error':request.value.status==='pending'?'warning':'default');
let timer=0,disposed=false,statusSequence=0;
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
async function load(fresh=false){
  if(busy.value||loading.value)return;loading.value=true;error.value='';
  try{
    const receipt=status(false).then(()=>{uncertain.value=false;},e=>{
      uncertain.value=true;error.value=(e as Error).message;
    });
    const [,directory]=await Promise.allSettled([receipt,refreshApprovers(fresh)]);
    if(directory.status==='rejected')throw directory.reason;
    if(!form.approver&&request.value.approver_open_id&&people.value.some((item:Dict)=>item.value===request.value.approver_open_id)){
      form.approver=request.value.approver_open_id;await nextTick();
      if(request.value.requested_access){form.level=request.value.requested_access.level;await nextTick();for(const key of ['category','scopes','modules'] as const)(form as Dict)[key]=request.value.requested_access[key];form.reason=request.value.reason||'';}
      else form.scopes=[...new Set([...form.scopes,...(request.value.requested_scopes||[])])];
      await nextTick();baseline.value=JSON.stringify(form);
    }
    if(matchesRequest())baseline.value=JSON.stringify(form);
  }
  catch(e){if(!disposed)error.value=(e as Error).message;}finally{loading.value=false;}
}
async function refreshApprovers(fresh=false){
  const sequence=statusSequence;
  try{
    const options=await requestPeople('/api/auth/permission-approvers',{signal:abort.signal,fresh});
    if(disposed||sequence!==statusSequence)return;
    data.value=options;
    if(form.approver&&!approver.value)form.approver='';
  }catch(e){
    if(disposed||sequence!==statusSequence)return;
    data.value={...data.value,items:[],empty_reason:''};
    throw e;
  }
}
async function status(refreshOptions=true){
  const id=request.value.request_id,sequence=++statusSequence;
  const result=await requestJson('/api/auth/permission-requests/current'+(id?'?request_id='+encodeURIComponent(id):''),{signal:abort.signal,cache:'no-store'});
  if(disposed||sequence!==statusSequence||id!==request.value.request_id)return;
  const previous=request.value.status;request.value=result.request||{};
  if(previous==='pending'&&request.value.status!=='pending'){
    emit('changed');
    if(refreshOptions&&request.value.status==='approved')await refreshApprovers(true);
  }
  window.clearTimeout(timer);
  if(request.value.status==='pending')timer=window.setTimeout(poll,10000);
}
async function poll(){if(disposed)return;try{if(!document.hidden&&!busy.value&&!loading.value)await status();}catch(e){if(!disposed)error.value=(e as Error).message;}finally{if(!disposed&&request.value.status==='pending'){window.clearTimeout(timer);timer=window.setTimeout(poll,10000);}}}
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
    <header class="serenity-heading access-heading"><div><h1>申请权限</h1><p>{{ user.name }} · {{ data.current?.level || user.level }}</p></div>
      <div class="serenity-actions"><NButton :loading="loading" :disabled="busy" aria-label="刷新申请结果" @click="load(true)"><RefreshCw :size="15" />刷新结果</NButton>
        <NButton @click="emit('back')"><ArrowLeft :size="15" />返回个人信息</NButton></div></header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <article v-if="request.request_id" class="serenity-panel access-receipt"><div><small>最近一次申请</small>
      <h2><NTag size="small" :bordered="false" :type="receiptTone">{{ receiptLabel }}</NTag></h2></div>
      <div class="receipt-copy"><strong v-if="request.approver_name">审批人 · {{ request.approver_name }}</strong><p>{{ request.reject_reason || request.reason }}</p>
        <p v-if="notificationLabel" role="status">{{ notificationLabel }}<template v-if="request.notification_error"> · {{ request.notification_error }}</template></p></div>
      <NButton v-if="request.status==='pending' && request.notification_status==='failed'" :disabled="busy||loading" @click="retry">重试审批提醒</NButton></article>
    <form class="access-grid" @submit.prevent="submit">
      <section class="serenity-panel access-settings"><h2><ShieldCheck :size="17" aria-hidden="true"/>申请信息</h2>
        <label>审批人<NSelect v-model:value="form.approver" :options="people" filterable :loading="loading" :disabled="busy || loading || !people.length" :placeholder="loading ? '读取审批人…' : people.length ? '选择接收飞书审批卡片的人' : '暂无可选审批人'" aria-label="审批人"/></label>
        <p v-if="!loading && !people.length" role="status">{{ emptyApprovers }}</p>
        <div class="access-pair"><label>权限等级<NSelect v-model:value="form.level" :options="approver?.level_options||[]" :disabled="!approver||busy" aria-label="权限等级"/></label>
        <label>岗位<NSelect v-model:value="form.category" :options="approver?.category_options||[]" :disabled="!approver||busy" aria-label="岗位"/></label></div>
        <label>申请原因<NInput v-model:value="form.reason" type="textarea" :autosize="{minRows:3,maxRows:6}" :disabled="busy" aria-label="申请原因" placeholder="填写申请该权限的事由"/></label>
      </section>
      <section class="serenity-panel access-grants"><header class="serenity-heading"><h2><LayoutGrid :size="17" aria-hidden="true"/>授权范围</h2></header>
        <p v-if="!approver" class="access-empty">{{ loading?'正在读取可申请的范围…':'选择审批人后可查看申请范围。' }}</p>
        <template v-else-if="approver"><h3>楼栋</h3><NCheckboxGroup v-model:value="form.scopes" :disabled="busy || ['L0','L1'].includes(form.level)">
          <NCheckbox v-for="option in scopeOptions" :key="option.value" :value="option.value">{{ option.label }}</NCheckbox></NCheckboxGroup>
          <h3>功能模块</h3><NCheckboxGroup v-model:value="form.modules" :disabled="busy"><div v-for="group in groups" :key="group.label" class="access-block">
            <strong>{{ group.label }}</strong><div><NCheckbox v-for="module in group.items" :key="module.value" :value="module.value">{{ module.label }}<small v-if="module.includes">含 {{ module.includes }}</small></NCheckbox></div></div></NCheckboxGroup></template>
      </section>
      <footer class="access-submit">
        <small v-if="approver">已选 {{ form.scopes.length }} 个楼栋 · {{ form.modules.length }} 项功能</small>
        <NButton type="primary" attr-type="submit" :loading="busy" :disabled="loading||uncertain||!approver||!form.level||!form.category||!form.scopes.length||!form.reason.trim()||(!dirty && request.status==='pending')"><Send :size="15" />提交申请</NButton>
      </footer>
    </form>
  </section>
</template>
<style scoped>
.access-desk{width:min(100%,1180px);margin-inline:auto}
.access-heading{padding:12px 15px;border:1px solid var(--soft-edge);border-radius:12px;background:var(--soft-title-line)}
.access-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.6fr);gap:10px;align-items:start}
.access-settings{display:grid;gap:10px}.access-settings label{display:grid;gap:5px;min-width:0;font-weight:550}
.access-pair{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
.access-desk h2{margin:0;font-size:16px;display:flex;align-items:center;gap:6px}
.access-desk h2 svg{color:var(--v2-blue)}.access-desk h3{font-size:13px;margin:10px 0 5px}
.access-block{display:grid;grid-template-columns:6em minmax(0,1fr);align-items:baseline;gap:7px;padding:6px 0;border-top:1px solid var(--soft-edge)}
.access-block .n-checkbox,.access-grants>.n-checkbox-group>.n-checkbox{margin:3px 8px 3px 0}
.access-block small{display:block;color:var(--v2-muted);line-height:1.4;white-space:normal}
.access-receipt{display:flex;align-items:center;flex-wrap:wrap;gap:8px 14px}.access-receipt h2{margin-bottom:4px}
.receipt-copy{min-width:0;flex:1 1 14rem;overflow-wrap:anywhere}
.access-receipt p,.access-settings p,.access-empty{color:var(--v2-muted);margin:3px 0}
.access-empty{padding-top:8px}
.access-submit{grid-column:1/-1;display:flex;justify-content:flex-end;flex-wrap:wrap;align-items:center;gap:8px;padding:9px 12px;border:1px solid var(--soft-edge);border-radius:10px;background:var(--soft-face)}
.access-submit small{margin-inline-end:auto;color:var(--v2-muted)}
.access-desk :deep(.n-button__content){gap:5px}
@media(max-width:700px){.access-grid,.access-block{grid-template-columns:minmax(0,1fr)}.access-block{gap:3px}.access-desk :deep(.n-button){min-height:44px}}
</style>
