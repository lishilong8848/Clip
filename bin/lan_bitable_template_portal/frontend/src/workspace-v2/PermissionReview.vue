<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NCheckboxGroup,NEmpty,NInput,NModal,NPagination,NSelect,NSpin,NTag,useMessage} from 'naive-ui';
import {Check,RefreshCw,Search,X} from 'lucide-vue-next';
import {ApiError,requestJson,type Dict} from './client';
import {registerNavigationGuard} from './navigation';
const props=defineProps<{scopes:{value:string;label:string}[];modules:{value:string;label:string}[];categories:{value:string;label:string}[]}>();
const emit=defineEmits<{changed:[]}>(),message=useMessage();
const rows=ref<Dict[]>([]),loading=ref(false),busy=ref(false),error=ref(''),notice=ref(''),uncertain=ref(false);
const status=ref('pending'),search=ref(''),page=ref(1),selected=ref<string[]>([]),review=ref<Dict[]>([]),action=ref('approve'),reason=ref('');
const choices=[{value:'pending',label:'待审批'},{value:'all',label:'全部申请'},{value:'approved',label:'已通过'},{value:'rejected',label:'已拒绝'}];
const statuses:Record<string,string>={pending:'待审批',approved:'已通过',rejected:'已拒绝',expired:'已过期',superseded:'已被新申请替代',failed:'已失败'};
const filtered=computed(()=>rows.value.filter(row=>(row.name+' '+row.reason+' '+(row.requested_scope_labels||[]).join(' ')).includes(search.value.trim())));
const visible=computed(()=>filtered.value.slice((page.value-1)*20,page.value*20));
const names=(values:string[],options:{value:string;label:string}[])=>values.map(value=>options.find(item=>item.value===value)?.label||value).join('、')||'无';
let read:AbortController|undefined,disposed=false;
async function load(){
 if(busy.value)return;read?.abort();const controller=new AbortController();read=controller;loading.value=true;error.value='';
 try{const result=await requestJson('/api/auth/permission-requests/admin?'+new URLSearchParams({status:status.value,limit:'200'}),{signal:controller.signal,cache:'no-store'});
  if(controller.signal.aborted)return;rows.value=result.items||[];selected.value=selected.value.filter(id=>rows.value.some(row=>row.request_id===id&&row.status==='pending'));uncertain.value=false;
 }catch(e){if(!controller.signal.aborted){error.value=(e as Error).message;if(e instanceof ApiError&&[401,403].includes(e.status)){rows.value=[];selected.value=[];review.value=[];}}}
 finally{if(read===controller)loading.value=false;}
}
function choose(row:Dict,checked:boolean){selected.value=checked?[...new Set([...selected.value,row.request_id])]:selected.value.filter(id=>id!==row.request_id);}
function open(items:Dict[],kind:string){if(busy.value||uncertain.value||!items.length)return;action.value=kind;reason.value='';error.value='';review.value=items.map(row=>({...row,review_scopes:[...(row.requested_scopes||[])]}));}
async function submit(){
 if(busy.value||uncertain.value||!review.value.length)return;
 if(action.value==='reject'&&!reason.value.trim()){error.value='请填写拒绝原因';return;}
 if(action.value==='approve'&&review.value.some(row=>!row.review_scopes.length)){error.value='请为每条申请选择审批楼栋';return;}
 busy.value=true;error.value='';notice.value='';
 try{
  const targets=review.value,ids=targets.map(row=>row.request_id),scopeMap=Object.fromEntries(targets.map(row=>[row.request_id,row.review_scopes]));
  const single=ids.length===1;
  const result=await requestJson('/api/auth/permission-requests/'+(single?encodeURIComponent(ids[0])+'/'+action.value:'bulk-'+action.value),{method:'POST',body:JSON.stringify(single?{scopes:targets[0].review_scopes,reason:reason.value}:{request_ids:ids,scopes_by_request_id:scopeMap,reason:reason.value})});
  const failures=result.failed||[];
  notice.value=single?(action.value==='approve'?'申请已通过，权限已生效':'申请已拒绝'):`已处理 ${result.items?.length||0} 条，未完成 ${failures.length} 条`;
  if(result.notification?.ok===false)notice.value+='；结果通知未送达，但审批已生效';
  if(failures.length)notice.value+='。'+failures.map((item:Dict)=>item.error||item.message||'请刷新核验').join('；');
  review.value=[];selected.value=failures.map((item:Dict)=>item.request_id);emit('changed');window.dispatchEvent(new Event('auth-account-changed'));
 }catch(e){error.value=(e as Error).message;if(!(e instanceof ApiError)||!e.status||e.status>=500){uncertain.value=true;error.value+=' 提交结果尚未确认，请先刷新申请结果，勿重复提交。';}}
 finally{busy.value=false;if(!review.value.length&&!disposed)await load();}
}
function close(){if(!busy.value)review.value=[];}
const stop=registerNavigationGuard(()=>{if(busy.value){message.warning('正在提交审批，请等待结果');return false;}return true;});
function unload(event:BeforeUnloadEvent){if(busy.value){event.preventDefault();event.returnValue='';}}
watch([status,search],()=>{page.value=1;});watch(status,load);watch(()=>filtered.value.length,()=>{page.value=Math.min(page.value,Math.max(1,Math.ceil(filtered.value.length/20)));});
onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});onBeforeUnmount(()=>{disposed=true;read?.abort();stop();window.removeEventListener('beforeunload',unload);});
</script>
<template>
 <section class="v2-permission-review" data-v2-inline-feedback>
  <div class="v2-review-filters"><NInput v-model:value="search" clearable placeholder="搜索申请人、楼栋或原因" :input-props="{'aria-label':'搜索权限申请'}"><template #prefix><Search :size="16" /></template></NInput><NSelect v-model:value="status" :options="choices" :disabled="busy" aria-label="申请状态"/><NButton :disabled="busy" :loading="loading" @click="load"><RefreshCw :size="16"/>刷新结果</NButton></div>
  <NAlert v-if="error&&!review.length" type="error" role="alert">{{error}}</NAlert><NAlert v-if="notice" type="info" role="status">{{notice}}</NAlert>
  <div v-if="selected.length" class="v2-review-selection"><span>已选 {{selected.length}} 条</span><NButton :disabled="busy||uncertain" @click="open(rows.filter(row=>selected.includes(row.request_id)),'approve')">审批所选</NButton><NButton :disabled="busy||uncertain" @click="open(rows.filter(row=>selected.includes(row.request_id)),'reject')">拒绝所选</NButton><NButton text :disabled="busy" @click="selected=[]">取消选择</NButton></div>
  <NSpin :show="loading"><NEmpty v-if="!visible.length&&!loading" :description="error?'申请读取失败':'暂无符合条件的申请'"/>
   <div v-else class="table-wrap"><table><thead><tr><th>选择</th><th>申请人</th><th>申请权限</th><th>状态</th><th>提交时间</th><th>操作</th></tr></thead><tbody><tr v-for="row in visible" :key="row.request_id">
    <td><NCheckbox :checked="selected.includes(row.request_id)" :disabled="busy||row.status!=='pending'" :aria-disabled="busy||row.status!=='pending'" :aria-label="'选择'+row.name+'的申请'" @update:checked="checked=>choose(row,checked)"/></td>
    <td><strong>{{row.name}}</strong><small>{{row.reason||'未填写原因'}}</small></td>
    <td><template v-if="row.requested_access"><NTag size="small">{{row.requested_access.level}}</NTag> {{names([row.requested_access.category],categories)}}<small>{{names(row.requested_access.modules||[],modules)}}</small></template><span v-else>增加楼栋访问权限</span><small>{{names(row.requested_scopes||[],scopes)}}</small></td>
    <td>{{statuses[row.status]||row.status}}<small v-if="row.reject_reason">{{row.reject_reason}}</small></td><td>{{row.created_at}}</td>
    <td><NButton v-if="row.status==='pending'" :disabled="busy||uncertain" secondary size="small" @click="open([row],'approve')">核对并审批</NButton><NButton v-if="row.status==='pending'" :disabled="busy||uncertain" text size="small" @click="open([row],'reject')">拒绝</NButton></td>
   </tr></tbody></table></div>
  </NSpin>
  <footer><span>{{filtered.length}} 条申请<span v-if="rows.length>=200">（最近200条）</span></span><NPagination v-model:page="page" :item-count="filtered.length" :page-size="20" :page-slot="5"/></footer>
  <NModal :show="!!review.length" preset="card" class="v2-review-modal" :title="action==='approve'?'核对申请权限':'拒绝权限申请'" :mask-closable="!busy" :close-on-esc="!busy" :closable="!busy" @update:show="value=>{if(!value)close()}">
   <NAlert v-if="error" type="error" role="alert" data-v2-inline-feedback>{{error}}<NButton v-if="uncertain" text :disabled="busy" @click="close();load()">刷新申请结果</NButton></NAlert>
   <section v-for="row in review" :key="row.request_id" class="v2-review-detail"><h3>{{row.name}}</h3><p>{{row.reason||'未填写原因'}}</p>
    <template v-if="row.requested_access"><dl><dt>目标级别 / 岗位</dt><dd>{{row.requested_access.level}} · {{names([row.requested_access.category],categories)}}</dd><dt>完整楼栋权限</dt><dd>{{names(row.requested_access.scopes,scopes)}}</dd><dt>完整模块权限</dt><dd>{{names(row.requested_access.modules,modules)}}</dd></dl><NAlert type="warning" :bordered="false" data-v2-inline-feedback>批准后以上述完整权限替换原权限，未选择的模块不授权。</NAlert></template>
    <NCheckboxGroup v-else v-model:value="row.review_scopes" :disabled="busy||action==='reject'"><NCheckbox v-for="scope in scopes.filter(item=>row.requested_scopes.includes(item.value))" :key="scope.value" :value="scope.value">{{scope.label}}</NCheckbox></NCheckboxGroup>
   </section>
   <label v-if="action==='reject'" class="v2-review-reason">拒绝原因<NInput v-model:value="reason" type="textarea" :disabled="busy" :maxlength="500" :autosize="{minRows:2,maxRows:4}" :input-props="{'aria-label':'拒绝原因'}"/></label>
   <template #footer><div class="v2-review-buttons"><NButton :disabled="busy" @click="close">取消</NButton><NButton :type="action==='approve'?'primary':'error'" :disabled="uncertain||busy" :loading="busy" data-v2-submit @click="submit"><Check v-if="action==='approve'" :size="16"/><X v-else :size="16"/>{{action==='approve'?'确认批准':'确认拒绝'}}</NButton></div></template>
  </NModal>
 </section>
</template>
<style>
.v2-permission-review{display:grid;gap:14px}.v2-review-filters{display:grid;grid-template-columns:minmax(160px,1fr) 160px auto;gap:10px}.v2-review-selection,.v2-review-buttons,.v2-permission-review>footer{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.v2-permission-review>footer{justify-content:space-between}.v2-review-modal{width:min(760px,calc(100vw - 24px));max-height:90dvh}.v2-review-modal>.n-card-content{overflow:auto;min-height:0}.v2-review-modal>.n-card__footer{flex-shrink:0}.v2-review-detail{padding:12px 0;border-bottom:1px solid var(--v2-border)}.v2-review-detail h3{margin:0 0 8px;font-size:16px}.v2-review-detail p{font-size:13px;white-space:pre-wrap;overflow-wrap:anywhere}.v2-review-detail dl{display:grid;grid-template-columns:130px minmax(0,1fr);gap:10px;font-size:13px}.v2-review-detail dd{margin:0;line-height:1.7;overflow-wrap:anywhere}.v2-review-detail .n-checkbox-group{display:flex;flex-wrap:wrap;gap:10px}.v2-review-buttons{justify-content:flex-end}.v2-review-reason{display:grid;gap:8px;margin-top:16px}.v2-permission-review td{max-width:320px;overflow-wrap:anywhere}.v2-permission-review .n-button__content{gap:6px}@media(max-width:760px){.v2-review-filters{grid-template-columns:minmax(0,1fr) auto}.v2-review-filters>.n-input{grid-column:1/-1}.v2-review-detail dl{grid-template-columns:1fr;gap:4px}.v2-review-detail dd{margin-bottom:8px}}
.v2-permission-review{min-width:0;width:100%;max-width:100%}.v2-permission-review>.table-wrap{min-width:0;max-width:100%;overflow:auto}.v2-review-filters>*{min-width:0}
.v2-permission-review table{width:100%;border-collapse:collapse;font-size:13px;text-align:left;line-height:1.6}
.v2-permission-review th,.v2-permission-review td{padding:10px 12px;border-bottom:1px solid var(--v2-border)}
.v2-permission-review th{font-weight:600;color:var(--v2-muted);white-space:nowrap}
.v2-permission-review td small{display:block;margin-top:3px;color:var(--v2-muted);font-size:12px}
.v2-permission-review td:last-child>.n-button+.n-button{margin-left:12px}
@media(max-width:760px){
 .workspace-v2 .accounts .v2-permission-review table{min-width:0}
 .v2-permission-review .table-wrap table,.v2-permission-review tbody{display:block;width:100%}
 .v2-permission-review thead{display:none}
 .v2-permission-review tr{display:grid;grid-template-columns:28px minmax(0,1fr);gap:4px 6px;border:1px solid var(--v2-border);border-radius:8px;padding:12px;margin-bottom:10px;background:var(--v2-surface,#fff)}
 .v2-permission-review td{grid-column:2;border:0;padding:2px 0;max-width:none;text-align:left;white-space:normal}
 .v2-permission-review td:first-child{grid-column:1;grid-row:1}
 .v2-permission-review td:nth-child(2){grid-row:1}
 .v2-permission-review td:last-child{display:flex;gap:16px;padding-top:8px}
 .v2-permission-review td:nth-child(5){font-size:12px;color:var(--v2-muted)}
 .v2-permission-review .n-pagination{max-width:100%;flex-wrap:wrap}
}
</style>
