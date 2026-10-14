<template>
  <section v-if="eligible" class="v2-notice-memory" aria-label="跨楼历史推荐">
    <header><span><Sparkles :size="15" />跨楼历史推荐</span><NButton v-if="error" size="tiny" text data-v2-navigation :disabled="locked||loading" @click="load">重新读取</NButton></header>
    <p v-if="loading" role="status"><NSpin size="small" />正在查找相同计划的成功历史，不影响继续填写。</p>
    <p v-else-if="error" role="status">{{ error }}</p>
    <p v-else-if="contextChanged">名称或楼栋已修改，原计划的推荐已停用。</p>
    <template v-else>
      <p v-if="notice">{{ notice }}</p>
      <NCollapse v-if="available.length" :default-expanded-names="['0']">
        <NCollapseItem v-for="(candidate,index) in available" :key="candidate.source_building+candidate.source_title+index" :name="String(index)">
          <template #header><span>{{ candidate.source_building }}楼 · {{ candidate.source_title }}<small>{{ candidate.source_updated_at }}</small></span></template>
          <dl><template v-for="field in fields(candidate)" :key="field.key"><dt>{{ field.label }}<NTag v-if="field.required" size="small" :bordered="false" type="warning">缺失</NTag></dt><dd><span>{{ candidate.suggested_values[field.key] }}<small v-if="candidate.needs_site_confirmation?.includes(field.key)">请核对本楼现场情况</small></span><NButton size="small" data-v2-navigation :disabled="locked" @click="apply(candidate,field.key)">采用</NButton></dd></template></dl>
          <NButton size="small" secondary data-v2-navigation :disabled="locked" @click="apply(candidate)">采用这组空白项</NButton>
        </NCollapseItem>
      </NCollapse>
      <p v-else>{{ applied ? '推荐已填入，发送前请核对。' : '暂无可用于空白项的同类成功历史，请按现场情况填写。' }}</p>
    </template>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,ref,watch } from 'vue';
import { NButton,NCollapse,NCollapseItem,NSpin,NTag,useDialog,useMessage } from 'naive-ui';
import { Sparkles } from 'lucide-vue-next';
import { requestJson,type Dict } from './client';
import { recommendationFields } from './noticeExperience';
import type { NoticeForm } from './noticeForm';
const props=defineProps<{form:NoticeForm;month:string;action:string;locked:boolean}>();
const emit=defineEmits<{applied:[]}>(),dialog=useDialog(),message=useMessage();
const loading=ref(false),error=ref(''),notice=ref(''),items=ref<Dict[]>([]),applied=ref(false),touched=ref(new Set<string>()),origin=ref('');
let controller:AbortController|undefined;
const context=()=>JSON.stringify([props.form.values.source_record_id,props.form.values.title,props.form.values.building_codes,props.form.values.work_type]);
const eligible=computed(()=>props.action==='start'&&!!props.form.values.source_record_id&&!props.form.values.target_record_id&&props.form.values.building_codes?.length===1);
const contextChanged=computed(()=>!!origin.value&&origin.value!==context());
const fields=(candidate:Dict)=>recommendationFields(props.form,candidate,touched.value);
const available=computed(()=>contextChanged.value?[]:items.value.filter(c=>fields(c).length));
watch(()=>props.form.fields.map(f=>String(props.form.values[f.key]??'')),(values,before)=>{if(before)props.form.fields.forEach((f,i)=>{if(values[i]!==before[i])touched.value.add(f.key);});},{flush:'sync'});
async function load(){
  controller?.abort();items.value=[];error.value='';notice.value='';if(!eligible.value)return;
  const c=new AbortController();controller=c;const current=props.form,identity=context();origin.value=identity;loading.value=true;
  try{
    const v=current.values,result=await requestJson('/api/workbench/planned-notice-prefill?'+new URLSearchParams({scope:v.building_codes[0],month:props.month,work_type:v.work_type,source_record_id:v.source_record_id}),{signal:c.signal,cache:'no-store'});
    if(c.signal.aborted||props.form!==current||identity!==context())return;
    if(!Array.isArray(result.recommendations)||result.selected?.source_record_id!==v.source_record_id)throw new Error('推荐资料尚未完整读取');
    if(String(result.draft?.title||'').trim()!==String(v.title||'').trim())throw new Error('当前名称与原计划不一致，请先核对所选计划');
    items.value=result.recommendations.filter((item:Dict)=>item&&item.work_type===v.work_type&&[...'ABCDEH'].includes(item.source_building)&&item.source_building!==v.building_codes[0]&&item.suggested_values&&typeof item.suggested_values==='object');notice.value=result.recommendations_notice||'';
  }catch(e){if(!c.signal.aborted)error.value='历史推荐暂不可用：'+(e as Error).message+'。可继续手动填写。';}
  finally{if(controller===c)loading.value=false;}
}
function apply(candidate:Dict,key?:string){
  if(props.locked||contextChanged.value)return;
  const current=props.form,identity=context(),selected=fields(candidate).filter(f=>!key||f.key===key);
  const commit=()=>{
    if(props.form!==current||identity!==context()||props.locked)return;
    const stillEligible=new Set(fields(candidate).map(f=>f.key));
    for(const field of selected)if(stillEligible.has(field.key))current.values[field.key]=candidate.suggested_values[field.key];
    applied.value=true;emit('applied');message.success('已填入空白项，请核对后发送');
  };
  if(selected.some(f=>candidate.needs_site_confirmation?.includes(f.key)))dialog.warning({title:'核对本楼现场情况',content:'来源为'+candidate.source_building+'楼。位置、执行方等可能与本楼不同，请确认适用后采用；已填写内容不会覆盖。',positiveText:'适用并采用',negativeText:'返回核对',onPositiveClick:commit});else commit();
}
watch(()=>[props.form.values.source_record_id,props.form.values.target_record_id,props.action,props.month],()=>void load(),{immediate:true});
onBeforeUnmount(()=>controller?.abort());
</script>
<style>
.v2-notice-memory{border-left:3px solid var(--v2-blue);padding:10px 14px;background:#f3f7fc;margin:0 0 16px;font-size:13px}.v2-notice-memory>header,.v2-notice-memory>header>span{display:flex;align-items:center;gap:7px}.v2-notice-memory>header{justify-content:space-between;font-weight:600}.v2-notice-memory p{display:flex;align-items:center;gap:8px;color:var(--v2-muted);margin:8px 0}.v2-notice-memory dl{margin:0 0 10px}.v2-notice-memory dt{display:flex;align-items:center;gap:8px;color:var(--v2-muted);margin-top:10px}.v2-notice-memory dd{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin:5px 0;white-space:pre-wrap;overflow-wrap:anywhere}.v2-notice-memory dd>span{min-width:0}.v2-notice-memory small{display:block;font-size:12px;color:var(--v2-muted);margin-top:4px}.v2-notice-memory .n-collapse-item__content-inner{padding-top:4px!important}
</style>
