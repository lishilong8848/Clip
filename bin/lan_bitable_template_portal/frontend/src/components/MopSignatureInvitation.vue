<template>
  <form class="mop-signature-invitation" :aria-label="resume ? '继续维护单签名请求' : '添加维护单签名人员'" @submit.prevent="submit">
    <div class="invitation-fields">
      <label><span>签名人员姓名 *</span><input v-model="draft.name" required maxlength="80" :disabled="started" autocomplete="off" /></label>
      <label><span>所属楼栋 *</span><NSelect v-model:value="draft.scope" :options="scopes" :disabled="started || loading" aria-label="签名人员楼栋" /></label>
      <label><span>工号</span><input v-model="draft.employee_no" maxlength="80" :disabled="started" /></label>
      <label><span>专业</span><input v-model="draft.specialty" maxlength="80" :disabled="started" /></label>
      <label class="invitation-recipient"><span>签名链接接收人 *</span><NSelect v-model:value="recipient" :options="recipients" filterable clearable :loading="loading" :disabled="busy || checking" placeholder="搜索姓名、工号或选择值班账号" aria-label="签名链接接收人" /></label>
    </div>
    <p v-if="error" class="invitation-error" role="alert">{{ error }}</p>
    <p v-else class="invitation-hint">链接交由上述人员本人签名；保存后自动更新到本维护单。</p>
    <footer data-v2-navigation>
      <NButton size="small" :disabled="busy" @click="$emit('close')">取消</NButton>
      <NButton v-if="loadError" size="small" :loading="loading" @click="load">重新读取接收人</NButton>
      <NButton size="small" type="primary" attr-type="submit" :loading="busy" :disabled="loading || busy || loadError || !checking && (!draft.name.trim() || !draft.scope || !recipient)">{{ checking ? '核对发送结果' : requestStatus==='send_failed' ? '重试发送链接' : retryable ? '重新发送链接' : started ? '重试原请求' : '添加并发送签名链接' }}</NButton>
    </footer>
  </form>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import { NButton, NSelect } from 'naive-ui';
import { requestJson, type Dict } from '../api/client';
import { randomHexId } from '../browserStorage';
const props=defineProps<{scope:string;noticeKey:string;role:'implementer'|'auditor';resume?:Dict|null}>();
const emit=defineEmits<{close:[];busy:[value:boolean];changed:[value:{scope:string;noticeKey:string;person?:Dict}]}>();
const draft=reactive({name:String(props.resume?.name||''),employee_no:String(props.resume?.employee_no||''),specialty:String(props.resume?.specialty||''),scope:String(props.resume?.scope||'')}),recipient=ref<string|null>(props.resume?.recipient||null);
const scopes=ref<{value:string;label:string}[]>([]),recipients=ref<{value:string;label:string}[]>([]);
const busy=ref(false),loading=ref(false),started=ref(Boolean(props.resume)),error=ref(String(props.resume?.signature_reason||'')),loadError=ref(false),requestStatus=ref(String(props.resume?.request_status||''));
const retryable=computed(()=>['send_failed','expired','revoked'].includes(requestStatus.value));
const checking=computed(()=>started.value&&Boolean(requestStatus.value)&&!retryable.value);
const operationId=String(props.resume?.operation_id||randomHexId());
let sendId=String(props.resume?.send_operation_id||randomHexId()),expectedRequest=String(props.resume?.request_id||''),disposed=false,read:AbortController|undefined;
async function load(){
  read?.abort();const controller=new AbortController();read=controller;loading.value=true;loadError.value=false;error.value=String(props.resume?.signature_reason||'');
  try{const data=await requestJson('/api/signatures/management/mop-people?'+new URLSearchParams({scope:props.scope,notice_key:props.noticeKey}),{signal:controller.signal,cache:'no-store'});
    if(disposed||controller.signal.aborted)return;
    if(!Array.isArray(data.scope_options)||!Array.isArray(data.recipients))throw new Error('接收人信息不完整，请重新读取。');
    scopes.value=data.scope_options;recipients.value=data.recipients;
    if(props.resume&&!scopes.value.some(s=>s.value===draft.scope))throw new Error('原签名人员所属楼栋已不可访问，请重新读取。');
    if(!props.resume)draft.scope=scopes.value.find(s=>s.value===props.scope)?.value||scopes.value[0]?.value||'';
  }catch(e){if(!disposed&&!controller.signal.aborted){loadError.value=true;error.value=e instanceof Error?e.message:'接收人读取失败。';}}
  finally{if(read===controller)loading.value=false;}
}
function applyResult(person:Dict){
  requestStatus.value=String(person.request_status||'pending');
  sendId=String(person.send_operation_id||sendId);expectedRequest=String(person.request_id||expectedRequest);
  recipient.value=person.recipient||recipient.value;
  emit('changed',{scope:props.scope,noticeKey:props.noticeKey,person});
  if(disposed)return;
  if(person.has_signature||['sent','completed'].includes(requestStatus.value)){emit('close');return;}
  error.value=person.signature_reason||'发送结果尚未确认，请核对原请求。';
}
async function submit(){
  if(busy.value||loading.value||loadError.value||!checking.value&&(!draft.name.trim()||!draft.scope||!recipient.value))return;
  busy.value=true;emit('busy',true);error.value='';
  try{
    if(checking.value){
      const data=await requestJson('/api/signatures/management/mop-list?'+new URLSearchParams({scope:props.scope,notice_key:props.noticeKey}),{cache:'no-store'});
      if(!Array.isArray(data.items))throw new Error('签名请求信息不完整，请重新核对。');
      const person=data.items.find((item:Dict)=>item.operation_id===operationId);
      if(!person){
        if(props.resume)throw new Error('未找到原签名请求，请重新读取人员列表。');
        requestStatus.value='';error.value='尚未查到原请求，可点击“重试原请求”；将复用原操作编号，不会重复新增。';return;
      }
      applyResult(person);return;
    }
    if(retryable.value)sendId=randomHexId();
    started.value=true;requestStatus.value='sending';
    const data=await requestJson('/api/signatures/management/mop-invite',{method:'POST',body:JSON.stringify({...draft,name:draft.name.trim(),notice_key:props.noticeKey,role:props.role,recipient:recipient.value,operation_id:operationId,send_operation_id:sendId,expected_request_id:expectedRequest})});
    if(!data.person?.request_id)throw new Error('发送回执不完整，请核对原请求。');
    applyResult(data.person);
  }catch(e){requestStatus.value='send_unknown';if(!disposed)error.value=e instanceof Error?e.message:'发送结果暂未确认，请核对原请求。';emit('changed',{scope:props.scope,noticeKey:props.noticeKey});}
  finally{busy.value=false;emit('busy',false);}
}
onMounted(()=>void load());
onBeforeUnmount(()=>{disposed=true;read?.abort();});
</script>
<style scoped>
.mop-signature-invitation{padding:12px;border:1px solid var(--v2-border,#e3e8ed);border-radius:8px;background:var(--v2-surface,#fff);color:var(--v2-ink,#24312e);min-width:0}.invitation-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.invitation-fields label{display:grid;gap:5px;min-width:0;font-size:12px}.invitation-fields input{box-sizing:border-box;min-width:0;width:100%;height:36px;border:1px solid var(--v2-border,#dbe3df);border-radius:6px;padding:0 10px;background:transparent;color:inherit;font:inherit}.invitation-fields input:focus-visible{outline:2px solid var(--v2-blue,#0071e3);outline-offset:2px}.invitation-recipient{grid-column:1/-1}.invitation-error,.invitation-hint{margin:10px 0;font-size:12px;line-height:1.6;overflow-wrap:anywhere}.invitation-error{color:#ba3045}.invitation-hint{color:var(--v2-muted,#63736d)}footer{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px}@media(max-width:480px){.invitation-fields{grid-template-columns:1fr}.invitation-fields input{height:44px}footer :deep(.n-button){min-height:44px}}
</style>
