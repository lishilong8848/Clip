<template>
  <section v-if="info" class="impersonation-banner" role="region" aria-label="管理员临时代登录" data-v2-inline-feedback>
    <ShieldAlert :size="20" aria-hidden="true" />
    <div class="impersonation-copy">
      <strong>正在以{{ info.target_name }}身份查看 · 实际操作人{{ info.operator_name }}</strong>
      <span v-if="!expired">临时代办 · 剩余 <time aria-label="代登录剩余时间">{{ remainingText }}</time></span>
      <span v-else role="status">代登录已失效，仅可退出{{ info.reason ? '：'+info.reason : '' }}</span>
      <small>不能代签、代审批、代答题或读取私人聊天；需本人操作的事项请交由本人完成。</small>
      <p v-if="error" role="alert">{{ error }}</p>
    </div>
    <button type="button" data-v2-navigation :disabled="busy" :aria-busy="busy" @click="stop">
      <LogOut :size="16" aria-hidden="true" />{{ busy ? '正在退出…' : error ? '重试退出代登录' : '退出代登录' }}
    </button>
  </section>
</template>

<script lang="ts">
import { navigate } from '../navigation';
import { invalidateReadCache, setReadCacheIdentity, type Dict } from '../api/client';
export type Impersonation = {id:string;operator_name:string;target_name:string;expires_at:number;active:boolean;reason?:string};
export function impersonationIdentity(user:Dict):string {
  const value=user.impersonation;
  return value?JSON.stringify([user.open_id,value.id,value.expires_at,value.active]):'';
}
export async function finishImpersonation(data:Dict,exit=false,beforeNavigate?:()=>void|Promise<void>):Promise<void> {
  const expected=exit?'/?admin=accounts':'/';
  if(typeof data.redirect_url!=='string')throw new Error('操作回执不完整，请重新核验当前身份。');
  const returned=new URL(data.redirect_url,window.location.origin);
  if(returned.origin!==window.location.origin||returned.pathname+returned.search+returned.hash!==expected)throw new Error('返回地址无效，请重新核验当前身份。');
  await beforeNavigate?.();
  // Keep this document's grant until unload so late writes cannot become admin writes.
  invalidateReadCache();setReadCacheIdentity('');
  const workspace=document.documentElement.classList.contains('workspace-v2');
  const url=new URL(exit?'/admin/settings?tab=permissions':'/',window.location.origin);
  if(!workspace)url.searchParams.set('frontend','legacy');
  // Bypass editor guards only after the server has confirmed the identity change.
  navigate(url,{hard:true,bypassGuard:true});
}
</script>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { LogOut, ShieldAlert } from 'lucide-vue-next';
import { requestJson } from '../api/client';
const props=defineProps<{info?:Impersonation|null}>();
const emit=defineEmits<{expired:[value:boolean];leaving:[]}>();
const now=ref(Date.now()),busy=ref(false),error=ref('');
const deadline=computed(()=>{const value=Number(props.info?.expires_at);return Number.isFinite(value)?value*(value<1e12?1000:1):0;});
const remaining=computed(()=>Math.max(0,Math.ceil((deadline.value-now.value)/1000)));
const expired=computed(()=>!!props.info&&(props.info.active!==true||remaining.value===0));
const remainingText=computed(()=>String(Math.floor(remaining.value/60)).padStart(2,'0')+':'+String(remaining.value%60).padStart(2,'0'));
const abort=new AbortController();let timer=0,attempted=false;
async function stop(){
  if(busy.value||!props.info)return;
  busy.value=true;error.value='';
  try{const data=await requestJson('/api/auth/impersonation/stop',{method:'POST',body:'{}',signal:abort.signal});if(!abort.signal.aborted)await finishImpersonation(data,true,async()=>{emit('leaving');await nextTick();});}
  catch(e:any){if(!abort.signal.aborted)error.value='退出未完成或结果尚未确认，请重试退出。'+(e.message?' '+e.message:'');}
  finally{if(!abort.signal.aborted)busy.value=false;}
}
function tick(){now.value=Date.now();}
watch(expired,value=>{
  emit('expired',value);
  if(value&&props.info?.active===true&&!attempted){attempted=true;void stop();}
},{immediate:true});
onMounted(()=>{timer=window.setInterval(tick,1000);document.addEventListener('visibilitychange',tick);window.addEventListener('focus',tick);});
onBeforeUnmount(()=>{abort.abort();window.clearInterval(timer);document.removeEventListener('visibilitychange',tick);window.removeEventListener('focus',tick);});
</script>

<style scoped>
.impersonation-banner{display:flex;align-items:center;gap:10px;flex:none;min-width:0;box-sizing:border-box;padding:10px 14px;border:1px solid var(--v2-border,#d5dfeb);border-left:4px solid var(--warning-color,#a86405);border-radius:6px;background:var(--cf-surface-soft,#fff7e7);color:var(--v2-ink,#3f4652);font-size:13px;line-height:1.5}
.impersonation-banner>svg{flex:none;color:var(--warning-color,#a86405)}
.impersonation-copy{display:flex;align-items:center;flex-wrap:wrap;gap:4px 14px;flex:1;min-width:0;overflow-wrap:anywhere}.impersonation-copy strong{font-weight:600}.impersonation-copy time{font-variant-numeric:tabular-nums}.impersonation-copy small{flex-basis:100%;font-size:12px}.impersonation-copy p{flex-basis:100%;margin:0;color:var(--error-color,#a52836)}
button{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:44px;padding:7px 12px;border:1px solid currentColor;border-radius:6px;background:transparent;color:inherit;font:inherit;cursor:pointer;flex:none}button:hover:not(:disabled){background:color-mix(in srgb,currentColor 7%,transparent)}button:disabled{opacity:.6;cursor:wait}button:focus-visible{outline:2px solid var(--v2-blue,#2364d2);outline-offset:3px}
@media(max-width:600px){.impersonation-banner{flex-wrap:wrap;gap:8px;padding:9px 10px}.impersonation-copy{flex-basis:calc(100% - 30px)}button{margin-left:30px}.impersonation-copy>span{flex-basis:100%}}
</style>
