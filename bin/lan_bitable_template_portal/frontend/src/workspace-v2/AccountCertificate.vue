<template>
  <section class="account-certificate" aria-label="HTTPS证书信息">
    <header><h2><ShieldCheck :size="18" />HTTPS 证书<NTag v-if="info" size="small" :type="!error&&info.state==='healthy'?'success':'warning'" :bordered="false">{{ error?'待核验':statusLabel }}</NTag></h2><NButton size="small" secondary :loading="loading" :disabled="loading" aria-label="刷新证书信息" @click="load"><RefreshCw :size="14" />刷新</NButton></header>
    <p v-if="loading&&!info" role="status">正在读取证书信息…</p>
    <NAlert v-if="error||info&&info.state!=='healthy'" type="warning" :bordered="false" role="status">{{ error||warning }}</NAlert>
    <dl v-if="info?.domain">
      <div><dt>域名</dt><dd>{{ info.domain }}</dd></div>
      <div><dt>签发机构</dt><dd>{{ info.issuer }}</dd></div>
      <div><dt>到期时间（北京时间）</dt><dd>{{ formatTime(info.not_after) }}</dd></div>
      <div><dt>剩余有效期</dt><dd>{{ info.state==='expired'?'已过期':info.remaining_days+' 天' }}</dd></div>
      <div><dt>自动续期</dt><dd>{{ info.renewal?.enabled?'已启用 · 免费':'未确认启用' }}</dd></div>
      <div><dt>最近检查</dt><dd>{{ formatTime(info.checked_at) }} · {{ info.renewal?.last_check_ok?'成功':'异常' }}</dd></div>
      <div><dt>下次检查（服务器时间）</dt><dd>{{ info.renewal?.next_check||'等待下次调度' }}</dd></div>
      <div><dt>预计续期时间（北京时间）</dt><dd>{{ info.renewal?.next_renewal_at?formatTime(info.renewal.next_renewal_at):'由签发机构自动安排' }}</dd></div>
    </dl>
    <p class="certificate-note">服务器每日检查，按需续期并校验、平滑加载新证书；无需重启灯塔或FRP。</p>
  </section>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { NAlert, NButton, NTag } from 'naive-ui';
import { RefreshCw, ShieldCheck } from 'lucide-vue-next';
import { requestJson, type Dict } from './client';

const info=ref<Dict|null>(null),loading=ref(false),error=ref('');
let active:AbortController|undefined;
const statusLabel=computed(()=>({healthy:'正常',expired:'已过期',stale:'待核验',preparing:'检查中',unavailable:'暂不可用',warning:'需关注'}[String(info.value?.state)]||'待核验'));
const warning=computed(()=>info.value?.state==='expired'?'证书已过期，请检查服务器续期任务。':info.value?.state==='stale'?'证书信息未及时更新，暂不能确认最新续期状态。':info.value?.state==='preparing'?'证书信息正在读取，请稍后刷新。':info.value?.state==='unavailable'?'暂未取得证书信息，局域网业务不受影响，请稍后重试。':'证书临近到期或续期检查异常，请核对服务器续期任务。');
function formatTime(value:number){return new Date(value*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false});}
async function load(){
  if(loading.value)return;
  loading.value=true;error.value='';const read=new AbortController();active=read;
  try{const result=await requestJson('/api/auth/certificate',{signal:read.signal,cache:'no-store'});if(!read.signal.aborted)info.value=result;}
  catch(e){if(!read.signal.aborted){if([401,403].includes(Number((e as Dict)?.status)))info.value=null;error.value=e instanceof Error?e.message:'证书信息暂不可用，请稍后重试。';}}
  finally{if(!read.signal.aborted)loading.value=false;}
}
onMounted(load);onBeforeUnmount(()=>active?.abort());
</script>
<style scoped>
.account-certificate{border-top:1px solid var(--v2-border);margin-top:24px;padding-top:20px;min-width:0}
header,h2{display:flex;align-items:center;gap:8px}header{justify-content:space-between;margin-bottom:14px}h2{font-size:15px;margin:0;flex-wrap:wrap}h2>svg{color:var(--v2-muted);flex:none}header :deep(.n-button__content){gap:6px}
dl{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:18px 20px;margin:16px 0}dt,.certificate-note{color:var(--v2-muted);font-size:12px;line-height:1.6}dd{margin:5px 0 0;font-size:14px;overflow-wrap:anywhere;line-height:1.5}.certificate-note{margin:12px 0 0}
@media(max-width:1000px){dl{grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}}
@media(max-width:480px){header :deep(.n-button){min-height:44px}dl{gap:14px 10px}dd{font-size:13px}h2 :deep(.n-tag){font-size:12px}}
</style>
