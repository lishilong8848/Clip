<template>
  <section class="event-board" aria-label="事件通告">
    <header class="event-board-tools"><strong>事件通告</strong><span>I3 发飞书 I3 群，其它级别不发飞书</span><button type="button" :disabled="!options.user_id" @click="openDefaults">默认钉钉机器人 · {{ defaultNames }}</button><button type="button" :disabled="loading" @click="load(true)">刷新</button></header>
    <div v-if="defaultsOpen" class="event-defaults" role="dialog" aria-label="默认钉钉机器人">
      <strong>我的默认钉钉机器人</strong><p>新增、更新、结束自动使用默认选择，可选多个并行发送；不改变已提交任务的重试对象。</p>
      <div class="default-choices"><label v-for="robot in options.robots||[]" :key="robot.id"><input v-model="selected" type="checkbox" :value="robot.id" />{{ robot.name }}<small v-if="robot.remark"> · {{ robot.remark }}</small></label></div>
      <p v-if="!options.robots?.length">暂无可用机器人，请由管理员在“系统设置 → 钉钉推送”中配置。</p>
      <p v-if="defaultsError" role="alert">{{ defaultsError }}</p><footer><button :disabled="defaultsBusy" @click="defaultsOpen=false">取消</button><button :disabled="defaultsBusy" @click="saveDefaults">保存默认选择</button></footer>
    </div>
    <p v-if="error" role="alert">{{ error }} <button @click="initialize">重试读取</button></p>
    <div v-if="!options.user_id" class="event-defaults"><h2>新建事件通告</h2><label>本次通告原文<textarea v-model="pendingText" rows="9" maxlength="15000" placeholder="粘贴完整的事件通告"></textarea></label><p>{{error?'暂未取得操作权限，请重试读取。':'正在读取权限和默认机器人，完成后即可发送。'}}</p></div>
    <template v-if="options.user_id">
      <h2>新建与进行中事件 <small>{{ ongoing.length }} 条</small></h2><p v-if="warning" class="event-warning">{{ warning }}</p>
      <div class="event-grid"><EventNoticeRawCard :key="context+':new'" creation :initial-text="pendingText" :options="options" :scope="scope" @changed="changed" @accepted="pendingText=''" /><EventNoticeRawCard v-for="row in ongoing" :key="context+':'+row.event_id" :row="row" :scope="scope" :options="options" @changed="changed" /></div>
      <p v-if="!ongoing.length&&!loading">暂无进行中事件</p>
      <header class="event-board-tools"><h2>历史事件</h2><label>月份 <input v-model="month" type="month" /></label></header>
      <details v-for="row in history" :key="row.event_id" class="event-history"><summary><strong>{{ historyTitle(row) }}</strong><span>{{ historyDate(row) }}</span><b v-if="row.missing?.length" class="missing">待补充资料 · {{ row.missing.length }} 项</b><span v-else>资料已补齐</span></summary><EventNoticeRawCard :row="row" :scope="scope" :options="options" @changed="changed" /></details>
      <p v-if="!history.length&&!loading">所选月份暂无已结束事件</p>
    </template>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {requestJson,type Dict} from '../workspace-v2/client';
import EventNoticeRawCard from './EventNoticeRawCard.vue';
const props=defineProps<{scope:string;active?:boolean}>();
const options=ref<Dict>({}),rows=ref<Dict[]>([]),error=ref(''),warning=ref(''),loading=ref(false),month=ref(new Date(Date.now()+8*3600000).toISOString().slice(0,7));
const pendingText=ref('');
const defaultsOpen=ref(false),defaultsBusy=ref(false),defaultsError=ref(''),selected=ref<string[]>([]);
let generation=0,timer=0,disposed=false;
const context=computed(()=>props.scope+':'+options.value.user_id);
const ongoing=computed(()=>rows.value.filter(row=>row.state!=='ended'));
const history=computed(()=>rows.value.filter(row=>row.state==='ended'&&historyDate(row).startsWith(month.value)));
const defaultNames=computed(()=> (options.value.defaults?.robot_ids||[]).map((id:string)=>options.value.robots?.find((r:Dict)=>r.id===id)?.name||'需核对的机器人').join('、')||'未设置');
const historyTitle=(row:Dict)=>row.metadata?.summary||String(row.text||'事件通告').split('\n').slice(0,2).join(' ').slice(0,160);
function historyDate(row:Dict){return String(row.metadata?.occurrence_time||new Date(Number(row.created_at||0)*1000+8*3600000).toISOString().slice(0,16).replace('T',' ')).replace(/\//g,'-');}
function schedule(){clearTimeout(timer);if(!disposed&&props.active!==false)timer=window.setTimeout(()=>{if(!document.hidden)void load(false,true);else schedule();},5000);}
async function initialize(){const seq=++generation;defaultsOpen.value=false;options.value={};rows.value=[];error.value='';try{const data=await requestJson('/api/workbench-events/options?work_type=event&scope='+encodeURIComponent(props.scope),{cache:'no-store'});if(seq!==generation||disposed)return;options.value=data;await load(false,true);if(seq===generation&&!disposed)void load();}catch(e){if(seq===generation)error.value=(e as Error).message;}}
async function load(fresh=false,local=false){if(!options.value.user_id||loading.value||disposed)return;const seq=generation;loading.value=true;try{const data=await requestJson('/api/workbench-events?work_type=event&scope='+encodeURIComponent(props.scope)+(fresh?'&refresh=1':'')+(local?'&local=1':''),{cache:'no-store'});if(seq!==generation||disposed)return;rows.value=data.items||[];warning.value=data.warning||'';if(!defaultsOpen.value)options.value={...options.value,...data};error.value='';}catch(e){if(seq===generation)error.value=(e as Error).message;}finally{if(seq===generation){loading.value=false;schedule();}}}
function changed(){window.dispatchEvent(new Event('clipflow-business-changed'));void load(false,true);}
function openDefaults(){selected.value=[...(options.value.defaults?.robot_ids||[])];defaultsError.value='';defaultsOpen.value=true;}
async function saveDefaults(){if(defaultsBusy.value)return;defaultsBusy.value=true;try{options.value.defaults=await requestJson('/api/workbench-events/defaults?work_type=event',{method:'POST',body:JSON.stringify({scope:props.scope,robot_ids:selected.value,revision:options.value.defaults?.revision||0})});defaultsOpen.value=false;}catch(e){defaultsError.value=(e as Error).message;}finally{defaultsBusy.value=false;}}
watch(()=>props.scope,()=>{loading.value=false;void initialize();},{immediate:true});
watch(()=>props.active,value=>{if(value)void load(false,true);else clearTimeout(timer);});
function visibility(){if(!document.hidden&&props.active!==false)void load(false,true);}
onMounted(()=>document.addEventListener('visibilitychange',visibility));
onBeforeUnmount(()=>{disposed=true;generation++;clearTimeout(timer);document.removeEventListener('visibilitychange',visibility);});
</script>
<style scoped>
.event-board{--cf-surface:var(--v2-bg,#fff);--cf-text:var(--v2-ink,#213547);--cf-border:var(--v2-border,#dbe4ef);--cf-primary:var(--v2-blue,#1762cf);display:grid;gap:12px;min-width:0}.event-board-tools{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.event-board-tools>span{flex:1;font-size:12px;opacity:.75}.event-board button,.event-board input[type=month]{min-height:36px;border:1px solid var(--cf-border);background:var(--cf-surface);color:inherit;border-radius:6px;padding:6px 10px;font:inherit;cursor:pointer}.event-board h2{font-size:17px;margin:8px 0 0}.event-board h2 small{font-weight:400}.event-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,720px),1fr));gap:14px;align-items:start}.event-history{border:1px solid var(--cf-border);border-radius:7px;background:var(--cf-surface)}.event-history summary{cursor:pointer;line-height:1.6;overflow-wrap:anywhere;padding:10px 12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.event-history summary strong{flex:1;min-width:180px}.event-history summary span{font-size:12px;opacity:.7}.event-history .missing{font-size:12px;color:#c22b38;font-weight:500}.event-history .raw-event-card{border:0;border-top:1px solid var(--cf-border)}.event-defaults{padding:14px;border:1px solid var(--cf-border);background:var(--cf-surface);border-radius:8px}.event-defaults textarea{display:block;width:100%;box-sizing:border-box;min-height:180px;font:inherit;padding:8px;background:var(--cf-surface);color:inherit;border:1px solid var(--cf-border)}.event-defaults p,.event-warning{font-size:13px;line-height:1.6}.default-choices{display:flex;gap:10px 18px;flex-wrap:wrap}.default-choices label{display:flex;gap:6px;align-items:center;min-height:36px}.default-choices input{width:18px;height:18px}.default-choices small{opacity:.7}.event-defaults footer{display:flex;justify-content:flex-end;gap:10px;margin-top:12px}@media(max-width:600px){.event-board-tools input{font-size:16px}.event-board-tools>span{flex-basis:100%}}
</style>
