<template>
  <article class="raw-event-card">
    <header><div><h3>{{ creation?'新建事件通告':title }}</h3><small>{{ stateLabel }}<span v-if="!creation&&unfilled.length"> · 待补充 {{ unfilled.length }} 项资料</span></small></div><button v-if="!creation&&options.can_delete" class="danger" :disabled="busy" @click="deleteOpen=true">删除</button></header>
    <div v-if="deleteOpen" role="alertdialog" class="feedback"><p>确认删除这条事件？已发群消息不会撤回，后台确认删除成功后才移除卡片。</p><button :disabled="busy" @click="deleteOpen=false">保留</button><button class="danger" :disabled="busy" @click="remove">确认删除</button></div>
    <div class="raw-event-layout">
      <section class="message-side" aria-label="事件通告原文">
        <template v-if="creation||editingAction">
          <label>本次通告原文<textarea ref="textBox" v-model="text" rows="9" maxlength="15000" :disabled="busy||!!frozen" placeholder="粘贴本次完整的事件通告。按原文发送，不拆分字段，不自动改写正文或状态。" @input="persist" /></label>
          <p class="muted">I3 自动发飞书 I3 群；非 I3 不发飞书。钉钉使用右上角的默认机器人。</p>
          <footer><button v-if="!creation&&!frozen" :disabled="busy" @click="editingAction=''">收起，保留填写</button><button v-if="!frozen" class="primary" :disabled="busy||!text.trim()" @click="send">{{ busy?'正在受理…':creation?'发送新增':'发送'+(editingAction==='end'?'结束':'更新') }}</button><button v-else class="primary" :disabled="busy" @click="send">核验并重试原发送</button></footer>
        </template>
        <template v-else><pre>{{ current.text }}</pre><footer v-if="current.state!=='ended'"><button :disabled="busy" @click="edit('update')">更新</button><button class="primary" :disabled="busy" @click="edit('end')">结束</button></footer></template>
        <p v-if="error" class="feedback danger" role="alert">{{ error }}</p>
        <section v-if="current.delivery" class="delivery-status" aria-label="群消息发送结果">
          <strong>群消息：{{ deliveryLabels[current.delivery.state]||'处理中' }}</strong>
          <p v-for="(target,index) in current.delivery.targets" :key="target.channel+index"><span>{{ target.channel==='feishu'?'飞书':'钉钉' }} · {{ target.name }}</span><b>{{ deliveryLabels[target.state]||'等待发送' }}</b><small v-if="target.reason">{{ target.reason }}</small></p>
          <button v-if="current.delivery.state==='failed'" :disabled="busy" @click="retry(false)">重试未送达的群</button>
          <div v-if="retryOpen" class="feedback" role="alertdialog"><p>部分消息结果不确定，请先核对群内是否收到。确认未收到后再重发，避免重复。</p><button @click="retryOpen=false">暂不重发</button><button :disabled="busy" @click="retry(true)">已核对，确认重发</button></div>
        </section>
      </section>
      <details class="metadata-side" :open="metadataOpen" @toggle="metadataOpen=($event.target as HTMLDetailsElement).open">
        <summary>事件资料 <span :class="{danger:unfilled.length}">{{ unfilled.length?'待补充 '+unfilled.length+' 项':'已补齐' }}</span></summary>
        <p class="muted">不影响群消息发送。填写自动保存、自动同步，结束后也可继续补充。</p>
        <div class="metadata-fields">
          <label class="wide">告警描述<textarea v-model="metadata.summary" rows="3" maxlength="5000" placeholder="填写实际事件概述" @input="queueSave" /></label>
          <label>楼栋<select v-model="metadata.building" aria-label="事件资料楼栋" @change="queueSave"><option value="">待补充</option><option v-if="metadata.building&&!options.scopes.includes(metadata.building)" :value="metadata.building">{{ metadata.building }}</option><option v-for="code in options.scopes" :key="code" :value="code">{{ code==='110'?'110站':code+'楼' }}</option></select></label>
          <label>专业<select v-model="metadata.specialty" aria-label="事件资料专业" @change="queueSave"><option value="">待补充</option><option v-for="name in options.specialties||['电气','暖通','消防','弱电']" :key="name">{{ name }}</option></select></label>
          <label>等级<select v-model="metadata.level" aria-label="事件资料等级" @change="queueSave"><option value="">待补充</option><option v-for="name in options.levels" :key="name">{{ name }}</option></select></label>
          <label>来源<select v-model="metadata.event_source" aria-label="事件资料来源" @change="queueSave"><option value="">待补充</option><option v-for="name in options.sources" :key="name">{{ name }}</option></select></label>
          <label class="wide">事件发生时间<input type="datetime-local" :value="datetime(metadata.occurrence_time)" @input="setDate('occurrence_time',$event)" /></label>
          <label class="wide">事件恢复时间（选填）<input type="datetime-local" :value="datetime(metadata.recover_time)" @input="setDate('recover_time',$event)" /></label>
          <label class="wide inline"><input v-model="metadata.transfer_to_overhaul" type="checkbox" @change="queueSave" />转检修</label>
        </div>
        <p v-if="unfilled.length" class="muted">待补充：{{ unfilled.join('、') }}</p>
        <p role="status" class="metadata-status">{{ saving?'正在保存资料…':saveError||syncLabel }}</p>
        <p v-for="(message,key) in current.metadata_issues||{}" :key="key" class="danger">{{ fields[String(key)]||key }}：{{ message }}</p>
      </details>
    </div>
  </article>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,ref,watch} from 'vue';
import {ApiError,requestJson,type Dict} from '../workspace-v2/client';
const props=defineProps<{row?:Dict;creation?:boolean;scope:string;options:Dict}>();
const emit=defineEmits<{changed:[];accepted:[]}>();
const uuid=()=>crypto.randomUUID().replace(/-/g,'');
const pointer='event-compose:'+props.options.user_id+':'+props.scope;
let initial=props.row?.event_id||'';
try{if(props.creation)initial=sessionStorage.getItem(pointer)||'';}catch{}
const eventId=ref(initial||'web_'+uuid()),current=ref<Dict>(props.row||{}),text=ref(''),editingAction=ref(''),busy=ref(false),saving=ref(false),error=ref(''),saveError=ref(''),frozen=ref<Dict|null>(null),deleteOpen=ref(false),retryOpen=ref(false),metadataOpen=ref(innerWidth>=850),textBox=ref<HTMLTextAreaElement|null>(null);
const fields:Record<string,string>={summary:'告警描述',building:'楼栋',specialty:'专业',level:'事件等级',event_source:'来源',occurrence_time:'发生时间'};
const empty=()=>({summary:'',building:'',specialty:'',level:'',event_source:'',occurrence_time:'',recover_time:'',transfer_to_overhaul:false});
const base=ref<Dict>({...empty(),...(props.row?.metadata||{})}),metadata=ref<Dict>({...base.value});
const deliveryLabels:Record<string,string>={pending:'等待发送',posting:'发送中',sent:'已送达',failed:'部分或全部未送达',uncertain:'结果待核对',superseded:'已由后续通告替代'};
const title=computed(()=>current.value.metadata?.summary||String(current.value.text||'事件通告').split('\n').slice(0,2).join(' ').slice(0,140));
const stateLabel=computed(()=>props.creation?'粘贴即可发送，右侧资料可后补':current.value.state==='ended'?'已结束':current.value.state==='active'?'进行中':'等待发送');
const unfilled=computed(()=>Object.entries(fields).filter(([key])=>!metadata.value[key]).map(([,label])=>label));
const syncLabel=computed(()=>current.value.sync_error||({synced:'多维资料已同步',pending:'资料已保存，后台同步中',retry:'资料已保存，后台将重试同步',waiting:'资料可稍后补充'} as Record<string,string>)[current.value.sync_state]||'填写后自动保存');
const datetime=(value:unknown)=>String(value||'').replace(' ','T').slice(0,16);
let saveTimer=0,disposed=false;
const cacheKey=()=> 'event-draft:'+props.options.user_id+':'+eventId.value;
function persist(){try{sessionStorage.setItem(cacheKey(),JSON.stringify({metadata:metadata.value,base:base.value,text:text.value,editingAction:editingAction.value,frozen:frozen.value}));if(props.creation)sessionStorage.setItem(pointer,eventId.value);}catch{saveError.value='浏览器无法保存草稿，请保持本页打开。';}}
try{const saved=JSON.parse(sessionStorage.getItem(cacheKey())||'null');if(saved){metadata.value={...empty(),...saved.metadata};base.value={...empty(),...saved.base};text.value=saved.text||'';editingAction.value=saved.editingAction||'';frozen.value=saved.frozen||null;}}catch{}
function merge(value:Dict){const previous=base.value,next={...empty(),...(value.metadata||{})};for(const key of Object.keys(next))if(metadata.value[key]===previous[key])metadata.value[key]=next[key];base.value=next;const issues=Object.fromEntries(Object.entries(value.metadata_issues||{}).filter(([key])=>metadata.value[key]!==next[key]));current.value={...value,metadata_issues:issues};if(!Object.keys(patch()).length&&!Object.keys(issues).length)saveError.value='';persist();}
watch(()=>props.row,value=>{if(value&&!props.creation)merge(value);},{deep:true});
function patch(){return Object.fromEntries(Object.keys(empty()).filter(key=>metadata.value[key]!==base.value[key]).map(key=>[key,metadata.value[key]]));}
function queueSave(){persist();clearTimeout(saveTimer);saveTimer=window.setTimeout(saveMetadata,650);}
function setDate(key:string,event:Event){metadata.value[key]=(event.target as HTMLInputElement).value.replace('T',' ');queueSave();}
async function saveMetadata(){clearTimeout(saveTimer);if(saving.value)return;const changes=patch();if(!Object.keys(changes).length)return;const identity=eventId.value;saving.value=true;saveError.value='';try{const result=await requestJson('/api/workbench-events/metadata?work_type=event',{method:'POST',body:JSON.stringify({scope:props.scope,event_id:identity,patch:changes,base:base.value})});if(!disposed&&eventId.value===identity){merge(result);if(current.value.metadata_issues&&Object.keys(current.value.metadata_issues).length)saveError.value='部分资料需核对，填写已保留。';}}catch(e){if(!disposed&&eventId.value===identity)saveError.value=(e as Error).message;}finally{saving.value=false;if(!disposed&&Object.keys(patch()).length&&!saveError.value)saveTimer=window.setTimeout(saveMetadata,650);}}
function edit(action:string){editingAction.value=action;error.value='';persist();void nextTick(()=>textBox.value?.focus());}
async function send(){if(busy.value||!text.value.trim())return;busy.value=true;error.value='';try{if(!frozen.value){frozen.value={scope:props.scope,event_id:eventId.value,operation_id:uuid(),action:props.creation?'start':editingAction.value,text:text.value,metadata_patch:patch(),metadata_base:base.value};persist();}const result=await requestJson('/api/workbench-events/send?work_type=event',{method:'POST',body:JSON.stringify(frozen.value)});if(disposed)return;merge(result);frozen.value=null;editingAction.value='';text.value='';persist();emit('changed');if(props.creation){clearTimeout(saveTimer);eventId.value='web_'+uuid();current.value={};metadata.value=empty();base.value=empty();saveError.value='';persist();emit('accepted');}}catch(e){if(!disposed){if(e instanceof ApiError&&e.status&&e.status<500)frozen.value=null;error.value=(e as Error).message;persist();}}finally{busy.value=false;}}
async function retry(confirmed:boolean){if(busy.value||!current.value.delivery)return;if(!confirmed&&current.value.delivery.targets?.some((r:Dict)=>['posting','uncertain'].includes(r.state))){retryOpen.value=true;return;}busy.value=true;try{const result=await requestJson('/api/workbench-events/retry?work_type=event',{method:'POST',body:JSON.stringify({scope:props.scope,event_id:eventId.value,operation_id:current.value.delivery.operation_id,confirm_uncertain:confirmed})});merge(result);retryOpen.value=false;emit('changed');}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
async function remove(){if(busy.value)return;busy.value=true;try{await requestJson('/api/workbench-events/delete?work_type=event',{method:'POST',body:JSON.stringify({scope:props.scope,event_id:eventId.value})});sessionStorage.removeItem(cacheKey());emit('changed');}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
onBeforeUnmount(()=>{clearTimeout(saveTimer);persist();void saveMetadata();disposed=true;});
</script>
<style scoped>
.raw-event-card{border:1px solid var(--cf-border,#d8e0eb);border-radius:10px;padding:14px;background:var(--cf-surface,#fff);color:var(--cf-text,#243447);min-width:0}.raw-event-card header,.raw-event-card footer{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:space-between}.raw-event-card h3{font-size:16px;margin:0;overflow-wrap:anywhere}.raw-event-card header>div{flex:1;min-width:0}.raw-event-card header small,.muted{font-size:12px;opacity:.75}.raw-event-layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(260px,320px);gap:16px;margin-top:12px}.message-side,.metadata-side{min-width:0}.message-side>label,.metadata-fields label{display:grid;gap:5px;font-size:13px}.message-side textarea{min-height:220px}.raw-event-card textarea,.raw-event-card select,.raw-event-card input:not([type=checkbox]){width:100%;min-width:0;box-sizing:border-box;border:1px solid var(--cf-border,#cbd5e1);border-radius:5px;background:var(--cf-surface,#fff);color:inherit;padding:8px;font:inherit;min-height:38px}.raw-event-card textarea{resize:vertical;line-height:1.6}.raw-event-card button{min-height:38px;padding:7px 12px;border:1px solid var(--cf-border,#cbd5e1);border-radius:6px;background:var(--cf-surface,#fff);color:inherit;font:inherit;cursor:pointer}.raw-event-card button:disabled{opacity:.55;cursor:wait}.raw-event-card .primary{background:var(--cf-primary,#1762cf);color:white;border-color:transparent}.raw-event-card .danger{color:#c22b38}.raw-event-card footer{justify-content:flex-end;margin-top:12px}.raw-event-card pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit;line-height:1.7;margin:0}.metadata-side{border-left:1px solid var(--cf-border,#d8e0eb);padding-left:14px}.metadata-side summary{font-weight:600;cursor:pointer;min-height:38px;padding:6px 0;box-sizing:border-box}.metadata-side summary span{font-size:12px;margin-left:8px}.metadata-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.metadata-fields .wide{grid-column:1/-1}.metadata-fields .inline{display:flex;align-items:center;gap:7px}.metadata-status,.delivery-status,.feedback{font-size:13px;overflow-wrap:anywhere}.feedback{padding:9px;background:color-mix(in srgb,var(--cf-primary,#1762cf) 7%,transparent);border-radius:6px}.delivery-status{margin-top:14px}.delivery-status p{display:flex;gap:7px;flex-wrap:wrap;margin:5px 0}.delivery-status small{width:100%;opacity:.8}.delivery-status b{font-weight:500}@media(max-width:850px){.raw-event-layout{grid-template-columns:minmax(0,1fr)}.metadata-side{border-left:0;border-top:1px solid var(--cf-border,#d8e0eb);padding:12px 0 0}.raw-event-card input:not([type=checkbox]),.raw-event-card textarea,.raw-event-card select{font-size:16px}.raw-event-card{padding:12px}}
</style>
