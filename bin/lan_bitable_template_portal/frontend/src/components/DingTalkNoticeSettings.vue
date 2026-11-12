<template>
  <section ref="rootElement" class="ding-settings" aria-label="钉钉通告推送设置">
    <header><div><h3>钉钉通告推送</h3><p>保存后用于后续通告，飞书推送保持原样。名称和备注用于选择和管理，不加入群消息。</p></div><button :disabled="busy||dirty" @click="load">刷新</button></header>
    <fieldset :disabled="busy||!loaded">
      <section class="contact-directory" aria-label="艾特通讯录">
        <div class="directory-head">
          <h4>艾特通讯录 · {{ contacts.length }} 人</h4>
          <span v-if="invalidContacts.length" class="error directory-flag">{{ invalidContacts.length }} 处待修改</span>
          <span v-else-if="blankContacts.length" class="directory-flag">{{ blankContacts.length }} 行未填写，保存时忽略</span>
          <button type="button" class="add-contact" :disabled="contacts.length>=1000" @click="addContact">添加艾特人员</button>
        </div>
        <p>手机号在这里维护。每个机器人可按楼栋选择人员，选择时显示备注。自动导入的人员可在此补上姓名或岗位。</p>
        <p v-if="!contacts.length" class="hint">尚无艾特人员；添加后需填写备注和手机号才会保存。</p>
        <div class="contact-list">
          <div v-for="(person,index) in contacts" :key="person.id" class="contact-row" :data-contact="person.id"
            :class="{'has-problem':problemsFor(person).length,'is-blank':isBlank(person)}">
            <span class="contact-index" aria-hidden="true">{{ index+1 }}</span>
            <label>人员备注<input v-model="person.remark" maxlength="80" placeholder="例如：运维主管 王工"
              :aria-invalid="hasIssue(person,'备注')||undefined"></label>
            <label>手机号<input v-model.trim="person.mobile" type="tel" inputmode="tel" maxlength="21" autocomplete="off" placeholder="钉钉绑定手机号"
              :aria-invalid="hasIssue(person,'手机号')||undefined"></label>
            <button type="button" :disabled="referencedContactIds.has(person.id)" :title="referencedContactIds.has(person.id) ? '请先取消各机器人的选择' : '从通讯录移除'" @click="contacts.splice(index,1)">移除人员</button>
            <p v-if="problemsFor(person).length" class="row-error" role="alert">第 {{ index+1 }} 位：{{ problemsFor(person).join('、') }}</p>
            <p v-else-if="isBlank(person)" class="row-hint">未填写，保存时会忽略这一行</p>
          </div>
        </div>
      </section>
      <article v-for="(robot,index) in robots" :key="robot.id||robot.localId" class="robot">
        <div class="row"><strong>机器人 {{ index+1 }}</strong><label><input v-model="robot.enabled" type="checkbox">启用</label><button type="button" @click="robots.splice(index,1)">移除</button></div>
        <div class="fields">
          <label>名称<input v-model="robot.name" maxlength="80" placeholder="例如：A楼通告群"></label>
          <label>备注<input v-model="robot.remark" maxlength="500" placeholder="群用途、负责人等，用于辨别发送群"></label>
          <label>机器人 Webhook<input v-model="robot.webhook" type="password" autocomplete="new-password" spellcheck="false" :placeholder="robot.configured?'已保存，留空保持原地址':'粘贴完整机器人 Webhook'"><small v-if="robot.webhook_hint">{{ robot.webhook_hint }}</small></label>
          <label>加签密钥<input v-model="robot.secret" type="password" autocomplete="new-password" spellcheck="false" :placeholder="robot.configured?'已保存，留空保持原密钥':'以 SEC 开头的密钥'"></label>
        </div>
        <details><summary>推送范围：{{ robot.scopes.length?robot.scopes.map(s=>s==='110'?'110站':s+'楼').join('、'):'所有楼栋' }} · {{ robot.types.length?robot.types.join('、'):'所有通告类型' }}</summary>
          <div class="choices"><span>楼栋（不选即全部）</span><label v-for="scope in scopes" :key="scope"><input v-model="robot.scopes" type="checkbox" :value="scope">{{ scope==='110'?'110站':scope+'楼' }}</label></div>
          <div class="choices"><span>类型（不选即全部）</span><label v-for="type in types" :key="type"><input v-model="robot.types" type="checkbox" :value="type">{{ type }}</label></div>
        </details>
        <details class="mentions"><summary>艾特设置：{{ mentionLabels[robot.mention_mode] }}</summary>
          <div class="fields">
            <label>默认艾特方式<select v-model="robot.mention_mode"><option value="none">不艾特</option><option value="people">指定成员</option><option value="all">所有人</option></select></label>
            <template v-if="robot.mention_mode==='people'">
              <label>钉钉用户 ID<textarea v-model="robot.mentionUsers" rows="2" placeholder="多个用户 ID 用逗号或换行分隔"></textarea></label>
            </template>
          </div>
          <template v-if="robot.mention_mode==='people'">
            <p>本机器人只艾特通用名单和本次通告涉及楼栋的人员，同一个人只艾特一次。</p>
            <p v-if="!contactOptions.length">请先在上方通讯录添加手机号并填写备注。</p>
            <div class="fields building-mentions">
              <label>
                <span>所有楼栋通用人员</span>
                <NSelect v-model:value="robot.mention_contact_ids" :options="contactOptions" :disabled="busy" multiple filterable clearable placeholder="按备注选择" aria-label="所有楼栋通用人员" data-mention-scope="common" />
              </label>
              <label v-for="scope in scopes" :key="scope">
                <span>{{ scope==='110'?'110站':scope+'楼' }}艾特人员</span>
                <NSelect v-model:value="robot.mention_building_contact_ids[scope]" :options="contactOptions" :disabled="busy" multiple filterable clearable placeholder="按备注选择" :aria-label="(scope==='110'?'110站':scope+'楼')+'艾特人员'" :data-mention-scope="scope" />
              </label>
            </div>
          </template>
          <p>仅群内成员可收到艾特。艾特标记位于通告最后；配置手机号时消息中会包含手机号。测试消息不会艾特。</p>
        </details>
        <div class="row"><small>开始、更新、结束时可在上述范围内勾选机器人，所选机器人并行推送。备注不会加入消息。</small><button :disabled="dirty||!robot.id||!robot.enabled" @click="test(robot)">发送测试消息</button></div>
      </article>
      <p v-if="!robots.length">尚未配置钉钉机器人。</p>
    </fieldset>
    <h4>最近投递记录</h4>
    <p v-if="!deliveries.length">暂无记录。</p>
    <table v-else class="delivery-table">
      <thead><tr><th scope="col">机器人</th><th scope="col">状态</th><th scope="col">段数</th><th scope="col">说明</th><th scope="col"><span class="visually-hidden">操作</span></th></tr></thead>
      <tbody>
        <tr v-for="delivery in visibleDeliveries" :key="delivery.id" :class="'state-'+delivery.state">
          <th scope="row">{{ delivery.robot_name }}</th>
          <td>{{ labels[delivery.state]||delivery.state }}</td>
          <td class="numeric">{{ delivery.sent_parts }}/{{ delivery.parts }}</td>
          <td class="delivery-detail">{{ delivery.summary }}<span v-if="delivery.reason">{{ delivery.reason }}</span></td>
          <td><button v-if="['failed','uncertain','posting'].includes(delivery.state)" :disabled="busy||dirty" @click="retryItem=delivery">核对后重试</button></td>
        </tr>
      </tbody>
    </table>
    <nav v-if="deliveries.length>5" aria-label="投递记录翻页"><button :disabled="deliveryPage<=1" @click="deliveryPage--">上一页</button><span> 第 {{deliveryPage}} / {{Math.ceil(deliveries.length/5)}} 页 </span><button :disabled="deliveryPage*5>=deliveries.length" @click="deliveryPage++">下一页</button></nav>
    <!-- Sticky so a failed save can never be scrolled away from: the old layout put the error at the
         top of the page and the button at the bottom, and users never saw why nothing persisted. -->
    <div ref="saveBar" class="save-bar">
      <p v-if="error" role="alert" class="error">{{ error }}</p>
      <p v-else-if="message" role="status">{{ message }}</p>
      <span class="save-state" :class="{'is-dirty':dirty}">{{ busy?'处理中…':dirty?'有未保存修改':'配置已是最新' }}</span>
      <button :disabled="busy||!loaded||robots.length>=20" @click="add">添加机器人</button>
      <button class="primary" :disabled="!dirty||busy||!loaded" @click="save">{{ busy?'保存中…':'保存配置' }}</button>
    </div>
    <NModal :show="!!retryItem" preset="dialog" title="核对钉钉推送" positive-text="按当前配置重试" negative-text="取消" @positive-click="retry" @negative-click="retryItem=null" @update:show="v=>{if(!v)retryItem=null}">
      <p>请先在对应钉钉群核对原消息。重试将使用该机器人当前保存的配置，仅发送未确认完成的部分；如果原消息实际已送达，重发可能造成重复。</p>
    </NModal>
    <NModal :show="!!leaveAction" preset="dialog" type="warning" title="有未保存的修改" aria-label="有未保存的修改"
      content="离开后本次对艾特通讯录和机器人的修改会丢失，需要先保存。"
      positive-text="仍要离开" negative-text="继续编辑"
      @positive-click="proceedLeave" @update:show="v=>{if(!v)leaveAction=null}" />
  </section>
</template>
<script setup lang="ts">
import {computed,nextTick,onMounted,ref,onBeforeUnmount,shallowRef,watch} from 'vue';
import {NModal,NSelect} from 'naive-ui';
import {requestJson} from '../api/client';
import {registerNavigationGuard} from '../navigation';
type Contact = { id: string; remark: string; mobile: string };
type Robot = {
  id?: string; localId?: string; name: string; remark: string; webhook: string; secret: string;
  enabled: boolean; scopes: string[]; types: string[]; configured?: boolean; webhook_hint?: string;
  mention_mode: 'none' | 'people' | 'all'; mentionUsers: string; mention_user_ids?: string[];
  mention_contact_ids: string[]; mention_building_contact_ids: Record<string, string[]>;
};
type Delivery={summary:string;id:string;robot_name:string;state:string;reason:string;sent_parts:number;parts:number};

const rootElement=ref<HTMLElement>(),saveBar=ref<HTMLElement>();
const robots=ref<Robot[]>([]),contacts=ref<Contact[]>([]),revision=ref(0),types=ref<string[]>([]),scopes=ref<string[]>([]),deliveries=ref<Delivery[]>([]);
const deliveryPage=ref(1);
const visibleDeliveries=computed(()=>deliveries.value.slice((deliveryPage.value-1)*5,deliveryPage.value*5));
const busy=ref(false),loaded=ref(false),error=ref(''),message=ref(''),baseline=ref(''),retryItem=ref<Delivery|null>(null);
const leaveAction=shallowRef<(()=>void)|null>(null);
const settingsSnapshot=()=>JSON.stringify([robots.value,contacts.value]);
const dirty=computed(()=>loaded.value && settingsSnapshot()!==baseline.value);
const contactOptions=computed(()=>contacts.value.filter(person=>person.remark.trim()).map(person=>({
  label:person.remark.trim(),value:person.id,
})));
const referencedContactIds=computed(()=>new Set(robots.value.flatMap(robot=>[
  ...robot.mention_contact_ids,...Object.values(robot.mention_building_contact_ids).flat(),
])));
let disposed=false;
let leaving=false;
let unregisterGuard:(()=>void)|undefined;
const labels:Record<string,string>={sent:'已送达',pending:'等待发送',posting:'正在发送',failed:'发送失败',uncertain:'结果待核验'};
const mentionLabels:Record<string,string>={none:'不艾特',people:'指定成员',all:'所有人'};
const parseMembers=(text:string)=>text.split(/[\s,，;；]+/).map(x=>x.trim()).filter(Boolean);

// Mirrors the server's directory rules so a doomed save is caught before it is sent. The server
// validates the whole list atomically, so one bad row used to silently discard everybody's edits.
const normalizedMobile=(value:string)=>{const digits=value.replace(/\D/g,'');return digits.length===13&&digits.startsWith('86')?digits.slice(2):digits;};
const isBlank=(person:Contact)=>!person.mobile.trim()&&!person.remark.trim();
const contactProblems=computed(()=>{
  const result=new Map<string,string[]>(),phones=new Set<string>(),remarks=new Set<string>();
  for(const person of contacts.value){
    const mobile=person.mobile.trim(),remark=person.remark.trim();
    if(!mobile&&!remark){result.set(person.id,[]);continue;}
    const issues:string[]=[];
    if(!/^\+?[0-9]{6,20}$/.test(mobile))issues.push('手机号无效');
    else if(phones.has(normalizedMobile(mobile)))issues.push('手机号与其他人员重复');
    if(!remark)issues.push('备注未填写');
    else if(remark.length>80)issues.push('备注超过80字');
    else if(remarks.has(remark.toLowerCase()))issues.push('备注与其他人员重复');
    if(!issues.length){phones.add(normalizedMobile(mobile));remarks.add(remark.toLowerCase());}
    result.set(person.id,issues);
  }
  return result;
});
const problemsFor=(person:Contact)=>contactProblems.value.get(person.id)||[];
const hasIssue=(person:Contact,keyword:string)=>problemsFor(person).some(issue=>issue.includes(keyword));
const invalidContacts=computed(()=>contacts.value.filter(person=>problemsFor(person).length>0));
const blankContacts=computed(()=>contacts.value.filter(isBlank));
// Rows added but never filled in are dropped rather than vetoing the rest of the directory.
const submittedContacts=computed(()=>contacts.value.filter(person=>!isBlank(person)));
// Fixing or removing the offending rows should retire the stale validation banner on its own.
watch(invalidContacts,rows=>{if(!rows.length&&error.value.startsWith('艾特通讯录有'))error.value='';});

function localId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)), byte=>byte.toString(16).padStart(2,'0')).join('');
}
function apply(data:any) {
  if(disposed)return;
  revision.value=data.revision;
  types.value=data.types;
  scopes.value=data.scopes;
  contacts.value=data.contacts||[];
  robots.value=data.robots.map((robot:Robot)=>({
    ...robot,webhook:'',secret:'',mention_mode:robot.mention_mode||'none',
    mentionUsers:(robot.mention_user_ids||[]).join('\n'),
    mention_contact_ids:robot.mention_contact_ids||[],
    mention_building_contact_ids:Object.fromEntries(scopes.value.map(scope=>[
      scope,robot.mention_building_contact_ids?.[scope]||[],
    ])),
  }));
  baseline.value=settingsSnapshot();
  if(data.deliveries) {
    deliveries.value=data.deliveries;
    deliveryPage.value=Math.min(deliveryPage.value,Math.max(1,Math.ceil(deliveries.value.length/5)));
  }
  loaded.value=true;
}
async function load(){busy.value=true;error.value='';try{apply(await requestJson('/api/dingtalk-notices'));}catch(e:any){if(!disposed)error.value=e.message||'读取失败';}finally{busy.value=false;}}
function addContact() {
  contacts.value.push({id:localId(),remark:'',mobile:''});
  void nextTick(()=>{
    const rows=rootElement.value?.querySelectorAll<HTMLElement>('.contact-row');
    const row=rows?.[rows.length-1];
    row?.scrollIntoView({block:'center'});
    row?.querySelector<HTMLInputElement>('input')?.focus({preventScroll:true});
  });
}
function revealContact(id:string){
  void nextTick(()=>{
    const row=rootElement.value?.querySelector<HTMLElement>('[data-contact="'+id+'"]');
    row?.scrollIntoView({block:'center'});
    row?.querySelector<HTMLInputElement>('input')?.focus({preventScroll:true});
  });
}
function add() {
  robots.value.push({
    localId:localId(),name:'',remark:'',webhook:'',secret:'',enabled:true,scopes:[],types:[],
    mention_mode:'none',mentionUsers:'',mention_contact_ids:[],mention_building_contact_ids:{},
  });
}
async function save() {
  error.value='';
  message.value='';
  if(invalidContacts.value.length){
    error.value='艾特通讯录有 '+invalidContacts.value.length+' 处需要修改，已在对应行标出；未修正前不会保存任何改动。';
    revealContact(invalidContacts.value[0].id);
    return;
  }
  busy.value=true;
  try {
    const payload=robots.value.map(({mentionUsers,...robot})=>({
      ...robot,mention_user_ids:parseMembers(mentionUsers),
      mention_building_contact_ids:Object.fromEntries(
        Object.entries(robot.mention_building_contact_ids).filter(([,ids])=>ids.length),
      ),
    }));
    apply(await requestJson('/api/dingtalk-notices',{
      method:'POST',
      body:JSON.stringify({revision:revision.value,robots:payload,contacts:submittedContacts.value}),
    }));
    message.value='配置已保存。事件使用默认选择，其他通告按本次勾选发送。';
  } catch(e:any) {
    error.value=e.message||'保存未确认，请刷新核对';
    saveBar.value?.scrollIntoView({block:'nearest'});
  } finally {
    busy.value=false;
  }
}
async function test(robot:Robot){busy.value=true;error.value='';message.value='';try{const result=await requestJson('/api/dingtalk-notices/test',{method:'POST',body:JSON.stringify({robot_id:robot.id,operation_id:localId()})}) as any;message.value=result.state==='sent'?'钉钉已确认接收测试消息。':(result.reason||'测试结果待核验');await load();}catch(e:any){error.value=e.message||'测试结果未确认';}finally{busy.value=false;}}
async function retry(){const item=retryItem.value;if(!item)return;busy.value=true;error.value='';try{await requestJson('/api/dingtalk-notices/retry',{method:'POST',body:JSON.stringify({id:item.id,confirm_uncertain:true})});retryItem.value=null;await load();}catch(e:any){error.value=e.message||'重试未确认';}finally{busy.value=false;}}
function canLeave(proceed:()=>void){
  if(leaving)return true;
  if(busy.value){error.value='正在处理，请稍后再离开。';return false;}
  if(!dirty.value)return true;
  leaveAction.value=proceed;
  return false;
}
function proceedLeave(){
  const action=leaveAction.value;
  leaveAction.value=null;
  leaving=true;
  try{action?.();}finally{leaving=false;}
}
defineExpose({canLeave});
function beforeUnload(event:BeforeUnloadEvent){if(dirty.value||busy.value)event.preventDefault();}
onMounted(()=>{
  window.addEventListener('beforeunload',beforeUnload);
  // AdminTools swaps tabs with v-else-if and no KeepAlive, so leaving used to drop edits silently.
  unregisterGuard=registerNavigationGuard((_target,proceed)=>canLeave(proceed));
  void load();
});
onBeforeUnmount(()=>{disposed=true;unregisterGuard?.();window.removeEventListener('beforeunload',beforeUnload);});
</script>
<style scoped>
.ding-settings{display:grid;gap:12px;min-width:0}.ding-settings h3,.ding-settings h4,.ding-settings p{margin:0}header,.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}header>div{flex:1}p,small{color:var(--cf-text-muted,#667789);font-size:13px}.robot{border:1px solid var(--cf-border,#dce3ec);padding:12px;margin-bottom:10px;display:grid;gap:10px;border-radius:8px}.fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.fields>label{display:grid;gap:4px;min-width:0;font-size:13px}:is(input:not([type=checkbox]),select,textarea){width:100%;min-width:0;box-sizing:border-box;min-height:38px;padding:7px;border:1px solid var(--cf-border,#cbd5e1);border-radius:5px;background:var(--cf-surface,#fff);color:inherit;font:inherit}.row>small{flex:1}.row>button:last-child{margin-left:auto}.choices{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}.choices>span{width:100%;font-size:12px}.choices label,.row label{display:flex;align-items:center;gap:4px}fieldset{border:0;margin:0;padding:0;min-width:0}button{min-height:36px;padding:6px 12px;border:1px solid var(--cf-border,#cbd5e1);background:var(--cf-surface,#fff);color:inherit;border-radius:6px;cursor:pointer}button:disabled{opacity:.5;cursor:default}.error{color:#be2431}summary{cursor:pointer;font-size:13px}
.hint{font-size:12px}
.visually-hidden{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
/* Directory stays open: hiding the phone book behind a disclosure is what let people edit rows
   they could no longer see the validation for. */
.contact-directory{border:1px solid var(--cf-border,#dce3ec);border-radius:8px;padding:12px;margin-bottom:12px;display:grid;gap:8px}
.directory-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.directory-head h4{flex:1;min-width:0}
.directory-flag{font-size:12px;white-space:nowrap}
.add-contact{margin-left:auto}
.contact-list{display:grid;gap:2px;max-height:min(46dvh,420px);overflow:auto}
.contact-row{display:grid;grid-template-columns:auto minmax(0,1fr) minmax(0,1fr) auto;align-items:end;gap:8px;
  padding:8px 6px;border-bottom:1px solid var(--cf-border,#eef2f7)}
.contact-row:last-child{border-bottom:0}
.contact-row>label{display:grid;gap:4px;min-width:0;font-size:13px}
.contact-index{align-self:center;min-width:20px;font-size:12px;color:var(--cf-text-muted,#8a97a8);font-variant-numeric:tabular-nums;text-align:right}
.contact-row.has-problem{background:color-mix(in srgb,var(--cf-surface,#fff) 92%,#be2431)}
.contact-row.has-problem input[aria-invalid=true]{border-color:#be2431}
.contact-row.is-blank{opacity:.72}
.row-error,.row-hint{grid-column:1/-1;margin:0;font-size:12px}
.row-error{color:#be2431}
/* Sticky action bar keeps the outcome of a save next to the button that triggered it. */
.save-bar{position:sticky;bottom:0;z-index:6;display:flex;align-items:center;gap:10px;flex-wrap:wrap;
  padding:10px 0 max(10px,env(safe-area-inset-bottom));border-top:1px solid var(--cf-border,#dce3ec);
  background:var(--cf-surface,#fff)}
.save-bar p{flex:1 1 220px;min-width:0}
.save-state{font-size:12px;color:var(--cf-text-muted,#667789);white-space:nowrap}
.save-state.is-dirty{color:#a65b00;font-weight:600}
.primary{background:#1762cf;color:white;margin-left:auto}
.delivery-table{width:100%;border-collapse:collapse;font-size:13px}
.delivery-table th,.delivery-table td{padding:8px 6px;border-bottom:1px solid var(--cf-border,#ddd);text-align:left;vertical-align:top;min-width:0}
.delivery-table thead th{font-size:12px;font-weight:600;color:var(--cf-text-muted,#667789)}
.delivery-table tbody th{font-weight:600}
.delivery-table .numeric{font-variant-numeric:tabular-nums;white-space:nowrap}
.delivery-detail{color:var(--cf-text-muted,#667789);overflow-wrap:anywhere}
.delivery-detail span{display:block}
.delivery-table tr.state-failed th,.delivery-table tr.state-uncertain th{color:#be2431}
.delivery-table td button{white-space:nowrap}
nav{display:flex;align-items:center;gap:8px;font-size:13px}
.building-mentions :deep(.n-base-selection){min-width:0}
.mentions .fields{margin-top:10px}.mentions p{margin-top:8px}.mentions textarea{resize:vertical}
@media(max-width:600px){
  .contact-row{grid-template-columns:auto minmax(0,1fr);padding:8px 2px}
  .contact-row>label:last-of-type{grid-column:2}
  .contact-row>button{grid-column:2;justify-self:end}
  .fields{grid-template-columns:1fr}
  :is(input:not([type=checkbox]),select,textarea){font-size:16px}
  .robot{padding:10px}
  .delivery-table{font-size:12px}
  .save-bar{gap:8px}
  .save-bar p{flex-basis:100%}
  .save-bar button{flex:1 1 auto}
  .primary{margin-left:0}
}
</style>
