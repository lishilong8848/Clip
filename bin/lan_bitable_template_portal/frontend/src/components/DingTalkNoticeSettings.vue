<template>
  <section class="ding-settings" aria-label="钉钉通告推送设置">
    <header><div><h3>钉钉通告推送</h3><p>保存后用于后续通告，飞书推送保持原样。名称和备注用于选择和管理，不加入群消息。</p></div><button :disabled="busy||dirty" @click="load">刷新</button></header>
    <p v-if="error" role="alert" class="error">{{ error }}</p><p v-if="message" role="status">{{ message }}</p>
    <fieldset :disabled="busy||!loaded">
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
              <label>所有楼栋通用手机号<textarea v-model="robot.mentionMobiles" rows="2" placeholder="选填；多个手机号用逗号或换行分隔"></textarea></label>
            </template>
          </div>
          <template v-if="robot.mention_mode==='people'"><p>按楼栋填写对应手机号；只艾特本次通告涉及楼栋及通用名单中的人员。</p><div class="fields building-mentions"><label v-for="scope in scopes" :key="scope">{{ scope==='110'?'110站':scope+'楼' }}手机号<textarea v-model="robot.mentionByBuilding[scope]" rows="2" placeholder="多个手机号用逗号或换行分隔" /></label></div></template>
          <p>仅群内成员可收到艾特。艾特标记位于通告最后；配置手机号时消息中会包含手机号。测试消息不会艾特。</p>
        </details>
        <div class="row"><small>开始、更新、结束时可在上述范围内勾选机器人，所选机器人并行推送。备注不会加入消息。</small><button :disabled="dirty||!robot.id||!robot.enabled" @click="test(robot)">发送测试消息</button></div>
      </article>
      <p v-if="!robots.length">尚未配置钉钉机器人。</p>
      <footer><button :disabled="robots.length>=20" @click="add">添加机器人</button><button class="primary" :disabled="!dirty" @click="save">{{ busy?'保存中…':'保存配置' }}</button></footer>
    </fieldset>
    <h4>最近投递记录</h4><p v-if="!deliveries.length">暂无记录。</p>
    <div v-for="delivery in visibleDeliveries" :key="delivery.id" class="delivery"><strong>{{ delivery.robot_name }}</strong><span>{{ labels[delivery.state]||delivery.state }} · {{ delivery.sent_parts }}/{{ delivery.parts }} 段</span><small>{{ delivery.summary }}<br />{{ delivery.reason }}</small><button v-if="['failed','uncertain','posting'].includes(delivery.state)" :disabled="busy||dirty" @click="retryItem=delivery">核对后重试</button></div>
    <nav v-if="deliveries.length>5" aria-label="投递记录翻页"><button :disabled="deliveryPage<=1" @click="deliveryPage--">上一页</button><span> 第 {{deliveryPage}} / {{Math.ceil(deliveries.length/5)}} 页 </span><button :disabled="deliveryPage*5>=deliveries.length" @click="deliveryPage++">下一页</button></nav>
    <NModal :show="!!retryItem" preset="dialog" title="核对钉钉推送" positive-text="按当前配置重试" negative-text="取消" @positive-click="retry" @negative-click="retryItem=null" @update:show="v=>{if(!v)retryItem=null}">
      <p>请先在对应钉钉群核对原消息。重试将使用该机器人当前保存的配置，仅发送未确认完成的部分；如果原消息实际已送达，重发可能造成重复。</p>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import {computed,onMounted,ref,onBeforeUnmount} from 'vue';
import {NModal} from 'naive-ui';
import {requestJson} from '../api/client';
type Robot={id?:string;localId?:string;name:string;remark:string;webhook:string;secret:string;enabled:boolean;scopes:string[];types:string[];configured?:boolean;webhook_hint?:string;mention_mode:'none'|'people'|'all';mentionUsers:string;mentionMobiles:string;mentionByBuilding:Record<string,string>;mention_user_ids?:string[];mention_mobiles?:string[];mention_building_mobiles?:Record<string,string[]>};
type Delivery={summary:string;id:string;robot_name:string;state:string;reason:string;sent_parts:number;parts:number};
const robots=ref<Robot[]>([]),revision=ref(0),types=ref<string[]>([]),scopes=ref<string[]>([]),deliveries=ref<Delivery[]>([]);
const deliveryPage=ref(1);
const visibleDeliveries=computed(()=>deliveries.value.slice((deliveryPage.value-1)*5,deliveryPage.value*5));
const busy=ref(false),loaded=ref(false),error=ref(''),message=ref(''),baseline=ref('[]'),retryItem=ref<Delivery|null>(null);
const dirty=computed(()=>JSON.stringify(robots.value)!==baseline.value);let disposed=false;
const labels:Record<string,string>={sent:'已送达',pending:'等待发送',posting:'正在发送',failed:'发送失败',uncertain:'结果待核验'};
const mentionLabels:Record<string,string>={none:'不艾特',people:'指定成员',all:'所有人'};
const parseMembers=(text:string)=>text.split(/[\s,，;；]+/).map(x=>x.trim()).filter(Boolean);
function apply(data:any){if(disposed)return;revision.value=data.revision;types.value=data.types;scopes.value=data.scopes;robots.value=data.robots.map((r:Robot)=>({...r,webhook:'',secret:'',mention_mode:r.mention_mode||'none',mentionUsers:(r.mention_user_ids||[]).join('\n'),mentionMobiles:(r.mention_mobiles||[]).join('\n'),mentionByBuilding:Object.fromEntries((data.scopes as string[]).map(scope=>[scope,(r.mention_building_mobiles?.[scope]||[]).join('\n')]))}));baseline.value=JSON.stringify(robots.value);if(data.deliveries){deliveries.value=data.deliveries;deliveryPage.value=Math.min(deliveryPage.value,Math.max(1,Math.ceil(deliveries.value.length/5)));}loaded.value=true;}
async function load(){busy.value=true;error.value='';try{apply(await requestJson('/api/dingtalk-notices'));}catch(e:any){if(!disposed)error.value=e.message||'读取失败';}finally{busy.value=false;}}
function add(){robots.value.push({localId:crypto.randomUUID(),name:'',remark:'',webhook:'',secret:'',enabled:true,scopes:[],types:[],mention_mode:'none',mentionUsers:'',mentionMobiles:'',mentionByBuilding:{}});}
async function save(){busy.value=true;error.value='';message.value='';try{const payload=robots.value.map(({mentionUsers,mentionMobiles,mentionByBuilding,...robot})=>({...robot,mention_user_ids:parseMembers(mentionUsers),mention_mobiles:parseMembers(mentionMobiles),mention_building_mobiles:Object.fromEntries(scopes.value.map(scope=>[scope,parseMembers(mentionByBuilding[scope]||'')]))}));apply(await requestJson('/api/dingtalk-notices',{method:'POST',body:JSON.stringify({revision:revision.value,robots:payload})}));message.value='配置已保存。事件使用默认选择，其他通告按本次勾选发送。';}catch(e:any){error.value=e.message||'保存未确认，请刷新核对';}finally{busy.value=false;}}
async function test(robot:Robot){busy.value=true;error.value='';message.value='';try{const result=await requestJson('/api/dingtalk-notices/test',{method:'POST',body:JSON.stringify({robot_id:robot.id,operation_id:crypto.randomUUID().replace(/-/g,'')})}) as any;message.value=result.state==='sent'?'钉钉已确认接收测试消息。':(result.reason||'测试结果待核验');await load();}catch(e:any){error.value=e.message||'测试结果未确认';}finally{busy.value=false;}}
async function retry(){const item=retryItem.value;if(!item)return;busy.value=true;error.value='';try{await requestJson('/api/dingtalk-notices/retry',{method:'POST',body:JSON.stringify({id:item.id,confirm_uncertain:true})});retryItem.value=null;await load();}catch(e:any){error.value=e.message||'重试未确认';}finally{busy.value=false;}}
onMounted(load);onBeforeUnmount(()=>{disposed=true;});
</script>
<style scoped>
.ding-settings{display:grid;gap:12px;min-width:0}.ding-settings h3,.ding-settings h4,.ding-settings p{margin:0}header,.row,footer{display:flex;gap:10px;align-items:center;flex-wrap:wrap}header>div{flex:1}p,small{color:var(--cf-text-muted,#667789);font-size:13px}.robot{border:1px solid var(--cf-border,#dce3ec);padding:12px;margin-bottom:10px;display:grid;gap:10px;border-radius:8px}.fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.fields>label{display:grid;gap:4px;min-width:0;font-size:13px}:is(input:not([type=checkbox]),select,textarea){width:100%;min-width:0;box-sizing:border-box;min-height:38px;padding:7px;border:1px solid var(--cf-border,#cbd5e1);border-radius:5px;background:var(--cf-surface,#fff);color:inherit;font:inherit}.row>small{flex:1}.row>button:last-child{margin-left:auto}.choices{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}.choices>span{width:100%;font-size:12px}.choices label,.row label{display:flex;align-items:center;gap:4px}fieldset{border:0;margin:0;padding:0;min-width:0}button{min-height:36px;padding:6px 12px;border:1px solid var(--cf-border,#cbd5e1);background:var(--cf-surface,#fff);color:inherit;border-radius:6px;cursor:pointer}button:disabled{opacity:.5;cursor:default}.primary{background:#1762cf;color:white;margin-left:auto}.error{color:#be2431}.delivery{display:flex;gap:8px;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--cf-border,#ddd);padding:8px 0}.delivery small{flex:1;min-width:160px}summary{cursor:pointer;font-size:13px}.mentions .fields{margin-top:10px}.mentions p{margin-top:8px}.mentions textarea{resize:vertical}@media(max-width:600px){.fields{grid-template-columns:1fr}:is(input:not([type=checkbox]),select,textarea){font-size:16px}.robot{padding:10px}.delivery{font-size:13px}}
</style>
