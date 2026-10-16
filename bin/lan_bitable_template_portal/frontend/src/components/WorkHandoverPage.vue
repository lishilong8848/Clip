<template>
  <NConfigProvider :locale="zhCN" :date-locale="dateZhCN" :theme-overrides="{common:{primaryColor:'#2563eb',primaryColorHover:'#1d4ed8',primaryColorPressed:'#1e40af',primaryColorSuppl:'#2563eb',borderRadius:'6px'}}">
    <VnetBackButton to="/?entry=tools" :disabled="busy" />
    <section class="work-handover" aria-label="人员工作交接" @paste="pasteFiles" @dragover.prevent @drop.prevent="dropFiles">
      <header class="handover-toolbar" data-v2-navigation>
        <div class="handover-tabs" role="tablist" aria-label="交接范围">
          <button v-for="tab in views" :key="tab.value" type="button" role="tab" :aria-selected="view===tab.value" :disabled="busy" @click="changeView(tab.value)">{{tab.label}}</button>
        </div>
        <div class="handover-tools"><NButton :disabled="busy||loading" @click="reload"><RefreshCw :size="16" />刷新</NButton><NButton type="primary" :disabled="busy" @click="newDraft"><Plus :size="16" />发起交接</NButton></div>
      </header>
      <NAlert v-if="error" type="error" :bordered="false" role="alert" class="handover-message">{{error}}</NAlert>
      <NAlert v-if="success" type="success" :bordered="false" role="status" class="handover-message">{{success}}</NAlert>
      <div class="handover-layout" :class="{'has-detail':!!detail||detailLoading}">
        <aside class="handover-list" aria-label="交接单列表">
          <NSpin :show="loading"><NEmpty v-if="!loading&&!list.length" description="暂无交接单" />
            <button v-for="row in list" :key="row.id" class="handover-list-row" :class="{selected:detail?.id===row.id}" type="button" :disabled="busy" @click="select(row.id)">
              <div><strong>{{row.title}}</strong><span class="handover-status">{{statusLabel(row.status)}}</span></div>
              <p>{{row.owner.name}} → {{row.recipient_name||'待选接收人'}}</p><small>{{row.remaining}} 项待处理 · {{row.total}} 项交接 · {{timeText(row.updated_at)}}</small>
            </button>
          </NSpin>
          <NPagination v-if="total>30" :page="page" :page-count="Math.ceil(total/30)" simple :disabled="busy" @update:page="changePage" />
        </aside>
        <main v-if="detail" class="handover-detail" aria-label="交接单详情">
          <header class="handover-detail-head"><NButton class="handover-list-back" text :disabled="busy" @click="leaveDetail"><ArrowLeft :size="17" />交接列表</NButton><NTag :type="detail.status==='accepted'?'success':detail.status==='pending'?'warning':'default'" size="small">{{statusLabel(detail.status)}}</NTag><span>{{detail.owner.name}} · {{timeText(detail.created_at)}}</span><small>版本 {{detail.version}}</small></header>
          <div v-if="editable" class="handover-fields">
            <label><span>交接名称</span><NInput v-model:value="form.title" :input-props="{'aria-label':'交接名称'}" :maxlength="200" :disabled="busy" placeholder="例如：E楼暖通工作交接" /></label>
            <label><span>接收人员</span><NSelect v-model:value="form.recipient_id" :options="peopleOptions" filterable clearable :disabled="busy" placeholder="搜索姓名或工号" /></label>
            <label class="handover-wide"><span>交接说明</span><NInput v-model:value="form.notes" type="textarea" :autosize="{minRows:2,maxRows:5}" :maxlength="6000" :disabled="busy" placeholder="交接范围、重点遗留问题及注意事项" /></label>
          </div>
          <template v-else><h2>{{detail.title}}</h2><p class="handover-people">{{detail.owner.name}} <ArrowRight :size="16" /> {{detail.recipient_name}}</p><p v-if="detail.notes" class="handover-note">{{detail.notes}}</p></template>
          <NAlert v-if="detail.status==='returned'" type="warning" :bordered="false">{{detail.decision_by}}：{{detail.decision_reason}}</NAlert>
          <NAlert v-if="detail.notification_failures?.length" type="warning" :bordered="false">
            {{detail.notification_failures.map((kind:string)=>notificationLabels[kind]||'飞书通知').join('、')}}暂未送达，后台会继续重试，交接状态不受影响。
            <NButton text :disabled="busy" @click="act('retry-notification')">重试通知</NButton>
          </NAlert>
          <NAlert v-else-if="detail.status==='pending'&&detail.is_owner" type="info" :bordered="false">
            {{detail.notification?.state==='sent'?'已发送接收邀请，等待对方本人确认。':'接收邀请正在等待飞书智能体发送。'}}
          </NAlert>
          <div class="handover-item-toolbar" data-v2-navigation>
            <div class="handover-tabs" role="tablist" aria-label="事项分类"><button v-for="tab in itemTabs" :key="tab.value" type="button" role="tab" :aria-selected="itemFilter===tab.value" @click="itemFilter=tab.value">{{tab.label}} <small>{{countItems(tab.value)}}</small></button></div>
            <div class="handover-tools"><NButton v-if="editable" size="small" :disabled="busy" @click="editItem()"><Plus :size="15" />添加事项</NButton><NButton v-if="editable" size="small" :disabled="busy" @click="sourceOpen=true"><ListPlus :size="15" />从业务添加</NButton><NButton v-if="!editable&&detail.items.some((i:Dict)=>i.source_id)" class="handover-refresh" size="small" :loading="refreshing" :disabled="busy" aria-label="刷新业务状态" title="刷新业务状态" @click="refreshSources"><RefreshCw :size="16" /><span>刷新业务状态</span></NButton></div>
          </div>
          <NAlert v-if="detail.refresh_warnings?.length" type="warning" :bordered="false">{{detail.refresh_warnings.join('；')}}</NAlert>
          <NEmpty v-if="!filteredItems.length" description="暂无该类事项" />
          <div class="handover-items">
            <article v-for="item in visibleItems" :key="item.id" class="handover-item" :class="{done:item.done,removed:item.removed}">
              <NCheckbox v-if="canCheck(item)" :checked="!!item.done" :disabled="busy||item.restricted" :aria-label="(item.done?'恢复未完成：':'确认完成：')+item.title" @update:checked="value=>act(value?'complete':'reopen',item.id)" />
              <div class="handover-item-body"><div class="handover-item-title"><strong>{{item.title}}</strong><NTag size="small" :bordered="false">{{scopeLabel(item.scope)}}</NTag><NTag size="small" :bordered="false">{{kindLabels[item.kind]}}</NTag></div>
                <div v-if="item.source_id" class="handover-item-meta"><span>业务：{{item.business_status||'待核对'}}</span><small>读取于 {{timeText(item.source_checked_at)}}</small></div>
                <p v-if="item.notes" class="handover-note">{{item.notes}}</p>
                <small v-if="item.done" class="handover-confirmed">{{item.confirmed_by}}于 {{timeText(item.confirmed_at)}} 确认清单完成</small>
                <small v-if="item.removed">{{item.removed_by}}于 {{timeText(item.removed_at)}} 从清单移除</small>
              </div>
              <div class="handover-item-actions"><NButton v-if="item.source_url&&!item.restricted" size="small" text :disabled="busy" @click="openBusiness(item.source_url)"><ExternalLink :size="15" />进入办理</NButton><NButton v-if="editable" size="small" text :disabled="busy" @click="editItem(item)"><Pencil :size="15" />编辑</NButton><NButton v-if="editable||detail.status==='accepted'&&detail.is_recipient&&!item.restricted" size="small" text :disabled="busy" @click="removeItem(item)"><RotateCcw v-if="item.removed" :size="15" /><Trash2 v-else :size="15" />{{item.removed?'恢复':'移除'}}</NButton></div>
            </article>
          </div>
          <NPagination v-if="filteredItems.length>20" v-model:page="itemPage" :page-count="Math.ceil(filteredItems.length/20)" :page-slot="5" />
          <section class="handover-files" aria-label="交接资料"><div class="handover-section-head"><strong>交接附件 <small>{{detail.files?.length||0}}</small></strong><NButton v-if="editable" size="small" :disabled="busy" @click="fileInput?.click()"><Upload :size="15" />添加资料</NButton></div><input ref="fileInput" type="file" multiple class="handover-file-input" accept=".pdf,.doc,.docx,.xls,.xlsx,.xlsm,.md,.txt,.csv,.jpg,.jpeg,.png,.webp" @change="pickFiles" />
            <div v-for="file in detail.files||[]" :key="file.id" class="handover-file"><a :href="'/api/work-handovers/files/'+file.id" target="_blank" rel="noopener noreferrer"><Paperclip :size="15" />{{file.name}}</a><small>{{Math.ceil(file.size/1024)}} KB</small><NButton v-if="editable" text :disabled="busy" :aria-label="'移除附件'+file.name" @click="removeFile(file.id)"><X :size="15" /></NButton></div>
            <small v-if="editable">单文件20MB，合计100MB；支持拖入文件或粘贴图片。</small>
          </section>
          <details class="handover-audit"><summary>交接记录</summary><p v-for="(row,index) in detail.audit||[]" :key="index"><span>{{timeText(row.time)}}</span> {{row.actor}} · {{row.action}}</p></details>
          <footer class="handover-footer">
            <span v-if="detail.status==='accepted'">{{detail.decision_by}}于 {{timeText(detail.decision_at)}} 接收 · 待处理 {{countItems('todo')}} 项</span><span v-else>{{dirty?'有未保存修改':'已保存'}}<template v-if="busy"> · 正在处理，请稍候</template></span>
            <div class="handover-tools"><template v-if="editable"><NButton :disabled="busy" :loading="busy" @click="save">保存草稿</NButton><NButton type="primary" :disabled="busy||!form.recipient_id||!form.items.length" @click="confirmAction='send'">发送交接</NButton></template><NButton v-if="detail.status==='pending'&&detail.is_owner" :disabled="busy" @click="confirmAction='withdraw'">撤回交接</NButton><template v-if="detail.status==='pending'&&detail.is_recipient"><NButton :disabled="busy" @click="confirmAction='return'">退回补充</NButton><NButton type="primary" :disabled="busy" @click="confirmAction='accept'">确认接收</NButton></template></div>
          </footer>
        </main>
        <div v-else-if="detailLoading" class="handover-detail" role="status"><NSpin size="small" /><span>正在读取交接单</span><NButton text @click="leaveDetail">取消读取</NButton></div>
        <NEmpty v-else class="handover-placeholder" description="选择交接单，或发起新的工作交接" />
      </div>
      <NModal :show="!!editingItem" preset="card" title="交接事项" class="handover-modal" :mask-closable="false" @update:show="value=>{if(!value)editingItem=null}">
        <div v-if="editingItem" class="handover-fields"><label class="handover-wide"><span>事项名称</span><NInput v-model:value="editingItem.title" :input-props="{'aria-label':'事项名称'}" :maxlength="240" /></label><label><span>事项分类</span><NSelect v-model:value="editingItem.kind" :options="kindOptions" /></label><label><span>楼栋</span><NSelect v-model:value="editingItem.scope" :disabled="!!editingItem.source_id" :options="scopeOptions" /></label><label class="handover-wide"><span>当前进度、下一步与注意事项</span><NInput v-model:value="editingItem.notes" :input-props="{'aria-label':'当前进度、下一步与注意事项'}" type="textarea" :autosize="{minRows:3,maxRows:6}" :maxlength="4000" /></label></div><template #footer><NButton type="primary" :disabled="!editingItem?.title?.trim()||!editingItem?.scope" @click="saveItem">加入清单</NButton></template>
      </NModal>
      <NModal v-model:show="sourceOpen" preset="card" title="选择现有业务事项" class="handover-modal handover-source-modal" :mask-closable="false">
        <div class="handover-source-filters"><label><span>业务</span><NSelect v-model:value="sourceModule" :options="modules" /></label><label><span>楼栋</span><NSelect v-model:value="sourceScope" :options="scopeOptions" /></label><label><span>月份</span><input v-model="sourcePeriod" type="month" /></label><NButton :loading="sourceLoading" :disabled="!sourceScope||!sourceModule" @click="loadSources">读取业务</NButton></div>
        <NAlert v-if="sourceError" type="warning" :bordered="false">{{sourceError}}</NAlert><NAlert v-if="sourceWarnings.length" type="info" :bordered="false">{{sourceWarnings.join('；')}}</NAlert>
        <div class="handover-source-search"><NInput v-model:value="sourceSearch" placeholder="搜索事项名称" clearable /><NCheckbox v-model:checked="mineOnly">有本人参与记录</NCheckbox></div>
        <div class="handover-source-list"><label v-for="row in sourceVisible" :key="row.id" class="handover-source-row"><NCheckbox :checked="sourceSelected.includes(row.id)" :disabled="form.items.some((i:Dict)=>i.source_id===row.id)" @update:checked="checked=>toggleSource(row.id,checked)" /><div><strong>{{row.title}}</strong><small>{{row.business_status}} · {{row.participated?'有本人参与记录':'需核对事项归属'}}</small></div></label><NEmpty v-if="!sourceLoading&&!sourceFiltered.length" description="暂无候选；可调整月份或添加手工事项" /></div>
        <NPagination v-if="sourceFiltered.length>20" v-model:page="sourcePage" :page-count="Math.ceil(sourceFiltered.length/20)" :page-slot="5" /><template #footer><NButton type="primary" :disabled="!sourceSelected.length||sourceLoading" @click="addSources">添加已选 {{sourceSelected.length}} 项</NButton></template>
      </NModal>
      <NModal :show="!!confirmAction" preset="dialog" :title="confirmLabels[confirmAction]||'请确认'" :mask-closable="false" positive-text="确认" negative-text="取消" :positive-button-props="{loading:busy,disabled:busy||confirmAction==='return'&&!returnReason.trim()}" :negative-button-props="{disabled:busy}" @positive-click="confirmed" @negative-click="confirmAction=''" @update:show="value=>{if(!value&&!busy)confirmAction=''}">
        <p>{{confirmAction==='send'?'将当前交接清单发送给所选接收人，只有对方本人确认后才生效。':confirmAction==='accept'?'确认已核对交接内容，接收后可逐项跟进办理。':confirmAction==='withdraw'?'撤回后对方不能再通过旧卡片确认，可修改后重新发送。':'请填写需要交接人补充的内容。'}}</p><NInput v-if="confirmAction==='return'" v-model:value="returnReason" type="textarea" :maxlength="1000" placeholder="需要补充的内容" />
      </NModal>
    </section>
  </NConfigProvider>
</template>

<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NConfigProvider,NEmpty,NInput,NModal,NPagination,NSelect,NSpin,NTag,dateZhCN,zhCN} from 'naive-ui';
import {ArrowLeft,ArrowRight,ExternalLink,ListPlus,Paperclip,Pencil,Plus,RefreshCw,RotateCcw,Trash2,Upload,X} from 'lucide-vue-next';
import {requestJson,requestBinaryJson,type Dict} from '../api/client';
import {navigate,registerNavigationGuard} from '../navigation';
import {randomHexId} from '../browserStorage';
import VnetBackButton from './VnetBackButton.vue';

const props=defineProps<{scope?:string;scopeOptions?:Array<{value:string;label:string}>;handoverId?:string}>();
const base='/api/work-handovers';
const views=[{value:'received',label:'我的接续工作'},{value:'pending',label:'待我接收'},{value:'sent',label:'我发出的交接'}];
const kindLabels:Record<string,string>={ongoing:'进行中',todo:'未完成',history:'历史完成',reference:'岗位资料'};
const kindOptions=Object.entries(kindLabels).map(([value,label])=>({value,label}));
const itemTabs=[{value:'todo',label:'待处理'},{value:'done',label:'已完成'},{value:'history',label:'历史资料'},{value:'removed',label:'已移除'}];
const confirmLabels:Record<string,string>={send:'发送工作交接',accept:'确认接收交接',return:'退回补充',withdraw:'撤回工作交接'};
const notificationLabels:Record<string,string>={invite:'接收邀请',result:'接收结果通知',refresh:'卡片状态更新'};
const view=ref('received'),page=ref(1),total=ref(0),list=ref<Dict[]>([]),detail=ref<Dict|null>(null);
const people=ref<Dict[]>([]),modules=ref<Array<{value:string;label:string}>>([]),scopes=ref<string[]>([]);
const loading=ref(false),detailLoading=ref(false),busy=ref(false),refreshing=ref(false),error=ref(''),success=ref('');
const form=ref<Dict>({title:'',recipient_id:null,notes:'',items:[]}),saved=ref('');
const editable=computed(()=>!!detail.value?.is_owner&&['draft','returned','withdrawn'].includes(detail.value.status));
const snapshot=()=>JSON.stringify(form.value);
const dirty=computed(()=>editable.value&&snapshot()!==saved.value);
const peopleOptions=computed(()=>people.value.map(p=>({value:p.record_id,label:p.name+(p.employee_no?' · '+p.employee_no:'')+' · '+(p.scopes||[]).map(scopeLabel).join('、')})));
const scopeOptions=computed(()=>scopes.value.map(value=>({value,label:scopeLabel(value)})));
const allItems=computed<Dict[]>(()=>editable.value?form.value.items:detail.value?.items||[]);
const itemFilter=ref('todo'),itemPage=ref(1),editingItem=ref<Dict|null>(null),confirmAction=ref(''),returnReason=ref('');
const fileInput=ref<HTMLInputElement>();
const sourceOpen=ref(false),sourceModule=ref(''),sourceScope=ref(''),sourcePeriod=ref(new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit'}).format(new Date()));
const sourceLoading=ref(false),sourceError=ref(''),sourceWarnings=ref<string[]>([]),sourceRows=ref<Dict[]>([]),sourceSelected=ref<string[]>([]),sourceSearch=ref(''),sourcePage=ref(1),mineOnly=ref(false);
const sourceFiltered=computed(()=>sourceRows.value.filter(r=>(!mineOnly.value||r.participated)&&r.title.toLowerCase().includes(sourceSearch.value.trim().toLowerCase())));
const sourceVisible=computed(()=>sourceFiltered.value.slice((sourcePage.value-1)*20,sourcePage.value*20));
function matches(item:Dict,filter:string){return filter==='removed'?!!item.removed:!item.removed&&(filter==='history'?['history','reference'].includes(item.kind):['ongoing','todo'].includes(item.kind)&&!!item.done===(filter==='done'));}
const filteredItems=computed(()=>allItems.value.filter(i=>matches(i,itemFilter.value)));
const visibleItems=computed(()=>filteredItems.value.slice((itemPage.value-1)*20,itemPage.value*20));
function countItems(filter:string){return allItems.value.filter(i=>matches(i,filter)).length;}
function scopeLabel(s:string){return s==='110'?'110站':s==='CAMPUS'?'园区':s+'楼';}
function statusLabel(s:string){return ({draft:'草稿',pending:'待接收',accepted:'已接收',returned:'退回补充',withdrawn:'已撤回'} as Record<string,string>)[s]||s;}
function timeText(value:string){return String(value||'').replace('T',' ').replace(/\+08:00$/,'');}
function id(){return randomHexId();}
function canCheck(item:Dict){return detail.value?.status==='accepted'&&detail.value.is_recipient&&!item.removed&&['ongoing','todo'].includes(item.kind);}
function apply(data:Dict){if(detail.value&&detail.value.id===data.id&&detail.value.version>data.version)return;detail.value=data;form.value={title:data.title,recipient_id:data.recipient_id||null,notes:data.notes,items:data.items.map((i:Dict)=>({...i}))};saved.value=snapshot();}
let disposed=false,readId=0,listReadId=0,createId='',readAbort:AbortController|undefined,sourceAbort:AbortController|undefined;
async function loadList(){const serial=++listReadId;loading.value=true;try{const data=await requestJson(base+'?'+new URLSearchParams({view:view.value,page:String(page.value)}),{cache:'no-store'});if(!disposed&&serial===listReadId){list.value=data.items;total.value=data.total;}}catch(e:any){if(!disposed&&serial===listReadId)error.value=e.message;}finally{if(serial===listReadId)loading.value=false;}}
async function directory(){const data=await requestJson(base+'/people',{cache:'no-store'});if(disposed)return;people.value=data.items;modules.value=data.modules;scopes.value=data.scopes;sourceScope.value=scopes.value.includes(props.scope||'')?props.scope!:scopes.value[0]||'';sourceModule.value=modules.value[0]?.value||'';}
function leaveAllowed(){return !busy.value&&(!dirty.value||window.confirm('有未保存的交接内容，确认放弃本次修改？'));}
function cancelDetailRead(){readId++;readAbort?.abort();readAbort=undefined;detailLoading.value=false;}
async function select(identity:string){if(!leaveAllowed())return;cancelDetailRead();const serial=readId;readAbort=new AbortController();detail.value=null;detailLoading.value=true;error.value='';success.value='';try{const data=await requestJson(base+'/'+identity,{signal:readAbort.signal,cache:'no-store'});if(disposed||serial!==readId)return;apply(data);itemFilter.value='todo';itemPage.value=1;if(data.status==='accepted')void refreshSources();}catch(e:any){if(serial===readId&&e.name!=='AbortError')error.value=e.message;}finally{if(serial===readId)detailLoading.value=false;}}
function leaveDetail(){if(leaveAllowed()){cancelDetailRead();detail.value=null;}}
async function changeView(value:string){if(!leaveAllowed())return;cancelDetailRead();view.value=value;page.value=1;detail.value=null;await loadList();}
async function changePage(value:number){if(busy.value)return;page.value=value;await loadList();}
async function reload(){if(!leaveAllowed())return;try{await directory();await loadList();if(detail.value)await select(detail.value.id);}catch(e:any){error.value=e.message;}}
async function newDraft(){if(!leaveAllowed())return;cancelDetailRead();busy.value=true;error.value='';createId ||= id();try{const data=await requestJson(base,{method:'POST',body:JSON.stringify({id:createId})});createId='';apply(data);view.value='sent';page.value=1;itemFilter.value='todo';await loadList();}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
async function persist(){if(!detail.value)return;const items=form.value.items.map((i:Dict)=>Object.fromEntries(['id','title','notes','scope','kind','source_id'].map(k=>[k,i[k]||''])));const data=await requestJson(base+'/'+detail.value.id,{method:'PUT',body:JSON.stringify({version:detail.value.version,data:{...form.value,items,recipient_id:form.value.recipient_id||''}})});apply(data);}
async function save(){if(busy.value)return;busy.value=true;error.value='';success.value='';try{await persist();success.value='交接草稿已保存';await loadList();}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
async function act(action:string,itemId=''){if(!detail.value||busy.value)return;busy.value=true;error.value='';success.value='';try{if(action==='send'&&dirty.value)await persist();const data=await requestJson(base+'/'+detail.value.id+'/actions',{method:'POST',body:JSON.stringify({version:detail.value.version,action,item_id:itemId,reason:action==='return'?returnReason.value:''})});apply(data);success.value=({send:'已提交交接，等待对方本人接收',accept:'已接收，事项已加入接续清单',return:'已退回补充',withdraw:'交接已撤回',complete:'已确认清单事项完成',reopen:'已恢复为未完成',remove:'已从接续清单移除，原业务记录保留',restore:'事项已恢复','retry-notification':'已安排重试通知'} as Record<string,string>)[action];confirmAction.value='';returnReason.value='';await loadList();}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
async function confirmed(){await act(confirmAction.value);return false;}
function editItem(item?:Dict){editingItem.value=item?{...item}:{id:id(),title:'',notes:'',scope:sourceScope.value,kind:'todo',source_id:''};}
function saveItem(){if(!editingItem.value)return;const item={...editingItem.value};const index=form.value.items.findIndex((i:Dict)=>i.id===item.id);if(index<0)form.value.items.push(item);else form.value.items[index]=item;itemFilter.value=['history','reference'].includes(item.kind)?'history':'todo';editingItem.value=null;}
function removeItem(item:Dict){if(item.removed){void act('restore',item.id);return;}if(!window.confirm('仅从交接清单移除此事项，原业务记录不会删除。确认移除？'))return;if(editable.value)form.value.items=form.value.items.filter((i:Dict)=>i.id!==item.id);else void act('remove',item.id);}
function openBusiness(url:string){navigate(url);}
async function loadSources(){sourceAbort?.abort();const controller=new AbortController();sourceAbort=controller;sourceLoading.value=true;sourceError.value='';sourceRows.value=[];sourceSelected.value=[];sourceWarnings.value=[];try{const data=await requestJson(base+'/sources?'+new URLSearchParams({module:sourceModule.value,scope:sourceScope.value,period:sourcePeriod.value}),{signal:controller.signal,timeoutMs:90000,cache:'no-store'});if(!disposed&&!controller.signal.aborted){sourceRows.value=data.items;sourceWarnings.value=data.warnings;sourcePage.value=1;}}catch(e:any){if(!controller.signal.aborted)sourceError.value=e.message;}finally{if(sourceAbort===controller)sourceLoading.value=false;}}
function toggleSource(identity:string,checked:boolean){sourceSelected.value=checked?[...sourceSelected.value,identity]:sourceSelected.value.filter(v=>v!==identity);}
function addSources(){for(const row of sourceRows.value.filter(r=>sourceSelected.value.includes(r.id))){if(!form.value.items.some((i:Dict)=>i.source_id===row.id))form.value.items.push({...row,id:id(),notes:'',source_id:row.id});}sourceOpen.value=false;sourceSelected.value=[];}
async function refreshSources(){if(!detail.value||refreshing.value||busy.value||dirty.value)return;const identity=detail.value.id;refreshing.value=true;try{const data=await requestJson(base+'/'+identity+'/refresh',{method:'POST',body:'{}',timeoutMs:90000});if(!disposed&&detail.value?.id===identity&&!dirty.value)apply(data);}catch(e:any){if(!disposed&&detail.value?.id===identity)error.value=e.message;}finally{refreshing.value=false;}}
async function uploadFiles(files:File[]){if(!editable.value||busy.value||!files.length)return;if(files.some(f=>!f.size||f.size>20*1024*1024)){error.value='请选择20MB以内的非空文件。';return;}busy.value=true;error.value='';try{if(dirty.value)await persist();for(const file of files){const data=await requestBinaryJson(base+'/'+detail.value!.id+'/files?'+new URLSearchParams({name:file.name}),file,{headers:{'Content-Type':'application/octet-stream'}});apply(data);}success.value='交接资料已保存';}catch(e:any){error.value=e.message;}finally{busy.value=false;if(fileInput.value)fileInput.value.value='';}}
function pickFiles(e:Event){void uploadFiles(Array.from((e.target as HTMLInputElement).files||[]));}
function pasteFiles(e:ClipboardEvent){const files=Array.from(e.clipboardData?.files||[]);if(files.length&&editable.value){e.preventDefault();void uploadFiles(files);}}
function dropFiles(e:DragEvent){void uploadFiles(Array.from(e.dataTransfer?.files||[]));}
async function removeFile(identity:string){if(!editable.value||busy.value)return;busy.value=true;try{if(dirty.value)await persist();apply(await requestJson(base+'/files/'+identity,{method:'DELETE',body:JSON.stringify({version:detail.value!.version})}));}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
watch([itemFilter,()=>detail.value?.id],()=>itemPage.value=1);
watch([sourceSearch,mineOnly],()=>sourcePage.value=1);
watch([sourceModule,sourceScope,sourcePeriod],()=>{sourceAbort?.abort();sourceLoading.value=false;sourceRows.value=[];sourceSelected.value=[];sourceWarnings.value=[];});
function unload(e:BeforeUnloadEvent){if(dirty.value||busy.value){e.preventDefault();e.returnValue='';}}
const unregister=registerNavigationGuard(()=>leaveAllowed());
onMounted(async()=>{window.addEventListener('beforeunload',unload);try{await directory();await loadList();const identity=props.handoverId||new URLSearchParams(window.location.search).get('id');if(identity)await select(identity);}catch(e:any){error.value=e.message;}});
onBeforeUnmount(()=>{disposed=true;cancelDetailRead();sourceAbort?.abort();unregister();window.removeEventListener('beforeunload',unload);});
</script>

<style scoped>
.work-handover{--handover-line:var(--v2-border,#d9e2ef);--handover-ink:var(--v2-ink,#23344e);color:var(--handover-ink);max-width:1600px;margin:auto;padding:16px 22px;min-width:0;font-size:14px}.handover-toolbar,.handover-tools,.handover-detail-head,.handover-section-head,.handover-item-toolbar,.handover-footer{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.handover-toolbar,.handover-section-head,.handover-item-toolbar,.handover-footer{justify-content:space-between}.handover-toolbar{padding-bottom:12px;border-bottom:1px solid var(--handover-line)}.handover-tabs{display:flex;gap:4px;flex-wrap:wrap}.handover-tabs button{border:0;border-radius:6px;padding:8px 12px;background:transparent;color:inherit;font:inherit;cursor:pointer;min-height:36px;transition:background 160ms,color 160ms}.handover-tabs button[aria-selected=true]{background:#eaf2ff;color:#145fbd}.handover-tabs small{font-variant-numeric:tabular-nums}.handover-layout{display:grid;grid-template-columns:280px minmax(0,1fr);gap:20px;min-width:0;padding-top:16px}.handover-list{border-right:1px solid var(--handover-line);padding-right:14px;min-width:0}.handover-list-row{display:block;width:100%;padding:12px 10px;border:0;border-bottom:1px solid var(--handover-line);border-radius:0;background:transparent;color:inherit;text-align:left;cursor:pointer;transition:background 160ms}.handover-list-row.selected{background:#eaf2ff;box-shadow:inset 3px 0 #2563eb}.handover-list-row>div{display:flex;justify-content:space-between;gap:8px}.handover-list-row strong{font-size:14px;overflow-wrap:anywhere}.handover-list-row p{margin:6px 0;font-size:13px}.handover-status{flex:none;font-size:12px;color:var(--v2-muted,#637389)}.handover-list-row small,.handover-detail-head span,.handover-detail-head small,.handover-item-meta,.handover-file small,.handover-files>small{font-size:12px;color:var(--v2-muted,#637389)}.handover-detail{min-width:0;display:flex;flex-direction:column;gap:14px}.handover-detail h2{margin:0;font-size:19px}.handover-detail-head small{margin-left:auto}.handover-list-back{display:none}.handover-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.handover-fields label,.handover-source-filters label{display:grid;gap:6px;min-width:0}.handover-fields label>span,.handover-source-filters label>span{font-size:12px;color:var(--v2-muted,#637389)}.handover-wide{grid-column:1/-1}.handover-people{display:flex;align-items:center;gap:10px;margin:0;font-size:13px}.handover-note{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.65;margin:5px 0;font-size:13px}.handover-item-toolbar{padding-block:6px;border-bottom:1px solid var(--handover-line)}.handover-items{display:grid;gap:0}.handover-item{display:flex;align-items:flex-start;gap:10px;padding:14px 2px;border-bottom:1px solid var(--handover-line)}.handover-item.done{background:#f5faf7}.handover-item.removed{opacity:.7}.handover-item-body{flex:1;min-width:0}.handover-item-title{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.handover-item-title strong{font-weight:600;overflow-wrap:anywhere}.handover-item-meta{display:flex;gap:12px;margin-top:5px;flex-wrap:wrap}.handover-item-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.handover-confirmed{color:#23734a}.handover-files{border-top:1px solid var(--handover-line);padding-top:12px}.handover-section-head{margin-bottom:8px}.handover-file{display:flex;align-items:center;gap:10px;padding:7px 0;min-width:0}.handover-file a{display:flex;gap:6px;align-items:center;overflow-wrap:anywhere;min-width:0;flex:1}.handover-file svg{flex:none}.handover-file-input{display:none}.handover-audit{font-size:12px;color:var(--v2-muted,#637389)}.handover-audit summary{cursor:pointer;min-height:32px}.handover-audit p{margin:6px 0}.handover-audit span{font-variant-numeric:tabular-nums}.handover-footer{position:sticky;bottom:0;background:var(--cf-surface,#fff);border-top:1px solid var(--handover-line);padding:12px 0;z-index:3}.handover-footer>span{font-size:12px;color:var(--v2-muted,#637389)}.handover-message{margin-block:12px}.handover-placeholder{align-self:center;padding:60px 16px}.handover-source-filters{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr) minmax(0,1fr) auto;gap:10px;align-items:end}.handover-source-filters input{min-width:0;width:100%;height:34px;border:1px solid var(--handover-line);border-radius:6px;padding:4px 8px;font:inherit;background:var(--cf-surface,#fff);color:inherit}.handover-source-search{display:flex;gap:12px;align-items:center;margin-block:14px}.handover-source-search :deep(.n-checkbox){flex:none}.handover-source-list{max-height:48dvh;overflow:auto}.handover-source-row{display:flex;gap:10px;padding:10px 0;border-bottom:1px solid var(--handover-line);cursor:pointer}.handover-source-row strong{font-size:13px;overflow-wrap:anywhere}.handover-source-row small{display:block;margin-top:5px;color:var(--v2-muted,#637389)}.work-handover :deep(.n-button__content){gap:5px}.work-handover :focus-visible{outline:2px solid #2563eb;outline-offset:2px}
:global(.handover-modal){width:min(700px,calc(100vw - 24px))!important;max-height:calc(100dvh - 32px);overflow:auto;border-radius:8px!important}:global(.handover-source-modal){width:min(900px,calc(100vw - 24px))!important}:global(.workspace-v2 .work-handover){padding:0 0 16px}
@media(max-width:900px){.handover-layout{grid-template-columns:230px minmax(0,1fr)}.handover-item{flex-wrap:wrap}.handover-item-actions{width:100%}}
@media(max-width:760px){.work-handover{padding:10px}.handover-layout{display:block;padding-top:10px}.handover-layout.has-detail>.handover-list{display:none}.handover-list{padding:0;border:0}.handover-list-back{display:inline-flex}.handover-placeholder{display:none}.handover-fields{grid-template-columns:minmax(0,1fr);gap:10px}.handover-source-filters{grid-template-columns:repeat(2,minmax(0,1fr))}.handover-source-search{flex-wrap:wrap}.handover-toolbar{gap:8px}.handover-tabs button{padding-inline:10px;min-height:44px}.handover-footer{bottom:0;padding:8px 0}.handover-footer>.handover-tools{margin-left:auto}.work-handover :deep(.n-button){min-height:40px}.handover-item-actions :deep(.n-button){min-height:44px}.handover-footer :deep(.n-button){min-height:44px}.handover-item{padding-block:12px}.handover-detail-head{gap:8px}.handover-files>small{display:block;line-height:1.6}:global(.workspace-v2 .handover-footer){bottom:calc(62px + env(safe-area-inset-bottom))}}
@media(prefers-reduced-motion:reduce){.handover-tabs button,.handover-list-row{transition:none}}
@media(max-width:760px){.handover-item-toolbar>.handover-tabs{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));width:100%;gap:2px}.handover-item-toolbar .handover-tabs button{font-size:12px;padding:6px 3px}.handover-detail-head>small{display:none}.handover-item-toolbar:has(.handover-refresh){position:relative;padding-right:42px}.handover-item-toolbar:has(.handover-refresh)>.handover-tools{position:absolute;right:0;top:7px}.handover-refresh span{display:none}.handover-refresh{width:36px;min-width:36px;padding:0}.handover-item-title{gap:5px}}
</style>
