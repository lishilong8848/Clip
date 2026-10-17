<template>
  <section class="v2-full-module" :data-module-widget="item.module" @click="followLink">
    <div class="v2-module-toolbar" data-v2-navigation><NButton v-if="backRoute" size="small" quaternary @click="back"><ArrowLeft :size="14" />返回</NButton><span>{{scopeLabel}}</span><NButton size="small" text @click="openPage"><Maximize2 :size="14" />{{initialUrl?'在原模块办理':'独立页面'}}</NButton></div>
    <div ref="filterTarget" class="v2-module-filters" data-v2-filter role="region" :aria-label="item.title+'筛选'" />
    <NAlert v-if="error" type="warning" :bordered="false" closable @close="error=''">{{error}}</NAlert>
    <NResult v-if="!permitted" status="403" title="没有该模块权限" />
    <NResult v-else-if="unsupported" status="info" title="当前楼栋尚未接入此业务" />
    <NoticeWorkspace v-else-if="moduleId==='notices'" :url="route.pathname+route.search" :active="true" :scope="localScope" :scopes="scopes" :user="user" />
    <NoticeSopManager v-else-if="moduleId==='sops'" :initial-scope="localScope" :initial-type="''" :scopes="scopes.filter(scope=>['110',...'ABCDEH'].includes(scope.value))" :types="modules.filter(module=>SOP_IDS.includes(module.id)&&(user.modules||[]).includes(module.id)).map(module=>({value:module.id,label:module.label}))" :can-delete="user.can_delete_business===true" />
    <NSpin v-else-if="moduleId==='handover'" :show="handoverLoading"><div class="v2-feature-grid"><NButton v-for="(url,building) in handovers" :key="building" tag="a" :href="String(url)" target="_blank" rel="noopener noreferrer">{{building}}楼审核</NButton></div><NEmpty v-if="!handoverLoading&&!Object.keys(handovers).length" description="暂无可用审核入口" /></NSpin>
    <AccountSignature v-else-if="pageName==='signatures'&&route.searchParams.get('self')==='1'" />
    <component v-else-if="currentPage" :is="currentPage" :key="pageKey" v-bind="pageProps" @switch-scope="switchScope" @status="status" @close="back" @select-tab="changeTab" @changed="changed" />
    <NEmpty v-else description="当前页面不可用，可使用独立页面查看" />
  </section>
</template>
<script setup lang="ts">
import { computed,defineAsyncComponent,provide,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NResult,NSpin } from 'naive-ui';
import { ArrowLeft,Maximize2 } from 'lucide-vue-next';
import AsyncPageState from '../components/AsyncPageState.vue';
import {requestJson,type Dict} from './client';
import type { Widget } from './dashboard/layout';
import { modules,navigationModules,NOTICE_IDS,SOP_IDS,moduleForRoute } from './modules';
import { navigate,requestEmbeddedChange } from './navigation';
import { embeddedContextKey } from './embeddedContext';
import { pageContext } from './pageLayout';
const props=defineProps<{item:Widget;user:Dict;scope:string;scopes:{value:string;label:string}[];initialUrl?:string}>();
const lazy=(loader:()=>Promise<any>)=>defineAsyncComponent({loader,loadingComponent:AsyncPageState,errorComponent:AsyncPageState,delay:80,timeout:30000});
const NoticeWorkspace=lazy(()=>import('./NoticeWorkspace.vue')),NoticeSopManager=lazy(()=>import('./NoticeSopManager.vue'));
const AccountSignature=lazy(()=>import('../components/AccountSignature.vue'));
const pages:Record<string,any>={event:lazy(()=>import('../components/EventManagementPage.vue')),repair_management:lazy(()=>import('./RepairWorkspace.vue')),water:lazy(()=>import('./WaterWorkspace.vue')),cabinet:lazy(()=>import('../components/CabinetPowerPage.vue')),cabinet_batch:lazy(()=>import('../components/CabinetPowerBatchPage.vue')),engineer_mop:lazy(()=>import('./MopWorkspace.vue')),critical_guard:lazy(()=>import('../components/CriticalGuardPage.vue')),drills:lazy(()=>import('../components/DrillManagementPage.vue')),daily:lazy(()=>import('../components/DailyTaskChecklistPage.vue')),learning:lazy(()=>import('../components/LearningPage.vue')),knowledge:lazy(()=>import('../components/KnowledgeBasePage.vue')),links:lazy(()=>import('../components/LinkDirectoryPage.vue')),signatures:lazy(()=>import('../components/SignatureManagementPage.vue')),plan_convergence:lazy(()=>import('../components/PlanConvergencePage.vue')),life_guide:lazy(()=>import('../components/LifeGuidePage.vue')),accounts:lazy(()=>import('../components/AdminTools.vue'))};
pages.signature=lazy(()=>import('../components/SignaturePage.vue'));pages.history=lazy(()=>import('../components/HistoryMemoryPage.vue'));
pages.work_handover=lazy(()=>import('../components/WorkHandoverPage.vue'));
pages.risk_inspection=lazy(()=>import('../components/RiskInspectionPage.vue'));
function initial(){const entry=navigationModules(props.user.modules||[]).find(module=>module.id===props.item.module);const url=new URL(entry?.path||'/',location.origin);url.searchParams.set('scope',props.scope);if(props.item.module==='water')url.searchParams.set('water_view','records');if(props.initialUrl){const target=new URL(props.initialUrl,location.origin);const kind=moduleForRoute(target);if(target.origin===location.origin&&(kind===props.item.module||props.item.module==='notices'&&(NOTICE_IDS.includes(kind)||kind==='all'))&&(!target.searchParams.get('scope')||props.scopes.some(s=>s.value===target.searchParams.get('scope'))))return target;}return url;}
const route=ref(initial()),history=ref<URL[]>([]),filterTarget=ref<HTMLElement|null>(null),error=ref('');
const moduleId=computed(()=>{const id=moduleForRoute(route.value);return NOTICE_IDS.includes(id)||id==='all'?'notices':id;});
const permitted=computed(()=>navigationModules(props.user.modules||[]).some(module=>module.id===props.item.module));
const localScope=computed(()=>{const scope=route.value.searchParams.get('scope')||props.scope;return props.scopes.some(item=>item.value===scope)?scope:props.scope;});
const scopeLabel=computed(()=>props.scopes.find(scope=>scope.value===localScope.value)?.label||localScope.value);
const adminMode=computed(()=>props.user.role==='admin'&&['drills','critical_guard'].includes(moduleId.value)&&(route.value.searchParams.get('mode')==='admin'||['ALL','CAMPUS'].includes(localScope.value)));
const unsupported=computed(()=>['drills','critical_guard','cabinet'].includes(moduleId.value)&&['H','110'].includes(localScope.value)&&!adminMode.value);
const pageName=computed(()=>route.value.pathname==='/cabinet-power/batches'?'cabinet_batch':route.value.pathname==='/signature'?'signature':route.value.pathname==='/admin/history-memory'?'history':moduleId.value);
const currentPage=computed(()=>pages[pageName.value]);
const pageKey=computed(()=>pageName.value+':'+(pageName.value==='learning'?route.value.search:localScope.value)+':'+(pageName.value==='cabinet_batch'?route.value.search:'')+':'+adminMode.value);
const backRoute=computed(()=>history.value.length>0);
function go(target:string|URL,options:{replace?:boolean;hard?:boolean;legacy?:boolean}={}){
  const next=new URL(target,route.value);if(next.origin!==location.origin||options.hard||options.legacy)return false;
  let id=moduleForRoute(next);if(NOTICE_IDS.includes(id)||id==='all')id='notices';
  if(id!==props.item.module)return false;
  const scope=next.searchParams.get('scope');if(scope&&!props.scopes.some(item=>item.value===scope)){error.value='没有该楼栋权限。';return true;}
  requestEmbeddedChange(props.item.id,()=>{if(!options.replace)history.value.push(route.value);route.value=next;error.value='';});return true;
}
function back(){const previous=history.value[history.value.length-1];if(previous)requestEmbeddedChange(props.item.id,()=>{history.value.pop();route.value=previous;});}
function switchScope(scope:string){const next=new URL(route.value);next.searchParams.set('scope',scope);go(next);}
function changeTab(tab:string){const next=new URL(route.value);next.searchParams.set('tab',tab);go(next,{replace:true});}
function openPage(){navigate(pageName.value==='signatures'&&route.value.searchParams.get('self')==='1'?'/?view=my':route.value.pathname+route.value.search+route.value.hash);}
function followLink(event:MouseEvent){const link=(event.target as Element)?.closest?.('a[href]') as HTMLAnchorElement|null;if(!link||event.defaultPrevented||link.hasAttribute('download')||link.target==='_blank'||event.ctrlKey||event.metaKey||event.shiftKey||event.button!==0)return;if(go(link.href))event.preventDefault();}
function status(text:string){if(/失败|异常|超时|无法/.test(text))error.value=text;}
function changed(){if(props.item.module==='accounts')window.dispatchEvent(new Event('auth-account-changed'));}
provide(embeddedContextKey,{id:props.item.id,route,filterTarget,go,back});
provide(pageContext,{user:computed(()=>props.user),scopes:computed(()=>props.scopes.map(item=>item.value)),scope:localScope,module:moduleId});
const pageProps=computed(()=>{const p=route.value.searchParams,admin=props.user.role==='admin',common={scope:localScope.value,scopeOptions:props.scopes,isAdmin:admin,checking:false,loggedIn:true,loginUrl:'/api/auth/login'};switch(pageName.value){case 'water':return {...common,initialView:p.get('water_view')==='overview'?'overview':'records'};case 'risk_inspection':return {...common,releaseId:p.get('id')||''};case 'repair_management':return {...common,userId:props.user.open_id,canDelete:props.user.can_delete_business===true,focusRecordId:p.get('record_id')||''};case 'learning':return {...common,scope:p.get('scope')||'',personId:p.get('person_id')||'',userId:props.user.open_id};case 'critical_guard':return {...common,scope:adminMode.value?'':localScope.value,user:props.user,adminMode:adminMode.value};case 'drills':return {...common,scope:adminMode.value?'':localScope.value,currentUser:props.user,adminMode:adminMode.value,printMode:false};case 'cabinet':return {...common,scope:['ALL','CAMPUS'].includes(localScope.value)?'':localScope.value,userId:props.user.open_id,readOnly:false,canDelete:props.user.can_delete_business===true};case 'cabinet_batch':return {...common,userId:props.user.open_id};case 'accounts':return {open:true,systemAdmin:admin,initialTab:p.get('tab')||'permissions',scopeOptions:props.scopes};case 'event':return {...common,refreshNonce:0};case 'daily':return {...common,printMode:false};default:return common;}});
watch([()=>props.item.module,()=>props.scope],()=>{route.value=initial();history.value=[];error.value='';});
const handovers=ref<Dict>({}),handoverLoading=ref(false);
watch(moduleId,async (id,_previous,onCleanup)=>{handovers.value={};if(id!=='handover'||!permitted.value)return;const controller=new AbortController();onCleanup(()=>controller.abort());handoverLoading.value=true;try{const data=await requestJson('/api/handover-links',{signal:controller.signal});if(!controller.signal.aborted)handovers.value=data.links||{};}catch(e){if(!controller.signal.aborted)error.value=e instanceof Error?e.message:'审核入口读取失败';}finally{if(!controller.signal.aborted)handoverLoading.value=false;}},{immediate:true});
</script>
