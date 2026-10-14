<template>
  <NConfigProvider :theme-overrides="theme" :locale="zhCN" :date-locale="dateZhCN">
    <NMessageProvider placement="bottom-right" :duration="0" closable><NDialogProvider>
      <div class="v2-shell" :class="{ 'is-collapsed': collapsed, 'is-print': printMode, 'is-public': publicLink, 'is-guest': user.role === 'guest', 'is-auth':!loggedIn&&!publicLink }" @dragstart.capture="preventSignatureDrag">
        <div v-if="loggedIn && !printMode && activeModule !== 'life_guide' && route.pathname !== '/workbench-lite'" class="v2-watermark" aria-hidden="true"><span v-for="n in 24" :key="n">内部资料 · {{ user.name }} · {{ watermarkDay }}</span></div>
        <header v-if="!printMode" class="v2-header">
          <NButton v-if="loggedIn && !publicLink && user.role !== 'guest'" data-v2-navigation quaternary circle aria-label="展开或收起导航" :aria-expanded="!collapsed" @click="collapsed = !collapsed"><Menu :size="21" /></NButton>
          <div id="page-back-slot" />
          <a :href="workspaceUrl(user.role === 'guest' ? '/cabinet-power' : '/')" class="v2-brand"><img :src="'/assets/vnet-logo.png'" alt="世纪互联 VNET" /><span>灯塔<small>南通运维工作空间</small></span></a>
          <div class="v2-header-actions">
            <NButton v-if="loggedIn || !checking && !publicLink" data-v2-navigation size="small" secondary :disabled="versionBusy" :title="versionBusy?'正在提交，请等待结果':undefined" :style="!loggedIn ? {minHeight:'44px'} : undefined" @click="requestVersion(switchLegacy)">切换旧版</NButton>
            <NButton v-if="loggedIn && user.role !== 'guest'" data-v2-navigation quaternary class="v2-person" aria-label="打开个人页面" title="个人信息与设置" @click="navigate('/?view=my')"><UserRound :size="17" /><span>{{ user.name }}</span></NButton>
            <NButton v-else-if="loggedIn" quaternary aria-label="退出登录" @click="logout"><LogOut :size="17" /></NButton>
          </div>
        </header>
        <div v-if="printMode" id="page-back-slot" hidden />
        <SidebarNavigation v-if="loggedIn && !publicLink && !printMode && user.role !== 'guest'" v-model:collapsed="collapsed" :options="menuOptions" :selected="selectedMenu" @select="selectMenu" />
        <main class="v2-main">
          <div v-if="loggedIn && !publicLink && !printMode && route.pathname !== '/workbench-lite' && scopes.length > 1 && scopeSelectable && !['my','permission'].includes(view)" class="v2-context">
            <span>业务范围</span>
            <NSelect class="v2-scope-select" :value="currentScope" :options="businessScopes" size="small" aria-label="当前楼栋" @update:value="changeScope" />
          </div>
          <NAlert v-if="authError" type="error" :bordered="false" class="v2-alert">{{ authError }} <NButton text type="primary" @click="loadAuth">重新读取</NButton></NAlert>
          <NAlert v-if="operationError" type="error" :bordered="false" closable role="alert" class="v2-alert" @close="operationError=''">{{ operationError }}</NAlert>
          <div v-if="checking" class="v2-loading"><NSpin size="medium" /><span>正在检查登录状态</span></div>
          <NResult v-else-if="denied" status="403" title="没有该功能权限" description="当前账号未获授此功能，请申请权限或联系管理员。"><template #footer><NButton @click="navigate('/')">返回工作台</NButton><NButton type="primary" @click="navigate('/?view=permission')">申请权限</NButton></template></NResult>
          <NResult v-else-if="loggedIn && unsupportedScope" status="info" :title="scopeLabel + '尚未接入此业务'" description="原业务目前仅支持 A–E 楼，未使用其他楼栋的数据代替。"><template #footer><NButton @click="navigate('/')">返回工作台</NButton></template></NResult>
          <AuthPanels v-else-if="!publicLink && (!loggedIn || passwordPage || permissionPage || !scopes.length && !['learning','knowledge'].includes(activeModule))"
            :checking="false" :logged-in="loggedIn" :user="user" :login-url="loginUrl" :busy="permissionBusy" :request="permission" :requestable-scopes="requestableScopes" :password-settings="passwordPage" :show-back="loggedIn" :title="permissionPage?'申请楼栋权限':''" @back="navigate('/')" @update-request="Object.assign(permission, $event)" @submit="submitPermission" @confirm="submitPermission" />
          <template v-else>
            <nav v-if="showDutyTabs" class="v2-duty-tabs" aria-label="值班常用业务">
              <NButton v-for="tab in dutyTabs" :key="tab.id" :type="dutyActive === tab.id ? 'primary' : 'default'" :secondary="dutyActive !== tab.id" @click="navigate(tab.url)"><component :is="tab.icon" :size="18" />{{ tab.label }}</NButton>
            </nav>
            <HomePage v-if="homeViews.includes(view) && route.pathname === '/' && !route.searchParams.get('mode')" :key="identityKey" :user="user" :scopes="scopes" :scope="currentScope" :view="view" :initial-kind="route.searchParams.get('kind')||undefined" @open="navigate($event)" />
            <section v-else-if="view === 'my' && route.pathname === '/'" class="v2-my">
              <header class="v2-profile-heading"><UserRound :size="26" /><div><h1>{{ user.name }}</h1><p>个人信息与设置</p></div></header>
              <NDescriptions bordered size="small" label-placement="left" :label-style="{width:'7em',whiteSpace:'nowrap'}" :column="1"><NDescriptionsItem label="角色">{{ levelLabel }} · {{ user.category_label || categoryLabel }}</NDescriptionsItem><NDescriptionsItem label="可访问楼栋">{{ scopes.map(s => s.label).join('、') || '暂无' }}</NDescriptionsItem><NDescriptionsItem label="登录方式">{{ user.login_method === 'password' ? '姓名密码登录' : '飞书登录' }}</NDescriptionsItem></NDescriptions>
              <div class="v2-profile-actions"><NButton v-if="granted.includes('accounts')" data-v2-navigation secondary type="primary" @click="navigate('/admin/settings')"><Settings :size="17" />账号与设置</NButton><NButton v-if="user.login_method === 'password'" data-v2-navigation @click="navigate('/?account=password')">修改密码</NButton><NButton data-v2-navigation @click="navigate('/?view=permission')">申请权限</NButton><NButton type="error" secondary :loading="logoutBusy" :disabled="logoutBusy" @click="logout"><LogOut :size="17" />退出登录</NButton></div>
              <section class="v2-profile-access" aria-label="功能权限"><h2>功能权限</h2><div class="v2-profile-modules"><NTag v-for="item in modules.filter(m=>granted.includes(m.id))" :key="item.id" size="small" :bordered="false">{{ item.label }}</NTag><span v-if="!granted.length">暂无已授权功能</span></div></section>
            </section>
            <section v-else-if="view === 'handover' && route.pathname === '/'" class="v2-links"><h2>交接班审核</h2><NSpin :show="handoverLoading"><div class="v2-feature-grid"><NButton v-for="(url, scope) in handovers" :key="scope" tag="a" :href="String(url)" target="_blank" rel="noopener noreferrer">{{ scope }}楼审核</NButton></div><NEmpty v-if="!handoverLoading && !Object.keys(handovers).length" description="暂无可用审核入口" /></NSpin></section>
            <section v-else-if="view === 'assistant' && route.pathname === '/'" class="v2-links"><h2>灯塔助手</h2><NButton type="primary" @click="openAssistant">打开会话</NButton></section>
            <NoticeSopManager v-else-if="activeModule==='sops'" :key="identityKey" :initial-scope="currentScope" :initial-type="route.searchParams.get('work_type')||''" :scopes="scopes.filter(s=>['110',...'ABCDEH'].includes(s.value))" :types="modules.filter(m=>SOP_IDS.includes(m.id)&&granted.includes(m.id)).map(m=>({value:m.id,label:m.label}))" :can-delete="user.can_delete_business===true" @close="returnFromSop" />
            <WorkOrderWorkspace v-else-if="route.pathname.startsWith('/polling-work-order')" :key="route.pathname+route.search" :url="route.pathname + route.search + route.hash" />
            <template v-else-if="route.pathname === '/workbench-lite'" />
            <OverviewPanel v-else-if="activeModule === 'water' && ['ALL','CAMPUS'].includes(currentScope)" kind="water" :scope="currentScope" :scopes="scopes" :user="user" expanded @open="navigate($event)" />
            <component v-else-if="activeComponent" :is="activeComponent" :key="componentKey" v-bind="componentProps" @switch-scope="changeScope" @status="onStatus" @close="navigate('/')" @select-tab="selectAdminTab" @changed="loadAuth" />
            <NResult v-else status="404" title="没有找到此页面"><template #footer><NButton @click="navigate('/')">返回工作台</NButton></template></NResult>
            <NoticeWorkspace v-if="noticeView" v-show="noticeActive" :key="noticeView.identity" :url="noticeView.url" :active="noticeActive" :scope="currentScope" :scopes="scopes" :user="user" />
          </template>
        </main>
        <MobileNavigation v-if="loggedIn && !publicLink && !printMode && user.role !== 'guest'" :tabs="mobileTabs" :active="route.pathname === '/' && !route.searchParams.get('mode') ? view : 'functions'" @select="value=>navigate('/?view='+value)" @direction="animateMobilePage" />
        <LighthouseAssistant v-if="loggedIn && user.role !== 'guest' && granted.includes('assistant') && !printMode && !publicLink" :key="String(user.open_id)" :user-id="String(user.open_id)" :user-name="String(user.name || '')" />
        <NModal v-model:show="confirmVersion" preset="dialog" title="切换后需要重新填写" content="当前填写内容尚未提交，切换版本不会保留密码或身份信息。" positive-text="放弃填写并切换" negative-text="继续填写" @positive-click="finishVersion" />
      </div>
    </NDialogProvider></NMessageProvider>
  </NConfigProvider>
</template>
<script setup lang="ts">
import { computed, defineAsyncComponent, h, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { NAlert, NButton, NConfigProvider, NDescriptions, NDescriptionsItem, NDialogProvider, NEmpty, NMenu, NMessageProvider, NModal, NResult, NSelect, NSpin, NTag, dateZhCN, zhCN } from 'naive-ui';
import { Activity, ClipboardList, Droplets, Grid2X2, House, LogOut, Menu, PanelLeft, Settings, ShieldCheck, UserRound } from 'lucide-vue-next';
import { AUTH_EXPIRED_EVENT, requestJson, setReadCacheIdentity, invalidateReadCache, type Dict } from './client';
import { setModulePermissions } from '../api/modulePermissions';
import { modules, moduleIcons, navigationModules, moduleForRoute, moduleUrl, NOTICE_IDS, SOP_IDS, beijingDate } from './modules';
import { personaFor } from './persona';
import { logicalUrl, saveVersion, workspaceUrl, loginVersionUrl } from './version';
import { navigate } from './navigation';
import { theme } from './theme';
import OverviewPanel from './OverviewPanel.vue';
import MobileNavigation from './MobileNavigation.vue';
import SidebarNavigation from './SidebarNavigation.vue';
import {useAuthVersionGuard} from './authVersionGuard';
const {busy:versionBusy,confirming:confirmVersion,request:requestVersion,confirm:finishVersion}=useAuthVersionGuard();
function animateMobilePage(direction:number){
  if(!window.matchMedia('(max-width:760px)').matches||window.matchMedia('(prefers-reduced-motion: reduce)').matches)return;
  requestAnimationFrame(()=>document.querySelector('.v2-main')?.animate([{opacity:.25,transform:`translateX(${direction*30}px)`},{opacity:1,transform:'translateX(0)'}],{duration:250,easing:'ease-out'}));
}
import { LighthouseAssistant } from '../lighthouseEntry';
import AsyncPageState from '../components/AsyncPageState.vue';
const lazy = (load: () => Promise<any>) => defineAsyncComponent({ loader: load, loadingComponent: AsyncPageState, errorComponent: AsyncPageState, delay: 100, timeout: 30000 });
const HomePage = lazy(() => import('./HomePage.vue'));
const WorkOrderWorkspace = lazy(() => import('./WorkOrderWorkspace.vue'));
const NoticeWorkspace = lazy(() => import('./NoticeWorkspace.vue'));
const NoticeSopManager = lazy(() => import('./NoticeSopManager.vue'));
const AuthPanels = lazy(() => import('../components/AuthPanels.vue'));
const pages: Record<string, any> = {
  event: lazy(() => import('../components/EventManagementPage.vue')),
  engineer_mop: lazy(() => import('../components/EngineerMopPage.vue')),
  repair_management: lazy(() => import('./RepairWorkspace.vue')),
  repair_status: lazy(() => import('./RepairWorkspace.vue')),
  learning: lazy(() => import('../components/LearningPage.vue')),
  plan_convergence: lazy(() => import('../components/PlanConvergencePage.vue')),
  life_guide: lazy(() => import('../components/LifeGuidePage.vue')),
  links: lazy(() => import('../components/LinkDirectoryPage.vue')),
  knowledge: lazy(() => import('../components/KnowledgeBasePage.vue')),
  daily: lazy(() => import('../components/DailyTaskChecklistPage.vue')),
  water: lazy(() => import('../components/WaterManagementPage.vue')),
  critical_guard: lazy(() => import('../components/CriticalGuardPage.vue')),
  cabinet: lazy(() => import('../components/CabinetPowerPage.vue')),
  cabinet_batch: lazy(() => import('../components/CabinetPowerBatchPage.vue')),
  drills: lazy(() => import('../components/DrillManagementPage.vue')),
  signatures: lazy(() => import('../components/SignatureManagementPage.vue')),
  signature: lazy(() => import('../components/SignaturePage.vue')),
  accounts: lazy(() => import('../components/AdminTools.vue')),
  history: lazy(() => import('../components/HistoryMemoryPage.vue')),
};
const route = ref(logicalUrl()), checking = ref(true), loggedIn = ref(false), user = ref<Dict>({}), scopes = ref<{value:string;label:string}[]>([]), authError = ref(''), loginUrl = ref('/api/auth/login'), collapsed = ref(false);
const operationError=ref(''),logoutBusy=ref(false);
let disposed = false, timer = 0, checkingAuth = false;
const lifetime = new AbortController();
const watermarkDay=beijingDate();
function preventSignatureDrag(event:DragEvent){const image=(event.target as Element)?.closest?.('img');if(image&&(image.src.includes('/api/signatures/')||image.alt.includes('签名')))event.preventDefault();}
const identityKey = computed(() => JSON.stringify([user.value.open_id, user.value.level, user.value.modules, scopes.value]));
const granted = computed<string[]>(() => Array.isArray(user.value.modules) ? user.value.modules : []);
const activeModule = computed(() => moduleForRoute(route.value));
const view = computed(() => route.value.searchParams.get('view') || 'home');
const homeViews = ['home', 'tasks', 'functions'];
const publicLink = computed(() => route.value.pathname.startsWith('/polling-work-order') || route.value.pathname === '/signature' && ['record_id', 'request_id', 'temporary_id'].some(k => route.value.searchParams.has(k)));
const printMode = computed(() => route.value.pathname.endsWith('/print'));
const denied = computed(() => loggedIn.value && !publicLink.value && (!!activeModule.value && !(activeModule.value === 'all' ? NOTICE_IDS.some(id => granted.value.includes(id)) : activeModule.value==='sops' ? SOP_IDS.some(id=>granted.value.includes(id)) : granted.value.includes(activeModule.value)) || !!route.value.searchParams.get('scope') && !scopes.value.some(s => s.value === route.value.searchParams.get('scope'))));
const previousScope=ref({identity:'',value:''});
const currentScope = computed(() => route.value.searchParams.get('scope') || (previousScope.value.identity===user.value.open_id && scopes.value.some(s=>s.value===previousScope.value.value)?previousScope.value.value:(scopes.value.some(s => s.value === 'ALL') ? 'ALL' : scopes.value[0]?.value || '')));
watch([route,identityKey],()=>{const value=route.value.searchParams.get('scope');if(user.value.open_id&&value&&scopes.value.some(s=>s.value===value))previousScope.value={identity:user.value.open_id,value};},{flush:'sync'});
const noticeActive = computed(() => loggedIn.value && !checking.value && !denied.value && route.value.pathname === '/workbench-lite');
const noticeView = ref<{identity:string;url:string}|null>(null);
// Keep the V2 workspace warm; permission/account changes discard its private state.
watch([route,identityKey,loggedIn,checking],()=>{
  if(noticeView.value?.identity!==identityKey.value || !loggedIn.value)noticeView.value=null;
  if(noticeActive.value)noticeView.value={identity:identityKey.value,url:route.value.pathname+route.value.search};
},{flush:'sync'});
const unsupportedScope=computed(()=>['critical_guard','drills','cabinet'].includes(activeModule.value)&&['H','110'].includes(currentScope.value));
const scopeLabel = computed(() => scopes.value.find(s => s.value === currentScope.value)?.label || currentScope.value);
const businessScopes = computed(()=>scopes.value.map(s=>({...s,label:s.value==='ALL'?'全部楼栋':s.label})));
const scopeSelectable = computed(() => !['sops','learning', 'accounts', 'signatures', 'links', 'knowledge', 'assistant', 'life_guide', 'plan_convergence', 'daily', 'engineer_mop'].includes(activeModule.value) && !(activeModule.value==='water'&&!['ALL','CAMPUS'].includes(currentScope.value)) && !printMode.value);
const passwordPage = computed(() => loggedIn.value && route.value.searchParams.get('account') === 'password' && user.value.login_method === 'password');
const permissionPage = computed(() => view.value === 'permission');
const levelLabel = computed(() => ({ L0:'超级管理员',L1:'全楼管理员',L2:'工程师',L3:'楼栋主管',L4:'成员' }[String(user.value.level)] || '成员'));
const categoryLabel = computed(() => ({ manager:'经理',supervisor:'主管',engineer:'工程师',duty:'值班账号',operator:'值班员',shift_leader:'值班长' }[String(user.value.category)] || '工作空间'));
const selectedMenu = computed(() => NOTICE_IDS.includes(activeModule.value)||activeModule.value==='all'?'notices':activeModule.value || view.value);
const permittedModules = computed(() => navigationModules(granted.value).filter(m=>m.id!=='accounts'));
const menuOptions = computed(() => [
  { label: '个人工作台', key: 'home', icon: () => h(House, {size:19}) },
  ...[...new Set(permittedModules.value.map(m => m.group))].map(group => ({ key: group, label: group, type: 'group' as const, children: permittedModules.value.filter(m => m.group === group).map(m => ({ key: m.id, label:m.label, icon:() => h(moduleIcons[m.icon as keyof typeof moduleIcons],{size:18}) })) })),
]);
const mobileTabs = [{value:'home',label:'首页',icon:House},{value:'tasks',label:'待办',icon:ClipboardList},{value:'functions',label:'功能',icon:Grid2X2},{value:'my',label:'我的',icon:UserRound}];
const dutyTabs = computed(() => [
  ...(NOTICE_IDS.some(id => granted.value.includes(id)) ? [{id:'notices',label:'通告处理',icon:ClipboardList,url:'/workbench-lite?work_type=all&scope='+currentScope.value}] : []),
  ...[{id:'water',label:'水耗处理',icon:Droplets},{id:'drills',label:'演练管理',icon:Activity},{id:'critical_guard',label:'风险管理',icon:ShieldCheck}].filter(tab => granted.value.includes(tab.id)).map(tab => ({...tab,url:moduleUrl(modules.find(m=>m.id===tab.id)!,currentScope.value)})),
]);
const dutyActive = computed(() => NOTICE_IDS.includes(activeModule.value) || activeModule.value === 'all' ? 'notices' : activeModule.value);
const showDutyTabs = computed(() => personaFor(user.value) === 'duty' && !publicLink.value && !printMode.value && ['notices','water','drills','critical_guard'].includes(dutyActive.value));
function selectMenu(key: string) { const module = permittedModules.value.find(m => m.id === key); navigate(module ? moduleUrl(module, currentScope.value) : '/?view=' + key); }
function returnFromSop(context:{scope:string;work_type:string}){navigate('/workbench-lite?'+new URLSearchParams(context));}
function changeScope(scope: string) { const url = logicalUrl(); url.searchParams.set('scope', scope); navigate(url); }
function selectAdminTab(tab: string) { navigate('/admin/settings?tab=' + encodeURIComponent(tab), {replace:true}); }
function openAssistant() { window.dispatchEvent(new CustomEvent('clipflow:assistant-open')); const trigger = document.querySelector<HTMLButtonElement>('[aria-label="打开灯塔助手"]'); trigger?.click(); }
function switchLegacy() { navigate(loggedIn.value ? '/?frontend=legacy' : loginVersionUrl('legacy'), {hard:true,legacy:true}); }
let statusRevision=0;
async function onStatus(text: string) {
  const revision=++statusRevision;operationError.value='';
  if(!/失败|异常|超时|无法/.test(text))return;
  await nextTick();if(disposed||revision!==statusRevision)return;
  const inline=[...document.querySelectorAll('.v2-main [role="alert"]:not(.v2-alert)')].some(el=>el.getClientRects().length&&el.textContent?.includes(text));
  if(!inline)operationError.value=text;
}
const componentName = computed(() => route.value.pathname === '/repair-status' ? 'repair_status' : route.value.pathname === '/cabinet-power/batches' ? 'cabinet_batch' : route.value.pathname === '/signature' ? 'signature' : route.value.pathname === '/admin/history-memory' ? 'history' : activeModule.value);
const activeComponent = computed(() => pages[componentName.value]);
watch(()=>route.value.pathname+':'+(componentName.value||view.value),()=>{
  if(window.matchMedia('(min-width:761px)').matches&&!window.matchMedia('(prefers-reduced-motion: reduce)').matches)
    requestAnimationFrame(()=>document.querySelector('.v2-main')?.animate([{opacity:.4},{opacity:1}],{duration:200,easing:'ease-out'}));
},{flush:'post'});
const componentKey = computed(() => identityKey.value + ':' + route.value.pathname + ':' + (activeModule.value === 'learning' ? route.value.search : currentScope.value) + (componentName.value === 'cabinet_batch' ? ':' + (route.value.searchParams.get('mode') || '') + ':' + (route.value.searchParams.get('batch_id') || '') : ''));
const componentProps = computed(() => {
  const p = route.value.searchParams, admin = user.value.role === 'admin';
  const common = { scope:currentScope.value, scopeOptions:scopes.value, isAdmin:admin, checking:false, loggedIn:loggedIn.value, loginUrl:loginUrl.value };
  switch(componentName.value) {
    case 'event': return {...common, refreshNonce:0};
    case 'repair_status':
    case 'repair_management': return {...common, userId:String(user.value.open_id || ''), canDelete:user.value.can_delete_business === true, focusRecordId:p.get('record_id') || ''};
    case 'learning': return {...common, scope:p.get('scope') || '', personId:p.get('person_id') || '', userId:String(user.value.open_id || '')};
    case 'critical_guard': return {...common, scope:['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, user:user.value, adminMode:admin && (p.get('mode') === 'admin'||['ALL','CAMPUS'].includes(currentScope.value))};
    case 'drills': return {...common, scope:['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, currentUser:user.value, adminMode:admin && (p.get('mode') === 'admin'||['ALL','CAMPUS'].includes(currentScope.value)), printMode:printMode.value};
    case 'cabinet': return {...common, scope:['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, userId:String(user.value.open_id || ''), readOnly:user.value.role === 'guest', canDelete:user.value.can_delete_business === true};
    case 'cabinet_batch': return {...common, userId:String(user.value.open_id || '')};
    case 'signature': return {defaultScope:currentScope.value};
    case 'accounts': return {open:true, systemAdmin:admin, initialTab:p.get('tab') || 'permissions', scopeOptions:scopes.value};
    case 'daily': return {...common, printMode:printMode.value};
    default:return common;
  }
});
async function loadAuth() {
  if (checkingAuth || disposed) return;
  checkingAuth = true;
  try {
    const data = await requestJson('/api/auth/status?next=' + encodeURIComponent(window.location.pathname + window.location.search + window.location.hash), {signal:lifetime.signal, cache:'no-store',timeoutMs:15000});
    if (disposed) return;
    loggedIn.value = !!data.logged_in; user.value = data.user || {}; scopes.value = data.scope_options || []; loginUrl.value = data.login_url || '/api/auth/login';
    setModulePermissions(granted.value); setReadCacheIdentity(identityKey.value); authError.value = data.directory_warning || '';
    if (loggedIn.value && user.value.role !== 'guest' && user.value.open_id) saveVersion(String(user.value.open_id), 'workspace');
    if (user.value.role === 'guest' && route.value.pathname !== '/cabinet-power') navigate('/cabinet-power', {replace:true,bypassGuard:true});
  } catch (e:any) { if (!disposed) authError.value = e.message || '登录状态读取失败'; }
  finally { checking.value = false; checkingAuth = false; window.clearTimeout(timer); if (!disposed && loggedIn.value && !document.hidden) timer = window.setTimeout(loadAuth,60000); }
}
async function logout() {
  if(logoutBusy.value)return;logoutBusy.value=true;operationError.value='';
  try { await requestJson('/api/auth/logout',{method:'POST',body:'{}'}); invalidateReadCache(); setModulePermissions([]); loggedIn.value=false; user.value={}; scopes.value=[]; navigate('/',{replace:true,bypassGuard:true}); }
  catch(e){operationError.value=e instanceof Error?e.message:'退出失败，请重试。';}
  finally{logoutBusy.value=false;}
}
function expired(event: Event) { (event as CustomEvent).preventDefault?.(); loggedIn.value=false; user.value={}; scopes.value=[]; setModulePermissions([]); invalidateReadCache(); navigate('/?login=password&next=' + encodeURIComponent(window.location.pathname + window.location.search), {replace:true,bypassGuard:true}); }
function routeChanged() { statusRevision++; route.value = logicalUrl(); authError.value=''; operationError.value=''; }
function visibility() { if(document.hidden) window.clearTimeout(timer); else void loadAuth(); }
const permission = reactive({scopes:[] as string[],reason:'',code:'',requestId:'',message:'',status:'',rejectReason:'',approverOpenId:'',notificationStatus:'',notificationError:''}), permissionBusy=ref(false);
const requestableScopes = ['110','A','B','C','D','E','H','CAMPUS'].map(value => ({value,label:value === 'CAMPUS' ? '园区' : value === '110' ? '110站' : value+'楼'}));
async function submitPermission() { if(permissionBusy.value)return; permissionBusy.value=true; try{const data=await requestJson('/api/auth/permission-requests',{method:'POST',body:JSON.stringify({scopes:permission.scopes,reason:permission.reason,approver_open_id:permission.approverOpenId})}); Object.assign(permission,{requestId:data.request_id || data.request?.request_id || '',status:data.request?.status || 'pending',notificationStatus:data.request?.notification_status||'',notificationError:data.request?.notification_error||'',rejectReason:'',message:'申请已提交，等待管理员审批。'});}catch(e:any){permission.message=e.message;}finally{permissionBusy.value=false;} }
watch(()=>[permissionPage.value,loggedIn.value,user.value.open_id],async(_value,_old,onCleanup)=>{
  Object.assign(permission,{scopes:[],reason:'',requestId:'',status:'',approverOpenId:'',notificationStatus:'',notificationError:'',rejectReason:'',message:''});
  if(!loggedIn.value||(!permissionPage.value&&scopes.value.length))return;
  const abort=new AbortController();onCleanup(()=>abort.abort());
  try{const data=await requestJson('/api/auth/permission-requests/current',{signal:abort.signal,cache:'no-store'}),r=data.request;if(!abort.signal.aborted&&r?.request_id)Object.assign(permission,{requestId:r.request_id,scopes:r.requested_scopes||[],reason:r.reason||'',status:r.status,approverOpenId:r.approver_open_id||'',notificationStatus:r.notification_status||'',notificationError:r.notification_error||'',rejectReason:r.reject_reason||''});}
  catch{/* A temporary read failure must not block a new application. */}
});
const handovers=ref<Dict>({}),handoverLoading=ref(false);
watch([view,loggedIn],async()=>{if(view.value!=='handover'||!loggedIn.value||!granted.value.includes('handover'))return; handoverLoading.value=true;try{handovers.value=(await requestJson('/api/handover-links',{signal:lifetime.signal})).links||{};}catch(e:any){authError.value=e.message;}finally{handoverLoading.value=false;} });
onMounted(()=>{void loadAuth();window.addEventListener('popstate',routeChanged);window.addEventListener('auth-account-changed',loadAuth);window.addEventListener('workspace-permissions-stale',loadAuth);window.addEventListener(AUTH_EXPIRED_EVENT,expired);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;lifetime.abort();window.clearTimeout(timer);window.removeEventListener('popstate',routeChanged);window.removeEventListener('auth-account-changed',loadAuth);window.removeEventListener('workspace-permissions-stale',loadAuth);window.removeEventListener(AUTH_EXPIRED_EVENT,expired);document.removeEventListener('visibilitychange',visibility);});
</script>
