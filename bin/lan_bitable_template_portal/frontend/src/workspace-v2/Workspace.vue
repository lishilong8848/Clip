<template>
  <NConfigProvider :theme="nightMode && !printMode && !printing ? darkTheme : null" :theme-overrides="nightMode && !printMode && !printing ? nightTheme : theme" :locale="zhCN" :date-locale="dateZhCN">
    <NMessageProvider placement="bottom-right" :duration="0" closable><NDialogProvider>
      <div class="v2-shell" :style="{'--v2-nav-color':navColor}" :class="{ 'is-collapsed': collapsed, 'is-print': printMode, 'is-public': publicLink, 'is-guest': user.role === 'guest', 'is-auth':!loggedIn&&!publicLink }" @dragstart.capture="preventSignatureDrag">
        <div v-if="loggedIn && !printMode && activeModule !== 'life_guide' && route.pathname !== '/workbench-lite'" class="v2-watermark" aria-hidden="true"><span v-for="n in 24" :key="n">内部资料 · {{ user.name }} · {{ watermarkDay }}</span></div>
        <header v-if="!printMode" class="v2-header">
          <NButton v-if="loggedIn && !publicLink && user.role !== 'guest'" data-v2-navigation quaternary circle aria-label="展开或收起导航" :aria-expanded="!collapsed" @click="collapsed = !collapsed"><Menu :size="21" /></NButton>
          <a :href="workspaceUrl(user.role === 'guest' ? '/cabinet-power' : '/')" class="v2-brand"><img :src="'/assets/vnet-logo.png'" alt="世纪互联 VNET" /><span>灯塔<small>南通运维工作空间</small></span></a>
          <div class="v2-header-actions">
            <NavThemePicker v-if="!publicLink" :user-id="String(user.open_id||'')" @change="navColor=$event" />
            <NButton v-if="loggedIn || !checking && !publicLink" data-v2-navigation size="small" secondary :disabled="versionBusy" :title="versionBusy?'正在提交，请等待结果':undefined" :style="!loggedIn ? {minHeight:'44px'} : undefined" @click="requestVersion(switchLegacy)">切换旧版</NButton>
            <NButton v-if="loggedIn && user.role !== 'guest'" data-v2-navigation quaternary class="v2-person" aria-label="打开个人页面" title="个人信息与设置" @click="navigate('/?view=my')"><UserRound :size="17" /><span>{{ user.name }}</span></NButton>
            <NButton v-else-if="loggedIn" quaternary aria-label="退出登录" @click="logout"><LogOut :size="17" /></NButton>
          </div>
        </header>
        <SidebarNavigation v-if="loggedIn && !publicLink && !printMode && user.role !== 'guest'" v-model:collapsed="collapsed" :options="menuOptions" :selected="selectedMenu" @select="selectMenu" />
        <main class="v2-main">
          <NAlert v-if="authError" type="error" :bordered="false" class="v2-alert">{{ authError }} <NButton text type="primary" @click="loadAuth">重新读取</NButton></NAlert>
          <NAlert v-if="operationError" type="error" :bordered="false" closable role="alert" class="v2-alert" @close="operationError=''">{{ operationError }}</NAlert>
          <div v-if="checking" class="v2-loading"><NSpin size="medium" /><span>正在检查登录状态</span></div>
          <NResult v-else-if="denied" status="403" title="没有该功能权限" description="当前账号未获授此功能，请申请权限或联系管理员。"><template #footer><NButton type="primary" @click="navigate('/?view=permission')">申请权限</NButton></template></NResult>
          <NResult v-else-if="loggedIn && unsupportedScope" status="info" :title="scopeLabel + '尚未接入此业务'" description="原业务目前仅支持 A–E 楼，未使用其他楼栋的数据代替。"><template #footer><NButton v-if="user.role === 'admin' && ['drills','critical_guard'].includes(activeModule)" type="primary" @click="navigate(route.pathname+'?mode=admin')"><Settings :size="16" />管理员入口</NButton></template></NResult>
          <AccessRequest v-else-if="loggedIn && permissionPage" :key="String(user.open_id)" :user="user" @back="navigate('/?view=my')" @changed="loadAuth" />
          <AuthPanels v-else-if="!publicLink && (!loggedIn || passwordPage || !scopes.length && !['learning','knowledge'].includes(activeModule))"
            :checking="false" :logged-in="loggedIn" :user="user" :login-url="loginUrl" :busy="permissionBusy" :request="permission" :requestable-scopes="requestableScopes" :password-settings="passwordPage" :show-back="loggedIn" :title="permissionPage?'申请楼栋权限':''" @back="navigate('/?view=my')" @update-request="Object.assign(permission, $event)" @submit="submitPermission" @confirm="submitPermission" />
          <template v-else>
            <nav v-if="showDutyTabs" class="v2-duty-tabs" aria-label="值班常用业务">
              <NButton v-for="tab in dutyTabs" :key="tab.id" :type="dutyActive === tab.id ? 'primary' : 'default'" :secondary="dutyActive !== tab.id" @click="navigate(tab.url)"><component :is="tab.icon" :size="18" />{{ tab.label }}</NButton>
            </nav>
            <HomePage v-if="homeViews.includes(view) && route.pathname === '/' && !route.searchParams.get('mode')" :key="identityKey" :user="user" :scopes="scopes" :scope="currentScope" :view="view" :initial-kind="route.searchParams.get('kind')||undefined" @open="openWorkspaceModule($event)" />
            <section v-else-if="view === 'my' && route.pathname === '/'" class="v2-my">
              <header class="v2-profile-heading"><NAvatar :size="48" :style="{background:'var(--v2-nav-active)',color:'var(--v2-nav-text)'}"><UserRound :size="25" /></NAvatar><div><h1>{{ user.name }}</h1><p>个人信息与设置</p></div></header>
              <div class="v2-profile-layout">
                <section class="v2-profile-identity" aria-label="账号信息">
                  <h2><UserRound :size="17" />账号信息</h2>
                  <dl><div><dt>角色</dt><dd>{{ levelLabel }} · {{ user.category_label || categoryLabel }}</dd></div><div><dt>登录方式</dt><dd>{{ user.login_method === 'password' ? '姓名密码登录' : '飞书登录' }}</dd></div></dl>
                  <h2><Building2 :size="17" />可访问楼栋</h2><div class="v2-profile-scopes"><NTag v-for="scope in scopes" :key="scope.value" :bordered="false" size="small">{{ scope.label }}</NTag><span v-if="!scopes.length">暂无</span></div>
                  <div class="v2-profile-actions">
                    <NButton v-if="granted.includes('accounts')" data-v2-navigation secondary type="primary" @click="navigate('/admin/settings')"><Settings :size="17" /><span>账号与设置</span><ChevronRight :size="16" /></NButton>
                    <NButton v-if="user.login_method === 'password'" data-v2-navigation secondary @click="navigate('/?account=password')"><KeyRound :size="17" /><span>修改密码</span><ChevronRight :size="16" /></NButton>
                    <NButton data-v2-navigation secondary @click="navigate('/?view=permission')"><ShieldCheck :size="17" /><span>申请权限</span><ChevronRight :size="16" /></NButton>
                    <NButton type="error" quaternary :loading="logoutBusy" :disabled="logoutBusy" @click="logout"><LogOut :size="17" /><span>退出登录</span></NButton>
                  </div>
                </section>
                <section class="v2-profile-access" aria-label="功能权限"><h2><ShieldCheck :size="17" />功能权限<NTag size="small" :bordered="false">{{ granted.length }} 项</NTag></h2><div v-for="group in profileGroups" :key="group.name" class="v2-profile-group"><h3>{{ group.name }}</h3><div class="v2-profile-modules"><NTag v-for="item in group.items" :key="item.id" size="small" :bordered="false">{{ item.label }}</NTag></div></div><NEmpty v-if="!granted.length" description="暂无已授权功能" /></section>
              </div>
              <AccountSignature v-if="granted.includes('signatures')" :key="identityKey" class="v2-my-signatures" />
            </section>
            <section v-else-if="view === 'handover' && route.pathname === '/'" class="v2-links"><h2>交接班审核</h2><PageCanvas page="handover" label="交接班审核" :blocks="[{id:'reviews',title:'楼栋审核',w:12,h:5,minW:4,minH:4}]"><template #reviews><NSpin :show="handoverLoading"><div class="v2-feature-grid"><NButton v-for="(url, scope) in handovers" :key="scope" tag="a" :href="String(url)" target="_blank" rel="noopener noreferrer">{{ scope }}楼审核</NButton></div><NEmpty v-if="!handoverLoading && !Object.keys(handovers).length" description="暂无可用审核入口" /></NSpin></template></PageCanvas></section>
            <section v-else-if="view === 'assistant' && route.pathname === '/'" class="v2-links"><h2>灯塔助手</h2><NButton type="primary" @click="openAssistant">打开会话</NButton></section>
            <NoticeSopManager v-else-if="activeModule==='sops'" :key="identityKey" :initial-scope="currentScope" :initial-type="route.searchParams.get('work_type')||''" :scopes="scopes.filter(s=>['110',...'ABCDEH'].includes(s.value))" :types="modules.filter(m=>SOP_IDS.includes(m.id)&&granted.includes(m.id)).map(m=>({value:m.id,label:m.label}))" :can-delete="user.can_delete_business===true" />
            <WorkOrderWorkspace v-else-if="route.pathname.startsWith('/polling-work-order')" :key="route.pathname+route.search" :url="route.pathname + route.search + route.hash" />
            <template v-else-if="route.pathname === '/workbench-lite'" />
            <component v-else-if="activeComponent" :is="activeComponent" :key="componentKey" v-bind="componentProps" @switch-scope="changeScope" @status="onStatus" @close="activeModule==='accounts'?navigate('/?view=my'):navigateBack()" @select-tab="selectAdminTab" @changed="loadAuth" />
            <NResult v-else status="404" title="没有找到此页面" description="请从功能导航选择要使用的模块。" />
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
import { computed, defineAsyncComponent, h, nextTick, onBeforeUnmount, onMounted, provide, reactive, ref, watch } from 'vue';
import { NAlert, NAvatar, NButton, NConfigProvider, NDialogProvider, NEmpty, NMenu, NMessageProvider, NModal, NResult, NSelect, NSpin, NTag, darkTheme, dateZhCN, zhCN } from 'naive-ui';
import { Activity, Building2, ChevronRight, ClipboardList, Droplets, Grid2X2, House, KeyRound, LogOut, Menu, PanelLeft, Settings, ShieldCheck, UserRound } from 'lucide-vue-next';
import { AUTH_EXPIRED_EVENT, requestJson, setReadCacheIdentity, invalidateReadCache, type Dict } from './client';
import { setModulePermissions } from '../api/modulePermissions';
import { modules, moduleIcons, navigationModules, moduleForRoute, moduleUrl, NOTICE_IDS, SOP_IDS, beijingDate } from './modules';
import { personaFor } from './persona';
import { logicalUrl, saveVersion, workspaceUrl, loginVersionUrl } from './version';
import { navigate, navigateBack } from './navigation';
import { DEFAULT_NAV_COLOR,theme,nightTheme,nightMode,loadNightMode } from './theme';
import NavThemePicker from './NavThemePicker.vue';
import MobileNavigation from './MobileNavigation.vue';
import SidebarNavigation from './SidebarNavigation.vue';
import {useAuthVersionGuard} from './authVersionGuard';
import { pageContext } from './pageLayout';
import {moduleScope,scopeFamily,scopeOwner,readScopeMemory,saveScopeMemory} from './scopeMemory';
const {busy:versionBusy,confirming:confirmVersion,request:requestVersion,confirm:finishVersion}=useAuthVersionGuard();
function animateMobilePage(direction:number){
  if(!window.matchMedia('(max-width:760px)').matches||window.matchMedia('(prefers-reduced-motion: reduce)').matches)return;
  requestAnimationFrame(()=>document.querySelector('.v2-main')?.animate([{opacity:.25,transform:`translateX(${direction*30}px)`},{opacity:1,transform:'translateX(0)'}],{duration:250,easing:'ease-out'}));
}
import { LighthouseAssistant } from '../lighthouseEntry';
import AsyncPageState from '../components/AsyncPageState.vue';
import AccountSignature from '../components/AccountSignature.vue';
const lazy = (load: () => Promise<any>) => defineAsyncComponent({ loader: load, loadingComponent: AsyncPageState, errorComponent: AsyncPageState, delay: 100, timeout: 30000 });
const HomePage = lazy(() => import('./HomePage.vue'));
const PageCanvas = lazy(() => import('./PageCanvas.vue'));
const WorkOrderWorkspace = lazy(() => import('./WorkOrderWorkspace.vue'));
const NoticeWorkspace = lazy(() => import('./NoticeWorkspace.vue'));
const NoticeSopManager = lazy(() => import('./NoticeSopManager.vue'));
const AuthPanels = lazy(() => import('../components/AuthPanels.vue'));
const AccessRequest = lazy(() => import('./AccessRequest.vue'));
const pages: Record<string, any> = {
  event: lazy(() => import('../components/EventManagementPage.vue')),
  engineer_mop: lazy(() => import('./MopWorkspace.vue')),
  work_handover: lazy(() => import('../components/WorkHandoverPage.vue')),
  repair_management: lazy(() => import('./RepairWorkspace.vue')),
  repair_status: lazy(() => import('./RepairWorkspace.vue')),
  learning: lazy(() => import('../components/LearningPage.vue')),
  plan_convergence: lazy(() => import('../components/PlanConvergencePage.vue')),
  life_guide: lazy(() => import('../components/LifeGuidePage.vue')),
  links: lazy(() => import('../components/LinkDirectoryPage.vue')),
  knowledge: lazy(() => import('../components/KnowledgeBasePage.vue')),
  daily: lazy(() => import('../components/DailyTaskChecklistPage.vue')),
  water: lazy(() => import('./WaterWorkspace.vue')),
  critical_guard: lazy(() => import('../components/CriticalGuardPage.vue')),
  risk_inspection: lazy(() => import('../components/RiskInspectionPage.vue')),
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
const navColor=ref(DEFAULT_NAV_COLOR);
watch(()=>String(user.value.open_id||''),loadNightMode,{immediate:true,flush:'sync'});
const printMedia=window.matchMedia('print'),printing=ref(printMedia.matches);
const syncPrinting=()=>{printing.value=printMedia.matches;};
onMounted(()=>printMedia.addEventListener('change',syncPrinting));
onBeforeUnmount(()=>printMedia.removeEventListener('change',syncPrinting));
let disposed = false, timer = 0, checkingAuth = false;
let authRead:AbortController|undefined;
function cancelAuth(){authRead?.abort();authRead=undefined;checkingAuth=false;window.clearTimeout(timer);}
function refreshAuth(){cancelAuth();void loadAuth();}
const watermarkDay=beijingDate();
function preventSignatureDrag(event:DragEvent){const image=(event.target as Element)?.closest?.('img');if(image&&(image.src.includes('/api/signatures/')||image.alt.includes('签名')))event.preventDefault();}
const identityKey = computed(() => JSON.stringify([user.value.open_id, user.value.level, user.value.modules, scopes.value]));
const profileGroups = computed(() => [...new Set(modules.map(item=>item.group))].map(name=>({name,items:modules.filter(item=>item.group===name&&granted.value.includes(item.id))})).filter(group=>group.items.length));
const granted = computed<string[]>(() => Array.isArray(user.value.modules) ? user.value.modules : []);
const activeModule = computed(() => moduleForRoute(route.value));
const view = computed(() => route.value.searchParams.get('view') || 'home');
const homeViews = ['home', 'tasks', 'functions'];
const publicLink = computed(() => route.value.pathname.startsWith('/polling-work-order') || route.value.pathname === '/signature' && ['record_id', 'request_id', 'temporary_id'].some(k => route.value.searchParams.has(k)));
const printMode = computed(() => route.value.pathname.endsWith('/print'));
const denied = computed(() => loggedIn.value && !publicLink.value && (!!activeModule.value && !(activeModule.value === 'all' ? NOTICE_IDS.some(id => granted.value.includes(id)) : activeModule.value==='sops' ? SOP_IDS.some(id=>granted.value.includes(id)) : granted.value.includes(activeModule.value)) || !!route.value.searchParams.get('scope') && !scopes.value.some(s => s.value === route.value.searchParams.get('scope'))));
const rememberedScopes=ref<Record<string,string>>({});
watch(()=>scopeOwner(user.value),()=>{rememberedScopes.value=readScopeMemory(user.value);},{immediate:true,flush:'sync'});
const authDefaultScope=ref('');
const noticeDefaultScope=computed(()=>scopes.value.some(s=>s.value==='ALL')?'ALL':scopes.value.some(s=>s.value===authDefaultScope.value)?authDefaultScope.value:scopes.value[0]?.value||'');
function scopeFor(module:string){return moduleScope(module,scopes.value,rememberedScopes.value[scopeFamily(module)]||'',scopeFamily(module)==='notices'?noticeDefaultScope.value:authDefaultScope.value);}
const currentScope = computed(() => route.value.pathname === '/cabinet-power' && route.value.searchParams.get('view') === 'overview' && !route.value.searchParams.has('scope') ? '' : route.value.searchParams.get('scope') || scopeFor(activeModule.value));
provide(pageContext,{user:computed(()=>user.value),scopes:computed(()=>scopes.value.map(scope=>scope.value)),scope:currentScope,module:computed(()=>NOTICE_IDS.includes(activeModule.value)||activeModule.value==='all'?'notices':activeModule.value),changeScope});
watch([route,identityKey],()=>{const value=route.value.searchParams.get('scope');if(scopeOwner(user.value)&&value&&scopes.value.some(s=>s.value===value)){rememberedScopes.value={...rememberedScopes.value,[scopeFamily(activeModule.value)]:value};saveScopeMemory(user.value,rememberedScopes.value);}},{flush:'sync'});
const noticeActive = computed(() => loggedIn.value && !checking.value && !denied.value && route.value.pathname === '/workbench-lite');
const noticeView = ref<{identity:string;url:string}|null>(null);
// Keep the V2 workspace warm; permission/account changes discard its private state.
watch([route,identityKey,loggedIn,checking],()=>{
  if(noticeView.value?.identity!==identityKey.value || !loggedIn.value)noticeView.value=null;
  if(noticeActive.value)noticeView.value={identity:identityKey.value,url:route.value.pathname+route.value.search};
},{flush:'sync'});
const moduleAdminMode=computed(()=>user.value.role==='admin'&&['critical_guard','drills'].includes(activeModule.value)&&(route.value.searchParams.get('mode')==='admin'||['ALL','CAMPUS'].includes(currentScope.value)));
const unsupportedScope=computed(()=>!moduleAdminMode.value&&['critical_guard','drills','cabinet'].includes(activeModule.value)&&['H','110'].includes(currentScope.value));
const scopeLabel = computed(() => scopes.value.find(s => s.value === currentScope.value)?.label || currentScope.value);
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
  ...(NOTICE_IDS.some(id => granted.value.includes(id)) ? [{id:'notices',label:'通告处理',icon:ClipboardList,url:'/workbench-lite?work_type=all'}] : []),
  ...[{id:'water',label:'水耗处理',icon:Droplets},{id:'drills',label:'演练管理',icon:Activity},{id:'critical_guard',label:'重保管理',icon:ShieldCheck}].filter(tab => granted.value.includes(tab.id)).map(tab => ({...tab,url:moduleUrl(modules.find(m=>m.id===tab.id)!,scopeFor(tab.id))})),
]);
const dutyActive = computed(() => NOTICE_IDS.includes(activeModule.value) || activeModule.value === 'all' ? 'notices' : activeModule.value);
const showDutyTabs = computed(() => personaFor(user.value) === 'duty' && !publicLink.value && !printMode.value && ['notices','water','drills','critical_guard'].includes(dutyActive.value));
function selectMenu(key: string) { const module = permittedModules.value.find(m => m.id === key); navigate(module ? moduleUrl(module, scopeFor(module.id)) : '/?view=' + key); }
function openWorkspaceModule(target:string){
 const url=logicalUrl(new URL(target,location.origin).href),module=moduleForRoute(url);
 const precise=['record_id','active_item_id','source_record_id','event_id','batch_id','paper_id','job_id','token'].some(key=>url.searchParams.has(key));
 if(module&&!precise)url.searchParams.set('scope',scopeFor(module));
 navigate(url);
}

function changeScope(scope: string) { const url = logicalUrl(); url.searchParams.set('scope', scope); if(['drills','critical_guard'].includes(activeModule.value))url.searchParams.delete('mode'); navigate(url); }
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
  // The workspace, not the document, owns PC scrolling. A new page starts at its header.
  void nextTick(()=>{document.querySelector('.v2-main')?.scrollTo({top:0,behavior:'instant'});window.scrollTo({top:0,behavior:'instant'});});
  if(window.matchMedia('(min-width:761px)').matches&&!window.matchMedia('(prefers-reduced-motion: reduce)').matches)
    requestAnimationFrame(()=>document.querySelector('.v2-main')?.animate([{opacity:.4},{opacity:1}],{duration:200,easing:'ease-out'}));
},{flush:'post'});
const componentKey = computed(() => identityKey.value + ':' + route.value.pathname + ':' + (['learning','work_handover','risk_inspection'].includes(activeModule.value) ? route.value.search : currentScope.value) + (['drills','critical_guard'].includes(activeModule.value)?':'+moduleAdminMode.value:'') + (componentName.value === 'cabinet_batch' ? ':' + (route.value.searchParams.get('mode') || '') + ':' + (route.value.searchParams.get('batch_id') || '') : ''));
const componentProps = computed(() => {
  const p = route.value.searchParams, admin = user.value.role === 'admin';
  const common = { scope:currentScope.value, scopeOptions:scopes.value, isAdmin:admin, checking:false, loggedIn:loggedIn.value, loginUrl:loginUrl.value };
  switch(componentName.value) {
    case 'work_handover': return {handoverId:p.get('id')||''};
    case 'risk_inspection': return {releaseId:p.get('id')||''};
    case 'event': return {...common, refreshNonce:0};
    case 'water': return {...common, initialView:p.get('water_view')==='records'?'records':'overview'};
    case 'repair_status':
    case 'repair_management': return {...common, userId:String(user.value.open_id || ''), canDelete:user.value.can_delete_business === true, focusRecordId:p.get('record_id') || ''};
    case 'learning': return {...common, scope:p.get('scope') || '', personId:p.get('person_id') || '', userId:String(user.value.open_id || '')};
    case 'critical_guard': return {...common, scope:moduleAdminMode.value||['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, user:user.value, adminMode:moduleAdminMode.value};
    case 'drills': return {...common, scope:moduleAdminMode.value||['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, currentUser:user.value, adminMode:moduleAdminMode.value, printMode:printMode.value};
    case 'cabinet': return {...common, scope:['ALL','CAMPUS'].includes(currentScope.value)?'':currentScope.value, userId:String(user.value.open_id || ''), readOnly:user.value.role === 'guest', canDelete:user.value.can_delete_business === true};
    case 'cabinet_batch': return {...common, userId:String(user.value.open_id || '')};
    case 'signature': return {defaultScope:currentScope.value};
    case 'accounts': return {open:true, systemAdmin:admin, initialTab:p.get('tab') || 'permissions', scopeOptions:scopes.value};
    case 'daily': return {...common, printMode:printMode.value};
    default:return common;
  }
});
async function loadAuth() {
  if (checkingAuth || disposed || logoutBusy.value) return;
  checkingAuth = true;
  const controller=new AbortController();authRead=controller;
  try {
    const data = await requestJson('/api/auth/status?next=' + encodeURIComponent(window.location.pathname + window.location.search), {signal:controller.signal, cache:'no-store',timeoutMs:15000});
    if (disposed || controller.signal.aborted || authRead!==controller) return;
    loggedIn.value = !!data.logged_in; user.value = data.user || {}; scopes.value = data.scope_options || []; authDefaultScope.value = String(data.default_scope||''); loginUrl.value = data.login_url || '/api/auth/login';
    setModulePermissions(granted.value); setReadCacheIdentity(identityKey.value); authError.value = data.directory_warning || '';
    if (loggedIn.value && user.value.role !== 'guest' && user.value.open_id) saveVersion(String(user.value.open_id), 'workspace');
    if (user.value.role === 'guest' && route.value.pathname !== '/cabinet-power' && !publicLink.value) navigate('/cabinet-power', {replace:true,bypassGuard:true});
  } catch (e:any) { if (!disposed && !controller.signal.aborted) authError.value = e.message || '登录状态读取失败'; }
  finally { if(authRead===controller){authRead=undefined;checking.value = false; checkingAuth = false; window.clearTimeout(timer); if (!disposed && loggedIn.value && !document.hidden) timer = window.setTimeout(loadAuth,60000);} }
}
async function logout() {
  if(logoutBusy.value)return;logoutBusy.value=true;operationError.value='';cancelAuth();
  try { await requestJson('/api/auth/logout',{method:'POST',body:'{}'}); invalidateReadCache(); setModulePermissions([]); loggedIn.value=false; user.value={}; scopes.value=[]; navigate('/',{replace:true,bypassGuard:true}); }
  catch(e){operationError.value=e instanceof Error?e.message:'退出失败，请重试。';}
  finally{logoutBusy.value=false;if(loggedIn.value&&!disposed&&!document.hidden)timer=window.setTimeout(loadAuth,60000);}
}
function expired(event: Event) { (event as CustomEvent).preventDefault?.(); cancelAuth();checking.value=false;loggedIn.value=false; user.value={}; scopes.value=[]; setModulePermissions([]); invalidateReadCache(); if(!publicLink.value)navigate('/?login=password&next=' + encodeURIComponent(window.location.pathname + window.location.search), {replace:true,bypassGuard:true}); }
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
watch([view,loggedIn,identityKey],async(_value,_old,onCleanup)=>{
  handovers.value={};handoverLoading.value=false;
  if(view.value!=='handover'||!loggedIn.value||!granted.value.includes('handover'))return;
  const controller=new AbortController();onCleanup(()=>controller.abort());handoverLoading.value=true;
  try{const data=await requestJson('/api/handover-links',{signal:controller.signal});if(!controller.signal.aborted)handovers.value=data.links||{};}
  catch(e:any){if(!controller.signal.aborted)authError.value=e.message;}
  finally{if(!controller.signal.aborted)handoverLoading.value=false;}
});
onMounted(()=>{void loadAuth();window.addEventListener('popstate',routeChanged);window.addEventListener('auth-account-changed',refreshAuth);window.addEventListener('workspace-permissions-stale',refreshAuth);window.addEventListener(AUTH_EXPIRED_EVENT,expired);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;cancelAuth();window.removeEventListener('popstate',routeChanged);window.removeEventListener('auth-account-changed',refreshAuth);window.removeEventListener('workspace-permissions-stale',refreshAuth);window.removeEventListener(AUTH_EXPIRED_EVENT,expired);document.removeEventListener('visibilitychange',visibility);});
</script>
<style scoped>
.v2-my-signatures { margin-top: 28px; }
@media (max-width: 760px) { .v2-my-signatures { margin-top: 20px; } }
</style>
