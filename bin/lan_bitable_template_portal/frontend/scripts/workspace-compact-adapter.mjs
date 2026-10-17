import {parse} from '@vue/compiler-sfc';

const classes=node=>(node.props?.find(p=>p.name==='class'&&p.type===6)?.value?.content||'').split(/\s+/);
function nodes(source,predicate){const result=[];function visit(node){if(node.type===1&&predicate(node))result.push(node);for(const child of node.children||[])visit(child);}visit(parse(/^\s*<template(?:\s|>)/.test(source)?source:'<template>'+source+'</template>').descriptor.template?.ast||{});return result;}
function replace(source,predicate,change){const found=nodes(source,predicate);if(found.length!==1)throw new Error('Compact page anchor changed: '+found.length);const node=found[0];return source.slice(0,node.loc.start.offset)+(typeof change==='function'?change(node.loc.source,node):change)+source.slice(node.loc.end.offset);}
const byClass=name=>node=>classes(node).includes(name);
const host=page=>`<span class="v2-toolbar-end" data-v2-layout-tools="${page}"></span>`;
function slot(source,tag,content){return replace(source,node=>node.tag===tag,(text)=>text.replace(/\s*\/>$/,`>${content}</${tag}>`));}

// Build-only rearrangement keeps the original bindings and business controllers.
export function adaptCompactWorkspaces(input,filename){
  const file=filename.replaceAll('\\','/').split('/').pop();let source=input.replaceAll('\r\n','\n');
  if(source.includes('v2-compact-adapted'))return source;
  if(file==='EventManagementPage.vue'){
    source=replace(source,byClass('event-command-head'),'');
    source=replace(source,node=>node.tag==='EventPeriodFilter',text=>`<div class="v2-event-controls">${text}</div>`);
  }
  if(file==='NoticeWorkspace.vue'){
    const header=nodes(source,byClass('v2-notice-heading'))[0];
    const tools=nodes(header.loc.source,byClass('v2-notice-tools'))[0].loc.source;
    source=replace(source,byClass('v2-notice-heading'),'');
    source=replace(source,byClass('v2-notice-types'),text=>`<div class="v2-command-row v2-notice-command">${text}${tools}${host('notices')}</div>`);
  }else if(file==='NoticeSopManager.vue'){
    source=source.replace(' title="SOP 与工单模板"',' title=" "');
    const search=nodes(source,node=>node.tag==='NInput'&&node.loc.source.includes('v-model:value="search"'))[0].loc.source;
    source=replace(source,node=>node.tag==='NInput'&&node.loc.source===search,'');
    source=replace(source,byClass('v2-sop-filters'),text=>text.replace('</div>',host('sops')+'</div>'));
    source=replace(source,node=>node.tag==='aside',text=>text.replace('>',`><div class="v2-sop-list-search" data-v2-filter>${search}</div>`));
  }else if(file==='EngineerMopPage.vue'){
    const header=nodes(source,node=>node.tag==='MopPageHeader')[0].loc.source;
    source=replace(source,node=>node.tag==='MopPageHeader','');
    source=replace(source,node=>node.tag==='MopFlowSteps','');
    source=slot(source,'MopBindingWorkspace',`<template #tools>${header}</template>`);
    const signatures=nodes(source,node=>node.tag==='MopSignatureRoleSummary')[0].loc.source;
    source=replace(source,byClass('mop-sign-panel'),text=>text.replace('v-if="activeSheet && !activeSheet.is_cover"','v-if="activeSheet && !activeSheet.is_cover && signatureManagerOpen"'));
    source=slot(source,'MopUploadFooter',`<template #signatures><div v-if="activeSheet && !activeSheet.is_cover" class="mop-footer-signatures" :inert="mopFillSaving || mopUploadSaving || mopResetting">${signatures}</div></template>`);
  }else if(file==='MopPreviewHeader.vue'){
    source=replace(source,byClass('preview-head-status'),'');
    source=source.replace('<strong>{{ title }}</strong>','<strong :title="title">{{ title }}</strong>');
  }else if(file==='MopSheetStatusPanel.vue'){
    source=replace(source,byClass('mop-file-status'),'');
  }else if(file==='MopUploadFooter.vue'){
    source=replace(source,byClass('floating-upload-action-row'),text=>text.replace('>','><slot name="signatures" />'));
  }else if(file==='MopBindingWorkspace.vue'){
    source=slot(source,'MopNoticeList','<template #tools><slot name="tools" /></template>');
  }else if(file==='MopNoticeList.vue'){
    source=replace(source,node=>node.tag==='aside',text=>text.replace('>','><slot name="tools" />'));
  }else if(file==='MopPageHeader.vue'){
    source=replace(source,node=>node.tag==='div'&&node.loc.source.includes('<strong>工程师 MOP 填写</strong>'),'');
  }else if(file==='MopSummaryStrip.vue'){
    source=replace(source,byClass('mop-summary'),text=>text.replace('</section>',host('mop')+'</section>'));
  }else if(file==='RepairWorkspace.vue'){
    const actions=nodes(source,byClass('v2-repair-actions'))[0].loc.source;
    source=replace(source,byClass('v2-repair-heading'),'');
    source=replace(source,byClass('v2-repair-filters'),text=>`<div class="v2-command-row v2-repair-command">${text}${actions}${host('repair')}</div>`);
    const health=nodes(source,node=>node.tag==='NAlert'&&node.loc.source.includes('v-else-if="r.repairHealthVisible"'))[0];
    if(health){source=replace(source,node=>node.loc.source===health.loc.source,'');source=replace(source,byClass('v2-repair-state-row'),text=>text.replace(/<\/div>$/,health.loc.source.replace('v-else-if="r.repairHealthVisible"','v-if="r.repairHealthVisible&&!r.v2RecordError" class="v2-repair-inline-health"')+'</div>'));}
  }else if(file==='CriticalGuardPage.vue'){
    const summary=nodes(source,byClass('admin-toolbar'))[0].loc.source;
    source=replace(source,byClass('admin-toolbar'),'');
    source=replace(source,byClass('guard-page__header'),text=>text.replace(/<div>\s*<span>风险管理<\/span>[\s\S]*?<\/div>/,'').replace('</header>',summary.replace('class="admin-toolbar"','v-if="viewMode===\'admin\'" class="admin-toolbar"')+host('guard-admin')+host('guard-building')+'</header>'));
    source=source.replace('v-if="isAdmin" data-v2-navigation class="v2-module-admin"','v-if="isAdmin && viewMode!==\'admin\'" data-v2-navigation class="v2-module-admin"');
  }else if(file==='DrillManagementPage.vue'){
    source=source.replace('  ChevronRight,','  ChevronRight,\n  ChevronLeft,');
    const header=nodes(source,byClass('drill-page__header'))[0];
    const buttons=nodes(header.loc.source,node=>node.tag==='button'||node.tag==='V2AdminButton').map(node=>node.loc.source).join('');
    source=replace(source,byClass('drill-page__header'),'');
    source=replace(source,byClass('building-toolbar'),text=>text.replace(/<\/div>$/,`<div class="v2-command-actions">${buttons}</div></div>`));
    const back='<button type="button" data-v2-navigation :disabled="busy" @click="requestDiscardChanges(\'返回将放弃未保存修改。\',()=>openBuilding(String(landingScopes.find(item=>item.authorized)?.value||\'A\')))"><ChevronLeft :size="16" />返回楼栋演练</button>';
    source=replace(source,byClass('command-bar'),text=>text.replace(/<\/div>$/,`<div class="v2-command-actions">${back}${buttons}</div></div>`));
    source=source.replaceAll('v-if="isAdmin" data-v2-navigation class="v2-module-admin"','v-if="isAdmin && viewMode!==\'admin\'" data-v2-navigation class="v2-module-admin"');
  }else if(file==='EventManagementPage.vue'){
    source=replace(source,node=>node.tag==='VnetBackButton'&&node.loc.source.includes('returnToOverview'),'');
    source=replace(source,node=>node.tag==='VnetBackButton'&&node.props.some(p=>p.name==='else'),'');
  }else if(file==='PlanConvergencePage.vue'){
    const settings=nodes(source,node=>node.tag==='button'&&node.loc.source.includes('@click="openSettings"'))[0].loc.source;
    const refresh=nodes(source,node=>node.tag==='button'&&node.loc.source.includes('@click="loadBlocks"'))[0].loc.source;
    source=replace(source,byClass('pc-head'),'');
    source=replace(source,byClass('pc-tabs'),text=>`<div class="v2-command-row v2-plan-command">${text}<div class="v2-command-actions">${settings}<span class="v2-toolbar-end" :data-v2-layout-tools="tab==='review'?'plan-review':tab==='rules'?'plan-rules':'plan-audit'"></span></div></div>`);
    source=replace(source,node=>classes(node).includes('pc-section-head')&&node.loc.source.includes('filteredBlocks.length'),'');
    source=source.replace('<aside class="pc-list">',`<aside class="pc-list"><div class="v2-plan-record-head"><h3>屏蔽记录 <small>{{filteredBlocks.length}}</small></h3><span class="pc-badge">屏蔽中 {{activeCount}}</span>${refresh}</div><small v-if="loadedAt" class="muted">更新于 {{timeText(loadedAt)}}</small>`);
    const status=nodes(source,node=>node.tag==='VnetSelect'&&node.loc.source.includes('input-id="pc-block-status"'))[0];
    if(status){source=replace(source,node=>node.loc.source===status.loc.source,'');source=replace(source,byClass('v2-plan-record-head'),text=>text.replace(refresh,status.loc.source+refresh));}
    source=source.replace('v-show="tab === \'review\'"','v-if="tab === \'review\'"').replace('v-if="visitedRules" v-show="tab === \'rules\'"','v-if="tab === \'rules\'"');
    source=replace(source,node=>node.tag==='div'&&classes(node).includes('pc-alert')&&node.loc.source.includes('智航查询需要运行灯塔'),'');
    const old=source.match(/function switchTab\(value:Tab\)\{[^\n]+/)[0];
    source=source.replace(old,`function v2ApplyPlanTab(value:Tab){noticeJumpRequest++;tab.value=value;if(value==='rules'){visitedRules.value=true;void loadRuleSets();}if((value==='maintenance'||value==='change')&&!noticeAudits[value].visited)void loadNoticeAudit(value);}
function switchTab(value:Tab){const url=new URL(location.href);url.searchParams.set('tab',value);navigate(url);}
function v2PlanRoute(){if(loading.value)return;const value=new URL(location.href).searchParams.get('tab')||'review';if(tabs.some(item=>item.id===value)&&value!==tab.value)v2ApplyPlanTab(value as Tab);}
onMounted(()=>window.addEventListener('popstate',v2PlanRoute));onBeforeUnmount(()=>window.removeEventListener('popstate',v2PlanRoute));`);
    source=source.replace('if(tabs.some(t=>t.id===initial))switchTab(initial);','if(tabs.some(t=>t.id===initial))v2ApplyPlanTab(initial);');
    source=source.replace("if(new URL(target).pathname==='/plan-convergence'||!hasUnsaved())return true;","if((new URL(target).pathname==='/plan-convergence'&&(new URL(target).searchParams.get('tab')||'review')===tab.value)||!hasUnsaved())return true;");
  }else return input;
  return source.replace('<script setup lang="ts">','<script setup lang="ts">\n// v2-compact-adapted');
}
