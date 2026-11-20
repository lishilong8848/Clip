import { parse } from '@vue/compiler-sfc';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const panels = {
  MopNoticeList: 'MopDeskNotices.html',
  MopBindingPanel: 'MopDeskFiles.html',
  MopSelectedFileCard: 'MopDeskAttachment.html',
};

// Apply after the compact, filter and PageCanvas adapters, before single-select
// bridging. The controller, refs, event handlers and editor guards stay live.
export function adaptProcedureDesign(source, filename, watchFile) {
  const path = filename.replaceAll('\\','/');
  const name = path.split('/').pop().replace(/\.vue$/,'');
  if (!path.endsWith('.vue') || source.includes('<!-- procedure-desk -->')) return source;
  const shared = path.includes('/src/components/');
  // Vite installs the matching views/*.html after this hook. Keep the
  // original event controllers and add only their V2 presentation bindings.
  if (shared && ['EventNoticeRawBoard','EventNoticeRawCard'].includes(name)) {
    const setup = parse(source).descriptor.scriptSetup;
    if (!setup) throw new Error('Missing event controller: '+name);
    const view = name === 'EventNoticeRawBoard'
      ? "import EventDeskBrowser from '../workspace-v2/EventDeskBrowser.vue';\nimport '../workspace-v2/EventDesk.css';"
      : "const composeExpanded=ref(Boolean(props.creation && (text.value.trim() || frozen.value)));\nwatch(()=>props.options.can_delete,allowed=>{if(!allowed)deleteOpen.value=false;});";
    const bindings = "\nimport EventDeskDialog from '../workspace-v2/EventDeskDialog.vue';\n"+view+"\n";
    return '<!-- procedure-desk -->\n'+source.slice(0,setup.loc.end.offset)+bindings+source.slice(setup.loc.end.offset);
  }
  const followups = path.endsWith('/src/workspace-v2/RepairFollowups.vue');
  const history = path.endsWith('/src/workspace-v2/RepairHistoryPicker.vue');
  if (!(shared && (panels[name] || ['EngineerMopPage','MopBindingWorkspace'].includes(name))) && !followups && !history) return source;

  const { descriptor } = parse(source);
  const template = descriptor.template;
  if (!template?.ast || !descriptor.scriptSetup) throw new Error('Missing procedure template or controller: '+name);
  const changes = [];
  const elements = (node) => (node.children || []).filter(child => child.type === 1);
  function find(node, predicate) {
    if (node.type === 1 && predicate(node)) return node;
    for (const child of elements(node)) {
      const result = find(child, predicate);
      if (result) return result;
    }
  }
  const byClass = (value) => node => node.props.some(p => p.type===6 && p.name==='class' && p.value?.content.split(/\s+/).includes(value));
  function required(predicate, label, parent=template.ast) {
    const result = find(parent, predicate);
    if (!result) throw new Error(name+' procedure layout requires '+label);
    return result;
  }
  const replace = (node, value) => changes.push([node.loc.start.offset,node.loc.end.offset,value]);
  if (panels[name]) {
    const view = fileURLToPath(new URL('../src/workspace-v2/'+panels[name],import.meta.url));
    watchFile?.(view);
    changes.push([template.loc.start.offset,template.loc.end.offset,'\n'+readFileSync(view,'utf8').trim()+'\n']);
  } else if (name==='MopBindingWorkspace') {
    // Keep the existing PageCanvas including saved block ids, models, mobile
    // selection and events. Move the scope/month/refresh slot to the title.
    const canvas = required(n=>n.tag==='PageCanvas','mop PageCanvas');
    const tools = required(n=>n.tag==='slot' && n.props.some(p=>p.name==='name' && p.value?.content==='tools'),'tools slot',canvas);
    replace(tools,'');
    const summary = required(n=>n.tag==='MopSummaryStrip','monthly counts');
    replace(summary,`<header class="mop-desk-bar"><h1>维保单办理</h1><slot name="tools" /><span data-v2-layout-tools="mop"></span></header>
    <dl class="mop-desk-counts" :class="{'v2-mop-summary-in-work':v2MopWorkOpen && selectedNotice}">
      <div><dt>{{ noticeMonth }}维保</dt><dd>{{ noticesCount }}</dd></div><div><dt>待绑定或待上传</dt><dd>{{ pending }}</dd></div>
      <div><dt>已绑定</dt><dd>{{ bound }}</dd></div><div><dt>已上传维护单</dt><dd>{{ uploaded }}</dd></div><div><dt>MOP 表格</dt><dd>{{ mopFiles }}</dd></div>
    </dl>`);
  } else if (name==='EngineerMopPage') {
    const panel=required(byClass('mop-sign-panel'),'signature dialog');
    const heading=required(byClass('sign-panel-head'),'dialog heading',panel);
    const roles=required(n=>n.tag==='MopSignatureRoleSummary','signature roles',panel);
    const tabs=required(byClass('signature-source-tabs'),'signature sources',panel);
    replace(heading,'<div class="mop-desk-sign-head">'+heading.loc.source+roles.loc.source+'</div>');
    replace(roles,'');
    replace(tabs,'<div class="mop-desk-sign-sources">'+tabs.loc.source+'</div>');
    const sheets=required(n=>n.tag==='MopSheetTabs','workbook sheets');
    const status=required(n=>n.tag==='MopSheetStatusPanel','sheet status');
    replace(status,'');
    replace(sheets,'<div class="mop-desk-sheetbar">'+sheets.loc.source+status.loc.source+'</div>');
  } else if (followups) {
    const devices=required(byClass('v2-repair-devices'),'ledger selection');
    const group=required(byClass('v2-repair-followup-section'),'followup field groups');
    const cmdb=required(n=>n.tag==='NCollapse' && n.loc.source.includes('openCmdbPicker'),'CMDB selection');
    replace(devices,'<div class="repair-desk-followup-equipment">'+devices.loc.source+cmdb.loc.source+'</div>');
    replace(group,'<div class="repair-desk-followup-grid">'+group.loc.source+'</div>');
    replace(cmdb,'');
  } else if (history) {
    const modal=required(n=>n.tag==='NModal','history dialog');
    const search=required(n=>n.tag==='NInput','history search',modal);
    const body=modal.loc.source.replace(search.loc.source,'').replace('title="历史跟进"','class="repair-desk-history" aria-label="历史跟进"')
      .replace("min(520px, calc(100vw - 32px))","min(760px, calc(100vw - 32px))")
      .replace('<section class="v2-repair-history-picker"', '<template #header><div class="repair-desk-history-bar"><strong>历史跟进</strong>'+search.loc.source+'</div></template><section class="v2-repair-history-picker"');
    replace(modal,body);
  }
  for (const [start,end,value] of changes.sort((a,b)=>b[0]-a[0])) source=source.slice(0,start)+value+source.slice(end);
  return '<!-- procedure-desk -->\n'+source;
}

