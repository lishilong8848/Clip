import { parse } from '@vue/compiler-sfc';

const containers = {
  'RepairWorkspace.vue': ['v2-repair-filters'],
  'NoticeWorkspace.vue': ['v2-notice-filters'],
  'NoticeSopManager.vue': ['v2-sop-filters'],
  'WaterWorkspace.vue': ['water-scope', 'water-filter-main', 'water-date-filter', 'water-search', 'water-filter-extra'],
  'EventManagementPage.vue': ['event-list-tools'],
  'CabinetPowerPage.vue': ['filter-bar', 'search'],
  'DrillManagementPage.vue': ['admin-month-filter'],
  'MopPageHeader.vue': ['scope-select'],
  'DailyTaskChecklistPage.vue': ['date-toolbar'],
  'KnowledgeBasePage.vue': ['kb-toolbar'],
  'LinkDirectoryPage.vue': ['toolbar'],
  'SignatureManagementPage.vue': ['filters'],
  'HistoryMemoryPage.vue': ['filters'],
  'PlanConvergencePage.vue': ['pc-list-filters', 'pc-toolbar', 'v2-plan-record-head'],
  'LearningPage.vue': ['toolbar'],
};

const localFilters = {
  'MetricWidget.vue': ['v2-metric-toolbar'],
  'PreShiftCheck.vue': ['v2-check-toolbar'],
  'EventPeriodFilter.vue': ['v2-event-period'],
  'NoticeWorkspace.vue': ['v2-notice-types', 'v2-notice-mobile-tabs'],
  'RepairWorkspace.vue': ['v2-repair-state-row'],
  'RepairFollowups.vue': ['v2-repair-history-search'],
  'PermissionReview.vue': ['v2-review-filters'],
  'AdminAccounts.vue': ['filters'],
  'AdminPermissionUsers.vue': ['permission-user-search'],
  'AdminPermissionDirectory.vue': ['directory-toolbar'],
  'MopNoticeList.vue': ['filters'],
  'CabinetPowerBatchPage.vue': ['text-toolbar', 'filters', 'filter-bar'],
  'DrillManagementPage.vue': ['building-toolbar'],
  'LearningDashboard.vue': ['table-head', 'segments'],
  'RecordPickerDialog.vue': ['record-picker-toolbar', 'record-picker-filters'],
  'RepairLedgerPicker.vue': ['ledger-filters'],
  'RepairPeoplePicker.vue': ['repair-people-search'],
  'LighthouseNoticePanel.vue': ['notice-filters'],
  'LighthouseKnowledge.vue': ['lk-search'],
  'LighthouseSkills.vue': ['skill-filters'],
  'LighthouseTools.vue': ['extension-toolbar'],
};

function markFilterStyles(source, file, allowed) {
  const names = [...allowed, ...(localFilters[file] || [])], edits = [];
  const { descriptor } = parse(source);
  function visit(node) {
    const classes = (node.props?.find(p => p.type === 6 && p.name === 'class')?.value?.content || '').split(/\s+/);
    const hasMark = node.props?.some(p => p.type === 6 && p.name === 'data-v2-filter');
    const eligible = file !== 'LearningPage.vue' || node.tag === 'form';
    if (node.type === 1 && eligible && classes.some(c => names.includes(c)) && !hasMark) {
      edits.push([node.loc.start.offset + 1 + node.tag.length, ' data-v2-filter']);
    }
    if (node.type === 1 && node.tag === 'input' && node.props?.some(p => p.type === 6 &&
        (p.name === 'type' && p.value?.content === 'search' || p.name === 'placeholder' && /^(搜索|查找|筛选)/.test(p.value?.content || '')))) {
      if (!node.props.some(p=>p.type===6&&p.name==='data-v2-filter-control')) edits.push([node.loc.start.offset + 1 + node.tag.length, ' data-v2-filter-control']);
    }
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template?.ast || {});
  for (const [offset, attribute] of edits.sort((a,b)=>b[0]-a[0])) source = source.slice(0,offset) + attribute + source.slice(offset);
  return source;
}

export function adaptPageFilters(source, filename) {
  const file = filename.replaceAll('\\', '/').split('/').pop();
  const allowed = containers[file] || [];
  if (file === 'PlanConvergencePage.vue') source = source.replace('<button v-for="item in tabs"', '<button data-v2-navigation v-for="item in tabs"');
  const { descriptor } = parse(source), edits = [];
  const anchors={'TaskWorkspace.vue':'v2-section-title','RepairWorkspace.vue':'v2-repair-filters','EventManagementPage.vue':'v2-event-controls','CabinetPowerPage.vue':'page-heading','CabinetPowerBatchPage.vue':'page-heading','CriticalGuardPage.vue':'guard-page__header'};
  let scopeAdded=false;
  function visit(node, overlay = false) {
    const classes = (node.props?.find(p => p.type === 6 && p.name === 'class')?.value?.content || '').split(/\s+/);
    overlay ||= ['NModal', 'NDrawer', 'dialog'].includes(node.tag) || classes.some(c => ['scrim', 'modal-backdrop', 'modal-layer', 'overlay'].includes(c));
    if(!overlay&&!scopeAdded&&classes.includes(anchors[file])){
      const end=Math.max(node.tag.length+1,...node.props.map(p=>p.loc.end.offset-node.loc.start.offset));
      const offset=node.loc.start.offset+node.loc.source.indexOf('>',end)+1;
      edits.push([offset,offset,file==='TaskWorkspace.vue'?'<WorkspaceScopePicker v-if="!embedded" />':'<WorkspaceScopePicker />']);scopeAdded=true;
    }
    if(node.tag==='h1'&&!overlay&&!node.props.some(p=>p.name==='data-v2-module-title')&&(['CabinetPowerPage.vue','CriticalGuardPage.vue','DrillManagementPage.vue','DailyTaskChecklistPage.vue','AdminTools.vue'].includes(file)||/^(?:画像学练|检修办理|水耗管理|机柜管理|演练管理|风险与重保|重保管理|共享知识库|多维表导航|签名管理|统一签名管理|每日任务|历史记忆|计划收敛审查|账号与权限|管理员设置|通告办理|个人工作台)$/.test(node.children?.map(n=>n.content||'').join('').trim())||node.loc.source.includes('personaTitles'))){
      edits.push([node.loc.start.offset+3,node.loc.start.offset+3,' data-v2-module-title']);
    }
    for (const child of node.children || []) visit(child, overlay);
  }
  visit(descriptor.template?.ast || {});
  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0])) source = source.slice(0, start) + text + source.slice(end);
  if(scopeAdded)source=source.replace('<script setup lang="ts">','<script setup lang="ts">\nimport WorkspaceScopePicker from "../workspace-v2/ScopePicker.vue";');
  return markFilterStyles(source, file, allowed);
}
