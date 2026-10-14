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
  'PlanConvergencePage.vue': ['pc-list-filters', 'pc-toolbar'],
  'LearningPage.vue': ['toolbar'],
};

export function adaptPageFilters(source, filename) {
  const file = filename.replaceAll('\\', '/').split('/').pop();
  const allowed = containers[file];
  if (!allowed) return source;
  if (file === 'PlanConvergencePage.vue') {
    source = source.replace('<button v-for="item in tabs"', '<button data-v2-navigation v-for="item in tabs"');
  }
  const { descriptor } = parse(source), edits = [];
  const hasSearch = node => node.props?.some(p => p.type === 6 && p.name === 'type' && p.value?.content === 'search') || node.children?.some(hasSearch);
  function visit(node, overlay = false) {
    const classes = (node.props?.find(p => p.type === 6 && p.name === 'class')?.value?.content || '').split(/\s+/);
    overlay ||= ['NModal', 'NDrawer', 'dialog'].includes(node.tag) || classes.some(c => ['scrim', 'modal-backdrop', 'modal-layer', 'overlay'].includes(c));
    const eligible = (file !== 'LearningPage.vue' || node.tag === 'form')
      && (!classes.includes('pc-toolbar') || hasSearch(node));
    if (file === 'DrillManagementPage.vue' && classes.includes('building-toolbar')) {
      for (const child of node.children || []) {
        if (child.type === 1 && child.tag === 'label') edits.push([child.loc.start.offset, child.loc.end.offset, `<Teleport :to="__v2FilterTarget() || 'body'" :disabled="!__v2FilterTarget()">${child.loc.source}</Teleport>`]);
      }
      return;
    }
    if (node.type === 1 && !overlay && eligible && classes.some(c => allowed.includes(c))) {
      const active = file === 'NoticeWorkspace.vue' ? ' v-if="active"'
        : file === 'PlanConvergencePage.vue' && classes.includes('pc-list-filters') ? ' v-if="tab === \'review\'"' : '';
      const content = file === 'PlanConvergencePage.vue'
        ? node.loc.source.replace('class="', 'class="v2-plan-filters ') : node.loc.source;
      edits.push([node.loc.start.offset, node.loc.end.offset, `<Teleport${active} :to="__v2FilterTarget() || 'body'" :disabled="!__v2FilterTarget()">${content}</Teleport>`]);
      return;
    }
    for (const child of node.children || []) visit(child, overlay);
  }
  visit(descriptor.template?.ast || {});
  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0])) source = source.slice(0, start) + text + source.slice(end);
  if (edits.length) source = source.replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { pageFilterTarget as __v2FilterTarget } from "../workspace-v2/pageFilters";');
  return source;
}
