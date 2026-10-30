// V2-only presentation adapter for the account settings (AdminTools + AdminAccounts)
// and the signature management page. It runs AFTER the existing admin/filter/
// page-layout transforms so it operates on the already-wrapped PageCanvas source.
//
// The integration contract expected by the main workspace-v2 config:
//   import { adaptAccountPresentation } from './scripts/workspace-account-presentation-adapter.mjs';
//   ...
//   result = adaptAccountPresentation(result, path);   // placed after adaptPageLayouts
//
// The adapter is idempotent: running it twice on the same source returns the
// already-adapted source unchanged (it guards on a marker comment).

import { parse } from '@vue/compiler-sfc';

export const afterTransformsOrder = 20;
export const toleratesBuildFailures = false;
export const adaptedFiles = ['AdminTools.vue', 'AdminAccounts.vue', 'SignatureManagementPage.vue'];

const MARKER = 'account-presentation-adapted';
const CSS_IMPORT = "import '../workspace-v2/accountPresentation.css';";
const HIERARCHY_IMPORT = "import AccountHierarchy from '../workspace-v2/AccountHierarchy.vue';";

const cls = node => (node.props?.find(p => p.type === 6 && p.name === 'class')?.value?.content || '').split(/\s+/);
const has = (node, value) => node.type === 1 && (node.tag === value || cls(node).includes(value));

function walk(node, visit) {
  if (!node) return;
  visit(node);
  for (const child of node.children || []) walk(child, visit);
}

function patchScript(source, code) {
  const anchor = '<script setup lang="ts">';
  if (!source.includes(anchor)) throw new Error('account-presentation: script setup anchor missing');
  const insert = `\n// ${MARKER}\n${code}`;
  return source.replace(anchor, anchor + insert);
}

function classPropEdit(node, extraClass) {
  const prop = node.props?.find(p => p.type === 6 && p.name === 'class');
  if (prop?.value) return [prop.value.loc.start.offset + 1, prop.value.loc.end.offset - 1, prop.value.content + ' ' + extraClass];
  return [node.loc.start.offset + 1 + node.tag.length, node.loc.start.offset + 1 + node.tag.length, ` class="${extraClass}"`];
}

// Move existing controls verbatim so permission guards and handlers stay owned
// by the shared component. Only the V2 template around them changes.
function inside(node) {
  return node.loc.source.slice(node.loc.source.indexOf('>') + 1, node.loc.source.lastIndexOf('</'));
}

function filterByModel(filters, model) {
  const label = filters.children.find(node => node.type === 1 && node.tag === 'label' &&
    node.loc.source.includes(`v-model="${model}"`));
  if (!label) throw new Error('account-presentation: filter changed: ' + model);
  return label.loc.source;
}

function accountCards(table) {
  let row = null;
  walk(table, node => {
    if (node.tag === 'tr' && node.props?.some(p => p.type === 7 && p.name === 'for' && p.exp?.content === 'item in pageItems')) row = node;
  });
  const cells = row?.children.filter(node => node.type === 1 && node.tag === 'td') || [];
  if (cells.length !== 11 || !has(cells[0], 'account-select-cell') || !has(cells[10], 'account-actions')) {
    throw new Error('account-presentation: account row changed');
  }
  const content = cells.map(inside);
  const loginCondition = cells[9].props.find(p => p.type === 7 && p.name === 'if')?.loc.source;
  if (!loginCondition) throw new Error('account-presentation: login visibility changed');
  return `<div class="v2-account-mobile-list" aria-label="人员账号列表">
    <article v-for="item in pageItems" :key="item.id" class="v2-account-person-card" :aria-label="item.name">
      <header class="v2-account-person-heading">
        <div class="account-select-cell">${content[0]}</div>
        <div class="v2-account-person-name">${content[1]}</div>
        <div class="v2-account-person-state">${content[8]}</div>
      </header>
      <dl class="v2-account-person-summary">
        <div><dt>所属楼栋</dt><dd>${content[4]}</dd></div>
        <div><dt>权限级别</dt><dd>${content[5]}</dd></div>
        <div class="v2-account-person-scopes"><dt>可访问楼栋</dt><dd>${content[7]}</dd></div>
      </dl>
      <div class="account-actions v2-account-person-actions">${content[10]}</div>
      <details class="v2-account-person-details">
        <summary>更多人员信息</summary>
        <dl>
          <div><dt>岗位分类</dt><dd>${content[2]}</dd></div>
          <div><dt>类型</dt><dd>${content[3]}</dd></div>
          <div><dt>角色</dt><dd>${content[6]}</dd></div>
          <div ${loginCondition}><dt>最近登录</dt><dd>${content[9]}</dd></div>
        </dl>
      </details>
    </article>
    <p v-if="!filtered.length" class="v2-account-empty">没有匹配的账号</p>
  </div>`;
}

/* ------------------------------------------------------------------ */
/* AdminAccounts: flat list (default, original table) + tree view      */
/* ------------------------------------------------------------------ */
function adaptAdminAccounts(source) {
  const { descriptor } = parse(source);
  let loading = null, footer = null, filters = null, header = null, bulk = null;
  const tables = [];
  walk(descriptor.template.ast, node => {
    if (node.type !== 1) return;
    if (node.tag === 'LoadingIndicator') loading = loading || node;
    if (has(node, 'table-wrap')) tables.push(node);
    if (has(node, 'pagination')) footer = footer || node;
    if (has(node, 'filters')) filters = node;
    if (has(node, 'account-header')) header = node;
    if (has(node, 'account-bulk-bar')) bulk = node;
  });
  if (!loading || !tables.length || !footer || !filters || !header || !bulk) throw new Error('AdminAccounts list region changed');
  // pick the table-wrap that belongs to the loading→pagination block (the first one)
  const table = tables[0];
  if (table.loc.start.offset < loading.loc.end.offset || footer.loc.start.offset < table.loc.end.offset) {
    throw new Error('AdminAccounts region ordering changed');
  }

  const tableSource = table.loc.source.replace('<div v-else class="table-wrap">', '<div class="table-wrap v2-account-desktop-table">');
  if (tableSource === table.loc.source) throw new Error('AdminAccounts table wrapper changed');
  const bar = `
      <template v-else>
        <div class="v2-account-mode-bar" role="tablist" aria-label="账号视图切换">
          <button type="button" role="tab" :aria-selected="v2AccountMode === 'list'" @click="v2AccountMode = 'list'">列表</button>
          <button type="button" role="tab" :aria-selected="v2AccountMode === 'tree'" @click="v2AccountMode = 'tree'">树形分组</button>
        </div>
        <template v-if="v2AccountMode === 'list'">
`;
  const tree = `
        </template>
        <AccountHierarchy v-else :accounts="filtered" :directory="items" :mode="'tree'" :show-toggle="false" :level-label="levelLabel" :building-label="value=>scopesLabel([value])" :category-label="categoryLabel" @select="openAccount" />
      </template>
`;
  const region = loading.loc.source + bar + tableSource + accountCards(table) + footer.loc.source + tree;

  const search = filters.children.find(node => has(node, 'search'));
  const title = header.children.find(node => node.tag === 'strong' && inside(node) === '账号与权限');
  const advanced = filters.children.filter(node => node.type === 1 && node !== search);
  if (!search || !title || advanced.length !== 5) throw new Error('AdminAccounts header/filters changed');
  const filterSource = `<div class="v2-account-filter-bar">
    <div class="filters v2-account-search" data-v2-filter>${search.loc.source}</div>
    <details class="v2-account-advanced">
      <summary>高级筛选<span v-if="kind || building || state || v2Level || category"> · 已选 {{ [kind, building, state, v2Level, category].filter(Boolean).length }} 项</span></summary>
      <div class="filters v2-account-advanced-fields" data-v2-filter>${advanced.map(node => node.loc.source).join('\n')}</div>
    </details>
  </div>`;

  const bulkButtons = bulk.children.filter(node => node.type === 1 && node.tag === 'button');
  const bulkLimit = bulk.children.find(node => has(node, 'account-bulk-limit'));
  const limitCondition = bulkLimit?.props.find(p => p.type === 7 && p.name === 'if')?.loc.source;
  if (bulkButtons.length !== 2 || !bulkLimit || !limitCondition) throw new Error('AdminAccounts bulk toolbar changed');

  const edits = [
    [loading.loc.start.offset, footer.loc.end.offset, region],
    [title.loc.start.offset, title.loc.end.offset, ''],
    [filters.loc.start.offset, filters.loc.end.offset, filterSource],
    [bulkButtons[0].loc.start.offset, bulkButtons[1].loc.end.offset,
      `<div v-if="selectedIds.size > 0" class="v2-account-bulk-actions">${bulkButtons.map(node => node.loc.source).join('\n')}</div>`],
    [bulkLimit.loc.start.offset, bulkLimit.loc.end.offset,
      `<details ${limitCondition} class="v2-account-bulk-help"><summary>批选说明</summary>${bulkLimit.loc.source}</details>`],
  ];
  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0])) source = source.slice(0, start) + text + source.slice(end);

  source = patchScript(source, `${HIERARCHY_IMPORT}\n${CSS_IMPORT}`);
  source = source.replace(
    "const v2Level=ref('');",
    "const v2Level=ref('');\nconst v2AccountMode = ref<'list' | 'tree'>('list');",
  );
  return source;
}

/* ------------------------------------------------------------------ */
/* AdminTools: compact header toolbar (back + advanced diagnostics +   */
/* stable PageCanvas layout anchor for the accounts page).             */
/* ------------------------------------------------------------------ */
function includesDescendant(node, tag) {
  for (const child of node.children || []) {
    if (child.type === 1 && (child.tag === tag || includesDescendant(child, tag))) return true;
  }
  return false;
}

function adaptAdminTools(source) {
  const { descriptor } = parse(source);
  let header = null, layout = null;
  walk(descriptor.template.ast, node => {
    if (node.type !== 1) return;
    if (node.tag === 'header' && includesDescendant(node, 'h1')) header = node;
    if (has(node, 'admin-settings-layout')) layout = node;
  });
  if (!header || !layout) throw new Error('AdminTools header/layout changed');

  source = source.slice(0, header.loc.start.offset) + source.slice(header.loc.end.offset);
  const chrome=source.match(/<AdminWorkspaceChrome\b[\s\S]*?\/>/);
  if(!chrome)throw new Error('AdminTools settings category changed');
  const tools=`<span data-v2-layout-tools="accounts" class="v2-admin-toolbar-anchor"></span>`;
  source=source.replace(chrome[0],chrome[0].replace(/\s*\/>$/,`><template #admin-tools>${tools}</template></AdminWorkspaceChrome>`)+`<div v-if="!systemAdmin" class="v2-admin-toolbar"><h1 class="v2-admin-toolbar-title">账号与权限</h1>${tools}</div>`);
  source=source.replace('advancedDiagnosticsVisible.value = !advancedDiagnosticsVisible.value;',`advancedDiagnosticsVisible.value = !advancedDiagnosticsVisible.value;
  if(advancedDiagnosticsVisible.value){selectAdminTab('pressure');return;}`);

  source = patchScript(source, CSS_IMPORT);
  return source;
}

/* ------------------------------------------------------------------ */
/* SignatureManagementPage: compact toolbar + inline summary, keep the */
/* person rows preserved with independent scrolling + layout anchor.   */
/* ------------------------------------------------------------------ */
function adaptSignaturePage(source) {
  const { descriptor } = parse(source);
  let hero = null, stats = null, directory = null, filters = null, sync = null;
  walk(descriptor.template.ast, node => {
    if (node.type !== 1) return;
    if (has(node, 'signature-hero')) hero = node;
    if (has(node, 'stats')) stats = node;
    if (has(node, 'directory-panel')) directory = node;
    if (has(node, 'filters')) filters = node;
    if (has(node, 'source-status')) sync = node;
  });
  if (!hero || !stats || !directory || !filters || !sync) throw new Error('SignatureManagementPage hero/stats/directory changed');

  const actions = hero.children.find(node => has(node, 'hero-actions'));
  const buttons = actions?.children.filter(node => node.type === 1 && node.tag === 'button') || [];
  if (buttons.length !== 3 || !buttons[1].loc.source.includes('@click="openCreate"') || !buttons[2].loc.source.includes('@click="loadDuplicates"')) {
    throw new Error('SignatureManagementPage actions changed');
  }

  const toolbar = `
      <header class="v2-signature-toolbar">
        <div class="v2-signature-title">
          <h2>签名管理</h2>
          <span class="v2-signature-summary" aria-label="签名状态统计">
            <span><b>{{ counts.signed || 0 }}</b> 已有可用签名</span>
            <span><b>{{ counts.unsigned || 0 }}</b> 未签名</span>
            <span><b>{{ counts.resign || 0 }}</b> 需重签</span>
            <span><b>{{ count }}</b> 当前筛选</span>
          </span>
        </div>
        <div class="v2-signature-actions">
          <VnetBackButton to="/" />
          ${buttons[0].loc.source}
          <SignatureManagementActions v-if="loggedIn && (canCreateTemporary || canManageDirectory)">
            ${buttons.slice(1).map(node => node.loc.source).join('\n')}
          </SignatureManagementActions>
          <span data-v2-layout-tools="signatures" class="v2-signature-toolbar-anchor"></span>
        </div>
      </header>
`;

  const search = filters.children.find(node => has(node, 'search-field'));
  const clear = filters.children.find(node => has(node, 'clear-filter'));
  if (!search || !clear) throw new Error('SignatureManagementPage search/clear changed');
  const filterSource = `<div class="filters v2-signature-filters" data-v2-filter>
    ${search.loc.source}
    <div class="v2-signature-primary-filters">${filterByModel(filters, 'scope')}${filterByModel(filters, 'status')}</div>
    <div class="v2-signature-filter-extra">
      <details class="v2-signature-advanced">
        <summary>更多筛选<span v-if="source"> · {{ source === 'staff' ? '正式人员' : '临时人员' }}</span></summary>
        <div class="v2-signature-advanced-fields">${filterByModel(filters, 'source')}</div>
      </details>
      ${clear.loc.source}
    </div>
  </div>`;
  const syncSource = `<details class="v2-signature-sync" :open="Object.values(sources).some(item => !item.ok)">
    <summary aria-label="人员目录同步状态，展开查看完整时间及异常详情">
      <span v-for="(item, key) in sources" :key="key" :class="{ error: !item.ok }" :title="item.ok ? dateText(item.loaded_at) : item.error">
        {{ key === 'staff' ? '正式' : '临时' }}：{{ !item.ok ? '同步失败' : item.loaded_at ? new Date(item.loaded_at * 1000).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false }) : '未同步' }}
      </span>
      <span class="v2-signature-sync-hint">同步详情</span>
    </summary>
    ${sync.loc.source}
  </details>`;

  // Signature statistics live only in the toolbar (v2-signature-summary). The
  // original `stats` section is removed entirely so the PageCanvas summary
  // slot becomes empty; main removes the summary widget definition.
  const edits = [
    [hero.loc.start.offset, hero.loc.end.offset, toolbar],
    [stats.loc.start.offset, stats.loc.end.offset, ''],
    classPropEdit(directory, 'v2-directory-scroll'),
    [filters.loc.start.offset, filters.loc.end.offset, filterSource],
    [sync.loc.start.offset, sync.loc.end.offset, syncSource],
  ];

  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0])) source = source.slice(0, start) + text + source.slice(end);

  source = patchScript(source, `${CSS_IMPORT}\nimport SignatureManagementActions from '../workspace-v2/SignatureManagementActions.vue';`);
  return source;
}

/* ------------------------------------------------------------------ */
/* Public entry point                                                 */
/* ------------------------------------------------------------------ */
export function adaptAccountPresentation(source, filename) {
  const file = filename.replaceAll('\\', '/').split('/').pop();
  if (!adaptedFiles.includes(file)) return source;
  if (source.includes(MARKER)) return source;

  switch (file) {
    case 'AdminAccounts.vue':
      return adaptAdminAccounts(source);
    case 'AdminTools.vue':
      return adaptAdminTools(source);
    case 'SignatureManagementPage.vue':
      return adaptSignaturePage(source);
    default:
      return source;
  }
}

export default adaptAccountPresentation;
