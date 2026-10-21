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

/* ------------------------------------------------------------------ */
/* AdminAccounts: flat list (default, original table) + tree view      */
/* ------------------------------------------------------------------ */
function adaptAdminAccounts(source) {
  const { descriptor } = parse(source);
  let loading = null, footer = null;
  const tables = [];
  walk(descriptor.template.ast, node => {
    if (node.type !== 1) return;
    if (node.tag === 'LoadingIndicator') loading = loading || node;
    if (has(node, 'table-wrap')) tables.push(node);
    if (has(node, 'pagination')) footer = footer || node;
  });
  if (!loading || !tables.length || !footer) throw new Error('AdminAccounts list region changed');
  // pick the table-wrap that belongs to the loading→pagination block (the first one)
  const table = tables[0];
  if (table.loc.start.offset < loading.loc.end.offset || footer.loc.start.offset < table.loc.end.offset) {
    throw new Error('AdminAccounts region ordering changed');
  }

  const tableSource = table.loc.source.replace('<div v-else class="table-wrap">', '<div class="table-wrap">');
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
  const region = loading.loc.source + bar + tableSource + footer.loc.source + tree;

  const edits = [[loading.loc.start.offset, footer.loc.end.offset, region]];
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
  const tools=`<button type="button" data-v2-navigation class="v2-admin-toolbar-back" @click="$emit('close')"><ArrowLeft :size="16" />返回个人页面</button><span data-v2-layout-tools="accounts" class="v2-admin-toolbar-anchor"></span>`;
  source=source.replace(chrome[0],chrome[0].replace(/\s*\/>$/,`><template #admin-tools>${tools}</template></AdminWorkspaceChrome>`)+`<div v-if="!systemAdmin" class="v2-admin-toolbar"><span>账号与权限</span>${tools}</div>`);
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
  let hero = null, stats = null, directory = null;
  walk(descriptor.template.ast, node => {
    if (node.type !== 1) return;
    if (has(node, 'signature-hero')) hero = node;
    if (has(node, 'stats')) stats = node;
    if (has(node, 'directory-panel')) directory = node;
  });
  if (!hero || !stats || !directory) throw new Error('SignatureManagementPage hero/stats/directory changed');

  const toolbar = `
      <header class="v2-signature-toolbar">
        <div class="v2-signature-title">
          <h2>签名管理</h2>
          <span class="v2-signature-summary" aria-label="签名状态统计">
            <b>{{ counts.signed || 0 }}</b> 已有可用签名 ·
            <b>{{ counts.unsigned || 0 }}</b> 未签名 ·
            <b>{{ counts.resign || 0 }}</b> 需重签 ·
            <b>{{ count }}</b> 当前筛选
          </span>
        </div>
        <div class="v2-signature-actions">
          <VnetBackButton to="/" />
          <button v-if="loggedIn" class="btn hero-ghost" :disabled="loading" @click="load(true)"><RefreshCw :size="16" :class="{ spin: loading }" />刷新签名</button>
          <button v-if="loggedIn && canCreateTemporary" class="btn hero-primary" @click="openCreate"><Plus :size="16" />新增临时人员</button>
          <button v-if="loggedIn && canManageDirectory" class="btn hero-ghost" :disabled="duplicateBusy" @click="loadDuplicates"><UsersRound :size="16" />重复人员核对</button>
          <span data-v2-layout-tools="signatures" class="v2-signature-toolbar-anchor"></span>
        </div>
      </header>
`;

  // Signature statistics live only in the toolbar (v2-signature-summary). The
  // original `stats` section is removed entirely so the PageCanvas summary
  // slot becomes empty; main removes the summary widget definition.
  const edits = [
    [hero.loc.start.offset, hero.loc.end.offset, toolbar],
    [stats.loc.start.offset, stats.loc.end.offset, ''],
    classPropEdit(directory, 'v2-directory-scroll'),
  ];

  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0])) source = source.slice(0, start) + text + source.slice(end);

  source = patchScript(source, CSS_IMPORT);
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
