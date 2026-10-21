// Focused validation for the V2 account hierarchy presentation (pure logic,
// Vue component, adapter transforms and idempotency). Runs the adapter on the
// ORIGINAL source after replaying the existing admin/page-layout transforms so
// results match the real workspace-v2 build pipeline.
//
//   node frontend/scripts/check-account-hierarchy.mjs

import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { buildSync } from 'esbuild';
import { parse, compileTemplate, compileScript } from '@vue/compiler-sfc';

const root = fileURLToPath(new URL('..', import.meta.url));
const v2Dir = path.join(root, 'src', 'workspace-v2');
const scriptsDir = path.join(root, 'scripts');

const read = p => readFileSync(path.join(root, p), 'utf8');

// ---- 1. Pure logic (accountHierarchy.ts) via esbuild bundle --------------
const tsSource = readFileSync(path.join(v2Dir, 'accountHierarchy.ts'), 'utf8');
const bundle = buildSync({
  stdin: { contents: tsSource, resolveDir: v2Dir, sourcefile: 'accountHierarchy.ts', loader: 'ts' },
  bundle: true, write: false, platform: 'node', format: 'esm',
});
const tmp = mkdtempSync(path.join(tmpdir(), 'v2-hierarchy-'));
const bundleFile = path.join(tmp, 'accountHierarchy.mjs');
writeFileSync(bundleFile, bundle.outputFiles[0].text);
const hierarchy = await import(pathToFileURL(bundleFile).href);

// Fixture: two records share the same identity (dedup), others are isolated.
const fixtures = [
  { id: 'a1', open_id: 'ou_dup', name: '去重甲', level: 'L3', role: 'building', scopes: ['A'] },
  { id: 'a2', open_id: 'ou_dup', name: '去重甲副本', level: 'L3', role: 'building', scopes: ['A'] },
  { id: 'b1', open_id: 'ou_boss', name: '主管乙', level: 'L1', role: 'admin', scopes: ['A', 'B'] },
  { id: 'c1', open_id: 'ou_c', name: '值班丙', level: 'L4', role: 'building', scopes: ['B'] },
  { id: 'd1', open_id: 'ou_off', name: '越权丁', level: 'L4', role: 'building', scopes: ['A', 'C'] },
];

const tree = hierarchy.buildAccountHierarchy(fixtures, {});
assert.equal(Array.isArray(tree), true, 'tree is an array');
assert.equal(tree.length, 3, 'three distinct permission levels');
const l1 = tree.find(n => n.level === 'L1');
assert.equal(l1.buildings.length, 2, 'L1 spans A and B');
assert.equal(l1.buildings.find(b => b.value === 'A').people.length, 1, 'L1 person once per building');
assert.equal(hierarchy.hierarchyPersonCount(tree), 4, 'four distinct people after same-id dedup');

// Authorized-scope filtering: only B allowed → 越权丁 drops, 主管乙 restricted to B.
const restricted = hierarchy.buildAccountHierarchy(fixtures, {
  allowedScopes: ['B'],
  scopeValue: a => Array.isArray(a.scopes) ? a.scopes : [],
});
const namesUnderB = restricted.flatMap(l => l.buildings.filter(b => b.value === 'B').flatMap(b => b.people)).map(p => p.name);
assert.equal(hierarchy.hierarchyPersonCount(restricted), 2, 'authorized subset excludes out-of-scope people');
assert.equal(namesUnderB.includes('主管乙'), true, 'L1 included under B');
assert.equal(namesUnderB.includes('越权丁'), false, 'out-of-scope person excluded');
assert.equal(namesUnderB.includes('去重甲'), false, 'A-only person excluded when A not allowed');

// Tree root count assertion (used by the account-settings test).
assert.ok(tree.length >= 1, 'tree root count is meaningful');

console.log('PASS: hierarchy grouping, same-ID dedup and authorized-scope behavior');

const groupedFixtures = [
  {id: 'manager', open_id: 'ou_manager', name: '经理', level: 'L1', category: 'manager', own_scopes: ['H'], scopes: ['A', 'B', 'H']},
  {id: 'engineer', open_id: 'ou_engineer', name: '工程师', level: 'L4', category: 'engineer', own_scopes: ['A', 'B', 'B'], scopes: ['A', 'B']},
  {id: 'engineer-copy', open_id: 'ou_engineer', name: '同人副本', level: 'L4', category: 'engineer', own_scopes: ['A'], scopes: ['A']},
  {id: 'operator', name: '值班员', level: 'L4', category: 'operator', home_scope: 'A', scopes: ['A', 'B']},
  {id: 'legacy', name: '历史账号', level: 'L3', scopes: ['C']},
  {id: 'unassigned', name: '待归属', own_scopes: [], scopes: ['A', 'B']},
];
const before = structuredClone(groupedFixtures);
const leaves = nodes => nodes.flatMap(node => node.type === 'person' ? [node] : leaves(node.children));
const allNodes = nodes => nodes.flatMap(node => [node, ...(node.type === 'person' ? [] : allNodes(node.children))]);
for (const groupBy of ['building', 'level']) {
  for (const depth of [1, 2, 3]) {
    const grouped = hierarchy.buildGroupedAccountHierarchy(groupedFixtures, {groupBy, depth});
    const nodes = allNodes(grouped);
    assert.ok(grouped.every(node => node.type === groupBy), 'root follows the chosen grouping');
    assert.equal(new Set(nodes.map(node => node.key)).size, nodes.length, 'full-path keys remain unique across branches');
    assert.equal(new Set(leaves(grouped).map(node => node.id)).size, 5, 'all authorized accounts retained, same identity deduplicated');
    for (const node of nodes.filter(node => node.type !== 'person')) {
      assert.equal(node.count, new Set(leaves(node.children).map(person => person.id)).size, 'group counts distinct people');
      assert.ok(node.depth <= depth, 'requested grouping depth is respected');
      if (node.depth === depth) assert.ok(node.children.every(child => child.type === 'person'), 'final group contains people directly');
    }
    assert.deepEqual(grouped, hierarchy.buildGroupedAccountHierarchy(groupedFixtures, {groupBy, depth}), 'keys and ordering are stable');
  }
}
assert.deepEqual(groupedFixtures, before, 'grouping never mutates account or permission data');
const byBuilding = hierarchy.buildGroupedAccountHierarchy(groupedFixtures);
assert.deepEqual(byBuilding.map(node => node.value), ['A', 'B', 'C', 'H', ''], 'building order and unassigned branch');
assert.deepEqual(leaves(byBuilding.filter(node => node.value === 'H')).map(node => node.id), ['manager'], 'cross-building access does not change home building');
assert.ok(!leaves(byBuilding.filter(node => node.value === 'A')).some(node => node.id === 'manager'), 'manager is not duplicated into authorized buildings');
assert.equal(byBuilding.find(node => node.value === '').label, '未归属楼栋', 'missing affiliation is not labeled global permission');
assert.deepEqual(leaves(hierarchy.buildGroupedAccountHierarchy(groupedFixtures, {allowedScopes: ['B']})).filter(node => node.building).map(node => node.id), ['engineer'], 'scope allow-list still filters concrete buildings');
const levelOnly = hierarchy.buildGroupedAccountHierarchy(groupedFixtures, {groupBy: 'level', depth: 1});
assert.deepEqual(levelOnly.map(node => node.value), ['L1', 'L3', 'L4', ''], 'permission levels sort from high to low');
assert.equal(levelOnly.find(node => node.value === 'L4').count, 2, 'multi-building person counted once within level');
assert.ok(allNodes(byBuilding).some(node => node.type === 'category' && node.label === '工程师'), 'role labels are localized');
assert.deepEqual(hierarchy.buildGroupedAccountHierarchy([]), [], 'empty directory stays empty');
console.log('PASS: both grouping orders, all depths, affiliation, unique keys, counts and immutable permissions');

// Compare the read-only projection against the live Python policy, not another JS oracle.
const manager = (id, level, scopes = ['H'], extra = {}) => ({id, open_id: 'ou_' + id, name: id,
  level, enabled: true, modules: ['accounts'], scopes, home_scope: scopes[0], own_scopes: scopes.slice(0, 1), ...extra});
const managers = [manager('root', 'L0', ['A', 'B', 'H']), manager('boss', 'L1', ['A', 'B', 'H']),
  manager('h-engineer', 'L2'), manager('a-supervisor', 'L3', ['A']), manager('ab-supervisor', 'L3', ['A', 'B']),
  manager('base', 'L4'), manager('disabled', 'L1', ['H'], {enabled: false}),
  manager('revoked', 'L1', ['H'], {modules: []}), manager('blocked', 'L1', ['H'], {blocked_reason: 'blocked'}),
  manager('incomplete', 'L1', ['H'], {enabled: undefined}), manager('unknown', 'unknown')];
const targets = [];
for (const level of ['L0', 'L1', 'L2', 'L3', 'L4', 'unknown']) for (const category of ['manager', 'supervisor', 'engineer', 'operator', 'shift_leader', 'duty']) {
  for (const own_scopes of [[], ['A'], ['B'], ['A', 'B']]) for (const flags of [{}, {locked: true}, {blocked_reason: 'blocked'}, {role: 'admin'}]) {
    targets.push({id: 'target-' + targets.length, name: '目标', level, category, own_scopes, role: 'building', ...flags});
  }
}
targets.push({...managers[0], id: 'alias-root', is_self: false}, {id: 'self-relative-to-viewer', level: 'L4', category: 'engineer', own_scopes: ['A'], is_self: true});
const comparisons = managers.flatMap(manager => targets.map(target => ({manager, target})));
const policyCheck = spawnSync(path.resolve(root, '../../.venv/Scripts/python.exe'), ['-c', `
import json, sys
sys.path.insert(0, sys.argv[1])
from lan_bitable_template_portal.account_policy import account_allowed
result = []
for case in json.load(sys.stdin):
    m, target = case['manager'], dict(case['target'])
    identity = lambda row: row.get('open_id') or row.get('id') or ''
    same = identity(m) == identity(target)
    target['is_self'] = same
    profile = {'level': m['level'], 'scope_rules': m.get('scopes', []), 'home_scope': m.get('home_scope'),
        'can_manage_accounts': m.get('enabled') is True and not m.get('blocked_reason')
            and m['level'] in ('L0', 'L1', 'L2', 'L3') and 'accounts' in m.get('modules', [])}
    result.append({action: not same and account_allowed(profile, target, action) for action in ('view', 'edit')})
json.dump(result, sys.stdout)
`, path.resolve(root, '../..')], {input: JSON.stringify(comparisons), encoding: 'utf8', maxBuffer: 8 * 1024 * 1024});
assert.equal(policyCheck.status, 0, 'Python policy comparison failed: ' + policyCheck.stderr);
const expectedAccess = JSON.parse(policyCheck.stdout);
for (const [index, {manager, target}] of comparisons.entries()) assert.deepEqual(
  hierarchy.accountManagementAccess(manager, target), expectedAccess[index], `policy mismatch: ${manager.id} / ${target.id}`);
console.log(`PASS: ${comparisons.length} management relationships match the existing backend view/edit policy`);

const relationPeople = [manager('root', 'L0', ['A', 'B', 'H']), manager('boss', 'L1', ['A', 'B', 'H']),
  manager('h-engineer', 'L2', ['H'], {category: 'engineer'}),
  manager('a-supervisor', 'L3', ['A'], {category: 'supervisor'}), manager('b-supervisor', 'L3', ['B'], {category: 'supervisor'}),
  manager('a-engineer', 'L4', ['A'], {category: 'engineer', modules: ['learning'], can_edit: false}),
  manager('b-engineer', 'L4', ['B'], {category: 'engineer', modules: ['learning']}),
  manager('a-operator', 'L4', ['A'], {category: 'operator', modules: ['learning']}),
  manager('a-duty', 'L4', ['A'], {category: 'duty', kind: 'duty', modules: ['learning']})];
const relationBefore = structuredClone(relationPeople), loadedPeople = new Set();
const visit = nodes => nodes.flatMap(node => [node, ...visit(node.children || [])]);
const relationTree = roots => hierarchy.buildAccountManagementTree(roots, relationPeople, {loadedPeople});
let relations = relationTree(relationPeople);
let rootPerson = visit(relations).find(node => node.type === 'person' && node.id === 'root');
assert.equal(rootPerson.children, undefined, 'unopened management branches are not materialized');
assert.equal(rootPerson.managementCount, 8);
loadedPeople.add(rootPerson.key);relations = relationTree(relationPeople);
const bossPerson = visit(relations).find(node => node.id === 'boss' && node.managementDepth === 1);
assert.ok(bossPerson && !bossPerson.isLeaf, 'manager inside a management range can itself expand');
loadedPeople.add(bossPerson.key);relations = relationTree(relationPeople);
const supervisor = visit(relations).find(node => node.id === 'a-supervisor' && node.managementDepth === 2);
loadedPeople.add(supervisor.key);relations = relationTree(relationPeople);
const supervisorLoaded = visit(relations).find(node => node.key === supervisor.key);
assert.deepEqual(leaves(supervisorLoaded.children).map(node => node.id).sort(), ['a-duty', 'a-engineer', 'a-operator'], 'nested supervisor retains strict home-building and role boundaries');
assert.equal(visit(supervisorLoaded.children).find(node => node.id === 'a-engineer').source.can_edit, false, 'selected manager never replaces current login edit flags');
const relationNodes = visit(relations);
assert.equal(new Set(relationNodes.map(node => node.key)).size, relationNodes.length, 'identical people in different paths have independent stable keys');
assert.ok(relationNodes.some(node => node.managementDepth === 3 && node.depth === 16), 'four people-management levels remain expandable');
assert.deepEqual(relationPeople, relationBefore, 'hierarchy does not mutate permission fields or account records');
const rootSearch = relationTree([relationPeople[3]]), selectedSupervisor = visit(rootSearch).find(node => node.type === 'person');
assert.equal(selectedSupervisor.managementCount, 3, 'root search does not discard its authorized management range');
const limitedDirectory = relationPeople.filter(person => ['boss', 'a-supervisor', 'a-engineer'].includes(person.id));
const limitedTree = hierarchy.buildAccountManagementTree([relationPeople[1], relationPeople[4]], limitedDirectory);
assert.equal(visit(limitedTree).filter(node => node.type === 'person').length, 1, 'roots outside the current login directory cannot be injected');
assert.equal(visit(limitedTree).find(node => node.id === 'boss').managementCount, 2, 'counts never include personnel missing from the backend-authorized directory');
const revoked = relationPeople.map(person => person.id === 'boss' ? {...person, modules: []} : person);
const revokedBoss = visit(hierarchy.buildAccountManagementTree(revoked, revoked, {loadedPeople})).find(node => node.id === 'boss' && node.managementDepth === 0);
assert.equal(revokedBoss.managementCount, 0);assert.equal(revokedBoss.children, undefined, 'revocation removes already-loaded management ranges');
console.log('PASS: lazy multi-person levels, stable paths, root filtering, current-login flags, scoped directory and revocation');

const peers = [
  manager('fixed-root', 'L0', ['H'], {name: '李世龙', locked: true, can_edit: false, can_grant_l0: false}),
  manager('peer-a', 'L0', ['H'], {can_edit: true, can_grant_l0: true}),
  manager('peer-b', 'L0', ['A'], {can_edit: true, can_grant_l0: true}),
  manager('member', 'L4', ['A'], {category: 'engineer', modules: ['learning'], can_edit: false}),
];
const peersBefore = structuredClone(peers);
assert.deepEqual(hierarchy.accountManagementAccess(peers[1], peers[2]), {view: true, edit: true});
assert.deepEqual(hierarchy.accountManagementAccess(peers[2], peers[1]), {view: true, edit: true});
assert.deepEqual(hierarchy.accountManagementAccess(peers[1], peers[0]), {view: true, edit: false}, 'fixed root remains read-only');
assert.deepEqual(hierarchy.accountManagementAccess(peers[1], peers[1]), {view: false, edit: false}, 'no self-management branch');
// Simulate expanding every branch: mutual L0 access must still terminate.
const allLoaded = {has: () => true};
const peerTree = hierarchy.buildAccountManagementTree(peers, peers, {loadedPeople: allLoaded});
function checkAncestry(nodes, ancestors = new Set()) {
  for (const node of nodes) {
    const next = new Set(ancestors);
    if (node.type === 'person') {
      const identity = hierarchy.personKey(node.source);
      assert(!next.has(identity), 'no repeated person in a management path');
      next.add(identity);
      assert(node.managementDepth < peers.length, 'expansion depth is bounded by distinct people');
      if (node.id === 'fixed-root') {
        assert.equal(node.source.can_edit, false);
        assert.equal(node.source.can_grant_l0, false);
      }
    }
    checkAncestry(node.children || [], next);
  }
}
checkAncestry(peerTree);
const peerNodes = visit(peerTree);
assert.equal(new Set(peerNodes.map(node => node.key)).size, peerNodes.length);
for (const [parent, child] of [['peer-a', 'peer-b'], ['peer-b', 'peer-a']]) {
  const branch = peerNodes.find(node => node.id === parent && node.managementDepth === 0);
  assert(visit(branch.children).some(node => node.id === child), 'ancestry guard must not suppress independent peer branches');
}
assert.deepEqual(peers, peersBefore, 'tree expansion never alters protected or editable account flags');
console.log('PASS: paired L0 peer editing, locked root, self exclusion and fully expanded cycle-safe paths');

// ---- 2. Vue component compiles -------------------------------------------
const componentSource = readFileSync(path.join(v2Dir, 'AccountHierarchy.vue'), 'utf8');
const comp = parse(componentSource, { filename: 'AccountHierarchy.vue' });
const compiledScript = compileScript(comp.descriptor, { id: 'check-account-hierarchy' });
assert.ok(compiledScript.content.includes('buildAccountHierarchy'), 'component uses hierarchy builder');
const compiledTemplate = compileTemplate({ source: comp.descriptor.template.content, filename: 'AccountHierarchy.vue', id: 'check-account-hierarchy' });
assert.deepEqual(compiledTemplate.errors, [], 'tree template compiles without errors');
const {adaptSingleSelects} = await import(pathToFileURL(path.join(scriptsDir, 'workspace-select-adapter.mjs')).href);
assert.equal(adaptSingleSelects(componentSource, path.join(v2Dir, 'AccountHierarchy.vue')), componentSource, 'existing select bridge is not wrapped a second time');
console.log('PASS: AccountHierarchy.vue compiles (script + template)');

// ---- 3. Adapter transforms (replay pipeline in build order) --------------
const adapterUrl = pathToFileURL(path.join(scriptsDir, 'workspace-account-presentation-adapter.mjs')).href;
const { adaptAccountPresentation, adaptedFiles } = await import(adapterUrl);
const { adaptAccounts, adaptAdminAccountLoading } = await import(
  pathToFileURL(path.join(scriptsDir, 'workspace-admin-adapter.mjs')).href,
);
const { adaptPageLayouts } = await import(
  pathToFileURL(path.join(scriptsDir, 'workspace-page-layout-adapter.mjs')).href,
);

function simulatePipeline(source, filename) {
  const file = path.basename(filename);
  let code = source;
  if (file === 'AdminAccounts.vue') code = adaptAccounts(code);
  if (file === 'AdminTools.vue') {
    // replicate vite config back-button strip + accounts-loading no-op + wrap
    code = code.replace('<button type="button" class="btn ghost" @click="$emit(\'close\')"><ArrowLeft :size="17" />返回首页</button>', '');
    code = adaptAdminAccountLoading(code);
    code = adaptPageLayouts(code, filename);
  }
  if (file === 'SignatureManagementPage.vue') code = adaptPageLayouts(code, filename);
  code = adaptAccountPresentation(code, filename);
  return code;
}

for (const version of ['legacy', 'v2']) {
  const original = read('src/components/AdminTools.vue');
  const source = version === 'v2' ? simulatePipeline(original, 'AdminTools.vue') : original;
  const script = parse(source).descriptor.scriptSetup.content;
  const start = script.indexOf('function selectAdminTab(');
  const end = script.indexOf('function toggleAdvancedDiagnostics(', start);
  assert(start >= 0 && end > start);
  const code = buildSync({stdin: {contents: script.slice(start, end), loader: 'ts'}, write: false}).outputFiles[0].text;
  const calls = {permissions: 0, requests: 0};
  const select = new Function('props', 'tab', 'emit', 'advancedDiagnosticsVisible', 'loadStatus',
    'loadPermissions', 'loadPermissionRequests', 'loadHandover', 'loadMopSettings',
    'let initializedTab = false;\n' + code + '\nreturn selectAdminTab;')(
    {systemAdmin: true}, {value: 'permissions'}, () => {}, {value: false}, () => {},
    () => calls.permissions++, () => calls.requests++, () => {}, () => {});
  select('permissions');
  select('permissions');
  assert.deepEqual(calls, {permissions: 0, requests: 1}, version + ': accounts owns the only initial account-directory read');
}
console.log('PASS: both admin entry routes avoid duplicate account loading and preserve request-slot loading');

function compileSfc(source, filename) {
  const { descriptor, errors } = parse(source, { filename });
  assert.deepEqual(errors, [], `parse errors in ${filename}: ${errors.map(e => e.message).join('; ')}`);
  const js = compileScript(descriptor, { id: 'check-' + filename });
  assert.ok(js.content.length > 0, `script compiled in ${filename}`);
  if (descriptor.template) {
    const tpl = compileTemplate({ source: descriptor.template.content, filename, id: 'check-' + filename });
    assert.ok(!tpl.errors || !tpl.errors.length, `template errors in ${filename}: ${(tpl.errors || []).join('; ')}`);
  }
}

const cases = [
  ['src/components/AdminTools.vue', 'AdminTools.vue'],
  ['src/components/AdminAccounts.vue', 'AdminAccounts.vue'],
  ['src/components/SignatureManagementPage.vue', 'SignatureManagementPage.vue'],
];

for (const [rel, file] of cases) {
  const original = read(rel);
  const abs = path.join(root, rel);
  const adapted = simulatePipeline(original, abs);
  assert.notEqual(adapted, original, `${file} must be transformed`);
  compileSfc(adapted, file);

  const twice = adaptAccountPresentation(adapted, abs);
  assert.equal(twice, adapted, `${file} adapter must be idempotent`);

  if (file === 'AdminTools.vue') {
    assert.ok(adapted.includes('data-v2-layout-tools="accounts"'), 'accounts layout anchor present');
    assert.ok(adapted.includes('v2-admin-toolbar'), 'admin toolbar present');
    assert.ok(adapted.includes('toggleAdvancedDiagnostics'), 'advanced diagnostics uses existing toggle');
    assert.ok(adapted.includes("$emit('close')"), 'back control emits close (guards respected)');
    assert.ok(adapted.includes('返回个人页面'), 'back control returns to personal page');
    assert.ok(!adapted.includes('返回个人工作台'), 'personal page label must not read 返回个人工作台');
    assert.ok(!/data-v2-layout-tools="accounts"[^>]*aria-hidden="true"/.test(adapted), 'accounts layout host must not be aria-hidden (interactive button is teleported inside)');
    assert.ok(adapted.includes("selectAdminTab('pressure')"), 'advanced diagnostics opens its actual page');
    assert.ok(adapted.includes('#admin-tools'), 'layout and back share the settings category row');
  }
  if (file === 'AdminAccounts.vue') {
    assert.ok(adapted.includes('v2-account-mode-bar'), 'tree/list toggle bar present');
    assert.ok(adapted.includes("v2AccountMode = ref<'list' | 'tree'>('list')"), 'mode ref defaults to list');
    assert.ok(adapted.includes("v2AccountMode === 'list'"), 'list branch guards original table');
    assert.ok(adapted.includes('AccountHierarchy'), 'hierarchy component imported');
    assert.ok(adapted.includes('@select="openAccount"'), 'tree selection calls original openAccount');
    assert.ok(adapted.includes(':level-label="levelLabel"'), 'tree reuses permission-level labels');
    assert.ok(adapted.includes(':category-label="categoryLabel"'), 'tree reuses role labels');
    assert.ok(adapted.includes(':directory="items"'), 'management ranges use only the original authorized directory');
    assert.ok(adapted.includes("tab === 'requests'"), 'permission review tab still present');
    assert.ok(adapted.includes('openAccount('), 'openAccount handler preserved');
    assert.ok(adapted.includes(':aria-label="`管理${item.name}`"'), 'list manage button aria-label preserved');
  }
  if (file === 'SignatureManagementPage.vue') {
    assert.ok(adapted.includes('v2-signature-toolbar'), 'compact signature toolbar present');
    assert.ok(adapted.includes('data-v2-layout-tools="signatures"'), 'signature layout anchor present');
    assert.ok(!/data-v2-layout-tools="signatures"[^>]*aria-hidden="true"/.test(adapted), 'signature layout host must not be aria-hidden (interactive button is teleported inside)');
    assert.ok(adapted.includes('v2-signature-summary'), 'signature statistics preserved in toolbar');
    assert.ok(!adapted.includes('v2-signature-summary-card'), 'duplicated summary card removed (PageCanvas summary slot becomes empty for main to drop)');
    assert.ok(adapted.includes('v2-directory-scroll'), 'directory scroll class added');
    assert.ok(adapted.includes('重复人员核对'), 'duplicates action preserved');
    assert.ok(adapted.includes('openPreview('), 'preview handler preserved');
    assert.ok(adapted.includes('openSend('), 'send handler preserved');
    assert.ok(adapted.includes('openCreate'), 'create handler preserved');
    assert.ok(!adapted.includes('class="signature-hero"'), 'decorative hero removed from template');
  }
  console.log(`PASS: ${file} adapted, compiled and idempotent`);
}

assert.equal(
  cases.every(([, file]) => adaptedFiles.includes(file)),
  true,
  'adaptedFiles contract matches the three target files',
);

console.log('\nALL CHECKS PASSED');
