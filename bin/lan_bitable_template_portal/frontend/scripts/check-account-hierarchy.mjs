// Focused validation for the V2 account hierarchy presentation (pure logic,
// Vue component, adapter transforms and idempotency). Runs the adapter on the
// ORIGINAL source after replaying the existing admin/page-layout transforms so
// results match the real workspace-v2 build pipeline.
//
//   node frontend/scripts/check-account-hierarchy.mjs

import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
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

// ---- 2. Vue component compiles -------------------------------------------
const componentSource = readFileSync(path.join(v2Dir, 'AccountHierarchy.vue'), 'utf8');
const comp = parse(componentSource, { filename: 'AccountHierarchy.vue' });
const compiledScript = compileScript(comp.descriptor, { id: 'check-account-hierarchy' });
assert.ok(compiledScript.content.includes('buildAccountHierarchy'), 'component uses hierarchy builder');
compileTemplate({ source: comp.descriptor.template.content, filename: 'AccountHierarchy.vue', id: 'check-account-hierarchy' });
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
