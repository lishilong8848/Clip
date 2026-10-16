import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse, compileScript, compileTemplate } from '@vue/compiler-sfc';
import { adaptCabinetPage } from './workspace-cabinet-adapter.mjs';
import { adaptPageLayouts } from './workspace-page-layout-adapter.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = await readFile(path.join(root, 'src/components/CabinetPowerPage.vue'), 'utf8');
// The REAL V2 pipeline runs both adapters in sequence (vite.workspace-v2.config.ts:
// adaptCabinetPage then adaptPageLayouts). Testing only the isolated adapter would
// miss that the cabinet summary must keep `metrics` so the customizable PageCanvas survives.
const adapted = adaptPageLayouts(adaptCabinetPage(source), path.join(root, 'src/components/CabinetPowerPage.vue'));

// ---- Structural assertions: the V2 cabinet status summary keeps the `metrics` hook ---- 
assert.notEqual(adapted, source, 'V2 cabinet page is adapted');
assert(adapted.includes('class="metrics v2-cabinet-status"'), 'summary keeps `metrics` (page-layout adapter contract) plus v2-cabinet-status');
assert(adapted.includes('class="v2-cabinet-rate"'), 'power rate block is injected');
assert(adapted.includes('class="v2-cabinet-counts"'), 'compact count strip is injected');
assert(adapted.includes('v-for="item in metricItems"'), 'original six metricItems are still iterated');
assert(adapted.includes('@click="showStateRacks(item.key)"'), 'original state navigation is kept');
assert(adapted.includes('overview.counts[item.key]'), 'original count source is kept');
assert(adapted.includes('v2-cabinet-rate-value'), 'rate value is exposed with a semantic class');
assert(adapted.includes('src="../workspace-v2/cabinetStatus.css"'), 'new scoped CSS is imported from the adapter');

// Rack palette and selection feedback are preserved by the existing adapter contract.
assert(adapted.includes('v2CabinetPalette(cell,v2CabinetNight.value)'), 'rack palette reuse is preserved');
assert(adapted.includes("'v2-rack-selected'"), 'rack selection feedback is preserved');

// ---- Full pipeline assertions: the cabinet PageCanvas is still created ---- 
assert(adapted.includes('<PageCanvas page="cabinet"'), 'customizable cabinet PageCanvas appears in the full chain');
assert(adapted.includes('label="机柜管理"'), 'cabinet layout label is present');
assert(adapted.includes('template #summary>'), 'summary block slot is wrapped');
assert(adapted.includes('template #workspace>'), 'workspace block slot is wrapped');
assert(adapted.includes('v2-layout-host'), 'layout host class is applied to the wrapped container');

// Defect 6: the summary must not keep the 256px vertical floor (minH 4). Override to 2 rows.
const canvasBlock = adapted.match(/:blocks="([^"]*)"/)[1];
assert(canvasBlock, 'PageCanvas block JSON is emitted');
assert(/(?:&quot;summary&quot;|"summary")[^}]*h:2/m.test(canvasBlock), 'cabinet summary default height is 2 rows');
assert(/(?:&quot;summary&quot;|"summary")[^}]*minH:2/m.test(canvasBlock), 'cabinet summary minH is overridden to 2 rows');
assert(!/(?:&quot;summary&quot;|"summary")[^}]*minH:4/m.test(canvasBlock), 'cabinet summary no longer forces the tall minH 4 floor');

// ---- Defect 2: injected TS helper carries an explicit parameter type ---- 
assert(/function v2CabinetRateText\(counts: Dict\): string/.test(adapted), 'v2CabinetRateText has explicit `counts: Dict` parameter type');

// ---- Defect 3: no literal mustache interpolation in a Vue attribute ---- 
assert(!/aria-label="[^"]*\{\{/.test(adapted), 'aria-label never uses literal interpolation');
// The accessible formula lives in a bound tooltip/aria-label, not a literal mustache.
assert(adapted.includes(':title="'), 'rate formula is provided through a bound accessible tooltip');
assert(adapted.includes(':aria-label="'), 'rate group carries a bound accessible label');

// ---- Defect 4: buttons must not masquerade as listitems; groups are the semantics ---- 
assert(!adapted.includes('role="listitem"'), 'no button is presented as a listitem');
assert((adapted.match(/role="group"/g) || []).length >= 2, 'rate and counts containers use role=group');

// ---- Defect 5: compact rate shows just `上电率 95%`; the full formula lives in the tooltip ----
assert(adapted.includes('>上电率</span>'), 'visible label is simply 上电率');
assert(adapted.includes('>{{ v2CabinetRate }}</strong>') || adapted.includes('{{ v2CabinetRate }}'), 'visible rate value renders the compact percentage');
assert(!adapted.includes('v2-cabinet-rate-detail'), 'redundant `已上电 N / 总数 M` detail line is removed');
assert(!adapted.includes('v2CabinetPowered }} / 总数 {{ v2CabinetTotal'), 'no duplicated detail text next to the six counts');

// ---- Compile the adapted SFC to keep template/script bindings valid ----
const { descriptor } = parse(adapted);
const script = compileScript(descriptor, { id: 'cabinet-status-summary' });
assert(!/v2CabinetRateText\(counts\)/.test(script.content), 'no untyped helper remains after compilation');
const compiledTemplate = compileTemplate({
  source: descriptor.template.content,
  filename: 'CabinetPowerPage.vue',
  id: 'cabinet-status-summary',
  compilerOptions: { bindingMetadata: script.bindings },
});
assert.deepEqual(compiledTemplate.errors, [], 'adapted cabinet template compiles cleanly');

// ---- Expose and exercise the injected power-rate helper for edge cases ----
// The injected helper is TypeScript (the adapter compiles within the Vite/TS pipeline),
// so strip the type annotations before executing in plain Node.
const rateStart = adapted.indexOf('function v2CabinetRateText(counts: Dict): string');
assert.notEqual(rateStart, -1, 'rate helper is injected in a testable form');
function sliceFn(str, start) {
  const open = str.indexOf('{', start);
  let depth = 0, i = open;
  for (; i < str.length; i++) {
    if (str[i] === '{') depth++;
    else if (str[i] === '}') { depth--; if (depth === 0) return str.slice(start, i + 1); }
  }
  throw new Error('unbalanced function body');
}
const rateFnSource = sliceFn(adapted, rateStart).replace(/\(counts: Dict\): string/, '(counts)');
const v2CabinetRateText = new Function('return ' + rateFnSource)();

// Compact display: only the percentage is visible.
assert.equal(v2CabinetRateText({ total: 1076, formal: 800, test: 222 }), '95.0%');
assert.equal(v2CabinetRateText({ total: 1076, powered: 1022 }), '95.0%');
assert.equal(v2CabinetRateText({ total: 1076, powered: 1022, formal: 800, test: 222 }), '95.0%');
// Zero total must render a dash, never invalid/NaN.
assert.equal(v2CabinetRateText({ total: 0, formal: 0, test: 0 }), '—');
assert.equal(v2CabinetRateText({ total: 0 }), '—');
assert.equal(v2CabinetRateText({}), '—');
assert.equal(v2CabinetRateText({ total: '0', powered: 0 }), '—');
// Partial/rounding behaviour stays deterministic.
assert.equal(v2CabinetRateText({ total: 3, formal: 1, test: 0 }), '33.3%');

// ---- The scoped stylesheet overrides the legacy `.metrics` card rules and keeps theme tokens ----
const css = await readFile(path.join(root, 'src/workspace-v2/cabinetStatus.css'), 'utf8');
assert(css.includes('.metrics.v2-cabinet-status'), 'scoped CSS targets the retained metrics hook');
assert(css.includes('display: flex'), 'default layout is one row for PC');
assert(css.includes('@media (max-width: 820px)'), 'mobile switches to the compact 2-row layout');
assert(css.includes('grid-row: 2'), 'counts strip occupies the second mobile row');
for (const token of ['--v2-ink', '--v2-muted', '--v2-border', '--v2-blue', '--v2-bg']) {
  assert(css.includes(token), `stylesheet uses theme token ${token} so dark mode adapts automatically`);
}

console.log('PASS: full-chain V2 cabinet summary (adaptCabinetPage → adaptPageLayouts) keeps the metrics PageCanvas hook, overrides summary minH to avoid the 256px floor, types the rate helper, binds aria/tooltip, drops listitem buttons and deduplicates the rate to `上电率 95.0%`. No build or server used.');