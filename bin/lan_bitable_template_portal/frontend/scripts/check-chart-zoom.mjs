import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { build } from 'esbuild';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
async function exportsOf(file) {
  const result = await build({ entryPoints: [path.join(root, file)], bundle: true, platform: 'node', format: 'cjs', write: false });
  const module = { exports: {} };
  new Function('module', 'exports', 'require', result.outputFiles[0].text)(module, module.exports, createRequire(import.meta.url));
  return module.exports;
}

const { chartOption } = await exportsOf('src/workspace-v2/vipboard.ts');

// A long series (>14) must keep interactive inside zoom but never render a visible
// slider track/outline. "no slider, inside zoom" is the required chart behavior.
const long = Array.from({ length: 40 }, (_, i) => ({ label: String(i), value: i }));
for (const view of ['bar', 'line', 'area', 'horizontal']) {
  const o = chartOption(long, view, '条');
  const zoom = o.dataZoom;
  assert(Array.isArray(zoom) && zoom.length === 1, `${view} has exactly one dataZoom (inside)`);
  const dz = zoom[0];
  assert.equal(dz.type, 'inside', `${view} uses inside zoom, no visible slider`);
  assert(!JSON.stringify(dz).includes('type:"slider"') && !o.dataZoom.some(x => x.type === 'slider'), `${view} has no slider component at all`);
  assert.equal(dz.zoomOnMouseWheel, 'ctrl', `${view} only Ctrl+wheel zooms`);
  assert.equal(dz.moveOnMouseWheel, false, `${view} ordinary wheel keeps scrolling the page`);
  assert.equal(dz.moveOnMouseMove, false, `${view} inside zoom does not pan on plain mouse move`);
  // Meaningful axes and data stay visible (zoom must not hide them).
  assert.equal(o.xAxis.show, true);
  assert.equal(o.yAxis.show, true);
  assert.equal(o.series[0].data.length, long.length);
}

// Short series should not force any zoom.
for (const view of ['bar', 'line', 'area']) {
  const short = [{ label: 'a', value: 1 }, { label: 'b', value: 2 }, { label: 'c', value: 3 }];
  assert.equal(chartOption(short, view).dataZoom.length, 0, `${view} short series has no zoom`);
}
// Calendar and circular views never show slider zoom.
assert.equal(chartOption(long, 'donut').dataZoom.length + chartOption(long, 'pie').dataZoom.length, 0, 'circular charts keep no slider');
for (const view of ['donut', 'pie']) {
  const option = chartOption(long, view);
  assert.deepEqual(option.xAxis, [], 'circular charts remove stale x axes');
  assert.deepEqual(option.yAxis, [], 'circular charts remove stale y axes');
  assert.equal(option.tooltip.trigger, 'item');
}
// The ECharts slider dataZoom artifact is fully removed: no visible track anywhere.
for (const view of ['bar', 'line', 'area', 'horizontal', 'donut', 'pie']) {
  const o = chartOption(long, view, '条');
  assert(!o.dataZoom.some(item => item.type === 'slider'), `${view} never emits a slider dataZoom`);
}

console.log(JSON.stringify({ ok: true, checks: 'no visible slider track/outline (slider removed), inside touch/drag zoom preserved, axes+data intact, short series and circular/calendar excluded' }));
