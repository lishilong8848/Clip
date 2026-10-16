import assert from 'node:assert/strict';
import postcss from 'postcss';
import {build} from 'esbuild';
import {runInNewContext} from 'node:vm';
import {fileURLToPath} from 'node:url';
import workspaceNightTheme,{nightColor} from './workspace-night-theme.mjs';

assert.equal(nightColor('#fff','text'),'#fff','white labels on primary buttons stay white');
assert.equal(nightColor('#0071e3','surface'),'#0071e3','primary button hues remain intact');
assert.notEqual(nightColor('#fff','surface'),'#fff');
assert.notEqual(nightColor('#24312e','text'),'#24312e');
assert.match(nightColor('rgba(255,255,255,.82)','surface'),/,0\.82\)$/,'retain opacity');
const source=`:root{--cf-text:#0f172a;--cf-surface:#fff}
.workspace-v2 body{background:#fff;color:#24312e}
.panel,.modal :is(input,textarea){background:rgba(255,255,255,.96);color:#334155;border:1px solid #ddd}
.primary{background:#0071e3;color:white}.photo{background:url('/photo#fff.png');filter:none}
.weather-lighthouse .wl-sun{fill:#fff;background:#fff}
.trend-plot text{fill:#64748b}
.repair{--repair-panel:#fff;--repair-soft:#f4f7f8;--repair-line:#dce3e7;--filter-selected:#eaf3ff;background:var(--repair-panel)}
.shared,.signature-pad{background:white;color:black}
.module .panel{background:transparent;border:0;box-shadow:none;color:var(--cf-text)}
.accent-button{background:var(--v2-blue);color:white}
@media(max-width:760px){.panel{background:#f8fafc}}
@media print{.sheet{background:white;color:black}}
@keyframes fade{from{color:#000}to{color:#fff}}`;
const {css}=await postcss([workspaceNightTheme()]).process(source,{from:undefined});
const result=postcss.parse(css);
const originals=[];result.walkRules(rule=>{if(!rule.selector.includes('data-v2-theme'))originals.push(rule.toString());});
const expected=[];postcss.parse(source).walkRules(rule=>expected.push(rule.toString()));
assert.deepEqual(originals,expected,'day styles, print, artwork and keyframes are untouched');
assert(css.includes('html.workspace-v2[data-v2-theme="dark"] body'));
assert(css.includes('html.workspace-v2[data-v2-theme="dark"] .modal :is(input,textarea)'));
assert(!css.includes('data-v2-theme="dark"] :root'));
assert(!css.includes('data-v2-theme="dark"] .weather-lighthouse'));
assert(css.includes('data-v2-theme="dark"] .photo'),'image declarations retain cascade without changing image colors');
assert(!css.includes('data-v2-theme="dark"] .sheet'));
assert(css.includes('data-v2-theme="dark"] .trend-plot text'));
assert(css.includes('data-v2-theme="dark"] .shared'));
assert(!css.includes('data-v2-theme="dark"] .signature-pad'));
const themedRepair=result.nodes.flatMap(node=>node.type==='atrule'?node.nodes||[]:[]).find(rule=>rule.selector?.endsWith(' .repair'));
for(const prop of ['--repair-panel','--repair-soft','--repair-line','--filter-selected'])assert.notEqual(themedRepair.nodes.find(decl=>decl.prop===prop).value,postcss.parse(source).nodes.find(rule=>rule.selector==='.repair').nodes.find(decl=>decl.prop===prop).value);
const override=result.nodes.flatMap(node=>node.type==='atrule'?node.nodes||[]:[]).find(rule=>rule.selector?.endsWith(' .module .panel'));
assert.deepEqual(override.nodes.map(({prop,value})=>[prop,value]),[['background','transparent'],['border','0'],['box-shadow','none'],['color','var(--cf-text)']],'night overrides retain the V2 layout, rather than resurrecting legacy fills/borders');
assert.match(css,/\.accent-button\{background:var\(--v2-blue\);color:var\(--v2-on-accent, #14202a\)\}/,'bright theme accents require a dark foreground');
const chart=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/vipboard.ts',import.meta.url))],bundle:true,write:false,format:'iife',globalName:'charts'});
const sandbox={};runInNewContext(chart.outputFiles[0].text,sandbox);
const items=[{label:'A楼',value:12},{label:'B楼',value:7}];
for(const kind of ['bar','line','pie']){
  const day=sandbox.charts.chartOption(items,kind,'条',false,false,false),night=sandbox.charts.chartOption(items,kind,'条',false,false,true);
  assert.deepEqual(night.series[0].data.map(i=>[i.name,i.value]),day.series[0].data.map(i=>[i.name,i.value]));
  assert.notEqual(night.series[0].label.color,day.series[0].label.color);
  assert.equal(night.tooltip.textStyle.color,'#edf0f4');
}
console.log('PASS: scoped night palette, unchanged day CSS, opaque labels, alpha colors, nested selectors, image/print/artwork exclusions.');
