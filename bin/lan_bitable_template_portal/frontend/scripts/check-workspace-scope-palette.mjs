import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';
async function module(file){const source=await readFile(new URL('../src/workspace-v2/'+file,import.meta.url),'utf8');return import('data:text/javascript;base64,'+Buffer.from(ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText).toString('base64'));}
const {cabinetPalette}=await module('cabinetPalette.ts');
const {moduleScope,scopeFamily,readScopeMemory,saveScopeMemory}=await module('scopeMemory.ts');
const values=new Map();globalThis.localStorage={getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value)};
const options=['ALL','110','A','B','H'].map(value=>({value}));
assert.equal(moduleScope('cabinet',options,'','H'),'ALL');
assert.equal(moduleScope('cabinet',options,'B','H'),'B');
assert.equal(moduleScope('event',options,'','H'),'H');
assert.equal(moduleScope('cabinet',[{value:'B'}],'A','H'),'B');
assert.equal(scopeFamily('maintenance'),scopeFamily('change'));
assert.notEqual(scopeFamily(''),scopeFamily('cabinet'));
saveScopeMemory({union_id:'first'},{workspace:'H',cabinet:'B'});
assert.deepEqual(readScopeMemory({union_id:'second'}),{});
assert.deepEqual(readScopeMemory({union_id:'first'}),{workspace:'H',cabinet:'B'});
const labels=[{text:'恒湿机',style:{fill:'#fff',font_color:'#000'}},{text:'空调',style:{}},{text:'普通单元格',style:{fill:'#ddd',font_color:'#ccc'}},{rack:'A01',state:'formal',color:'#f00',style:{}},{rack:'A02',state:'test',color:'#ff0',style:{}},{rack:'A03',state:'off',color:'#00b050',style:{}}];
function luminance(hex){const rgb=hex.slice(1).match(/.{2}/g).map(c=>parseInt(c,16)/255).map(s=>s<=.04045?s/12.92:((s+.055)/1.055)**2.4);return rgb[0]*.2126+rgb[1]*.7152+rgb[2]*.0722;}
function expand(hex){return hex.length===4?'#'+[...hex.slice(1)].map(c=>c+c).join(''):hex;}
for(const cell of labels){const palette=cabinetPalette(cell,true),a=luminance(expand(palette['--v2-cell-background'])),b=luminance(expand(palette['--v2-cell-ink']));assert((Math.max(a,b)+.05)/(Math.min(a,b)+.05)>=4.5,cell.text||cell.rack);if(cell.color)assert.equal(palette['--v2-cell-background'],cell.color,'state fill stays unchanged');}
assert.equal(cabinetPalette(labels[0],false)['--v2-cell-background'],'#fff');
console.log('PASS: module scopes stay separate, supported authorized scopes only, per-person restore; night equipment/rack text contrast >=4.5 with state fills preserved.');
