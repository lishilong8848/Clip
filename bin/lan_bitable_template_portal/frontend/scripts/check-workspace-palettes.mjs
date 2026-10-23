import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {runInNewContext} from 'node:vm';
import {fileURLToPath} from 'node:url';

const bundled=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/theme.ts',import.meta.url))],bundle:true,write:false,format:'iife',globalName:'themes'});
const values=new Map(),sandbox={console,setTimeout,clearTimeout,document:{documentElement:{dataset:{},style:{setProperty(){}}},createElement:()=>({})},localStorage:{getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value)}};
runInNewContext(bundled.outputFiles[0].text,sandbox);
const api=sandbox.themes;
assert.equal(api.PALETTES.length,6);
for(const palette of api.PALETTES)for(const dark of [false,true]){
  const c=api.paletteColors(palette.id,dark);
  for(const background of [c.bg,c.surface,c.soft]){
    assert(api.contrast(c.text,background)>=4.5,palette.id+' body text');
    assert(api.contrast(c.muted,background)>=4.5,palette.id+' secondary text');
    assert(api.contrast(c.accentText,background)>=4.5,palette.id+' accent text');
  }
  for(const background of [c.accent,c.hover,c.pressed])assert(Math.max(api.contrast('#000000',background),api.contrast('#ffffff',background))>=4.5,palette.id+' button label');
  assert(api.contrast('#f4f8ff',c.header)>=4.5,palette.id+' header');
  assert(api.contrast(c.text,c.selection)>=4.5,palette.id+' selected menu');
}
for(const custom of ['#000000','#ffffff','#fdc941','#ff0000','#808080'])for(const dark of [false,true]){
  const c=api.paletteColors('custom',dark,custom);
  assert(api.contrast(c.accentText,c.surface)>=4.5,'custom accent text');
  assert(api.contrast(c.onAccent,c.accent)>=4.5,'custom filled button');
}
api.savePalette('a','gold');api.saveNightMode('a',true);
api.loadPalette('b');api.loadNightMode('b');
assert.equal(api.paletteId.value,'cobalt');assert.equal(api.nightMode.value,false);
api.loadPalette('a');api.loadNightMode('a');
assert.equal(api.paletteId.value,'gold');assert.equal(api.nightMode.value,true);
values.set('clipflow:workspace-nav-color:old','#16836d');api.loadPalette('old');
assert.equal(api.paletteId.value,'custom');assert.equal(api.customNavColor.value,'#16836d');
values.set('clipflow:workspace-palette:bad','<script>');values.set('clipflow:workspace-nav-color:bad','invalid');api.loadPalette('bad');
assert.equal(api.paletteId.value,'cobalt');
const before=api.paletteId.value;assert.equal(api.savePalette('a','missing'),false);assert.equal(api.paletteId.value,before);
const oldSet=sandbox.localStorage.setItem;sandbox.localStorage.setItem=()=>{throw new Error('Storage blocked');};
assert.equal(api.savePalette('a','mint'),false);assert.equal(api.paletteId.value,'mint');sandbox.localStorage.setItem=oldSet;
console.log('PASS: 6 palettes / 12 modes, readable text and button labels, custom extremes, legacy preference recovery, account isolation, storage failure.');
