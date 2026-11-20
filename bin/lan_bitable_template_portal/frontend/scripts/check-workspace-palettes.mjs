import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {runInNewContext} from 'node:vm';
import {fileURLToPath} from 'node:url';

const bundled=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/theme.ts',import.meta.url))],bundle:true,write:false,format:'iife',globalName:'themes'});
const values=new Map(),sandbox={console,setTimeout,clearTimeout,document:{documentElement:{dataset:{},style:{setProperty(){}}},createElement:()=>({})},localStorage:{getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value)}};
runInNewContext(bundled.outputFiles[0].text,sandbox);
const api=sandbox.themes;
function luminance(hex) {
  const channels=hex.slice(1).match(/../g).map(pair=>parseInt(pair,16)/255)
    .map(c=>c<=0.04045?c/12.92:((c+0.055)/1.055)**2.4);
  return channels[0]*.2126+channels[1]*.7152+channels[2]*.0722;
}
function readable(text,bg){
  const [high,low]=[luminance(text),luminance(bg)].sort((a,b)=>b-a);
  assert((high+.05)/(low+.05)>=4.5,text+' on '+bg);
}
for(const c of Object.values(api.mineral)){
  for(const bg of [c.bg,c.surface,c.soft,c.selection])for(const text of [c.text,c.muted,c.accentText])readable(text,bg);
  for(const bg of [c.accent,c.hover,c.pressed])readable(c.onAccent,bg);
}
api.saveNightMode('a',true);api.loadNightMode('b');assert.equal(api.nightMode.value,false);
api.loadNightMode('a');assert.equal(api.nightMode.value,true);
sandbox.localStorage.setItem=()=>{throw Error('blocked');};
assert.equal(api.saveNightMode('a',false),false);assert.equal(api.nightMode.value,false);
console.log('PASS: light and dark contrast, per-account display preferences and storage errors.');
