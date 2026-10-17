import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir,readFile} from 'node:fs/promises';
import {gunzipSync} from 'node:zlib';
import ts from 'typescript';
import {build,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {adaptCabinetPage} from './workspace-cabinet-adapter.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const outDir=path.resolve(root,'../../../output/cabinet-archive-build');
const shots=path.resolve(root,'../../../output/playwright/cabinet-archive');
await mkdir(shots,{recursive:true});
const compile=async file=>import('data:text/javascript;base64,'+Buffer.from(ts.transpileModule(await readFile(path.join(root,'src',file),'utf8'),{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText).toString('base64'));
const {monthlyArchivePreview}=await compile('cabinetSchedule.ts');
const now=Date.parse('2026-10-17T15:00:00+08:00');
assert.deepEqual(monthlyArchivePreview(1,'02:00',now,false),{next_run_at:'2026-11-01 02:00',pending_run_at:'2026-10-01 02:00'});
assert.deepEqual(monthlyArchivePreview(20,'18:30',now,false),{next_run_at:'2026-10-20 18:30',pending_run_at:''});
assert.equal(monthlyArchivePreview(20,'18:30',now,true).next_run_at,'2026-11-20 18:30');
for(const [date,expected] of [['2026-02-27T23:00:00+08:00','2026-02-28 09:30'],['2028-02-28T23:00:00+08:00','2028-02-29 09:30'],['2026-12-31T10:00:00+08:00','2027-01-31 09:30']])assert.equal(monthlyArchivePreview(31,'09:30',Date.parse(date),false).next_run_at,expected);
for(const [day,clock] of [[0,'02:00'],[32,'02:00'],[1.5,'02:00'],[1,'25:00']])assert.equal(monthlyArchivePreview(day,clock,now,false),null);
const {cabinetPalette}=await compile('workspace-v2/cabinetPalette.ts');
const layouts=JSON.parse(gunzipSync(await readFile(path.resolve(root,'../templates/cabinet_power/layouts.json.gz'))));
const luminance=color=>{
 const hex=color.slice(1),rgb=hex.length===3?[...hex].map(c=>parseInt(c+c,16)):[0,2,4].map(i=>parseInt(hex.slice(i,i+2),16));
 return rgb.map(c=>c/255).map(c=>c<=.04045?c/12.92:((c+.055)/1.055)**2.4).reduce((sum,c,i)=>sum+c*[.2126,.7152,.0722][i],0);
};
let checked=0;
for(const rooms of Object.values(layouts))for(const layout of Object.values(rooms))for(const cell of layout?.cells||[]){
 if(!cell.text)continue;const palette=cabinetPalette(cell,true),bg=luminance(palette['--v2-cell-background']),fg=luminance(palette['--v2-cell-ink']);
 assert((Math.max(bg,fg)+.05)/(Math.min(bg,fg)+.05)>=4.5,cell.text);
 assert.equal(palette['--v2-cell-weight'],cell.style?.bold?'700':'600');checked++;
}
assert(checked>7000);
const source=await readFile(path.join(root,'src/components/CabinetPowerPage.vue'),'utf8');
const adapted=path.join(root,'src/components/__cabinet_archive_v2.vue').replaceAll('\\','/');
await build({root,configFile:false,publicDir:false,logLevel:'error',plugins:[{
 name:'cabinet-archive-fixture',enforce:'pre',
 resolveId(id){if(id==='virtual:cabinet-check')return '\0cabinet-check';if(id==='virtual:cabinet-v2')return adapted;},
 load(id){if(id===adapted)return adaptCabinetPage(source);if(id!=='\0cabinet-check')return;return `
 import {createApp,h} from 'vue';import Cabinet from '/src/components/CabinetPowerPage.vue';import Night from 'virtual:cabinet-v2';import Batch from '/src/components/CabinetPowerBatchPage.vue';
 import UiTransition from '/src/components/UiTransition.vue';import LoadingIndicator from '/src/components/LoadingIndicator.vue';import {applyNightMode} from '/src/workspace-v2/theme';
 import '/src/global.css';import '/src/workspace-v2/compactWorkspaces.css';
 const q=new URLSearchParams(location.search),night=q.has('night');if(night){document.documentElement.classList.add('workspace-v2');applyNightMode(true);}
 const app=createApp({render:()=>h(q.has('batch')?Batch:night?Night:Cabinet,{scope:night?'A':'',isAdmin:true,userId:'archive-fixture',scopeOptions:['A','B','C','D','E'].map(value=>({value,label:value+'楼'}))})});
 app.component('UiTransition',UiTransition);app.component('LoadingIndicator',LoadingIndicator);app.mount('#app');`;},
},vue(),{
 name:'cabinet-fixture-html',enforce:'post',
 generateBundle(_options,bundle){const entry=Object.values(bundle).find(item=>item.type==='chunk'&&item.isEntry),css=Object.values(bundle).filter(item=>item.fileName.endsWith('.css'));this.emitFile({type:'asset',fileName:'index.html',source:`<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(item=>`<link rel="stylesheet" href="/${item.fileName}">`).join('')}</head><body style="margin:0"><div id="page-back-slot"></div><div id="app"></div><script type="module" src="/${entry.fileName}"></script></body></html>`});},
}],build:{outDir,emptyOutDir:true,cssCodeSplit:false,rollupOptions:{input:'virtual:cabinet-check'}}});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir},preview:{host:'127.0.0.1',port:33000+Math.floor(Math.random()*5000)}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,browser=await chromium.launch();
let page,done=false,saves=0;
const errors=[],buildings='ABCDE'.split('').map(scope=>({scope,counts:{total:1000,formal:890,test:10,off:100,unknown:0},record_count:10,inventory_only:0,bootstrap_status:'succeeded'}));
const batch=id=>({batch_id:id,status:done?'succeeded':'running',phase:done?'completed':'archiving',retry_at:done?0:now/1000+30,created_at:'2026-10-17 15:36:27',items:Object.fromEntries(buildings.map(item=>[item.scope,{scope:item.scope,status:'succeeded',result:{export_id:'fixture'+item.scope,created_at:'2026-10-17 15:36:31'}}]))});
const mapCells=[{text:'恒湿机1',style:{font_name:'宋体',font_color:'#FF0000'}},{text:'空调',style:{font_name:'宋体',fill:'#fff'}},{text:'A01',rack:'A01',state:'formal',color:'#ff0000'},{text:'A02',rack:'A02',state:'test',color:'#ffc000'},{text:'A03',rack:'A03',state:'off',color:'#00b050'}].map((cell,i)=>({...cell,ref:'cell'+i,x:i*120,y:10,width:110,height:70}));
try{
 const context=await browser.newContext({viewport:{width:1440,height:900},timezoneId:'America/Los_Angeles',reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}}),p=url.pathname.replace('/api/cabinet-power/','');
  if(p==='bootstrap')return ok({status:'succeeded',ready:5,total:5,buildings:buildings.map(row=>({...row,status:'succeeded'}))});
  if(p==='buildings')return ok({buildings});
  if(p==='export-schedule'){
   const initial={enabled:true,day:1,time:'02:00',server_time:'2026-10-17T15:00:00+08:00',current_month_batch:null,next_run_at:'2026-11-01 02:00',pending_run_at:'2026-10-01 02:00'};
   if(request.method()==='PUT'){saves++;assert.equal(request.postDataJSON().day,20);assert.equal(request.postDataJSON().time,'18:30');return ok({...initial,...request.postDataJSON()});}
   return ok(initial);
  }
  if(p==='export-batches'){assert.equal(request.method(),'POST');return ok(batch(request.postDataJSON().batch_id));}
  if(p.startsWith('export-batches/')){assert.equal(request.method(),'GET','Polling never submits a new batch');return ok(batch(p.split('/')[1]));}
  assert.equal(request.method(),'GET','No other business writes');
  if(p==='overview')return ok({version:1,counts:{total:3,powered:2,formal:1,test:1,off:1,unknown:0},rooms:[{id:'201',name:'201-M2机房',total:3,counts:{formal:1,test:1,off:1,unknown:0},types:{网络机柜:1,服务器机柜:2}}],racks:[],issues:[]});
  if(p.includes('/layout'))return ok({layout:{width:620,height:100,cells:mapCells},racks:[]});
  return ok({items:[],records:[],pending_count:0});
 });
 page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));page.on('console',message=>{if(message.type()==='error')errors.push(message.text());});await page.clock.install({time:new Date(now)});
 await page.goto(origin+'/');await page.getByRole('button',{name:'一键导出/上传所有楼栋',exact:true}).waitFor();
 await page.getByRole('button',{name:'设置',exact:true}).click();const dialog=page.getByRole('dialog',{name:'机柜月度自动归档设置'});
 await dialog.getByText('下次计划：2026-11-01 02:00',{exact:true}).waitFor();await dialog.getByText(/待补执行：2026-10-01/).waitFor();
 await dialog.getByRole('spinbutton',{name:'每月日期'}).fill('20');await dialog.getByLabel('开始时间',{exact:true}).fill('18:30');
 await dialog.getByText('保存后下次计划：2026-10-20 18:30',{exact:true}).waitFor();assert.equal(await dialog.getByText(/待补执行/).count(),0);assert.equal(saves,0);
 await dialog.getByRole('button',{name:'保存设置',exact:true}).click();await dialog.waitFor({state:'hidden'});assert.equal(saves,1);
 await page.getByRole('button',{name:'一键导出/上传所有楼栋',exact:true}).click();await page.getByText('飞书处理中，稍后自动继续原批次',{exact:true}).waitFor();
 assert.equal(await page.locator('.all-export-items>div').count(),5);const y=await page.locator('.all-export-items>div').evaluateAll(items=>items.map(item=>Math.round(item.getBoundingClientRect().top)));assert.equal(new Set(y).size,1);
 for(const scope of 'ABCDE')assert.match(await page.locator('.all-export-items>div').filter({hasText:scope+'楼'}).getByRole('link',{name:'下载'}).getAttribute('href'),new RegExp('fixture'+scope+'/download$'));
 await page.screenshot({path:path.join(shots,'legacy-five-buildings-1440.png'),fullPage:true});
 for(const width of [360,375,390]){await page.setViewportSize({width,height:width===360?640:667});await page.clock.runFor(50);assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));assert((await page.locator('.heading-title h1').boundingBox()).height<50,'phone heading must not be squeezed into a vertical column: '+await page.locator('.heading-title').evaluate(node=>JSON.stringify({viewport:innerWidth,width:node.getBoundingClientRect().width,flex:getComputedStyle(node).flex,font:getComputedStyle(node.querySelector('h1')).fontSize})));for(const link of await page.locator('.all-export-items a').all())assert((await link.boundingBox()).height>=44);await page.screenshot({path:path.join(shots,'legacy-five-buildings-'+width+'.png'),fullPage:true});}
 done=true;await page.clock.fastForward(2000);await page.getByText('五楼已归档',{exact:true}).waitFor();
 await page.goto(origin+'/?batch=1&mode=new');await page.getByRole('textbox',{name:'粘贴机柜确认文本',exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'粘贴文本识别',exact:true}).getAttribute('class'),'active');assert.equal(await page.getByRole('heading',{name:'上传确认单',exact:true}).count(),0);
 for(const width of [1440,390]){await page.setViewportSize({width,height:900});await page.goto(origin+'/?night=1');await page.getByRole('button',{name:'原始平面图',exact:true}).click();await page.locator('.map-cell').first().waitFor();
  for(const cell of await page.locator('.map-cell').all()){const paint=await cell.evaluate(node=>({text:node.textContent,ink:getComputedStyle(node).color,fill:getComputedStyle(node).backgroundColor,weight:getComputedStyle(node).fontWeight,family:getComputedStyle(node).fontFamily}));assert(Number(paint.weight)>=600);assert(!paint.family.includes('宋体'));const [r,g,b]=paint.ink.match(/\d+/g).map(Number);assert(r===g&&g===b&&[0,255].includes(r));}
  await page.screenshot({path:path.join(shots,'night-floorplan-'+width+'.png'),fullPage:true});
 }
 assert.deepEqual(errors,[]);console.log(`PASS: schedule live preview and catchup separation, no unsaved write, five columns/phone wrap, original batch waiting, text-first input, ${checked} real template labels checked and PC/mobile dark floorplan. No real business writes. Screenshots: ${shots}`);
}catch(error){if(page)await page.screenshot({path:path.join(shots,'failure.png'),fullPage:true});if(errors.length)console.error(errors);throw error;}
finally{await browser.close();await server.close();}
