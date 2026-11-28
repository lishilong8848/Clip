import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {createServer} from 'vite';

// Locks the EOP scenario page's step layout: photos always visible (never behind a disclosure),
// steps as cards several per row, thumbnails that zoom slightly on hover and open a preview on click.
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const workspace=process.env.EOP_WORKSPACE==='1',previewOnly=process.env.EOP_PREVIEW_ONLY==='1';
const output=path.resolve(root,'../../../build_output/eop-step-cards');await mkdir(output,{recursive:true});
const svg=label=>'<svg xmlns="http://www.w3.org/2000/svg" width="320" height="240"><rect width="320" height="240" fill="#dfe8f5"/><text x="16" y="128" font-size="24" fill="#22384f">'+label+'</text></svg>';
const detail={id:'s1',fields:{'场景名称':'冷却泵切换','场景编号':'EOP-001','专业':'暖通','系统/设备':'冷冻水','原优先级':'P0'},
 revision:'1',editable:true,options:{},readonly_fields:[],
 steps:[
  {key:'a',text:'确认冷冻泵运行状态与出口压力。\n1. 记录读数\n2、核对现场',images:['t1','t2']},
  {key:'b',text:'切换备用泵并观察电流回落。',images:['t3']},
  {key:'c',text:'恢复阀门开度并记录读数。',images:[]},
  {key:'d',text:'通知值班主管确认切换结果。',images:[]}],
 images:[{token:'t1',name:'泵组全景',url:'/img/pump'},{token:'t2',name:'压力表',url:'/img/gauge'},{token:'t3',name:'控制柜',url:'/img/panel'}]};
const deliveries=[];
const server=await createServer({root,configFile:workspace?path.join(root,'vite.workspace-v2.config.ts'):undefined,
 cacheDir:path.join(root,'node_modules',workspace?'.vite-eop-v2':'.vite-eop-legacy'),
 ...(workspace?{optimizeDeps:{noDiscovery:true,include:['vue','naive-ui','lucide-vue-next']}}:{}),
 server:{host:'127.0.0.1',port:0},plugins:[{
 name:'eop-fixture',
 configureServer(s){
 s.middlewares.use('/api/',async(req,res)=>{
  const pathname=String(req.url||'').split('?')[0],ok=data=>{
   res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,data}));
  };
  if(pathname==='/auth/status')return ok({logged_in:true,user:{open_id:'fixture-eop',name:'核对人员',role:'admin',level:'L0',modules:['eop_scenarios']},allowed_scopes:['D']});
  if(pathname==='/health')return ok({service:'clipflow_backend',instance_id:'eop-fixture'});
  if(pathname==='/eop-scenarios')return ok({items:[{id:'s1',fields:detail.fields,has_steps:true,has_images:true}],updated_at:1});
  if(pathname==='/eop-scenarios/s1')return ok(detail);
  if(pathname==='/eop-scenarios/s1/send-card'){
   let input='';for await(const chunk of req) input+=chunk;
   deliveries.push(JSON.parse(input));
   const second=deliveries.length>1;
   return ok({recipients:[{name:'H楼值班账号',sent:true},{name:'李世龙',sent:second,...(!second?{error:'模拟暂未发送'}:{})}]});
  }
  return ok({});
 });
 s.middlewares.use('/img/',(req,res)=>{res.setHeader('Content-Type','image/svg+xml');res.end(svg(decodeURIComponent((req.url||'').slice(1).split('?')[0])));});
 s.middlewares.use('/__eop_fixture',async(_req,res)=>{res.setHeader('Content-Type','text/html');res.end(await s.transformIndexHtml('/__eop_fixture',`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><div id="app"></div><script type="module">
 import {createApp,h} from 'vue'; import Page from '/src/components/EopScenarioPage.vue';
 ${workspace?"import '/src/global.css'; import '/src/workspace-v2/workspace.css'; import '/src/workspace-v2/softInterface.css'; import '/src/workspace-v2/businessLayout.css'; document.documentElement.classList.add('workspace-v2');":''}
 createApp({setup(){return ()=>h('main',[h(Page)]);}}).mount('#app');
 if(new URLSearchParams(location.search).has('detail')) {
   const observer=new MutationObserver(()=>{const row=document.querySelector('.eop-list-row');if(row){observer.disconnect();row.click();}});
   observer.observe(document.getElementById('app'),{childList:true,subtree:true});
 }
 </script></body></html>`));});
}}]});
await server.listen();
const url='http://127.0.0.1:'+server.httpServer.address().port+'/__eop_fixture';
if(previewOnly){
 console.log(url+'?detail=1');
 await new Promise(resolve=>server.httpServer.once('close',resolve));
 process.exit(0);
}
const browser=await chromium.launch({headless:true});
try{
 const page=await browser.newPage({viewport:workspace?{width:390,height:844}:{width:1440,height:900}});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(url,{waitUntil:'domcontentloaded',timeout:90000});
 await page.locator('.eop-list-row').first().click({timeout:60000}).catch(async error=>{
   console.error((await page.locator('body').innerText()).slice(0,800),errors);
   throw error;
 });
 const steps=page.locator('.eop-step');
 await steps.first().waitFor({timeout:20000});
 assert.equal(await steps.count(),4);
 const send=page.getByRole('button',{name:'发送飞书卡片'});
 await send.click();
 await page.getByText(/已送达 H楼值班账号.*李世龙.*模拟暂未发送/).waitFor();
 await send.click();
 await page.getByText(/飞书卡片已发送给 H楼值班账号、李世龙/).waitFor();
 assert.match(deliveries[0].delivery_id,/^[0-9a-f]{32}$/);
 assert.equal(deliveries[0].revision,detail.revision);
 assert.deepEqual(deliveries[1],deliveries[0]);
 if(workspace){
  assert.equal(await page.locator('.scene-desk').count(),1,'实际使用新版模板');
  const outside=await page.locator('.scene-desk *').evaluateAll(elements=>elements.filter(element=>{
   const {width,left,right}=element.getBoundingClientRect();
   return width>0&&(left<-1||right>innerWidth+1)&&getComputedStyle(element).visibility==='visible';
  }).map(element=>element.className));
  assert.deepEqual(outside,[],'手机端场景内容无横向溢出');
  await page.screenshot({path:path.join(output,'workspace-phone.png')});
  assert.deepEqual(errors,[]);
  console.log('新版手机视口 EOP 发送与部分失败重试：OK');
 }else{

 // Photos are visible immediately: no disclosure to open, all three thumbnails on screen.
 assert.equal(await page.locator('.eop-step details').count(),0,'步骤图片不再折叠在 details 里');
 const figures=page.locator('.eop-step .eop-gallery figure');
 assert.equal(await figures.count(),3);
 for(let i=0;i<3;i++)assert(await figures.nth(i).isVisible(),'第 '+(i+1)+' 张缩略图直接可见');
 assert.match(await page.locator('.eop-step-media').first().innerText(),/对应图片/);

 // Steps are cards laid out several per row, not one tall row each.
 const tops=await steps.evaluateAll(items=>items.map(item=>Math.round(item.getBoundingClientRect().top)));
 assert.equal(tops[0],tops[1],'前两步应并排成卡片：'+JSON.stringify(tops));
 assert.ok(new Set(tops).size<tops.length,'应存在同一行内的多张步骤卡片');

 // Hover zooms the thumbnail slightly in place.
 const thumb=page.locator('.eop-step .eop-thumb img').first();
 await thumb.hover();
 await page.waitForTimeout(300);
 const transform=await thumb.evaluate(el=>getComputedStyle(el).transform);
 assert.match(transform,/^matrix\(1\.0[0-9]/,'悬停应轻微放大：'+transform);

 // Click opens the enlarged preview.
 await page.locator('.eop-step .eop-thumb').first().click();
 const preview=page.locator('.eop-preview');
 await preview.waitFor();
 await preview.locator('.eop-original').waitFor();
 assert.match(await preview.locator('.eop-original').getAttribute('src')||'',/\/img\/pump/);
 await page.screenshot({path:path.join(output,'preview.png')});
 await page.keyboard.press('Escape');
 await preview.waitFor({state:'hidden'});
 await page.screenshot({path:path.join(output,'cards-1440.png'),fullPage:true});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),true,'无横向溢出');
 assert.deepEqual(errors,[]);
 console.log('EOP 步骤卡片、图片常显、悬停微放大与点击预览：OK');
 }
}finally{await browser.close();await server.close();}
