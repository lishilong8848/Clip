import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const shots=path.resolve(root,'../../../output/playwright/cabinet-buildings');
await mkdir(shots,{recursive:true});
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch();
const buildings='ABCDE'.split('').map((scope,i)=>({scope,bootstrap_status:'succeeded',counts:{total:1000+i*10,formal:800+i*10,test:20,off:180,unknown:0},record_count:50+i,inventory_only:0}));
const errors=[],reads=[],writes=[];
let allowed='ABCDE',guest=false,complete=false,failBuildings=false,partialInit=false,releaseOverview,page;
try{
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  const layouts=layoutApiFixture();
  await context.route('**/*',async route=>{
    try{
      const req=route.request(),url=new URL(req.url());
      if(url.origin!==origin)return route.abort();
      if(!url.pathname.startsWith('/api/'))return route.continue();
      if(await layouts.handle(route,'buildings-'+allowed+guest))return;
      reads.push(url);
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'cabinet-buildings-isolated'}});
      if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'buildings-'+allowed+guest,name:'隔离验收',role:guest?'guest':allowed.length===5?'admin':'member',level:guest?'L4':'L1',modules:['cabinet']},scope_options:[...allowed,...(allowed.length===5?['H']:[])].map(value=>({value,label:value+'楼'})),default_scope:allowed[0]});
      const p=url.pathname.replace('/api/cabinet-power/',''),visible=buildings.filter(b=>allowed.includes(b.scope));
      if(p==='bootstrap')return ok({status:partialInit?'partial':'succeeded',ready:partialInit?4:visible.length,total:visible.length,buildings:visible.map(row=>({scope:row.scope,status:partialInit&&row.scope==='E'?'failed':'succeeded',error:'隔离初始化失败'}))});
      if(p==='buildings'){
        if(failBuildings)return route.fulfill({status:503,json:{ok:false,error:'隔离总览读取失败'}});
        return ok({buildings:visible.map(b=>partialInit&&b.scope==='E'?{...b,bootstrap_status:'failed',bootstrap_error:'隔离初始化失败'}:b)});
      }
      if(p==='overview'){
        const scope=url.searchParams.get('scope');assert(allowed.includes(scope),'no unauthorized overview request');
        const row=buildings.find(b=>b.scope===scope);
        const data={version:scope,updated_at:'2026-10-17 12:00',record_count:row.record_count,issues:[],counts:{...row.counts,powered:row.counts.formal+row.counts.test},rooms:[{id:'201',name:scope+'-201机房',total:row.counts.total,counts:row.counts,types:{网络机柜:100,服务器机柜:row.counts.total-100}}]};
        if(scope==='B'&&!releaseOverview)return new Promise(resolve=>{releaseOverview=async()=>{try{await ok(data);}catch{}finally{resolve();}};});
        return ok(data);
      }
      if(p==='racks')return ok({version:url.searchParams.get('scope'),items:[]});
      if(p==='export-batches'&&req.method()==='POST'){writes.push(req.postDataJSON());}
      else assert.equal(req.method(),'GET','no unrequested write in isolated browser test');
      if(p==='export-batches'||p.startsWith('export-batches/')){
        const id=p==='export-batches'?req.postDataJSON().batch_id:p.split('/')[1];
        return ok({batch_id:id,status:complete?'succeeded':'running',phase:complete?'completed':'archiving',retry_at:complete?0:Date.now()/1000+30,items:Object.fromEntries(buildings.map(b=>[b.scope,{scope:b.scope,status:'succeeded',result:{export_id:'export-'+b.scope}}]))});
      }
      return ok({items:[],records:[],total:0,pending_count:0});
    }catch(error){errors.push(error.message);await route.abort();}
  });
  page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  const go=(query='scope=A')=>page.goto(origin+'/assets/workspace-v2/index.html?__page=/cabinet-power&'+query);
  const picker=page.getByRole('combobox',{name:'选择机柜楼栋',exact:true});
  const choose=async scope=>{await picker.click();await page.locator('.v2-single-select-menu .n-base-select-option').getByText(scope+'楼',{exact:true}).click();};
  const overview=page.getByRole('button',{name:'五楼总览',exact:true});
  await go();await page.locator('.v2-cabinet-status').waitFor();
  for(const width of [1440,360,375,390]){
    await page.setViewportSize({width,height:width<700?640:900});
    await overview.click();await page.locator('.buildings .building').last().waitFor();
    assert.equal(await page.locator('.buildings .building').count(),5);
    assert.equal(await page.locator('.v2-cabinet-status').count(),0,'overview must not retain single-building data');
    assert.equal(await overview.getAttribute('aria-pressed'),'true');
    for(const row of buildings)assert.match(await page.locator('.building').filter({hasText:row.scope+'楼'}).innerText(),new RegExp(row.counts.total+'[\\s\\S]*上电率'));
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'no overview overflow '+width);
    await page.screenshot({path:path.join(shots,'overview-'+width+'.png')});
    await choose('C');await page.getByRole('button',{name:/^C-201机房/}).waitFor();
    assert.equal(await overview.getAttribute('aria-pressed'),'false');
    assert.match(await page.locator('.v2-cabinet-status').innerText(),/1020/);
    await page.screenshot({path:path.join(shots,'single-C-'+width+'.png')});
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'no single page overflow '+width);
    await page.reload();await page.getByRole('button',{name:/^C-201机房/}).waitFor();
  }
  await choose('B');await page.getByText('正在读取台账…',{exact:true}).waitFor();
  await choose('D');await page.getByRole('button',{name:/^D-201机房/}).waitFor();
  await releaseOverview();await page.waitForTimeout(100);
  assert.equal(await page.getByRole('button',{name:/^B-201机房/}).count(),0,'late prior-building response never replaces current data');
  await overview.click();await page.getByRole('button',{name:'一键导出/上传所有楼栋',exact:true}).click();
  await page.getByText('五份附件合并写入同一条记录，飞书处理中，稍后自动继续原批次',{exact:true}).waitFor();
  assert(await page.getByRole('button',{name:'核验归档进度',exact:true}).isEnabled());
  assert.equal(writes.length,1);const id=writes[0].batch_id;
  await page.reload();await page.getByText('五份附件合并写入同一条记录，飞书处理中，稍后自动继续原批次',{exact:true}).waitFor();
  assert.equal(writes.length,1,'reload only queries the same batch');
  complete=true;await page.getByText('五楼已归档',{exact:true}).waitFor();
  assert(reads.some(u=>u.pathname.endsWith('/export-batches/'+id)));
  failBuildings=true;await page.reload();await page.getByText('隔离总览读取失败',{exact:true}).first().waitFor();
  assert.equal(await page.locator('.building').count(),0,'failed read is not presented as zero counts');failBuildings=false;
  partialInit=true;await page.reload();await page.getByRole('button').filter({hasText:'隔离初始化失败'}).waitFor();
  assert(await page.locator('.building').filter({hasText:'E楼'}).isDisabled(),'uninitialized building cannot be treated as loaded');partialInit=false;
  allowed='AC';await go('view=overview');await page.getByRole('button',{name:'楼栋总览',exact:true}).waitFor();
  assert.equal(await page.locator('.building').count(),2);await picker.click();
  assert.equal(await page.locator('.v2-single-select-menu .n-base-select-option').count(),3);assert.equal(await page.locator('.v2-single-select-menu').getByText('B楼',{exact:true}).count(),0);
  await page.keyboard.press('Escape');await go('scope=B');await page.getByText('没有该功能权限',{exact:true}).waitFor();
  allowed='A';guest=true;await go('view=overview');await page.locator('.building').waitFor();
  assert.equal(await page.locator('.building').count(),1);
  assert.equal(await page.getByRole('button',{name:'一键导出/上传所有楼栋',exact:true}).count(),0);
  assert.equal(await page.getByRole('button',{name:'批量登记',exact:true}).count(),0);
  await choose('A');await page.getByRole('button',{name:/^A-201机房/}).waitFor();
  assert.equal(await page.getByRole('button',{name:'一键导出/上传所有楼栋',exact:true}).count(),0);
  assert.deepEqual(errors,[]);
  console.log('PASS: V2 five-building overview, authorized single-building switch, reload, late reads, pending original export, failure/initialization/partial permission/guest, 1440/360/375/390. All APIs isolated. Screenshots: '+shots);
}catch(error){if(page){await page.screenshot({path:path.join(shots,'failure.png')});console.error(page.url(),await page.locator('.v2-main').innerText(),reads.slice(-12).map(u=>u.pathname+u.search),errors);}throw error;}
finally{if(releaseOverview)await releaseOverview();await browser.close();await server.close();}
