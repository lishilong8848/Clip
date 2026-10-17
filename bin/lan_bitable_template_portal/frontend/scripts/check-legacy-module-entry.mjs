import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/legacy-module-entry');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],riskScopes=[],writes=[];
let page,modules=['risk_inspection','critical_guard','daily','work_handover'];
try{
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(req.method()!=='GET'){writes.push(req.url());return route.abort();}
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_entry_fixture',name:'隔离测试',role:'building',level:'L4',category:'engineer',home_scope:'A',scope_rules:['A','B'],modules},scope_options:['A','B'].map(value=>({value,label:value+'楼'}))});
    if(url.pathname==='/api/scope-overview')return ok({scopes:{A:{},B:{}},source_snapshot_ready:true});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'entry-fixture'}});
    if(url.pathname==='/api/critical-guard/bootstrap')return ok({scopes:['A','B'].map(value=>({value,label:value+'楼',authorized:true,pending:0,completed:0}))});
    if(url.pathname==='/api/risk-inspections'){riskScopes.push(url.searchParams.get('scope'));return ok({items:[],total:0,page:1,is_manager:false,can_edit_template:false});}
    if(url.pathname==='/api/work-handovers/people')return ok({items:[],modules:[],scopes:['A','B'],default_modules:[],self:{home_scope:'A'}});
    if(url.pathname==='/api/work-handovers')return ok({items:[],total:0});
    return ok({items:[],records:[],stats:{},summary:{}});
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:width<700?844:900});
    await page.goto(origin+'/');
    const card=page.locator('.module-risk');await card.waitFor();
    assert.equal(await card.getByRole('button').count(),1,'risk has one homepage entry, not a separate inspection shortcut');
    await card.getByRole('button',{name:'进入风险管理',exact:true}).click();
    await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();
    await page.getByRole('button',{name:'选择重保管理',exact:true}).waitFor();
    await page.screenshot({path:path.join(output,'risk-menu-'+width+'.png'),fullPage:true});
    await page.getByRole('button',{name:'选择风险排查',exact:true}).click();
    await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    assert.equal(await page.locator('.scope-card').count(),2);
    assert.equal(await page.locator('.scope-summary-strip').count(),0,'inspection must not show unrelated notice counts');
    await page.screenshot({path:path.join(output,'risk-buildings-'+width+'.png'),fullPage:true});
    await page.locator('.scope-card').filter({hasText:'B楼'}).getByRole('button',{name:'进入风险排查',exact:true}).click();
    await page.waitForURL('**/risk-inspections?scope=B');
    await page.getByRole('region',{name:'风险排查',exact:true}).getByRole('button',{name:'返回',exact:true}).click();
    await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    await page.locator('#page-back-slot .vnet-back-button').click();
    await page.getByRole('button',{name:'选择重保管理',exact:true}).click();
    await page.getByRole('heading',{name:'选择楼栋',exact:true}).waitFor();
    assert.equal(await page.locator('.building-grid .building-card').count(),2);
    await page.waitForFunction(()=>document.querySelectorAll('#page-back-slot .vnet-back-button').length===1);
    await page.locator('#page-back-slot .vnet-back-button').click();
    await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();
    await page.goto(origin+'/?entry=tools');
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).click();
    await page.getByRole('heading',{name:'选择楼栋查看每日任务',exact:true}).waitFor();
    assert.equal(await page.locator('.scope-card').count(),2);
    await page.locator('#page-back-slot .vnet-back-button').click();
    await page.getByRole('button',{name:'选择人员工作交接',exact:true}).click();
    await page.waitForURL('**/work-handover');
    await page.getByRole('region',{name:'人员工作交接',exact:true}).getByRole('button',{name:'返回',exact:true}).click();
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).waitFor();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  }
  modules=['risk_inspection'];await page.goto(origin+'/');
  await page.locator('.module-risk').getByRole('button',{name:'进入风险管理',exact:true}).click();
  await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'选择重保管理',exact:true}).count(),0,'module grants are not broadened');
  assert(riskScopes.length>=2&&riskScopes.every(scope=>scope==='B'),'selected scope reaches the existing API');
  assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
  console.log('PASS: legacy risk menu/building selection, scoped requests, daily-task building selection, inline back buttons, permission-filtered entries, PC/mobile. No real business writes.');
}catch(error){if(page)await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw error;}
finally{await browser.close();await server.close();}
