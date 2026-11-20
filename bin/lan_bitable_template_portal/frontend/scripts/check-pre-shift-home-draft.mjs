import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

// Exercise the actual V2 home, including its read-only consumption of older saved widget positions.
const root=fileURLToPath(new URL('../',import.meta.url));
const buildDir=path.resolve(root,process.env.CLIPFLOW_WORKSPACE_BUILD_DIR || 'dist/assets/workspace-v2');
const origin='http://pre-shift-fixture.invalid',owner='fixture-operator';
const api='/api/pre-shift-checks',layoutPath='/api/workspace-layouts/desktop/workspace';
const cacheKey='clipflow:account-layout:v1:'+owner+':desktop:workspace';
const saved={revision:9,value:{version:1,widgets:[
  {id:'my-check',kind:'pre_shift',title:'班前检查',x:3,y:2,w:6,h:5},
  {id:'my-tasks',kind:'tasks',title:'个人待办',x:0,y:0,w:3,h:4,taskKinds:['maintenance']},
]}};
const layouts=layoutApiFixture();
layouts.records.set(owner+':'+layoutPath,structuredClone(saved));
const now='2026-11-19T08:00:00+08:00',seconds=Date.parse(now)/1000;
const items=[{id:'handover',text:'核对值班交接记录'},{id:'alerts',text:'确认告警情况'}];
const tasks=['白','夜'].map((shift,i)=>({id:'check-'+i,scope:'A',day:'2026-11-19',shift,
  definition_version:3,due:seconds+i*3600,deadline:seconds+86400,items,can_submit:true,
  can_upload_handover:true,people:[],files:[],answers:[]}));
const browser=await chromium.launch();
try{
  const page=await browser.newPage({viewport:{width:1440,height:960},reducedMotion:'reduce'});
  const calls=[],errors=[];
  page.on('pageerror',err=>errors.push(err.message));
  await page.addInitScript(({cacheKey,saved})=>localStorage.setItem(cacheKey,JSON.stringify(saved)),{cacheKey,saved});
  await page.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),p=url.pathname;
    if(url.origin!==origin){errors.push('Unexpected request outside fixture');return route.abort();}
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(p.startsWith('/assets/workspace-v2/'))return route.fulfill({path:path.join(buildDir,path.basename(p))});
    if(p==='/assets/vnet-logo.png')return route.fulfill({path:path.join(root,'public/assets/vnet-logo.png')});
    if(!p.startsWith('/api/'))return route.fulfill({status:404,body:''});
    calls.push([p,req.method()]);
    if(await layouts.handle(route,owner))return;
    if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:owner,name:'隔离验收人员',role:'building',level:'L4',category:'operator',home_scope:'A',modules:['maintenance','polling','adjust']},scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
    if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated'}});
    if(p===api)return ok({scope:'A',scopes:['A'],definition:{version:3,items},server_time:now,roster_ready:true,tasks});
    if(p===api+'/history')return ok({scope:'A',page:1,page_size:20,total:0,items:[]});
    return ok({items:[],records:[],stats:{},summary:{}});
  });
  await page.goto(origin+'/assets/workspace-v2/index.html');
  const check=page.locator('.v2-shift-check'),row=check.locator('fieldset').first();
  await row.getByRole('radio',{name:'异常',exact:true}).locator('xpath=ancestor::label[1]').click();
  const note=row.getByRole('textbox'),text='等待现场复核';
  await note.fill(text);
  assert.equal(await check.getAttribute('data-v2-layout-protected'),'true');
  await Promise.all([page.waitForResponse(r=>new URL(r.url()).pathname===api),check.getByRole('button',{name:'刷新班前检查'}).click()]);
  assert.equal(await note.inputValue(),text);
  await check.locator('.v2-check-choice').nth(1).click();
  assert.equal(await check.getByRole('radio',{checked:true}).count(),0);
  await check.locator('.v2-check-choice').first().click();
  assert.equal(await note.inputValue(),text);
  await check.getByRole('button',{name:'历史检查记录',exact:true}).click();
  await check.getByRole('button',{name:'返回检查清单',exact:true}).click();
  assert.equal(await note.inputValue(),text);
  assert(calls.some(([p,m])=>p===layoutPath && m==='GET'));
  assert.equal(layouts.writes,0);
  assert.deepEqual(layouts.records.get(owner+':'+layoutPath),saved);
  assert.equal(await page.evaluate(k=>localStorage.getItem(k),cacheKey),JSON.stringify(saved));
  assert(!calls.some(([p])=>p===api+'/submit'));
  assert.deepEqual(errors,[]);
  console.log('PASS 当前首页读取原布局；班次、历史与刷新切换保留同版本草稿，无隐式提交或布局写入。');
}finally{await browser.close();}
