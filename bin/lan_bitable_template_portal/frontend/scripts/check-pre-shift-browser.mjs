import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {preview} from 'vite';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),output=path.resolve(root,'../../../output/playwright/pre-shift');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
let writes=0,definitionWrites=0,submitted=false,fail=false,mode='operator',page;
const items=[{id:'x',text:'核对测试清单一'},{id:'y',text:'核对测试清单二：较长的检查说明应在手机屏幕自动换行，不挤压检查结果与异常备注'}];
let definition={version:1,items:structuredClone(items)};
try{
 const context=await browser.newContext({viewport:{width:1440,height:960},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const req=route.request(),u=new URL(req.url());if(u.origin!==origin)return route.abort();if(!u.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+mode,name:'隔离验收人员',role:mode==='manager'?'admin':'building',level:mode==='manager'?'L1':'L4',category:mode,home_scope:'A',modules:mode==='restricted'?['change']:['maintenance','polling','adjust'],can_delete_business:mode==='manager'},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
  if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'pre-shift-fixture'}});
  if(u.pathname==='/api/pre-shift-checks')return ok({scope:'A',scopes:mode==='manager'?['A','B']:['A'],can_manage:mode==='manager',definition,roster_ready:true,roster_error:'',tasks:[{id:'fixture-day',day:'2026-10-14',scope:'A',shift:'白',definition_version:1,items,people:[{name:'值班员一'},{name:'值班长二'}],submitted:submitted?'2026-10-14T09:05:00+08:00':'',actor_name:'隔离验收人员',can_submit:!submitted,deadline:Date.now()/1000+10000,answers:submitted?[{...items[0],status:'normal',note:''},{...items[1],status:'abnormal',note:'测试异常说明'}]:[],sync_status:submitted?'synced':'pending'}]});
  if(u.pathname==='/api/pre-shift-checks/submit'){writes++;if(fail){fail=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试：暂未确认，请重试'}});}assert.deepEqual(req.postDataJSON().answers,[{id:'x',status:'normal',note:''},{id:'y',status:'abnormal',note:'测试异常说明'}]);submitted=true;return ok({completed:true});}
  if(u.pathname==='/api/pre-shift-checks/definition'){definitionWrites++;definition={...req.postDataJSON(),version:definition.version+1};return ok(definition);}
  if(u.pathname==='/api/scope-overview')return ok({scopes:{A:{maintenance_pending:0,maintenance_ongoing:0}},source_snapshot_ready:true});
  if(u.pathname==='/api/workbench')return ok({records:[],ongoing:[],records_pagination:{total:0},ongoing_pagination:{total:0}});
  return ok({items:[],records:[],stats:{},summary:{}});
 });
 page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
 await page.goto(origin+'/assets/workspace-v2/index.html');
 const check=page.getByRole('region',{name:'班前检查',exact:true}).last();
 await page.getByText('本班待检查',{exact:true}).waitFor();
 assert.equal(await check.getByRole('radio',{checked:true}).count(),0,'no item is pre-checked');
 await page.getByRole('button',{name:'自定义工作台',exact:true}).click();assert.equal(await page.getByRole('button',{name:'移除班前检查',exact:true}).count(),0);await page.locator('.v2-dashboard-actions').getByRole('button',{name:'取消',exact:true}).click();
 await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('请逐项选择检查结果，并填写异常项备注。',{exact:true}).waitFor();assert.equal(writes,0);
 const rows=check.locator('fieldset');await rows.nth(0).locator('.n-radio-button').filter({hasText:'无异常'}).click();await rows.nth(1).locator('.n-radio-button').filter({hasText:'异常'}).last().click();
 await check.getByRole('button',{name:'提交检查',exact:true}).click();assert.equal(writes,0,'abnormal requires a note');await rows.nth(1).getByRole('textbox').fill('测试异常说明');
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:960});await check.scrollIntoViewIfNeeded();assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(output,'check-'+width+'.png'),fullPage:true});}
 await page.setViewportSize({width:1440,height:960});fail=true;await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('隔离测试：暂未确认，请重试',{exact:true}).waitFor();assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'测试异常说明');
 await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('本班已完成',{exact:true}).waitFor();assert.equal(writes,2);await check.getByText('已同步飞书',{exact:true}).waitFor();assert.equal(await check.getByRole('button',{name:'提交检查',exact:true}).count(),0);
 await page.reload();await page.getByText('本班已完成',{exact:true}).waitFor();assert.equal(writes,2);
 mode='manager';await page.reload();await page.getByRole('button',{name:'管理清单',exact:true}).click();const editor=page.locator('.v2-check-editor');await editor.getByRole('button',{name:'添加检查项',exact:true}).click();await editor.getByRole('textbox',{name:'检查项3',exact:true}).fill('新增测试检查项');await editor.getByRole('button',{name:'保存清单',exact:true}).click();await editor.waitFor({state:'hidden'});assert.equal(definitionWrites,1);assert.equal(definition.items.length,3);
 await page.getByRole('menuitem',{name:'SOP 步骤编辑',exact:true}).click();await page.locator('.v2-sop-manager.is-page').waitFor();assert.equal(await page.locator('.n-modal:visible').count(),0,'SOP editor is a page, not a notice modal');await page.getByRole('button',{name:'新增',exact:true}).click();await page.getByRole('textbox',{name:'SOP名称',exact:true}).fill('未保存模板');
 await page.getByRole('menuitem',{name:'个人工作台',exact:true}).click();await page.getByText('放弃未保存的SOP修改？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续编辑',exact:true}).click();
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:960});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(output,'sop-page-'+width+'.png'),fullPage:true});}
 await page.getByRole('button',{name:'返回通告办理',exact:true}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();await page.locator('.v2-sop-manager').waitFor({state:'detached'});assert.equal(await page.locator('.n-dialog:visible').count(),0,'leaving SOP confirms once');
 mode='restricted';await page.goto(origin+'/assets/workspace-v2/index.html?view=sops');await page.getByText('没有该功能权限',{exact:true}).waitFor();assert.equal(await page.getByRole('menuitem',{name:'SOP 步骤编辑',exact:true}).count(),0,'change-only accounts cannot open SOP editor');
 assert.deepEqual(errors,[]);console.log('PASS: mandatory widget, no default results, validation, abnormal note, retry preserves draft, completed reload, checklist editing, standalone SOP navigation/dirty guard, PC/mobile. No real writes.');
}catch(e){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw e;}finally{await browser.close();await server.close();}
