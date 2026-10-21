import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { preview } from 'vite';
import { chromium } from 'playwright';
import { parse, compileScript } from '@vue/compiler-sfc';
import { adaptLearningSharing } from './workspace-sharing-adapter.mjs';
import { adaptModuleAdminEntry } from './workspace-admin-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/sharing-admin');await mkdir(output,{recursive:true});
for(const name of ['LearningSharing','DrillManagementPage','CriticalGuardPage']){
  const source=await readFile(path.join(root,'src/components/'+name+'.vue'),'utf8');
  const adapted=name==='LearningSharing'?adaptLearningSharing(source):adaptModuleAdminEntry(source,name+'.vue');
  const parsed=parse(adapted);assert.deepEqual(parsed.errors,[]);compileScript(parsed.descriptor,{id:name,inlineTemplate:true});
}
const server=await preview({root,logLevel:'error',build:{outDir:process.env.CLIPFLOW_SHARING_PREVIEW_DIR || 'dist'},preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[],reads=[];
let role='admin',version=1,failSave=true,publishes=0,deletes=0,saved;
const scopes=['ALL','A','B','C','D','E','H','110'].map(value=>({value,label:value==='ALL'?'全部楼栋':value+'楼'}));
const batch={id:'sharing-fixture',title:'变压器告警排查与现场安全分享',url:'https://example.test/minutes',status:'draft',version,question_count:8,completed:1,total:26,
  questions:Array.from({length:8},(_,i)=>({id:'q'+i,type:i===1?'multiple':i===2?'interview':'single',stem:'第'+(i+1)+'题：操作前应先核对哪些条件？',options:i===2?[]:['核实现场条件','直接操作','核对设备编号','确认人员到位'],correct:i===1?[0,2,3]:i===2?[]:[0],reference_answer:i===2?'确认安全条件并按批准流程操作。':'',explanation:'以纪要原文为依据。',evidence:'操作前必须核实设备及现场条件。'})),
  participants:Array.from({length:26},(_,i)=>({open_id:'person-'+i,name:'参会人员'+i,scope:i%2?'B':'A',exempted:false,submitted_at:i===0?'2026-10-14T10:00:00':'',choice_count:i===0?4:0,correct:i===0?3:0})),
  results:[{open_id:'person-0',name:'参会人员0',question_ids:['q0','q1','q2','q3','q4'],answers:{q0:[0],q2:'核对安全条件。'},submitted_at:'2026-10-14T10:00:00'}],
  bank_sync:{total:8,synced:6,failed:2,error:'隔离同步失败记录'},weekly_reports:[{date:'2026-10-14',end:'10-14 10:00',part:1,parts:1,status:'sent',sent_at:'2026-10-14T10:00:00'}]};
try{
 const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url()),p=url.pathname;
  if(url.origin!==origin)return route.abort();if(!p.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});reads.push(p+url.search);
  if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'sharing-admin-fixture'}});
  if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'sharing-'+role,name:'隔离验收',role,level:role==='admin'?'L0':'L4',modules:['learning','drills','critical_guard','accounts']},scope_options:scopes});
  if(p==='/api/learning/bootstrap')return ok({is_admin:role==='admin',can_answer:true,can_view_buildings:true,self_person:{id:'person-self',name:'隔离本人'},self_scope:'A',scopes,today:'2026-10-14',settings:{enabled:true},sync:{}});
  if(p==='/api/learning/sharing')return ok({items:deletes?[]:[batch],total:deletes?0:1});
  if(p==='/api/learning/sharing/sharing-fixture'){
   if(req.method()==='PUT'){
    if(failSave){failSave=false;return route.fulfill({status:503,json:{ok:false,error:'隔离保存失败'}});}
    saved=req.postDataJSON();assert.equal(saved.version,batch.version);batch.questions=saved.questions;batch.participants.forEach(item=>item.exempted=saved.excluded_open_ids.includes(item.open_id));batch.version++;
   }
   return ok(batch);
  }
  if(p==='/api/learning/sharing/sharing-fixture/publish'){
    assert.equal(req.postDataJSON().version,batch.version);publishes++;batch.status='published';batch.version++;return ok({id:batch.id});
  }
  if(p==='/api/learning/sharing/sharing-fixture/delete'){
    assert.equal(req.postDataJSON().version,batch.version);deletes++;return ok({id:batch.id,deleted:true});
  }
  assert.equal(req.method(),'GET','unexpected write '+p);
  if(p==='/api/drills/bootstrap')return ok({scopes,drills:[],items:[]});
  if(p==='/api/critical-guard/bootstrap')return ok({scopes,tasks:[]});
  if(p==='/api/auth/accounts')return ok({items:[],scope_options:scopes,category_options:[],module_options:[],policy:{level:'L0'}});
  return ok({items:[],records:[],users:[],scopes:[],fields:[],stats:{},summary:{},settings:{}});
 });
 const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
 const entry=origin+'/assets/workspace-v2/index.html';
 const go=async route=>page.goto(entry+'?__page='+route);
 const section=()=>page.locator('.sharing-exercises');
 const tab=async text=>section().locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:text}).click();
 const screenshot=async name=>{await page.waitForTimeout(250);await page.screenshot({path:path.join(output,name+'.png'),animations:'disabled'});};
 await go('/learning&tab=sharing');await page.locator('.sharing-batch').click();
 await section().getByLabel('题干',{exact:true}).first().fill('核对题干修改应跨分区保留');
 assert.equal(await page.locator('.v2-sharing-questions>.sharing-question').count(),5,'five questions per page');
 assert.equal(await page.getByRole('heading',{name:'参会人答题情况',exact:true}).count(),0);
 await page.locator('.sharing-question[data-type=multiple]').waitFor();
 await section().getByRole('checkbox',{name:'第2题选项B为正确答案',exact:true}).check();
 await section().getByLabel('参考答案（问答题不自动判分）').fill('核对参考答案修改');
 await screenshot('questions-pc');
 await tab('参会人');assert.equal(await section().locator('tbody tr').count(),12,'bounded attendee rows');
 await section().getByRole('checkbox',{name:'参加答题：参会人员1',exact:true}).uncheck();
 await section().getByRole('searchbox',{name:'搜索参会人'}).fill('参会人员25');
 assert.equal(await section().locator('tbody tr').count(),1);
 await section().getByRole('searchbox',{name:'搜索参会人'}).clear();
 await section().getByRole('button',{name:'查看答卷',exact:true}).first().click();
 await page.locator('.v2-sharing-paper').waitFor();assert.equal(await page.locator('.v2-sharing-paper .sharing-question').count(),5);
 await page.keyboard.press('Escape');await page.locator('.v2-sharing-paper').waitFor({state:'hidden'});
 await tab('通知与同步');await section().getByText('题库同步 6 / 8 题',{exact:true}).waitFor();
 assert.equal(await section().locator('.sharing-question').count(),0);
 await tab('题目核对');
 assert.equal(await section().getByLabel('题干',{exact:true}).first().inputValue(),'核对题干修改应跨分区保留');
 await section().getByRole('button',{name:'保存题目与名单',exact:true}).click();await page.getByText('隔离保存失败',{exact:true}).waitFor();
 assert.equal(await section().getByLabel('题干',{exact:true}).first().inputValue(),'核对题干修改应跨分区保留');
 await section().getByRole('button',{name:'保存题目与名单',exact:true}).click();await page.getByText('题目与答题名单已保存。',{exact:true}).waitFor();
 assert.deepEqual(saved.excluded_open_ids,['person-1']);assert.deepEqual(saved.questions[1].correct,[0,1,2,3]);assert.equal(saved.questions[2].reference_answer,'核对参考答案修改');
 for(const width of [1366,390]){
  await page.setViewportSize({width,height:width<761?844:768});await screenshot('questions-'+width);
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await tab('参会人');await screenshot('people-'+width);await tab('题目核对');
 }
 await section().getByRole('button',{name:'核对后发布给参会人',exact:true}).click();assert.equal(publishes,0);
 await page.getByRole('dialog').getByRole('button',{name:'确认',exact:true}).click();await section().getByRole('button',{name:'关闭本批',exact:true}).waitFor();assert.equal(publishes,1);
 await page.reload();await page.locator('.sharing-batch').click();await page.getByRole('heading',{name:'参会人答题情况',exact:true}).waitFor();
 assert.equal(await section().getByRole('checkbox').count(),0,'published roster is immutable');
 await page.setViewportSize({width:1440,height:900});
 if(!process.argv.includes('--sharing-only')) {
 for(const url of ['/drill-management','/critical-guard']){
  for(const scope of ['A','H']){
   await go(url+'&scope='+scope);const admin=page.getByRole('button',{name:'管理员入口',exact:true});await admin.waitFor();await admin.click();
   await page.locator(url.includes('drill')?'.drill-page .admin-view':'.guard-page .admin-view').waitFor();
   assert(await page.getByRole('button',{name:'管理员入口',exact:true}).isDisabled());await screenshot(url.slice(1)+'-'+scope);
  }
  await page.getByLabel('当前楼栋',{exact:true}).click();await page.locator('.n-base-select-option').getByText('B楼',{exact:true}).click();
  await page.locator(url.includes('drill')?'.drill-page .building-view':'.guard-page .building-view').waitFor();
  assert(!await page.getByRole('button',{name:'管理员入口',exact:true}).isDisabled(),'building switch leaves the admin view');
 }
 await go('/admin/settings');await page.getByRole('searchbox',{name:'搜索账号',exact:true}).waitFor();
 assert.equal(await page.locator('.admin-workspace-tabs').count(),0);assert.equal(await page.locator('.v2-sidebar').count(),1);
 await page.locator('.v2-settings-picker .n-select').click();await page.locator('.n-base-select-option').filter({hasText:'MOP 配置'}).click();
 await screenshot('settings');assert.equal(await page.locator('.v2-settings-picker').count(),1);
 role='member';await go('/drill-management&scope=A&mode=admin');await page.locator('.drill-page').waitFor();assert.equal(await page.getByRole('button',{name:'管理员入口',exact:true}).count(),0);assert.equal(await page.locator('.drill-page .admin-view').count(),0);
 await go('/critical-guard&scope=A');await page.locator('.guard-page').waitFor();assert.equal(await page.getByRole('button',{name:'管理员入口',exact:true}).count(),0);
 }
 role='member';
 await go('/learning&tab=sharing');await page.locator('.sharing-batch').click();await tab('我的题目');
 assert.equal(await section().locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:'通知与同步'}).count(),0);assert.equal(await section().locator('textarea').count(),0);
 assert.equal(await section().getByRole('button',{name:'删除批次',exact:true}).count(),0);
 role='admin';await go('/learning&tab=sharing');await page.locator('.sharing-batch').click();
 await section().getByRole('button',{name:'删除批次',exact:true}).click();const confirmation=page.locator('.n-dialog');
 assert.match(await confirmation.innerText(),/已上传到多维题库的题目不会删除/);assert.equal(deletes,0);
 await confirmation.getByRole('button',{name:'取消',exact:true}).click();assert.equal(deletes,0);
 await section().getByRole('button',{name:'删除批次',exact:true}).click();await confirmation.getByRole('button',{name:'删除批次',exact:true}).click();
 await section().getByText('批次已删除，已上传题目保留。',{exact:true}).waitFor();assert.equal(deletes,1);assert.equal(await page.locator('.sharing-batch').count(),0);
 assert.deepEqual(errors,[]);console.log('PASS: sharing sections, five-question editing, draft preservation/retry, participant pagination, result modal, original confirmation, PC/mobile and delete confirmation. All writes mocked.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
