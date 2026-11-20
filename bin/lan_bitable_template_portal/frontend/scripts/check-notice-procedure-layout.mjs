// Run against responsive-audit-server.mjs serving an isolated V2 build.
import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdir,writeFile} from 'node:fs/promises';
import {chromium} from 'playwright';

const [address, directory, suite='all']=process.argv.slice(2);
const origin=new URL(address).origin;
assert.equal(new URL(origin).hostname,'127.0.0.1');
assert.equal((await (await fetch(origin+'/api/health')).json()).instance_id,'responsive-isolated-preview');
const output=path.resolve(directory);
await mkdir(output,{recursive:true});
const kinds=['maintenance','change','repair','polling','adjust','power'];
const robotId='c'.repeat(32), robotName='隔离值班群';
const browser=await chromium.launch();
const metrics=[];
const entry=origin+'/assets/workspace-v2/index.html?';
const workbench=kind=>entry+'__page=/workbench-lite&scope=A&work_type='+kind;
const button=(parent,name)=>parent.getByRole('button',{name,exact:true});
const historyRow={record_id:'history-demo',work_type:'maintenance',notice_type:'维保通告',title:'A楼冷冻水泵月度维护',building:'A楼',start_time:'2026-11-19 09:00',end_time:null,status:'进行中'};

async function check(width){
 const context=await browser.newContext({viewport:{width,height:width===393?852:950},serviceWorkers:'block',reducedMotion:'reduce'});
 const page=await context.newPage(),writes=[],errors=[],drafts=new Map();
 page.setDefaultTimeout(8000);
 page.on('pageerror',e=>errors.push(e.message));
 let confirmed=false,historyValue='运行参数已记录';
 const proof='隔离变更核验',change={target_record_id:'proof-demo',building_codes:['A'],building:'A楼',title:proof,started_at:'2026-11-19 09:15',screenshot_items:[{file_token:'evidence',name:'现场确认.png'}],screenshot_count:1};
 await context.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url()),p=url.pathname;
   if(url.origin!==origin)return route.abort();
   if(!['GET','HEAD'].includes(req.method()))writes.push({p,body:req.postDataJSON()});
   const ok=data=>route.fulfill({json:{ok:true,data}});
   if(p==='/api/workbench/draft'){
     const params=req.method()==='GET'?Object.fromEntries(url.searchParams):req.postDataJSON();
     const key=JSON.stringify(['scope','work_type','active_item_id','source_record_id','target_record_id','manual_id'].map(k=>params[k]||''));
     if(req.method()==='PUT')drafts.set(key,{...params,version:(drafts.get(key)?.version||0)+1});
     return ok({draft:drafts.get(key)||null});
   }
   if(p==='/api/workbench' && kinds.includes(url.searchParams.get('work_type'))){
     const payload=await (await route.fetch()).json();
     for(const [i,row] of payload.data.records.entries()){
       row.plan_start='2026-11-'+(19+i)+' 09:00';row.plan_end='2026-11-'+(19+i)+' 18:30';
     }
     return route.fulfill({json:payload});
   }
   if(p==='/api/workbench/lite-detail' && url.searchParams.get('work_type')==='repair'){
     // Repair stores expected completion in start_time and discovery in end_time.
     const payload=await (await route.fetch()).json();
     payload.data.fragments.detail=payload.data.fragments.detail.replace(/(name="start_time" type="datetime-local" value="[0-9-]+T)09:00/g,(_,prefix)=>prefix+'18:30')
       .replace(/(name="end_time" type="datetime-local" value="[0-9-]+T)18:30/g,(_,prefix)=>prefix+'09:00');
     return route.fulfill({json:payload});
   }
   if(/^\/api\/jobs\/audit-job-\d+$/.test(p)){
     const payload=await (await route.fetch()).json();
     Object.assign(payload.data,{message_first:true,feishu_group_state:'sent',message_delivery_state:'skipped',dingtalk_state:'sent',archive_state:'success'});
     return route.fulfill({json:payload});
   }
   if(p==='/api/workbench/dingtalk-options')return ok({robots:[{id:robotId,name:robotName,scopes:['A'],types:[]}],selected_robot_ids:[]});
   if(p==='/api/change-confirmations')return ok({items:[{...change,state:confirmed?'confirmed':'awaiting_confirmation'}],can_confirm:true});
   if(p==='/api/change-confirmations/proof-demo/confirm'){confirmed=true;return ok({});}
   if(p.endsWith('/screenshot/preview'))return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="240" height="120"><rect width="240" height="120" fill="#cee9dd"/><text x="25" y="64">A楼 · 现场确认</text></svg>'});
   if(p==='/api/notice-history')return ok({month:url.searchParams.get('month'),items:[historyRow],total:1,page:1,page_size:20,warnings:[],types:[{value:'maintenance',label:'维保通告'}],scope_options:[{value:'A',label:'A楼'}]});
   if(p==='/api/notice-history/detail')return ok({record:historyRow,revision:2,editable:true,groups:[{key:'target',label:'已发通告',record_id:historyRow.record_id,fields:[{id:'progress',name:'现场进展',type:1,value:historyValue,editable:true}]}]});
   if(p==='/api/notice-history/save'){
     const body=req.postDataJSON();assert.equal(body.expected_revision,2);historyValue=body.changes.target.progress;
     return ok({operation_id:body.operation_id,status:'completed',message:'更正已保存，未发送消息。',steps:[]});
   }
   return route.continue();
 });
 async function snap(name,focus){
   if(focus){await focus.waitFor();await focus.scrollIntoViewIfNeeded();}
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),name+' horizontal overflow');
   await page.screenshot({path:path.join(output,name+'-'+width+'.png'),animations:'disabled'});
 }
 const editor=page.locator('.v2-notice-editor');
 async function more(name){await button(page,'更多办理').click();await page.getByText(name,{exact:true}).last().click();}
 try{
   if(['all','notice'].includes(suite)){
     for(const kind of kinds){
       await page.goto(workbench(kind));
       const plan=page.locator('[data-list=planned] .desk-record').first(),start=button(plan,'开始');
       await start.waitFor();
       await plan.scrollIntoViewIfNeeded();
       const box=await plan.evaluate(el=>{
         const r=el.getBoundingClientRect(),a=el.querySelector('.primary-action').getBoundingClientRect(),t=el.querySelector('h3').getBoundingClientRect();
         return {height:r.height,offset:a.top-r.top,titleGap:Math.abs(a.top-t.top),actionHeight:a.height,spill:el.scrollWidth-el.clientWidth};
       });
       assert(box.titleGap<4 && box.offset<18 && box.spill<=1 && box.actionHeight>=(width===393?44:32),JSON.stringify(box));
       metrics.push({kind,width,...box});await snap(kind+'-plan',plan);
       await start.click();await editor.locator('form').waitFor();
       await button(editor,'关闭').click();await editor.waitFor({state:'hidden'});
       if(width===393)await page.getByRole('group',{name:'当前列表'}).getByRole('button',{name:/进行中/}).click();
       await button(page.locator('[data-list=ongoing]'),'更新').first().click();
       const progress=editor.locator('[data-notice-field=progress] textarea'),text='现场已复核设备反馈，备用通道运行正常。';
       await progress.fill(text);
       if(kind==='maintenance'){
         await button(editor,'关闭').click();await button(page,'保留副本并离开').click();await editor.waitFor({state:'hidden'});
         await button(page.locator('[data-list=ongoing]'),'更新').first().click();
         await progress.waitFor();assert.equal(await progress.inputValue(),text);await snap('restored-draft',progress);
       }
       await snap(kind+'-edit',progress);
       const send=button(editor,'预览并确认更新');
       assert(await send.isDisabled());await editor.getByRole('checkbox',{name:robotName,exact:true}).check();
       await send.click();
       const review=editor.getByLabel('发送前确认',{exact:true});
       await review.waitFor();assert((await review.innerText()).includes(text));
       const backBox=await button(editor,'返回修改').boundingBox();
       const submitBox=await button(editor,'确认发送更新').boundingBox();
       assert(backBox && submitBox && Math.abs(backBox.y-submitBox.y)<3,'confirmation actions share a row');
       await snap(kind+'-confirm',review.getByText('通告全文',{exact:true}));
       const before=writes.filter(v=>v.p==='/api/workbench-actions').length;
       await button(editor,'确认发送更新').click();
       await editor.getByRole('heading',{name:'通告办理完成'}).waitFor();
       const sent=writes.filter(v=>v.p==='/api/workbench-actions');assert.equal(sent.length,before+1);
       assert.equal(sent.at(-1).body.patch.progress,text);assert.equal(sent.at(-1).body.work_type,kind);
       assert.deepEqual(sent.at(-1).body.dingtalk_robot_ids,[robotId]);
       await snap(kind+'-receipt',editor.locator('.notice-receipt h2'));
       console.log('PASS',kind,width,box);
     }
     await button(editor,'返回列表').click();await editor.waitFor({state:'hidden'});
     await more('本机办理记录');const records=page.getByLabel('本机办理记录',{exact:true});
     await records.getByText('已完成',{exact:true}).first().waitFor();await button(records,'刷新状态').click();
     await snap('local-records',records.locator('li').first());await button(records,'关闭').click();
   }
   if(['all','history'].includes(suite)){
     await page.goto(workbench('maintenance'));await button(page,'查看历史通告').click();
     const history=page.locator('dialog.nh-workspace');await history.locator('.nh-row').first().click();
     const field=history.getByLabel(/现场进展/).first();await field.fill('已复核压差和备用泵切换记录');
     await button(history,'返回列表').click();await button(page,'继续编辑').click();
     assert.equal(await field.inputValue(),'已复核压差和备用泵切换记录');
     await snap('history-detail',field);await button(history,'保存修改').click();await history.getByText('更正已保存，未发送消息。').first().waitFor();
     assert.equal(historyValue,'已复核压差和备用泵切换记录');await button(history,'关闭').first().click();
     await more('近三天回退');await button(page,'回退上一步').first().click();
     await snap('undo-confirm',button(page,'确认回退'));await button(page,'确认回退').click();await page.getByText('回退已完成',{exact:true}).waitFor();
     await page.goto(workbench('change')+'&change_confirmation=proof-demo');
     const inbox=page.locator('.change-inbox');
     await inbox.getByRole('img',{name:'现场确认.png'}).click();await page.keyboard.press('Escape');
     await button(inbox,'确认变更').click();
     const confirm=button(page.locator('.n-dialog'),'确认变更');await snap('change-confirm',confirm);await confirm.click();
     await inbox.getByText('已确认',{exact:true}).first().waitFor();assert(confirmed);assert(await button(inbox,'确认变更').isDisabled());
     console.log('PASS history, exit guard, save, undo and administrator confirmation',width);
   }
   if(['all','procedure'].includes(suite)){
     await page.goto(entry+'__page=/repair-management&scope=A');
     await page.locator('.repair-desk-record .record-followup-button').first().click();
     const detail=page.locator('.v2-repair-detail');
     await detail.getByRole('button',{name:/历史跟进 ·/}).click();await page.locator('.v2-repair-history button').first().click();
     await button(detail,'编辑这条').click();
     const summary=detail.locator('.v2-repair-field-summary textarea:visible');
     await summary.fill('值班人员已复核反馈信号并记录，进度 65%。');
     const percent=detail.locator('.v2-repair-progress-input input:visible').first();await percent.fill('65');await percent.press('Tab');
     await snap('repair-followup',summary);
     await button(detail,'返回列表').click();await snap('repair-guard',button(page,'继续填写'));await button(page,'继续填写').click();
     assert((await summary.inputValue()).includes('65%'));
     await button(detail,'保存跟进修改').click();
     await page.waitForFunction(()=>document.querySelector('.v2-repair-detail')?.textContent.includes('已更新')||document.querySelector('.v2-repair-detail')?.textContent.includes('已保存'));
     assert(writes.some(v=>v.p.includes('/followups/')&&JSON.stringify(v.body).includes('反馈信号')));
     await page.goto(entry+'view=sops&scope=A&work_type=maintenance');
     await page.locator('.sop-desk__card').first().click();await button(page,'修改').click();
     await page.locator('.sop-step-selector button').nth(1).click();
     const step=page.getByLabel('步骤2内容',{exact:true});
     const updatedStep=(await step.inputValue()).endsWith('双人复核。')?'连续观察三遍，记录反馈信号。':'连续观察三遍，完成双人复核。';
     await step.fill(updatedStep);
     await snap('sop-step',step);
     await button(page,'取消修改').click();await button(page,'继续编辑').click();
     assert((await step.inputValue()).includes('三遍'));await button(page,'保存SOP').click();await page.getByText('SOP已保存',{exact:true}).waitFor();
     assert(writes.some(v=>v.p.includes('/polling-sops/')&&v.body.steps?.[1].content.includes('三遍')));
     console.log('PASS repair and SOP edits, guards and saves',width);
   }
   assert.deepEqual(errors,[]);
 }catch(e){console.error(width,(await page.locator('body').innerText()).slice(-1600));await page.screenshot({path:path.join(output,'failure-'+suite+'-'+width+'.png')});throw e;}
 finally{await context.close();}
}
try{for(const width of [1440,393])await check(width);if(metrics.length)await writeFile(path.join(output,'plan-measurements.json'),JSON.stringify(metrics,null,2));}finally{await browser.close();}

