import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {build} from 'esbuild';
import {runInNewContext} from 'node:vm';
import {chromium} from 'playwright';
import {preview} from 'vite';
import {adaptNoticeSopControls} from './workspace-notice-adapter.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const bundled=await build({entryPoints:[path.join(root,'src/workspace-v2/noticeExperience.ts')],bundle:true,write:false,format:'cjs'});
const module={exports:{}};runInNewContext(bundled.outputFiles[0].text,{module,exports:module.exports,Set,Date});
const {noticeInScope,noticeBuildings,recommendationFields}=module.exports;
assert(noticeInScope({building_codes:['A','B']},'A'));assert(!noticeInScope({building_codes:['B']},'A'));
assert(noticeInScope({building:'ABCDE楼'},'CAMPUS'));assert(!noticeInScope({building:'H楼'},'CAMPUS'));
assert.equal(noticeBuildings({building:'南通110站'})[0],'110');
const mockForm={values:{work_type:'maintenance',content:'手填',reason:'',progress:'',execution_party:''},fields:['content','reason','progress','execution_party'].map(key=>({key,readonly:false,options:key==='execution_party'?[{value:'自维'}]:[],type:key==='execution_party'?'select-one':'textarea'}))};
const candidate={suggested_values:{content:'不要覆盖',reason:'可采用',progress:'不要复用',execution_party:'未知执行方'}};
assert.equal(recommendationFields(mockForm,candidate).map(f=>f.key).join(','),'reason');
assert.equal(recommendationFields(mockForm,candidate,new Set(['reason'])).length,0);
const original=await readFile(path.join(root,'src/components/LighthouseNoticeSop.vue'),'utf8'),adapted=adaptNoticeSopControls(original);
assert(!/<select\b|<VnetSelect\b/.test(adapted));assert.equal((adapted.match(/<NoticeSopSelect\b/g)||[]).length,9);
assert.throws(()=>adaptNoticeSopControls('changed'),/contract changed/);
assert(adapted.includes('function onPollingFrom')&&adapted.includes('setCustomValidity'),'business selection and validation remain shared');

const output=path.resolve(root,'../../../output/playwright/workspace-notices');await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});let page;const errors=[],writes=[],uploads=[];
let jobPhase='queued',prefillDelay=0,useSop=false,photoRequired=true;
const escape=v=>String(v).replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;');
const form=(ongoing=false)=>`<form id="lite-notice-form" data-action="${ongoing?'update':'start'}">
${Object.entries({scope:'E',work_type:'maintenance',notice_type:'维保通告',source_record_id:'plan-E',target_record_id:ongoing?'target-E':'',active_item_id:ongoing?'active-E':'',site_photo_count:0}).map(([k,v])=>`<input type="hidden" name="${k}" value="${v}">`).join('')}
<input type="checkbox" name="building_codes" checked value="E">
${Object.entries({title:'EA118机房E楼BA系统资料维护',content:ongoing?'原有内容':'',reason:'本楼现场原因',impact:'',progress:'现场检查已完成',start_time:'2026-10-14T09:00',end_time:'2026-10-14T18:30'}).map(([k,v])=>`<label><span>${({title:'名称',content:'内容',reason:'原因',impact:'影响',progress:'进度',start_time:'开始时间',end_time:'结束时间'})[k]}</span>${k.endsWith('time')?`<input type="datetime-local" name="${k}" value="${v}" required>`:k==='title'?`<input name="${k}" value="${escape(v)}" required>`:`<textarea name="${k}" required>${v}</textarea>`}</label>`).join('')}
${photoRequired?'<div data-site-photo-panel data-existing-images="[]"></div>':''}${useSop?'<div data-polling-work-order-select></div>':''}
${(ongoing?['update','end']:['start']).map(v=>`<button name="submit_action" value="${v}"></button>`).join('')}</form>`;
const ok=(route,data)=>route.fulfill({json:{ok:true,data}});
try{
 const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const req=route.request(),u=new URL(req.url());if(u.origin!==origin)return route.abort();if(!u.pathname.startsWith('/api/'))return route.continue();
  if(req.method()!=='GET')writes.push(u.pathname);
  if(u.pathname==='/api/auth/status')return ok(route,{logged_in:true,user:{open_id:'experience-fixture',name:'隔离测试',role:'admin',level:'L1',modules:['maintenance','change']},scope_options:[{value:'E',label:'E楼'},{value:'A',label:'A楼'}]});
  if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'experience-fixture'}});
  if(u.pathname==='/api/workbench')return ok(route,{records:[{record_id:'plan-E',work_type:'maintenance',title:u.searchParams.get('month')+'计划',status:'未开始',building:'E楼'}],ongoing:[{active_item_id:'active-E',work_type:'maintenance',title:'E楼进行中',status:'开始',building:'E楼'}],records_pagination:{total:1},ongoing_pagination:{total:1}});
  if(u.pathname==='/api/workbench/lite-detail')return ok(route,{fragments:{detail:form(u.searchParams.has('active_item_id'))}});
  if(u.pathname==='/api/workbench/draft')return ok(route,{draft:null});
  if(u.pathname==='/api/workbench/planned-notice-prefill'){
   if(prefillDelay)await new Promise(r=>setTimeout(r,prefillDelay));
   return ok(route,{selected:{source_record_id:'plan-E'},draft:{title:'EA118机房E楼BA系统资料维护'},recommendations:[{work_type:'maintenance',source_building:'A',source_title:'EA118机房A楼BA系统资料维护',source_updated_at:'2026-10-13 10:00',suggested_values:{content:'E楼BA资料核验',impact:'对IT业务无影响',reason:'A楼原因不可覆盖',progress:'旧进度不可采用',start_time:'旧日期'},needs_site_confirmation:[]}]});
  }
  if(u.pathname==='/api/polling-sops')return ok(route,{items:[{sop_id:'sop-E',name:'E楼标准工单',version:2,steps:[],mode:'normal'}]});
  if(u.pathname==='/api/signatures/people')return ok(route,{people:[{record_id:'operator-E',name:'操作员'},{record_id:'reviewer-E',name:'审核员'}]});
  if(u.pathname==='/api/notice-attachments'){
   const name=u.searchParams.get('file_name');uploads.push(name);
   if(name==='失败.png'&&uploads.filter(v=>v===name).length===1)return route.fulfill({status:400,json:{ok:false,error:'模拟第二张失败'}});
   return ok(route,{upload_id:'upload-'+name,file_name:name});
  }
  if(u.pathname==='/api/workbench-actions'){
   const payload=req.postDataJSON();assert.equal(payload.source_record_id,'plan-E');assert.equal(payload.patch.reason,'本楼现场原因');assert.equal(payload.patch.content,'E楼BA资料核验');assert.equal(payload.patch.site_photos.length,2);
   return ok(route,{job_id:'job-experience',phase:'queued'});
  }
  if(u.pathname==='/api/jobs/job-experience')return ok(route,{job_id:'job-experience',phase:jobPhase});
  if(u.pathname==='/api/change-confirmations')return ok(route,{can_confirm:true,items:[{target_record_id:'change-A',building_codes:['A'],building:'A楼',title:'A楼不可混入',state:'missing_screenshot'},{target_record_id:'change-E',building_codes:['E'],building:'E楼',title:'E楼待确认',state:'missing_screenshot'}]});
  if(req.method()!=='GET')throw new Error('Unexpected isolated write: '+u.pathname);
  return ok(route,{items:[],records:[],settings:{},available:false});
 });
 page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));await page.addInitScript(()=>sessionStorage.clear());
 const goto=async(kind='maintenance')=>{await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=E&month=10月&work_type='+kind);await page.locator('.v2-notice-entry').first().waitFor();};
 const pane=prefix=>page.locator('.v2-notice-pane-tabs').getByRole('button',{name:new RegExp('^'+prefix)}).click();
 const open=async()=>{await page.locator('[data-list=planned]').getByRole('button',{name:'开始',exact:true}).click();await page.getByRole('textbox',{name:'名称',exact:true}).waitFor();await pane('推荐填写');};
 await goto();
 await page.getByLabel('计划月份',{exact:true}).click();await page.locator('.n-base-select-option').getByText('11月',{exact:true}).click();
 assert.match(await page.locator('.v2-notice-heading').innerText(),/10月计划/);assert.match(await page.locator('[data-list=planned]').innerText(),/10月计划/);
 await page.getByRole('button',{name:'筛选',exact:true}).click();await page.locator('[data-list=planned]').getByText('11月计划',{exact:true}).waitFor();
 await goto();await open();await page.getByRole('button',{name:'采用这组空白项',exact:true}).waitFor();
 await page.screenshot({path:path.join(output,'cross-building-recommendations.png'),fullPage:true});
 assert.equal(await page.locator('.v2-notice-field-grid [data-notice-field]').first().getAttribute('data-notice-field'),'content','missing fields appear before prefilled title');
 await page.getByRole('button',{name:'采用这组空白项',exact:true}).click();
 assert.equal(await page.getByRole('textbox',{name:'内容',exact:true}).inputValue(),'E楼BA资料核验');
 assert.equal(await page.locator('.v2-notice-field-grid [data-notice-field]').first().getAttribute('data-notice-field'),'content','filling a missing field does not move it away from the cursor');
 await page.getByRole('button',{name:'展开其它字段',exact:true}).click();assert.equal(await page.getByRole('textbox',{name:'原因',exact:true}).inputValue(),'本楼现场原因');
 assert.equal(await page.getByRole('textbox',{name:'进度',exact:true}).inputValue(),'现场检查已完成');
 await pane('照片');await page.getByLabel('添加现场照片',{exact:true}).setInputFiles([{name:'成功.png',mimeType:'image/png',buffer:Buffer.from('fixture1')},{name:'失败.png',mimeType:'image/png',buffer:Buffer.from('fixture2')}]);
 await page.getByRole('button',{name:'重试此张',exact:true}).waitFor();await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('alert').filter({hasText:'还有图片未上传成功'}).waitFor();
 await page.getByRole('button',{name:'重试此张',exact:true}).click();await page.locator('.v2-photo-queue').waitFor({state:'detached'});
 assert.deepEqual(uploads,['成功.png','失败.png','失败.png']);assert.equal(await page.locator('[data-notice-upload-kind=site] figure').count(),2);
 await page.screenshot({path:path.join(output,'missing-recommendation-desktop.png'),fullPage:true});
 await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByLabel('发送前确认',{exact:true}).waitFor();assert(!writes.includes('/api/workbench-actions'));
 await page.getByRole('button',{name:'确认发送开始',exact:true}).click();await page.getByRole('status',{name:'后台处理中',exact:true}).waitFor();
 assert.equal(await page.getByRole('textbox',{name:'内容',exact:true}).count(),0,'accepted jobs show results, not a locked form');
 jobPhase='success';await page.getByRole('button',{name:'查询进度',exact:true}).click();await page.getByRole('status',{name:'通告办理完成',exact:true}).waitFor();
 await page.getByRole('button',{name:'返回列表',exact:true}).click();
 await goto('change');await page.getByRole('button',{name:'更多办理',exact:true}).click();await page.getByText('变更确认',{exact:true}).click();await page.getByText('E楼待确认',{exact:true}).waitFor();assert.equal(await page.getByText('A楼不可混入',{exact:true}).count(),0);
 await page.getByRole('button',{name:'关闭',exact:true}).click();
 await page.evaluate(()=>sessionStorage.clear());prefillDelay=900;await goto();await open();await pane('通告内容');await page.getByRole('textbox',{name:'内容',exact:true}).fill('用户先填的内容');
 await pane('推荐填写');await page.getByRole('button',{name:'采用这组空白项',exact:true}).click();assert.equal(await page.getByRole('textbox',{name:'内容',exact:true}).inputValue(),'用户先填的内容');
 await page.getByRole('textbox',{name:'内容',exact:true}).fill('');assert.equal(await page.locator('.v2-notice-memory').getByRole('button',{name:'采用',exact:true}).count(),0,'clearing a touched field does not restore suggestions');
 await page.getByRole('textbox',{name:'名称',exact:true}).fill('另一条通告');await pane('推荐填写');await page.getByText('名称或楼栋已修改，原计划的推荐已停用。',{exact:true}).waitFor();
 await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
 await page.evaluate(()=>sessionStorage.clear());useSop=true;prefillDelay=0;await goto();await open();await page.getByRole('button',{name:'采用这组空白项',exact:true}).click();
 await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('alert').filter({hasText:'请完成本次工单'}).waitFor();
 await page.getByRole('button',{name:'选择工单及人员',exact:true}).click();
 await page.locator('.lhs-sop .n-select').click();await page.locator('.n-base-select-option').getByText('E楼标准工单',{exact:true}).click();
 await page.waitForFunction(()=>[...document.querySelectorAll('.n-base-select-menu')].every(el=>!el.getClientRects().length));
 await page.locator('.lhs-people .n-select').first().click();await page.locator('.n-base-select-menu:visible').getByText('操作员',{exact:true}).click();
 await page.waitForFunction(()=>[...document.querySelectorAll('.n-base-select-menu')].every(el=>!el.getClientRects().length));
 await page.locator('.lhs-people .n-select').last().click();await page.locator('.n-base-select-menu:visible').getByText('审核员',{exact:true}).click();
 await page.waitForFunction(()=>[...document.querySelectorAll('.n-base-select-menu')].every(el=>!el.getClientRects().length));
 await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByLabel('发送前确认',{exact:true}).waitFor();
 await page.getByRole('button',{name:'返回修改',exact:true}).click();await pane('通告内容');await page.setViewportSize({width:390,height:844});await page.screenshot({path:path.join(output,'missing-recommendation-mobile.png'),fullPage:true});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 await page.setViewportSize({width:1440,height:1000});useSop=false;await goto();await page.locator('[data-list=ongoing]').getByRole('button',{name:'结束',exact:true}).click();await page.getByRole('textbox',{name:'影响',exact:true}).fill('对IT业务无影响');
 await page.getByRole('button',{name:'预览并确认结束',exact:true}).click();await page.getByRole('button',{name:'补充现场照片',exact:true}).click();
 const section=await page.locator('[data-notice-section=photos]').boundingBox();assert(section.y>0&&section.y<900,'missing-photo link locates the photo section');
 assert.equal(await page.getByLabel('发送前确认',{exact:true}).count(),0,'missing photo cannot submit');
 assert.deepEqual(errors,[]);console.log('PASS: recommendation/touched-field guards, missing-first form, applied filters, single-image retry, task results, building isolation, Naive SOP controls. All writes mocked.');
}catch(e){if(page){await page.screenshot({path:path.join(output,'experience-error.png'),fullPage:true});console.error(errors,await page.locator('.n-modal').allTextContents());}throw e;}finally{await browser.close();await server.close();}
