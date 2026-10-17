import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {chromium} from 'playwright';
import {preview} from 'vite';
import {adaptNoticeParserClient} from './workspace-notice-adapter.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const source=await readFile(path.join(root,'src/api/client.ts'),'utf8');
assert(!source.includes('/workbench-lite/parse'),'legacy transport is unchanged');
assert(adaptNoticeParserClient(source).includes('await response.text()'));
assert.throws(()=>adaptNoticeParserClient('changed'),/contract changed/);
const output=path.resolve(root,'../../../output/playwright/workspace-notices');await mkdir(output,{recursive:true});
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:4173}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),entry='/assets/workspace-v2/index.html?__page=/workbench-lite&scope=A&work_type=';
const errors=[],writes=[];let page,user='fixture-manager',role='admin',target='rec-original',sops=[],changeState='missing_screenshot',canConfirm=true,sopVersion=0,groupState='',orphan=false;
const escape=v=>String(v).replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;');
const form=(kind='maintenance',ongoing=false,parsed=false)=>`<form id="lite-notice-form" data-detail-mode="${ongoing?'ongoing':'planned'}" data-action="${parsed?'start':ongoing?'update':'start'}">
${Object.entries({scope:'A',work_type:kind,notice_type:kind==='change'?'变更通告':kind==='repair'?'设备检修':'维保通告',source_record_id:parsed?'':'plan-fixture',manual:parsed?'1':'',manual_id:parsed?'manual:lite:A:maintenance:fixture':'',active_item_id:ongoing?'active-fixture':'',target_record_id:ongoing?target:'',site_photo_count:1}).map(([k,v])=>`<input type="hidden" name="${k}" value="${escape(v)}">`).join('')}
<input name="building_codes" type="checkbox" checked value="A"><label><span>名称</span><input name="title" value="${parsed?'原文识别通告':'A楼测试通告'}" required></label><label><span>内容</span><textarea name="content" required>原有内容</textarea></label><label><span>进度</span><textarea name="progress" required>已检查</textarea></label><input type="hidden" name="start_time" value="2026-10-14 09:00"><input type="hidden" name="end_time" value="2026-10-14 18:30">
${parsed?'<section data-manual-source-binding><input type="hidden" name="manual_binding_choice" value=""></section>':''}${kind==='change'?'<div data-site-photo-panel data-existing-images="[]"></div>':''}${groupState?'<div data-polling-group-id="group-fixture"></div>':''}${(ongoing?['update','end']:['start']).map(a=>`<button name="submit_action" value="${a}">${a}</button>`).join('')}</form>`;
const ok=(route,data)=>route.fulfill({json:{ok:true,data}});
try{
 const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url()),kind=url.searchParams.get('work_type')||'maintenance',body=()=>req.postDataJSON();
  if(url.origin!==origin)return route.abort();
  if(url.pathname==='/workbench-lite/parse'){writes.push({path:url.pathname,text:req.postData()});return route.fulfill({contentType:'text/html',body:form('maintenance',false,true)+'<script>window.badLegacyScript=true</script>'});}
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(req.method()!=='GET')writes.push({path:url.pathname,method:req.method(),body:req.headers()['content-type']?.includes('application/json')?body():null});
  if(url.pathname==='/api/auth/status')return ok(route,{logged_in:true,user:{open_id:user,name:'隔离管理员',role,level:role==='admin'?'L1':'L4',can_delete_business:role==='admin',modules:['maintenance','change','repair','polling','adjust','power','engineer_mop','accounts']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-tools'}});
  if(url.pathname==='/api/workbench')return ok(route,{records:[{record_id:'plan-fixture',title:'A楼计划',work_type:kind,source_progress:orphan?'进行中':'未开始'}],ongoing:[{active_item_id:'active-fixture',title:'A楼进行中',work_type:kind,status:'开始',scope:'A'}],records_pagination:{total:1},ongoing_pagination:{total:1}});
  if(url.pathname==='/api/workbench/lite-detail')return ok(route,{fragments:{detail:form(kind,url.searchParams.has('active_item_id'))}});
  if(url.pathname==='/api/workbench/draft')return ok(route,{draft:null});
  if(url.pathname==='/api/workbench/source-options')return ok(route,{items:[{source_record_id:'plan-parsed-fixture',title:'原文识别通告',building:'A楼',maintenance_cycle:'半年',progress:'未开始'}]});
  if(url.pathname==='/api/workbench/polling-delay-status')return ok(route,{state:groupState,due_at_ts:0,enabled:false,missing_completion:false});
  if(url.pathname==='/api/polling-sops'&&req.method()==='GET')return ok(route,{items:sops.filter(s=>s.scope===url.searchParams.get('scope')&&s.work_type===kind)});
  if(/^\/api\/polling-sops(?:\/sop-fixture)?$/.test(url.pathname)&&['POST','PUT'].includes(req.method())){const p=body();assert.equal(p.scope,'A');assert.equal(p.expected_version,sopVersion);sopVersion++;sops=[{...p,sop_id:'sop-fixture',version:sopVersion,attachments:sops[0]?.attachments||[]}];return ok(route,sops[0]);}
  if(url.pathname==='/api/polling-sops/sop-fixture/attachments'&&req.method()==='POST'){sopVersion++;sops[0]={...sops[0],version:sopVersion,attachments:[{attachment_id:'file-fixture',file_name:'SOP.txt'}]};return ok(route,sops[0]);}
  if(url.pathname==='/api/polling-sops/sop-fixture'&&req.method()==='DELETE'){assert.equal(Number(url.searchParams.get('expected_version')),sopVersion);sops=[];return ok(route,{deleted:true});}
  if(url.pathname==='/api/notice-target-candidates')return ok(route,{candidates:[{target_record_id:'rec-matched',title:'核验后的目标通告 · EA118_C01机房B楼B-120-ES-505AU采集器故障检修',building:'A楼',status:'开始'}]});
  if(url.pathname==='/api/notice-identity/bind'){assert.equal(body().target_record_id,'rec-matched');target='rec-matched';return ok(route,{active_item_id:'active-fixture',target_record_id:target});}
  if(url.pathname==='/api/workbench/repair-event-candidates')return ok(route,{records:[{repair_management_record_id:'project-fixture',event_record_id:'event-fixture',title:'关联检修事件',building:'A楼'}],total:1});
  if(url.pathname==='/api/workbench/repair-event-bind'){assert.equal(body().candidate_project_record_id,'project-fixture');assert.equal(body().event_record_id,'event-fixture');return ok(route,{active_item_id:'active-fixture'});}
  if(url.pathname==='/api/change-confirmations')return ok(route,{can_confirm:canConfirm,items:[{target_record_id:'change-fixture',title:'待确认变更',building:'A楼',state:changeState,local_screenshot_items:[]}]});
  if(url.pathname==='/api/notice-attachments')return ok(route,{upload_id:'upload-fixture',local_image_id:'12345678901234567890123456789012',file_name:'确认.png'});
  if(url.pathname==='/api/change-confirmations/change-fixture/screenshot'){assert.equal(body().upload_id,'upload-fixture');changeState='awaiting_confirmation';return ok(route,{});}
  if(url.pathname==='/api/change-confirmations/rec-matched/screenshot'){assert.equal(body().upload_id,'upload-fixture');return ok(route,{});}
  if(url.pathname==='/api/change-confirmations/change-fixture/confirm'){changeState='confirmed';return ok(route,{});}
  if(url.pathname==='/api/jobs/batch'){assert.equal(url.searchParams.get('ids'),'job-fixture');return ok(route,{items:[{job_id:'job-fixture',phase:'failed',error_retryable:true,error:'模拟待重试',scope:'A',work_type:'maintenance'}],missing:[],denied:[]});}
  if(url.pathname==='/api/jobs/job-fixture/retry')return ok(route,{job_id:'job-fixture',phase:'queued'});
  if(req.method()!=='GET')throw new Error('Unmocked write: '+url.pathname);
  return ok(route,{items:[],records:[],available:false});
 });
 page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
 const goto=async(kind='maintenance')=>{await page.goto(origin+entry+kind,{waitUntil:'domcontentloaded',timeout:60000});await page.locator('.v2-notice-entry').first().waitFor();console.log('notice tools:',kind);};
 const more=async(label)=>{await page.getByRole('button',{name:'更多办理',exact:true}).click();await page.getByText(label,{exact:true}).click();};
 await goto();await page.getByRole('button',{name:'粘贴原文',exact:true}).click();
 const original=page.getByRole('textbox',{name:'通告原文',exact:true});
 const paste=async text=>original.evaluate((input,text)=>{const data=new DataTransfer();data.setData('text/plain',text);input.dispatchEvent(new ClipboardEvent('paste',{clipboardData:data,bubbles:true,cancelable:true}));},text);
 await paste('\r\n【维保通告】状态：开始\r\n\t \r\n【名称】原文识别通告\r\n\r\n');
 assert.equal(await original.inputValue(),'【维保通告】状态：开始\n【名称】原文识别通告','blank lines disappear immediately on paste');
 await original.fill('前文\n替换\n后文');await original.evaluate(input=>{input.focus();input.setSelectionRange(3,5);});await paste('\n  新内容\n\t\n');
 assert.equal(await original.inputValue(),'前文\n  新内容\n后文','selected text is replaced without removing indentation or joining content lines');
 await original.fill('');await paste('x'.repeat(30010));assert.equal((await original.inputValue()).length,30000,'paste respects the existing length limit');
 await original.fill('\n【维保通告】状态：开始\n\t\n【名称】原文识别通告\n\n');
 await page.getByRole('button',{name:'解析并填写',exact:true}).click();await page.getByRole('textbox',{name:'名称',exact:true}).waitFor();assert.equal(await page.getByRole('textbox',{name:'名称',exact:true}).inputValue(),'原文识别通告');assert(!await page.evaluate(()=>window.badLegacyScript));
 assert.match(writes.find(w=>w.path==='/workbench-lite/parse').text.replace(/\r\n/g,'\n'),/name="paste_text"\n\n【维保通告】状态：开始\n【名称】原文识别通告\n--/,'parse normalizes typed blank lines too');
 const planBinding=page.locator('[data-notice-section="binding"]');
 await planBinding.getByRole('alert').filter({hasText:'独立通告 · 未关联计划'}).waitFor();
 await planBinding.getByText('独立通告',{exact:true}).click();assert.equal(await planBinding.getByRole('alert').count(),0);assert.equal(await planBinding.getByLabel('源表计划事项',{exact:true}).count(),0);assert.equal(await planBinding.getByRole('button',{name:'确认关联',exact:true}).count(),0);
 await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByLabel('发送前确认',{exact:true}).waitFor();assert.equal(await page.locator('.n-modal:visible').count(),1,'preview stays in one modal');assert.match(await page.getByLabel('待发送通告全文',{exact:true}).innerText(),/原文识别通告/);assert(!writes.some(w=>w.path==='/api/workbench-actions'));
 await page.getByRole('button',{name:'返回修改',exact:true}).click();assert.equal(await page.getByRole('textbox',{name:'内容',exact:true}).inputValue(),'原有内容');
 await planBinding.getByText('关联计划',{exact:true}).click();await planBinding.getByRole('button',{name:'应用推荐计划',exact:true}).waitFor();
 assert(await planBinding.getByRole('button',{name:'确认关联',exact:true}).isDisabled());await planBinding.getByRole('button',{name:'应用推荐计划',exact:true}).click();
 await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('alert').filter({hasText:'请选择并确认关联计划'}).waitFor();
 assert.equal(await page.getByLabel('发送前确认',{exact:true}).count(),0,'Apply alone cannot bind or send');
 await planBinding.getByRole('button',{name:'确认关联',exact:true}).click();await planBinding.getByText('已关联计划',{exact:true}).waitFor();
 assert(!writes.some(w=>w.path==='/api/notice-identity/bind'||w.path==='/api/workbench-actions'),'new manual association is only a local choice until the original send flow');
 await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
 console.log('notice tools: paste cleanup, explicit binding, Apply/confirm and single preview passed');
 await more('SOP与工单模板');assert.equal(await page.getByRole('button',{name:'修改',exact:true}).count(),0,'no modify action before selecting a template');await page.getByRole('button',{name:'新增',exact:true}).click();await page.getByLabel('SOP所属楼栋',{exact:true}).click();await page.locator('.n-base-select-option').getByText('A楼',{exact:true}).click();await page.getByRole('textbox',{name:'SOP名称',exact:true}).fill('新SOP');await page.getByRole('textbox',{name:'步骤1内容',exact:true}).fill('确认状态');await page.getByRole('button',{name:'添加循环（总遍数含首次）',exact:true}).click();await page.getByLabel('总执行遍数（含首次）',{exact:true}).fill('4');await page.getByRole('button',{name:'保存SOP',exact:true}).click();await page.getByText('SOP已保存',{exact:true}).waitFor();assert.equal(writes.find(w=>w.path==='/api/polling-sops'&&w.method==='POST').body.steps[0].repeat_rules[0].count,4);
 assert.equal(await page.getByRole('button',{name:'同步',exact:true}).count(),0);assert(await page.getByRole('textbox',{name:'SOP名称',exact:true}).isDisabled());
 assert.equal(await page.locator('[data-widget-id="block_editor"] .v2-widget-head').getByRole('button',{name:'修改',exact:true}).count(),1,'modify action belongs to the SOP steps card header');
 assert.equal(await page.getByRole('button',{name:'返回SOP列表',exact:true}).count(),0);
 await page.getByRole('button',{name:'修改',exact:true}).click();await page.getByLabel('添加SOP附件',{exact:true}).setInputFiles({name:'SOP.txt',mimeType:'text/plain',buffer:Buffer.from('test')});await page.getByRole('button',{name:'SOP.txt',exact:true}).waitFor();
 const uploadPath='/api/polling-sops/sop-fixture/attachments',uploads=()=>writes.filter(w=>w.path===uploadPath).length;
 const beforeUpload=uploads();
 await page.getByLabel('添加SOP附件',{exact:true}).evaluate(input=>{
  const file=new File(['x'],'oversize.bin');Object.defineProperty(file,'size',{value:200*1024*1024+1});
  const transfer=new DataTransfer();transfer.items.add(file);input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));
 });
 await page.getByRole('alert').filter({hasText:'请选择非空且不超过200MB的附件'}).waitFor();
 assert.equal(uploads(),beforeUpload,'over-limit selection must not send a request');
 const uploaded=page.waitForResponse(r=>new URL(r.url()).pathname===uploadPath&&r.request().method()==='POST');
 await page.getByLabel('添加SOP附件',{exact:true}).setInputFiles({name:'SOP.txt',mimeType:'application/octet-stream',buffer:Buffer.alloc(21*1024*1024,120)});
 assert.equal((await uploaded).status(),200);
 await page.waitForFunction(()=>!document.querySelector('input[aria-label="添加SOP附件"]')?.disabled);
 assert.equal(uploads(),beforeUpload+1,'an attachment above the old 20MB limit must upload');
 assert.equal(await page.getByRole('alert').filter({hasText:'请选择非空且不超过200MB的附件'}).count(),0);
 console.log('notice tools: SOP 21MB upload and 200MB limit passed');
 await page.getByLabel('适用类型',{exact:true}).click();await page.locator('.n-base-select-option').getByText('设备轮巡',{exact:true}).click();await page.getByRole('button',{name:'保存SOP',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.v2-sop-manager')&&!document.body.textContent.includes('确认转换SOP类型'));
 assert.equal(sops[0].work_type,'polling');assert.equal(sops[0].attachments.length,1);await page.screenshot({path:path.join(output,'sop-manager.png'),fullPage:true});
 await page.getByRole('button',{name:'修改',exact:true}).click();await page.getByRole('button',{name:'删除SOP',exact:true}).click();await page.getByRole('button',{name:'确认删除',exact:true}).click();await page.locator('.v2-sop-content').waitFor({state:'detached'});assert.equal(sops.length,0);assert.equal(await page.getByRole('button',{name:'返回SOP列表',exact:true}).count(),0);await goto();
 console.log('notice tools: SOP CRUD passed');
 await page.locator('[data-list="ongoing"]').getByRole('button',{name:'更新',exact:true}).click();await page.getByRole('button',{name:'重新查找目标通告',exact:true}).click();await page.getByLabel('关联候选',{exact:true}).click();await page.locator('.n-base-select-option').filter({hasText:'核验后的目标通告'}).click();assert(!writes.some(w=>w.path==='/api/notice-identity/bind'));await page.getByRole('button',{name:'核对并保存关联',exact:true}).click();await page.getByRole('button',{name:'确认关联并重新读取',exact:true}).click();await page.getByRole('textbox',{name:'名称',exact:true}).waitFor();assert.equal(target,'rec-matched');await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();
 await goto('repair');await page.locator('[data-list="ongoing"]').getByRole('button',{name:'更新',exact:true}).click();await page.getByRole('button',{name:'补充故障关联',exact:true}).click();await page.getByLabel('关联候选',{exact:true}).click();await page.locator('.n-base-select-option').filter({hasText:'关联检修事件'}).click();await page.getByRole('button',{name:'核对并保存关联',exact:true}).click();await page.getByRole('button',{name:'确认关联并重新读取',exact:true}).click();await page.getByRole('textbox',{name:'名称',exact:true}).waitFor();await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();
 await goto('change');await more('变更确认');assert(await page.getByRole('button',{name:'确认变更',exact:true}).isDisabled());await page.getByLabel('选择确认截图',{exact:true}).setInputFiles({name:'确认.png',mimeType:'image/png',buffer:Buffer.from('isolated')});await page.getByRole('button',{name:'上传截图',exact:true}).click();await page.getByText('待确认',{exact:true}).waitFor();await page.getByRole('button',{name:'确认变更',exact:true}).click();await page.locator('.n-dialog').getByRole('button',{name:'确认变更',exact:true}).click();await page.getByText('当前没有符合条件的变更',{exact:true}).waitFor();assert.equal(changeState,'confirmed');await page.getByRole('button',{name:'关闭',exact:true}).click();
 await page.evaluate(()=>sessionStorage.setItem('v2-notice-jobs:fixture-manager',JSON.stringify([{job_id:'job-fixture',title:'原任务',scope:'A',work_type:'maintenance',at:Date.now()}])));await more('本机办理记录');await page.getByText('原任务',{exact:true}).waitFor();await page.getByRole('button',{name:'重试原任务',exact:true}).click();assert(writes.some(w=>w.path==='/api/jobs/job-fixture/retry'));await page.getByRole('button',{name:'关闭',exact:true}).click();
 user='another-person';await goto();await more('本机办理记录');await page.getByText('本机暂无已确认的办理记录',{exact:true}).waitFor();assert.equal(await page.getByText('原任务',{exact:true}).count(),0);await page.getByRole('button',{name:'关闭',exact:true}).click();
 role='user';await goto('change');await page.getByRole('button',{name:'更多办理',exact:true}).click();assert.equal(await page.getByText('变更确认',{exact:true}).count(),0,'privileged confirmation entry stays restricted');await page.keyboard.press('Escape');await page.locator('[data-list="ongoing"]').getByRole('button',{name:'更新',exact:true}).click();await page.locator('[data-notice-field="progress"] textarea').fill('现场修改保持不变');
 await page.getByRole('button',{name:'重新查找目标通告',exact:true}).click();await page.getByRole('button',{name:'更换关联',exact:true}).click();
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:width<700?844:1000});
  const relations=page.getByRole('region',{name:'关联核验',exact:true});await relations.scrollIntoViewIfNeeded();
  const targetBox=await relations.getByLabel('关联候选',{exact:true}).boundingBox(),sourceBox=await page.getByLabel('源表计划事项',{exact:true}).boundingBox();
  assert(targetBox.width<=321&&sourceBox.width<=321,'relation search controls stay compact, including long selected titles');
  if(width>700){const identity=await relations.locator('.v2-notice-editor-context').boundingBox(),tools=await relations.locator('.v2-notice-tools').boundingBox();assert(Math.abs(identity.y+identity.height/2-tools.y-tools.height/2)<2,'type, building and target binding share one row');}
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await page.screenshot({path:path.join(output,'compact-relations-'+width+'.png')});
  const photos=page.locator('[data-notice-section=photos]'),ali=page.locator('[data-notice-section=ali]');await photos.scrollIntoViewIfNeeded();const a=await photos.boundingBox(),b=await ali.boundingBox();assert(Math.abs(a.y-b.y)<2&&b.x>a.x+a.width-1,'photo inputs share two columns');assert.equal(await page.locator('.v2-notice-edit-surface>[data-notice-section]').first().getAttribute('data-notice-section'),'relations');await page.screenshot({path:path.join(output,'relations-photos-'+width+'.png')});
 }await page.setViewportSize({width:1440,height:1000});
await page.getByLabel('添加阿里确认截图',{exact:true}).setInputFiles({name:'确认.png',mimeType:'image/png',buffer:Buffer.from('isolated')});await page.getByRole('button',{name:'仅上传确认截图',exact:true}).click();await page.getByText('确认截图已上传，未发送通告',{exact:true}).waitFor();assert.equal(await page.locator('[data-notice-field="progress"] textarea').inputValue(),'现场修改保持不变');assert(!writes.some(w=>w.path==='/api/workbench-actions'));await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
 groupState='in_progress';await goto();await page.locator('[data-list="ongoing"]').getByRole('button',{name:'结束',exact:true}).click();await page.getByText('工单执行中',{exact:true}).waitFor();await page.getByRole('button',{name:'预览并确认结束',exact:true}).click();await page.getByRole('alert').filter({hasText:'关联工单步骤尚未全部完成'}).waitFor();assert.equal(await page.getByLabel('发送前确认',{exact:true}).count(),0);groupState='upload_pending';await page.getByRole('button',{name:'刷新工单状态',exact:true}).click();await page.getByText('步骤已全部完成，工单表格正在上传',{exact:true}).waitFor();await page.getByRole('button',{name:'预览并确认结束',exact:true}).click();await page.getByLabel('发送前确认',{exact:true}).waitFor();await page.getByRole('button',{name:'返回修改',exact:true}).click();await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();groupState='';
 orphan=true;await goto();await page.locator('[data-list="planned"]').getByRole('button',{name:'核验',exact:true}).click();await page.getByRole('alert').filter({hasText:'计划显示进行中'}).waitFor();await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('alert').filter({hasText:'请先核验原目标通告'}).waitFor();assert.equal(await page.getByLabel('发送前确认',{exact:true}).count(),0);await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();orphan=false;
 for(const width of [390,1366]){await page.setViewportSize({width,height:width<700?844:900});await more('SOP与工单模板');await page.getByRole('button',{name:'新增',exact:true}).click();assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);await page.screenshot({path:path.join(output,'sop-'+width+'.png'),fullPage:true});assert.equal(await page.getByRole('button',{name:'返回SOP列表',exact:true}).count(),0);await page.getByRole('button',{name:'取消修改',exact:true}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();assert.match(page.url(),/view=sops/);await goto();}
 assert.deepEqual(errors,[]);console.log('PASS: notice parsing, single-modal review, SOP CRUD/version/attachments/repeat, target and event binding, change confirmation, isolated task recovery. All writes mocked.');
}catch(error){if(page){await page.screenshot({path:path.join(output,'tools-error.png'),fullPage:true});console.error(errors,await page.locator('.n-modal').allTextContents());}throw error;}finally{await browser.close();await server.close();}
