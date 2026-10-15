import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';

assert(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR,'Use an isolated build; never send test business requests to a live service.');
const output=path.resolve('../../../output/playwright/module-widgets');await mkdir(output,{recursive:true});
const server=await preview({configFile:'vite.workspace-v2.config.ts',logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
const errors=[],writes=[],reads=[];
const user={open_id:'module-widget-test',name:'隔离测试人员',role:'building',level:'L4',category:'engineer',modules:['maintenance','change','event','water','links']};
const plan=Array.from({length:20},(_,i)=>({record_id:'plan-'+i,title:'A楼计划维护'+(i+1),work_type:'maintenance',building:'A楼',source_progress:'未开始',cycle:'月度',start_time:'2026-10-15 09:30',end_time:'2026-10-15 18:30'}));
const active=Array.from({length:20},(_,i)=>({active_item_id:'active-'+i,record_id:'rec-'+i,target_record_id:'rec-'+i,title:'A楼进行中维护'+(i+1),work_type:'maintenance',scope:'A',status:'开始'}));
let draft=null;
await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.resolve('public/assets/vnet-logo.png'),contentType:'image/png'});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(req.method()!=='GET')writes.push({path:url.pathname,method:req.method(),body:req.postDataJSON()});else reads.push(url.pathname+url.search);
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-module-widgets'}});
  if(url.pathname==='/api/workbench')return ok({records:plan,ongoing:active,source_snapshot_ready:true,records_pagination:{total:20},ongoing_pagination:{total:20}});
  if(url.pathname==='/api/workbench/lite-detail'){
    const ongoing=url.searchParams.has('active_item_id');
    return ok({fragments:{detail:`<form id="lite-notice-form"><input type="hidden" name="scope" value="A"><input type="checkbox" name="building_codes" value="A" checked><input type="hidden" name="work_type" value="maintenance"><input type="hidden" name="notice_type" value="维保通告"><input type="hidden" name="source_record_id" value="plan-0">${ongoing?'<input type="hidden" name="target_record_id" value="rec-0"><input type="hidden" name="active_item_id" value="active-0">':''}<label><span>名称</span><input required name="title" value="A楼隔离测试维护"></label><label><span>内容</span><textarea required name="content">维护内容</textarea></label><label><span>本次进度</span><textarea name="progress">进行中</textarea></label>${(ongoing?['update','end']:['start']).map(action=>`<button name="submit_action" value="${action}"></button>`).join('')}</form>`}});
  }
  if(url.pathname==='/api/workbench/draft'){if(req.method()==='PUT'){const data=req.postDataJSON();draft={...data,version:(draft?.version||0)+1};}return ok({draft});}
  if(url.pathname==='/api/workbench-actions')return ok({job_id:'isolated-job',phase:'queued'});
  if(url.pathname==='/api/jobs/isolated-job')return ok({job_id:'isolated-job',phase:'success'});
  if(url.pathname==='/api/capacity/water/bootstrap')return ok({scope:url.searchParams.get('scope')||'A',options:{meters:[],frequencies:['日'],shifts:['白','夜']},permissions:{can_create:true},summary:{},snapshot:{exists:true}});
  if(url.pathname==='/api/capacity/water/records')return ok({records:Array.from({length:50},(_,i)=>({record_id:'water-'+i,meter:'测试水表',frequency:'日',shift:'白',statistic_date:'2026-10-15',meter_value:100+i,computed_usage:1,photo_count:1})),total:50,snapshot:{exists:true}});
  if(url.pathname==='/api/events/monthly')return ok({records:[],snapshot_exists:true});
  if(url.pathname==='/api/link-directory')return ok({can_edit:true,items:Array.from({length:20},(_,i)=>({id:'link-'+i,name:'业务链接'+(i+1),category:'业务',purpose:'测试说明',url:'https://example.com/item/'+i,sort_order:i+1}))});
  return ok({items:[],records:[],total:0,groups:[],scopes:[],settings:{},permissions:{},stats:{},people:[],fields:[],summary:{},snapshot:{exists:true}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
const widget=kind=>page.locator(`[data-widget-kind="module"]:has(>.v2-widget[aria-label="${kind==='notices'?'通告办理':'水耗管理'}"])`);
async function resizeModule(width,height,title='通告办理'){await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'编辑'+title,exact:true}).click();const editor=page.locator('.v2-widget-editor');await editor.locator('.n-form-item').filter({hasText:'组件宽度'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText(width,{exact:true}).click();await editor.locator('.n-form-item').filter({hasText:'组件高度'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText(height,{exact:true}).click();await editor.getByRole('button',{name:'应用设置',exact:true}).click();await page.getByRole('button',{name:'保存布局',exact:true}).click();}
try{
  await page.goto(entry);await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  for(const name of ['通告办理','水耗管理']){await page.getByRole('button',{name:'添加组件',exact:true}).click();await page.locator('.v2-library-item').filter({hasText:name+' · 完整模块'}).click();await page.locator('.v2-widget-editor').getByRole('button',{name:'应用设置',exact:true}).click();}
  await page.getByRole('button',{name:'保存布局',exact:true}).click();await widget('notices').scrollIntoViewIfNeeded();
  await widget('notices').locator('.v2-notice-entry').first().waitFor();
  assert.equal(await widget('notices').locator('.v2-page-canvas').count(),0,'module embedding does not nest layout editors');
  assert.equal(await widget('notices').locator('iframe').count(),0,'full native module, not a legacy iframe');
  assert.equal(await page.locator('#workspace-page-filters .v2-notice-filters').count(),0,'module filters stay local');
  const originalURL=page.url(),before=reads.filter(url=>url.startsWith('/api/workbench?')).length;
  await resizeModule('紧凑 · 1/3','紧凑');await widget('notices').scrollIntoViewIfNeeded();
  await page.waitForFunction(()=>document.querySelector('[data-module-widget="notices"]').closest('.v2-responsive-body').dataset.capacity==='compact');
  const small=await widget('notices').locator('.v2-notice-entry:visible').count();assert(small>=3&&small<40);
  await page.screenshot({path:path.join(output,'compact.png')});
  await resizeModule('整行','完整工作区');await widget('notices').scrollIntoViewIfNeeded();
  await page.waitForFunction(small=>document.querySelectorAll('[data-module-widget="notices"] .v2-notice-entry:not([data-v2-capacity-hidden])').length>=small,small);
  await page.waitForTimeout(400);const large=await widget('notices').locator('.v2-notice-entry:visible').count();assert(large>small,'larger module displays more records');
  assert.equal(reads.filter(url=>url.startsWith('/api/workbench?')).length,before,'resizing does not reload the business controller');
  const kindSelect=widget('notices').locator('.v2-notice-kind-tabs');
  if(await kindSelect.count())await kindSelect.getByRole('tab',{name:/维保/}).click();
  await widget('water').scrollIntoViewIfNeeded();await widget('water').locator('.v2-module-filters').waitFor();
  assert.equal(page.url(),originalURL,'embedded module views never rewrite the dashboard route');
  await widget('notices').scrollIntoViewIfNeeded();
  for(const action of ['start','update','end']){
    draft=null;
    const label={start:'开始',update:'更新',end:'结束'}[action],lane=action==='start'?'planned':'ongoing';
    await widget('notices').locator(`[data-list="${lane}"]`).getByRole('button',{name:label,exact:true}).first().click();
    const editor=page.locator('.v2-notice-editor');await editor.locator('.v2-notice-field-grid').waitFor();
    assert.equal(await editor.locator('.n-radio-button--checked').innerText(),label);
    const contents=editor.locator('textarea[aria-label="内容"]');if(await contents.count())await contents.fill('完整模块内填写的维护内容');
    assert.equal(writes.filter(item=>item.path==='/api/workbench-actions').length,['start','update','end'].indexOf(action),'editing is not confirmation');
    await editor.getByRole('button',{name:'预览并确认'+label,exact:true}).click();await editor.getByRole('button',{name:'确认发送'+label,exact:true}).click();
    await editor.getByText('已按原业务流程完成，列表正在同步。',{exact:true}).waitFor();
    const submitted=writes.filter(item=>item.path==='/api/workbench-actions').at(-1).body;assert.equal(submitted.action,action);assert.equal(submitted.scope,'A');assert.equal(submitted.work_type,'maintenance');assert(submitted.operation_id);
    await editor.getByRole('button',{name:'返回列表',exact:true}).click();await editor.waitFor({state:'hidden'});
    assert.equal(page.url(),originalURL);
  }
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'添加组件',exact:true}).click();assert.equal(await page.locator('.v2-library-item').filter({hasText:'通告办理 · 完整模块'}).count(),0,'no duplicate full modules');await page.locator('.v2-widget-library .n-base-close').click();await page.locator('.v2-dashboard-actions').getByRole('button',{name:'取消',exact:true}).click();
  await page.reload();await widget('notices').scrollIntoViewIfNeeded();await widget('notices').locator('.v2-notice-entry').first().waitFor();assert.equal(await widget('water').count(),1,'full modules survive reload');
  await widget('water').scrollIntoViewIfNeeded();await widget('water').getByRole('button',{name:'新增录入',exact:true}).click();await widget('water').getByRole('textbox',{name:'本次水表读数',exact:true}).fill('123.4');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'移除水耗管理',exact:true}).click();await page.getByText('放弃未保存的水耗填写？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续填写',exact:true}).click();assert.equal(await widget('water').count(),1,'removing a dirty module requires the original form guard');await page.locator('.v2-dashboard-actions').getByRole('button',{name:'取消',exact:true}).click();assert.equal(await widget('water').getByRole('textbox',{name:'本次水表读数',exact:true}).inputValue(),'123.4','layout cancel preserves the mounted business draft');
  await widget('water').getByRole('button',{name:'关闭水耗编辑',exact:true}).click();await page.getByRole('button',{name:'放弃并离开',exact:true}).click();
  await resizeModule('紧凑 · 1/3','紧凑','水耗管理');await widget('water').scrollIntoViewIfNeeded();await page.waitForTimeout(250);const waterSmall={rows:await widget('water').locator('tbody tr:visible').count(),cols:await widget('water').locator('thead th:visible').count()};assert.equal(waterSmall.cols,4);await resizeModule('整行','完整工作区','水耗管理');await widget('water').scrollIntoViewIfNeeded();await page.waitForTimeout(250);assert(await widget('water').locator('tbody tr:visible').count()>waterSmall.rows);assert(await widget('water').locator('thead th:visible').count()>waterSmall.cols,'native business tables expose additional fields as they grow');
  for(const width of [1920,1366,390]){await page.setViewportSize({width,height:1000});await widget('notices').scrollIntoViewIfNeeded();await page.waitForTimeout(200);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'no page overflow at '+width);await page.screenshot({path:path.join(output,'modules-'+width+'.png')});}
  await page.setViewportSize({width:1024,height:900});await page.goto(entry+'?__page=/link-directory');await page.locator('.link-table tbody tr').first().waitFor();
  async function sizeTable(width,height){await page.getByRole('button',{name:'自定义多维表导航布局',exact:true}).click();await page.getByRole('button',{name:'设置链接目录',exact:true}).click();const settings=page.locator('.v2-page-layout-library');await settings.locator('.n-form-item').filter({hasText:'宽度'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText(width,{exact:true}).click();await settings.locator('.n-form-item').filter({hasText:'高度'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText(height,{exact:true}).click();await settings.getByRole('button',{name:'完成设置',exact:true}).click();await page.getByRole('button',{name:'保存布局',exact:true}).click();}
  await sizeTable('6 / 12 列','256 像素');await page.waitForTimeout(300);const tableSmall={rows:await page.locator('.link-table tbody tr:visible').count(),cols:await page.locator('.link-table th:visible').count()};
  assert.equal(tableSmall.rows,3);assert.equal(tableSmall.cols,4);assert.equal(await page.locator('.link-table tbody tr:visible').first().getByRole('button',{name:'编辑链接',exact:true}).count(),1,'compact tables retain row actions');
  const nextRow=await page.locator('.link-table tbody tr').nth(3).getAttribute('data-link-id');await page.getByRole('button',{name:'下一组',exact:true}).click();assert.equal(await page.locator('.link-table tbody tr:visible').first().getAttribute('data-link-id'),nextRow);await page.getByRole('button',{name:'回到首组',exact:true}).click();
  await sizeTable('整行','896 像素');await page.waitForTimeout(300);assert(await page.locator('.link-table tbody tr:visible').count()>tableSmall.rows);assert(await page.locator('.link-table th:visible').count()>tableSmall.cols);
  await page.reload();await page.locator('.link-table tbody tr').first().waitFor();assert.equal(await page.locator('[data-page-layout="links"] [data-widget-id="block_links"]').getAttribute('gs-h'),'14','page layout is restored');await page.screenshot({path:path.join(output,'responsive-table.png')});
  await page.goto(entry);
  user.modules=['event'];await page.reload();await page.getByRole('button',{name:'自定义工作台',exact:true}).waitFor();assert.equal(await widget('notices').count()+await widget('water').count(),0,'revoked modules are removed');
  assert.deepEqual(errors,[]);console.log(`PASS: add full modules, native forms, responsive records (${small} -> ${large}), no resize refetch, original start/update/end API, local filters/routes, duplicate prevention, reload, permissions, desktop/mobile.`);
}catch(error){await page.screenshot({path:path.join(output,'failure.png')});console.error(errors,await page.locator('.v2-notice-editor').allInnerTexts());throw error;}
finally{await browser.close();await server.close();}
