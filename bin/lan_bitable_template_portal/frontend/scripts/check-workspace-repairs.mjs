import assert from 'node:assert/strict';
import { readFile, mkdir } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { parse, compileScript } from '@vue/compiler-sfc';
import { adaptRepairController, controllerBindings } from './workspace-repair-adapter.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/workspace-repairs');
const build=process.argv.includes('--isolated')?path.resolve(root,'../../../output/repair-v2-review'):path.resolve(root,'dist');
await mkdir(output,{recursive:true});
for(const [kind,legacy,view] of [['management','RepairManagementPage','RepairWorkspace'],['followup','RepairFollowupPanel','RepairFollowups']]){
  const original=readFileSync(path.join(root,'src/components',legacy+'.vue'),'utf8');
  const adapted=adaptRepairController(original,kind,controllerBindings(readFileSync(path.join(root,'src/workspace-v2',view+'.vue'),'utf8')));
  compileScript(parse(adapted).descriptor,{id:kind});
  if(kind==='followup'){assert.doesNotMatch(adapted,/2021-03-31T00:00/);assert.match(adapted,/if\(props\.readOnly\|\|saving\.value\)return/);assert.ok(adapted.indexOf('const v2ReadError')<adapted.lastIndexOf('watch('));}
  else{assert.match(adapted,/registerNavigationGuard/);assert.match(adapted,/recordDetailRequestVersion\+\+;recordDetailAbortController\?\.abort/);assert.match(adapted,/if\(!props\.canDelete\)return/);assert.doesNotMatch(adapted,/<Teleport/);}
}
const manifest=JSON.parse(await readFile(path.join(build,'assets/workspace-v2/entry.json'),'utf8'));
await readFile(path.join(build,'assets/workspace-v2',manifest.entry));
const server=createServer(async(req,res)=>{
  try{
    const url=new URL(req.url,'http://localhost');
    const target=path.resolve(build,'.'+decodeURIComponent(url.pathname));
    if(!target.startsWith(build+path.sep))throw new Error('outside static root');
    let data;try{data=await readFile(target);}catch{
      if(url.pathname==='/assets/vnet-logo.png')data=await readFile(path.join(root,'dist/assets/vnet-logo.png'));else throw new Error('missing static asset');
    }
    res.writeHead(200,{'content-type':({'.html':'text/html','.js':'text/javascript','.json':'application/json','.css':'text/css','.png':'image/png','.svg':'image/svg+xml'})[path.extname(target)]||'application/octet-stream','cache-control':'no-store'});res.end(data);
  }catch{res.writeHead(404);res.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
const entry='/assets/workspace-v2/index.html?__page=/repair-management&scope=A';
const field=(field_name,field_type=1,extra={})=>({field_name,field_type,editable:true,...extra});
const projectFields=[field('维修名称'),field('故障发生时间',5),field('故障维修原因'),field('所属专业',3,{options:['电气','暖通']}),field('所属数据中心/楼栋-使用',3,{options:['A楼']}),field('维修结束时间（2026）-L',5),field('当前维修进度',2),field('备注')];
const followupFields=[field('维修进展描述'),field('维修进度',2),field('设备名称'),field('设备编号'),field('设备品牌',3,{options:['测试品牌']}),field('设备型号',3,{options:['型号A']}),field('设备生产日期',5),field('设备使用年限',2),field('设备容量KW/AH'),field('维修方',3,{options:['我方','厂家']}),field('跟进项（如有）'),field('后续整改措施（如有）')];
const project=(id=1,extra={})=>({record_id:'project-'+id,title:`A-${id} 配电设备通信异常检修`,workflow:'维修中',building:'A楼',building_codes:['A'],specialty:'电气',progress_percent:35,followup_count:1,fault_time:'2026-10-14 09:10',summary_only:true,is_completed:false,...extra});
const historic={record_id:'followup-history',title:'完成现场检查，待更换模块',progress:.35,created_time:'2026-10-14 09:30',ledger_device_ids:['device-1'],ledger_devices:[{record_id:'device-1','设备编号':'A-101','设备名称':'配电设备','机楼':'A楼'}],raw_fields:{'维修进展描述':'完成现场检查，待更换模块','维修进度':.35,'设备名称':'配电设备'},display_fields:{'维修进展描述':'完成现场检查，待更换模块','维修进度':'35%','设备名称':'配电设备'}};
const fixture={canDelete:false,locked:false,conflict:false,modules:['repair_management','repair','event'],delayDetail:0,failRecords:false,failStats:false,delaySave:0,failSave:false,followups:[historic],writes:[],reads:[],errors:[],scopes:[{value:'A',label:'A楼'}]};
let ctx;
async function newPage(width=1440){
  if(ctx)await ctx.close();
  ctx=await browser.newContext({viewport:{width,height:950},reducedMotion:'reduce'});
  await ctx.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    fixture.reads.push({path:url.pathname,method:request.method(),params:Object.fromEntries(url.searchParams)});
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'isolated-person-A',name:'隔离验收人员',role:'admin',level:'L1',category:'engineer',can_delete_business:fixture.canDelete,modules:fixture.modules},scope_options:fixture.scopes});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-repairs'}});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(url.pathname==='/api/repair-management/changes')return ok({changes:[],cursor:0});
    if(url.pathname==='/api/repair-management/status')return fixture.failStats?route.fulfill({status:503,json:{ok:false,error:'隔离测试：状态暂不可用'}}):ok({stats:{total:30,without_followup:2,in_progress:28,completed_total:5},records:[],total:30});
    if(url.pathname==='/api/repair-management/records'&&request.method()==='GET'){
      if(fixture.failRecords)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：列表读取失败'}});
      const completed=url.searchParams.get('state')==='completed';
      return ok({fields:projectFields,records:completed?[project(90,{workflow:'维修完成',progress_percent:100,is_completed:true,read_only:true})]:Array.from({length:30},(_,i)=>project(i+1)),total:completed?1:30,offset:0});
    }
    if(/^\/api\/repair-management\/records\/[^/]+$/.test(url.pathname)&&request.method()==='GET'){
      if(fixture.delayDetail)await new Promise(resolve=>setTimeout(resolve,fixture.delayDetail));
      const id=url.pathname.split('/').pop();return ok({record:project(Number(id.split('-')[1]),{state_locked:fixture.locked,summary_only:false,record_version:'v1',source_event_id:'event-1',source_repair_ids:['notice-1'],display_fields:{'维修名称':'配电设备通信异常检修','故障发生时间':'2026-10-14 09:10','故障维修原因':'通信模块异常','所属专业':'电气','所属数据中心/楼栋-使用':'A楼','维修结束时间（2026）-L':'2026-10-20 18:30','当前维修进度':'35%'},raw_fields:{}})});
    }
    if(url.pathname==='/api/repair-management/followups'&&request.method()==='GET')return ok({fields:followupFields,records:fixture.followups,total:fixture.followups.length,shared_fields:{},brand_model_options:{},device_brand_model_options:{}});
    if(url.pathname==='/api/repair-management/ledger-candidates'){
      const p=Number(url.searchParams.get('page')||1);return ok({records:[{record_id:'device-'+p,'设备编号':'A-10'+p,'设备名称':'配电设备','机楼':'A楼','系统名称':'配电','大设备类型':'配电柜','品牌':'测试品牌','型号':'型号A'}],total:51,page:p,options:{'机楼':['A楼'],'系统名称':['配电'],'大设备类型':['配电柜'],'设备名称':['配电设备']},cache:{ready:true,refreshing:false},can_force_refresh:false});
    }
    if(url.pathname==='/api/repair-management/followups'&&request.method()==='POST'){
      fixture.writes.push({path:url.pathname,body:request.postDataJSON()});
      if(fixture.delaySave)await new Promise(resolve=>setTimeout(resolve,fixture.delaySave));
      if(fixture.failSave)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：保存响应待核验'}});
      return ok({record_id:'followup-new',fields:request.postDataJSON().fields,record_version:'v2'});
    }
    if(/^\/api\/repair-management\/(records|followups)\//.test(url.pathname)&&['PUT','DELETE'].includes(request.method())){fixture.writes.push({path:url.pathname,body:request.method()==='DELETE'?null:request.postDataJSON()});if(fixture.conflict)return route.fulfill({status:409,json:{ok:false,error:'记录已由其他人修改，请核对最新版。'}});return ok({record_id:url.pathname.split('/').pop(),fields:request.method()==='DELETE'?{}:request.postDataJSON().fields,record_version:'v2',deleted:true});}
    if(url.pathname.startsWith('/api/repair-management/operations/'))return ok({status:'processing',retryable:false});
    if(url.pathname==='/api/repair-management/sync-status')return ok({status:'completed'});
    if(url.pathname==='/api/repair-management/health')return ok({pending_count:0,failed_count:0});
    if(request.method()!=='GET'){fixture.writes.push({path:url.pathname,body:request.postDataJSON()});return ok({});}
    return ok({records:[],items:[],fields:[],people:[],stats:{},warnings:[],total:0});
  });
  const page=await ctx.newPage();page.on('pageerror',error=>fixture.errors.push(error.message));
  await page.goto(origin+entry);await page.locator('.record-row').first().waitFor();
  return page;
}
const detail=page=>page.locator('.v2-repair-detail');
async function openFirst(page){await page.locator('.record-row').first().click();await detail(page).getByRole('heading',{name:'本次跟进',exact:true}).waitFor();}
async function noOverflow(page){assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),true,'no page horizontal overflow');}
try{
  let page=await newPage();
  assert.ok(fixture.reads.some(item=>item.path==='/api/repair-management/records'&&item.params.state==='active'));
  assert.ok(await page.locator('.record-row').count()<30,'project list remains virtualized');
  await openFirst(page);await noOverflow(page);
  assert.equal(await detail(page).getByRole('button',{name:'检修通告',exact:true}).count(),1,'notice access independent of delete permission');
  assert.equal(await detail(page).getByRole('button',{name:'删除'}).count(),0);
  await page.screenshot({path:path.join(output,'pc-followup.png'),fullPage:true});
  await detail(page).getByRole('textbox',{name:'维修进展描述',exact:true}).fill('已检查模块，安排更换。');
  await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();
  await detail(page).getByText('请选择至少一台台账设备。',{exact:true}).waitFor();assert.equal(fixture.writes.length,0,'missing equipment never writes');
  await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();
  const picker=page.getByRole('dialog').filter({has:page.getByRole('heading',{name:'选择台账设备',exact:true})});
  await picker.getByRole('checkbox').first().check();await picker.getByRole('button',{name:'下一页',exact:true}).click();await picker.getByText('A-102',{exact:true}).waitFor();await picker.getByRole('checkbox').first().check();await picker.getByRole('button',{name:'确认',exact:true}).click();
  assert.equal(await detail(page).locator('.v2-repair-device-tags .n-tag').count(),2,'selection retained across equipment pages');
  await page.getByRole('link',{name:/灯塔/}).click();await page.getByText('填写尚未保存',{exact:true}).waitFor();await page.getByRole('button',{name:'继续填写',exact:true}).click();assert.equal(await detail(page).count(),1,'navigation protects draft');
  fixture.delaySave=800;await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();assert.equal(await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).isDisabled(),true,'cannot close during write');await detail(page).getByText('维修跟进记录已新增。',{exact:false}).waitFor();
  const write=fixture.writes.find(item=>item.path==='/api/repair-management/followups');assert.ok(write);assert.deepEqual(write.body.ledger_device_ids,['device-1','device-2']);assert.equal(write.body.fields['维修进展描述'],'已检查模块，安排更换。');assert.equal(write.body.fields['设备生产日期'],undefined);assert.equal(write.body.fields['设备使用年限'],undefined);assert.ok(write.body.operation_id);
  assert.equal(await detail(page).getByRole('button',{name:'编辑这条',exact:true}).count(),1,'saved followup returns to read mode');
  fixture.delaySave=0;
  await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).click();fixture.delayDetail=700;await page.locator('.record-row').nth(1).click();await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).click();await page.waitForTimeout(900);assert.equal(await detail(page).count(),0,'late detail must not reopen');fixture.delayDetail=0;
  const refreshBefore=fixture.reads.filter(item=>item.path==='/api/repair-refresh').length;await page.getByRole('button',{name:'刷新显示',exact:true}).click();await page.waitForTimeout(250);assert.equal(fixture.reads.filter(item=>item.path==='/api/repair-refresh').length,refreshBefore,'display refresh does not trigger remote sync');
  fixture.failRecords=true;await page.getByRole('button',{name:'刷新显示',exact:true}).click();await page.getByText(/暂显示上次读取的/).waitFor();fixture.failRecords=false;
  fixture.canDelete=true;page=await newPage(1366);await openFirst(page);await detail(page).getByText('维修单信息',{exact:true}).click();await detail(page).getByRole('button',{name:'维修单更多操作'}).click();await page.getByText('删除维修项目',{exact:true}).waitFor();await detail(page).getByRole('button',{name:'维修单更多操作'}).click();await page.screenshot({path:path.join(output,'pc-project.png'),fullPage:true});
  if(!await detail(page).count()){await openFirst(page);await detail(page).getByText('维修单信息',{exact:true}).click();}
  assert.match(await detail(page).getByRole('textbox',{name:'维修结束时间（2026）-L',exact:true}).inputValue(),/2026-10-20 18:30/);
  await detail(page).getByRole('textbox',{name:'备注',exact:true}).fill('核对现场进度');await detail(page).getByRole('button',{name:'保存修改',exact:true}).click();await detail(page).getByText('维修项目已保存。',{exact:true}).waitFor();
  const update=fixture.writes.find(item=>item.path==='/api/repair-management/records/project-1');assert.ok(update);assert.equal(update.body.expected_version,'v1');assert.deepEqual(update.body.fields,{'备注':'核对现场进度'},'untouched end time is not cleared');
  fixture.conflict=true;await detail(page).getByRole('textbox',{name:'备注',exact:true}).fill('当前填写应保留');await detail(page).getByRole('button',{name:'保存修改',exact:true}).click();await detail(page).locator('.n-alert').filter({hasText:'记录已由其他人修改，请核对最新版。'}).waitFor();assert.equal(await detail(page).getByRole('textbox',{name:'备注',exact:true}).inputValue(),'当前填写应保留');fixture.conflict=false;
  fixture.canDelete=false;fixture.failStats=true;page=await newPage(390);await page.getByText('数量暂不可用',{exact:false}).waitFor();assert.match(await page.locator('.v2-repair-state-row .n-radio-button').filter({hasText:'未完成'}).innerText(),/—/,'failed count not zero');fixture.failStats=false;await noOverflow(page);assert.equal(await page.locator('.v2-repair-row-state').first().isVisible(),true,'mobile keeps status');assert.equal(await page.locator('.v2-repair-row-progress').first().isVisible(),true,'mobile keeps progress');await page.screenshot({path:path.join(output,'mobile-list.png'),fullPage:true});await openFirst(page);await noOverflow(page);assert.equal(await page.locator('.v2-repair-list').isVisible(),false);await page.screenshot({path:path.join(output,'mobile-followup.png'),fullPage:true});
  page=await newPage(360);await openFirst(page);await noOverflow(page);
  fixture.locked=true;page=await newPage();await page.locator('.record-row').first().click();await detail(page).getByText('跟进状态尚未核验，当前只读。',{exact:true}).waitFor();assert.equal(await detail(page).getByRole('button',{name:'绑定已有',exact:true}).count(),0);assert.equal(await detail(page).getByRole('button',{name:/保存/}).count(),0);fixture.locked=false;
  fixture.failSave=true;page=await newPage();await openFirst(page);await detail(page).getByRole('textbox',{name:'维修进展描述',exact:true}).fill('等待核验原任务');await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();await page.getByRole('dialog').getByRole('checkbox').first().check();await page.getByRole('dialog').getByRole('button',{name:'确认',exact:true}).click();await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();await detail(page).getByRole('button',{name:'核验原任务',exact:true}).waitFor();assert.equal(await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).isDisabled(),true,'uncertain write cannot be resubmitted as new');fixture.failSave=false;
  fixture.modules=['repair_management'];page=await newPage();await openFirst(page);assert.equal(await detail(page).getByRole('button',{name:'检修通告',exact:true}).count(),0,'module revocation removes notice access');
  assert.deepEqual(fixture.errors,[],'no browser runtime errors');
  console.log('Repair V2: isolated PC/mobile, permissions, draft guard, late requests, equipment pagination, original API payload and refresh checks passed.');
}catch(error){
  console.error('Repair browser errors:',fixture.errors);console.error('Recent fixture requests:',fixture.reads.slice(-5));
  const last=ctx?.pages().at(-1);if(last){console.error((await last.locator('body').innerText()).slice(0,700));await last.screenshot({path:path.join(output,'failed-flow.png'),fullPage:true});}
  if(last&&await last.locator('.v2-repair-detail').count())console.error('Detail:',(await last.locator('.v2-repair-detail').innerText()).slice(0,1700));
  throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
