import assert from 'node:assert/strict';
import { readFile, mkdir } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { parse, compileScript } from '@vue/compiler-sfc';
import { adaptRepairController, controllerBindings } from './workspace-repair-adapter.mjs';
import { layoutApiFixture } from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,process.env.CLIPFLOW_REPAIR_SCREENSHOT_DIR||'../../../output/playwright/workspace-repairs');
const build=process.env.CLIPFLOW_REPAIR_PREVIEW_ROOT?path.resolve(process.env.CLIPFLOW_REPAIR_PREVIEW_ROOT):process.argv.includes('--isolated')?path.resolve(root,'../../../output/repair-v2-review'):path.resolve(root,'dist');
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
    if(url.pathname.startsWith('/api/')){
      const chunks=[];for await(const chunk of req)chunks.push(chunk);
      const body=Buffer.concat(chunks).toString();
      return await handleFixture({request:()=>({url:()=>origin+req.url,method:()=>req.method,postDataJSON:()=>body?JSON.parse(body):{}}),fulfill:({status=200,json,body='',headers={}})=>{res.writeHead(status,{'content-type':json?'application/json':'text/plain',...headers});res.end(json?JSON.stringify(json):body);},abort:()=>{res.writeHead(403);res.end();}},previewLayouts);
    }
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
const previewOnly=process.argv.includes('--preview');
const browser=previewOnly?null:await chromium.launch({headless:true});
const previewLayouts=layoutApiFixture();
const entry='/assets/workspace-v2/index.html?__page=/repair-management&scope=A';
const field=(field_name,field_type=1,extra={})=>({field_name,field_type,editable:true,...extra});
const projectFields=[field('维修名称'),field('故障发生时间',5),field('故障维修原因'),field('所属专业',3,{options:['电气','暖通']}),field('所属数据中心/楼栋-使用',3,{options:['A楼']}),field('维修结束时间（2026）-L',5),field('当前维修进度',2),field('备注')];
const followupFields=[field('维修进展描述'),field('维修进度',2),field('设备名称'),field('设备编号'),field('设备品牌',3,{options:['测试品牌']}),field('设备型号',3,{options:['型号A']}),field('设备生产日期',5),field('设备使用年限',2),field('设备容量KW/AH'),field('维修方',3,{options:['我方','厂家']}),field('跟进项（如有）'),field('后续整改措施（如有）')];
followupFields.push(field('随工人员（我方维修人员）',11));
const project=(id=1,extra={})=>({record_id:'project-'+id,title:`A-${id} 配电设备通信异常检修`,workflow:'维修中',building:'A楼',building_codes:['A'],specialty:'电气',progress_percent:35,followup_count:1,fault_time:'2026-10-14 09:10',summary_only:true,is_completed:false,...extra});
const historic={record_id:'followup-history',title:'完成现场检查，待更换模块',progress:.35,created_time:'2026-10-14 09:30',ledger_device_ids:['device-1'],ledger_devices:[{record_id:'device-1','设备编号':'A-101','设备名称':'配电设备','机楼':'A楼'}],raw_fields:{'维修进展描述':'完成现场检查，待更换模块','维修进度':.35,'设备名称':'配电设备'},display_fields:{'维修进展描述':'完成现场检查，待更换模块','维修进度':'35%','设备名称':'配电设备'}};
const fixture={canDelete:false,locked:false,conflict:false,unlinked:false,modules:['repair_management','repair','event'],delayDetail:0,statusDelay:0,largeFilter:false,delayRecords:0,failRecords:false,failStats:false,delaySave:0,failSave:false,followups:[historic],writes:[],reads:[],errors:[],scopes:[{value:'A',label:'A楼'}]};
let ctx;
async function handleFixture(route,layouts){
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(await layouts.handle(route,'isolated-person-A'))return;
    fixture.reads.push({path:url.pathname,method:request.method(),params:Object.fromEntries(url.searchParams)});
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'isolated-person-A',name:'隔离验收人员',role:'admin',level:'L1',category:'engineer',can_delete_business:fixture.canDelete,modules:fixture.modules},scope_options:fixture.scopes});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-repairs'}});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(url.pathname==='/api/repair-management/changes')return ok({changes:[],cursor:0});
    if(url.pathname==='/api/repair-management/status'){
      if(fixture.failStats)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：状态暂不可用'}});
      const state=url.searchParams.get('state'),limit=Number(url.searchParams.get('limit')||30),offset=Number(url.searchParams.get('offset')||0);
      if(state==='without_followup'&&fixture.statusDelay)await new Promise(resolve=>setTimeout(resolve,fixture.statusDelay));
      const total=state==='no_followup'?(fixture.noFollowupCount??(fixture.largeFilter?65:3)):state==='without_followup'?2:28;
      const records=limit===1?[]:Array.from({length:Math.max(0,Math.min(limit,total-offset))},(_,i)=>project(offset+i+1,{state,workflow:state==='without_followup'?'未开始':'维修中',followup_count:state==='in_progress'?1:0}));
      return ok({stats:{total:30,without_followup:2,in_progress:28,no_followup:fixture.noFollowupCount??(fixture.largeFilter?65:3),completed_total:5},records,total,offset,has_more:offset+records.length<total});
    }
    if(url.pathname==='/api/repair-management/records'&&request.method()==='GET'){
      if(fixture.delayRecords)await new Promise(resolve=>setTimeout(resolve,fixture.delayRecords));
      if(fixture.failRecords)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：列表读取失败'}});
      const completed=url.searchParams.get('state')==='completed';
      return ok({fields:projectFields,records:completed?[project(90,{workflow:'维修完成',progress_percent:100,is_completed:true,read_only:true})]:Array.from({length:30},(_,i)=>project(i+1)),total:completed?1:30,offset:0});
    }
    if(/^\/api\/repair-management\/records\/[^/]+$/.test(url.pathname)&&request.method()==='GET'){
      if(fixture.delayDetail)await new Promise(resolve=>setTimeout(resolve,fixture.delayDetail));
      const id=url.pathname.split('/').pop();return ok({record:project(Number(id.split('-')[1]),{state_locked:fixture.locked,summary_only:false,record_version:'v1',source_event_id:fixture.unlinked?'':'event-1',source_repair_ids:fixture.unlinked?[]:['notice-1'],display_fields:{'维修名称':'配电设备通信异常检修','故障发生时间':'2026-10-14 09:10','故障维修原因':'ATS通信模块异常','所属专业':'电气','所属数据中心/楼栋-使用':'A楼','维修结束时间（2026）-L':'2026-10-20 18:30','当前维修进度':'35%'},raw_fields:{}})});
    }
    if(url.pathname==='/api/repair-management/followups'&&request.method()==='GET')return ok({fields:followupFields,records:fixture.followups,total:fixture.followups.length,shared_fields:{},brand_model_options:{},device_brand_model_options:{}});
    if(url.pathname==='/api/repair-management/ledger-candidates'){
      const p=Number(url.searchParams.get('page')||1);return ok({records:[{record_id:'device-'+p,'设备编号':'A-10'+p,'设备名称':'配电设备','机楼':'A楼','系统名称':'配电','大设备类型':'配电柜','品牌':'测试品牌','型号':'型号A'}],total:51,page:p,options:{'机楼':['A楼'],'系统名称':['配电'],'大设备类型':['配电柜'],'设备名称':['配电设备']},cache:{ready:true,refreshing:false},can_force_refresh:false});
    }
    if(url.pathname==='/api/repair-management/followups'&&request.method()==='POST'){
      fixture.writes.push({path:url.pathname,body:request.postDataJSON()});
      if(fixture.delaySave)await new Promise(resolve=>setTimeout(resolve,fixture.delaySave));
      if(fixture.failSave)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：保存响应待核验'}});
      if(fixture.trackFilteredSave){fixture.noFollowupCount=0;fixture.followups=[{...historic,record_id:'followup-new',raw_fields:request.postDataJSON().fields}];}
      return ok({record_id:'followup-new',fields:request.postDataJSON().fields,record_version:'v2'});
    }
    if(/^\/api\/repair-management\/(records|followups)\//.test(url.pathname)&&['PUT','DELETE'].includes(request.method())){fixture.writes.push({path:url.pathname,body:request.method()==='DELETE'?null:request.postDataJSON()});if(fixture.conflict)return route.fulfill({status:409,json:{ok:false,error:'记录已由其他人修改，请核对最新版。'}});return ok({record_id:url.pathname.split('/').pop(),fields:request.method()==='DELETE'?{}:request.postDataJSON().fields,record_version:'v2',deleted:true});}
    if(url.pathname.startsWith('/api/repair-management/operations/'))return ok({status:'processing',retryable:false});
    if(url.pathname==='/api/repair-management/sync-status')return ok({status:'completed'});
    if(url.pathname==='/api/repair-management/health')return ok({pending_count:0,failed_count:0});
    if(url.pathname==='/api/repair-management/people')return ok({people:[{user_id:'person-mobile',name:'验收人员',employee_no:'00001',building:'A楼',selectable:true}],total:1});
    if(url.pathname==='/api/repair-management/event-candidates')return ok({records:[{record_id:'event-mobile',title:'A楼配电设备通信异常事件',alarm_desc:'通信模块异常',building:'A楼',specialty:'电气'}],total:1});
    if(url.pathname==='/api/repair-management/repair-candidates')return ok({records:[{record_id:'notice-mobile',title:'A楼配电设备通信异常检修通告',building:'A楼',specialty:'电气'}],total:1});
    if(url.pathname==='/api/repair-management/prefill'){
      const input=request.postDataJSON();
      fixture.reads.at(-1).body=input;
      return ok({fields:{'维修名称':'关联资料自动填入的维修项目','故障发生时间':'2026-10-15T09:10','故障维修原因':'通信模块异常','所属专业':'电气','所属数据中心/楼栋-使用':'A楼',...(input.source_repair_ids.length?{'维修结束时间（2026）-L':'2026-10-20T18:30'}:{})},warnings:[]});
    }
    if(request.method()!=='GET'){fixture.writes.push({path:url.pathname,body:request.postDataJSON()});return ok({});}
    return ok({records:[],items:[],fields:[],people:[],stats:{},warnings:[],total:0});
}
async function newPage(width=1440,height=950,night=false){
  if(ctx)await ctx.close();
  ctx=await browser.newContext({viewport:{width,height},reducedMotion:'reduce'});
  await ctx.addInitScript(()=>{
    const NativeEventSource=window.EventSource;
    window.EventSource=class extends EventTarget{
      constructor(url,options){super();if(!String(url).includes('/repair-management/stream'))return new NativeEventSource(url,options);window.__repairTestStream=this;queueMicrotask(()=>this.dispatchEvent(new Event('open')));}
      close(){if(window.__repairTestStream===this)window.__repairTestStream=null;}
    };
  });
  const layouts=layoutApiFixture();
  if(night)await ctx.addInitScript(()=>localStorage.setItem('clipflow:workspace-night-mode:isolated-person-A','dark'));
  await ctx.route('**/*',route=>handleFixture(route,layouts));
  const page=await ctx.newPage();page.on('pageerror',error=>fixture.errors.push(error.message));
  await page.goto(origin+entry);await page.locator('.record-row').first().waitFor();
  return page;
}
const detail=page=>page.locator('.v2-repair-detail');
async function openFirst(page,followups=true){
  await page.locator('.record-row').first().click();
  await detail(page).locator('.v2-repair-project-form:visible').waitFor();
  assert.equal(await detail(page).getAttribute('role'),'dialog','record opens above the list');
  assert.equal(await detail(page).getByRole('heading',{name:'本次跟进',exact:true}).count(),0,'opening a record starts with its editable repair sheet');
  if(followups){await detail(page).getByText(/^跟进记录 \d+$/).click();await detail(page).getByRole('heading',{name:'本次跟进',exact:true}).waitFor();}
}
async function noOverflow(page){assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),true,'no page horizontal overflow');}
async function fieldRect(page,label){return detail(page).locator('.n-form-item').filter({has:page.getByLabel(label,{exact:true})}).evaluate(el=>{const box=el.getBoundingClientRect();return {x:box.x,y:box.y,width:box.width};});}
async function checkRefreshPosition(page){
  const heading=detail(page).locator('.v2-repair-followup-heading');
  const refresh=heading.getByRole('button',{name:'刷新历史跟进',exact:true});
  const button=await refresh.boundingBox(),box=await heading.boundingBox();
  assert(button&&box&&Math.abs(button.x+button.width-box.x-box.width)<3,'followup refresh stays on the right of its heading');
  assert.equal(await detail(page).locator('.v2-repair-history-search button').count(),0,'search does not consume a separate action row');
}
async function checkFieldContrast(page){
  const editable=await detail(page).getByLabel('维修名称',{exact:true}).evaluate(el=>getComputedStyle(el.closest('.n-input')).backgroundColor);
  const readonly=await detail(page).getByLabel('故障原因',{exact:true}).evaluate(el=>getComputedStyle(el).backgroundColor);
  assert.notEqual(editable,readonly,'writable controls must differ visibly from source-owned values');
  assert.notEqual(editable,await detail(page).evaluate(el=>getComputedStyle(el).backgroundColor),'editable fields must stand out from the surrounding form');
  assert.equal(await detail(page).getByLabel('楼栋',{exact:true}).getAttribute('aria-readonly'),'true','source values are readable summaries');
  const notes=detail(page).getByLabel('备注',{exact:true});
  assert.equal(await notes.getAttribute('placeholder'),'补充需要说明的事项');
  assert.equal(await notes.inputValue(),'','guidance is never a submitted field value');
  assert.match(await detail(page).getByLabel('故障原因',{exact:true}).innerText(),/来自关联资料/,'source-owned values explain their origin');
  assert.match(await detail(page).getByLabel('故障原因',{exact:true}).innerText(),/^ATS通信模块异常/,'source summaries must preserve letters in equipment names');
  const colors=await notes.evaluate(el=>({ink:getComputedStyle(el,'::placeholder').color,bg:getComputedStyle(el.closest('.n-input')).backgroundColor}));
  const light=color=>color.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
  const ink=light(colors.ink),bg=light(colors.bg);
  assert((Math.max(ink,bg)+.05)/(Math.min(ink,bg)+.05)>=4.5,'placeholder remains readable in the selected theme');
}
async function checkWorkflowActions(page){
  const actions=detail(page).locator('.v2-repair-detail-footer');
  await actions.getByRole('button').first().evaluate(el=>el.scrollIntoView({block:'center',behavior:'instant'}));
  for(const button of await actions.locator('.n-button:not(.n-button--disabled):has(svg)').all()){
    const paint=await button.evaluate(el=>{
      const content=el.querySelector('.n-button__content'),icon=content.querySelector('svg'),style=getComputedStyle(el),text=getComputedStyle(content);
      const rgb=[0,0,0];let coverage=0;
      for(let node=el;node&&coverage<1;node=node.parentElement){
        const values=getComputedStyle(node).backgroundColor.match(/[\d.]+/g).map(Number),weight=(values[3]??1)*(1-coverage);
        for(let i=0;i<3;i++)rgb[i]+=values[i]*weight;
        coverage+=weight;
      }
      const box=el.getBoundingClientRect(),hit=document.elementFromPoint(box.x+box.width/2,box.y+box.height/2);
      return {button:style.color,text:text.color,icon:getComputedStyle(icon).color,font:parseFloat(text.fontSize),buttonFont:parseFloat(style.fontSize),background:'rgb('+rgb.join(',')+')',width:el.clientWidth,contentWidth:content.scrollWidth,clickable:el.contains(hit)};
    });
    assert.equal(paint.text,paint.button,'workflow labels inherit the button theme, not muted help text');
    assert.equal(paint.icon,paint.text,'workflow icon and label have the same contrast');
    assert.equal(paint.font,paint.buttonFont,'workflow labels keep the component font size');
    const luminance=color=>color.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
    const foreground=luminance(paint.text),background=luminance(paint.background);
    assert((Math.max(foreground,background)+.05)/(Math.min(foreground,background)+.05)>=4.5,'workflow action text needs 4.5:1 contrast: '+JSON.stringify(paint));
    assert(paint.contentWidth<=paint.width,'workflow labels fit inside the button');
    assert(paint.clickable,'workflow action must not be covered by fixed toolbars');
  }
}
if(previewOnly){console.log('Isolated repair preview: '+origin+entry);await new Promise(()=>{});}
try{
  for(const width of [1440,402,440]){
    const page=await newPage(width,900);
    const last=page.locator('.record-row').last();
    await last.scrollIntoViewIfNeeded();
    const scroll=await page.evaluate(()=>scrollY);
    await last.click();await detail(page).locator('.v2-repair-project-form:visible').waitFor();
    assert.equal(await page.evaluate(()=>scrollY),scroll,'opening the last row preserves list position');
    const box=await detail(page).boundingBox();
    assert(box.y>=0&&box.y+box.height<=900,'editor is inside the current viewport, not after the list');
    await detail(page).getByLabel('备注',{exact:true}).fill('保留填写内容');
    await detail(page).getByText(/^跟进记录 \d+$/).click();
    await detail(page).getByText('维修单信息',{exact:true}).click();
    assert.equal(await detail(page).getByLabel('备注',{exact:true}).inputValue(),'保留填写内容','tab changes preserve unsaved repair information');
    await detail(page).getByRole('button',{name:'返回列表',exact:true}).click();
    await page.getByRole('button',{name:'继续填写',exact:true}).click();
    assert.equal(await detail(page).getByLabel('备注',{exact:true}).inputValue(),'保留填写内容','cancel closing preserves the draft');
    await detail(page).getByRole('button',{name:'返回列表',exact:true}).click();
    await page.getByRole('button',{name:'放弃并继续',exact:true}).click();
    await detail(page).waitFor({state:'hidden'});
    assert.equal(await page.evaluate(()=>scrollY),scroll,'returning restores the same row without a page jump');
    assert.equal(await page.evaluate(()=>document.body.style.overflow),'','scroll locking is released');
    assert.equal(fixture.writes.length,0,'navigation has no business writes');
    await page.screenshot({path:path.join(output,`list-position-${width}.png`)});
  }
  let page=await newPage();
  assert.equal(await page.locator('.v2-repair-overview[aria-label="维修项目状态"]').count(),1,'one status filter group serves the full list');
  assert.equal(await page.locator('.v2-repair-workspace>.v2-repair-state-row').count(),0,'no duplicate state toolbar above the list');
  assert.ok(fixture.reads.some(item=>item.path==='/api/repair-management/records'&&item.params.state==='active'));
  assert.equal(await page.locator('.record-row').count(),30,'one bounded page uses the page scroll, not a nested virtual viewport');
  await openFirst(page,false);await noOverflow(page);await checkWorkflowActions(page);
  assert.equal(await detail(page).locator('.v2-repair-detail-tabs .n-radio-button').count(),2,'only sheet and followups are primary destinations');
  assert.equal(await detail(page).getByText('办理流程',{exact:true}).count(),0);
  assert.equal(await detail(page).getByText('完成核对',{exact:true}).count(),0);
  await page.screenshot({path:path.join(output,'repair-sheet-desktop.png')});
  await checkFieldContrast(page);
  const name=await fieldRect(page,'维修名称'),faultTime=await fieldRect(page,'故障发生时间'),cause=await fieldRect(page,'故障原因');
  const building=await fieldRect(page,'楼栋'),specialty=await fieldRect(page,'专业');
  assert(Math.abs(name.y-faultTime.y)<2&&faultTime.x>name.x,'name and fault time share one desktop row');
  assert(Math.abs(cause.y-building.y)<2&&Math.abs(cause.y-specialty.y)<2,'short project fields use three columns on desktop');
  await detail(page).getByText(/^跟进记录 \d+$/).click();
  assert.equal(await detail(page).getByLabel('维修进展描述',{exact:true}).getAttribute('placeholder'),'填写本次处理内容和处理结果');
  assert.equal(await detail(page).getByLabel('维修进展描述',{exact:true}).inputValue(),'');
  const descriptionBox=await fieldRect(page,'维修进展描述'),progressBox=await fieldRect(page,'维修进度');
  assert(Math.abs(descriptionBox.y-progressBox.y)<2&&progressBox.x>descriptionBox.x,'description and progress share a desktop row');
  const partyBox=await fieldRect(page,'维修方'),peopleBox=await detail(page).locator('.repair-people-picker').boundingBox();
  assert(Math.abs(partyBox.y-peopleBox.y)<3&&peopleBox.x>partyBox.x,'repair party and people picker share a desktop row');
  const followupItem=await fieldRect(page,'跟进项'),measure=await fieldRect(page,'后续整改措施');
  assert(Math.abs(followupItem.y-measure.y)<2&&measure.x>followupItem.x,'supplementary followup text fields share a desktop row');
  await checkRefreshPosition(page);
  assert.equal(fixture.writes.length,0,'step navigation only switches the existing form');
  assert.equal(await detail(page).getByRole('button',{name:'办理检修通告',exact:true}).count(),1,'notice access independent of delete permission');
  assert.equal(await detail(page).getByRole('button',{name:'删除'}).count(),0);
  await page.screenshot({path:path.join(output,'pc-followup.png'),fullPage:true});
  await detail(page).getByRole('textbox',{name:'维修进展描述',exact:true}).fill('已检查模块，安排更换。');
  await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();
  await detail(page).getByText('请选择至少一台台账设备。',{exact:true}).waitFor();assert.equal(fixture.writes.length,0,'missing equipment never writes');
  await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();
  const picker=page.getByRole('dialog').filter({has:page.getByRole('heading',{name:'选择台账设备',exact:true})});
  await picker.getByRole('checkbox').first().check();await picker.getByRole('button',{name:'下一页',exact:true}).click();await picker.getByText('A-102',{exact:true}).waitFor();await picker.getByRole('checkbox').first().check();await picker.getByRole('button',{name:'确认',exact:true}).click();
  assert.equal(await detail(page).locator('.v2-repair-device-tags .n-tag').count(),2,'selection retained across equipment pages');
  await detail(page).getByRole('button',{name:'返回列表',exact:true}).click();await page.getByText('放弃未保存修改？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续填写',exact:true}).click();assert.equal(await detail(page).count(),1,'closing protects draft');
  fixture.delaySave=800;await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();assert.equal(await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).isDisabled(),true,'cannot close during write');await detail(page).getByText('维修跟进记录已新增。',{exact:false}).waitFor();
  const write=fixture.writes.find(item=>item.path==='/api/repair-management/followups');assert.ok(write);assert.deepEqual(write.body.ledger_device_ids,['device-1','device-2']);assert.equal(write.body.fields['维修进展描述'],'已检查模块，安排更换。');assert.equal(write.body.fields['设备生产日期'],undefined);assert.equal(write.body.fields['设备使用年限'],undefined);assert.ok(write.body.operation_id);
  assert.equal(await detail(page).getByRole('button',{name:'编辑这条',exact:true}).count(),1,'saved followup returns to read mode');
  fixture.delaySave=0;
  await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).click();fixture.delayDetail=700;await page.locator('.record-row').nth(1).click();await detail(page).getByRole('button',{name:'关闭维修项目',exact:true}).click();await page.waitForTimeout(900);assert.equal(await detail(page).count(),0,'late detail must not reopen');fixture.delayDetail=0;
  const refreshBefore=fixture.reads.filter(item=>item.path==='/api/repair-refresh').length;await page.getByRole('button',{name:'刷新显示',exact:true}).click();await page.waitForTimeout(250);assert.equal(fixture.reads.filter(item=>item.path==='/api/repair-refresh').length,refreshBefore,'display refresh does not trigger remote sync');
  fixture.failRecords=true;await page.getByRole('button',{name:'刷新显示',exact:true}).click();await page.getByText(/暂显示上次读取的/).waitFor();fixture.failRecords=false;
  fixture.canDelete=true;page=await newPage(1366);await openFirst(page);await detail(page).getByText('维修单信息',{exact:true}).click();await detail(page).getByRole('button',{name:'维修单更多操作'}).click();await page.getByText('删除维修项目',{exact:true}).waitFor();await detail(page).getByRole('button',{name:'维修单更多操作'}).click();await page.screenshot({path:path.join(output,'pc-project.png'),fullPage:true});
  if(!await detail(page).count()){await openFirst(page);await detail(page).getByText('维修单信息',{exact:true}).click();}
  assert.match(await detail(page).getByRole('textbox',{name:'维修结束时间',exact:true}).inputValue(),/2026-10-20 18:30/);
  await detail(page).getByRole('textbox',{name:'备注',exact:true}).fill('核对现场进度');await detail(page).getByRole('button',{name:'保存修改',exact:true}).click();await detail(page).getByText('维修项目已保存。',{exact:true}).waitFor();
  const update=fixture.writes.find(item=>item.path==='/api/repair-management/records/project-1');assert.ok(update);assert.equal(update.body.expected_version,'v1');assert.deepEqual(update.body.fields,{'备注':'核对现场进度'},'untouched end time is not cleared');
  fixture.conflict=true;await detail(page).getByRole('textbox',{name:'备注',exact:true}).fill('当前填写应保留');await detail(page).getByRole('button',{name:'保存修改',exact:true}).click();await detail(page).locator('.n-alert').filter({hasText:'记录已由其他人修改，请核对最新版。'}).waitFor();assert.equal(await detail(page).getByRole('textbox',{name:'备注',exact:true}).inputValue(),'当前填写应保留');fixture.conflict=false;
  fixture.canDelete=false;fixture.failStats=true;page=await newPage(390);await page.getByText('数量暂不可用',{exact:false}).waitFor();assert.match(await page.locator('.v2-repair-overview .n-button').filter({hasText:'未完成'}).innerText(),/—/,'failed count not zero');fixture.failStats=false;await noOverflow(page);assert.equal(await page.locator('.v2-repair-row-state').first().isVisible(),true,'mobile keeps status');assert.equal(await page.locator('.v2-repair-row-progress').first().isVisible(),true,'mobile keeps progress');await page.screenshot({path:path.join(output,'mobile-list.png'),fullPage:true});await openFirst(page);await noOverflow(page);assert.equal(await page.evaluate(()=>document.body.style.overflow),'hidden','modal locks background scroll');await page.screenshot({path:path.join(output,'mobile-followup.png'),fullPage:true});
  for(const width of [360,402,440]){
    page=await newPage(width,640);await openFirst(page);
    await checkRefreshPosition(page);
    const shortItem=await fieldRect(page,'跟进项'),shortMeasure=await fieldRect(page,'后续整改措施');
    assert(Math.abs(shortItem.y-shortMeasure.y)<2&&shortMeasure.x>shortItem.x,'short followup notes share a mobile row');
    assert.equal(await detail(page).evaluate(el=>Math.round(el.getBoundingClientRect().height)),640,'mobile dialog uses the full viewport');
    const listBlock=page.locator('.grid-stack-item').filter({has:page.locator('.v2-repair-list')});
    if(await listBlock.count())assert.equal(await listBlock.locator('.v2-repair-detail').count(),0,'detail is not a canvas block below the list');
    const detailHeader=detail(page).locator('.v2-repair-detail-head');
    const closeBox=await detailHeader.getByRole('button',{name:'关闭维修项目',exact:true}).boundingBox(),titleBox=await detailHeader.locator('h2').boundingBox();
    assert(closeBox.x>=titleBox.x+titleBox.width,'close stays next to the title');
    const description=detail(page).getByRole('textbox',{name:'维修进展描述',exact:true});
    assert.equal(await description.getAttribute('placeholder'),'填写本次处理内容和处理结果');
    assert.equal(await description.evaluate(el=>getComputedStyle(el).fontSize),'16px','mobile text input avoids iOS zoom');
    const shortHeight=await description.evaluate(el=>el.closest('.n-input').getBoundingClientRect().height);
    assert(shortHeight>=44&&shortHeight<=48,'empty mobile text field starts at one touch-sized row');
    const longDescription='现场检查通信模块状态，记录设备现象并核对处理结果。'.repeat(12);
    await description.fill(longDescription);
    await page.waitForFunction(height=>document.querySelector('[aria-label="维修进展描述"]').closest('.n-input').getBoundingClientRect().height>height+60,shortHeight);
    assert.equal(await description.inputValue(),longDescription,'auto-growing input preserves the complete text');
    assert(await description.evaluate(el=>el.scrollHeight<=el.clientHeight+2),'long mobile text has no nested vertical scroll');
    await page.screenshot({path:path.join(output,`mobile-autogrow-${width}.png`),fullPage:true});
    await description.fill('已完成现场检查。');
    await page.waitForFunction(height=>document.querySelector('[aria-label="维修进展描述"]').closest('.n-input').getBoundingClientRect().height<=height+1,shortHeight);
    await description.fill('');
    assert.equal(await detail(page).locator('.v2-repair-followups').evaluate(el=>Number.parseFloat(getComputedStyle(el).rowGap)<=8),true,'mobile sections keep compact spacing');
    await detail(page).getByLabel('维修方',{exact:true}).selectOption('我方');
    assert.equal(await detail(page).getByLabel('维修方',{exact:true}).inputValue(),'我方');
    const spares=await detail(page).locator('.v2-repair-spares').boundingBox();
    const party=await detail(page).getByLabel('维修方',{exact:true}).evaluate(el=>{const r=el.closest('.n-form-item').getBoundingClientRect();return {left:r.left,top:r.top,bottom:r.bottom};});
    assert(party.left>=spares.x+spares.width&&party.top<spares.y+spares.height&&party.bottom>spares.y,'short switch and select share a mobile row');
    await detail(page).getByLabel('随工人员（我方维修人员）',{exact:true}).fill('验收');
    await page.getByRole('option',{name:/验收人员/}).click();
    assert.equal(await detail(page).locator('.repair-person-chip').innerText(),'验收人员');
    assert.equal(await detail(page).locator('.repair-people-popover').evaluate(el=>getComputedStyle(el).position),'relative','people results do not cover subsequent fields');
    await detail(page).getByRole('heading',{name:'本次跟进',exact:true}).click();
    await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();
    const mobilePicker=page.getByRole('dialog').filter({has:page.getByRole('heading',{name:'选择台账设备',exact:true})});
    assert.equal(await mobilePicker.locator('.ledger-filters').isVisible(),false,'filters start collapsed on mobile');
    await mobilePicker.getByRole('button',{name:'筛选设备',exact:true}).click();
    const filters=await mobilePicker.locator('.ledger-filters label').evaluateAll(items=>items.map(el=>{const b=el.getBoundingClientRect();return {x:b.x,y:b.y,right:b.right};}));
    assert.equal(filters[0].y,filters[1].y);assert(filters[2].y>filters[0].y,'filters use two rows');
    assert(filters.every(item=>item.x>=0&&item.right<=width),'filter inputs stay inside viewport');
    await mobilePicker.getByRole('combobox',{name:'机楼',exact:true}).click();await page.locator('.v2-single-select-menu').getByText('A楼',{exact:true}).click();
    assert.equal(fixture.reads.filter(item=>item.path==='/api/repair-management/ledger-candidates').at(-1).params['机楼'],'A楼','the visible filter updates the original request');
    await mobilePicker.getByRole('button',{name:/收起筛选/}).click();
    await mobilePicker.getByRole('checkbox').first().check();
    await mobilePicker.getByRole('button',{name:'下一页',exact:true}).click();await mobilePicker.getByText('A-102',{exact:true}).waitFor();await mobilePicker.getByRole('checkbox').first().check();
    const sizes=await mobilePicker.locator('.picker-close,.picker-confirm,.picker-pager button').evaluateAll(items=>items.map(el=>el.getBoundingClientRect().height));assert(sizes.every(h=>h>=44),'picker actions are touch sized');
    assert.equal(await mobilePicker.locator('.record-picker-table-wrap').evaluate(el=>el.scrollWidth<=el.clientWidth+1),true,'equipment details need no sideways scroll');
    await page.screenshot({path:path.join(output,`mobile-equipment-${width}.png`)});
    await mobilePicker.getByRole('button',{name:'确认',exact:true}).click();
    assert.equal(await detail(page).locator('.v2-repair-device-tags .n-tag').count(),2,'mobile multi-select survives pages');
    await noOverflow(page);
    await page.screenshot({path:path.join(output,`mobile-form-${width}.png`)});
    await detail(page).locator('.v2-repair-spares').evaluate(el=>el.scrollIntoView({block:'center',behavior:'instant'}));
    await page.waitForFunction(()=>{const r=document.querySelector('.v2-repair-spares').getBoundingClientRect();return r.top>=68&&r.bottom<innerHeight-150;});
    await page.screenshot({path:path.join(output,`mobile-compact-controls-${width}.png`)});
    const saveBox=await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).boundingBox();
    assert(saveBox.y>=0&&saveBox.y+saveBox.height<=640,'small-screen save button remains in the dialog viewport');
    page=await newPage(width);
    const create=page.getByRole('button',{name:'新建维修单',exact:true});
    assert(await create.locator('.n-button__content').evaluate(el=>Number.parseFloat(getComputedStyle(el).fontSize)>=12),'mobile create action retains its visible label');
    await create.click();
    const flow=detail(page).getByRole('region',{name:'关联资料自动填写'});
    await flow.waitFor();assert.match(await flow.innerText(),/未关联事件/);
    await flow.scrollIntoViewIfNeeded();await page.screenshot({path:path.join(output,`mobile-association-empty-${width}.png`)});
    assert.equal(await flow.getByRole('button',{name:'选择检修通告并带入资料',exact:true}).isDisabled(),true);
    await flow.getByRole('button',{name:'选择事件并带入资料',exact:true}).click();
    await page.locator('.record-picker-dialog').getByRole('radio').first().check();await page.locator('.record-picker-dialog').getByRole('button',{name:'确认',exact:true}).click();
    await detail(page).getByLabel('维修名称',{exact:true}).waitFor();
    await page.waitForFunction(()=>document.querySelector('[aria-label="维修名称"]').value==='关联资料自动填入的维修项目');
    assert.equal((await detail(page).getByLabel('故障发生时间',{exact:true}).innerText()).split('\n')[0],'2026-10-15 09:10');
    await flow.getByRole('button',{name:'选择检修通告并带入资料',exact:true}).click();
    await page.locator('.record-picker-dialog').getByRole('radio').first().check();await page.locator('.record-picker-dialog').getByRole('button',{name:'确认',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('[aria-label="维修结束时间"]').value==='2026-10-20T18:30');
    assert.match(await flow.innerText(),/已关联 1 条检修通告/);assert.equal(await flow.locator('details').getAttribute('open'),null,'completed associations collapse to their summary');
    assert.deepEqual(fixture.reads.filter(item=>item.path==='/api/repair-management/prefill').at(-1).body.source_repair_ids,['notice-mobile']);
    await noOverflow(page);await page.screenshot({path:path.join(output,`mobile-association-${width}.png`)});
  }
  fixture.unlinked=true;page=await newPage(390);await openFirst(page);
  assert(await detail(page).getByRole('textbox',{name:'维修进展描述',exact:true}).isVisible(),'no notice association is required to enter followup');
  assert.equal(await detail(page).getByRole('button',{name:/完成现场检查，待更换模块/}).isVisible(),false,'new followup prioritizes the current form');
  await detail(page).getByText('历史跟进 · 1 条',{exact:true}).click();
  assert(await detail(page).getByRole('button',{name:/完成现场检查，待更换模块/}).isVisible(),'saved history remains available on demand');
  await detail(page).getByText('维修单信息',{exact:true}).click();
  await detail(page).getByRole('region',{name:'关联资料自动填写'}).waitFor();fixture.unlinked=false;
  fixture.locked=true;page=await newPage();await page.locator('.record-row').first().click();await detail(page).getByText('跟进状态尚未核验，当前只读。',{exact:true}).waitFor();assert.equal(await detail(page).getByRole('button',{name:'关联已有跟进',exact:true}).count(),0);assert.equal(await detail(page).getByRole('button',{name:/保存/}).count(),0);fixture.locked=false;
  fixture.failSave=true;page=await newPage();await openFirst(page);await detail(page).getByRole('textbox',{name:'维修进展描述',exact:true}).fill('等待核验原任务');await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();await page.locator('.record-picker-dialog').getByRole('checkbox').first().check();await page.locator('.record-picker-dialog').getByRole('button',{name:'确认',exact:true}).click();await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();await detail(page).getByRole('button',{name:'核验原任务',exact:true}).waitFor();assert.equal(await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).isDisabled(),true,'uncertain write cannot be resubmitted as new');fixture.failSave=false;
  page=await newPage(1440,900,true);await openFirst(page,false);await checkWorkflowActions(page);
  await detail(page).locator('.v2-repair-detail-footer').screenshot({path:path.join(output,'workflow-actions-dark.png')});
  await detail(page).getByText('维修单信息',{exact:true}).click();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark');await checkFieldContrast(page);await noOverflow(page);
  await detail(page).locator('.v2-repair-project-form').screenshot({path:path.join(output,'dark-project-fields.png')});
  fixture.modules=['repair_management'];page=await newPage();await openFirst(page);assert.equal(await detail(page).getByRole('button',{name:'办理检修通告',exact:true}).count(),0,'module revocation removes notice access');
  // Filter the complete server result, preserving pagination, schema and source identity.
  fixture.modules=['repair_management','repair','event'];fixture.largeFilter=true;page=await newPage();
  await page.waitForFunction(()=>{const body=document.querySelector('[data-widget-id="block_records"] .v2-responsive-body');return body&&body.scrollHeight<=body.clientHeight+2;});
  const filters=page.getByRole('navigation',{name:'维修项目状态'});
  await filters.getByRole('button',{name:/^暂无跟进/}).click();
  await page.waitForFunction(()=>document.querySelector('.v2-repair-list-caption')?.textContent.includes('65'));
  assert.equal(await page.locator('.record-row').count(),30);
  await page.locator('.v2-repair-pager').getByText('2',{exact:true}).click();
  await page.getByRole('button',{name:'打开维修项目：A-31 配电设备通信异常检修',exact:true}).waitFor();
  assert(fixture.reads.some(item=>item.path==='/api/repair-management/status'&&item.params.state==='no_followup'&&item.params.offset==='30'));
  await page.locator('.record-row').first().click();await detail(page).locator('.v2-repair-project-form:visible').waitFor();
  assert.equal(await detail(page).locator('h2').innerText(),'A-31 配电设备通信异常检修','detail keeps the list title');
  assert.equal(await detail(page).locator('.v2-repair-source-flow details').getAttribute('open'),null);
  await detail(page).locator('.v2-repair-source-flow summary').click();
  await detail(page).getByRole('button',{name:'更换检修通告',exact:true}).waitFor();
  await detail(page).getByRole('button',{name:'返回列表',exact:true}).click();
  fixture.statusDelay=600;await filters.getByRole('button',{name:/^未开始/}).click();await filters.getByRole('button',{name:/^维修中/}).click();
  await page.waitForFunction(()=>document.querySelector('.v2-repair-list-caption')?.textContent.includes('维修中'));
  await page.waitForTimeout(700);
  assert.equal(await page.locator('.record-row').count(),28,'late filter responses cannot replace the current filter');
  assert((await page.locator('.v2-repair-row-state').allTextContents()).every(value=>value.includes('维修中')));
  fixture.statusDelay=0;fixture.largeFilter=false;
  await page.getByRole('button',{name:'自定义检修办理布局',exact:true}).click();
  await page.locator('.v2-page-canvas.is-customizing').waitFor();
  await page.locator('.v2-page-layout-tools').getByRole('button',{name:'取消',exact:true}).click();
  await page.waitForFunction(()=>{const body=document.querySelector('[data-widget-id="block_records"] .v2-responsive-body');return body&&body.scrollHeight<=body.clientHeight+2;});
  await page.screenshot({path:path.join(output,'status-filters-desktop.png')});
  // Click a state before the first list/schema response: subsequent editing must still work.
  fixture.delayRecords=500;const loadingPage=ctx.newPage();page=await loadingPage;
  await page.goto(origin+entry);await page.getByRole('navigation',{name:'维修项目状态'}).getByRole('button',{name:/^未开始/}).click();
  await page.locator('.record-row').first().waitFor();await page.locator('.record-row').first().click();
  await detail(page).getByLabel('维修名称',{exact:true}).waitFor();fixture.delayRecords=0;
  // A remote change invalidates cached filters/counts, without overwriting a draft.
  fixture.noFollowupCount=3;page=await newPage();
  await page.getByRole('navigation',{name:'维修项目状态'}).getByRole('button',{name:/^暂无跟进/}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.record-row').length===3);
  fixture.noFollowupCount=2;
  const recordsBeforeRemote=fixture.reads.filter(item=>item.path==='/api/repair-management/records').length;
  await page.evaluate(()=>window.__repairTestStream.dispatchEvent(new MessageEvent('repair_change',{data:JSON.stringify({cursor:1,changes:[{entity_type:'followup',action:'update',payload:{record_patch:{record_id:'project-1',followup_count:1,workflow:'维修中'}}}]})})));
  await page.waitForFunction(()=>document.querySelectorAll('.record-row').length===2);
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.v2-repair-overview button')).some(el=>/暂无跟进\s*2/.test(el.textContent)));
  await page.getByRole('navigation',{name:'维修项目状态'}).getByRole('button',{name:/^未完成/}).click();
  await page.locator('.record-row').nth(10).waitFor();
  assert(fixture.reads.filter(item=>item.path==='/api/repair-management/records').length>recordsBeforeRemote,'remote updates invalidate both list caches');
  await page.getByRole('navigation',{name:'维修项目状态'}).getByRole('button',{name:/^暂无跟进/}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.record-row').length===2);
  await openFirst(page,false);await detail(page).getByLabel('备注',{exact:true}).fill('正在填写，不可覆盖');
  fixture.noFollowupCount=1;
  await page.evaluate(()=>window.__repairTestStream.dispatchEvent(new MessageEvent('repair_change',{data:JSON.stringify({cursor:2,changes:[{entity_type:'project',action:'update',payload:{record_patch:{record_id:'project-1',followup_count:2,workflow:'维修中',display_fields:{'备注':'另一人保存'}}}}]})})));
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.v2-repair-overview button')).some(el=>/暂无跟进\s*1/.test(el.textContent)));
  assert.equal(await detail(page).getByLabel('备注',{exact:true}).inputValue(),'正在填写，不可覆盖','live refresh preserves unsaved text');
  // Saving the first followup removes the record from the zero-followup list.
  fixture.trackFilteredSave=true;fixture.noFollowupCount=1;fixture.followups=[];page=await newPage();
  await page.getByRole('navigation',{name:'维修项目状态'}).getByRole('button',{name:/^暂无跟进/}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.record-row').length===1);
  await openFirst(page);await detail(page).getByLabel('维修进展描述',{exact:true}).fill('新增首次跟进');
  await detail(page).getByRole('button',{name:'选择台账设备',exact:true}).click();
  await page.locator('.record-picker-dialog').getByRole('checkbox').first().check();
  await page.locator('.record-picker-dialog').getByRole('button',{name:'确认',exact:true}).click();
  await detail(page).getByRole('button',{name:'保存本次跟进',exact:true}).click();
  await detail(page).getByText('维修跟进记录已新增。',{exact:false}).waitFor();
  await page.waitForFunction(()=>document.querySelectorAll('.record-row').length===0);
  assert.equal(await detail(page).count(),1,'the saved detail stays open after leaving the list filter');
  await detail(page).getByRole('button',{name:'返回列表',exact:true}).click();
  await page.getByText('当前没有“暂无跟进”的维修项目',{exact:true}).waitFor();
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.v2-repair-overview button')).some(el=>/暂无跟进\s*0/.test(el.textContent)));
  assert.deepEqual(fixture.errors,[],'no browser runtime errors');
  console.log('Repair V2: isolated PC/mobile, permissions, draft guard, late requests, equipment pagination, original API payload and refresh checks passed.');
}catch(error){
  console.error('Repair browser errors:',fixture.errors);console.error('Recent fixture requests:',fixture.reads.slice(-5));
  const last=ctx?.pages().at(-1);if(last){console.error((await last.locator('body').innerText()).slice(0,700));await last.screenshot({path:path.join(output,'failed-flow.png'),fullPage:true});}
  if(last&&await last.locator('.v2-repair-detail').count())console.error('Detail:',(await last.locator('.v2-repair-detail').innerText()).slice(0,1700));
  throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
