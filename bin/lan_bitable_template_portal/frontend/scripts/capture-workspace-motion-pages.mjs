import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {chromium} from 'playwright';

// Screenshot the shipped pages; all business responses stay in this browser context.
const root=fileURLToPath(new URL('..',import.meta.url)),dist=path.join(root,'dist');
const output=path.resolve(root,'../../../output/playwright/actual-pages-20261015');
await mkdir(output,{recursive:true});
const server=createServer(async(req,res)=>{
  try{
    const url=new URL(req.url,'http://localhost'),file=path.resolve(dist,'.'+decodeURIComponent(url.pathname));
    if(!file.startsWith(dist+path.sep))throw new Error('outside static root');
    const bytes=await readFile(file);
    res.writeHead(200,{'content-type':({'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json','.png':'image/png','.svg':'image/svg+xml'})[path.extname(file)]||'application/octet-stream','cache-control':'no-store'});res.end(bytes);
  }catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port,entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true}),errors=[],writes=[];
const scopes=[{value:'A',label:'A楼'},{value:'E',label:'E楼'}];
const user={open_id:'screen-preview-person',name:'界面预览',role:'admin',level:'L1',category:'engineer',can_delete_business:true,modules:['assistant','cabinet','maintenance','repair_management','repair','event']};
const f=(field_name,field_type=1,extra={})=>({field_name,field_type,editable:true,...extra});
const projectFields=[f('维修名称'),f('故障发生时间',5),f('故障维修原因'),f('所属专业',3,{options:['电气','暖通']}),f('维修结束时间（2026）-L',5),f('当前维修进度',2),f('备注')];
const followupFields=[f('维修进展描述'),f('维修进度',2),f('维修方',3,{options:['我方','厂家']}),f('随工人员（我方维修人员）',11),f('设备名称'),f('设备编号'),f('跟进项（如有）')];
const projectNames=['A-301-RPP-DC-003 通信模块异常检修','A楼冷冻二次泵传感器检修','A-345-HVDC-212 整流模块检修','A楼空调风机故障处理'];
const projects=projectNames.map((title,i)=>({record_id:'preview-project-'+i,title,workflow:'维修中',building:'A楼',building_codes:['A'],specialty:i===1?'暖通':'电气',progress_percent:[35,60,80,20][i],followup_count:2,fault_time:'2026-10-15 09:10',summary_only:true,is_completed:false}));
const history={record_id:'preview-followup',title:'已完成现场检查，待更换通信模块',progress:.35,created_time:'2026-10-15 10:30',ledger_device_ids:['preview-device'],ledger_devices:[{record_id:'preview-device','设备编号':'A-301-RPP-DC-003','设备名称':'列头柜','机楼':'A楼'}],raw_fields:{'维修进展描述':'已检查供电与接线，安排更换通信模块。','维修进度':.35,'设备名称':'列头柜'},display_fields:{'维修进展描述':'已检查供电与接线，安排更换通信模块。','维修进度':'35%','设备名称':'列头柜'}};
const rows=Array.from({length:6},(_,i)=>({row_id:'preview-rack-'+i,scope:'A',room:i<3?'201-M2':'202-M1',rack:'A'+String(i+1).padStart(2,'0'),rack_type:'服务器机柜',action:i===4?'下测试电':'上正式电',current_power_state:i===4?'test':'off',expected:'2026-10-15 16:30:00',actual:'2026-10-15 16:05:00',result:'成功',status:i<3?'completed':'queued',editable:false,confirmable:false,issues:[],evidence_images:[]}));
const batch={batch_id:'20261015-preview',source:'text',created_at:'2026-10-15 15:40',status:'running',scopes:['A'],rows,images:[],can_confirm_all:true,can_download_files:true,stats:{total:6,completed:3,confirmable:0}};

async function openPage(device,route){
  const context=await browser.newContext({viewport:device==='pc'?{width:1440,height:1000}:{width:390,height:844},isMobile:device==='mobile',hasTouch:device==='mobile',deviceScaleFactor:1,timezoneId:'Asia/Shanghai'});
  let answer='';
  const chat=route.includes('view=my');
  const question='帮我整理这批机柜上下电记录，哪些事项需要继续核对？';
  await context.route('**/*',async handler=>{
    const request=handler.request(),url=new URL(request.url());
    if(url.origin!==origin)return handler.abort();
    if(url.pathname==='/assets/connection-guard.js')return handler.fulfill({contentType:'text/javascript',body:''});
    if(!url.pathname.startsWith('/api/'))return handler.continue();
    const ok=data=>handler.fulfill({json:{ok:true,data}});
    if(request.method()!=='GET'){
      writes.push(url.pathname);
      if(url.pathname==='/api/assistant/notice-panel/open')return ok({run:{id:'preview-run',revision:1,scope:'A',slot:'evening',date:'2026-10-15',state:'edit',items:[]}});
      throw new Error('Unexpected write from screenshot flow: '+url.pathname);
    }
    if(url.pathname==='/api/health')return handler.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'screenshot-only'}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
    if(url.pathname==='/api/assistant/appearance')return ok({shape:'weather-lighthouse-mini',color:'encre',size:56});
    if(url.pathname==='/api/assistant/icon-weather')return ok({available:false,city:'南通'});
    if(url.pathname==='/api/assistant/conversation')return ok({conversation_id:'preview-chat',configured:true,enabled:true,busy:chat&&!answer,model_name:'灯塔助手',model_options:[],can_manage_settings:false,turns:chat?[{operation_id:'preview-answer',question,answer,status:answer?'completed':'pending',at:1792051200,process:[]} ]:[]});
    if(url.pathname==='/api/assistant/notice-panel')return ok({edition:{id:'today',date:'2026-10-15',slot:'evening',label:'17:00通告待办'},scopes});
    if(url.pathname.startsWith('/api/cabinet-power/batches/'))return ok(batch);
    if(url.pathname==='/api/cabinet-power/batches')return ok({batches:[batch],total:1});
    if(url.pathname==='/api/repair-management/stream')return handler.fulfill({status:204,body:''});
    if(url.pathname==='/api/repair-management/changes')return ok({changes:[],cursor:0});
    if(url.pathname==='/api/repair-management/status')return ok({stats:{total:4,in_progress:4,without_followup:0,completed_total:0},records:[],total:4});
    if(url.pathname==='/api/repair-management/records')return ok({fields:projectFields,records:projects,total:projects.length,offset:0});
    if(/^\/api\/repair-management\/records\//.test(url.pathname))return ok({record:{...projects[0],summary_only:false,record_version:'preview-v1',source_event_id:'preview-event',source_repair_ids:['preview-notice'],display_fields:{'维修名称':projectNames[0],'故障发生时间':'2026-10-15 09:10','故障维修原因':'通信模块异常','所属专业':'电气','维修结束时间（2026）-L':'2026-10-15 18:30','当前维修进度':'35%'},raw_fields:{}}});
    if(url.pathname==='/api/repair-management/followups')return ok({fields:followupFields,records:[history],total:1,shared_fields:{},brand_model_options:{},device_brand_model_options:{}});
    if(url.pathname==='/api/repair-management/ledger-candidates')return ok({records:[{record_id:'preview-device','设备编号':'A-301-RPP-DC-003','设备名称':'列头柜','机楼':'A楼','系统名称':'配电','大设备类型':'列头柜'}],total:1,page:1,options:{},cache:{ready:true,refreshing:false}});
    if(url.pathname==='/api/repair-management/health')return ok({pending_count:0,failed_count:0});
    if(url.pathname==='/api/repair-management/sync-status')return ok({status:'completed'});
    if(url.pathname==='/api/repair-management/people')return ok({people:[],total:0});
    return ok({items:[],records:[],fields:[],people:[],scopes:[],stats:{},settings:{},warnings:[],total:0});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  await page.goto(entry+route);
  return {page,context,reply:()=>{answer='### 本批次处理情况\n\n这批预览记录共有 **6 台机柜**，其中 **3 台已完成，3 台等待处理**。\n\n### 接下来核对这三项\n\n1. 核对包间、机柜号及上电／下电方向，避免选错设备。\n2. 检查期望时间、实际时间和操作结果，缺少的内容先补齐。\n3. 确认证明照片与机柜对应，再继续原待办任务。\n\n**正在处理的记录不要重复提交。** 可在机柜待办中查看进度，并对异常项单独核对。';}};
}

try{
  for(const device of ['pc','mobile']){
    let session=await openPage(device,'?view=my');let {page}=session;
    await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
    await page.locator('.assistant-thinking').waitFor();
    const sidebar=page.getByRole('button',{name:'侧边栏（通告待办 / 知识库）',exact:true});
    if(await sidebar.count()&&await sidebar.getAttribute('aria-expanded')==='true')await sidebar.click();
    await page.screenshot({path:path.join(output,`assistant-thinking-${device}.png`)});
    session.reply();await page.locator('.assistant-rich-reply strong').first().waitFor();
    await page.waitForFunction(()=>!document.querySelector('.v2-reply [hidden]'));
    await page.waitForTimeout(450);
    const thread=page.locator('.thread');await thread.evaluate(el=>{el.scrollTop=0;});await page.waitForTimeout(100);
    await page.screenshot({path:path.join(output,`assistant-${device}.png`)});
    if(await sidebar.count()){
    if(await sidebar.getAttribute('aria-expanded')!=='true')await sidebar.click();
    const building=page.locator('.notice-panel .n-select[aria-label="楼栋"]');await building.click();
    await page.locator('.n-base-select-option').getByText('E楼',{exact:true}).click();
    await page.locator('.notice-panel .n-select[aria-label="时段"]').click();
    await page.locator('.n-base-select-option').getByText('今日 08:00',{exact:true}).click();
    await building.click();await page.locator('.n-base-select-option').getByText('A楼',{exact:true}).waitFor();
    await page.waitForTimeout(450);
    await page.screenshot({path:path.join(output,`assistant-select-${device}.png`)});
    await page.keyboard.press('Escape');
    }
    await session.context.close();

    session=await openPage(device,'?__page=/cabinet-power/batches&scope=A&batch_id=20261015-preview');page=session.page;
    await page.locator('.detail-table tbody tr').first().waitFor();
    await page.locator('.v2-motion-progress[aria-valuenow="50"]').waitFor();await page.waitForTimeout(850);
    await page.screenshot({path:path.join(output,`cabinet-${device}.png`)});
    await session.context.close();

    session=await openPage(device,'?__page=/repair-management&scope=A');page=session.page;
    await page.locator('.record-row').first().click();
    const detail=page.locator('.v2-repair-detail');await detail.getByRole('heading',{name:'本次跟进',exact:true}).waitFor();
    await detail.getByRole('textbox',{name:'维修进展描述',exact:true}).fill('已完成模块更换与接线复核，正在观察通信恢复情况。');
    const progress=detail.locator('.v2-repair-followups .v2-repair-progress-input .n-input-number input');
    await progress.fill('60');await progress.press('Tab');
    await detail.getByRole('button',{name:'选择台账设备',exact:true}).click();
    const picker=page.getByRole('dialog');await picker.getByRole('checkbox').first().check();await picker.getByRole('button',{name:'确认',exact:true}).click();
    await detail.locator('.v2-repair-followup-heading').evaluate((el,device)=>{
      if(device==='mobile'){window.scrollBy({top:el.getBoundingClientRect().top-80,behavior:'instant'});return;}
      const main=document.querySelector('.v2-main');
      main.scrollTop+=el.getBoundingClientRect().top-main.getBoundingClientRect().top-24;
      window.scrollTo({top:0,behavior:'instant'});
    },device);
    await page.waitForTimeout(350);await page.screenshot({path:path.join(output,`followup-${device}.png`)});
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'No page overflow');
    await session.context.close();
  }
  assert.deepEqual(errors,[]);assert(writes.every(p=>p==='/api/assistant/notice-panel/open'));
  console.log(JSON.stringify({output,files:['assistant-pc.png','assistant-mobile.png','cabinet-pc.png','cabinet-mobile.png','followup-pc.png','followup-mobile.png'],mode:'production pages with isolated sample responses; no business writes'},null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));}
