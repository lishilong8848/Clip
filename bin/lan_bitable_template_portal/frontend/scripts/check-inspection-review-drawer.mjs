import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

// Covers the per-sheet review drawer and the round-level profession grouping.
// The fixture deliberately puts the SAME measurement in DIFFERENT cell addresses per round
// (C2 in 白1, D2 in 白2), which is how the real backend maps non-chiller sheets. Comparing by
// address would find nothing; only the semantic identity join reports the difference.
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const dist=path.resolve(process.env.INSPECTION_LEGACY_DIR || path.join(root,'dist'));
const output=path.resolve(root,'../../../build_output/inspection-review-drawer');await mkdir(output,{recursive:true});
const server=createServer(async(req,res)=>{try{let name=new URL(req.url,'http://localhost').pathname;name=name.startsWith('/assets/')?name.slice(1):'index.html';const file=path.resolve(dist,name);if(!file.startsWith(dist+path.sep))throw Error();res.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'})[path.extname(file)]||'application/octet-stream');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
 const page=await browser.newPage({viewport:{width:1440,height:900}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 const duty={scope:'D',date:'2026-11-04',shift:'白'},template='D.xlsx',sheet='配电间巡检表';
 const points=[
  {id:'pt1',point_id:'p1',enabled:true,template,sheet,round_cells:{'白2':'D2'}},
  {id:'pt2',point_id:'p2',enabled:true,template,sheet,round_cells:{'白2':'D3'}}];
 const device={id:'dev1',scope:'D',name:'D楼配电柜',profession:'电气',sections:[],points,
   parameters:[{id:'a',name:'电压',point:'pt1',reference:220},{id:'b',name:'电流',point:'pt2',reference:''}]};
 const active={id:'Dround',duty,round:2,pending:false,draft_key:'Ddraft',reviews:{},review_version:1,started_by:'系统自动巡检',
   batch:{batch:1,count:1,devices:[device]},professions:['电气'],worksheet_mode:true,profession_versions:{电气:'v-electric'},available_professions:['电气','暖通'],visited_sheets:[],profession_confirmations:{}};
 const state={template_version:'v1',duty,view_scope:'D',view_scopes:['D'],can_edit:true,history_scopes:['D'],
   history:[],completed_rounds:1,round_limit:2,active};
 const templates=[{scope:'D',name:template,profession:'电气',sheets:[
   {name:'封面',rows:[['封面']],merges:[]},
   {name:sheet,rows:[['设备','参数','白班1单元格','白班2单元格'],['配电柜','电压',null,null],['配电柜','电流',null,null]],merges:[]}]}];
 const report={template_version:'v1',scope:'D',date:duty.date,draft_key:'D-owner-date',sheet_groups:{},
   can_edit_history:true,original_templates:templates,rounds:[
    {id:'recD',round_id:'Dold',shift:'白',round:1,version:'old1',status:'已归档',
     sheet_groups:{'["D.xlsx","配电间巡检表"]':'暖通'},
     original_cells:[{template,sheet,cell:'C2',value:'210',parameter:'电压',device:'dev1',kind:'point',point_id:'p1',value_type:'数值',read_only:false}]},
    {round_id:'Dround',shift:'白',round:2,version:1,status:'进行中',
     original_cells:[
      {template,sheet,cell:'D2',value:'220',parameter:'电压',device:'dev1',kind:'point',point_id:'p1',value_type:'数值',read_only:false},
      {template,sheet,cell:'D3',value:'',parameter:'电流',device:'dev1',kind:'point',point_id:'p2',value_type:'数值',read_only:false}]}]};
 await page.route(url=>url.pathname.startsWith('/api/'),async route=>{
  const req=route.request(),url=new URL(req.url()),ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture',name:'核对人员',role:'admin',category:'engineer',level:'L0',modules:['daily']},allowed_scopes:['D'],scope_options:[{value:'D',label:'D楼'}],default_scope:'D'});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'fixture'}});
  if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
  if(url.pathname==='/api/daily-inspection')return ok(state);
  if(url.pathname==='/api/daily-inspection/status')return ok({unchanged:true,history_scopes:['D'],visited_sheets:active.visited_sheets});
  if(url.pathname==='/api/daily-inspection/view-sheet'){
   active.visited_sheets=req.postDataJSON().visited_sheets;
   return ok({visited_sheets:active.visited_sheets,presence:[]});
  }
  if(url.pathname==='/api/daily-inspection/confirm-profession'){
   assert.deepEqual(req.postDataJSON(),{id:'Dround',profession:'电气',profession_version:'v-electric'});
   active.profession_confirmations.电气={actor:'核对人员',version:'v-electric'};
   return ok(state);
  }
  if(url.pathname==='/api/daily-inspection/source')return ok({scope:'D',devices:[device],orders:{},batch:1});
  if(url.pathname==='/api/daily-inspection/templates')return ok({scope:'D',version:'v1',templates});
  if(url.pathname==='/api/daily-inspection/day-sheet')return ok(structuredClone(report));
  if(url.pathname==='/api/daily-inspection/draft')return ok(state);
  return ok({items:[],revision:0,value:null});
 });
 await page.goto(origin+'/daily-inspection?frontend=legacy');
 const book=page.locator('.inspection-workbook');
 await book.getByRole('cell',{name:'D2 220',exact:true}).waitFor({timeout:20000});

 // --- §5.2 pending count lives in the status bar, on every viewport ---
 const review=book.locator('.status-review');
 await review.waitFor({timeout:5000});
 assert.doesNotMatch(await book.locator('.book-status').innerText(),/系统自动巡检|本次巡检|历史巡检/);
 assert.equal((await review.getByRole('button',{name:/^待填/}).innerText()).trim(),'待填 1','本表待填计数');

 // --- §5.1 the drawer joins rounds by measurement identity, not by cell address ---
 await review.getByRole('button',{name:'本表核对',exact:true}).click();
 const dialog=page.getByRole('dialog');await dialog.waitFor();
 const filters=dialog.locator('.review-filters');
 assert.equal((await filters.getByRole('button',{name:/^全部/}).innerText()).trim(),'全部 2');
 assert.equal((await filters.getByRole('button',{name:/^待填/}).innerText()).trim(),'待填 1');
 assert.equal((await filters.getByRole('button',{name:/^与上轮有差异/}).innerText()).trim(),'与上轮有差异 1');
 assert.match(await dialog.locator('.settings-note').first().innerText(),/上轮＝白1/,'说明比对基准轮次');
 const voltage=dialog.locator('.review-row').filter({hasText:'电压'});
 assert.equal(await voltage.count(),1);
 assert.match(await voltage.locator('.review-label').innerText(),/配电柜 · 电压/,'用原表行标题+测点名，不用设备标识');
 assert.match(await voltage.locator('.review-label small').innerText(),/^D2$/);
 assert.equal(await voltage.locator('.review-values i').innerText(),'220','本轮值');
 assert.equal(await voltage.locator('.review-values em').innerText(),'210','上轮值来自 C2，按测点身份配对成功');
 assert.equal(await voltage.locator('.review-values b').innerText(),'+4.76%');
 await page.screenshot({path:path.join(output,'review-all.png')});

 // --- filter to pending and jump straight into that cell ---
 await filters.getByRole('button',{name:/^待填/}).click();
 assert.equal(await dialog.locator('.review-row').count(),1);
 const current=dialog.locator('.review-row').filter({hasText:'电流'});
 assert.equal(await current.locator('.review-values i').innerText(),'待填写');
 await current.click();
 await dialog.waitFor({state:'hidden'});
 await book.getByRole('textbox',{name:'编辑 D3'}).waitFor({timeout:5000});
 assert.equal(await book.getByRole('textbox',{name:'编辑 D3'}).inputValue(),'','点击待填项直接进入该单元格编辑');
 await book.getByRole('textbox',{name:'编辑 D3'}).press('Escape');

 // --- §5.2 next-empty jumps without opening the drawer ---
 await review.getByRole('button',{name:/^待填/}).click();
 await book.getByRole('textbox',{name:'编辑 D3'}).waitFor({timeout:5000});
 await book.getByRole('textbox',{name:'编辑 D3'}).press('Escape');

 // --- §3.4 an archived round shows its own frozen grouping, not the live shift's ---
 await book.getByRole('group',{name:'巡检轮次与状态'}).getByRole('button',{name:/白1/}).click();
 await book.getByRole('button',{name:'表格设置',exact:true}).click();
 const settings=page.getByRole('dialog');await settings.waitFor();
 await settings.getByText('该轮专业归属',{exact:true}).waitFor({timeout:5000});
 const historicalText=await settings.locator('.settings-section').filter({hasText:'该轮专业归属'}).innerText();
 assert.match(historicalText,/配电间巡检表 · 暖通/);
 assert.match(historicalText,/本班对应归属为 电气/);
 assert.equal(await settings.getByRole('button',{name:'暖通',exact:true}).count(),0,'查看归档轮次时不提供归属编辑，避免改到进行中班次');
 await page.screenshot({path:path.join(output,'frozen-grouping.png')});
 await settings.getByRole('button',{name:'完成',exact:true}).click();await settings.waitFor({state:'hidden'});

 // --- back on the active round the editable control returns ---
 await book.getByRole('group',{name:'巡检轮次与状态'}).getByRole('button',{name:/白2/}).click();
 await book.getByRole('button',{name:/^专业分工/}).click();
 const again=page.getByRole('dialog');await again.waitFor();
 await again.getByText('本班专业归属',{exact:true}).waitFor({timeout:5000});
 await again.getByRole('combobox',{name:'配电间巡检表归属'}).waitFor();
 await page.screenshot({path:path.join(output,'active-grouping.png')});
 await again.getByRole('button',{name:'完成',exact:true}).click();
 await again.waitFor({state:'hidden'});
 const submit=book.getByRole('button',{name:'确认提交',exact:true});
 assert(await submit.isDisabled(),'已查看所有工作表仍需专业确认');
 await book.getByRole('button',{name:/^电气确认/}).click();
 await book.getByText(/电气 · 核对人员/).waitFor();
 assert(await submit.isEnabled(),'已选专业确认后可提交');
 await book.getByRole('button',{name:'定位单元格',exact:true}).click();
 await page.getByRole('searchbox').fill('电压');
 await page.getByRole('dialog').getByRole('button',{name:/电压.*B2/}).click();
 await page.waitForFunction(()=>document.activeElement?.getAttribute('data-address')==='B2');
 assert.deepEqual(errors,[]);
 console.log('本表核对抽屉、跨轮身份比对、待填跳转和轮次级专业归属：OK');
 await page.close();
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
