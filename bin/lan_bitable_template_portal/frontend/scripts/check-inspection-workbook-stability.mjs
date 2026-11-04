import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../build_output/inspection-mobile-review');await mkdir(output,{recursive:true});
const server=createServer(async(req,res)=>{try{let name=new URL(req.url,'http://localhost').pathname;name=name.startsWith('/assets/')?name.slice(1):'index.html';const file=path.resolve(root,'dist',name);if(!file.startsWith(path.join(root,'dist')+path.sep))throw Error();res.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'})[path.extname(file)]||'application/octet-stream');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function setup(page,{slow=false,lose=false,actual=false}={}){
 const states={},reports={},writes=[],errors=[],reads={D:0,E:0};let release,releaseOld,oldWait=false;
 const gate=new Promise(r=>release=r),oldGate=new Promise(r=>releaseOld=r);let requested;const waiting=new Promise(r=>requested=r);
 const templates=actual?JSON.parse(await readFile(path.resolve(root,'../../resources/daily_inspection_templates.json'),'utf8')).templates:null;
 for(const s of ['D','E']){
  const duty={scope:s,date:'2026-11-04',shift:'白'};
  const device={id:s+':dev',scope:s,name:s+'楼配电柜',profession:'电气',sections:[{location:s+'-245配电间',form:'电气',integrity:'完整',missing:'',note:''}],points:[],parameters:[{id:'v',name:'电压',reference:s==='D'?220:380,section:0}]};
  states[s]={duty,view_scope:s,view_scopes:['D','E'],can_edit:true,history_scopes:['D','E'],history:[],completed_rounds:0,round_limit:2,active:{id:s+'round',duty,round:2,pending:false,draft_key:s+'draft',reviews:{},review_version:1,batch:{batch:1,count:1,devices:[device]},professions:['电气']}};
  const name=s+'.xlsx';
  reports[s]={scope:s,date:duty.date,draft_key:s+'-owner-date',sheet_groups:{},can_edit_history:true,original_templates:templates?templates.filter(t=>t.scope===s):[{scope:s,name,profession:'电气',sheets:[{name:'配电间巡检表',rows:[['时间','电压','电流','状态','备用1','备用2'],['9:30',null,null,null,null,null],['16:00',null,null,null,null,null],...Array.from({length:25},()=>[null,null,null,null,null,null])],merges:[]},{name:'机房巡检表',rows:[['设备','读数'],['配电柜',null]],merges:[]}]}],rounds:[{id:'rec'+s,round_id:s+'old',shift:'白',round:1,version:'old1',status:'已归档',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B2',value:'210'}]},{round_id:s+'round',shift:'白',round:2,version:1,status:'进行中',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B3',parameter:'电压',value:s==='D'?'220':'380'},{template:name,sheet:'配电间巡检表',cell:'C3',parameter:'电流',value:'30'}]}]};
 }
 const seen=new Map();page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/api/**',async route=>{
  const req=route.request(),url=new URL(req.url());const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-person',name:'隔离预览',role:'admin',category:'engineer',level:'L0',modules:['daily']},allowed_scopes:['D','E'],scope_options:[{value:'D',label:'D楼'},{value:'E',label:'E楼'}],default_scope:'D'});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'inspection-fixture'}});
  if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
  if(url.pathname==='/api/daily-inspection')return ok(states[url.searchParams.get('view_scope')||'D']);
  if(url.pathname==='/api/daily-inspection/status')return ok({unchanged:true,history_scopes:['D','E']});
  if(url.pathname==='/api/daily-inspection/day-sheet'){
   const body=req.postDataJSON();reads[body.scope]++;if(oldWait&&body.scope==='D'){oldWait=false;await oldGate;}return ok(reports[body.scope]);
  }
  if(url.pathname==='/api/daily-inspection/draft'){
   const body=req.postDataJSON(),s=url.searchParams.get('view_scope'),fingerprint=JSON.stringify(body);writes.push({scope:s,body});
   if(writes.length===1&&slow){requested();await gate;}
   if(!seen.has(fingerprint)){
    assert.equal(body.id,states[s].active.id);assert.equal(body.review_version,states[s].active.review_version);
    for(const c of body.sheet_cells){const r=reports[s].rounds[1];const existing=r.original_cells.find(x=>x.template===c.template&&x.sheet===c.sheet&&x.cell===c.cell);if(existing)existing.value=c.value;else(r.extra_cells||=[]).push(c);}
    states[s].active.review_version++;reports[s].rounds[1].version=states[s].active.review_version;seen.set(fingerprint,true);
   }
   if(lose&&writes.length===1)return route.abort('failed');
   return ok(states[s]);
  }
  if(url.pathname==='/api/daily-inspection/history-edit'){
   const body=req.postDataJSON(),s=url.searchParams.get('scope');assert.equal(body.id,'rec'+s);
   for(const c of body.cells)reports[s].rounds[0].original_cells.find(x=>x.cell===c.cell).value=c.value;
   reports[s].rounds[0].version='old2';return ok({id:body.id,scope:s,version:'old2'});
  }
  return ok({items:[],revision:0,value:null});
 });
 return {writes,reads,states,reports,errors,waiting,release,delayD:()=>{oldWait=true;},releaseOld};
}
try{
 for(const [name,width,height,mobile] of [['phone',402,874,true],['small',360,640,true],['landscape',874,402,true],['desktop',1440,900,false],['legacy',402,874,true]]){
  if(process.env.INSPECTION_VIEWPORT&&name!==process.env.INSPECTION_VIEWPORT)continue;
  const context=await browser.newContext({viewport:{width,height},isMobile:mobile,hasTouch:mobile});const page=await context.newPage();page.setDefaultTimeout(15000);
  const f=await setup(page,{slow:true,lose:name==='phone'});
  await page.goto(name==='legacy'?origin+'/daily-inspection?frontend=legacy':origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');const root=page.locator('.inspection-workbook');await root.getByText('220',{exact:true}).waitFor();
  const first=root.getByRole('cell',{name:'B3 220',exact:true});await first.click();const input=root.getByRole('textbox',{name:'编辑 B3'});await input.fill('226');await input.click();assert.equal(await input.inputValue(),'226','再次点击不能重置输入');await input.press('Enter');await f.waiting;
  await root.getByRole('cell',{name:'C3 30',exact:true}).click();await root.getByRole('textbox',{name:'编辑 C3'}).fill('31');await root.getByRole('textbox',{name:'编辑 C3'}).press('Enter');
  const stored=await page.evaluate(()=>localStorage.getItem('inspection-sheet-edits:D-owner-date'));assert.ok(stored.length<1800,'只暂存修改和轮次标识，不复制完整历史表');
  f.release();if(name==='phone'){await root.getByRole('button',{name:'重试保存',exact:true}).waitFor();await page.waitForTimeout(250);await root.getByRole('button',{name:'重试保存',exact:true}).click();}
  await page.waitForFunction(()=>document.querySelector('.book-status')?.textContent.includes('单元格修改自动保存'));await page.waitForTimeout(400);
  assert.equal(f.states.D.active.review_version,3);assert.equal(f.states.E.active.review_version,1);assert.equal(f.reports.D.rounds[1].original_cells[1].value,'31');assert.equal(f.reads.D,1,'确认保存后不重读整张表');
  assert.equal(f.writes.at(-1).body.review_version,2,'连续修改使用已确认的新版本');
  const scroll=root.locator('.book-scroll');await scroll.evaluate(el=>{el.scrollLeft=120;el.scrollTop=180;});const previous=await scroll.evaluate(el=>[el.scrollLeft,el.scrollTop]);
  await root.getByRole('button',{name:/机房巡检表/}).click();await root.getByRole('button',{name:/配电间巡检表/}).click();assert.deepEqual(await scroll.evaluate(el=>[el.scrollLeft,el.scrollTop]),previous);
  // An older D response must not replace E after a fast building switch.
  f.delayD();await root.getByRole('button',{name:'刷新',exact:true}).click();await page.waitForTimeout(100);await root.locator('.book-tools label').filter({hasText:'楼栋'}).locator('select').selectOption('E');await root.getByText('380',{exact:true}).waitFor();f.releaseOld();await page.waitForTimeout(150);assert.equal(await root.locator('.book-tools label').filter({hasText:'楼栋'}).locator('select').inputValue(),'E');assert.equal(await root.getByText('226',{exact:true}).count(),0);
  await root.getByRole('cell',{name:'B3 380',exact:true}).click();await root.getByRole('textbox',{name:'编辑 B3'}).fill('381');await root.getByRole('textbox',{name:'编辑 B3'}).press('Enter');await page.waitForTimeout(1200);assert.equal(f.reports.E.rounds[1].original_cells[0].value,'381');assert.equal(f.reports.D.rounds[1].original_cells[0].value,'226');
  await page.screenshot({path:path.join(output,'audit-'+name+'.png')});assert.deepEqual(f.errors,[]);
  const metrics=await page.evaluate(()=>({overflow:document.documentElement.scrollWidth-innerWidth,tableHeight:document.querySelector('.book-scroll').clientHeight}));assert.equal(metrics.overflow,0);
  console.log(JSON.stringify({name,...metrics,saves:f.writes.length,reads:f.reads,storedBytes:stored.length,errors:f.errors}));await context.close();
 }
 const page=await browser.newPage({viewport:{width:402,height:874}});const f=await setup(page,{actual:true});await page.goto(origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');await page.locator('.book-scroll td').first().waitFor();
 const measurements=[];for(let i=0;i<10;i++){const start=Date.now();await page.locator('.sheet-tabs button').nth(i).click();await page.locator('.book-scroll td').first().waitFor();measurements.push(Date.now()-start);}
 await page.screenshot({path:path.join(output,'audit-real-template.png')});console.log(JSON.stringify({name:'real-template',switchMs:measurements,errors:f.errors}));assert.deepEqual(f.errors,[]);
 await page.evaluate(()=>localStorage.setItem('clipflow:workspace-night-mode:fixture-person','dark'));await page.reload();await page.locator('.book-scroll td').first().waitFor();await page.screenshot({path:path.join(output,'audit-dark.png')});
}finally{await browser.close();await new Promise(r=>server.close(r));}
