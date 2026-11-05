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
  states[s]={template_version:'fixture-layout-v1',duty,view_scope:s,view_scopes:['D','E'],can_edit:true,history_scopes:['D','E'],history:[],completed_rounds:0,round_limit:2,active:{id:s+'round',duty,round:2,pending:false,draft_key:s+'draft',reviews:{},review_version:1,batch:{batch:1,count:1,devices:[device]},professions:['电气']}};
  const name=s+'.xlsx';
  reports[s]={template_version:'fixture-layout-v1',scope:s,date:duty.date,draft_key:s+'-owner-date',sheet_groups:{},can_edit_history:true,original_templates:templates?templates.filter(t=>t.scope===s):[{scope:s,name,profession:'电气',sheets:[{name:'配电间巡检表',rows:[['时间','电压','电流','状态','备用1','备用2'],['9:30',null,null,null,null,null],['16:00',null,null,null,null,null],...Array.from({length:25},()=>[null,null,null,null,null,null])],merges:[]},{name:'机房巡检表',rows:[['设备','读数'],['配电柜',null]],merges:[]}]}],rounds:[{id:'rec'+s,round_id:s+'old',shift:'白',round:1,version:'old1',status:'已归档',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B2',value:'210'}]},{round_id:s+'round',shift:'白',round:2,version:1,status:'进行中',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B3',parameter:'电压',value:s==='D'?'220':'380'},{template:name,sheet:'配电间巡检表',cell:'C3',parameter:'电流',value:'30'}]}]};
 }
 const seen=new Map();page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/api/**',async route=>{
  const req=route.request(),url=new URL(req.url());const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-person',name:'隔离预览',role:'admin',category:'engineer',level:'L0',modules:['daily']},allowed_scopes:['D','E'],scope_options:[{value:'D',label:'D楼'},{value:'E',label:'E楼'}],default_scope:'D'});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'inspection-fixture'}});
  if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
  if(url.pathname==='/api/daily-inspection')return ok(states[url.searchParams.get('view_scope')||'D']);
  if(url.pathname==='/api/daily-inspection/status')return ok({unchanged:true,history_scopes:['D','E']});
  if(url.pathname==='/api/daily-inspection/templates'){const s=url.searchParams.get('scope');return ok({scope:s,version:'fixture-layout-v1',templates:reports[s].original_templates});}
  if(url.pathname==='/api/daily-inspection/day-sheet'){
   const body=req.postDataJSON();reads[body.scope]++;if(oldWait&&body.scope==='D'){oldWait=false;await oldGate;}const value=structuredClone(reports[body.scope]);if(body.template_version===value.template_version)delete value.original_templates;return ok(value);
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
async function openMenu(page,expanded=true){const button=page.getByRole('button',{name:'巡检菜单',exact:true});if(await button.isVisible()&&(await button.getAttribute('aria-expanded')==='true')!==expanded)await button.click();}
async function chooseSheet(page,pattern){const picker=page.getByLabel('切换巡检工作表',{exact:true});if(await picker.isVisible()){const options=await picker.locator('option').evaluateAll(items=>items.map(o=>({value:o.value,text:o.textContent})));const option=options.find(o=>pattern.test(o.text));assert.ok(option);await picker.selectOption(option.value);}else await page.getByRole('button',{name:pattern}).click();}
async function chooseSheetIndex(page,index){const picker=page.getByLabel('切换巡检工作表',{exact:true});if(await picker.isVisible())await picker.selectOption({index});else await page.locator('.sheet-tabs button').nth(index).click();}
async function pinchSheet(page,start,end){const cdp=await page.context().newCDPSession(page);try{await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:180-start/2,y:180,id:1},{x:180+start/2,y:180,id:2}]});for(let i=1;i<=5;i++){const width=start+(end-start)*i/5;await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:180-width/2,y:180,id:1},{x:180+width/2,y:180,id:2}]});await delay(30);}await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await delay(80);}finally{await cdp.detach();}}
function validateGrid(page,templates){return (async()=>{
 const text=await page.locator('.sheet-tabs button[aria-pressed=true]').innerText();
 const candidates=templates.flatMap(t=>t.sheets).filter(s=>text.includes(s.name.trim())).sort((a,b)=>b.name.trim().length-a.name.trim().length);
 const sheet=candidates[0];assert.ok(sheet,text);
 const coord=a=>{const m=/^([A-Z]+)(\d+)$/.exec(a);let col=0;for(const c of m[1])col=col*26+c.charCodeAt(0)-64;return [Number(m[2])-1,col-1];};
 const address=(r,c)=>{let s='';for(c++;c;c=Math.floor((c-1)/26))s=String.fromCharCode(65+(c-1)%26)+s;return s+(r+1);};
 const hidden=new Set(),spans=new Map();for(const range of sheet.merges){const [a,b=a]=range.split(':');const [r,c]=coord(a),[rr,cc]=coord(b);spans.set(a,[rr-r+1,cc-c+1]);for(let y=r;y<=rr;y++)for(let x=c;x<=cc;x++)if(y!==r||x!==c)hidden.add(address(y,x));}
 const expected=[];sheet.rows.forEach((row,r)=>row.forEach((v,c)=>{const a=address(r,c);if(!hidden.has(a))expected.push({address:a,rowspan:spans.get(a)?.[0]||1,colspan:spans.get(a)?.[1]||1,text:String(v??'').trim()});}));
 const actual=await page.locator('.book-scroll td').evaluateAll(cells=>cells.map(c=>({address:c.dataset.address,rowspan:c.rowSpan,colspan:c.colSpan,text:c.textContent.trim()})));
 assert.deepEqual(actual,expected,'原表坐标/合并/文字必须一致: '+sheet.name);
})();}
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
  await chooseSheet(page,/机房巡检表/);await chooseSheet(page,/配电间巡检表/);assert.deepEqual(await scroll.evaluate(el=>[el.scrollLeft,el.scrollTop]),previous);
  // An older D response must not replace E after a fast building switch.
  await openMenu(page);f.delayD();await root.getByRole('button',{name:'刷新',exact:true}).click();await page.waitForTimeout(100);await root.locator('.book-tools label').filter({hasText:'楼栋'}).locator('select').selectOption('E');await root.getByText('380',{exact:true}).waitFor();f.releaseOld();await page.waitForTimeout(150);assert.equal(await root.locator('.book-tools label').filter({hasText:'楼栋'}).locator('select').inputValue(),'E');assert.equal(await root.getByText('226',{exact:true}).count(),0);await openMenu(page,false);
  await root.getByRole('cell',{name:'B3 380',exact:true}).click();await root.getByRole('textbox',{name:'编辑 B3'}).fill('381');await root.getByRole('textbox',{name:'编辑 B3'}).press('Enter');await page.waitForTimeout(1200);assert.equal(f.reports.E.rounds[1].original_cells[0].value,'381');assert.equal(f.reports.D.rounds[1].original_cells[0].value,'226');
  if(mobile){const table=root.locator('table');const original=(await table.boundingBox()).width;await root.getByRole('button',{name:'放大巡检表',exact:true}).click();assert.ok((await table.boundingBox()).width>original);await root.getByRole('button',{name:'恢复巡检表原始大小',exact:true}).click();assert.ok(Math.abs((await table.boundingBox()).width-original)<1);assert.ok((await scroll.boundingBox()).height>height*.8,'表格占据手机至少80%高度');assert.ok(await scroll.evaluate(el=>{const b=el.getBoundingClientRect();return el.contains(document.elementFromPoint(b.left+100,b.top+150));}),'表格内容不能被父容器裁切');const saves=f.writes.length;await pinchSheet(page,100,180);assert.ok((await table.boundingBox()).width>original*1.5,'双指张开放大');await pinchSheet(page,180,100);assert.ok(Math.abs((await table.boundingBox()).width-original)<3,'双指合拢缩小');assert.equal(f.writes.length,saves,'缩放不修改数据');assert.equal(await root.locator('td input,td select').count(),0,'缩放不误触单元格编辑');await root.getByRole('button',{name:'恢复巡检表原始大小',exact:true}).click();}
  await page.screenshot({path:path.join(output,'audit-'+name+'.png')});assert.deepEqual(f.errors,[]);
  const metrics=await page.evaluate(()=>({overflow:document.documentElement.scrollWidth-innerWidth,tableHeight:document.querySelector('.book-scroll').clientHeight}));assert.equal(metrics.overflow,0);
  console.log(JSON.stringify({name,...metrics,saves:f.writes.length,reads:f.reads,storedBytes:stored.length,errors:f.errors}));await context.close();
 }
 const page=await browser.newPage({viewport:{width:402,height:874}});const f=await setup(page,{actual:true});await page.goto(origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');await page.locator('.book-scroll td').first().waitFor();
 const measurements=[];for(let i=0;i<await page.locator('.sheet-tabs button').count();i++){const start=Date.now();await chooseSheetIndex(page,i);await page.locator('.book-scroll td').first().waitFor();measurements.push(Date.now()-start);await validateGrid(page,f.reports.D.original_templates);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth),0);}
 await chooseSheet(page,/M2HVDC/);assert.equal(await page.locator('.column-number').last().innerText(),'K');await page.screenshot({path:path.join(output,'audit-D-M2.png')});
 await openMenu(page);await page.getByRole('button',{name:'表格设置',exact:true}).click();await page.waitForTimeout(600);await page.screenshot({path:path.join(output,'audit-settings.png')});await page.getByRole('dialog').getByRole('button',{name:'完成',exact:true}).click();
 await page.getByLabel('楼栋',{exact:true}).selectOption('E');await page.waitForTimeout(400);await openMenu(page,false);for(let i=0;i<await page.locator('.sheet-tabs button').count();i++){await chooseSheetIndex(page,i);await validateGrid(page,f.reports.E.original_templates);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth),0);}
 await page.screenshot({path:path.join(output,'audit-real-template.png')});console.log(JSON.stringify({name:'real-template',switchMs:measurements,errors:f.errors}));assert.deepEqual(f.errors,[]);
 await page.evaluate(()=>localStorage.setItem('clipflow:workspace-night-mode:fixture-person','dark'));await page.reload();await page.locator('.book-scroll td').first().waitFor();await page.screenshot({path:path.join(output,'audit-dark.png')});
 const editorPage=await browser.newPage({viewport:{width:402,height:874},isMobile:true,hasTouch:true});const editFixture=await setup(editorPage);await editorPage.goto(origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');
 const book=editorPage.locator('.inspection-workbook');await book.getByRole('cell',{name:'B3 220',exact:true}).click();await book.getByRole('textbox',{name:'编辑 B3'}).fill('228');
 assert.equal(await book.getByRole('textbox',{name:'编辑 B3'}).getAttribute('inputmode'),'decimal');await book.getByRole('button',{name:'下一格',exact:true}).click();await book.getByRole('textbox',{name:'编辑 C3'}).fill('33');await book.locator('.cell-editor-tools').getByRole('button',{name:'完成',exact:true}).click();await editorPage.waitForTimeout(1200);
 editFixture.delayD();await editorPage.reload();await editorPage.getByText('模板已就绪，正在读取本轮及历史数据…',{exact:true}).waitFor();assert.ok(await editorPage.locator('.book-scroll td').count()>0);assert.equal(await editorPage.getByRole('button',{name:'确认提交',exact:true}).count(),0);editFixture.releaseOld();await editorPage.getByText('228',{exact:true}).waitFor();console.log('cached template visible before live data: OK');
 assert.equal(editFixture.reports.D.rounds[1].original_cells[0].value,'228');assert.equal(editFixture.reports.D.rounds[1].original_cells[1].value,'33');assert.deepEqual(editFixture.errors,[]);console.log('mobile next-cell and done: OK');
}finally{await browser.close();await new Promise(r=>server.close(r));}
