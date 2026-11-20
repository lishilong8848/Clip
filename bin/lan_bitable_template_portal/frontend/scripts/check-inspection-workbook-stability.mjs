import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {inspectionCellText} from '../src/components/inspectionCellFormat.ts';
assert.equal(inspectionCellText('09:30:00','h:mm'),'9:30');
assert.equal(inspectionCellText(0.5,'h:mm'),'12:00');
assert.equal(inspectionCellText(0.1234,'0.0%'),'12.3%');
assert.equal(inspectionCellText('0012','@'),'0012');
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const dist=path.resolve(process.env.INSPECTION_LEGACY_DIR || path.join(root,'dist'));
const output=path.resolve(root,'../../../build_output/inspection-mobile-review');await mkdir(output,{recursive:true});
const server=createServer(async(req,res)=>{try{let name=new URL(req.url,'http://localhost').pathname;name=name.startsWith('/assets/')?name.slice(1):'index.html';const file=path.resolve(dist,name);if(!file.startsWith(dist+path.sep))throw Error();res.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'})[path.extname(file)]||'application/octet-stream');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function setup(page,{slow=false,lose=false,actual=false}={}){
 const states={},reports={},writes=[],errors=[],reads={D:0,E:0};let release,releaseOld,heldD,holdConcurrentReads;
 const gate=new Promise(r=>release=r);
 const templates=actual?JSON.parse(await readFile(path.resolve(root,'../../resources/daily_inspection_templates.json'),'utf8')).templates:null;
 for(const s of ['D','E']){
  const duty={scope:s,date:'2026-11-04',shift:'白'};
  const device={id:s+':dev',scope:s,name:s+'楼配电柜',profession:'电气',sections:[{location:s+'-245配电间',form:'电气',integrity:'完整',missing:'',note:''}],points:[],parameters:[{id:'v',name:'电压',reference:s==='D'?220:380,section:0}]};
  states[s]={template_version:'fixture-layout-v1',duty,view_scope:s,view_scopes:['D','E'],can_edit:true,history_scopes:['D','E'],history:[],completed_rounds:0,round_limit:2,active:{id:s+'round',duty,round:2,pending:false,source_locked:true,draft_key:s+'draft',reviews:{},review_version:1,batch:{batch:1,count:1,devices:[device]},professions:['电气']}};
  const name=s+'.xlsx';
  device.points=[['v','B3'],['a','C3']].map(([id,cell])=>({id,point_id:id,enabled:true,template:name,sheet:'配电间巡检表',round_cells:{白2:cell}}));
  device.parameters=device.points.map(point=>({id:'p:'+point.id,point:point.id,name:point.id==='v'?'电压':'电流'}));
  states[s].active.manual_sheet_cells=[];
  reports[s]={template_version:'fixture-layout-v1',scope:s,date:duty.date,draft_key:s+'-owner-date',sheet_groups:{},can_edit_history:true,original_templates:templates?templates.filter(t=>t.scope===s):[{scope:s,name,profession:'电气',sheets:[{name:'配电间巡检表',rows:[['时间','电压','电流','状态','备用1','备用2'],['9:30',null,null,null,null,null],['16:00',null,null,null,null,null],...Array.from({length:25},()=>[null,null,null,null,null,null])],merges:[]},{name:'机房巡检表',rows:[['设备','读数'],['配电柜',null]],merges:[]}]}],rounds:[{id:'rec'+s,round_id:s+'old',shift:'白',round:1,version:'old1',status:'已归档',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B2',value:'210'}]},{round_id:s+'round',shift:'白',round:2,version:1,status:'进行中',original_cells:[{template:name,sheet:'配电间巡检表',cell:'B3',parameter:'电压',value:s==='D'?'220':'380'},{template:name,sheet:'配电间巡检表',cell:'C3',parameter:'电流',value:'30'}]}]};
  if(actual)for(const t of reports[s].original_templates)for(const sheet of t.sheets){const merged=new Set();for(const range of sheet.merges){const [a,b=a]=range.split(':');const xy=x=>{const m=/^([A-Z]+)(\d+)$/.exec(x);let c=0;for(const letter of m[1])c=c*26+letter.charCodeAt(0)-64;return [+m[2]-1,c-1];};const [r,c]=xy(a),[rr,cc]=xy(b);for(let y=r;y<=rr;y++)for(let x=c;x<=cc;x++)merged.add(y+':'+x);}let index=0;for(let r=5;r<sheet.rows.length&&index<2;r++)for(let c=1;c<sheet.rows[r].length&&index<2;c++){if(sheet.rows[r][c]!=null||merged.has(r+':'+c))continue;let letters='',col=c+1;for(;col;col=Math.floor((col-1)/26))letters=String.fromCharCode(65+(col-1)%26)+letters;reports[s].rounds[index].original_cells.push({template:t.name,sheet:sheet.name,cell:letters+(r+1),value:index?'123.456789':'√'});index++;}}
  if(!actual)reports[s].original_templates[0].sheets[0].column_widths=[110,110,110,110,110,110];
 }
 if(actual)for(const scope of ['D','E']){
  states[scope].active.manual_sheet_cells=reports[scope].rounds[1].original_cells;
 }
 const seen=new Map();page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.origin!==origin)return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-person',name:'隔离预览',role:'admin',category:'engineer',level:'L0',modules:['daily']},allowed_scopes:['D','E'],scope_options:[{value:'D',label:'D楼'},{value:'E',label:'E楼'}],default_scope:'D'});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'inspection-fixture'}});
  if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
  if(url.pathname==='/api/daily-inspection')return ok(states[url.searchParams.get('view_scope')||'D']);
  if(url.pathname==='/api/daily-inspection/view-sheet')return ok({visited_sheets:req.postDataJSON().visited_sheets,presence:[]});
  if(url.pathname==='/api/daily-inspection/status')return ok({unchanged:true,history_scopes:['D','E']});
  if(url.pathname==='/api/daily-inspection/templates'){const s=url.searchParams.get('scope');return ok({scope:s,version:'fixture-layout-v1',templates:reports[s].original_templates});}
  if(url.pathname==='/api/daily-inspection/day-sheet'){
   const body=req.postDataJSON(),value=structuredClone(reports[body.scope]);
   reads[body.scope]++;
   if(heldD&&body.scope==='D'){
    const hold=heldD;
    if(!holdConcurrentReads)heldD=undefined;
    await hold;
   }
   value.summary_only=Boolean(body.summary_only);
   value.sheet_filter=body.view_sheet||null;
   for(const round of value.rounds){
    if(value.summary_only)round.original_cells=[];
    else if(value.sheet_filter){
     const [template,sheet]=value.sheet_filter;
     round.original_cells=round.original_cells.filter(cell=>cell.template===template&&cell.sheet===sheet);
    }
   }
   if(body.template_version===value.template_version)delete value.original_templates;
   return ok(value);
  }
  if(url.pathname==='/api/daily-inspection/draft'){
   const body=req.postDataJSON(),s=url.searchParams.get('view_scope'),fingerprint=JSON.stringify(body);writes.push({scope:s,body});
   if(writes.length===1&&slow)await gate;
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
 return {writes,reads,states,reports,errors,release,delayD:({concurrent=false}={})=>{
  holdConcurrentReads=concurrent;
  heldD=new Promise(r=>releaseOld=r);
  return page.waitForRequest(r=>new URL(r.url()).pathname==='/api/daily-inspection/day-sheet'&&r.postDataJSON().scope==='D');
 },releaseOld:()=>{heldD=undefined;releaseOld();}};
}
async function openMenu(page,expanded=true){
 const button=page.getByRole('button',{name:/^(巡检菜单|轮次与选表|收起操作)$/});
 if(await button.isVisible()&&(await button.getAttribute('aria-expanded')==='true')!==expanded)await button.click();
}
async function chooseSheet(page,pattern,legacy=false){
 if(!legacy){
  await openMenu(page);
  const picker=page.getByRole('combobox',{name:'工作表',exact:true});
  const options=await picker.locator('option').allTextContents();
  const index=options.findIndex(label=>pattern.test(label));
  assert.notEqual(index,-1,`找不到工作表 ${pattern}`);
  await picker.selectOption({index});
  return;
 }
 const tabs=page.locator('.sheet-tabs');
 if(await tabs.isVisible()){
  await tabs.getByRole('button',{name:pattern}).click();
  return;
 }
 await page.locator('.sheet-picker .n-base-selection').click();
 await page.locator('.n-base-select-option').filter({hasText:pattern}).first().click();
}
async function pinchSheet(page,start,end){
 const box=await page.locator('.book-scroll').boundingBox();
 const x=box.x+box.width/2,y=box.y+box.height/2;
 const points=width=>[{x:x-width/2,y,id:1},{x:x+width/2,y,id:2}];
 const cdp=await page.context().newCDPSession(page);
 try{
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:points(start)});
  for(let i=1;i<=5;i++){
   await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:points(start+(end-start)*i/5)});
   await delay(40);
  }
  await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
 }finally{await cdp.detach();}
}
async function validateGrid(page,templates,rounds){
 const [template,sheetName]=JSON.parse(await page.locator('.ins-sheet-choice select').inputValue());
 const sheet=templates.find(t=>t.name===template)?.sheets.find(s=>s.name===sheetName);
 assert.ok(sheet,sheetName);
 await page.locator('.ins-foot').getByText(/工作表已载入/).waitFor();
 const coord=a=>{const m=/^([A-Z]+)(\d+)$/.exec(a);let col=0;for(const c of m[1])col=col*26+c.charCodeAt(0)-64;return [Number(m[2])-1,col-1];};
 const address=(r,c)=>{let s='';for(c++;c;c=Math.floor((c-1)/26))s=String.fromCharCode(65+(c-1)%26)+s;return s+(r+1);};
 const hidden=new Set(),spans=new Map();for(const range of sheet.merges){const [a,b=a]=range.split(':');const [r,c]=coord(a),[rr,cc]=coord(b);spans.set(a,[rr-r+1,cc-c+1]);for(let y=r;y<=rr;y++)for(let x=c;x<=cc;x++)if(y!==r||x!==c)hidden.add(address(y,x));}
 const slot=sheet.name.trim()==='暖通冷机系统巡检表'?'白2':undefined;const filled=new Map();for(const round of rounds||[])if(!slot||round.shift+round.round===slot)for(const c of round.original_cells)if(c.template===template&&c.sheet===sheet.name)filled.set(c.cell,c.value);
 const fixedSlashes=new Set(sheet.slash_cells);
 const expected=[];
 sheet.rows.forEach((row,r)=>row.forEach((value,c)=>{
  const a=address(r,c);
  if(hidden.has(a))return;
  const format=sheet.excel_styles?.[sheet.style_ids?.[r]?.[c]??0]?.number_format;
  expected.push({address:a,rowspan:spans.get(a)?.[0]||1,colspan:spans.get(a)?.[1]||1,
   text:fixedSlashes.has(a)?'/':inspectionCellText(filled.has(a)?filled.get(a):value,format).trim()});
 }));
 const actual=await page.locator('.book-scroll td').evaluateAll(cells=>cells.map(c=>({address:c.dataset.address,rowspan:c.rowSpan,colspan:c.colSpan,text:c.textContent.trim()})));
 assert.equal(actual.length,expected.length,'原表格数: '+sheet.name);
 for(let i=0;i<actual.length;i++)assert.deepEqual(actual[i],expected[i],sheet.name+' · '+expected[i].address);
 const heights=await page.locator('.book-scroll tbody tr').evaluateAll(rows=>rows.map(r=>r.getBoundingClientRect().height));
 const scale=12/14;
 heights.forEach((h,i)=>assert.ok(Math.abs(h-sheet.row_heights[i]*scale)<1.1,`${sheet.name} 第${i+1}行实际${h}，模板缩放后${sheet.row_heights[i]*scale}`));
 const widths=await page.locator('.book-scroll colgroup col').evaluateAll(cols=>cols.slice(1).map(c=>c.getBoundingClientRect().width));
 widths.forEach((w,i)=>assert.ok(Math.abs(w-sheet.column_widths[i]*scale)<1.1,`${sheet.name} 第${i+1}列实际${w}，模板缩放后${sheet.column_widths[i]*scale}`));
}
try{
 for(const [name,width,height,mobile] of [['phone',402,874,true],['small',360,640,true],['landscape',874,402,true],['desktop',1440,900,false],['legacy',402,874,true]]){
  if(process.env.INSPECTION_VIEWPORT&&name!==process.env.INSPECTION_VIEWPORT)continue;
  const legacy=name==='legacy';
  const context=await browser.newContext({viewport:{width,height},isMobile:mobile,hasTouch:mobile});const page=await context.newPage();page.setDefaultTimeout(15000);
  const f=await setup(page,{slow:true,lose:name==='phone'});
  await page.goto(legacy?origin+'/daily-inspection?frontend=legacy':origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');
  const root=page.locator('.inspection-workbook');
  await root.getByRole('cell',{name:'B3 220',exact:true}).waitFor();
  const readsBeforeSave=f.reads.D;
  const storageKey='inspection-sheet-edits:D-owner-date';
  const first=root.getByRole('cell',{name:'B3 220',exact:true});await first.click();const input=root.getByRole('textbox',{name:'编辑 B3'});await input.fill('226');await input.click();assert.equal(await input.inputValue(),'226','再次点击不能重置输入');
  const draftStarted=page.waitForRequest(r=>new URL(r.url()).pathname==='/api/daily-inspection/draft');
  await input.press('Enter');await draftStarted;
  await root.getByRole('cell',{name:'C3 30',exact:true}).click();await root.getByRole('textbox',{name:'编辑 C3'}).fill('31');await root.getByRole('textbox',{name:'编辑 C3'}).press('Enter');
  const stored=await page.evaluate(key=>localStorage.getItem(key),storageKey);
  assert.ok(stored&&stored.length<1800,'只暂存修改和轮次标识，不复制完整历史表');
  f.release();
  if(name==='phone')await root.getByRole('button',{name:'重试保存',exact:true}).click();
  await page.waitForFunction(key=>localStorage.getItem(key)===null,storageKey);
  assert.equal(f.states.D.active.review_version,3);assert.equal(f.states.E.active.review_version,1);assert.equal(f.reports.D.rounds[1].original_cells[1].value,'31');assert.equal(f.reads.D,readsBeforeSave,'确认保存后不重读整张表');
  assert.equal(f.writes.length,name==='phone'?3:2);
  if(name==='phone')assert.deepEqual(f.writes[0],f.writes[1],'丢失回执后使用原楼栋、轮次和版本重试');
  assert.equal(f.writes.at(-1).body.review_version,2,'连续修改使用已确认的新版本');
  const scroll=root.locator('.book-scroll');await scroll.evaluate(el=>{el.scrollLeft=120;el.scrollTop=180;});const previous=await scroll.evaluate(el=>[el.scrollLeft,el.scrollTop]);
  assert.ok(previous[1]>0,'表格可纵向滚动');
  const otherSheet=page.waitForResponse(r=>r.url().endsWith('/api/daily-inspection/day-sheet')&&r.request().postDataJSON().view_sheet?.[1]==='机房巡检表');
  await chooseSheet(page,/机房巡检表/,legacy);await otherSheet;
  await chooseSheet(page,/配电间巡检表/,legacy);
  await root.getByRole('cell',{name:'B3 226',exact:true}).waitFor();
  assert.deepEqual(await scroll.evaluate(el=>[el.scrollLeft,el.scrollTop]),previous);
  // An older D response must not replace E after a fast building switch.
  const olderRead=f.delayD();
  await chooseSheet(page,/机房巡检表/,legacy);await olderRead;
  await openMenu(page);
  const scope=root.getByRole('combobox',{name:'楼栋',exact:true});
  await scope.selectOption('E');
  await root.getByRole('cell',{name:'B3 380',exact:true}).waitFor();
  f.releaseOld();
  assert.equal(await scope.inputValue(),'E');
  assert.equal(await root.getByText('226',{exact:true}).count(),0);
  await openMenu(page,false);
  await root.getByRole('cell',{name:'B3 380',exact:true}).click();
  await root.getByRole('textbox',{name:'编辑 B3'}).fill('381');
  await root.getByRole('textbox',{name:'编辑 B3'}).press('Enter');
  await page.waitForFunction(()=>localStorage.getItem('inspection-sheet-edits:E-owner-date')===null);
  assert.equal(f.writes.at(-1).scope,'E');
  assert.equal(f.reports.E.rounds[1].original_cells[0].value,'381');
  assert.equal(f.reports.D.rounds[1].original_cells[0].value,'226');
  if(mobile){
   if(legacy)await openMenu(page);
   const table=scroll.locator('table');
   // Phones can render the workbook rotated, so measure its layout width before the transform.
   const tableWidth=()=>table.evaluate(el=>el.offsetWidth);
   const original=await tableWidth();
   await root.getByRole('button',{name:legacy?'放大巡检表':'放大工作表',exact:true}).click();
   assert.ok(await tableWidth()>original,'放大后表格应变宽');
   await root.getByRole('button',{name:legacy?'恢复紧凑显示':'缩小工作表',exact:true}).click();
   await openMenu(page);
   const heightWithControls=await scroll.evaluate(el=>el.clientHeight);
   await openMenu(page,false);
   assert.equal(await tableWidth(),original);
   assert.ok(await scroll.evaluate(el=>{
    const workbook=el.closest('.inspection-workbook');
    return el.clientHeight>workbook.clientHeight/2;
   }),'收起操作后表格占据工作簿主要区域');
   assert.ok(await scroll.evaluate(el=>el.clientHeight)>heightWithControls,'收起菜单会还原表格可用空间');
   const hit=await scroll.evaluate(el=>{
    const box=el.getBoundingClientRect();
    const center=document.elementFromPoint(box.left+box.width/2,box.top+box.height/2);
    const hierarchy=[];
    if(!el.contains(center))for(let node=el;node;node=node.parentElement){
     const style=getComputedStyle(node),rect=node.getBoundingClientRect();
     hierarchy.push([node.className,rect.y,rect.height,style.overflow,style.position,style.transform,style.clipPath,node.scrollTop]);
    }
    return {reachesSheet:el.contains(center),element:center?.className,box:box.toJSON(),hierarchy};
   });
   if(!hit.reachesSheet)await page.screenshot({path:path.join(output,'audit-'+name+'-failure.png')});
   assert.ok(hit.reachesSheet,'表格内容不能被父容器裁切 '+JSON.stringify(hit));
   const saves=f.writes.length;
   await pinchSheet(page,100,180);
   await page.waitForFunction(width=>document.querySelector('.book-scroll table').offsetWidth>width*1.5,original);
   const enlarged=await tableWidth();
   await pinchSheet(page,180,100);
   await page.waitForFunction(width=>document.querySelector('.book-scroll table').offsetWidth<width*.75,enlarged);
   assert.equal(f.writes.length,saves,'缩放不修改数据');
   assert.equal(await root.getByRole('textbox',{name:/^编辑 /}).count(),0,'缩放不误触单元格编辑');
  }
  await page.screenshot({path:path.join(output,'audit-'+name+'.png')});assert.deepEqual(f.errors,[]);
  const metrics=await page.evaluate(()=>({overflow:document.documentElement.scrollWidth-innerWidth,tableHeight:document.querySelector('.book-scroll').clientHeight}));assert.equal(metrics.overflow,0);
  console.log(JSON.stringify({name,...metrics,saves:f.writes.length,reads:f.reads,storedBytes:stored.length,errors:f.errors}));await context.close();
 }
 if(!process.env.INSPECTION_VIEWPORT){
 const page=await browser.newPage({viewport:{width:402,height:874}});page.setDefaultTimeout(15000);
 const f=await setup(page,{actual:true});await page.goto(origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');await page.locator('.book-scroll td').first().waitFor();
 const measurements=[];
 const picker=page.getByRole('combobox',{name:'工作表',exact:true});
 async function verifySheets(scope){
  await openMenu(page);
  const count=f.reports[scope].original_templates.flatMap(t=>t.sheets).filter(s=>s.name.trim()!=='封面').length;
  assert.equal(await picker.locator('option').count(),count);
  for(let i=0;i<count;i++){
   await openMenu(page);
   const start=Date.now();await picker.selectOption({index:i});
   await validateGrid(page,f.reports[scope].original_templates,f.reports[scope].rounds);
   measurements.push(Date.now()-start);
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth),0);
  }
 }
 await verifySheets('D');
 await chooseSheet(page,/M2HVDC/);assert.equal(await page.locator('.column-number').last().innerText(),'K');await page.screenshot({path:path.join(output,'audit-D-M2.png')});
 await openMenu(page);
 await page.getByRole('button',{name:'表格设置',exact:true}).click();
 const settings=page.getByRole('dialog',{name:'表格设置',exact:true});
 await settings.getByRole('heading',{name:'免填范围',exact:true}).waitFor();
 await page.screenshot({path:path.join(output,'audit-settings.png')});
 await settings.getByRole('button',{name:'关闭',exact:true}).click();await settings.waitFor({state:'hidden'});
 await openMenu(page);
 await page.getByRole('combobox',{name:'楼栋',exact:true}).selectOption('E');
 await page.locator('.ins-title').getByText(/^E楼/).waitFor();
 await page.locator('.ins-foot').getByText(/工作表已载入/).waitFor();
 await verifySheets('E');
 await page.screenshot({path:path.join(output,'audit-real-template.png')});console.log(JSON.stringify({name:'real-template',switchMs:measurements,errors:f.errors}));assert.deepEqual(f.errors,[]);
 await page.evaluate(()=>localStorage.setItem('clipflow:workspace-night-mode:fixture-person','dark'));await page.reload();await page.locator('.book-scroll td').first().waitFor();await page.screenshot({path:path.join(output,'audit-dark.png')});
 const editorPage=await browser.newPage({viewport:{width:402,height:874},isMobile:true,hasTouch:true});editorPage.setDefaultTimeout(15000);
 const editFixture=await setup(editorPage);await editorPage.goto(origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');
 const book=editorPage.locator('.inspection-workbook');await book.getByRole('cell',{name:'B3 220',exact:true}).click();await book.getByRole('textbox',{name:'编辑 B3'}).fill('228');
 assert.equal(await book.getByRole('textbox',{name:'编辑 B3'}).getAttribute('inputmode'),'decimal');
 await book.getByRole('button',{name:'下一格',exact:true}).click();
 await book.getByRole('textbox',{name:'编辑 C3'}).fill('33');
 await book.getByRole('button',{name:'完成编辑',exact:true}).click();
 await editorPage.waitForFunction(()=>localStorage.getItem('inspection-sheet-edits:D-owner-date')===null);
 const awaitingReport=editFixture.delayD({concurrent:true});await editorPage.reload();await awaitingReport;
 await book.locator('.book-scroll td').first().waitFor();
 await openMenu(editorPage);
 const archive=editorPage.getByRole('button',{name:'确认归档',exact:true});
 assert.equal(await archive.and(editorPage.locator(':enabled')).count(),0,'实时数据未到达前不能归档');
 assert.equal(await book.getByRole('cell',{name:'B3 228',exact:true}).count(),0,'缓存模板不冒充已经读取的轮次数据');
 editFixture.releaseOld();await book.getByRole('cell',{name:'B3 228',exact:true}).waitFor();
 await archive.waitFor();
 console.log('cached template visible before live data: OK');
 assert.equal(editFixture.reports.D.rounds[1].original_cells[0].value,'228');assert.equal(editFixture.reports.D.rounds[1].original_cells[1].value,'33');assert.deepEqual(editFixture.errors,[]);console.log('mobile next-cell and done: OK');
 for(const legacy of [false,true]){
  const statePage=await browser.newPage({viewport:{width:402,height:874},isMobile:true,hasTouch:true});statePage.setDefaultTimeout(15000);const stateFixture=await setup(statePage);
  stateFixture.reports.D.rounds[1].original_cells.push({template:'D.xlsx',sheet:'配电间巡检表',cell:'D3',parameter:'自定义检查项目',value_type:'状态',value:''});
  await statePage.goto(legacy?origin+'/daily-inspection?frontend=legacy':origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');
  await statePage.locator('td[data-address="D3"][tabindex="0"]').click();
  const select=statePage.getByRole('combobox',{name:'编辑 D3',exact:true});
  const saved=statePage.waitForResponse(r=>r.url().includes('/api/daily-inspection/draft')&&r.status()===200);
  await select.selectOption('√');
  await saved;
  assert.equal(stateFixture.reports.D.rounds[1].original_cells.find(c=>c.cell==='D3').value,'√');assert.deepEqual(stateFixture.errors,[]);
  console.log((legacy?'legacy':'v2')+' empty custom state cell saves selection: OK');await statePage.close();
 }
 console.log('PASS: V2 and legacy workbook editing, retries, concurrent scope switching, zoom and original D/E templates. Isolated data only.');
 }
}finally{await browser.close();await new Promise(r=>server.close(r));}
