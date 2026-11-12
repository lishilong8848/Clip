import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {chromium} from 'playwright';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const point=(id,cell,enabled=true)=>({id,point_id:id,enabled,template:'D.xlsx',sheet:'原表',round_cells:{白1:cell}});
const active={id:'round1',duty:{scope:'D',date:'2026-11-06',shift:'白'},round:1,pending:false,review_version:0,manual_sheet_cells:[],reviews:{},batch:{
  devices:[{id:'device',points:[point('p1','B2'),point('p2','C2')],sections:[{location:'D-178',form:'原表'}],parameters:[{id:'p:p1',point:'p1'},{id:'p:p2',point:'p2'},{id:'s:0:未配置',section:0}]}],
  form_devices:[{id:'supplement',points:[point('original-supplement','F2')],sections:[],parameters:[{id:'p:original-supplement',point:'original-supplement'}]}]}};
const template={scope:'D',name:'D.xlsx',profession:'电气',sheets:[{name:'原表',rows:[['名称','已配置','已配置空值','未配置','手工格','状态'],['本次',null,null,null,null,null],['历史',null,null,null,null,null]],merges:[]}]};
const report={scope:'D',date:'2026-11-06',draft_key:'test-only',can_edit_history:true,template_version:'test',original_templates:[template],rounds:[
  {round_id:'round1',shift:'白',round:1,version:0,status:'进行中',original_cells:[
    {template:'D.xlsx',sheet:'原表',cell:'B2',value:'230',device:'device',kind:'point',point_id:'p1',parameter:'电压'},
    {template:'D.xlsx',sheet:'原表',cell:'D2',value:'42',device:'device',kind:'section',location:'D-178',form:'原表',parameter:'未配置'},
    {template:'D.xlsx',sheet:'原表',cell:'F2',value:'√',device:'supplement',kind:'point',point_id:'original-supplement',parameter:'运行状态',value_type:'状态'}]},
  {id:'old',round_id:'old',shift:'白',round:2,version:'v1',status:'已归档',original_cells:[{template:'D.xlsx',sheet:'原表',cell:'B3',value:'210'}]}]};
const dist=path.resolve(process.env.INSPECTION_LEGACY_DIR || path.join(root,'dist'));
const server=createServer(async(request,response)=>{
 const url=new URL(request.url,'http://localhost');
 const name=url.pathname.startsWith('/assets/')?url.pathname.slice(1):'index.html';
 const file=path.resolve(dist,name);
 try{
  if(!file.startsWith(dist+path.sep))throw new Error('Invalid asset');
  response.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css'})[path.extname(file)]||'application/octet-stream');
  response.end(await readFile(file));
 }catch{response.writeHead(404);response.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const entry=origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D';
const state=()=>({duty:active.duty,active,view_scope:'D',history_scopes:['D'],history:[],round_limit:2,can_edit:true,template_version:'test'});
const browser=await chromium.launch({headless:true});
try{
 for(const [width,height] of [[402,874],[1440,900]]){
  active.manual_sheet_cells=[];active.review_version=0;report.rounds[0].version=0;report.rounds[0].original_cells[1].value='42';
  const context=await browser.newContext({viewport:{width,height},serviceWorkers:'block'}),page=await context.newPage(),errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.route(url=>url.pathname.startsWith('/api/'),async route=>{const req=route.request(),url=new URL(req.url());let data;
   if(url.pathname==='/api/auth/status')data={logged_in:true,user:{open_id:'fixture-cells',name:'测试人员',role:'admin',level:'L0',modules:['daily']},allowed_scopes:['D'],scope_options:[{value:'D',label:'D楼'}],default_scope:'D'};
   else if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'cell-fixture'}});
   else if(url.pathname==='/api/daily-inspection'||url.pathname==='/api/daily-inspection/status')data=state();
   else if(url.pathname==='/api/daily-inspection/templates')data={scope:'D',version:'test',templates:[template]};
   else if(url.pathname==='/api/daily-inspection/view-sheet')data={visited_sheets:req.postDataJSON().visited_sheets,presence:[]};
   else if(url.pathname==='/api/daily-inspection/day-sheet')data=structuredClone(report);
   else if(url.pathname==='/api/daily-inspection/draft'){
    for(const edit of req.postDataJSON().sheet_cells){const reading=report.rounds[0].original_cells.find(item=>item.cell===edit.cell);if(reading)reading.value=edit.value;else(report.rounds[0].extra_cells||=[]).push(edit);active.manual_sheet_cells.push({template:edit.template,sheet:edit.sheet,cell:edit.cell});}
    active.review_version++;report.rounds[0].version=active.review_version;data=state();
   }else if(url.pathname.startsWith('/api/workspace-layouts/'))data={revision:0,value:null};
   else data={items:[]};
   await route.fulfill({json:{ok:true,data}});
  });
  await page.goto(entry);await page.locator('td[data-address="B2"]').waitFor();
  const cell=address=>page.locator(`td[data-address="${address}"]`);
  await page.waitForFunction(()=>document.querySelector('td[data-address="B2"]')?.textContent?.trim()==='230');
  assert.equal((await cell('C2').innerText()).trim(),'待填写');
  assert.equal((await cell('D2').innerText()).trim(),'');assert.equal((await cell('F2').innerText()).trim(),'');
  assert.equal((await cell('B3').innerText()).trim(),'210');
  await cell('D2').click();await page.getByRole('textbox',{name:'编辑 D2'}).fill('77');await page.getByRole('textbox',{name:'编辑 D2'}).press('Enter');
  await page.waitForResponse(response=>response.url().includes('/api/daily-inspection/draft')&&response.ok());
  await page.waitForFunction(()=>document.querySelector('td[data-address="D2"]')?.textContent?.trim()==='77');
  await page.reload();await page.waitForFunction(()=>document.querySelector('td[data-address="D2"]')?.textContent?.trim()==='77');
  await cell('F2').click();const select=page.getByRole('combobox',{name:'编辑 F2'});await select.waitFor();
  assert.equal((await select.locator('option').first().innerText()).trim(),'空白');await select.selectOption('√');
  await page.waitForResponse(response=>response.url().includes('/api/daily-inspection/draft')&&response.ok());
  await page.waitForFunction(()=>document.querySelector('td[data-address="F2"]')?.textContent?.trim()==='√');
  assert.deepEqual(errors,[]);await context.close();console.log(`inspection sheet ${width}x${height}: OK`);
 }
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
