import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const dist=path.resolve(process.env.INSPECTION_LEGACY_DIR || path.join(root,'dist'));
const output=path.resolve(root,'../../../build_output/inspection-scope-orientation');await mkdir(output,{recursive:true});
const server=createServer(async(req,res)=>{try{let name=new URL(req.url,'http://localhost').pathname;name=name.startsWith('/assets/')?name.slice(1):'index.html';const file=path.resolve(dist,name);if(!file.startsWith(dist+path.sep))throw Error();res.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css'})[path.extname(file)]||'application/octet-stream');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
 for(const legacy of [false,true]){
  const page=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true}),errors=[],starts=[];page.on('pageerror',e=>errors.push(e.message));
  const states={},reports={},values={D:'220',E:'380'},writes=[],preferences={};
  for(const scope of ['D','E']){
   const duty={scope,date:'2026-11-06',shift:scope==='D'?'白':'夜'},template=scope+'.xlsx';
   const round=scope==='D'?1:2;
   const point={id:'point',point_id:'source',enabled:true,template,sheet:'巡检表一',round_cells:{[duty.shift+round]:'B2'}};
   const device={id:scope+'-device',scope,name:scope+'设备',profession:'电气',points:[point],sections:[],parameters:[{id:'p',name:'电压',point:'point',reference:scope==='D'?220:380}]};
   const active={id:scope+'-round',duty,round,pending:false,draft_key:scope+'-draft',reviews:{},review_version:0,batch:{devices:[device],count:1,batch:1}};
   const templates=[{scope,name:template,profession:'电气',sheets:[{name:'封面',rows:[['封面']],merges:[]},{name:'巡检表一',rows:[['项目','数值'],['电压',null]],merges:[]},{name:'巡检表二',rows:[['设备','状态'],['设备',null]],merges:[]}]}];
   states[scope]={template_version:'v1',duty,view_scope:scope,view_scopes:['D','E'],can_edit:true,history_scopes:['D','E'],history:[],completed_rounds:0,round_limit:2,active:scope==='D'?active:null};
   reports[scope]={scope,date:duty.date,draft_key:scope+'-owner-date',template_version:'v1',sheet_groups:{},can_edit_history:true,original_templates:templates,rounds:[]};
   states[scope]._fixtureActive=active;
  }
  function report(scope){const value=structuredClone(reports[scope]);const active=states[scope].active;if(active)value.rounds=[{round_id:active.id,shift:active.duty.shift,round:active.round,version:active.review_version,status:'进行中',original_cells:[{template:scope+'.xlsx',sheet:'巡检表一',cell:'B2',parameter:'电压',value:values[scope]}]}];return value;}
  await page.route(url=>url.pathname.startsWith('/api/'),async route=>{const req=route.request(),url=new URL(req.url()),s=url.searchParams.get('view_scope')||'D',ok=data=>route.fulfill({json:{ok:true,data}});
   if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture',name:'隔离人员',role:'admin',category:'engineer',level:'L0',modules:['daily']},allowed_scopes:['D','E'],scope_options:[{value:'D',label:'D楼'},{value:'E',label:'E楼'}],default_scope:'D'});
   if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'fixture'}});
   if(url.pathname.includes('/inspection-sheets-')){
    const entry=preferences[url.pathname]||{revision:0,value:null};
    if(req.method()==='GET')return ok(entry);
    const {revision,value}=req.postDataJSON();assert.equal(revision,entry.revision);
    return ok(preferences[url.pathname]={revision:revision+1,value});
   }
   if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
   if(url.pathname==='/api/daily-inspection')return ok(states[s]);
   if(url.pathname==='/api/daily-inspection/status')return ok({unchanged:true,history_scopes:['D','E']});
   if(url.pathname==='/api/daily-inspection/source')return ok({scope:s,devices:states[s]._fixtureActive.batch.devices,orders:{},batch:1});
   if(url.pathname==='/api/daily-inspection/start'){starts.push(s);states[s].active=states[s]._fixtureActive;return ok(states[s]);}
   if(url.pathname==='/api/daily-inspection/templates'){const scope=url.searchParams.get('scope');return ok({scope,version:'v1',templates:reports[scope].original_templates});}
   if(url.pathname==='/api/daily-inspection/day-sheet'){const body=req.postDataJSON();const value=report(body.scope);if(body.template_version==='v1')delete value.original_templates;return ok(value);}
   if(url.pathname==='/api/daily-inspection/draft'){const body=req.postDataJSON();assert.equal(body.id,states[s].active.id);assert.equal(body.review_version,states[s].active.review_version);writes.push({scope:s,body});for(const cell of body.sheet_cells)if(cell.cell==='B2')values[s]=cell.value;states[s].active.review_version++;return ok(states[s]);}
   return ok({items:[],revision:0,value:null});
  });
  await page.goto(legacy?origin+'/daily-inspection?frontend=legacy':origin+'/assets/workspace-v2/index.html?__page=%2Fdaily-inspection&scope=D');
  const book=page.locator('.inspection-workbook');try{await book.getByText('220',{exact:true}).waitFor({timeout:15000});}catch(e){console.error('页面状态:',(await page.locator('body').innerText()).slice(0,2000));console.error('页面错误:',errors);await page.screenshot({path:path.join(output,'fixture-error.png')});throw e;}
  assert(await book.evaluate(el=>el.classList.contains('rotate-landscape')),'手机默认横屏，表格和文字共同旋转');
  const tabs=book.getByRole('group',{name:'巡检工作表',exact:true});
  const menu=book.getByRole('button',{name:'巡检菜单'});
  await book.getByText('白1 · 进行中',{exact:true}).waitFor();
  assert.equal(await tabs.getByRole('button',{name:'封面',exact:true}).count(),0);
  assert.equal(await book.getByLabel('切换巡检工作表').count(),0);
  assert.equal(await book.getByRole('group',{name:'巡检轮次与状态'}).count(),0);
  // The submit footer stays visible on phones: a gated button must explain itself instead of vanishing.
  const submit=book.locator('.book-submit');await submit.waitFor({timeout:15000});
  assert.equal(await submit.getByRole('button',{name:'确认提交',exact:true}).isDisabled(),true,'未逐页查看前提交按钮禁用');
  assert.match(await submit.innerText(),/已查看\s*1\s*\/\s*2\s*页/,'显示逐页查看进度');
  assert.match(await submit.getByRole('button',{name:'下一未查看表'}).getAttribute('title'),/巡检表二/,'点名还未查看的工作表');
  await menu.click();await book.getByRole('group',{name:'巡检轮次与状态'}).getByRole('button',{name:/白1/}).click();
  assert.equal(await menu.getAttribute('aria-expanded'),'false');
  await tabs.getByRole('button',{name:'巡检表二',exact:true}).click();
  await page.waitForFunction(()=>{const button=document.querySelector('.book-submit .submit-action');return !!button&&!button.disabled&&button.textContent.trim()==='确认提交';},{},{timeout:15000});
  await book.getByRole('button',{name:'调整工作表顺序'}).click();
  const sort=page.getByRole('dialog');await sort.getByRole('button',{name:'上移'}).nth(1).click();
  await sort.getByRole('button',{name:'保存排序'}).click();await sort.waitFor({state:'hidden'});
  assert.equal(await tabs.getByRole('button').first().getAttribute('aria-label'),'巡检表二');
  const viewedD=await page.evaluate(()=>JSON.parse(localStorage.getItem('inspection-viewed:D-owner-date:D-round')||'[]'));
  assert.equal(viewedD.length,2);
  await menu.click();await book.getByLabel('楼栋',{exact:true}).selectOption('E');
  const start=book.getByRole('button',{name:/开始第.*次巡检/});
  await start.waitFor();
  assert.deepEqual(starts,[],'未点击时不发送开轮请求');
  await start.click();
  await book.getByText('380',{exact:true}).waitFor();assert.deepEqual(starts,['E'],'E楼独立手动开始');
  await book.getByText('夜2 · 进行中',{exact:true}).waitFor();
  assert.equal(await tabs.getByRole('button').first().getAttribute('aria-label'),'巡检表一');
  assert.equal(await book.getByText('220',{exact:true}).count(),0);
  await book.getByRole('button',{name:'切换竖屏'}).click();assert(!await book.evaluate(el=>el.classList.contains('rotate-landscape')));
  await book.getByRole('button',{name:'切换横屏'}).click();assert(await book.evaluate(el=>el.classList.contains('rotate-landscape')));
  if(await book.getByRole('button',{name:'巡检菜单'}).getAttribute('aria-expanded')!=='true')await book.getByRole('button',{name:'巡检菜单'}).click();
  await book.getByRole('button',{name:'表格设置'}).click();
  const dialog=page.getByRole('dialog');await dialog.waitFor();await page.waitForTimeout(250);
  const bounds=await dialog.boundingBox();assert(bounds.x>=-1&&bounds.y>=-1&&bounds.x+bounds.width<=391&&bounds.y+bounds.height<=845,'横屏弹窗完整显示：'+JSON.stringify(bounds));
  await page.screenshot({path:path.join(output,(legacy?'legacy':'v2')+'-settings.png')});
  await dialog.getByRole('button',{name:'完成',exact:true}).click();await dialog.waitFor({state:'hidden'});
  const scroll=book.locator('.book-scroll'),rect=await scroll.boundingBox(),cdp=await page.context().newCDPSession(page);
  const centerX=rect.x+rect.width/2,centerY=rect.y+Math.min(rect.height/2,200),originalWidth=await book.locator('table').evaluate(el=>el.offsetWidth);
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:centerX-30,y:centerY,id:1},{x:centerX+30,y:centerY,id:2}]});
  await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:centerX-55,y:centerY,id:1},{x:centerX+55,y:centerY,id:2}]});
  await page.waitForTimeout(150);await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await cdp.detach();
  assert((await book.locator('table').evaluate(el=>el.offsetWidth))>originalWidth,'横屏双指放大');
  await book.getByRole('button',{name:'恢复巡检表原始大小'}).click();
  await book.getByRole('cell',{name:'B2 380',exact:true}).click();await book.getByRole('textbox',{name:'编辑 B2'}).fill('381');
  await book.getByRole('textbox',{name:'编辑 B2'}).press('Enter');await page.waitForFunction(()=>!localStorage.getItem('inspection-sheet-edits:E-owner-date'));
  assert.equal(values.E,'381');assert.equal(values.D,'220');assert.equal(writes.length,1);
  await book.getByLabel('楼栋',{exact:true}).selectOption('D');
  await tabs.getByRole('button',{name:'巡检表一',exact:true}).click();await book.getByText('220',{exact:true}).waitFor();
  assert.equal(await menu.getAttribute('aria-expanded'),'false');
  assert.equal(await tabs.getByRole('button').first().getAttribute('aria-label'),'巡检表二');
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('inspection-viewed:D-owner-date:D-round')||'[]').length),2);
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('inspection-viewed:E-owner-date:E-round')||'[]').length),1);
  await page.screenshot({path:path.join(output,(legacy?'legacy':'v2')+'-landscape.png')});assert.deepEqual(errors,[]);
  console.log((legacy?'旧版':'新版')+'：横竖屏、楼栋切换、独立进度和E楼开始通过');await page.close();
 }
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
