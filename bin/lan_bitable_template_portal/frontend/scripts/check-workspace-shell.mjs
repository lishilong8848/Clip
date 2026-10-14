import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/workspace-shell');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:900}});
const errors=[];
let botAppearance={};
const user={open_id:'shell-fixture',name:'界面验收管理员',role:'admin',level:'L1',category:'manager',login_method:'password',modules:['accounts','plan_convergence','maintenance','change','repair','polling','adjust','power','signatures','learning','knowledge','links']};
const scopes=['A','B','C','D','E','H','110'].map(value=>({value,label:value==='110'?'110站':value+'楼'}));
await context.route('**/*',route=>{
  const req=route.request(),url=new URL(req.url());
  if(url.origin!==origin)return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  assert.equal(req.method(),'GET','no business writes during layout verification');
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'shell-fixture'}});
  if(url.pathname==='/api/plan-convergence/bootstrap')return ok({is_admin:true,catalog_ready:true,blocks:{items:[{blockId:'1',blockName:'A楼冷机检修屏蔽',status:'1'},{blockId:'2',blockName:'B楼设备维护屏蔽',status:'0'}]}});
  if(url.pathname==='/api/plan-convergence/rulesets')return ok([]);
  if(/^\/api\/plan-convergence\/(maintenance|change)\/records$/.test(url.pathname))return ok([{record_id:'fixture-notice',name:'A楼冷机检修',building:'A楼',status:'开始',check_status:'pending'}]);
  if(url.pathname==='/api/auth/accounts')return ok({items:Array.from({length:18},(_,i)=>({id:'person-'+i,name:'验收人员'+i,employee_no:'TEST'+i,kind:i%2?'external':'vnet',category:'engineer',position:'电气工程师',own_scopes:['A'],scopes:['A'],modules:['maintenance'],level:'L4',role:'user',enabled:true,can_edit:true,can_edit_access:true,can_delete:false,can_set_password:true})),scope_options:scopes,category_options:[{value:'engineer',label:'工程师'}],module_options:[],policy:{level:'L1',modules:user.modules}});
  if(url.pathname==='/api/auth/permissions')return ok({users:[],scope_options:scopes});
  if(url.pathname==='/api/assistant/appearance')return ok(botAppearance);
  return ok({items:[],records:[],users:[],scopes:[],settings:{},stats:{}});
});
const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html';
const shot=async name=>{await page.waitForTimeout(250);await page.screenshot({path:path.join(output,name+'.png'),animations:'disabled'});};
async function shellGeometry(){
  const rects=await page.evaluate(()=>{
    const box=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return {x:r.x,y:r.y,right:r.right,bottom:r.bottom};};
    return {header:box('.v2-header'),side:box('.v2-sidebar'),main:box('.v2-main'),height:innerHeight,overflow:document.documentElement.scrollWidth>innerWidth+1};
  });
  assert.equal(rects.overflow,false,'no page-wide overflow');
  assert(Math.abs(rects.header.bottom-rects.side.y)<=1&&Math.abs(rects.main.y-rects.side.y)<=1,'all regions share the same upper baseline');
  assert(Math.abs(rects.side.right-rects.main.x)<=1,'content never slides underneath navigation');
  assert(Math.abs(rects.main.bottom-rects.height)<=1,'content scroll region ends at viewport');
}
try{
  await page.goto(entry+'?view=my');await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
  await shot('profile');await shellGeometry();
  await page.getByRole('button',{name:'账号与设置',exact:true}).click();await page.getByRole('searchbox',{name:'搜索账号',exact:true}).waitFor();
  await shot('accounts');await shellGeometry();
  const nav=await page.locator('.admin-workspace-tabs').boundingBox(),content=await page.locator('.admin-settings-content').boundingBox();
  assert(nav.y+nav.height<=content.y+1,'admin sections are above the account table, not a second sidebar');
  assert(Math.abs(nav.x-content.x)<=1,'admin sections and account list align');
  const main=page.locator('.v2-main');
  await page.mouse.move(1100,650);await page.mouse.wheel(0,400);await page.waitForFunction(()=>document.querySelector('.v2-main').scrollTop>100);
  await shellGeometry();assert.equal(await page.evaluate(()=>window.scrollY),0,'desktop scrolling never moves the header');
  const before=await main.evaluate(el=>el.scrollTop);
  await page.setViewportSize({width:1366,height:480});await page.waitForTimeout(250);
  await page.mouse.move(100,300);await page.mouse.wheel(0,400);await page.waitForFunction(()=>document.querySelector('.v2-sidebar').scrollTop>0);
  assert.equal(await main.evaluate(el=>el.scrollTop),before,'sidebar scroll does not move the business area');
  await page.locator('.v2-sidebar').evaluate(el=>el.scrollTop=0);
  await page.getByRole('button',{name:'收起导航',exact:true}).click();
  for(let frame=0;frame<6;frame++){await shellGeometry();await page.waitForTimeout(35);}
  await page.getByRole('button',{name:'展开导航',exact:true}).click();
  for(let frame=0;frame<6;frame++){await shellGeometry();await page.waitForTimeout(35);}
  for(const width of [360,390,430]){
    await page.setViewportSize({width,height:844});await shot('accounts-'+width);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'mobile table scroll remains inside the table');
  }
  await page.goto(entry+'?view=my');await page.getByRole('heading',{name:user.name,exact:true}).waitFor();await shot('profile-mobile');
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await page.setViewportSize({width:1440,height:900});
  await page.goto(entry+'?__page=/plan-convergence');await page.getByText('A楼冷机检修屏蔽',{exact:true}).waitFor();
  await shot('plan-review');await shellGeometry();
  const filters=page.locator('#workspace-page-filters');
  await page.getByRole('button',{name:'检修核对',exact:true}).click();await page.getByText('A楼冷机检修',{exact:true}).waitFor();
  await page.waitForFunction(()=>document.querySelector('.pc-tabs [aria-current=page]')?.textContent.includes('检修核对'));
  await shot('plan-maintenance');
  assert.equal(await filters.getByRole('searchbox').count(),1,'only the active tab search belongs in the shared toolbar');
  assert.equal(await filters.getByRole('searchbox',{name:'搜索屏蔽记录'}).count(),0,'hidden review filters must not escape through Teleport');
  await filters.getByRole('searchbox').fill('无匹配项');await page.getByText('暂无匹配的检修记录',{exact:true}).waitFor();
  await page.getByRole('button',{name:'变更核对',exact:true}).click();await page.getByRole('searchbox',{name:'搜索变更记录'}).waitFor();
  assert.equal(await filters.getByRole('searchbox').count(),1);
  for(const width of [360,390,430,1024,1366,1920]){
    await page.setViewportSize({width,height:width<761?844:900});await shot('plan-'+width);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    const searchBox=await filters.getByRole('searchbox').boundingBox(),filterBox=await filters.boundingBox();
    assert(searchBox.height>=35&&searchBox.x>=filterBox.x&&searchBox.x+searchBox.width<=filterBox.x+filterBox.width,'search is styled and contained');
  }
  await page.getByRole('button',{name:'核对台',exact:true}).click();
  await filters.getByRole('searchbox',{name:'搜索屏蔽记录'}).fill('B楼');
  assert.equal(await page.locator('.pc-record').count(),1,'review search still filters original records');
  await filters.getByRole('searchbox').fill('');
  await page.getByRole('button',{name:'规则配置',exact:true}).click();
  assert.equal(await filters.getByRole('searchbox').count(),0,'review controls are not shown on rules');
  await page.setViewportSize({width:1440,height:900});
  user.modules.push('assistant');await page.goto(entry+'?view=my');
  await page.locator('.lighthouse-bot .weather-lighthouse').first().waitFor();
  assert(await page.locator('.lighthouse-bot').first().getAttribute('title'),'new-account default is the weather lighthouse');
  botAppearance={shape:'nuage'};await page.reload();
  await page.waitForFunction(()=>JSON.parse(localStorage.getItem('lighthouse_appearance:shell-fixture')||'{}').shape==='nuage');
  assert.equal(await page.locator('.lighthouse-bot .weather-lighthouse:visible').count(),0,'explicit personal shapes are not overwritten');
  assert.equal(await main.evaluate(el=>getComputedStyle(el).scrollbarWidth),'thin');
  await page.emulateMedia({forcedColors:'active'});
  assert.equal(await main.evaluate(el=>getComputedStyle(el).scrollbarWidth),'auto','high-contrast mode keeps native scrollbars');
  assert.deepEqual(errors,[]);
  console.log('PASS: shared header/sidebar geometry during collapse, independent wheel scrolling, account layout, current-tab plan search, responsive widths, default lighthouse/personal shape preservation and accessible scrollbars. All requests isolated.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
