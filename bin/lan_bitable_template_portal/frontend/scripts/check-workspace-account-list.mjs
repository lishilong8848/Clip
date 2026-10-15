import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

assert(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR,'Use an isolated workspace build.');
const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/account-list');await mkdir(output,{recursive:true});
const server=await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,entry=origin+'/assets/workspace-v2/index.html?__page=/admin/settings';
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
const errors=[],writes=[],scopes=['A','B','C','D','E','H'].map(value=>({value,label:value+'楼'}));
const fixture=(name,level,category,home='H')=>({id:'fixture-'+name,record_id:'fixture-'+name,open_id:'ou_fixture_'+name,name,employee_no:'TEST',kind:'vnet',position:{manager:'运维经理',supervisor:'运维主管',engineer:'专业工程师',operator:'值班员',duty:'固定值班账号'}[category],category,own_scopes:[home],home_scope:home,scopes:level==='L1'?scopes.map(s=>s.value):[home],level,role:level==='L1'?'admin':'building',enabled:true,modules:['learning'],can_edit:false,can_edit_access:false,can_delete:false,can_set_password:false});
const items=[fixture('H楼','L1','duty'),fixture('值班员甲','L1','operator'),fixture('工程师甲','L1','engineer'),fixture('工程师乙','L1','engineer'),fixture('经理甲','L1','manager'),fixture('工程师丙','L1','engineer'),fixture('经理乙','L1','manager'),
  ...['A','B','C','E','D'].map(home=>fixture(home+'楼主管','L3','supervisor',home)),fixture('工程师丁','L2','engineer'),fixture('工程师戊','L2','engineer'),
  ...Array.from({length:30},(_,i)=>fixture('普通人员'+String(i+1).padStart(2,'0'),'L4','operator','A'))];
const categories=['manager','supervisor','engineer','operator','duty'].map(value=>({value,label:{manager:'经理',supervisor:'主管',engineer:'工程师',operator:'值班员',duty:'固定值班账号'}[value]}));
await context.route('**/*',route=>{
  const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.join(root,'public/assets/vnet-logo.png')});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(request.method()!=='GET'){writes.push(url.pathname);return route.abort();}
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'account-list-test',name:'隔离验收管理员',level:'L0',role:'admin',category:'manager',modules:['accounts']},scope_options:scopes});
  if(url.pathname==='/api/auth/health'||url.pathname==='/api/health')return ok({service:'clipflow_backend',instance_id:'isolated-account-list'});
  if(url.pathname==='/api/auth/accounts')return ok({items,category_options:categories,scope_options:scopes,level_options:[],module_options:[],policy:{level:'L0',scope_rules:scopes.map(s=>s.value),modules:['accounts']}});
  return ok({items:[],records:[],users:[],settings:{}});
});
const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',error=>errors.push(error.message));
try{
  await page.goto(entry);await page.getByRole('searchbox',{name:'搜索账号'}).waitFor();
  const levels=page.getByRole('combobox',{name:'筛选权限级别'}),jobs=page.getByRole('combobox',{name:'筛选岗位'});
  async function check(level,expected){
    await levels.selectOption(level);await page.waitForTimeout(200);
    assert.equal(await page.locator('.accounts tbody tr:visible').count(),expected,'every matching '+level+' account is displayed');
    assert.equal(await page.locator('.accounts [data-v2-capacity-hidden],.accounts [data-v2-capacity-column]').count(),0,'no hidden account rows or columns');
    assert.equal(await page.locator('.v2-visible-count').count(),0,'no second pagination in account management');
  }
  await check('L3',5);assert(await page.getByRole('button',{name:'管理D楼主管',exact:true}).isVisible());
  await page.screenshot({path:path.join(output,'all-five-supervisors.png')});
  await check('L1',7);assert(await page.getByRole('button',{name:'管理经理甲',exact:true}).isVisible());assert(await page.getByRole('button',{name:'管理经理乙',exact:true}).isVisible());
  await jobs.selectOption('manager');await page.waitForTimeout(150);assert.equal(await page.locator('.accounts tbody tr:visible').count(),2);
  await jobs.selectOption('');await check('L2',2);
  assert(!await page.getByRole('button',{name:'管理工程师甲',exact:true}).count(),'existing L1 overrides are not reclassified as L2');
  await levels.selectOption('');await page.getByRole('searchbox',{name:'搜索账号'}).fill('D楼主管');
  assert.equal(await page.locator('.accounts tbody tr:visible').count(),1);
  await page.getByRole('searchbox',{name:'搜索账号'}).fill('');await page.waitForTimeout(150);
  assert.equal(await page.locator('.accounts tbody tr:visible').count(),20);
  await page.getByRole('button',{name:'下一页',exact:true}).click();assert.equal(await page.locator('.accounts tbody tr:visible').count(),20);
  await check('L3',5);assert.equal(await page.getByRole('button',{name:'上一页',exact:true}).isEnabled(),false,'filtering resets native pagination');
  const viewport=page.locator('.accounts').locator('xpath=ancestor::*[contains(@class,"v2-responsive-body")][1]');
  await viewport.evaluate(el=>{el.style.height='260px';el.style.maxHeight='260px';});await page.waitForTimeout(200);
  await check('L1',7);assert(await viewport.evaluate(el=>el.scrollHeight>el.clientHeight),'compact panels scroll instead of clipping accounts');
  await page.getByRole('button',{name:'管理经理乙',exact:true}).scrollIntoViewIfNeeded();
  assert(await page.getByRole('button',{name:'管理经理乙',exact:true}).isVisible());
  for(const width of [1366,390,360]){
    await page.setViewportSize({width,height:844});await check('L3',5);await check('L1',7);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'responsive table stays within its scroll region');
  }
  assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
  console.log('PASS: L0 sees all L1/L2/L3 matches, preserved effective levels, search, native pagination, resized panels and mobile. No business writes.');
}finally{await browser.close();await server.close();}
