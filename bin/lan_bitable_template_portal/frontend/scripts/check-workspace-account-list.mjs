import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

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
let listedItems=items,accountReads=0;
let actor={open_id:'account-list-test',name:'隔离验收管理员',level:'L0',role:'admin',category:'manager',modules:['accounts'],can_manage_accounts:true};
const layouts=layoutApiFixture();
await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.join(root,'public/assets/vnet-logo.png')});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(await layouts.handle(route,'account-list-test'))return;
  if(request.method()!=='GET'){writes.push(url.pathname);return route.abort();}
  const ok=data=>route.fulfill({json:{ok:true,data}});
  const allowedScopes=actor.level==='L3'?scopes.filter(scope=>scope.value==='A'):scopes;
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:actor,scope_options:allowedScopes});
  if(url.pathname==='/api/auth/health'||url.pathname==='/api/health')return ok({service:'clipflow_backend',instance_id:'isolated-account-list'});
  if(url.pathname==='/api/auth/accounts'){accountReads++;return ok({items:listedItems,category_options:categories,scope_options:allowedScopes,level_options:[],module_options:[],policy:{level:actor.level,scope_rules:allowedScopes.map(s=>s.value),modules:['accounts']}});}
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
  await page.setViewportSize({width:1440,height:900});await levels.selectOption('');
  await viewport.evaluate(el=>{el.style.removeProperty('height');el.style.removeProperty('max-height');});
  await page.getByRole('tab',{name:'树形分组',exact:true}).click();
  const tree=page.getByRole('tree',{name:'账号多层分组树'});await tree.waitFor();
  const group=(type,depth)=>tree.locator(`[data-account-group="${type}"][data-group-depth="${depth}"]:visible`);
  const waitPeople=count=>page.waitForFunction(count=>document.querySelectorAll('.v2-account-native-tree [data-account-person]').length===count,count);
  assert.equal(await group('building',1).count(),6,'building-first groups by actual affiliation, not every authorized building');
  assert.equal(await group('category',3).count(),11,'default tree exposes building, level and role groups');
  const firstBuilding=group('building',1).first();
  await firstBuilding.click();assert.equal(await firstBuilding.getAttribute('aria-expanded'),'false');
  await page.waitForFunction(()=>document.querySelectorAll('.v2-account-native-tree [data-account-group="level"][data-group-depth="2"]').length===6);
  assert.equal(await group('level',2).count(),6,'collapsing one building does not collapse other branches');
  await firstBuilding.click();assert.equal(await firstBuilding.getAttribute('aria-expanded'),'true');
  await page.getByRole('button',{name:'展开全部',exact:true}).click();
  await waitPeople(44);
  assert.equal(await tree.locator('[data-account-person]:visible').count(),44,'all filtered accounts appear without table pagination');
  assert.equal(await tree.locator('[data-account-person="fixture-经理甲"]:visible').count(),1,'H manager is not duplicated under every permitted building');
  await page.mouse.move(10,10);await page.waitForTimeout(350);
  await page.screenshot({path:path.join(output,'tree-building-pc.png')});
  await page.locator('.v2-account-tree-toolbar').getByText('按级别',{exact:true}).click();
  assert(await page.getByRole('radio',{name:'按级别',exact:true}).isChecked());
  assert.equal(await group('level',1).count(),4,'level-first groups preserve effective permission overrides');
  assert.equal(await group('building',2).count(),8);
  async function setDepth(label,value){
    await page.getByRole('combobox',{name:'分组层级',exact:true}).click();
    await page.locator('.n-base-select-option').getByText(label,{exact:true}).click();
    assert.equal(await page.locator('.v2-account-tree-wrap').getAttribute('data-group-depth'),String(value));
  }
  await setDepth('级别 → 人员',1);
  await waitPeople(44);
  assert.equal(await group('building',2).count(),0);
  assert.equal(await tree.locator('[data-account-person]:visible').count(),44);
  await setDepth('级别 → 楼栋 → 人员',2);
  await waitPeople(44);
  assert.equal(await group('building',2).count(),8);
  assert.equal(await tree.locator('[data-account-person]:visible').count(),44);
  await setDepth('级别 → 楼栋 → 岗位 → 人员',3);
  await page.getByRole('button',{name:'收起全部',exact:true}).click();
  await waitPeople(0);
  await page.waitForFunction(()=>document.querySelectorAll('.v2-account-native-tree [data-account-group="building"][data-group-depth="2"]').length===0);
  assert.equal(await tree.locator('[data-account-person]:visible').count(),0);
  assert.equal(await group('building',2).count(),0);
  await tree.focus();await page.keyboard.press('ArrowDown');await page.keyboard.press('ArrowRight');
  await page.waitForTimeout(100);
  assert.equal(await tree.locator('.n-tree-node--pending').getAttribute('aria-expanded'),'true','keyboard expands the focused tree branch');
  await page.getByRole('button',{name:'展开全部',exact:true}).click();
  await waitPeople(44);
  await tree.locator('[data-account-person="fixture-工程师甲"]').click();
  const editor=page.getByRole('dialog',{name:'工程师甲',exact:true});await editor.waitFor();
  assert.equal(await editor.getByRole('button',{name:'保存启用状态',exact:true}).isEnabled(),false,'tree selection retains backend read-only restrictions');
  assert.equal(await editor.getByRole('button',{name:'保存权限',exact:true}).count(),0,'tree does not grant access editing');
  await editor.getByRole('button',{name:'关闭账号详情',exact:true}).click();
  for(const width of [390,360]){
    await page.setViewportSize({width,height:844});
    await page.locator('.v2-account-tree-toolbar').getByText('按楼栋',{exact:true}).click();
    await page.getByRole('button',{name:'展开全部',exact:true}).click();
    await waitPeople(44);
    assert.equal(await tree.locator('[data-account-person]:visible').count(),44);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'deep tree and toolbar fit mobile viewport');
    await page.locator('.v2-account-tree-toolbar').scrollIntoViewIfNeeded();
    await page.mouse.move(1,1);await page.waitForTimeout(350);
    await page.screenshot({path:path.join(output,`tree-building-mobile-${width}.png`)});
    await page.getByRole('searchbox',{name:'搜索账号'}).fill('D楼主管');
    assert.equal(await group('building',1).count(),1,'existing search filters every hierarchy level');
    await page.getByRole('button',{name:'展开全部',exact:true}).click();
    await waitPeople(1);
    assert.equal(await tree.locator('[data-account-person]:visible').count(),1);
    await page.getByRole('searchbox',{name:'搜索账号'}).fill('');
  }
  await page.setViewportSize({width:1440,height:900});
  await page.locator('.v2-account-tree-toolbar').getByText('按级别',{exact:true}).click();
  assert(await page.getByRole('radio',{name:'按级别',exact:true}).isChecked());
  await page.mouse.move(10,10);await page.waitForTimeout(350);
  await page.screenshot({path:path.join(output,'tree-level-pc.png')});
  const picker=page.locator('.v2-settings-picker');assert(await picker.locator('[data-v2-layout-tools="accounts"] button').isVisible());
  await picker.getByRole('button',{name:'高级诊断',exact:true}).click();await page.waitForFunction(()=>new URL(location.href).searchParams.get('tab')==='pressure');
  await page.getByRole('button',{name:'返回个人页面',exact:true}).click();await page.waitForFunction(()=>new URL(location.href).searchParams.get('view')==='my');

  const relation=(name,level,category,home='H',extra={})=>({...fixture(name,level,category,home),modules:level==='L4'?['learning']:['accounts'],...extra});
  const relations=[relation('当前管理员','L0','manager','H',{open_id:actor.open_id,is_self:true,role:'admin'}),
    relation('跨楼经理','L1','manager'),relation('H工程师','L2','engineer'),
    relation('A主管','L3','supervisor','A'),relation('B主管','L3','supervisor','B'),
    relation('A工程师','L4','engineer','A'),relation('B工程师','L4','engineer','B'),
    relation('A值班员','L4','operator','A'),relation('A值班账号','L4','duty','A',{kind:'duty'}),
    relation('停用主管','L3','supervisor','A',{enabled:false,modules:[]}),
    relation('锁定工程师','L4','engineer','A',{locked:true}),relation('受阻工程师','L4','engineer','B',{blocked_reason:'隔离测试：受阻'})];
  const person=(name,depth=0)=>tree.locator(`[data-account-person="fixture-${name}"][data-management-depth="${depth}"]`);
  async function showTree(){
    await page.goto(entry);await page.getByRole('tab',{name:'树形分组',exact:true}).click();
    await page.getByRole('button',{name:'展开全部',exact:true}).click();
    await person(listedItems[0].name).waitFor();
  }
  async function openRange(name,depth){
    const row=person(name,depth);await row.click();
    await page.waitForFunction(({name,depth})=>document.querySelector(`[data-account-person="fixture-${name}"][data-management-depth="${depth}"]`)?.getAttribute('aria-expanded')==='true',{name,depth});
    await page.getByRole('button',{name:'展开全部',exact:true}).click();
    await tree.locator(`[data-management-depth="${depth+1}"]`).first().waitFor();
  }
  listedItems=relations;
  await showTree();
  await setDepth('楼栋 → 人员',1);await page.waitForTimeout(250);
  assert.equal(await tree.locator('[data-management-depth="0"]').count(),12);
  assert.equal(await tree.locator('[data-management-depth="1"]').count(),0,'a shallow grouping must not eagerly expand every manager');
  await setDepth('楼栋 → 级别 → 岗位 → 人员',3);
  await page.getByRole('button',{name:'展开全部',exact:true}).click();
  await person('当前管理员',0).waitFor();
  const readsBeforeExpand=accountReads;
  assert.equal(await tree.locator('[data-management-depth="0"]').count(),12);
  await openRange('当前管理员',0);
  await openRange('跨楼经理',1);
  await openRange('A主管',2);
  const deepest=tree.locator('[data-management-depth="3"]');
  assert.deepEqual((await deepest.evaluateAll(rows=>rows.map(row=>row.dataset.accountPerson))).sort(),
    ['A工程师','A值班员','A值班账号','锁定工程师'].map(name=>'fixture-'+name).sort(),'nested supervisors never expose another building');
  assert.equal(accountReads,readsBeforeExpand,'expanding a person never requests a wider directory or impersonates them');
  assert.equal(await person('当前管理员',1).count(),0,'management paths never contain their own manager');
  await person('A工程师',3).getByRole('button',{name:'查看A工程师的详细信息',exact:true}).click();
  let details=page.getByRole('dialog',{name:'A工程师',exact:true});await details.waitFor();
  assert(await details.getByRole('button',{name:'保存启用状态',exact:true}).isDisabled(),'descendant details keep current-login edit restrictions');
  assert.equal(await details.getByRole('button',{name:'保存权限',exact:true}).count(),0);
  await details.getByRole('button',{name:'关闭账号详情',exact:true}).click();
  await page.emulateMedia({reducedMotion:'no-preference'});
  const managerDetails=person('跨楼经理',1).getByRole('button',{name:'查看跨楼经理的详细信息',exact:true});
  await managerDetails.focus();await page.keyboard.press('Enter');
  details=page.getByRole('dialog',{name:'跨楼经理',exact:true});await details.waitFor();
  await details.getByRole('button',{name:'关闭账号详情',exact:true}).click();
  assert.equal(await person('跨楼经理',1).getAttribute('aria-expanded'),'true','viewing details never collapses the management range');
  assert.equal(await managerDetails.getAttribute('data-v2-action'),null,'opening details is navigation, not a submission animation');
  await page.emulateMedia({reducedMotion:'reduce'});
  await person('A主管',2).scrollIntoViewIfNeeded();await page.mouse.move(10,10);await page.waitForTimeout(350);
  await page.screenshot({path:path.join(output,'management-tree-pc.png')});
  for(const width of [390,360]){
    await page.setViewportSize({width,height:width===360?640:844});
    await person('A工程师',3).scrollIntoViewIfNeeded();await page.mouse.move(1,1);await page.waitForTimeout(200);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'four management levels fit mobile without page overflow');
    const bounds=await person('A工程师',3).getByRole('button',{name:'查看A工程师的详细信息',exact:true}).boundingBox();
    assert(bounds.width>=44&&bounds.height>=44,'mobile details button retains a 44px touch target');
    await page.screenshot({path:path.join(output,`management-tree-mobile-${width}.png`)});
    await person('A工程师',3).click();details=page.getByRole('dialog',{name:'A工程师',exact:true});await details.waitFor();
    await details.getByRole('button',{name:'关闭账号详情',exact:true}).click();
  }
  await page.getByRole('searchbox',{name:'搜索账号'}).fill('A主管');
  await page.getByRole('button',{name:'展开全部',exact:true}).click();
  await openRange('A主管',0);
  assert.equal(await tree.locator('[data-management-depth="0"]').count(),1,'search scopes roots only');
  assert.equal(await tree.locator('[data-management-depth="1"]').count(),4,'root search preserves its permitted personnel');

  actor={...actor,open_id:'ou_fixture_A主管',name:'A主管',level:'L3',role:'building',category:'supervisor'};
  listedItems=relations.filter(row=>['A主管','A工程师','A值班员','A值班账号','锁定工程师'].includes(row.name));
  await page.setViewportSize({width:1440,height:900});await showTree();await openRange('A主管',0);
  assert.equal(await tree.locator('[data-account-person="fixture-B工程师"],[data-account-person="fixture-跨楼经理"]').count(),0,'single-building viewer cannot traverse to hidden managers or personnel');
  await person('A主管',0).getByRole('button',{name:'查看A主管的详细信息',exact:true}).click();
  details=page.getByRole('dialog',{name:'A主管',exact:true});await details.waitFor();
  assert.equal(await details.getByRole('button',{name:'保存权限',exact:true}).count(),0,'self details never grant self-authorization');
  await details.getByRole('button',{name:'关闭账号详情',exact:true}).click();

  actor={...actor,open_id:'ou_fixture_H工程师',name:'H工程师',level:'L2',category:'engineer'};
  listedItems=relations.filter(row=>['H工程师','A工程师','B工程师'].includes(row.name)).map(row=>{const copy={...row};delete copy.employee_no;return copy;});
  await showTree();await openRange('H工程师',0);
  assert.deepEqual((await tree.locator('[data-management-depth="1"]').evaluateAll(rows=>rows.map(row=>row.dataset.accountPerson))).sort(),['fixture-A工程师','fixture-B工程师'],'L2 range includes only the provided cross-building engineer edit selector');
  assert.equal(await tree.locator('.v2-tree-person-meta').filter({hasText:'TEST'}).count(),0,'redacted fields are never reconstructed');
  assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
  console.log('PASS: account list and grouping, four person-management levels, details, current-login permissions, L2/L3 limits, redaction, search, lazy reads and PC/mobile. No business writes.');
}catch(error){await page.screenshot({path:path.join(output,'failure.png')});throw error;}
finally{await browser.close();await server.close();}
