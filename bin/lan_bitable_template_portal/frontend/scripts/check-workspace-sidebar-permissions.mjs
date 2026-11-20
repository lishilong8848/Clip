import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/sidebar-permissions');
const tabs = ['permissions','handover','mop','feishu','dingtalk','status','development','pressure'].map(k => 'settings:'+k);
const modules = ['maintenance','change','repair','polling','adjust','power','event','engineer_mop','repair_management',
  'critical_guard','risk_inspection','daily','cabinet','water','plan_convergence','drills','learning','knowledge',
  'links','life_guide','handover','work_handover','signatures','accounts','assistant'];
const all = ['daily','daily_inspection','cold_source','water','work_handover','notices','plan_convergence',
  'repair_management','engineer_mop','sops','eop_scenarios','event','drills','risk_inspection','critical_guard',
  'cabinet','tool_management','learning','life_guide','knowledge','documents','links','signatures','zhihang_data',...tabs];
const allSections = ['operations','notices','repairs','maintenance','safety','resources','learning','administration'];
const scenarios = [
  {id:'global',level:'L0',role:'admin',grants:modules,scopes:['ALL','A','D','E'],tool:true, entries:all, order:allSections},
  {id:'public-admin',level:'L0',role:'admin',grants:modules,scopes:['ALL'],tool:true,diagnostics:false,
    entries:all.filter(id=>!['settings:status','settings:pressure'].includes(id)),order:allSections},
  {id:'h-duty-admin',category:'duty',level:'L1',role:'admin',grants:modules,scopes:['ALL','H'],tool:true,entries:all,order:allSections},
  {id:'manager',category:'manager',level:'L1',grants:['event','critical_guard','repair_management','learning'],scopes:['ALL'],
    entries:['event','notices','critical_guard','repair_management','learning','signatures'],order:['safety','repairs','notices','learning','administration']},
  {id:'supervisor',category:'supervisor',level:'L3',grants:['event','handover','cabinet','repair_management','risk_inspection','accounts'],scopes:['A'],
    entries:['event','notices','cabinet','repair_management','risk_inspection','settings:permissions','signatures'],
    order:['safety','repairs','resources','notices','administration']},
  {id:'engineer',category:'engineer',level:'L2',grants:['repair_management','change','engineer_mop','risk_inspection'],scopes:['B'],
    entries:['notices','repair_management','engineer_mop','risk_inspection','signatures'],order:['repairs','maintenance','safety','notices','administration']},
  {id:'specialist',category:'engineer',level:'L2',grants:['repair_management','drills','event','polling'],scopes:['A','E'],
    entries:['repair_management','drills','eop_scenarios','event','notices','sops','signatures'],order:['repairs','maintenance','safety','notices','administration']},
  {id:'admin-office',category:'administrator',level:'L3',grants:['accounts','daily','risk_inspection'],tool:true,scopes:['A'],
    entries:['settings:permissions','signatures','daily','cold_source','risk_inspection','tool_management'],
    order:['resources','administration','operations','safety']},
  ...['duty','shift_leader','operator'].map(category => ({id:category,category,grants:['daily','water','repair','drills','power'],scopes:['E'],
    entries:['daily','daily_inspection','cold_source','water','notices','drills','eop_scenarios','signatures'],order:['operations','notices','safety','maintenance','administration']})),
  {id:'d-history',grants:[],scopes:['D'],entries:['daily_inspection','signatures'],order:['operations','administration']},
  {id:'event-only',grants:['event'],scopes:['A'],entries:['event','notices','signatures'],order:['notices','safety','administration']},
  {id:'no-modules',grants:[],scopes:['A'],entries:['signatures'],order:['administration']},
  {id:'tool-only',grants:[],tool:true,scopes:['C'],entries:['tool_management','signatures'],order:['resources','administration']},
  {id:'impersonated',grants:['learning','knowledge','signatures','accounts','daily','water'],scopes:['D'],impersonated:true,tool:true,
    entries:['daily','water','learning'],order:['operations','learning']},
  {id:'guest',role:'guest',grants:['cabinet'],scopes:['A'],entries:[],order:[]},
];
const app = await preview({root,configFile:false,logLevel:'error',build:{outDir:process.env.RESPONSIVE_BUILD_DIR||'dist'},preview:{host:'127.0.0.1',port:0},
  plugins:[{name:'isolated-rail-accounts',configurePreviewServer(server){
    server.middlewares.use('/api',(req,res)=>{
      if(req.method !== 'GET'){res.statusCode=405;res.end();return;}
      const account = scenarios.find(s=>req.headers.cookie?.split('; ').includes('profile='+s.id)) || scenarios[0];
      const user = {open_id:'fixture-'+account.id,name:'隔离测试',enabled:true, role:account.role||'building',
        level:account.level||'L4', category:account.category||'member', home_scope:account.scopes[0],
        scope_rules:account.scopes, modules:account.grants,tool_management_enabled:!!account.tool,
        ...(account.impersonated?{impersonation:{id:'fixture',active:true,expires_at:Date.now()+600000,operator_name:'操作员',target_name:'隔离测试'}}:{})};
      const pathname = req.url.split('?')[0];
      const data = pathname === '/handover-links' ? {scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'},{value:'D',label:'D楼'}],
        links:{A:'/invalid',B:new URL('/fixture/review/B',origin).href,D:''}} : pathname === '/auth/status' ? {logged_in:true,user,backend_diagnostics_available:account.diagnostics,
        scope_options:account.scopes.map(value=>({value,label:value})),default_scope:account.scopes[0]} : {items:[],total:0,records:[],scopes:[]};
      res.setHeader('Content-Type','application/json; charset=utf-8');
      res.end(JSON.stringify(pathname==='/health'?{ok:true,service:'clipflow_backend',instance_id:'fixture'}:{ok:true,data}));
    });
  }}]});
const origin = 'http://127.0.0.1:'+app.httpServer.address().port;
const entry = origin+'/assets/workspace-v2/index.html?view=functions';
if(process.argv.includes('--preview')){
  console.log(entry);
} else {
  await mkdir(output,{recursive:true});
  const browser = await chromium.launch();
  try {
    for(const account of scenarios){
      const context = await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
      await context.addCookies([{url:origin,name:'profile',value:account.id}]);
      await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
      const page = await context.newPage();
      try {
        await page.goto(entry);
        const rail = page.getByRole('navigation',{name:'功能导航'}), bottom = page.getByRole('navigation',{name:'手机导航'});
        if(account.role==='guest'){
          await page.waitForURL(url=>url.searchParams.get('__page')==='/cabinet-power');
          assert.equal(await rail.count(),0);
          await page.setViewportSize({width:390,height:844});assert.equal(await bottom.count(),0);
          continue;
        }
        const directory = page.locator('.v2-functions');
        await directory.getByRole('heading',{name:'全部功能'}).waitFor();
        const ids = locator=>locator.locator('[data-nav-entry]').evaluateAll(nodes=>nodes.map(n=>n.dataset.navEntry).sort());
        assert.deepEqual(await ids(rail),['home',...account.entries].sort(),account.id+' rail IDs');
        assert.deepEqual(await rail.locator('[data-group]').evaluateAll(nodes=>nodes.map(n=>n.dataset.group)),['home',...account.order],account.id+' role order');
        if(account.id==='duty'||account.id==='admin-office'){
          const parent = rail.locator('[data-group="'+(account.id==='duty'?'operations':'administration')+'"]');
          await parent.click();
          assert.equal(await parent.getAttribute('aria-expanded'),'true');
          assert.equal(await rail.getByRole('button',{name:account.id==='duty'?'当班任务':'人员与权限',exact:true}).getAttribute('aria-expanded'),'true');
        }
        if(account.id==='global'){
          const operations=rail.getByRole('button',{name:'运行值守',exact:true});
          const safety=rail.getByRole('button',{name:'应急保障',exact:true});
          await operations.click();
          await page.keyboard.press('Tab');
          assert(await rail.getByRole('button',{name:'当班任务',exact:true}).evaluate(el=>el===document.activeElement));
          await page.keyboard.press('Enter');
          assert(await rail.locator('[data-nav-entry="daily"]').isVisible());
          await safety.click();
          const menu=rail.getByRole('region',{name:'应急保障',exact:true});
          assert((await menu.boundingBox()).width<=205);
          await rail.getByRole('button',{name:'事件与演练',exact:true}).click();
          await rail.getByRole('button',{name:'风险与保障',exact:true}).click();
          for(const key of ['event','risk_inspection']){
            const entry=menu.locator('[data-nav-entry="'+key+'"]');
            await entry.hover();
            assert(await entry.isVisible());
          }
          await page.keyboard.press('Escape');
          assert(await safety.evaluate(el=>el===document.activeElement));
          await operations.click();
          assert(await rail.locator('[data-nav-entry="daily"]').isVisible(),'expanded branch persists when returning to the group');
          await safety.click();
          assert(await menu.locator('[data-nav-entry="event"]').isVisible());
          assert(await menu.locator('[data-nav-entry="risk_inspection"]').isVisible());
          await page.screenshot({path:path.join(output,'desktop.png')});
          await rail.getByRole('button',{name:'个人工作台',exact:true}).click();
          assert.equal(await page.getByRole('button',{name:'交接班审核',exact:true}).count(),0);
          await page.getByRole('searchbox',{name:'全局搜索'}).fill('交接班审核');
          await page.getByRole('region',{name:'搜索结果'}).getByRole('button',{name:'交接班审核',exact:true}).click();
          const dialog=page.getByRole('dialog',{name:'交接班审核'});
          await dialog.getByRole('button',{name:'B楼审核'}).waitFor();
          await dialog.screenshot({path:path.join(output,'handover-desktop.png')});
          await page.keyboard.press('Escape');
          await page.goto(entry);
        }
        if(account.id==='engineer'){
          await rail.getByRole('button',{name:'维修跟进',exact:true}).click();
          await page.waitForURL(url=>url.searchParams.get('__page')==='/repair-management'&&url.searchParams.get('scope')==='B');
          assert.equal(await rail.getByRole('button',{name:'维修跟进'}).getAttribute('aria-current'),'page');
          await page.goto(entry);
        }
        await page.setViewportSize({width:390,height:844});
        await bottom.waitFor();assert.equal(await rail.count(),0);
        await directory.getByRole('heading',{name:'全部功能'}).waitFor();
        assert.deepEqual(await bottom.getByRole('button').allTextContents(),['首页','待办','功能','我的']);
        assert.deepEqual(await ids(directory),[...account.entries].sort(),account.id+' phone IDs');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
        if(account.id==='public-admin'){
          await directory.locator('[data-nav-entry="settings:permissions"]').click();
          const picker=page.getByRole('combobox',{name:'设置分类'});
          await picker.click();
          const options=page.locator('.n-base-select-menu .n-base-select-option');
          await options.first().waitFor();
          assert.deepEqual((await options.allTextContents()).map(label=>label.trim()).sort(),
            ['账号与权限','交接班链接','MOP 配置','飞书智能体','钉钉推送','开发问题与更新记录'].sort());
        }
        if(account.id==='global')await page.screenshot({path:path.join(output,'mobile.png')});
        if(['global','supervisor','event-only'].includes(account.id)){
          let reads=0;
          page.on('request',request=>{if(new URL(request.url()).pathname==='/api/handover-links')reads++;});
          await bottom.getByRole('button',{name:'首页'}).click();
          await page.locator('.wb-home').getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
          assert.equal(await page.getByRole('button',{name:'交接班审核',exact:true}).count(),0);
          await page.getByRole('searchbox',{name:'全局搜索'}).fill('交接班审核');
          const review=page.getByRole('region',{name:'搜索结果'}).getByRole('button',{name:'交接班审核',exact:true});
          if(account.id==='event-only'){
            assert.equal(await review.count(),0);
            await page.goto(entry.replace('view=functions','view=handover'));
            await page.getByText('没有该功能权限',{exact:true}).waitFor();
            assert.equal(reads,0);
          }else{
            assert.equal(await review.count(),1);
            const before=page.url();
            await review.click();
            const dialog=page.getByRole('dialog',{name:'交接班审核'});
            if(account.id==='global'){
              await dialog.getByRole('button',{name:'B楼审核'}).waitFor();
              await page.screenshot({path:path.join(output,'handover-mobile.png')});
            }
            else await dialog.getByText('当前授权楼栋暂无已配置的审核链接').waitFor();
            assert.equal(page.url(),before);
            await page.keyboard.press('Escape');
            await dialog.waitFor({state:'detached'});
            await page.goto(entry.replace('view=functions','view=handover'));
            await page.waitForURL(url=>url.searchParams.get('view')==='home');
            await dialog.waitFor();
            if(account.id==='global'){
              await page.route(origin+'/fixture/review/B',route=>route.fulfill({contentType:'text/html',body:'<h1>Review B</h1>'}));
              await dialog.getByRole('button',{name:'B楼审核'}).click();
              await page.getByRole('heading',{name:'Review B'}).waitFor();
              assert.equal(context.pages().length,1);
            }
          }
        }
        console.log('PASS',account.id,account.entries.length,'authorized destinations');
      } finally {await context.close();}
    }
    console.log('PASS',scenarios.length,'account scenarios');
  } finally {await browser.close();await new Promise(resolve=>app.httpServer.close(resolve));}
}
