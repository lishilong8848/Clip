import assert from 'node:assert/strict';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse, compileScript, compileTemplate } from '@vue/compiler-sfc';
import { preview } from 'vite';
import { chromium } from 'playwright';
import { adaptPageFilters } from './workspace-filters-adapter.mjs';
const embeddedTasks=adaptPageFilters('<template><div class="v2-section-title"></div></template><script setup lang="ts">const embedded=true;</script>','TaskWorkspace.vue');
assert.match(embeddedTasks,/<WorkspaceScopePicker v-if="!embedded"/,'embedded widgets must not add another global building selector');
import { adaptCabinetPage } from './workspace-cabinet-adapter.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const home=await readFile(path.join(root,'src/workspace-v2/HomePage.vue'),'utf8');
assert.doesNotMatch(adaptPageFilters(home,'HomePage.vue'),/WorkspaceScopePicker/,'home keeps building selection inside widgets only');
for(const file of ['EventManagementPage','CabinetPowerPage','DrillManagementPage','DailyTaskChecklistPage','KnowledgeBasePage','LinkDirectoryPage','SignatureManagementPage','HistoryMemoryPage','PlanConvergencePage','LearningPage','MopPageHeader']){
  const source=await readFile(path.join(root,'src/components',file+'.vue'),'utf8');
  const adapted=adaptPageFilters(source,file+'.vue'),{descriptor}=parse(adapted);
  assert.notEqual(adapted,source,file+' has a V2 filter adapter');
  const script=compileScript(descriptor,{id:file});
  const result=compileTemplate({source:descriptor.template.content,filename:file+'.vue',id:file,compilerOptions:{bindingMetadata:script.bindings}});
  assert.deepEqual(result.errors,[],file+' preserves valid template branches');
}
const cabinet=await readFile(path.join(root,'src/components/CabinetPowerPage.vue'),'utf8');
assert(!cabinet.includes('trackButtonPresentation'),'legacy drawer behavior is unchanged');
assert(!adaptCabinetPage(cabinet).includes('trackButtonPresentation'),'opening rack details no longer uses submission feedback');
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[],queries=[],writes=[];
let page,active=0,maximum=0,scopes=['A','B'];
let historyReply;
const rack={room:'201',rack:'A01',state:'formal',rack_type:'服务器机柜'};
const sop=(id,scope,work_type,name)=>({sop_id:id,scope,work_type,name,version:1,steps:[{step_id:'step-'+id,content:'原操作内容',repeat_rules:[]}],attachments:[]});
let templates=[sop('sop-a','A','maintenance','A楼维保模板'),sop('sop-b','B','polling','B楼轮巡模板')];
try{
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'filter-fixture',name:'隔离验收',role:'admin',level:'L1',category:'manager',modules:['maintenance','polling','adjust','links','knowledge','cabinet','critical_guard']},scope_options:scopes.map(value=>({value,label:value+'楼'}))});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'filters-fixture'}});
    if(url.pathname==='/api/polling-sops'&&req.method()==='GET'){
      const scope=url.searchParams.get('scope'),type=url.searchParams.get('work_type');queries.push({scope,type});active++;maximum=Math.max(maximum,active);await new Promise(resolve=>setTimeout(resolve,25));active--;
      assert(scopes.includes(scope),'never request an unauthorized building');return ok({items:templates.filter(item=>item.scope===scope&&item.work_type===type)});
    }
    if(url.pathname==='/api/polling-sops/sop-b'&&req.method()==='PUT'){
      const body=req.postDataJSON();writes.push(body);assert.equal(body.scope,'B','editing from ALL must use the selected SOP building');const next={...templates[1],...body,version:2};templates[1]=next;return ok(next);
    }
    if(url.pathname==='/api/cabinet-power/bootstrap')return ok({status:'succeeded',ready:1,total:1,buildings:[{scope:'A',status:'succeeded'}]});
    if(url.pathname==='/api/cabinet-power/overview')return ok({version:1,record_count:1,issues:[],counts:{total:1,formal:1,test:0,off:0,unknown:0},rooms:[{id:'201',name:'201机房',sheet:'201',total:1,counts:{formal:1,test:0,off:0,unknown:0},types:{'服务器机柜':1,'网络机柜':0}}],sheet_formats:[]});
    if(url.pathname==='/api/cabinet-power/racks')return ok({version:1,items:[rack]});
    if(url.pathname==='/api/cabinet-power/rooms/201/layout')return ok({layout:{width:720,height:360,cells:[{ref:'title',x:0,y:0,width:300,height:32,text:'201机房',style:{}},{...rack,ref:'A1',x:120,y:80,width:70,height:40,text:'A01',style:{}}]}});
    if(url.pathname==='/api/cabinet-power/operations')return new Promise(resolve=>{historyReply=async()=>{await ok({items:[],total:0,page:1,rack_state:rack});resolve();};});
    if(req.method()!=='GET')throw new Error('Unexpected write '+url.pathname);
    return ok({items:[],records:[],scopes:[],settings:{},total:0,groups:[],permissions:{},engine:{},stats:{}});
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  const entry=origin+'/assets/workspace-v2/index.html';
  await page.goto(entry+'?view=sops');await page.locator('.v2-sop-item').filter({hasText:'B楼轮巡模板'}).waitFor();
  assert.equal(await page.locator('.v2-sop-item').count(),2);assert.equal(queries.length,6);assert(maximum<=3,'all-building reads use the existing bounded request queue');
  const filters=page.locator('.v2-main');assert.equal(await page.locator('#workspace-page-filters').count(),0);assert.equal(await filters.getByLabel('SOP楼栋',{exact:true}).count(),1);assert.equal(await filters.getByRole('textbox',{name:'查找SOP',exact:true}).count(),1);
  await filters.getByLabel('SOP楼栋',{exact:true}).click();await page.locator('.n-base-select-option').getByText('B楼',{exact:true}).click();await page.waitForFunction(()=>document.querySelectorAll('.v2-sop-item').length===1);assert.equal(queries.length,6,'switching building reuses local reads');await filters.getByLabel('SOP楼栋',{exact:true}).click();await page.locator('.n-base-select-option').getByText('全部可访问楼栋',{exact:true}).click();await page.waitForFunction(()=>document.querySelectorAll('.v2-sop-item').length===2);
  await filters.getByRole('textbox',{name:'查找SOP',exact:true}).fill('B楼');assert.equal(await page.locator('.v2-sop-item').count(),1);await filters.getByRole('textbox',{name:'查找SOP',exact:true}).fill('');
  await page.locator('.v2-sop-item').filter({hasText:'B楼轮巡模板'}).click();assert(await page.getByRole('textbox',{name:'步骤1内容',exact:true}).isDisabled());assert(await page.getByRole('checkbox',{name:'操作照片',exact:true}).isDisabled());assert.equal(writes.length,0);
  await page.getByRole('button',{name:'修改',exact:true}).click();await page.getByRole('textbox',{name:'步骤1内容',exact:true}).fill('修改后的测试内容');await page.getByRole('button',{name:'保存SOP',exact:true}).click();await page.getByText('SOP已保存',{exact:true}).waitFor();assert.equal(writes.length,1);assert(await page.getByRole('textbox',{name:'步骤1内容',exact:true}).isDisabled());
  for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:900});await page.waitForTimeout(200);assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));assert((await filters.boundingBox()).x<width*.3);}
  await page.setViewportSize({width:1440,height:900});await page.goto(entry+'?__page=/link-directory');await filters.getByRole('searchbox',{name:'按名称、用途或分类搜索',exact:true}).waitFor();assert.equal(await page.locator('.link-directory-page .toolbar').count(),1,'link search stays next to its business list');
  await page.goto(entry+'?__page=/knowledge-base');await filters.getByRole('searchbox',{name:'搜索知识库',exact:true}).waitFor();
  await page.emulateMedia({reducedMotion:'no-preference'});await page.goto(entry+'?__page=/cabinet-power&scope=A');
  await page.getByRole('button',{name:'原始平面图',exact:true}).click();await filters.getByRole('textbox',{name:'搜索平面图',exact:true}).waitFor();
  const cell=page.locator('.map-cell').filter({hasText:'A01'});await cell.waitFor();
  const beforeBox=await cell.boundingBox();await cell.click();
  await page.getByRole('dialog',{name:'机柜完整历史',exact:true}).waitFor();
  assert.equal(await cell.getAttribute('data-v2-action'),null,'opening rack details keeps lightweight selected feedback');
  assert.equal(await cell.evaluate(el=>getComputedStyle(el).position),'absolute','button feedback must preserve the floor plan coordinate system');
  const afterBox=await cell.boundingBox();assert(Math.abs(afterBox.x-beforeBox.x)<1&&Math.abs(afterBox.y-beforeBox.y)<1,'rack does not jump when feedback appears');
  assert.equal(await cell.getAttribute('aria-pressed'),'true','the opened rack remains visibly selected');
  assert.equal(await cell.locator('.v2-action-feedback').count(),0,'rack contents remain readable instead of turning into a checkmark');
  assert.equal(typeof historyReply,'function');await historyReply();await page.getByRole('button',{name:'关闭历史',exact:true}).click();
  assert.equal(await cell.getAttribute('aria-pressed'),'false');
  await page.waitForFunction(()=>!document.querySelector('.map-cell[data-v2-action]'));await page.emulateMedia({reducedMotion:'reduce'});
  await cell.focus();await cell.press('Enter');await page.getByRole('dialog',{name:'机柜完整历史',exact:true}).waitFor();
  assert.equal(await cell.getAttribute('aria-pressed'),'true','keyboard/reduced-motion users still get selected feedback');
  assert.equal(await cell.getAttribute('data-v2-action'),null);await historyReply();await page.getByRole('button',{name:'关闭历史',exact:true}).click();
  scopes=['A'];templates=templates.slice(0,1);await page.goto(entry+'?view=sops');await page.locator('.v2-sop-item').filter({hasText:'A楼维保模板'}).waitFor();assert.equal(await page.locator('.v2-sop-item').count(),1,'ALL does not expand a single-building account');
  assert.deepEqual(errors,[]);const output=path.resolve(root,'../../../output/playwright/workspace-filters');await mkdir(output,{recursive:true});await page.screenshot({path:path.join(output,'sop-all.png'),fullPage:true});
  console.log('PASS: V2-only local filters, valid templates, all-authorized SOPs/types, 3-read limit, correct building on edits, search placement, PC/mobile. All writes mocked.');
}catch(error){if(page){const output=path.resolve(root,'../../../output/playwright/workspace-filters');await mkdir(output,{recursive:true});await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(await page.evaluate(()=>[...document.querySelectorAll('body *')].filter(el=>el.getBoundingClientRect().right>innerWidth+1).slice(0,15).map(el=>({tag:el.tagName,class:el.className,right:el.getBoundingClientRect().right}))),errors);}throw error;}finally{await browser.close();await server.close();}
