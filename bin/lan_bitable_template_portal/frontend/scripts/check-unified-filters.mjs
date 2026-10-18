import assert from 'node:assert/strict';
import { readFile, readdir, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { parse, compileTemplate } from '@vue/compiler-sfc';
import { preview } from 'vite';
import { chromium } from 'playwright';
import { adaptPageFilters } from './workspace-filters-adapter.mjs';

function handlers(source){
  const values=[];
  function visit(node){for(const prop of node.props||[])if(prop.type===7&&['model','on'].includes(prop.name))values.push([prop.name,prop.arg?.content,prop.exp?.content,prop.modifiers?.map(item=>item.content)].join('|'));for(const child of node.children||[])visit(child);}
  visit(parse(source).descriptor.template?.ast||{});return values.sort();
}
let marked=0;
for(const dir of ['src/components','src/workspace-v2','src/workspace-v2/dashboard'])for(const file of (await readdir(dir)).filter(file=>file.endsWith('.vue'))){
  const source=await readFile(path.join(dir,file),'utf8'),result=adaptPageFilters(source,file);
  if(result===source)continue;
  assert.deepEqual(handlers(result),handlers(source),file+' keeps every value binding and handler');
  const {descriptor,errors}=parse(result);
  const template=compileTemplate({id:file,filename:file,source:descriptor.template.content});
  assert.deepEqual([...errors,...template.errors],[],file+' compiles');marked++;
}
assert(marked>=40);

const output=path.resolve('../../../output/playwright/unified-filters');await mkdir(output,{recursive:true});
const server=await preview({...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:'vite.workspace-v2.config.ts'}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`,entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true}),errors=[],writes=[];
let page;
try{
  const ctx=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
  await ctx.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();
    if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.resolve('public/assets/vnet-logo.png'),contentType:'image/png'});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(req.method()!=='GET'){writes.push(url.pathname);return route.fulfill({status:400,json:{ok:false,error:'No business writes in filter preview'}});}
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'filter-fixture',name:'测试管理员',role:'admin',level:'L1',category:'manager',modules:['maintenance','change','repair','polling','adjust','power','event','repair_management','knowledge','links','water','signatures','plan_convergence','drills','learning','cabinet','accounts']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'unified-filter-preview'}});
    if(url.pathname==='/api/workbench')return ok({records:[],ongoing:[],records_pagination:{total:0},ongoing_pagination:{total:0}});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(url.pathname==='/api/events/monthly')return ok({records:[],snapshot_exists:true,last_refreshed_at:1792029600});
    if(url.pathname==='/api/capacity/water/bootstrap')return ok({scope:'A',options:{meters:[],frequencies:['日','周'],shifts:['白','夜']},summary:{},snapshot:{exists:true},permissions:{can_create:true,is_admin:true}});
    if(url.pathname==='/api/capacity/water/records')return ok({records:[],total:0,snapshot:{exists:true}});
    if(url.pathname==='/api/signatures/people')return ok({people:[],sources:{},permissions:{is_admin:true},can_manage:true});
    if(url.pathname==='/api/plan-convergence/bootstrap')return ok({blocks:{items:[],loaded_at:1792029600},zh_ready:true,catalog_ready:true});
    if(url.pathname==='/api/plan-convergence/rulesets')return ok([]);
    return ok({items:[],records:[],total:0,groups:[],scopes:[],settings:{},permissions:{},engine:{},stats:{},fields:[],people:[],changes:[],cursor:0,snapshot:{exists:true}});
  });
  page=await ctx.newPage();page.on('pageerror',error=>errors.push(error.message));
  const cases=[
    ['notice','?__page=/workbench-lite&scope=A&work_type=maintenance','.v2-notice-plan-search .n-input'],
    ['sop','?view=sops&scope=A','.v2-sop-layout .n-input'],
    ['repair','?__page=/repair-management&scope=A','.v2-repair-filters .n-input'],
    ['event','?__page=/&mode=events&detail=1&scope=A&period=custom&event_start=2026-10-01&event_end=2026-10-15','.event-list-tools input[type=search]'],
    ['knowledge','?__page=/knowledge-base','.kb-search'],
    ['links','?__page=/link-directory','.search-field'],
    ['water','?__page=/water-management&scope=A&water_view=records','.water-search'],
    ['convergence','?__page=/plan-convergence&scope=A','.pc-list-filters input[type=search]'],
  ];
  const only=process.argv.find(value=>value.startsWith('--only='))?.slice(7),selectedCases=only?cases.filter(([name])=>name===only):cases;
  assert(selectedCases.length,'Unknown filter test case');
  for(const [name,query,searchSelector] of selectedCases){
    await page.goto(entry+query);
    if(name==='event'){const expand=page.getByRole('button',{name:'展开事件明细',exact:true});if(await expand.count())await expand.click();}
    const search=page.locator('.v2-main '+searchSelector).first();await search.waitFor();
    for(const width of [1440,390]){
      await page.setViewportSize({width,height:1000});
      const dimensions=await search.evaluate(el=>({height:el.getBoundingClientRect().height,radius:getComputedStyle(el).borderRadius}));
      assert(Math.abs(dimensions.height-(width<700?44:36))<1,name+' search height '+JSON.stringify(dimensions));
      assert.equal(dimensions.radius,'6px',name+' search radius');
      if(['knowledge','links'].includes(name))assert.equal(await search.locator('input').evaluate(el=>getComputedStyle(el).borderTopWidth),'0px',name+' search has one border only');
      const controls=await page.locator('[data-v2-filter] .n-base-selection').evaluateAll(els=>els.filter(el=>el.getBoundingClientRect().height).map(el=>({height:el.getBoundingClientRect().height,radius:getComputedStyle(el).borderRadius})));
      for(const control of controls){assert(Math.abs(control.height-(width<700?44:36))<1,name+' select height '+JSON.stringify(control));assert.equal(control.radius,'6px',name+' select radius');}
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,name+' page overflow '+width);
      await page.screenshot({path:path.join(output,`${name}-${width}.png`)});
    }
    const input=search.locator('input').first();
    if(await input.count())await input.fill('测试查询');else await search.fill('测试查询');
    if(name==='convergence'){
      await page.getByRole('combobox',{name:'屏蔽状态',exact:true}).click();
      await page.locator('.v2-single-select-menu').waitFor();
      await page.locator('.n-base-select-option').getByText('屏蔽中',{exact:true}).click();
      assert.match(await page.getByRole('combobox',{name:'屏蔽状态',exact:true}).innerText(),/屏蔽中/);
    }
  }
  assert.deepEqual(errors,[]);assert.deepEqual(writes,[]);
  console.log(`PASS: ${marked} adapted templates preserve handlers; ${selectedCases.length} selected modules share desktop/mobile filter sizes and styles; query and dropdown interaction retained.`);
}catch(error){if(page){await page.screenshot({path:path.join(output,'failure.png')});console.error(errors,await page.locator('#workspace-page-filters').innerText().catch(()=>''));}throw error;}
finally{await browser.close();await server.close();}
