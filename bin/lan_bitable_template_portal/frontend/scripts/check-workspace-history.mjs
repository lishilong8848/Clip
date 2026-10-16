import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse,compileScript} from '@vue/compiler-sfc';
import {createServer,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {adaptHistoryMemory} from './workspace-history-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url)),baseline=process.argv.includes('--baseline'),built=process.argv.includes('--built');
const sourcePath=path.join(root,'src/components/HistoryMemoryPage.vue'),source=await readFile(sourcePath,'utf8');
const view=await readFile(path.join(root,'src/workspace-v2/HistoryMemoryWorkspace.vue'),'utf8');
const adapted=adaptHistoryMemory(source,view),original=parse(source).descriptor.scriptSetup.content;
assert(adapted.includes(original.replace('onMounted(() => document.addEventListener("keydown", handleReviewKeydown));','')),'only the removed drawer keyboard listener is detached; business controller is unchanged');
compileScript(parse(adapted).descriptor,{id:'history-check',inlineTemplate:true});
assert.throws(()=>adaptHistoryMemory(source,view+' r.missingBinding'),/binding changed/);
const output=path.resolve(root,'../../../output/playwright/history-memory');
const fixture=`<template><NConfigProvider :theme="dark?darkTheme:null" :theme-overrides="dark?nightTheme:theme"><NDialogProvider><NMessageProvider><div class="v2-shell"><header class="v2-header"><a class="v2-brand"><img src="/assets/vnet-logo.png" alt="VNET"/><span>灯塔<small>南通运维工作空间</small></span></a></header><aside class="v2-sidebar"><div class="v2-nav-top">工作空间</div><NMenu :options="[{label:'历史通告记忆导入',key:'history'}]" value="history"/></aside><main class="v2-main"><HistoryPage v-if="mounted" :checking="false" :logged-in="logged" :is-admin="admin" login-url="/?login=feishu" /></main></div></NMessageProvider></NDialogProvider></NConfigProvider></template>
<script setup>import {ref,watch} from 'vue';import {NConfigProvider,NDialogProvider,NMessageProvider,NMenu,darkTheme} from 'naive-ui';import {theme,nightTheme} from '/src/workspace-v2/theme';import HistoryPage from '/src/components/HistoryMemoryPage.vue';import '/src/global.css';import '/src/workspace-v2/workspace.css';import '/src/workspace-v2/header.css';import '/src/workspace-v2/night.css';import '/src/workspace-v2/scrollbars.css';
const dark=ref(false),mounted=ref(true),logged=ref(true),admin=ref(true);document.documentElement.classList.add('workspace-v2');watch(dark,value=>document.documentElement.dataset.v2Theme=value?'dark':'light');window.fixture={dark,mounted,logged,admin};</script>`;
const server=built?await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0}}):await createServer({root,configFile:false,cacheDir:path.join(root,'node_modules/.vite-history-check'),logLevel:'error',optimizeDeps:{include:['vue','naive-ui','lucide-vue-next']},plugins:[{name:'history-check',enforce:'pre',resolveId(id){if(['/history.vue','/history.js'].includes(id))return id;},load(id){if(id==='/history.vue')return fixture;if(id==='/history.js')return `import {createApp} from 'vue';import App from '/history.vue';import LoadingIndicator from '/src/components/LoadingIndicator.vue';import UiTransition from '/src/components/UiTransition.vue';createApp(App).component('LoadingIndicator',LoadingIndicator).component('UiTransition',UiTransition).mount('#app');`;},transform(code,id){if(!baseline&&id.replaceAll('\\','/')===sourcePath.replaceAll('\\','/'))return adaptHistoryMemory(code,view);}},vue()],server:{host:'127.0.0.1',port:0}});
if(!built)await server.listen();const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});let page,failSave=false,failScan=false,holdScan,holdSave,empty=false;
const errors=[],scans=[],saves=[];
const titles=['BA系统资料维护','消防设施月度维护','配电设备通信模块检修','冷却水系统维护','空调过滤网更换','消防系统软件升级变更'];
const sources=Array.from({length:120},(_,i)=>({id:'source-'+i,title:`EA118机房${i%2?'B':'A'}楼${titles[i%6]}${i>5?' · '+i:''}`,building:i%2?'B楼':'A楼',building_codes:[i%2?'B':'A'],work_type:i%6===2?'repair':i%6===5?'change':'maintenance',source_record_id:'record-'+i,memory_name:'memory-'+i,source_status:'未开始',specialty:i%6===2?'电气':'暖通',maintenance_cycle:'月度',current_fields:{content:'本月当前事项内容',location:'机房'},memory:i===0?{content:'已保存的本楼记忆',location:'A楼机房',impact:'不影响正常运行'}:{}}));
const candidates=sources.filter((_,i)=>i%4!==3).map((s,i)=>({id:'candidate-'+s.id,title:s.title,work_type:s.work_type,building:s.building,building_codes:s.building_codes,business_time:'2026-09-15 09:00',fields:{content:'历史候选维护内容 '+i,location:s.building+'机房',reason:'计划维护',impact:'不影响正常运行',specialty:s.specialty}}));
const matches=candidates.map(c=>({source_id:c.id.slice('candidate-'.length),candidate_id:c.id,selected:true}));
const data=()=>({source_items:empty?[]:sources,candidates:empty?[]:candidates,matches:empty?[]:matches,warnings:[],counts:{source:empty?0:sources.length,candidates:empty?0:candidates.length,recommended:empty?0:matches.length}});
try{
  await mkdir(output,{recursive:true});page=await browser.newPage({viewport:{width:1440,height:960},reducedMotion:'reduce'});page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',async route=>{
    const req=route.request(),u=new URL(req.url());if(u.origin!==origin)return route.abort();
    if(u.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/history.js"></script>'});
    if(built&&u.pathname==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
    if(built&&u.pathname==='/api/auth/status')return route.fulfill({json:{ok:true,data:{logged_in:true,user:{open_id:'history-test-only',name:'隔离验收管理员',role:'admin',level:'L0',modules:['accounts','maintenance','change','repair']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]}}});
    if(built&&u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'history-fixture'}});
    if(!u.pathname.startsWith('/api/'))return route.continue();
    if(u.pathname.endsWith('/history-scan')){assert.equal(req.method(),'POST');scans.push(req.postDataJSON());if(holdScan)await new Promise(resolve=>holdScan=resolve);return route.fulfill(failScan?{status:500,json:{ok:false,error:'隔离扫描失败'}}:{json:{ok:true,data:data()}});}
    if(u.pathname.endsWith('/history-save')){assert.equal(req.method(),'POST');saves.push(req.postDataJSON());if(holdSave)await new Promise(resolve=>holdSave=resolve);return route.fulfill(failSave?{status:500,json:{ok:false,error:'隔离保存失败'}}:{json:{ok:true,data:{saved_count:saves.at(-1).matches.length,overwritten_count:1,skipped_count:0}}});}
    if(built&&req.method()==='GET')return route.fulfill({json:{ok:true,data:{items:[],scopes:{},records:[],settings:{}}}});
    throw new Error('Unexpected API '+u.pathname);
  });
  await page.goto(origin+(built?'/assets/workspace-v2/index.html?__page=/admin/history-memory':'/fixture'));assert.equal(scans.length,0,'no automatic cloud scan');
  await page.getByRole('button',{name:'扫描历史通告',exact:true}).waitFor();await page.waitForTimeout(200);
  const shot=name=>page.screenshot({path:path.join(output,(built?'built-':baseline?'before-':'after-')+name+'.png'),animations:'disabled'});
  await shot('initial');
  await page.getByRole('button',{name:'扫描历史通告',exact:true}).click();
  if(built){
    await page.locator('.v2-history-row').first().waitFor();await page.waitForFunction(()=>!document.querySelector('.v2-history-scan button')?.disabled);await shot('desktop');
    await page.locator('.v2-history-candidate .n-select').click();await page.locator('.n-base-select-option').first().waitFor();const popup=await page.locator('.n-base-select-menu').boundingBox();assert(popup.x>=0&&popup.x+popup.width<=1440&&popup.y+popup.height<=960,'candidate menu stays in viewport');await page.keyboard.press('Escape');
    await page.getByRole('button',{name:'套用此候选',exact:true}).click();await page.getByRole('button',{name:'预览并保存',exact:true}).click();await page.getByRole('button',{name:'确认保存 1 条',exact:true}).click();await page.locator('.v2-history-confirm').waitFor({state:'hidden'});assert.equal(saves.length,1);assert.equal(saves[0].matches[0].source_item.id,'source-0');
    await page.setViewportSize({width:390,height:844});await page.locator('.v2-history-row button').first().click();await page.locator('#history-field-content').waitFor({state:'visible'});for(const close of await page.locator('[data-v2-feedback-close]:visible').all())await close.click();await shot('mobile');assert(!await page.locator('.v2-main').evaluate(el=>el.scrollWidth>el.clientWidth+1));
    const footer=await page.locator('.v2-history-footer').boundingBox(),nav=await page.locator('.v2-mobile-nav').boundingBox();assert(footer.y+footer.height<=nav.y+1,'save actions stay above mobile navigation');
    assert.equal(await page.getByRole('button',{name:'预览并保存',exact:true}).locator('.n-button__content').evaluate(el=>getComputedStyle(el).color),'rgb(255, 255, 255)','primary button text is not dimmed by summary styling');
    await page.getByRole('button',{name:'事项列表',exact:true}).click();await page.locator('.v2-history-row').first().waitFor({state:'visible'});assert.deepEqual(errors,[]);
    console.log('PASS: built V2 route, actual unified client payload, desktop/mobile, dropdown portal, confirmation and save. No live business writes.');
  }else if(baseline){await page.locator('.source-row').first().waitFor();await shot('desktop');await page.locator('.source-row').first().click();await shot('review');}
  else{
    await page.locator('.v2-history-row').first().waitFor();assert.deepEqual(scans[0],{months:3,work_types:['maintenance','change','repair']});assert.equal(await page.locator('#history-field-content').inputValue(),'已保存的本楼记忆','recommended candidate is not silently applied');
    assert(await page.locator('.v2-history-row').count()<25,'long lists only render visible rows');assert.equal(saves.length,0);
    await shot('desktop');await page.getByRole('button',{name:'套用此候选',exact:true}).click();assert.match(await page.locator('#history-field-content').inputValue(),/^历史候选维护内容/);
    await page.locator('#history-field-content').fill('手工核对后保留的内容');
    await page.getByRole('button',{name:'套用此候选',exact:true}).click();await page.getByRole('button',{name:'保留当前填写',exact:true}).click();assert.equal(await page.locator('#history-field-content').inputValue(),'手工核对后保留的内容');
    await page.getByRole('button',{name:'扫描历史通告',exact:true}).click();await page.getByRole('button',{name:'保留当前填写',exact:true}).click();assert.equal(scans.length,1);
    await page.getByRole('button',{name:'预览并保存',exact:true}).click();await page.locator('.v2-history-confirm').waitFor();assert.equal(saves.length,0,'preview cannot write');
    await shot('confirm');await page.getByRole('button',{name:'继续核对',exact:true}).click();assert.equal(saves.length,0);
    failSave=true;await page.getByRole('button',{name:'预览并保存',exact:true}).click();await page.getByRole('button',{name:'确认保存 1 条',exact:true}).click();await page.locator('.v2-history-confirm').getByText('隔离保存失败',{exact:true}).waitFor();assert.equal(await page.locator('#history-field-content').inputValue(),'手工核对后保留的内容');
    assert.equal(saves[0].matches[0].source_item.id,'source-0');assert.equal(saves[0].matches[0].fields.content,'手工核对后保留的内容');assert.equal(saves[0].matches[0].candidate_id,'candidate-source-0');
    failSave=false;holdSave=true;await page.getByRole('button',{name:'确认保存 1 条',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.v2-history-confirm .n-button--loading'));assert(await page.getByRole('button',{name:'继续核对',exact:true}).isDisabled());holdSave();holdSave=null;await page.locator('.v2-history-confirm').waitFor({state:'hidden'});
    for(const width of [1366,1920,768,390,360]){
      await page.setViewportSize({width,height:width<801?844:960});await page.waitForTimeout(200);assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));assert(!await page.locator('.v2-main').evaluate(el=>el.scrollWidth>el.clientWidth+1));
      if(await page.locator('.v2-history-row button').first().isVisible())await page.locator('.v2-history-row button').first().click();await page.locator('#history-field-content').waitFor({state:'visible'});await shot('review-'+width);
      if(width<801){await page.locator('.v2-history-candidate .n-select').click();await page.locator('.n-base-select-option').first().waitFor();await page.keyboard.press('Escape');assert(await page.locator('.v2-history-review').isVisible(),'Escape closes the picker, not the mobile review');await page.getByRole('button',{name:'事项列表',exact:true}).click();await page.locator('.v2-history-row').first().waitFor({state:'visible'});await shot('list-'+width);}
    }
    await page.setViewportSize({width:1440,height:960});await page.getByRole('textbox',{name:'搜索当前月事项',exact:true}).fill('不存在的事项');await page.getByText('没有符合筛选条件的事项',{exact:true}).waitFor();await page.getByRole('button',{name:'清空筛选',exact:true}).click();
    await page.getByLabel('筛选楼栋',{exact:true}).click();await page.locator('.n-base-select-option').getByText('B楼',{exact:true}).click();await page.getByRole('checkbox',{name:'勾选筛选结果',exact:true}).check();assert.equal(scans.length,1,'filters stay local');
    await page.getByRole('button',{name:'预览并保存',exact:true}).click();await page.getByRole('button',{name:'确认保存 61 条',exact:true}).click();await page.locator('.v2-history-confirm').waitFor({state:'hidden'});assert.equal(saves.at(-1).matches.length,61);assert.equal(saves.at(-1).matches.filter(item=>item.source_item.building==='B楼').length,60,'batch selection respects the filter and keeps earlier selection');
    await page.getByRole('button',{name:'清空筛选',exact:true}).click();await page.getByRole('button',{name:'填充推荐并预览',exact:true}).click();await page.getByRole('button',{name:'确认保存 90 条',exact:true}).click();await page.locator('.v2-history-confirm').waitFor({state:'hidden'});assert(saves.at(-1).matches.every(item=>item.field_origin==='candidate'));
    failScan=true;await page.getByRole('button',{name:'扫描历史通告',exact:true}).click();await page.getByText('隔离扫描失败',{exact:true}).waitFor();assert(await page.locator('.v2-history-row').count()>0,'scan error retains prior data');failScan=false;
    await page.evaluate(()=>window.fixture.dark.value=true);await shot('dark');await page.evaluate(()=>window.fixture.dark.value=false);
    empty=true;await page.getByRole('button',{name:'扫描历史通告',exact:true}).click();await page.getByText('本次扫描没有当前月事项',{exact:true}).waitFor();await shot('empty');
    await page.evaluate(()=>window.fixture.admin.value=false);await page.getByText('仅管理员可使用历史通告记忆导入。',{exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'扫描历史通告',exact:true}).count(),0);
    await page.evaluate(()=>window.fixture.logged.value=false);await page.getByRole('link',{name:'登录灯塔',exact:true}).waitFor();
    await page.evaluate(()=>window.fixture.mounted.value=false);await page.locator('.v2-history').waitFor({state:'detached'});
    assert.deepEqual(errors,[]);assert.equal(await readFile(sourcePath,'utf8'),source,'legacy component remains untouched');
    console.log('PASS: history scan/save controller reuse; no auto scan; candidate/manual memory integrity; confirmation and retry; filtered batch selection; virtualization; PC/mobile; empty/error/auth states. Mocked business writes only.');
  }
}catch(error){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw error;}finally{if(typeof holdSave==='function')holdSave();if(typeof holdScan==='function')holdScan();await browser.close();await server.close();}
