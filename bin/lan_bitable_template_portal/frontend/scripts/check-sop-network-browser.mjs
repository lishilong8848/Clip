import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/sop-network');
const fixture=`
import {createApp,h} from 'vue';
import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
import Sop from '/src/workspace-v2/NoticeSopManager.vue';
import Cabinet from '/src/components/CabinetPowerPage.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import UiTransition from '/src/components/UiTransition.vue';
import {theme} from '/src/workspace-v2/theme.ts';
import '/src/global.css';
import '/src/workspace-v2/workspace.css';
import '/src/workspace-v2/dashboard/dashboard.css';
import '/src/workspace-v2/feedback.css';
document.documentElement.classList.add('workspace-v2');
const cabinet=new URLSearchParams(location.search).get('mode')==='cabinet';
const app=createApp({render:()=>h(NConfigProvider,{themeOverrides:theme},()=>h(NMessageProvider,null,()=>h(NDialogProvider,null,()=>h('div',{class:'v2-shell'},[
  h('header',{class:'v2-header'},[h('strong','灯塔'),h('div',{id:'page-back-slot'})]),
  h('main',{class:'v2-main'},[cabinet?h(Cabinet,{scope:'',isAdmin:true,userId:'fixture-user'}):h(Sop,{initialScope:'A',initialType:'maintenance',scopes:[{value:'A',label:'A楼'}],types:[{value:'maintenance',label:'维保'}],canDelete:true})])
]))))});
app.component('LoadingIndicator',LoadingIndicator).component('UiTransition',UiTransition).mount('#app');`;
const server=await createServer({root,configFile:false,plugins:[{name:'isolated-sop-network',resolveId:id=>id==='/__side-entry.js'?id:null,load:id=>id==='/__side-entry.js'?fixture:null,configureServer(server){server.middlewares.use((req,res,next)=>{if(!req.url.startsWith('/__side-check'))return next();res.setHeader('Content-Type','text/html');res.end('<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/__side-entry.js"></script>');});}},vue()],logLevel:'error',server:{host:'127.0.0.1',port:0}});
await server.listen();
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
const networkMessage='暂时无法连接服务，请检查网络连接后重试。';
let page,fail='',bootstrapMode='succeeded',bootstrapWrites=0;
const counts={total:100,formal:70,test:10,off:20,unknown:0};
const bootstrap=()=>({status:bootstrapMode,total:5,ready:bootstrapMode==='succeeded'?5:0,failed:0,buildings:[...'ABCDE'].map(scope=>({scope,status:bootstrapMode==='succeeded'?'succeeded':'idle'}))});
try{
  await mkdir(output,{recursive:true});
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/polling-sops')return ok({items:[{sop_id:'fixture-sop',name:'隔离测试模板',scope:'A',work_type:'maintenance',version:1,attachments:[],steps:[{step_id:'one',content:'核对现场测试条件',operator_required:true,reviewer_required:true,photo_required:false,time_limit_seconds:0,delay_reminder_minutes:0,repeat_rules:[]}]}]});
    if(url.pathname==='/api/polling-sops/fixture-sop'&&req.method()==='PUT')return route.abort('connectionreset');
    if(url.pathname==='/api/cabinet-power/bootstrap'){
      if(req.method()==='POST'){bootstrapWrites++;assert.equal(bootstrapMode,'idle','only a confirmed empty store can initialize');bootstrapMode='succeeded';return route.abort('connectionreset');}
      if(fail==='bootstrap')return route.abort('connectionreset');
      return ok(bootstrap());
    }
    if(url.pathname==='/api/cabinet-power/buildings'){
      if(fail==='buildings')return route.abort('connectionreset');
      return ok({buildings:[...'ABCDE'].map(scope=>({scope,bootstrap_status:'succeeded',counts,record_count:100,inventory_only:0}))});
    }
    assert.equal(req.method(),'GET','no other writes allowed');
    return ok({items:[],pending_count:0});
  });
  page=await context.newPage();page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));
  await page.goto(origin+'/__side-check');
  await page.getByRole('button',{name:'隔离测试模板',exact:false}).click();
  const actions=page.locator('.v2-sop-actions'),save=actions.getByRole('button',{name:'保存SOP',exact:true});
  assert.equal(await page.locator('.v2-sop-manager>.n-card__footer').count(),0,'no full-width footer');
  assert(await save.isDisabled(),'unchanged template cannot save');
  for(const width of [1440,390,360]){
    await page.setViewportSize({width,height:900});
    const panel=await page.locator('.v2-sop-manager').boundingBox(),buttons=await actions.boundingBox();
    const heading=await page.locator('.v2-sop-manager>.n-card-header>.n-card-header__main').boundingBox();
    assert(heading.height<36,'page title stays on one line instead of being squeezed by actions');
    assert(buttons.width<400&&buttons.x>=panel.x&&buttons.x+buttons.width<=panel.x+panel.width+1);
    assert(buttons.y<panel.y+120,'actions remain beside the page heading');
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    await page.screenshot({path:path.join(output,'sop-actions-'+width+'.png'),fullPage:true});
    await page.evaluate(()=>{document.querySelector('.v2-main').scrollTop=300;window.scrollTo(0,300);});
    await page.waitForTimeout(80);
    const scrolled=await actions.boundingBox();
    assert(scrolled.y>=55&&scrolled.y<180,'actions stay visible below the application header while scrolling');
    await page.evaluate(()=>{document.querySelector('.v2-main').scrollTop=0;window.scrollTo(0,0);});
  }
  await page.getByRole('textbox',{name:'SOP名称',exact:true}).fill('填写内容保留测试');
  await save.click();await page.getByRole('alert').filter({hasText:networkMessage}).waitFor();
  assert.equal(await page.getByRole('textbox',{name:'SOP名称',exact:true}).inputValue(),'填写内容保留测试');
  assert(!/Failed to fetch|NetworkError|Load failed/.test(await page.locator('body').innerText()));
  assert.match(await page.getByRole('alert').filter({hasText:networkMessage}).innerText(),/保存结果尚未确认/);

  await page.setViewportSize({width:1440,height:900});
  await page.goto(origin+'/__side-check?mode=cabinet');
  await page.locator('.buildings .building').nth(4).waitFor();
  assert.equal(bootstrapWrites,0,'ready local data never posts bootstrap on page entry');
  fail='buildings';await page.getByRole('button',{name:'刷新',exact:true}).click();
  await page.getByRole('alert').filter({hasText:networkMessage}).waitFor();
  assert.equal(await page.locator('.buildings .building').count(),5,'disconnect preserves loaded building cards');
  assert.equal(await page.locator('.bootstrap-notice').count(),0);
  assert.equal(bootstrapWrites,0);

  fail='bootstrap';await page.reload();
  await page.getByRole('button',{name:'重新连接',exact:true}).waitFor();
  assert.equal(await page.locator('.bootstrap-notice').count(),0,'unknown status cannot become 0/5 initialization');
  assert.equal(bootstrapWrites,0,'failed read cannot start initialization');
  fail='';await page.getByRole('button',{name:'重新连接',exact:true}).click();
  await page.locator('.buildings .building').nth(4).waitFor();
  assert.equal(bootstrapWrites,0,'reconnection only reads existing data');

  bootstrapMode='idle';await page.reload();
  await page.getByRole('button',{name:'重新连接',exact:true}).waitFor();
  assert.equal(bootstrapWrites,1,'genuinely missing data can still initialize once');
  assert.equal(await page.locator('.bootstrap-notice').count(),0,'lost submission response is not fabricated running progress');
  await page.getByRole('button',{name:'重新连接',exact:true}).click();
  await page.locator('.buildings .building').nth(4).waitFor();
  assert.equal(bootstrapWrites,1,'lost response rechecks the original status without resubmission');
  assert.deepEqual(errors,[]);
  console.log('PASS: SOP compact header actions at 1440/390/360; Chinese network errors and preserved input; cabinet ready/offline/reconnect/response-loss never reset or repeat initialization. Isolated data only.');
}catch(error){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw error;}
finally{await browser.close();await server.close();}
