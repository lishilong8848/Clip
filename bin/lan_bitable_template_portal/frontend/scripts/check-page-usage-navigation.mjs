import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

const root=fileURLToPath(new URL('../',import.meta.url));
const cache=await mkdtemp(join(tmpdir(),'clipflow-usage-vite-'));
const browser=await chromium.launch();
try {
  for (const v2 of [true,false]) {
    const base=v2?'/assets/workspace-v2/':'/';
    const server=await createServer({root,configFile:resolve(root,v2?'vite.workspace-v2.config.ts':'vite.config.ts'),
      cacheDir:join(cache,v2?'v2':'legacy'),logLevel:'error',server:{host:'127.0.0.1',port:0,hmr:false,preTransformRequests:false,
        fs:{allow:[resolve(root,'..'),cache]}},optimizeDeps:{entries:[],noDiscovery:true,include:['vue','naive-ui','lucide-vue-next','@ai-sdk/vue','ai','gridstack','@tanstack/vue-virtual','dompurify','marked','echarts/core','echarts/charts','echarts/components','echarts/renderers']}});
    const context=await browser.newContext();
    try {
      await server.listen();
      const origin='http://127.0.0.1:'+server.httpServer.address().port;
      const url=v2?origin+base+'index.html?__page=/work-handover':origin+'/work-handover?frontend=legacy';
      const visits=[], errors=[];
      let authenticated=false;
      const session={role:'user',open_id:'fixture',name:'SECRET-NAME',modules:['work_handover','assistant']};
      await context.route('**/*',route=>{
        const req=route.request(), u=new URL(req.url());
        if(u.origin!==origin)return route.abort();
        if(req.resourceType()==='document')return route.fulfill({contentType:'text/html',body:
          `<div id="app"></div><div id="clipflow-lighthouse-widget" data-user-id="fixture"></div><script type="module" src="${base}src/${u.pathname==='/assistant-fixture'?'lighthouseWidget.ts':v2?'workspace-v2/main.ts':'main.ts'}"></script>`});
        if(u.pathname==='/api/usage/events'){
          const {events}=req.postDataJSON();visits.push(...events);
          return route.fulfill({json:{ok:true,data:{accepted:events.length}}});
        }
        if(u.pathname.startsWith('/api/')){
          const item={id:'test-handover',title:'隔离交接单',status:'accepted',items:[],owner:{name:'测试'},version:1,total:0,remaining:0};
          const day=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai'}).format(new Date());
          const calendar={month:u.searchParams.get('month'),days:{[day]:{staff:[],items:[1,2,3].map(i=>({kind:'变更',title:'测试日程'+i,start:day+'T11:00:00+08:00'}))}},unavailable:[]};
          const payload=u.pathname==='/api/personal-calendar'?calendar:u.pathname.endsWith('/test-handover')?item:
            u.pathname.endsWith('/work-handovers')?{items:[item],total:1}:
            u.pathname==='/api/auth/status'?{logged_in:authenticated,user:authenticated?session:{},scope_options:authenticated?[{value:'D',label:'D楼'}]:[]}:
            {items:[],scopes:[],modules:[],total:0,enabled:true,configured:true};
          return route.fulfill({json:{ok:true,data:payload}});
        }
        return route.continue();
      });
      const page=await context.newPage();
      page.on('pageerror',e=>errors.push(e.message));
      async function observed(module,view){
        for(let i=0;i<200;i++){
          if(visits.at(-1)?.module===module && visits.at(-1)?.page===view)return;
          await page.waitForTimeout(50);
        }
        throw new Error('Missing visit '+module+'/'+view+'; got '+JSON.stringify(visits)+'; '+errors.join(','));
      }
      page.on('console',msg=>{if(msg.type()==='error') console.error(msg.text().slice(0,200));});
      console.log('Testing '+(v2?'v2':'legacy')+' login');
      await page.goto(v2 ? origin+base+'index.html?__page=/' : origin+'/?frontend=legacy');
      await page.locator('#login-tab-password').click({timeout:30000});
      await observed('auth','password');
      assert.ok(visits.every(e=>e.module==='auth'));
      console.log('Testing business views'); authenticated=true; await page.goto(url);
      await observed('work_handover','received');
      await page.getByText('待我接收',{exact:true}).first().click();
      await observed('work_handover','pending');
      const n=visits.length;
      await page.getByRole('button',{name:'刷新',exact:true}).first().click();
      await page.waitForTimeout(250);
      assert.equal(visits.length,n,'read refresh counted as navigation');
      await page.getByText('我的接续工作',{exact:true}).first().click();
      await observed('work_handover','received');
      assert.deepEqual(visits.filter(v=>v.module==='work_handover').map(v=>v.page),['received','pending','received']);
      assert.ok(visits.every(e=>e.client===(v2?'web_v2':'web_legacy')));
      if (v2) {
        await page.getByText('隔离交接单',{exact:true}).click();
        await observed('work_handover','detail');
        await page.getByRole('button',{name:'返回交接清单'}).click();
        await observed('work_handover','received');
        await page.goto(origin+base+'index.html?__page=/&scope=D');
        await observed('portal','home');
        await page.getByRole('button',{name:/展开全部 .* 项日程/}).click();
        await page.getByRole('dialog',{name:'当天全部日程'}).waitFor();
        await observed('portal','agenda');
        assert.equal(visits.filter(e=>e.page==='agenda').length,1);
      }
      if(!v2){
        await page.goto(origin+'/assistant-fixture');
        const launcher=page.locator('.assistant-launcher');
        await launcher.waitFor();
        assert.ok(!visits.some(e=>e.module==='assistant'));
        await launcher.click();
        await observed('assistant','chat');
        assert.equal(visits.at(-1).client,'web_standalone');
      }
      assert.deepEqual(errors,[]);
      console.log('PASS '+(v2?'workspace':'legacy and standalone')+' real login, navigation, return and refresh');
    } catch(error) { console.error(error); throw error; } finally { await context.close(); await server.close(); }
  }
} finally { await browser.close(); await rm(cache,{recursive:true,force:true}); }
