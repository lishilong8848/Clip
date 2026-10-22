import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';
import {preview} from 'vite';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/assistant-shared-query');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
try {
  for(const width of [402,440,1440]) {
    for(const legacy of [false,true]) {
      const context=await browser.newContext({viewport:{width,height:width===1440?900:956}});
      const layouts=layoutApiFixture(),errors=[];
      let guest=false;
      await context.route('**/*',async route=>{
        const url=new URL(route.request().url());
        if(url.origin!==origin)return route.abort();
        if(!url.pathname.startsWith('/api/'))return route.continue();
        if(await layouts.handle(route,'ou_query'))return;
        if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'shared-query-fixture'}});
        let data={items:[],records:[],stats:{},summary:{}};
        if(url.pathname==='/api/auth/status')data={logged_in:true,user:{open_id:'ou_query',name:'共享查询测试',role:guest?'guest':'building',modules:[],home_scope:'A'},scope_options:[{value:'A',label:'A楼'}]};
        if(url.pathname==='/api/assistant/conversation')data={configured:true,enabled:true,turns:[],conversation_id:'isolated',models:[],can_manage_settings:false};
        await route.fulfill({json:{ok:true,data}});
      });
      await context.route('**/assistant.html',async route=>{
        const response=await route.fetch();
        await route.fulfill({response,body:(await response.text()).replace('id="clipflow-lighthouse-widget"','id="clipflow-lighthouse-widget" data-user-id="ou_query"')});
      });
      const page=await context.newPage();
      page.on('pageerror',error=>errors.push(error.message));
      const url=origin+(legacy?'/assistant.html':'/assets/workspace-v2/index.html?__page='+encodeURIComponent('/?view=assistant'));
      await page.goto(url);
      await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
      await page.getByRole('textbox',{name:'询问灯塔助手'}).waitFor();
      await page.locator('.lighthouse').evaluate(async element=>{
        await Promise.all(element.getAnimations({subtree:true}).filter(animation=>animation.effect?.getTiming().iterations!==Infinity).map(animation=>animation.finished.catch(()=>{})));
      });
      assert.equal(await page.getByText('没有该功能权限',{exact:true}).count(),0);
      const box=await page.locator('.assistant-panel').boundingBox();
      assert(box&&box.x>=-1&&box.y>=-1&&box.x+box.width<=width+1);
      await page.screenshot({path:path.join(output,(legacy?'legacy':'workspace')+'-'+width+'.png')});
      if(legacy) {
        guest=true;
        const auth=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/auth/status');
        await page.reload();await auth;
        assert.equal(await page.locator('.lighthouse').count(),0,'guest must not mount an assistant');
      }
      assert.deepEqual(errors,[]);
      await context.close();
    }
  }
  console.log('Shared query entry: legacy/workspace, no module grant, phone/desktop and guest isolation passed.');
} finally {await browser.close();await server.close();}
