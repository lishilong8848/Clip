import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const fixture=`<template><NConfigProvider><NDialogProvider><NMessageProvider><DailyInspectionPage embedded /></NMessageProvider></NDialogProvider></NConfigProvider></template>
<script setup>import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';import DailyInspectionPage from '/src/components/DailyInspectionPage.vue';</script>`;
const server=await createServer({root,configFile:false,logLevel:'error',
  cacheDir:path.join(root,'node_modules/.vite-inspection-live'),
  optimizeDeps:{include:['vue','naive-ui','lucide-vue-next']},server:{host:'127.0.0.1',port:0},
  plugins:[{name:'inspection-live-fixture',enforce:'pre',resolveId(id){if(id.startsWith('/@live.'))return id;},
    load(id){if(id==='/@live.vue')return fixture;if(id==='/@live.js')return `import {createApp} from 'vue';import Fixture from '/@live.vue';createApp(Fixture).mount('#app');`;}},vue()]});
await server.listen();
const origin='http://127.0.0.1:'+server.httpServer.address().port;
let browser;
try{
  browser=await chromium.launch({headless:true});
  for(const viewport of [{width:1440,height:900},{width:390,height:844}]){
    const page=await browser.newPage({viewport}),errors=[],starts=[];
    page.on('pageerror',error=>errors.push(error.message));
    const now=Date.now(),date=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
    const duty={scope:'D',date,shift:'白'},template='D.xlsx';
    const active={id:'fixture-live-round',duty,round:1,pending:false,draft_key:'fixture-live',reviews:{},review_version:1,
      source_locked:false,worksheet_mode:true,professions:['电气','暖通'],batch:{devices:[],form_devices:[],count:1,batch:now,source_time:now-30000}};
    const state={duty,view_scope:'D',view_scopes:['D','E'],can_edit:true,history_scopes:['D','E'],history:[],
      completed_rounds:0,round_limit:2,next_round:1,active};
    await page.route('**/fixture',route=>route.fulfill({contentType:'text/html',body:`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/@live.js"></script>`}));
    await page.route(url=>url.pathname.startsWith('/api/'),route=>{
      const url=new URL(route.request().url()),ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/daily-inspection/start'){
        starts.push(route.request().postDataJSON());
        assert.equal(url.searchParams.get('view_scope'),'D');
        if(starts.length===1)return ok({...state,collection:{state:'running'}});
        state.active={...state.active,source_locked:true,review_version:2};
        return ok(state);
      }
      if(['/api/daily-inspection','/api/daily-inspection/status'].includes(url.pathname))return ok(state);
      if(url.pathname==='/api/daily-inspection/day-sheet')return ok({scope:'D',date,draft_key:'fixture-live-report',
        can_edit_history:true,template_version:'fixture',original_templates:[{name:template,profession:'电气',sheets:[{name:'数据',rows:[['项目','数值'],['电压',null]],merges:[]}]}],
        rounds:[{round_id:active.id,shift:'白',round:1,version:state.active.review_version,status:'进行中',source_locked:state.active.source_locked,original_cells:[{template,sheet:'数据',cell:'B2',parameter:'电压',value:'220'}]}]});
      if(url.pathname==='/api/daily-inspection/source')return ok({scope:'D',batch:now,count:0,devices:[]});
      if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
      return ok({});
    });
    await page.goto(origin+'/fixture');
    await page.getByRole('button',{name:'开始第 1 次巡检',exact:true}).click();
    await page.getByText('正在采集并核验本楼最新数据，完成后自动进入巡检…',{exact:true}).waitFor();
    assert.equal(await page.getByLabel('楼栋',{exact:true}).isDisabled(),true);
    assert.equal(await page.getByLabel('巡检日期',{exact:true}).isDisabled(),true);
    await page.locator('[data-inspection-source-time]').waitFor({state:'attached',timeout:15000}).catch(async error=>{
      throw new Error(error.message+'\n'+await page.locator('body').innerText()+'\n'+JSON.stringify({starts,errors}));
    });
    assert.equal(starts.length,2);
    assert.deepEqual(starts[0],starts[1]);
    assert.equal(await page.getByRole('button',{name:'开始第 1 次巡检',exact:true}).count(),0);
    assert.equal(await page.locator('body').evaluate(body=>body.scrollWidth>innerWidth+1),false);
    assert.deepEqual(errors,[]);
    await page.close();
  }
  console.log('PASS: desktop/mobile start waits for verified capture, retries the same round, then enters the locked workbook; all network mocked.');
}finally{await browser?.close();await server.close();}
