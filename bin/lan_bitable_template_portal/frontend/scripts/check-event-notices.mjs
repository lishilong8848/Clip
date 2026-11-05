import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import {chromium} from 'playwright';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../build_output/event-notice-browser');await mkdir(output,{recursive:true});
const server=await createServer({root,server:{host:'127.0.0.1',port:0},plugins:[{name:'event-fixture',configureServer(server){server.middlewares.use('/__event_fixture',async (_req,res)=>{res.setHeader('Content-Type','text/html');res.end(await server.transformIndexHtml('/__event_fixture',`<html lang="zh"><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{margin:12px;font-family:Arial,sans-serif;background:#f0f4fa;color:#25364b}*{box-sizing:border-box}</style><div id="app"></div><script type="module">import {createApp} from 'vue';import Board from '/src/components/EventNoticeBoard.vue';import Legacy from '/src/components/WorkbenchLitePage.vue';const legacy=new URL(location.href).searchParams.has('legacy');createApp(legacy?Legacy:Board,legacy?{search:'?scope=D&work_type=event',active:true}:{scope:'D',active:true}).mount('#app');</script></html>`));});}}]});
await server.listen();const url='http://127.0.0.1:'+server.httpServer.address().port+'/__event_fixture';
const browser=await chromium.launch({headless:true});
try{
for(const legacy of [false,true])for(const [width,height] of [[402,874],[440,956],[1440,900]]){
 const context=await browser.newContext({viewport:{width,height},serviceWorkers:'block'}),page=await context.newPage(),errors=[],posts=[];
 let deletes=0,failStart=true,metadataWrites=0;
 const required=['summary','building','specialty','level','event_source','occurrence_time'];
 const publicRow=row=>({...row,missing:required.filter(key=>!row.metadata[key])});
 const createRow=(id,text,state='active')=>publicRow({event_id:id,created_by:'isolated-user',created_at:Date.now()/1000,state,text,metadata:{},metadata_revision:0,sync_state:'waiting',access_scopes:['D']});
 let rows=[createRow('web_'+'e'.repeat(32),'隔离事件卡片'),createRow('web_'+'f'.repeat(32),'已结束的隔离事件','ended')];
 let options={scopes:['D'],user_id:'isolated-user',levels:['I3','I2'],sources:['巡检发现'],specialties:['电气'],can_delete:true,defaults:{robot_ids:[],revision:0},robots:[{id:'a'.repeat(32),name:'钉钉测试群一',remark:'隔离目的地'}]};
 page.on('pageerror',error=>errors.push(error.message));
 await page.route(url=>url.pathname.startsWith('/api/'),async route=>{const req=route.request(),pathname=new URL(req.url()).pathname;let data={},status=200;
  if(pathname==='/api/workbench-events/options')data=options;
  else if(pathname==='/api/workbench-events')data={...options,items:rows};
  else if(pathname==='/api/workbench-events/defaults'){options.defaults={robot_ids:req.postDataJSON().robot_ids,revision:options.defaults.revision+1};data=options.defaults;}
  else if(pathname==='/api/workbench-events/send'){
   const body=req.postDataJSON();posts.push(body);if(failStart){failStart=false;status=503;}
   else{let row=rows.find(r=>r.event_id===body.event_id);if(!row){row=createRow(body.event_id,body.text);rows.unshift(row);}row.text=body.text;row.state=body.action==='end'?'ended':'active';row.delivery={state:'sent',action:body.action,operation_id:body.operation_id,targets:[{channel:'dingtalk',name:'钉钉测试群一',state:'sent'}]};data=row;}
  }else if(pathname==='/api/workbench-events/metadata'){metadataWrites++;const body=req.postDataJSON();let row=rows.find(r=>r.event_id===body.event_id);if(!row){row=createRow(body.event_id,'','draft');rows.push(row);}row.metadata={...row.metadata,...body.patch};row.metadata_revision++;row.sync_state='synced';Object.assign(row,publicRow(row));data=row;
  }else if(pathname==='/api/workbench-events/delete'){deletes++;if(deletes===1)status=503;else rows=rows.filter(row=>row.event_id!==req.postDataJSON().event_id);}
  else throw new Error('Unexpected isolated API: '+pathname);
  await route.fulfill({status,contentType:'application/json',body:JSON.stringify(status===200?{ok:true,data}:{ok:false,error:'模拟故障，原任务保留'})});
 });
 await page.goto(url+(legacy?'?legacy=1':''));await page.getByRole('heading',{name:'新建事件通告',exact:true}).waitFor();
 await page.getByRole('button',{name:/默认钉钉机器人/}).click();const dialog=page.getByRole('dialog',{name:'默认钉钉机器人'});await dialog.getByLabel('钉钉测试群一',{exact:false}).check();await dialog.getByRole('button',{name:'保存默认选择'}).click();
 const card=page.locator('.raw-event-card').first(),raw='【事件通告】状态：新增\n【标题】D楼I2测试\n保留完整原文';
 await card.getByLabel('本次通告原文').fill(raw);await page.screenshot({path:path.join(output,`event-${legacy?'legacy':'new'}-${width}.png`),fullPage:true});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'horizontal overflow');
 await card.getByRole('button',{name:'发送新增',exact:true}).click();await card.getByRole('button',{name:'核验并重试原发送'}).waitFor();await page.reload();await card.getByRole('button',{name:'核验并重试原发送'}).waitFor();assert.equal(await card.getByLabel('本次通告原文').inputValue(),raw);
 await card.getByRole('button',{name:'核验并重试原发送'}).click();const sent=page.locator('.event-grid .raw-event-card').filter({hasText:'D楼I2测试'});await sent.waitFor();
 assert.equal(posts.length,2);assert.deepEqual(posts[0],posts[1]);assert.equal(posts[0].dingtalk_robot_ids,undefined);assert.deepEqual(posts[0].metadata_patch,{});
 await sent.getByRole('button',{name:'结束',exact:true}).click();await sent.getByLabel('本次通告原文').fill('【事件通告】状态：结束\n【标题】D楼I2测试');await sent.getByRole('button',{name:'发送结束',exact:true}).click();
 const history=page.locator('.event-history').filter({hasText:'D楼I2测试'});await history.locator(':scope > summary').click();const ended=history.locator('.raw-event-card');if(width<850)await ended.locator('.metadata-side > summary').click();
 await ended.getByLabel('告警描述',{exact:true}).fill('结束后补充的资料');await ended.getByRole('status').filter({hasText:'多维资料已同步'}).waitFor();assert.equal(posts.length,3);assert(metadataWrites>0);
 const existing=page.locator('.event-grid .raw-event-card').filter({has:page.getByRole('heading',{name:'隔离事件卡片',exact:true})});
 await existing.getByRole('button',{name:'删除',exact:true}).click();await existing.getByRole('button',{name:'确认删除',exact:true}).click();await existing.getByText('模拟故障，原任务保留',{exact:true}).waitFor();assert.equal(await existing.count(),1);await existing.getByRole('button',{name:'确认删除',exact:true}).click();await existing.waitFor({state:'detached'});
 assert.deepEqual(errors,[]);await context.close();console.log(`raw event ${legacy?'legacy':'new'} ${width}x${height}: PASS`);
}
}finally{await browser.close();await server.close();}
