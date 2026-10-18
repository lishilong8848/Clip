import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {build,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const outDir=path.resolve(root,'../../../output/pre-shift-responsive-build');
const screenshots=path.resolve(root,'../../../output/playwright/pre-shift-responsive');
await mkdir(screenshots,{recursive:true});
await build({root,configFile:false,publicDir:false,logLevel:'error',plugins:[{
  name:'pre-shift-isolated',enforce:'post',resolveId(id){if(id==='virtual:check-preview')return '\0check-preview';},
  load(id){if(id!=='\0check-preview')return;return `
    import {createApp,h} from 'vue';
    import {NConfigProvider,NMessageProvider,NDialogProvider} from 'naive-ui';
    import Check from '/src/workspace-v2/dashboard/PreShiftCheck.vue';
    import Legacy from '/src/components/PreShiftChecklistPage.vue';
    import '/src/global.css';import '/src/workspace-v2/workspace.css';
    const params=new URLSearchParams(location.search),legacy=params.has('legacy'),manager=params.get('role')!=='operator';
    if(!legacy)document.documentElement.classList.add('workspace-v2');
    const user={open_id:manager?'ou_admin':'ou_one',name:'隔离测试',level:manager?'L1':'L4',category:manager?'manager':'operator',home_scope:'A',modules:['pre_shift'],scope_rules:params.has('limited')?['A','B']:manager?['ALL']:['A']};
    createApp({render:()=>h(NConfigProvider,{},()=>h(NMessageProvider,{},()=>h(NDialogProvider,{},()=>legacy?h(Legacy,{user}):h(Check,{user,registerGuard:()=>()=>{}}))))}).mount('#app');`;
  },
  generateBundle(_options,bundle){const entry=Object.values(bundle).find(item=>item.type==='chunk'&&item.isEntry);const css=Object.values(bundle).filter(item=>item.fileName.endsWith('.css'));this.emitFile({type:'asset',fileName:'index.html',source:`<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{box-sizing:border-box}</style>${css.map(item=>`<link rel="stylesheet" href="/${item.fileName}">`).join('')}</head><body style="margin:0;padding:10px"><main id="stage" style="width:1120px;max-width:100%;height:740px;overflow:auto;padding:12px;border:1px solid #dedee3;border-radius:6px;background:white"><div id="page-back-slot"></div><div id="app"></div></main><script type="module" src="/${entry.fileName}"></script></body></html>`});},
},vue()],build:{outDir,emptyOutDir:true,cssCodeSplit:false,rollupOptions:{input:'virtual:check-preview'}}});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
let page;
const errors=[],items=[{id:'x',text:'核对监控告警及设备状态'},{id:'y',text:'核对机房环境与现场遗留事项'},{id:'z',text:'核对当班作业和工单安排'}];
const now=Date.parse('2026-10-17T09:00:00+08:00');
let submitted=false,photo=null,writes=0,historyMalformed=false;
try{
  const context=await browser.newContext({viewport:{width:1366,height:900},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/pre-shift-checks/history'){
      if(historyMalformed)return ok({items:[]});
      const scope=url.searchParams.get('scope'),pageNumber=Number(url.searchParams.get('page'));
      assert(['A','B'].includes(scope));assert.equal(url.searchParams.get('page_size'),'20');
      const records=Array.from({length:23},(_,index)=>({id:scope+'-history-'+index,scope,day:'2025-01-01',shift:index%2?'夜':'白',due:1735692600,deadline:1735725600,submitted:'2025-01-01T09:00:00+08:00',actor_name:scope+'楼历史检查人'+index,normal_count:1,abnormal_count:1,sync_status:'synced',files:[{id:'fixture-history-photo',item_id:'y',name:'历史异常照片.png',is_image:true,size:80}],handover_photo:null,answers:[{id:'x',text:'历史设备检查',status:'normal',note:''},{id:'y',text:'历史环境检查',status:'abnormal',note:'历史逐项异常备注\n已通知工程师'}]}));
      return ok({scope,page:pageNumber,page_size:20,total:records.length,items:records.slice((pageNumber-1)*20,pageNumber*20)});
    }
    if(url.pathname==='/api/pre-shift-checks'){
      const params=new URL(page.url()).searchParams,manager=params.get('role')!=='operator',scope=url.searchParams.get('scope')||'A';
      const scopes=params.has('limited')?['A','B']:manager?['110',...'ABCDEH']:['A'];
      assert(scopes.includes(scope),'no unauthorized building is requested');
      return ok({scope,scopes,can_manage:manager,server_time:new Date(now).toISOString(),definition:{version:1,items},roster_ready:true,roster_error:'',
        overview:manager?{rows:scopes.flatMap(scope=>['白','夜'].map(shift=>({id:scope+shift,scope,shift,day:'2026-10-17',total:3,completed:scope==='A'&&shift==='白'?3:0,status:shift==='夜'?'未到时间':scope==='A'?'已完成':'待完成',abnormal:0,submitted_by:scope==='A'&&shift==='白'?'人员一':'',submitted_at:'2026-10-17T08:55:00+08:00'})))}:null,
        tasks:['白','夜'].map(shift=>({id:scope+shift,scope,shift,day:'2026-10-17',due:shift==='白'?now/1000-600:now/1000+30000,deadline:now/1000+50000,definition_version:1,items,people:[{name:'人员一'},{name:'人员二'}],submitted:submitted&&shift==='白'?'2026-10-17T09:00:00+08:00':'',actor_name:'人员一',can_submit:(!manager||params.has('editable'))&&!submitted,can_upload_handover:!manager&&!photo,files:[],handover_photo:photo,answers:submitted?items.map(item=>({...item,status:'normal',note:''})):[],sync_status:'synced'}))});
    }
    if(url.pathname==='/api/pre-shift-checks/submit'){writes++;assert.equal(request.postDataJSON().answers.length,3);submitted=true;return ok({completed:true,actor_name:'人员一'});}
    if(url.pathname==='/api/pre-shift-checks/files'){assert.equal(url.searchParams.get('kind'),'handover');assert.equal(photo,null);photo={id:'fixture-photo',name:'交接班.png',is_image:true,size:80};return ok(photo);}
    if(url.pathname.startsWith('/api/pre-shift-checks/files/'))return route.fulfill({contentType:'image/png',body:Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jh98AAAAASUVORK5CYII=','base64')});
    return ok({});
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin);await page.locator('.v2-check-building').nth(6).waitFor();
  const boxes=()=>page.locator('.v2-check-building').evaluateAll(nodes=>nodes.map(node=>{const r=node.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,bottom:r.bottom};}));
  let bounds=await boxes();assert(new Set(bounds.map(row=>Math.round(row.x))).size>=3,'wide widgets show more buildings in columns');assert(bounds[0].width<400,'a building row does not stretch across the widget');
  const visibleCount=()=>page.locator('#stage').evaluate(el=>{const clip=el.getBoundingClientRect();return [...el.querySelectorAll('.v2-check-building')].filter(node=>{const box=node.getBoundingClientRect();return box.top>=clip.top&&box.bottom<=clip.bottom;}).length;});
  await page.locator('#stage').evaluate(el=>el.style.height='320px');const shortCount=await visibleCount();
  await page.locator('#stage').evaluate(el=>el.style.height='780px');assert(await visibleCount()>shortCount,'taller widgets expose more complete building groups');
  await page.screenshot({path:path.join(screenshots,'administrator-pc.png'),fullPage:true});
  await page.getByRole('button',{name:'查看A楼检查',exact:true}).click();await page.locator('.v2-check-task').waitFor();
  const nav=await page.locator('.v2-check-shifts').boundingBox(),body=await page.locator('.v2-check-task').boundingBox();assert(nav&&body&&nav.x+nav.width<=body.x,'wide single-building view places shifts left and checklist right');
  await page.screenshot({path:path.join(screenshots,'single-building-pc.png'),fullPage:true});
  await page.goto(origin+'/?role=operator');await page.locator('fieldset').nth(2).waitFor();
  assert.equal(await page.locator('.v2-check-attachments[aria-label="检查附件上传区"]').count(),0,'optional attachment controls are collapsed initially');
  await page.getByRole('button',{name:items[0].text+'附件',exact:true}).click();assert.equal(await page.locator('.v2-check-attachments[aria-label="检查附件上传区"]').count(),1);
  await page.getByLabel('上传交接班照片',{exact:true}).setInputFiles({name:'交接班.png',mimeType:'image/png',buffer:Buffer.from('isolated-photo')});await page.getByText('本班已上传',{exact:true}).waitFor();assert.equal(await page.locator('.v2-check-handover').getByRole('button',{name:'上传照片',exact:true}).count(),0,'one photo removes the second-upload control');
  for(const width of [360,375,390]){await page.setViewportSize({width,height:width===360?640:667});await page.locator('#stage').evaluate(el=>{el.style.width='100%';el.style.height='calc(100dvh - 20px)';});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(screenshots,'operator-'+width+'.png'),fullPage:true});}
  for(const row of await page.locator('fieldset').all())await row.locator('.n-radio-button').filter({hasText:'无异常'}).click();
  await page.getByRole('button',{name:'提交检查',exact:true}).click();await page.getByText('本次已完成',{exact:true}).waitFor();assert.equal(writes,1);
  await page.getByRole('button',{name:'历史检查记录',exact:true}).click();
  const history=page.getByRole('region',{name:'历史检查记录',exact:true});
  await history.locator('.n-collapse-item').nth(19).waitFor();
  await history.locator('.n-collapse-item__header').first().click();
  await history.getByText('备注：历史逐项异常备注\n已通知工程师',{exact:true}).waitFor();
  assert.equal(await history.getByText('正常',{exact:true}).count(),1);assert.equal(await history.getByText('异常',{exact:true}).count(),1);
  assert.equal(await history.getByRole('button',{name:'提交检查',exact:true}).count(),0,'history is read-only');
  assert.equal(await history.getByRole('button',{name:'上传照片',exact:true}).count(),0,'history cannot mutate attachments');
  for(const width of [1366,360,375]){await page.setViewportSize({width,height:width<700?640:900});await page.locator('#stage').evaluate(el=>{el.style.width=innerWidth<700?'100%':'1120px';el.style.height='calc(100dvh - 20px)';});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(screenshots,'history-'+width+'.png'),fullPage:true});}
  await history.locator('.n-pagination-item').filter({hasText:/^2$/}).click();
  await history.getByText('A楼历史检查人22',{exact:true}).waitFor();assert.equal(await history.locator('.n-collapse-item').count(),3,'all old records are accessible, not just the current shift');
  historyMalformed=true;await page.getByRole('button',{name:'刷新历史检查记录',exact:true}).click();await history.getByRole('alert').filter({hasText:'历史检查记录返回不完整，请重新读取。'}).waitFor();assert.equal(await history.locator('.n-collapse-item').count(),3,'a failed read retains the last confirmed history');
  historyMalformed=false;await history.getByRole('button',{name:'重试',exact:true}).click();await history.getByRole('alert').waitFor({state:'detached'});
  await page.getByRole('button',{name:'返回检查',exact:true}).click();await page.getByText('本次已完成',{exact:true}).waitFor();assert.equal(writes,1,'history never resubmits completed work');
  submitted=false;photo=null;
  await page.setViewportSize({width:1440,height:900});
  await page.goto(origin+'/?legacy&limited&editable');await page.locator('.legacy-pre-shift-buildings fieldset').nth(5).waitFor();
  assert.equal(await page.locator('.legacy-pre-shift-buildings>.v2-shift-check').count(),2,'legacy lists exactly the two authorized buildings');
  assert.equal(await page.getByRole('combobox',{name:'检查楼栋'}).count(),0,'legacy does not require selecting buildings');
  assert.equal(await page.locator('.v2-check-overview').count(),0,'legacy does not duplicate the all-building overview');
  await page.screenshot({path:path.join(screenshots,'legacy-authorized-pc.png'),fullPage:true});
  for(const scope of ['A','B'])await page.getByRole('region',{name:scope+'楼班前检查',exact:true}).locator('fieldset').first().locator('.n-radio-button').filter({hasText:'无异常'}).click();
  await page.getByRole('button',{name:'返回',exact:true}).click();
  await page.getByRole('button',{name:'放弃并离开',exact:true}).click();
  await page.getByRole('button',{name:'放弃并离开',exact:true}).last().waitFor();
  assert(new URL(page.url()).searchParams.has('legacy'),'one building cannot bypass another building\'s unsaved guard');
  await page.getByRole('button',{name:'放弃并离开',exact:true}).last().click();
  await page.waitForURL(origin+'/');
  await page.goto(origin+'/?legacy&role=operator');await page.locator('fieldset').nth(2).waitFor();
  assert.equal(await page.locator('.legacy-pre-shift-buildings>.v2-shift-check').count(),1);
  await page.setViewportSize({width:360,height:640});await page.locator('#stage').evaluate(el=>{el.style.width='100%';el.style.height='calc(100dvh - 20px)';});
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await page.screenshot({path:path.join(screenshots,'legacy-operator-360.png'),fullPage:true});
  assert.deepEqual(errors,[]);console.log('PASS: adaptive checklists, paginated read-only history with per-item results/notes/photos, failed reads, 360/375/390px, and legacy building/draft isolation. No production writes or dist changes.');
}catch(error){if(page)await page.screenshot({path:path.join(screenshots,'failure.png'),fullPage:true});throw error;}
finally{await browser.close();await server.close();}
