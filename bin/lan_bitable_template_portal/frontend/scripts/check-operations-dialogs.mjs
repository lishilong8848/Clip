import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {operationsFixture} from './fixtures/operations-dialogs.mjs';
import {auditLearning,auditSharing} from './operations-learning-dialogs.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const assets=process.env.DIALOG_AUDIT_ASSETS;
assert(assets,'Set DIALOG_AUDIT_ASSETS to the flat isolated V2 build directory');
const out=path.resolve(process.env.DIALOG_AUDIT_OUT||path.resolve(root,'../../../output/playwright/operations-dialogs'));
const only=process.env.DIALOG_AUDIT_ONLY?.split(',')||[];
const report=[];
await mkdir(out,{recursive:true});
const entryPath='/assets/workspace-v2/index.html';
async function serverFor(fixture){
  const srv=createServer(async(req,res)=>{
    const url=new URL(req.url,'http://127.0.0.1');
    try{
      if(url.pathname.startsWith('/api/')){
        const chunks=[];for await(const c of req)chunks.push(c);const raw=Buffer.concat(chunks).toString();
        const body=raw&&req.headers['content-type']?.includes('application/json')?JSON.parse(raw):{};
        const result=await fixture.handle(url,req.method,body);
        res.writeHead(result.status||200,{'Content-Type':result.contentType||'application/json;charset=utf-8','Cache-Control':'no-store'});res.end(result.json?JSON.stringify(result.json):result.body);return;
      }
      const name=url.pathname.replace('/assets/workspace-v2/','');
      if(!/^[\w.-]+$/.test(name)||name==='..'){res.writeHead(404).end();return;}
      const file=await readFile(path.join(assets,name));res.writeHead(200,{'Content-Type':({'.js':'application/javascript','.html':'text/html','.css':'text/css','.json':'application/json','.png':'image/png','.svg':'image/svg+xml'})[path.extname(name)]||'application/octet-stream'});res.end(file);
    }catch(e){res.writeHead(e.code==='ENOENT'?404:500).end();}
  });
  // This host also allocates low ephemeral ports, some of which browsers refuse.
  for(let port=18080; ;port++){
    try{
      await once(srv.listen(port,'127.0.0.1'),'listening');
      return srv;
    }catch(error){if(error.code!=='EADDRINUSE'||port>=18110)throw error;}
  }
}
const button=(root,name)=>root.getByRole('button',{name,exact:true});
async function zhihang({page,go,capture}){
  await go('/zhihang-data');const menu=page.getByRole('group',{name:'选择任务类型'});const dialog=page.getByRole('dialog');
  for(const [kind,label] of [['risk','风险排查'],['change','变更'],['drill','演练'],['event','事件'],['inspection','巡检']]){
    await menu.getByRole('button',{name:new RegExp('^'+label)}).click();await button(page,'定时设置').click();
    await dialog.getByLabel('运行时间（北京时间）').waitFor();await capture('zh-settings-'+kind,label+'定时设置',dialog);
    if(kind==='inspection')await button(dialog,'保存设置').click();else await page.keyboard.press('Escape');await dialog.waitFor({state:'hidden'});
  }
  await button(page,'查看巡检执行详情').click();await dialog.getByText('失败阶段',{exact:true}).waitFor();await capture('zh-failure','拉取失败阶段与核验',dialog);
  await page.keyboard.press('Escape');await button(page,'拉取预览').click();await dialog.getByText('冷却水泵轴承温度',{exact:true}).waitFor();await capture('zh-preview','拉取预览与明细',dialog);
  await button(dialog,'同步原表').click();const confirm=page.getByRole('dialog',{name:'确认同步原表'});await capture('zh-confirm','原表同步二次确认',confirm);
  await button(confirm,'取消').click();await button(dialog,'同步原表').click();await button(confirm,'确认同步').click();await dialog.getByText('待核验',{exact:true}).waitFor();
  await capture('zh-pending','同步回读中',dialog);await dialog.getByText('已发送',{exact:true}).waitFor();await capture('zh-receipt','同步记录与通知回执',dialog);await page.keyboard.press('Escape');
  await button(page,'查看同步').click();await dialog.getByText('已发送',{exact:true}).waitFor();await capture('zh-reopen','从任务回执打开最近同步',dialog);
}
async function events({page,go,capture}){
  await go('/&mode=events&scope=H&detail=1');const dialog=page.getByRole('dialog');
  for(const [key,label] of [['repairing','检修中'],['i3','I3'],['i2plus','I2及以上']]){
    await page.locator('[data-metric='+key+']').click();await capture('ev-'+key,label+'指标记录',dialog);
    if(key==='i2plus')await dialog.getByRole('button',{name:/H楼冷却水泵运行告警/}).click();else await button(dialog,'关闭').click();
  }
  await dialog.getByText('检修办理',{exact:true}).waitFor();await capture('ev-details','指标记录打开事件概况',dialog);
  for(const [key,label] of [['timeline','时间线'],['fields','原表字段']]){
    await button(dialog,label).click();await capture('ev-'+key,'事件'+label,dialog);
  }
  await button(dialog,'关闭').click();await page.locator('.ops-event-row').first().click();await button(dialog,'标记转检修').click();
  await button(dialog,'填写维修单').waitFor();await capture('ev-transfer','台账打开事件并标记转检修',dialog);
  await button(dialog,'填写维修单').click();await page.waitForURL(u=>u.searchParams.get('__page')==='/repair-management'&&u.searchParams.get('from_event_record_id')==='evt-audit');
}
async function daily({page,go,capture}){
  await go('/daily-tasks&scope=H');const dialog=page.getByRole('dialog');
  await button(page,'发送今日汇总').click();await dialog.locator('input').first().fill('隔离');await dialog.getByText('隔离接收人',{exact:true}).first().click();
  await capture('day-send','汇总发送 · 已选接收人',dialog);await button(dialog,'确认发送').click();await dialog.getByText('已成功发送给 1 人。').waitFor();await capture('day-sent','汇总发送回执',dialog);await page.keyboard.press('Escape');
  await button(page,'晨会表格').click();await dialog.getByLabel('干球温度（℃）').fill('21.5');await capture('day-morning','晨会预览与环境读数',dialog);await button(dialog,'生成表格').click();
  await dialog.getByText('晨会表格已生成，可下载或打印。').waitFor();await capture('day-generated','已生成晨会表格',dialog);
  const pop=page.context().waitForEvent('page');await button(dialog,'打印').click();const print=await pop;await print.locator('.ops-desk--print').getByText(/冷却|机房/).first().waitFor();await capture('day-print','晨会独立打印内容',print.locator('.ops-desk--print'));await print.close();
}
async function cold({page,go,capture}){
  await go('/cold-source-monitor');const desk=page.getByRole('region',{name:'冷源运行状态'});await desk.getByText('21.5',{exact:false}).first().waitFor();await capture('cold-readings','冷源楼栋运行读数',desk);
  await button(desk,'通知记录').click();await desk.getByText('备用通知通道回执待核实').waitFor();await capture('cold-history','冷源通知记录与目标回执',desk);await button(desk,'下一页').click();await desk.getByText(/第 2 页/).waitFor();await capture('cold-page','冷源通知末页',desk);
}
const flows={zhihang,events,daily,learning:auditLearning,sharing:auditSharing,cold};
if(process.argv.includes('--serve')){
  const srv=await serverFor(operationsFixture());console.log('LOCAL '+ 'http://127.0.0.1:'+srv.address().port+entryPath);await new Promise(()=>{});
}
const browser=await chromium.launch();
try{
  for(const width of [1440,390]){
    for(const [module,run] of Object.entries(flows)){
      if(only.length&&!only.includes(module))continue;
      const fixture=operationsFixture(),srv=await serverFor(fixture),origin='http://127.0.0.1:'+srv.address().port;
      const ctx=await browser.newContext({viewport:{width,height:width===390?844:950},deviceScaleFactor:1,reducedMotion:'reduce',timezoneId:'Asia/Shanghai',serviceWorkers:'block'});
      await ctx.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
      await ctx.addInitScript(()=>{window.print=()=>{};});
      const page=await ctx.newPage(),errors=[];page.setDefaultTimeout(6500);page.on('pageerror',e=>errors.push(e.message));
      const go=route=>page.goto(origin+entryPath+'?__page='+route);
      async function capture(key,title,locator){
        await locator.waitFor({state:'visible'});const target=locator.page();await target.waitForTimeout(180);
        const box=await locator.boundingBox();assert(box&&box.width>0&&box.height>0,key+' missing surface');
        const overflow=await target.evaluate(()=>document.documentElement.scrollWidth-innerWidth);
        const filename=width+'-'+key+'.png';await target.screenshot({path:path.join(out,filename),animations:'disabled'});
        report.push({module,width,key,title,filename,overflow,box:{x:Math.round(box.x),y:Math.round(box.y),w:Math.round(box.width),h:Math.round(box.height)}});
      }
      try{await run({page,go,capture});assert.deepEqual(errors,[]);console.log('PASS',module,width);}
      catch(e){report.push({module,width,error:e.message});console.error('FAIL',module,width,e.message);console.error((await page.locator('body').innerText()).replace(/内部资料[^\n]*/g,'').trim().slice(-1150));await page.screenshot({path:path.join(out,width+'-'+module+'-failure.png')}).catch(()=>{});}
      finally{await writeFile(path.join(out,'coverage.json'),JSON.stringify(report,null,2));await ctx.close();await new Promise(done=>srv.close(done));}
    }
  }
}finally{await browser.close();}
if(report.some(r=>r.error||r.overflow>1))process.exitCode=1;

