import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/work-handover');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];
let currentPage;
try{
  for(const version of ['legacy','v2']){
    const docs=new Map(),layouts=layoutApiFixture();
    let delayedId='',releaseRead=()=>{},readStarted=()=>{},readFinished=()=>{};
    const entry=(p='')=>version==='v2'?origin+'/assets/workspace-v2/index.html?__page=/work-handover'+(p?'&id='+p:''):origin+'/work-handover'+(p?'?id='+p:'');
    const contexts=[];
    for(const who of ['alice','bob']){
      const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
      contexts.push(context);
      const name=who==='alice'?'交接人员甲':'接收人员乙';
      const publicDoc=data=>({...structuredClone(data),is_owner:who==='alice',is_recipient:who==='bob',files:data.files||[],audit:[],notification:{state:'sent'},total:data.items.length,remaining:data.items.filter(i=>!i.done&&!i.removed&&['ongoing','todo'].includes(i.kind)).length});
      await context.route('**/*',async route=>{
        const req=route.request(),u=new URL(req.url());
        if(u.origin!==origin)return route.abort();
        if(u.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
        if(u.pathname==='/assets/vnet-logo.png')return route.fulfill({body:await readFile(path.join(root,'public/assets/vnet-logo.png')),contentType:'image/png'});
        if(!u.pathname.startsWith('/api/'))return route.continue();
        if(await layouts.handle(route,who))return;
        const ok=data=>route.fulfill({json:{ok:true,data}});
        if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+who,name,level:'L4',role:'building',category:'other',home_scope:'A',scope_rules:['A'],modules:['work_handover','maintenance']},scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
        if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'handover-fixture'}});
        if(!u.pathname.startsWith('/api/work-handovers')){assert.equal(req.method(),'GET','No unrelated business writes');return ok({items:[],records:[],scopes:{},summary:{},total:0});}
        if(req.method()!=='GET')writes.push({version,path:u.pathname,body:req.postDataJSON()});
        const p=u.pathname.slice('/api/work-handovers'.length);
        if(p==='/people')return ok({items:[{record_id:'bob',name:'接收人员乙',employee_no:'002',scopes:['A']}],self:{id:who,name},scopes:['A'],modules:[{value:'maintenance',label:'维保通告'}]});
        if(p==='/sources')return ok({items:[{id:'f'.repeat(64),title:'A楼测试维保通告',kind:'ongoing',business_status:'进行中',scope:'A',module:'maintenance',period:'2026-10',source_url:'/workbench-lite?scope=A&record_id=fixture-notice',source_checked_at:'2026-10-16T10:00:00+08:00',participated:true}],warnings:[]});
        if(!p&&req.method()==='GET')return ok({items:[...docs.values()].filter(d=>u.searchParams.get('view')==='sent'?who==='alice':who==='bob'&&d.status!=='draft').map(publicDoc),total:docs.size,page:1});
        if(!p){const {id}=req.postDataJSON();const d={id,title:'工作交接',recipient_id:'',recipient_name:'',owner:{id:'alice',name},status:'draft',version:1,notes:'',items:[],created_at:'2026-10-16T10:00:00+08:00',updated_at:'2026-10-16T10:00:00+08:00'};docs.set(id,d);return ok(publicDoc(d));}
        const id=p.split('/')[1],d=docs.get(id);assert(d,'Known handover');
        if(who==='alice'&&req.method()==='GET'&&id===delayedId){
          delayedId='';const snapshot=publicDoc(d);
          await new Promise(resolve=>{releaseRead=resolve;readStarted();});
          await ok(snapshot);readFinished();return;
        }
        if(p.endsWith('/refresh'))return ok(publicDoc(d));
        if(req.method()==='PUT'){
          const body=req.postDataJSON();assert.equal(body.version,d.version);Object.assign(d,body.data,{recipient_name:'接收人员乙'});d.version++;
          for(const i of d.items)if(i.source_id)Object.assign(i,{module:'maintenance',business_status:'进行中',source_url:'/workbench-lite?scope=A&record_id=fixture-notice',source_checked_at:'2026-10-16T10:00:00+08:00',period:'2026-10'});
          return ok(publicDoc(d));
        }
        if(p.endsWith('/actions')){
          const {version:v,action,item_id}=req.postDataJSON();assert.equal(v,d.version);
          if(action==='send'){d.status='pending';d.sent_version=d.version+1;}
          if(action==='accept'){assert.equal(who,'bob');d.status='accepted';d.decision_by=name;d.decision_at='2026-10-16T10:01:00+08:00';}
          if(action==='retry-notification'){d.notification_failures=[];return ok(publicDoc(d));}
          const i=d.items.find(i=>i.id===item_id);
          if(action==='complete'||action==='reopen')Object.assign(i,{done:action==='complete',confirmed_by:name,confirmed_at:'2026-10-16T10:02:00+08:00'});
          if(action==='remove'||action==='restore')Object.assign(i,{removed:action==='remove',removed_by:name,removed_at:'2026-10-16T10:03:00+08:00'});
          d.version++;return ok(publicDoc(d));
        }
        return ok(publicDoc(d));
      });
    }
    const sender=await contexts[0].newPage();currentPage=sender;
    sender.on('pageerror',e=>errors.push(e.message));sender.on('dialog',dialog=>dialog.accept());
    await sender.goto(version==='legacy'?origin+'/?entry=tools':entry());
    if(version==='legacy')await sender.getByRole('button',{name:/人员工作交接/}).click();
    else await sender.locator('.v2-sidebar').getByText('人员工作交接',{exact:true}).waitFor();
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.getByLabel('交接名称',{exact:true}).fill('A楼岗位工作交接');
    await sender.locator('.handover-fields .n-select').first().click();
    await sender.locator('.n-base-select-option').filter({hasText:'接收人员乙'}).click();
    await sender.getByRole('button',{name:'添加事项',exact:true}).click();
    await sender.locator('.handover-modal').getByLabel('事项名称',{exact:true}).fill('核对维修项目资料');
    await sender.locator('.handover-modal').getByLabel('当前进度、下一步与注意事项',{exact:true}).fill('已整理现场记录，下一步核对维护单。');
    await sender.getByRole('button',{name:'加入清单',exact:true}).click();
    await sender.getByRole('button',{name:'从业务添加',exact:true}).click();
    await sender.getByRole('button',{name:'读取业务',exact:true}).click();
    await sender.locator('.handover-source-row').getByRole('checkbox').check();
    await sender.getByRole('button',{name:'添加已选 1 项',exact:true}).click();
    await sender.getByRole('button',{name:'保存草稿',exact:true}).click();
    await sender.getByText('交接草稿已保存',{exact:true}).waitFor();
    assert.equal(docs.size,1);const d=[...docs.values()][0];assert.equal(d.items.length,2);
    await sender.screenshot({path:path.join(output,version+'-draft-pc.png')});
    await sender.getByRole('button',{name:'发送交接',exact:true}).click();
    await sender.getByRole('dialog').getByRole('button',{name:'确认',exact:true}).click();
    await sender.getByText('已提交交接，等待对方本人接收',{exact:true}).waitFor();
    assert.equal(d.status,'pending');
    delayedId=d.id;
    const started=new Promise(resolve=>{readStarted=resolve;}),finished=new Promise(resolve=>{readFinished=resolve;});
    await sender.locator('.handover-list-row').filter({hasText:'A楼岗位工作交接'}).click();
    await started;
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.getByLabel('交接名称',{exact:true}).fill('新草稿不能被旧读取覆盖');
    releaseRead();await finished;
    await sender.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    assert.equal(await sender.getByLabel('交接名称',{exact:true}).inputValue(),'新草稿不能被旧读取覆盖');
    await sender.getByRole('button',{name:'保存草稿',exact:true}).click();
    await sender.getByText('交接草稿已保存',{exact:true}).waitFor();
    const receiver=await contexts[1].newPage();currentPage=receiver;
    receiver.on('pageerror',e=>errors.push(e.message));receiver.on('dialog',dialog=>dialog.accept());
    await receiver.goto(entry(d.id));
    await receiver.getByRole('button',{name:'确认接收',exact:true}).click();
    await receiver.getByRole('dialog').getByRole('button',{name:'确认',exact:true}).click();
    await receiver.getByText('已接收，事项已加入接续清单',{exact:true}).waitFor();
    await receiver.getByRole('checkbox',{name:'确认完成：核对维修项目资料',exact:true}).click();
    await receiver.getByText('已确认清单事项完成',{exact:true}).waitFor();
    await receiver.getByRole('tab',{name:/已完成/}).click();
    await receiver.locator('.handover-item').getByRole('button',{name:'移除',exact:true}).click();
    await receiver.getByText('已从接续清单移除，原业务记录保留',{exact:true}).waitFor();
    await receiver.getByRole('tab',{name:/已移除/}).click();
    await receiver.locator('.handover-item').getByRole('button',{name:'恢复',exact:true}).click();
    await receiver.getByText('事项已恢复',{exact:true}).waitFor();
    await receiver.getByRole('tab',{name:/已完成/}).click();
    await receiver.getByRole('checkbox',{name:'恢复未完成：核对维修项目资料',exact:true}).click();
    await receiver.getByText('已恢复为未完成',{exact:true}).waitFor();
    await receiver.getByRole('tab',{name:/待处理/}).click();
    assert.equal(d.items.find(i=>i.source_id)?.business_status,'进行中','Checklist actions never complete source business');
    d.notification_failures=['result','refresh'];
    const acceptedVersion=d.version;
    await receiver.reload();
    await receiver.getByRole('button',{name:'重试通知',exact:true}).click();
    await receiver.getByText('已安排重试通知',{exact:true}).waitFor();
    assert.equal(d.version,acceptedVersion,'Retry never repeats business acceptance');
    assert.equal(d.status,'accepted');
    for(const width of [1440,1366,390,360,430]){
      await receiver.setViewportSize({width,height:width>700?900:844});
      assert.equal(await receiver.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,version+' fits '+width);
      await receiver.screenshot({path:path.join(output,`${version}-received-${width}.png`)});
    }
    for(const context of contexts)await context.close();
  }
  assert.deepEqual(errors,[]);
  assert(writes.some(w=>w.body?.action==='accept'));
  console.log('PASS: legacy/V2 lifecycle, stale-read draft protection, failed result-card retry, PC and phone. APIs isolated; no real notifications.');
}catch(e){if(currentPage)await currentPage.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw e;}
finally{await browser.close();await server.close();}
