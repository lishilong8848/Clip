import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const outDir=path.resolve(root,process.env.WORK_HANDOVER_DIST||'dist');
const output=path.resolve(root,process.env.WORK_HANDOVER_RESULTS||'../../../output/playwright/work-handover');
await mkdir(output,{recursive:true});
const devOrigin=process.env.WORKFLOW_DETAIL_ORIGIN;
if(devOrigin){assert.equal(new URL(devOrigin).hostname,'127.0.0.1');assert.equal((await (await fetch(new URL('/api/health',devOrigin))).json()).instance_id,'workflow-detail-isolated');}
const server=devOrigin?null:await preview({root,configFile:false,logLevel:'error',build:{outDir},preview:{host:'127.0.0.1',port:0}});
const origin=devOrigin||'http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];
let currentPage;
try{
  for(const version of (devOrigin?['v2']:['legacy','v2'])){
    const docs=new Map(),layouts=layoutApiFixture();
    let category='other',defaultFailure=false,defaultReads=0,directoryFailure=false,restrictSenderSource=false;
    let pauseDefaults=false,defaultStarted=()=>{},releaseDefaults=()=>{},defaultFinished=()=>{};
    const responsibilities=()=>category==='engineer'?['repair_management','risk_inspection']:category==='supervisor'?['event','repair_management','risk_inspection','cabinet']:[];
    const roleSources=()=>responsibilities().map((module,i)=>({id:String(i+1).repeat(64),
      title:({repair_management:'A楼冷机未完成维修',risk_inspection:'A楼配电风险排查',event:'A楼未闭环事件',cabinet:'A楼待处理上电批次'})[module],
      kind:i%2?'ongoing':'todo',business_status:i%2?'进行中':'未完成',scope:'A',module,period:'2026-10',
      source_url:'/'+module,source_checked_at:'2026-10-17T10:00:00+08:00',participated:false,responsible:true}));
    let delayedId='',releaseRead=()=>{},readStarted=()=>{},readFinished=()=>{};
    const entry=(p='')=>version==='v2'?origin+'/assets/workspace-v2/index.html?__page=/work-handover'+(p?'&id='+p:''):origin+'/work-handover?frontend=legacy'+(p?'&id='+p:'');
    const contexts=[];
    for(const who of ['alice','bob']){
      const context=await browser.newContext({viewport:process.env.WORKFLOW_PHONE?{width:390,height:844}:{width:1440,height:900},reducedMotion:'reduce'});
      contexts.push(context);
      const name=who==='alice'?'交接人员甲':'接收人员乙';
      const publicDoc=data=>{
        const doc=structuredClone(data);
        if(restrictSenderSource&&doc.owner.id===who&&doc.items.some(i=>i.source_id)){
          doc.title='部分交接事项权限已变更';doc.notes='';
          for(const i of doc.items)if(i.source_id)Object.assign(i,{title:'该事项的业务权限已变更',notes:'',source_url:'',restricted:true});
        }
        return {...doc,is_owner:doc.owner.id===who,is_recipient:doc.recipient_id===who,files:doc.files||[],audit:[],
          notification:{state:'sent'},total:doc.items.length,remaining:doc.items.filter(i=>!i.done&&!i.removed&&['ongoing','todo'].includes(i.kind)).length};
      };
      await context.route('**/*',async route=>{
        const req=route.request(),u=new URL(req.url());
        if(u.origin!==origin)return route.abort();
        if(u.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
        if(u.pathname==='/assets/vnet-logo.png')return route.fulfill({body:await readFile(path.join(root,'public/assets/vnet-logo.png')),contentType:'image/png'});
        if(!u.pathname.startsWith('/api/'))return route.continue();
        if(await layouts.handle(route,who))return;
        const ok=data=>route.fulfill({json:{ok:true,data}});
        if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+who,name,level:category==='supervisor'?'L3':'L4',role:'building',category,home_scope:'A',scope_rules:['A'],modules:['work_handover','maintenance',...responsibilities()]},scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
        if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'handover-fixture'}});
        if(!u.pathname.startsWith('/api/work-handovers')){assert.equal(req.method(),'GET','No unrelated business writes');return ok({items:[],records:[],scopes:{},summary:{},total:0});}
        if(req.method()!=='GET')writes.push({version,who,path:u.pathname,body:req.postDataJSON()});
        const p=u.pathname.slice('/api/work-handovers'.length);
        if(p==='/people'&&directoryFailure)return route.fulfill({status:503,json:{ok:false,error:'人员目录暂未读取成功'}});
        if(p==='/people')return ok({items:[{record_id:'bob',name:'接收人员乙',employee_no:'002',scopes:['A'],position:'暖通工程师',selectable:true},
          {record_id:'revoked',name:'无交接权限人员',employee_no:'003',scopes:['A'],selectable:false,unavailable_reason:'尚无人员工作交接权限'}],
          self:{id:who,name,category,home_scope:'A'},scopes:['A'],default_modules:responsibilities(),
          modules:[{value:'maintenance',label:'维保通告'},...responsibilities().map(value=>({value,label:({repair_management:'维修项目',risk_inspection:'风险排查',event:'事件',cabinet:'机柜上下电'})[value]}))]});
        if(p==='/sources'){
          if(u.searchParams.get('mode')==='responsibilities'){
            defaultReads++;assert.equal(u.searchParams.get('scope'),'A');
            if(pauseDefaults){
              pauseDefaults=false;
              await new Promise(resolve=>{releaseDefaults=resolve;defaultStarted();});
              await ok({items:[{...roleSources()[0],id:'e'.repeat(64),title:'过期岗位事项不得串入新草稿'}],warnings:[],failed_modules:[]});
              defaultFinished();return;
            }
            if(defaultFailure)return route.fulfill({status:503,json:{ok:false,error:'岗位事项暂未读取成功'}});
            return ok({items:roleSources(),warnings:[],failed_modules:[]});
          }
          return ok({items:category!=='other'?roleSources():[{id:'f'.repeat(64),title:'A楼测试维保通告',kind:'ongoing',business_status:'进行中',scope:'A',module:'maintenance',period:'2026-10',source_url:'/workbench-lite?scope=A&record_id=fixture-notice',source_checked_at:'2026-10-16T10:00:00+08:00',participated:true}],warnings:[]});
        }
        if(!p&&req.method()==='GET'){
          const view=u.searchParams.get('view');
          const visible=[...docs.values()].filter(d=>d.status!=='deleted'&&(view==='sent'?d.owner.id===who
            :d.recipient_id===who&&!['draft','withdrawn'].includes(d.status)&&(view!=='pending'||d.status==='pending')));
          return ok({items:visible.map(publicDoc),total:visible.length,page:1});
        }
        if(!p){assert.equal(req.method(),'POST');const {id}=req.postDataJSON();const d={id,title:'工作交接',recipient_id:'',recipient_name:'',owner:{id:who,name},status:'draft',version:1,notes:'',items:[],created_at:'2026-10-16T10:00:00+08:00',updated_at:'2026-10-16T10:00:00+08:00'};docs.set(id,d);return ok(publicDoc(d));}
        const id=p.split('/')[1],d=docs.get(id);assert(d,'Known handover');
        const owner=d.owner.id===who,recipient=d.recipient_id===who;
        if(!owner&&(!recipient||['draft','withdrawn','deleted'].includes(d.status)))return route.fulfill({status:403,json:{ok:false,error:'无权查看此交接单。'}});
        if(who==='alice'&&req.method()==='GET'&&id===delayedId){
          delayedId='';const snapshot=publicDoc(d);
          await new Promise(resolve=>{releaseRead=resolve;readStarted();});
          await ok(snapshot);readFinished();return;
        }
        if(p.endsWith('/refresh')){assert.equal(req.method(),'POST');return ok(publicDoc(d));}
        if(req.method()==='PUT'){
          assert(owner&&['draft','returned','withdrawn'].includes(d.status),'Only the sender may edit an editable document');
          const body=req.postDataJSON();assert.equal(body.version,d.version);Object.assign(d,body.data,{recipient_name:'接收人员乙'});d.version++;
          for(const i of d.items)if(i.source_id)Object.assign(i,{module:'maintenance',business_status:'进行中',source_url:'/workbench-lite?scope=A&record_id=fixture-notice',source_checked_at:'2026-10-16T10:00:00+08:00',period:'2026-10'});
          return ok(publicDoc(d));
        }
        if(p.endsWith('/actions')){
          assert.equal(req.method(),'POST');
          const {version:v,action,item_id,reason}=req.postDataJSON();assert.equal(v,d.version);
          if(action==='delete_draft'){assert.equal(who,'alice');assert.equal(d.status,'draft');assert(!d.sent_version);d.status='deleted';return ok({id:d.id,deleted:true});}
          if(action==='send'){assert(owner&&['draft','returned','withdrawn'].includes(d.status));assert.equal(d.recipient_id,'bob');assert(d.items.length);d.status='pending';d.sent_version=d.version+1;}
          if(action==='withdraw'){assert(owner&&d.status==='pending');d.status='withdrawn';}
          if(action==='accept'||action==='return'){
            assert(recipient&&d.status==='pending');
            if(action==='return')assert(reason.trim(),'A return requires a reason');
            d.status=action==='accept'?'accepted':'returned';d.decision_by=name;d.decision_reason=reason;d.decision_at='2026-10-16T10:01:00+08:00';
          }
          if(action==='retry-notification'){d.notification_failures=[];return ok(publicDoc(d));}
          const i=d.items.find(i=>i.id===item_id);
          if(['complete','reopen','remove','restore'].includes(action))assert(recipient&&d.status==='accepted'&&i&&!i.restricted);
          if(action==='complete'||action==='reopen')Object.assign(i,{done:action==='complete',confirmed_by:name,confirmed_at:'2026-10-16T10:02:00+08:00'});
          if(action==='remove'||action==='restore')Object.assign(i,{removed:action==='remove',removed_by:name,removed_at:'2026-10-16T10:03:00+08:00'});
          d.version++;return ok(publicDoc(d));
        }
        assert.equal(req.method(),'GET','Only a detail read may fall through');
        assert.equal(p,'/'+id);return ok(publicDoc(d));
      });
    }
    const sender=await contexts[0].newPage();currentPage=sender;
    sender.on('pageerror',e=>errors.push(e.message));sender.on('dialog',dialog=>dialog.accept());
    async function evidence(page,state){
      if(!devOrigin)return;
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,state+' fits viewport');
      await page.screenshot({path:path.join(output,state+'.png'),animations:'disabled'});
    }
    const confirmButton=(page,label)=>page.getByRole('dialog').getByRole('button',{name:version==='v2'?label:'确认',exact:true});
    await sender.goto(version==='legacy'?origin+'/?entry=special&frontend=legacy':entry());
    if(version==='legacy'){
      await sender.getByRole('heading',{name:'专项任务',exact:true}).waitFor();
      assert.equal(await sender.locator('.tool-card').count(),1,'Work-handover permission alone exposes no other special task');
      await sender.getByRole('button',{name:'选择人员工作交接',exact:true}).click();
      await sender.getByRole('region',{name:'人员工作交接',exact:true}).getByRole('button',{name:'返回',exact:true}).click();
      await sender.waitForURL(origin+'/?entry=special&frontend=legacy');
      await sender.getByRole('button',{name:'选择人员工作交接',exact:true}).click();
    }
    else await sender.getByRole('region',{name:'人员工作交接',exact:true}).waitFor();
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.getByLabel('交接名称',{exact:true}).fill('A楼岗位工作交接');
    await sender.locator(version==='v2'?'.transfer-recipient .n-select':'.handover-fields .n-select').first().click();
    const unavailable=sender.locator('.n-base-select-option').filter({hasText:'无交接权限人员'});
    assert.match(await unavailable.getAttribute('class'),/n-base-select-option--disabled/,'A receiver without handover permission cannot be chosen');
    await sender.locator(version==='v2'?'.transfer-recipient input':'.handover-recipient input').fill('002');
    assert.equal(await sender.locator('.n-base-select-option').filter({hasText:'接收人员乙'}).count(),1,'Employee number search finds receiver');
    await sender.locator('.n-base-select-option').filter({hasText:'接收人员乙'}).click();
    assert(await sender.getByRole('button',{name:'发送交接',exact:true}).isDisabled(),'A recipient alone is insufficient to send an empty document');
    await sender.getByRole('button',{name:'添加事项',exact:true}).click();
    await sender.locator(version==='v2'?'.transfer-dialog':'.handover-modal').getByLabel('事项名称',{exact:true}).fill('核对维修项目资料');
    await sender.locator(version==='v2'?'.transfer-dialog':'.handover-modal').getByLabel('当前进度、下一步与注意事项',{exact:true}).fill('已整理现场记录，下一步核对维护单。');
    await evidence(sender,'交接-事项编辑');
    await sender.getByRole('button',{name:'加入清单',exact:true}).click();
    await sender.getByRole('button',{name:'从业务添加',exact:true}).click();
    await sender.getByRole('button',{name:'读取业务',exact:true}).click();
    await sender.locator(version==='v2'?'[data-handover-source]':'.handover-source-row').getByRole('checkbox').check();
    await evidence(sender,'交接-业务选择');
    await sender.getByRole('button',{name:'添加已选 1 项',exact:true}).click();
    await sender.getByRole('button',{name:'保存草稿',exact:true}).click();
    await sender.getByText('交接草稿已保存',{exact:true}).waitFor();
    assert.equal(docs.size,1);const d=[...docs.values()][0];assert.equal(d.items.length,2);
    await sender.screenshot({path:path.join(output,version+'-draft-pc.png')});
    await sender.getByRole('button',{name:'发送交接',exact:true}).click();
    await evidence(sender,'交接-发送确认');
    await confirmButton(sender,'发送工作交接').click();
    await sender.getByText('已提交交接，等待对方本人接收',{exact:true}).waitFor();
    assert.equal(d.status,'pending');
    assert.equal(await sender.getByRole('button',{name:'确认接收',exact:true}).count(),0);
    const receiver=await contexts[1].newPage();currentPage=receiver;
    receiver.on('pageerror',e=>errors.push(e.message));receiver.on('dialog',dialog=>dialog.accept());
    await receiver.goto(entry(d.id));
    await receiver.getByRole('button',{name:'确认接收',exact:true}).waitFor();
    assert.equal(await receiver.getByRole('button',{name:'撤回交接',exact:true}).count(),0);
    currentPage=sender;
    await sender.getByRole('button',{name:'撤回交接',exact:true}).click();
    await evidence(sender,'交接-撤回确认');
    await confirmButton(sender,'撤回工作交接').click();
    await sender.getByText('交接已撤回',{exact:true}).waitFor();
    assert.equal(d.status,'withdrawn');
    assert.equal(await sender.getByRole('button',{name:'删除草稿',exact:true}).count(),0,'A previously sent document cannot be deleted as an unsent draft');
    currentPage=receiver;
    await receiver.reload();
    await receiver.getByText('无权查看此交接单。',{exact:true}).waitFor();
    assert.equal(await receiver.getByRole('button',{name:'确认接收',exact:true}).count(),0);
    assert.equal(await receiver.getByText(d.title,{exact:true}).count(),0,'Withdrawn document is absent from both list and detail');
    currentPage=sender;
    await sender.getByRole('button',{name:'发送交接',exact:true}).click();
    await confirmButton(sender,'发送工作交接').click();
    await sender.getByText('已提交交接，等待对方本人接收',{exact:true}).waitFor();
    currentPage=receiver;
    await receiver.reload();
    await receiver.getByRole('button',{name:'退回补充',exact:true}).click();
    assert(await confirmButton(receiver,'退回补充').isDisabled(),'A return without a reason cannot be submitted');
    const reason='请补充运行记录的交接说明';
    await receiver.getByRole('dialog').getByRole('textbox').fill(reason);
    await evidence(receiver,'交接-退回原因');
    await confirmButton(receiver,'退回补充').click();
    await receiver.getByText('已退回补充',{exact:true}).waitFor();
    assert.equal(d.status,'returned');assert.equal(d.decision_reason,reason);
    currentPage=sender;
    await sender.getByRole('button',{name:'刷新',exact:true}).click();
    await sender.getByText(reason,{exact:false}).waitFor();
    assert(await sender.getByLabel('交接名称',{exact:true}).isEditable());
    restrictSenderSource=true;
    await sender.getByRole('button',{name:'刷新',exact:true}).click();
    const restrictedRow=sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').filter({hasText:'该事项的业务权限已变更'});
    await restrictedRow.waitFor();
    await evidence(sender,'交接-来源权限变更');
    assert.equal(await restrictedRow.getByRole('button',{name:'打开业务',exact:true}).count(),0);
    const beforeRemoval=writes.length;
    await restrictedRow.getByRole('button',{name:version==='v2'?'移除事项':'移除',exact:true}).click();
    await restrictedRow.waitFor({state:'detached'});
    assert.equal(writes.length,beforeRemoval,'Removing a restricted item from an editable draft is local until saved');
    assert.equal(d.items.length,2);
    restrictSenderSource=false;
    await sender.getByRole('button',{name:'刷新',exact:true}).click();
    await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').filter({hasText:'A楼测试维保通告'}).waitFor();
    await sender.getByRole('button',{name:'发送交接',exact:true}).click();
    await confirmButton(sender,'发送工作交接').click();
    await sender.getByText('已提交交接，等待对方本人接收',{exact:true}).waitFor();
    delayedId=d.id;
    const started=new Promise(resolve=>{readStarted=resolve;}),finished=new Promise(resolve=>{readFinished=resolve;});
    if(version==='v2')await sender.getByRole('button',{name:'返回交接清单',exact:true}).click();
    await sender.locator(version==='v2'?'.transfer-ledger>button':'.handover-list-row').filter({hasText:'A楼岗位工作交接'}).click();
    await started;
    if(version==='v2')assert(await sender.getByRole('button',{name:'返回交接清单',exact:true}).isEnabled(),'Pending reads remain cancellable');
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.getByLabel('交接名称',{exact:true}).fill('新草稿不能被旧读取覆盖');
    releaseRead();await finished;
    await sender.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    assert.equal(await sender.getByLabel('交接名称',{exact:true}).inputValue(),'新草稿不能被旧读取覆盖');
    await sender.getByRole('button',{name:'保存草稿',exact:true}).click();
    await sender.getByText('交接草稿已保存',{exact:true}).waitFor();
    currentPage=receiver;
    await receiver.goto(entry(d.id));
    await receiver.getByRole('button',{name:'确认接收',exact:true}).click();
    await evidence(receiver,'交接-接收确认');
    await confirmButton(receiver,'确认接收交接').click();
    await receiver.getByText('已接收，事项已加入接续清单',{exact:true}).waitFor();
    if(version==='v2')await receiver.locator('[data-handover-item]').filter({hasText:'核对维修项目资料'}).getByRole('button',{name:'确认完成',exact:true}).click();
    else await receiver.getByRole('checkbox',{name:'确认完成：核对维修项目资料',exact:true}).click();
    await receiver.getByText('已确认清单事项完成',{exact:true}).waitFor();
    await receiver.getByRole(version==='v2'?'button':'tab',{name:/已完成/}).click();
    await receiver.locator(version==='v2'?'[data-handover-item]':'.handover-item').getByRole('button',{name:version==='v2'?'移除事项':'移除',exact:true}).click();
    await receiver.getByText('已从接续清单移除，原业务记录保留',{exact:true}).waitFor();
    await receiver.getByRole(version==='v2'?'button':'tab',{name:/已移除/}).click();
    await receiver.locator(version==='v2'?'[data-handover-item]':'.handover-item').getByRole('button',{name:version==='v2'?'恢复事项':'恢复',exact:true}).click();
    await receiver.getByText('事项已恢复',{exact:true}).waitFor();
    await receiver.getByRole(version==='v2'?'button':'tab',{name:/已完成/}).click();
    if(version==='v2')await receiver.locator('[data-handover-item]').filter({hasText:'核对维修项目资料'}).getByRole('button',{name:'恢复待办',exact:true}).click();
    else await receiver.getByRole('checkbox',{name:'恢复未完成：核对维修项目资料',exact:true}).click();
    await receiver.getByText('已恢复为未完成',{exact:true}).waitFor();
    await receiver.getByRole(version==='v2'?'button':'tab',{name:/待处理/}).click();
    assert.equal(d.items.find(i=>i.source_id)?.business_status,'进行中','Checklist actions never complete source business');
    d.notification_failures=['result','refresh'];
    const acceptedVersion=d.version;
    await receiver.reload();
    await receiver.getByRole('button',{name:'重试通知',exact:true}).click();
    await receiver.getByText('已安排重试通知',{exact:true}).waitFor();
    assert.equal(d.version,acceptedVersion,'Retry never repeats business acceptance');
    assert.equal(d.status,'accepted');
    assert.deepEqual(writes.filter(w=>w.version===version&&w.path==='/api/work-handovers/'+d.id+'/actions').map(w=>[w.who,w.body.action]),[
      ['alice','send'],['alice','withdraw'],['alice','send'],['bob','return'],['alice','send'],['bob','accept'],
      ['bob','complete'],['bob','remove'],['bob','restore'],['bob','reopen'],['bob','retry-notification'],
    ],'Each document transition and each checklist action is sent exactly once by the right account');
    for(const width of [1440,1366,390,360,430]){
      await receiver.setViewportSize({width,height:width>700?900:844});
      assert.equal(await receiver.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,version+' fits '+width);
      await receiver.screenshot({path:path.join(output,`${version}-received-${width}.png`)});
    }
    currentPage=sender;
    for(const role of ['engineer','supervisor']){
      category=role;await sender.goto(entry());
      await sender.getByRole('button',{name:'发起交接',exact:true}).click();
      const expected=role==='engineer'?2:4;
      await sender.locator(version==='v2'?'.transfer-form':'.handover-responsibilities').getByText('已纳入 '+expected+' 项',{exact:false}).waitFor();
      assert.equal(await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').count(),expected);
      if(role==='engineer')assert.equal(await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').filter({hasText:'未闭环事件'}).count(),0);
      await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').first().getByRole('button',{name:'编辑',exact:true}).click();
      await sender.locator(version==='v2'?'.transfer-dialog':'.handover-modal').getByLabel('当前进度、下一步与注意事项',{exact:true}).fill('人工补充的进度不能被重新读取覆盖');
      await sender.getByRole('button',{name:'加入清单',exact:true}).click();
      await sender.getByRole('button',{name:'补充岗位事项',exact:true}).click();
      await sender.locator(version==='v2'?'.transfer-form':'.handover-responsibilities').getByText('已纳入 '+expected+' 项',{exact:false}).waitFor();
      assert.equal(await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').count(),expected,'Re-read never duplicates source items');
      assert.equal(await sender.getByText('人工补充的进度不能被重新读取覆盖',{exact:true}).count(),1);
      await sender.locator(version==='v2'?'.transfer-recipient .n-select':'.handover-recipient .n-select').click();
      await sender.locator(version==='v2'?'.transfer-recipient input':'.handover-recipient input').fill('接收人员乙');
      await sender.locator('.n-base-select-option').filter({hasText:'接收人员乙'}).click();
      await sender.getByRole('button',{name:'从业务添加',exact:true}).click();
      await sender.getByRole('button',{name:'读取业务',exact:true}).click();
      await sender.getByRole('checkbox',{name:'本人参与或岗位负责',exact:true}).check();
      assert.equal(await sender.locator(version==='v2'?'[data-handover-source]':'.handover-source-row').count(),expected,'Role responsibilities stay visible without fabricated participation');
      assert.equal(await sender.getByText('有本人参与记录',{exact:false}).count(),0);
      await sender.locator(version==='v2'?'.transfer-sources .n-base-close':'.handover-source-modal .n-base-close').click();
      for(const viewport of [{width:1440,height:900},{width:390,height:844},{width:360,height:640}]){
        await sender.setViewportSize(viewport);
        assert.equal(await sender.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,version+' '+role+' fits '+viewport.width);
        await sender.screenshot({path:path.join(output,`${version}-${role}-${viewport.width}.png`)});
        await sender.getByRole('button',{name:'发送交接',exact:true}).scrollIntoViewIfNeeded();
        assert.equal(await sender.getByRole('button',{name:'发送交接',exact:true}).isEnabled(),true);
      }
      await sender.setViewportSize({width:1440,height:900});
    }
    defaultFailure=true;await sender.goto(entry());
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.getByText('岗位事项暂未读取成功；未读取成功不代表没有待交接事项，可重试或从业务添加。',{exact:false}).waitFor();
    await sender.getByLabel('交接名称',{exact:true}).fill('读取失败后仍保留填写');
    defaultFailure=false;
    await sender.getByRole('button',{name:'补充岗位事项',exact:true}).click();
    await sender.locator(version==='v2'?'.transfer-form':'.handover-responsibilities').getByText('已纳入 4 项',{exact:false}).waitFor();
    assert.equal(await sender.getByLabel('交接名称',{exact:true}).inputValue(),'读取失败后仍保留填写');
    assert(defaultReads>=6);
    pauseDefaults=true;
    const defaultStart=new Promise(resolve=>{defaultStarted=resolve;}),defaultFinish=new Promise(resolve=>{defaultFinished=resolve;});
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();await defaultStart;
    assert.equal(await sender.getByRole('button',{name:'保存草稿',exact:true}).isDisabled(),true);
    assert.equal(await sender.getByRole('button',{name:'添加资料',exact:true}).isDisabled(),true);
    await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    await sender.locator(version==='v2'?'.transfer-form':'.handover-responsibilities').getByText('已纳入 4 项',{exact:false}).waitFor();
    releaseDefaults();await defaultFinish;
    await sender.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    assert.equal(await sender.getByText('过期岗位事项不得串入新草稿',{exact:true}).count(),0);
    assert.equal(await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').count(),4);
    directoryFailure=true;await sender.goto(entry(d.id));
    await sender.locator(version==='v2'?'.transfer-desk h1':'.handover-detail h2').filter({hasText:'A楼岗位工作交接'}).waitFor();
    assert.equal(await sender.locator(version==='v2'?'[data-handover-item]':'.handover-item').count(),2,'Directory failure never hides existing handover records');
    directoryFailure=false;
    category='other';await sender.goto(entry());await sender.getByRole('button',{name:'发起交接',exact:true}).click();
    const unsent=[...docs.values()].at(-1),countBeforeDelete=docs.size;
    for(const viewport of [{width:1440,height:900},{width:375,height:667},{width:360,height:640}]){
      await sender.setViewportSize(viewport);await sender.getByRole('button',{name:'删除草稿',exact:true}).scrollIntoViewIfNeeded();
      const limit=version==='v2'&&viewport.width<700?(await sender.locator('.v2-mobile-nav').boundingBox()).y:viewport.height;
      for(const name of ['删除草稿','保存草稿','发送交接']){const rect=await sender.getByRole('button',{name,exact:true}).boundingBox();assert(rect.x>=0&&rect.x+rect.width<=viewport.width&&rect.y>=0&&rect.y+rect.height<=limit+1,`${version} ${viewport.width}: ${name} stays usable`);}
    }
    await sender.getByRole('button',{name:'删除草稿',exact:true}).click();await sender.getByRole('dialog').getByRole('button',{name:'取消',exact:true}).click();
    assert.equal(unsent.status,'draft','Cancelling deletion does not write or discard the draft');
    await sender.getByRole('button',{name:'删除草稿',exact:true}).click();await confirmButton(sender,'删除交接草稿').click();
    await sender.getByText('交接草稿已删除',{exact:true}).waitFor();assert.equal(await sender.locator(version==='v2'?'.transfer-form':'.handover-detail').count(),0);
    assert.equal(unsent.status,'deleted');assert.equal(docs.size,countBeforeDelete);assert.equal(d.status,'accepted','Sent business is untouched');
    await sender.screenshot({path:path.join(output,`${version}-draft-deleted-360.png`)});
    for(const context of contexts)await context.close();
    console.log('PASS '+version+': sender/receiver permissions, send/withdraw/return/resend/accept, item actions, drafts, stale reads and responsive controls.');
  }
  assert.deepEqual(errors,[]);
  assert(writes.some(w=>w.body?.action==='accept'));
  console.log('PASS: '+(devOrigin?'V2 source':'legacy/V2')+' receiver name/number search, role defaults, deduplication, manual edits, read-failure retry, lifecycle, stale-read protection, PC and small phones. APIs isolated; no real notifications.');
}catch(e){if(currentPage)await currentPage.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw e;}
finally{await browser.close();await server?.close();}
