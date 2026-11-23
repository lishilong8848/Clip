import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, devices } from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const dist=path.resolve(process.argv[2]||path.join(root,'dist'));
const output=path.resolve(root,'../../../output/workbench-processing-review/screens');
await mkdir(output,{recursive:true});
const scopeOptions=['A','B'].map(value=>({value,label:value+'楼'}));
const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml','.png':'image/png'};
const user={open_id:'work-status-processing-fixture',role:'admin',level:'L1',name:'工作台验收',category:'manager',
  enabled:true,home_scope:'A',modules:['maintenance','repair_management']};
const notices=['A','B'].flatMap(scope=>[1,2].map(n=>({scope,id:scope+'-notice-'+n,title:'隔离维保通告'+n,ended:false})));
const repairs=['A','B'].flatMap(scope=>[1,2].map(n=>({scope,id:scope+'-repair-'+n,title:'隔离维修跟进'+n,
  endTime:n===1?'2026-11-18 10:23':'',followup:null})));
const drafts=new Map(), jobs=new Map(), pendingRepairs=new Map(), creates=[], retries=[], saves=[], repairsRecovered=[], failures=[];
const field=(field_name,field_type=1)=>({field_name,field_type,editable:true});
const endedRepair=row=>Boolean(row.endTime&&Number(row.followup?.raw_fields['维修进度'])===1);
const repairRow=row=>({
  title:row.title,record_id:row.id,building:row.scope+'楼',building_codes:[row.scope],specialty:'电气',
  workflow:endedRepair(row)?'维修完成':'维修中',state:endedRepair(row)?'completed':'in_progress',
  repair_start_time:'2026-11-18 08:00',repair_end_time:row.endTime,followup_count:Number(!!row.followup),
  is_completed:endedRepair(row),record_version:'repair-v1',summary_only:false,raw_fields:{},display_fields:{
    '维修名称':row.title,'维修结束时间（2026）-L':row.endTime,
  },
});
const noticeRow=row=>({
  record_id:'plan-'+row.id,source_record_id:'plan-'+row.id,work_type:'maintenance',title:row.title,
  building:row.scope+'楼',building_codes:[row.scope],source_progress:row.ended?'已结束':'进行中',
  planned_start_time:'2026-11-18 08:00',planned_end_time:'2026-11-18 18:00',
});
const start='2026-11-18T08:00',end='2026-11-18T18:00';
function form(row){
  const data={scope:row.scope,work_type:'maintenance',notice_type:'维保通告',active_item_id:row.id,target_record_id:'target-'+row.id,
    source_record_id:'plan-'+row.id,site_photo_count:0};
  const values={title:['名称',row.title],content:['内容','隔离内容'],reason:['原因','定期检查'],impact:['影响','无业务影响'],
    progress:['进度','现场正在维护'],start_time:['开始时间',start],end_time:['结束时间',end]};
  return '<form id="lite-notice-form" data-action="update">'+
    Object.entries(data).map(([key,value])=>'<input type="hidden" name="'+key+'" value="'+value+'">').join('')+
    '<input type="checkbox" name="building_codes" checked value="'+row.scope+'">'+
    Object.entries(values).map(([key,[label,value]])=>'<label><span>'+label+'</span><input name="'+key+'" value="'+value+'"'+
      (key.endsWith('_time')?' type="datetime-local"':'')+' required></label>').join('')+
    '<button name="submit_action" value="update"></button><button name="submit_action" value="end"></button></form>';
}
function saveFollowup(row,body){
  assert(!row.followup,'a repair recovery must only write one followup');
  row.followup={record_id:'followup-'+row.id,summary_record_id:row.id,raw_fields:body.fields,display_fields:body.fields,
    ledger_device_ids:body.ledger_device_ids,created_time:'2026-11-18 11:00',record_version:'followup-v1'};
  return {record_id:row.followup.record_id,fields:body.fields,record_version:'followup-v1'};
}

const server=createServer(async(req,res)=>{
  const url=new URL(req.url,'http://localhost'), q=url.searchParams, ep=url.pathname;
  const scope=q.get('scope'), scoped=row=>row.scope===scope;
  function send(body,type,status=200){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(body);}
  const json=(data,status=200)=>send(JSON.stringify(data),'application/json; charset=utf-8',status);
  const ok=data=>json({ok:true,data}), reject=(error,status=400)=>json({ok:false,error},status);
  try{
    if(!ep.startsWith('/api/')){
      const asset=path.resolve(dist,'.'+ep);
      if(!asset.startsWith(dist+path.sep))return send('Unknown file','text/plain',404);
      const file=await readFile(asset).catch(()=>null);
      return file?send(file,types[path.extname(asset)]||'application/octet-stream'):send('Unknown file','text/plain',404);
    }
    if(req.method!=='GET'){
      const chunks=[];for await(const part of req)chunks.push(part);
      const body=JSON.parse(Buffer.concat(chunks).toString()||'{}');
      if(ep==='/api/workbench/draft'&&req.method==='PUT'){
        const key=[body.scope,body.target_record_id,body.action].join('|');
        assert(notices.some(row=>row.scope===body.scope&&'target-'+row.id===body.target_record_id&&!row.ended));
        const draft={...body,version:(drafts.get(key)?.version||0)+1};
        drafts.set(key,draft);
        return ok({draft});
      }
      if(ep==='/api/workbench-actions'){
        const row=notices.find(item=>item.scope===body.scope&&'target-'+item.id===body.target_record_id);
        assert(row&&!row.ended&&body.operation_id);
        assert.equal(body.action,'end');
        assert.equal(body.source_record_id,'plan-'+row.id);
        assert.equal(body.patch.progress,'隔离验收完成 '+row.id);
        const id='job-'+row.id;
        assert(!jobs.has(id),'recovery must not create another notice job');
        creates.push(row.id);
        jobs.set(id,{row,phase:row.id==='A-notice-1'?'failed':'success'});
        return ok({job_id:id,phase:'queued'});
      }
      if(/^\/api\/jobs\/[^/]+\/retry$/.test(ep)){
        const id=ep.split('/')[3], job=jobs.get(id);
        assert(job?.phase==='failed');
        retries.push(id);
        job.phase='success';
        return ok({job_id:id,phase:'queued'});
      }
      if(ep==='/api/repair-management/followups'){
        const row=repairs.find(item=>item.scope===body.scope&&item.id===body.summary_record_id);
        assert(row&&!row.followup&&body.operation_id);
        assert.deepEqual(body.ledger_device_ids,['ledger-'+row.scope]);
        assert.equal(Number(body.fields['维修进度']),1);
        assert.equal(body.fields['维修进展描述'],'隔离跟进完成 '+row.id);
        saves.push({id:row.id,operation:body.operation_id});
        if(row.id==='A-repair-1'&&saves.length===1){
          pendingRepairs.set(body.operation_id,{row,body});
          return reject('隔离保存响应中断，请核验原任务',503);
        }
        return ok(saveFollowup(row,body));
      }
      if(ep.startsWith('/api/repair-management/operations/')){
        const id=ep.split('/').pop(), pending=pendingRepairs.get(id);
        assert(pending,'repair recovery must reuse the original operation');
        pendingRepairs.delete(id);
        repairsRecovered.push(id);
        return ok({status:'completed',result:saveFollowup(pending.row,pending.body)});
      }
      throw Error('Unexpected write '+req.method+' '+ep);
    }
    if(ep==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopeOptions});
    if(ep==='/api/health')return json({ok:true,service:'clipflow_backend',instance_id:'work-status-fixture'});
    if(ep.startsWith('/api/workspace-layouts/'))return ok({value:null,revision:0});
    if(ep==='/api/personal-calendar')return ok({month:q.get('month'),days:{},unavailable:[],seal_status:'ready',seal_windows:[]});
    if(ep==='/api/workbench'){
      const rows=notices.filter(scoped);
      const ongoing=rows.filter(row=>!row.ended).map(row=>({...noticeRow(row),active_item_id:row.id,status:'开始',start_time:start}));
      return ok({scope,records:rows.map(noticeRow),ongoing,source_snapshot_ready:true,ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
    }
    if(ep==='/api/workbench/lite-detail'){
      const row=notices.find(item=>scoped(item)&&item.id===q.get('active_item_id'));
      assert(row&&!row.ended,'notice opens the correct floor and record');
      return ok({fragments:{detail:form(row)}});
    }
    if(ep==='/api/workbench/draft')return ok({draft:drafts.get([scope,q.get('target_record_id'),q.get('action')].join('|'))||null});
    if(ep==='/api/workbench/dingtalk-options')return ok({
      robots:[{id:'fixture-ding',name:'隔离钉钉群',enabled:true,scopes:[],types:[]}],selected_robot_ids:['fixture-ding'],
    });
    if(ep.startsWith('/api/jobs/')){
      const id=ep.split('/').pop(), job=jobs.get(id);
      assert(job);
      if(job.phase==='success')job.row.ended=true;
      return ok({job_id:id,phase:job.phase,error_retryable:true,can_retry:true,error:job.phase==='failed'?'隔离发送故障':''});
    }
    if(ep==='/api/repair-management/status'||ep==='/api/repair-management/records'){
      const rows=repairs.filter(scoped).filter(row=>!q.has('focus_record_id')||row.id===q.get('focus_record_id')).map(repairRow);
      return ok({fields:[field('维修名称'),field('维修结束时间（2026）-L',5)],records:rows,total:rows.length,stats:{total:rows.length}});
    }
    if(ep.startsWith('/api/repair-management/records/')){
      const row=repairs.find(item=>scoped(item)&&item.id===ep.split('/').pop());
      assert(row,'repair opens the correct floor and record');
      return ok({record:repairRow(row)});
    }
    if(ep==='/api/repair-management/followups'){
      const row=repairs.find(item=>scoped(item)&&item.id===q.get('summary_record_id'));
      assert(row);
      return ok({fields:[field('维修进展描述'),field('维修进度',2)],records:row.followup?[row.followup]:[],total:Number(!!row.followup)});
    }
    if(ep==='/api/repair-management/ledger-candidates')return ok({
      records:[{record_id:'ledger-'+scope,'设备编号':scope+'-101','设备名称':'隔离配电柜','机楼':scope+'楼'}],
      total:1,page:1,options:{},cache:{ready:true},
    });
    if(ep==='/api/repair-management/sync-status')return ok({status:'completed'});
    if(ep.startsWith('/api/repair-management/operations/')){
      assert(pendingRepairs.has(ep.split('/').pop()));
      return ok({status:'processing',retryable:false});
    }
    if(ep==='/api/repair-management/stream')return send(': isolated\n\n','text/event-stream');
    return ok({items:[],records:[],settings:{},available:false});
  }catch(error){failures.push(error.stack);reject(error.message,500);}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch();
try{
  const phone=devices['iPhone 13'];
  const context=await browser.newContext({...phone,userAgent:phone.userAgent+' Lark/7.60',reducedMotion:'reduce'});
  await context.route('**/*',route=>route.request().url().startsWith(origin+'/')?route.continue():route.abort());
  const page=await context.newPage();
  page.setDefaultTimeout(9000);
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.clock.setFixedTime(new Date('2026-11-18T03:00:00Z'));
  await page.goto(origin+'/assets/workspace-v2/index.html?scope=A');
  const status=page.getByRole('region',{name:'工作状态',exact:true});
  const queue=page.getByRole('region',{name:'待办办理',exact:true});
  const filter=(name,count)=>status.getByRole('button',{name:name+' '+count,exact:true});
  const entry=title=>status.getByRole('button',{name:new RegExp(title+'.*办理')});
  const panel=title=>page.getByRole('region',{name:'办理 '+title,exact:true});
  const tile=(kind,count)=>queue.locator('[data-group="'+kind+'"][data-count="'+count+'"]');
  async function tap(button){
    await button.scrollIntoViewIfNeeded();
    assert(await button.evaluate(el=>{
      const rect=el.getBoundingClientRect(), target=document.elementFromPoint(rect.x+rect.width/2,rect.y+rect.height/2);
      return rect.left>=-1&&rect.right<=innerWidth+1&&rect.top>=-1&&rect.bottom<=innerHeight+1&&el.contains(target);
    }),await button.textContent()+' is usable inside the mobile viewport');
    await button.tap();
  }
  async function close(title){
    await tap(panel(title).getByRole('button',{name:'关闭待办办理',exact:true}));
    await panel(title).waitFor({state:'hidden'});
  }
  async function counts(noticeCount,repairCount){
    await filter('全部',noticeCount+repairCount).waitFor();
    await filter('进行中',noticeCount+repairCount).waitFor();
    if(noticeCount)await tile('notices:维保',noticeCount).waitFor();
    else await queue.locator('[data-group="notices:维保"]').waitFor({state:'hidden'});
    await tile('repair_management',repairCount).waitFor();
  }
  async function finishNotice(row,failFirst){
    await tap(entry(row.title));
    const editor=panel(row.title);
    const endAction=editor.getByRole('radio',{name:'结束',exact:true});
    const endButton=editor.locator('.n-radio-button').filter({has:page.getByRole('radio',{name:'结束',exact:true})});
    await tap(endButton);
    assert(await endAction.isChecked());
    await tap(editor.getByRole('checkbox',{name:'隔离钉钉群',exact:true}));
    await editor.getByRole('textbox',{name:'进度',exact:true}).fill('隔离验收完成 '+row.id);
    const preview=editor.getByRole('button',{name:'预览并确认结束',exact:true});
    const [radioBox,previewBox]=await Promise.all([endButton.boundingBox(),preview.boundingBox()]);
    assert(radioBox&&previewBox&&(radioBox.y+radioBox.height<=previewBox.y+1||radioBox.x+radioBox.width<=previewBox.x+1),
      'notice footer controls do not overlap when wrapping');
    if(page.viewportSize().width===320){
      await preview.scrollIntoViewIfNeeded();
      await page.screenshot({path:path.join(output,'notice-footer-320.png')});
    }
    await tap(preview);
    const send=editor.getByRole('button',{name:'确认发送结束',exact:true});
    if(failFirst){
      await send.scrollIntoViewIfNeeded();
      await page.screenshot({path:path.join(output,'notice-confirm-390.png')});
    }
    await tap(send);
    if(failFirst){
      const retry=editor.getByRole('button',{name:'重试原任务',exact:true});
      await retry.waitFor();
      assert(!row.ended);
      await counts(2,2);
      await tap(retry);
    }
    await editor.getByRole('status',{name:'通告办理完成',exact:true}).waitFor();
    assert.equal(await page.locator('.n-message-container').getByText('草稿已保存',{exact:true}).count(),0);
    await close(row.title);
    await entry(row.title).waitFor({state:'hidden'});
  }
  async function finishRepair(row,failFirst){
    await tap(entry(row.title));
    const editor=panel(row.title);
    const description=editor.getByRole('textbox',{name:'维修进展描述',exact:true});
    await description.fill('隔离跟进完成 '+row.id);
    const progress=editor.locator('.v2-repair-progress-input input');
    await progress.fill('100');
    await progress.press('Tab');
    await tap(editor.getByRole('button',{name:'选择台账设备',exact:true}));
    const picker=page.locator('.record-picker-dialog:visible');
    await picker.getByRole('checkbox').first().check();
    await tap(picker.getByRole('button',{name:'确认',exact:true}));
    const save=editor.getByRole('button',{name:'保存本次跟进',exact:true});
    if(failFirst){
      await save.scrollIntoViewIfNeeded();
      await page.screenshot({path:path.join(output,'repair-save-390.png')});
    }
    await tap(save);
    if(failFirst){
      await editor.getByText('隔离保存响应中断，请核验原任务',{exact:false}).waitFor();
      assert.equal(await description.inputValue(),'隔离跟进完成 '+row.id);
      assert(!row.followup);
      await counts(0,2);
      assert(await save.isDisabled(),'an uncertain write must not create another followup');
      await tap(editor.getByRole('button',{name:'核验原任务',exact:true}));
    }
    await editor.getByText(failFirst?'跟进已保存，维修项目汇总按原流程同步。':'维修跟进记录已新增。',{exact:false}).waitFor();
    await close(row.title);
  }

  await counts(2,2);
  await tap(filter('进行中',4));
  await finishNotice(notices[0],true);
  await counts(1,2);
  await finishNotice(notices[1],false);
  await counts(0,2);
  await finishRepair(repairs[0],true);
  await counts(0,1);
  await entry(repairs[0].title).waitFor({state:'hidden'});
  await finishRepair(repairs[1],false);
  await counts(0,1);
  await entry(repairs[1].title).waitFor();
  assert(!endedRepair(repairs[1]),'saving progress does not end a repair without an end time');

  const scopePicker=page.getByLabel('工作台楼栋',{exact:true});
  await scopePicker.selectOption('B');
  await counts(2,2);
  await page.setViewportSize({width:320,height:650});
  await tap(filter('进行中',4));
  await finishNotice(notices[2],false);
  await counts(1,2);
  await scopePicker.selectOption('A');
  await counts(0,1);
  await tap(filter('进行中',1));
  await entry(repairs[1].title).waitFor();
  assert.equal(await entry(repairs[0].title).count(),0);
  await tap(tile('repair_management',1));
  const queueRows=page.getByRole('dialog',{name:'模块待办条目'});
  await queueRows.getByRole('button',{name:'办理 '+repairs[1].title,exact:true}).waitFor();
  assert.equal(await queueRows.getByRole('button',{name:'办理 '+repairs[0].title,exact:true}).count(),0);
  await tap(queueRows.getByRole('button',{name:'关闭待办条目',exact:true}));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  assert.deepEqual(creates,['A-notice-1','A-notice-2','B-notice-1']);
  assert.deepEqual(retries,['job-A-notice-1']);
  assert.deepEqual(repairsRecovered,[saves[0].operation]);
  assert.deepEqual(saves.map(item=>item.id),['A-repair-1','A-repair-2']);
  assert.deepEqual(failures,[]);
  assert.deepEqual(errors,[]);
  console.log('PASS: work status notice and repair submissions, original-task retry, both counts, same-title floors, and unobscured touch controls.');
}catch(error){
  const pages=browser.contexts().flatMap(context=>context.pages());
  if(pages.length)await pages[0].screenshot({path:path.join(output,'failure.png')}).catch(()=>{});
  if(failures.length)console.error(failures);
  throw error;
}finally{
  await browser.close();
  await new Promise(resolve=>server.close(resolve));
}
