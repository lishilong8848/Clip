import {openWorkspaceRail,selectWorkspaceAccount,selectWorkspaceSetting} from './workspace-account-actions.mjs';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { build } from 'esbuild';
import { runInNewContext } from 'node:vm';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repo = path.resolve(root, '../../..');
const output = path.join(repo, 'output/playwright/workspace-notices');
const rebindOnly=process.argv.includes('--rebind');
await mkdir(output, { recursive: true });
const generated = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import json,sys
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import NoticeCommandRegressionBase,VALID_STARTS
test=NoticeCommandRegressionBase();test.setUp()
try:
    pages={}
    for kind,draft in VALID_STARTS.items():
        label={'maintenance':'维保','change':'变更','repair':'设备检修','power':'上电','polling':'设备轮巡','adjust':'设备调整'}[kind]
        rows=[{**draft,'title':'A楼'+label+'事项'+str(i+1),'record_id':'plan-'+kind+'-'+str(i),'work_type':kind,
            'source_progress':'未开始','display_fields':{'名称':'A楼'+label+'事项'+str(i+1),'维护总项':'A楼'+label+'事项'+str(i+1),'楼栋':'A楼','专业':'电气','计划进度':'未开始'}} for i in range(8)]
        ongoing=[{**draft,'title':'A楼'+label+'进行中'+str(i+1),'active_item_id':'active-'+kind+'-'+str(i),'target_record_id':'rec-'+kind+'-'+str(i),'record_id':'rec-'+kind+'-'+str(i),
            'work_type':kind,'scope':'A','building_codes':['A'],'status':'开始','site_photo_count':1} for i in range(6)]
        rows[0]['title']='EA118机房A楼制冷单元及二次泵控制系统年度'+label+'（第三阶段设备状态核验与恢复）'
        ongoing[0]['title']='EA118机房A楼配电系统与电池组'+label+'（多设备联合处理及运行状态核验）'
        if ${rebindOnly?'True':'False'} and kind=='maintenance':
            ongoing[0].update(polling_work_order_group_id=ongoing[0]['target_record_id'],
                              polling_work_order_operator_link='https://fixture.invalid/old-link')
        args=dict(payload={'records':rows,'ongoing':ongoing},session={'role':'building','can_delete_business':False,'allowed_modules':list(VALID_STARTS),'user':{'name':'隔离测试'}},scope='A',work_type=kind,month='10月',scope_options=[{'value':'A','label':'A楼'}])
        html=render_workbench_lite(**args)
        detail=extract_workbench_lite_fragments(render_workbench_lite(**args,active_item_id=ongoing[0]['active_item_id']))['detail']
        plan=extract_workbench_lite_fragments(render_workbench_lite(**args,record_id=rows[0]['record_id']))['detail']
        pages[kind]={'html':html,'detail':detail,'plan':plan,'payload':{**args['payload'],'records_pagination':{'total':8},'ongoing_pagination':{'total':6}},'fragments':extract_workbench_lite_fragments(html)}
    print(json.dumps(pages,ensure_ascii=False))
finally:test.tmp.cleanup()
`], { cwd: repo, encoding: 'utf8', maxBuffer: 20 * 1024 * 1024, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });
assert.equal(generated.status, 0, generated.stderr);
const fixtures = JSON.parse(generated.stdout);
const helper=await build({entryPoints:[path.join(root,'src/workspace-v2/noticeForm.ts')],bundle:true,write:false,format:'iife',globalName:'noticeTest'});
const server = await preview({ root, ...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}), logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const errors = [], writes = [], oldPages = [], detailRequests = [], entry = '/assets/workspace-v2/index.html';
const pixel = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6oDQAAAAASUVORK5CYII=','base64');
const layouts=layoutApiFixture();
let failLogout = true;
const rebindRequests=[];
try {
  const context = await browser.newContext({ viewport: rebindOnly?{width:402,height:874}:{width:1440,height:1000} });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if(await layouts.handle(route,'ou_notice_fixture'))return;
    const kind = url.searchParams.get('work_type') || 'maintenance';
    const data = fixtures[kind] || fixtures.maintenance;
    if (url.pathname === '/workbench-lite') { oldPages.push(req.url()); return route.fulfill({ contentType: 'text/html', body: data.html }); }
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'ou_notice_fixture', name: '测试值班员', role: 'building', level: 'L4', category: 'operator', can_delete_business: false, modules: [...Object.keys(fixtures),'event'] }, scope_options: [{ value: 'A', label: 'A楼' }] });
    if (url.pathname === '/api/auth/logout') return failLogout ? route.fulfill({ status: 503, json: { ok: false, error: '退出暂时失败，请重试' } }) : ok({});
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-notice-ui' } });
    if (url.pathname === '/api/workbench-events/parse') return ok({}); // Read-only parsing, not a business write.
    if (url.pathname === '/api/workbench-events'||url.pathname === '/api/workbench-events/options') return ok({user_id:'ou_notice_fixture',scopes:['A'],levels:['I3'],sources:['巡检发现'],specialties:['电气'],can_delete:false,defaults:{robot_ids:[],revision:0},robots:[],items:[]});
    if (url.pathname === '/api/workbench/dingtalk-options') return ok({robots:[{id:'fixture-robot',name:'隔离测试机器人',types:[],scopes:[]}],selected_robot_ids:[]});
    if (url.pathname === '/api/workbench') return ok(data.payload);
    if(rebindOnly&&url.pathname==='/api/workbench/polling-delay-status')return ok({state:'active',sop_name:'旧模板'});
    if(rebindOnly&&url.pathname==='/api/polling-sops')return ok({items:[{sop_id:'sop-new',name:'隔离新模板',version:2,scope:'A',steps:[]}]});
    if(rebindOnly&&url.pathname==='/api/signatures/people')return ok({people:[{record_id:'opA',name:'隔离操作人'},{record_id:'reviewA',name:'隔离审核人'}]});
    if(rebindOnly&&url.pathname==='/api/polling-work-orders/rec-maintenance-0/rebind'){
      rebindRequests.push(req.postDataJSON());
      return rebindRequests.length===1?route.fulfill({status:503,json:{ok:false,error:'结果待核验'}}):ok({state:'binding_pending',sop_name:'隔离新模板'});
    }
    if (url.pathname === '/api/workbench/lite-fragment') return ok({ canonical_url: '/workbench-lite?' + url.searchParams, fragments: data.fragments });
    if (url.pathname === '/api/workbench/lite-detail') {
      detailRequests.push(Object.fromEntries(url.searchParams));
      return ok({ canonical_url: '/workbench-lite?' + url.searchParams, fragments: { detail: url.searchParams.has('active_item_id') ? data.detail : data.plan } });
    }
    if (url.pathname === '/api/notice-alert-tags') return ok({status:'ready',title:'EA118机房B楼12台柴油发电机组维护保养变更',action:'update',tags:[{label:'变更',content:'12台柴油发电机组维护保养变更'}]});
    if (url.pathname === '/api/notice-attachments') return ok({upload_id:'isolated-image',file_name:'确认截图.png',preview_url:'/api/notice-images/'+ 'a'.repeat(32)});
    if (url.pathname.startsWith('/api/notice-images/')) return route.fulfill({contentType:'image/png',body:pixel});
    if (req.method() !== 'GET') {
      writes.push(url.pathname);
      if (url.pathname === '/api/workbench/draft') return ok({ draft: { version: 1 } });
      return route.fulfill({ status: 400, json: { ok: false, error: '隔离测试禁止业务提交' } });
    }
    return ok({ items: [], records: [], fields: [], options: [], summary: {}, settings: {}, stats: {}, available: false });
  });
  const page = await context.newPage();
  const plannedLane=page.locator('[data-list=planned]');
  const ongoingLane=page.locator('[data-list=ongoing]');
  const lists=page.getByRole('group',{name:'当前列表',exact:true});
  const types=page.getByRole('navigation',{name:'通告类型',exact:true});
  const editor=page.locator('.desk-editor');
  const footer=editor.locator('.v2-notice-editor-footer');
  const selectList=async name=>{
    if(await lists.isVisible())await lists.getByRole('button',{name:new RegExp('^'+name)}).click();
  };
  const closeEditor=async()=>{
    await footer.getByRole('button',{name:'关闭',exact:true}).click();
    const keep=page.getByRole('button',{name:'保留副本并离开',exact:true});
    await Promise.race([editor.waitFor({state:'detached'}),keep.waitFor()]);
    if(await keep.isVisible())await keep.click();
    await editor.waitFor({state:'detached'});
  };
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin+entry+'?__page=/workbench-lite&scope=A&work_type=maintenance');
  if(rebindOnly){
    await page.locator('.desk-switch').getByRole('button',{name:/进行中/}).click();
    await ongoingLane.locator('.desk-record').first().click();
    const replace=editor.getByRole('button',{name:'更换工单',exact:true});
    await replace.click();
    const modal=page.locator('.v2-notice-confirm').filter({hasText:'更换关联工单'});
    await modal.locator('.lhs-sop .n-select').click();
    await page.locator('.n-base-select-option').getByText('隔离新模板',{exact:true}).click();
    for(const [index,label] of ['隔离操作人','隔离审核人'].entries()){
      await modal.locator('.lhs-people-cols .n-select').nth(index).click();
      await page.locator('.n-base-select-option:visible').getByText(label,{exact:true}).last().click();
    }
    const submit=modal.getByRole('button',{name:'确认更换并通知',exact:true});
    assert(!(await submit.isDisabled()));
    await page.screenshot({path:path.join(output,'rebind-mobile.png'),animations:'disabled'});
    await submit.click();
    await modal.getByText('结果待核验',{exact:true}).waitFor();
    await submit.click();
    await editor.getByText('新工单待同步，旧链接已作废',{exact:true}).waitFor();
    assert.equal(await editor.locator('[href*="old-link"]').count(),0);
    assert.equal(rebindRequests.length,2);
    assert.deepEqual(rebindRequests[0],rebindRequests[1]);
    assert.equal(rebindRequests[0].polling_sop_id,'sop-new');
    assert.deepEqual(errors,[]);
    console.log('PASS: replace a sent notice work order from its existing editor, retry the same request and hide the old link.');
    await browser.close();await server.close();process.exit(0);
  }
  await page.addScriptTag({content:helper.outputFiles[0].text});
  const contracts=await page.evaluate(fixtures=>{
    const result=[];
    for(const [kind,fixture] of Object.entries(fixtures)){
      for(const view of ['plan','detail']){
        const form=noticeTest.parseNoticeForm(fixture[view]),action=view==='plan'?'start':'update';
        const command=noticeTest.noticeCommand(form,action,'fixture-idempotency',4,{exempt:true},[{upload_id:'fixture-photo'}],[]);
        result.push({kind,view,command,preview:noticeTest.noticePreview(form,action),photoRequired:form.photoRequired,fields:form.fields.map(f=>f.key),expected:form.values,actions:form.actions});
      }
    }
    const bad=noticeTest.parseNoticeForm('<form id="lite-notice-form"><input type="hidden" name="source_record_id" value="rec-source"><input type="hidden" name="work_type" value="maintenance"><label><span>名称</span><input name="title" required></label><button name="submit_action" value="start"></button><script>window.fixtureExecuted=true<\/script></form>');
    const missingRobot=noticeTest.validateNotice(bad,'start',{},[]);
    bad.values.dingtalk_robot_ids=['fixture-robot'];
    const sample={scope:'D',work_type:'adjust',notice_type:'设备调整',manual:'1',title:'第一条',start_time:'2026-11-08T09:00',end_time:'2026-11-08T18:00',building_codes:['D']};
    const key=noticeTest.noticeCacheKey(sample,'fixture');
    const keys=[noticeTest.noticeCacheKey({...sample,title:'第二条'},'fixture'),noticeTest.noticeCacheKey({...sample,scope:'E'},'fixture'),noticeTest.noticeCacheKey({...sample,start_time:'2026-12-08T09:00'},'fixture')];
    return {missingRobot,items:result,executed:!!window.fixtureExecuted,missing:noticeTest.validateNotice(bad,'start',{},[]),key,keys};
  },fixtures);
  assert.match(contracts.missingRobot,/钉钉/);
  assert.equal(contracts.executed,false,'HTML metadata never executes scripts');assert.match(contracts.missing,/名称/);
  for(const key of contracts.keys)assert.notEqual(key,contracts.key,'independent notices must not share pending tasks');
  for(const {kind,view,command,expected,preview,fields,photoRequired} of contracts.items){
    assert.equal(command.operation_id,'fixture-idempotency');assert.equal(command.work_type,kind);
    assert.equal(command.record_id,view==='plan'?(expected.source_record_id||expected.manual_id||expected.record_id||''):(expected.target_record_id||expected.record_id||''));
    assert.deepEqual(command.patch.building_codes,expected.building_codes);
    assert.equal(command.patch.site_photos[0].upload_id,'fixture-photo');
    if(kind==='repair'){assert.equal(command.patch.fault_time,expected.end_time);assert.equal(command.patch.expected_time,expected.start_time);}
    assert.equal(photoRequired,['maintenance','change','repair'].includes(kind),'photo gate matches the original business contract');
    assert(fields.includes('title'));
    const literal=fixtures[kind].html.match(/const previewTemplates = (\{[\s\S]*?\n    \});/)[1];
    const template=runInNewContext('('+literal+')',{}, {timeout:1000})[kind];
    const value=key=>{const text=String(expected[key]??'').trim();return ['start_time','end_time','actual_action_time','response_time'].includes(key)?text.replace('T',' '):text;};
    const heading=kind==='power'&&['上电通告','下电通告'].includes(expected.notice_type)?expected.notice_type:template.heading;
    const expectedPreview=[`【${heading}】状态：${view==='plan'?'开始':'更新'}`,...template.fields.map(([label,key])=>`【${label}】${key==='__range'?[value('start_time'),value('end_time')].filter(Boolean).join('~'):value(key)}`)].join('\n');
    assert.equal(preview,expectedPreview,kind+' preview preserves existing labels, order and time formatting');
  }
  for (const [index, kind] of Object.keys(fixtures).entries()) {
    await page.setViewportSize({width:1440,height:1000});
    const category={maintenance:/维保/,change:/变更/,repair:/检修/,power:/上下电/,polling:/轮巡/,adjust:/调整/}[kind];
    if(index)await types.getByRole('button',{name:category}).click();
    await page.waitForURL(url=>url.searchParams.get('work_type')===kind);
    const planTitle=fixtures[kind].payload.records[0].title;
    const ongoingTitle=fixtures[kind].payload.ongoing[0].title;
    const plan=plannedLane.getByRole('article').filter({has:page.getByRole('button',{name:planTitle,exact:true})});
    const entryCard=ongoingLane.getByRole('article').filter({has:page.getByRole('button',{name:ongoingTitle,exact:true})});
    await plan.waitFor();
    assert.equal(await types.getByRole('button',{name:category}).getAttribute('aria-pressed'),'true');
    assert.equal(await plannedLane.getByRole('searchbox',{name:'搜索计划通告'}).count()+await plannedLane.getByRole('textbox',{name:'搜索计划通告'}).count(),1,'plan-only search stays inside the planned list');
    assert.equal(await ongoingLane.locator('form').count(),0);
    assert.equal(await plannedLane.getByRole('article').count(),8);
    assert.equal(await ongoingLane.getByRole('article').count(),6);
    assert.equal(await page.locator('iframe').count(), 0, 'new notice page must not embed legacy HTML');
    for (const width of index === 0 ? [1440,1920,360,390,430] : [1366]) {
      await page.setViewportSize({ width, height: width < 700 ? 844 : 1000 });
      await selectList('计划通告');
      await page.screenshot({ path: path.join(output, `${process.argv.includes('--baseline') ? 'before' : 'after'}-${kind}-${width}.png`), fullPage: true });
      assert.equal(await plan.evaluate(el=>{const box=el.getBoundingClientRect();return box.top>=0&&box.bottom<=innerHeight-(innerWidth<=760?80:0);}),true,'the first task is fully visible without scrolling');
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
      assert.equal(await page.locator('.v2-context').count(),0,'building selection stays in the notice toolbar');
      if(width>760){
        const [left,right]=await Promise.all([plannedLane.boundingBox(),ongoingLane.boundingBox()]);
        assert(left&&right&&Math.abs(left.y-right.y)<2&&right.x>=left.x+left.width,'planned and ongoing appear side by side');
      }else{
        assert.equal(await ongoingLane.isVisible(),false);
        await selectList('进行中');await entryCard.waitFor();
        assert.equal(await plannedLane.isVisible(),false);
      }
    }
    if(index===0){
      await page.setViewportSize({width:1920,height:1000});
      await page.getByRole('button',{name:'自定义通告办理布局',exact:true}).click();
      for(const [list,width] of [['planned',4],['ongoing',6],['planned',12],['ongoing',12],['planned',6],['ongoing',4]]){
        const lane=page.locator(`[data-list="${list}"]`);
        await lane.evaluate((el,w)=>{const item=el.closest('.grid-stack-item');item.closest('.grid-stack').gridstack.update(item,{w});},width);
        const [laneWidth,columns]=await lane.locator('.desk-records').evaluate(el=>[el.parentElement.clientWidth,getComputedStyle(el).gridTemplateColumns.split(' ').length]);
        assert.equal(columns,laneWidth>=1060?3:laneWidth>=700?2:1,list+' adapts to its own resized panel');
      }
      await page.screenshot({path:path.join(output,'resized-independent-lists.png'),fullPage:true});
      await page.getByRole('button',{name:'取消',exact:true}).click();await plan.waitFor();
    }
    await selectList('进行中');
    const beforeReads=detailRequests.length;
    await entryCard.click({position:{x:5,y:Math.min(50,(await entryCard.boundingBox()).height/2)}});
    try{await page.locator('.v2-notice-editor .v2-notice-field-grid').first().waitFor({timeout:10000});}
    catch(e){await page.screenshot({path:path.join(output,'form-error.png'),fullPage:true});console.error(errors,await page.locator('.v2-notice-editor').innerText());throw e;}
    assert.equal(await page.locator('.v2-notice-editor-footer').isVisible(), true);
    assert.equal(detailRequests.length,beforeReads+1,'card whitespace opens one editor, without double activation');
    assert.equal(detailRequests.at(-1).active_item_id,'active-'+kind+'-0');
    assert.equal(await page.locator('.v2-notice-editor-context [role=radiogroup]').count(),0);
    assert.equal(await page.locator('.v2-notice-submit-group [role=radiogroup]').count(),1,'action and preview share one footer group');
    assert.equal(await page.getByRole('button',{name:'保存草稿',exact:true}).count(),0);
    await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'删除通告',exact:true}).click();
    await page.getByRole('button',{name:'确认删除',exact:true}).waitFor();
    assert.equal(writes.filter(url=>url==='/api/ongoing-items/delete').length,0,'opening deletion confirmation must not delete');
    await page.getByRole('button',{name:'取消',exact:true}).click();
    assert.equal(await page.locator('#lite-notice-form').count(), 0, 'old form HTML is metadata only, never mounted');
    assert.equal(await page.locator('.v2-notice-editor input[aria-label="名称"],.v2-notice-editor input[aria-label="标题"]').count() > 0, true);
    assert.equal(await page.locator('.v2-notice-upload input[aria-label="添加现场照片"]').count(),['maintenance','change','repair'].includes(kind)?1:0);
    if(!process.argv.includes('--baseline')){
      const editor=page.locator('.v2-notice-editor'),warnings=editor.locator('[data-cycle-warning]');
      const controls=['content','reason','progress'].map(key=>({key,input:editor.locator(`[data-notice-field="${key}"] textarea,[data-notice-field="${key}"] input`).first()}));
      const originals=[];
      for(const control of controls)if(await control.input.count())originals.push({...control,value:await control.input.inputValue()});
      const originalCycle=contracts.items.find(item=>item.kind===kind&&item.view==='detail').expected.maintenance_cycle;
      const selectCycle=async value=>{await editor.locator('[data-notice-field="maintenance_cycle"] .n-base-selection').click();await page.locator('.n-base-select-option:visible').getByText(value,{exact:true}).click();};
      try{
        if(kind==='maintenance')await selectCycle('每月');
        for(const control of originals.filter(item=>['content','reason'].includes(item.key)))await control.input.fill('季度维保检查');
        if(kind==='maintenance'){
          await editor.locator('[data-cycle-warning="reason"]').waitFor();assert.equal(await warnings.count(),2);
          for(const key of ['content','reason'])assert(await editor.locator(`[data-notice-field="${key}"] .n-input`).evaluate(el=>el.classList.contains('n-input--error-status')),key+' has a red advisory border');
          const viewport=page.viewportSize();
          for(const [width,height] of [[402,874],[440,956],[1440,900]]){
            await page.setViewportSize({width,height});await warnings.first().scrollIntoViewIfNeeded();
            assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
            await page.screenshot({path:path.join(output,`cycle-warning-${width}.png`)});
          }
          await page.setViewportSize(viewport);
          const content=originals.find(item=>item.key==='content').input;
          await content.fill('每月维保检查');await page.waitForFunction(()=>!document.querySelector('[data-cycle-warning="content"]'));
          assert.equal(await warnings.count(),1,'fixing content clears only its warning');await content.fill('季度维保检查');
          await selectCycle('每季');await page.waitForFunction(()=>!document.querySelector('[data-cycle-warning]'));
          await selectCycle('每月');await editor.locator('[data-cycle-warning="reason"]').waitFor();
          await originals.find(item=>item.key==='progress').input.fill('现场已核对');
          const preview=editor.getByRole('button',{name:'预览并确认更新',exact:true});assert(!await preview.isEnabled(),'missing robot disables sending');await editor.getByRole('checkbox',{name:'隔离测试机器人'}).check();assert(await preview.isEnabled());await preview.click();
          assert.equal(await editor.getByRole('button',{name:'确认发送更新',exact:true}).count(),0,'cycle conflicts must block confirmation');
          assert((await editor.innerText()).includes('修改一致后再发送'));
          await content.fill('每月维保检查');await originals.find(item=>item.key==='reason').input.fill('按月度计划维保');
          await preview.click();await editor.getByLabel('发送前确认',{exact:true}).waitFor();
          assert(!(await editor.getByLabel('待发送通告全文',{exact:true}).innerText()).includes('周期核对提醒'),'advisory never rewrites the notice');
          assert(!writes.includes('/api/workbench-actions'),'advisory preview never submits a notice');
          await editor.getByRole('button',{name:'返回修改',exact:true}).click();
        }else{
          assert.equal(await warnings.count(),0,kind+' ignores cycle words in content/reason');
          assert.equal(await editor.locator('.has-cycle-warning,[data-cycle-warning-summary]').count(),0);
        }
      }finally{
        if(await editor.getByRole('button',{name:'返回修改',exact:true}).isVisible())await editor.getByRole('button',{name:'返回修改',exact:true}).click();
        for(const control of originals)await control.input.fill(control.value);
        if(kind==='maintenance'){await selectCycle(originalCycle);await editor.getByRole('checkbox',{name:'隔离测试机器人'}).uncheck();}
      }
      assert.equal(await warnings.count(),0,'restore original fields before unchanged-layout baselines');
      // Reopen also clears attempted-validation state, not just the edited field values.
      await closeEditor();
      await entryCard.getByRole('button',{name:'更新',exact:true}).click();await editor.locator('.v2-notice-field-grid').waitFor();
    }
    await page.waitForTimeout(350);
    const footerBox=await page.locator('.v2-notice-editor-footer').boundingBox();
    assert(footerBox.y+footerBox.height<=page.viewportSize().height+1,'editor actions must remain inside the viewport');
    await page.setViewportSize({width:1366,height:768});
    assert.equal(await page.getByRole('button',{name:'展开其它字段',exact:true}).count(),0,'all notice fields are shown directly');
    assert.equal(await page.locator('.v2-notice-pane-tabs').count(),0,'editing does not require switching sections');
    await page.waitForTimeout(100);
    const body=editor.locator(':scope > .n-card-content');
    assert.equal(await body.evaluate(el=>getComputedStyle(el).overflowY),'auto','one body scrolls through all notice information');
    const bounds=await body.boundingBox();
    const actions=await page.locator('.v2-notice-editor-footer').boundingBox();assert(bounds.y+bounds.height<=actions.y+2,kind+' scrolling body stays above fixed actions');
    if(kind==='maintenance'){
      for(const [width,height] of [[360,640],[375,667],[390,844]]){
        await page.setViewportSize({width,height});
        const group=page.locator('.v2-notice-submit-group');
        const radio=await group.getByRole('radiogroup',{name:'本次操作'}).boundingBox();
        const preview=group.getByRole('button',{name:/预览并确认/});const box=await preview.boundingBox();
        await page.screenshot({path:path.join(output,`compact-footer-${width}.png`)});
        assert(radio&&box&&Math.abs(radio.y+radio.height/2-box.y-box.height/2)<4,'mobile operation and preview remain adjacent: '+JSON.stringify({radio,box}));
        assert(box.y>=0&&box.y+box.height<=height&&box.x+box.width<=width,'preview stays on screen');
        assert(await preview.evaluate(el=>{const b=el.getBoundingClientRect();return el.contains(document.elementFromPoint(b.x+b.width/2,b.y+b.height/2));}),'preview is not covered');
        await page.screenshot({path:path.join(output,`compact-footer-${width}.png`)});
      }
      await page.setViewportSize({width:1366,height:768});
    }
    await page.locator('[data-notice-section=relations]').scrollIntoViewIfNeeded();
    assert(await page.getByRole('button',{name:/^(重新)?查找目标通告$/}).isVisible(),'related-record tools are immediately accessible on the same page');
    await body.evaluate(el=>el.scrollTo({top:0}));
    const tags=editor.getByRole('region',{name:'通告推荐标签',exact:true});
    await tags.getByText('【变更】12台柴油发电机组维护保养变更',{exact:true}).waitFor();
    assert.equal(await tags.locator('xpath=ancestor::aside').getByRole('heading',{name:'关联与接收',exact:true}).count(),1);
    for(const uploadKind of kind==='change'?['site','ali']:['maintenance','repair'].includes(kind)?['site']:[]){
      await page.getByLabel(uploadKind==='ali'?'添加阿里确认截图':'添加现场照片',{exact:true}).setInputFiles({name:'确认截图.png',mimeType:'image/png',buffer:pixel});
      const upload=page.locator('[data-notice-upload-kind='+uploadKind+']');await upload.getByText('本次新增 · 已就绪',{exact:true}).waitFor();
      assert(await upload.locator('figure').last().getByText('确认截图.png',{exact:true}).isVisible());
      await page.screenshot({path:path.join(output,'compact-'+kind+'-'+uploadKind+'-upload.png'),fullPage:true});
      await upload.getByRole('button',{name:'移除本次图片',exact:true}).click();
    }
    assert.equal(await page.locator('.v2-notice-page-fields .n-pagination').count(),0,'notice fields never paginate');
    await page.screenshot({ path: path.join(output, `form-${kind}.png`), fullPage: true });
    await closeEditor();
    await plan.getByRole('button',{name:'开始',exact:true}).click();
    await editor.locator('.v2-notice-field-grid').waitFor();
    assert.equal(detailRequests.at(-1).record_id,'plan-'+kind+'-0');
    assert.match(await footer.locator('.n-radio-button--checked').innerText(),/开始/);
    await closeEditor();
    if(kind==='maintenance'){
      const beforeEnd=writes.length;
      await entryCard.getByRole('button',{name:'结束',exact:true}).click();
      await page.locator('.v2-notice-editor .n-radio-button--checked').filter({hasText:'结束'}).waitFor();
      assert.equal(await page.locator('[data-notice-field="progress"] textarea').inputValue(),'工作已完成，设备运行正常，请知晓！','end opens the correct action and default progress');
      assert.equal(writes.length,beforeEnd,'opening an end form must not send');
      await closeEditor();
    }
  }
  fixtures.maintenance.payload.records[0] = { ...fixtures.maintenance.payload.records[0],
    source_progress: '', source_status: '', status: '已结束' };
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 1000 });
    await page.goto(origin + entry + '?__page=/workbench-lite&scope=A&work_type=maintenance');
    const planned = plannedLane.getByRole('article').first();
    await planned.getByText('状态未取得', { exact: true }).waitFor();
    assert(await planned.getByRole('button', { name: /请重新读取/ }).isDisabled());
    const beforeDetail=detailRequests.length;
    await planned.getByRole('heading').getByRole('button').click();
    assert.equal(await page.locator('.v2-notice-editor').count(), 0, 'unknown source status never starts a plan via its title');
    assert.equal(detailRequests.length,beforeDetail);
    await page.screenshot({ path: path.join(output, `source-status-unknown-${width}.png`), fullPage: true });
    await selectList('进行中');
    assert(await ongoingLane.getByRole('button',{name:'更新',exact:true}).first().isEnabled(),'source read failure does not disable existing notice operations');
  }
  for (const width of [402,440,1440]) {
    await page.setViewportSize({width,height:900});
    await page.goto(origin+entry+'?__page=/workbench-lite&scope=A&work_type=event');
    if(width<=760){
      const selector=page.getByRole('combobox',{name:'通告类型',exact:true});
      await selector.waitFor();
      await selector.selectOption('maintenance');
      await plannedLane.waitFor();
      await selector.selectOption('event');
      assert.equal(await selector.inputValue(),'event');
    }else{
      await types.getByRole('button',{name:'事件通告',exact:true,pressed:true}).waitFor();
    }
    assert.equal(await page.locator('[data-list="planned"]').count(),0,'events have no planned list');
    const card=page.locator('.raw-event-card.evt-compose');
    const composeToggle=card.locator('summary').filter({hasText:'新建事件通告'});
    await composeToggle.waitFor();
    if(!await composeToggle.evaluate(el=>el.closest('details').open))await composeToggle.click();
    await card.getByLabel('本次通告原文',{exact:true}).fill('【事件通告】状态：新增\n【标题】A楼I3隔离草稿');
    await card.getByRole('button',{name:'发送新增',exact:true}).scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await card.getByRole('button',{name:'发送新增',exact:true}).click({trial:true});
    assert(await card.getByRole('button',{name:'发送新增',exact:true}).isVisible());
    await page.screenshot({path:path.join(output,'event-viewport-'+width+'.png')});
    await page.screenshot({path:path.join(output,'event-production-'+width+'.png'),fullPage:true});
  }
  if (!process.argv.includes('--baseline')) {
    await selectWorkspaceAccount(page,'个人信息');
    await selectWorkspaceAccount(page,'退出登录');await page.getByRole('button',{name:'继续',exact:true}).click();
    await page.getByRole('alert').filter({ hasText: '退出暂时失败' }).waitFor();
    assert.equal(await page.getByRole('button', { name: /^账户菜单：/ }).isEnabled(), true);
    assert.match(page.url(), /view=my/);
    assert.equal(writes.filter(url => url !== '/api/workbench/draft').length, 0, 'no real notice writes');
  }
  assert.deepEqual(errors, []);
  assert.deepEqual(oldPages, [], 'no legacy document navigation');
  console.log('PASS: V2 notice lists and editors for six categories, original record IDs, mobile controls, maintenance cycle validation, attachments, event composer and logout recovery. Isolated data only.');
} finally { await browser.close(); await server.close(); }
