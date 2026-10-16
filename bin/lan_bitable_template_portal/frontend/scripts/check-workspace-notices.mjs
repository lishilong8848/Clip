import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { build } from 'esbuild';
import { runInNewContext } from 'node:vm';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repo = path.resolve(root, '../../..');
const output = path.join(repo, 'output/playwright/workspace-notices');
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
const errors = [], writes = [], oldPages = [], entry = '/assets/workspace-v2/index.html';
let failLogout = true;
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    const kind = url.searchParams.get('work_type') || 'maintenance';
    const data = fixtures[kind] || fixtures.maintenance;
    if (url.pathname === '/workbench-lite') { oldPages.push(req.url()); return route.fulfill({ contentType: 'text/html', body: data.html }); }
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'ou_notice_fixture', name: '测试值班员', role: 'building', level: 'L4', category: 'operator', can_delete_business: false, modules: Object.keys(fixtures) }, scope_options: [{ value: 'A', label: 'A楼' }] });
    if (url.pathname === '/api/auth/logout') return failLogout ? route.fulfill({ status: 503, json: { ok: false, error: '退出暂时失败，请重试' } }) : ok({});
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-notice-ui' } });
    if (url.pathname === '/api/workbench') return ok(data.payload);
    if (url.pathname === '/api/workbench/lite-fragment') return ok({ canonical_url: '/workbench-lite?' + url.searchParams, fragments: data.fragments });
    if (url.pathname === '/api/workbench/lite-detail') return ok({ canonical_url: '/workbench-lite?' + url.searchParams, fragments: { detail: url.searchParams.has('active_item_id') ? data.detail : data.plan } });
    if (req.method() !== 'GET') {
      writes.push(url.pathname);
      if (url.pathname === '/api/workbench/draft') return ok({ draft: { version: 1 } });
      return route.fulfill({ status: 400, json: { ok: false, error: '隔离测试禁止业务提交' } });
    }
    return ok({ items: [], records: [], fields: [], options: [], summary: {}, settings: {}, stats: {}, available: false });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin+entry+'?__page=/workbench-lite&scope=A&work_type=maintenance');
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
    return {items:result,executed:!!window.fixtureExecuted,missing:noticeTest.validateNotice(bad,'start',{},[])};
  },fixtures);
  assert.equal(contracts.executed,false,'HTML metadata never executes scripts');assert.match(contracts.missing,/名称/);
  for(const {kind,view,command,expected,preview,fields,photoRequired} of contracts.items){
    assert.equal(command.operation_id,'fixture-idempotency');assert.equal(command.work_type,kind);
    assert.equal(command.record_id,view==='plan'?(expected.source_record_id||expected.manual_id||expected.record_id||''):(expected.target_record_id||expected.record_id||''));
    assert.deepEqual(command.patch.building_codes,expected.building_codes);
    assert.equal(command.patch.site_photos[0].upload_id,'fixture-photo');
    if(kind==='repair'){assert.equal(command.patch.fault_time,expected.end_time);assert.equal(command.patch.expected_time,expected.start_time);}
    assert.equal(photoRequired,['maintenance','change','repair'].includes(kind),'photo gate matches the original business contract');
    assert(fields.includes('title'));
    const literal=fixtures[kind].html.match(/const previewTemplates = (\{[\s\S]*?\n    \});/)[1];
    const template=runInNewContext('('+literal+')',{}, {timeout:100})[kind];
    const value=key=>{const text=String(expected[key]??'').trim();return ['start_time','end_time','actual_action_time','response_time'].includes(key)?text.replace('T',' '):text;};
    const heading=kind==='power'&&['上电通告','下电通告'].includes(expected.notice_type)?expected.notice_type:template.heading;
    const expectedPreview=[`【${heading}】状态：${view==='plan'?'开始':'更新'}`,...template.fields.map(([label,key])=>`【${label}】${key==='__range'?[value('start_time'),value('end_time')].filter(Boolean).join('~'):value(key)}`)].join('\n');
    assert.equal(preview,expectedPreview,kind+' preview preserves existing labels, order and time formatting');
  }
  for (const [index, kind] of Object.keys(fixtures).entries()) {
    await page.goto(origin + entry + '?__page=/workbench-lite&scope=A&work_type=' + kind);
    await page.locator('.v2-notice-entry').first().waitFor();
    assert.equal(await page.locator('iframe').count(), 0, 'new notice page must not embed legacy HTML');
    for (const width of index === 0 ? [1440,1920,360,390,430] : [1366]) {
      await page.setViewportSize({ width, height: width < 700 ? 844 : 1000 });
      await page.screenshot({ path: path.join(output, `${process.argv.includes('--baseline') ? 'before' : 'after'}-${kind}-${width}.png`), fullPage: true });
      assert.equal(await page.locator('.v2-notice-list:visible .v2-notice-entry').first().evaluate(el => {const box=el.getBoundingClientRect();return box.top>=0&&box.bottom<=innerHeight-(innerWidth<=760?80:0);}), true, 'the first task is fully visible without scrolling');
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
      assert.equal(await page.locator('.v2-context').count(),0,'building selection stays in the notice toolbar');
      if(width>760){const columns=await page.locator('.v2-notice-section').evaluateAll(els=>els.map(el=>el.getBoundingClientRect().toJSON()));assert.equal(columns.length,2);assert(Math.abs(columns[0].top-columns[1].top)<2);assert(columns[1].left>=columns[0].right,'planned and ongoing appear side by side');}
    }
    if(page.viewportSize().width<=760)await page.getByRole('tab', { name: /进行中/ }).click();
    await page.locator('[data-list="ongoing"] .v2-notice-open').first().click();
    try{await page.locator('.v2-notice-editor .v2-notice-field-grid').first().waitFor({timeout:10000});}
    catch(e){await page.screenshot({path:path.join(output,'form-error.png'),fullPage:true});console.error(errors,await page.locator('.v2-notice-editor').innerText());throw e;}
    assert.equal(await page.locator('.v2-notice-editor-footer').isVisible(), true);
    await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'删除通告',exact:true}).click();
    await page.getByRole('button',{name:'确认删除',exact:true}).waitFor();
    assert.equal(writes.length,0,'opening deletion confirmation must not delete');
    await page.getByRole('button',{name:'取消',exact:true}).click();
    assert.equal(await page.locator('#lite-notice-form').count(), 0, 'old form HTML is metadata only, never mounted');
    assert.equal(await page.locator('.v2-notice-editor input[aria-label="名称"],.v2-notice-editor input[aria-label="标题"]').count() > 0, true);
    assert.equal(await page.locator('.v2-notice-upload input[aria-label="添加现场照片"]').count(),['maintenance','change','repair'].includes(kind)?1:0);
    await page.waitForTimeout(350);
    const footerBox=await page.locator('.v2-notice-editor-footer').boundingBox();
    assert(footerBox.y+footerBox.height<=page.viewportSize().height+1,'editor actions must remain inside the viewport');
    await page.setViewportSize({width:1366,height:768});
    assert.equal(await page.getByRole('button',{name:'展开其它字段',exact:true}).count(),0,'all notice fields are shown directly');
    assert.equal(await page.locator('.v2-notice-pane-tabs').count(),0,'editing does not require switching sections');
    await page.waitForTimeout(100);
    const body=page.locator('.v2-notice-handling>.n-card-content');
    assert.equal(await body.evaluate(el=>getComputedStyle(el).overflowY),'auto','one body scrolls through all notice information');
    const bounds=await body.boundingBox();
    const actions=await page.locator('.v2-notice-editor-footer').boundingBox();assert(bounds.y+bounds.height<=actions.y+2,kind+' scrolling body stays above fixed actions');
    await page.locator('[data-notice-section=relations]').scrollIntoViewIfNeeded();
    assert(await page.getByRole('button',{name:/^(重新)?查找目标通告$/}).isVisible(),'related-record tools are immediately accessible on the same page');
    await body.evaluate(el=>el.scrollTo({top:0}));
    assert.equal(await page.locator('.v2-notice-page-fields .n-pagination').count(),0,'notice fields never paginate');
    await page.screenshot({ path: path.join(output, `form-${kind}.png`), fullPage: true });
    await page.locator('.v2-notice-editor-footer').getByRole('button', { name: '关闭', exact: true }).click();
    await page.locator('.v2-notice-editor').waitFor({ state: 'detached' });
    if(kind==='maintenance'){
      const ongoing=page.locator('[data-list="ongoing"] .v2-notice-entry').first();
      await ongoing.getByRole('button',{name:'结束',exact:true}).click();
      await page.locator('.v2-notice-editor .n-radio-button--checked').filter({hasText:'结束'}).waitFor();
      assert.equal(await page.locator('[data-notice-field="progress"] textarea').inputValue(),'工作已完成，设备运行正常，请知晓！','end opens the correct action and default progress');
      assert.equal(writes.length,0,'opening an end form must not send');
      await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();
      await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
      await page.locator('.v2-notice-editor').waitFor({state:'detached'});
    }
  }
  fixtures.maintenance.payload.records[0] = { ...fixtures.maintenance.payload.records[0],
    source_progress: '', source_status: '', status: '已结束' };
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 1000 });
    await page.goto(origin + entry + '?__page=/workbench-lite&scope=A&work_type=maintenance');
    const planned = page.locator('[data-list="planned"] .v2-notice-entry').first();
    await planned.getByText('状态未取得', { exact: true }).waitFor();
    assert(await planned.getByRole('button', { name: /请重新读取/ }).isDisabled());
    await planned.locator('.v2-notice-open').click();
    assert.equal(await page.locator('.v2-notice-editor').count(), 0, 'unknown source status never starts a plan via its title');
    await page.screenshot({ path: path.join(output, `source-status-unknown-${width}.png`), fullPage: true });
    if (width <= 760) await page.getByRole('tab', { name: /进行中/ }).click();
    assert(await page.locator('[data-list="ongoing"]').getByRole('button', { name: '更新', exact: true }).first().isEnabled(), 'source read failure does not disable existing notice operations');
  }
  if (!process.argv.includes('--baseline')) {
    await page.getByRole('button', { name: '打开个人页面', exact: true }).click();
    await page.getByRole('button', { name: '退出登录', exact: true }).click();
    await page.getByRole('alert').filter({ hasText: '退出暂时失败' }).waitFor();
    assert.equal(await page.getByRole('button', { name: '退出登录', exact: true }).isEnabled(), true);
    assert.match(page.url(), /view=my/);
    assert.equal(writes.filter(url => url !== '/api/workbench/draft').length, 0, 'no real notice writes');
  }
  assert.deepEqual(errors, []);
  assert.deepEqual(oldPages, [], 'no legacy document navigation');
  console.log('PASS: isolated production notice pages, six categories, compact handling layout, existing forms and menus, logout failure.');
} finally { await browser.close(); await server.close(); }
