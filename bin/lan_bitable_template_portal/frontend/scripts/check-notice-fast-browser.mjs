import assert from 'node:assert/strict';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const dist = path.join(repo, 'bin/lan_bitable_template_portal/frontend/dist');
const output = path.join(repo, 'output/playwright/notice-fast');
await mkdir(output, { recursive: true });
const result = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import NoticeCommandRegressionBase,VALID_STARTS
test=NoticeCommandRegressionBase();test.setUp()
try:
    pages={};plans={}
    for kind,draft in VALID_STARTS.items():
        label={'maintenance':'维保','change':'变更','repair':'设备检修','power':'上电','polling':'设备轮巡','adjust':'设备调整'}[kind]
        decision=test._start_decision(kind,body_extra={'manual_binding_choice':'unbound'})
        decision['operations'][0]['body']['patch']['title']='A楼'+label+'测试'
        decision['operations'][0]['body']['patch']['progress']='已完成60%'
        plan=test.agent.prepare({'id':'fixture','scopes':['A'],'is_admin':True},decision,'browser-fast-'+kind,[])
        plan['status']='awaiting_confirmation'
        plans[kind]=test.agent.public_plan(plan)
        row={**draft,'title':'A楼'+label+'测试','active_item_id':'active-'+kind,'target_record_id':'rec-'+kind,'record_id':'rec-'+kind,
             'work_type':kind,'scope':'A','building_codes':['A'],'status':'开始'}
        payload={'records':[],'ongoing':[row]}
        args=dict(payload=payload,session={'is_admin':True,'user':{'name':'Fixture'}},scope='A',work_type=kind,month='10月',
                  scope_options=[{'value':'A','label':'A楼'}])
        html=render_workbench_lite(**args)
        detail=extract_workbench_lite_fragments(render_workbench_lite(**args,active_item_id=row['active_item_id']))['detail']
        linked_record={**draft,'record_id':'plan-'+kind,'work_type':kind,'linked_ongoing':row,
                       'display_fields':{'名称':row['title'],'楼栋':'A楼','专业':'电气','计划进度':'进行中'}}
        linked_html=render_workbench_lite(**{**args,'payload':{'records':[linked_record],'ongoing':[row]}})
        pages[kind]={'html':html,'detail':detail,'linked_html':linked_html,
                     'fragments':extract_workbench_lite_fragments(html)}
    print(json.dumps({'plans':plans,'pages':pages},ensure_ascii=False))
finally: test.tmp.cleanup()
`], { cwd: repo, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });
assert.equal(result.status, 0, result.stderr);
const fixture = JSON.parse(result.stdout);
let frameLoads = 0, fragments = 0, frameGate = null;
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === '/native') {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      const entry = fixture.pages[url.searchParams.get('kind')];
      return res.end(url.searchParams.has('source') ? entry.linked_html : entry.html);
    }
    if (url.pathname === '/workbench-lite' && url.searchParams.has('_assistant_frame')) {
      frameLoads++;
      if (frameGate) await frameGate;
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.end(fixture.pages.maintenance.html);
    }
    const appPage = ['/cabinet-power', '/workbench-lite', '/home-test', '/plan-convergence'].includes(url.pathname);
    const filename = path.resolve(dist, '.' + (appPage ? '/index.html' : url.pathname === '/' ? '/assistant.html' : url.pathname));
    assert(filename.startsWith(dist + path.sep));
    let bytes = await readFile(filename);
    if (filename.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="notice-fast-fixture" data-user-name="Fixture"'));
    res.setHeader('Content-Type', filename.endsWith('.js') ? 'application/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [];
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  let details = 0;
  let detailGate = null;
  let reviewKind = 'maintenance';
  let conversationTurns = null, confirmationCalls = 0, previewMessage = null;
  let artifactRun = '', artifactStream = '';
  let checkTagRecovery = false, tagReads = 0;
  let switchGate = null, switchFailure = false;
  const planReads = [];
  await context.route(base + '/api/**', async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/cabinet-power/bootstrap' && route.request().method() === 'POST') {
      return route.fulfill({ json: { ok: true, data: { status: 'succeeded', ready: 5, total: 5,
        buildings: [...'ABCDE'].map(scope => ({ scope, status: 'succeeded' })) } } });
    }
    if (url.pathname === '/api/workbench/draft' && route.request().method() === 'PUT') return route.fulfill({ json: { ok: true, data: { draft: { version: 1 } } } });
    assert.equal(route.request().method(), ['/api/assistant/plans/continued-plan/confirm', '/api/assistant/messages'].includes(url.pathname) ? 'POST' : 'GET', 'Only isolated assistant fixtures may receive a POST');
    let data = {};
    if (url.pathname === '/api/assistant/runs/' + artifactRun + '/stream' && artifactStream) {
      const body = artifactStream;
      artifactRun = ''; artifactStream = '';
      conversationTurns[0] = { ...conversationTurns[0], status: 'failed', error: '模型后续说明中断，文件已保存。',
        output_files: [conversationTurns[0].fixture_file] };
      return route.fulfill({ contentType: 'text/event-stream', headers: { 'x-vercel-ai-ui-message-stream': 'v1' }, body });
    }
    if (url.pathname === '/api/assistant/messages') {
      previewMessage = route.request().postDataJSON();
      const turn = { operation_id: previewMessage.operation_id, attempt_id: previewMessage.attempt_id,
        question: previewMessage.question, preview_file: conversationTurns[0].output_files[3],
        answer: '已取得你正在查看的文件引用。', status: 'completed' };
      conversationTurns.push(turn);
      return route.fulfill({ json: { ok: true, data: { conversation_id: 'notice-fast', run_id: '', turn } } });
    }
    if (url.pathname.startsWith('/api/assistant/files/')) {
      const id = url.pathname.split('/').pop();
      if (id === '1'.repeat(32)) data = { kind: 'markdown', text: '# 运行检查报告\n\n**检查结果**：本次检查已完成。\n\n| 项目 | 状态 |\n|---|---|\n| 设备 | 正常 |' };
      else if (id === '2'.repeat(32)) data = { kind: 'html', text: '<html><head><style>body{font-family:Arial;padding:24px}h1{color:#2364aa}</style></head><body><h1>设备检查报告</h1><p>设备运行正常</p><img src="https://example.com/forbidden-image"><script>parent.document.body.dataset.unsafe="yes"</script><a href="https://example.com/leak">外部链接</a></body></html>' };
      else if (id === '3'.repeat(32)) data = { kind: 'text', text: 'def bubble_sort(values):\n    result = list(values)\n    for end in range(len(result) - 1, 0, -1):\n        for i in range(end):\n            if result[i] > result[i + 1]:\n                result[i], result[i + 1] = result[i + 1], result[i]\n    return result\n' };
      else data = { kind: 'spreadsheet', sheets: [{ name: '检查表', rows: [['项目', '数量'], ['空调', '3']] }] };
      return route.fulfill({ json: { ok: true, data } });
    }
    if (url.pathname === '/api/notice-alert-tags' && checkTagRecovery) {
      tagReads++;
      data = { status: tagReads === 1 ? 'failed' : tagReads === 2 ? 'pending' : 'ready', action: 'update',
        retry_after: Date.now() / 1000, error: '推荐标签获取失败，通告业务不受影响。',
        tags: [{ label: '维护', content: '测试维护', basis: '已发维保通告', notes: '现场核对' }] };
      return route.fulfill({ json: { ok: true, data } });
    }
    if (url.pathname.startsWith('/api/plan-convergence/')) {
      planReads.push(url.pathname + url.search);
      if (url.pathname === '/api/plan-convergence/bootstrap') data = { is_admin: true, catalog_ready: true, points_ready: false,
        counts: { devices: 2, rules: 1 }, zh_ready: true, blocks: { items: [{ blockId: '123', blockName: '离线缓存记录', status: 1 }], loaded_at: 123 } };
      else if (url.pathname === '/api/plan-convergence/rulesets') data = [];
      else if (url.pathname === '/api/plan-convergence/catalog') data = { items: [] };
      else if (url.pathname === '/api/plan-convergence/blocks' && url.searchParams.get('refresh') === '1') {
        return route.fulfill({ status: 502, json: { ok: false, code: 'zh_connection_unavailable',
          error: '无法连接智航，请在运行灯塔的电脑上连接 VPN 后重试。其他功能和本地规则配置不受影响' } });
      } else assert.fail('Unexpected remote access: ' + url.pathname);
      return route.fulfill({ json: { ok: true, data } });
    }
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'notice-fast-fixture' } });
    if (url.pathname === '/api/workbench/lite-detail') {
      details++;
      const gate = detailGate;
      if (gate) await gate;
      else await new Promise(resolve => setTimeout(resolve, 180));
      data = { canonical_url: '/workbench-lite?' + url.searchParams, fragments: { detail: fixture.pages[url.searchParams.get('work_type')].detail } };
    } else if (url.pathname === '/api/workbench/lite-fragment') {
      if (switchGate) {
        const gate = switchGate;
        await gate;
        if (switchFailure) return route.fulfill({ status: 503, json: { ok: false, error: '隔离读取失败' } });
        return route.fulfill({ json: { ok: true, data: { canonical_url: '/workbench-lite?' + url.searchParams,
          fragments: fixture.pages[url.searchParams.get('work_type')].fragments } } });
      }
      fragments++;
      return route.fulfill({ status: 503, json: { ok: false, error: 'Fixture must not reload unchanged lists' } });
    } else if (url.pathname === '/api/auth/status') data = { logged_in: true, user: { open_id: 'fixture-nav', name: 'Fixture', role: 'admin' },
      scope_options: [...'ABCDE'].map(value => ({ value, label: value + '楼' })) };
    else if (url.pathname === '/api/cabinet-power/buildings') data = { buildings: [...'ABCDE'].map(scope => ({ scope, bootstrap_status: 'succeeded',
      counts: { total: 100, powered: 80, formal: 60, test: 20, off: 20, unknown: 0 }, record_count: 100, inventory_only: 0 })) };
    else if (url.pathname === '/api/cabinet-power/exports/batches') data = { items: [] };
    else if (url.pathname === '/api/scope-overview') data = { scopes: {} };
    else if (url.pathname === '/api/handover-links') data = { links: {} };
    else if (url.pathname === '/api/assistant/appearance') data = { color: 'encre', size: 56, shape: 'cercle', snap_back: true };
    else if (url.pathname === '/api/assistant/plans/continued-plan/confirm') {
      confirmationCalls++;
      assert.equal(route.request().postDataJSON().version, 3);
      assert.equal(route.request().postDataJSON().stage, 'review');
      data = { ...conversationTurns[1].plan, status: 'completed', fields: [] };
      for (const turn of conversationTurns) turn.plan = data;
    }
    else if (url.pathname === '/api/assistant/conversation') data = { configured: true, enabled: true, conversation_id: 'notice-fast', busy: !!artifactRun, active_run_id: artifactRun,
      turns: conversationTurns || [{ operation_id: 'notice-fast', question: '发送通告', answer: '请核对通告后确认。', status: 'completed', plan: fixture.plans[reviewKind] }] };
    return route.fulfill({ json: { ok: true, data } });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(base + '/native?kind=adjust');
  for (const [index, kind] of Object.keys(fixture.pages).entries()) {
    await page.setViewportSize({ width: [1280, 1366, 1920][index % 3], height: 768 });
    let releaseSwitch;
    switchGate = new Promise(resolve => { releaseSwitch = resolve; });
    await page.locator(`.type-tab[href*="work_type=${kind}"]`).click();
    await page.locator('.workspace.is-switching .workspace-loading-status').waitFor();
    const layout = await page.locator('.workspace-loading-status').evaluate(node => {
      const bounds = element => { const r = element.getBoundingClientRect(); return { left: r.left, top: r.top, right: r.right, bottom: r.bottom }; };
      return { workspace: bounds(node.closest('.workspace')), status: bounds(node), spinner: bounds(node.firstElementChild),
        label: bounds(node.lastElementChild), viewport: innerWidth, animation: getComputedStyle(node.firstElementChild).animationName,
        scrolling: document.documentElement.scrollWidth > innerWidth };
    });
    const inside = (outer, inner) => inner.left >= outer.left && inner.right <= outer.right && inner.top >= outer.top && inner.bottom <= outer.bottom;
    assert(inside(layout.workspace, layout.status), 'loading status stays inside the workspace');
    assert(inside(layout.status, layout.spinner) && inside(layout.status, layout.label), 'spinner and text stay inside one status box');
    assert(layout.spinner.right < layout.label.left, 'spinner never overlaps the label');
    assert.equal(layout.animation, 'liteSpin');
    assert.equal(layout.scrolling, false, 'switching never creates horizontal overflow');
    if (index === 0) await page.screenshot({ path: path.join(output, 'category-loading.png') });
    switchFailure = index === 5;
    releaseSwitch();
    await page.locator('.workspace.is-switching').waitFor({ state: 'detached' });
    assert.equal(await page.locator('.workspace-loading-status:visible').count(), 0, 'success and failure both clear the loading state');
    switchGate = null; switchFailure = false;
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base + '/native?kind=maintenance');
  let releaseSwitch;
  switchGate = new Promise(resolve => { releaseSwitch = resolve; });
  for (const kind of ['change', 'repair']) {
    const requested = page.waitForRequest(request => new URL(request.url()).pathname === '/api/workbench/lite-fragment'
      && new URL(request.url()).searchParams.get('work_type') === kind);
    await page.locator(`.type-tab[href*="work_type=${kind}"]`).click();
    await requested;
  }
  assert.equal(await page.locator('.workspace-loading-status').count(), 1, 'rapid switching reuses one loading status');
  releaseSwitch();
  await page.locator('.workspace.is-switching').waitFor({ state: 'detached' });
  assert.equal(new URL(page.url()).searchParams.get('work_type'), 'repair', 'an outdated response cannot override the latest category');
  switchGate = null;
  for (const kind of Object.keys(fixture.pages)) {
    let releaseDetail;
    detailGate = new Promise(resolve => { releaseDetail = resolve; });
    await page.goto(base + '/native?kind=' + kind);
    const row = page.locator('.ongoing-row').first();
    await row.click();
    try { await page.locator('#lite-notice-detail-overlay.open').waitFor(); }
    catch (error) {
      await page.screenshot({ path: path.join(output, 'opening-failure-' + kind + '.png') });
      console.error(kind, await page.locator('#lite-notice-detail-overlay').evaluate(el => ({ outer: el.outerHTML.slice(0, 1200), css: { display: getComputedStyle(el).display, opacity: getComputedStyle(el).opacity, visibility: getComputedStyle(el).visibility } })), errors);
      throw error;
    }
    assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-' + kind);
    assert.equal(await page.locator('[name=active_item_id]').inputValue(), 'active-' + kind);
    await page.evaluate(() => { window.fixtureDrawer = document.getElementById('lite-notice-detail-overlay'); });
    const before = await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width);
    assert.equal(await page.locator('#lite-notice-detail-overlay').evaluate(el => getComputedStyle(el).backdropFilter), 'none');
    assert.equal(await page.locator('button[name=submit_action]').count(), 2);
    assert.equal(await page.locator('.manual-source-binding-head strong').innerText(), '源表事项关联（更新前可选）');
    assert.equal(await page.locator('.manual-source-binding-actions [data-manual-binding-mode=unbound]').isVisible(), false);
    if (kind === 'maintenance') {
      await page.locator('[name=content]').fill('User edit must survive background verification');
      detailGate = null; releaseDetail();
      await page.waitForTimeout(450);
      assert.equal(await page.locator('[name=content]').inputValue(), 'User edit must survive background verification');
      checkTagRecovery = true;
      await page.evaluate(() => refreshNoticeTags());
      await page.locator('#lite-alert-tags').getByText('推荐标签获取失败，通告业务不受影响。', { exact: true }).waitFor();
      await page.locator('#lite-alert-tags .tag-loading').waitFor({ timeout: 16000 });
      await page.locator('#lite-alert-tags').getByText('【维护】测试维护', { exact: true }).waitFor({ timeout: 10000 });
      assert.equal(tagReads, 3);
      await page.screenshot({ path: path.join(output, 'recovered-tags.png'), animations: 'disabled' });
      checkTagRecovery = false;
      await page.screenshot({ path: path.join(output, 'ongoing-detail.png') });
      await page.evaluate(() => {
        const form = document.getElementById('lite-notice-form');
        setFormValue(form, 'source_record_id', 'fixture-existing-binding');
        setFormValue(form, 'manual_binding_choice', 'bind');
        syncManualSourceBindingControls(form);
      });
      assert.equal(await page.locator('.manual-source-binding-head strong').innerText(), '源表');
      assert.equal(await page.locator('.manual-source-recommendations').isVisible(), false);
      assert.equal(await page.getByRole('button', { name: '重新绑定', exact: true }).isVisible(), true);
    } else { detailGate = null; releaseDetail(); await page.waitForTimeout(450); }
    assert(Math.abs(await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width) - before) < 1);
    assert(await page.evaluate(() => window.fixtureDrawer === document.getElementById('lite-notice-detail-overlay')), 'detail must not recreate the drawer shell');
    const preview = page.locator('#lite-notice-preview');
    const originalPreview = await preview.textContent();
    assert.equal(await page.locator('#lite-preview-action').innerText(), '更新');
    assert.equal(await preview.locator('.preview-label').first().innerText(), originalPreview.split('】')[0] + '】');
    assert.equal(await preview.locator('.preview-title').innerText(), originalPreview.split('\n')[1]);
    assert.equal(await page.locator('.preview-head #lite-completion-hint').count(), 0);
    assert.equal(await page.locator('.preview-note#lite-completion-hint').count(), 1);
    const originalAction = await page.locator('#lite-notice-form').getAttribute('data-action');
    await page.locator('.notice-drawer-body').evaluate(node => { node.scrollTop = node.scrollHeight; });
    const previewHeight = (await preview.boundingBox()).height;
    await preview.hover();
    assert(Math.abs((await preview.boundingBox()).height - previewHeight) < 1, 'hover must not resize the preview');
    await page.getByRole('button', { name: '展开全文', exact: true }).click();
    assert.equal(await page.locator('[data-preview-toggle]').getAttribute('aria-expanded'), 'true');
    assert.equal(await preview.textContent(), originalPreview, 'preview expansion cannot change notice text');
    assert.equal(await page.locator('#lite-notice-form').getAttribute('data-action'), originalAction);
    await page.getByRole('button', { name: '收起全文', exact: true }).click();
    assert.equal(await page.locator('.form-action-buttons button[name=submit_action]').count(), 2);
    assert.equal(await page.locator('.form-action-buttons button[data-ongoing-delete-mode]').count(), 1);
    await page.evaluate(() => setButtonBusy(document.querySelector('.form-action-buttons button[value=end]'), true));
    assert.equal(await page.locator('.form-action-buttons button[value=end]').evaluate(button =>
      getComputedStyle(button, '::after').borderTopColor), 'rgb(8, 108, 67)', 'end action needs a visible loading spinner');
    await page.evaluate(() => setButtonBusy(document.querySelector('.form-action-buttons button[value=end]'), false));
    await page.locator('.notice-drawer-body').evaluate(node => { node.scrollTop = node.scrollHeight; });
    for (const { width, height } of [{ width: 1280, height: 800 }, { width: 1366, height: 768 }, { width: 1920, height: 1080 }, { width: 1440, height: 1000 }]) {
      await page.setViewportSize({ width, height });
      await page.locator('.notice-drawer-body').evaluate(node => { node.scrollTop = node.scrollHeight; });
      const sizing = await page.locator('.notice-submit-bar').evaluate(node => ({
        width: node.clientWidth, content: node.scrollWidth,
        contextWidth: node.querySelector('.notice-submit-context').getBoundingClientRect().width,
        buttons: [...node.querySelectorAll('button')].map(button => {
          const box = button.getBoundingClientRect();
          return { left: box.left, right: box.right, top: box.top, bottom: box.bottom };
        }),
      }));
      assert(sizing.content <= sizing.width + 1, `${kind} footer overflows at ${width}`);
      assert(sizing.contextWidth >= sizing.width - 28, 'footer rows should use the available width');
      for (const box of sizing.buttons) {
        assert(box.left >= 0 && box.right <= width && box.top >= 0 && box.bottom <= height,
          `${kind} footer action is clipped at ${width}x${height}`);
      }
      for (let i = 1; i < sizing.buttons.length; i++) {
        assert(sizing.buttons[i].left >= sizing.buttons[i - 1].right || sizing.buttons[i].top >= sizing.buttons[i - 1].bottom);
      }
    }
    await page.locator('#detail-panel').screenshot({ path: path.join(output, `footer-${kind}.png`), animations: 'disabled' });
    if (kind === 'maintenance') {
      const longText = Array.from({ length: 40 }, (_, index) => `第${index + 1}项：核对设备运行状态，确认本次操作内容。`).join('\n');
      await preview.evaluate((node, text) => { node.textContent = text; }, longText);
      const toggle = page.locator('[data-preview-toggle]');
      await toggle.focus();
      await toggle.press('Enter');
      assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
      assert((await preview.boundingBox()).height > 248, 'long notice should expand beyond the compact preview');
      assert.equal(await preview.textContent(), longText);
      await toggle.press('Space');
      assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
      assert((await preview.boundingBox()).height <= 248, 'keyboard collapse should restore the compact height');
      await page.evaluate(() => {
        const form = document.getElementById('lite-notice-form');
        setUnuploadedSubmitButtons(form);
      });
      assert.equal(await page.locator('.form-action-buttons button[value=start]').count(), 1);
      assert.equal(await page.locator('.notice-submit-toolbar #lite-job-status').count(), 1);
      assert.equal(await page.locator('.notice-submit-context [name=actual_action_time]').count(), 1);
      assert.equal(await page.locator('#lite-preview-action').innerText(), '开始');
      await page.evaluate(() => {
        const form = document.getElementById('lite-notice-form');
        setOngoingSubmitButtons(form);
        document.getElementById('lite-job-status').textContent = '上传未完成，填写内容已保留。'.repeat(12);
      });
      assert.equal(await page.locator('.form-action-buttons button[name=submit_action]').count(), 2);
      assert.equal(await page.locator('.form-actions > button[name=submit_action]').count(), 0);
      const fits = await page.locator('.notice-submit-bar').evaluate(node => node.scrollWidth <= node.clientWidth + 1);
      assert(fits, 'long submission status must wrap without pushing actions outside the footer');
      const content = '<img src=x onerror="window.previewInjected=true"> & 字段原文';
      await page.locator('[name=content]').fill(content);
      assert((await preview.textContent()).includes(content), 'formatted preview must preserve literal user content');
      assert.equal(await preview.locator('img,script').count(), 0, 'preview may not render input as HTML');
      assert.equal(await page.evaluate(() => window.previewInjected), undefined);
    }
  }
  assert.equal(details, 6);
  await page.goto(base + '/native?kind=maintenance&source=1');
  const linkedSource = page.locator('.notice-row[data-linked-ongoing="1"]').first();
  assert.equal(await linkedSource.getAttribute('aria-disabled'), 'true', 'started plans keep their original disabled entry');
  await page.locator('.ongoing-row').first().click();
  await page.locator('#lite-notice-detail-overlay.open').waitFor();
  const linkedWidth = await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width);
  assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-maintenance');
  assert.equal(await page.locator('[name=active_item_id]').inputValue(), 'active-maintenance');
  assert.equal(await page.locator('button[name=submit_action][value=update]').count(), 1);
  await page.waitForTimeout(1300);
  assert(Math.abs(await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width) - linkedWidth) < 1, 'loading a binding panel must not resize the drawer');
  assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-maintenance');
  assert.equal(details, 7, 'a linked plan is operated through its original ongoing notice, not another start');
  await page.locator('#lite-notice-drawer-close').click();
  await page.evaluate(() => { window.fixtureTimeout = window.setTimeout; window.setTimeout = (fn, ms, ...args) => window.fixtureTimeout(fn, ms === 15000 ? 60 : ms, ...args); });
  detailGate = new Promise(() => {});
  await page.evaluate(async () => {
    await navigateLite('/workbench-lite?scope=A&work_type=maintenance&active_item_id=active-maintenance', { detailOnly: true })
      .then(() => { throw new Error('Expected timeout'); }, error => { if (!error.message.includes('读取超时')) throw error; });
  });
  assert.equal(await page.locator('#detail-panel.loading').count(), 0);
  assert.equal(await page.locator('.ongoing-row').count(), 1, 'timeout retains the current list');
  detailGate = null;
  await page.evaluate(() => { window.setTimeout = window.fixtureTimeout; });
  let releaseLate;
  detailGate = new Promise(resolve => { releaseLate = resolve; });
  await page.locator('.ongoing-row').first().click();
  await page.locator('#lite-notice-detail-overlay.open').waitFor();
  await page.locator('#lite-notice-drawer-close').click();
  detailGate = null; releaseLate();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('#lite-notice-detail-overlay.open').count(), 0, 'a late response must not reopen a closed notice');
  for (const kind of Object.keys(fixture.plans)) {
    reviewKind = kind;
    await page.goto(base);
    await page.locator('.assistant-launcher').waitFor();
    const opener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    if (await opener.count()) await opener.click();
    await page.locator('.operation-plan').waitFor();
    try {
      await page.waitForFunction(() => document.querySelector('.operation-plan')?.innerText.includes('作为独立通告'));
    } catch (error) {
      await page.screenshot({ path: path.join(output, `assistant-${kind}-review-failed.png`) });
      throw error;
    }
    const text = await page.locator('.operation-plan').innerText();
    for (const internal of ['building', 'location', 'notice_type', 'building_codes', 'polling_run_count', 'polling_runs']) assert(!text.includes(internal), 'raw field visible: ' + internal);
    assert(text.includes('作为独立通告'));
    if (kind === 'adjust') assert(!text.includes('进度') && !text.includes('已完成60%'));
    else assert(text.includes(kind === 'repair' ? '完成情况' : '进度'));
    if (kind === 'power') assert(text.includes('柜号') && !text.includes('原因'));
    if (kind === 'polling') assert(text.includes('设备') && !text.includes('维保周期'));
    await page.screenshot({ path: path.join(output, 'assistant-' + kind + '-review.png') });
  }
  reviewKind = 'maintenance';
  fixture.plans.maintenance.status = 'running';
  fixture.plans.maintenance.results = [{ ok: true, api_id: 'POST /api/workbench-actions', data: { job_id: 'same-job' },
    job_result: { ok: true, data: { job_id: 'same-job', phase: 'uploading', status: 'running' } } }];
  await page.goto(base);
  await page.locator('.assistant-launcher').waitFor();
  const progressOpener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  if (await progressOpener.count()) await progressOpener.click();
  await page.locator('.operation-plan').waitFor();
  const runningText = await page.locator('.plan-title > span').innerText();
  assert.equal(runningText, '正在上传多维');
  assert.equal(await page.locator('.plan-results > div > span').innerText(), '正在上传多维');

  const continuedPlan = { id: 'continued-plan', title: '冷水机组月度维护', status: 'needs_input', version: 3,
    fields: [{ name: 'note', label: '本次进度', type: 'text', value: '', required: true }],
    planned_notice: { stage: 'preview', month: '10月' }, operations: [], results: [] };
  conversationTurns = [1, 3, 2].map((version, index) => ({ operation_id: 'continued-' + index, at: index + 1,
    question: ['发送冷水机组月度维护', 'A楼', '第二条'][index], answer: '已保留这项操作，请补充现场进度。', status: 'completed',
    plan: { ...structuredClone(continuedPlan), version } }));
  await page.goto(base);
  const continuedOpener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  if (await continuedOpener.count()) await continuedOpener.click();
  await page.locator('.operation-plan').waitFor();
  assert.equal(await page.locator('.operation-plan').count(), 1, 'one plan must have only one editable form');
  assert.equal(await page.locator('.plan-reference').count(), 2);
  assert.equal(await page.locator('.header-status').innerText(), '待你补充');
  assert.equal(await page.getByRole('textbox', { name: '本次进度', exact: true }).count(), 1);
  await page.getByRole('textbox', { name: '本次进度', exact: true }).fill('工程师已到场核对');
  await page.screenshot({ path: path.join(output, 'assistant-continued-plan.png') });
  for (const turn of conversationTurns) turn.plan.status = 'awaiting_confirmation';
  await page.goto(base);
  const confirmOpener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  if (await confirmOpener.count()) await confirmOpener.click();
  await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
  assert.equal(await page.locator('.header-status').innerText(), '待你确认');
  await page.getByRole('textbox', { name: '询问灯塔助手', exact: true }).fill('确认发送');
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.operation-plan')?.getAttribute('data-plan-status') === 'completed');
  assert.equal(confirmationCalls, 1, 'same-plan history must not be mistaken for multiple operations');
  assert.equal(await page.locator('.operation-plan').count(), 1);
  assert.equal(await page.locator('.assistant-error').count(), 0);
  const outputs = ['检查报告.md', '检查报告.html', 'bubble_sort.py', '检查表.xlsx'].map((name, i) => {
    const id = String(i + 1).repeat(32);
    return { id, name, url: '/api/assistant/files/' + id, size: 512 };
  });
  conversationTurns = [{ operation_id: 'artifacts', question: '整理检查报告并生成文件', status: 'completed',
    answer: '已生成文件，可查看或下载。', output_files: outputs }];
  let externalRequests = 0;
  await context.route('https://example.com/**', route => { externalRequests++; return route.abort(); });
  await page.goto(base);
  const artifactOpener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  if (await artifactOpener.count()) await artifactOpener.click();
  await page.getByRole('button', { name: '预览 检查报告.md', exact: true }).click();
  await page.locator('.artifact-preview h1').waitFor();
  await page.waitForFunction(() => !document.querySelector('.lighthouse')?.classList.contains('resizing'));
  assert.equal(await page.locator('.artifact-preview strong').last().innerText(), '检查结果');
  const bounds = await page.locator('.assistant-shell').evaluate(node => {
    const chat = node.querySelector('.assistant-panel').getBoundingClientRect();
    const preview = node.querySelector('.artifact-preview').getBoundingClientRect();
    return { chatRight: chat.right, previewLeft: preview.left, right: preview.right, width: innerWidth };
  });
  assert(bounds.previewLeft >= bounds.chatRight - 1, 'file preview must be to the right of the conversation');
  await page.screenshot({ path: path.join(output, 'assistant-artifact-markdown.png') });
  assert(bounds.right <= bounds.width + 1, 'preview must stay inside the desktop viewport: ' + JSON.stringify(bounds));
  await page.screenshot({ path: path.join(output, 'assistant-artifact-markdown.png') });
  await page.getByRole('button', { name: '预览 检查报告.html', exact: true }).click();
  const webPreview = page.frameLocator('.artifact-preview iframe');
  await webPreview.getByRole('heading', { name: '设备检查报告' }).waitFor();
  assert.equal(await page.getAttribute('body', 'data-unsafe'), null);
  assert.equal(await webPreview.locator('script,a[href],meta[http-equiv=refresh]').count(), 0);
  assert.equal(externalRequests, 0, 'generated webpage preview may not access external resources');
  await page.getByRole('button', { name: '源码', exact: true }).click();
  assert((await page.locator('.artifact-preview .source-text').innerText()).includes('<html>'));
  await page.getByRole('button', { name: '预览 bubble_sort.py', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.artifact-preview .source-text')?.textContent.includes('def bubble_sort'));
  await page.getByRole('button', { name: '预览 检查表.xlsx', exact: true }).click();
  await page.locator('.artifact-preview table').waitFor();
  assert.equal(await page.locator('.artifact-preview td').last().innerText(), '3');
  await page.screenshot({ path: path.join(output, 'assistant-artifact-spreadsheet.png') });
  await page.getByRole('textbox', { name: '询问灯塔助手', exact: true }).fill('将这个表格中的数量更正为4');
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.assistant-rich-reply:last-of-type')?.textContent.includes('文件引用')
    || document.querySelector('.thread')?.textContent.includes('文件引用'));
  assert.equal(previewMessage.preview_file_id, '4'.repeat(32), 'file followup must reference the current preview');
  assert.equal(previewMessage.file_ids.length, 0, 'preview does not silently upload a second attachment');
  await page.getByRole('button', { name: '关闭文件预览', exact: true }).click();
  assert.equal(await page.locator('.artifact-preview').count(), 0);
  artifactRun = 'a'.repeat(32);
  conversationTurns = [{ operation_id: 'artifact-interrupted', run_id: artifactRun, question: '生成代码示例',
    status: 'pending', answer: '', output_files: [], fixture_file: outputs[2] }];
  artifactStream = [
    { type: 'start', messageId: 'reply_' + artifactRun },
    { type: 'text-start', id: 'artifact-text' },
    { type: 'data-turn', data: { operation_id: 'artifact-interrupted', run_id: artifactRun, output_files: [outputs[2]] }, transient: true },
    { type: 'data-turn', data: { operation_id: 'artifact-interrupted', run_id: artifactRun, status: 'failed', error: '模型后续说明中断，文件已保存。' }, transient: true },
    { type: 'text-end', id: 'artifact-text' },
    { type: 'finish', finishReason: 'error' },
  ].map(value => 'data: ' + JSON.stringify(value) + '\n\n').join('') + 'data: [DONE]\n\n';
  await page.reload();
  if (await artifactOpener.count()) await artifactOpener.click();
  await page.getByText('模型后续说明中断，文件已保存。', { exact: true }).waitFor();
  assert.equal(await page.locator('.output-file').count(), 1, 'a failed explanation must retain the completed file');
  await page.getByRole('button', { name: '预览 bubble_sort.py', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.artifact-preview .source-text')?.textContent.includes('def bubble_sort'));
  await page.screenshot({ path: path.join(output, 'assistant-artifact-interrupted.png') });
  await page.reload();
  if (await artifactOpener.count()) await artifactOpener.click();
  await page.getByRole('button', { name: '预览 bubble_sort.py', exact: true }).waitFor();
  assert.equal(await page.locator('.output-file').count(), 1, 'reloading must keep the same completed file');
  conversationTurns = null;

  const shell = await context.newPage();
  shell.on('pageerror', error => errors.push(error.message));
  await shell.goto(base + '/workbench-lite?scope=A&work_type=maintenance');
  const native = shell.frameLocator('iframe[title="通告管理"]');
  await native.locator('.ongoing-row').first().waitFor();
  // Home entry omits month and pagination; retained child URLs include them.
  await shell.locator('iframe').evaluate(el => { el.contentWindow.setLiteLocation('/workbench-lite?work_type=maintenance&scope=A&month=' + (new Date().getMonth()+1) + '%E6%9C%88&pending_page=1&ongoing_page=1', true); });
  await shell.locator('iframe').evaluate(el => { el.contentWindow.fixtureDocument = 'preserve-me'; });
  // Use the application's existing history navigation rather than reloading the document.
  const noticeExit = await shell.evaluate(async () => {
    history.pushState({}, '', '/home-test'); dispatchEvent(new PopStateEvent('popstate'));
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const frame = document.querySelector('.workbench-frame-page');
    return { inert: frame.inert, displayed: getComputedStyle(frame).display !== 'none',
      transitioning: frame.classList.contains('ui-page-leave-active'), duration: getComputedStyle(frame).transitionDuration };
  });
  assert(noticeExit.inert && noticeExit.displayed && noticeExit.transitioning, 'retained notices must fade out without accepting actions: ' + JSON.stringify(noticeExit));
  assert(noticeExit.duration.includes('0.18s'));
  await shell.waitForFunction(() => document.querySelector('iframe')?.closest('section')?.style.display === 'none');
  const noticeEntry = await shell.evaluate(async () => {
    history.pushState({}, '', '/workbench-lite?scope=A&work_type=maintenance'); dispatchEvent(new PopStateEvent('popstate'));
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const frame = document.querySelector('.workbench-frame-page');
    return { inert: frame.inert, entering: frame.classList.contains('ui-page-enter-active') };
  });
  assert(noticeEntry.entering && !noticeEntry.inert, 'reopened cached notices must fade in and restore interaction');
  await native.locator('.ongoing-row').first().waitFor();
  assert.equal(frameLoads, 1, 'returning to notices must reuse the native document');
  assert.equal(await shell.locator('iframe').evaluate(el => el.contentWindow.fixtureDocument), 'preserve-me');
  await shell.waitForTimeout(400);
  assert.equal(fragments, 0, 'returning with equivalent defaults must not fetch the workbench again');
  await shell.locator('iframe').evaluate(el => {
    el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'same-version' });
    el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'changed-version' });
  });
  await shell.waitForTimeout(400);
  assert.equal(fragments, 1, 'a real source version change still refreshes the retained list');
  assert.equal(await native.locator('.ongoing-row').count(), 1, 'failed refresh retains visible records');
  await shell.locator('iframe').evaluate(el => { el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'changed-version' }); });
  await shell.waitForTimeout(200);
  assert.equal(fragments, 1, 'the same source version never repeatedly refreshes');
  await shell.evaluate(() => { history.pushState({}, '', '/cabinet-power'); dispatchEvent(new PopStateEvent('popstate')); });
  await shell.locator('.buildings .building').nth(4).waitFor();
  await shell.waitForTimeout(230);
  const queryMotion = await shell.evaluate(async () => {
    window.fixtureCabinetPage = document.querySelector('.cabinet-page');
    history.pushState({}, '', '/cabinet-power?motion-check=1'); dispatchEvent(new PopStateEvent('popstate'));
    await new Promise(resolve => requestAnimationFrame(resolve));
    const animations = document.querySelector('.page-content').getAnimations();
    return { samePage: window.fixtureCabinetPage === document.querySelector('.cabinet-page'),
      duration: animations[0]?.effect.getTiming().duration, count: animations.length };
  });
  assert.deepEqual(queryMotion, { samePage: true, duration: 180, count: 1 }, 'query navigation fades without remounting a cached component');
  await shell.emulateMedia({ reducedMotion: 'reduce' });
  const reducedMotionCount = await shell.evaluate(async () => {
    history.pushState({}, '', '/cabinet-power?motion-check=2'); dispatchEvent(new PopStateEvent('popstate'));
    await new Promise(resolve => requestAnimationFrame(resolve));
    return document.querySelector('.page-content').getAnimations().length;
  });
  assert.equal(reducedMotionCount, 0, 'reduced motion cancels the active route animation');
  await shell.emulateMedia({ reducedMotion: 'no-preference' });
  for (const width of [1366, 1440, 1920]) {
    await shell.setViewportSize({ width, height: 1000 });
    const boxes = await shell.locator('.buildings .building').evaluateAll(elements => elements.map(el => {
      const r = el.getBoundingClientRect(); return { top: r.top, left: r.left, right: r.right };
    }));
    assert.equal(boxes.length, 5);
    assert(boxes.every(box => Math.abs(box.top - boxes[0].top) < 2), 'five PC buildings must occupy one row');
    const pageWidth = await shell.locator('.cabinet-page').evaluate(el => el.getBoundingClientRect().width);
    assert(pageWidth > Math.min(width - 100, 1750), 'cabinet pages must not shrink to the intrinsic card width');
  }
  await shell.screenshot({ path: path.join(output, 'cabinet-wide.png'), fullPage: true });
  await shell.close();
  let releaseFrame;
  frameGate = new Promise(resolve => { releaseFrame = resolve; });
  const late = await context.newPage();
  late.on('pageerror', error => errors.push(error.message));
  await late.addInitScript(() => { const original = window.setTimeout; window.setTimeout = (fn, ms, ...args) => original(fn, ms === 15000 ? 1200 : ms, ...args); });
  await late.goto(base + '/workbench-lite?scope=A&work_type=maintenance', { waitUntil: 'domcontentloaded' });
  await late.locator('.async-page-state.failed').waitFor();
  frameGate = null; releaseFrame();
  await late.frameLocator('iframe[title="通告管理"]').locator('.ongoing-row').first().waitFor();
  await late.locator('.frame-state').waitFor({ state: 'detached' });
  await late.close();
  const offline = await context.newPage();
  offline.on('pageerror', error => errors.push(error.message));
  await offline.goto(base + '/plan-convergence');
  await offline.locator('.pc-record').first().waitFor();
  assert.match(await offline.locator('.pc-alert.warning').innerText(), /电脑连接 VPN/);
  assert.deepEqual(planReads, ['/api/plan-convergence/bootstrap', '/api/plan-convergence/rulesets'], 'opening must only read local state');
  await offline.getByRole('button', { name: '刷新屏蔽记录', exact: true }).click();
  await offline.locator('.pc-alert.error').waitFor();
  assert.match(await offline.locator('.pc-alert.error').innerText(), /VPN/);
  assert.match(await offline.locator('.pc-record').innerText(), /离线缓存记录/);
  assert.equal(await offline.getByRole('button', { name: '刷新屏蔽记录', exact: true }).isEnabled(), true);
  await offline.screenshot({ path: path.join(output, 'vpn-offline-plan.png'), fullPage: true });
  await offline.getByRole('button', { name: '规则配置', exact: true }).click();
  await offline.locator('.pc-rules').waitFor({ state: 'visible' });
  await offline.evaluate(() => { history.pushState({}, '', '/cabinet-power'); dispatchEvent(new PopStateEvent('popstate')); });
  await offline.locator('.buildings .building').nth(4).waitFor();
  await offline.screenshot({ path: path.join(output, 'vpn-offline-other-page.png'), fullPage: true });
  await offline.close();
  assert.deepEqual(errors, []);
  console.log('[NoticeFastBrowser] OK: stable local drawers, cached page transitions, reduced motion, wide PC buildings and isolated VPN failures. No business writes.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
