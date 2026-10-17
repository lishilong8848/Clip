import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build, preview } from 'vite';
import { chromium } from 'playwright';
import { layoutApiFixture } from './layout-api-fixture.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/plan-chart-recovery');
await mkdir(output, { recursive: true });
const styles = (await readFile(path.join(root, 'src/workspace-v2/main.ts'), 'utf8'))
  .split(/\r?\n/).filter(line => /^import ['"].+\.css['"];/.test(line)).join('\n');
// Exercise production adapters and components without touching the shared dist or business service.
const fixture = `
import {createApp,h,provide,computed,reactive} from 'vue';
import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
import Plan from '/src/components/PlanConvergencePage.vue';
import Motion from '/src/workspace-v2/MotionChart.vue';
import Learning from '/src/workspace-v2/LearningTrend.vue';
import Water from '/src/workspace-v2/WaterBuildingTrend.vue';
import UiTransition from '/src/components/UiTransition.vue';
import {pageContext} from '/src/workspace-v2/pageLayout';
import {installActionMotion} from '/src/workspace-v2/actionMotion';
${styles}
document.documentElement.classList.add('workspace-v2');installActionMotion();
const params=new URLSearchParams(location.search),admin=params.get('viewer')!=='1';
const items=Array.from({length:20},(_,n)=>({label:'2026-10-'+String(n+1).padStart(2,'0'),value:(n*7)%53+1,start:Date.parse('2026-10-01T00:00:00+08:00')+n*86400000,end:Date.parse('2026-10-01T00:00:00+08:00')+(n+1)*86400000}));
const state=reactive({view:'line',items,loading:false,learning:items,water:{days:items.map(r=>r.label),series:[{scope:'A',label:'A楼',values:items.map(r=>r.value)},{scope:'B',label:'B楼',values:items.map(r=>r.value+1)}],missing:false}});
window.chartFixture=state;
createApp({setup(){
 provide(pageContext,{user:computed(()=>({open_id:'isolated-review',name:'隔离测试',role:admin?'admin':'user',modules:['plan_convergence']})),scopes:computed(()=>['A']),scope:computed(()=>'A'),module:computed(()=>'plan_convergence')});
 return ()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(NMessageProvider,{},()=>params.get('charts')==='1'?h('main',{style:'max-width:1100px;margin:auto;padding:12px'},[
 h(Motion,{title:'切换坐标系回归',items:state.items,view:state.view,loading:state.loading}),
 h(Learning,{items:state.learning,unit:'%',percentage:true,loading:state.loading}),
 h(Water,{data:state.water,loading:state.loading,failed:false})]):h(Plan))));
}}).component('UiTransition',UiTransition).mount('#app');
`;
const buildDir = path.join(output, 'build/assets/workspace-v2');
process.env.CLIPFLOW_WORKSPACE_BUILD_DIR = buildDir;
await build({
  root, configFile: path.join(root, 'vite.workspace-v2.config.ts'),
  build: { minify: false },
  cacheDir: path.join(output, 'vite-cache'), logLevel: 'error',
  plugins: [{ name: 'isolated-plan-chart-fixture', enforce: 'pre',
    load(id) { if (id.replaceAll('\\', '/').endsWith('/src/workspace-v2/main.ts')) return fixture; },
  }],
});
const server = await preview({ root, configFile: false, base: '/assets/workspace-v2/',
  build: { outDir: buildDir }, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, proxy: {} } });
let browser, activePage;
try {
  const base = `http://127.0.0.1:${server.httpServer.address().port}`;
  const entry = base + '/assets/workspace-v2/index.html?__page=/plan-convergence';
  browser = await chromium.launch({ headless: true });
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    const page = await context.newPage(), errors = [], calls = [], layouts = layoutApiFixture();
    activePage = page;
    let admin = true, failCheck = true, rule, nextId = 1, failRecords = false, incompleteRecords = false;
    let readDelay = 0, failRuleList = false, failRuleDetail = false;
    let holdRead = false, releaseRead;
    const waitRead = () => holdRead ? new Promise(resolve => { releaseRead = resolve; }) : new Promise(resolve => setTimeout(resolve, readDelay));
    page.setDefaultTimeout(20000);
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (/Failed to resolve component/.test(message.text())) errors.push(message.text()); });
    await page.route('**/api/**', async route => {
      if (await layouts.handle(route, 'isolated-review')) return;
      const request = route.request(), url = new URL(request.url());
      assert.equal(url.origin, base, 'all business calls stay on the public page origin');
      const method = request.method(), pathname = url.pathname;
      calls.push({ method, pathname, body: method === 'GET' ? null : request.postDataJSON() });
      const reply = data => route.fulfill({ json: { ok: true, data } });
      if (pathname.endsWith('/bootstrap')) return reply({ is_admin: admin, catalog_ready: true, points_ready: true, blocks: { items: [] } });
      if (pathname.endsWith('/settings/browser-login')) return reply({ status: 'idle' });
      if (pathname.endsWith('/settings')) return reply({ has_token: true, expired: false });
      if (/\/(maintenance|change)\/records$/.test(pathname)) {
        await waitRead();
        if (failRecords) { failRecords = false; return route.fulfill({ status: 502, json: { ok: false, error: '记录读取中断' } }); }
        if (incompleteRecords) { incompleteRecords = false; return reply({}); }
        return reply([{ record_id: 'original', name: 'A楼原记录', building: 'A楼', status: '开始', hits: [{ blockId: 1, blockName: '上次已匹配', reason: '设备一致' }], check_status: 'ready' }]);
      }
      if (/\/(maintenance|change)\/check$/.test(pathname)) {
        await new Promise(resolve => setTimeout(resolve, 180));
        if (failCheck) { failCheck = false; return route.fulfill({ status: 502, json: { ok: false, code: 'zh_connection_unavailable', error: '无法连接智航，请在运行灯塔的电脑上连接 VPN 后重试。' } }); }
        return reply({ records: [{ record_id: 'original', name: 'A楼原记录', building: 'A楼', hits: [], check_status: 'ready' }], stats: {} });
      }
      if (pathname.endsWith('/rulesets')) {
        if (method === 'POST') { rule = { ...request.postDataJSON(), id: nextId++, version: 'v1', items: [] }; return reply(rule); }
        await waitRead();
        if (failRuleList) { failRuleList = false; return reply({}); }
        return reply(rule ? [{ ...rule, item_count: rule.items.length }] : []);
      }
      if (/\/rulesets\/\d+\/expand$/.test(pathname)) return reply({ count: 1, devices: [{ inst_name: '冷机一号' }] });
      if (/\/rulesets\/\d+$/.test(pathname)) {
        if (method === 'DELETE') { rule = undefined; return reply({ deleted: true }); }
        if (method === 'PUT') { assert.equal(request.postDataJSON().expected_version, rule.version); rule = { ...rule, ...request.postDataJSON(), version: 'v2' }; }
        if (method === 'GET') {
          await waitRead();
          if (failRuleDetail) { failRuleDetail = false; return route.fulfill({ status: 502, json: { ok: false, error: '规则读取中断' } }); }
        }
        return reply(rule);
      }
      if (pathname.endsWith('/catalog')) {
        const kind = url.searchParams.get('kind');
        return reply({ items: kind === 'types' ? [{ obj_name: '冷机', devices: 1 }] : kind === 'zones' ? [{ zone: '测试区', devices: 1 }] : kind === 'devices' ? [{ inst_name: '冷机一号', ins_id: 'test-1', obj_name: '冷机' }] : kind === 'rules' ? [{ alarm_name: '高温', alarm_config_id: 'test-alarm', classify_model: '冷机' }] : [], has_more: false });
      }
      throw new Error('Unexpected API request: ' + method + ' ' + pathname);
    });
    for (const [kind, label] of [['maintenance', '检修'], ['change', '变更']]) {
      failCheck = true;
      await page.goto(entry + '&tab=' + kind);
      await page.getByText('A楼原记录', { exact: true }).waitFor();
      const refresh = page.getByRole('button', { name: '刷新' + label + '记录', exact: true });
      assert.equal(await refresh.getAttribute('data-v2-refresh'), '');
      const before = calls.filter(call => call.pathname.endsWith('/' + kind + '/records')).length;
      readDelay = 400;
      for (let n = 0; n < 2; n++) {
        holdRead = true;
        await refresh.click();
        await refresh.locator('.v2-action-feedback[aria-label="处理中"]').waitFor();
        await page.waitForFunction(button => button.dataset.v2Action === 'pending', await refresh.elementHandle());
        await refresh.evaluate(button => button.click());
        holdRead = false; releaseRead();
        await refresh.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
      }
      readDelay = 0;
      assert.equal(calls.filter(call => call.pathname.endsWith('/' + kind + '/records')).length, before + 2, 'manual reads bypass the 30-second cache');
      await page.getByRole('button', { name: '核对', exact: true }).evaluate(button => { button.click(); button.click(); });
      const alert = page.locator('.pc-maintenance .pc-alert.error');
      await alert.getByText(/本次未取得核对结论/).waitFor();
      assert.match(await page.locator('.pc-maintenance').innerText(), /上次已匹配/);
      await alert.getByRole('button', { name: '连接设置', exact: true }).click();
      await page.getByRole('dialog', { name: '计划收敛平台设置' }).waitFor();
      await page.getByRole('button', { name: '关闭弹窗', exact: true }).click();
      await page.locator('.pc-overlay').waitFor({ state: 'detached' });
      await page.screenshot({ path: path.join(output, `${kind}-retry-${width}.png`), fullPage: true });
      await alert.getByRole('button', { name: '重试', exact: true }).click();
      await alert.waitFor({ state: 'detached' });
      const checks = calls.filter(call => call.pathname.endsWith('/' + kind + '/check'));
      assert.deepEqual(checks.map(call => call.body), [{ record_id: 'original' }, { record_id: 'original' }]);
      await page.getByText('未找到匹配屏蔽', { exact: true }).waitFor();
      failRecords = true;
      await refresh.click();
      await alert.getByText(/记录读取中断/).waitFor();
      await refresh.locator('.v2-action-feedback[aria-label="操作未完成"]').waitFor();
      assert.equal(await refresh.getAttribute('data-v2-action'), 'error');
      assert.match(await page.locator('.pc-maintenance').innerText(), /A楼原记录/);
      await refresh.click();
      await refresh.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
      await alert.waitFor({ state: 'detached' });
      incompleteRecords = true;
      await refresh.click();
      await refresh.locator('.v2-action-feedback[aria-label="操作未完成"]').waitFor();
      assert.match(await alert.innerText(), /通告列表读取不完整/);
      await refresh.click();
      await refresh.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
    }
    await page.getByRole('button', { name: '规则配置', exact: true }).click();
    const editor = page.locator('.pc-rules');
    await editor.locator('.new-set-form input').fill('公网规则回归');
    await editor.getByRole('button', { name: '新建规则集', exact: true }).click();
    await editor.getByLabel('规则集名称', { exact: true }).waitFor();
    const refreshSets = editor.getByRole('button', { name: '刷新规则集', exact: true });
    const reloadSet = editor.getByRole('button', { name: '读取最新规则', exact: true });
    readDelay = 400;
    for (let n = 0; n < 2; n++) {
      holdRead = true;
      await refreshSets.click();
      await refreshSets.locator('.v2-action-feedback[aria-label="处理中"]').waitFor();
      holdRead = false; releaseRead();
      await refreshSets.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
    }
    failRuleList = true;
    await refreshSets.click();
    await refreshSets.locator('.v2-action-feedback[aria-label="操作未完成"]').waitFor();
    assert.match(await editor.innerText(), /规则集列表读取不完整/);
    await refreshSets.click();
    await refreshSets.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
    const ruleName = editor.getByLabel('规则集名称', { exact: true });
    await ruleName.fill('未保存的本次填写');
    const detailReads = () => calls.filter(call => call.method === 'GET' && /\/rulesets\/\d+$/.test(call.pathname)).length;
    const beforeCancel = detailReads();
    await reloadSet.click();
    await page.getByRole('dialog', { name: '读取最新规则', exact: true }).getByRole('button', { name: '取消', exact: true }).click();
    assert.equal(await ruleName.inputValue(), '未保存的本次填写');
    assert.equal(detailReads(), beforeCancel, 'cancel does not read or overwrite the draft');
    assert.equal(await reloadSet.getAttribute('data-v2-action'), null, 'cancel never shows success');
    failRuleDetail = true;
    await reloadSet.click();
    await page.getByRole('dialog', { name: '读取最新规则', exact: true }).getByRole('button', { name: '确认', exact: true }).click();
    await reloadSet.locator('.v2-action-feedback[aria-label="操作未完成"]').waitFor();
    assert.equal(await ruleName.inputValue(), '未保存的本次填写', 'failed reload preserves current fields');
    await reloadSet.click();
    await page.getByRole('dialog', { name: '读取最新规则', exact: true }).getByRole('button', { name: '确认', exact: true }).click();
    await reloadSet.locator('.v2-action-feedback[aria-label="操作完成"]').waitFor();
    assert.equal(await ruleName.inputValue(), rule.name);
    readDelay = 0;
    await editor.locator('.picker-col').first().locator('.cand').filter({ hasText: '冷机' }).getByRole('checkbox').check();
    await editor.locator('.picker-col').nth(2).locator('.cand').filter({ hasText: '冷机一号' }).getByRole('checkbox').check();
    await editor.locator('.picker-col').nth(3).locator('.cand').filter({ hasText: '高温' }).getByRole('checkbox').check();
    await editor.getByRole('button', { name: '添加屏蔽范围', exact: true }).click();
    await editor.getByLabel('第1组名称').waitFor();
    await editor.getByRole('button', { name: '保存规则集', exact: true }).click();
    await editor.getByText(/已保存规则集/).waitFor();
    assert.equal(rule.items[0].inst_name, '冷机一号');
    assert.equal(rule.items[0].alarm_config_id, 'test-alarm');
    await editor.getByRole('button', { name: '查看覆盖', exact: true }).click();
    await page.locator('.pc-modal').getByText('冷机一号', { exact: true }).waitFor();
    await page.keyboard.press('Escape');
    await editor.getByLabel('规则集名称', { exact: true }).fill('已编辑公网规则');
    await editor.getByRole('button', { name: '保存规则集', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('.pc-rules .toast')?.textContent.includes('已保存规则集'));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'rule editor must fit its viewport');
    if (width < 760) {
      const columns = await editor.locator('.grid-stack-item').evaluateAll(items => items.map(el => el.getBoundingClientRect().width));
      assert.ok(columns.every(value => value > 300), 'mobile rule columns stack instead of squeezing into desktop tracks');
    }
    await page.screenshot({ path: path.join(output, `rules-${width}.png`), fullPage: true });
    await editor.getByRole('button', { name: '删除当前规则集', exact: true }).click();
    await page.getByRole('dialog').getByRole('button', { name: '删除', exact: true }).click();
    await editor.getByText(/暂无规则集/).waitFor();
    admin = false;
    await page.goto(entry + '&tab=rules&viewer=1');
    await editor.getByText('只读', { exact: true }).waitFor();
    assert.equal(await editor.getByRole('button', { name: '新建规则集', exact: true }).count(), 0);
    assert.equal(await editor.getByRole('button', { name: '添加屏蔽范围', exact: true }).isDisabled(), true);
    await page.goto(entry + '&tab=maintenance&viewer=1');
    await page.getByText('A楼原记录', { exact: true }).waitFor();
    failCheck = true;
    await page.getByRole('button', { name: '核对', exact: true }).click();
    await page.locator('.pc-maintenance .pc-alert.error').waitFor();
    assert.equal(await page.getByRole('button', { name: '连接设置', exact: true }).count(), 0);

    await page.goto(entry + '&charts=1');
    const canvas = page.locator('.v2-motion-chart .v2-chart-canvas');
    await canvas.locator('svg').waitFor();
    const learning = page.locator('.v2-learning-trend [role=img]');
    await learning.scrollIntoViewIfNeeded();
    await learning.locator('svg').waitFor();
    for (const view of ['bar', 'pie', 'horizontal', 'area', 'donut', 'calendar', 'line', 'bar']) {
      await canvas.scrollIntoViewIfNeeded();
      const box = await canvas.boundingBox();
      await page.mouse.move(box.x + box.width * .65, box.y + box.height * .4);
      await page.evaluate(view => { window.chartFixture.view = view; }, view);
      await page.waitForTimeout(120);
      assert.ok(await canvas.locator('svg path').count(), view + ' must render nonblank');
      assert.equal(await page.getByText('图表暂不可用，请查看数据明细', { exact: true }).count(), 0);
    }
    for (const empty of [[], [{ label: '暂无成绩', value: null }]]) {
      await page.evaluate(empty => { window.chartFixture.learning = empty; window.chartFixture.items = []; window.chartFixture.water.series = []; }, empty);
      await learning.focus();
      await page.keyboard.press('ArrowRight');
      await page.keyboard.press('Enter');
      await page.waitForTimeout(80);
    }
    await page.evaluate(() => { window.chartFixture.loading = true; });
    await page.waitForTimeout(80);
    await page.evaluate(() => { const s = window.chartFixture; s.loading = false; s.items = [{ label: '2026-10-01', value: 5 }, { label: '2026-10-02', value: 10 }]; s.learning = s.items; s.water.series = [{ scope: 'A', label: 'A楼', values: [5, 10] }]; s.water.days = s.items.map(r => r.label); });
    await page.waitForTimeout(250);
    assert.ok(await canvas.locator('svg path').count());
    await page.setViewportSize({ width: width === 1440 ? 900 : 360, height: 850 });
    await page.screenshot({ path: path.join(output, `charts-${width}.png`), fullPage: true });
    assert.deepEqual(errors, [], 'no uncaught chart or rule-page errors');
    await context.close();
    console.log(`${width}px: immediate refresh, check recovery, rule CRUD/read-only, chart coordinate/empty/resize recovery OK`);
  }
} catch(error) {
  await activePage?.screenshot({path:path.join(output,'refresh-failure.png'),fullPage:true});
  throw error;
} finally {
  await browser?.close();
  await server.close();
}
