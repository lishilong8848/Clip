import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const [address, output] = process.argv.slice(2);
const origin = new URL(address).origin;
assert.equal(new URL(origin).hostname, '127.0.0.1');
assert.equal((await (await fetch(origin + '/api/health')).json()).instance_id, 'responsive-isolated-preview');
await mkdir(output, { recursive: true });

const date = '2026-11-20';
const titles = ['冷却水系统及相关设备联合核验（跨系统长标题与现场恢复确认）', 'BA系统资料核对'];
const items = ['A', 'B'].flatMap(scope => titles.map((title, index) => ({
  title: 'EA118机房' + scope + '楼维保' + title + '（进行中）',
  active_item_id: 'audit-active-' + scope + '-maintenance-' + index,
  target_record_id: 'audit-target-' + scope + '-maintenance-' + index,
  scope,
  work_type: 'maintenance',
  status: '开始',
  start_time: date + ' 09:00',
  deadline: date + ' 10:00',
  owner_level: index === 0 ? 'L4' : 'L2',
  created_by_open_id: index === 0 ? 'isolated-basic' : 'isolated-other',
})));
const target = items[0];
const robotId = 'd'.repeat(32);
const jobId = 'isolated-workbench-end';
const button = (parent, name) => parent.getByRole('button', { name, exact: true });
const browser = await chromium.launch();

async function setup(actor, mobile = false, keepSibling = false) {
  const context = await browser.newContext({
    viewport: mobile ? { width: 393, height: 852 } : { width: 1440, height: 1000 },
    isMobile: mobile, hasTouch: mobile, reducedMotion: 'reduce', serviceWorkers: 'block',
  });
  const page = await context.newPage();
  page.setDefaultTimeout(12000);
  await page.clock.setFixedTime(new Date(date + 'T03:00:00Z'));
  const state = { closed: false, submissions: [], retries: 0, details: [], errors: [], outside: [] };
  page.on('pageerror', error => state.errors.push(error.message));
  await context.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    if (url.origin !== origin) {
      state.outside.push(url.origin);
      return route.abort();
    }
    const ok = data => route.fulfill({ json: { ok: true, data } });
    const scope = url.searchParams.get('scope');
    if (url.pathname === '/api/auth/status') {
      return ok({ logged_in: true, user: { enabled: true, name: '隔离验收', modules: ['maintenance'],
        home_scope: 'A', login_method: 'password', role: 'building', ...actor },
      scope_options: ['A', 'B'].map(value => ({ value, label: value + '楼' })),
      allowed_scopes: ['A', 'B'], default_scope: 'A' });
    }
    if (url.pathname === '/api/workbench') {
      const ongoing = items.filter(item => item.scope === scope && !(state.closed && item === target))
        .map(item=>keepSibling && item===items[1]?{...item,created_by_open_id:actor.open_id}:item);
      return ok({ ongoing, records: [], ongoing_pagination: { total_pages: 1 }, records_pagination: { total_pages: 1 } });
    }
    if (url.pathname === '/api/workbench/lite-detail') {
      state.details.push([scope, url.searchParams.get('active_item_id')]);
    }
    if (url.pathname === '/api/workbench/draft') {
      return ok({ draft: request.method() === 'PUT' ? { ...request.postDataJSON(), version: 1 } : null });
    }
    if (url.pathname === '/api/workbench/dingtalk-options') {
      return ok({ robots: [{ id: robotId, name: '隔离值班群', scopes: ['A'], types: [] }], selected_robot_ids: [] });
    }
    if (url.pathname === '/api/workbench-actions' && request.method() === 'POST') {
      state.submissions.push(request.postDataJSON());
      return ok({ job_id: jobId, phase: 'running' });
    }
    if (url.pathname === '/api/jobs/' + jobId + '/retry' && request.method() === 'POST') {
      state.retries++;
      return ok({ job_id: jobId, phase: 'running' });
    }
    if (url.pathname === '/api/jobs/' + jobId) {
      if (!state.retries) return ok({ job_id: jobId, phase: 'failed', error_retryable: true, can_retry: true, error: '隔离提交失败' });
      state.closed = true;
      return ok({ job_id: jobId, phase: 'success', archive_state: 'success', message_sent: true,
        target_record_id: target.target_record_id });
    }
    return route.continue();
  });
  await page.goto(origin + '/assets/workspace-v2/index.html?__page=/');
  const queue = page.getByRole('region', { name: '待办办理', exact: true });
  const priorities = page.getByRole('region', { name: '紧急事项', exact: true });
  await queue.locator('.wb-task-tile[data-group="notices:维保"]').waitFor();
  await page.waitForFunction(() => document.querySelector('.queue-panel')?.getAttribute('aria-busy') === 'false');
  return { page, context, state, queue, priorities };
}

async function press(control, mobile) {
  await control.evaluate(element => element.scrollIntoView({ block: 'center', inline: 'nearest' }));
  assert(await control.evaluate(element => {
    const { x, y, width, height } = element.getBoundingClientRect();
    return document.documentElement.scrollWidth <= innerWidth + 1 &&
      element.contains(document.elementFromPoint(x + width / 2, y + height / 2));
  }), 'control fits its viewport and receives the pointer');
  if (mobile) await control.tap();
  else await control.click();
}

try {
  for (const [actor, expected, testBuilding] of [
    [{ open_id: 'isolated-global', level: 'L0', role: 'admin' }, items, 'A'],
    [{ open_id: 'isolated-engineer', level: 'L2', category: 'engineer' }, items.slice(0, 2), 'B'],
    [{ open_id: 'isolated-supervisor', level: 'L3', category: 'supervisor' }, [target]],
    [{ open_id: 'isolated-basic', level: 'L4' }, [target]],
    [{ open_id: 'isolated-duty', level: 'L4', category: 'duty' }, items.slice(0, 2)],
  ]) {
    const { page, context, state, queue, priorities } = await setup(actor);
    try {
      const tile=queue.locator('.wb-task-tile[data-group="notices:维保"]');
      async function check(rows) {
        if (rows.length > 3 && await button(priorities,'查看全部').count()) await button(priorities, '查看全部').click();
        assert.deepEqual((await priorities.locator('strong').allTextContents()).sort(), rows.map(item => item.title).sort());
        await tile.click();
        const pop=page.getByRole('dialog',{name:'模块待办条目'});
        assert.equal(Number(await tile.getAttribute('data-count')),rows.length);
        const names=await pop.locator('.task-title strong').allTextContents();
        assert.deepEqual([...names].sort(),rows.map(item=>item.title).sort());
        assert.deepEqual(names.map(name=>Array.from(name).length),names.map(name=>Array.from(name).length).sort((a,b)=>a-b));
        await button(pop,'关闭待办条目').click();
      }
      await check(expected);
      if (testBuilding) {
        await page.getByRole('combobox', { name: '工作台楼栋' }).selectOption(testBuilding);
        await priorities.getByText(items.find(item => item.scope === testBuilding).title, { exact: true }).waitFor();
        await check(testBuilding === 'B' ? [items[2]] : items.slice(0, 2));
      }
      assert.deepEqual(state.outside, []);
      assert.deepEqual(state.errors, []);
      console.log('PASS permissions', actor.open_id);
    } finally {
      await context.close();
    }
  }
  for (const mobile of [false, true]) {
    const actor = { open_id: 'isolated-basic', level: 'L4' };
    // On the phone one other authorized item stays in the same group after success.
    const keepSibling=mobile;
    const { page, context, state, queue, priorities } = await setup(actor,mobile,keepSibling);
    try {
      const urgent = button(priorities, '已逾期，办理 ' + target.title);
      const tile=queue.locator('.wb-task-tile[data-group="notices:维保"]');
      const pop=page.getByRole('dialog',{name:'模块待办条目'});
      const ordinary=button(pop,'办理 '+target.title);
      const editor = page.locator('.v2-notice-editor');
      const device = mobile ? 'phone' : 'desktop';
      await page.screenshot({ path: path.join(output, device + '-workbench.png'), animations: 'disabled' });
      await press(tile,mobile);await pop.waitFor();
      await pop.screenshot({path:path.join(output,device+'-items.png'),animations:'disabled'});
      await press(ordinary, mobile);await pop.waitFor({state:'hidden'});
      await press(button(editor, '关闭'), mobile);
      await editor.waitFor({ state: 'hidden' });

      await press(urgent, mobile);
      const operation = editor.getByRole('radiogroup', { name: '本次操作' });
      await press(operation.getByText('结束', { exact: true }), mobile);
      await editor.getByRole('checkbox', { name: '隔离值班群', exact: true }).check();
      await press(button(editor, '预览并确认结束'), mobile);
      await press(button(editor, '确认发送结束'), mobile);
      await editor.getByText('隔离提交失败', { exact: false }).first().waitFor();
      assert.deepEqual(state.details, [['A', target.active_item_id], ['A', target.active_item_id]]);
      assert.equal(await urgent.count(), 1);
      assert.equal(await tile.getAttribute('data-count'),keepSibling?'2':'1','failed submission retains the item in its tile');
      const retry = button(editor, '重试原任务');
      await retry.scrollIntoViewIfNeeded();
      await page.screenshot({ path: path.join(output, device + '-retry.png'), animations: 'disabled' });
      await press(retry, mobile);
      await editor.getByRole('heading', { name: '通告办理完成' }).waitFor();
      await urgent.waitFor({ state: 'detached' });
      if(keepSibling)await queue.locator('.wb-task-tile[data-group="notices:维保"][data-count="1"]').waitFor();
      else await tile.waitFor({ state: 'detached' });
      await press(button(editor, '返回列表'), mobile);
      await editor.waitFor({ state: 'hidden' });
      if(keepSibling){
        await press(tile,mobile);await pop.getByText(items[1].title,{exact:true}).waitFor();
        assert.equal(await ordinary.count(),0);assert.equal(await pop.locator('.wb-task-row').count(),1);
        assert.equal(await priorities.locator('strong').textContent(),items[1].title);
      }else await queue.getByText('暂无待办', { exact: true }).waitFor();
      assert.equal(state.submissions.length, 1);
      assert.equal(state.submissions[0].action, 'end');
      assert.equal(state.submissions[0].active_item_id, target.active_item_id);
      assert.equal(state.submissions[0].target_record_id, target.target_record_id);
      assert.equal(state.retries, 1);
      assert.deepEqual(state.errors, []);
      assert.deepEqual(state.outside, []);
      console.log('PASS ' + device + ' exact record, fail, retry the original job, and remove from both views');
    } finally {
      await context.close();
    }
  }
} finally {
  await browser.close();
}
