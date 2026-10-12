import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const output = path.join(root, 'output/playwright/notice-undo');
const data = await mkdtemp(path.join(os.tmpdir(), 'clipflow-notice-undo-'));
await mkdir(output, { recursive: true });

const mkDeferred = () => {
  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
};

const extractFragment = (html, name) => {
  const start = `<!--LITE_FRAGMENT:${name}:START-->`;
  const end = `<!--LITE_FRAGMENT:${name}:END-->`;
  const from = html.indexOf(start);
  const to = html.indexOf(end, from + start.length);
  assert(from >= 0 && to > from, `missing LITE_FRAGMENT:${name}`);
  return html.slice(from + start.length, to);
};

let browser;
try {
  const generated = spawnSync(
    path.join(root, 'bin/.venv/Scripts/python.exe'),
    ['-c', `
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite
payload={'records':[],'ongoing':[],'daily_summary':{'stats':{}}}
session={'user':{'name':'Test'},'is_admin':True}
opts=[{'value':'E','label':'E栋'},{'value':'B','label':'B栋'}]
def undo(item_id,title):
    return {'undo_id':item_id,'title':title,'undo_label':'回退结束','undo_action_type':'end','undo_created_at':1700000000}
undo_a=undo('undo-a','隔离回退A')
undo_b=undo('undo-b','隔离回退B')
html_initial=render_workbench_lite(payload=payload,session=session,scope='E',work_type='maintenance',month='10月',scope_options=opts,notice_undos=[undo_a,undo_b])
html_after=render_workbench_lite(payload=payload,session=session,scope='E',work_type='maintenance',month='10月',scope_options=opts,notice_undos=[undo_b])
print(json.dumps({'initial':html_initial,'after':html_after}))
`],
    { cwd: root, encoding: 'utf8', env: { ...process.env, CLIPFLOW_DATA_DIR: data, PYTHONIOENCODING: 'utf-8' }, maxBuffer: 16 * 1024 * 1024 }
  );
  assert.equal(generated.status, 0, `python generation failed: ${generated.stderr}`);
  const html = JSON.parse(generated.stdout);

  const fragmentsAfter = {
    subtitle: extractFragment(html.after, 'subtitle'),
    summary: extractFragment(html.after, 'summary'),
    toolbar: extractFragment(html.after, 'toolbar'),
    workspace: extractFragment(html.after, 'workspace'),
  };

  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  page.setDefaultTimeout(10000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));

  // No-op the SSE helpers so the page never opens real streaming sockets.
  await page.addInitScript(() => {
    class FakeEventSource {
      constructor() {}
      addEventListener() {}
      removeEventListener() {}
      close() {}
    }
    window.EventSource = FakeEventSource;
  });

  const applyPosts = [];
  const applyDefers = [];
  let jobPolls = 0;
  let fragmentRequests = 0;

  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    const { pathname, searchParams } = url;
    if (pathname === '/workbench-lite') {
      return route.fulfill({ contentType: 'text/html', body: html.initial });
    }
    const applyMatch = pathname.match(/^\/api\/notice-undo\/([^/]+)\/apply$/);
    if (applyMatch) {
      applyPosts.push({
        undoId: decodeURIComponent(applyMatch[1]),
        body: route.request().postData() ? JSON.parse(route.request().postData()) : null,
      });
      const idx = applyPosts.length - 1;
      if (applyDefers[idx]) await applyDefers[idx].promise;
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data: { job_id: 'job-a' } }) });
    }
    if (pathname === '/api/jobs/job-a') {
      jobPolls += 1;
      if (jobPolls === 1) {
        return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data: { phase: 'failed', error: 'TextFieldConvFail' } }) });
      }
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data: { phase: 'success' } }) });
    }
    if (pathname === '/api/workbench/lite-fragment') {
      fragmentRequests += 1;
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data: { fragments: fragmentsAfter } }) });
    }
    return route.fulfill({ contentType: 'text/plain', body: '' });
  });

  await page.goto('http://workbench.undo-fixture/workbench-lite?scope=E&token=fixture&work_type=maintenance');
  await page.locator('#lite-undo-fold').waitFor();

  // Expand undo fold (data-loaded=1, so no /api/notice-undo/available is fetched).
  await page.evaluate(() => { document.getElementById('lite-undo-fold').open = true; });
  await page.locator('.undo-row[data-undo-id="undo-a"]').waitFor();

  // Optional lightweight check: a scope change must not reset the pending undo identity.
  await page.click('.undo-row[data-undo-id="undo-b"]');
  assert.equal(await page.locator('#lite-undo-confirm').evaluate(el => !el.hidden), true, 'undo-b confirm should be open');
  assert.equal(await page.locator('#lite-undo-confirm-name').textContent(), '隔离回退B');
  await page.evaluate(() => {
    const select = document.getElementById('lite-scope-select');
    select.value = 'B';
  });
  assert.equal(await page.locator('#lite-undo-confirm-name').textContent(), '隔离回退B', 'scope value change must not reset pending undo identity');
  await page.click('#lite-undo-confirm-cancel');
  await page.evaluate(() => {
    const select = document.getElementById('lite-scope-select');
    select.value = 'E';
  });
  assert.equal(await page.locator('#lite-scope-select').inputValue(), 'E');
  assert.equal(applyPosts.length, 0, 'opening confirm must not send any apply request');
  assert.equal(jobPolls, 0, 'no poll should occur before applying');

  // Open undo-a confirm. Still no request.
  await page.click('.undo-row[data-undo-id="undo-a"]');
  assert.equal(await page.locator('#lite-undo-confirm').evaluate(el => !el.hidden), true, 'undo-a confirm should be open');
  assert.equal(await page.locator('#lite-undo-confirm-name').textContent(), '隔离回退A');
  assert.equal(applyPosts.length, 0, 'clicking undo row must only open the confirm dialog, no request');
  assert.equal(jobPolls, 0);

  // First apply -> deferred apply, then failed poll.
  applyDefers.push(mkDeferred());
  await page.click('#lite-undo-confirm-apply');
  assert.equal(applyPosts.length, 1, 'exactly one apply POST after confirming');
  const firstPost = applyPosts[0];
  assert.equal(firstPost.undoId, 'undo-a', 'apply must target the clicked undoId');
  assert.deepEqual(firstPost.body, { scope: 'E' }, 'apply body must carry scope E');

  const busyState = await page.evaluate(() => ({
    applyDisabled: document.getElementById('lite-undo-confirm-apply').disabled,
    applyBusy: document.getElementById('lite-undo-confirm-apply').classList.contains('is-busy'),
    undoDisabled: document.querySelector('.undo-row[data-undo-id="undo-a"]').disabled,
    statusHidden: document.getElementById('lite-undo-confirm-status').hidden,
    statusText: document.getElementById('lite-undo-confirm-status').textContent,
  }));
  assert.equal(busyState.applyDisabled, true, 'apply button must be disabled while applying');
  assert.equal(busyState.applyBusy, true, 'apply button must be busy while applying');
  assert.equal(busyState.undoDisabled, true, 'undo list button must be disabled while applying');
  assert.equal(busyState.statusHidden, false, 'inline status must be visible while applying');
  assert.match(busyState.statusText, /正在回退/, 'inline status should report progress');

  // Attempt a duplicate apply while the request is in-flight; the busy guard must ignore it.
  await page.evaluate(() => {
    document.getElementById('lite-undo-confirm-apply').dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
  });
  await page.waitForTimeout(50);
  assert.equal(applyPosts.length, 1, 'in-flight duplicate apply must be ignored (only one POST)');

  // Let the apply resolve; pollUndoJob then queries the job (failed) and keeps the dialog open.
  applyDefers[0].resolve();
  await page.waitForRequest(req => new URL(req.url()).pathname === '/api/jobs/job-a', { timeout: 5000 });
  await page.waitForFunction(() => {
    const undoBtn = document.querySelector('.undo-row[data-undo-id="undo-a"]');
    const applyBtn = document.getElementById('lite-undo-confirm-apply');
    return undoBtn && applyBtn && !undoBtn.disabled && !applyBtn.disabled;
  });

  assert.equal(jobPolls, 1, 'first polling request should report the failed phase');
  assert.equal(await page.locator('#lite-undo-confirm').evaluate(el => !el.hidden), true, 'dialog must stay open after job failed');
  const failStatus = await page.evaluate(() => ({
    hidden: document.getElementById('lite-undo-confirm-status').hidden,
    text: document.getElementById('lite-undo-confirm-status').textContent,
    blocked: document.getElementById('lite-undo-confirm-status').classList.contains('blocked'),
  }));
  assert.equal(failStatus.hidden, false, 'inline status must remain visible after failure');
  assert.match(failStatus.text, /TextFieldConvFail/, 'failure error must be visible inline');
  assert.equal(failStatus.blocked, true, 'failure status should be marked blocked');
  assert.equal(applyPosts.length, 1, 'first failure must not duplicate the apply POST');
  // First-failure window is still visible here; keep a screenshot before any retry.
  await page.screenshot({ path: path.join(output, 'notice-undo-failed.png'), fullPage: true });

  // Second apply -> same undoId, same scope; poll succeeds -> close + refresh.
  applyDefers.push(mkDeferred());
  await page.click('#lite-undo-confirm-apply');
  assert.equal(applyPosts.length, 2, 'exactly two apply POSTs total for the two retries');
  assert.equal(applyPosts[1].undoId, 'undo-a', 'retry must still target the clicked undoId');
  assert.deepEqual(applyPosts[1].body, { scope: 'E' }, 'retry must still carry scope E');

  const fragReady = page.waitForRequest(req => new URL(req.url()).pathname === '/api/workbench/lite-fragment', { timeout: 5000 });
  applyDefers[1].resolve();
  await fragReady;

  await page.waitForFunction(() => {
    const a = document.querySelectorAll('.undo-row[data-undo-id="undo-a"]').length;
    const b = document.querySelectorAll('.undo-row[data-undo-id="undo-b"]').length;
    return a === 0 && b === 1;
  });
  assert.equal(await page.locator('#lite-undo-confirm').evaluate(el => el.hidden), true, 'dialog must close after successful undo');
  assert.equal(jobPolls, 2, 'successful retry finished with a second successful poll');
  assert.equal(fragmentRequests, 1, 'list refresh should fetch the workspace fragment once');

  await page.screenshot({ path: path.join(output, 'notice-undo.png'), fullPage: true });
  assert.deepEqual(errors, [], `unhandled page errors: ${errors.join(' | ')}`);

  console.log(JSON.stringify({
    ok: true,
    screenshots: output,
    applyPosts,
    jobPolls,
    fragmentRequests,
  }));
} finally {
  await browser?.close();
  await rm(data, { recursive: true, force: true });
}