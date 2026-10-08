import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

// Browser plugin is unavailable; use local Playwright against isolated fixtures.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/learning-personal');
await mkdir(output, { recursive: true });
const fixturePage = async (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(await server.transformIndexHtml('/__personal_check', `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><style>body{margin:0;background:#f6f9fc;font:14px Arial,'Microsoft YaHei',sans-serif}</style></head><body><div id="page-back-slot"></div><div id="app"></div><script type="module">
    import { createApp, ref, h } from 'vue';
    import LearningPage from '/src/components/LearningPage.vue';
    import PlanConvergenceRules from '/src/components/PlanConvergenceRules.vue';
    import UiTransition from '/src/components/UiTransition.vue';
    const rules = location.search.includes('rules');
    createApp({ setup() { const draft = ref({name:'测试规则',items:[]}), search=ref(location.search); window.addEventListener('popstate',()=>search.value=location.search); return () => { const params=new URLSearchParams(search.value); return rules ? h(PlanConvergenceRules, { isAdmin:true, modelValue:draft.value, 'onUpdate:modelValue':value=>draft.value=value }) : h(LearningPage, {key:search.value,scope:params.get('scope')||'A',personId:params.get('person_id')||'',userId:'offline-account'}); }; } }).component('UiTransition', UiTransition).mount('#app');
    </script></body></html>`));
};
const server = await createServer({ root, server: { host: '127.0.0.1', port: 0, hmr: false }, logLevel: 'error', plugins: [{ name: 'personal-test-page', configureServer(server) { server.middlewares.use('/__personal_check', fixturePage); server.middlewares.use('/learning', fixturePage); } }] });
await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const papers = new Map(), requests = [], errors = [];
const people = [{ id: 'p1', name: '测试人员甲', employee_no: '1001', scopes: ['A'], active: true }, { id: 'p2', name: '测试人员乙', employee_no: '1002', scopes: ['A'], active: true }];
const today = '2026-10-08';
const summary = (id) => {
  const all = id ? [papers.get(id)].filter(Boolean) : [...papers.values()];
  const attempts = all.flatMap(p => p.questions.filter(q => q.attempt));
  const correct = attempts.filter(q => q.attempt.correct).length;
  return { assigned: all.length * 15, answered: attempts.length, task_answered: attempts.length, choice_answered: attempts.length, independent_answered: attempts.length, independent_correct: correct, correct, wrong: attempts.length - correct, accuracy: attempts.length ? 100 * correct / attempts.length : null, independent_accuracy: attempts.length ? 100 * correct / attempts.length : null, received_people: all.length, answered_people: all.filter(p => p.questions.some(q => q.attempt)).length, not_started_people: all.filter(p => !p.questions.some(q => q.attempt)).length, completed_people: 0, papers: all.length, learning_days: attempts.length ? 1 : 0, interview_total: 0, interview_ratings: {}, practice_count: 0 };
};
function makePaper(id) {
  return { id: 'personal-' + id, person_id: id, person: people.find(p => p.id === id), scope: 'A', date: today, version: 0, status: 'pending', shortage: {}, questions: Array.from({ length: 15 }, (_, n) => ({ id: 'q' + n, version: 'v1', stem: `隔离测试题目${n + 1}：应先核对哪项安全条件？`, bank: n < 8 ? 'written' : n < 10 ? n === 8 ? 'duty' : 'professional' : 'supplemental', type: n === 8 || n === 9 ? 'interview' : 'single', options: [{ id: 'a', text: '现场安全条件' }, { id: 'b', text: '直接开始操作' }], has_hint: true, attachments: [] })) };
}
function publicPaper(p) { return { ...structuredClone(p), stats: { total: 15, answered: p.questions.filter(q => q.attempt).length, shortage: 0 } }; }
let browser, page;
try {
  browser = await chromium.launch({ headless: true });
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.on('pageerror', error => errors.push(error.stack));
  page.on('console', message => { if (['error', 'warning'].includes(message.type())) errors.push(message.text()); });
  await page.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== base) { errors.push('Unexpected external request: ' + url.origin); return route.abort(); }
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const body = req.postData() ? JSON.parse(req.postData()) : {};
    requests.push({ path: url.pathname, body });
    const ok = data => route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data }) });
    if (url.pathname === '/api/health') return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, service: 'clipflow_backend', instance_id: 'personal-fixture' }) });
    if (url.pathname.endsWith('/catalog')) {
      const kind = url.searchParams.get('kind');
      const items = kind === 'types' ? [{ obj_name: '设备类型甲', devices: 500 }, { obj_name: '设备类型乙', devices: 500 }]
        : kind === 'zones' ? [{ zone: '测试园区', devices: 500 }]
        : kind === 'devices' ? Array.from({ length: 500 }, (_, i) => ({ inst_name: `设备${i}`, ins_id: `ins${i}`, obj_name: '设备类型甲' }))
        : kind === 'rules' ? Array.from({ length: 500 }, (_, i) => ({ alarm_name: `规则${i}`, alarm_config_id: `rule${i}`, classify_model: '设备类型甲' })) : [];
      return ok({ items, limit: 2000, has_more: false });
    }
    if (url.pathname.endsWith('/settings')) return ok({ enabled: true, publish_time: '08:00', reminder_enabled: false });
    if (url.pathname.endsWith('/bootstrap')) return ok({ is_admin: true, can_answer: true, scopes: [...'ABCDEH'].map(value => ({ value, label: value + '楼' })), scope: 'A', today, silent_manual_publish: true, sync: { status: 'ready', pending: 0 }, settings: { enabled: true } });
    if (url.pathname.endsWith('/people')) return ok({ items: people.filter(p => `${p.name}${p.employee_no}`.includes(url.searchParams.get('q') || '')), total: 2, page: 1, ready: true, issues: [] });
    if (url.pathname.endsWith('/profile')) { const id = url.searchParams.get('person_id'); const s = summary(id); return ok({ person: people.find(p => p.id === id), summary: s, today_summary: s, published: true, trend: [{ date: today, answered: s.answered, accuracy: s.accuracy, people: s.answered_people }], distribution: [{ label: '100%', count: s.answered_people }], banks: [{ label: '笔试题库', wrong: s.wrong }], topics: [], people: [...papers.values()].map(p => ({ person_id: p.person_id, name: p.person.name, employee_no: p.person.employee_no, summary: summary(p.person_id), today: publicPaper(p).stats, last_answered_at: s.answered ? today + 'T10:00' : '' })) }); }
    if (url.pathname.endsWith('/papers/claim')) { if (!papers.has(body.person_id)) papers.set(body.person_id, makePaper(body.person_id)); return ok(publicPaper(papers.get(body.person_id))); }
    if (/\/(papers|history)$/.test(url.pathname)) { const id = url.searchParams.get('person_id'); const items = [...papers.values()].filter(p => !id || p.person_id === id).map(publicPaper); return ok({ items, total: items.length, page: 1, today, published: true }); }
    if (url.pathname.endsWith('/answer')) { const p = papers.get(body.person_id); assert(url.pathname.includes(p.id)); const q = p.questions.find(q => q.id === body.question_id); assert.equal(body.version, p.version); q.attempt = { correct: body.option_ids.includes('a'), option_ids: body.option_ids, submitted_at: today + 'T10:00:00+08:00' }; p.version++; return ok(publicPaper(p)); }
    errors.push('Unexpected fixture endpoint: ' + url.pathname);
    return route.fulfill({ status: 500, body: 'Unknown fixture endpoint' });
  });
  await page.goto(base + '/__personal_check');
  await page.getByRole('heading', { name: '画像学练', exact: true }).waitFor();
  assert.equal(await page.getByRole('button', { name: '选择答题人员', exact: true }).count(), 1);
  assert.equal(await page.getByRole('button', { name: '手动发布题单', exact: true }).count(), 0);
  await page.getByRole('button', { name: '选择答题人员', exact: true }).click();
  await page.getByRole('dialog').getByRole('button', { name: /测试人员甲/ }).click();
  await page.waitForURL('**/learning?scope=A&person_id=p1');
  await page.getByRole('button', { name: '继续今日学练' }).click();
  await page.locator('.question-numbers button').first().waitFor();
  assert.equal(await page.locator('.question-numbers button').count(), 15);
  await page.getByRole('button', { name: '下一题', exact: true }).click();
  await page.getByRole('button', { name: '上一题', exact: true }).click();
  await page.getByText('现场安全条件', { exact: true }).click();
  assert.equal(await page.locator('.options label.checked').count(), 1);
  await page.screenshot({ path: path.join(output, 'answer-selected.png'), fullPage: true });
  await page.getByRole('button', { name: '确认作答', exact: true }).click();
  await page.locator('.result').filter({ hasText: '回答正确' }).waitFor();
  assert.equal(await page.locator('.options input:enabled').count(), 0);
  assert.match(await page.locator('.rail-progress').innerText(), /1 \/ 15/);
  await page.getByRole('button', { name: '切换人员', exact: true }).click();
  await page.getByRole('dialog').getByRole('button', { name: /测试人员乙/ }).click();
  await page.getByRole('button', { name: '继续今日学练' }).click();
  assert.equal(await page.locator('.options input:checked').count(), 0);
  await page.getByRole('button', { name: '切换人员', exact: true }).click();
  await page.getByRole('dialog').getByRole('button', { name: /测试人员甲/ }).click();
  assert.equal(papers.size, 2);
  await page.getByRole('region', { name: '题目完成进度', exact: true }).getByRole('img').waitFor();
  assert.match(await page.locator('.completion-ring').getAttribute('aria-label'), /已完成1题.*6.7%/);
  assert.match(await page.locator('.method-row').first().innerText(), /100.0%/);
  await page.screenshot({ path: path.join(output, 'personal-profile.png'), fullPage: true });
  await page.reload();
  await page.getByRole('button', { name: '继续今日学练' }).waitFor();
  assert.match(await page.locator('.learner-strip').innerText(), /测试人员甲/);
  await page.getByRole('button', { name: '返回', exact: true }).click();
  await page.screenshot({ path: path.join(output, 'building-dashboard.png'), fullPage: true });
  assert(await page.locator('.learning-dashboard table').innerText().then(t => t.includes('测试人员甲')));
  assert.equal(await page.locator('.building-tabs button').count(), 6);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  assert.equal(await page.getByRole('button', { name: '选择答题人员', exact: true }).count(), 1);
  await page.getByRole('button', { name: '发布设置', exact: true }).click();
  assert.equal(await page.getByRole('button', { name: '手动发布题单', exact: true }).count(), 1);
  assert.equal(await page.getByRole('button', { name: '同步题库与人员', exact: true }).count(), 1);
  await page.goto(base + '/__personal_check?rules');
  const columns = page.locator('.picker-col');
  await columns.nth(0).locator('.col-all input').check();
  await columns.nth(1).locator('.col-all input').check();
  await columns.nth(2).locator('.col-all input').check();
  await columns.nth(3).locator('.col-all input').check();
  const started = Date.now();
  await page.getByRole('button', { name: '添加屏蔽范围', exact: true }).click();
  await page.getByText(/当前选择将展开为 250,000 项/).waitFor();
  assert(Date.now() - started < 2000, 'large selection must be rejected before expansion');
  assert.equal(await page.locator('.draft-panel .draft-item').count(), 0);
  await page.screenshot({ path: path.join(output, 'rules-large-selection.png'), fullPage: true });
  assert.deepEqual(errors, []);
  console.log('[PersonalLearningBrowser] OK', { papers: papers.size, requests: requests.length, output });
} catch (error) {
  console.error('[BrowserErrors]', errors, await page?.locator('body').innerText());
  await page?.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally { await browser?.close(); await server.close(); }
