import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

// No Browser plugin is available. Exercise real Vue components with local fixtures only.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.join(os.tmpdir(), 'clipflow-learning-dashboard-qa');
const summary = { assigned: 90, task_answered: 60, answered: 60, choice_answered: 48, correct: 36, wrong: 12,
  independent_answered: 32, independent_correct: 26, accuracy: 75, independent_accuracy: 81.25,
  learning_days: 5, practice_count: 8, attempt_count: 68, interview_total: 12, interview_ratings: { '需复习': 8, '部分掌握': 4 },
  answered_people: 6, received_people: 8, completed_people: 2, not_started_people: 2, papers: 6 };
const person = { id: 'fixture-person', name: '示例人员', employee_no: '1001' };
const data = { summary, today_summary: { assigned: 15, task_answered: 5, papers: 1, accuracy: 80, answered_people: 2, completed_people: 1, not_started_people: 2 },
  published: true, without_choice_answers: 2,
  trend: [0, 10, 0, 15, 10, 15, 10].map((answered, i) => ({ date: `2026-10-0${i + 1}`, answered, attempt_count: answered + [0, 2, 0, 2, 1, 2, 1][i], people: [0, 2, 0, 3, 1, 3, 2][i], accuracy: [null, 87.5, null, 75, 75, 66.7, 75][i] })),
  distribution: [{ label: '低于60%', count: 1 }, { label: '60–79%', count: 2 }, { label: '80–99%', count: 2 }, { label: '100%', count: 1 }],
  banks: [{ label: '笔试题库', wrong: 7 }, { label: '专项题库', wrong: 5 }],
  topics: [{ label: '电气系统与设备运行安全条件核对及异常处置', wrong: 8 }, { label: '暖通', wrong: 3 }, { label: '消防', wrong: 1 }],
  people: [{ person_id: person.id, name: person.name, employee_no: person.employee_no, summary, today: { answered: 5, total: 15 }, last_answered_at: '2026-10-07T16:30:00' }] };
const server = await createServer({ root, server: { host: '127.0.0.1', port: 0, hmr: false }, logLevel: 'error', plugins: [{ name: 'dashboard-fixture', configureServer(vite) {
  vite.middlewares.use('/__dashboard', async (_req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(await vite.transformIndexHtml('/__dashboard', `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>学练组件预览</title><style>body{margin:0;background:#f6f9fc;font:14px 'Microsoft YaHei',sans-serif}header.preview{padding:18px 30px;background:#2162b6;color:white;display:flex;align-items:center;gap:20px}main{padding:24px 30px;max-width:1740px;margin:auto}button.preview{padding:8px 14px;background:white;color:#28568c;border:1px solid #d4dfeb;border-radius:6px;cursor:pointer}</style></head><body><header class="preview"><strong>画像学练 · 组件预览</strong><span>隔离示例数据，不连接业务表</span><button class="preview" onclick="window.togglePerson()">切换个人 / 楼栋</button></header><main id="app"></main><script type="module">
      import { createApp, ref, h } from 'vue';
      import LearningDashboard from '/src/components/LearningDashboard.vue';
      const data = ref(${JSON.stringify(data)}), person = ref(${JSON.stringify(person)});
      window.setDashboard = (d,p) => { data.value=d; person.value=p; };
      window.togglePerson = () => { person.value=person.value?null:${JSON.stringify(person)}; };
      createApp({setup:()=>()=>h(LearningDashboard,{data:data.value,person:person.value})}).mount('#app');
    </script></body></html>`));
  });
} }] });
await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
if (process.argv.includes('--preview')) {
  console.log(`[LearningDashboardPreview] ${base}/__dashboard`);
} else {
  let browser;
  const errors = [];
  try {
    await mkdir(output, { recursive: true });
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/*', route => {
      const url = new URL(route.request().url());
      if (url.origin !== base || url.pathname.startsWith('/api/')) { errors.push(`Unexpected request ${url.pathname}`); return route.abort(); }
      return route.continue();
    });
    await page.goto(base + '/__dashboard');
    await page.locator('.ring-fill').waitFor();
    assert.match(await page.locator('.ring-fill').evaluate(el => getComputedStyle(el).animationName), /^learning-ring/);
    assert.match(await page.locator('.completion-ring').getAttribute('aria-label'), /已完成60题，待完成30题，完成率66.7%/);
    assert.match(await page.locator('.method-row').nth(0).innerText(), /81.3%[\s\S]*26 \/ 32/);
    assert.match(await page.locator('.method-row').nth(1).innerText(), /62.5%[\s\S]*10 \/ 16/);
    assert.match(await page.locator('.interview-panel').innerText(), /需复习[\s\S]*8[\s\S]*部分掌握[\s\S]*4/);
    const ring = page.locator('.completion-ring');
    const before = await ring.boundingBox();
    await page.getByRole('group', { name: '完成进度时段' }).getByRole('button', { name: '今日', exact: true }).click();
    assert.match(await ring.getAttribute('aria-label'), /已完成5题，待完成10题，完成率33.3%/);
    assert.deepEqual(await ring.boundingBox(), before, 'period switch must not resize the chart');
    await page.getByRole('group', { name: '趋势指标' }).getByRole('button', { name: '正确率', exact: true }).click();
    assert.equal(await page.locator('.trend-line').count(), 2, 'missing-answer dates must break the accuracy line');
    await page.getByLabel('错题分类', { exact: true }).selectOption('topics');
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({ width, height: 1000 });
      for (const p of [person, null]) {
        await page.evaluate(([d, p]) => window.setDashboard(d, p), [data, p]);
        await page.waitForTimeout(800);
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
        const panels = await page.locator('.dashboard-panels > section').evaluateAll(els => els.map(el => ({ width: el.clientWidth, overflow: el.scrollWidth > el.clientWidth + 1 })));
        assert(panels.every(p => p.width > 200 && !p.overflow), JSON.stringify(panels));
        assert.equal(await page.evaluate(() => document.getAnimations().filter(a => a.playState === 'running').length), 0, 'dashboard must be idle after entry');
        await page.screenshot({ path: path.join(output, `${p ? 'personal' : 'building'}-${width}.png`), fullPage: true });
      }
    }
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.evaluate(([d, p]) => window.setDashboard(d, p), [data, person]);
    assert.equal(await page.locator('.ring-fill').evaluate(el => getComputedStyle(el).animationName), 'none');
    assert.equal(await page.locator('.method-track > i').first().evaluate(el => getComputedStyle(el).transitionDuration), '0s');
    const empty = { summary: {}, today_summary: {}, trend: [], banks: [], topics: [], people: [], published: false };
    await page.evaluate(d => window.setDashboard(d, null), empty);
    assert.match(await page.locator('.completion-ring').getAttribute('aria-label'), /尚无已领取题目/);
    assert.equal(await page.locator('.ring-fill').count(), 0);
    assert.match(await page.locator('.answer-method-panel').innerText(), /数据不足/);
    assert.match(await page.locator('.interview-panel').innerText(), /暂无问答自评/);
    assert.equal(await page.locator('.learning-dashboard').innerText().then(t => /NaN|Infinity|0.0%/.test(t)), false);
    await page.screenshot({ path: path.join(output, 'empty.png'), fullPage: true });
    for (const correct of [0, 48]) {
      const d = structuredClone(data);
      Object.assign(d.summary, { correct, independent_correct: correct ? 32 : 0 });
      await page.evaluate(([d, p]) => window.setDashboard(d, p), [d, person]);
      assert.match(await page.locator('.method-row').nth(1).innerText(), correct ? /100.0%/ : /0.0%/);
    }
    const practiceSummary = { assigned: 0, answered: 0, practice_count: 1, attempt_count: 1, learning_days: 1, answered_people: 1, choice_answered: 0, correct: 0, accuracy: null };
    const practiceOnly = { ...data, summary: practiceSummary, today_summary: practiceSummary,
      trend: [{ date: '2026-10-09', answered: 0, practice_count: 1, attempt_count: 1, people: 1, accuracy: null }],
      people: [{ ...data.people[0], summary: practiceSummary, last_answered_at: '2026-10-09T09:00:00' }] };
    await page.evaluate(([d, p]) => window.setDashboard(d, p), [practiceOnly, person]);
    await page.getByRole('group', { name: '趋势指标' }).getByRole('button', { name: '作答次数', exact: true }).click();
    assert.equal(await page.locator('.trend-point').count(), 1);
    assert.match(await page.locator('.trend-point').getAttribute('aria-label'), /1次/);
    await page.evaluate(d => window.setDashboard(d, null), practiceOnly);
    assert.equal(await page.locator('tbody tr').count(), 1, 'a practice-only learner must remain in the building table');
    assert.match(await page.locator('tbody tr').innerText(), /复习 1 次/);
    assert.deepEqual(errors, []);
    console.log('[LearningDashboardBrowser] OK', { output });
  } finally { await browser?.close(); await server.close(); }
}
