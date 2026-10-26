import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { adaptLearningSharing } from './workspace-sharing-adapter.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/sharing-resilience');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true }), checks = [], errors = [];
const bid = 'a'.repeat(32);
function fixture() {
  const questions = Array.from({ length: 5 }, (_, index) => ({ id: 'q' + index, origin: 'manual', type: 'single',
    stem: `现场检查项目${index + 1}应如何核对？`, options: ['核对现场并按审批流程处理', '未核对直接操作'], correct: [0], explanation: '核对现场后按审批流程执行。', evidence: '核对现场后按审批流程执行。' }));
  return { id: bid, title: '分享会隔离验收', status: 'draft', version: 1, questions, participants: [{ name: '本人', person_id: 'p1', open_id: 'ou_self', scope: 'A', exempted: false, delivery_status: 'sent', reminder_delivery_status: 'failed', reminder_delivery_error: '隔离催答失败' }],
    total: 1, completed: 0, question_count: 5, question_types: ['single'], bank_sync: { total: 5, synced: 5, failed: 0 }, weekly_reports: [],
    evidence_locations: { q0: [{ title: '分享材料 · 第2页', excerpt: questions[0].evidence, references: [{ page: 2 }] }] } };
}
try {
  for (const version of ['legacy', 'v2']) {
    let batch = fixture(), conflict = false, closeOnConflict = false, submits = 0, writes = [], progressReads = 0;
    const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{margin:0;padding:12px;font:14px sans-serif}*{box-sizing:border-box}#app{max-width:1280px;margin:auto}</style><div id="app" class="${version === 'v2' ? 'workspace-v2' : ''}"></div><script type="module">import {createApp,h} from 'vue';import {NConfigProvider,NDialogProvider} from 'naive-ui';import Sharing from '/src/components/LearningSharing.vue';import UiTransition from '/src/components/UiTransition.vue';const user=new URL(location).searchParams.get('user')||'ou_self';createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(Sharing,{isAdmin:true,userId:user,onDirty:v=>window.sharingDirty=v,onBusy:v=>window.sharingBusy=v})))}).component('UiTransition',UiTransition).mount('#app');</script>`;
    const server = await createServer({ root, configFile: false, cacheDir: path.join(output, 'vite-' + version), logLevel: 'error',
      plugins: [{ name: 'sharing-recovery-fixture', enforce: 'pre', transform(code, id) { if (version === 'v2' && id.replaceAll('\\', '/').endsWith('/components/LearningSharing.vue')) return adaptLearningSharing(code); },
        configureServer(dev) { dev.middlewares.use(async (req, res, next) => {
          const url = new URL(req.url, 'http://localhost');
          if (url.pathname === '/__sharing-audit') { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(await dev.transformIndexHtml(url.pathname, html)); return; }
          if (!url.pathname.startsWith('/api/')) return next();
          const reply = (data, status = 200) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(status === 200 ? { ok: true, data } : { ok: false, error: '答题进度已变化，请刷新后重试；未覆盖新答案。' })); };
          let body = ''; for await (const chunk of req) body += chunk;
          const data = body && req.headers['content-type']?.includes('json') ? JSON.parse(body) : {};
          if (url.pathname === '/api/learning/sharing/people') return reply({ items: [{ person_id: 'p2', id: 'p2', name: '补充人员', open_id: 'ou_second', scope: 'B' }] });
          if (url.pathname === '/api/learning/sharing') return reply({ items: batch.deleted ? [] : [batch], total: batch.deleted ? 0 : 1 });
          if (url.pathname === `/api/learning/sharing/${bid}/delete`) { batch.deleted = true; return reply({ id: bid, deleted: true }); }
          if (url.pathname === `/api/learning/sharing/${bid}/answers`) {
            writes.push(data);
            if (conflict) { conflict = false; batch.paper.draft_answers.q0 = [1]; batch.paper.progress_version++; if (closeOnConflict) batch.status = 'closed'; return reply({}, 409); }
            batch.paper.draft_answers = Object.fromEntries(Object.entries(data.answers).map(([id, value]) => [id, value.map(Number)]));
            batch.paper.progress_version++;
            if (data.submit) { submits++; batch.paper.submitted_at = '2026-10-23T12:00:00+08:00'; batch.completed = 1; }
            return reply({ id: bid, submitted: !!data.submit });
          }
          if (url.pathname === `/api/learning/sharing/${bid}`) {
            if (req.method === 'PUT') { writes.push(data); batch.version++; if (data.participant_ids?.includes('p2')) batch.participants.push({ person_id: 'p2', name: '补充人员', scope: 'B', open_id: 'ou_second', exempted: false }); batch.total = batch.participants.filter(p => !p.exempted).length; }
            const view = structuredClone(batch);
            if (view.paper) { view.my_questions = view.questions.map(q => ({ ...q, selected: view.paper.draft_answers[q.id] })); if (!view.paper.submitted_at) for (const q of view.my_questions) for (const field of ['correct', 'evidence', 'explanation']) delete q[field]; }
            if (url.searchParams.get('progress') === '1') { progressReads++; for (const field of ['questions', 'my_questions', 'evidence_locations', 'results']) delete view[field]; }
            return reply(view);
          }
          return reply({}, 403);
        }); } }, vue()], server: { host: '127.0.0.1', port: 0 } });
    await server.listen();
    const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
    try {
      await page.goto(origin + '/__sharing-audit');
      await page.getByRole('button', { name: 'AI出题', exact: true }).click();
      await page.getByLabel('练习标题（可选）').fill('可以恢复的新建草稿');
      await page.getByLabel('资料链接', { exact: true }).fill('https://vnet.feishu.cn/wiki/SourceFixture');
      await page.getByRole('button', { name: '添加链接', exact: true }).click();
      await page.waitForFunction(() => window.sharingDirty === true);
      await page.reload();
      assert.equal(await page.getByLabel('练习标题（可选）').inputValue(), '可以恢复的新建草稿');
      await page.goto(origin + '/__sharing-audit?user=ou_other');
      assert.equal(await page.getByLabel('练习标题（可选）').count(), 0, 'Other accounts cannot see the saved creation draft');
      await page.goto(origin + '/__sharing-audit');
      await page.getByRole('button', { name: '返回批次列表', exact: true }).click();
      await page.getByRole('dialog').getByRole('button', { name: '确认', exact: true }).click();
      await page.locator('.sharing-batch').first().click();
      await page.locator('.n-tabs-tab').filter({ hasText: '参会人' }).click();
      await page.getByText('补充参会人', { exact: true }).click();
      await page.getByRole('button', { name: /补充人员 B/ }).click();
      await page.getByRole('button', { name: '保存题目与名单', exact: true }).click();
      await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      assert.deepEqual(writes.at(-1).participant_ids, ['p2']);
      await page.locator('.n-tabs-tab').filter({ hasText: '题目核对' }).click();
      await page.getByRole('button', { name: '定位题目依据', exact: true }).first().click();
      await page.getByText('分享材料 · 第2页', { exact: true }).waitFor();
      await page.keyboard.press('Escape');
      checks.push(version + ': account-scoped draft recovery, participant addition and original evidence');

      batch.status = 'published'; batch.version++;
      batch.paper = { id: 'attempt', open_id: 'ou_self', question_ids: batch.questions.map(q => q.id), draft_answers: {}, progress_version: 0, submitted_at: '' };
      await page.reload(); await page.locator('.sharing-batch').first().click();
      await page.getByRole('region', { name: '我的答题', exact: true }).waitFor();
      for (let i = 1; i <= 5; i++) await page.getByRole('radio', { name: `第${i}题选项A`, exact: true }).check();
      conflict = true;
      await page.getByRole('button', { name: '保存本页进度', exact: true }).click();
      await page.getByRole('region', { name: '答题冲突核对', exact: true }).waitFor();
      assert.equal(submits, 0);
      await page.getByRole('button', { name: '保留本页答案', exact: true }).click();
      await page.getByRole('button', { name: '提交全部答题', exact: true }).click();
      await page.getByText('已提交本批全部题目，可查看答案与解析。', { exact: true }).waitFor();
      assert.equal(submits, 1); assert.deepEqual(writes.at(-1).answers.q0, ['0']); assert.equal(writes.at(-1).version, 1);
      await page.waitForTimeout(8300);
      assert(progressReads > 0, 'Automatic refresh uses the light progress endpoint');
      checks.push(version + ': admin answers own paper, conflict keeps input and progress polling stays lightweight');
      await page.locator('.n-tabs-tab').filter({ hasText: '通知与同步' }).click();
      const row = page.getByRole('row').filter({ hasText: '本人' });
      assert.match(await row.innerText(), /已发送/); assert.match(await row.innerText(), /隔离催答失败/);
      for (const view of [{ name: 'MacBookPro', width: 1440, height: 900 }, { name: 'iPhone17Pro', width: 402, height: 874 }, { name: 'iPhone17ProMax', width: 440, height: 956 }, { name: 'small', width: 360, height: 640 }]) {
        await page.setViewportSize({ width: view.width, height: view.height });
        await page.locator('.n-tabs-tab').filter({ hasText: '我的答题' }).click();
        await page.evaluate(async () => { await Promise.all(document.getAnimations().filter(a => a.effect?.getTiming().iterations !== Infinity).map(a => a.finished.catch(() => {}))); });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, version + ': ' + view.name);
        await page.screenshot({ path: path.join(output, version + '-' + view.name + '.png'), fullPage: true });
      }
      batch.paper.submitted_at = ''; batch.status = 'published'; closeOnConflict = true; conflict = true;
      await page.reload(); await page.locator('.sharing-batch').first().click();
      await page.getByRole('button', { name: '保存本页进度', exact: true }).click();
      await page.getByText('此批次已关闭，本页未提交输入仍保留，不能继续保存或提交。', { exact: true }).waitFor();
      assert.equal(await page.getByRole('button', { name: '保存本页进度', exact: true }).count(), 0);
      assert.equal(submits, 1);
      await page.getByRole('button', { name: '删除批次', exact: true }).click();
      await page.getByRole('dialog').getByRole('button', { name: '确认', exact: true }).click();
      await page.getByText('批次已删除，已上传题目保留。', { exact: true }).waitFor();
      assert(batch.deleted);
      checks.push(version + ': split notification states, responsive views and shared deletion workflow');
    } catch (error) { console.log(await page.locator('body').innerText()); console.log(errors); await page.screenshot({ path: path.join(output, version + '-failure.png'), fullPage: true }); throw error; }
    finally { await context.close(); await server.close(); }
  }
  assert.deepEqual(errors, []);
  await writeFile(path.join(output, 'checks.json'), JSON.stringify(checks, null, 2));
  console.log(checks.join('\n'));
} finally { await browser.close(); }
