// Source fixtures: node scripts/check-learning-sharing-images.mjs
// After the main agent builds: node scripts/check-learning-sharing-images.mjs --dist
// Keep the isolated V2 fixture available to Responsively: append --responsively.
import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer, preview } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import postcss from 'postcss';
import { adaptLearningSharing } from './workspace-sharing-adapter.mjs';
import { batchId, deniedId, generationNote, sourceImages, fixtureHtml, createFixture } from './fixtures/learning-sharing-images.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const dist = process.argv.includes('--dist');
const output = path.resolve(root, '../../../output/playwright/sharing-images', dist ? 'dist' : 'source');
const viewports = [{ width: 1440, height: 900 }, { width: 402, height: 874 }, { width: 440, height: 956 }];
await mkdir(output, { recursive: true });
const errors = [], checks = [];
const darkTokens = {};
postcss.parse(await readFile(path.join(root, 'src/workspace-v2/night.css'), 'utf8')).walkRules(rule => {
  if (rule.selector === 'html.workspace-v2[data-v2-theme=dark]' && rule.parent?.params?.startsWith('screen')) {
    rule.walkDecls(decl => { if (decl.prop.startsWith('--cf-')) darkTokens[decl.prop] = decl.value; });
  }
});
const browser = await chromium.launch({ headless: true });
const entry = (v2, role = 'admin', id = batchId) => (v2 ? '/assets/workspace-v2/index.html?__page=%2Flearning' : '/learning?') + `${v2 ? '&' : ''}tab=sharing&batch_id=${id}&role=${role}`;
let activePage, label = '', previewDone;

async function checkLayout(page, name, selector = '.sharing-exercises') {
  const overflow = await page.evaluate(() => ({ width: innerWidth, scroll: document.documentElement.scrollWidth }));
  assert(overflow.scroll <= overflow.width + 1, `${name}: page overflow ${JSON.stringify(overflow)}`);
  checks.push({ name, ...overflow });
  await page.locator(selector).screenshot({ path: path.join(output, `${name}.png`) });
}
async function checkPreview(page, trigger) {
  await trigger.click();
  const dialog = page.getByRole('dialog', { name: '题目原图预览', exact: true });
  await dialog.waitFor();
  await dialog.locator('img').evaluate(image => image.decode());
  if (label.includes('-dark')) {
    assert(await dialog.evaluate(element => {
      const color = getComputedStyle(element).backgroundColor.match(/[\d.]+/g).slice(0, 3).map(Number);
      return Math.max(...color) < 80;
    }), 'teleported preview inherits dark surface');
  }
  assert(await dialog.locator('img').evaluate(image => image.naturalWidth === 1600));
  await dialog.getByRole('button', { name: '原大小查看', exact: true }).click();
  assert.equal(await dialog.locator('output').innerText(), '100%');
  assert(await dialog.locator('img').evaluate(image => Math.abs(image.getBoundingClientRect().width - image.naturalWidth) < 1));
  await dialog.getByRole('button', { name: '放大图片', exact: true }).click();
  assert.equal(await dialog.locator('output').innerText(), '125%');
  await dialog.getByRole('button', { name: '缩小图片', exact: true }).click();
  assert.equal(await dialog.locator('output').innerText(), '100%');
  await checkLayout(page, `${label}-original`, 'dialog[open]');
  await dialog.getByRole('button', { name: '适应窗口', exact: true }).click();
  assert(await dialog.locator('img').evaluate(image => {
    const rect = image.getBoundingClientRect(), parent = image.parentElement.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0 && rect.width <= parent.width + 1 && rect.height <= parent.height + 1;
  }));
  await checkLayout(page, `${label}-preview`, 'dialog[open]');
  for (let n = 0; n < 9; n++) {
    await page.keyboard.press('Tab');
    assert(await dialog.evaluate(element => document.activeElement === document.body || element.contains(document.activeElement)), 'native dialog excludes background controls from tab order');
  }
  await page.keyboard.press('Escape');
  await dialog.waitFor({ state: 'hidden' });
  await page.waitForFunction(() => document.body.style.overflow !== 'hidden');
  assert(await trigger.evaluate(element => document.activeElement === element), 'focus returns to thumbnail');
  await trigger.click(); await dialog.getByRole('button', { name: '关闭原图', exact: true }).click();
  await dialog.waitFor({ state: 'hidden' });
  await page.waitForFunction(() => document.body.style.overflow !== 'hidden');
  assert(await page.evaluate(() => document.body.style.overflow !== 'hidden'), 'body scroll restored');
}

try {
  for (const v2 of [false, true]) {
    const previewFixtures = { admin: createFixture(), member: createFixture() };
    previewFixtures.member.role = 'member'; previewFixtures.member.batch.status = 'published';
    const middleware = server => async (req, res, next) => {
      const url = new URL(req.url, 'http://localhost');
      if (url.pathname === '/__sharing-images-stop') { res.end('stopped'); previewDone?.(); return; }
      if (url.pathname.startsWith('/api/')) {
        let raw = ''; for await (const part of req) raw += part;
        const referer = new URL(req.headers.referer || '/', 'http://localhost');
        const fixture = previewFixtures[referer.searchParams.get('role') === 'member' ? 'member' : 'admin'];
        try {
          const result = fixture.handle(url.pathname, req.method, raw ? JSON.parse(raw) : {});
          res.statusCode = result.status; res.setHeader('Content-Type', result.contentType || 'application/json');
          res.end(result.json ? JSON.stringify(result.json) : result.body);
        } catch (error) { res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: error.message })); }
        return;
      }
      if (!dist && ['/learning', '/assets/workspace-v2/index.html'].includes(url.pathname)) {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(await server.transformIndexHtml(url.pathname, fixtureHtml(v2))); return;
      }
      next();
    };
    const plugin = { name: 'sharing-images-fixture', enforce: 'pre',
      transform(code, id) { if (!dist && v2 && id.replaceAll('\\', '/').endsWith('/components/LearningSharing.vue')) return adaptLearningSharing(code); },
      configureServer(server) { server.middlewares.use(middleware(server)); },
      configurePreviewServer(server) { server.middlewares.use(middleware(server)); },
    };
    const server = dist
      ? await preview({ root, configFile: false, logLevel: 'error', plugins: [plugin], build: { outDir: process.env.CLIPFLOW_SHARING_PREVIEW_DIR || 'dist' }, preview: { host: '127.0.0.1', port: 0 } })
      : await createServer({ root, configFile: false, logLevel: 'error', plugins: [plugin, vue()], cacheDir: path.join(output, `cache-${v2}`), server: { host: '127.0.0.1', port: 0, hmr: false } });
    if (!dist) await server.listen();
    const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
    const context = await browser.newContext({ viewport: viewports[0], reducedMotion: 'reduce', serviceWorkers: 'block' });
    const page = await context.newPage(); activePage = page;
    page.on('pageerror', error => errors.push(error.message));
    let fixture = createFixture();
    await context.route('**/*', async route => {
      const request = route.request(), url = new URL(request.url());
      if (url.origin !== origin) { errors.push(`External request blocked: ${url.origin}`); return route.abort(); }
      if (!url.pathname.startsWith('/api/')) return route.continue();
      try { await route.fulfill(fixture.handle(url.pathname, request.method(), request.postData() ? request.postDataJSON() : {})); }
      catch (error) { errors.push(error.message); await route.fulfill({ status: 500, json: { ok: false, error: error.message } }); }
    });
    try {
      label = `${v2 ? 'v2' : 'legacy'}-admin-1440`;
      await page.goto(origin + entry(v2));
      await page.getByRole('heading', { name: fixture.batch.title, exact: true }).waitFor();
      assert(!fixture.requests.includes('GET /api/learning/sharing'), 'deep link bypasses first list page');
      assert.equal(await page.getByText(generationNote, { exact: true }).count(), 1);
      const q1 = page.locator('.sharing-question').filter({ has: page.locator('#sharing-q1') });
      const confirmed = q1.getByRole('checkbox', { name: '已对照原图核对题目与答案', exact: true });
      const publish = page.getByRole('button', { name: '核对后发布给参会人', exact: true });
      const save = page.getByRole('button', { name: '保存题目与名单', exact: true });
      assert(!await confirmed.isChecked()); assert(await publish.isDisabled());
      await q1.locator('.question-image-picker > summary').click();
      await q1.getByRole('checkbox', { name: `绑定图片：${sourceImages[1].label}`, exact: true }).check();
      await q1.getByRole('checkbox', { name: `绑定图片：${sourceImages[2].label}`, exact: true }).check();
      assert(await q1.getByRole('checkbox', { name: `绑定图片：${sourceImages[3].label}`, exact: true }).isDisabled());
      await save.click(); await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      assert.deepEqual(fixture.saves.at(-1).questions[0].image_ids, sourceImages.slice(0, 3).map(image => image.id));
      assert.equal(fixture.saves.at(-1).questions[0].image_confirmed, false, 'unconfirmed draft saves');
      assert(await publish.isDisabled());
      await confirmed.check();
      await q1.getByRole('checkbox', { name: `绑定图片：${sourceImages[2].label}`, exact: true }).uncheck();
      assert(!await confirmed.isChecked(), 'binding edit resets confirmation');
      const changes = [
        () => q1.locator('#sharing-q1').fill('修订题干：对照流程图应先核对什么？'),
        () => q1.locator('#sharing-option-q1-0').fill('先核对运行状态'),
        () => q1.getByRole('radio', { name: '第1题选项B为正确答案', exact: true }).check(),
        () => q1.getByLabel('答案解析', { exact: true }).fill('修订解析，仅提交后可见。'),
        () => q1.getByLabel(/^纪要\/图片依据/).fill('修订题目依据，仅管理员可见。'),
        () => q1.getByRole('button', { name: '增加选项', exact: true }).click(),
        () => q1.getByRole('button', { name: '删除第1题选项C', exact: true }).click(),
        () => q1.locator('#sharing-type-q1').selectOption('multiple'),
        () => q1.locator('#sharing-type-q1').selectOption('single'),
      ];
      for (const change of changes) { await confirmed.check(); await change(); assert(!await confirmed.isChecked(), 'question edit resets confirmation'); }
      const q4 = page.locator('.sharing-question').filter({ has: page.locator('#sharing-q4') });
      await q4.getByLabel('参考答案（问答题不自动判分）', { exact: true }).fill('修订参考答案。');
      assert(!await q4.getByRole('checkbox', { name: '已对照原图核对题目与答案', exact: true }).isChecked());
      await q4.getByRole('checkbox', { name: '已对照原图核对题目与答案', exact: true }).check();
      assert.equal(await page.locator('.sharing-question').filter({ has: page.locator('#sharing-q2') }).locator('.sharing-image-confirm').count(), 0, 'OCR needs no confirmation');
      await confirmed.check();
      await save.click(); await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      assert.match(await publish.getAttribute('title'), /第6题/, 'off-page vision is checked before publish');
      await page.getByRole('button', { name: '题目下一页', exact: true }).click();
      await page.getByRole('checkbox', { name: '已对照原图核对题目与答案', exact: true }).check();
      await page.getByRole('button', { name: '新增单选题', exact: true }).click();
      const manual = page.locator('.sharing-question[data-origin="manual"]').last();
      await manual.getByLabel('题干', { exact: true }).fill('手动图片题');
      for (const [index, option] of (await manual.locator('.sharing-option textarea').all()).entries()) await option.fill(`手动选项${index + 1}`);
      await manual.getByRole('radio').first().check();
      await manual.getByLabel('答案解析', { exact: true }).fill('手动题答案解析。');
      await manual.locator('.question-image-picker > summary').click();
      await manual.getByRole('checkbox', { name: `绑定图片：${sourceImages[1].label}`, exact: true }).check();
      await manual.getByRole('checkbox', { name: `绑定图片：${sourceImages[0].label}`, exact: true }).check();
      await manual.getByRole('checkbox', { name: '已对照原图核对题目与答案', exact: true }).check();
      await save.click(); await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      assert.deepEqual(fixture.saves.at(-1).questions.at(-1).image_ids, [sourceImages[1].id, sourceImages[0].id]);
      await manual.getByRole('button', { name: /^删除第\d+题$/ }).click();
      await save.click(); await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      await page.getByRole('button', { name: '题目上一页', exact: true }).click();
      for (const viewport of viewports) {
        await page.setViewportSize(viewport); label = `${v2 ? 'v2' : 'legacy'}-admin-${viewport.width}`;
        await checkLayout(page, `${label}-selection`);
        await checkPreview(page, q1.getByRole('button', { name: `查看原图：${sourceImages[0].label}`, exact: true }));
      }
      for (const viewport of [viewports[0], viewports[1]]) {
        await page.setViewportSize(viewport); label = `${v2 ? 'v2' : 'legacy'}-admin-dark-${viewport.width}`;
        // Reuse actual theme tokens to check shared legacy markup and teleported overlays without altering production styles.
        await page.evaluate(tokens => {
          for (const [name, value] of Object.entries(tokens)) document.documentElement.style.setProperty(name, value);
          document.documentElement.dataset.v2Theme = 'dark';
        }, darkTokens);
        await checkLayout(page, `${label}-images`, '.sharing-question:has(#sharing-q1) .question-images');
        await checkPreview(page, q1.getByRole('button', { name: `查看原图：${sourceImages[0].label}`, exact: true }));
      }
      await page.evaluate(tokens => {
        for (const name of Object.keys(tokens)) document.documentElement.style.removeProperty(name);
        document.documentElement.dataset.v2Theme = 'light';
      }, darkTokens);
      await page.setViewportSize(viewports[0]);
      assert(!await publish.isDisabled());
      await publish.click(); const confirmation = page.getByRole('dialog').filter({ hasText: '发布分享会练习' });
      await confirmation.getByRole('button', { name: '确认', exact: true }).click();
      await page.getByRole('button', { name: '关闭本批', exact: true }).waitFor();
      assert.equal(fixture.publishes, 1); assert.equal(await page.locator('.question-image-picker').count(), 0, 'published cannot bind images');

      fixture = createFixture(); fixture.role = 'member'; fixture.batch.status = 'published';
      await page.goto(origin + entry(v2, 'member'));
      await page.locator('.sharing-answer-pane').waitFor();
      assert.equal(await page.locator('.sharing-answer-pane .sharing-question').count(), 5);
      assert.equal(await page.locator('.sharing-answer').count(), 0);
      assert.equal(await page.locator('.question-image-picker,.sharing-image-confirm').count(), 0);
      const text = await page.locator('body').innerText();
      assert(!text.includes('仅管理员') && !text.includes('仅提交后') && !text.includes(generationNote));
      assert(fixture.images.every(id => [sourceImages[0].id, sourceImages[1].id].includes(id)), 'no unrelated source image requests');
      for (const viewport of viewports) {
        await page.setViewportSize(viewport); label = `${v2 ? 'v2' : 'legacy'}-member-${viewport.width}`;
        await checkLayout(page, `${label}-answer`);
        await checkPreview(page, page.locator('.sharing-answer-pane .sharing-question').first().getByRole('button', { name: /^查看原图/ }));
        assert(!await page.locator('body').innerText().then(text => /仅管理员|仅提交后/.test(text)));
      }
      for (let index = 0; index < 2; index++) {
        for (const article of await page.locator('.sharing-answer-pane .sharing-question').all()) {
          if (await article.locator('textarea').count()) await article.locator('textarea').fill('参会人的回答');
          else await article.getByRole('radio').first().check();
        }
        if (!index) { await page.getByRole('button', { name: '答题下一页', exact: true }).click(); await page.getByText('第 2 / 2 页', { exact: true }).waitFor(); }
        else { await page.getByRole('button', { name: '提交全部答题', exact: true }).click(); await page.getByRole('heading', { name: '我的答卷', exact: true }).waitFor(); }
      }
      assert.equal(fixture.answerPosts.length, 2);
      assert.equal(fixture.answerPosts[0].submit, false); assert.equal(fixture.answerPosts[1].submit, true);
      assert.match(await page.locator('.sharing-answer-pane').innerText(), /仅提交后解析/);
      assert(!await page.locator('body').innerText().then(text => /仅管理员/.test(text)));
      const answered = fixture.answerPosts.length;
      await page.getByRole('button', { name: '答题下一页', exact: true }).click();
      assert.equal(fixture.answerPosts.length, answered, 'submitted navigation never writes');

      await page.goto(origin + entry(v2, 'member', deniedId));
      await page.getByRole('alert').filter({ hasText: '无权查看此分享会批次' }).waitFor();
      assert.equal(await page.locator('.question-images').count(), 0);
      const beforeInvalid = fixture.requests.length;
      await page.goto(origin + entry(v2, 'member', 'invalid'));
      await page.getByRole('alert').filter({ hasText: '分享会批次编号无效' }).waitFor();
      assert(!fixture.requests.slice(beforeInvalid).some(item => item.includes('/api/learning/sharing/')));
      assert.deepEqual(fixture.unexpected, []);
      if (process.argv.includes('--responsively') && v2) {
        console.log(`ISOLATED_PREVIEW=${origin}${entry(v2)}`);
        console.log(`ISOLATED_MEMBER_PREVIEW=${origin}${entry(v2, 'member')}`);
        await new Promise(resolve => { previewDone = resolve; const timeout = setTimeout(resolve, 600000); timeout.unref(); });
      }
    } catch (error) {
      await page.screenshot({ path: path.join(output, `${v2 ? 'v2' : 'legacy'}-failure.png`), fullPage: true });
      throw error;
    } finally {
      await context.close();
      if (dist) await new Promise(resolve => server.httpServer.close(resolve)); else await server.close();
    }
  }
  assert.deepEqual(errors, []);
  await writeFile(path.join(output, 'checks.json'), JSON.stringify({ mode: dist ? 'dist' : 'source', checks, errors }, null, 2));
  console.log(`PASS (${dist ? 'existing dist' : 'source fixtures'}): legacy/V2, image IDs, three-image limit, edit resets, vision publish gate, OCR, manual binding, preview/focus/zoom, 402x874/440x956/1440x900, page-scoped answers, protected deep links. No real data or messages.`);
} catch (error) {
  console.error({ label, errors, message: error.message });
  if (activePage && !activePage.isClosed()) await activePage.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally { await browser.close(); }
