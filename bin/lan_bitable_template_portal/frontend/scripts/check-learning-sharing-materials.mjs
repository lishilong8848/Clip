import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { adaptLearningSharing } from './workspace-sharing-adapter.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/sharing-materials');
await mkdir(output, { recursive: true });
const errors = [];
const browser = await chromium.launch({ headless: true });
function question(index, origin = 'manual') {
  const type = index % 3 === 2 ? 'interview' : index % 3 ? 'multiple' : 'single';
  return { id: `q${index + 1}`, origin, type, stem: `材料知识点${index + 1}：如何核实运行条件？`,
    options: type === 'interview' ? [] : ['核对现场', '按批准流程处理'], correct: type === 'interview' ? [] : type === 'multiple' ? [0, 1] : [0],
    reference_answer: type === 'interview' ? '按审批流程核对现场。' : '', explanation: '专业经验解析', evidence: origin === 'ai' ? '材料记录：按审批流程核对现场。' : '' };
}
function batchFor(manual) {
  const questions = Array.from({ length: Math.max(5, manual) }, (_, i) => question(i, i < manual ? 'manual' : 'ai'));
  return { id: 'fixture', title: '分享材料与人工题隔离验收', url: '', status: 'published', version: 2,
    question_types: ['single', 'multiple', 'interview'], question_count: questions.length, completed: 0, total: 1, questions,
    participants: [{ name: '测试人员', open_id: 'ou_fixture', person_id: 'p1', scope: 'A', exempted: false }],
    paper: { id: 'opaque-attempt-not-openid', open_id: 'ou_fixture', question_ids: questions.map(q => q.id), draft_answers: {}, progress_version: 0, submitted_at: '' } };
}
try {
  for (const v2 of [false, true]) {
    let batch = batchFor(0), role = 'admin', operations = new Map(), posts = 0, loss = false, saved, previewDone, manualCreates = 0;
    const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      <style>body{margin:0;padding:16px;font:14px sans-serif;background:#f7f9fc}#app{max-width:1280px;margin:auto}*{box-sizing:border-box}</style>
      <div id="app" class="${v2 ? 'workspace-v2' : ''}"></div><script type="module">
      import {createApp,h} from 'vue';import {NConfigProvider,NDialogProvider} from 'naive-ui';
      import Sharing from '/src/components/LearningSharing.vue';import UiTransition from '/src/components/UiTransition.vue';
      const admin = new URL(location.href).searchParams.get('role') !== 'member';
      createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(Sharing,{isAdmin:admin,userId:'ou_fixture'})))}).component('UiTransition',UiTransition).mount('#app');</script>`;
    const server = await createServer({ root, configFile: false, cacheDir: path.join(output, `cache-${v2}`), logLevel: 'error',
      plugins: [{ name: 'sharing-materials-fixture', enforce: 'pre',
        transform(code, id) { if (v2 && id.replaceAll('\\', '/').endsWith('/components/LearningSharing.vue')) return adaptLearningSharing(code); },
        configureServer(dev) {
          dev.middlewares.use(async (req, res, next) => {
            const url = new URL(req.url, 'http://localhost');
            if(url.pathname==='/__sharing-stop'){res.end('stopped');previewDone?.();return;}
            if (url.pathname === '/__sharing-materials') {
              res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(await dev.transformIndexHtml(url.pathname, html)); return;
            }
            if (!url.pathname.startsWith('/api/')) return next();
            const ok = (data, status = 200) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ ok: status === 200, data, error: status === 200 ? '' : '隔离回执丢失，请重试原操作' })); };
            let raw = ''; for await (const part of req) raw += part;
            const body = req.headers['content-type']?.includes('application/json') ? JSON.parse(raw || '{}') : {};
            if (url.pathname === '/api/learning/sharing/people') return ok({ items: [{ id: 'p1', person_id: 'p1', name: '测试人员', open_id: 'ou_fixture', scope: 'A' }] });
            if (url.pathname === '/api/learning/sharing/materials') return ok({ items: [{ id: 'fixture-file', name: 'slides.pptx', size: 7 }] });
            if (url.pathname === '/api/learning/sharing') {
              if (req.method === 'POST') {
                assert.equal(body.url, ''); assert.deepEqual(body.participant_ids, ['p1']);
                if (body.title === '纯手工无链接练习') {
                  assert.deepEqual(body.material_urls, []); assert.deepEqual(body.material_ids, []); manualCreates++;
                } else {
                  assert.deepEqual(body.material_urls, ['https://vnet.feishu.cn/wiki/Fixture', 'https://vnet.feishu.cn/file/FileFixture']);
                  assert.deepEqual(body.material_ids, ['fixture-file']);
                }
                assert.equal(body.auto_generate, false);
                batch = { ...batchFor(0), status: 'draft', version: 1, questions: [], question_count: 0 }; delete batch.paper;
                return ok({ id: batch.id });
              }
              const items = role === 'admin' ? [batch, ...Array.from({length:4},(_,i)=>({...batch,id:`history-${i}`,title:`第${i+1}批 · A楼2号冷机轮巡过程中触发冷却水流量过低告警排查处理过程分享会${' · 长标题'.repeat(i+1)}`,status:'failed'}))] : [batch];
              return ok({ items, page: 1, total: items.length });
            }
            if (url.pathname === '/api/learning/sharing/fixture') {
              if (req.method === 'PUT') { saved = body; batch.questions = body.questions; batch.question_count = body.questions.length; batch.status = 'draft'; batch.version++; }
              const view = structuredClone(batch);
              if (role !== 'admin' && !view.paper.submitted_at) for (const q of view.questions) for (const k of ['correct', 'explanation', 'evidence', 'reference_answer']) delete q[k];
              if (view.paper?.submitted_at) for (const q of view.questions) q.selected = view.paper.draft_answers[q.id];
              return ok(view);
            }
            if (url.pathname === '/api/learning/sharing/fixture/answers') {
              posts++;
              assert.equal(typeof body.version, 'number');
              assert.deepEqual(Object.keys(body.answers), batch.paper.question_ids.slice(body.page * 5, body.page * 5 + 5));
              if (!operations.has(body.operation_id)) {
                assert.equal(body.version, batch.paper.progress_version);
                for (const [id, answer] of Object.entries(body.answers)) {
                  if (Array.isArray(answer)) { assert(answer.every(i => typeof i === 'string')); batch.paper.draft_answers[id] = answer.map(Number); }
                  else batch.paper.draft_answers[id] = answer;
                }
                batch.paper.progress_version++; operations.set(body.operation_id, JSON.stringify(body));
                if (body.submit) { assert.equal(Object.keys(batch.paper.draft_answers).length, batch.questions.length); batch.paper.submitted_at = '2026-10-20T10:00:00+08:00'; batch.completed = 1; }
                if (loss) { loss = false; return ok({}, 503); }
              } else assert.equal(operations.get(body.operation_id), JSON.stringify(body), 'uncertain save retries the same operation');
              return ok({ submitted: !!batch.paper.submitted_at });
            }
            throw new Error(`Unexpected isolated API ${req.method} ${url.pathname}`);
          });
        } }, vue()], server: { host: '127.0.0.1', port: 0, hmr: false } });
    await server.listen();
    const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    const page = await context.newPage(); page.setDefaultTimeout(15000); page.setDefaultNavigationTimeout(60000); page.on('pageerror', e => errors.push(e.message));
    try {
      await page.goto(origin + '/__sharing-materials');
      for (const [width,height] of [[1440,900],[402,874],[440,956]]) {
        await page.setViewportSize({width,height});
        await page.locator('.sharing-batch').last().waitFor();
        const layout=await page.evaluate(()=>{
          const rows=[...document.querySelectorAll('.sharing-batch')];
          return {overflow:document.documentElement.scrollWidth>innerWidth+1, overlap:rows.some((row,i)=>i && row.getBoundingClientRect().top < rows[i-1].getBoundingClientRect().bottom-1), clipped:rows.some(row=>{
            const rect=row.getBoundingClientRect();return [...row.querySelectorAll('.sharing-batch-title,.sharing-batch-meta,strong,small')].some(child=>{const r=child.getBoundingClientRect();return r.top<rect.top-1||r.bottom>rect.bottom+1||r.right>rect.right+1;});
          }), fields:[...document.querySelectorAll('.sharing-source')].map(el=>({top:el.getBoundingClientRect().top,left:el.getBoundingClientRect().left}))};
        });
        assert.equal(layout.overflow,false);assert.equal(layout.overlap,false);assert.equal(layout.clipped,false,`${v2}-${width}: history content must fit its own row`);
        assert.equal(layout.fields.length,0,'Creation is separate from the batch list');
        await page.screenshot({path:path.join(output,`history-${v2}-${width}.png`),fullPage:true});
      }
      await page.setViewportSize({width:1440,height:900});
      await page.getByRole('button',{name:'新建分享会练习',exact:true}).click();
      await page.getByLabel('飞书纪要链接', {exact:true}).fill('invalid-unused-url');
      await page.getByLabel('资料链接', {exact:true}).fill('unused material draft');
      await page.getByLabel('选择资料文件').setInputFiles({name:'unused.pptx',mimeType:'application/octet-stream',buffer:Buffer.from('fixture')});
      await page.getByText('纯手工出题',{exact:true}).click();
      assert.equal(await page.locator('.sharing-source').count(), 0, 'pure manual mode has no required materials');
      assert(await page.getByRole('button',{name:'创建空白草稿',exact:true}).isDisabled(), 'explicit recipients are still required');
      await page.getByRole('button',{name:'读取人员目录',exact:true}).click();
      await page.getByRole('button',{name:'添加参会人：测试人员',exact:true}).click();
      await page.getByLabel('练习标题（可选）', {exact:true}).fill('纯手工无链接练习');
      assert(await page.getByRole('button',{name:'创建空白草稿',exact:true}).isEnabled(), 'unused pending materials must not block manual creation');
      await page.getByText('资料出题',{exact:true}).click();
      assert.equal(await page.getByLabel('飞书纪要链接',{exact:true}).inputValue(), 'invalid-unused-url', 'switching modes preserves unfinished inputs');
      await page.getByText('纯手工出题',{exact:true}).click();
      await page.reload();
      await page.getByRole('radio',{name:'纯手工出题',exact:true}).waitFor();
      assert(await page.getByRole('radio',{name:'纯手工出题',exact:true}).isChecked(), 'manual mode survives restoring a creation draft');
      for (const [width,height] of [[1440,900],[402,874],[440,956]]) {
        await page.setViewportSize({width,height});
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
        assert(await page.evaluate(()=>{
          const mode=document.querySelector('.sharing-mode').getBoundingClientRect();
          const title=document.querySelector('label[for="sharing-title"]').getBoundingClientRect();
          const search=document.querySelector('.sharing-people-tools input').getBoundingClientRect();
          const read=document.querySelector('.sharing-people-tools button').getBoundingClientRect();
          const titleInput=document.querySelector('#sharing-title').getBoundingClientRect();
          const chosen=document.querySelector('.sharing-create-actions .sharing-chips').getBoundingClientRect();
          const submit=document.querySelector('.sharing-create-actions>button').getBoundingClientRect();
          return title.left>=mode.right && titleInput.top<mode.bottom && Math.abs(search.top-read.top)<4
            && Math.abs((chosen.top+chosen.bottom)-(submit.top+submit.bottom))<8
            && [...document.querySelectorAll('.sharing-mode .n-radio-button')].every(el=>{
              const r=el.getBoundingClientRect(); return r.top>=mode.top && r.bottom<=mode.bottom+1 && r.right<=title.left;
            });
        }), 'mode/title and people search/actions must share rows without overlap');
        await page.screenshot({path:path.join(output,`pure-manual-${v2}-${width}.png`),fullPage:true});
      }
      await page.getByRole('button',{name:'创建空白草稿',exact:true}).click();
      await page.getByRole('button',{name:'新增问答题',exact:true}).click();
      await page.getByLabel('题干',{exact:true}).fill('纯手工题目');
      await page.getByLabel('参考答案（问答题不自动判分）',{exact:true}).fill('人工参考答案');
      await page.getByLabel('答案解析',{exact:true}).fill('人工解析');
      await page.getByRole('button',{name:'保存题目与名单',exact:true}).click();
      await page.getByText('题目与答题名单已保存。',{exact:true}).waitFor();
      assert.equal(manualCreates,1); assert.equal(saved.questions[0].origin,'manual'); assert.equal(saved.questions[0].evidence,'');
      await page.getByRole('button',{name:'批次列表',exact:true}).click();
      await page.setViewportSize({width:1440,height:900});
      await page.getByRole('button',{name:'新建分享会练习',exact:true}).click();
      assert(await page.locator('.sharing-source').count() >= 2);
      await page.getByLabel('使用飞书纪要').uncheck();
      await page.getByLabel('AI生成题目').uncheck();
      await page.getByLabel('资料链接', { exact: true }).fill('https://vnet.feishu.cn/wiki/Fixture');
      await page.getByRole('button', { name: '添加链接', exact: true }).click();
      await page.getByLabel('资料链接', { exact: true }).fill('https://vnet.feishu.cn/file/FileFixture/?from=share#fragment');
      await page.getByLabel('选择资料文件').setInputFiles({ name: 'slides.pptx', mimeType: 'application/octet-stream', buffer: Buffer.from('fixture') });
      await page.getByRole('button', { name: '上传 1 份' }).click();
      assert(await page.getByRole('button', { name: '创建空白草稿', exact: true }).isDisabled(), 'material-only roster is explicit');
      await page.getByRole('button', { name: '读取人员目录' }).click();
      await page.getByRole('button', { name: '添加参会人：测试人员', exact: true }).click();
      await page.getByRole('button', { name: '创建空白草稿', exact: true }).click();
      await page.getByRole('button', { name: '新增问答题', exact: true }).click();
      await page.getByLabel('题干', { exact: true }).fill('人工经验问答题');
      await page.getByLabel('参考答案（问答题不自动判分）', { exact: true }).fill('经验参考答案');
      await page.getByLabel('答案解析', { exact: true }).fill('经验解析无需伪造证据');
      await page.getByRole('button', { name: '新增单选题', exact: true }).click();
      assert.equal(await page.locator('.sharing-question').count(), 2, 'dirty draft stays editable');
      await page.getByLabel('删除第2题', { exact: true }).click();
      await page.getByRole('button', { name: '保存题目与名单', exact: true }).click();
      await page.getByText('题目与答题名单已保存。', { exact: true }).waitFor();
      assert.equal(saved.questions[0].origin, 'manual'); assert.equal(saved.questions[0].evidence, '');
      await page.screenshot({ path: path.join(output, `manual-${v2}.png`), fullPage: true });
      batch.status='failed';batch.error='模型未返回有效回答';
      await page.reload();await page.locator('.sharing-batch').first().click();
      await page.getByRole('button',{name:'新增问答题',exact:true}).click();
      await page.getByLabel('题干',{exact:true}).last().fill('模型失败后补录的题目');
      await page.getByLabel('参考答案（问答题不自动判分）',{exact:true}).last().fill('人工参考答案');
      await page.getByLabel('答案解析',{exact:true}).last().fill('人工解析');
      assert(await page.getByRole('button',{name:'重新生成',exact:true}).isDisabled(),'unsaved manual edits cannot be discarded by retry');
      await page.getByRole('button',{name:'保存题目与名单',exact:true}).click();
      await page.getByText('题目与答题名单已保存。',{exact:true}).waitFor();
      assert.equal(saved.questions.length,2);assert.equal(batch.status,'draft');
      role = 'member'; batch = batchFor(11); batch.status = 'closed';
      batch.paper.draft_answers = { q1: [0], q3: '此前保存的答案' };
      await page.goto(origin + '/__sharing-materials?role=member'); await page.locator('.sharing-batch').click();
      await page.getByRole('heading', { name: '我的答题记录', exact: true }).waitFor();
      assert(await page.locator('.sharing-answer-pane input').first().isChecked());
      assert(await page.locator('.sharing-answer-pane input').first().isDisabled());
      assert.equal(await page.locator('.sharing-answer-pane textarea').inputValue(), '此前保存的答案');
      assert.equal(await page.getByRole('button', { name: '保存本页进度', exact: true }).count(), 0);
      const closedPosts = posts;
      await page.getByRole('button', { name: '答题下一页', exact: true }).click();
      await page.getByText('第 2 / 3 页', { exact: true }).waitFor();
      assert.equal(posts, closedPosts, 'closed paper navigation never saves');
      if (v2) {
        await page.locator('.v2-sharing-tabs .n-tabs-tab').filter({ hasText: '参会人' }).click();
        assert.equal(await page.locator('.sharing-answer-pane').count(), 0, 'people tab hides the paper');
        await page.locator('.v2-sharing-tabs .n-tabs-tab').filter({ hasText: '我的答题' }).click();
        await page.getByText('第 2 / 3 页', { exact: true }).waitFor();
      }
      for (const count of [0, 4, 5, 11]) {
        role = 'member'; batch = batchFor(count); operations = new Map(); posts = 0;
        await page.goto(origin + '/__sharing-materials?role=member'); await page.locator('.sharing-batch').click();
        const pages = Math.ceil(batch.questions.length / 5);
        for (let index = 0; index < pages; index++) {
          const articles = page.locator('.sharing-answer-pane .sharing-question');
          await articles.first().waitFor(); assert.equal(await articles.count(), Math.min(5, batch.questions.length - index * 5));
          for (const article of await articles.all()) {
            if (await article.locator('textarea').count()) await article.locator('textarea').fill('本人答题内容');
            else await article.locator('input').first().check();
          }
          if (count === 11 && index === 0) {
            loss = true; await page.getByRole('button', { name: '保存本页进度', exact: true }).click();
            await page.getByRole('alert').filter({ hasText: '隔离回执丢失' }).waitFor();
            await page.getByRole('button', { name: '保存本页进度', exact: true }).click();
            await page.getByText('答题进度已保存，尚未提交整卷。', { exact: true }).waitFor();
            assert.equal(batch.paper.progress_version, 1);
            await page.reload(); await page.locator('.sharing-batch').click();
            assert(await page.locator('.sharing-answer-pane input').first().isChecked(), 'saved answers survive reload');
          }
          if (index + 1 < pages) { await page.getByRole('button', { name: '答题下一页', exact: true }).click(); await page.getByText(`第 ${index + 2} / ${pages} 页`, { exact: true }).waitFor(); assert.equal(batch.completed, 0); }
          else { await page.getByRole('button', { name: '提交全部答题', exact: true }).click(); await page.getByRole('heading', { name: '我的答卷', exact: true }).waitFor(); }
        }
        assert.equal(batch.completed, 1); assert.equal(operations.size, pages + (count === 11 ? 1 : 0));
        assert.equal(await page.getByRole('button', { name: '重新答题', exact: true }).count(), 0);
        for (const width of [402, 440]) {
          await page.setViewportSize({ width, height: width===402?874:956 });
          assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
          await page.screenshot({ path: path.join(output, `result-${v2}-${count}-${width}.png`), fullPage: true });
        }
        await page.setViewportSize({ width: 1440, height: 900 });
      }
      if (process.argv.includes('--responsively') && v2) {
        role = 'admin'; batch = batchFor(6); batch.status = 'draft'; delete batch.paper;
        console.log(`ISOLATED_PREVIEW=${origin}/__sharing-materials`);
        await new Promise(resolve => {previewDone=resolve;setTimeout(resolve,600000).unref();});
      }
    } finally { await context.close(); await server.close(); }
  }
  assert.deepEqual(errors, []);
  console.log('PASS: legacy/V2 materials + explicit roster, manual draft add/edit/delete, papers 0/4/5/11 manual, page-scoped saves, lost receipt idempotency, reload, full-submit only, immutable results and mobile fit. No external requests.');
} finally { await browser.close(); }
