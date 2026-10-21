// Isolated synthetic preview and browser regression, without a backend or cloud.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { mkdir } from 'node:fs/promises';
const root = fileURLToPath(new URL('./lan_bitable_template_portal/frontend/', import.meta.url));
const v2 = process.argv.includes('--v2');
const require = createRequire(path.join(root, 'package.json'));
const { createServer } = require('vite');
const { chromium } = require('playwright');
const output = fileURLToPath(new URL('../output/playwright/learning-completion/', import.meta.url));
await mkdir(output, { recursive: true });
const states = ['未安排', '未到时', '待答', '答题中', '已完成', '逾期', '待核对'];
const requests = [], errors = [];
const date = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Shanghai' });
let unavailable = false;
const rows = states.map((status, index) => ({
  id: `daily:${date}:synthetic-${index}`, date, person_id: `synthetic-${index}`,
  person: { id: `synthetic-${index}`, name: `测试人员${index + 1}` }, scopes: ['A'],
  status, assigned: index ? 15 : 0, answered: index === 4 ? 15 : index === 3 ? 3 : 0,
  correct: index === 4 ? 12 : 0, accuracy: index === 4 ? 80 : null,
  answer_viewed: index === 4 ? 2 : 0, answer_view_unknown: index === 6 ? 3 : 0,
  practice_questions: index === 4 ? 5 : 0, practice_attempts: index === 4 ? 7 : 0,
  review_count: index === 4 ? 2 : 0, updated_at: `${date}T14:00:00+08:00`, delivery_failed: index === 4,
  source_papers: index === 4 ? ['paper-completed'] : [],
  tasks: index ? [{ task_id: `task-${index}`, source: 'paper', scope: 'A', shift: index % 2 ? '白' : '夜',
    assigned: 15, answered: index === 4 ? 15 : index === 3 ? 3 : 0, status,
    delivery_status: index === 4 ? '发送失败' : '已发送' }] : [],
}));
const server = await createServer({ root, configFile: path.join(root, v2 ? 'vite.workspace-v2.config.ts' : 'vite.config.ts'),
  ...(v2 ? { optimizeDeps: { noDiscovery: true, include: ['vue', 'naive-ui', 'lucide-vue-next'] } } : {}),
  logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false },
  plugins: [{ name: 'completion-fixture', configureServer(vite) {
    vite.middlewares.use(async (req, res, next) => {
      const url = new URL(req.url, 'http://fixture.local');
      if(url.pathname==='/__completion_stop'){res.end('stopped');setTimeout(()=>void server.close(),20);return;}
      if (url.pathname === '/__completion_check') {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.end(await vite.transformIndexHtml(req.url, `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{margin:0;background:#f6f9fc;font:14px Arial,'Microsoft YaHei',sans-serif}</style></head><body class="${v2 ? 'workspace-v2' : ''}"><div id="app"></div><script type="module">
          import {createApp,h,provide,ref} from 'vue'; import LearningPage from '/src/components/LearningPage.vue'; import UiTransition from '/src/components/UiTransition.vue';
          import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui'; import {pageContext} from '/src/workspace-v2/pageLayout';
          ${v2 ? "import '/src/workspace-v2/learning.css';" : ''}
          createApp({setup(){provide(pageContext,{user:ref({open_id:'synthetic-preview',name:'验收人员',modules:['learning']}),scope:ref('A'),scopes:ref(['A']),module:ref('learning')});return()=>h(NConfigProvider,null,{default:()=>h(NDialogProvider,null,{default:()=>h(NMessageProvider,null,{default:()=>h(LearningPage,{scope:'A',userId:'synthetic-preview'})})})});}}).component('UiTransition',UiTransition).mount('#app');
        </script></body></html>`));
      }
      if (!url.pathname.startsWith('/api/')) return next();
      requests.push({ path: url.pathname, method: req.method, query: Object.fromEntries(url.searchParams) });
      res.setHeader('Content-Type', 'application/json');
      const ok = data => res.end(JSON.stringify({ ok: true, data }));
      if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
      if (url.pathname === '/api/health') return res.end(JSON.stringify({ ok: true, service: 'clipflow_backend', instance_id: 'completion-fixture' }));
      if (url.pathname.endsWith('/bootstrap')) return ok({ is_admin: true, can_answer: false, can_view_buildings: true,
        scopes: [...'ABCDEH'].map(value => ({ value, label: `${value}楼` })), scope: 'A', today: date,
        settings: { enabled: true }, sync: { status: 'ready', pending: 0 } });
      if(url.pathname.endsWith('/papers/paper-completed'))return ok({id:'paper-completed',date,scope:'A',person_id:'synthetic-4',person:{id:'synthetic-4',name:'测试人员5'},status:'completed',questions:[{id:'synthetic-question',version:'1',type:'single',bank:'written',stem:'核对首次作答及复习结果',options:[{id:'A',text:'先核对现场安全条件'},{id:'B',text:'直接开始'}],attempt:{correct:false,option_ids:['B'],submitted_at:`${date}T09:00:00+08:00`},practice:[{correct:true,option_ids:['A'],submitted_at:`${date}T10:00:00+08:00`}]}]});
      if (url.pathname.endsWith('/completion')) {
        if (unavailable) { res.statusCode = 503; return res.end(JSON.stringify({ ok: false, error: '合成测试：读取暂不可用' })); }
        const items = rows.filter(row => (!url.searchParams.get('status') || row.status === url.searchParams.get('status'))
          && (!url.searchParams.get('shift') || row.tasks.some(task => task.shift === url.searchParams.get('shift')))
          && (!url.searchParams.get('q') || row.person.name.includes(url.searchParams.get('q'))));
        return ok({ items, total: items.length, summary: Object.fromEntries(states.map(state => [state, items.filter(r => r.status === state).length])) });
      }
      errors.push(`Unexpected endpoint ${req.method} ${url.pathname}`); res.statusCode = 500;
      return res.end(JSON.stringify({ ok: false, error: 'unexpected fixture request' }));
    });
  } }],
});
await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const url = `${base}/__completion_check?tab=completion&view=overview`;
let browser, page;
try {
  browser = await chromium.launch({ headless: true });
  page = await browser.newPage();
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
  for (const width of [1440, 402, 440]) {
    await page.setViewportSize({ width, height: width === 402 ? 874 : width === 440 ? 956 : 900 });
    await page.goto(url);
    await page.locator('.completion tbody tr').nth(6).waitFor();
    assert.equal(await page.locator('.completion tbody tr').count(), 7);
    await page.screenshot({ path: path.join(output, `${v2 ? 'v2-' : ''}${width}.png`), fullPage: true });
    const overflow = await page.evaluate(() => [...document.querySelectorAll('body *')].filter(element => {
      const rect = element.getBoundingClientRect();
      return rect.right > innerWidth + 1 && !element.closest('.completion-table');
    }).map(element => `${element.tagName}.${element.className}`).slice(0, 15));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, `overflow ${width}: ${overflow}`);
  }
  await page.locator('.completion-filters').getByRole('combobox').first().click();
  if (v2) await page.locator('.n-base-select-option').filter({ hasText: /^已完成$/ }).click();
  else await page.getByRole('option', { name: '已完成', exact: true }).click();
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.waitForFunction(() => document.querySelectorAll('.completion tbody tr').length === 1);
  assert.match(await page.locator('.completion tbody').innerText(), /已完成/);
  assert.match(await page.locator('.completion tbody').innerText(), /发送失败/);
  assert.equal(requests.at(-1).query.status, '已完成');
  unavailable = true;
  await page.getByRole('button', { name: '刷新完成情况', exact: true }).click();
  await page.locator('.completion-error').waitFor();
  assert.equal(await page.locator('.completion tbody tr').count(),1,'A transient refresh must preserve the last confirmed rows');
  unavailable = false;
  await page.getByRole('button', { name: '刷新完成情况', exact: true }).click();
  await page.locator('.completion tbody tr').waitFor();
  await page.locator('.completion-records summary').click();
  await page.getByRole('button',{name:'练习与复习记录',exact:true}).click();
  await page.getByRole('heading',{name:/核对首次作答及复习结果/}).waitFor();
  await page.locator('.attempt-records summary').click();
  assert.match(await page.locator('.attempt-records').innerText(),/首次作答/);
  assert.match(await page.locator('.attempt-records').innerText(),/回答正确/);
  await page.getByRole('button',{name:'返回每日完成',exact:true}).click();
  await page.locator('.completion').waitFor();
  assert.equal(requests.every(request => request.method === 'GET'), true);
  assert.deepEqual(errors, []);
  console.log(`Completion browser checks passed (${v2 ? 'v2' : 'legacy'}): MacBook/iPhone17Pro/Max, filters, retry, paper drilldown and GET only. Preview: ${url}`);
} catch(error) {
  if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(await page.locator('body').innerText(),requests.slice(-5));}
  throw error;
} finally {
  await browser?.close();
  if (!process.argv.includes('--serve')) await server.close();
}
