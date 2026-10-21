import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/risk-import');
await mkdir(output, {recursive: true});
const fields = [
  {key: 'number', field_id: 'number', field_name: '任务编号', type: 1, role: 'number', is_primary: true},
  {key: 'result', field_id: 'result', field_name: '排查结果', type: 1, role: 'input', property: {}},
];
const imported = {name: '导入检查', fields, items: [{title: '检查设备', values: {result: '保持预填'}}],
  warnings: [], source: {kind: 'file', filename: 'checks.csv'}};
const entry = '/src/__risk_import_test.ts';
const harness = {
  name: 'risk-import-offline-harness',
  resolveId(id) { if (id === entry) return path.join(root, entry.slice(1)); },
  load(id) {
    if (id.replaceAll('\\', '/') !== path.join(root, entry.slice(1)).replaceAll('\\', '/')) return;
    return `import {createApp,h} from 'vue';
import {NConfigProvider,zhCN,dateZhCN} from 'naive-ui';
import RiskInspectionPage from '/src/components/RiskInspectionPage.vue';
import RiskInspectionTemplateLibrary from '/src/components/RiskInspectionTemplateLibrary.vue';
const direct=new URLSearchParams(location.search).has('direct');
createApp({render:()=>h(NConfigProvider,{locale:zhCN,dateLocale:dateZhCN},()=>direct?h(RiskInspectionTemplateLibrary,{kind:'base'}):h(RiskInspectionPage))}).mount('#app');`;
  },
  configureServer(server) {
    server.middlewares.use(async (req, res, next) => {
      if (!req.url?.split('?')[0].endsWith('/__risk-import.html')) return next();
      res.setHeader('Content-Type', 'text/html');
      res.end(await server.transformIndexHtml(req.url, `<!doctype html><html lang="zh-CN"><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Risk import fixture</title></head><body><div id="app"></div><script type="module" src="${entry}"></script></body></html>`));
    });
  },
};
const reports = [];
const browser = await chromium.launch({headless: true, args: ['--host-resolver-rules=MAP risk-import.test 127.0.0.1', '--no-proxy-server']});
try {
  for (const version of ['legacy', 'v2']) {
    const server = await createServer({root, configFile: version === 'v2' ? path.join(root, 'vite.workspace-v2.config.ts') : false,
      cacheDir: path.join(output, 'vite-cache-' + version), publicDir: false, base: '/',
      plugins: [harness, ...version === 'legacy' ? [vue()] : []],
      server: {host: '127.0.0.1', port: 0, hmr: false, proxy: {}, allowedHosts: ['risk-import.test']},
      optimizeDeps: {noDiscovery: true, include: ['vue', 'naive-ui', 'lucide-vue-next']}, logLevel: 'error'});
    try {
      await server.listen();
      for (const scenario of [
        {level: 'L0', secure: true}, {level: 'L0', secure: false},
        {level: 'L1', secure: false}, {level: 'L3', secure: false, recovery: true}, {level: 'L4', secure: false},
      ]) {
        const origin = `http://${scenario.secure ? '127.0.0.1' : 'risk-import.test'}:${server.httpServer.address().port}`;
        const viewport = scenario.level === 'L1' ? {width: 402, height: 874} : scenario.level === 'L3' ? {width: 440, height: 956} : {width: 1440, height: 900};
        const context = await browser.newContext({viewport, reducedMotion: 'reduce', serviceWorkers: 'block'});
        const page = await context.newPage(), errors = [], unexpected = [], writes = [];
        page.setDefaultTimeout(10000);
        page.on('pageerror', e => errors.push(e.message));
        const allowed = scenario.level !== 'L4';
        let denyPreview = !allowed, failSave = scenario.recovery, denySave = false, templateError = false;
        await context.route('**/*', async route => {
          const req = route.request(), url = new URL(req.url());
          if (url.origin !== origin) { unexpected.push(req.url()); return route.abort(); }
          if (!url.pathname.startsWith('/api/')) return route.continue();
          const p = url.pathname, ok = data => route.fulfill({json: {ok: true, data}});
          const denied = () => route.fulfill({status: 403, json: {ok: false, error: '仅管理员或楼栋主管可导入基础模板。'}});
          if (req.method() !== 'GET') writes.push({path: p, body: p.endsWith('import-file') ? null : req.postDataJSON()});
          if (p === '/api/risk-inspections' && req.method() === 'GET') return ok({items: [], total: 0, page: 1,
            is_manager: allowed, can_edit_template: ['L0', 'L1'].includes(scenario.level)});
          if (p === '/api/risk-inspections/template' && req.method() === 'GET') {
            if (templateError) return route.fulfill({status: 503, json: {ok: false, error: '字段库暂不可用，请重试'}});
            return ok({fields, version: 'library-v1'});
          }
          if (p === '/api/risk-inspections/base-templates/import-file' || p === '/api/risk-inspections/base-templates/import-bitable') {
            assert.equal(req.method(), 'POST');
            return denyPreview ? denied() : ok(imported);
          }
          if (p === '/api/risk-inspections/base-templates') {
            if (!allowed) return denied();
            if (req.method() === 'GET') return ok({items: [], total: 0, page: 1});
            assert.equal(req.method(), 'POST');
            if (denySave) return denied();
            if (failSave) { failSave = false; return route.fulfill({status: 503, json: {ok: false, error: '模板暂未保存，请重试'}}); }
            const data = req.postDataJSON();
            assert.match(data.id, /^[a-f0-9]{32}$/);
            assert.deepEqual(data.fields, imported.fields);
            assert.deepEqual(data.items, imported.items);
            return ok({...data, version: 'saved-v1', revision: 1, creator: scenario.level, created: Date.now() / 1000});
          }
          unexpected.push(req.method() + ' ' + p);
          return route.abort();
        });
        const modal = page.locator('.risk-history-import-modal');
        const apply = modal.getByRole('button', {name: '确认带入新草稿', exact: true});
        async function preview(kind = 'file') {
          await page.getByRole('button', {name: kind === 'file' ? '导入表格' : '导入多维表', exact: true}).click();
          if (kind === 'file') await modal.getByLabel('选择导入表格').setInputFiles({name: 'checks.csv', mimeType: 'text/csv', buffer: Buffer.from('事项,排查结果\n检查设备,保持预填\n')});
          else await modal.getByRole('textbox', {name: '多维表链接'}).fill('https://fixture.invalid/base/example?table=fixture');
          await modal.getByRole('button', {name: '预览导入', exact: true}).click();
        }
        try {
          await page.goto(origin + '/__risk-import.html');
          assert.deepEqual(await page.evaluate(() => ({secure: isSecureContext, uuid: typeof crypto.randomUUID, random: typeof crypto.getRandomValues})),
            {secure: scenario.secure, uuid: scenario.secure ? 'function' : 'undefined', random: 'function'});
          if (!allowed) {
            await page.getByText('暂无分配的风险排查', {exact: true}).first().waitFor();
            assert.equal(await page.getByRole('button', {name: '基础模板', exact: true}).count(), 0);
            await page.goto(origin + '/__risk-import.html?direct=1');
            await preview();
            await modal.getByRole('alert').filter({hasText: '仅管理员或楼栋主管'}).waitFor();
            assert(await apply.isDisabled());
            assert(await modal.getByRole('button', {name: '预览导入', exact: true}).isEnabled());
          } else {
            await page.getByRole('button', {name: '基础模板', exact: true}).click();
            if (scenario.recovery) {
              denyPreview = true;
              await preview();
              await modal.getByRole('alert').filter({hasText: '仅管理员或楼栋主管'}).waitFor();
              assert(await apply.isDisabled());
              denyPreview = false;
              await modal.getByRole('button', {name: '预览导入', exact: true}).click();
            } else await preview();
            await page.waitForFunction(() => [...document.querySelectorAll('button')].some(b => b.textContent.includes('确认带入新草稿') && !b.disabled));
            if (scenario.recovery) {
              await page.evaluate(() => {
                const original = crypto.getRandomValues.bind(crypto);
                crypto.getRandomValues = () => { crypto.getRandomValues = original; throw new Error('隔离测试：草稿标识生成失败'); };
              });
              await apply.click();
              await modal.getByRole('alert').filter({hasText: '草稿标识生成失败'}).waitFor();
              assert(await apply.isEnabled());
              assert.equal(await page.locator('.risk-field-picker').count(), 0);
              templateError = true;
            }
            await apply.click();
            await page.locator('.risk-field-picker').waitFor({timeout: 5000}).catch(e => { throw new Error(e.message + '\nBrowser errors: ' + errors.join('; ')); });
            await modal.waitFor({state: 'hidden'});
            assert.equal(await page.getByRole('textbox', {name: '基础模板名称', exact: true}).inputValue(), imported.name);
            await page.getByText('导入内容已带入新草稿，请核对后生成基础模板。', {exact: true}).waitFor();
            assert.equal(writes.filter(w => !w.path.includes('/import-')).length, 0, 'Draft creation must not save or publish');
            if (scenario.recovery) {
              await page.getByText('字段库暂不可用，请重试', {exact: true}).waitFor();
              templateError = false;
              await page.getByRole('button', {name: '重新读取字段库', exact: true}).click();
            }
            await page.getByRole('button', {name: '生成基础模板', exact: true}).click();
            const confirm = page.getByRole('button', {name: '确认保存', exact: true});
            await confirm.click();
            if (scenario.recovery) {
              await page.getByRole('alert').filter({hasText: '模板暂未保存，请重试'}).waitFor();
              assert(await confirm.isEnabled());
              denySave = true;
              await confirm.click();
              await page.getByRole('alert').filter({hasText: '仅管理员或楼栋主管'}).waitFor();
              assert(await confirm.isEnabled());
              denySave = false;
              await confirm.click();
            }
            await confirm.waitFor({state: 'hidden'});
            const saves = writes.filter(w => w.path.endsWith('/base-templates'));
            assert(saves.length);
            for (const save of saves) assert.deepEqual(save.body, saves[0].body, 'Save retries preserve draft ID and data');
            if (scenario.recovery) {
              await page.getByRole('textbox', {name: '基础模板名称', exact: true}).fill('保留当前草稿');
              await preview('bitable');
              await apply.click();
              await page.getByRole('button', {name: '继续编辑', exact: true}).click();
              assert.equal(await page.getByRole('textbox', {name: '基础模板名称', exact: true}).inputValue(), '保留当前草稿');
              await apply.click();
              await page.evaluate(() => {
                const original = window.structuredClone;
                window.structuredClone = value => {
                  if (Array.isArray(value) && value[0]?.title === '检查设备') {
                    window.structuredClone = original;
                    throw new Error('隔离测试：草稿内容转换失败');
                  }
                  return original(value);
                };
              });
              await page.getByRole('button', {name: '确认带入', exact: true}).click();
              await modal.getByRole('alert').filter({hasText: '草稿内容转换失败'}).waitFor();
              assert.equal(await page.getByRole('textbox', {name: '基础模板名称', exact: true}).inputValue(), '保留当前草稿');
              assert.equal(await page.getByRole('button', {name: '保存新版本', exact: true}).count(), 1, 'Failed conversion preserves the original selected template');
              assert(await apply.isEnabled());
              await apply.click();
              await page.getByRole('button', {name: '确认带入', exact: true}).click();
              await modal.waitFor({state: 'hidden'});
              assert.equal(await page.getByRole('textbox', {name: '基础模板名称', exact: true}).inputValue(), imported.name);
              await page.getByRole('button', {name: '生成基础模板', exact: true}).click();
              await confirm.click();
              await confirm.waitFor({state: 'hidden'});
              assert.notEqual(writes.at(-1).body.id, saves[0].body.id, 'A new import gets its own ID');
            }
          }
          assert.deepEqual(errors, [], 'No swallowed or uncaught browser errors');
          assert.deepEqual(unexpected, [], 'All APIs are isolated; no external traffic');
          assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'Import stays within the viewport');
          const tag = `${version}-${scenario.level}-${scenario.secure ? 'localhost' : 'http'}`;
          await page.screenshot({path: path.join(output, tag + '.png'), fullPage: true, animations: 'disabled'});
          reports.push({tag, passed: true, writes: writes.map(w => w.path)});
          console.log('PASS ' + tag);
        } finally { await context.close(); }
      }
    } finally { await server.close(); }
  }
} finally { await browser.close(); }
await writeFile(path.join(output, 'report.json'), JSON.stringify(reports, null, 2));
console.log(`Risk import checks passed: ${reports.length} isolated role/origin/version cases.`);
