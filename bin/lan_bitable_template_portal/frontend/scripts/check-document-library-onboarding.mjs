import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';
import {documentFixture, output, startDocumentLibraryPreview} from './document-library-preview.mjs';

const server = await startDocumentLibraryPreview();
const {fixture, origin} = server, browser = await chromium.launch({headless: true});
const results = [], base = 'https://vnet.feishu.cn/base/baseSyntheticFixture?table=tblSyntheticFixture';
let page;
async function run(version, viewport) {
  Object.assign(fixture, documentFixture());
  const context = await browser.newContext({viewport, reducedMotion: 'reduce'}), errors = [];
  await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  page = await context.newPage(); page.setDefaultTimeout(10000); page.on('pageerror', error => errors.push(error.message));
  const name = `onboarding-${version}-${viewport.width}`;
  const settings = page.locator('.n-drawer-content').filter({has: page.locator('.document-settings')});
  const submit = () => settings.getByRole('button', {name: /^(接入数据表|保存配置)$/});
  const link = () => settings.getByLabel('飞书数据表链接', {exact: true});
  async function choose(label, option, multiple = false) {
    await settings.getByLabel(label, {exact: true}).click();
    await page.locator('.n-base-select-option:visible').filter({hasText: option}).click();
    if (multiple) await page.keyboard.press('Escape');
    await page.locator('.n-base-select-menu:visible').first().waitFor({state: 'hidden'});
  }
  async function read(url) {
    if (url !== undefined) await link().fill(url);
    await settings.getByRole('button', {name: '读取数据表', exact: true}).click();
  }
  async function ownership(label) {
    const button = settings.locator('.n-radio-button').filter({hasText: label});
    await button.click();
    assert(await button.locator('input[type="radio"]').isChecked(), 'visible radio button changes ownership');
  }
  async function cancel() {
    await settings.getByRole('button', {name: '取消', exact: true}).click();
    await page.getByRole('button', {name: '放弃修改', exact: true}).click();
    await settings.waitFor({state: 'hidden'});
  }
  async function capture(suffix) {
    await submit().scrollIntoViewIfNeeded();
    const box = await page.locator('.n-drawer').boundingBox();
    assert(box && box.x >= -1 && box.x + box.width <= viewport.width + 1 && box.y >= -1 && box.y + box.height <= viewport.height + 1);
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1));
    await page.screenshot({path: path.join(output, `${name}-${suffix}.png`), animations: 'disabled'});
  }
  try {
    await page.goto(origin + '/?version=' + version);
    await page.getByRole('button', {name: '数据表配置', exact: true}).click();
    assert(!await submit().isDisabled(), 'existing configuration remains editable without preview');
    await settings.getByLabel('数据表显示名称', {exact: true}).fill('保留原配置的资料表');
    const original = structuredClone(fixture.tables[0]);
    await submit().click(); await settings.waitFor({state: 'hidden'});
    const edited = fixture.writes.at(-1).body;
    for (const key of ['id', 'base_token', 'table_id', 'read_scopes', 'write_scopes', 'display_fields', 'attachment_fields', 'scope_field', 'fixed_scopes', 'enabled']) assert.deepEqual(edited[key], original[key], 'existing ' + key);
    assert.equal(edited.expected_revision, original.revision);
    assert.equal(fixture.calls.filter(call => call.path === '/preview').length, 0);

    await page.getByRole('button', {name: '接入数据表', exact: true}).click();
    assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).count(), 0);
    assert.equal(await settings.getByLabel('配置标识', {exact: true}).count(), 0);
    assert.equal(await settings.getByLabel('多维表 Base Token', {exact: true}).count(), 0);
    assert.equal(await settings.getByLabel('数据表 ID', {exact: true}).count(), 0);
    assert(await submit().isDisabled()); await capture('link');
    if (viewport.width === 1440) {
      for (const [url, flag, message] of [
        ['https://example.invalid/base/no', '', '请输入有效的飞书'],
        [base, 'previewDenied', '没有读取此数据表的权限'],
        ['https://vnet.feishu.cn/wiki/notBitable', '', '此 Wiki 节点不是多维表'],
        [base, 'previewMalformed', '数据表读取结果不完整'],
        [base, 'previewEmptyFields', '数据表读取结果不完整'],
        [base, 'previewUnknownTable', '数据表读取结果不完整'],
        ['https://vnet.feishu.cn/wiki/wikiFixture', 'previewNoTables', '数据表读取结果不完整'],
      ]) {
        if (flag) fixture.flags[flag] = true;
        await read(url); await settings.getByRole('alert').filter({hasText: message}).waitFor();
        assert(await submit().isDisabled());
        assert.equal(fixture.writes.length, 1, 'read failures cannot create configuration writes');
        if (flag) fixture.flags[flag] = false;
      }
    }
    await read('https://vnet.feishu.cn/wiki/wikiFixture');
    await settings.getByLabel('选择数据表', {exact: true}).waitFor();
    assert(await submit().isDisabled());
    assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).count(), 0);
    await choose('选择数据表', '测试归档资料');
    await settings.getByLabel('数据表显示名称', {exact: true}).waitFor();
    assert.deepEqual(await settings.locator('.document-field-order li>span').allTextContents(), ['资料名称', '资料分类', '所属楼栋', '文件页数'], 'new source defaults to the first four non-attachment fields');
    assert(await submit().isDisabled(), 'resolved table still requires explicit ownership');
    await choose('选择数据表', '第二张资料表');
    await settings.getByText('已读取 1 个字段', {exact: true}).waitFor();
    let tableRead = fixture.calls.filter(call => call.path === '/preview').at(-1).body;
    assert.equal(new URL(tableRead.url).searchParams.get('table'), 'tblSecondFixture');
    assert.equal(tableRead.table_id, 'tblSecondFixture', 'table picker updates both URL and explicit selection');
    assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).inputValue(), '第二张资料表');
    assert.deepEqual(await settings.locator('.document-field-order li>span').allTextContents(), ['另一表标题'], 'different source receives only its own display defaults');
    await choose('选择数据表', '测试归档资料');
    await settings.getByText('已读取 12 个字段', {exact: true}).waitFor();
    tableRead = fixture.calls.filter(call => call.path === '/preview').at(-1).body;
    assert.equal(new URL(tableRead.url).searchParams.get('table'), 'tblSyntheticFixture');
    assert.equal(tableRead.table_id, 'tblSyntheticFixture');
    await ownership('固定楼栋');
    assert(await submit().isDisabled(), 'fixed ownership has no first-building default');
    await choose('固定归属楼栋', 'A楼', true);
    await ownership('按字段');
    assert(await submit().isDisabled());
    await choose('楼栋字段', '所属楼栋');
    await choose('附件字段', '资料附件', true);
    for (const field of ['资料名称', '资料分类', '所属楼栋', '文件页数']) await choose('列表显示字段', field, true);
    await read(); await settings.getByText('已读取 12 个字段', {exact: true}).waitFor();
    assert.equal(await settings.locator('.document-field-order').count(), 0, 'same-source reread preserves an explicitly cleared display selection');
    await choose('列表显示字段', '资料名称', true);
    await choose('列表显示字段', '资料分类', true);
    await settings.getByRole('button', {name: '上移资料分类', exact: true}).click();
    await settings.getByLabel('数据表显示名称', {exact: true}).fill('手动配置的显示名');
    await read(); await settings.getByText('已读取 12 个字段', {exact: true}).waitFor();
    assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).inputValue(), '手动配置的显示名');
    assert.deepEqual(await settings.locator('.document-field-order li>span').allTextContents(), ['资料分类', '资料名称']);
    await capture('configured');
    await submit().click(); await settings.waitFor({state: 'hidden'});
    const created = fixture.writes.at(-1).body;
    assert.equal(fixture.writes.length, 2); assert.equal(fixture.tables.length, 3);
    assert.equal(created.id, 'fixtureTable'); assert.equal(created.base_token, 'baseSyntheticFixture'); assert.equal(created.table_id, 'tblSyntheticFixture');
    assert.equal(created.url, base); assert.equal(created.scope_field, 'fldScope'); assert.deepEqual(created.fixed_scopes, []);
    assert.deepEqual(created.read_scopes, ['A', 'E']); assert.deepEqual(created.write_scopes, []);
    assert.deepEqual(created.attachment_fields, ['fldFiles']); assert.deepEqual(created.display_fields, ['fldKind', 'fldTitle']);
    assert(!('expected_revision' in created));

    if (viewport.width === 1440) {
      await page.getByRole('button', {name: '数据表配置', exact: true}).click();
      await link().fill('https://vnet.feishu.cn/wiki/changed');
      assert(await submit().isDisabled(), 'editing the source invalidates existing fields');
      assert.equal(await settings.getByLabel('楼栋字段', {exact: true}).count(), 0);
      await cancel();
      await page.getByRole('button', {name: '数据表配置', exact: true}).click();
      assert.equal(await link().inputValue(), base, 'cancel never changes original source');
      assert(!await submit().isDisabled());
      await settings.getByRole('button', {name: '取消', exact: true}).click(); await settings.waitFor({state: 'hidden'});

      await page.getByRole('button', {name: '接入数据表', exact: true}).click();
      fixture.previewDelay = 450;
      const pending = page.waitForRequest(request => request.url().endsWith('/preview'));
      await read(base); await pending;
      fixture.previewDelay = 0;
      await read('https://vnet.feishu.cn/base/baseSyntheticFixture?table=tblSecondFixture');
      await settings.getByText('已读取 1 个字段', {exact: true}).waitFor();
      await page.waitForTimeout(550);
      assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).inputValue(), '第二张资料表');
      assert.deepEqual(await settings.locator('.document-field-order li>span').allTextContents(), ['另一表标题']);
      assert(await submit().isDisabled(), 'new source cannot reuse previous ownership');
      await cancel();
      await page.getByRole('button', {name: '接入数据表', exact: true}).click();
      assert.equal(await link().inputValue(), ''); assert(await submit().isDisabled());
      fixture.previewDelay = 450;
      const closingRead = page.waitForRequest(request => request.url().endsWith('/preview'));
      await read(base); await closingRead; await cancel();
      await page.getByRole('button', {name: '接入数据表', exact: true}).click();
      await page.waitForTimeout(550);
      assert.equal(await link().inputValue(), ''); assert.equal(await settings.getByLabel('数据表显示名称', {exact: true}).count(), 0);
      fixture.previewDelay = 0;
      await read('https://vnet.feishu.cn/base/baseSyntheticFixture?table=tblSecondFixture');
      await ownership('固定楼栋'); await choose('固定归属楼栋', 'E楼', true);
      fixture.flags.configLoseReceipt = true;
      const before = fixture.writes.length;
      await submit().click(); await settings.getByText('配置结果尚未确认。', {exact: false}).waitFor();
      assert(await submit().isDisabled()); assert(await link().isDisabled());
      await page.waitForTimeout(250); assert.equal(fixture.writes.length, before + 1, 'uncertain save never automatically repeats');
      await settings.getByRole('button', {name: '关闭并核验', exact: true}).click(); await settings.waitFor({state: 'hidden'});
      assert.equal(fixture.tables.filter(table => table.id === 'secondFixture').length, 1);
    }
    assert.deepEqual(errors, []);
    results.push({name, status: 'PASS', writes: fixture.writes.length, previews: fixture.calls.filter(call => call.path === '/preview').length});
    console.log('PASS ' + name);
  } catch (error) {
    results.push({name, status: 'FAIL', error: error.stack, errors});
    await writeFile(path.join(output, name + '-geometry.json'), JSON.stringify(await settings.evaluate(el => {
      const rect = node => {const box = node.getBoundingClientRect(); return {x: box.x, y: box.y, width: box.width, height: box.height};};
      const radios = [...el.querySelectorAll('.n-radio-button')].map(node => {
        const input = node.querySelector('input'), box = input?.getBoundingClientRect();
        return {text: node.textContent, display: getComputedStyle(node).display, rect: rect(node), input: input && rect(input),
          hit: box && document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2)?.outerHTML.slice(0, 500)};
      });
      return {radios, menus: [...document.querySelectorAll('.n-base-select-menu')].map(node => ({rect: rect(node), display: getComputedStyle(node).display}))};
    }), null, 2));
    await page.screenshot({path: path.join(output, name + '-failure.png'), animations: 'disabled'}); throw error;
  } finally { await context.close(); }
}
try {
  for (const version of ['legacy', 'v2']) for (const viewport of [{width: 1440, height: 900}, {width: 402, height: 874}, {width: 440, height: 956}]) await run(version, viewport);
  console.log('PASS: actual dist Base/Wiki onboarding, explicit ownership, scoped defaults, old configuration, stale/cancelled reads and uncertain save; mocked only.');
} finally {
  await writeFile(path.join(output, 'onboarding-results.json'), JSON.stringify(results, null, 2));
  await browser.close(); await server.close();
}
