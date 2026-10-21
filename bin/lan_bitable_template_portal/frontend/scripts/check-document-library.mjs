import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { documentFixture, output, startDocumentLibraryPreview } from './document-library-preview.mjs';

const server = await startDocumentLibraryPreview();
const browser = await chromium.launch({ headless: true });
const { fixture, origin } = server;
const errors = [], checks = [];
const viewports = [
  { name: 'MacBook', width: 1440, height: 900 },
  { name: 'iPhone17ProMax', width: 440, height: 956 },
  { name: 'iPhone17Pro', width: 402, height: 874 },
  { name: 'small-phone', width: 360, height: 844 },
];
let page;
function reset() { Object.assign(fixture, documentFixture()); }
async function open(version = 'v2', dark = false) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin + '/?version=' + version + (dark ? '&dark=1' : ''));
  await page.getByRole('heading', { name: '南通文件', exact: true }).waitFor();
  return context;
}
const drawer = () => page.locator('.n-drawer-content').filter({ has: page.locator('.document-detail') });
async function stableLayout() {
  await page.evaluate(async () => {
    await Promise.all(document.getAnimations().filter(animation => animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {})));
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  });
}
async function firstDetail() {
  await page.getByRole('button', { name: '查看资料：A楼高压配电操作规程', exact: true }).click();
  await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
}
async function editTitle(title, reason = '核对原始资料后更新') {
  await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
  await drawer().getByLabel('资料名称', { exact: true }).fill(title);
  await drawer().getByLabel('变更原因', { exact: true }).fill(reason);
}
try {
  reset(); fixture.flags.listFailure = true;
  let context = await open();
  await page.getByText('资料列表暂时读取失败', { exact: false }).waitFor();
  assert.equal(await page.getByText('没有符合条件的资料', { exact: true }).count(), 0, 'Read failure must not masquerade as an empty library');
  fixture.flags.listFailure = false; await page.getByRole('button', { name: '重试读取列表', exact: true }).click();
  await firstDetail();
  await drawer().getByText('系统原始值', { exact: true }).waitFor();
  await editTitle('A楼更新后的配电操作规程');
  const readOnly = drawer().locator('.document-detail-field').filter({ hasText: '系统汇总' });
  assert.equal(await readOnly.getByRole('textbox').count(), 0, 'Formula fields remain readonly');
  assert.equal(await drawer().locator('.document-detail-field').filter({ hasText: '原始系统字段' }).getByRole('textbox').count(), 0, 'Unknown field types remain readonly');
  fixture.flags.conflict = true;
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('资料已被更新，尚未保存的输入已保留。', { exact: false }).waitFor();
  assert.equal(await drawer().getByLabel('资料名称', { exact: true }).inputValue(), 'A楼更新后的配电操作规程');
  await drawer().getByRole('button', { name: '读取最新版本，保留输入', exact: true }).click();
  await drawer().getByLabel('文件页数', { exact: true }).waitFor();
  assert.equal(await drawer().getByLabel('资料名称', { exact: true }).inputValue(), 'A楼更新后的配电操作规程');
  assert.equal(await drawer().getByLabel('文件页数', { exact: true }).inputValue(), '18', 'Unchanged fields rebase to the server revision');
  assert.equal(await drawer().getByLabel('变更原因', { exact: true }).inputValue(), '核对原始资料后更新');
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('资料保存成功。', { exact: true }).waitFor();
  assert.equal(fixture.applied, 1); assert.deepEqual(fixture.writes.at(-1).body.fields, { fldTitle: 'A楼更新后的配电操作规程' });
  assert.notEqual(fixture.writes[0].body.operation_id, fixture.writes[1].body.operation_id, 'A resolved 409 starts a fresh explicit intent');
  checks.push('list failure/retry; typed readonly fields; conflict preserves changes and reason; delta-only revisioned save');

  await editTitle('A楼回执丢失核验资料'); fixture.flags.loseReceipt = true;
  const before = fixture.applied;
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('操作回执尚未确认，输入及原操作编号已保留。', { exact: false }).waitFor();
  const original = structuredClone(fixture.writes.at(-1).body);
  await page.waitForTimeout(300); assert.equal(fixture.applied, before + 1, 'No automatic write retry');
  assert.equal(await drawer().getByLabel('资料名称', { exact: true }).inputValue(), 'A楼回执丢失核验资料');
  await drawer().getByRole('button', { name: '按原操作重试保存', exact: true }).click();
  await drawer().getByText('资料保存成功。', { exact: true }).waitFor();
  assert.deepEqual(fixture.writes.at(-1).body, original); assert.equal(fixture.applied, before + 1, 'A lost receipt never creates a second business write');
  checks.push('lost save receipt locks input; only explicit retry uses the original operation and payload');

  await editTitle('A楼回执丢失核验资料');
  await drawer().getByLabel('资料负责人',{exact:true}).click();
  await page.locator('.n-base-select-option').filter({hasText:'值班资料员'}).click();
  await drawer().getByRole('button',{name:'保存资料',exact:true}).click();
  await drawer().getByText('资料保存成功。',{exact:true}).waitFor();
  assert.deepEqual(fixture.writes.at(-1).body.fields,{fldOwner:[{id:'ou_fixturePeer',name:'值班资料员'}]},'single-person fields replace rather than append the current person');
  checks.push('single-person selector uses current directory options and persists one selected identity');

  fixture.flags.indexFailure = true;
  await drawer().getByLabel('选择资料附件', { exact: true }).setInputFiles({ name: '规程补充.txt', mimeType: 'text/plain', buffer: Buffer.from('synthetic document attachment') });
  await drawer().getByLabel('附件变更原因', { exact: true }).fill('补充验收附件');
  await drawer().getByRole('button', { name: '上传附件', exact: true }).click();
  await drawer().getByText('资料已保存，知识索引待重试。', { exact: true }).waitFor();
  assert.equal(fixture.writes.at(-1).body.field_id, 'fldFiles');
  assert.equal(fixture.writes.at(-1).body.file.name, '规程补充.txt');
  assert.equal(fixture.records[0].attachments.length, 2);
  fixture.flags.loseReceipt = true;
  await drawer().getByLabel('选择资料附件', { exact: true }).setInputFiles({ name: '回执核验附件.txt', mimeType: 'text/plain', buffer: Buffer.from('synthetic receipt recovery') });
  await drawer().getByLabel('附件变更原因', { exact: true }).fill('核验同一附件操作');
  const appliedBeforeUpload = fixture.applied;
  await drawer().getByRole('button', { name: '上传附件', exact: true }).click();
  await drawer().getByText('操作回执尚未确认，输入及原操作编号已保留。', { exact: false }).waitFor();
  const originalUpload = structuredClone(fixture.writes.at(-1).body);
  await drawer().getByRole('button', { name: '按原操作重试上传', exact: true }).click();
  await drawer().getByRole('link', { name: '打开附件：回执核验附件.txt', exact: true }).waitFor();
  assert.deepEqual(fixture.writes.at(-1).body, originalUpload);
  assert.equal(fixture.applied, appliedBeforeUpload + 1, 'Lost upload receipt never repeats an attachment write');
  fixture.flags.versionsFailure = true;
  await drawer().getByText('附件版本', { exact: true }).click();
  await drawer().getByText('版本记录暂时读取失败', { exact: false }).waitFor();
  fixture.flags.versionsFailure = false; await drawer().getByRole('button', { name: '重试读取版本', exact: true }).click();
  await drawer().getByRole('link', { name: '打开历史附件：规程补充.txt', exact: true }).waitFor();
  const href = await drawer().getByRole('link', { name: '打开历史附件：规程补充.txt', exact: true }).getAttribute('href');
  assert(href.startsWith('/api/document-library/tables/electrical/records/recDoc1/attachments/'));
  assert(href.endsWith('/file?version=2'));
  assert(!/app_secret|access_token|tenant_access_token/.test(href));
  assert.match(await drawer().locator('.document-versions').innerText(),/资料管理员/);
  checks.push('multipart upload carries field/revision/reason/operation; save success survives index failure; protected version history retries');
  await drawer().getByRole('button', { name: '关闭', exact: true }).click();

  await page.getByLabel('搜索资料', { exact: true }).fill('找不到的资料');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.getByText('没有符合条件的资料', { exact: true }).waitFor();
  await page.getByLabel('搜索资料', { exact: true }).fill(''); await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.getByRole('button', { name: '下一页资料', exact: true }).click();
  await page.getByRole('button', { name: '查看资料：E楼运行资料', exact: true }).waitFor();
  await page.getByRole('button', { name: '上一页资料', exact: true }).click();
  await page.getByRole('button', { name: '查看资料：A楼回执丢失核验资料', exact: true }).waitFor();
  checks.push('search empty state and cursor navigation use actual server results');

  await page.getByLabel('文件数据表', { exact: true }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '暖通技术资料' }).click();
  await page.getByRole('button', { name: '查看资料：A楼回执丢失核验资料', exact: true }).click();
  await drawer().getByText('资料附件', { exact: true }).waitFor();
  assert.equal(await drawer().getByRole('button', { name: '编辑资料', exact: true }).count(), 0);
  assert.equal(await drawer().getByRole('button', { name: '上传附件', exact: true }).count(), 0);
  await drawer().getByRole('button', { name: '关闭', exact: true }).click();
  assert.equal(await page.getByRole('button', { name: '新建资料', exact: true }).count(), 0);
  checks.push('readonly table capabilities never expose record or attachment writes');
  assert.equal(fixture.authReads, 1, 'Actor context is read once per mounted page, never passed as props');
  await context.close();

  reset(); context = await open();
  await page.getByRole('button', { name: '新建资料', exact: true }).click();
  await drawer().getByLabel('资料名称', { exact: true }).fill('新增资料完整回执');
  await drawer().getByLabel('变更原因', { exact: true }).fill('归档原始资料');
  await drawer().getByRole('button', { name: '取消', exact: true }).click();
  await page.getByRole('button', { name: '继续填写', exact: true }).click();
  assert.equal(await drawer().getByLabel('资料名称', { exact: true }).inputValue(), '新增资料完整回执');
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('资料保存成功。', { exact: true }).waitFor();
  assert(fixture.records.some(record => record.fields.fldTitle === '新增资料完整回执')); assert.equal(fixture.applied, 1);
  await drawer().getByRole('button', { name: '关闭', exact: true }).click();
  await page.getByRole('button', { name: '接入数据表', exact: true }).click();
  const settings = page.locator('.n-drawer-content').filter({ has: page.locator('.document-settings') });
  await settings.getByLabel('配置标识', { exact: true }).fill('fixtureTable');
  await settings.getByLabel('数据表显示名称', { exact: true }).fill('测试归档资料');
  await settings.getByLabel('多维表 Base Token', { exact: true }).fill('baseSyntheticFixture');
  await settings.getByLabel('数据表 ID', { exact: true }).fill('tblSyntheticFixture');
  await settings.getByRole('button', { name: '接入数据表', exact: true }).click();
  await page.getByText('数据表已接入，请核对显示字段及编辑范围。', { exact: true }).waitFor();
  assert.equal(fixture.tables.length, 3);
  await page.getByRole('button', { name: '数据表配置', exact: true }).click();
  await settings.getByLabel('列表显示字段', { exact: true }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '资料名称' }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '资料分类' }).click();
  await page.keyboard.press('Escape');
  await settings.getByRole('button', { name: '上移资料分类', exact: true }).click();
  await settings.getByRole('button', { name: '保存配置', exact: true }).click();
  await page.getByText('数据表配置已保存。', { exact: true }).waitFor();
  assert.deepEqual(fixture.tables.at(-1).display_fields, ['fldKind', 'fldTitle']);
  checks.push('new record receipt; discard confirmation retains input; admin table connection and explicit display ordering');
  await context.close();

  reset(); fixture.flags.tablesFailure = true; context = await open();
  await page.getByText('数据表列表暂时不可用', { exact: false }).waitFor();
  assert.equal(await page.getByText('尚无可访问的数据表', { exact: true }).count(), 0);
  fixture.flags.tablesFailure = false; fixture.flags.schemaFailure = true;
  await page.getByRole('button', { name: '重新读取数据表', exact: true }).click();
  await page.getByText('字段读取暂时失败', { exact: false }).waitFor();
  await page.getByRole('button', { name: '数据表配置', exact: true }).click();
  await page.locator('.document-settings').waitFor();
  await page.locator('.n-drawer-content').filter({ has: page.locator('.document-settings') }).getByRole('button', { name: '取消', exact: true }).click();
  fixture.flags.schemaFailure = false; await page.getByRole('button', { name: '重试读取列表', exact: true }).click();
  fixture.flags.detailFailure = true;
  await page.getByRole('button', { name: '查看资料：A楼高压配电操作规程', exact: true }).click();
  await drawer().getByText('资料详情暂时读取失败', { exact: false }).waitFor();
  fixture.flags.detailFailure = false; await drawer().getByRole('button', { name: '重试读取详情', exact: true }).click();
  await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
  checks.push('table/schema/detail failures retain explicit error and retry; schema outage does not lock administrators out of configuration');
  await context.close();

  reset(); fixture.canConfigure = false; context = await open();
  await page.getByRole('button', { name: '新建资料', exact: true }).waitFor();
  assert.equal(await page.getByRole('button', { name: '接入数据表', exact: true }).count(), 0);
  assert.equal(await page.getByRole('button', { name: '数据表配置', exact: true }).count(), 0);
  await firstDetail();
  assert.equal(await drawer().getByRole('button', { name: '编辑资料', exact: true }).count(), 1, 'Business editing follows record capabilities, not a manager-only gate');
  checks.push('configuration stays admin-only while normal record access follows the table ACL');
  await context.close();

  reset(); fixture.personMultiple=true; context = await open(); await firstDetail();
  await editTitle('原字段类型完整保存');
  await drawer().getByLabel('文件页数', { exact: true }).fill('21');
  await drawer().getByLabel('资料分类', { exact: true }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '图纸资料' }).click();
  await drawer().getByLabel('专业标签', { exact: true }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '暖通' }).click(); await page.keyboard.press('Escape');
  await drawer().getByLabel('资料负责人', { exact: true }).click();
  await page.locator('.n-base-select-option').filter({ hasText: '值班资料员' }).click(); await page.keyboard.press('Escape');
  await drawer().getByLabel('生效日期', { exact: true }).fill('2026-10-20');
  await drawer().getByLabel('生效日期', { exact: true }).press('Enter');
  await drawer().getByRole('switch', { name: '当前有效', exact: true }).click();
  await drawer().getByLabel('关联链接链接', { exact: true }).fill('javascript:invalid');
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('请输入有效的 HTTP 或 HTTPS 链接', { exact: true }).waitFor();
  assert.equal(fixture.applied, 0, 'Invalid URL must not issue a write');
  await drawer().getByLabel('关联链接链接', { exact: true }).fill('https://example.invalid/updated');
  await drawer().getByLabel('关联链接链接名称', { exact: true }).fill('核验后的关联资料');
  await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
  await drawer().getByText('资料保存成功。', { exact: true }).waitFor();
  const typed = fixture.writes.at(-1).body.fields;
  assert.equal(typed.fldCount, 21); assert.equal(typed.fldKind, '图纸资料');
  assert.deepEqual(typed.fldTags, ['电气', '暖通']);
  assert.deepEqual(typed.fldOwner.map(person => person.id), ['ou_fixtureDoc', 'ou_fixturePeer']);
  assert.equal(typeof typed.fldDate, 'number'); assert.equal(typed.fldValid, false);
  assert.deepEqual(typed.fldLink, { link: 'https://example.invalid/updated', text: '核验后的关联资料' });
  checks.push('all writable field controls emit original typed numbers/selections/dates/people/URLs/booleans; invalid links never write');
  await context.close();

  for (const [version, dark] of [['legacy', false], ['v2', false], ['v2', true]]) {
    reset(); context = await open(version, dark);
    await page.getByRole('button', { name: '查看资料：A楼高压配电操作规程', exact: true }).waitFor();
    for (const { name, width, height } of viewports) {
      await page.setViewportSize({ width, height });
      await stableLayout();
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, `${version}/${dark}/${name} list overflow`);
      await page.screenshot({ path: path.join(output, `${version}${dark ? '-dark' : ''}-list-${width}x${height}.png`), fullPage: true });
    }
    await page.locator('.document-mobile-title').first().click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    for (const { name, width, height } of viewports) {
      await page.setViewportSize({ width, height });
      await stableLayout();
      const frame = await page.locator('.n-drawer').boundingBox();
      assert(frame && frame.x >= -1 && frame.x + frame.width <= width + 1 && frame.y >= -1 && frame.y + frame.height <= height + 1, `${version}/${dark}/${name} drawer stays within viewport`);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, `${version}/${dark}/${name} drawer overflow`);
      await page.screenshot({ path: path.join(output, `${version}${dark ? '-dark' : ''}-detail-${width}x${height}.png`), fullPage: true });
    }
    await drawer().getByRole('button', { name: '关闭', exact: true }).click();
    await page.getByRole('button', { name: '返回共享知识库', exact: true }).click();
    assert.equal(new URL(page.url()).pathname, '/knowledge-base', 'Return uses the shared navigation adapter');
    await context.close();
  }
  checks.push('legacy/v2/light/dark; 1440x900/768x1024/440x956/402x874/360x844 list and drawer; navigation adapter');
  assert.deepEqual(errors, [], 'No browser runtime errors');
  await writeFile(path.join(output, 'acceptance.json'), JSON.stringify({ checks, errors, viewports, syntheticOnly: true }, null, 2));
  console.log(`Document library: ${checks.length} workflow groups passed; screenshots in ${output}`);
} catch (error) {
  if (page && !page.isClosed()) await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }).catch(() => {});
  throw error;
} finally { await browser.close(); await server.close(); }
