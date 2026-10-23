import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {auditUrl} from './responsive-audit-pages.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const css = await readFile(path.join(root, 'src/workspace-v2/surfaces.css'), 'utf8');
assert.match(await readFile(path.join(root, 'src/workspace-v2/main.ts'), 'utf8'), /import '\.\/surfaces\.css'/);
assert.doesNotMatch(await readFile(path.join(root, 'src/main.ts'), 'utf8'), /surfaces\.css/);
const browser = await chromium.launch({headless: true});
const paint = locator => locator.evaluate(el => {
  const s = getComputedStyle(el), r = el.getBoundingClientRect();
  return {shadow:s.boxShadow, background:s.backgroundColor, border:s.borderColor, outline:s.outlineStyle, transition:s.transitionDuration, width:r.width, height:r.height};
});
try {
  const page = await browser.newPage({reducedMotion:'reduce'});
  await page.setContent(`<style>
    html{--v2-border:#dbe3df;--v2-ink:#24312e;--v2-blue:#087b69;--v2-bg:#eef2ef;--cf-surface:#fff}
    body{margin:16px}.record-row{padding:12px;background:white;border-bottom:1px solid #ddd}
    .normal{color:#148058;background:#e5f7ed}.abnormal{color:#b91c1c;background:#fee2e2}
    .grid-stack-item{margin:12px}.v2-widget{border:1px solid #ddd}.v2-widget-head{padding:8px}
    button{padding:12px;border:1px solid #ddd;background:white}td{padding:8px}
  </style><main class="v2-main"><div class="grid-stack-item"><section class="v2-widget">
    <header class="v2-widget-head">Repair</header><div class="v2-repair-workspace">
      <article class="record-row" tabindex="0">Record <button>Open</button></article>
    </div><button class="v2-check-choice" aria-pressed="false">Shift</button>
    <button class="v2-sop-item" disabled>Disabled</button>
    <button class="normal">Normal</button><button class="abnormal">Abnormal</button>
    <table class="document-table"><tbody><tr><td class="document-check"><span class="n-checkbox">Select</span></td><td>Document</td></tr></tbody></table>
  </section></div></main>`);
  const row = page.locator('.record-row'), widget = page.locator('.v2-widget');
  const legacy = await paint(row);
  await page.addStyleTag({content:css});
  assert.deepEqual(await paint(row), legacy, 'V2 styles must not change legacy pages');
  await page.locator('html').evaluate(el => el.classList.add('workspace-v2'));
  const normal = await paint(row);
  assert.notEqual(normal.shadow, 'none');
  assert.notEqual((await paint(widget)).shadow, 'none');
  assert.equal(normal.width, legacy.width);
  assert.equal(normal.height, legacy.height, 'borders/shadows must not change record size');
  await row.hover();
  const hover = await paint(row);
  assert.notEqual(hover.shadow, normal.shadow);
  assert.equal(await row.getAttribute('class'), 'record-row', 'hover is not selection');
  await row.evaluate(el => el.classList.add('active'));
  const selected = await paint(row);
  assert.notEqual(selected.background, normal.background);
  assert.match(selected.shadow, /3px 0px/, 'selection has a persistent leading mark');
  await page.mouse.move(0,0);
  assert.deepEqual(await paint(row), selected, 'selection must persist after the pointer leaves');
  await row.evaluate(el => el.classList.remove('active'));
  await row.focus();
  assert.equal((await paint(row)).outline, 'solid');
  assert.equal((await paint(row)).background, normal.background, 'focus does not select the record');
  const choice = page.locator('.v2-check-choice');
  const unselectedChoice = await paint(choice);
  await choice.evaluate(el => el.setAttribute('aria-pressed','true'));
  assert.notEqual((await paint(choice)).background, unselectedChoice.background);
  const disabled = page.locator('.v2-sop-item');
  const disabledBefore = await paint(disabled);
  await disabled.hover();
  assert.deepEqual(await paint(disabled), disabledBefore);
  assert.equal(await page.locator('.normal').evaluate(el => getComputedStyle(el).color), 'rgb(20, 128, 88)');
  assert.equal(await page.locator('.abnormal').evaluate(el => getComputedStyle(el).color), 'rgb(185, 28, 28)');
  const cell = page.locator('td').last(), cellBefore = await paint(cell);
  await page.locator('.n-checkbox').evaluate(el => el.classList.add('n-checkbox--checked'));
  assert.notEqual((await paint(cell)).background, cellBefore.background);
  await page.locator('html').evaluate(el => {
    el.dataset.v2Theme='dark';
    el.style.cssText='--v2-border:#414952;--v2-ink:#edf0f4;--v2-blue:#83baff;--cf-surface:#202429';
  });
  assert.notEqual((await paint(choice)).background, (await paint(row)).background);
  assert.equal((await paint(row)).transition, '0s');
  await page.emulateMedia({media:'print'});
  assert.equal((await paint(widget)).shadow, 'none');
  assert.equal((await paint(choice)).shadow, 'none');
  await page.close();

  const origin = process.env.WORKSPACE_SURFACE_PREVIEW_URL;
  if (origin) {
    const output = path.resolve(root, '../../../output/surface-review/screenshots');
    await mkdir(output,{recursive:true});
    for (const [name,width,height] of [['desktop',1512,982],['phone-pro',402,874],['phone-max',440,956]]) {
      const context = await browser.newContext({viewport:{width,height},reducedMotion:'reduce'});
      await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
      const live = await context.newPage();
      const errors=[];
      live.on('pageerror', error=>errors.push(error.message));
      await live.goto(origin+'/assets/workspace-v2/index.html?__page=/repair-management&scope=A');
      const record=live.locator('.record-row').first();
      await record.waitFor();
      const before=await paint(record);
      assert.notEqual(before.shadow,'none','built application includes record shadows');
      await live.screenshot({path:path.join(output,name+'-records.png'),fullPage:true});
      await record.click();
      await live.locator('.record-row.active').waitFor({state:'attached'});
      await live.locator('.v2-repair-detail').waitFor();
      await live.mouse.move(0,0);
      assert.match((await paint(live.locator('.record-row.active'))).shadow,/3px 0px/,'built record uses selection highlight');
      assert(await live.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'no horizontal overflow');
      await live.screenshot({path:path.join(output,name+'.png'),fullPage:true});
      await live.getByRole('button',{name:'导航配色',exact:true}).click();
      await live.getByRole('switch',{name:'夜间模式',exact:true}).click();
      await live.getByRole('button',{name:'导航配色',exact:true}).click();
      await live.mouse.move(0,0);
      await live.locator('.v2-theme-picker').waitFor({state:'hidden'});
      assert.equal(await live.locator('html').getAttribute('data-v2-theme'),'dark');
      await live.screenshot({path:path.join(output,name+'-dark.png'),fullPage:true});
      assert.deepEqual(errors,[]);
      await context.close();
    }
    for (const width of [1512,402]) {
      const context=await browser.newContext({viewport:{width,height:width===402?874:982},reducedMotion:'reduce'});
      await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
      const live=await context.newPage();
      for (const [name,route,selector] of [
        ['notices','/workbench-lite?work_type=maintenance&scope=A','.v2-notice-entry'],
        ['checks','/?entry=pre_shift&scope=A','.v2-check-choice'],
        ['knowledge','/knowledge-base','.kb-row'],
      ]) {
        await live.goto(auditUrl(origin,'v2',route));
        const item=live.locator(selector).first();
        await item.waitFor();
        assert.notEqual((await paint(item)).shadow,'none',name+' has a visible record edge');
        assert(await live.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),name+' stays within viewport');
        await live.screenshot({path:path.join(output,name+'-'+width+'.png'),fullPage:true});
      }
      await context.close();
    }
  }
  console.log('PASS workspace surfaces: legacy isolation, shadows, selection, focus, disabled, status colors, dark mode, reduced motion and print'+(origin?', built desktop/phone checks':''));
} finally {
  await browser.close();
}
