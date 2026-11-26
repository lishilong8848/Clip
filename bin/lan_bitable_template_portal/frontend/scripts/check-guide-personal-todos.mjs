import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

// Run against the isolated responsive preview. Both accounts share one browser profile.
const origin = new URL(process.argv[2]);
const output = process.argv[3];
if(output)await mkdir(output,{recursive:true});
assert.equal(origin.hostname, '127.0.0.1');
assert.equal((await (await fetch(new URL('/api/health', origin))).json()).instance_id, 'responsive-isolated-preview');
const browser = await chromium.launch();
try {
  for (const width of [1440, 390, 820]) {
    const context = await browser.newContext({viewport:{width,height:width===390?844:900},hasTouch:width<=820});
    let owner = 'guide-A';
    await context.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.origin !== origin.origin) return route.abort();
      if (url.pathname === '/api/auth/status') return route.fulfill({json:{ok:true,data:{
        logged_in:true,user:{open_id:owner,name:owner,role:'building',level:'L4',category:'other',modules:['life_guide'],home_scope:'A',scopes:['A']},
        default_scope:'A',scope_options:[{value:'A',label:'A楼'}]
      }}});
      return route.continue();
    });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(new URL('/assets/workspace-v2/index.html?__page=/life-guide',origin).href);
    const frame = page.frameLocator('iframe[title="参考人生指南"]');
    const count = frame.locator('#rail-tcount');
    const entries = frame.locator('.entry');
    const originalTitle = await entries.first().locator('h3').innerText();
    const bottomGap=await page.locator('iframe[title="参考人生指南"]').evaluate(el=>{
      const bottom=document.querySelector('.v2-mobile-nav')?.getBoundingClientRect().top ?? innerHeight;
      return bottom-el.getBoundingClientRect().bottom;
    });
    assert(bottomGap>=-1 && bottomGap<=40,'reader should fill the usable height, gap='+bottomGap);
    const expectCount = value => count.filter({hasText:new RegExp('^'+value+'$')}).waitFor();
    await expectCount(0);
    await entries.first().getByRole('button',{name:'加入待办'}).click();
    await expectCount(1);
    await page.reload();
    await expectCount(1);
    await frame.locator('#rail-all').click();
    await frame.getByRole('dialog').getByText(originalTitle,{exact:true}).waitFor();
    if(output)await page.screenshot({path:path.join(output,`guide-${width}.png`)});
    await page.keyboard.press('Escape');
    await frame.getByRole('dialog').waitFor({state:'hidden'});
    const toggle=frame.locator('#rail-toggle');
    await toggle.click();
    assert.equal(await toggle.getAttribute('aria-expanded'),await frame.locator('#todo-list').isVisible()?'true':'false');
    // Unattributed old storage must survive, but must never be imported into a new account.
    await page.evaluate(()=>localStorage.setItem('clipflow-life-guide:htlb_todos_v1','["2-1"]'));
    owner = 'guide-B';
    await page.reload();
    await expectCount(0);
    await entries.nth(1).getByRole('button',{name:'加入待办'}).click();
    await expectCount(1);
    owner = 'guide-A';
    await page.reload();
    await frame.locator('#rail-all').click();
    const dialog=frame.getByRole('dialog');
    await dialog.getByText(originalTitle,{exact:true}).waitFor();
    await dialog.getByRole('button',{name:'删除',exact:true}).click();
    await page.keyboard.press('Escape');
    await expectCount(0);
    owner = 'guide-B';
    await page.reload();
    await expectCount(1);
    assert.equal(await page.evaluate(()=>localStorage.getItem('clipflow-life-guide:htlb_todos_v1')),'["2-1"]');
    assert.equal(await entries.first().evaluate(el=>el.ownerDocument.documentElement.scrollWidth>el.ownerDocument.documentElement.clientWidth+1),false);
    await page.goto(new URL('/life-guide?frontend=legacy',origin).href);
    await page.locator('.life-guide-page:not(.serenity-guide) iframe').waitFor();
    await count.filter({hasText:/^1$/}).waitFor({state:'attached'});
    assert.deepEqual(errors,[]);
    await context.close();
    console.log('PASS guide account isolation, persistence, dialog and removal at',width);
  }
} finally { await browser.close(); }
