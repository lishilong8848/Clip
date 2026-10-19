import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

// Run against responsive-audit-server.mjs only; never proxy production traffic.
const origin='http://127.0.0.1:14536';
const folder=fileURLToPath(new URL('../../../../output/responsive-audit/final-browser-checks/',import.meta.url));
await mkdir(folder,{recursive:true});
const browser=await chromium.launch({headless:true}),results=[];
try{
  for(const [width,height] of [[402,874],[440,956],[1440,900]]){
    const page=await browser.newPage({viewport:{width,height}});
    await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
    for(const [name,query] of [['daily','scope=A&__page=%2Fdaily-tasks'],['notice-power','scope=A&work_type=power&__page=%2Fworkbench-lite']]){
      await page.goto(origin+'/assets/workspace-v2/index.html?'+query);
      if(name==='daily'){
        await page.locator('.daily-header__actions button').last().waitFor();
        const buttons=await page.locator('.daily-header__actions button').evaluateAll(items=>items.map(b=>({text:b.innerText,rect:b.getBoundingClientRect().toJSON(),whiteSpace:getComputedStyle(b).whiteSpace,scrollWidth:b.scrollWidth,clientWidth:b.clientWidth})));
        assert(buttons.length>=2);
        for(const b of buttons){assert(b.rect.x>=0&&b.rect.right<=width+1);assert(b.scrollWidth<=b.clientWidth+2);assert.equal(b.whiteSpace,'nowrap');}
        if(width===1440)assert(Math.abs(buttons[0].rect.top-buttons.at(-1).rect.top)<2,'Desktop actions should share a row');
        results.push({name,width,buttons});
      }else{
        await page.locator('.v2-notice-types button[aria-pressed=true]').waitFor();
        await page.waitForTimeout(500);
        const geometry=await page.locator('.v2-notice-types').evaluate(nav=>({container:nav.getBoundingClientRect().toJSON(),button:nav.querySelector('[aria-pressed=true]').getBoundingClientRect().toJSON(),label:nav.querySelector('[aria-pressed=true]').innerText}));
        assert(geometry.label.includes('上下电'));
        assert(geometry.button.left>=geometry.container.left-1&&geometry.button.right<=geometry.container.right+1,JSON.stringify(geometry));
        results.push({name,width,geometry});
      }
      await page.screenshot({path:folder+'/'+name+'-'+width+'.png'});
    }
    await page.close();
  }
  await writeFile(folder+'/checks.json',JSON.stringify(results,null,2));
  console.log('PASS 6 built-page boundary checks at 402, 440, 1440; fixture data only.');
}finally{await browser.close();}
