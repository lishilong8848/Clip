import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {workspaceExtensionFixture} from './workspace-extension-fixtures.mjs';
import {accountFixture} from './responsive-audit-account-fixtures.mjs';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';
import {collabFixture} from './responsive-audit-collab-fixtures.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/workspace-extensions');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
const versions=['legacy','v2'],viewports=[{width:402,height:874},{width:440,height:956},{width:1440,height:900}];
try{
  for(const version of versions)for(const viewport of viewports){
    const context=await browser.newContext({viewport,serviceWorkers:'block'});
    await context.route('**/*',async route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin!==origin)return route.abort();
      if(!url.pathname.startsWith('/api/'))return route.continue();
      const method=request.method(),body=request.headers()['content-type']?.includes('json')?request.postDataJSON():{};
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend'}});
      for(const fixture of [workspaceExtensionFixture,accountFixture,coreFixture,collabFixture]){
        const result=await fixture({url,method,body});
        if(result)return route.fulfill({status:result.status||200,...(result.json?{json:result.json}:{contentType:result.contentType,body:result.body})});
      }
      if(url.pathname==='/api/scope-overview')return route.fulfill({json:{ok:true,data:{scopes:{}}}});
      return route.fulfill({status:503,json:{ok:false,error:'隔离预览未配置 '+url.pathname}});
    });
    const page=await context.newPage();page.setDefaultTimeout(20000);
    page.on('pageerror',error=>errors.push(error.message));
    const visit=route=>{
      const url=new URL(route,origin);
      if(version==='v2'){url.searchParams.set('__page',url.pathname);url.pathname='/assets/workspace-v2/index.html';}
      return page.goto(url.href);
    };
    const check=async name=>{
      await page.screenshot({path:path.join(output,`${version}-${viewport.width}-${name}.png`)});
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),`${version} ${viewport.width} ${name}: horizontal overflow`);
    };
    await visit('/admin/settings?tab=development');
    await page.locator('.release-entry').first().waitFor();
    await check('journal');
    await page.locator('.release-entry').first().click();
    await page.locator('.release-detail .release-issue').waitFor();
    assert.match(await page.locator('.release-detail').innerText(),/解决方法/);
    await page.getByText('关联提交',{exact:true}).click();
    await page.locator('.release-commits code').waitFor();
    assert.equal(await page.locator('.release-commits code').innerText(),'01234567');
    await check('journal-detail');
    const box=await page.locator('.release-detail').boundingBox();
    assert(box.x>=0&&box.x+box.width<=viewport.width+1&&box.y>=0&&box.y+box.height<=viewport.height+1,'release detail fits viewport');
    await page.keyboard.press('Escape');
    await page.getByPlaceholder('搜索版本、问题或解决方法').fill('每日答题');
    await page.waitForFunction(()=>document.querySelectorAll('.release-entry').length===11);
    assert((await page.locator('.release-entry').allTextContents()).every(text=>text.includes('每日答题')));
    await visit(version==='v2'?'/?view=functions':'/?entry=tools');
    const entry=name=>version==='v2'?page.locator('.v2-feature-entry').filter({hasText:name}):page.getByRole('button',{name:'选择'+name,exact:true});
    await entry('南通文件').waitFor();
    await entry('共享知识库').waitFor();
    await check('independent-entries');
    await entry('南通文件').click();
    await page.locator('.document-library').waitFor();
    assert.equal(new URL(page.url()).searchParams.get('__page')||new URL(page.url()).pathname,'/document-library');
    assert.equal(await page.getByRole('button',{name:'返回共享知识库',exact:true}).count(),0);
    await check('documents');
    if(version==='legacy'){
      await page.getByRole('button',{name:'返回',exact:true}).click();
      await entry('共享知识库').waitFor();
    }else await visit('/?view=functions');
    await entry('共享知识库').click();
    await page.locator('.kb-page').waitFor();
    assert.equal(new URL(page.url()).searchParams.get('__page')||new URL(page.url()).pathname,'/knowledge-base');
    assert.equal(await page.getByRole('button',{name:'南通文件',exact:true}).count(),0);
    await check('knowledge');
    await context.close();
  }
  assert.deepEqual(errors,[]);
  console.log('PASS: both versions, three viewports, admin journal detail/search and independent document/knowledge navigation.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
