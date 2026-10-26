import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/notice-photo-paste');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,build:{outDir:'dist'},logLevel:'error',preview:{host:'127.0.0.1',port:19120}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],uploads=[],unexpected=[];
let page,failNext=false,holdUpload,releaseUpload;
try{
 for(const [width,height,dark=false] of [[1440,900],[402,874],[440,956],[360,640],[1440,900,true],[402,874,true]]){
  const context=await browser.newContext({viewport:{width,height},reducedMotion:'reduce',permissions:['clipboard-read','clipboard-write']});
  await context.addInitScript(dark=>localStorage.setItem('clipflow:workspace-night-mode:ou_responsive_fixture',dark?'dark':'light'),dark);
  await context.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url());
   if(url.origin!==origin)return route.abort();
   if(!url.pathname.startsWith('/api/'))return route.continue();
   if(url.pathname==='/api/notice-attachments'){
    uploads.push({kind:url.searchParams.get('kind'),name:url.searchParams.get('file_name'),scope:url.searchParams.get('scope'),identity:url.searchParams.get('identity')});
    if(holdUpload)await holdUpload;
    if(failNext){failNext=false;return route.fulfill({status:400,json:{ok:false,error:'隔离测试：上传失败'}});}
    return route.fulfill({json:{ok:true,data:{upload_id:'test-'+uploads.length,file_name:url.searchParams.get('file_name'),preview_url:'/api/notice-images/'+'a'.repeat(32)}}});
   }
   if(req.method()!=='GET'&&!url.pathname.startsWith('/api/workspace-layouts/')){unexpected.push(url.pathname);return route.abort();}
   const result=await coreFixture({url,method:req.method(),body:req.postData()});
   return route.fulfill(result||{json:{ok:true,data:{items:[],records:[],available:false}}});
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  let chooserCount=0;page.on('filechooser',()=>chooserCount++);
  await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=A&work_type=change');
  if(width<=760)await page.getByRole('tab',{name:/进行中/}).click();
  await page.locator('[data-list=ongoing]').getByRole('button',{name:'更新',exact:true}).first().click();
  const site=page.getByRole('group',{name:'现场照片上传区',exact:true}),ali=page.getByRole('group',{name:'阿里确认截图上传区',exact:true});
  await site.waitFor();await ali.waitFor();
  const topClose=page.locator('.v2-notice-handling>.n-card-header .n-card-header__close');
  const top=await topClose.boundingBox();
  assert(top.width>=44&&top.height>=44&&top.x>=0&&top.x+top.width<=width&&top.y+top.height<=height,'top close is a visible 44px target');
  assert(await topClose.evaluate(el=>{const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}),'top close is not covered');
  const close=page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true});
  const closeStyle=await close.evaluate(el=>getComputedStyle(el).backgroundColor);
  assert.notEqual(closeStyle,await page.getByRole('button',{name:'预览并确认更新',exact:true}).evaluate(el=>getComputedStyle(el).backgroundColor),'close differs from submit');
  assert.notEqual(closeStyle,'rgba(0, 0, 0, 0)');
  for(const area of [site,ali]){
   await area.scrollIntoViewIfNeeded();const box=await area.boundingBox(),picker=await area.locator('.v2-notice-upload').boundingBox();
   assert(picker.width<=130,'file picker is compact');
   await area.click({position:{x:box.width-4,y:box.height-4}});
   assert(await area.evaluate(el=>document.activeElement===el),'blank area receives paste focus without opening picker');
   assert.equal(chooserCount,0);
   await page.evaluate(async()=>{const canvas=document.createElement('canvas');canvas.width=4;canvas.height=4;const ctx=canvas.getContext('2d');ctx.fillStyle='#2186d8';ctx.fillRect(0,0,4,4);const blob=await new Promise(resolve=>canvas.toBlob(resolve));await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]);});
   const before=uploads.length;
   await page.keyboard.press('Control+V');
   await area.getByText('本次新增 · 已就绪',{exact:true}).waitFor();
   assert.equal(uploads.length,before+1,'paste uploads exactly once');
   assert.equal(uploads.at(-1).kind,area===ali?'ali':'site','image stays in the focused category');
   assert.equal(uploads.at(-1).scope,'A');assert(uploads.at(-1).identity);
  }
  const beforeLimit=uploads.length;
  await ali.focus();await page.keyboard.press('Control+V');
  await ali.getByRole('alert').filter({hasText:'请先移除本次截图'}).waitFor();
  assert.equal(uploads.length,beforeLimit,'single screenshot limit remains enforced');
  await ali.getByRole('button',{name:'移除本次图片',exact:true}).click();
  const nonImage=await ali.evaluate(el=>{const data=new DataTransfer();data.setData('text/plain','normal text');const event=new ClipboardEvent('paste',{bubbles:true,cancelable:true,clipboardData:data});el.dispatchEvent(event);return event.defaultPrevented;});
  assert.equal(nonImage,false,'text paste is not converted to an upload');assert.equal(uploads.length,beforeLimit);
  await site.scrollIntoViewIfNeeded();
  const picker=page.waitForEvent('filechooser');await site.locator('.v2-notice-upload').click();
  await (await picker).setFiles({name:'selected.png',mimeType:'image/png',buffer:Buffer.from('isolated')});
  await site.getByText('selected.png',{exact:true}).waitFor();assert.equal(chooserCount,1,'only the picker opens file selection');
  failNext=true;
  await ali.focus();await page.keyboard.press('Control+V');await ali.getByRole('button',{name:'重试此张',exact:true}).waitFor();
  const failedCount=uploads.length;
  await ali.getByRole('button',{name:'重试此张',exact:true}).click();await ali.getByText('本次新增 · 已就绪',{exact:true}).waitFor();
  assert.equal(uploads.length,failedCount+1);assert.equal(uploads.at(-1).kind,'ali');
  await ali.getByRole('button',{name:'移除本次图片',exact:true}).click();
  holdUpload=new Promise(resolve=>{releaseUpload=resolve;});
  await ali.focus();await page.keyboard.press('Control+V');await ali.locator('.v2-photo-queue').waitFor();
  assert(await close.isDisabled(),'close stays locked while upload is active');
  assert.equal(await topClose.count(),0,'top close respects the same upload lock');
  const busyCount=uploads.length;
  await site.evaluate(el=>{const data=new DataTransfer();data.items.add(new File(['test'],'busy.png',{type:'image/png'}));el.dispatchEvent(new ClipboardEvent('paste',{bubbles:true,cancelable:true,clipboardData:data}));});
  assert.equal(uploads.length,busyCount,'locked area cannot enqueue another upload');
  releaseUpload();holdUpload=null;await ali.getByText('本次新增 · 已就绪',{exact:true}).waitFor();
  await ali.scrollIntoViewIfNeeded();
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  for(const area of [site,ali])assert(await area.evaluate(el=>el.scrollWidth<=el.clientWidth+1),'uploaded images and picker fit their area');
  await page.screenshot({path:path.join(output,`photos-${width}${dark?'-dark':''}.png`)});
  await page.locator('.v2-notice-handling>.n-card-content').evaluate(el=>el.scrollTo({top:0}));
  await page.screenshot({path:path.join(output,`close-${width}${dark?'-dark':''}.png`)});
  const content=page.getByRole('textbox',{name:'内容',exact:true});
  const draftText='未提交内容 '+width+(dark?' dark':' light');
  await content.fill(draftText);
  const receiptIds=()=>page.evaluate(()=>Object.keys(sessionStorage).filter(key=>key.startsWith('v2-notice:')).map(key=>({key,...JSON.parse(sessionStorage.getItem(key))})).find(value=>value.snapshot.values.content.startsWith('未提交内容 ')));
  await topClose.click();await page.getByRole('button',{name:'继续填写',exact:true}).click();
  await page.locator('.n-dialog').filter({hasText:'当前填写尚未保存'}).waitFor({state:'detached'});
  assert.equal(await content.inputValue(),draftText,'cancel close keeps manual edits');
  assert.equal(await site.locator('figure').count(),2);assert.equal(await ali.locator('figure').count(),1);
  await close.click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
  await page.locator('.v2-notice-handling').waitFor({state:'detached'});
  const stored=await receiptIds();assert(stored);
  const retained={site:stored.snapshot.photos.map(photo=>photo.upload_id),ali:stored.snapshot.aliPhotos.map(photo=>photo.upload_id)};
  assert.equal(retained.site.length,2);assert.equal(retained.ali.length,1);
  assert(!retained.site.includes(retained.ali[0]),'the draft retains independent category receipts');
  const beforeReopen=uploads.length;
  await page.locator('[data-list=ongoing]').getByRole('button',{name:'更新',exact:true}).nth(1).click();
  await content.waitFor();assert.notEqual(await content.inputValue(),draftText,'another notice does not restore this draft');
  assert.equal(await site.getByText('本次新增 · 已就绪',{exact:true}).count(),0);
  assert.equal(await ali.getByText('本次新增 · 已就绪',{exact:true}).count(),0);
  await close.click();await page.locator('.v2-notice-handling').waitFor({state:'detached'});
  await page.reload();
  if(width<=760)await page.getByRole('tab',{name:/进行中/}).click();
  await page.locator('[data-list=ongoing]').getByRole('button',{name:'更新',exact:true}).first().click();
  await content.waitFor();assert.equal(await content.inputValue(),draftText,'reopening after reload restores unsubmitted text');
  assert.equal(await site.getByText('本次新增 · 已就绪',{exact:true}).count(),2);
  assert.equal(await ali.getByText('本次新增 · 已就绪',{exact:true}).count(),1);
  assert.equal(uploads.length,beforeReopen,'restoring receipts never uploads files again');
  if(width===1440&&!dark){
   await page.evaluate(()=>{window.restoreNoticeStorage=Storage.prototype.setItem;window.failNoticeStorage=function(key,value){if(this===sessionStorage&&key.startsWith('v2-notice:'))throw new DOMException('quota test','QuotaExceededError');return window.restoreNoticeStorage.call(this,key,value);};Storage.prototype.setItem=window.failNoticeStorage;});
   await content.fill(draftText+' storage failure');
   for(const button of [topClose,close]){
    await button.click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
    await page.getByRole('button',{name:'继续填写',exact:true}).waitFor();
    assert(await page.locator('.v2-notice-handling').isVisible(),'failed storage must not close the editor');
    await page.getByRole('button',{name:'继续填写',exact:true}).click();
    await page.locator('.n-dialog').filter({hasText:'当前填写尚未保存'}).waitFor({state:'detached'});
    assert.equal(await content.inputValue(),draftText+' storage failure','failed storage keeps current input, not the older saved copy');
   }
   const currentUrl=page.url();
   await page.evaluate(()=>{const target=new URL(location.href);target.searchParams.set('__page','/');history.pushState({},'',target);window.dispatchEvent(new PopStateEvent('popstate'));});
   await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
   await page.getByRole('button',{name:'继续填写',exact:true}).waitFor();
   assert.equal(page.url(),currentUrl,'storage failure blocks leaving the open draft via browser navigation');
   await page.getByRole('button',{name:'继续填写',exact:true}).click();
   await page.locator('.n-dialog').filter({hasText:'当前填写尚未保存'}).waitFor({state:'detached'});
   assert.equal(await content.inputValue(),draftText+' storage failure');
   await page.evaluate(()=>{Storage.prototype.setItem=window.restoreNoticeStorage;});
   await close.click();await page.getByRole('button',{name:'保留副本并离开',exact:true}).click();
   await page.locator('.v2-notice-handling').waitFor({state:'detached'});
   assert.equal((await receiptIds()).snapshot.values.content,draftText+' storage failure','storage recovery saves the latest text');
   await page.evaluate(()=>{Storage.prototype.setItem=window.failNoticeStorage;});
   await page.getByRole('navigation',{name:'通告类型',exact:true}).getByRole('button',{name:'维保通告',exact:true}).click();
   await page.waitForURL(url=>url.searchParams.get('work_type')==='maintenance');
   await page.locator('[data-list=ongoing]').getByRole('button',{name:'更新',exact:true}).first().click();
   await content.waitFor();await close.click();
   await page.locator('.v2-notice-handling').waitFor({state:'detached'});
   await page.evaluate(()=>{Storage.prototype.setItem=window.restoreNoticeStorage;delete window.restoreNoticeStorage;delete window.failNoticeStorage;});
  }
  await context.close();
 }
 assert.deepEqual(errors,[]);assert.deepEqual(unexpected,[]);
 console.log('PASS: real Ctrl+V, category/notice isolation, no duplicate upload on restore, upload retry/lock, draft reload and storage-failure close guards; desktop/402/440/360 including dark mode (6 cases). No real business writes.');
}catch(error){releaseUpload?.();if(page&&!page.isClosed())await page.screenshot({path:path.join(output,'failure.png')});throw error;}
finally{await browser.close();await server.close();}
