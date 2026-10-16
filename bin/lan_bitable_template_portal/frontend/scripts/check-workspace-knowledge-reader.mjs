import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
const root=fileURLToPath(new URL('..',import.meta.url)),out=path.resolve(root,'../../../output/playwright/workspace-reader');await mkdir(out,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin=`http://127.0.0.1:${server.httpServer.address().port}`,browser=await chromium.launch({headless:true});
let fail=true,delay=250;const errors=[];
const doc={id:'guide',name:'公司操作规程.txt',active_version:2,version:4,status:'ready',size:1600,chunks:21,can_edit:false};
try{
 const page=await browser.newPage({viewport:{width:1440,height:1000},reducedMotion:'reduce'});page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();assert.equal(req.method(),'GET');
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'reader-fixture',name:'测试人员',role:'admin',level:'L0',modules:['knowledge']},scope_options:[{value:'E',label:'E楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'reader-fixture'}});
  if(url.pathname==='/api/assistant/knowledge')return ok({items:[doc],total:1,page:1,page_size:20,is_admin:false});
  if(url.pathname==='/api/assistant/knowledge/documents/guide'){
   await new Promise(resolve=>setTimeout(resolve,delay));if(fail)return route.fulfill({status:503,json:{ok:false,error:'原文暂不可用'}});
   assert.equal(url.searchParams.get('version'),'2');const page=Number(url.searchParams.get('page')||1);
   return ok({document:{...doc,view_version:2},sections:Array.from({length:page===1?20:1},(_,i)=>({ordinal:(page-1)*20+i,location:'操作说明',text:'先检查现场安全条件，再核对操作对象。'.repeat(12)})),total:21,page,page_size:20});
  }
  return ok({items:[],groups:[],settings:{},engine:{},stats:{}});
 });
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/knowledge-base');await page.locator('.kb-row-main').click();await page.getByRole('dialog',{name:doc.name}).waitFor();await page.getByText('正在读取原文…',{exact:true}).waitFor();await page.getByRole('alert').filter({hasText:'原文暂不可用'}).waitFor();fail=false;
 await page.getByRole('button',{name:'重新读取',exact:true}).click();await page.locator('.v2-kb-reader .kb-section').first().waitFor();assert.equal(await page.locator('.v2-kb-reader .kb-section').count(),20);
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:width<700?844:1000});await page.waitForTimeout(120);
  const box=await page.getByRole('dialog',{name:doc.name}).boundingBox();assert(box.x>=0&&box.width<=width&&box.y>=0&&box.y+box.height<=(width<700?844:1000));
  assert(await page.locator('.kb-sections').evaluate(el=>el.scrollHeight>el.clientHeight),'only document body scrolls');const close=await page.getByRole('button',{name:'关闭预览'}).boundingBox();assert(close.y>=0&&close.y+close.height<=(width<700?844:1000),'close stays on screen');
  await page.screenshot({path:path.join(out,'reader-'+width+'.png')});
 }
 await page.getByRole('button',{name:'下一页片段'}).click();await page.waitForFunction(()=>document.querySelectorAll('.v2-kb-reader .kb-section').length===1);await page.getByRole('button',{name:'关闭预览'}).click();await page.getByRole('dialog',{name:doc.name}).waitFor({state:'hidden'});
 delay=600;await page.locator('.kb-row-main').click();await page.getByText('正在读取原文…',{exact:true}).waitFor();await page.keyboard.press('Escape');await page.waitForTimeout(750);assert.equal(await page.locator('.v2-kb-reader:visible').count(),0,'late response cannot reopen closed reader');assert.deepEqual(errors,[]);
 console.log('PASS: document modal immediate loading, retry, original version, paging, close/abort, PC/mobile reading area. Read-only mocked APIs.');
}finally{await browser.close();await server.close();}
