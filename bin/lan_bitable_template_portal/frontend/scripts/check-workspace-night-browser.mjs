import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile,mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const preview=path.resolve(root,'../../../output/night-mode-preview');
const output=path.resolve(root,'../../../output/playwright/workspace-night-mode');
await mkdir(output,{recursive:true});
const server=http.createServer(async(req,res)=>{
  const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
  const bases=pathname.startsWith('/assets/workspace-v2/')?[preview]:[path.join(root,'public'),path.join(root,'dist')];
  for(const base of bases){
    const file=path.resolve(base,'.'+(pathname==='/'?'/index.html':pathname));
    if(!file.startsWith(base+path.sep))continue;
    try{const data=await readFile(file);res.setHeader('Content-Type',file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':file.endsWith('.svg')?'image/svg+xml':file.endsWith('.json')?'application/json':'text/html; charset=utf-8');res.end(data);return;}catch{}
  }
  res.writeHead(404);res.end();
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port,entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
let account='night-a',loggedIn=true;const errors=[],requests=[];
const item={id:'fixture-doc',name:'夜间验收文档.txt',owner_name:'验收人员',can_edit:true,version:1,status:'ready',size:100,chunks:2,updated_at:1792044000};
try{
  const context=await browser.newContext({viewport:{width:1440,height:900}});
  await context.route('**/*',route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    requests.push(url.pathname);assert.equal(request.method(),'GET','night mode must not write business data');
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?{open_id:account,name:'夜间验收人员',role:'admin',level:'L1',category:'manager',modules:['accounts','maintenance','repair_management','knowledge','learning'],login_method:'password'}:{},scope_options:loggedIn?[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]:[]});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'night-mode-fixture'}});
    if(url.pathname==='/api/assistant/knowledge')return ok({items:[item],total:1,is_admin:true,settings:{mode:'local',engine_status:'ready'}});
    if(url.pathname==='/api/assistant/knowledge/groups')return ok({items:[]});
    if(url.pathname==='/api/assistant/knowledge/documents/fixture-doc')return ok({document:item,sections:[{text:'仅用于隔离界面验收，不是业务资料。',location:'第1段',ordinal:0}],total:1});
    return ok({items:[],records:[],users:[],scopes:[],scope_options:[],settings:{},summary:{},stats:{}});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  const theme=async(mode)=>{
    const button=page.getByRole('button',{name:'导航配色',exact:true});
    if(!await page.getByRole('switch',{name:'夜间模式'}).isVisible())await button.click();
    const toggle=page.getByRole('switch',{name:'夜间模式'});
    if(await toggle.getAttribute('aria-checked')!==String(mode==='dark'))await toggle.click();
    await page.waitForFunction(value=>document.documentElement.dataset.v2Theme===value,mode);
    await page.waitForTimeout(250);
  };
  const paint=selector=>page.locator(selector).first().evaluate(el=>{
    const s=getComputedStyle(el);return{background:s.backgroundColor,color:s.color,scheme:s.colorScheme};
  });
  const light=rgb=>rgb.match(/[\d.]+/g).slice(0,3).map(Number).map(n=>n/255).map(n=>n<=.04045?n/12.92:((n+.055)/1.055)**2.4).reduce((sum,n,i)=>sum+n*[.2126,.7152,.0722][i],0);
  const contrast=(a,b)=>(Math.max(light(a),light(b))+.05)/(Math.min(light(a),light(b))+.05);
  await page.goto(entry+'?view=my');await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  const day=await paint('body');assert(light(day.background)>.5);
  const before=requests.length;await theme('dark');assert.equal(requests.length,before,'switching themes does not refetch business data');
  const night=await paint('body');assert(light(night.background)<.05);assert(contrast(night.color,night.background)>=4.5);
  const popup=await paint('.n-popover');assert(light(popup.background)<.08,'portalled component-library popover is dark');
  assert(light((await paint('.v2-nav-search')).background)<.08,'navigation search does not retain a pale background');
  await page.getByRole('switch',{name:'夜间模式'}).focus();await page.keyboard.press('Space');
  await page.waitForFunction(()=>document.documentElement.dataset.v2Theme==='light');
  await page.keyboard.press('Space');await page.waitForFunction(()=>document.documentElement.dataset.v2Theme==='dark');
  for(const label of ['海湾蓝','松石青','石墨灰','莓果红']){
    await page.getByRole('button',{name:label,exact:true}).click();
    const colors=await page.evaluate(()=>{
      const el=document.createElement('span');document.querySelector('.v2-shell').append(el);
      el.style.background='var(--v2-nav-side)';el.style.color='var(--v2-nav-text)';
      const s=getComputedStyle(el),v={background:s.backgroundColor,color:s.color};el.remove();return v;
    });
    // Canvas resolves color-mix consistently across browser output formats.
    const rgb=await page.evaluate(colors=>{const c=document.createElement('canvas'),ctx=c.getContext('2d');return Object.fromEntries(Object.entries(colors).map(([k,v])=>{ctx.fillStyle=v;ctx.fillRect(0,0,1,1);return[k,'rgb('+[...ctx.getImageData(0,0,1,1).data].slice(0,3).join(',')+')'];}));},colors);
    assert(contrast(rgb.color,rgb.background)>=4.5);
  }
  await page.screenshot({path:path.join(output,'desktop-night.png')});
  await page.reload();await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark','preference survives reload');
  account='night-b';await page.reload();await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'light','another account does not inherit night mode');
  account='night-a';await page.goto(entry+'?__page=/knowledge-base');await page.getByText(item.name,{exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark');
  const panel=await paint('.kb-pane');assert(light(panel.background)<.1,'reused business panel has a dark surface');
  await page.getByRole('button',{name:'上传文件',exact:true}).click();await page.getByRole('button',{name:'粘贴文本',exact:true}).click();
  const input=page.getByRole('textbox',{name:'文档名称',exact:true});await input.fill('未提交的草稿');
  const field=await paint('.kb-text-input input, .kb-text-upload input, input[aria-label="文档名称"]');
  assert(light(field.background)<.1&&contrast(field.color,field.background)>=4.5,'native form fields remain legible');
  await theme('light');assert.equal(await input.inputValue(),'未提交的草稿','switch preserves unsaved input');
  assert(light((await paint('body')).background)>.5);
  await theme('dark');assert.equal(await input.inputValue(),'未提交的草稿');
  await page.getByRole('button',{name:'导航配色',exact:true}).click();
  await page.getByRole('switch',{name:'夜间模式'}).waitFor({state:'hidden'});
  await page.getByRole('button',{name:'移入回收站',exact:true}).click();
  const dialog=page.getByRole('dialog');await dialog.waitFor();
  await page.waitForTimeout(400);
  const dialogPaint=await dialog.evaluate(el=>{const s=getComputedStyle(el);return{bg:s.backgroundColor,color:s.color};});
  assert(light(dialogPaint.bg)<.1,'teleported native business dialog has a dark surface');
  await page.screenshot({path:path.join(output,'business-dialog-night.png')});
  await dialog.getByRole('button',{name:'取消',exact:true}).click();
  for(const width of [390,360]){
    await page.setViewportSize({width,height:844});await theme('dark');
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    const box=await page.getByRole('switch',{name:'夜间模式'}).boundingBox();assert(box.x>=0&&box.x+box.width<=width);
    await page.screenshot({path:path.join(output,'mobile-night-'+width+'.png')});
  }
  await page.emulateMedia({media:'print'});assert.equal((await paint('body')).background,'rgb(255, 255, 255)','printing is still white');
  await page.emulateMedia({media:'screen',reducedMotion:'reduce'});await theme('light');await theme('dark');
  await page.evaluate(()=>{
    window.fixtureStorageSet=Storage.prototype.setItem;
    Storage.prototype.setItem=function(key,value){if(key.startsWith('clipflow:workspace-night-mode:'))throw new DOMException('blocked','SecurityError');return window.fixtureStorageSet.call(this,key,value);};
  });
  await theme('light');await page.getByText('当前浏览器无法保存外观设置，刷新后可能恢复默认。',{exact:true}).waitFor();
  await page.evaluate(()=>{Storage.prototype.setItem=window.fixtureStorageSet;delete window.fixtureStorageSet;});
  await page.goto(origin+'/?frontend=legacy');await page.waitForTimeout(600);
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),null,'legacy entry never receives the theme');
  assert.deepEqual(errors,[]);
  console.log('PASS: day/night toggle, contrast, per-account persistence, native/Naive UI surfaces, unsaved fields, mobile, print, reduced motion and legacy isolation. No business writes.');
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
