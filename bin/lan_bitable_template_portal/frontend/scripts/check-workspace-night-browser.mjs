import assert from 'node:assert/strict';
import http from 'node:http';
import {tmpdir} from 'node:os';
import {readFile,mkdtemp} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const bundle=path.resolve(root,process.env.CLIPFLOW_WORKSPACE_BUILD_DIR||'dist/assets/workspace-v2');
const output=await mkdtemp(path.join(tmpdir(),'workspace-night-'));
console.log('Screenshots:',output);
const previewPort=process.env.CLIPFLOW_ISOLATED_PREVIEW_PORT;
if(previewPort)assert.match(previewPort,/^\d{1,5}$/);
const server=previewPort?null:http.createServer(async(req,res)=>{
  const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
  const v2=pathname.startsWith('/assets/workspace-v2/');
  const base=v2?bundle:path.join(root,'dist');
  const resource=v2?pathname.slice('/assets/workspace-v2'.length):pathname==='/'?'/index.html':pathname;
  const file=path.resolve(base,'.'+resource);
  if(file.startsWith(base+path.sep)){
    try{const data=await readFile(file);res.setHeader('Content-Type',file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':file.endsWith('.svg')?'image/svg+xml':file.endsWith('.json')?'application/json':'text/html; charset=utf-8');res.end(data);return;}catch{}
  }
  res.writeHead(404);res.end();
});
if(server)await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+(previewPort||server.address().port),entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
let account='night-a',loggedIn=true;const errors=[],requests=[];
const item={id:'fixture-doc',name:'夜间验收文档.txt',owner_open_id:'night-a',owner_name:'夜间验收人员',can_edit:true,version:1,status:'ready',size:100,chunks:2,updated_at:1792044000};
try{
  const context=await browser.newContext({serviceWorkers:'block',viewport:{width:1440,height:900}});
  await context.route('**/*',route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return request.method()==='GET'?route.continue():route.abort();
    requests.push(url.pathname);assert.equal(request.method(),'GET','night mode must not write business data');
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?{open_id:account,name:'夜间验收人员',role:'building',level:'L4',category:'operator',home_scope:'A',modules:account==='night-a'?['knowledge']:['maintenance'],login_method:'password'}:{},scope_options:loggedIn?[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]:[]});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'night-mode-fixture'}});
    if(url.pathname.startsWith('/api/assistant/knowledge'))assert(loggedIn&&account==='night-a','knowledge read without its module grant');
    if(url.pathname==='/api/assistant/knowledge')return ok({items:[item],total:1,is_admin:false,settings:{mode:'local',engine_status:'ready'}});
    if(url.pathname==='/api/assistant/knowledge/groups')return ok({items:[]});
    if(url.pathname==='/api/assistant/knowledge/documents/fixture-doc')return ok({document:item,sections:[{text:'仅用于隔离界面验收，不是业务资料。',location:'第1段',ordinal:0}],total:1});
    return ok({items:[],records:[],users:[],scopes:[],scope_options:[],settings:{},summary:{},stats:{}});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  const themeToggle=page.getByRole('button',{name:'夜间模式',exact:true});
  const paletteMenu=page.getByRole('button',{name:'主题配色',exact:true});
  const theme=async(mode)=>{
    if(await themeToggle.getAttribute('aria-pressed')!==String(mode==='dark'))await themeToggle.click();
    await page.waitForFunction(value=>document.documentElement.dataset.v2Theme===value,mode);
    await page.waitForTimeout(400);
  };
  const paint=selector=>page.locator(selector).first().evaluate(el=>{
    const s=getComputedStyle(el),canvas=document.createElement('canvas'),ctx=canvas.getContext('2d');
    const rgb=value=>{ctx.clearRect(0,0,1,1);ctx.fillStyle=value;ctx.fillRect(0,0,1,1);return 'rgb('+[...ctx.getImageData(0,0,1,1).data].slice(0,3).join(', ')+')';};
    return{background:rgb(s.backgroundColor),color:rgb(s.color),scheme:s.colorScheme};
  });
  const light=rgb=>rgb.match(/[\d.]+/g).slice(0,3).map(Number).map(n=>n/255).map(n=>n<=.04045?n/12.92:((n+.055)/1.055)**2.4).reduce((sum,n,i)=>sum+n*[.2126,.7152,.0722][i],0);
  const contrast=(a,b)=>(Math.max(light(a),light(b))+.05)/(Math.min(light(a),light(b))+.05);
  await page.goto(entry+'?view=my');await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  await page.waitForLoadState('networkidle');
  const day=await paint('body');assert(light(day.background)>.5);
  const before=requests.length;await theme('dark');assert.equal(requests.length,before,'switching themes does not refetch business data');
  const night=await paint('body');assert(light(night.background)<.05);assert(contrast(night.color,night.background)>=4.5);
  await paletteMenu.click();
  const popup=await paint('.n-dropdown-menu');assert(light(popup.background)<.08,'portalled palette menu is dark');
  await page.keyboard.press('Escape');
  assert(light((await paint('.v2-topnav')).background)<.08,'desktop navigation has a dark surface');
  await themeToggle.focus();await page.keyboard.press('Space');
  await page.waitForFunction(()=>document.documentElement.dataset.v2Theme==='light');
  await page.keyboard.press('Space');await page.waitForFunction(()=>document.documentElement.dataset.v2Theme==='dark');
  const palettes={mineral:'松绿',ocean:'海蓝',violet:'暮紫',panorama:'全景蓝',neon:'霓虹玫红',lava:'熔岩橙',graphite:'石墨银',cyber:'赛博青'};
  async function choosePalette(id){
    await paletteMenu.click();
    await page.locator('.n-dropdown-menu:visible').getByText(palettes[id],{exact:true}).click();
    await page.waitForFunction(id=>document.documentElement.dataset.v2Palette===id,id);
  }
  for(const mode of ['light','dark']){
    await theme(mode);
    const accents=new Set();
    for(const id of Object.keys(palettes)){
      await choosePalette(id);
      const colors=await page.locator('.v2-shell').evaluate(el=>{
        const style=getComputedStyle(el);
        const names=['bg','surface','soft','selection','text','muted','accent-text','accent','hover','pressed','on-accent'];
        const vars=Object.fromEntries(names.map(key=>[key,style.getPropertyValue('--v2-palette-'+key).trim()]));
        return {...vars,blue:style.getPropertyValue('--v2-blue').trim(),nav:style.getPropertyValue('--v2-nav-side').trim(),
          navText:style.getPropertyValue('--v2-nav-text').trim()};
      });
      const rgb=hex=>{
        assert.match(hex,/^#[a-f0-9]{6}$/i,'resolved palette color');
        return 'rgb('+hex.slice(1).match(/../g).map(pair=>parseInt(pair,16)).join(',')+')';
      };
      const readable=(fg,bg)=>assert(contrast(rgb(colors[fg]),rgb(colors[bg]))>=4.5,`${id} ${mode}: ${fg} on ${bg}`);
      assert.equal(colors.blue,colors['accent-text'],'business accent follows the selected palette');
      for(const bg of ['bg','surface','soft','selection'])for(const fg of ['text','muted','accent-text'])readable(fg,bg);
      for(const bg of ['accent','hover','pressed'])readable('on-accent',bg);
      readable('navText','nav');
      accents.add(colors.accent);
    }
    assert.equal(accents.size,Object.keys(palettes).length,'each palette applies a distinct accent in '+mode);
  }
  await page.screenshot({path:path.join(output,'desktop-night.png')});
  await page.reload();await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark','preference survives reload');
  assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'cyber','palette survives reload');
  account='night-b';await page.reload();await page.getByRole('heading',{name:'夜间验收人员',exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'light','another account does not inherit night mode');
  assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'panorama','new account starts with the default palette');
  await choosePalette('violet');
  await page.goto(entry+'?__page=/knowledge-base');
  await page.getByText('没有该功能权限',{exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'violet');
  account='night-a';await page.goto(entry+'?__page=/knowledge-base');await page.getByText(item.name,{exact:true}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark');
  assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'cyber','returning account restores its own palette');
  const panel=await paint('.sk-shelf');assert(light(panel.background)<.1,'business panel has a dark surface');
  await page.getByRole('button',{name:'上传文件',exact:true}).click();await page.getByRole('button',{name:'粘贴文本',exact:true}).click();
  const input=page.getByRole('textbox',{name:'文件名',exact:true});await input.fill('未提交的草稿');
  const field=await paint('.sk-write input');
  assert(light(field.background)<.1&&contrast(field.color,field.background)>=4.5,'native form fields remain legible');
  await theme('light');assert.equal(await input.inputValue(),'未提交的草稿','switch preserves unsaved input');
  assert(light((await paint('body')).background)>.5);
  await theme('dark');assert.equal(await input.inputValue(),'未提交的草稿');
  await page.getByLabel('文件操作 '+item.name,{exact:true}).click();
  await page.getByRole('button',{name:'移入回收站',exact:true}).click();
  const dialog=page.getByRole('dialog');await dialog.waitFor();
  await page.waitForTimeout(400);
  const dialogPaint=await dialog.evaluate(el=>{const s=getComputedStyle(el);return{bg:s.backgroundColor,color:s.color};});
  assert(light(dialogPaint.bg)<.1,'teleported native business dialog has a dark surface');
  await page.screenshot({path:path.join(output,'business-dialog-night.png')});
  await dialog.getByRole('button',{name:'取消',exact:true}).click();
  await page.setViewportSize({width:390,height:844});
  await page.locator('.v2-mobile-nav').getByRole('button',{name:'我的',exact:true}).click();
  for(const width of [390,360]){
    await page.setViewportSize({width,height:844});await theme('dark');
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    const box=await themeToggle.boundingBox();assert(box.x>=0&&box.x+box.width<=width);
    await page.screenshot({path:path.join(output,'mobile-night-'+width+'.png')});
  }
  await page.emulateMedia({media:'print'});assert.equal((await paint('body')).background,'rgb(255, 255, 255)','printing is still white');
  await page.emulateMedia({media:'screen',reducedMotion:'reduce'});await theme('light');await theme('dark');
  await page.evaluate(()=>{
    window.fixtureStorageSet=Storage.prototype.setItem;
    Storage.prototype.setItem=function(key,value){if(key.startsWith('clipflow:workspace-night-mode:'))throw new DOMException('blocked','SecurityError');return window.fixtureStorageSet.call(this,key,value);};
  });
  await theme('light');await page.getByText('当前外观已切换，浏览器未能保存偏好。',{exact:true}).waitFor();
  await page.evaluate(()=>{Storage.prototype.setItem=window.fixtureStorageSet;delete window.fixtureStorageSet;});
  loggedIn=false;await page.goto(entry);await page.locator('.session-entry').waitFor();
  await page.getByRole('tab',{name:'姓名登录',exact:true}).click();await page.locator('#personnel-password').waitFor();
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:900});
    for(const mode of ['dark','light']){
      await theme(mode);
      const field=await paint('#personnel-password');
      assert(contrast(field.color,field.background)>=4.5,'password form text is legible in both themes: '+JSON.stringify({mode,width,field}));
      assert.equal(light(field.background)<.1,mode==='dark');
      assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
      await page.screenshot({path:path.join(output,`login-${mode}-${width}.png`)});
    }
  }
  await page.goto(origin+'/?frontend=legacy');await page.locator('.login-panel').getByRole('tablist',{name:'登录方式'}).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-theme'),null,'legacy entry never receives the theme');
  assert.deepEqual(errors,[]);
  console.log('PASS: day/night toggle, contrast, per-account persistence, native/Naive UI surfaces, unsaved fields, login, mobile, print, reduced motion and legacy isolation. No business writes.');
}finally{await browser.close();if(server)await new Promise(resolve=>server.close(resolve));}
