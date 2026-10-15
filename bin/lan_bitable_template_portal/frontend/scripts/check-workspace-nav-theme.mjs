import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/nav-theme');
const entry='/assets/workspace-v2/index.html';
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
let account='fixture-a',page;const errors=[];
try{
  await mkdir(output,{recursive:true});
  const context=await browser.newContext({viewport:{width:1440,height:900}});
  await context.route('**/*',route=>{
    const req=route.request(),u=new URL(req.url());if(u.origin!==origin)return route.abort();
    if(!u.pathname.startsWith('/api/'))return route.continue();
    assert.equal(req.method(),'GET','appearance preferences never write business data');
    if(u.pathname==='/api/auth/status')return route.fulfill({json:{ok:true,data:{logged_in:true,user:{open_id:account,name:'界面验收人员',role:'admin',level:'L1',category:'manager',modules:['maintenance','repair_management','critical_guard']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]}}});
    if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'nav-theme-fixture'}});
    return route.fulfill({json:{ok:true,data:{items:[],records:[],scopes:{}}}});
  });
  page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
  await page.goto(origin+entry+'?__page=/&view=my');
  await page.getByRole('heading',{name:'界面验收人员',exact:true}).waitFor();
  const colors=()=>page.evaluate(()=>{
    const shell=document.querySelector('.v2-shell'),probe=document.createElement('div'),canvas=document.createElement('canvas'),ctx=canvas.getContext('2d');
    shell.append(probe);
    const rgb=name=>{probe.style.backgroundColor='var('+name+')';ctx.fillStyle=getComputedStyle(probe).backgroundColor;ctx.fillRect(0,0,1,1);return [...ctx.getImageData(0,0,1,1).data].slice(0,3);};
    const result={top:rgb('--v2-nav-top'),side:rgb('--v2-nav-side'),text:rgb('--v2-nav-text'),selected:rgb('--v2-nav-active'),color:shell.style.getPropertyValue('--v2-nav-color'),header:getComputedStyle(document.querySelector('.v2-header')).backgroundImage,sidebar:getComputedStyle(document.querySelector('.v2-sidebar')).backgroundImage};probe.remove();return result;
  });
  const luminance=rgb=>rgb.map(n=>n/255).map(n=>n<=.04045?n/12.92:((n+.055)/1.055)**2.4).reduce((sum,n,i)=>sum+n*[.2126,.7152,.0722][i],0);
  const contrast=(a,b)=>(Math.max(luminance(a),luminance(b))+.05)/(Math.min(luminance(a),luminance(b))+.05);
  const assertHeaderSeam=async()=>{
    const header=page.locator('.v2-header'),sidebar=page.locator('.v2-sidebar');
    const box=await header.boundingBox(),brand=await page.locator('.v2-brand').boundingBox();
    assert.equal(Math.round(box.height),page.viewportSize().width>760?60:56,'compact header height');
    assert(Math.abs(brand.y+brand.height/2-(box.y+box.height/2))<=1,'brand is vertically centered without the old fade padding');
    const fade=await header.evaluate(el=>{const style=getComputedStyle(el,'::after');return {height:style.height,mask:style.maskImage,pointer:style.pointerEvents};});
    assert.equal(fade.height,'14px');assert.match(fade.mask,/linear-gradient/);assert.equal(fade.pointer,'none','bottom fade never blocks the header or content');
    if(await sidebar.isVisible()){
      const side=await sidebar.boundingBox(),head=await page.locator('.v2-nav-head').boundingBox(),main=await page.locator('.v2-main').boundingBox();
      assert(Math.abs(box.y+box.height-side.y)<=1,'sidebar starts exactly at header bottom');
      assert(Math.abs(side.y-head.y)<=1,'navigation heading has no gap above it');
      assert(Math.abs(main.y-side.y)<=1,'content and navigation start on the same baseline');
      assert(Math.abs(main.x-(side.x+side.width))<=1,'content follows the sidebar edge');
    }
  };
  const initial=await colors();
  await page.getByRole('button',{name:'导航配色',exact:true}).click();
  await page.screenshot({path:path.join(output,'theme-picker-open.png')});
  for(const label of ['松石青','石墨灰','莓果红','海湾蓝']){
    await page.getByRole('button',{name:label,exact:true}).click();const next=await colors();
    assert(luminance(next.side)>luminance(next.top),'sidebar stays lighter than header');
    assert(contrast(next.text,next.side)>=4.5&&contrast(next.text,next.selected)>=4.5,'sidebar text remains readable');
    assert(contrast([244,248,255],next.top)>=4.5,'header text remains readable');
    if(label!=='海湾蓝'){assert.notEqual(next.header,initial.header);assert.notEqual(next.sidebar,initial.sidebar);}
    assert.equal(await page.getByRole('button',{name:label,exact:true}).getAttribute('aria-pressed'),'true');
    await assertHeaderSeam();
  }
  await page.locator('.v2-theme-picker .n-color-picker').click();
  for(const custom of ['#ffffff','#000000','#e0bb66']){
    const input=page.locator('.n-color-picker-input input').first();
    await input.fill(custom);await input.press('Tab');
    await page.waitForFunction(color=>document.querySelector('.v2-shell').style.getPropertyValue('--v2-nav-color')===color,custom);
    const next=await colors();assert(luminance(next.side)>luminance(next.top));
    assert(contrast(next.text,next.side)>=4.5&&contrast([244,248,255],next.top)>=4.5,'custom extremes remain readable');
    await assertHeaderSeam();
    assert.equal(await page.evaluate(()=>localStorage.getItem('clipflow:workspace-nav-color:fixture-a')),custom,'custom color is persisted');
  }
  await page.getByText('导航配色',{exact:true}).last().click();
  await page.getByRole('button',{name:'松石青',exact:true}).click();
  await page.getByRole('heading',{name:'界面验收人员',exact:true}).click();
  await page.reload();await page.getByRole('heading',{name:'界面验收人员',exact:true}).waitFor();
  assert.equal((await colors()).color,'#16836d','personal color survives reload');
  account='fixture-b';await page.reload();await page.getByRole('heading',{name:'界面验收人员',exact:true}).waitFor();
  assert.equal((await colors()).color,'#2364d2','different account starts with its own preference');
  account='fixture-a';await page.reload();await page.getByRole('heading',{name:'界面验收人员',exact:true}).waitFor();
  assert.equal((await colors()).color,'#16836d');
  const logo=page.locator('.v2-brand img');
  assert.equal(await logo.evaluate(img=>getComputedStyle(img).backgroundColor),'rgba(0, 0, 0, 0)');
  assert(await logo.evaluate(img=>img.complete&&img.naturalWidth===300),'original logo loads unchanged');
  for(const width of [1440,1150,1100,1024,390,360]){
    await page.setViewportSize({width,height:900});await page.waitForTimeout(250);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'header controls fit viewport');
    const box=await page.getByRole('button',{name:'导航配色',exact:true}).boundingBox();assert(box.x>=0&&box.x+box.width<=width);
    await assertHeaderSeam();
    await page.screenshot({path:path.join(output,'theme-'+width+'.png'),fullPage:true});
    if(width>760){
      const sidebar=page.locator('.v2-sidebar'),head=page.locator('.v2-nav-head');
      const sideBox=await sidebar.boundingBox(),headBox=await head.boundingBox();
      assert(sideBox.width>=200,'expanded navigation keeps room for heading and search');
      assert(headBox.x>=sideBox.x&&headBox.x+headBox.width<=sideBox.x+sideBox.width+1,'navigation heading stays inside sidebar');
      assert(!await sidebar.evaluate(el=>el.scrollWidth>el.clientWidth+1),'navigation has no horizontal overflow');
      await page.getByRole('button',{name:'收起导航',exact:true}).click();await page.waitForTimeout(250);
      assert.equal(Math.round((await sidebar.boundingBox()).width),64,'collapsed navigation is consistent');
      assert(!await sidebar.evaluate(el=>el.scrollWidth>el.clientWidth+1),'collapsed controls fit sidebar');
      assert.equal(await sidebar.locator('.n-menu-item-group-title:visible').count(),0,'collapsed navigation must not wrap group titles between icons');
      const iconCenters=await sidebar.locator('.n-menu-item-content__icon').evaluateAll(items=>items.map(el=>{const r=el.getBoundingClientRect();return r.x+r.width/2;}));
      assert(iconCenters.length>0&&iconCenters.every(x=>Math.abs(x-sideBox.x-32)<=1),'collapsed icons remain centered');
      await sidebar.locator('.n-menu-item-content').first().hover();
      await page.locator('.n-tooltip').filter({hasText:'个人工作台'}).waitFor();
      await page.getByRole('heading',{name:'界面验收人员',exact:true}).hover();
      await assertHeaderSeam();
      await page.screenshot({path:path.join(output,'theme-collapsed-'+width+'.png')});
      await page.getByRole('button',{name:'展开导航',exact:true}).click();await page.waitForTimeout(250);
      await assertHeaderSeam();
    }
  }
  assert.deepEqual(errors,[]);
  console.log('PASS: linked palettes, readable contrast, per-account persistence, transparent logo, seamless header, desktop responsive collapse and mobile header. No business writes.');
}catch(error){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw error;}
finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
