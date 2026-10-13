import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { build } from 'esbuild';
import { preview } from 'vite';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('../', import.meta.url));
const compiled = await build({entryPoints:[path.join(root,'src/workspace-v2/version.ts')],bundle:true,format:'esm',write:false});
const {loginVersionUrl, WORKSPACE_ENTRY, readVersion} = await import('data:text/javascript;base64,' + Buffer.from(compiled.outputFiles[0].text).toString('base64'));
const testOrigin = 'http://127.0.0.1:18766';
let selected = 'password';
globalThis.document = {querySelector:()=>({id:'login-tab-' + selected})};
globalThis.window = {location:new URL('/?login=feishu&next=%2Flearning%3Fview%3Dpractice',testOrigin)};
const nextVersion = new URL(loginVersionUrl('workspace'));
assert.equal(nextVersion.pathname,WORKSPACE_ENTRY);
assert.equal(nextVersion.searchParams.get('login'),'password');
let next = new URL(nextVersion.searchParams.get('next'),testOrigin);
assert.equal(next.pathname,WORKSPACE_ENTRY);
assert.equal(next.searchParams.get('__page'),'/learning');
assert.equal(next.searchParams.get('view'),'practice');
window.location = nextVersion;
selected = 'guest';
const legacy = new URL(loginVersionUrl('legacy'));
assert.equal(legacy.pathname,'/');
assert.equal(legacy.searchParams.get('login'),'guest');
next = new URL(legacy.searchParams.get('next'),testOrigin);
assert.equal(next.pathname,'/learning');
assert.equal(next.searchParams.get('view'),'practice');
assert.equal(next.searchParams.get('frontend'),'legacy');
window.location = new URL(WORKSPACE_ENTRY,testOrigin);
const initialLegacy = new URL(loginVersionUrl('legacy'));
next = new URL(initialLegacy.searchParams.get('next'),testOrigin);
assert.equal(next.pathname,'/');
assert.equal(next.searchParams.get('frontend'),'legacy');
assert.equal(next.searchParams.has('login'),false);
for(const unsafe of ['https://outside.invalid/','//outside.invalid/','/api/auth/logout']){
  window.location = new URL('/?next='+encodeURIComponent(unsafe),testOrigin);
  assert.equal(new URL(loginVersionUrl('workspace')).searchParams.has('next'),false);
}
globalThis.localStorage = {getItem:key=>key.endsWith('alice')?'workspace':null,setItem:()=>assert.fail('Anonymous switching must not change account preferences')};
assert.equal(readVersion('alice'),'workspace');
assert.equal(readVersion('bob'),'legacy');
delete globalThis.window;delete globalThis.document;delete globalThis.localStorage;

const server = await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const output = path.resolve(root,'../../../output/playwright/login-version-switch');
await mkdir(output,{recursive:true});
let browser;
try{
  browser = await chromium.launch({headless:true});
  for(const width of [1440,390]){
    const context = await browser.newContext({viewport:{width,height:960}});
    const errors=[],writes=[];
    await context.route('**/*',route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin!==origin)return route.abort();
      if(!url.pathname.startsWith('/api/'))return route.continue();
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'login-version-fixture'}});
      if(request.method()!=='GET')writes.push(url.pathname);
      const data=url.pathname==='/api/auth/status'
        ? {logged_in:false,user:{},scope_options:[],login_url:'/api/auth/login?next='+encodeURIComponent(url.searchParams.get('next')||'/')}
        : {items:[],people:[],options:[],scope_options:[],stats:{},settings:{}};
      return route.fulfill({json:{ok:true,data}});
    });
    const page = await context.newPage();
    page.setDefaultTimeout(15000);
    page.on('pageerror',error=>errors.push(error.message));
    await page.goto(origin+'/?login=feishu&next='+encodeURIComponent('/learning?view=practice'));
    await page.locator('#login-tab-password').click();
    const toNew=page.getByRole('button',{name:'切换新版',exact:true});
    await toNew.waitFor();
    const oldBox=await toNew.boundingBox();
    await page.screenshot({path:path.join(output,`legacy-${width}.png`),fullPage:true,animations:'disabled'});
    assert(oldBox.x>width/2 && oldBox.x+oldBox.width<=width && oldBox.height>=44,JSON.stringify({width,oldBox}));
    await toNew.click();
    await page.waitForURL(url=>url.pathname===WORKSPACE_ENTRY);
    await page.locator('#login-tab-password[aria-selected="true"]').waitFor();
    assert.equal(new URL(page.url()).searchParams.get('login'),'password');
    await page.reload();
    await page.locator('#login-tab-password[aria-selected="true"]').waitFor();
    const toOld=page.getByRole('button',{name:'切换旧版',exact:true});
    const newBox=await toOld.boundingBox();
    assert(newBox.x>width/2 && newBox.x+newBox.width<=width && newBox.height>=44);
    await page.screenshot({path:path.join(output,`workspace-${width}.png`),fullPage:true,animations:'disabled'});
    await page.locator('#login-tab-guest').click();
    await toOld.click();
    await page.waitForURL(url=>url.pathname==='/');
    await page.locator('#login-tab-guest[aria-selected="true"]').waitFor();
    assert.equal(new URL(new URL(page.url()).searchParams.get('next'),origin).pathname,'/learning');
    await page.locator('#login-tab-feishu').click();
    await page.getByRole('button',{name:'切换新版',exact:true}).click();
    await page.waitForURL(url=>url.pathname===WORKSPACE_ENTRY);
    await page.locator('#login-tab-feishu[aria-selected="true"]').waitFor();
    const oauth=new URL(await page.locator('a.feishu-btn').getAttribute('href'),origin);
    assert.equal(new URL(oauth.searchParams.get('next'),origin).pathname,WORKSPACE_ENTRY);
    await page.goto(origin+WORKSPACE_ENTRY);
    await page.getByRole('button',{name:'切换旧版',exact:true}).click();
    await page.waitForURL(url=>url.pathname==='/');
    await page.locator('a.feishu-btn').waitFor();
    const oldOauth=new URL(await page.locator('a.feishu-btn').getAttribute('href'),origin);
    assert.equal(new URL(oldOauth.searchParams.get('next'),origin).searchParams.get('frontend'),'legacy');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    assert.deepEqual(errors,[]);
    assert.deepEqual(writes,[],'Switching UI must not submit credentials or business requests');
    await context.close();
  }
  console.log('PASS: PC/mobile login version switches, selected tabs, refresh, OAuth return destination, unsafe links and no writes.');
}finally{
  await browser?.close();
  await new Promise(resolve=>server.httpServer.close(resolve));
}
