import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import net from 'node:net';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {adaptSingleSelects} from './workspace-select-adapter.mjs';
const root=fileURLToPath(new URL('..',import.meta.url));
const probe=net.createServer();await new Promise(r=>probe.listen(0,'127.0.0.1',r));const port=probe.address().port;await new Promise(r=>probe.close(r));
const server=await createServer({root,configFile:false,plugins:[{name:'single-select-test',enforce:'pre',transform(code,id){if(id.endsWith('.vue'))return adaptSingleSelects(code,id);}},vue()],logLevel:'error',server:{host:'127.0.0.1',port,strictPort:true}});
await server.listen();const origin=`http://127.0.0.1:${port}`,browser=await chromium.launch({headless:true});
const errors=[],writes=[];
try{
 const page=await browser.newPage({viewport:{width:1440,height:900}});page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',route=>{
  const url=new URL(route.request().url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/scripts/fixtures/select-regression.ts"></script>'});
  if(url.pathname==='/api/notice-target-candidates')return route.fulfill({json:{ok:true,data:{candidates:[{target_record_id:'existing',title:'当前通告'},{target_record_id:'new',title:'另一目标通告',building:'E楼'}]}}});
  if(url.pathname==='/api/notice-identity/bind'){writes.push(route.request().postDataJSON());return route.fulfill({json:{ok:true,data:{target_record_id:'new'}}});}
  if(url.pathname.startsWith('/api/'))throw Error('Unexpected request '+url.pathname);
  return route.continue();
 });
 await page.goto(origin+'/fixture');await page.locator('#result').waitFor();
 const select=name=>page.getByRole('combobox',{name,exact:true});
 const choose=async(name,text)=>{await select(name).click();await page.locator('.n-base-select-option').getByText(text,{exact:true}).click();};
 await page.getByRole('button',{name:'保存',exact:true}).click();assert.equal(JSON.parse(await page.locator('#result').innerText()).submitted,0);await page.keyboard.press('Escape');
 await choose('数量','两个');assert.equal(JSON.parse(await page.locator('#result').innerText()).count,2);
 await choose('对象','乙');assert.equal(JSON.parse(await page.locator('#result').innerText()).object.name,'乙');
 await page.getByRole('button',{name:'恢复数量'}).click();await page.waitForFunction(()=>document.querySelector('.v2-single-select').textContent.includes('一个'));
 await page.getByRole('button',{name:'新增选项'}).click();await choose('对象','丙');assert.equal(JSON.parse(await page.locator('#result').innerText()).object.name,'丙');
 await page.getByRole('button',{name:'锁定切换'}).click();assert.equal(await select('状态').getAttribute('aria-disabled'),'true');await page.getByRole('button',{name:'锁定切换'}).click();
 await select('状态').click();await page.keyboard.press('ArrowDown');await page.keyboard.press('Enter');await page.keyboard.press('Escape');
 await page.getByRole('button',{name:'打开对话框'}).click();await select('时段').click();assert.equal(await page.locator('dialog .n-base-select-menu').count(),1);await page.waitForTimeout(350);assert.equal(await page.locator('dialog .n-base-select-option--selected').evaluate(el=>getComputedStyle(el,'::before').backgroundColor),'rgb(36, 74, 102)','selected option follows assistant theme');await page.locator('.n-base-select-option').getByText('17:00',{exact:true}).click();await page.getByRole('button',{name:'关闭',exact:true}).click();
 await page.getByText('已绑定目标通告',{exact:true}).waitFor();await page.getByRole('button',{name:'重新查找目标通告'}).click();await page.getByText('已绑定当前选中记录',{exact:true}).waitFor();assert(await page.getByRole('button',{name:'核对并保存关联'}).isDisabled());
 await page.getByLabel('关联候选',{exact:true}).click();await page.locator('.n-base-select-option').filter({hasText:'另一目标通告'}).click();assert.equal(writes.length,0);
 await page.getByRole('button',{name:'核对并保存关联'}).click();await page.getByRole('button',{name:'返回',exact:true}).click();assert.equal(writes.length,0);
 await page.getByRole('button',{name:'核对并保存关联'}).click();await page.getByRole('button',{name:'确认关联并重新读取'}).click();await page.waitForFunction(()=>document.querySelector('#bound').textContent.includes('new'));assert.equal(writes.length,1);assert.equal(writes[0].target_record_id,'new');
 await page.setViewportSize({width:390,height:844});await select('数量').click();await page.locator('.n-base-select-option').getByText('一个',{exact:true}).click();assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));assert.deepEqual(errors,[]);
 console.log('PASS: single-select numeric/object values, reactive updates, disabled fieldsets, native validation, keyboard, dialog portal, mobile, bound identity and explicit save confirmation. Only mocked writes.');
}finally{await browser.close();await server.close();}
