import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { build } from 'esbuild';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const out=path.resolve(root,'../../../output/playwright/vipboard');await mkdir(out,{recursive:true});
async function exportsOf(file){const result=await build({entryPoints:[path.join(root,file)],bundle:true,platform:'node',format:'cjs',write:false});const module={exports:{}};new Function('module','exports','require',result.outputFiles[0].text)(module,module.exports,createRequire(import.meta.url));return module.exports;}
const {chartOption}=await exportsOf('src/workspace-v2/vipboard.ts');
const {widget,sanitize,chartTemplates}=await exportsOf('src/workspace-v2/dashboard/layout.ts');
const user={open_id:'fixture-a',modules:['maintenance','event','water','learning']};
for(const t of chartTemplates){const w=widget(t.kind,user,{display:t.display,presentation:t.presentation});const clean=sanitize({version:1,widgets:[w]},user,['A']);assert.equal(clean.widgets[0].display,w.display);assert.equal(clean.widgets[0].presentation,w.presentation);assert.equal(sanitize(clean,{...user,modules:[]},['A']).widgets.length,0);}
const data=[{label:'<img src=x onerror=alert(1)>',value:0}];
for(const view of ['bar','horizontal','line','area','donut','pie']){
 const o=chartOption(data,view,'t',true,false);assert.equal(o.animation,false);assert.equal(o.tooltip.renderMode,'richText');assert.equal(o.series[0].data[0].value,0);assert(!JSON.stringify(o).includes('NaN'));
}
assert.equal(chartOption(Array.from({length:50},(_,i)=>({label:String(i),value:i})),'bar').dataZoom.length,1);
const server=await createServer({root,configFile:false,cacheDir:path.join(root,'node_modules/.vite-vipboard-check'),plugins:[vue()],optimizeDeps:{include:['vue','naive-ui','lucide-vue-next','echarts/core','echarts/charts','echarts/components','echarts/renderers','echarts/features']},logLevel:'error',server:{host:'127.0.0.1',port:0}});await server.listen();
const origin=`http://127.0.0.1:${server.httpServer.address().port}`,browser=await chromium.launch({headless:true}),errors=[];
try{
 const page=await browser.newPage({viewport:{width:1440,height:900}});page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',route=>{const u=new URL(route.request().url());if(u.origin!==origin)return route.abort();if(u.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:'<meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/scripts/fixtures/vipboard.ts"></script>'});return route.continue();});
 await page.goto(origin+'/fixture');await page.locator('.v2-chart-canvas svg').waitFor();await page.waitForTimeout(1000);
 assert.equal(await page.getByRole('textbox',{name:'搜索功能'}).count(),0,'work navigation no longer adds a search row');
 await page.getByRole('menuitem',{name:'通告办理',exact:true}).click();await page.waitForFunction(()=>window.chartFixture.selected.value==='notices');
 await page.getByRole('button',{name:'收起导航',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.v2-sidebar').clientWidth===64);
 await page.getByRole('button',{name:'展开导航',exact:true}).click();await page.getByRole('menuitem',{name:'事件管理',exact:true}).waitFor();
 await page.setViewportSize({width:1024,height:900});await page.waitForTimeout(250);assert.equal(await page.getByRole('menuitem',{name:'事件管理',exact:true}).isVisible(),true,'expanded labels remain visible on narrow desktop');
 await page.setViewportSize({width:1440,height:900});
 const instance=await page.locator('.v2-chart-canvas').getAttribute('_echarts_instance_');
 for(const view of ['horizontal','line','area','donut','pie','bar']){
  await page.evaluate(view=>window.chartFixture.view.value=view,view);await page.waitForTimeout(700);
  assert.equal(await page.locator('.v2-chart-canvas').getAttribute('_echarts_instance_'),instance);
  assert.equal(await page.locator('.v2-chart-canvas svg').count(),1);
  assert(await page.locator('.v2-chart-canvas svg path[fill="#197f70"], .v2-chart-canvas svg path[stroke="#197f70"]').count()>0,view+' renders a real data mark, not an empty SVG');
 }
 await page.getByText('数据明细',{exact:false}).click();await page.getByRole('button',{name:'变更 6 条'}).click();assert.equal(await page.evaluate(()=>window.chartFixture.selection[0].label),'变更');
 await page.evaluate(()=>{window.chartFixture.items.value=[{label:'全部为零',value:0}];});await page.waitForTimeout(300);assert.equal(await page.getByText('暂无数据',{exact:true}).count(),0);
 await page.evaluate(()=>{window.chartFixture.items.value=[{label:'<script>恶意内容</script>',value:3}];window.chartFixture.view.value='donut';});await page.waitForTimeout(700);assert.equal(await page.locator('script:not([type=module])').count(),0);
 await page.evaluate(()=>window.chartFixture.failed.value=true);await page.getByText('数据暂不可用',{exact:true}).waitFor();assert.equal(await page.locator('.v2-chart-details').count(),0);
 await page.evaluate(()=>{window.chartFixture.failed.value=false;window.chartFixture.items.value=[{label:'A楼',value:12},{label:'B楼',value:8},{label:'C楼',value:5}];});
 for(const width of [1366,1440,1920,360,390,430]){
  await page.setViewportSize({width,height:900});await page.waitForTimeout(300);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await page.screenshot({path:path.join(out,`chart-${width}.png`)});
 }
 await page.emulateMedia({reducedMotion:'reduce'});await page.evaluate(()=>window.chartFixture.view.value='line');await page.waitForTimeout(200);
 await page.evaluate(()=>window.chartFixture.mounted.value=false);await page.locator('.v2-motion-chart').waitFor({state:'detached'});
 await page.evaluate(()=>window.chartFixture.mounted.value=true);await page.locator('.v2-chart-canvas svg').waitFor();
 assert.deepEqual(errors,[]);
 console.log('PASS: compact sidebar navigation/collapse/narrow desktop, template persistence+permissions, safe options, six chart types, instance reuse, zero/error states, drill-down, unmount/remount, desktop/mobile and reduced motion.');
}catch(error){const page=browser.contexts()[0]?.pages()[0];if(page){await page.screenshot({path:path.join(out,'failure.png'),fullPage:true});console.error(await page.locator('body').ariaSnapshot());}console.error(errors);throw error;}
finally{await browser.close();await server.close();}
