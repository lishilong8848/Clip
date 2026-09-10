import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const out=path.join(root,'output/playwright/cabinet');
await fs.mkdir(out,{recursive:true});
const server=spawn('python',[path.join(root,'bin/tools/cabinet_ui_fixture.py'),'8793'],{cwd:root,windowsHide:true,stdio:['ignore','pipe','pipe']});
let log=''; server.stdout.on('data',x=>log+=x); server.stderr.on('data',x=>log+=x);
let browser, page;
try {
  for(let i=0;i<60;i++) { try { if((await fetch('http://127.0.0.1:8793/api/auth/status')).ok) break; } catch {} if(i===59) throw Error(log); await new Promise(r=>setTimeout(r,500)); }
  browser=await chromium.launch({headless:true});
  page=await browser.newPage({viewport:{width:1440,height:1000}});
  const errors=[]; page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:8793/cabinet-power?scope=D');
  await page.getByRole('heading',{name:'D楼机柜上下电'}).waitFor();
  await page.getByRole('button',{name:'机柜台账',exact:true}).waitFor();
  assert.equal(await page.getByText('预检历史导入').count(),0);
  await page.screenshot({path:path.join(out,'desktop-overview.png'),fullPage:true});
  for (const label of ['已上电','正式电','测试电','未上电 / 已下电']) {
    await page.getByRole('button',{name:'查看'+label+'机柜',exact:true}).click();
    await page.getByRole('table',{name:'机柜状态明细'}).waitFor();
    assert(await page.getByRole('table',{name:'机柜状态明细'}).locator('tbody tr').count()>0);
  }
  await page.screenshot({path:path.join(out,'desktop-state-racks.png'),fullPage:true});
  await page.getByRole('button',{name:'查看平面图',exact:true}).first().click();
  await page.locator('.map-cell.found').first().waitFor();
  await page.screenshot({path:path.join(out,'desktop-located-rack.png'),fullPage:true});
  await page.getByRole('button',{name:'机柜台账',exact:true}).click();
  assert.equal(await page.locator('.source-table th').count(),32);
  await page.getByLabel('筛选历史').selectOption('empty');
  await page.getByText('共 221 条记录',{exact:true}).waitFor();
  await page.screenshot({path:path.join(out,'desktop-idle-racks.png'),fullPage:true});
  await page.getByLabel('筛选历史').selectOption('down');
  await page.getByText('共 285 条记录',{exact:true}).waitFor();
  await page.getByRole('button',{name:'第 6 页',exact:true}).click();
  await page.locator('.page-number.active',{hasText:'6'}).waitFor();
  await page.getByRole('button',{name:'第 1 页',exact:true}).click();
  await page.getByLabel('编辑记录').first().waitFor();
  await page.getByLabel('编辑记录').first().click();
  await page.getByRole('dialog',{name:'编辑机柜记录'}).waitFor();
  await page.getByRole('button',{name:'添加一组'}).waitFor();
  assert.equal(await page.locator('.group-editor.current-operation').count(),1);
  assert(await page.locator('.group-editor.history-operation').count()>0);
  for(let i=0;i<24;i++) await page.getByRole('button',{name:'添加一组',exact:true}).click();
  for(const viewport of [{width:1440,height:1000},{width:1280,height:720},{width:1024,height:768}]) {
    await page.setViewportSize(viewport);
    await page.screenshot({path:path.join(out,'editor-many-'+viewport.width+'.png'),fullPage:true});
    const bounds=await page.locator('.editor').evaluate(modal=>{
      const r=modal.getBoundingClientRect(),body=modal.querySelector('.editor-body'),footer=modal.querySelector('footer').getBoundingClientRect();
      return {top:r.top,bottom:r.bottom,right:r.right,bodyScroll:body.scrollHeight>body.clientHeight,footerBottom:footer.bottom,horizontal:body.scrollWidth>body.clientWidth+2};
    });
    assert(bounds.top>=0 && bounds.bottom<=viewport.height && bounds.right<=viewport.width && bounds.footerBottom<=bounds.bottom+1 && bounds.bodyScroll && !bounds.horizontal,JSON.stringify(bounds));
    await page.locator('.editor-body').evaluate(el=>el.scrollTop=el.scrollHeight);
    assert(await page.getByRole('button',{name:'保存',exact:true}).isVisible());
  }
  await page.setViewportSize({width:1440,height:1000});
  await page.locator('.editor-body').evaluate(el=>el.scrollTop=0);
  await page.screenshot({path:path.join(out,'desktop-editor.png'),fullPage:true});
  await page.getByLabel('功率（W）').fill('12345');
  await page.reload();
  await page.getByRole('dialog',{name:'恢复未保存的机柜记录？'}).waitFor();
  await page.getByRole('button',{name:'恢复编辑',exact:true}).click();
  assert.equal(await page.getByLabel('功率（W）').inputValue(),'12345');
  await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.getByRole('dialog',{name:'放弃当前修改？'}).waitFor();
  await page.getByRole('button',{name:'继续编辑',exact:true}).click();
  await page.getByRole('dialog',{name:'编辑机柜记录'}).waitFor();
  await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.getByRole('button',{name:'放弃修改',exact:true}).click();
  await page.getByRole('button',{name:'原始平面图',exact:true}).click();
  await page.locator('.map-cell').first().waitFor();
  assert(await page.locator('.map-cell').count()>100);
  await page.screenshot({path:path.join(out,'desktop-map.png'),fullPage:true});
  await page.locator('.map-cell').filter({hasText:/^A01$/}).click();
  await page.getByRole('button',{name:'登记正式电',exact:true}).click();
  await page.getByRole('dialog',{name:'编辑机柜记录'}).getByLabel('实际完成时间',{exact:true}).first().fill('2026-09-09T12:00');
  await page.getByLabel('功率（W）').fill('12347');
  let statusFailures=0;
  await page.route('**/api/cabinet-power/writes/*',async route=>{
    if(route.request().method()==='GET' && !statusFailures++) return route.fulfill({status:502,json:{error:'temporary status failure'}});
    return route.continue();
  });
  await page.getByRole('button',{name:'保存',exact:true}).click();
  await page.getByRole('button',{name:'后台继续',exact:true}).click();
  assert.equal(await page.getByRole('dialog',{name:'编辑机柜记录'}).count(),0);
  assert.equal(await page.getByText('已保存到飞书，回读核验成功。',{exact:true}).count(),0);
  await page.getByRole('button',{name:'查看提交内容',exact:true}).first().click();
  assert(await page.getByLabel('功率（W）').isDisabled());
  assert.equal(await page.getByLabel('功率（W）').inputValue(),'12347');
  await page.getByRole('button',{name:'后台继续',exact:true}).click();
  await page.reload();
  await page.getByText('已保存到飞书，回读核验成功。',{exact:true}).waitFor();
  assert.equal(await page.getByRole('dialog',{name:'恢复未保存的机柜记录？'}).count(),0);
  await page.unroute('**/api/cabinet-power/writes/*');
  const changed=await (await fetch('http://127.0.0.1:8793/api/cabinet-power/overview?scope=D')).json();
  assert.equal(changed.data.record_count,988);
  assert.equal(changed.data.racks.find(r=>r.room==='201' && r.rack==='A01').state,'formal');
  await page.getByRole('button',{name:'原始平面图',exact:true}).click();
  await page.locator('.map-cell').filter({hasText:/^B03$/}).click();
  await page.locator('.state-facts').getByText('正式电转测试电',{exact:true}).waitFor();
  assert((await page.locator('.event-times').first().innerText()).includes('期望完成时间'));
  assert((await page.locator('.event-times').first().innerText()).includes('实际完成时间'));
  assert((await page.locator('.state-facts').innerText()).includes('2026-08-19 20:00'));
  await page.getByLabel('关闭历史').click();
  await page.route('**/api/cabinet-power/operations?*',async route=>{
    const url=new URL(route.request().url());
    if(url.searchParams.get('rack')!=='B03') return route.continue();
    const response=await route.fetch(),data=await response.json(),op=data.data.items[0];
    op.source='原始多行历史记录'.repeat(30); op.issues=['原始内容待核实'.repeat(60)];
    op.events=Array.from({length:80},(_,i)=>({...op.events[0],id:'stress-'+i}));
    op.groups=Array.from({length:40},(_,i)=>({...op.groups[0],id:'stress-group-'+i,action:'上正式电\n正式电转测试电',expected:'2026-08-15 17:25:00\n2026-08-20 20:00:00',actual:'2026-08-18 17:57:00\n2026-08-19 20:00:00'}));
    return route.fulfill({response,json:data});
  });
  await page.locator('.map-cell').filter({hasText:/^B03$/}).click();
  await page.locator('.timeline li').nth(79).waitFor({state:'attached'});
  await page.getByText('原始操作内容',{exact:true}).click();
  await page.setViewportSize({width:1024,height:768});
  assert(await page.locator('.drawer-body').evaluate(el=>el.scrollHeight>el.clientHeight && el.scrollWidth<=el.clientWidth+2));
  await page.screenshot({path:path.join(out,'drawer-many-1024.png'),fullPage:true});
  await page.locator('.history-record').getByRole('button',{name:'编辑',exact:true}).click();
  assert.equal(await page.locator('.group-editor').count(),40);
  await page.locator('.group-editor textarea').first().evaluate(el=>el.style.height='420px');
  assert(await page.locator('.editor-body').evaluate(el=>el.scrollHeight>el.clientHeight && el.scrollWidth<=el.clientWidth+2));
  assert(await page.locator('.editor footer').evaluate(el=>el.getBoundingClientRect().bottom<=window.innerHeight));
  await page.screenshot({path:path.join(out,'editor-multiline-1024.png'),fullPage:true});
  await page.getByLabel('功率（W）').fill('12346');
  await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.getByRole('dialog',{name:'放弃当前修改？'}).waitFor();
  assert(await page.locator('.cabinet-page .confirm-modal').evaluate(el=>{const r=el.getBoundingClientRect();return r.top>=0 && r.bottom<=window.innerHeight && el.scrollWidth<=el.clientWidth+2;}));
  await page.screenshot({path:path.join(out,'confirm-1024.png'),fullPage:true});
  await page.getByRole('button',{name:'放弃修改',exact:true}).click();
  await page.getByLabel('关闭历史').click();
  await page.unroute('**/api/cabinet-power/operations?*');
  await page.setViewportSize({width:1440,height:1000});
  for(const scope of ['A','B','C','E']) {
    await page.goto('http://127.0.0.1:8793/cabinet-power?scope='+scope);
    await page.getByRole('heading',{name:scope+'楼机柜上下电'}).waitFor();
    const expectedPowered={A:970,B:1022,C:970,E:402}[scope];
    await page.getByRole('button',{name:'查看已上电机柜',exact:true}).waitFor();
    assert.equal(await page.getByRole('button',{name:'查看已上电机柜',exact:true}).locator('strong').innerText(),String(expectedPowered));
    await page.getByRole('button',{name:'查看已上电机柜',exact:true}).click();
    await page.getByText(expectedPowered+' 个已编号机柜',{exact:true}).waitFor();
    if(scope==='C') await page.screenshot({path:path.join(out,'desktop-c-powered-racks.png'),fullPage:true});
    if(scope==='B') {
      await page.getByRole('navigation',{name:'机柜视图'}).getByRole('button',{name:'运营商机房',exact:true}).click();
      await page.getByRole('heading',{name:'B-216运营商机房'}).waitFor();
      await page.getByRole('heading',{name:'B-247运营商机房'}).waitFor();
      assert.equal(await page.getByRole('table',{name:'机柜状态明细'}).locator('tbody tr').count(),12);
      await page.screenshot({path:path.join(out,'desktop-b-carrier.png'),fullPage:true});
    }
    await page.getByRole('button',{name:'原始平面图',exact:true}).click();
    await page.locator('.map-cell').first().waitFor();
    assert(await page.locator('.map-cell').count()>100);
    if(scope==='C') {
      await page.getByRole('button',{name:/202 包间/}).click();
      await page.locator('.map-cell').filter({hasText:/^659$/}).waitFor();
      await page.locator('.map-cell').filter({hasText:/^311$/}).waitFor();
      await page.locator('.map-cell').filter({hasText:/^970$/}).waitFor();
      await page.screenshot({path:path.join(out,'desktop-c-color-map.png'),fullPage:true});
    }
  }
  await page.getByRole('button',{name:'机柜台账',exact:true}).click();
  await page.getByLabel('筛选历史').selectOption('empty');
  await page.getByText('共 870 条记录',{exact:true}).waitFor();
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+2));
  await page.screenshot({path:path.join(out,'desktop-e-records.png'),fullPage:true});
  await page.getByLabel('编辑记录').first().click();
  await page.getByRole('dialog',{name:'编辑机柜记录'}).waitFor();
  const power=page.getByLabel('功率（W）'); await power.fill('16000');
  await page.getByRole('button',{name:'保存',exact:true}).click();
  await page.getByText('已保存到飞书，回读核验成功。',{exact:true}).waitFor();
  await page.getByRole('button',{name:'原始平面图',exact:true}).click();
  await page.locator('.map-cell').first().waitFor();
  await page.screenshot({path:path.join(out,'desktop-e-map.png'),fullPage:true});
  await page.locator('.heading').getByRole('button',{name:/返回/}).click();
  await page.getByRole('button').filter({has:page.getByRole('heading',{name:'E楼',exact:true})}).click();
  await page.getByRole('button',{name:'机柜台账',exact:true}).click();
  await page.getByLabel('编辑记录').first().click();
  await page.getByLabel('功率（W）').fill('23456');
  await page.evaluate(()=>history.back());
  await page.getByRole('dialog',{name:'放弃当前修改？'}).waitFor();
  await page.keyboard.press('Escape');
  assert.equal(await page.getByLabel('功率（W）').inputValue(),'23456');
  await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.getByRole('button',{name:'放弃修改',exact:true}).click();
  await page.locator('.heading').getByRole('button',{name:/返回/}).click();
  await page.getByRole('heading',{name:'机柜上下电',exact:true}).waitFor();
  assert.equal(errors.length,0,errors.join('\n'));
  console.log(JSON.stringify({ok:true,views:'PC: five building maps, history filters, grouped editor, save/readback',screenshots:out}));
} catch(error) {
  console.log(log);
  if(page) { console.log((await page.locator('body').innerText()).slice(0,4000)); await page.screenshot({path:path.join(out,'failure.png'),fullPage:true}); }
  throw error;
} finally {
  await browser?.close(); server.kill();
}
