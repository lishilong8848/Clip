import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {auditUrl} from './responsive-audit-pages.mjs';

const origin = new URL(process.argv[2]);
assert.equal(origin.hostname, '127.0.0.1');
assert.equal((await (await fetch(new URL('/api/health',origin))).json()).instance_id, 'responsive-isolated-preview');
const rows = [
  {target_record_id:'demo-a',building:'A楼',building_codes:['A'],title:'一层冷机控制器冗余切换',started_at:'2026-11-19 09:30',state:'awaiting_confirmation',
    screenshot_items:[{file_token:'demo-proof',name:'现场确认.png'}],screenshot_count:1},
  {target_record_id:'demo-b',building:'A楼',building_codes:['A'],title:'机柜支路监控校验',state:'missing_screenshot'},
  {target_record_id:'demo-c',building:'C楼',building_codes:['C'],title:'其他楼记录',state:'missing_screenshot'},
];
const browser=await chromium.launch(), writes=[], errors=[];
try {
  const context=await browser.newContext({viewport:{width:1380,height:880}});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin.origin)return route.abort();
    if(url.pathname==='/api/change-confirmations')return route.fulfill({json:{ok:true,data:{items:rows,can_confirm:true}}});
    if(url.pathname==='/api/change-confirmations/demo-a/screenshot/preview'){
      assert.equal(url.searchParams.get('file_token'),'demo-proof');
      return route.fulfill({contentType:'image/png',body:Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZQAAAABJRU5ErkJggg==','base64')});
    }
    if(req.method()==='POST' && url.pathname==='/api/change-confirmations/demo-a/confirm'){
      writes.push(url.pathname);rows[0].state='confirmed';return route.fulfill({json:{ok:true,data:{}}});
    }
    if(!['GET','HEAD'].includes(req.method()))return route.fulfill({status:405,json:{ok:false,error:'测试不允许这项写入'}});
    return route.continue();
  });
  const page=await context.newPage();page.setDefaultTimeout(8000);page.on('pageerror',e=>errors.push(e.message));
  await page.goto(auditUrl(origin.origin,'v2','/workbench-lite?scope=A&work_type=change&change_confirmation=demo-a'));
  const modal=page.locator('.change-inbox'),proof=modal.locator('.change-proof');
  await proof.getByRole('heading',{name:rows[0].title}).waitFor();
  assert.equal(await modal.getByText('其他楼记录').count(),0);
  await proof.getByText('变更开始 · 2026-11-19 09:30').waitFor();
  await proof.getByRole('img',{name:'现场确认.png'}).click();await page.locator('.n-image-preview').waitFor();
  await page.keyboard.press('Escape');await page.locator('.n-image-preview').waitFor({state:'hidden'});
  await modal.locator('.change-queue button').filter({hasText:rows[1].title}).click();
  assert(await proof.getByRole('button',{name:'确认变更'}).isDisabled());
  await page.setViewportSize({width:390,height:844});
  await modal.locator('.change-queue button').filter({hasText:rows[0].title}).click();
  await proof.getByRole('button',{name:'确认变更'}).click();
  await page.locator('.n-dialog').getByRole('button',{name:'确认变更'}).click();
  await proof.getByText('已确认',{exact:true}).waitFor();
  assert.deepEqual(writes,['/api/change-confirmations/demo-a/confirm']);
  assert(await proof.getByRole('button',{name:'确认变更'}).isDisabled());
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  let edited;
  await page.route('**/api/critical-guard/responses/*',async route=>{
    const req=route.request();
    assert.equal(req.method(),'PUT');
    const body=req.postDataJSON(), id=new URL(req.url()).pathname.split('/').pop();
    assert.equal(body.scope,'A');assert.equal(body.generate_image,false);assert(body.operation_id);
    assert.equal(edited,undefined,'one save produces one update');
    edited=body.cells;
    return route.fulfill({json:{ok:true,data:{response_id:id,sheet_type:'检查清单',scope:'A',cells:edited,status:'draft',version:body.expected_version+1}}});
  });
  await page.goto(auditUrl(origin.origin,'v2','/critical-guard?scope=A'));
  const form=page.locator('.guard-sheet'), first=form.locator('.guard-checks>div').first();
  await first.getByRole('button',{name:'异常',exact:true}).click();
  await first.getByLabel('异常说明').fill('需核对泵房水位');
  await form.getByRole('button',{name:'保存',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.guard-sheet header')?.textContent.includes('已保存'));
  assert(Object.values(edited.checks).some(row=>row.status==='abnormal'&&row.note==='需核对泵房水位'));
  assert(await form.getByRole('button',{name:'保存',exact:true}).isDisabled());
  await form.getByRole('button',{name:'编辑模板'}).click();
  await page.getByRole('dialog',{name:'编辑检查模板'}).waitFor();
  await page.keyboard.press('Escape');await page.getByRole('dialog',{name:'编辑检查模板'}).waitFor({state:'hidden'});
  const cold=await context.newPage();
  let resumeStyle, styleRequested;
  const held=new Promise(resolve=>{resumeStyle=resolve});
  const pendingStyle=new Promise(resolve=>{styleRequested=resolve});
  await cold.route('**/assets/workspace-v2/*.css',async route=>{
    styleRequested();await held;
    return route.continue();
  });
  await cold.goto(auditUrl(origin.origin,'v2','/?view=my'),{waitUntil:'commit'});
  await pendingStyle;
  await cold.locator('#entry-status').waitFor({state:'attached'});
  assert.equal(await cold.locator('.v2-shell').count(),0,'the UI cannot mount with missing layout styles');
  resumeStyle();
  await cold.locator('.identity-desk').waitFor();
  await cold.unroute('**/assets/workspace-v2/*.css');
  await cold.route('**/assets/workspace-v2/*.css',route=>route.abort());
  await cold.reload({waitUntil:'domcontentloaded'});
  await cold.getByRole('button',{name:'重新加载新版'}).waitFor();
  assert.equal(await cold.locator('.v2-shell').count(),0,'a failed style resource must offer recovery');
  assert.deepEqual(errors,[]);
  console.log('PASS: change confirmation, checklist draft, modal exit and cold-start style recovery.');
} finally {await browser.close();}

