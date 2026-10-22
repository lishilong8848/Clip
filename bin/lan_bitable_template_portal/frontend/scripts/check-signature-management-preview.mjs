import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { layoutApiFixture } from './layout-api-fixture.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const workspace = process.argv.includes('--workspace');
const output = path.resolve(root, '../../../output/playwright/signature-management', workspace ? 'v2' : 'legacy');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const entry = workspace ? '/assets/workspace-v2/index.html?__page=/signature-management' : '/signature-management';
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, serviceWorkers: 'block', reducedMotion: 'reduce' });
page.setDefaultTimeout(12000);
const errors = [], requests = [], previewRequests = [], apiPosts = [];
let role = 'admin';
let previewFailNext = false;
const layouts = layoutApiFixture(), creations = new Map(), deliveries = new Map(), recipientReads = [];
let allowCreate = true, createScopes = [{ value: 'A', label: 'A楼' }], recipientFailure = '', recipientGate = null;
let loseCreate = false, loseSend = false, sendStatus = 'sent';
const relays = Array.from({length:602}, (_,i) => ({record_id:'relay-'+i,name:'代收人员'+i,employee_no:'EMP'+i,building:i?'B楼':'A楼',position:'工程师',team:'一班',specialty:'暖通'}));

const PNG_SIGNATURE = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAMgAAABQCAYAAABcbTqwAAAAxUlEQVR4nO3bMQrAIAAEwfz/0wYsU2xKo5mB6wXd0usCAAAAAAAAAAAAAAB4NVYfAL5MIBAEAkEgEARyuGG2cBzOJUMQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgTCt/rtsew0mjwGCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCOSHVv9nNnuOTbk8CAKBIBAIAgEAAAAAAAAAAAAAAMoNzMn+EJC9rUEAAAAASUVORK5CYII=',
  'base64',
);

const people = [
  { person_key: 'staff:rec_staff_1', record_id: 'rec_staff_1', name: '张三', employee_no: 'A001', building: 'A楼',
    source: 'staff', position: '工程师', account_nature: '正式', can_receive_message: true,
    has_signature: true, signature_status: 'signed', signature_reason: '', effective_has_signature: true,
    effective_source: 'staff' },
  { person_key: 'external:rec_ext_1', record_id: 'rec_ext_1', name: '李四', employee_no: 'B002', building: 'B楼',
    source: 'external', position: '临时人员', official_name: '李四',
    has_signature: true, signature_status: 'signed', signature_reason: '', effective_has_signature: true,
    effective_source: 'external' },
  { person_key: 'staff:rec_staff_2', record_id: 'rec_staff_2', name: '王五', employee_no: 'A003', building: 'A楼',
    source: 'staff', position: '工程师', account_nature: '正式', can_receive_message: true,
    has_signature: false, signature_status: 'unsigned', signature_reason: '', effective_has_signature: false,
    effective_source: 'staff' },
];
const duplicateGroups = [{
  name: '张三',
  people: [
    { record_id: 'rec_dup_1', name: '张三', employee_no: 'D001', building: 'D楼', source: 'external', has_signature: true },
    { record_id: 'rec_dup_2', name: '张三', employee_no: 'D002', building: 'D楼', source: 'external', has_signature: true },
  ],
}];

page.on('pageerror', error => errors.push(error.message));
await page.route('**/*', async route => {
  const req = route.request(), url = new URL(req.url());
  if (url.origin !== origin) return route.abort();
  const pathname = url.pathname;
  if (!pathname.startsWith('/api/')) return route.continue();
  if (await layouts.handle(route,role)) return;
  requests.push([req.method(), pathname, url.search]);
  if (pathname === '/api/signatures/management/preview') {
    previewRequests.push([req.method(), pathname, url.search]);
    if(role!=='admin')assert.equal(url.searchParams.get('source'),'external','ordinary staff must never request protected formal previews');
    assert.notEqual(url.searchParams.get('record_id'),'linked-protected','linked temporary signatures remain protected');
    if (previewFailNext) {
      previewFailNext = false;
      return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ ok: false, error: '签名图片读取失败，请重试。' }) });
    }
    assert.equal(req.method(), 'GET');
    assert.ok(url.searchParams.get('record_id'), 'preview must carry record_id');
    assert.ok(['staff', 'external'].includes(url.searchParams.get('source')), 'preview source must be staff or external');
    assert.ok(url.searchParams.get('_rt'), 'preview must cache-bust on each open/retry');
    return route.fulfill({ status: 200, contentType: 'image/png', body: PNG_SIGNATURE });
  }
  if (req.method() === 'POST') apiPosts.push([req.method(), pathname, req.postDataJSON?.() || null]);
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (pathname === '/api/auth/status') return ok({
    logged_in: true, user: { open_id: 'signature-fixture', role, level:role==='admin'?'L0':'L4',category:'operator',home_scope:'A',scope_rules:['A'],modules:role==='denied'?[]:['signatures'], name: role === 'admin' ? '管理员' : '普通用户' },
    scope_options: [{ value: 'A', label: 'A楼' }], login_url: '/api/auth/login',
  });
  if (pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-signature' } });
  if (pathname === '/api/signatures/management/people') {
    assert.equal(url.searchParams.has('message_capable'),false,'relay selection must not reuse the scoped/paginated directory');
    const rows=people.map(p=>({...p,can_preview:role==='admin'||p.source==='external',can_request:role==='admin'||p.source==='external'}));
    if(role==='user')rows.push(
      {person_key:'staff:recBoss',record_id:'recBoss',name:'验收上级',source:'staff',building:'H楼',position:'经理',signature_status:'restricted',invite_only:true,can_request:true,can_preview:false,can_receive_message:true},
      {person_key:'staff:recCross',record_id:'recCross',name:'跨楼目录人员',employee_no:'C900',source:'staff',building:'C楼',position:'运维主管',signature_status:'restricted',directory_only:true,invite_only:false,can_request:false,can_preview:false,can_receive_message:true},
      {person_key:'external:shared-other',record_id:'shared-other',name:'其他人员创建的临时签名',source:'external',building:'C楼',has_signature:false,signature_status:'unsigned',can_request:true,can_preview:false},
      {person_key:'external:linked-protected',record_id:'linked-protected',name:'受保护的已关联签名',source:'external',building:'H楼',origin_staff_record_id:'recBoss',has_signature:true,signature_status:'restricted',can_request:false,can_preview:false},
    );
    rows.push(...creations.values());
    return ok({people:rows,can_manage_directory:role==='admin',can_create_temporary:allowCreate,create_scope_options:createScopes,scope_options:['A','B','C','H'].map(value=>({value,label:value+'楼'})),recipients:[{value:'A',label:'A楼值班账号'}],sources:{staff:{ok:true,loaded_at:1791510000},external:{ok:true,loaded_at:1791510000}},counts:{signed:2,unsigned:1,resign:0},count:rows.length});
  }
  if(pathname==='/api/signatures/management/recipients'){
    assert.equal(req.method(),'GET');
    assert.deepEqual([...url.searchParams.keys()].sort(),['record_id','source'],'recipient query contains the subject, not the sender building or a pagination cutoff');
    const id=url.searchParams.get('record_id');assert(id);assert.notEqual(id,'recBoss','upward invitations never fetch relay people');recipientReads.push({source:url.searchParams.get('source'),id});
    const candidates=url.searchParams.get('source')==='external'&&id!=='linked-protected'?relays:relays.slice(0,1);
    if(recipientGate){const gate=recipientGate;recipientGate=null;gate.started();await gate.promise;return ok({people:candidates}).catch(()=>{});}
    if(recipientFailure==='error')return route.fulfill({status:503,json:{ok:false,error:'隔离测试：接收人员读取失败'}});
    if(recipientFailure==='malformed')return ok({});
    return ok({people:candidates});
  }
  if(pathname==='/api/signatures/management/temporary'){
    const body=req.postDataJSON();assert.equal(req.method(),'POST');assert(allowCreate);assert(createScopes.some(scope=>scope.label===body.building));assert(body.operation_id);
    if(!creations.has(body.operation_id))creations.set(body.operation_id,{...body,person_key:'external:new-'+creations.size,record_id:'new-'+creations.size,source:'external',has_signature:false,signature_status:'unsigned',can_request:true,can_preview:false});
    if(loseCreate){loseCreate=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试：新增回执未取得'}});}
    return ok(creations.get(body.operation_id));
  }
  if(pathname==='/api/signatures/management/requests'){
    const body=req.postDataJSON();assert.equal(req.method(),'POST');assert(body.operation_id);
    if(body.record_id==='recBoss')assert.equal(body.recipient,'','upward requests must go directly to the subject');
    if(!deliveries.has(body.operation_id))deliveries.set(body.operation_id,body);
    else assert.deepEqual(body,deliveries.get(body.operation_id),'an uncertain retry must retain the original operation and recipient');
    if(loseSend){loseSend=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试：发送回执未取得'}});}
    return ok({status:sendStatus,recipient_label:body.record_id==='recBoss'?'验收上级':'跨楼代收人员',...(sendStatus==='sent'?{}:{error:'隔离测试：发送结果待核验'})});
  }
  if (pathname === '/api/signatures/management/duplicates') {assert.equal(role,'admin');return ok({ groups: duplicateGroups.map(group=>({...group,people:group.people.map(p=>({...p,can_preview:true,can_request:true}))})) });}
  assert.equal(req.method(),'GET','No unexpected business action');
  return ok({});
});

async function openPreviewAndCheckLoaded(expectedPreviewCount) {
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  const dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(previewRequests.length, expectedPreviewCount, 'exactly one signature loaded per open attempt (no bulk previews)');
  const img = page.locator('.preview-area img');
  assert.ok((await img.evaluate(el => el.naturalWidth)) > 0, 'visible image must have decoded naturalWidth');
  return dlg;
}

try {
  // ---- Admin flow ----
  await page.goto(origin + entry);
  await page.getByText('张三', { exact: true }).first().waitFor();
  assert.equal(previewRequests.length, 0, 'no signature preview requests during initial list load');
  assert.equal(await page.getByRole('button', { name: '查看签名', exact: true }).count(), 2, 'admin sees preview for each signed person');
  const unsignedCard = page.locator('.person-card').filter({ hasText: '王五' }).first();
  assert.equal(await unsignedCard.getByRole('button', { name: '查看签名', exact: true }).count(), 0, 'unsigned row exposes no preview control');
  assert.equal(await unsignedCard.locator('img').count(), 0, 'unsigned row renders no signature image');

  // First successful preview from a normal person row.
  let dlg = await openPreviewAndCheckLoaded(1);
  assert.match((await dlg.getAttribute('aria-label')) || '', /张三/, 'preview dialog aria-label carries person name');
  const previewImg = page.locator('.preview-area img');
  assert.equal(await previewImg.evaluate(el => el.naturalWidth), 200, 'synthetic preview naturalWidth must be 200px (not 1px)');
  assert.equal(await previewImg.evaluate(el => el.naturalHeight), 80, 'synthetic preview naturalHeight must be 80px (not 1px)');
  await page.screenshot({ path: path.join(output, 'admin-preview-desktop.png'), fullPage: true });

  // Accidental Enter/submit on the preview form must not send a signing link.
  await dlg.evaluate(() => {
    const form = document.querySelector('dialog form');
    form.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
  });
  await page.waitForTimeout(120);
  assert.equal(apiPosts.length, 0, 'dispatching submit on the preview form must not issue any business POST');

  await dlg.getByRole('button', { name: '关闭' }).first().click();
  assert.equal(await page.locator('.preview-area img').count(), 0, 'close removes preview bytes from DOM');

  // Re-open preview and switch to send form without POSTing.
  dlg = await openPreviewAndCheckLoaded(2);
  await dlg.getByRole('button', { name: '重新签名', exact: true }).click();
  const sendDlg = page.getByRole('dialog', { name: '发送签名链接', exact: true });
  await sendDlg.waitFor();
  assert.equal(apiPosts.length, 0, 're-sign opens the send form without any business POST');
  assert.equal(await sendDlg.count(), 1, 'send form dialog has correct aria label');
  assert.ok(await sendDlg.getByText(/张三/).count() > 0, 're-sign opens send form for the correct signed person (张三)');
  assert.equal(await sendDlg.locator('.preview-area img').count(), 0, 're-sign send form has no leftover preview image');
  await sendDlg.getByRole('button', { name: '取消', exact: true }).click();

  // Retry after a failed read.
  previewFailNext = true;
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.getByText('签名图片读取失败，请重试。', { exact: true }).waitFor();
  assert.equal(previewRequests.length, 3, 'failed attempt issues one preview request');
  await page.screenshot({ path: path.join(output, 'admin-preview-error.png'), fullPage: true });
  await dlg.getByRole('button', { name: '重试', exact: true }).click();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(previewRequests.length, 4, 'retry rerequests with a fresh cache-bust');
  const failedRt = new URLSearchParams(previewRequests[2][2]).get('_rt');
  const retryRt = new URLSearchParams(previewRequests[3][2]).get('_rt');
  assert.ok(failedRt && retryRt && failedRt !== retryRt, 'open and retry cache-busters must differ');
  await page.screenshot({ path: path.join(output, 'admin-preview-retry-desktop.png'), fullPage: true });
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  // Duplicate signed rows: preview must not toggle the merge checkbox.
  await page.getByRole('button', { name: '重复人员核对', exact: true }).click();
  await page.getByText('重复人员预检', { exact: true }).waitFor();
  const dupRow = page.locator('.duplicate-row').filter({ hasText: '张三' }).first();
  const checkbox = dupRow.locator('input[type=checkbox]').first();
  const before = await checkbox.isChecked();
  await dupRow.getByRole('button', { name: '查看签名', exact: true }).click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(await checkbox.isChecked(), before, 'preview from duplicate row must not toggle merge checkbox');
  assert.equal(previewRequests.length, 5);
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  // Narrow desktop screenshot of a live preview.
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(await page.locator('.preview-area img').evaluate(el => el.naturalWidth), 200, 'narrow preview retains synthetic width 200');
  assert.equal(await page.locator('.preview-area img').evaluate(el => el.naturalHeight), 80, 'narrow preview retains synthetic height 80');
  await page.screenshot({ path: path.join(output, 'admin-preview-narrow.png'), fullPage: true });
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  assert.deepEqual(errors, []);

  // ---- Normal user flow (fresh navigation resets app auth state) ----
  const previewCountBeforeUserNav = previewRequests.length;
  role = 'user';
  await page.goto(origin + entry);
  await page.getByText('张三', { exact: true }).first().waitFor();
  const crossDirectory = page.locator('.person-card').filter({hasText:'跨楼目录人员'});
  await crossDirectory.waitFor();
  assert.equal(await crossDirectory.getByText('签名受限', {exact:true}).count(), 1);
  assert.equal(await crossDirectory.getByRole('button').count(), 0, 'directory visibility grants no signature actions');
  assert.equal(await page.locator('.person-card').filter({hasText:'张三'}).getByRole('button', { name: '查看签名', exact: true }).count(), 0, 'normal user gets no unauthorized formal preview');
  assert.equal(await page.locator('.person-card').filter({hasText:'受保护的已关联签名'}).getByRole('button', { name: '查看签名', exact: true }).count(), 0, 'linked temporary signatures keep formal authority');
  assert.equal(await page.getByRole('button', { name: '查看签名', exact: true }).count(), 1, 'shared unlinked temporary signature remains usable');
  assert.equal(previewRequests.length, previewCountBeforeUserNav, 'normal user triggers no additional signature image requests');
  assert.equal(await page.locator('.preview-area img').count(), 0, 'normal user renders no preview image');
  const superior=page.locator('.person-card').filter({hasText:'验收上级'});
  await superior.getByRole('button',{name:'发送签名邀请',exact:true}).click();
  const invite=page.getByRole('dialog',{name:'发送签名链接',exact:true});await invite.waitFor();
  assert.equal(await invite.getByRole('button',{name:/发送给本人/}).count(),1);
  assert.equal(await invite.getByRole('group',{name:'值班账号代收'}).count(),0);
  assert.equal(await invite.getByPlaceholder('搜索姓名、工号、岗位或楼栋').count(),0);
  assert.equal(apiPosts.length,0,'opening an upward invitation only previews');
  await invite.getByRole('button',{name:'发送链接',exact:true}).click();
  await invite.waitFor({state:'hidden'});
  assert.equal(apiPosts.length,1);
  assert.equal(apiPosts[0][2].record_id,'recBoss');
  assert.equal(apiPosts[0][2].recipient,'');
  await page.getByText('签名链接已发送至验收上级，等待本人签名。',{exact:true}).waitFor();
  await page.screenshot({ path: path.join(output, 'normal-user-no-preview.png'), fullPage: true });

  const sharedCard=()=>page.locator('.person-card').filter({hasText:'李四'});
  const sendDialog=()=>page.getByRole('dialog',{name:'发送签名链接',exact:true});
  const candidate=dialog=>dialog.locator('.staff-results > button').filter({hasText:'EMP601'});
  async function waitRecipients(dialog){await page.waitForFunction(()=>document.querySelectorAll('dialog[open] .staff-results > button').length===602);assert.equal(await dialog.locator('.staff-results > button').count(),602,'all eligible recipients, including beyond 500, are available');}
  async function selectCrossBuilding(dialog){await dialog.getByPlaceholder('搜索姓名、工号、岗位或楼栋',{exact:true}).fill('EMP601');await candidate(dialog).click();assert.equal(await dialog.locator('.staff-results > button').count(),1);assert.match(await candidate(dialog).innerText(),/B楼/);}
  async function doubleSubmit(dialog){await dialog.locator('form').evaluate(form=>{form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));});}
  for(const viewport of [{width:1440,height:900},{width:402,height:874},{width:440,height:956}]){
    await page.setViewportSize(viewport);await page.goto(origin+entry);await sharedCard().waitFor();
    assert.equal(await page.getByRole('button',{name:'重复人员核对',exact:true}).count(),0,'creation permission does not expose directory merge');
    assert.equal(await page.getByRole('button',{name:'关联正式人员',exact:true}).count(),0,'creation permission does not expose association');
    await page.getByText('其他人员创建的临时签名',{exact:true}).waitFor();
    assert.equal(await sharedCard().getByRole('button',{name:'查看签名',exact:true}).count(),1,'another creator/building does not hide shared unlinked signatures');
    await sharedCard().getByRole('button',{name:'查看签名',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('.preview-area img')?.naturalWidth===200);
    await page.getByRole('dialog',{name:/签名预览/}).locator('footer').getByRole('button',{name:'关闭',exact:true}).click();
    await page.getByRole('button',{name:'新增临时人员',exact:true}).click();
    const create=page.getByRole('dialog',{name:'新增临时人员',exact:true});await create.waitFor();
    const building=create.locator('select');
    assert.deepEqual(await building.locator('option').allTextContents(),['请选择','A楼'],'creation uses actor scopes, never B/C/H scopes from the shared directory');
    await create.getByLabel('姓名',{exact:true}).fill('新增临时'+viewport.width);
    if(workspace){
      await create.getByRole('combobox',{name:'楼栋',exact:true}).click();
      const option=page.locator('.n-base-select-option').getByText('A楼',{exact:true});await option.waitFor();
      assert(await option.evaluate(el=>Boolean(el.closest('dialog[open]'))),'building menu stays in the native dialog top layer');
      await option.click();
    }
    else await building.selectOption('A楼');
    await create.getByLabel('专业',{exact:true}).fill('暖通');
    const createPosts=()=>apiPosts.filter(row=>row[1].endsWith('/temporary'));
    const beforeCreatePosts=createPosts().length,beforeCreations=creations.size;
    loseCreate=true;await doubleSubmit(create);await create.getByText('隔离测试：新增回执未取得',{exact:true}).waitFor();
    assert.equal(createPosts().length,beforeCreatePosts+1,'double submit starts one create request');
    const createId=createPosts().at(-1)[2].operation_id;
    await create.getByRole('button',{name:'新增并继续',exact:true}).click();
    const send=sendDialog();await send.waitFor();await waitRecipients(send);
    assert.equal(createPosts().at(-1)[2].operation_id,createId,'lost create receipt reuses its operation');
    assert.equal(creations.size,beforeCreations+1,'create recovery never duplicates the person');
    assert.equal(await send.getByRole('group',{name:'值班账号代收'}).getByRole('button').count(),1,'pinned duty choices remain scoped');
    await selectCrossBuilding(send);
    const submit=send.getByRole('button',{name:'发送链接',exact:true});await submit.scrollIntoViewIfNeeded();await submit.click({trial:true});
    await submit.evaluate(el=>Promise.all(el.getAnimations().map(animation=>animation.finished.catch(()=>{}))));
    const contrast=await submit.evaluate(el=>{
      const light=color=>color.match(/[\d.]+/g).slice(0,3).map(Number).map(value=>value/255).map(value=>value<=0.04045?value/12.92:((value+0.055)/1.055)**2.4).reduce((sum,value,index)=>sum+value*[0.2126,0.7152,0.0722][index],0);
      const s=getComputedStyle(el),a=light(s.color),b=light(s.backgroundColor);return (Math.max(a,b)+0.05)/(Math.min(a,b)+0.05);
    });
    assert(contrast>=4.5,'hovered primary signature button keeps readable text');
    assert(await send.evaluate(el=>{const r=el.getBoundingClientRect();return r.left>=0&&r.right<=innerWidth+1&&r.top>=0&&r.bottom<=innerHeight+1;}),'send dialog fits viewport');
    await page.screenshot({path:path.join(output,'cross-building-'+viewport.width+'.png')});
    const beforePosts=apiPosts.length,beforeDeliveries=deliveries.size;
    loseSend=true;await doubleSubmit(send);await send.getByText('隔离测试：发送回执未取得',{exact:true}).waitFor();
    assert.equal(apiPosts.length,beforePosts+1,'double submit starts one send request');
    const original=apiPosts.at(-1)[2];assert.equal(original.recipient,'staff:relay-601');
    for(sendStatus of ['send_unknown','sending','saving']){
      await submit.click();await send.getByText('隔离测试：发送结果待核验',{exact:true}).waitFor();
      assert.deepEqual(apiPosts.at(-1)[2],original,'unknown/in-flight send retries preserve the operation ID and subject');
      assert.equal(deliveries.size,beforeDeliveries+1,'unknown retries create no second delivery side effect');
      assert.equal(await send.isVisible(),true,'uncertain outcomes never report success');
    }
    sendStatus='sent';await submit.click();await send.waitFor({state:'hidden'});
    assert.equal(deliveries.size,beforeDeliveries+1);assert.equal(creations.size,beforeCreations+1);
    await page.getByText('签名链接已发送至跨楼代收人员，等待本人签名。',{exact:true}).waitFor();

    await sharedCard().getByRole('button',{name:'重新签名',exact:true}).click();await waitRecipients(send);await selectCrossBuilding(send);
    await send.getByRole('button',{name:'取消',exact:true}).click();
    recipientFailure='error';await sharedCard().getByRole('button',{name:'重新签名',exact:true}).click();
    await send.getByRole('alert').filter({hasText:'隔离测试：接收人员读取失败'}).waitFor();
    assert.equal(await send.locator('.staff-results > button').count(),0,'failed fetch discards previous candidates');
    assert(await submit.isDisabled(),'failed fetch cannot submit a previously selected recipient');
    recipientFailure='';await send.getByRole('button',{name:'重新读取接收人',exact:true}).click();await waitRecipients(send);
    assert(await submit.isDisabled(),'retry requires a fresh deliberate recipient selection');
    await send.getByRole('button',{name:'取消',exact:true}).click();
    recipientFailure='malformed';await sharedCard().getByRole('button',{name:'重新签名',exact:true}).click();
    await send.getByRole('alert').filter({hasText:'接收人员信息不完整，请重新读取。'}).waitFor();assert(await submit.isDisabled());
    await send.getByRole('button',{name:'取消',exact:true}).click();recipientFailure='';

    let release,started;const readStarted=new Promise(resolve=>{started=resolve;});
    recipientGate={started,promise:new Promise(resolve=>{release=resolve;})};
    await sharedCard().getByRole('button',{name:'重新签名',exact:true}).click();await readStarted;
    await send.getByRole('button',{name:'取消',exact:true}).click();
    const beforeUpward=recipientReads.length;
    await page.locator('.person-card').filter({hasText:'验收上级'}).getByRole('button',{name:'发送签名邀请',exact:true}).click();release();
    await send.getByRole('button',{name:/发送给本人/}).waitFor();
    assert.equal(await send.locator('.staff-results,.recipient-pinned').count(),0,'late external directory never appears in an upward invitation');
    assert.equal(recipientReads.length,beforeUpward,'upward invitation never queries relay data');
    await send.getByRole('button',{name:'取消',exact:true}).click();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    console.log('PASS '+(workspace?'v2':'legacy')+' '+viewport.width+'x'+viewport.height+': shared temporary rows, scoped create, cross-building relay, stale/failure isolation, idempotent retries and upward protection');
  }
  allowCreate=false;await page.goto(origin+entry);await sharedCard().waitFor();
  assert.equal(await page.getByRole('button',{name:'新增临时人员',exact:true}).count(),0,'server permission controls create entry');
  allowCreate=true;createScopes=[];await page.goto(origin+entry);await sharedCard().waitFor();
  assert.equal(await page.getByRole('button',{name:'新增临时人员',exact:true}).count(),0,'no allowed creation building means no entry');
  role='denied';const beforeDenied=apiPosts.length;await page.goto(origin+entry);await page.getByText('没有该功能权限',{exact:true}).waitFor();
  assert.equal(await page.locator('.signature-management').count(),0,'without the signature module no signature-management surface mounts');assert.equal(apiPosts.length,beforeDenied);
  assert.deepEqual(errors,[]);

  console.log(JSON.stringify({ ok: true, screenshots: output, checks: 'admin preview/retry and merge selection preserved; scoped non-admin creation; shared unlinked temporary signatures; 602 cross-building recipient candidates; no unauthorized formal/linked preview; upward direct-only invitation; failed/malformed/late recipient isolation; stable create/send IDs through lost and uncertain receipts; 1440x900,402x874,440x956' }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
