import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { layoutApiFixture } from './layout-api-fixture.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, process.env.CLIPFLOW_MOP_SCREENSHOTS || '../../../output/playwright/workspace-mop');
const before = process.argv.includes('--before');
const site = process.env.CLIPFLOW_MOP_PREVIEW_SITE || path.resolve(root, before ? '../../../output/header-polish-build' : 'dist');
await mkdir(output, { recursive: true });
const server = await preview({ root, configFile: false, logLevel: 'error', build: { outDir: site }, preview: { host: '127.0.0.1', port: 0 } });
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [], writes = [], reads = [], fileActions = [];
let failUpload=false, releaseUpload;
const layouts = layoutApiFixture();
const titles = ['冷水机组月度维护', 'BA系统资料维护', '空调过滤网更换维护', '旁滤装置月度维护', '制冷单元季度维护', '水处理系统维护'];
const notices = Array.from({ length: 12 }, (_, i) => ({ notice_key: 'fixture-notice-' + i, source_record_id: 'fixture-source-' + i,
  title: `EA118机房D楼${titles[i % titles.length]}${i > 5 ? '（第二阶段）' : ''}`, building: 'D楼', scope: 'D', specialty: '暖通',
  status: i > 8 ? '已结束' : '开始', maintenance_cycle: '月度', start_time: '2026-10-14 09:00', end_time: '2026-10-14 18:30',
  mop_uploaded: i > 8, mop_attachment_count: i > 8 ? 1 : 0 }));
const candidates = Array.from({ length: 15 }, (_, i) => ({ record_id: 'fixture-mop-' + i, title: titles[i % titles.length] + '标准作业指导书',
  file_no: 'JSNT-MOP-' + String(101 + i), specialty: '暖通', maintenance_type: '月度', version: 'V1.2', file_status: '有效', attachment_count: 2,
  fields: { 文件编号: 'JSNT-MOP-' + String(101 + i), 专业: '暖通', 维护类型: '月度' },
  attachments: [{ file_token: 'fixture-file-' + i, name: '维护记录表.xlsx' }, { file_token: 'fixture-cover-' + i, name: '维护计划表.xlsx' }] }));
const people=[
  {record_id:'fixture-implementer',name:'测试实施人',has_signature:true,usage_confirmed:true,source:'staff'},
  {record_id:'fixture-auditor',name:'测试审核人',has_signature:true,usage_confirmed:true,source:'staff'},
];
const rows = [['设备维护记录表', '', '', '', '', '', ''],
  ['维护开始时间','','','','','',''],['维护完成时间','','','','','',''],['审核确认时间','','','','','',''],
  ...Array.from({length:6},()=>['','','','','','','']),
  ['序号', '设备编号', '检查项目', '检查要求', '正常/异常', '检查结果', '备注'],
  ...Array.from({ length: 28 }, (_, i) => [String(i + 1), 'A-CH-' + String(i + 1).padStart(3, '0'), ['运行状态', '水系统', '电气检查'][i % 3], '按作业指导书核查', '', '', ''])];
const sheet = { name: '维护记录', is_cover: false, row_count: rows.length, column_count: 7, rows, merged_cells: [],
  checkbox_cells: [{ row: 11, col: 4, cell_ref: 'E12' }, { row: 12, col: 4, cell_ref: 'E13' }], maintenance_fields: [
    { row: 1, label_col: 0, value_col: 1, label: '维护开始时间', value_cell_ref: 'B2' },
    { row: 2, label_col: 0, value_col: 1, label: '维护完成时间', value_cell_ref: 'B3' },
    { row: 3, label_col: 0, value_col: 1, label: '审核确认时间', value_cell_ref: 'B4' },
  ] };
const localPath = 'engineer_mop_files\\2026-10-16\\C楼\\21V-JSNTFOC-MOPJL-KT-301-冷却塔月度维保保养记录单V1.0_adfa8b807e.xlsx';
const attachmentName='21V-JSNTFOC-MOPJL-KT-301-冷却塔月度维保保养记录单V1.0_adfa8b807e.xlsx';
let page;
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, hasTouch:true, reducedMotion: process.argv.includes('--motion')?'no-preference':'reduce' });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (url.pathname === '/assets/vnet-logo.png') return route.fulfill({ contentType: 'image/png', body: await readFile(path.join(root, 'public/assets/vnet-logo.png')) });
    if (url.pathname === '/assets/connection-guard.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (!url.pathname.startsWith('/api/')) return route.continue();
    if (await layouts.handle(route, 'mop-layout-fixture')) return;
    const ok = data => route.fulfill({ json: { ok: true, data } });
    reads.push(url.pathname + url.search);
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'mop-layout-fixture', name: '界面预览', role: 'admin', level: 'L1', category: 'manager', modules: ['engineer_mop'] }, scope_options: [{ value: 'D', label: 'D楼' }, { value: 'E', label: 'E楼' }] });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-mop' } });
    if (url.pathname === '/api/signatures/people') return ok({people,count:people.length});
    if (url.pathname === '/api/engineer/mop/history') {
      const scope=url.searchParams.get('scope'), specialty=url.searchParams.get('profession_view'), page=Number(url.searchParams.get('page')||1);
      const records=scope==='E'?[]:[{
        record_id:'recOldMop',title:'2025 年已归档暖通维保',building:'D楼',specialty:'暖通',cycle:'月度',status:'已结束',
        planned:'2025-04-01',attachments:[{id:'a'.repeat(24),name:'暖通维保归档.xlsx'}],
      }];
      return ok({scope,specialties:scope==='E'?[]:['暖通','电气'],records:specialty==='电气'?[]:records,
        page,page_size:20,total:specialty==='电气'||scope==='E'?0:21});
    }
    if (url.pathname === '/api/engineer/mop/history/recOldMop/'+'a'.repeat(24)) {
      assert.equal(url.searchParams.get('scope'),'D');
      assert.equal(url.searchParams.get('profession_view'),'暖通');
      return route.fulfill({contentType:'application/octet-stream',body:'history attachment fixture',
        headers:{'Content-Disposition':"attachment; filename*=UTF-8''"+encodeURIComponent('暖通维保归档.xlsx')}});
    }
    if (url.pathname === '/api/engineer/mop/bootstrap') return ok({ notices: url.searchParams.get('scope') === 'E' ? [] : structuredClone(notices), mop_candidates: candidates, warnings: ['隔离测试提示：请核对本次维护单填写内容。'], auth: { user: { open_id: 'mop-layout-fixture', name: '界面预览' } } });
    if (url.pathname === '/api/engineer/mop/bind') {
      const body = req.postDataJSON(); writes.push(body);
      assert(body.notice_key.startsWith('fixture-notice-'));
      return ok({ binding: body });
    }
    if (url.pathname === '/api/engineer/mop/preview') return ok({ local_file: { path: 'fixture.xlsx', relative_path: localPath, file_name: attachmentName }, mop_title: '冷却塔月度维保保养记录单', attachment: { name: attachmentName }, sheets: [sheet, { name: '封面', is_cover: true, row_count: 1, column_count: 2, rows: [['设备维护作业', 'D楼']] }], warnings: [] });
    if (url.pathname === '/api/engineer/mop/fill' || url.pathname === '/api/engineer/mop/upload-signed') {
      assert.equal(req.method(),'POST');
      const body=req.postDataJSON();fileActions.push({path:url.pathname,body});
      assert.equal(body.notice_key,'fixture-notice-1');
      assert.equal(body.mop_record_id,'fixture-mop-1');
      assert.deepEqual(body.signatures.map(item=>[item.role,item.record_id]),[['implementer','fixture-implementer'],['auditor','fixture-auditor']]);
      assert.equal(body.fields.length,3);
      assert(body.cell_edits.some(item=>String(item.value).includes('小屏维护备注')),'The current edit is included in the original API payload');
      if(url.pathname.endsWith('/fill')){
        assert.equal(url.searchParams.get('download'),'1');
        return route.fulfill({contentType:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',headers:{'Content-Disposition':"attachment; filename*=UTF-8''mop-small-screen.xlsx"},body:Buffer.from('isolated-download-fixture')});
      }
      if(failUpload){failUpload=false;return route.fulfill({status:500,json:{ok:false,error:'隔离测试上传失败，填写内容已保留。'.repeat(8)}});}
      await new Promise(resolve=>{releaseUpload=resolve;});releaseUpload=undefined;
      return ok({uploaded_at:'2026-10-16 11:00',notice_key:body.notice_key,source_record_id:body.source_record_id});
    }
    assert.equal(req.method(), 'GET', 'Unexpected business action in isolated preview: ' + url.pathname);
    return ok({ people: [], items: [], total: 0, signatures: [], records: [], summary: {} });
  });
  page = await context.newPage(); page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin + '/assets/workspace-v2/index.html?__page=/engineer/mop&scope=D');
  await page.getByRole('button', { name: '打开填写', exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, before ? 'before-desktop.png' : 'desktop.png'), fullPage: before });
  if (!before) {
    assert.equal(await page.locator('.v2-mop-workspace').count(), 1);
    assert.equal(await page.locator('[aria-label="MOP填写流程"]:visible').count(), 0, 'The current compact workspace does not show the retired step strip');
    const left = await page.locator('.mop-desk-notices').boundingBox(), right = await page.locator('.mop-desk-files').boundingBox();
    assert(left.x + left.width <= right.x, 'Desktop keeps notice and MOP selection side by side');
    const upload = await page.locator('.mop-desk-local').boundingBox();
    assert(upload.y + upload.height < 900, 'Local-file upload stays in the desktop viewport');
    for (const [width,height] of [[1366,768],[1024,768],[390,844]]) {
      await page.setViewportSize({width,height});
      if(width<=900)await page.getByRole('button',{name:'2 填写维护单'}).click();
      await page.screenshot({path:path.join(output,`binding-${width}.png`),animations:'disabled'});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'binding page stays within viewport');
      const panel=await page.locator('.mop-desk-files').boundingBox(),card=await page.locator('.mop-desk-attachment').boundingBox();
      assert(card.x>=panel.x&&card.x+card.width<=panel.x+panel.width+1,'selected file stays inside its panel');
      if(width>=1366){const box=await page.locator('.mop-desk-local').boundingBox();assert(box.y+box.height<=height,'local upload must remain reachable on a 768px-high desktop');}
    }
    await page.setViewportSize({width:1440,height:900});
    await page.getByRole('button',{name:'填写记录',exact:true}).click();
    const history=page.getByRole('region',{name:'填写记录',exact:true});
    await history.getByText('2025 年已归档暖通维保',{exact:true}).waitFor();
    await Promise.all([
      page.waitForRequest(request=>{
        const url=new URL(request.url());
        return url.pathname==='/api/engineer/mop/history' && url.searchParams.get('refresh')==='1';
      }),
      history.getByRole('button',{name:'刷新填写记录'}).click(),
    ]);
    await history.getByRole('button',{name:'表单显示'}).click();
    await history.getByRole('table',{name:'已上传维保单'}).waitFor();
    await history.getByRole('button',{name:'下一页'}).click();
    await history.getByText('2 / 2',{exact:true}).waitFor();
    await history.getByRole('combobox',{name:'筛选专业'}).selectOption('电气');
    await history.getByText('暂无符合条件的已上传维保单',{exact:true}).waitFor();
    await history.getByRole('combobox',{name:'筛选专业'}).selectOption('暖通');
    const receipt=page.waitForEvent('download');
    await history.getByRole('button',{name:'下载 暖通维保归档.xlsx'}).click();
    assert.equal((await receipt).suggestedFilename(),'暖通维保归档.xlsx');
    await page.screenshot({path:path.join(output,'history-desktop.png')});
    await page.setViewportSize({width:390,height:844});
    await history.getByRole('button',{name:'卡片显示'}).click();
    await page.screenshot({path:path.join(output,'history-mobile.png')});
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    await history.getByRole('combobox',{name:'填写记录楼栋'}).selectOption('E');
    await history.getByText('暂无符合条件的已上传维保单',{exact:true}).waitFor();
    assert.equal(await history.getByRole('button',{name:'下载 暖通维保归档.xlsx'}).count(),0);
    await history.getByRole('combobox',{name:'填写记录楼栋'}).selectOption('D');
    await page.setViewportSize({width:1440,height:900});
    await page.getByRole('navigation',{name:'维保单办理分区'}).getByRole('button',{name:'办理',exact:true}).click();
    const search = page.getByPlaceholder('搜索通告、楼栋、专业');
    await search.fill('BA系统'); assert.equal(await page.locator('.mop-desk-notice').count(), 2);
    await page.locator('.mop-desk-notice').first().click();
    await page.locator('.mop-desk-target').getByText('EA118机房D楼BA系统资料维护', { exact: true }).waitFor();
    await search.fill('');
    await page.getByPlaceholder('搜索 MOP 名称、文件编号、专业').fill('JSNT-MOP-102');
    assert.equal(await page.locator('.mop-desk-file').count(), 1);
    await page.locator('.mop-desk-file').click();
    await page.locator('.mop-desk-attachment-row [role="combobox"]').click();
    await page.locator('.v2-single-select-menu').getByText('维护计划表.xlsx', {exact:true}).click();
    await page.getByRole('button', { name: '打开填写', exact: true }).click();
    await page.locator('.preview-scroll table').waitFor();
    await page.locator('.floating-upload-blocked').waitFor();
    assert(!/结束通告|通告尚未.*结束/.test(await page.locator('.floating-upload-blocked').innerText()), 'an ongoing notice can still be filled and archived');
    assert.equal(writes.length, 1);
    assert.equal(writes[0].notice_key, 'fixture-notice-1');
    assert.equal(writes[0].mop_record_id, 'fixture-mop-1');
    assert.equal(writes[0].mop_attachment_token, 'fixture-cover-1');
    const footer = await page.locator('.floating-upload-action-row').boundingBox();
    assert(footer.y + footer.height < 900, 'Editor actions stay in the desktop viewport');
    assert.equal(await page.locator('.mop-file-status,.preview-head-status').count(),0,'No file path or duplicated completion summaries');
    assert(!(await page.locator('.mop-preview-page').innerText()).includes(localPath));
    const signatures=page.locator('.floating-upload-action-row .signature-role-summary');
    assert.equal(await signatures.count(),1,'Signature controls belong to the lower action row');
    const signBox=await signatures.boundingBox(),uploadBox=await page.locator('.floating-upload-signed-mop').boundingBox();
    assert(signBox.x+signBox.width<=uploadBox.x&&signBox.y<uploadBox.y+uploadBox.height&&uploadBox.y<signBox.y+signBox.height,'Desktop signatures sit to the left of upload in the same row');
    await signatures.getByRole('button').first().click();
    const dialog = page.getByRole('dialog', { name: '维护人员签名', exact: true });
    await dialog.waitFor();
    await dialog.getByRole('button', { name: '关闭', exact: true }).click();
    await dialog.waitFor({ state: 'hidden' });
    await page.screenshot({ path: path.join(output, 'editor-desktop.png') });
    await page.getByRole('button', { name: '第10行后全正常', exact: true }).click();
    await page.locator('.preview-scroll td.normal').first().waitFor();
    await page.getByRole('button', { name: '封面', exact: true }).click();
    await page.locator('.preview-scroll').getByText('设备维护作业', { exact: true }).waitFor();
    await page.getByRole('button', { name: '维护记录', exact: true }).click();
    for (const [width,height] of [[1024,768],[1366,768],[1440,900],[1920,1080],[760,844],[360,640],[375,667],[390,667],[360,740],[390,844],[430,932]]) {
      await page.setViewportSize({ width, height });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'No page overflow');
      if(width<700){
        await page.getByRole('button',{name:'人员与时间',exact:true}).click();
        const start=page.getByRole('region',{name:'维护单快捷填写'}).getByLabel('维护开始时间',{exact:true});
        if(width===360)await start.fill('2026-10-16T09:00');
        assert.equal(await start.inputValue(),'2026-10-16T09:00','Edits survive responsive layout and form-step changes');
        await start.focus();
        const inputBox=await start.boundingBox(),scrollBox=await page.locator('.preview-scroll').boundingBox();
        assert(inputBox.y>=scrollBox.y&&inputBox.y+inputBox.height<=scrollBox.y+scrollBox.height,'Focused date field is not covered by the action bar');
      }
      const action=await page.locator('.floating-upload-action-row').boundingBox();
      const nav=await page.locator('.v2-mobile-nav').isVisible()?await page.locator('.v2-mobile-nav').boundingBox():null;
      assert(action.y>=0&&action.y+action.height<=(nav?.y??height),'Signatures and upload remain above mobile navigation and within the first screen: '+width);
      await page.waitForFunction(()=>{
        const footer=document.querySelector('.floating-upload-box').getBoundingClientRect();
        return [...document.querySelectorAll('[data-v2-feedback-visible="true"]')].every(el=>el.getBoundingClientRect().bottom<footer.top);
      });
      await page.getByRole('button',{name:/^缺项 \d+$/}).click();
      const expanded=await page.locator('.floating-upload-action-row').boundingBox();
      assert(expanded.y+expanded.height<=(nav?.y??height),'Expanded missing fields do not push upload below the screen: '+width);
      await page.locator('.floating-upload-box').getByRole('button',{name:'收起',exact:true}).click();
      const scroll=page.locator('.preview-scroll');
      await scroll.evaluate(el=>{el.scrollTop=el.scrollHeight;});
      assert(await scroll.evaluate(el=>el.scrollHeight-el.clientHeight-el.scrollTop<2),'The last form/table row remains reachable');
      if(width<700){
        await signatures.getByRole('button').last().click();
        await dialog.waitFor();
        await dialog.getByRole('button',{name:'关闭',exact:true}).click();
        await dialog.waitFor({state:'hidden'});
        assert.equal(await page.evaluate(()=>document.body.style.overflow),'','Closing signatures restores page scrolling');
        await page.getByRole('button',{name:'检查与备注',exact:true}).click();
        assert(await signatures.isVisible(),'Signatures remain reachable on the mobile check page too');
        const box=await page.locator('.floating-upload-action-row').boundingBox();
        assert(box.y+box.height<=(nav?.y??height),'Upload is reachable on both mobile form steps');
      }
      await page.screenshot({path:path.join(output,`editor-${width}x${height}.png`)});
    }
    for(const close of await page.locator('[data-v2-feedback-visible="true"] [data-v2-feedback-close]').all())await close.click();
    async function checkActions(){
      await page.waitForFunction(()=>!document.querySelector('.floating-upload-action-row [data-v2-action]'));
      const nav=await page.locator('.v2-mobile-nav').boundingBox();
      const footer=await page.locator('.floating-upload-box').boundingBox();
      assert(footer.y>=0&&footer.y+footer.height<=nav.y,'The entire action bar, including upload feedback, stays above mobile navigation: '+JSON.stringify({footer,nav,viewport:page.viewportSize()}));
      for(const button of await page.locator('.floating-upload-action-row button').all()){
        const box=await button.boundingBox();
        assert(box.height>=44&&box.y>=0&&box.y+box.height<=nav.y,'Every action has a visible 44px touch target');
        const coveredBy=await button.evaluate(el=>{const b=el.getBoundingClientRect(),hit=document.elementFromPoint(b.x+b.width/2,b.y+b.height/2);return el.contains(hit)?null:{action:el.innerText,cover:hit?.outerHTML.slice(0,200),width:innerWidth};});
        assert.equal(coveredBy,null,'An action is covered: '+JSON.stringify(coveredBy));
      }
    }
    for(const [width,height] of [[360,640],[375,667],[393,852]]){
      await page.setViewportSize({width,height});
      await page.getByRole('button',{name:'人员与时间',exact:true}).click();
      const form=page.getByRole('region',{name:'维护单快捷填写'});
      for(const [label,time] of [['维护开始时间','09:00'],['维护完成时间','10:00'],['审核确认时间','11:00']])await form.getByLabel(label,{exact:true}).fill('2026-10-16T'+time);
      for(const [index,name] of [[0,'测试实施人'],[1,'测试审核人']]){
        await checkActions();
        await signatures.getByRole('button').nth(index).tap();
        await dialog.waitFor();
        const person=dialog.locator('.sign-person').filter({hasText:name});
        if(!await person.locator('.selected-corner').count())await person.tap();
        await dialog.getByRole('button',{name:'关闭',exact:true}).tap();
        await dialog.waitFor({state:'hidden'});
        assert.equal(await signatures.getByRole('button').nth(index).locator('.role-title-row b').innerText(),'1 个可用','Selected signatures survive closing and resizing');
      }
      await page.getByRole('button',{name:'检查与备注',exact:true}).click();
      await form.getByRole('button',{name:'第10行后全正常',exact:true}).click();
      await form.getByRole('textbox',{name:/备注/}).first().fill('小屏维护备注 '+width);
      const downloadButton=page.locator('.mop-download'),uploadButton=page.locator('.floating-upload-signed-mop');
      assert(await downloadButton.isEnabled()&&await uploadButton.isEnabled(),'The original validation enables file actions after filling and signing');
      await checkActions();
      const downloadEvent=page.waitForEvent('download');await downloadButton.tap();
      const download=await downloadEvent;assert.equal(await download.failure(),null);assert.equal(download.suggestedFilename(),'mop-small-screen.xlsx');
      await page.getByText('最新 MOP 已生成，浏览器已开始下载。',{exact:true}).waitFor();
      if(width===360){
        failUpload=true;await uploadButton.tap();
        await page.locator('.floating-upload-result.failed').waitFor();
        assert((await form.getByRole('textbox',{name:/备注/}).first().inputValue()).includes('小屏维护备注'));
        await checkActions();
      }
      const count=fileActions.filter(item=>item.path.endsWith('/upload-signed')).length;
      await uploadButton.tap();
      await page.waitForFunction(()=>document.querySelector('.floating-upload-signed-mop').disabled);
      assert.equal(fileActions.filter(item=>item.path.endsWith('/upload-signed')).length,count+1);
      await uploadButton.evaluate(el=>{el.click();el.click();});
      assert(await signatures.locator('..').evaluate(el=>el.inert),'Signature changes are blocked while uploading');
      assert.equal(fileActions.filter(item=>item.path.endsWith('/upload-signed')).length,count+1,'Repeated clicks do not create a second request');
      releaseUpload();
      await page.locator('.floating-upload-result.success').waitFor();
      await page.locator('.floating-uploaded-at').waitFor();
      await checkActions();
      for(const label of await signatures.locator('.role-title-row span').all()){
        assert(await label.evaluate(el=>el.clientWidth>=el.scrollWidth),'the timestamp must leave both signature role names readable');
      }
      await page.screenshot({path:path.join(output,`completed-${width}x${height}.png`)});
    }
    await page.setViewportSize({width:1440,height:900});
    const excel=page.locator('.preview-scroll table:visible');
    const timeCell=excel.locator('tbody tr').nth(3).locator('td').nth(1);
    await timeCell.click();
    const popover=page.locator('.floating-cell-popover:visible');
    await popover.locator('input[type=datetime-local]').fill('2026-10-16T11:30');
    await popover.getByRole('button',{name:'填入',exact:true}).click();
    await timeCell.getByText('2026年10月16日11时30分',{exact:true}).waitFor();
    await page.screenshot({path:path.join(output,'filled-desktop.png')});
    const desktopCount=fileActions.filter(item=>item.path.endsWith('/upload-signed')).length;
    await page.locator('.floating-upload-signed-mop').click();
    await page.waitForFunction(()=>document.querySelector('.floating-upload-signed-mop').disabled);
    assert.equal(fileActions.filter(item=>item.path.endsWith('/upload-signed')).length,desktopCount+1);
    assert(/11:30|11时30分/.test(JSON.stringify(fileActions.at(-1).body.fields)));
    releaseUpload();
    await page.locator('.floating-upload-result.success').waitFor();
    await page.screenshot({path:path.join(output,'completed-desktop.png')});
    await page.setViewportSize({width:393,height:852});
    await page.getByRole('button',{name:'返回',exact:true}).click();
    await page.getByRole('button', { name: '打开填写', exact: true }).waitFor();
    if(!await page.getByPlaceholder('搜索通告、楼栋、专业').isVisible())await page.getByRole('button',{name:'1 选择通告'}).click();
    await page.getByPlaceholder('搜索通告、楼栋、专业').fill('冷水');
    await page.locator('.mop-desk-notice').first().click();
    await page.locator('.mop-desk-target').getByText('EA118机房D楼冷水机组月度维护', { exact: true }).waitFor();
    await page.screenshot({ path: path.join(output, 'mobile.png') });
    assert.equal(writes.length, 1, 'Choosing notices does not submit a business action');
    await page.getByRole('combobox', { name: '切换楼栋', exact: true }).click();
    await page.locator('.v2-single-select-menu').getByText('E楼',{exact:true}).click();
    await page.getByText('暂无维保通告', { exact: true }).waitFor();
    assert(reads.some(url => url.includes('bootstrap?scope=E')));

    await page.setViewportSize({width:1440,height:900});
    await page.goto(origin+'/engineer/mop?frontend=legacy&scope=D');
    await page.getByRole('button',{name:'填写记录',exact:true}).click();
    await history.getByRole('combobox',{name:'筛选专业'}).selectOption('电气');
    await history.getByText('暂无符合条件的已上传维保单',{exact:true}).waitFor();
    await history.getByRole('combobox',{name:'筛选专业'}).selectOption('暖通');
    await history.getByText('2025 年已归档暖通维保',{exact:true}).waitFor();
    await page.getByRole('navigation',{name:'维保单办理分区'}).getByRole('button',{name:'办理',exact:true}).click();
    await page.locator('.notice-row').filter({hasText:'EA118机房D楼BA系统资料维护'}).first().click();
    await page.getByPlaceholder('搜索 MOP 名称、文件编号、专业').fill('JSNT-MOP-102');
    await page.locator('.mop-row').click();
    await page.getByLabel('表格附件').selectOption({label:'维护计划表.xlsx'});
    await page.getByRole('button',{name:'打开填写',exact:true}).click();
    await page.locator('.preview-scroll table').waitFor();
    assert.equal(writes.length,2,'each version binds once when opening its selected file');
    for(const key of ['notice_key','mop_record_id','mop_attachment_token'])assert.equal(writes[1][key],writes[0][key]);
    await page.screenshot({path:path.join(output,'legacy-preview.png')});
  }
  assert.deepEqual(errors, []);
  console.log(before ? 'Captured existing MOP layout.' : 'PASS: native MOP controller, PC/mobile and 360x640/375x667 layouts, touch filling/signatures, download, upload failure/retry, busy guards and unchanged API payloads. All APIs mocked.');
} catch (error) {
  if (page) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); console.error(await page.locator('body').innerText()); }
  throw error;
} finally { releaseUpload?.();await browser.close(); await server.close(); }
