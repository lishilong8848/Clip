import assert from 'node:assert/strict';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {auditUrl, layoutEvidence} from './responsive-audit-pages.mjs';

const origin = 'http://127.0.0.1:14536';
const output = fileURLToPath(new URL('../../../../output/responsive-audit/', import.meta.url));
const discovery = process.argv.includes('--discover');
const patched = process.argv.includes('--patched');
const only = process.argv.find(arg=>arg.startsWith('--only='))?.slice(7);
const images = output + 'continued-operations/'+(patched?'after/':'before/');
const repairSource = await readFile(new URL('../src/components/RepairFollowupPanel.vue',import.meta.url),'utf8');
const repairPatch = ['.followup-panel','.followup-title strong'].map(selector=>{
  const rule=repairSource.slice(repairSource.indexOf('<style scoped>')).match(new RegExp(selector.replaceAll('.','\\.')+' \\{([^}]+)\\}'));
  assert(rule,selector);
  return selector+' {'+rule[1].split('\n').filter(line=>/grid-template-columns|display: block/.test(line)).join('\n')+'}';
}).join('\n');
const v2RepairSource = await readFile(new URL('../src/workspace-v2/repair.css',import.meta.url),'utf8');
const v2RepairPatch = '@media(max-width:760px){'+v2RepairSource.match(/  (\.v2-repair-workspace \.v2-repair-detail-body>\.n-radio-group\{display:flex;width:100%;min-height:44px\})/)[1]+'}';
const viewports = discovery ? [[402,874]] : [[402,874],[440,956],[1440,900]];
const results = [];
await mkdir(images, {recursive:true});
const health = await fetch(origin + '/api/health').then(r => r.json());
assert.match(health.instance_id, /responsive.*(isolated|audit)/, 'Only the isolated fixture server is allowed');
const cleanup = [];
if(!only||only==='sop') {
  const previous=JSON.parse(await readFile(output+'continued-operations-review.json','utf8').catch(()=>'{"results":[]}'));
  const owned=new Set(previous.results.flatMap(r=>r.checks||[]).flatMap(c=>c.result?.attachments||[]).map(a=>a.attachment_id).filter(id=>/^audit-sop-upload-\d+$/.test(id)));
  for(const id of owned) {
    const response=await fetch(origin+'/api/polling-sops/audit-sop-A-maintenance-0/attachments/'+id,{method:'DELETE'});
    assert(response.ok,'Cannot clean up previously uploaded isolated test attachment');
    cleanup.push(id);
  }
}
const browser = await chromium.launch({headless:true});
try {
  for (const version of ['legacy','v2']) {
    for (const [width,height] of viewports) {
      for (const [name,logical] of [['mop','/engineer/mop?scope=A'],['repair','/repair-management?scope=A'],['sop',version==='legacy'?'/workbench-lite?scope=A&work_type=maintenance':'/?view=sops&scope=A']]) {
        if(only&&name!==only)continue;
        const page = await browser.newPage({viewport:{width,height}, acceptDownloads:true});
        page.setDefaultTimeout(9000);
        const requests = [], errors = [];
        await page.route('**/*', route => {
          const url = new URL(route.request().url());
          return url.origin === origin || ['data:','blob:'].includes(url.protocol) ? route.continue() : route.abort();
        });
        page.on('response', response => {
          if (response.url().includes('/api/')) requests.push({method:response.request().method(),path:new URL(response.url()).pathname,status:response.status()});
        });
        page.on('pageerror', error => errors.push(error.message));
        page.on('dialog', dialog => dialog.dismiss());
        const item = {version,name,width,height,entry:logical,states:[],checks:[],requests,errors};
        results.push(item);
        const capture = async state => {
          await page.waitForTimeout(350);
          const evidence = await page.evaluate(layoutEvidence);
          const containers = await page.locator('.project-drawer-body,.followup-panel,.followup-workspace,.followup-head,.followup-title,.followup-editor,.v2-repair-followup-heading,.v2-repair-detail-body>.n-radio-group,.v2-repair-detail-body>.n-radio-group .n-radio-button,.v2-repair-followup-heading>div,.floating-upload-box,.preview-scroll').evaluateAll(elements=>elements.map(e=>({class:e.className,rect:e.getBoundingClientRect().toJSON(),scrollLeft:e.scrollLeft,scrollWidth:e.scrollWidth,clientWidth:e.clientWidth,display:getComputedStyle(e).display,minWidth:getComputedStyle(e).minWidth,columns:getComputedStyle(e).gridTemplateColumns,overflowX:getComputedStyle(e).overflowX})));
          const screenshot = `${version}-${name}-${width}-${state}.png`;
          await page.screenshot({path:images+screenshot});
          item.states.push({state,screenshot:'continued-operations/'+(patched?'after/':'before/')+screenshot,...evidence,containers});
        };
        const check = async (control, action) => {
          try { const result=await action();item.checks.push({control,result,status:result?.blocked?'blocked':'observed'}); }
          catch(error) { item.checks.push({control,status:'blocked',reason:error.message}); }
        };
        try {
          await page.goto(auditUrl(origin,version,logical),{waitUntil:'networkidle'});
          if(version==='legacy') assert.equal(new URL(page.url()).searchParams.get('frontend'),'legacy');
          if(patched&&version==='legacy'&&name==='repair') await page.addStyleTag({content:repairPatch});
          if(patched&&version==='v2'&&name==='repair') await page.addStyleTag({content:v2RepairPatch});
          await capture('entry');
          if (name === 'mop') {
            await check('local-workbook-upload',async()=>{
              await page.locator('.local-mop-upload').scrollIntoViewIfNeeded();
              await capture('local-upload');
              const response=page.waitForResponse(r=>r.url().includes('/api/engineer/mop/upload-local'));
              await page.locator('.local-mop-file-input').setInputFiles(fileURLToPath(new URL('../../templates/轮巡操作流程.xlsx',import.meta.url)));
              const r=await response,payload=await r.json();assert(r.ok()&&payload.ok!==false);
              await capture('local-uploaded');
              return {status:r.status(),scope:'Existing repository workbook selected; fixture substitutes its canned MOP preview and does not parse workbook bytes'};
            });
            await page.getByRole('button',{name:'打开填写',exact:true}).click();
            await page.locator('.mop-preview-page').waitFor();
            await capture('fill');
            await check('maintenance-time',async()=>{
              for (const [i,label] of ['维护开始时间','维护完成时间','审核确认时间'].entries()) {
                if(width<720) await page.getByLabel(label,{exact:true}).fill('2026-10-19T'+['09:00','10:00','11:00'][i]);
                else {
                  await page.locator(`[data-mop-cell-key$=":${i+1}:1"]`).click();
                  await page.locator('.floating-cell-popover input').fill('2026-10-19T'+['09:00','10:00','11:00'][i]);
                  await page.getByRole('button',{name:'填入',exact:true}).click();
                }
              }
              await capture('time-filled');
              return 'Three actual date/time inputs filled';
            });
            await page.locator('.signature-role-summary button').first().click();
            await page.locator('.mop-sign-panel.manager-open').waitFor();
            await capture('signature');
            await check('select-signatures',async()=>{
              await page.locator('.sign-person').filter({hasText:'测试实施人'}).click();
              await page.locator('.mop-sign-panel.manager-open .signature-role-summary button').nth(1).click();
              await page.locator('.sign-person').filter({hasText:'测试审核人'}).click();
              await capture('signature-selected');
              return (await page.locator('.mop-sign-panel.manager-open').innerText()).slice(-2200);
            });
            await check('write-signatures-to-workbook',async()=>{
              const response=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/engineer/mop/fill'&&r.request().method()==='POST');
              await page.getByRole('button',{name:'签名写入MOP',exact:true}).click();
              const r=await response,payload=await r.json();assert(r.ok()&&payload.ok!==false);
              await capture('signature-written');
              return {status:r.status(),scope:'Fixture receipt only; no real workbook signature rendering verified'};
            });
            await check('signature-invitation-form',async()=>{
              await page.getByRole('tab',{name:/临时\/外部人员/}).click();
              const response=page.waitForResponse(r=>r.url().includes('/api/signatures/management/mop-people'));
              await page.getByRole('button',{name:'添加人员',exact:true}).click();
              const r=await response;
              await page.locator('.mop-signature-invitation').waitFor();
              await page.locator('.mop-signature-invitation').scrollIntoViewIfNeeded();
              await capture('signature-invitation');
              const result={httpStatus:r.status(),blocked:!r.ok(),reason:!r.ok()?'Fixture does not provide invitation recipient contract':'Read-only invitation form inspected',sent:false};
              await page.locator('.mop-signature-invitation').getByRole('button',{name:'取消',exact:true}).click();
              return result;
            });
            await page.getByRole('button',{name:'关闭',exact:true}).click();
            if(width<720) await page.getByRole('button',{name:'检查与备注',exact:true}).click();
            await check('checks-and-remarks',async()=>{
              if(width<720) {
                await page.getByRole('button',{name:'正常',exact:true}).first().click();
                await page.locator('.quick-checks textarea').first().fill('隔离验收：检查与备注控件输入');
              } else await page.getByRole('button',{name:/第10行后全正常/}).click();
              await capture('checks');
              return 'Actual check controls edited';
            });
            await check('download-filled-maintenance',async()=>{
              const button=page.getByRole('button',{name:'下载文件',exact:true});
              await button.scrollIntoViewIfNeeded();
              if(await button.isDisabled()) throw new Error('Disabled: '+await button.getAttribute('title'));
              const download=page.waitForEvent('download');
              await button.click();
              const file=await download;
              return {filename:file.suggestedFilename(),failure:await file.failure(),scope:'Fixture transport only; fixture returns text, not an actual exported workbook'};
            });
            await check('upload-signed-maintenance',async()=>{
              const button=page.locator('.floating-upload-signed-mop');
              await button.scrollIntoViewIfNeeded();
              if(await button.isDisabled()) throw new Error('Disabled: '+await button.getAttribute('title'));
              const response=page.waitForResponse(r=>r.url().includes('/api/engineer/mop/upload-signed'));
              await button.click();
              const r=await response;
              const payload=await r.json();
              assert(r.ok()&&payload.ok!==false,JSON.stringify(payload));
              return {status:r.status(),payload,scope:'Isolated fixture only'};
            });
            await page.locator('.floating-upload-box').scrollIntoViewIfNeeded();
            await capture('upload');
          } else if (name === 'repair') {
            await page.locator('.record-followup-button').first().click();
            await page.locator('.followup-panel').waitFor();
            await page.locator('.followup-panel textarea').first().waitFor();
            if (version === 'v2') {
              if(!await page.locator('.v2-repair-history button').first().isVisible()) await page.getByText(/历史跟进 ·/).click();
              await page.waitForTimeout(650);
              await capture('history');
              await page.locator('.v2-repair-history button').first().click();
              await page.getByRole('button',{name:'编辑这条',exact:true}).click();
            } else {
              await page.locator('.followup-timeline-list button').first().click();
            }
            await page.locator('.followup-panel textarea').first().waitFor();
            await capture('editing');
            await check('edit-existing-followup',async()=>{
              const input=page.locator('.followup-panel textarea').first();
              await input.fill('隔离验收：实际历史跟进编辑输入，核对手机与PC布局。');
              await capture('description');
              const save=version==='v2'?page.getByRole('button',{name:'保存跟进修改',exact:true}):page.locator('.followup-action-bar button').last();
              await save.scrollIntoViewIfNeeded();
              await capture('save');
              assert(await input.inputValue() !== '');
              return {saveLabel:await save.innerText(),disabled:await save.isDisabled(),submitted:false};
            });
            await check('repair-device-picker',async()=>{
              await page.getByRole('button',{name:version==='v2'?'调整设备':'重新选择台账设备',exact:true}).click();
              await page.getByRole('dialog').last().waitFor();
              await capture('device-picker');
              return 'Actual ledger picker opened; no confirmation or submission';
            });
          } else if (version === 'v2') {
            await page.locator('.v2-sop-item').filter({hasText:'A楼维保通告设备状态核验'}).first().click();
            await page.getByRole('button',{name:'修改',exact:true}).click();
            await capture('editing');
            await page.getByLabel('添加SOP附件',{exact:true}).scrollIntoViewIfNeeded();
            await capture('attachment');
            await check('sop-attachment-upload',async()=>{
              const response=page.waitForResponse(r=>r.url().includes('/attachments')&&r.request().method()==='POST');
              await page.getByLabel('添加SOP附件',{exact:true}).setInputFiles({name:'responsive-operations.txt',mimeType:'text/plain',buffer:Buffer.from('Isolated responsive control check. No production data.')});
              const r=await response,payload=await r.json();assert(r.ok()&&payload.ok!==false);
              await capture('attachment-uploaded');
              return {status:r.status(),attachments:payload.data?.attachments};
            });
            await check('sop-edit-step',async()=>{
              await page.getByLabel('步骤1内容',{exact:true}).fill('隔离验收：步骤内容输入及保存按钮布局');
              await capture('step-input');
              const save=page.getByRole('button',{name:'保存SOP',exact:true});
              await save.scrollIntoViewIfNeeded();await capture('save');
              return {saveDisabled:await save.isDisabled(),submitted:false};
            });
          } else {
            await page.locator('#lite-polling-sop-open').click();
            await page.locator('.polling-sop-row').first().click();
            await capture('editing');
            await check('legacy-sop-step',async()=>{
              await page.locator('.polling-step-edit textarea').first().fill('隔离验收：步骤内容输入及保存按钮布局');
              const save=page.getByRole('button',{name:'保存 SOP',exact:true});
              await save.scrollIntoViewIfNeeded();await capture('save');
              return {disabled:await save.isDisabled(),submitted:false};
            });
            await check('legacy-sop-attachment',async()=>{
              const input=page.locator('#lite-polling-sop-editor input[type=file]');
              await page.locator('#lite-polling-sop-editor .polling-sop-drop').scrollIntoViewIfNeeded();await capture('attachment');
              const response=page.waitForResponse(r=>r.url().includes('/attachments')&&r.request().method()==='POST');
              await input.setInputFiles({name:'responsive-operations.txt',mimeType:'text/plain',buffer:Buffer.from('Isolated responsive control check. No production data.')});
              const r=await response,payload=await r.json();assert(r.ok()&&payload.ok!==false);
              await capture('attachment-uploaded');
              return {status:r.status(),attachments:payload.data?.attachments};
            });
          }
          if (discovery) console.log(JSON.stringify({version,name,text:(await page.locator('body').innerText()).replace(/内部资料[^\n]*\n/g,'').slice(-13000)}));
        } catch (error) {
          item.failure = error.message;
          await capture('blocked').catch(() => {});
        } finally {
          await page.close();
          console.log(JSON.stringify({version,name,width,states:item.states.length,failure:item.failure?.split('\n')[0],blocked:item.checks.filter(c=>c.status==='blocked').map(c=>c.control)}));
        }
      }
    }
  }
} finally {
  await browser.close();
  await writeFile(output+(discovery?'continued-operations-discovery.json':only?'continued-operations-'+only+'-'+(patched?'after':'before')+'.json':'continued-operations-review.json'),JSON.stringify({origin,tool:'Playwright',sourceCssInjected:patched,cssVerification:patched?'Existing built application plus the three source CSS declarations; no build performed':'Existing built application',repairPatch,v2RepairPatch,cleanup,viewports,results},null,2));
}
