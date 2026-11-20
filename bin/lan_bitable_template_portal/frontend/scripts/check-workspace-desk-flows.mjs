import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';

const directory = process.env.DESK_AUDIT;
if (!directory) throw new Error('Set DESK_AUDIT to the isolated build and fixture directory');
const origin = (await fs.readFile(path.join(directory, 'origin.txt'), 'utf8')).trim();
assert.equal(new URL(origin).hostname, '127.0.0.1');
const health = await (await fetch(origin + '/api/health')).json();
assert.equal(health.instance_id, 'desk-isolated');
const out = path.join(directory, process.env.DESK_PHASE || 'after');
await fs.mkdir(out, {recursive:true});
const browser = await chromium.launch();
const results = [];
try {
  for (const width of [1440,393]) {
    const height = width === 393 ? 852 : 900;
    for (const module of ['cabinet','batch','water','assistant','journal']) {
      if (process.env.DESK_MODULE && !process.env.DESK_MODULE.split(',').includes(module)) continue;
      const context = await browser.newContext({viewport:{width,height},reducedMotion:'reduce',hasTouch:width===393});
      const page = await context.newPage();
      page.setDefaultTimeout(4500);
      const row = {width,height,module,states:[],writes:[],errors:[]};
      results.push(row);
      const button = (name, root = page) => root.getByRole('button',{name,exact:true});
      const go = route => page.goto(origin + route);
      const click = name => button(name).click();
      const shot = async (name, anchor) => {
        if (anchor) await anchor.scrollIntoViewIfNeeded();
        await page.evaluate(()=>document.fonts.ready);
        const png = module + '-' + width + '-' + name + '.png';
        await page.screenshot({path:path.join(out,png),animations:'disabled'});
        const overflow = await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth);
        row.states.push({name,png,url:page.url(),overflow});
        assert(overflow <= 1, 'page overflow '+overflow);
      };
      const waterWrites = [];
      await context.route('**/*', async route => {
        const request = route.request(), url = new URL(request.url());
        if (url.origin !== origin && !['blob:','data:'].includes(url.protocol)) return route.abort();
        if (url.pathname.startsWith('/api/') && request.method() !== 'GET') {
          row.writes.push({method:request.method(),path:url.pathname});
          if (request.method()==='PATCH' && url.pathname.startsWith('/api/capacity/water/records/')) {
            const data = request.postDataJSON(); waterWrites.push(data);
            if (!data.large_change_confirmed) return route.fulfill({status:409,json:{ok:false,error:'请复核隔离读数',error_code:'confirmation_required',details:{kind:'water_large_change',old_value:100,new_value:data.meter_value,ratio:1}}});
            return route.fulfill({json:{ok:true,data:{record_id:url.pathname.split('/').at(-1),warnings:[]}}});
          }
        }
        return route.continue();
      });
      page.on('pageerror', e => row.errors.push(e.message));
      try {
        if (module==='cabinet') {
          await go('/cabinet-power');
          const building=page.locator('button.cab-building').first();
          await building.waitFor(); await shot('campus',building);
          await click('设置');
          const schedule=page.getByRole('dialog',{name:'机柜月度自动归档设置'});
          await schedule.getByRole('checkbox').waitFor();
          await shot('schedule',schedule); await click('关闭设置');
          await building.click();
          await click('扫码查机柜');const scanner=page.getByRole('dialog',{name:'扫码查机柜'});
          await scanner.getByRole('searchbox').fill('A01');await shot('scan',scanner);await button('查询',scanner).click();
          await click('关闭历史');await click('历史导出');const exports=page.getByRole('dialog',{name:'楼栋导出历史'});
          await button('下载',exports).first().waitFor();await shot('exports',exports);await click('关闭导出历史');
          await click('原始平面图');
          const rack=button('A01 正式电'); await rack.waitFor();
          await shot('map',rack); await rack.click();
          const history=page.getByRole('dialog',{name:'机柜完整历史'});
          await button('编辑',history).waitFor(); await shot('history',history);
          await button('编辑',history).click();
          const edit=page.getByRole('dialog',{name:'机柜操作登记'});
          await edit.getByLabel('机柜功率（W）').fill('4350');
          await shot('edit',edit); await click('关闭编辑');
          await page.getByText('确认放弃修改？',{exact:true}).waitFor();
          await shot('discard'); await click('保留修改'); assert.equal(await edit.getByLabel('机柜功率（W）').inputValue(),'4350');
        }
        if (module==='batch') {
          await go('/cabinet-power/batches?scope=A');
          const first = button('audit-batch'); await first.waitFor(); await shot('list',first); await first.click();
          const detail = button('详情'); await detail.waitFor(); await shot('rows',detail); await detail.click();
          const locked=page.getByRole('dialog',{name:'机柜待办行详情'});
          await locked.getByText('详情与审计',{exact:true}).click();
          assert.equal(await locked.getByRole('textbox').count(),0);
          assert.equal(await locked.locator('.cab-row-facts').getByText('上正式电',{exact:true}).count(),1);
          assert.equal(await button('保存并收起',locked).count(),0);
          await shot('history',locked);await button('关闭',locked).click();
          await button('编辑').first().click();const edit=page.getByRole('dialog',{name:'编辑机柜待办行'});
          const rack=edit.locator('input[type=text]').nth(1);
          const patched=page.waitForRequest(r=>r.method()==='PATCH' && r.url().includes('/cabinet-power/batches/'));
          const target=(await rack.inputValue())==='A02'?'A09':'A02';
          await rack.fill(target);const patch=await patched;assert(patch.postDataJSON().rows.some(r=>r.rack===target));
          await shot('edit',edit);await button('保存并收起',edit).click();
          await click('确认整批'); await button('取消').waitFor(); await shot('confirm');
          const submitted=page.waitForRequest(r=>r.method()==='POST' && r.url().endsWith('/confirm'));
          await button('确认整批').last().click();assert((await submitted).postDataJSON().all);
          await click('文本回填');await page.getByRole('dialog').getByRole('textbox').first().waitFor();await shot('text-fill');
          await go('/cabinet-power/batches?scope=A&mode=new');
          const paste=page.getByRole('textbox',{name:'粘贴机柜确认文本',exact:true});
          await paste.evaluate(el=>{const clipboardData=new DataTransfer();clipboardData.setData('text/plain','A楼\\t201\\tA01\\t上正式电');el.dispatchEvent(new ClipboardEvent('paste',{clipboardData,bubbles:true,cancelable:true}));});
          const parsed=page.getByRole('textbox',{name:'文本记录包间',exact:true}).first();await parsed.waitFor();await parsed.fill('203');await shot('create-preview',parsed);
          await page.reload();assert.equal(await parsed.inputValue(),'203');
          const created=page.waitForRequest(r=>r.method()==='POST' && new URL(r.url()).pathname==='/api/cabinet-power/batches');
          await page.getByRole('button',{name:/确认并创建待办/}).click();await created;await button('确认整批').waitFor();
        }
        if (module==='water') {
          await go('/water-management?scope=A');
          const building=page.getByRole('button',{name:'查看A楼水耗记录'});await building.waitFor();await shot('overview',building);await building.click();
          const record=page.getByRole('button',{name:/^填写.*水表/}).first();
          await record.waitFor();await shot('records',record);await record.click();
          const input=page.getByLabel('本次水表读数',{exact:true});await input.waitFor();
          await input.fill('9876');await shot('edit',input);
          await click('保存记录'); const note=page.getByLabel('异常备注',{exact:true});
          await note.fill('现场复核后的演示读数');await shot('confirm',note);
          await click('确认并保存');await page.locator('.water-editor').waitFor({state:'detached'});
          assert.equal(waterWrites.length,2);assert.equal(waterWrites[1].operation_id,waterWrites[0].operation_id);
          assert.equal(waterWrites[1].abnormal_note,'现场复核后的演示读数');
        }
        if (module==='assistant') {
          await go('/?view=assistant');
          const surface=page.locator('[data-lighthouse-surface=conversation]');
          const preview=button('预览演示说明.txt',surface);await preview.waitFor();
          assert.equal(await surface.count(),1);row.color=await surface.evaluate(el=>getComputedStyle(el).getPropertyValue('--bot-color').trim());assert(row.color);
          await shot('conversation',preview);await preview.click();
          const closeFile=button('关闭文件预览');await closeFile.waitFor();
          const rect=await closeFile.boundingBox();assert(rect.x>=0 && rect.x+rect.width<=width+1,'attachment close outside viewport');
          await page.getByText(/隔离验收演示附件。/, {exact:false}).last().waitFor();await shot('file');await closeFile.click();
          const question='模拟复核 '+width;await page.locator('#assistant-question').fill(question);await click('发送问题');
          await page.locator('.turn').filter({hasText:question}).getByText(/这是隔离页面验收的演示回答/).first().waitFor();
          await click('模型设置');
          await button('编辑').first().click();const name=page.getByLabel('显示名称',{exact:true});
          await name.fill('模拟模型设置');await shot('edit',name);await click('返回会话');
          await page.getByText('离开模型设置？',{exact:true}).waitFor();await shot('confirm');
        }
        if (module==='journal') {
          await go('/admin/settings?tab=development');
          const search=page.getByRole('textbox',{name:'搜索更新记录'});await search.fill('断网');
          await page.getByText('共 12 个版本',{exact:true}).waitFor();
          const entry=page.locator('button.release-journal-entry').first();await shot('filtered',entry);await entry.click();
          const dialog=page.getByRole('dialog');await dialog.getByText('只重试只读查询，并保留脱敏诊断信息',{exact:true}).waitFor();
          await dialog.getByText('关联提交',{exact:true}).click();await dialog.getByText('0123456',{exact:true}).waitFor();
          await shot('detail',dialog);await page.keyboard.press('Escape');await dialog.waitFor({state:'hidden'});assert.equal(await search.inputValue(),'断网');
        }
        assert.deepEqual(row.errors,[]);
      } catch(e) {
        row.failure=e.message;await page.screenshot({path:path.join(out,module+'-'+width+'-failure.png')});
        await fs.writeFile(path.join(out,module+'-'+width+'-failure.txt'),await page.locator('body').innerText());
      } finally {await context.close();}
      console.log(width,module,row.failure?.split('\n')[0]||'passed',row.states.map(s=>s.name).join(','));
    }
  }
} finally {await browser.close();await fs.writeFile(path.join(out,'operations.json'),JSON.stringify(results,null,2));}
if (results.some(row=>row.failure)) process.exitCode=1;
