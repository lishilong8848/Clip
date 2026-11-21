import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {fixture,eventId,secondId} from './event-notice-desk-fixture.mjs';

const here=path.dirname(fileURLToPath(import.meta.url));
const frontend=path.resolve(here,'..');
const assets=path.resolve(process.argv[2],'assets/workspace-v2');
await fs.access(path.join(assets,'entry.json'));
const browser=await chromium.launch();
const output=path.resolve(process.argv[3]);
await fs.mkdir(output,{recursive:true});
async function run(canDelete,width){
  const mock=await fixture(assets,frontend,canDelete),ctx=await browser.newContext({viewport:{width,height:width===393?852:1000},reducedMotion:'reduce',serviceWorkers:'block'});
  const page=await ctx.newPage();page.setDefaultTimeout(12000);
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await ctx.route('**/*',r=>new URL(r.request().url()).origin===mock.origin?r.continue():r.abort());
  const list=page.locator('.evt-index-rows'),card=page.locator('.evt-selected .evt-record:visible'),composer=page.locator('.evt-compose');
  const item=(title)=>list.getByRole('button',{name:new RegExp(title)});
  const bodyText=card.getByLabel('本次通告原文');
  const posts=(name)=>mock.writes.filter(w=>w.p==='/api/workbench-events/'+name).map(w=>w.body);
  async function snap(name,markers){
    for(const marker of markers)await marker.waitFor();
    if(width===393)await markers.at(-1).scrollIntoViewIfNeeded();
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    const bounds=await markers.at(-1).boundingBox();
    assert(bounds && bounds.x>=-1 && bounds.x+bounds.width<=width+1);
    await page.screenshot({path:path.join(output,name+'-'+width+'.png'),animations:'disabled'});
  }
  try {
    if(!canDelete){
      await page.goto(mock.origin+'/assets/workspace-v2/index.html?scope=A');
      await page.getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
      await page.locator('.wb-task-tile[data-group="events"]').click();
      const queue=page.getByRole('dialog',{name:'模块待办条目'});
      await queue.getByRole('button',{name:'办理 冷冻水回水温度偏高',exact:true}).click();
      const focused=page.getByRole('dialog',{name:'待办办理窗口',exact:true});
      await focused.locator('.evt-record').getByRole('heading',{name:'冷冻水回水温度偏高',exact:true}).waitFor();
      assert.equal(await focused.locator('.evt-record').count(),1);
      assert.equal(await focused.locator('.evt-compose,.evt-index').count(),0);
      await focused.getByRole('button',{name:'更新',exact:true}).click();
      await focused.getByLabel('本次通告原文').fill('【进展】工作台核验备用机组运行状态。');
      await focused.getByRole('button',{name:'重试未送达的群'}).click();
      const nested=page.getByRole('alertdialog',{name:'核对未送达的群'});
      await nested.getByRole('button',{name:'暂不重发'}).click();
      assert.equal(posts('retry').length,0);
      await snap('07-工作台-指定事件办理',[focused.locator('.evt-record').getByRole('heading',{name:'冷冻水回水温度偏高',exact:true}),focused.getByLabel('本次通告原文')]);
      await focused.getByRole('button',{name:'收起，保留填写'}).click();
      await focused.getByRole('button',{name:'关闭待办办理'}).click();
      await focused.waitFor({state:'hidden'});
    }
    await page.goto(mock.origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=A&work_type=event');
    const title='冷冻水回水温度偏高';
    await item(title).click();
    assert.equal(await card.getByRole('button',{name:'删除',exact:true}).count(),canDelete?1:0);
    if(canDelete){
      await card.getByRole('button',{name:'删除',exact:true}).click();
      const dialog=page.getByRole('alertdialog',{name:'删除事件通告'});
      await dialog.getByRole('button',{name:'保留'}).click();assert.equal(posts('delete').length,0);
      await card.getByRole('button',{name:'删除',exact:true}).click();
      await dialog.getByRole('button',{name:'确认删除'}).click();
      await dialog.getByRole('alert').filter({hasText:'记录仍保留'}).waitFor();
      await snap('04-有删除权限-失败后保留记录',[dialog.getByRole('alert'),dialog.getByRole('button',{name:'确认删除'}),list.locator('[aria-current=true]')]);
      assert(mock.rows.has(eventId));
      await dialog.getByRole('button',{name:'确认删除'}).click();
      await item(title).waitFor({state:'detached'});assert(!mock.rows.has(eventId));
      assert.deepEqual(posts('delete').map(b=>b.event_id),[eventId,eventId]);
      await item('配电监控').click();await card.getByRole('button',{name:'删除',exact:true}).click();
      mock.options.can_delete=false;
      await dialog.waitFor({state:'hidden'});assert.equal(await card.getByRole('button',{name:'删除',exact:true}).count(),0);
      assert.equal(posts('delete').length,2);
    }else{
      const settingsButton=page.getByRole('button',{name:/默认钉钉机器人/});
      await settingsButton.click();
      const settings=page.getByRole('dialog',{name:'默认钉钉机器人'});
      await settings.getByLabel(/设施协同群/).uncheck();
      await snap('05-默认机器人-提交前选择',[settings.getByRole('button',{name:'保存默认选择'}),settings.getByLabel(/A楼运行群/)]);
      await settings.getByRole('button',{name:'保存默认选择'}).click();await settings.waitFor({state:'hidden'});
      assert.equal(posts('defaults')[0].revision,3);assert.equal(posts('defaults')[0].robot_ids.length,1);
      await card.getByRole('button',{name:'更新',exact:true}).click();
      const update='【事件通告】状态：更新\n【标题】冷冻水回水温度偏高\n【进展】已复核回水温度测点，正在跟踪备用机组投入后的趋势。';
      await bodyText.fill(update);await page.waitForTimeout(380);
      await snap('01-进行中事件-更新草稿和投递回执',[list.locator('[aria-current=true]'),bodyText,card.getByRole('button',{name:'发送更新'}),card.getByRole('button',{name:'重试未送达的群'})]);
      await item('配电监控').click();await item(title).click();assert.equal(await bodyText.inputValue(),update);
      await card.getByRole('button',{name:'重试未送达的群'}).click();
      const retry=page.getByRole('alertdialog',{name:'核对未送达的群'});
      await retry.getByText(/设施协同群/).waitFor();assert.equal(posts('retry').length,0);
      await snap('02-失败回执-核对原投递对象',[list.locator('[aria-current=true]'),retry.getByText(/设施协同群/),retry.getByRole('button',{name:'已核对，确认重发'})]);
      await retry.getByRole('button',{name:'暂不重发'}).click();assert.equal(posts('retry').length,0);
      await card.getByRole('button',{name:'重试未送达的群'}).click();await retry.getByRole('button',{name:'已核对，确认重发'}).click();
      await retry.waitFor({state:'hidden'});assert.deepEqual(posts('retry')[0],{scope:'A',event_id:eventId,operation_id:'fixture-'+eventId,confirm_uncertain:true});
      await card.getByRole('button',{name:'发送更新'}).click();await card.getByRole('button',{name:'更新',exact:true}).waitFor();
      assert.equal(posts('send').at(-1).action,'update');assert.equal(posts('send').at(-1).text,update);
      await item('配电监控').click();await card.getByRole('button',{name:'重试未送达的群'}).click();
      await card.getByRole('button',{name:'重试未送达的群'}).waitFor({state:'hidden'});
      assert.deepEqual(posts('retry').at(-1),{scope:'A',event_id:secondId,operation_id:'fixture-'+secondId,confirm_uncertain:false});
      await item(title).click();await card.getByRole('button',{name:'结束',exact:true}).click();
      const end='【事件通告】状态：结束\n【标题】冷冻水回水温度偏高\n【进展】回水温度恢复，完成本次跟踪。';
      await bodyText.fill(end);await snap('06-结束事件-恢复情况原文',[list.locator('[aria-current=true]'),bodyText,card.getByRole('button',{name:'发送结束'})]);await card.getByRole('button',{name:'发送结束'}).click();
      await page.waitForFunction(()=>document.querySelector('.evt-segment [aria-pressed=true]')?.textContent.includes('历史事件'));
      assert.equal(mock.rows.get(eventId).state,'ended');assert.equal(await card.getByRole('button',{name:'更新',exact:true}).count(),0);
      if(!await card.getByLabel('告警描述',{exact:true}).isVisible())await card.locator('.evt-meta summary').click();
      await card.getByLabel('告警描述',{exact:true}).fill('回水温度恢复，现场监测记录已补齐');
      await page.waitForResponse(res=>res.url().includes('/workbench-events/metadata')&&res.status()===200);
      await page.getByRole('button',{name:/^进行中/}).click();
      await composer.locator('summary').first().click();const input=composer.getByLabel('本次通告原文');
      const start='【事件通告】状态：新增\n【标题】A楼备用泵运行反馈延迟\n【进展】值班人员已到场核对。';await input.fill(start);
      if(!await composer.getByLabel('告警描述',{exact:true}).isVisible())await composer.locator('.evt-meta summary').click();
      await composer.getByLabel('告警描述',{exact:true}).fill('备用泵反馈延迟，现场复核中');
      await page.waitForResponse(res=>res.url().includes('/workbench-events/metadata')&&res.status()===200);
      assert.equal(await composer.getByLabel('告警描述',{exact:true}).inputValue(),'备用泵反馈延迟，现场复核中');
      await composer.getByRole('button',{name:'发送新增'}).click();await composer.getByRole('alert').waitFor();
      await page.reload();await composer.getByRole('button',{name:'核验并重试原发送'}).waitFor();assert.equal(await input.inputValue(),start);assert(await input.isDisabled());
      await snap('03-新建事件-原报文待核验',[input,composer.getByRole('status').filter({hasText:'原发送结果待核验'}),composer.getByRole('button',{name:'核验并重试原发送'})]);
      await composer.getByRole('button',{name:'核验并重试原发送'}).click();await composer.getByRole('button',{name:'发送新增'}).waitFor();
      const starts=posts('send').filter(b=>b.action==='start');assert.equal(starts.length,2);assert.deepEqual(starts[0],starts[1]);assert.equal(starts[0].dingtalk_robot_ids,undefined);
      if(await composer.getAttribute('open')!==null)await composer.locator('summary').first().click();
      assert.equal(posts('delete').length,0);
    }
    assert.deepEqual(errors,[]);console.log(canDelete?'PASS authorised removal, cancel, failed retention and confirmed success':'PASS creation, raw replay, local drafts, update/end, history metadata, channel retry, robot revision and no delete grant');
  }catch(e){console.error((await page.locator('body').innerText()).slice(-1100),errors);throw e}
  finally{await ctx.close();await mock.close();}
}
try{for(const width of [1440,393]){await run(false,width);await run(true,width);}}finally{await browser.close();}

