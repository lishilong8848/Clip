import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createServer} from 'node:net';
import {mkdir, mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../../..');
const output=path.join(root,'output/playwright/text-fill');
await mkdir(output,{recursive:true});
const data=await mkdtemp(path.join(tmpdir(),'clipflow-text-fill-ui-'));
const reservation=createServer();
await new Promise(resolve=>reservation.listen(0,'127.0.0.1',resolve));
const port=reservation.address().port;
await new Promise(resolve=>reservation.close(resolve));
const base=`http://127.0.0.1:${port}`;
const server=spawn(process.env.PYTHON||'python',['bin/tools/cabinet_ui_fixture.py',String(port)],{
  cwd:root,windowsHide:true,stdio:['ignore','pipe','pipe'],
  env:{...process.env,CLIPFLOW_DATA_DIR:data,PYTHONPATH:path.join(root,'output/offline-audit')},
});
let log='',browser,page;
server.stdout.on('data',value=>log+=value);server.stderr.on('data',value=>log+=value);
const api=async(url,body,method='POST')=>{
  const response=await fetch(base+'/api/cabinet-power/'+url,{method,headers:{'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
  const payload=await response.json();assert(response.ok,JSON.stringify(payload));return payload.data;
};
try{
  for(let index=0;index<120;index++){
    try{if((await fetch(base+'/api/auth/status')).ok)break;}catch{}
    if(server.exitCode!==null || index===119)throw Error(log||'Fixture did not start');
    await new Promise(resolve=>setTimeout(resolve,500));
  }
  const row=(rack)=>({scope:'E',room:'202',rack,action:'上正式电',expected:'',actual:'',result:'成功',rack_type:'服务器机柜'});
  const batch=await api('batches',{rows:[row('A11'),row('A12'),row('A12')]});
  browser=await chromium.launch();page=await browser.newPage({viewport:{width:1366,height:900}});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto(`${base}/cabinet-power/batches?scope=E&status=todo`);
  await page.getByRole('button',{name:'处理待办',exact:true}).first().click();
  await page.getByRole('button',{name:'粘贴文本识别',exact:true}).click();
  const dialog=page.getByRole('dialog',{name:'粘贴文本识别',exact:true});
  const paste=async(text)=>{
    const response=page.waitForResponse(response=>response.url().endsWith('/text-preview'));
    await dialog.locator('textarea').evaluate((node,text)=>{
      const clipboardData=new DataTransfer();clipboardData.setData('text/plain',text);
      node.dispatchEvent(new ClipboardEvent('paste',{clipboardData,bubbles:true,cancelable:true}));
    },text);
    await (await response).finished();
    await dialog.getByRole('status').waitFor({state:'hidden'});
  };
  const line=(rack,action='测试电转正式电')=>`EA118-E2-2\t${rack}\t${action}\t2026-09-16 16:02:59\t2026-09-14 16:03:16`;
  await paste([line('A11'),line('A12'),line('Z99')].join('\n'));
  assert.equal(await dialog.locator('tbody tr').count(),3);
  assert.equal(await dialog.locator('textarea').inputValue(),'');
  assert(await dialog.getByText('本批次无此机柜',{exact:true}).isVisible());
  assert.equal((await api('batches/'+batch.batch_id,undefined,'GET')).rows[0].actual,'');
  assert(await dialog.getByText('同柜多条，请在待办中核对',{exact:true}).isVisible());
  await paste(line('A11','上测试电'));
  assert.equal(await dialog.getByText('内容冲突，请移除错误记录',{exact:true}).count(),2);
  assert(await dialog.getByRole('button',{name:/填入本批次/}).isDisabled());
  await dialog.getByLabel('移除识别记录 A11',{exact:true}).last().click();
  await page.screenshot({path:path.join(output,'desktop-preview.png')});
  for(const width of [1366,1024,600]){
    await page.setViewportSize({width,height:800});
    assert(await dialog.evaluate(node=>{const r=node.getBoundingClientRect();return r.left>=0&&r.top>=0&&r.right<=innerWidth&&r.bottom<=innerHeight;}));
    const footer=await dialog.locator('footer').boundingBox();assert(footer.y+footer.height<=800);
    await page.screenshot({path:path.join(output,`preview-${width}.png`)});
  }
  await page.setViewportSize({width:1366,height:900});
  await dialog.getByRole('button',{name:/填入本批次/}).click();
  await dialog.waitFor({state:'hidden'});
  const saved=await api('batches/'+batch.batch_id,undefined,'GET');
  assert.equal(saved.rows.length,3);assert.equal(saved.rows[0].actual,'2026-09-14 16:03:16');
  assert.equal(saved.rows[1].actual,'');assert.equal(saved.rows[2].action,'上正式电');
  assert.equal(saved.rows[0].edits.filter(edit=>edit.source==='text_fill').length,3);

  await page.getByRole('button',{name:'粘贴文本识别',exact:true}).click();
  await paste('EA118-E2-2 A11');
  assert(await dialog.getByRole('alert').isVisible());assert.equal(await dialog.locator('tbody tr').count(),0);
  await paste(line('A11','正式电转测试电'));
  const current=await api('batches/'+batch.batch_id,undefined,'GET');
  await api('batches/'+batch.batch_id,{version:current.version,rows:[{row_id:batch.rows[0].row_id,action:'下正式电'}]},'PATCH');
  await dialog.getByRole('button',{name:/填入本批次/}).click();
  await dialog.getByRole('button',{name:'重新核对',exact:true}).waitFor();
  assert(await dialog.getByRole('button',{name:/填入本批次/}).isDisabled());
  await dialog.getByRole('button',{name:'重新核对',exact:true}).click();
  await dialog.getByText('原：下正式电',{exact:true}).waitFor();
  // A lost apply response must be reconciled before another apply, without duplicate audit entries.
  await page.route('**/text-apply',async route=>{await route.fetch();await route.abort('connectionreset');},{times:1});
  await dialog.getByRole('button',{name:/填入本批次/}).click();
  await dialog.getByRole('button',{name:'重新核对',exact:true}).waitFor();
  await dialog.getByRole('button',{name:'重新核对',exact:true}).click();
  await dialog.getByText('内容一致，跳过',{exact:true}).waitFor();
  const afterLoss=await api('batches/'+batch.batch_id,undefined,'GET');
  assert(await dialog.getByRole('button',{name:/填入本批次/}).isDisabled());
  await dialog.getByRole('button',{name:'关闭文本识别',exact:true}).click();
  await page.getByRole('button',{name:'放弃回填',exact:true}).click();
  await dialog.waitFor({state:'hidden'});
  const afterRetry=await api('batches/'+batch.batch_id,undefined,'GET');
  assert.deepEqual(afterRetry.rows[0].edits,afterLoss.rows[0].edits);

  await page.getByRole('button',{name:'粘贴文本识别',exact:true}).click();
  await paste(Array.from({length:100},()=>line('A11')).join('\n'));
  assert.equal(await dialog.locator('tbody tr').count(),25);
  assert(await dialog.getByRole('button',{name:'填入本批次（1）',exact:true}).isEnabled());
  await page.screenshot({path:path.join(output,'hundred-rows.png')});
  assert(await dialog.getByRole('button',{name:'上一页',exact:true}).isEnabled());
  await dialog.getByRole('button',{name:'上一页',exact:true}).click();
  assert.equal(await dialog.locator('tbody tr').count(),25);
  await dialog.getByRole('button',{name:'关闭文本识别',exact:true}).click();
  await page.getByRole('button',{name:'继续核对',exact:true}).click();
  assert(await dialog.isVisible());
  await page.evaluate(()=>history.back());
  await page.getByRole('button',{name:'继续核对',exact:true}).click();
  assert(page.url().includes('batch_id='));assert(await dialog.isVisible());
  await dialog.getByRole('button',{name:/填入本批次/}).focus();await page.keyboard.press('Tab');
  assert(await dialog.getByRole('button',{name:'关闭文本识别',exact:true}).evaluate(node=>node===document.activeElement));
  await page.keyboard.press('Escape');
  await page.getByRole('button',{name:'放弃回填',exact:true}).click();
  await dialog.waitFor({state:'hidden'});
  assert.deepEqual(errors,[]);
  console.log('PASS: paste/match/preview, ambiguous targets, duplicates, version conflicts, response loss, 100 rows, viewport bounds; fake data only');
}catch(error){
  if(page&&!page.isClosed())await page.screenshot({path:path.join(output,'failure.png')});
  console.error(log.slice(-2000));throw error;
}finally{
  await browser?.close();
  if(server.exitCode===null){const stopped=once(server,'exit');server.kill();await stopped;}
  if(path.dirname(path.resolve(data))===path.resolve(tmpdir())&&path.basename(data).startsWith('clipflow-text-fill-ui-'))await rm(data,{recursive:true,force:true});
}
