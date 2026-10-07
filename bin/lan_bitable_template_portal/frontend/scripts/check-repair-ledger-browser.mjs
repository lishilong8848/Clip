import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/repair-ledger');
await mkdir(output, { recursive: true });
const html = `<html><head><meta charset="utf-8"></head><body><div id="app"></div><script type="module">
import {createApp,h} from 'vue';
import Followup from '/src/components/RepairFollowupPanel.vue';
import UiTransition from '/src/components/UiTransition.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import '/src/global.css';
createApp({render:()=>h(Followup,{scope:'A',summaryRecordId:'rec_parent',summaryTitle:'A楼变压器维修'})})
.component('UiTransition',UiTransition).component('LoadingIndicator',LoadingIndicator).mount('#app');
</script></body></html>`;
const server = await createServer({root, logLevel:'error', server:{host:'127.0.0.1',port:0,hmr:false}, plugins:[{
  name:'ledger-fixture',configureServer(dev){dev.middlewares.use('/__ledger',async(_req,res)=>{
    res.setHeader('Content-Type','text/html');res.end(await dev.transformIndexHtml('/__ledger',html));
  });},
}]});
const rows = Array.from({length:125},(_,i)=>({record_id:`rec_dev${i}`, '设备编号':`A-TRB-${String(i+1).padStart(3,'0')}`, '机楼':'南通A楼',
  '系统名称':i<70?'供配电系统':'动力系统', '大设备类型':i<100?'变压器':'柴油发电机', '设备名称':i<100?'干式变压器':'柴油发电机',
  '产品其它参数':'温控器 PT100', '品牌':'维谛', '安装位置':'A-201 配电间', '型号':'SCB-1600', '设备类型标识':'TRB', '容量':'1600kVA'}));
const filterFields=['机楼','系统名称','大设备类型','设备名称'];
const options=Object.fromEntries(filterFields.map(f=>[f,[...new Set(rows.map(r=>r[f]))]]));
let browser,page,saved=null,saving=false,initializing=true;
const requests=[],writes=[],errors=[];
async function filter(field,value){
  await page.locator('.ledger-filters label').filter({hasText:field}).getByRole('combobox').click();
  await page.getByRole('option',{name:value,exact:true}).click();
  await page.waitForFunction(()=>!document.querySelector('.picker-search')?.disabled);
}
try{
  await server.listen();browser=await chromium.launch({headless:true});page=await browser.newPage({viewport:{width:1440,height:960}});
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/api/**',async route=>{
    const request=route.request(),url=new URL(request.url());
    if (!url.pathname.startsWith('/api/')) return route.continue();
    let data;
    if(url.pathname==='/api/repair-management/followups'&&request.method()==='GET') data={records:saved?[saved]:[],total:saved?1:0,fields:[{field_name:'维修进展描述',field_type:1,editable:true}],shared_fields:{}};
    else if(url.pathname==='/api/repair-management/followups'&&request.method()==='POST'){
      const body=request.postDataJSON();writes.push(body);saving=true;await new Promise(r=>setTimeout(r,300));
      saved={record_id:'rec_followup',title:'设备已选择',raw_fields:body.fields,display_fields:body.fields,cmdb_record_ids:[],ledger_device_ids:body.ledger_device_ids,ledger_devices:rows.filter(r=>body.ledger_device_ids.includes(r.record_id))};
      data={record_id:saved.record_id,fields:body.fields};saving=false;
    }else if(url.pathname==='/api/repair-management/ledger-candidates'){
      requests.push(Object.fromEntries(url.searchParams));
      let filtered=rows.filter(r=>filterFields.every(f=>!url.searchParams.get(f)||r[f]===url.searchParams.get(f)));
      const q=url.searchParams.get('q')||'';if(q)filtered=filtered.filter(r=>Object.values(r).join(' ').includes(q));
      const p=Number(url.searchParams.get('page')||1);
      data={records:initializing?[]:filtered.slice((p-1)*50,p*50),total:initializing?0:filtered.length,page:p,options,
        cache:{ready:!initializing,refreshing:initializing,error:null},can_force_refresh:true};
      initializing=false;
    }else{errors.push(`Unexpected API ${request.method()} ${url.pathname}`);await route.fulfill({status:400,contentType:'application/json',body:JSON.stringify({ok:false,message:'Unexpected API'})});return;}
    await route.fulfill({contentType:'application/json',body:JSON.stringify({ok:true,data})});
  });
  await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/__ledger`);
  await page.getByRole('button',{name:'选择台账设备',exact:true}).click();
  const dialog=page.getByRole('dialog',{name:'选择台账设备'});
  await dialog.getByText('正在初始化设备台账，完成后自动显示。',{exact:true}).waitFor();
  await dialog.getByText('A-TRB-001',{exact:true}).waitFor();
  assert.equal(await dialog.locator('tbody tr').count(),50);
  await dialog.getByRole('checkbox',{name:'选择A-TRB-001',exact:true}).check();
  await dialog.getByRole('button',{name:'下一页',exact:true}).click();
  await dialog.getByText('A-TRB-051',{exact:true}).waitFor();
  await dialog.getByRole('checkbox',{name:'选择A-TRB-051',exact:true}).check();
  await filter('机楼','南通A楼');await filter('大设备类型','变压器');
  assert.equal(requests.at(-1)['机楼'],'南通A楼');assert.equal(requests.at(-1)['大设备类型'],'变压器');assert(!requests.at(-1)['系统名称']);
  await dialog.getByRole('searchbox').fill('PT100');
  await page.waitForFunction(()=>document.querySelector('.picker-search')?.disabled===false);
  await page.waitForTimeout(350);
  assert.equal(requests.at(-1).q,'PT100');
  await dialog.getByRole('button',{name:'下一页',exact:true}).click();await dialog.getByText('A-TRB-051',{exact:true}).waitFor();
  assert(await dialog.getByRole('checkbox',{name:'选择A-TRB-051',exact:true}).isChecked(),'filter/page changes preserve selections');
  const bounds=await dialog.boundingBox(),footer=await dialog.locator('footer').boundingBox();
  assert(bounds.width>1300&&footer.y+footer.height<=960,'large modal keeps confirmation visible');
  await page.screenshot({path:path.join(output,'equipment-picker.png')});
  await dialog.getByRole('button',{name:'确认',exact:true}).click();await dialog.waitFor({state:'detached'});
  assert.equal(writes.length,0,'selecting is not saving');
  const save=page.locator('.followup-action-bar button.primary');
  await save.click();await page.waitForTimeout(40);assert(await save.isDisabled(),'save cannot double submit');
  await page.getByText('维修跟进记录已新增。',{exact:true}).waitFor();
  assert.deepEqual(writes[0].ledger_device_ids,['rec_dev0','rec_dev50']);assert.deepEqual(writes[0].cmdb_record_ids,[]);
  await page.waitForTimeout(550);await page.getByRole('button',{name:'重新选择台账设备',exact:true}).click();
  await dialog.getByText('A-TRB-001',{exact:true}).waitFor();assert(await dialog.getByRole('checkbox',{name:'选择A-TRB-001',exact:true}).isChecked());
  await dialog.getByRole('checkbox',{name:'选择A-TRB-002',exact:true}).check();
  await dialog.getByRole('button',{name:'取消',exact:true}).click();await dialog.waitFor({state:'detached'});
  await page.getByRole('button',{name:'重新选择台账设备',exact:true}).click();await dialog.getByText('A-TRB-002',{exact:true}).waitFor();
  assert(!(await dialog.getByRole('checkbox',{name:'选择A-TRB-002',exact:true}).isChecked()),'cancel discards only tentative changes');
  await page.keyboard.press('Escape');await dialog.waitFor({state:'detached'});
  assert.equal(writes.length,1);assert.equal(saving,false);assert.deepEqual(errors,[]);
  console.log('[pass] local multi-select, 4 AND filters, search, 50-row pages, cancel/restore, explicit followup save, visible footer');
}catch(e){console.error(errors);if(page)await page.screenshot({path:path.join(output,'failure.png')}).catch(()=>{});throw e;}
finally{await browser?.close();await server.close();}
