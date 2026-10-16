import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/cabinet-details');
await mkdir(output,{recursive:true});
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`,browser=await chromium.launch({headless:true});
const racks=[{room:'201',rack:'A01',state:'formal',rack_type:'服务器机柜',power:4200,positions:['A1']},{room:'202',rack:'B01',state:'off',rack_type:'网络机柜',power:0,positions:['B1']},{room:'201',rack:'A02',state:'off',rack_type:'服务器机柜',power:0,positions:['A2']}];
const op={record_id:'fixture-record',scope:'A',room:'201',rack:'A01',rack_type:'服务器机柜',power:4200,source:'机柜上电时间统计',source_row:8,category:'up',version:'v1',issues:[],groups:[{id:'g1',action:'上正式电',expected:'2026-10-15 08:00',actual:'2026-10-15 08:20',result:'成功'}],events:[{id:'e1',action:'上正式电',expected:'2026-10-15 08:00',actual:'2026-10-15 08:20',result:'成功'}]};
const rooms=['201','202'].map((id,i)=>({id,name:id+'机房',sheet:id,total:i?1:2,counts:{formal:i?0:1,test:0,off:1,unknown:0},types:{'服务器机柜':i?0:2,'网络机柜':i?1:0}}));
const format={sheet:op.source,groups:[{}],columns:[{column:1,field:'room',label:'包间',width:100},{column:2,field:'rack',label:'机柜',width:100},{column:3,field:'rack_type',label:'类型',width:160}]};
let guest=false,failHistory=false,delayMap=false,releaseMap,delayHistory=false,releaseHistory,paginated=false;
const reads=[],errors=[];
try{
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    try{
      const req=route.request(),url=new URL(req.url()),p=url.pathname;
      if(url.origin!==origin)return route.abort();
      if(p==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
      if(!p.startsWith('/api/'))return route.continue();
      assert.equal(req.method(),'GET','No real writes in cabinet verification');
      reads.push(url);const ok=data=>route.fulfill({json:{ok:true,data}});
      if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'cabinet-fixture',name:'隔离机柜验收',role:guest?'guest':'admin',level:guest?'L4':'L0',modules:['cabinet']},scope_options:[{value:'A',label:'A楼'}]});
      if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'cabinet-details-fixture'}});
      if(p==='/api/cabinet-power/bootstrap')return ok({status:'succeeded',ready:1,total:1,buildings:[{scope:'A',status:'succeeded'}]});
      if(p==='/api/cabinet-power/overview')return ok({version:1,updated_at:'2026-10-15 12:00',record_count:1,issues:[],rooms,counts:{total:3,powered:1,formal:1,test:0,off:2,unknown:0},sheet_formats:[format]});
      if(p==='/api/cabinet-power/racks')return ok({version:1,items:racks});
      if(p.includes('/rooms/')&&p.endsWith('/layout')){
        const room=p.split('/')[4];
        const data={layout:{width:900,height:600,cells:racks.filter(r=>r.room===room).map((r,i)=>({...r,ref:r.rack,x:100+i*90,y:80,width:70,height:60,text:r.rack,color:r.state==='formal'?'#f00':'#00b050',style:{}}))}};
        if(delayMap&&room==='202')return new Promise(resolve=>{releaseMap=async()=>{releaseMap=undefined;try{await ok(data);}finally{resolve();}};});
        return ok(data);
      }
      if(p==='/api/cabinet-power/operations'){
        if(failHistory&&url.searchParams.get('rack'))return route.fulfill({status:503,json:{ok:false,error:'隔离测试：机柜历史暂时不可用'}});
        const room=url.searchParams.get('room'),rack=url.searchParams.get('rack');
        const number=Number(url.searchParams.get('page')||1);
        const items=!rack||room==='201'&&rack==='A01'?[number===2?{...op,record_id:'fixture-history-2',source:'较早的操作记录'}:op]:[];
        const data={items,total:paginated&&rack==='A01'?2:items.length,page:number,page_size:50,rack_state:racks.find(r=>r.room===room&&r.rack===rack)};
        if(delayHistory&&rack==='A01'&&number===2)return new Promise(resolve=>{releaseHistory=async()=>{releaseHistory=undefined;try{await ok(data);}finally{resolve();}};});
        return ok(data);
      }
      return ok({items:[],records:[],total:0,pending:0,settings:{}});
    }catch(error){errors.push(error.message);await route.abort();}
  });
  const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
  const go=()=>page.goto(origin+'/assets/workspace-v2/index.html?__page=/cabinet-power&scope=A');
  const drawer=page.getByRole('dialog',{name:'机柜完整历史',exact:true});
  const shot=name=>page.screenshot({path:path.join(output,name+'.png'),animations:'disabled'});
  try{
    for(const width of [1440,390]){
      await page.setViewportSize({width,height:width===390?844:900});await go();
      await page.getByRole('button',{name:'原始平面图',exact:true}).click();
      const cell=page.locator('.map-cell[aria-label="A01 正式电"]');await cell.click();
      await drawer.getByText('上正式电 · 成功',{exact:true}).waitFor();
      assert.match(await drawer.innerText(),/4200 W/);assert.match(await drawer.innerText(),/2026-10-15 08:20/);
      const box=await drawer.boundingBox();assert(box.height>400&&box.y>=0&&box.y+box.height<=(width===390?844:900)+1,'details are inside the viewport');
      assert(await drawer.locator('.history-record').evaluate(el=>el.getBoundingClientRect().height>30),'history content is not collapsed');
      await shot('history-'+width);await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();
      await page.getByRole('button',{name:'机柜状态',exact:true}).click();
      await page.getByRole('table',{name:'机柜状态明细'}).getByRole('button',{name:'A01',exact:true}).click();
      await drawer.getByText('上正式电 · 成功',{exact:true}).waitFor();await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();
      await page.getByRole('button',{name:'机柜台账',exact:true}).click();
      await page.getByRole('button',{name:'查看完整历史',exact:true}).click();
      await drawer.getByText('上正式电 · 成功',{exact:true}).waitFor();await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();
      await page.getByRole('button',{name:'包间汇总',exact:true}).click();
      await page.getByRole('button',{name:'201包间已上电',exact:true}).click();
      assert.equal(await page.getByRole('table',{name:'机柜状态明细'}).locator('tbody tr').count(),1);
    }
    await page.setViewportSize({width:1440,height:900});await go();
    await page.getByRole('button',{name:'原始平面图',exact:true}).click();await page.locator('.map-cell[aria-label="A01 正式电"]').waitFor();
    delayMap=true;await page.locator('.room-sidebar button').filter({hasText:'202 包间'}).click();
    await page.waitForFunction(()=>document.body.textContent.includes('正在读取平面图'));
    await page.locator('.room-sidebar button').filter({hasText:'201 包间'}).click();
    await page.locator('.map-cell[aria-label="A01 正式电"]').waitFor();
    assert(releaseMap);await releaseMap();delayMap=false;await page.waitForTimeout(200);
    assert.equal(await page.locator('.map-cell[aria-label="B01 未上电/已下电"]').count(),0,'late map response cannot replace the cached selected room');
    failHistory=true;await page.locator('.map-cell[aria-label="A01 正式电"]').click();
    await drawer.getByText('隔离测试：机柜历史暂时不可用',{exact:true}).waitFor();
    failHistory=false;await drawer.getByRole('button',{name:'重试读取',exact:true}).click();await drawer.getByText('上正式电 · 成功',{exact:true}).waitFor();
    await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();await page.locator('.map-cell[aria-label="A02 未上电/已下电"]').click();
    await drawer.getByText('该机柜暂无已登记的操作历史。',{exact:true}).waitFor();
    await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();
    paginated=true;delayHistory=true;await page.locator('.map-cell[aria-label="A01 正式电"]').click();
    await drawer.getByRole('button',{name:'加载更多',exact:true}).click();
    assert(await drawer.getByRole('button',{name:'正在读取…',exact:true}).isDisabled());
    await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();await page.locator('.map-cell[aria-label="A02 未上电/已下电"]').click();
    await drawer.getByText('该机柜暂无已登记的操作历史。',{exact:true}).waitFor();
    assert(releaseHistory);await releaseHistory();delayHistory=false;await page.waitForTimeout(200);
    assert.equal(await drawer.locator('.history-record').count(),0,'late pagination cannot append another cabinet history');
    await drawer.getByRole('button',{name:'关闭历史',exact:true}).click();await page.locator('.map-cell[aria-label="A01 正式电"]').click();
    failHistory=true;await drawer.getByRole('button',{name:'加载更多',exact:true}).click();
    await drawer.getByText('隔离测试：机柜历史暂时不可用',{exact:true}).waitFor();
    assert.equal(await drawer.locator('.history-record').count(),1,'pagination failure retains loaded history');
    failHistory=false;await drawer.getByRole('button',{name:'重试读取',exact:true}).click();
    await drawer.getByText('较早的操作记录 第 8 行',{exact:true}).waitFor();
    assert.equal(await drawer.locator('.history-record').count(),2);
    assert.equal(await drawer.getByRole('button',{name:'加载更多',exact:true}).count(),0);
    guest=true;await go();await page.getByRole('button',{name:'原始平面图',exact:true}).click();await page.locator('.map-cell[aria-label="A01 正式电"]').click();
    await drawer.getByText('上正式电 · 成功',{exact:true}).waitFor();
    assert.equal(await drawer.getByRole('button',{name:'修改机柜功率',exact:true}).count(),0);
    assert.equal(await drawer.getByRole('button',{name:'编辑',exact:true}).count(),0);
    assert(reads.some(url=>url.pathname.endsWith('/operations')&&url.searchParams.get('scope')==='A'&&url.searchParams.get('room')==='201'&&url.searchParams.get('rack')==='A01'));
    assert.deepEqual(errors,[]);
    console.log('PASS: populated rack history, map/state/ledger drilldowns, summary filtering, cached-room race, history retry/empty/pagination isolation, guest read-only, PC/mobile. All APIs mocked.');
  }catch(error){await shot('failure');console.error(await page.locator('body').innerText(),errors);throw error;}
}finally{if(releaseMap)await releaseMap().catch(()=>{});if(releaseHistory)await releaseHistory().catch(()=>{});await browser.close();await server.close();}
