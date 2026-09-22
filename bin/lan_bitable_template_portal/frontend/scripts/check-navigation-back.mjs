import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { once } from 'node:events';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const reservation=createServer();
await new Promise(resolve=>reservation.listen(0,'127.0.0.1',resolve));
const port=reservation.address().port;
await new Promise(resolve=>reservation.close(resolve));
const base=`http://127.0.0.1:${port}`;
const output=path.join(root,'output/playwright/navigation');
await fs.mkdir(output,{recursive:true});
const server=spawn('python',[path.join(root,'bin/tools/cabinet_ui_fixture.py'),String(port)],{cwd:root,windowsHide:true,stdio:['ignore','pipe','pipe']});
let log=''; server.stdout.on('data',x=>log+=x); server.stderr.on('data',x=>log+=x);
let browser,page;
try {
  for(let i=0;i<60;i++){try{if((await fetch(base+'/api/health')).ok)break}catch{}if(i===59)throw Error(log);await new Promise(r=>setTimeout(r,250))}
  browser=await chromium.launch({headless:true});
  page=await browser.newPage({viewport:{width:1440,height:900}});
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  page.on('console',message=>{if(['warning','error'].includes(message.type())&&message.text().includes('Teleport'))errors.push(message.text());});
  async function assertBack(label){
    const back=page.locator('#page-back-slot .vnet-back-button');
    await back.waitFor({state:'visible'});
    assert.equal(await page.locator('.vnet-back-button:visible').count(),1,label);
    const rect=await back.boundingBox();
    const header=await page.locator('#page-navigation').boundingBox();
    const borderLeft=await page.locator('#page-navigation').evaluate(node=>node.clientLeft);
    assert.equal(Math.round(rect.x-header.x-borderLeft),24,label);
    assert(Math.abs(rect.y-header.y-header.height+14)<=2,`${label}: header boundary`);
    assert.equal(Math.round(rect.height),page.viewportSize().width<=920?44:40,label);
    assert(Number.parseFloat(await back.evaluate(node=>getComputedStyle(node).borderRadius))>=20,`${label}: rounded back button`);
    for(const child of await page.locator('#page-navigation > :not(#page-back-slot):not(.page-header-fade)').all()){
      const bounds=await child.boundingBox();
      if(bounds)assert(bounds.y+bounds.height<=rect.y||bounds.x>=rect.x+rect.width,`${label}: header content overlap`);
    }
    if(await page.evaluate(()=>window.scrollY===0)){
      const next=await page.locator('#page-navigation').evaluate(node=>node.nextElementSibling?.getBoundingClientRect().top);
      if(next!==undefined)assert(next>=rect.y+rect.height,`${label}: page content overlap`);
    }
    await page.evaluate(()=>window.scrollTo(0,document.body.scrollHeight));
    const scrolled=await back.boundingBox();
    const scrolledHeader=await page.locator('#page-navigation').boundingBox();
    assert(Math.abs(scrolled.y-scrolledHeader.y-scrolledHeader.height+14)<=2,`${label}: scroll boundary`);
    assert.equal(Math.round(scrolled.x-scrolledHeader.x-borderLeft),24,`${label}: scroll`);
    await page.evaluate(()=>window.scrollTo(0,0));
    return back;
  }
  async function assertOriginalHeader(label){
    const [current,original]=await page.evaluate(()=>{
      const header=document.querySelector('#page-navigation');
      const slot=document.querySelector('#page-back-slot');
      const fade=header.querySelector('.page-header-fade');
      const sheet=[...document.styleSheets].find(item=>item.href?.endsWith('/assets/page-navigation.css'));
      const measure=()=>[header,...header.querySelectorAll('.brand, .brand-logo, .brand h1, .brand p, .topbar-actions, .top-actions, .user-chip, .scope-switch')].map(node=>{
        const rect=node.getBoundingClientRect(),style=getComputedStyle(node);
        return {x:rect.x,y:rect.y,width:rect.width,height:rect.height,display:style.display,
          fontSize:style.fontSize,padding:style.padding,gap:style.gap,background:style.background};
      });
      const current=measure();
      sheet.disabled=true;slot.style.display='none';if(fade)fade.style.display='none';
      const original=measure();
      sheet.disabled=false;slot.style.removeProperty('display');if(fade)fade.style.removeProperty('display');
      return [current,original];
    });
    assert.deepEqual(current,original,`${label}: original header layout`);
    const fade=page.locator('#page-navigation > .page-header-fade');
    assert.equal(await fade.count(),1,`${label}: gradient edge`);
    assert(await fade.evaluate(node=>{
      const style=getComputedStyle(node);
      return style.position==='absolute'&&style.height==='56px'&&style.pointerEvents==='none'&&style.backgroundImage.includes('linear-gradient');
    }),`${label}: non-interactive gradient`);
  }
  const targets=[
    ['/cabinet-power?scope=D','/cabinet-power'], ['/daily-tasks?scope=D','/'], ['/water-management?scope=D','/?entry=water'],
    ['/critical-guard?scope=D','/critical-guard'], ['/drill-management?scope=D','/drill-management'], ['/repair-management?scope=D','/'],
    ['/repair-status?scope=D','/repair-management?scope=D'], ['/engineer/mop?scope=D','/'], ['/?scope=D&mode=events','/'],
    ['/admin/history-memory','/'], ['/signature-management','/'],
    ['/cabinet-power/batches?scope=E&status=todo','/cabinet-power?scope=E'],
    ['/cabinet-power/batches?scope=E&mode=new&origin=cabinet','/cabinet-power?scope=E'],
  ];
  for(const [target,expected] of targets){
    await page.goto(base+'/cabinet-power');
    await page.getByRole('heading',{name:'机柜上下电',exact:true}).waitFor();
    await page.evaluate(url=>{history.pushState({},'',url);window.dispatchEvent(new Event('popstate'))},target);
    const back=await assertBack(target);
    await assertOriginalHeader(target);
    await page.screenshot({path:path.join(output,`page-${targets.findIndex(item=>item[0]===target)}.png`)});
    await back.click();
    await page.waitForURL(base+expected);
    assert.equal(new URL(page.url()).pathname+new URL(page.url()).search,expected,target);
  }
  await page.goto(base+'/cabinet-power');
  await page.evaluate(()=>location.assign('/workbench-lite?scope=D&work_type=maintenance'));
  await assertBack('workbench');
  await assertOriginalHeader('workbench');
  await page.screenshot({path:path.join(output,'workbench.png')});
  await page.locator('#lite-back-link').click();
  await page.waitForURL(base+'/?entry=maintenance');
  await page.goto(base+'/');
  await page.getByRole('button',{name:'进入维护管理',exact:true}).click();
  await page.getByRole('heading',{name:'选择楼栋进入维护管理',exact:true}).waitFor();
  await page.getByRole('button',{name:'进入维护管理：D楼',exact:true}).click();
  await page.locator('#lite-back-link').click();
  await page.waitForURL(base+'/?entry=maintenance');
  await page.getByRole('heading',{name:'选择楼栋进入维护管理',exact:true}).waitFor();
  await page.locator('.vnet-back-button').click();
  await page.waitForURL(base+'/');
  await page.getByRole('button',{name:'进入容量管理',exact:true}).click();
  await page.getByRole('heading',{name:'选择容量管理模板',exact:true}).waitFor();
  await page.getByRole('button',{name:'选择水耗管理',exact:true}).click();
  await page.getByRole('heading',{name:'选择楼栋进入水耗管理',exact:true}).waitFor();
  await page.locator('.vnet-back-button').click();
  await page.waitForURL(base+'/?entry=capacity');
  await page.getByRole('button',{name:'选择机柜管理',exact:true}).click();
  await page.getByRole('heading',{name:'机柜上下电',exact:true}).waitFor();
  await page.locator('.vnet-back-button').click();
  await page.waitForURL(base+'/?entry=capacity');
  await page.goto(base+'/workbench-lite?scope=D&work_type=maintenance');
  await page.goto(base+'/workbench-lite?scope=D&work_type=change');
  await page.locator('#lite-back-link').click();
  await page.waitForURL(base+'/?entry=change');
  const entryPages={
    maintenance:'选择楼栋进入维护管理', maintenance_mop:'选择楼栋进入 MOP 填写',
    change:'选择楼栋进入变更管理', repair:'选择楼栋进入检修通告管理',
    repair_management:'选择楼栋进入检修单管理', capacity:'选择容量管理模板', water:'选择楼栋进入水耗管理',
    tools:'选择辅助工具', daily:'选择楼栋查看每日任务',
    power:'选择楼栋进入上/下电通告', polling:'选择楼栋进入设备轮巡',
    adjust:'选择楼栋进入设备调整', handover:'选择楼栋打开交接班审核页',
  };
  for(const [entry,title] of Object.entries(entryPages)){
    await page.goto(base+'/?entry='+entry);
    await page.getByRole('heading',{name:title,exact:true}).waitFor();
    await assertBack(entry);
  }
  await page.goto(base+'/repair-management?scope=D');
  for(let i=0;i<3;i++){
    await assertBack('repair management cached');
    await page.getByRole('button',{name:'检修状态',exact:true}).click();
    await page.waitForURL(base+'/repair-status?scope=D');
    await (await assertBack('repair status cached')).click();
    await page.waitForURL(base+'/repair-management?scope=D');
  }
  for(const width of [1920,1280,1024,390]){
    await page.setViewportSize({width,height:900});
    await page.goto(base+'/cabinet-power/batches?scope=E&status=todo');
    await assertBack(`viewport ${width}`);
    assert(await page.locator('#page-navigation').evaluate(node=>node.scrollWidth<=node.clientWidth+1));
    await page.screenshot({path:path.join(output,`viewport-${width}.png`)});
  }
  await page.setViewportSize({width:1440,height:900});
  await page.route('**/api/cabinet-power/batches?*',route=>route.fulfill({json:{ok:true,data:{
    items:Array.from({length:20},(_,index)=>({batch_id:`scroll-${index}`,title:`滚动检查批次${index+1}`,
      scopes:['E'],source:'manual',status:'pending',is_todo:true,stats:{total:1,confirmable:1}})),
    total:20,page:1,page_size:20,
  }}}));
  for(const width of [1440,1024,390]){
    await page.setViewportSize({width,height:700});
    await page.goto(base+'/cabinet-power/batches?scope=E&status=todo');
    await page.getByText('滚动检查批次20',{exact:true}).waitFor();
    await page.evaluate(()=>window.scrollTo(0,400));
    await page.waitForFunction(()=>window.scrollY>200);
    await assertBack(`long-page ${width}`);
  }
  await page.unroute('**/api/cabinet-power/batches?*');
  await page.route('**/api/auth/status*',route=>route.fulfill({json:{ok:true,data:{logged_in:true,
    user:{name:'南通基地设施运行管理超长测试账户名称',open_id:'fixture-user',role:'admin',is_admin:true},
    scope_options:['A','B','C','D','E'].map(value=>({value,label:value+'楼'})),
  }}}));
  for(const width of [1440,1024,921,390]){
    await page.setViewportSize({width,height:800});
    await page.goto(base+'/?scope=D&mode=events');
    await page.locator('#page-navigation .scope-switch').waitFor();
    assert.equal(await page.locator('#page-navigation .user-chip').innerText(),'南通基地设施运行管理超长测试账户名称');
    await assertBack(`full toolbar ${width}`);
    assert(await page.locator('#page-navigation').evaluate(node=>node.scrollWidth<=node.clientWidth+1));
    const logo=page.locator('#page-navigation .brand-logo');
    if(await logo.isVisible()){
      const bounds=await logo.boundingBox(),actions=await page.locator('#page-navigation .topbar-actions').boundingBox();
      assert(bounds.y+bounds.height<=actions.y||bounds.x+bounds.width<=actions.x,`header overlap ${width}`);
    }
    await assertOriginalHeader(`full toolbar ${width}`);
    await page.screenshot({path:path.join(output,`toolbar-${width}.png`)});
  }
  await page.unroute('**/api/auth/status*');
  await page.setViewportSize({width:1440,height:900});
  for(const route of ['/signature?record_id=fixture','/drill-management/print?scope=D','/daily-tasks/morning-meeting/print?scope=D']){
    await page.goto(base+'/repair-management?scope=D');
    await assertBack('before standalone page');
    await page.evaluate(url=>{history.pushState({},'',url);window.dispatchEvent(new Event('popstate'));},route);
    await page.locator('#page-navigation').waitFor({state:'hidden'});
    await page.evaluate(()=>{history.pushState({},'','/repair-management?scope=D');window.dispatchEvent(new Event('popstate'));});
    await assertBack('after standalone page');
  }
  await page.route('**/api/engineer/mop/bootstrap?*',route=>route.fulfill({json:{ok:true,data:{
    notices:[{notice_key:'nav-notice',title:'导航测试维保',scope:'E',building:'E楼',source_record_id:'fixture-notice'}],
    mop_candidates:[{record_id:'fixture-mop',title:'导航测试MOP',attachments:[{file_token:'fixture-file',name:'test.xlsx'}]}],
  }}}));
  await page.route('**/api/engineer/mop/bind',route=>route.fulfill({json:{ok:true,data:{binding:{mop_record_id:'fixture-mop',mop_attachment_token:'fixture-file'}}}}));
  await page.route('**/api/engineer/mop/preview?*',route=>route.fulfill({json:{ok:true,data:{
    local_file:{path:'fixture.xlsx'},sheets:[{name:'导航测试表',is_cover:true,row_count:0,column_count:0,rows:[]}],
  }}}));
  await page.goto(base+'/engineer/mop?scope=E');
  await page.getByRole('button',{name:'打开填写',exact:true}).click();
  await page.locator('.preview-head').waitFor();
  await (await assertBack('MOP preview')).click();
  await page.locator('.preview-head').waitFor({state:'hidden'});
  assert.equal(new URL(page.url()).pathname,'/engineer/mop','preview back must not leave the MOP page');
  await assertBack('MOP binding');
  await page.goto(base+'/');
  await page.getByRole('button',{name:'申请楼栋权限',exact:true}).click();
  await (await assertBack('permission request')).click();
  await page.getByRole('button',{name:'进入维护管理',exact:true}).waitFor();
  assert.equal(await page.locator('.vnet-back-button:visible').count(),0);
  await page.route('**/api/polling-work-orders/session?*',route=>route.fulfill({json:{ok:true,data:{
    version:1,current_run_index:1,work_type:'maintenance',role_label:'操作人',assigned_person:{name:'测试'},
    title:'工单返回测试',sop_name:'测试步骤',work_orders:[{run_index:1,label:'第一轮'}],steps:[],can_release_selection:false,
  }}}));
  await page.goto(base+'/polling-work-order/steps?token=fixture&run_index=1');
  await page.getByRole('heading',{name:'工单返回测试',exact:true}).waitFor();
  await (await assertBack('work order')).click();
  await page.waitForURL(base+'/polling-work-order?token=fixture');
  await page.goto(base+'/');
  await page.getByRole('button',{name:'进入维护管理',exact:true}).waitFor();
  assert.equal(await page.locator('.vnet-back-button:visible').count(),0,'home has no back button');
  assert.deepEqual(errors,[]);
  for(const file of ['bin/lan_bitable_template_portal/server.py','bin/clipflow_backend/main.py']){
    const source=await fs.readFile(path.join(root,file),'utf8');
    assert(!source.includes('history.back()'),file);
  }
  console.log(JSON.stringify({ok:true,pages:targets.length+1,errorPages:2,restoredEntryPages:Object.keys(entryPages).length}));
} catch(error) {
  if(page&&!page.isClosed())await page.screenshot({path:path.join(output,'failure.png')});
  console.error(log.slice(-1600));throw error;
} finally {
  await browser?.close();
  if(server.exitCode===null){const exited=once(server,'exit');server.kill();await exited;}
}
