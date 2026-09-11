import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const base='http://127.0.0.1:8794';
const server=spawn('python',[path.join(root,'bin/tools/cabinet_ui_fixture.py'),'8794'],{cwd:root,windowsHide:true,stdio:['ignore','pipe','pipe']});
let log=''; server.stdout.on('data',x=>log+=x); server.stderr.on('data',x=>log+=x);
let browser;
try {
  for(let i=0;i<60;i++){try{if((await fetch(base+'/api/health')).ok)break}catch{}if(i===59)throw Error(log);await new Promise(r=>setTimeout(r,250))}
  browser=await chromium.launch({headless:true});
  const page=await browser.newPage({viewport:{width:1440,height:900}});
  const targets=[
    ['/cabinet-power?scope=D','/cabinet-power'], ['/daily-tasks?scope=D','/'], ['/water-management?scope=D','/'],
    ['/critical-guard?scope=D','/critical-guard'], ['/drill-management?scope=D','/drill-management'], ['/repair-management?scope=D','/'],
    ['/repair-status?scope=D','/repair-management?scope=D'], ['/engineer/mop?scope=D','/'], ['/?scope=D&mode=events','/'],
    ['/admin/history-memory','/'], ['/signature-management','/'],
  ];
  for(const [target,expected] of targets){
    await page.goto(base+'/cabinet-power');
    await page.getByRole('heading',{name:'机柜上下电',exact:true}).waitFor();
    await page.evaluate(url=>{history.pushState({},'',url);window.dispatchEvent(new Event('popstate'))},target);
    const back=target==='/signature-management'
      ? page.locator('.hero-actions button',{hasText:'返回'}).first()
      : page.locator('.vnet-back-button').first();
    await back.waitFor({state:'visible'});
    await back.click();
    await page.waitForURL(base+expected);
    assert.equal(new URL(page.url()).pathname+new URL(page.url()).search,expected,target);
  }
  await page.goto(base+'/cabinet-power');
  await page.evaluate(()=>location.assign('/workbench-lite?scope=D&work_type=maintenance'));
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
  await page.goto(base+'/workbench-lite?scope=D&work_type=maintenance');
  await page.goto(base+'/workbench-lite?scope=D&work_type=change');
  await page.locator('#lite-back-link').click();
  await page.waitForURL(base+'/?entry=change');
  const entryPages={
    maintenance:'选择楼栋进入维护管理', maintenance_mop:'选择楼栋进入 MOP 填写',
    change:'选择楼栋进入变更管理', repair:'选择楼栋进入检修通告管理',
    repair_management:'选择楼栋进入检修单管理', water:'选择楼栋进入水耗管理',
    tools:'选择辅助工具', daily:'选择楼栋查看每日任务',
    power:'选择楼栋进入上/下电通告', polling:'选择楼栋进入设备轮巡',
    adjust:'选择楼栋进入设备调整', handover:'选择楼栋打开交接班审核页',
  };
  for(const [entry,title] of Object.entries(entryPages)){
    await page.goto(base+'/?entry='+entry);
    await page.getByRole('heading',{name:title,exact:true}).waitFor();
  }
  for(const file of ['bin/lan_bitable_template_portal/server.py','bin/clipflow_backend/main.py']){
    const source=await fs.readFile(path.join(root,file),'utf8');
    assert(!source.includes('history.back()'),file);
  }
  console.log(JSON.stringify({ok:true,pages:targets.length+1,errorPages:2,restoredEntryPages:Object.keys(entryPages).length}));
} finally {
  await browser?.close(); server.kill();
}
