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
    '/cabinet-power?scope=D', '/daily-tasks?scope=D', '/water-management?scope=D',
    '/critical-guard?scope=D', '/drill-management?scope=D', '/repair-management?scope=D',
    '/repair-status?scope=D', '/engineer/mop?scope=D', '/?scope=D&mode=events',
    '/admin/history-memory', '/signature-management',
  ];
  for(const target of targets){
    await page.goto(base+'/cabinet-power');
    await page.getByRole('heading',{name:'机柜上下电',exact:true}).waitFor();
    await page.evaluate(url=>{history.pushState({},'',url);window.dispatchEvent(new Event('popstate'))},target);
    const back=target==='/signature-management'
      ? page.locator('.hero-actions button',{hasText:'返回'}).first()
      : page.locator('.vnet-back-button').first();
    await back.waitFor({state:'visible'});
    await back.click();
    await page.waitForURL(base+'/cabinet-power');
    assert.equal(new URL(page.url()).pathname,'/cabinet-power',target);
  }
  await page.goto(base+'/cabinet-power');
  await page.evaluate(()=>location.assign('/workbench-lite?scope=D&work_type=maintenance'));
  await page.locator('#lite-back-link').click();
  await page.waitForURL(base+'/cabinet-power');
  for(const file of ['bin/lan_bitable_template_portal/server.py','bin/clipflow_backend/main.py']){
    const source=await fs.readFile(path.join(root,file),'utf8');
    assert(source.includes('onclick=\\"if(history.length>1){event.preventDefault();history.back()}\\"'),file);
  }
  console.log(JSON.stringify({ok:true,pages:targets.length+1,errorPages:2}));
} finally {
  await browser?.close(); server.kill();
}
