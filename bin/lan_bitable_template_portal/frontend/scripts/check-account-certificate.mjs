import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/account-certificate');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:900}});
const user={open_id:'certificate-fixture',name:'证书验收管理员',role:'admin',level:'L1',category:'manager',home_scope:'H',modules:['accounts']};
const info={state:'healthy',domain:'dt.sjhl.online',issuer:"CN=YE1,O=Let's Encrypt,C=US",not_before:1792045518,not_after:1799821517,checked_at:1792300000,remaining_days:87,
  renewal:{enabled:true,last_check_ok:true,serving_current:true,next_check:'Mon 2026-10-19 04:20:00 CST',next_renewal_at:1797185923}};
let reads=0,failed=false,forbidden=false;
const errors=[];
await context.route('**/*',route=>{
  const req=route.request(),url=new URL(req.url());
  if(url.origin!==origin)return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  assert.equal(req.method(),'GET','certificate view never modifies business data');
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:[{value:'A',label:'A楼'},{value:'H',label:'H楼'}]});
  if(url.pathname==='/api/auth/certificate'){
    reads++;
    if(forbidden)return route.fulfill({status:403,json:{ok:false,error:'没有证书权限'}});
    if(failed)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：证书读取失败'}});
    return ok(info);
  }
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'certificate-fixture'}});
  return ok({items:[],records:[],settings:{},stats:{}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html?view=my';
try{
  await page.goto(entry);
  const section=page.getByRole('region',{name:'HTTPS证书信息'});
  await section.getByText('87 天',{exact:true}).waitFor();
  assert.equal(reads,1);
  for(const width of [1440,390,360]){
    await page.setViewportSize({width,height:width<761?640:900});
    await section.scrollIntoViewIfNeeded();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'certificate never overflows mobile');
    await section.getByRole('button',{name:'刷新证书信息'}).click();
    await page.screenshot({path:path.join(output,'profile-'+width+'.png'),animations:'disabled'});
  }
  failed=true;
  await section.getByRole('button',{name:'刷新证书信息'}).click();
  await section.getByText('隔离测试：证书读取失败',{exact:true}).waitFor();
  assert.equal(await section.getByText('正常',{exact:true}).count(),0,'failure must not leave a green success label');
  failed=false;info.state='stale';
  await section.getByRole('button',{name:'刷新证书信息'}).click();
  await section.getByText('证书信息未及时更新，暂不能确认最新续期状态。',{exact:true}).waitFor();
  info.state='expired';info.remaining_days=0;
  await section.getByRole('button',{name:'刷新证书信息'}).click();
  await section.getByText('证书已过期，请检查服务器续期任务。',{exact:true}).waitFor();
  info.state='healthy';info.remaining_days=87;
  for(const level of ['L0','L1','L2','L3','L4']){
    user.level=level;user.role=['L0','L1'].includes(level)?'admin':'building';
    const before=reads;
    await page.goto(entry);await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
    if(['L0','L1'].includes(level))await section.getByText('87 天',{exact:true}).waitFor();
    else{await page.waitForTimeout(350);assert.equal(await section.count(),0);assert.equal(reads,before,'lower levels never request certificate metadata');}
  }
  user.role='admin';user.level='L1';user.category='duty';
  await page.goto(entry);await section.getByText('87 天',{exact:true}).waitFor();
  forbidden=true;
  await section.getByRole('button',{name:'刷新证书信息'}).click();
  await section.getByText('没有证书权限',{exact:true}).waitFor();
  assert.equal(await section.getByText('dt.sjhl.online',{exact:true}).count(),0,'revocation clears previously shown metadata');
  assert.deepEqual(errors,[]);
  console.log('PASS: certificate PC/mobile, L0/L1/H duty visibility, lower-level isolation, failures, expiry and revocation; no real writes.');
}finally{await browser.close();await server.close();}
