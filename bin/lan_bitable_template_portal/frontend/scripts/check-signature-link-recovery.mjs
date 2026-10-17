import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {preview} from 'vite';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/signature-link-recovery');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[];
try {
  for(const version of ['legacy','workspace'])for(const width of [1440,390]){
    let reads=0,writes=0,complete=false,auth='expired';
    const ctx=await browser.newContext({viewport:{width,height:width===390?844:900},reducedMotion:'reduce'});
    await ctx.route('**/*',async route=>{
      const req=route.request(),url=new URL(req.url());
      if(url.origin!==origin)return route.abort();
      if(url.pathname==='/fixture-back')return route.fulfill({contentType:'text/html',body:'<p>隔离返回页</p>'});
      if(!url.pathname.startsWith('/api/'))return route.continue();
      assert(!req.url().includes('fixture-sign-token'),'token is never sent in URL queries');
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'signature-fixture'}});
      if(url.pathname==='/api/auth/status')return auth==='expired'
        ?route.fulfill({status:401,json:{ok:false,auth_required:true,error:'登录已过期'}})
        :ok({logged_in:true,user:{open_id:'fixture-guest',role:'guest',modules:['cabinet']},scope_options:[]});
      if(url.pathname==='/api/signatures/management/request'){
        reads++;assert.equal(req.headers()['x-signature-token'],'fixture-sign-token');
        if(reads===1)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：签名读取暂未完成'}});
        if(complete)return route.fulfill({status:410,json:{ok:false,error:'签名已完成，此链接已失效。'}});
        return ok({request_id:'fixture-request',name:'隔离签名人员',building:'B楼',status:'sent'});
      }
      if(url.pathname==='/api/signatures/management/submit'){
        const body=req.postDataJSON();
        assert.equal(body.request_id,'fixture-request');assert.equal(body.token,'fixture-sign-token');
        assert.equal(body.confirmed_self,true);assert.match(body.signature_png,/^data:image\/png;base64,/);
        writes++;complete=true;return ok({name:'隔离签名人员',status:'completed'});
      }
      assert.equal(req.method(),'GET','No unrelated writes');
      return ok({items:[],records:[],scopes:{}});
    });
    const page=await ctx.newPage();page.on('pageerror',e=>errors.push(e.message));
    const link=origin+(version==='workspace'?'/assets/workspace-v2/index.html?__page=/signature&':'/signature?')+'request_id=fixture-request#token=fixture-sign-token';
    await page.goto(link);
    await page.getByText('暂时无法读取签名',{exact:true}).waitFor();
    assert.equal(writes,0);assert.equal(new URL(page.url()).hash,'#token=fixture-sign-token');
    await page.getByRole('button',{name:'重新读取签名',exact:true}).click();
    await page.locator('.signature-page canvas').waitFor();
    await page.evaluate(()=>sessionStorage.clear());
    auth='guest';
    await page.goto(origin+'/fixture-back');await page.goBack();
    const canvas=page.locator('.signature-page canvas');await canvas.waitFor();
    assert(!page.url().includes('signature-management')&&!page.url().includes('cabinet-power'));
    await page.reload();await canvas.waitFor();
    await page.getByRole('checkbox',{name:/我确认由/}).check();
    const box=await canvas.boundingBox();
    await page.mouse.move(box.x+box.width*.3,box.y+box.height*.4);
    await page.mouse.down();await page.mouse.move(box.x+box.width*.65,box.y+box.height*.65,{steps:12});await page.mouse.up();
    assert(await canvas.evaluate(el=>el.getContext('2d').getImageData(0,0,el.width,el.height).data.some((v,i)=>i%4===3&&v>0)),'first usable canvas accepts ink');
    await page.screenshot({path:path.join(output,`${version}-${width}.png`)});
    await page.getByRole('button',{name:'保存到签名表',exact:true}).click();
    await page.getByText('签名已完成',{exact:true}).waitFor();assert.equal(writes,1);
    await page.reload();await page.getByText('签名已完成，此链接已失效。',{exact:true}).waitFor();
    assert.equal(await page.locator('canvas').count(),0);assert.equal(writes,1);
    await page.evaluate(()=>sessionStorage.clear());
    await page.goto(link.split('#')[0]);
    await page.getByText('签名链接缺少验证信息，请从原消息重新打开完整链接。',{exact:true}).waitFor();
    assert(!page.url().includes('signature-management'));
    await ctx.close();
  }
  assert.deepEqual(errors,[]);
  console.log('PASS: legacy/V2 public signing, failed-first-read retry, back/reopen without storage, expired login and guest session, visible ink, one submission, used/missing-token links. All APIs mocked.');
}finally{await browser.close();await server.close();}
