import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdir,mkdtemp,rm,writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url)),repo=path.resolve(root,'../../..');
const output=path.join(repo,'output/playwright/convergence-confirmation');
await mkdir(output,{recursive:true});
const scratch=await mkdtemp(path.join(os.tmpdir(),'clipflow-convergence-ui-'));
let result;
try{
 result=spawnSync(path.join(repo,'bin/.venv/Scripts/python.exe'),['-c',`
import sys,json,datetime as dt
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import VALID_STARTS
now=dt.datetime.now()
row={**VALID_STARTS['change'],'title':'EA118数据中心ABCDEH楼消防月度维保超低风险变更（二阶段）',
 'active_item_id':'active_fixture','record_id':'recTarget','target_record_id':'recTarget',
 'source_record_id':'recSource','scope':'A','building':'A楼','work_type':'change','notice_type':'变更通告',
 'status':'开始','site_photo_count':1,'start_time':(now-dt.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M'),
 'end_time':(now+dt.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M')}
html=render_workbench_lite(session={'role':'building','allowed_modules':['change']},scope='A',work_type='change',
 month='10月',payload={'records':[],'ongoing':[row]},active_item_id='active_fixture')
print(json.dumps({'html':html,'detail':extract_workbench_lite_fragments(html)['detail'],'row':row},ensure_ascii=False))
`],{cwd:repo,encoding:'utf8',maxBuffer:12*1024*1024,env:{...process.env,CLIPFLOW_DATA_DIR:scratch,PYTHONIOENCODING:'utf-8'}});
}finally{assert.equal(path.dirname(scratch),os.tmpdir());assert(path.basename(scratch).startsWith('clipflow-convergence-ui-'));await rm(scratch,{recursive:true,force:true});}
assert.equal(result.status,0,result.stderr);
const fixture=JSON.parse(result.stdout);
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:19144,strictPort:true}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
try{
 for(const legacy of [true,false])for(const action of ['update','end'])for(const width of [1440,402,440]){
  const label=action==='end'?'结束':'更新',mode=legacy?'legacy':'v2';
  const context=await browser.newContext({viewport:{width,height:width===402?874:width===440?956:900},reducedMotion:'reduce'});
  const page=await context.newPage(),errors=[],requests=[],accepted=[],browserDialogs=[];
  page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  page.on('dialog',async dialog=>{browserDialogs.push(dialog.message());await dialog.dismiss();});
  let draft=null,ticket='a'.repeat(48),expire=width===1440&&action==='update';
  await context.route('**/*',async route=>{
   const request=route.request(),url=new URL(request.url());
   if(url.origin!==origin)return route.abort();
   const ok=data=>route.fulfill({json:{ok:true,data}});
   if(url.pathname==='/__legacy')return route.fulfill({contentType:'text/html',body:fixture.html});
   if(!url.pathname.startsWith('/api/'))return route.continue();
   if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_fixture',name:'测试用户',role:'building',level:'L4',modules:['change']},scope_options:[{value:'A',label:'A楼'}]});
   if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend'}});
   if(url.pathname==='/api/workbench')return ok({records:[],ongoing:[fixture.row],source_snapshot_ready:true,records_pagination:{total:0},ongoing_pagination:{total:1}});
   if(url.pathname==='/api/workbench/lite-detail')return ok({fragments:{detail:fixture.detail}});
   if(url.pathname==='/api/workbench/draft'){
    if(request.method()==='PUT'){const body=request.postDataJSON();draft={...body,version:(draft?.version||0)+1};}
    return ok({draft});
   }
   if(url.pathname==='/api/workbench-actions'){
    const body=request.postDataJSON();requests.push(body);
    assert.equal(body.action,action);
    if(body.plan_convergence_confirmation&&expire){expire=false;ticket='b'.repeat(48);}
    if(body.plan_convergence_confirmation!==ticket)return route.fulfill({status:409,json:{ok:false,error_code:'confirmation_required',error:'核对未完成，当前尚未发送。',details:{kind:'plan_convergence_unmatched',confirmation:ticket,notice_name:fixture.row.title,reason:'计划收敛核对未完成，请检查 VPN 和智航登录；不能判断为已匹配。'}}});
    accepted.push(body);return ok({job_id:'job_fixture',initial_phase:'accepted'});
   }
   if(url.pathname==='/api/jobs/job_fixture')return ok({job_id:'job_fixture',phase:'accepted'});
   if(request.method()!=='GET')throw new Error('Unexpected business write '+url.pathname);
   return ok({items:[],records:[],settings:{},available:false});
  });
  try{
   await page.goto(origin+(legacy?'/__legacy':'/assets/workspace-v2/index.html?scope=A&__page=/workbench-lite&work_type=change&active_item_id=active_fixture'));
   const field=legacy?page.locator('#lite-notice-form [name=content]'):page.locator('[data-notice-field=content] textarea').first();
   await field.waitFor();await field.fill('用户手填内容，取消确认后必须保留。');
   async function submit(){
    if(legacy){await page.locator('#lite-notice-form button[value='+action+']').click();if(action==='end')await page.locator('#lite-end-check-confirm').click();}
    else{await page.locator('.v2-notice-submit-group .n-radio-button').filter({hasText:label}).click();await page.getByRole('button',{name:'预览并确认'+label,exact:true}).click();await page.getByRole('button',{name:'确认发送'+label,exact:true}).click();}
   }
   const modal=legacy?page.locator('#lite-convergence-confirm'):page.locator('.n-dialog').filter({hasText:'计划收敛核对待确认'}).last();
   await submit();await modal.waitFor();
   assert.equal(requests.length,1,'first submission must show confirmation, without a second click');
   assert.equal(accepted.length,0);assert.match(await modal.innerText(),new RegExp(fixture.row.title.replace(/[（）]/g,'.')));
   const send=modal.getByRole('button',{name:'仍然发送'+label,exact:true});
   const box=await send.boundingBox();assert(box&&box.y>=0&&box.y+box.height<=page.viewportSize().height,'confirmation action stays in viewport');
   assert(await send.evaluate(el=>{const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}),'confirmation is not hidden under the editor');
   await page.screenshot({path:path.join(output,`${mode}-${action}-${width}.png`)});
   if(width===1440&&action==='update'){
    const css=await page.evaluate(()=>Array.from(document.styleSheets).map(sheet=>{try{return Array.from(sheet.cssRules).map(r=>r.cssText).join('\n');}catch{return '';}}).join('\n'));
    const html=(await page.content()).replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace('</head>','<style>'+css+'</style></head>');
    await writeFile(path.join(output,mode+'-preview.html'),html+(legacy?'<script>const d=document.getElementById("lite-convergence-confirm");d.close();d.showModal();</script>':''));
   }
   if(legacy&&width===402)await page.keyboard.press('Escape');
   else await modal.getByRole('button',{name:'返回核对',exact:true}).click();
   await modal.waitFor({state:'hidden'});
   assert.equal(accepted.length,0);assert.equal(requests.length,1);assert.equal(await field.inputValue(),'用户手填内容，取消确认后必须保留。');
   await field.fill('用户更正后的内容');await submit();await modal.waitFor();
   assert.equal(requests.length,2);
   const before=requests.at(-1);
   const success=page.waitForResponse(response=>response.url().endsWith('/api/workbench-actions')&&response.status()===200);
   success.catch(()=>{});
   const expired=width===1440&&action==='update'?page.waitForResponse(response=>response.url().endsWith('/api/workbench-actions')&&response.status()===409):null;
   await send.evaluate(el=>{el.click();el.click();});
   if(expired){await expired;await modal.waitFor();await send.click();}
   await success;
   assert.equal(accepted.length,1,'confirmation dispatches one operation');
   const {plan_convergence_confirmation,...after}=accepted[0];assert.deepEqual(after,before,'confirmation reuses the exact body and operation ID');
   assert.equal(after.patch.content,'用户更正后的内容');assert.equal(plan_convergence_confirmation,ticket);
   assert.deepEqual(browserDialogs,[]);assert.deepEqual(errors,[]);
   console.log(`PASS ${mode} ${action} ${width}: first-click confirmation, cancel keeps edits, same operation sends once.`);
  }catch(error){await page.screenshot({path:path.join(output,`${mode}-${action}-${width}-failure.png`)});console.log('FAIL',mode,action,width,'requests',requests.map(r=>({action:r.action,confirmed:!!r.plan_convergence_confirmation})),(await page.locator('body').innerText()).slice(-2600),errors);throw error;}
  finally{await context.close();}
 }
}finally{await browser.close();await server.close();}
