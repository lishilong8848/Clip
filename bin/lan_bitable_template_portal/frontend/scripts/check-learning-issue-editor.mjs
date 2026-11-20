// Component-level acceptance, served from source. No built site or remote backend.
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const browser=await chromium.launch();
async function verify(v2){
 const cacheDir=await mkdtemp(path.join(os.tmpdir(),'learning-editor-vite-'));
 const server=await createServer({root,base:'/',optimizeDeps:{entries:[],include:['vue','lucide-vue-next','naive-ui']},configFile:v2?path.join(root,'vite.workspace-v2.config.ts'):false,
   cacheDir,plugins:[...(v2?[]:[vue()]),{
     name:'learning-review-test-entry',
     resolveId(id){if(id==='/review-test.js')return id;},
     load(id){if(id==='/review-test.js')return 'import {createApp,h} from "vue";import UiTransition from "/src/components/UiTransition.vue";import Page from "/src/components/LearningPage.vue";createApp({render:()=>h(Page,{userId:"reviewer"})}).component("UiTransition",UiTransition).mount("#app");';},
   }],server:{host:'127.0.0.1',port:0,hmr:false},logLevel:'error'});
 let context,page;
 try{
  await server.listen();
  const origin='http://127.0.0.1:'+server.httpServer.address().port;
  context=await browser.newContext({serviceWorkers:'block'});
  page=await context.newPage();page.setDefaultTimeout(30000);page.setDefaultNavigationTimeout(90000);
  let question={id:'q1',bank:'written',status:'published',type:'single',stem:'当前题库题干',options:[{id:'a',text:'原选项'},{id:'b',text:'保留选项'}],correct_option_ids:['a'],answer_text:'原选项',analysis:'原解析',attachments:[],version:'v1'};
  let failure=503,revision='issues-1',resolved=false,isAdmin=false,saves=0;
  const requests=[],errors=[];
  page.on('pageerror',e=>{errors.push(e.message);console.error('pageerror',e.stack);});
  page.on('console',msg=>{if(msg.type()==='error' && !/WebSocket|websocket/.test(msg.text()))console.error(msg.text());});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),p=url.pathname;
    if(url.origin!==origin)return route.abort();
    if(p==='/learning')return route.fulfill({contentType:'text/html;charset=utf-8',body:'<div id="page-back-slot"></div><div id="app"></div><script type="module" src="/review-test.js"></script>'});
    if(!p.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(p==='/api/learning/bootstrap')return ok({is_admin:isAdmin,can_answer:false,today:'2026-11-20',scope:'A',scopes:[{value:'A',label:'A楼'}],settings:{enabled:true},sync:{pending:0}});
    const issue={id:'i1',question_id:'q1',current_stem:question.stem,question:{stem:'提交时的旧题'},description:'选项需核对',status:resolved?'resolved':'pending',scope:'A',can_update:isAdmin};
    if(p==='/api/learning/issues')return ok({items:[issue],total:1,page:1});
    if(p==='/api/learning/questions/q1'){
      assert(isAdmin);
      if(req.method()==='PUT'){
        const change=req.postDataJSON();
        assert.equal(change.version,question.version);
        question={...question,...change,version:'bank-revision'};
      }
      return ok(question);
    }
    if(p==='/api/learning/issues/i1/question'){
      if(!isAdmin)assert.equal(url.searchParams.get('review_ticket'),'card-fixture');
      if(req.method()==='PUT'){
        const data=req.postDataJSON();requests.push(data);
        if(failure){const status=failure;failure=0;return route.fulfill({status,json:{ok:false,error:status===409?'版本已变化':'模拟服务暂不可用'}});}
        assert.equal(data.version,question.version);assert.equal(data.issues_version,revision);
        question={...question,...data,stem:data.stem.trim(),version:'v'+(saves+2)};saves++;revision='closed';resolved=true;
      }
      return ok({question,issues_version:revision,editable:isAdmin||!resolved,resolved});
    }
    if(!['GET','HEAD'].includes(req.method()))throw Error('Unexpected write '+p);
    return ok({items:[],total:0});
  });
  await page.goto(origin+'/learning?tab=issues&issue_id=i1&review_ticket=card-fixture');
  const modal=page.getByRole('dialog').filter({has:page.locator('.learning-question-fields')});
  const stem=modal.getByLabel('题干',{exact:true}),save=modal.getByRole('button',{name:'保存题目',exact:true});
  await stem.waitFor();assert.equal(await stem.inputValue(),'当前题库题干');
  await stem.fill(' 修订后的题干 ');await modal.getByRole('button',{name:'删除选项'}).first().click();
  await modal.getByRole('button',{name:'添加选项'}).click();
  await modal.getByLabel('选项 B 内容').fill('新增正确项');await modal.getByLabel('选项 B 为正确答案').check();
  await modal.getByLabel('解析',{exact:true}).fill('依据设备状态判断。');
  await save.click();await modal.getByRole('alert').first().waitFor();
  assert.equal(await stem.inputValue(),' 修订后的题干 ');assert(await save.isEnabled());
  await save.click();await modal.getByText(/^已保存最新版，(质疑|反馈)已处理。$/).waitFor();
  assert.deepEqual(requests[0],requests[1]);assert.equal(saves,1);
  assert.equal(question.correct_option_ids[0],question.options[1].id);assert.equal(question.options[0].id,'b');
  assert.equal(question.analysis,'依据设备状态判断。');assert.equal(await stem.inputValue(),'修订后的题干');
  await page.reload();await stem.waitFor();assert.equal(await stem.inputValue(),question.stem);assert(await stem.isDisabled());
  isAdmin=true;await page.goto(origin+'/learning?view=overview&tab=issues');
  await page.getByRole('button',{name:'修改题目与答案',exact:true}).first().click();await stem.waitFor();
  assert.equal(await stem.inputValue(),question.stem);
  await stem.fill('并发中的手动填写');failure=409;question={...question,version:'v9',stem:'其他人已保存的版本'};
  await save.click();await modal.getByRole('button',{name:'读取最新版本'}).waitFor();
  assert.equal(await stem.inputValue(),'并发中的手动填写');assert.equal(saves,1);
  await modal.getByRole('button',{name:'读取最新版本'}).click();
  const dismiss=page.getByRole('button',{name:/取消|继续编辑/}).last();await dismiss.click();
  assert.equal(await stem.inputValue(),'并发中的手动填写');
  // The same editor also preserves multiple choices and the interview answer field.
  for(const type of ['multiple','interview']){
    question={...question,type,bank:type==='interview'?'duty':'written',correct_option_ids:['a'],options:[{id:'a',text:'选项一'},{id:'b',text:'选项二'}],answer_text:'原参考答案'};
    await page.goto(origin+'/learning?view=overview&tab=issues&issue_id=i1');
    await stem.waitFor();
    if(type==='multiple'){
      await modal.getByLabel('选项 B 为正确答案').check();
      await save.click();await modal.getByText(/^已保存最新版/).waitFor();
      assert.deepEqual(question.correct_option_ids,['a','b']);
    }else{
      await modal.getByLabel('参考答案',{exact:true}).fill('修订的参考答案');
      await save.click();await modal.getByText(/^已保存最新版/).waitFor();
      assert.equal(question.answer_text,'修订的参考答案');
    }
  }
  await page.goto(origin+'/learning?view=overview&edit_question=q1');
  await stem.waitFor();assert.equal(await modal.getByLabel('参考答案',{exact:true}).inputValue(),'修订的参考答案');
  await modal.getByLabel('解析',{exact:true}).fill('题库入口补充的解析');
  await save.click();await modal.getByText('题目已保存。',{exact:true}).waitFor();
  assert.equal(question.version,'bank-revision');assert.equal(question.analysis,'题库入口补充的解析');
  assert.deepEqual(errors,[]);console.log('PASS',v2?'V2':'legacy','card and web editor, option IDs, failure retry, saved revision, read-only reopen and 409 draft retention');
 }catch(error){
   console.error(v2?'V2':'legacy',(await page?.locator('body').innerText())?.slice(-900));
   throw error;
 }finally{
   await context?.close();
   await server.close();
   assert.equal(path.dirname(cacheDir),path.resolve(os.tmpdir()));
   assert(path.basename(cacheDir).startsWith('learning-editor-vite-'));
   await rm(cacheDir,{recursive:true,force:true});
 }
}
try{await verify(false);await verify(true);}finally{await browser.close();}

