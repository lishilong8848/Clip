import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {adaptLearningSharing} from './workspace-sharing-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/question-import');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
const errors=[];
try {
  for(const v2 of [false,true]) {
    let batch=null, uploads=0, creates=0, importCreates=0, release;
    const operations=new Map();
    const html=`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      <style>body{margin:0;padding:16px;font:14px sans-serif;background:#f7f9fc}#app{max-width:1280px;margin:auto}*{box-sizing:border-box}</style>
      <div id="app" class="${v2?'workspace-v2':''}"></div><script type="module">
      import {createApp,h} from 'vue';import {NConfigProvider,NDialogProvider} from 'naive-ui';
      import Sharing from '/src/components/LearningSharing.vue';import UiTransition from '/src/components/UiTransition.vue';
      createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(Sharing,{isAdmin:true,userId:'fixture'})))}).component('UiTransition',UiTransition).mount('#app');</script>`;
    const server=await createServer({root,configFile:false,cacheDir:path.join(output,'cache-'+v2),logLevel:'error',
      plugins:[{name:'question-import-fixture',enforce:'pre',transform(code,id){if(v2&&id.replaceAll('\\','/').endsWith('/components/LearningSharing.vue'))return adaptLearningSharing(code);},
        configureServer(dev){dev.middlewares.use(async(req,res,next)=>{
          const url=new URL(req.url,'http://localhost');
          if(url.pathname==='/__stop'){res.end('stopped');release?.();return;}
          if(url.pathname==='/__questions'){res.setHeader('Content-Type','text/html; charset=utf-8');res.end(await dev.transformIndexHtml(url.pathname,html));return;}
          if(!url.pathname.startsWith('/api/'))return next();
          const ok=(data,status=200)=>{res.statusCode=status;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:status===200,data,error:status===200?'':'隔离回执丢失，请重试'}));};
          let raw='';for await(const part of req)raw+=part;
          const body=req.headers['content-type']?.includes('application/json')?JSON.parse(raw||'{}'):{};
          if(url.pathname==='/api/learning/sharing/people')return ok({items:[{id:'p1',person_id:'p1',open_id:'ou_fixture',name:'测试人员',scope:'A'}]});
          if(url.pathname==='/api/learning/sharing/materials'){uploads++;return ok({items:[{id:'file-fixture',name:'题库.xlsx',size:500}]});}
          if(url.pathname==='/api/learning/sharing'){
            if(req.method==='GET')return ok({items:batch?[batch]:[],total:batch?1:0,page:1});
            creates++;
            if(operations.has(body.operation_id)){assert.deepEqual(body,operations.get(body.operation_id));return ok({id:batch.id});}
            operations.set(body.operation_id,structuredClone(body));
            assert.deepEqual(body.participant_ids,['p1']);
            if(body.import_questions){
              importCreates++;assert.equal(body.auto_generate,true);assert.deepEqual(body.material_ids,['file-fixture']);assert.equal(uploads,1);
            } else {
              assert.equal(body.auto_generate,false);assert.equal(body.initial_questions.length,2);
              assert.deepEqual(body.material_urls,['https://vnet.feishu.cn/docx/ManualReference']);
              assert.equal(body.initial_questions[0].stem,'人工填写的第一题');assert.deepEqual(body.initial_questions[0].correct,[0]);
              assert.equal(body.initial_questions[1].reference_answer,'手工参考答案');
            }
            const questions=body.import_questions?[{id:'q1',origin:'manual',type:'single',stem:'原文件题干',options:['原选项甲','原选项乙'],correct:[1],explanation:'原文件解析',evidence:'原题依据'}]:body.initial_questions;
            batch={id:'fixture',title:body.title||'隔离题目导入',status:'draft',version:1,questions,question_count:questions.length,
              participants:[{person_id:'p1',open_id:'ou_fixture',name:'测试人员',scope:'A'}],total:1,completed:0,
              generation_note:body.import_questions?'已提取原文件题目，请核对后发布。':''};
            return ok({id:batch.id},body.import_questions?200:503);
          }
          if(url.pathname==='/api/learning/sharing/fixture'&&req.method==='GET')return ok(batch);
          throw new Error('Unexpected fixture request '+req.method+' '+url.pathname);
        });}},vue()],server:{host:'127.0.0.1',port:Number(process.env.CLIPFLOW_SHARING_TEST_PORT||0),strictPort:!!process.env.CLIPFLOW_SHARING_TEST_PORT,hmr:false}});
    await server.listen();const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
    const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
    await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
    const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
    async function capture(label){for(const [width,height] of [[402,874],[440,956],[1440,900]]){
      await page.setViewportSize({width,height});assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),label+' overflow');
      if(label==='ai-create'&&width===1440){
        const sources=await page.locator('.sharing-source').evaluateAll(nodes=>nodes.map(n=>n.getBoundingClientRect().y));
        assert.equal(sources.length,3);assert(sources.every(y=>Math.abs(y-sources[0])<2),'AI source controls share a row');
        assert((await page.locator('#sharing-title').boundingBox()).width<=520,'Title input does not stretch across desktop');
      }
      if(label==='manual'){
        const cards=await page.locator('.sharing-manual-create .sharing-question').evaluateAll(nodes=>nodes.map(node=>{const r=node.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width};}));
        if(width===1440){
          assert.equal(cards.length,2);assert(Math.abs(cards[0].y-cards[1].y)<2,'Desktop questions share a row');
          const options=await page.locator('.sharing-question').first().locator('.sharing-option').evaluateAll(nodes=>nodes.map(node=>node.getBoundingClientRect().y));
          assert(Math.abs(options[0]-options[1])<2,'Desktop options share a row');
        }else{assert(cards[1].y>cards[0].y,'Phone questions stack without overlap');}
      }
      await page.screenshot({path:path.join(output,`${label}-${v2}-${width}.png`),fullPage:true});
    }}
    try{
      await page.goto(origin+'/__questions');
      await page.getByRole('button',{name:'AI出题',exact:true}).click();
      await capture('ai-create');
      await page.getByRole('button',{name:'返回批次列表',exact:true}).click();
      await page.getByRole('button',{name:'人工出题',exact:true}).click();
      await page.getByLabel('题干',{exact:true}).fill('人工填写的第一题');
      await page.locator('.sharing-option textarea').nth(0).fill('手工选项甲');
      await page.locator('.sharing-option textarea').nth(1).fill('手工选项乙');
      await page.getByLabel('第1题选项A为正确答案',{exact:true}).check();
      await page.getByLabel('答案解析',{exact:true}).fill('手工解析');
      await page.getByRole('button',{name:'新增问答题',exact:true}).click();
      await page.getByLabel('题干',{exact:true}).last().fill('人工填写的问答题');
      await page.getByLabel('参考答案（问答题不自动判分）',{exact:true}).fill('手工参考答案');
      await page.getByRole('button',{name:'新增单选题',exact:true}).click();
      await page.getByLabel('第3题选项B为正确答案',{exact:true}).check();
      assert(await page.getByLabel('第1题选项A为正确答案',{exact:true}).isChecked(),'Question answers remain independent');
      await page.getByLabel('删除第3题选项A',{exact:true}).click();
      assert(await page.getByLabel('第3题选项A为正确答案',{exact:true}).isChecked(),'Deleting an option adjusts the answer index');
      await page.getByRole('button',{name:'删除此题',exact:true}).last().click();
      await page.reload();await page.getByLabel('题干',{exact:true}).first().waitFor();
      assert.equal(await page.getByLabel('题干',{exact:true}).first().inputValue(),'人工填写的第一题');
      await capture('manual');
      await page.getByText('资料出题',{exact:true}).click();
      await page.getByLabel('AI生成题目').uncheck();
      assert.equal(await page.getByLabel('题干',{exact:true}).first().inputValue(),'人工填写的第一题');
      await page.getByLabel('资料链接',{exact:true}).fill('https://vnet.feishu.cn/docx/ManualReference');
      await page.getByRole('button',{name:'添加链接',exact:true}).click();
      await page.getByRole('button',{name:'读取人员目录',exact:true}).click();
      await page.getByRole('button',{name:'添加参会人：测试人员',exact:true}).click();
      await page.getByRole('button',{name:'保存人工题草稿',exact:true}).click();
      await page.getByRole('alert').filter({hasText:'隔离回执丢失'}).waitFor();
      assert.equal(await page.getByLabel('题干',{exact:true}).first().inputValue(),'人工填写的第一题');
      await page.getByRole('button',{name:'保存人工题草稿',exact:true}).click();
      await page.getByRole('button',{name:'批次列表',exact:true}).waitFor();assert.equal(creates,2);assert.equal(operations.size,1);
      assert(await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).isDisabled());
      await page.getByRole('button',{name:'批次列表',exact:true}).click();
      await page.getByRole('button',{name:'导入表格/文档',exact:true}).click();
      assert.equal(await page.getByLabel('AI生成题目').count(),0);
      await page.getByLabel('选择资料文件').setInputFiles({name:'题库.xlsx',mimeType:'application/octet-stream',buffer:Buffer.from('isolated fixture')});
      await page.getByRole('button',{name:'读取人员目录',exact:true}).click();
      await page.getByRole('button',{name:'添加参会人：测试人员',exact:true}).click();
      await capture('file-import');
      await page.getByRole('button',{name:'导入并提取题目',exact:true}).click();
      await page.getByLabel('题干',{exact:true}).waitFor();
      assert.equal(await page.getByLabel('题干',{exact:true}).inputValue(),'原文件题干');
      assert(await page.getByLabel('第1题选项B为正确答案',{exact:true}).isChecked());
      assert.equal(importCreates,1);assert.equal(uploads,1);assert.equal(operations.size,2);
      await capture('imported-editable');
      if(process.argv.includes('--responsively')&&v2){console.log(`ISOLATED_PREVIEW=${origin}/__questions`);await new Promise(resolve=>{release=resolve;setTimeout(resolve,600000).unref();});}
    }finally{await context.close();await server.close();}
  }
  assert.deepEqual(errors,[]);console.log('PASS: legacy/V2 manual fields, optional materials without AI, multi-column questions/options, independent answers, draft reload, uncertain receipt dedupe, file upload/import and phone/desktop layout.');
}finally{await browser.close();}
