import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/sharing-selection');
await mkdir(output, { recursive: true });
const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{margin:0;padding:16px;font:14px sans-serif;background:#f7f9fc}#app{max-width:1280px;margin:auto}*{box-sizing:border-box}</style>
<div id="app"></div><script type="module">
import {createApp,h} from 'vue';import Sharing from '/src/components/LearningSharing.vue';import UiTransition from '/src/components/UiTransition.vue';
createApp({render:()=>h(Sharing,{isAdmin:true,userId:'ou_fixture'})}).component('UiTransition',UiTransition).mount('#app');
</script>`;
const server = await createServer({ root, configFile:false, cacheDir:path.join(output,'vite-cache'), logLevel:'error',
  plugins:[vue(),{name:'sharing-selection-fixture',configureServer(dev){dev.middlewares.use('/__sharing',async(req,res)=>{
    res.setHeader('Content-Type','text/html; charset=utf-8');res.end(await dev.transformIndexHtml('/__sharing',html));
  });}}], server:{host:'127.0.0.1',port:0,hmr:false} });
await server.listen();
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({headless:true});
const errors=[];
try {
  for (const width of [1440,390]) {
    const context=await browser.newContext({viewport:{width,height:900},reducedMotion:'reduce'});
    const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
    let sends=0,creates=0,failSave=true;
    const batch={id:'fixture',title:'隔离测试纪要',url:'https://example.test/docx/fixture',status:'draft',version:1,
      question_types:['single','multiple','interview'],question_count:5,completed:0,total:2,questions:Array.from({length:5},(_,i)=>({id:'q'+i,stem:'测试题'+i,
        type:i<3?'single':i===3?'multiple':'interview',options:i===4?[]:['是','否'],correct:i<3?[0]:i===3?[0,1]:[],reference_answer:i===4?'核验纪要后按流程处理':'',explanation:'测试解析',evidence:'测试依据'})),
      participants:[{open_id:'ou_one',name:'测试人员甲',scope:'A',exempted:false},{open_id:'ou_two',name:'测试人员乙',scope:'H',exempted:false}]};
    await context.route('**/*',async route=>{
      const req=route.request(),u=new URL(req.url());
      if(u.origin!==origin)return route.abort();
      if(!u.pathname.startsWith('/api/'))return route.continue();
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(u.pathname==='/api/learning/sharing'){
        if(req.method()==='POST'){
          assert.deepEqual(req.postDataJSON().question_types,['single','multiple','interview']);creates++;return ok({id:'fixture'});
        }
        return ok({items:[batch],total:1,page:1});
      }
      if(u.pathname==='/api/learning/sharing/fixture'){
        if(req.method()==='PUT'){
          if(failSave){failSave=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试保存失败'}});}
          const payload=req.postDataJSON();assert.deepEqual(payload.excluded_open_ids,['ou_two']);
          assert.equal(payload.questions[4].reference_answer,'新的参考答案，按批准流程处理。');
          batch.questions=payload.questions;
          batch.participants.forEach(p=>p.exempted=payload.excluded_open_ids.includes(p.open_id));batch.total=1;batch.version++;
        }
        return ok(batch);
      }
      assert.equal(u.pathname,'/api/learning/sharing/fixture/publish');
      assert.equal(req.postDataJSON().version,batch.version);sends++;batch.status='published';batch.version++;
      batch.bank_sync={total:5,synced:0,failed:0,error:''};
      return ok({id:batch.id});
    });
    await page.goto(origin+'/__sharing');
    const single=page.getByRole('checkbox',{name:'单选题',exact:true});
    assert(await single.isChecked());
    await page.getByLabel('飞书纪要链接').fill('https://vnet.feishu.cn/docx/fixture');
    await single.uncheck();assert(await page.getByRole('button',{name:'读取纪要并生成题目'}).isDisabled());
    await single.check();await page.getByRole('checkbox',{name:'多选题',exact:true}).check();await page.getByRole('checkbox',{name:'问答题',exact:true}).check();
    await page.screenshot({path:path.join(output,'types-'+width+'.png'),fullPage:true});
    await page.getByRole('button',{name:'读取纪要并生成题目'}).click();
    await page.getByRole('heading',{name:'隔离测试纪要',exact:true}).waitFor();assert.equal(creates,1);
    await page.getByLabel('参考答案（问答题不自动判分）').fill('新的参考答案，按批准流程处理。');
    const first=page.getByRole('checkbox',{name:'参加答题：测试人员甲',exact:true});
    const second=page.getByRole('checkbox',{name:'参加答题：测试人员乙',exact:true});
    assert(await first.isChecked());assert(await second.isChecked());
    await second.uncheck();await first.uncheck();
    assert(await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).isDisabled());
    await first.check();await page.getByRole('searchbox',{name:'搜索参会人'}).fill('甲');
    await page.getByText('参会 2 人 · 参加答题 1 人 · 本次不做 1 人',{exact:true}).waitFor();
    await page.getByRole('searchbox',{name:'搜索参会人'}).clear();assert(!await second.isChecked());
    await page.getByRole('button',{name:'保存题目与名单',exact:true}).click();
    await page.getByRole('alert').filter({hasText:'隔离测试保存失败'}).waitFor();assert(!await second.isChecked());
    await page.getByRole('button',{name:'保存题目与名单',exact:true}).click();
    await page.getByText('题目与答题名单已保存。',{exact:true}).waitFor();
    await page.reload();await page.locator('.sharing-batch').click();assert(!await second.isChecked());
    await page.screenshot({path:path.join(output,'selection-'+width+'.png'),fullPage:true});
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).click();
    const dialog=page.getByRole('dialog');await dialog.waitFor();assert.match(await dialog.innerText(),/向1位参会人/);assert.equal(sends,0);
    assert.match(await dialog.innerText(),/全部5道题同步至纪要题库/);
    await dialog.getByRole('button',{name:'确认',exact:true}).click();
    await page.getByRole('button',{name:'关闭本批',exact:true}).waitFor();assert.equal(sends,1);assert.equal(await first.count(),0);
    await page.getByText('免做，不推送',{exact:true}).waitFor();
    await page.getByText('题库同步 0 / 5 题',{exact:true}).waitFor();
    await page.getByText('新的参考答案，按批准流程处理。',{exact:false}).waitFor();
    batch.bank_sync={total:5,synced:5,failed:0,error:''};
    await page.getByRole('button',{name:'刷新',exact:true}).click();
    await page.getByText('题库同步 5 / 5 题',{exact:true}).waitFor();
    await page.screenshot({path:path.join(output,'published-'+width+'.png'),fullPage:true});
    await context.close();
  }
  assert.deepEqual(errors,[]);
  console.log('PASS: PC/mobile type selection, reference editing, attendee exemptions, save retry, reload, exact confirmation, bank progress, immutable published roster. No real messages.');
} finally {await browser.close();await server.close();}
