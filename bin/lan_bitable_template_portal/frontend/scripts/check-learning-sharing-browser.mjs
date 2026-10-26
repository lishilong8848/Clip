import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { preview } from 'vite';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/learning-sharing');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', build: { outDir: process.env.CLIPFLOW_SHARING_PREVIEW_DIR || 'dist' }, preview: { host: '127.0.0.1', port: 0, strictPort: false } });
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const questions = Array.from({length:20}, (_,i) => ({id:`q${i+1}`,type:i%2?'multiple':'single',stem:`第${i+1}个知识点：机组轮巡发现流量告警，应如何核验实际流量与运行参数？`,options:['核验运行参数及现场实际状态','持续重复复位设备','跳过现场核验直接认定误报','按已审批流程处置'],correct:i%2?[0,3]:[0],explanation:'仅依据纪要的处置原则，现场操作仍须执行已批准的规程。',evidence:'核查实际流量及运行状态，单次复位无效后停机排查，不反复启机。'}));
const errors=[];
let activePage, lastUnexpected, label='';
try {
  for (const width of [1440, 390]) for (const v2 of [false, true]) {
    const context = await browser.newContext({ viewport:{width,height:1000}, serviceWorkers:'block' });
    await context.addInitScript(() => Object.defineProperty(globalThis.crypto, 'randomUUID', {value:undefined,configurable:true}));
    const page = await context.newPage(); page.on('pageerror',e=>errors.push(e.message));
    activePage=page;label=`${v2?'v2':'old'}-${width}`;
    let actor='admin', published=0, created=0;
    let batch={id:'batch-fixture',title:'A楼冷机轮巡流量告警排查分享会',url:'https://vnet.feishu.cn/docx/Fixture',status:'draft',version:1,created_at:'2026-10-14T10:00:00+08:00',question_count:20,completed:0,total:2,questions:structuredClone(questions),participants:[{name:'测试人员甲',open_id:'ou_one',scope:'A',submitted_at:'',correct:null,delivery_status:'pending'},{name:'测试人员乙',open_id:'ou_two',scope:'B',submitted_at:'',correct:null,delivery_status:'pending'}]};
    const unexpected=[];
    lastUnexpected=unexpected;
    await context.route('**/*',async route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin!==base) return route.abort();
      if(!url.pathname.startsWith('/api/')) return route.continue();
      const p=url.pathname, method=request.method();
      const ok=data=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:true,data})});
      if(p==='/api/health')return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:true,service:'clipflow_backend',instance_id:'sharing-fixture'})});
      if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:actor==='admin'?'ou_admin':'ou_one',name:'测试用户',role:actor==='admin'?'admin':'user',modules:['learning']},scope_options:[{value:'A',label:'A楼'}]});
      if(p==='/api/assistant/appearance')return ok({enabled:false});
      if(p==='/api/assistant/icon-weather')return ok({enabled:false});
      if(p==='/api/handover-links')return ok({links:{}});
      if(p==='/api/auth/permission-requests/current')return ok({});
      if(p==='/api/scope-overview')return ok({scopes:{}});
      if(p==='/api/learning/bootstrap')return ok({is_admin:actor==='admin',can_answer:true,can_view_buildings:actor==='admin',self_person:{id:'p1',name:'测试人员甲',scopes:['A']},self_scope:'A',scopes:[{value:'A',label:'A楼'}],settings:{enabled:true},sync:{status:'ready',pending:0},today:'2026-10-14'});
      if(p==='/api/learning/sharing') {
        if(method==='POST'){ assert.deepEqual(request.postDataJSON().question_types,['single','multiple']);created++;return ok({id:batch.id}); }
        return ok({items:[batch],page:1,total:1});
      }
      if(p===`/api/learning/sharing/${batch.id}`) {
        if(method==='PUT'){ const payload=request.postDataJSON();batch.questions=payload.questions;batch.participants.forEach(p=>p.exempted=payload.excluded_open_ids.includes(p.open_id));batch.total=batch.participants.filter(p=>!p.exempted).length;batch.version++; }
        return ok(actor==='admin'?batch:{...batch,questions:batch.questions.slice(0,5).map(({correct,explanation,evidence,...q})=>q),participants:batch.participants.slice(0,1),total:1});
      }
      if(p.endsWith('/publish')){ published++;batch.status='published';batch.version++;return ok({id:batch.id}); }
      unexpected.push(`${method} ${p}`);return route.fulfill({status:500,contentType:'application/json',body:JSON.stringify({ok:false,error:'Unexpected fixture endpoint'})});
    });
    const entry=v2?'/assets/workspace-v2/index.html?__page=%2Flearning&tab=sharing':'/learning?tab=sharing';
    await page.goto(base+entry);
    await page.getByRole('heading',{name:'分享会练习',exact:true}).waitFor();
    await page.getByRole('button',{name:'AI出题',exact:true}).click();
    await page.getByLabel('飞书纪要链接',{exact:true}).fill('https://vnet.feishu.cn/docx/Fixture');
    await page.getByRole('checkbox',{name:'多选题',exact:true}).check();
    await page.getByRole('button',{name:'读取资料并生成题目',exact:true}).click();
    await page.getByRole('heading',{name:batch.title,exact:true}).waitFor();
    assert.equal(created,1);assert.equal(published,0);
    await page.locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:'参会人'}).click();
    await page.getByRole('checkbox',{name:'参加答题：测试人员乙',exact:true}).uncheck();
    await page.getByText('参会 2 人 · 参加答题 1 人 · 本次不做 1 人',{exact:true}).waitFor();
    await page.locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:'题目核对'}).click();
    await page.locator('#sharing-q1').fill('修改后的题干：出现流量告警后，首先应核对什么？');
    assert.equal(await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).isDisabled(),true);
    await page.getByRole('button',{name:'保存题目与名单',exact:true}).click();
    await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).click();
    const dialog=page.getByRole('dialog');await dialog.waitFor();assert.equal(published,0);assert.match(await dialog.innerText(),/向1位参会人/);
    await dialog.getByRole('button',{name:'取消',exact:true}).click();assert.equal(published,0);
    await page.locator('.confirm-backdrop').waitFor({state:'detached'});
    await page.screenshot({path:path.join(output,`draft-${v2?'v2':'old'}-${width}.png`),fullPage:true});
    await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).click();
    await dialog.getByRole('button',{name:'确认',exact:true}).click();
    await page.getByRole('button',{name:'关闭本批',exact:true}).waitFor();assert.equal(published,1);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),true);
    actor='personal';await page.reload();
    await page.locator('.sharing-batch').click();
    await page.locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:'我的题目'}).click();
    await page.getByRole('heading',{name:'我的5道题',exact:true}).waitFor();
    assert.equal(await page.getByRole('button',{name:'核对后发布给参会人',exact:true}).count(),0);
    assert.equal(await page.locator('.sharing-answer').count(),0);
    assert.equal(await page.locator('.sharing-question').count(),5);
    await page.screenshot({path:path.join(output,`personal-${v2?'v2':'old'}-${width}.png`),fullPage:true});
    assert.deepEqual(unexpected,[]);await context.close();
  }
  assert.deepEqual(errors,[]);console.log('[LearningSharingBrowser] OK: old/v2, PC/mobile, preview/confirm, answer visibility');
} catch (e) {
  console.error({label,errors,unexpected:lastUnexpected,url:activePage?.url(),body:(await activePage?.locator('body').innerText().catch(()=>''))?.slice(-5000)});
  await activePage?.screenshot({path:path.join(output,'failure.png'),fullPage:true});
  throw e;
} finally {await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
