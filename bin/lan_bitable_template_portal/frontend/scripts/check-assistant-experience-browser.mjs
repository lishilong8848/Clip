import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/assistant-experience');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const page=await browser.newPage({viewport:{width:1440,height:960}});
page.setDefaultTimeout(12000);
const errors=[];page.on('pageerror',error=>errors.push(error.message));
const user={open_id:'experience-fixture',name:'隔离体验测试',role:'admin',level:'L1',modules:['assistant','maintenance']};
const scopes=[{value:'E',label:'E楼'},{value:'A',label:'A楼'}];
const turns=[{operation_id:'old',question:'历史消息',answer:'这是已完成的历史回答，不重新播放动效。',status:'completed',at:1},
  {operation_id:'new',question:'查询一下今天热点新闻',answer:'',status:'pending',at:Date.now()/1000,process:[{label:'正在思考回答'},{label:'正在查询公开资料'}]}];
const run={id:'experience-run',revision:1,scope:'E',slot:'evening',date:'2026-10-15',state:'edit',items:[],error:''};
await page.route('**/*',route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.origin!==origin)return route.abort();
  if(url.pathname==='/assets/connection-guard.js')return route.fulfill({contentType:'application/javascript',body:''});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(request.method()!=='GET')assert.equal(url.pathname,'/api/assistant/notice-panel/open','only the isolated preview draft may be opened');
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'experience-fixture'}});
  if(url.pathname==='/api/assistant/appearance')return ok({shape:'weather-lighthouse-mini',color:'encre',size:56});
  if(url.pathname==='/api/assistant/icon-weather')return ok({available:false,city:'南通'});
  if(url.pathname==='/api/assistant/conversation')return ok({conversation_id:'experience',turns,configured:true,enabled:true,busy:turns.some(t=>t.status==='pending'),model_name:'隔离模型',model_options:[],can_manage_settings:false});
  if(url.pathname==='/api/assistant/notice-panel')return ok({edition:{id:'today',date:'2026-10-15',slot:'evening',label:'17:00通告待办'},scopes});
  if(url.pathname.startsWith('/api/assistant/notice-panel/'))return ok({run});
  return ok({items:[],records:[],users:[],scopes:[],settings:{},stats:{}});
});
try{
  await page.goto(origin+'/assets/workspace-v2/index.html?view=my');
  await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
  await page.locator('.assistant-thinking').waitFor();
  const answer=page.locator('.turn').last().locator('.answer');
  assert.equal((await answer.innerText()).match(/正在思考/g)?.length,1,'one thinking label only');
  assert.equal(await answer.locator('.thinking-dots i').count(),3);
  assert.equal(await answer.locator('.process,.answer-state,.spin').count(),0,'no duplicate processing header or spinner');
  assert.equal(await answer.evaluate(node=>getComputedStyle(node,'::after').content),'none');
  assert.equal(await page.locator('.turn').first().locator('.v2-word').count(),0,'history is not reanimated');
  await page.getByLabel('楼栋',{exact:true}).selectOption('E');
  await page.getByLabel('时段',{exact:true}).waitFor();
  for(const color of ['#0a0a0c','#3b93f0','#e152b0']){
    await page.locator('.lighthouse').evaluate((node,color)=>node.style.setProperty('--bot-color',color),color);
    await page.waitForTimeout(200);
    const themed=await page.locator('.notice-filters').evaluate(node=>{
      const probe=document.createElement('span');node.append(probe);
      probe.style.color='var(--lh-surface-subtle)';const background=getComputedStyle(probe).color;
      probe.style.color='var(--lh-charcoal)';const foreground=getComputedStyle(probe).color;probe.remove();
      return [...node.querySelectorAll('select,input')].map(input=>({background:getComputedStyle(input).backgroundColor==='rgba(0, 0, 0, 0)'?getComputedStyle(input.parentElement).backgroundColor:getComputedStyle(input).backgroundColor,foreground:getComputedStyle(input).color,expectedBackground:background,expectedForeground:foreground,scheme:getComputedStyle(input).colorScheme}));
    });
    assert(themed.length>=3);
    for(const item of themed){assert.equal(item.background,item.expectedBackground);assert.equal(item.foreground,item.expectedForeground);assert.equal(item.scheme,'dark');}
  }
  await page.locator('.lighthouse').evaluate(node=>node.style.setProperty('--bot-color','#0a0a0c'));
  await page.waitForTimeout(250);
  assert.equal(await page.getByLabel('搜索通告',{exact:true}).evaluate(input=>getComputedStyle(input).borderTopWidth),'0px','notice search has one themed border, not an input inside another box');
  await page.screenshot({path:path.join(output,'thinking-and-themed-notices.png')});
  const text='## 今日要闻\n\n'+('这是一段用于核对逐词淡入效果的中文测试回答，保留原有业务接口和真实资料。').repeat(15)+'\n\n**重点内容**\n\n| 标题 | 来源 |\n|---|---|\n| 测试资料 | 官方来源 |\n\n```python\nprint("test")\n```';
  turns[1]={...turns[1],answer:text,status:'completed'};
  await answer.locator('.v2-word[hidden]').first().waitFor({state:'attached'});
  const hiddenBefore=await answer.locator('.v2-word[hidden]').count();
  await page.waitForTimeout(250);
  assert(await answer.locator('.v2-word[hidden]').count()<hiddenBefore,'words progressively enter after a buffered answer arrives');
  assert.equal(await answer.locator('.assistant-thinking').count(),0);
  assert.equal(await answer.locator('.v2-reply-caret').count(),1);
  assert(await answer.locator('.v2-reply-caret').evaluate(node=>!node.previousElementSibling?.hasAttribute('hidden')),'caret follows the visible word');
  await page.waitForFunction(()=>!document.querySelector('.turn:last-child .v2-reply [hidden]'));
  assert.equal(await answer.locator('strong').last().innerText(),'重点内容');
  assert.equal(await answer.locator('pre code').innerText(),'print("test")\n');
  assert.equal(await answer.locator('table tbody tr').count(),1);
  assert.equal(await answer.locator('.v2-reply-caret').isVisible(),false,'completed reply leaves no blinking cursor');
  await page.waitForFunction(()=>{const thread=document.querySelector('.thread');return thread.scrollHeight-thread.clientHeight-thread.scrollTop<=2;});
  await page.screenshot({path:path.join(output,'reply-desktop.png')});
  await page.setViewportSize({width:390,height:844});await page.waitForTimeout(350);
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await page.screenshot({path:path.join(output,'reply-mobile.png')});
  assert.deepEqual(errors,[]);
  console.log('PASS: production V2 themed notice filters, single thinking indicator, buffered word reveal, cursor, rich formatting, follow-bottom and mobile viewport; synthetic APIs only.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
