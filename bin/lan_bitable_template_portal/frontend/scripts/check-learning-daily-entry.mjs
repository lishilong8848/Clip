import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('../',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/learning-daily-entry');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
const day='2026-10-28',person={id:'self',name:'隔离学练人员',scopes:['A'],active:true};
try{
  for(const v2 of [false,true]){
    if(process.argv.includes('--serve') && v2!==!process.argv.includes('--legacy'))continue;
    let published=false,daily=null,practice=null,admin=false,profileFailure=false,profileDay=day,profileReads=0;
    const claims=[],errors=[];let auditPage;
    const makePaper=(id,mode)=>({id,mode,person_id:person.id,person,scope:'A',date:day,status:'pending',version:1,
      questions:Array.from({length:2},(_,n)=>({id:id+'-q'+n,version:'1',bank:'written',type:'single',
        stem:`隔离${mode==='daily'?'每日题单':'自主练习'}第${n+1}题`,options:[{id:'A',text:'核对安全条件'},{id:'B',text:'直接操作'}]}))});
    const publicPaper=p=>p?{...structuredClone(p),status:p.questions.every(q=>q.attempt)?'completed':'pending',
      stats:{total:2,answered:p.questions.filter(q=>q.attempt).length}}:null;
    const server=createServer(async(req,res)=>{
        const url=new URL(req.url,'http://fixture');
        if(!url.pathname.startsWith('/api/')){
          const file=['/','/learning'].includes(url.pathname)?'index.html':url.pathname.slice(1);
          const target=path.resolve(root,'dist',file),dist=path.resolve(root,'dist')+path.sep;
          if(!target.startsWith(dist)){res.statusCode=403;res.end();return;}
          try{const data=await readFile(target);res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml','.png':'image/png'})[path.extname(file)]||'application/octet-stream');res.end(data);}
          catch{res.statusCode=404;res.end();}return;
        }
        let raw='';for await(const part of req)raw+=part;
        const body=raw?JSON.parse(raw):{};
        const ok=data=>{res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,data}));};
        if(url.pathname==='/api/health'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,service:'clipflow_backend',instance_id:'learning-entry-fixture'}));return;}
        if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_fixture',name:person.name,role:admin?'admin':'member',level:admin?'L0':'L4',modules:['learning'],home_scope:'A'},scope_options:[{value:'A',label:'A楼'}]});
        if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
        if(url.pathname==='/api/assistant/appearance')return ok({enabled:false});
        if(url.pathname==='/api/learning/bootstrap')return ok({self_person:person,self_scope:'A',scope:'A',can_answer:true,is_admin:admin,can_view_buildings:admin,
          scopes:[],today:day,settings:{enabled:true},sync:{status:'ready',pending:0}});
        if(url.pathname==='/api/learning/profile'){
          profileReads++;
          if(profileFailure){res.statusCode=400;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:false,error:'隔离测试：进度暂不可用'}));return;}
          return ok({to:profileDay,person:url.searchParams.get('person_id')?person:null,summary:{learning_days:3,answered:15,accuracy:80},today_summary:{assigned:2,task_answered:0,answered:0,accuracy:null},people:[],trend:[],banks:[],topics:[],distribution:[]});
        }
        if(['/api/learning/review','/api/learning/history','/api/learning/issues','/api/learning/sharing'].includes(url.pathname))return ok({items:[],total:0});
        if(url.pathname==='/api/learning/papers')return ok({today:day,published,items:[url.searchParams.get('mode')==='practice'?practice:daily].filter(Boolean).map(publicPaper)});
        if(url.pathname==='/api/learning/papers/claim'){
          claims.push(body);assert.equal(body.person_id,person.id);
          if(body.mode==='practice'){practice=makePaper('practice','practice');return ok(publicPaper(practice));}
          assert(published);daily=makePaper('daily','daily');return ok(publicPaper(daily));
        }
        if(url.pathname==='/api/learning/completion'){
          const answered=[daily,practice].flatMap(p=>p?.questions||[]).filter(q=>q.attempt).length;
          const status=answered>=2?'已完成':'待答';
          return ok({items:[{id:'daily:self',date:day,person_id:person.id,person,scopes:['A'],status,assigned:2,answered,correct:answered,accuracy:100,
            answer_viewed:0,answer_view_unknown:0,practice_questions:practice?.questions.filter(q=>q.attempt).length||0,
            practice_attempts:2,review_count:0,tasks:[],source_papers:[],updated_at:day+'T14:00:00+08:00'}],summary:{[status]:1},total:1});
        }
        const match=url.pathname.match(/^\/api\/learning\/papers\/(daily|practice)(?:\/(start|answer))?$/);
        if(match){const paper=match[1]==='daily'?daily:practice;assert(paper);
          if(match[2]==='start')paper.started_at=day+'T14:00:00+08:00';
          if(match[2]==='answer'){
            assert.equal(body.person_id,person.id);const q=paper.questions.find(q=>q.id===body.question_id);assert(q);
            assert(!q.attempt,'confirmed answers must not be submitted twice');
            q.attempt={option_ids:body.option_ids,correct:true,submitted_at:day+'T14:00:00+08:00'};q.answer={correct_option_ids:['A'],analysis:'核对现场安全条件。'.repeat(120)};
          }
          return ok(publicPaper(paper));
        }
        if(!url.pathname.startsWith('/api/learning/'))return ok({items:[],stats:{},links:{},options:[]});
        errors.push(req.method+' '+url.pathname);res.statusCode=500;res.end('{}');
    });
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
    const entry=v2?'/assets/workspace-v2/index.html?__page=/learning&view=practice':'/learning?frontend=legacy&view=practice';
    if(process.argv.includes('--serve')&&v2===!process.argv.includes('--legacy')){published=true;admin=process.argv.includes('--admin');daily=makePaper('daily','daily');console.log('Learning design preview: '+origin+entry.replace('&view=practice',admin?'&view=overview':''));await new Promise(()=>{});}
    try{
      for(const [width,height] of [[402,874],[440,956],[1440,900]]){
        console.log('[DailyEntry]',{v2,width});
        published=false;daily=null;practice=null;admin=false;profileFailure=false;profileDay=day;profileReads=0;claims.length=0;
        const context=await browser.newContext({viewport:{width,height},serviceWorkers:'block',reducedMotion:'reduce'});
        await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
        const page=await context.newPage();auditPage=page;page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.stack));
        await page.clock.setFixedTime(new Date(day+'T14:00:00+08:00'));
        const ready=async()=>{await page.waitForFunction(()=>document.querySelector('.learning-page')?.getAttribute('aria-busy')==='false');assert.deepEqual(errors,[]);};
        const dailyButton=()=>page.getByRole('button',{name:v2?/^(开始今日答题|继续今日答题|查看今日题单)$/: '打开每日题单'});
        const switchMode=mode=>v2?page.locator('[aria-label="学练方式"] .n-radio-button').filter({hasText:mode}).click():page.getByRole('group',{name:'学练方式'}).getByRole('button',{name:mode,exact:true}).click();
        await page.goto(origin+entry.replace('&view=practice',''));await page.getByRole('region',{name:'个人学习首页'}).waitFor();await ready();assert.equal(claims.length,0,'learning home is read-only');
        assert.equal(await page.getByRole('navigation',{name:'学练页面'}).count(),0,'home cards are not duplicated by a second navigation row');
        const homeEntries=await page.locator('.home-entries strong').allTextContents();assert.equal(new Set(homeEntries).size,6);assert(homeEntries.includes('复习中心'));assert(!homeEntries.includes('收藏与笔记'),'favorites and notes stay in the review center');
        const home=page.getByRole('region',{name:'个人学习首页'});
        profileFailure=true;await home.getByRole('button',{name:'刷新学习进度'}).click();await ready();
        await home.getByText('学习进度暂未更新',{exact:true}).waitFor();assert.equal(await home.getByText('今天还没有必答题单',{exact:true}).count(),0,'read failures must not claim no assignments');
        if(!v2){await page.getByRole('button',{name:'关闭错误提示',exact:true}).click();assert(await home.getByText('学习进度暂未更新',{exact:true}).isVisible(),'dismissing an error is not a successful refresh');}
        profileFailure=false;await home.getByRole('button',{name:'刷新学习进度'}).click();await ready();await home.getByText('完成今天的学习任务',{exact:true}).waitFor();
        profileDay='2026-10-29';const beforeRollover=profileReads;await page.clock.setFixedTime(new Date(profileDay+'T09:00:00+08:00'));await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
        await page.waitForFunction(()=>document.querySelector('.home-date')?.textContent==='2026-10-29');assert(profileReads>beforeRollover,'home refreshes its statistics after a Beijing date rollover');
        profileDay=day;await page.clock.setFixedTime(new Date(day+'T14:00:00+08:00'));await home.getByRole('button',{name:'刷新学习进度'}).click();await ready();
        for(const label of ['复习中心','资料练习','学习记录','学习画像','题目反馈']){
          await home.getByRole('button',{name:new RegExp(label)}).click();await ready();
          if(label==='资料练习')await page.locator('.sharing-exercises').waitFor();
          await page.getByRole('navigation',{name:'学练页面'}).getByRole('button',{name:'学习首页',exact:true}).click();await home.waitFor();await ready();
        }
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-home.png`),fullPage:true,animations:'disabled'});
        await page.getByRole('region',{name:'个人学习首页'}).getByRole('button',{name:/打开每日题单/}).click();await ready();
        const transition=await page.evaluate(()=>{const reject=(name,message)=>{const event=new PromiseRejectionEvent('unhandledrejection',{cancelable:true,promise:Promise.resolve(),reason:new DOMException(message,name)});window.dispatchEvent(event);return event.defaultPrevented;};return [reject('InvalidStateError','Transition was aborted because of invalid state. ViewTransition opt-in disabled'),reject('InvalidStateError','Business state mismatch')];});assert.deepEqual(transition,[true,false]);
        await dailyButton().click();await ready();assert.equal(claims.length,0,'daily entry never silently creates practice');
        published=true;await page.goto(origin+entry);await page.locator('.options input[value="A"]').waitFor();await ready();
        assert.equal(claims.length,1);assert.notEqual(claims[0].mode,'practice');
        await page.locator('.options input[value="A"]').check();
        assert.equal(await dailyButton().count(),0,'an open paper does not show another start/continue button');
        await switchMode('自主练习');await ready();await switchMode('每日题单');
        await ready();
        assert(await page.locator('.options input[value="A"]').isChecked(),'reopening daily keeps unsent answer draft');
        assert(daily.questions.every(q=>!q.attempt));
        await switchMode('自主练习');await ready();
        await page.getByRole('button',{name:v2?'新练习15题':'开始自主练习',exact:true}).click();await ready();
        await page.locator('.stem').filter({hasText:'自主练习第1题'}).waitFor();
        assert.equal(claims.filter(c=>c.mode==='practice').length,1);
        for(let i=0;i<2;i++){
          const bar=page.locator('[aria-label="答题操作"]');const before=await bar.boundingBox();assert(before&&before.y>=0&&before.y+before.height<=height,'answer controls visible before submit');
          await page.locator('.options input[value="A"]').check();await page.getByRole('button',{name:'确认作答',exact:true}).click();await ready();
          const after=await bar.boundingBox();assert(Math.abs(after.y-before.y)<2&&Math.abs(after.height-before.height)<2,'long answers do not move action controls: '+JSON.stringify({v2,width,before,after}));await page.getByRole('button',{name:'已确认作答',exact:true}).waitFor();assert(await page.getByRole('button',{name:'已确认作答',exact:true}).isDisabled());
          if(i===0)await page.getByRole('button',{name:'下一题',exact:true}).click();
        }
        assert(practice.questions.every(q=>q.attempt));assert(daily.questions.every(q=>!q.attempt));
        await page.getByRole('button',{name:'查看每日完成',exact:true}).click();
        const row=page.locator('.completion tbody tr');await row.waitFor();
        assert.match(await row.textContent(),/已完成/);assert.match(await row.textContent(),/2 \/ 2 \/ 2/);
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-completion.png`),fullPage:true,animations:'disabled'});
        await page.goto(origin+entry);await ready();
        assert(await page.locator('.options input[value="A"]').isChecked(),'switching practice and reload preserve daily draft');
        assert.equal(claims.length,2,'reload reuses existing paper');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'toolbar fits viewport');
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-daily.png`),fullPage:true,animations:'disabled'});
        daily.questions[0].invalid=true;
        await page.goto(origin+entry+'&paper_id=daily');await ready();
        const invalidAction=page.getByRole('button',{name:'题目已失效',exact:true});await invalidAction.waitFor();assert(await invalidAction.isDisabled());
        assert.equal(await page.getByRole('button',{name:'已确认作答',exact:true}).count(),0,'invalid unanswered questions are not shown as submitted');
        delete daily.questions[0].invalid;
        for(const [label,selector] of [['每日完成','.completion'],['分享会练习','.sharing-exercises']]){
          let releasePapers,markEntered;const held=new Promise(resolve=>releasePapers=resolve),entered=new Promise(resolve=>markEntered=resolve);
          await page.route('**/api/learning/papers?*',async route=>{markEntered();await held;await route.continue();},{times:1});
          try{
            await page.goto(origin+entry);await entered;
            if(v2){await page.getByRole('button',{name:'更多',exact:true}).click();await page.locator('.n-dropdown-option').filter({hasText:label}).click();}
            else await page.getByRole('navigation',{name:'学练页面'}).getByRole('button',{name:label,exact:true}).click();
            await page.locator(selector).waitFor();
            const response=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/learning/papers');releasePapers();await (await response).finished();
            await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
            assert(await page.locator(selector).isVisible(),'late paper result must not replace '+label);
            assert.equal(await page.locator('[aria-label="答题操作"]').count(),0,'leaving the paper removes its teleported controls');
          }finally{releasePapers();}
        }
        admin=true;await page.goto(origin+entry.replace('view=practice','view=overview'));await page.getByRole('region',{name:'学习管理工作台'}).waitFor();await ready();assert.equal(await page.getByRole('region',{name:'学习管理工作台'}).getByRole('button').count(),6);assert.equal(await page.getByRole('navigation',{name:'学练页面'}).count(),0,'management cards are not duplicated by tabs');await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-admin.png`),fullPage:true,animations:'disabled'});
        await context.close();
      }
      assert.deepEqual(errors,[]);
    }catch(error){console.error(errors,await auditPage?.locator('body').innerText());await auditPage?.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw error;}finally{await new Promise(resolve=>server.close(resolve));}
  }
  console.log('[LearningDailyEntry] OK: legacy/V2 home navigation, failed-read recovery, Beijing rollover, delayed response isolation, invalid questions, fixed controls, autonomous credit and drafts; 402/440/1440');
}finally{await browser.close();}
