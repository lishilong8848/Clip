import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('../',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/learning-daily-entry');
const designTest=process.argv.includes('--design-test');
const designBuild=process.env.CLIPFLOW_LEARNING_V2_BUILD;
const legacyBuild=process.env.CLIPFLOW_LEARNING_LEGACY_BUILD||path.resolve(root,'dist');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
const serve=process.argv.includes('--serve');
const day=serve?new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date()):'2026-10-28';
const person={id:'self',name:'隔离学练人员',scopes:['A'],active:true};
const longAnalysis='核对现场安全条件。'.repeat(120);
try{
  for(const v2 of [false,true]){
    if(designTest && !v2)continue;
    if(process.argv.includes('--serve') && v2!==!process.argv.includes('--legacy'))continue;
    let published=false,daily=null,practice=null,admin=false,onDuty=true,profileFailure=false,profileDay=day,profileReads=0;
    const claims=[],errors=[];let auditPage;
    const makePaper=(id,mode)=>({id,mode,person_id:person.id,person,scope:'A',date:day,status:'pending',version:1,
      questions:Array.from({length:2},(_,n)=>({id:id+'-q'+n,version:'1',bank:'written',type:'single',
        stem:`隔离${mode==='daily'?'每日题单':'自主练习'}第${n+1}题`,options:[{id:'A',text:'核对安全条件'},{id:'B',text:'直接操作'}]}))});
    const groupFeedback=[{id:'issue-one',scope:'A',description:'题干建议明确问的是第一步。',created_at:day},{id:'issue-two',scope:'D',description:'建议补充判断依据。',created_at:day}];
    const revision={id:'daily-q1',version:'v1',stem:'设备异常时，应先核对什么？',bank:'written',type:'single',status:'published',options:[{id:'A',text:'确认人员安全与设备状态'},{id:'B',text:'立即复位全部设备'}],correct_option_ids:['A']};
    const editRequests=[];
    const publicPaper=p=>p?{...structuredClone(p),status:p.questions.every(q=>q.attempt)?'completed':'pending',
      stats:{total:2,answered:p.questions.filter(q=>q.attempt).length}}:null;
    const server=createServer(async(req,res)=>{
        const url=new URL(req.url,'http://fixture');
        if(!url.pathname.startsWith('/api/')){
          const workspaceAsset=Boolean(designBuild&&url.pathname.startsWith('/assets/workspace-v2/'));
          const file=workspaceAsset?url.pathname.slice('/assets/workspace-v2/'.length):['/','/learning'].includes(url.pathname)?'index.html':url.pathname.slice(1);
          const directory=workspaceAsset?designBuild:legacyBuild;
          const target=path.resolve(directory,file),dist=path.resolve(directory)+path.sep;
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
          scopes:[],today:profileDay,daily_duty:onDuty?{date:profileDay,shift:'白',scope:'A',end_at:serve?new Date(Date.now()+10800000).toISOString():profileDay+'T18:00:00+08:00'}:null,
          settings:{enabled:true},sync:{status:'ready',pending:0}});
        if(url.pathname==='/api/learning/profile'){
          profileReads++;
          if(profileFailure){res.statusCode=400;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:false,error:'隔离测试：进度暂不可用'}));return;}
          return ok({to:profileDay,person:url.searchParams.get('person_id')?person:null,
            summary:{learning_days:3,answered:96,accuracy:88,wrong:8},today_summary:{assigned:20,task_answered:19},
            daily_tasks:[{date:profileDay,shift:'夜',scope:'A',assigned:18,answered:18},
              {date:profileDay,shift:'白',scope:'A',assigned:2,answered:designTest?1:0}],
            people:[],trend:designTest?[9,14,11,15,12,17,18].map((attempt_count,index)=>({date:`2026-10-${22+index}`,attempt_count})):[],banks:[],topics:[],distribution:[]});
        }
        if(url.pathname==='/api/learning/issues/pending-questions'){
          assert(admin,'ordinary users must not read the queue');
          const items=designTest&&groupFeedback.length?[{question_id:revision.id,issue_id:'issue-one',stem:revision.stem,type:revision.type,feedback_count:groupFeedback.length}]:[];
          return ok({items,total:items.length,page:1,page_size:20});
        }
        if(designTest&&['/api/learning/issues/issue-one/question',`/api/learning/questions/${revision.id}/revision`].includes(url.pathname)){
          assert(admin,'ordinary users must not read the original question');
          const grouped=url.pathname.includes('/issues/');
          if(grouped) assert.equal(url.searchParams.get('group'),'1');
          if(req.method==='PUT'){
            assert.equal(body.version,revision.version);
            revision.stem=body.stem;revision.version+='-next';
            if(grouped)groupFeedback.length=0;
          }
          editRequests.push({method:req.method,grouped});
          return ok({question:revision,editable:true,feedback:grouped?groupFeedback:[],issues_version:grouped?'open-feedback-v1':''});
        }
        if(designTest&&url.pathname==='/api/learning/questions'){assert(admin);return ok({items:[revision],total:1,page:1});}
        if(designTest && url.pathname==='/api/learning/history')return ok({items:daily?[publicPaper(daily)]:[],total:daily?1:0});
        if(['/api/learning/review','/api/learning/history','/api/learning/issues','/api/learning/sharing'].includes(url.pathname))return ok({items:[],total:0});
        if(url.pathname==='/api/learning/papers')return ok({today:day,published,items:[url.searchParams.get('mode')==='practice'?practice:daily].filter(Boolean).map(publicPaper)});
        if(url.pathname==='/api/learning/papers/claim'){
          claims.push(body);assert.equal(body.person_id,person.id);
          if(body.mode==='practice'){practice=makePaper('practice','practice');return ok(publicPaper(practice));}
          assert(published&&onDuty);assert.equal(body.date,profileDay);assert.equal(body.shift,'白');
          daily=makePaper('daily','daily');return ok(publicPaper(daily));
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
            q.attempt={option_ids:body.option_ids,correct:true,submitted_at:day+'T14:00:00+08:00'};q.answer={correct_option_ids:['A'],analysis:longAnalysis};
          }
          return ok(publicPaper(paper));
        }
        if(!url.pathname.startsWith('/api/learning/'))return ok({items:[],stats:{},links:{},options:[]});
        errors.push(req.method+' '+url.pathname);res.statusCode=500;res.end('{}');
    });
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
    const entry=v2?'/assets/workspace-v2/index.html?__page=/learning&view=practice':'/learning?frontend=legacy&view=practice';
    if(process.argv.includes('--serve')&&v2===!process.argv.includes('--legacy')){
      published=true;admin=process.argv.includes('--admin');daily=makePaper('daily','daily');
      if(designTest) daily.questions[0].attempt={correct:true,option_ids:['A'],submitted_at:day+'T14:00:00+08:00'};
      console.log('Learning design preview: '+origin+entry.replace('&view=practice',admin&&!designTest?'&view=overview':''));
      await new Promise(()=>{});
    }
    try{
      if(designTest){
        admin=true;published=true;daily=makePaper('daily','daily');
        daily.questions[0].attempt={correct:true,option_ids:['A'],submitted_at:day+'T14:00:00+08:00'};
        const homeUrl=origin+entry.replace('&view=practice','');
        const context=await browser.newContext({viewport:{width:1440,height:1000},serviceWorkers:'block',reducedMotion:'reduce'});
        await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
        const page=await context.newPage();auditPage=page;page.on('pageerror',error=>errors.push(error.message));
        await page.clock.setFixedTime(new Date(day+'T14:00:00+08:00'));
        await page.goto(homeUrl);
        const tile=page.getByRole('button',{name:'继续答题',exact:true});
        await tile.waitFor();
        assert(await tile.evaluate(el=>getComputedStyle(el).color===getComputedStyle(el.closest('.learning-personal-home')).color),'duty card uses a readable foreground on the themed surface');
        assert.equal(await tile.getByRole('progressbar').getAttribute('aria-valuenow'),'50');
        await page.getByRole('button',{name:'题库管理',exact:true}).waitFor();
        await page.getByRole('button',{name:'题目反馈',exact:true}).waitFor();
        await page.screenshot({path:path.join(output,'redesign-desktop-home.png'),fullPage:true});
        for(const [width,height] of [[402,874],[820,1180]]){
          await page.setViewportSize({width,height});
          assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
          const cta=tile.locator('.daily-action'), box=await cta.boundingBox();
          assert(box.width<=100&&box.height<=36&&box.x>=0&&box.x+box.width<=width,'continue action stays compact and within the viewport');
          assert((await tile.boundingBox()).height>=44,'the full card remains a comfortably sized target');
          await page.screenshot({path:path.join(output,`redesign-${width}-home.png`),fullPage:true});
          if(width===402){
            await cta.click();
            await page.locator('.lp-question .stem').getByText('隔离每日题单第2题',{exact:true}).waitFor();
            await page.getByRole('navigation',{name:'学练页面'}).getByRole('button',{name:'学习首页',exact:true}).click();
            await tile.waitFor();
          }
        }
        await page.setViewportSize({width:1440,height:1000});
        await page.getByRole('button',{name:'题库管理',exact:true}).click();
        const bank=page.getByRole('region',{name:'题库列表'});
        await bank.getByText(revision.stem,{exact:true}).waitFor();
        const nav=page.getByRole('navigation',{name:'学练页面'});
        const expectedTabs=['学习首页','个人画像','今日学练','错题与复习','学习历史','题目反馈','分享会练习','每日完成','题库管理','发布设置'];
        const tabNames=()=>nav.getByRole('button').allTextContents().then(items=>items.map(item=>item.trim()));
        assert.deepEqual(await tabNames(),expectedTabs);
        await nav.getByRole('button',{name:'学习首页',exact:true}).click();
        await tile.waitFor();
        assert.deepEqual(await tabNames(),expectedTabs,'the home must retain all authorized section buttons');
        assert.equal(await page.getByRole('navigation',{name:'学习入口'}).count(),0,'the new homepage does not repeat the section buttons');
        await page.getByRole('button',{name:'学习管理',exact:true}).click();
        await page.getByRole('region',{name:'学习管理工作台'}).waitFor();
        assert.deepEqual(await tabNames(),expectedTabs.map(label=>label==='个人画像'?'楼栋汇总':label));
        await nav.getByRole('button',{name:'今日学练',exact:true}).click();
        await page.getByRole('region',{name:'题单与作答'}).waitFor();
        assert.deepEqual(await tabNames(),expectedTabs);
        await nav.getByRole('button',{name:'题库管理',exact:true}).click();
        await bank.getByText(revision.stem,{exact:true}).waitFor();
        assert.equal(await bank.getByRole('button',{name:'批量启用'}).count(),0);
        await page.screenshot({path:path.join(output,'redesign-desktop-bank.png'),fullPage:true});
        await page.setViewportSize({width:390,height:844});
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
        await page.screenshot({path:path.join(output,'redesign-mobile-bank.png'),fullPage:true});
        await bank.getByRole('button',{name:'导入题目',exact:true}).click();
        const importDialog=page.getByRole('dialog',{name:'批量导入题目'});
        await importDialog.getByText('下载模板',{exact:true}).waitFor();
        await importDialog.getByRole('button',{name:'关闭',exact:true}).click();
        await page.setViewportSize({width:1440,height:1000});
        await page.getByRole('button',{name:'题目反馈',exact:true}).click();
        const feedback=page.getByRole('group',{name:'反馈视图'});
        await feedback.getByRole('button',{name:'待处理 1',exact:true}).waitFor();
        await feedback.getByRole('button',{name:'全部反馈',exact:true}).click();
        await page.getByText('当前筛选下没有反馈',{exact:true}).waitFor();
        await feedback.getByRole('button',{name:'待处理 1',exact:true}).click();
        await page.getByText('题干建议明确问的是第一步。',{exact:true}).waitFor();
        await page.getByText('题目反馈 · 2 条',{exact:true}).waitFor();
        await page.screenshot({path:path.join(output,'redesign-desktop-pending.png'),fullPage:true});
        await page.setViewportSize({width:402,height:874});
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
        await page.getByRole('button',{name:'返回待处理列表',exact:true}).click();
        await page.getByRole('button',{name:new RegExp('设备异常时')}).click();
        await page.getByText('题干建议明确问的是第一步。',{exact:true}).waitFor();
        await page.screenshot({path:path.join(output,'redesign-402-pending.png'),fullPage:true});
        await page.setViewportSize({width:1440,height:1000});
        const editor=page.getByRole('dialog',{name:'编辑题目',exact:true});
        await page.getByRole('button',{name:'编辑本题',exact:true}).click();
        assert.equal(await editor.getByLabel('题干',{exact:true}).inputValue(),revision.stem);
        await editor.getByLabel('题干',{exact:true}).fill('设备异常时，第一步应核对什么？');
        await editor.getByRole('button',{name:'保存题目',exact:true}).click();
        await feedback.getByRole('button',{name:'待处理 0',exact:true}).waitFor();
        await page.goto(homeUrl);await tile.click();
        await page.locator('.stem').filter({hasText:'隔离每日题单第2题'}).waitFor();
        await page.getByRole('button',{name:'编辑本题',exact:true}).click();
        assert.equal(await editor.getByLabel('题干',{exact:true}).inputValue(),revision.stem);
        await editor.getByRole('button',{name:'关闭',exact:true}).click();
        await page.goto(origin+entry+'&paper_id=daily');
        await page.locator('.stem').waitFor();
        await page.getByRole('button',{name:'下一题',exact:true}).click();
        await page.getByRole('button',{name:'编辑本题',exact:true}).click();
        await editor.getByLabel('题干',{exact:true}).fill('回顾页修订后的题干');
        await editor.getByRole('button',{name:'保存题目',exact:true}).click();
        await editor.getByText('已保存题库最新版。',{exact:true}).waitFor();
        assert.equal(daily.questions[1].stem,'隔离每日题单第2题');
        assert(editRequests.some(row=>row.method==='PUT'&&!row.grouped));
        admin=false;await page.goto(homeUrl);await tile.waitFor();
        assert.equal(await page.getByRole('button',{name:/待处理题目/}).count(),0);
        await tile.click();await page.locator(v2?'.lp-question':'.question-body').getByRole('button',{name:v2?'反馈本题':'题目有疑问',exact:true}).waitFor();
        assert.equal(await page.getByRole('button',{name:'编辑本题',exact:true}).count(),0);
        onDuty=false;await page.setViewportSize({width:390,height:844});await page.goto(homeUrl);
        const home=page.getByRole('region',{name:'个人学习首页'});
        await nav.getByRole('button',{name:'分享会练习',exact:true}).waitFor();
        assert.equal(await home.getByText('当班题单',{exact:true}).count(),0);
        await home.getByText('历史题单',{exact:true}).waitFor();
        await page.screenshot({path:path.join(output,'redesign-mobile-off-duty.png'),fullPage:true});
        await nav.getByRole('button',{name:'分享会练习',exact:true}).click();
        await page.locator('.sharing-exercises').waitFor();
        assert.equal(claims.length,0);
        assert.deepEqual(errors,[]);
        await context.close();
        console.log('[LearningRedesign] OK: progress tile, grouped feedback, original revision, paper review, permission gates');
        continue;
      }
      for(const [width,height] of [[402,874],[440,956],[1440,900]]){
        console.log('[DailyEntry]',{v2,width});
        published=false;daily=null;practice=null;admin=false;profileFailure=false;profileDay=day;profileReads=0;claims.length=0;
        const context=await browser.newContext({viewport:{width,height},serviceWorkers:'block',reducedMotion:'reduce'});
        await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
        const page=await context.newPage();auditPage=page;page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.stack));
        await page.clock.setFixedTime(new Date(day+'T14:00:00+08:00'));
        const ready=async()=>{await page.waitForFunction(()=>document.querySelector('.learning-page')?.getAttribute('aria-busy')==='false');assert.deepEqual(errors,[]);};
        const dailyButton=()=>page.getByRole('button',{name:'打开当班题单',exact:true});
        const switchMode=mode=>page.getByRole('group',{name:v2?'题单类型':'学练方式'}).getByRole('button',{name:mode==='每日题单'?'当班题单':mode,exact:true}).click();
        const openSection=async(label)=>{
          const nav=page.getByRole('navigation',{name:'学练页面'});
          await nav.getByRole('button',{name:label,exact:true}).click();
        };
        const firstAnswer=v2?page.getByRole('group',{name:'作答选项'}).getByRole('button',{name:/核对安全条件/}):page.locator('.options input[value="A"]');
        const chooseFirst=()=>v2?firstAnswer.click():firstAnswer.check();
        const firstIsSelected=async()=>v2?(await firstAnswer.getAttribute('aria-pressed'))==='true':await firstAnswer.isChecked();
        await page.goto(origin+entry.replace('&view=practice',''));await page.getByRole('region',{name:'个人学习首页'}).waitFor();await ready();assert.equal(claims.length,0,'learning home is read-only');
        assert.equal(await page.getByRole('navigation',{name:'学练页面'}).count(),v2?1:0);
        const home=page.getByRole('region',{name:'个人学习首页'});
        const homeEntries=await home.getByRole('navigation',{name:'学习入口'}).getByRole('button').allTextContents();
        assert.deepEqual(homeEntries,v2?[]:['自主练习','分享会练习','复习中心']);
        profileFailure=true;await page.getByRole('button',{name:'刷新当前页面',exact:true}).click();await ready();
        await home.getByText(/进度暂未读取/).waitFor();assert.equal(await home.getByText(/暂无已领取题目/).count(),0,'read failures must not claim no assignments');
        if(!v2){await page.getByRole('button',{name:'关闭错误提示',exact:true}).click();assert(await home.getByText(/进度暂未读取/).isVisible(),'dismissing an error is not a successful refresh');}
        profileFailure=false;await home.getByRole('button',{name:'刷新学习进度'}).click();await ready();await home.getByText('待作答 2 题',{exact:true}).waitFor();
        profileDay='2026-10-29';const beforeRollover=profileReads;await page.clock.setFixedTime(new Date(profileDay+'T09:00:00+08:00'));await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
        await page.locator('.learning-page > header time').filter({hasText:/^2026-10-29$/}).waitFor();assert(profileReads>beforeRollover,'home refreshes its statistics after a Beijing date rollover');
        profileDay=day;await page.clock.setFixedTime(new Date(day+'T14:00:00+08:00'));await page.getByRole('button',{name:'刷新当前页面',exact:true}).click();await ready();
        for(const label of (v2?['错题与复习','分享会练习','学习历史','个人画像','题目反馈']:['复习中心','分享会练习','查看全部','个人学习画像','题目反馈'])){
          if(v2)await openSection(label);
          else await home.getByRole('button',{name:new RegExp(label)}).click();
          await ready();
          if(label==='分享会练习')await page.locator('.sharing-exercises').waitFor();
          await openSection('学习首页');
          await home.waitFor();await ready();
        }
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-home.png`),fullPage:true,animations:'disabled'});
        await page.getByRole('region',{name:'个人学习首页'}).getByRole('button',{name:'打开当班题单',exact:true}).click();await ready();
        const transition=await page.evaluate(()=>{const reject=(name,message)=>{const event=new PromiseRejectionEvent('unhandledrejection',{cancelable:true,promise:Promise.resolve(),reason:new DOMException(message,name)});window.dispatchEvent(event);return event.defaultPrevented;};return [reject('InvalidStateError','Transition was aborted because of invalid state. ViewTransition opt-in disabled'),reject('InvalidStateError','Business state mismatch')];});assert.deepEqual(transition,[true,false]);
        await dailyButton().click();await ready();assert.equal(claims.length,0,'daily entry never silently creates practice');
        published=true;await page.goto(origin+entry);await firstAnswer.waitFor();await ready();
        assert.equal(claims.length,1);assert.notEqual(claims[0].mode,'practice');
        await chooseFirst();
        assert.equal(await dailyButton().count(),0,'an open paper does not show another start/continue button');
        await switchMode('自主练习');await ready();await switchMode('每日题单');
        await ready();
        assert(await firstIsSelected(),'reopening daily keeps unsent answer draft');
        assert(daily.questions.every(q=>!q.attempt));
        await switchMode('自主练习');await ready();
        await page.getByRole('button',{name:v2?'新练习15题':'开始自主练习',exact:true}).click();await ready();
        await page.locator('.stem').filter({hasText:'自主练习第1题'}).waitFor();
        assert.equal(claims.filter(c=>c.mode==='practice').length,1);
        for(let i=0;i<2;i++){
          const bar=page.locator('[aria-label="答题操作"]');const before=await bar.boundingBox();assert(before&&before.y>=0&&before.y+before.height<=height,'answer controls visible before submit');
          await chooseFirst();await page.getByRole('button',{name:'确认作答',exact:true}).click();await ready();
          const confirmed=bar.getByRole('button',{name:'已确认作答',exact:true});await confirmed.waitFor();assert(await confirmed.isDisabled());
          if(v2)await page.getByText(longAnalysis,{exact:false}).waitFor();
          const after=await bar.boundingBox();assert(after&&Math.abs(after.y-before.y)<2&&Math.abs(after.height-before.height)<2,'long answers do not move action controls: '+JSON.stringify({v2,width,before,after}));
          if(i===0)await page.getByRole('button',{name:'下一题',exact:true}).click();
        }
        assert(practice.questions.every(q=>q.attempt));assert(daily.questions.every(q=>!q.attempt));
        await page.getByRole('button',{name:'查看每日完成',exact:true}).click();
        const row=page.locator('.completion tbody tr');await row.waitFor();
        assert.match(await row.textContent(),/已完成/);assert.match(await row.textContent(),/2 \/ 2 \/ 2/);
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-completion.png`),fullPage:true,animations:'disabled'});
        await page.goto(origin+entry);await ready();
        assert(await firstIsSelected(),'switching practice and reload preserve daily draft');
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
            await openSection(label);
            await page.locator(selector).waitFor();
            const response=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/learning/papers');releasePapers();await (await response).finished();
            await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
            assert(await page.locator(selector).isVisible(),'late paper result must not replace '+label);
            assert.equal(await page.locator('[aria-label="答题操作"]').count(),0,'leaving the paper removes its teleported controls');
          }finally{releasePapers();}
        }
        admin=true;await page.goto(origin+entry.replace('view=practice','view=overview')+(v2?'&tab=home':''));const management=page.getByRole('region',{name:'学习管理工作台'});await management.waitFor();await ready();
        if(v2)await page.getByRole('navigation',{name:'学练页面'}).getByRole('button',{name:'题目反馈',exact:true}).waitFor();
        else {
          assert.equal(await management.getByRole('button').count(),6);
          await page.getByRole('navigation',{name:'学练页面'}).getByRole('button',{name:'待处理题目 0',exact:true}).waitFor();
        }
        await page.screenshot({path:path.join(output,`${v2?'v2':'legacy'}-${width}-admin.png`),fullPage:true,animations:'disabled'});
        await context.close();
      }
      assert.deepEqual(errors,[]);
    }catch(error){console.error(errors,await auditPage?.locator('body').innerText());await auditPage?.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw error;}finally{await new Promise(resolve=>server.close(resolve));}
  }
  if(!designTest)console.log('[LearningDailyEntry] OK: legacy/V2 home navigation, failed-read recovery, Beijing rollover, delayed response isolation, invalid questions, fixed controls, autonomous credit and drafts; 402/440/1440');
}finally{await browser.close();}
