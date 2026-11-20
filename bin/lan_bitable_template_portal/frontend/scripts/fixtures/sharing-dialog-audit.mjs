export async function auditSharing({page,go,capture}){
  const sampleId='a'.repeat(64);
  const make=(id,title,status)=>{
    const image={id:sampleId,label:'水泵回路示意',url:'/api/learning/sharing/'+id+'/images/'+sampleId};
    const questions=Array.from({length:8},(_,i)=>({id:'q'+i,type:'single',stem:'巡查冷却水泵时应先核对什么？ '+(i+1),options:['设备编号和现场状态','直接操作'],correct:[0],explanation:'先核对记录。',evidence:'资料第2段：核对设备编号。',images:i===0?[image]:[],image_ids:i===0?[sampleId]:[],image_confirmed:true}));
    const participants=[{open_id:'member',name:'当班工程师',scope:'A',submitted_at:'2026-11-19T10:00',choice_count:5,correct:4},{open_id:'member2',name:'值班员',scope:'H',exempted:false}];
    const ids=questions.slice(0,5).map(q=>q.id);
    const result={...participants[0],question_ids:ids,answers:{q0:[0],q1:[0],q2:[1],q3:[0],q4:[0]}};
    return {id,title,status,version:1,url:'https://example.invalid/minutes',question_count:8,completed:1,total:2,participants,questions,results:[result],bank_sync:{total:8,synced:7,failed:1},source_images:[image],evidence_locations:{q0:[{title:'隔离纪要 · 水泵巡查',references:[]}]},...(status==='published'?{paper:{open_id:'ou_responsive_fixture',question_ids:ids,draft_answers:{},progress_version:1}}:{})};
  };
  const draft=make('draft','水泵巡查 · 待发布','draft'),published=make('published','冷源运行 · 已发布','published'),failed=make('failed','设备巡查 · 生成失败','failed'),batches=[draft,published,failed];
  await page.route('**/api/learning/sharing**',async route=>{
    const u=new URL(route.request().url()),[,id,action]=u.pathname.slice('/api/learning/sharing'.length).split('/');const b=batches.find(v=>v.id===id);
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(!id)return ok({items:batches,total:batches.length});
    if(id==='people')return ok({items:[],total:0});
    if(!b)return route.fulfill({status:404,json:{ok:false,error:'隔离批次不存在'}});
    if(action==='images')return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="460" height="180"><rect width="460" height="180" fill="#e4f3ef"/><path d="M20 90h160V55h100v35h160" stroke="#328575" fill="none" stroke-width="8"/><text x="24" y="32">隔离题图</text></svg>'});
    if(u.searchParams.has('person'))return ok(b.results[0]);
    if(route.request().method()==='GET')return ok(b);
    if(action==='answers'){
      const payload=route.request().postDataJSON();
      b.paper.draft_answers={...b.paper.draft_answers,...payload.answers};b.paper.progress_version++;
      if(payload.submit){
        b.paper.submitted_at='2026-11-19T10:00';
        for(const question of b.questions)question.selected=b.paper.draft_answers[question.id]||[];
      }
      return ok(b);
    }
    if(action==='retry'){b.status='draft';return ok({id:b.id});}
    return route.fulfill({status:405,json:{ok:false,error:'隔离只读批次'}});
  });
  const section=page.locator('.sharing-exercises');
  const btn=(root,name)=>root.getByRole('button',{name,exact:true});
  const tab=label=>section.locator('.v2-sharing-tabs .n-tabs-tab').filter({hasText:label}).click();
  async function confirm(key,title,accept=false){
    const box=page.getByRole('dialog').filter({hasText:title}).last();await capture('share-confirm-'+key,title,box);await btn(box,accept?'确认':'取消').click();
  }
  async function enter(b){await go('/learning&tab=sharing');await section.locator('.sharing-batch').filter({hasText:b.title}).click();await section.getByRole('heading',{name:b.title}).waitFor();}
  await go('/learning&tab=sharing');
  for(const [key,label] of [['ai','AI出题'],['manual','人工出题'],['import','导入表格/文档']]){
    await btn(section,label).click();const form=section.locator('.sharing-create');await capture('share-create-'+key,label+' · 新建办理',form);
    if(key==='manual'){await form.locator('input:not([type=checkbox],[type=radio],[type=file])').first().fill('隔离手工录题');await btn(section,'返回批次列表').click();await confirm('create-discard','放弃新建草稿',true);}
    else await btn(section,'返回批次列表').click();
    await form.waitFor({state:'hidden'});
  }
  await enter(draft);await capture('share-questions','草稿题目核对与发布操作',section);
  await section.getByRole('button',{name:'定位题目依据'}).first().click();const evidence=page.locator('.sharing-evidence');await capture('share-evidence','题目依据及来源定位',evidence);await page.keyboard.press('Escape');
  await section.getByRole('button',{name:/查看原图/}).first().click();const image=page.getByRole('dialog',{name:'题目原图预览'});await capture('share-image','题目原图与缩放',image);await page.keyboard.press('Escape');
  await tab('参会人');await capture('share-roster','参会人处理与投递状态',section);await section.getByRole('button',{name:'查看答卷'}).first().click();await capture('share-paper','参会人答卷明细',page.locator('.v2-sharing-paper'));await page.keyboard.press('Escape');
  await tab('通知与同步');await capture('share-delivery','通知与题库同步记录',section);
  await tab('题目核对');await btn(section,'核对后发布给参会人').click();await confirm('publish','发布分享会练习');await btn(section,'删除批次').click();await confirm('delete','删除分享会批次');
  await section.getByLabel('题干',{exact:true}).first().fill('隔离：已复核设备编号');await btn(section,'放弃修改，读取最新').click();await confirm('discard','放弃未保存修改',true);
  await enter(published);await capture('share-my-paper','发布后个人作答',section);await btn(section,'重试未完成任务').click();await confirm('retry','重试未确认发送');await btn(section,'关闭本批').click();await confirm('close','关闭此批次');
  const paper=section.getByRole('region',{name:'我的答题',exact:true});
  for(let i=1;i<=5;i++)await paper.getByRole('radio',{name:'第'+i+'题选项A',exact:true}).check();
  await btn(paper,'保存本页进度').click();await section.getByText('答题进度已保存，尚未提交整卷。',{exact:true}).waitFor();
  await capture('share-saved-answers','已保存五题作答进度',paper);
  await btn(paper,'提交全部答题').click();await section.getByRole('heading',{name:'我的答卷',exact:true}).waitFor();
  await capture('share-submitted','整卷提交与作答回读',section);
  await enter(failed);await btn(section,'重新生成').click();await btn(section,'核对后发布给参会人').waitFor();await capture('share-retry','生成失败后重试回执',section);
}

