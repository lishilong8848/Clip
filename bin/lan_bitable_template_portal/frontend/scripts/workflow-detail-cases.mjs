import assert from 'node:assert/strict';
const dialog=p=>p.getByRole('dialog').last();
const risk='/risk-inspections?scope=A',drill='/drill-management?scope=A',guard='/critical-guard?scope=A';
async function riskDetail({p,go},tab){
  await go(risk);
  if(tab)await p.getByRole('tab',{name:tab,exact:true}).click();
  await p.locator('.risk-task-name').first().click();
  await p.locator('.rk-brief').waitFor();
}
async function isolatedCollab(p,origin,device,api){
  const {collabFixture}=await import('./responsive-audit-collab-fixtures.mjs?case='+encodeURIComponent(api+device));
  await p.route(origin+'/api/'+api+'**',async route=>{
    const req=route.request(),result=await collabFixture({url:new URL(req.url()),method:req.method(),body:req.postDataJSON()});
    if(!result)return route.fallback();
    await route.fulfill(result);
  });
}
export const detailCases={
  async 人员工作交接({p,go,shot,b,click,device,origin}){
    await isolatedCollab(p,origin,device,'work-handovers');
    await go('/work-handover?scope=A');
    const ledger=p.locator('.transfer-ledger'),form=p.locator('.transfer-form'),confirm=p.locator('.transfer-confirm');
    await shot('交接清单',ledger.locator('button'));
    await ledger.getByRole('button').filter({hasText:'草稿'}).first().click();
    await shot('交接草稿与接收人',form);
    await click('添加事项');
    const editor=p.locator('.transfer-dialog');
    assert(await b('加入清单',editor).isDisabled());
    const title='隔离验收：复核泵房读数';
    await editor.getByLabel('事项名称').fill(title);
    await editor.getByLabel('当前进度、下一步与注意事项').fill('下一班核对水位并记录结果');
    await shot('事项名称楼栋与进度',editor);await click('加入清单',editor);
    const saved=p.waitForResponse(r=>r.request().method()==='PUT'&&new URL(r.url()).pathname.startsWith('/api/work-handovers/'));
    await click('保存草稿',form);
    const response=await saved,body=response.request().postDataJSON();
    assert(response.ok());assert.equal(body.version,1);assert(body.data.items.some(item=>item.title===title&&item.scope==='A'));
    await p.getByText('交接草稿已保存',{exact:true}).waitFor();
    await click('返回交接清单');await ledger.getByRole('button').filter({hasText:'草稿'}).first().click();
    await shot('重开草稿已保存事项',p.locator('[data-handover-item]').filter({hasText:title}));
    await click('发送交接',form);await shot('发送工作交接确认',confirm);await click('取消',confirm);
    await click('返回交接清单');await click('待我接收');
    await ledger.getByRole('button').filter({hasText:'待接收'}).first().click();
    await click('退回补充');assert(await b('退回补充',confirm).isDisabled());
    await confirm.getByLabel('退回原因').fill('请补齐现场记录');assert(await b('退回补充',confirm).isEnabled());
    await shot('退回补充原因',confirm);await click('取消',confirm);
    await click('确认接收');await shot('接收确认',confirm);await click('确认接收交接',confirm);
    await p.getByText('已接收，事项已加入接续清单',{exact:true}).waitFor();
    const item=p.locator('[data-handover-item]').first(),itemTitle=await item.locator('strong').innerText();
    await click('确认完成',item);await p.getByText('已确认清单事项完成',{exact:true}).waitFor();
    await b(/^已完成 1$/).click();
    const completed=p.locator('[data-handover-item]').filter({hasText:itemTitle});
    await shot('已接收事项完成后可恢复',b('恢复待办',completed));
    await click('恢复待办',completed);await p.getByText('已恢复为未完成',{exact:true}).waitFor();
    assert(await b(/^已完成 0$/).isVisible());
  },
  async 账号权限审批({p,go,shot,b,click,device,origin}){
    const {accountFixture}=await import('./responsive-audit-account-fixtures.mjs?approval='+encodeURIComponent(device));
    const writes=[];
    await p.route(origin+'/api/auth/**',async route=>{
      const req=route.request(),url=new URL(req.url());
      const result=await accountFixture({url,method:req.method(),body:req.postDataJSON()});
      if(!result)return route.fallback();
      if(req.method()==='POST'&&url.pathname.includes('/permission-requests/'))writes.push({path:url.pathname,body:req.postDataJSON()});
      await route.fulfill(result);
    });
    await go('/admin/settings','.accounts');await b(/^权限申请/).click();
    const panel=p.locator('.v2-permission-review'),rows=panel.locator('tbody tr'),modal=p.locator('.v2-review-modal');
    await shot('待审批申请',rows);assert.equal(await rows.count(),2);
    const approved=await rows.first().locator('strong').innerText();
    await click('核对并审批',rows.first());await shot('核对完整权限',modal.locator('dl'));
    await click('取消',modal);assert.equal(writes.length,0);
    await click('核对并审批',rows.first());await click('确认批准',modal);
    await rows.filter({hasText:approved}).waitFor({state:'detached'});
    const rejected=await rows.first().locator('strong').innerText();
    await click('拒绝',rows.first());await click('确认拒绝',modal);
    await shot('拒绝原因必填',modal.getByText('请填写拒绝原因'));assert.equal(writes.length,1);
    const reason='隔离验收：请补充申请依据';
    await modal.getByLabel('拒绝原因').fill(reason);await shot('拒绝原因与确认',modal);
    await click('确认拒绝',modal);await panel.getByText('暂无符合条件的申请').waitFor();
    assert.deepEqual(writes,[{path:'/api/auth/permission-requests/audit-request-0/approve',body:{scopes:['110'],reason:''}},
      {path:'/api/auth/permission-requests/audit-request-1/reject',body:{scopes:['110'],reason}}]);
    await panel.locator('.n-select').click();await p.locator('.n-base-select-option').filter({hasText:'全部申请'}).click();
    await rows.filter({hasText:approved}).getByText('已通过',{exact:true}).waitFor();
    await shot('重新读取已通过和已拒绝',rows.filter({hasText:rejected}).getByText('已拒绝',{exact:true}));
    await panel.locator('.n-select').click();await p.locator('.n-base-select-option').filter({hasText:'待审批'}).click();
    const refreshed=p.waitForResponse(r=>r.url().includes('/permission-requests/admin?status=pending'));
    await click('刷新结果',panel);assert.deepEqual((await (await refreshed).json()).data.items,[]);
    await panel.getByText('暂无符合条件的申请').waitFor();
    const tab=b(/^权限申请/);await shot('刷新待审批列表与页签计数',tab);
    assert.equal((await tab.innerText()).trim(),'权限申请','待审批接口及列表均为 0，页签应清除待审批数量');
  },
  async 工作台日程({p,go,shot,b,click}){
    await go('/');await p.locator('.wb-task-row').first().waitFor();
    await shot('首页与阅读进度',p.locator('.wb-identity'));
    await click('天气详情');await shot('天气预报与更新',p.locator('.forecast ul'));await p.keyboard.press('Escape');
    await click(/^消息通知/);await shot('未读消息',p.locator('.notification'));await click('查看全部未读');
    assert.equal(await b('未读').getAttribute('aria-pressed'),'true');await shot('未读筛选',p.locator('.queue-switches'));
    await click('展开月历');await shot('整月日程',p.locator('.planner-head'));await click('上个月');await click('今天');
    await p.getByRole('combobox',{name:'日程楼栋'}).click();await p.locator('.n-base-select-option').filter({hasText:'A楼'}).click();await shot('当日日程与排班',p.locator('.workbench-roster'));
    await click('收起月历');await p.getByLabel('搜索待办').fill('无匹配的隔离记录');await shot('搜索无匹配',p.locator('.wb-todo-panel'));
    await p.getByLabel('搜索待办').fill('');await p.locator('.wb-task-row .record-key').first().click();
    await shot('就地办理',p.locator('.case-panel:visible,[role=dialog]:visible').last());
  },
  async 业务待办({p,go,shot,click}){
    await go('/?view=tasks&scope=A');const group=p.getByRole('region',{name:'重保填写待办',exact:true});
    await shot('业务分组与队列',group.locator('.v2-todo-row'));
    await group.locator('.v2-todo-row').first().click();
    await shot('原单办理',p.locator('.v2-todo-handler .guard-sheet'));await click('关闭待办办理');await p.locator('.v2-todo-handler').waitFor({state:'hidden'});
    await p.locator('.v2-todo-toolbar .n-select').click();await shot('选择业务及顺序',p.locator('.n-base-select-menu'));
  },
  async 交接班审核({p,go,shot,b,click,ok}){
    let mode='failed';
    await p.route('**/api/handover-links',route=>mode==='failed'?route.fulfill({status:503,json:{ok:false,error:'隔离测试：楼栋入口读取失败'}}):mode==='empty'?ok(route,{links:{},scope_options:[]}):route.fallback());
    await go('/?view=handover');await shot('读取失败与重试',b('重新读取'));mode='available';await click('重新读取');
    await shot('授权楼栋选择',b('A楼审核'));await click('A楼审核');await p.waitForURL('**/review-fixture/A');
    mode='empty';await go('/?view=handover');await shot('没有已配置入口',p.getByText('当前授权楼栋暂无已配置的审核链接'));
  },
  async 每日任务({p,go,shot,b,click,patch,ok,origin}){
    await patch('**/api/daily-tasks?*',(data,req)=>({...data,scope:new URL(req.url()).searchParams.get('scope')}));
    await p.route(origin+'/api/repair-management/people?**',r=>ok(r,{people:[{name:'隔离接收员',open_id:'ou_fixture_daily',selectable:true}],total:1}));
    await go('/daily-tasks?scope=H','.ops-desk');await shot('当班任务清单',p.locator('.ops-task-row'));
    await click('发送今日汇总');await shot('选择接收人',p.getByText('接收名单 · 已选 0 人'));assert(await b('确认发送').isDisabled());
    await p.route(origin+'/api/daily-tasks/send',r=>ok(r,{sent_count:1,failed_count:0}));
    await p.locator('#daily-recipients').click();
    await p.getByRole('option',{name:/隔离接收员/}).click();await p.getByText('接收名单 · 已选 1 人').waitFor();
    const sending=p.waitForRequest(req=>req.method()==='POST'&&req.url()===origin+'/api/daily-tasks/send');
    await click('确认发送');const sent=(await sending).postDataJSON();assert.deepEqual(sent.recipient_open_ids,['ou_fixture_daily']);assert.equal(sent.scope,'H');assert(sent.operation_id);
    await shot('模拟发送回执',p.getByText('已成功发送给 1 人。'));
    await click('关闭发送对话框');await click('晨会表格');await shot('晨会预览与环境读数',p.getByLabel('干球温度（℃）'));
    await click('生成表格');await shot('生成失败可重试',p.locator('dialog .ops-alert.is-error'));
    await p.route('**/api/daily-tasks/morning-meeting/generate',route=>ok(route,{generated_at:new Date().toISOString(),download_url:'/api/local-demo/sheet',print_url:'/daily-tasks/morning-meeting/print'}));
    await click('生成表格');await shot('生成结果与打印入口',b('重新生成'));assert(await b('下载').isEnabled());await p.keyboard.press('Escape');
    await click('前一天');assert(await b('发送今日汇总').isDisabled());await shot('历史日期发送锁定',p.locator('.ops-daily-tools'));
  },
  async 演练办理({p,go,shot,b,click,origin,device,patch}){
    await isolatedCollab(p,origin,device,'drills');
    await patch('**/api/drills/bootstrap?*',data=>{
      data.drills[0].configuration.mapping.assessment.score_rows=[{row:9,value_cell:'C9',score_cell:'D9',score:10}];return data;
    });
    await go(drill);await shot('楼栋执行与签名',p.locator('.execution-panel h2'));
    await click('演练评估表');await shot('评估与签名',p.locator('.assessment-fields'));
    await p.locator('.drd-preview summary').click();await shot('工作表预览',p.locator('.drd-preview[open] table'));
    await click('演练记录表');await p.locator('.execution-panel textarea').first().fill('隔离演练现场复核');
    await click('管理员入口');await shot('未保存离开确认',dialog(p));await click('继续编辑',dialog(p));
    await click('保存草稿');await p.getByText(/最近保存/).first().waitFor();
    await go(drill);await p.locator('.scenario-field textarea').waitFor();
    assert.equal(await p.locator('.scenario-field textarea').inputValue(),'隔离演练现场复核');
    await click('管理员入口');await shot('演练配置与发布',p.locator('.configuration-panel'));
    await click('归档演练');await shot('演练归档确认',dialog(p));await click('取消',dialog(p));
    for(const [summary,label] of [['步骤表格映射','步骤与区域映射'],['步骤与签名人数','识别步骤'],['评分行','评分行']]){
      const fold=p.locator('.configuration-panel details').filter({has:p.locator('summary',{hasText:summary})});
      if(!await fold.evaluate(el=>el.open))await fold.locator('summary').click();
      if(summary==='步骤与签名人数')assert.equal(await fold.getByLabel('步骤签名人数').count(),3);
      if(summary==='评分行')assert.equal(await fold.getByLabel('得分单元格').inputValue(),'D9');
      await shot(label,fold);
    }
    await click('上传演练文件');await shot('工作簿上传',p.locator('.upload-panel'));assert(await b('上传并识别').isDisabled());
    await p.locator('.upload-panel input[type=file]').setInputFiles({name:'现场演练.xlsx',mimeType:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',buffer:Buffer.from('isolated')});
    await shot('选中文件与分配楼栋',p.locator('.drd-drop'));
  },
  async 值班演练({p,go,shot,click,ok,origin}){
    const run={id:'local-preview',mode:'preview',status:'success',created_at:new Date().toISOString(),counts:{total:1,create:1,update:0}};
    const rows=[{演练编号:'H-01',文本:'值班交接核对',楼栋:'H'}];
    let previewed=false,syncRequests=0;
    await p.route(origin+'/api/drills/zhihang-h**',route=>{
      const u=new URL(route.request().url());
      if(u.pathname.endsWith('/preview')&&route.request().method()==='POST'){previewed=true;return ok(route,run);}
      if(u.pathname.endsWith('/sync')){syncRequests++;return ok(route,{});}
      if(u.pathname.endsWith('/records'))return ok(route,{rows});
      if(u.pathname.includes('/runs/'))return ok(route,{...run,rows,total:1});
      return ok(route,{enabled:false,account_ready:true,account_error:'',login_name:'隔离验收',table_url:'',latest:previewed?run:null,history:previewed?[run]:[]});
    });
    await go(drill);await click('管理员入口');await click('H楼值班演练');
    await shot('值班同步设置',p.locator('#h-duty-login'));await click('拉取当月预览');await click('拉取预览');
    await shot('预览记录',p.getByLabel('智航演练预览').getByText('值班交接核对',{exact:true}));assert(previewed);await click('同步原表');
    await shot('同步原表确认',p.getByRole('dialog'));await click('取消',p.getByRole('dialog'));
    assert.equal(syncRequests,0);
  },
  async 演练打印({p,go,shot,patch}){
    await p.addInitScript(()=>{window.print=()=>{window.__printRequested=true}});
    for(const sheet of ['record','assessment']){
      const title=sheet==='record'?'演练记录表':'演练评估表';
      await patch('**/api/drills/collab-drill-1/print-model?*sheet='+sheet,data=>{
        data.model.rows[0][0]=title;return data;
      });
      await go('/drill-management/print?scope=A&drill_id=collab-drill-1&sheet='+sheet);
      await p.waitForFunction(()=>window.__printRequested===true);
      await p.emulateMedia({media:'print'});await shot(sheet==='record'?'记录表打印':'评估表打印',p.locator('.print-sheet').getByText(title,{exact:true}));await p.emulateMedia({media:'screen'});
    }
  },
  async 排查管理({p,go,shot,b,click}){
    await riskDetail({p,go});await shot('发布管理详情',p.locator('.rk-brief'));
    await click('更换审核人');await shot('更换审核人',p.locator('.risk-confirm'));await p.locator('.risk-confirm .n-select').click();await shot('可选审核人',p.locator('.risk-confirm .n-base-select-option'));await p.locator('.risk-confirm .n-base-select-option').last().click();await click('取消',p.locator('.risk-confirm'));
    await b(/^删除任务：/).click();await shot('删除任务确认',p.locator('.risk-confirm'));await click('取消',p.locator('.risk-confirm'));
    await b(/^处理 /).first().click();await shot('查看人员反馈',p.locator('.risk-response'));for(const label of ['撤销事项','撤销该人员']){
      await click(label);await shot(label+'原因',p.locator('.risk-confirm'));assert(await b('确认',p.locator('.risk-confirm')).isDisabled());await click('取消',p.locator('.risk-confirm'));
    }
  },
  async 排查反馈({p,go,shot,b,click,origin,device}){
    await isolatedCollab(p,origin,device,'risk-inspections');
    await riskDetail({p,go},'我的排查任务');
    const actionLabel=await b(/^处理 /).first().getAttribute('aria-label');assert(actionLabel);await click(actionLabel);
    await shot('个人填写与附件',p.locator('.risk-response'));
    await click('提交审核');await shot('必填校验与定位',p.locator('.risk-missing-summary'));
    const input=p.locator('.risk-response input:not([type=file]):not([readonly]):not([disabled]),.risk-response textarea:not([readonly]):not([disabled])');
    await input.first().fill('已核对现场情况');await click('提交审核');await shot('提交审核确认',p.locator('.risk-confirm'));await click('取消',p.locator('.risk-confirm'));
    await p.locator('.risk-decision-layer').waitFor({state:'detached'});await p.locator('.risk-response button').first().focus();await p.keyboard.press('Escape');await shot('未保存反馈确认',p.getByRole('dialog',{name:'放弃修改'}));await click('继续填写',p.getByRole('dialog',{name:'放弃修改'}));
    assert.equal(await input.first().inputValue(),'已核对现场情况');
    await click('提交审核');
    const result=p.waitForResponse(r=>r.url().endsWith('/actions')&&r.request().method()==='POST');
    await click('确认',p.locator('.risk-confirm'));const reply=await result,payload=reply.request().postDataJSON();
    assert.equal(payload.action,'submit');assert.equal(payload.values.f1,'已核对现场情况');assert(reply.ok());
    const item=(await reply.json()).data.people[0].items[0];assert.equal(item.status,'submitted');assert.equal(item.id,payload.item_id);
    await riskDetail({p,go},'我的排查任务');await click(actionLabel);
    await shot('重开已提交反馈',p.locator('.risk-response .n-tag').getByText('待审核',{exact:true}));
    assert.equal(await b('提交审核',p.locator('.risk-response')).count(),0);
  },
  async 排查审核({p,go,shot,b,click,patch}){
    let responseDetail;
    await patch('**/api/risk-inspections/'+ '1'.repeat(32)+'*',data=>{data.people[0].items[0].status='submitted';data.people[0].items[0].values={f1:'已核实'};responseDetail=data;return data;});
    await p.route('**/api/risk-inspections/*/people/*/actions',async route=>{
      const body=route.request().postDataJSON(),person=responseDetail.people[0],item=person.items[0];
      assert.equal(body.version,person.version);assert.equal(body.item_id,item.id);assert.equal(body.action,'return');assert.equal(body.reason,'请补充现场资料');
      person.version++;item.status='returned';item.reason=body.reason;await route.fulfill({json:{ok:true,data:responseDetail}});
    });
    await riskDetail({p,go},'我的审核任务');await b(/^处理 /).first().click();
    for(const label of ['审核通过','退回修改']){await click(label);await shot(label+'确认',p.locator('.risk-confirm'));if(label==='退回修改'){assert(await b('确认',p.locator('.risk-confirm')).isDisabled());await p.locator('.risk-confirm').getByRole('textbox').fill('请补充现场资料');await p.locator('.risk-confirm button:enabled').filter({hasText:/^确认$/}).waitFor();await shot('退回原因已填可提交',p.locator('.risk-confirm'));await click('确认',p.locator('.risk-confirm'));await shot('已保存退回结果',p.getByText('请补充现场资料',{exact:true}).first());}else await click('取消',p.locator('.risk-confirm'));}
  },
  async 排查模板({p,go,shot,b,click}){
    await go(risk);await click('基础模板');await shot('基础模板库',p.locator('.history-list article'));
    for(const name of ['导入表格','导入多维表']){await click(name);await shot(name,dialog(p));await click('取消',dialog(p));}
    await click('新建基础模板');await shot('新建基础模板',p.getByLabel('基础模板名称'));
    await b(/新增字段|新建字段|添加字段/).first().click();await shot('字段名称与类型',p.getByLabel('字段库名称'));await click('取消',dialog(p));
    await go(risk);await click('历史模板库');await b('查看 / 编辑').first().click();await shot('历史模板编辑',p.getByLabel('历史模板名称'));
    await go(risk);await click('发布排查');await b('选用并填写').first().click();await shot('发布任务与分派',p.getByRole('heading',{name:'排查事项'}));
  },
  async 重保签名及楼栋清单({p,go,shot,b,click,patch}){
    const sheets=[
      {name:'物资',kind:'materials',title:'防汛物资核对',sections:[{key:'stock',title:'物资清点',columns:[{key:'name',label:'品名'},{key:'quantity',label:'实存数量'}]}]},
      {name:'值班',kind:'duty',title:'当班联络人',duty:{title:'当班安排',columns:[{key:'name',label:'姓名'}]},groups:[{key:'oncall',title:'应急联络'}],group_columns:[{key:'name',label:'联系人'}]},
      {name:'附件',kind:'check',input_mode:'file',title:'楼栋清单附件'}];
    await patch('**/api/critical-guard/bootstrap*',data=>{
      data.catalog.sheets[0].has_signature=true;
      data.catalog.sheets.push(...sheets);data.sheet_types=data.catalog.sheets.map(x=>x.name);return data;
    });
    await patch('**/api/critical-guard/tasks/collab-guard-1*',data=>{
      const original=data.responses[0];
      data.sheet_types.push(...sheets.map(x=>x.name));
      for(const sheet of sheets)data.responses.push({...original,response_id:'local-'+sheet.name,sheet_type:sheet.name,
        cells:{stock:[{name:'抽水泵',quantity:'2'}],duty:[{name:'隔离值班员'}],groups:{oncall:[{name:'隔离工程师'}]}},
        source_file:sheet.input_mode?{file_id:'audit-xlsx',file_name:'现场楼栋清单.xlsx',size:1200,preview_url:'/assets/vnet-logo.png'}:null});
      return data;
    });
    await go(guard);await b(/^检查人签名/).click();
    const drawer=p.getByRole('dialog',{name:'选择检查人签名'});await shot('公司检查人签名',drawer);
    await drawer.locator('.sign-person').filter({hasText:'已有签名'}).first().click();
    await shot('已选公司检查人',drawer.getByText('已选 1 人',{exact:true}).first());
    await b(/临时.*外部人员/,drawer).click();await shot('临时与外部签名',drawer);await click('关闭检查人选择',drawer);
    const form=p.locator('.guard-sheet');
    await click('物资',form);await shot('未保存签名切表确认',dialog(p));await click('确认',dialog(p));
    await shot('物资填写',form.getByLabel('实存数量'));
    await click('值班',form);await shot('值班填写',form.getByLabel('联系人'));
    await click('附件',form);await shot('楼栋清单上传与预览',b('上传楼栋清单',form));
    await form.locator('.gd-attach__thumb').click();await shot('源文件放大查看',p.locator('.gd-lightbox img'));
    await p.waitForFunction(()=>document.querySelector('.gd-lightbox img')?.naturalWidth>0);
    await click('关闭',p.locator('.gd-lightbox'));
  },
  async 重保办理({p,go,shot,b,click,origin,device}){
    await isolatedCollab(p,origin,device,'critical-guard');
    await go(guard);const form=p.locator('.guard-sheet');
    await shot('楼栋任务与检查项',form.locator('.guard-checks'));await b('异常',form).first().click();await form.getByLabel('异常说明').fill('复查泵房水位');
    await shot('检查异常说明',form.getByLabel('异常说明'));
    await click('全部正常',form);await shot('全部正常覆盖确认',dialog(p));await click('取消',dialog(p));
    await click('刷新',p.locator('.guard-station'));await shot('未保存检查离开确认',dialog(p));await click('取消',dialog(p));
    await click('编辑模板',form);const editor=p.getByRole('dialog',{name:'编辑检查模板'});
    await editor.locator('input').first().waitFor();await shot('模板与检查项',editor);await click('添加检查项',editor);await click('关闭',editor);
    await shot('放弃模板修改确认',p.getByRole('dialog',{name:'放弃模板修改？'}));await click('确认',p.getByRole('dialog',{name:'放弃模板修改？'}));
    await click('保存',form);await p.waitForFunction(()=>document.querySelector('.guard-sheet header')&&!document.querySelector('.guard-sheet header').innerText.includes('未保存'));
    await go(guard);await form.getByLabel('异常说明').waitFor();assert.equal(await form.getByLabel('异常说明').inputValue(),'复查泵房水位');
    await shot('保存后的检查结果',form.getByLabel('异常说明'));await click('管理任务');await shot('管理天气与任务',p.getByRole('heading',{name:'天气戒备'}));
    await click('发布任务');await shot('发布任务表单',p.locator('.gd-publish'));
    await b(/^删除 /).first().click();await shot('删除重保任务确认',dialog(p));await click('取消',dialog(p));
  }
};

