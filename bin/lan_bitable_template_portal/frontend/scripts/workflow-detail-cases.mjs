import assert from 'node:assert/strict';
const dialog=p=>p.getByRole('dialog').last();
const risk='/risk-inspections?scope=A',drill='/drill-management?scope=A',guard='/critical-guard?scope=A';
async function riskDetail({p,go},tab){
  await go(risk);
  if(tab)await p.getByRole('tab',{name:tab,exact:true}).click();
  await p.locator('.risk-task-name').first().click();
  await p.locator('.rk-brief').waitFor();
}
export const detailCases={
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
  async 每日任务({p,go,shot,b,click,patch,ok}){
    await patch('**/api/daily-tasks?*',(data,req)=>({...data,scope:new URL(req.url()).searchParams.get('scope')}));
    await go('/daily-tasks?scope=H');await shot('当班任务清单',p.locator('.ops-task-row'));
    await click('发送今日汇总');await shot('选择接收人',p.getByText('接收名单 · 已选 0 人'));assert(await b('确认发送').isDisabled());
    await p.keyboard.press('Escape');await click('晨会表格');await shot('晨会预览与环境读数',p.getByLabel('干球温度（℃）'));
    await click('生成表格');await shot('生成失败可重试',p.locator('dialog .ops-alert.is-error'));
    await p.route('**/api/daily-tasks/morning-meeting/generate',route=>ok(route,{generated_at:new Date().toISOString(),download_url:'/api/local-demo/sheet',print_url:'/daily-tasks/morning-meeting/print'}));
    await click('生成表格');await shot('生成结果与打印入口',b('重新生成'));assert(await b('下载').isEnabled());await p.keyboard.press('Escape');
    await click('前一天');assert(await b('发送今日汇总').isDisabled());await shot('历史日期发送锁定',p.locator('.ops-daily-tools'));
  },
  async 演练办理({p,go,shot,b,click}){
    await go(drill);await shot('楼栋执行与签名',p.locator('.execution-panel h2'));
    await click('演练评估表');await shot('评估与签名',p.locator('.assessment-fields'));
    await p.locator('.drd-preview summary').click();await shot('工作表预览',p.locator('.drd-preview[open]'));
    await click('演练记录表');await p.locator('.execution-panel textarea').first().fill('隔离演练现场复核');
    await click('管理员入口');await shot('未保存离开确认',dialog(p));await click('继续编辑',dialog(p));
    await click('保存草稿');await p.getByText(/最近保存/).first().waitFor();
    await click('管理员入口');await shot('演练配置与发布',p.locator('.configuration-panel'));
    await click('归档演练');await shot('演练归档确认',dialog(p));await click('取消',dialog(p));
    for(const [i,label] of ['步骤与区域映射','识别步骤','评分行'].entries()){const fold=p.locator('.configuration-panel details').nth(i);if(await fold.count()){await fold.evaluate(el=>{if(!el.open)el.querySelector('summary').click()});await shot(label,fold);}}
    await click('上传演练文件');await shot('工作簿上传',p.locator('.upload-panel'));assert(await b('上传并识别').isDisabled());
    await p.locator('.upload-panel input[type=file]').setInputFiles({name:'现场演练.xlsx',mimeType:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',buffer:Buffer.from('isolated')});
    await shot('选中文件与分配楼栋',p.locator('.drd-drop'));
  },
  async 值班演练({p,go,shot,click,ok}){
    const run={id:'local-preview',mode:'preview',status:'success',created_at:new Date().toISOString(),counts:{total:1,create:1,update:0}};
    const rows=[{drill_no:'H-01',name:'值班交接核对',building:'H'}];
    await p.route('**/api/drills/zhihang-h**',route=>{
      const u=new URL(route.request().url());
      if(u.pathname.endsWith('/records'))return ok(route,{rows});
      if(u.pathname.includes('/runs/'))return ok(route,{...run,rows,total:1});
      return ok(route,{enabled:false,account_ready:true,account_error:'',login_name:'隔离验收',table_url:'',latest:run,history:[run]});
    });
    await go(drill);await click('管理员入口');await click('H楼值班演练');
    await shot('值班同步设置',p.locator('#h-duty-login'));await click('拉取预览');
    await shot('预览记录',p.getByRole('button',{name:'同步原表',exact:true}));await click('同步原表');
    await shot('同步原表确认',p.getByRole('dialog'));await click('取消',p.getByRole('dialog'));
  },
  async 演练打印({p,go,shot}){
    await p.addInitScript(()=>{window.print=()=>{window.__printRequested=true}});
    for(const sheet of ['record','assessment']){
      await go('/drill-management/print?scope=A&drill_id=collab-drill-1&sheet='+sheet);
      await p.emulateMedia({media:'print'});await shot(sheet==='record'?'记录表打印':'评估表打印',p.locator('.print-sheet'));await p.emulateMedia({media:'screen'});
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
  async 排查反馈({p,go,shot,b,click}){
    await riskDetail({p,go},'我的排查任务');await b(/^处理 /).first().click();await shot('个人填写与附件',p.locator('.risk-response'));
    await click('提交审核');await shot('必填校验与定位',p.locator('.risk-missing-summary'));
    const input=p.locator('.risk-response input:not([type=file]):not([readonly]):not([disabled]),.risk-response textarea:not([readonly]):not([disabled])');
    await input.first().fill('已核对现场情况');await click('提交审核');await shot('提交审核确认',p.locator('.risk-confirm'));await click('取消',p.locator('.risk-confirm'));
    await p.locator('.risk-decision-layer').waitFor({state:'detached'});await p.locator('.risk-response button').first().focus();await p.keyboard.press('Escape');await shot('未保存反馈确认',p.getByRole('dialog',{name:'放弃修改'}));await click('继续填写',p.getByRole('dialog',{name:'放弃修改'}));
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
    await b(/临时.*外部人员/,drawer).click();await shot('临时与外部签名',drawer);await click('关闭检查人选择',drawer);
    const form=p.locator('.guard-sheet');
    for(const [sheet,field] of [['物资','实存数量'],['值班','联系人']]){await click(sheet,form);await shot(sheet+'填写',form.getByLabel(field));}
    await click('附件',form);await shot('楼栋清单上传与预览',b('上传楼栋清单',form));
    await form.locator('.gd-attach__thumb').click();await shot('源文件放大查看',p.locator('.gd-lightbox img'));
    await click('关闭',p.locator('.gd-lightbox'));
  },
  async 重保办理({p,go,shot,b,click}){
    await go(guard);const form=p.locator('.guard-sheet');
    await shot('楼栋任务与检查项',form.locator('.guard-checks'));await b('异常',form).first().click();await form.getByLabel('异常说明').fill('复查泵房水位');
    await shot('检查异常说明',form.getByLabel('异常说明'));
    await click('全部正常',form);await shot('全部正常覆盖确认',dialog(p));await click('取消',dialog(p));
    await click('刷新',p.locator('.guard-station'));await shot('未保存检查离开确认',dialog(p));await click('取消',dialog(p));
    await click('编辑模板',form);const editor=p.getByRole('dialog',{name:'编辑检查模板'});
    await editor.locator('input').first().waitFor();await shot('模板与检查项',editor);await click('添加检查项',editor);await click('关闭',editor);
    await shot('放弃模板修改确认',p.getByRole('dialog',{name:'放弃模板修改？'}));await click('确认',p.getByRole('dialog',{name:'放弃模板修改？'}));
    await click('保存',form);await p.waitForFunction(()=>document.querySelector('.guard-sheet header')&&!document.querySelector('.guard-sheet header').innerText.includes('未保存'));
    await shot('保存后的检查结果',form.locator('header'));await click('管理任务');await shot('管理天气与任务',p.getByRole('heading',{name:'天气戒备'}));
    await click('发布任务');await shot('发布任务表单',p.locator('.gd-publish'));
    await b(/^删除 /).first().click();await shot('删除重保任务确认',dialog(p));await click('取消',dialog(p));
  }
};

