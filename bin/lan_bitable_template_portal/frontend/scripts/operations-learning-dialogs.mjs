export async function auditSharing(args){
  const {auditSharing:run}=await import('./fixtures/sharing-dialog-audit.mjs');
  return run(args);
}
export async function auditLearning({page,go,capture}){
  const btn=(scope,name)=>scope.getByRole('button',{name,exact:true});
  const modal=page.locator('.lp-modal[role=dialog]');
  const decision=title=>page.getByRole('dialog').filter({has:page.getByRole('button',{name:'确认',exact:true})}).filter({hasText:title}).last();
  async function cancel(key,title){
    const box=decision(title);await capture('lp-confirm-'+key,title,box);await btn(box,'取消').click();await box.waitFor({state:'hidden'});
  }
  async function section(label){
    const nav=page.getByRole('navigation',{name:'学练页面'}),picker=nav.getByRole('combobox');
    if(await picker.count())await picker.selectOption({label});else await btn(nav,label).click();
  }
  await go('/learning&view=practice&tab=today&scope=A');
  const reader=page.getByRole('region',{name:'题单与作答'});await reader.locator('.stem').waitFor();
  await btn(reader,'题面附件 · 冷却水泵巡查示意.svg').click();await modal.getByRole('img').waitFor();await capture('lp-attachment','答题附件预览',modal);await page.keyboard.press('Escape');
  await btn(reader,'反馈本题').click();await modal.getByLabel('问题说明').fill('请确认泵组编号。');await capture('lp-new-issue','从当前题目发起反馈',modal);
  await btn(modal,'提交反馈').click();await modal.getByText('反馈记录已保存。',{exact:true}).waitFor();await page.keyboard.press('Escape');
  await section('题目反馈');await btn(page.getByRole('group',{name:'反馈视图'}),'全部反馈').click();await page.getByRole('button',{name:'处理反馈'}).first().click();await capture('lp-handle-issue','办理已有反馈与关联原题',modal);
  await modal.getByLabel('补充说明或处理结论').fill('正在复核现场铭牌');await btn(modal,'编辑原题').click();await cancel('issue-edit','编辑原题');
  await btn(modal,'关闭').click();await cancel('dirty-editor','关闭窗口');await btn(modal,'关闭').click();await btn(decision('尚有未保存内容'),'确认').click();
  await section('题库管理');await btn(page,'新建题目').click();await modal.getByLabel('题干',{exact:true}).fill('隔离练习：如何核对水泵工况？');await capture('lp-new-question','新建题目、选项与附件',modal);
  await btn(modal,'关闭').click();await btn(decision('尚有未保存内容'),'确认').click();
  await page.getByRole('button',{name:'编辑',exact:true}).first().click();await modal.getByLabel('题干',{exact:true}).waitFor();await capture('lp-edit-question','编辑现有题目',modal);
  await btn(modal,'删除附件').click();await cancel('attachment-delete','删除附件');await page.keyboard.press('Escape');
  await page.locator('.lp-list input[type=checkbox]').first().check();
  await btn(page,'设为已发布').click();await cancel('status','修改题目状态');await btn(page,'设为已删除').click();await cancel('trash','移入回收站');
  await btn(page,'导入题目').click();const q={bank:'written',type:'single',stem:'如何核对冷却水温？',options:[{id:'a',text:'按表复核'},{id:'b',text:'估算'}],correct_option_ids:['a'],status:'draft'};
  await modal.locator('input[type=file]').setInputFiles({name:'隔离题库.json',mimeType:'application/json',buffer:Buffer.from(JSON.stringify({questions:[q]}))});
  await modal.getByText(q.stem,{exact:true}).waitFor();await capture('lp-import','导入题库预检',modal);await btn(modal,'确认导入').click();await cancel('import','确认导入题库');await page.keyboard.press('Escape');
  await section('学习历史');await page.getByRole('button',{name:'删除题单'}).first().click();await cancel('paper-delete','删除已发布题单');
  await section('发布设置');await btn(page,'手动发布题单').click();await cancel('publish','手动发布今日题单');
  await page.getByLabel('每日自动发布',{exact:true}).check();await btn(page,'保存设置').click();await cancel('auto','启用每日自动发布');
  await page.getByRole('button',{name:'刷新当前页面'}).click();await cancel('refresh','刷新设置');await section('学习历史');await cancel('settings-leave','离开发布设置');
  await page.getByLabel('每日自动发布',{exact:true}).uncheck();
  await go('/learning&view=overview&tab=profile&scope=A');await btn(page,'选择人员').click();await modal.getByText('隔离工程师乙',{exact:true}).waitFor();await capture('lp-people','选择人员与画像入口',modal);await page.keyboard.press('Escape');
  await go('/learning&view=practice&tab=completion&scope=A');await page.locator('.completion tbody tr').first().waitFor();
  const row=page.locator('.completion tbody tr').first();await row.locator('summary').filter({hasText:'题单与作答记录'}).click();await row.locator('summary').filter({hasText:/任务/}).click();
  await capture('lp-completion','每日完成明细',page.locator('.completion'));await row.getByRole('button',{name:/题单/}).click();await btn(page,'返回每日完成').waitFor();await capture('lp-completion-paper','每日完成关联题单',reader);await btn(page,'返回每日完成').click();
  await go('/learning&view=practice&tab=today&scope=A');
  const loaded=page.waitForResponse(r=>{const url=new URL(r.url());return url.pathname==='/api/learning/papers'&&url.searchParams.get('mode')==='practice';});
  await btn(reader,'自主练习').click();await loaded;
  const empty=reader.getByText('暂无自主练习，可新建一份练习。',{exact:true});
  await reader.locator('.stem').or(empty).first().waitFor();
  if(await empty.isVisible())await btn(reader,'新练习15题').click();
  await reader.locator('.stem').waitFor();
  await btn(reader,'新练习15题').click();await cancel('practice','开始新练习');
}

