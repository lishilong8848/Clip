export async function openWorkspaceRail(page){
  await page.locator('.v2-shell.has-topnav,.v2-shell.is-mobile-workspace').waitFor();
  if(await page.locator('.v2-shell.is-mobile-workspace').count()){
    await page.locator('.v2-mobile-nav').getByRole('button',{name:'我的',exact:true}).click();
    await page.getByRole('button',{name:/^账户菜单：/}).waitFor();return null;
  }
  return page.getByRole('navigation',{name:'功能导航',exact:true});
}
export async function selectWorkspaceAccount(page,label){
  await openWorkspaceRail(page);
  if(await page.locator('.v2-shell.is-mobile-workspace').count()&&label==='个人信息')return;
  if(await page.locator('.v2-shell.is-mobile-workspace').count()&&['系统设置','账号与权限'].includes(label)){await page.getByRole('button',{name:label,exact:true}).click();return;}
  await page.getByRole('button',{name:/^账户菜单：/}).click();
  await page.locator('.n-dropdown-menu').getByText(label,{exact:true}).click();
}
export async function selectWorkspaceSetting(page,label){
  const rail=await openWorkspaceRail(page);
  if(!rail){await page.getByRole('button',{name:/^(系统设置|账号与权限)$/}).click();const picker=page.getByRole('combobox',{name:'设置分类',exact:true});if(await picker.count()){if(await picker.evaluate(e=>e.tagName)==='SELECT')await picker.selectOption({label});else{await picker.click();await page.locator('.n-base-select-menu').getByText(label,{exact:true}).click();}}return;}
  const section=rail.locator('#nav-panel-administration');
  const group=rail.locator('[data-group="administration"]');
  if (await group.getAttribute('data-nav-entry')) {
    if(await group.getAttribute('aria-description')!==label)throw Error('Missing authorized setting '+label);
    await group.click();
    return;
  }
  if(await group.getAttribute('aria-expanded')==='false')await group.click();
  const button=page.getByRole('button',{name:label,exact:true,includeHidden:true});
  const item=section.getByRole('button',{name:label,exact:true});
  const branch=section.locator('.topnav-branch').filter({has:button}).locator('.topnav-subtitle');
  if(await branch.count()&&await branch.getAttribute('aria-expanded')==='false')await branch.click();
  await item.click();
}
export async function openWorkspaceEntry(page,key){
  const mobile=page.locator('.v2-mobile-nav');
  const entry='[data-nav-entry="'+key+'"]';
  if(await mobile.isVisible()){
    await mobile.getByRole('button',{name:key==='home'?'首页':'功能',exact:true}).click();
    if(key!=='home')await page.locator('.v2-functions').locator(entry).click();
    return;
  }
  const nav=page.getByRole('navigation',{name:'功能导航',exact:true});
  const item=nav.locator(entry);
  await item.waitFor({state:'attached'});
  if(!await item.isVisible()){
    const panel=await item.evaluate(el=>el.closest('section').id);
    const group=nav.locator('[aria-controls="'+panel+'"]');
    if(await group.getAttribute('aria-expanded')==='false')await group.click();
    if(!await item.isVisible()){
      const branch=await item.evaluate(el=>el.parentElement.id);
      await nav.locator('[aria-controls="'+branch+'"]').click();
    }
  }
  await item.click();
}
