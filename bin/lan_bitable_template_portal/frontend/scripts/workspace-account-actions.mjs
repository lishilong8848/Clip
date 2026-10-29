export async function openWorkspaceRail(page){
  await page.locator('.v2-shell.has-rail,.v2-shell.is-mobile-workspace').waitFor();
  const rail=page.locator('.v2-rail');
  if(await page.locator('.v2-shell.is-mobile-workspace').count()){await page.locator('.v2-mobile-nav').getByRole('button',{name:'我的',exact:true}).click();await page.getByRole('button',{name:/^账户菜单：/}).waitFor();return null;}
  else if(await page.evaluate(()=>matchMedia('(hover:hover)').matches))await page.locator('.v2-rail-brand').hover();else if(await rail.getAttribute('data-collapsed')==='true')await rail.getByRole('button',{name:'展开导航',exact:true}).click();
  await page.waitForFunction(()=>{const el=document.querySelector('.v2-rail'),rect=el?.getBoundingClientRect();return el?.getAttribute('data-collapsed')==='false'&&Math.round(rect.width)===240&&Math.round(rect.x)===0;});
  return rail;
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
  const item=rail.getByRole('menuitem',{name:label,exact:true});
  if(!await item.isVisible())await rail.getByRole('menuitem',{name:'系统设置',exact:true}).click();
  await item.click();
}
