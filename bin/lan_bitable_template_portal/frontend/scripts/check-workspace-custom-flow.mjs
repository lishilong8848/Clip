import assert from 'node:assert/strict';
import path from 'node:path';

export async function checkCustomization(page,output){
  await page.setViewportSize({width:1440,height:1000});
  await page.evaluate(()=>window.scrollTo(0,0));
  const grid=page.locator('.v2-widget-grid'),count=await grid.locator('.grid-stack-item').count();
  const customize=page.getByRole('button',{name:'自定义工作台',exact:true});
  const buttonSize=await customize.boundingBox();assert(buttonSize.width<110&&buttonSize.height<=34,'customize stays a compact secondary control');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  await page.getByRole('button',{name:'移除常用办理',exact:true}).click();
  await page.locator('.v2-widget[aria-label="常用办理"]').waitFor({state:'detached'});
  assert.equal(await grid.locator('.grid-stack-item').count(),count-1);
  await page.waitForFunction(()=>document.querySelector('.v2-widget[aria-label="收敛核对 · 需关注"]').parentElement.getAttribute('gs-y')===document.querySelector('.v2-widget[aria-label="待处理事项"]').parentElement.getAttribute('gs-y'));
  await page.getByRole('menuitem',{name:'事件管理',exact:true}).click();
  await page.getByText('布局尚未保存',{exact:true}).waitFor();
  await page.getByRole('button',{name:'继续编辑',exact:true}).click();
  await page.locator('.v2-dashboard-actions').getByRole('button',{name:'取消',exact:true}).click();
  assert.equal(await grid.locator('.grid-stack-item').count(),count,'cancel restores deleted components');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  await page.getByRole('button',{name:'添加组件',exact:true}).click();
  await page.locator('.v2-library-item').filter({hasText:'通告分布'}).click();
  const editor=page.locator('.v2-widget-editor');
  await editor.getByPlaceholder('请输入组件名称').fill('我的通告分布');
  await editor.locator('.n-form-item').filter({hasText:'组件宽度'}).locator('.n-select').click();
  await page.locator('.n-base-select-option').getByText('整行',{exact:true}).click();
  await editor.locator('.n-form-item').filter({hasText:'展示方式'}).locator('.n-select').click();
  await page.locator('.n-base-select-option').getByText('环形图',{exact:true}).click();
  await editor.getByRole('button',{name:'应用设置',exact:true}).click();
  await editor.waitFor({state:'hidden'});
  const chart=page.locator('.v2-widget[aria-label="我的通告分布"]');
  await chart.scrollIntoViewIfNeeded();await chart.locator('.v2-chart-canvas svg path').first().waitFor();
  assert.equal(await chart.locator('xpath=..').getAttribute('gs-w'),'12');
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  await page.getByRole('button',{name:'自定义工作台',exact:true}).waitFor();
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:ou_v2_fixture')));
  assert.equal(saved.widgets.length,count+1);assert.equal(saved.widgets.find(w=>w.title==='我的通告分布').display,'donut');
  await page.reload();await page.getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
  await chart.scrollIntoViewIfNeeded();await chart.locator('.v2-chart-canvas svg path').first().waitFor();
  assert.equal(await grid.locator('.grid-stack-item').count(),count+1,'layout survives reload');
  const key=await chart.locator('xpath=..').getAttribute('data-widget-id');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  await chart.scrollIntoViewIfNeeded();
  const resize=chart.locator('xpath=..').locator('.ui-resizable-se');
  const before=Number(await chart.locator('xpath=..').getAttribute('gs-h'));
  const box=await resize.boundingBox();assert(box,'resize handle is available');
  await page.mouse.move(box.x+box.width/2,box.y+box.height/2);await page.mouse.down();await page.mouse.move(box.x+box.width/2,box.y+box.height/2+95,{steps:12});await page.mouse.up();
  await page.waitForFunction(({key,before})=>Number(document.querySelector(`[data-widget-id="${key}"]`).getAttribute('gs-h'))>before,{key,before});
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  const afterResize=await page.evaluate(()=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:ou_v2_fixture')));
  assert(afterResize.widgets.find(w=>w.id===key).h>before,'resize is persisted');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await chart.scrollIntoViewIfNeeded();
  const shrinkBox=await resize.boundingBox();
  await page.mouse.move(shrinkBox.x+shrinkBox.width/2,shrinkBox.y+shrinkBox.height/2);await page.mouse.down();await page.mouse.move(shrinkBox.x+shrinkBox.width/2,shrinkBox.y-350,{steps:15});await page.mouse.up();
  await page.waitForFunction(key=>Number(document.querySelector(`[data-widget-id="${key}"]`).getAttribute('gs-h'))===3,key);
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  assert.equal(await page.evaluate(key=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:ou_v2_fixture')).widgets.find(w=>w.id===key).h,key),3,'minimum size is saved even when the grid omits default dimensions');
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  await page.evaluate(()=>window.scrollTo(0,0));
  const quick=page.locator('.grid-stack-item[data-widget-kind="shortcuts"]'),dragKey=await quick.getAttribute('data-widget-id');
  const handle=page.getByRole('button',{name:'拖动常用办理',exact:true}),handleBox=await handle.boundingBox();
  const oldY=Number(await quick.getAttribute('gs-y'));
  await page.mouse.move(handleBox.x+10,handleBox.y+10);await page.mouse.down();await page.mouse.move(handleBox.x+10,handleBox.y+330,{steps:20});
  await page.screenshot({path:path.join(output,'custom-drag-active.png')});
  await page.mouse.up();
  await page.waitForFunction(({key,oldY})=>Number(document.querySelector(`[data-widget-id="${key}"]`).getAttribute('gs-y'))!==oldY,{key:dragKey,oldY});
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  const packed=await page.evaluate(()=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:ou_v2_fixture')).widgets);
  for(const a of packed)for(const b of packed)if(a.id!==b.id)assert(a.x+a.w<=b.x||b.x+b.w<=a.x||a.y+a.h<=b.y||b.y+b.h<=a.y,'automatic packing must not overlap components');
  await page.setViewportSize({width:390,height:844});
  await page.locator('.v2-edit-banner').waitFor({state:'hidden'});
  await page.locator('.n-message').first().waitFor({state:'hidden'});
  await page.waitForFunction(()=>[...document.querySelectorAll('.grid-stack-item')].every(el=>{const b=el.getBoundingClientRect();return b.left>=-1&&b.right<=innerWidth+1;}));
  assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:ou_v2_fixture')).widgets),packed,'responsive arrangement does not replace the saved desktop layout');
  const blankSpace=await page.locator('.v2-widget-body').evaluateAll(items=>items.map(el=>el.clientHeight-el.firstElementChild.getBoundingClientRect().height));
  assert(blankSpace.every(value=>value<30),'mobile panels use content height rather than desktop blank space');
  await page.screenshot({path:path.join(output,'custom-dashboard-mobile.png'),fullPage:true});
  await page.setViewportSize({width:1440,height:1000});
  await page.waitForFunction(()=>document.querySelector('.v2-widget-grid').gridstack?.getColumn()===12);
  const actual=await grid.evaluate(el=>el.gridstack.save(false,false,undefined,12));
  for(const item of packed){const p=actual.find(w=>w.id===item.id),restored={...p,w:p.w??p.minW??1,h:p.h??p.minH??1};for(const k of ['x','y','w','h'])assert.equal(restored[k]??0,item[k],`desktop ${k} restores after mobile stacking`);}
  await page.waitForFunction(()=>[...document.querySelectorAll('.grid-stack-item')].every(el=>{const b=el.getBoundingClientRect();return b.left>=-1&&b.right<=innerWidth+1;}));
  await page.screenshot({path:path.join(output,'custom-dashboard-desktop.png'),fullPage:true});
  await page.setViewportSize({width:1440,height:600});
  const sidebar=page.locator('.v2-sidebar'),main=page.locator('.v2-main');
  await main.evaluate(el=>el.scrollTop=200);
  const sidebarTop=(await sidebar.boundingBox()).y;assert.equal(await page.evaluate(()=>window.scrollY),0,'desktop content scrolls inside the right pane');
  await sidebar.hover();await page.mouse.wheel(0,1600);
  await page.waitForFunction(()=>document.querySelector('.v2-sidebar').scrollTop>0);
  assert.equal((await sidebar.boundingBox()).y,sidebarTop,'left workspace stays fixed');
  assert.equal(await main.evaluate(el=>el.scrollTop),200,'sidebar wheel must not scroll the right pane');
  await main.hover({position:{x:15,y:130}});await page.mouse.wheel(0,400);
  await page.waitForFunction(()=>document.querySelector('.v2-main').scrollTop>200);
  assert.equal(await page.evaluate(()=>window.scrollY),0);
  await page.setViewportSize({width:1440,height:1000});
  await main.evaluate(el=>el.scrollTop=0);
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'恢复默认',exact:true}).click();
  await page.locator('.n-dialog').getByRole('button',{name:'恢复默认',exact:true}).click();
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  await page.waitForFunction(count=>document.querySelectorAll('.v2-widget-grid>.grid-stack-item').length===count,count);
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  for(const name of ['待办时间线','学练进度环','水耗指标卡']){
    await page.getByRole('button',{name:'添加组件',exact:true}).click();
    await page.locator('.v2-motion-template').filter({hasText:name}).click();
    if(name==='待办时间线'){await editor.locator('.n-form-item').filter({hasText:'待办业务'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText('维保通告',{exact:true}).click();}
    await editor.getByRole('button',{name:'应用设置',exact:true}).click();await editor.waitFor({state:'hidden'});
    const panel=page.locator(`.v2-widget[aria-label="${name}"]`);await panel.scrollIntoViewIfNeeded();
    if(name==='待办时间线')await panel.locator('.v2-task-timeline .v2-task-row').first().waitFor();
    if(name==='学练进度环'){await panel.locator('.v2-progress-label').getByText('40%',{exact:true}).waitFor();assert.equal(await panel.locator('.n-progress').count(),1,'one progress display, no duplicate bar');}
    if(name==='水耗指标卡'){await panel.locator('.v2-metric-tiles').waitFor();assert.equal(await panel.locator('.v2-metric-tiles>.n-button').count(),3);assert.equal(await panel.locator('button button').count(),0);}
  }
  await page.getByRole('button',{name:'保存布局',exact:true}).click();
  await page.reload();await page.getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
  assert.equal(await page.locator('.v2-widget').count(),count+3,'motion template selection persists');
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:width<700?844:1000});
    const panel=page.locator('.v2-widget[aria-label="学练进度环"]');await panel.scrollIntoViewIfNeeded();await panel.getByText('40%',{exact:true}).waitFor();
    await page.locator('.v2-widget[aria-label="水耗指标卡"]').scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await page.screenshot({path:path.join(output,`motion-blocks-widgets-${width}.png`),fullPage:true});
  }
  await page.setViewportSize({width:1440,height:1000});await main.evaluate(el=>el.scrollTop=0);
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'恢复默认',exact:true}).click();await page.locator('.n-dialog').getByRole('button',{name:'恢复默认',exact:true}).click();await page.getByRole('button',{name:'保存布局',exact:true}).click();
  await page.waitForFunction(count=>document.querySelectorAll('.v2-widget-grid>.grid-stack-item').length===count,count);
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  for(const name of ['通告占比环','通告横向对比','通告分类饼图','事件面积趋势','事件折线趋势','楼栋水耗对比','今日答题构成']){
    await page.getByRole('button',{name:'添加组件',exact:true}).click();
    await page.getByRole('textbox',{name:'搜索组件'}).fill(name).catch(async error=>{await page.screenshot({path:path.join(output,'vipboard-library-failure.png'),fullPage:true});console.error(await page.locator('.v2-widget-library').ariaSnapshot());throw error;});
    await page.locator('.v2-chart-template').filter({hasText:name}).click();
    await editor.getByRole('button',{name:'应用设置',exact:true}).click();await editor.waitFor({state:'hidden'});
    const panel=page.locator(`.v2-widget[aria-label="${name}"]`);await panel.scrollIntoViewIfNeeded();await panel.locator('.v2-chart-canvas svg').first().waitFor();
  }
  await page.getByRole('button',{name:'保存布局',exact:true}).click();await page.reload();await page.getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
  assert.equal(await page.locator('.v2-widget').count(),count+7,'all chart templates persist across reload');
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:1000});
    for(const name of ['通告占比环','通告横向对比','通告分类饼图','事件面积趋势','事件折线趋势','楼栋水耗对比','今日答题构成']){
      const panel=page.locator(`.v2-widget[aria-label="${name}"]`);await panel.scrollIntoViewIfNeeded();
      const plot=panel.locator('.v2-motion-chart[aria-busy="false"] .v2-chart-canvas[aria-hidden="false"] svg');await plot.waitFor();await plot.locator('path').first().waitFor({state:'attached'});
    }
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await page.screenshot({path:path.join(output,`vipboard-widgets-${width}.png`),fullPage:true});
  }
  await page.setViewportSize({width:1440,height:1000});await main.evaluate(el=>el.scrollTop=0);
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();await page.getByRole('button',{name:'恢复默认',exact:true}).click();await page.locator('.n-dialog').getByRole('button',{name:'恢复默认',exact:true}).click();await page.getByRole('button',{name:'保存布局',exact:true}).click();
  await page.waitForFunction(count=>document.querySelectorAll('.v2-widget-grid>.grid-stack-item').length===count,count);
  console.log('PASS: add/edit/remove/cancel, navigation guard, persisted sizes, drag, seven chart templates, desktop/mobile and role-default restore.');
}
