import {chromium} from 'playwright';

const URL='http://127.0.0.1:14536/assets/workspace-v2/index.html?scope=A&__page=/repair-management';
const sizes=[[402,874],[440,956],[768,1024],[1440,900]];

const browser=await chromium.launch({headless:true});
for(const [w,h] of sizes){
  const page=await browser.newPage({viewport:{width:w,height:h}});
  await page.goto(URL,{waitUntil:'load',timeout:60000});
  await page.waitForSelector('.v2-repair-command',{timeout:20000}).catch(()=>{});
  await page.waitForTimeout(1800);
  const data=await page.evaluate(() => {
    const vis=function(e){const s=getComputedStyle(e),r=e.getBoundingClientRect();return s.visibility!=='hidden'&&s.display!=='none'&&Number(s.opacity)!==0&&r.width>0&&r.height>0;};
    const rect=function(e){const r=e.getBoundingClientRect();return {x:Math.round(r.x),y:Math.round(r.y),w:Math.round(r.width),h:Math.round(r.height)};};
    const search=[].slice.call(document.querySelectorAll('input')).find(function(i){return i.getAttribute('aria-label')==='搜索维修项目'||/搜索维修名称/.test(i.getAttribute('placeholder')||'');});
    const btn=[].slice.call(document.querySelectorAll('button')).find(function(b){const t=(b.textContent||'').trim();return t.indexOf('新建维修单')>=0||b.getAttribute('aria-label')==='新建维修单';});
    const custom=[].slice.call(document.querySelectorAll('button,[role=button]')).find(function(e){const t=(e.textContent||'').trim();return t.indexOf('自定义布局')>=0||t.indexOf('自定义检修办理布局')>=0;});
    const f2=document.querySelector('.v2-repair-filters');
    const row2=document.querySelector('.v2-command-row');
    const o={};
    if(f2){const cs=getComputedStyle(f2);o.filtersComputed={minWidth:cs.minWidth,flex:cs.flex,flexWrap:cs.flexWrap,display:cs.display,width:cs.width};}
    if(row2){const cs=getComputedStyle(row2);o.rowComputed={flexWrap:cs.flexWrap,display:cs.display,width:cs.width,minWidth:cs.minWidth};}
    const wv=document.querySelector('.workspace-v2')||document.querySelector('html.workspace-v2');
    o.hasWorkspaceV2=!!wv;
    if(f2)o.filtersClasses=f2.className;
    ['search','new','custom'].forEach(function(k){
      const el={search:search,new:btn,custom:custom}[k];
      if(el&&vis(el))o[k]=rect(el);
    });
    const f=document.querySelector('.v2-repair-filters');
    const a=document.querySelector('.v2-repair-actions');
    const row=document.querySelector('.v2-command-row');
    if(f)o.filters=rect(f);
    if(a)o.actions=rect(a);
    if(row)o.commandRow=rect(row);
    return o;
  });
  let overlap='none';
  if(data.search&&data.new){
    if(data.search.x<data.new.x+data.new.w&&data.search.x+data.search.w>data.new.x&&data.search.y<data.new.y+data.new.h&&data.search.y+data.search.h>data.new.y)overlap='OVERLAP';
  }
  console.log('VIEWPORT',w+'x'+h, JSON.stringify(data), 'search_vs_new:', overlap);
  await page.screenshot({path:'D:/桌面/ShiJian_Code/pythonProject/上传维保变更设备调整/output/_verify_repair_'+w+'.png'});
  await page.close();
}
await browser.close();
console.log('DONE');