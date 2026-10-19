import {chromium} from 'playwright';
const URL='http://127.0.0.1:14536/assets/workspace-v2/index.html?scope=A&__page=/repair-management';
const browser=await chromium.launch({headless:true});
const page=await browser.newPage({viewport:{width:402,height:874}});
await page.goto(URL,{waitUntil:'load',timeout:60000});
await page.waitForSelector('.v2-repair-command',{timeout:20000}).catch(()=>{});
await page.waitForTimeout(1500);
const data=await page.evaluate(()=>{
  const row=document.querySelector('.v2-command-row.v2-repair-command');
  const info=[];
  function walk(el,depth){
    if(!el||depth>3)return;
    const cs=getComputedStyle(el);
    const r=el.getBoundingClientRect();
    info.push({tag:el.tagName,cls:(typeof el.className==='string'?el.className:''),depth,pos:'x'+Math.round(r.x)+' y'+Math.round(r.y)+' '+Math.round(r.width)+'x'+Math.round(r.height),display:cs.display,flex:cs.flex,minW:cs.minWidth,flexWrap:cs.flexWrap,position:cs.position});
    for(const c of Array.from(el.children).slice(0,8))walk(c,depth+1);
  }
  walk(row,0);
  return info;
});
console.log(JSON.stringify(data,null,1));
await browser.close();
