import {chromium} from 'playwright';
const URL='http://127.0.0.1:14536/assets/workspace-v2/index.html?scope=A&__page=/repair-management';
const browser=await chromium.launch({headless:true});
const page=await browser.newPage({viewport:{width:402,height:874}});
await page.goto(URL,{waitUntil:'load',timeout:60000});
await page.waitForSelector('.v2-repair-filters',{timeout:20000}).catch(()=>{});
await page.waitForTimeout(1500);
const cdp=await page.context().newCDPSession(page);
await cdp.send('DOM.enable'); await cdp.send('CSS.enable');
const doc=(await cdp.send('DOM.getDocument')).root.nodeId;
const filtNode=(await cdp.send('DOM.querySelector',{nodeId:doc,selector:'.v2-repair-filters'})).nodeId;
const wrapped=await cdp.send('CSS.getMatchedStylesForNode',{nodeId:filtNode});
const out=[];
for(const entry of wrapped.matchedCSSRules||[]){
  const r=entry.rule;
  out.push({sel:r.selectorList?.text||'', origin:entry.origin, css:(r.style?.cssText||'').replace(/\n/g,' '), media:(r.media&&r.media.length?r.media.map(m=>m.text).join(','):'')});
}
const comp=await cdp.send('CSS.getComputedStyleForNode',{nodeId:filtNode});
const cs={};for(const prop of comp.computedStyle||[]) if(/min-width|flex|display|width/.test(prop.name)) cs[prop.name]=prop.value;
console.log(JSON.stringify({computed:cs,matched:out},null,2));
await browser.close();
