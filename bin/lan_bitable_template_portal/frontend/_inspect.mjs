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
const q={nodeId:doc,selector:'.v2-repair-filters'};
const filtNode=(await cdp.send('DOM.querySelector',q)).nodeId;
const wrapped=await cdp.send('CSS.getMatchedStylesForNode',{nodeId:filtNode});
const out=[];
for(const entry of wrapped.matchedCSSRules||[]){
  const r=entry.rule;
  const txt=[];
  for(const prop of (r.style||{}).properties||[]){
    if(/min-width|flex|display/.test(prop.name)) txt.push(prop.name+'='+prop.value+(prop.important?'!important':''));
  }
  if(txt.length||/repair-filters|command-row/.test(r.selectorList?.text||''))
    out.push({sel:r.selectorList?.text||'', origin:entry.origin, txt});
}
console.log(JSON.stringify({computedMinWidth:(wrapped.inlineStyle||{}).getPropertyValue?.('min-width'), matched:out},null,2));
await browser.close();
