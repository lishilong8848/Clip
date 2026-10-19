import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {connectResponsively,captureResponsively} from './responsive-audit-responsively.mjs';
import {auditPages,auditUrl,layoutEvidence} from './responsive-audit-pages.mjs';

const root=fileURLToPath(new URL('..',import.meta.url)),repo=path.resolve(root,'../../..');
const [phase='before',version='v2',...only]=process.argv.slice(2);
const origin='http://127.0.0.1:'+String(process.env.RESPONSIVE_PORT||14536);
const base=path.join(repo,'output/responsive-audit',phase,version);
await mkdir(base,{recursive:true});
const pages=only.length?auditPages.filter(p=>only.includes(p[0])):auditPages;
const reports=[];
for(const [id,name,logical] of pages){
  const folder=path.join(base,id),url=auditUrl(origin,version,logical);
  console.log('CAPTURE',version,id,url);
  try{await captureResponsively(url,folder);}catch(error){console.error(error);reports.push({id,name,error:error.message});continue;}
  const client=await connectResponsively();
  try{
    // Wait for local async components, not an arbitrary production/network retry.
    await new Promise(resolve=>setTimeout(resolve,500));
    const result=await client.evaluate(`Promise.all(Array.from(document.querySelectorAll('webview')).filter(v=>!/ipad|tablet/i.test(v.id)).map(async v=>({device:v.id,evidence:await Promise.race([v.executeJavaScript(${JSON.stringify('('+layoutEvidence.toString()+')()')}),new Promise(resolve=>setTimeout(()=>resolve({error:'Layout inspection timed out'}),8000))])})))`);
    await writeFile(path.join(folder,'layout.json'),JSON.stringify(result,null,2));
    reports.push({id,name,logical,folder,viewports:result.map(({device,evidence})=>({device,width:evidence.width,height:evidence.height,scrollWidth:evidence.scrollWidth,clipped:evidence.clipped,occluded:evidence.occluded}))});
  }finally{client.close();}
  await writeFile(path.join(base,'summary.json'),JSON.stringify(reports,null,2));
}
await writeFile(path.join(base,'summary.json'),JSON.stringify(reports,null,2));
console.log('AUDIT_CAPTURE_COMPLETE',base);
