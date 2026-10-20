import {pathToFileURL} from 'node:url';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';

export async function connectResponsively(targetId){
  const port=process.env.RESPONSIVELY_PORT||9224;
  const targets=await(await fetch('http://127.0.0.1:'+port+'/json/list')).json();
  const app=targets.find(t=>targetId?t.id===targetId:t.title==='Responsively App');
  if(!app)throw new Error('Responsively App must be running with local debugging on '+port);
  const ws=new WebSocket(app.webSocketDebuggerUrl),pending=new Map();let seq=0;
  await new Promise((resolve,reject)=>{ws.addEventListener('open',resolve,{once:true});ws.addEventListener('error',reject,{once:true});});
  ws.addEventListener('message',e=>{const result=JSON.parse(e.data),p=pending.get(result.id);if(!p)return;pending.delete(result.id);clearTimeout(p.timer);result.error?p.reject(new Error(result.error.message)):p.resolve(result.result);});
  const command=(method,params={})=>new Promise((resolve,reject)=>{const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(new Error('Responsively command timed out: '+method+' '+String(params.expression||'').slice(0,100)));},30000);pending.set(id,{resolve,reject,timer});ws.send(JSON.stringify({id,method,params}));});
  const evaluate=async expression=>{const result=await command('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(result.exceptionDetails)throw new Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);return result.result.value;};
  return {evaluate,command,close:()=>ws.close()};
}

async function connectPreview(width){
  const targets=await(await fetch('http://127.0.0.1:'+(process.env.RESPONSIVELY_PORT||9224)+'/json/list')).json();
  for(const target of targets.filter(target=>target.type==='webview')){
    const client=await connectResponsively(target.id);
    let matched=false;
    try{matched=await client.evaluate('innerWidth')===width;if(matched)return client;}
    finally{if(!matched)client.close();}
  }
  throw new Error('Responsively preview not found at width '+width);
}

export async function captureResponsively(url,output,{navigate=true}={}){
  const client=await connectResponsively();
  try{
    await mkdir(output,{recursive:true});
    await client.evaluate(`{window.__lighthouseAuditSizes?.forEach(o=>o.disconnect());window.__lighthouseAuditSizes=[];const sizes={'iPhone 17 Pro':[402,874],'iPhone 17 Pro Max':[440,956],'MacBook Pro':[1440,900]};for(const view of document.querySelectorAll('webview')){const size=sizes[view.id];if(size){view.style.width=size[0]+'px';view.style.height=size[1]+'px';}}true;}`);
    const views=await client.evaluate(`Array.from(document.querySelectorAll('webview'),v=>({id:v.id,width:parseInt(v.style.width),height:parseInt(v.style.height)})).filter(v=>!/ipad|tablet/i.test(v.id))`);
    if(navigate){
      // Let Electron own navigation; direct CDP navigation can detach its webview target.
      await client.evaluate(`document.querySelector('webview').loadURL(${JSON.stringify(url)}).catch(error=>{if(!error.message.includes('ERR_ABORTED'))throw error;})`);
      await new Promise(resolve=>setTimeout(resolve,1500));
    }
    const records=[];
    for(const view of views){
      const element=`document.getElementById(${JSON.stringify(view.id)})`;
      await client.evaluate(`${element}.scrollIntoView({block:'center',inline:'center'})`);
      await client.evaluate(`{const v=${element};v.style.width=${JSON.stringify(view.width+'px')};v.style.height=${JSON.stringify(view.height+'px')};true;}`);
      await new Promise(resolve=>setTimeout(resolve,150));
      const preview=await connectPreview(view.width);
      try{
      const inspect=`JSON.stringify({url:location.href,width:innerWidth,height:innerHeight,text:document.body.innerText,scrollWidth:document.documentElement.scrollWidth,loading:/正在打开新版工作台|正在打开页面|正在检查登录状态|正在读取|正在加载|正在获取|正在同步/.test(document.body.innerText)||Array.from(document.querySelectorAll('.n-skeleton,.v2-notice-skeleton,.v2-repair-skeleton')).some(e=>{const r=e.getBoundingClientRect();return r.width>0&&r.height>0&&r.top<innerHeight&&r.bottom>0})})`;
      let state;
      for(let attempt=0;attempt<60;attempt++){
        if(await client.evaluate(`${element}.isLoading()`)){await new Promise(resolve=>setTimeout(resolve,250));continue;}
        await client.evaluate(`{const v=${element};v.style.width=${JSON.stringify(view.width+'px')};v.style.height=${JSON.stringify(view.height+'px')};true;}`);
        await new Promise(resolve=>setTimeout(resolve,100));
        state=JSON.parse(await preview.evaluate(inspect));
        const actual=new URL(state.url), expected=new URL(url);
        if(!state.loading&&actual.origin===expected.origin&&actual.pathname===expected.pathname&&[...expected.searchParams].every(([key,value])=>actual.searchParams.get(key)===value))break;
        await new Promise(resolve=>setTimeout(resolve,250));
      }
      if(!state||state.loading)throw new Error('Page is still loading: '+view.id);
      if(new URL(state.url).pathname!==new URL(url).pathname||![...new URL(url).searchParams].every(([key,value])=>new URL(state.url).searchParams.get(key)===value))throw new Error('Page did not reach the requested route: '+view.id+' '+state.url);
      if(state.width!==view.width||state.height!==view.height)throw new Error('Unexpected viewport: '+view.id+' '+state.width+'x'+state.height);
      await new Promise(resolve=>setTimeout(resolve,400));
      const capture=await preview.command('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});
      const data='data:image/png;base64,'+capture.data;
      if(!data.startsWith('data:image/png;base64,')||data.length<1000)throw new Error('Responsively returned an empty screenshot: '+view.id);
      const file=path.join(output,view.id.replace(/[^a-z0-9_-]/gi,'_')+'-'+state.width+'x'+state.height+'.png');
      await writeFile(file,Buffer.from(data.split(',')[1],'base64'));
      records.push({device:view.id,screenshot:file,...state});
      }finally{preview.close();}
    }
    await writeFile(path.join(output,'preview.json'),JSON.stringify(records,null,2));
    return records;
  }finally{client.close();}
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  const [mode,...args]=process.argv.slice(2),client=await connectResponsively();
  try{
    if(mode==='inspect')console.log(await client.evaluate('JSON.stringify({text:document.body.innerText,views:Array.from(document.querySelectorAll("webview"),v=>({id:v.id,width:v.style.width,height:v.style.height})),buttons:Array.from(document.querySelectorAll("button"),b=>({text:b.textContent,title:b.title,aria:b.getAttribute("aria-label")}))})'));
    else if(mode==='setup'){
      console.log(await client.evaluate(`JSON.stringify(Array.from(document.querySelectorAll('webview'),v=>{const sizes={'iPhone 17 Pro':[402,874],'iPhone 17 Pro Max':[440,956],'iPad':[768,1024],'MacBook Pro':[1440,900]};const size=sizes[v.id];if(size){v.style.width=size[0]+'px';v.style.height=size[1]+'px';}return {id:v.id,width:v.style.width,height:v.style.height};}))`));
    }else if(mode==='eval')console.log(await client.evaluate(args.join(' ')));
    else if(mode==='capture'){client.close();console.log((await captureResponsively(args[0],args[1],{navigate:args[2]!=='current'})).map(({text,...record})=>record));}
    else throw new Error('Usage: inspect | setup | eval <expression> | capture <url> <folder> [current]');
  }finally{client.close();}
}
