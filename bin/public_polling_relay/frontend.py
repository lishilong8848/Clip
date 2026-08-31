"""Dependency-free public polling work-order pages.

The relay owns authentication and persistence.  These pages only exchange a
fragment capability for an HttpOnly session cookie and render relay snapshots.
"""

from __future__ import annotations


_STYLE = r"""
*{box-sizing:border-box}html{background:#eef4ff}body{margin:0;min-height:100vh;overflow-x:hidden;background:linear-gradient(145deg,#eef4ff,#f8fbff 46%,#eef5ff);color:#0f274d;font-family:"Microsoft YaHei",system-ui,sans-serif}.shell{width:min(760px,100%);margin:auto;padding:18px 14px calc(34px + env(safe-area-inset-bottom))}.head{position:sticky;top:0;z-index:5;margin-bottom:12px;padding:14px 16px;border:1px solid #cfe0f7;border-radius:20px;background:#fffffff2;box-shadow:0 14px 32px #174f9417;backdrop-filter:blur(12px)}.head span{color:#1663d8;font-size:12px;font-weight:900}.head h1{margin:4px 0;font-size:20px}.head p{margin:0;color:#526780;font-size:12px}.connectivity,.status{margin:10px 0;padding:10px 12px;border-radius:13px;background:#fff;color:#52657f;font-size:12px;font-weight:800}.connectivity.online,.status.success{background:#e9fff3;color:#087443}.connectivity.offline,.status.error{background:#fff1f0;color:#b42318}.connectivity.waiting{background:#fff8dd;color:#855800}.work-orders,.steps{display:grid;gap:10px}.work-order{width:100%;min-height:76px;padding:15px;border:1px solid #cfe0f7;border-radius:17px;background:#fff;text-align:left;color:#0c244d;cursor:pointer;transition:border-color .2s,box-shadow .2s}.work-order strong,.work-order small,.work-order span{display:block}.work-order strong{font-size:16px}.work-order small{margin:5px 0;color:#1663d8;font-weight:900}.work-order span{color:#536a84;font-size:12px}.work-order.active{border:2px solid #1678ff;box-shadow:0 12px 28px #1467e220}.work-order.completed{background:#effbf4;border-color:#b9e4ca}.work-order:disabled{cursor:not-allowed;opacity:.62}.toolbar{display:flex;flex-wrap:wrap;gap:9px;margin:10px 0}.back,.secondary{min-height:44px;padding:9px 12px;border:1px solid #b9ccea;border-radius:12px;background:#fff;color:#0757d7;font-weight:900;cursor:pointer}.secondary.danger{border-color:#f2b8b5;color:#b42318}.secondary:disabled,.back:disabled{cursor:not-allowed;opacity:.55}.rollback{width:100%;margin-top:10px}.step{min-width:0;border:1px solid #d7e4f6;border-radius:18px;padding:13px;background:#ffffffcf;opacity:.66}.step.current{border:2px solid #1678ff;padding:16px;background:#fff;opacity:1;box-shadow:0 18px 40px #1467e22b;transform:scale(1.01)}.step header{display:flex;justify-content:space-between;gap:8px;align-items:center}.step header b{color:#0757d7}.step header span{border-radius:999px;padding:4px 8px;background:#eef5ff;color:#4d6582;font-size:11px;font-weight:900}.step small{display:block;margin-top:7px;color:#0757d7;font-weight:900}.step p{margin:10px 0;line-height:1.7;white-space:pre-wrap;overflow-wrap:anywhere}.checks{display:flex;flex-wrap:wrap;gap:6px}.checks em{border-radius:999px;padding:4px 8px;background:#f0f4f8;color:#526780;font-size:11px;font-style:normal;font-weight:850}.checks em.done{background:#e8fff3;color:#087443}.checks em.waiting{background:#fff4d6;color:#8a4b00}.photos{display:flex;flex-wrap:wrap;gap:7px;margin-top:10px}.photo-thumb{display:block;width:84px;height:64px;border:1px solid #c9daf1;border-radius:10px;object-fit:cover;background:#eef5ff}.photo-upload{display:flex;align-items:center;justify-content:center;min-height:44px;margin-top:10px;border:1px dashed #8db5ea;border-radius:12px;color:#0757d7;background:#f5f9ff;font-size:12px;font-weight:900;cursor:pointer}.photo-upload.disabled{cursor:not-allowed;opacity:.55}.photo-upload input{position:absolute;width:1px;height:1px;opacity:0;pointer-events:none}.action{width:100%;min-height:48px;margin-top:13px;border:0;border-radius:16px;background:linear-gradient(135deg,#1f63ff,#0757d7);color:#fff;font-size:15px;font-weight:950;cursor:pointer}.action:disabled{opacity:.55;cursor:not-allowed}button:focus-visible,a:focus-visible,.photo-upload:focus-within{outline:3px solid #005bff55;outline-offset:2px}.links{margin-top:12px;color:#526780;font-size:11px}.empty{padding:28px 16px;border:1px dashed #b9ccea;border-radius:16px;background:#ffffffb8;text-align:center;color:#526780}.pending{margin-top:7px;color:#855800;font-size:11px;font-weight:800}@media(max-width:520px){.shell{padding:10px 9px calc(26px + env(safe-area-inset-bottom))}.head{border-radius:16px}.step.current{transform:none}.step p{font-size:16px}.toolbar>*{flex:1 1 140px}}@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto!important;transition:none!important}}
"""


_COMMON_SCRIPT = r"""
const API={
  exchange:'/api/v1/link-sessions/exchange',
  session:'/api/v1/work-orders/session',
  commands:'/api/v1/work-orders/commands',
  uploads:'/api/v1/work-orders/uploads',
  photos:'/api/v1/work-orders/photos'
};
const relay={snapshot:null,csrf:sessionStorage.getItem('polling-relay-csrf')||'',busy:false,stopped:false,transportOnline:navigator.onLine,receivedAt:Date.now(),controller:null};
const byId=id=>document.getElementById(id);
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const randomId=()=>crypto.randomUUID?crypto.randomUUID():`${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
function setStatus(text,tone=''){const node=byId('status');if(!node)return;node.textContent=text;node.className=`status ${tone}`}
function apiError(payload,fallback){return new Error(String(payload?.error||payload?.message||fallback))}
async function jsonResponse(response,fallback){if(response.status===204)return null;const payload=await response.json().catch(()=>({}));if(!response.ok||payload.ok===false){const error=apiError(payload,fallback);error.status=response.status;error.code=payload.error_code||'';throw error}return payload.data??payload}
function fragmentCapability(){
  const raw=location.hash.replace(/^#/,'');if(!raw)return null;
  const params=new URLSearchParams(raw);let linkId=params.get('link_id')||'',secret=params.get('secret')||'';
  const compact=params.get('link')||params.get('cap')||'';
  if((!linkId||!secret)&&compact.includes('.')){const offset=compact.indexOf('.');linkId=compact.slice(0,offset);secret=compact.slice(offset+1)}
  return linkId&&secret?{link_id:linkId,secret}:null;
}
function deviceNonce(){let value=sessionStorage.getItem('polling-relay-device')||'';if(!value){value=randomId();sessionStorage.setItem('polling-relay-device',value)}return value}
async function exchangeCapability(){
  const capability=fragmentCapability();if(!capability)return true;
  setStatus('正在验证工单链接...');
  try{
    const response=await fetch(API.exchange,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','Cache-Control':'no-store'},body:JSON.stringify({...capability,device_nonce:deviceNonce()})});
    const data=await jsonResponse(response,'工单链接验证失败');
    relay.csrf=String(data.csrf_token||'');if(!relay.csrf)throw new Error('中继未返回操作校验信息');
    sessionStorage.setItem('polling-relay-csrf',relay.csrf);
    history.replaceState(null,'',`${location.pathname}${location.search}`);
    if(data.snapshot)applySnapshot(data.snapshot);
    return true;
  }catch(error){setStatus(error.message||'工单链接验证失败','error');relay.stopped=true;return false}
}
function expectedVersion(){return Number(relay.snapshot?.authority_version??relay.snapshot?.version??0)}
function authorityOnline(){return Boolean(relay.snapshot?.authority_online)}
function writeAvailable(){return navigator.onLine&&relay.transportOnline&&authorityOnline()&&Boolean(relay.csrf)}
function canWrite(){return !relay.busy&&writeAvailable()}
function renderConnectivity(){
  const node=byId('connectivity');if(!node)return;
  let text='已连接内网工单服务',tone='online';
  if(!navigator.onLine){text='当前设备已离线，页面会在网络恢复后自动重连';tone='offline'}
  else if(!relay.transportOnline){text='公网中继连接中断，正在自动重连';tone='offline'}
  else if(relay.snapshot&&!authorityOnline()){text='内网工单服务离线，仅可查看最后同步状态，所有操作已禁用';tone='waiting'}
  else if(!relay.csrf){text='当前页面只可查看；如需操作，请重新从原始能力链接进入';tone='waiting'}
  node.textContent=text;node.className=`connectivity ${tone}`;
}
function applySnapshot(snapshot){
  if(!snapshot||typeof snapshot!=='object')return;
  const incoming=Number(snapshot.projection_revision||0),current=Number(relay.snapshot?.projection_revision||0);
  if(relay.snapshot&&incoming&&current&&incoming<current)return;
  relay.snapshot=snapshot;relay.receivedAt=Date.now();relay.transportOnline=true;renderConnectivity();renderSnapshot(snapshot);
}
async function readSession({waitSeconds=0,afterRevision=null}={}){
  const revision=afterRevision===null?Number(relay.snapshot?.projection_revision||0):Number(afterRevision||0);
  const url=new URL(API.session,location.origin);url.searchParams.set('after_revision',String(revision));url.searchParams.set('wait_seconds',String(waitSeconds));
  const response=await fetch(`${url.pathname}${url.search}`,{credentials:'same-origin',cache:'no-store',signal:relay.controller?.signal});
  if(response.status===204){relay.transportOnline=true;renderConnectivity();return null}
  const data=await jsonResponse(response,'工单状态读取失败');
  const snapshot=data?.snapshot||data;applySnapshot(snapshot);return snapshot;
}
async function refreshSession(){try{return await readSession({waitSeconds:0,afterRevision:0})}catch(error){relay.transportOnline=false;renderConnectivity();throw error}}
async function sessionLoop(){
  while(!relay.stopped){
    if(!navigator.onLine){relay.transportOnline=false;renderConnectivity();await sleep(1500);continue}
    try{relay.controller=new AbortController();await readSession({waitSeconds:relay.snapshot?20:0});relay.controller=null}
    catch(error){relay.controller=null;if(relay.stopped)return;relay.transportOnline=false;renderConnectivity();if([401,403,404].includes(error.status)){relay.stopped=true;setStatus(error.message||'工单链接已失效','error');return}setStatus(error.message||'连接中断，正在重试','error');await sleep(2500)}
  }
}
async function waitCommand(commandId){
  const deadline=Date.now()+50000;
  while(Date.now()<deadline){
    const response=await fetch(`${API.commands}/${encodeURIComponent(commandId)}`,{credentials:'same-origin',cache:'no-store'});
    const data=await jsonResponse(response,'工单操作状态读取失败');
    const status=String(data.status||'').toLowerCase();
    if(data.snapshot)applySnapshot(data.snapshot);
    if(['completed','succeeded','success','acked','applied'].includes(status))return data;
    if(['failed','rejected','expired','cancelled','retryable_error'].includes(status))throw apiError(data,'工单操作失败');
    await sleep(700);
  }
  return {status:'pending'};
}
async function submitCommand(type,fields={}){
  if(!writeAvailable())throw new Error(authorityOnline()?'当前连接不可写，请等待恢复':'内网工单服务离线，请稍后重试');
  const response=await fetch(API.commands,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':relay.csrf,'Idempotency-Key':randomId()},body:JSON.stringify({type,expected_version:expectedVersion(),...fields})});
  const data=await jsonResponse(response,'工单操作提交失败');
  if(!data.command_id)throw new Error('中继未返回操作编号');
  const result=await waitCommand(data.command_id);await refreshSession();return result;
}
async function runCommand(type,fields,pendingText,successText){
  if(relay.busy)return false;relay.busy=true;renderSnapshot(relay.snapshot);setStatus(pendingText);
  try{const result=await submitCommand(type,fields);const pending=result.status==='pending';setStatus(pending?'操作已排队，等待内网处理':successText,pending?'':'success');return !pending}
  catch(error){if(error.status===503||error.code==='authority_offline')relay.snapshot={...relay.snapshot,authority_online:false};setStatus(error.message||'工单操作失败','error');try{await refreshSession()}catch{}return false}
  finally{relay.busy=false;renderConnectivity();if(relay.snapshot)renderSnapshot(relay.snapshot)}
}
async function sha256Hex(file){const bytes=await file.arrayBuffer(),hash=await crypto.subtle.digest('SHA-256',bytes);return [...new Uint8Array(hash)].map(value=>value.toString(16).padStart(2,'0')).join('')}
async function uploadOnePhoto(stepKey,file){
  if(!writeAvailable())throw new Error('内网工单服务离线，暂不能上传照片');
  if(!String(file.type||'').startsWith('image/'))throw new Error('只能上传图片');
  if(!file.size||file.size>8*1024*1024)throw new Error('单张照片不能超过 8MB');
  const version=expectedVersion(),digest=await sha256Hex(file),headers={'Content-Type':'application/json','X-CSRF-Token':relay.csrf,'Idempotency-Key':randomId()};
  let response=await fetch(API.uploads,{method:'POST',credentials:'same-origin',headers,body:JSON.stringify({step_key:stepKey,expected_version:version,file_name:file.name||'step_photo.jpg',content_type:file.type,size:file.size,sha256:digest})});
  let data=await jsonResponse(response,'照片上传初始化失败'),uploadId=String(data.upload_id||'');if(!uploadId)throw new Error('中继未返回上传编号');
  response=await fetch(`${API.uploads}/${encodeURIComponent(uploadId)}/content`,{method:'PUT',credentials:'same-origin',headers:{'Content-Type':file.type,'X-Content-SHA256':digest,'X-CSRF-Token':relay.csrf},body:file});
  await jsonResponse(response,'照片内容上传失败');
  response=await fetch(`${API.uploads}/${encodeURIComponent(uploadId)}/complete`,{method:'POST',credentials:'same-origin',headers:{'X-CSRF-Token':relay.csrf}});data=await jsonResponse(response,'照片上传确认失败');if(!data.command_id)throw new Error('中继未返回照片关联操作编号');await waitCommand(data.command_id);await refreshSession();
}
async function uploadPhotos(stepKey,files){
  if(relay.busy||!files?.length)return;relay.busy=true;renderSnapshot(relay.snapshot);
  try{let count=0;for(const file of files){setStatus(`正在上传照片 ${count+1}/${files.length}...`);await uploadOnePhoto(stepKey,file);count+=1}setStatus(`已上传 ${count} 张操作照片`,'success')}
  catch(error){if(error.status===503||error.code==='authority_offline')relay.snapshot={...relay.snapshot,authority_online:false};setStatus(error.message||'操作照片上传失败','error');try{await refreshSession()}catch{}}
  finally{relay.busy=false;renderConnectivity();if(relay.snapshot)renderSnapshot(relay.snapshot)}
}
function photoUrl(photo){return `${API.photos}/${encodeURIComponent(String(photo.photo_id||photo.id||''))}`}
function pendingText(snapshot){const items=Array.isArray(snapshot?.pending_commands)?snapshot.pending_commands:[];return items.length?`有 ${items.length} 个操作正在等待内网确认`:''}
async function startRelayPage(){
  window.addEventListener('offline',()=>{relay.transportOnline=false;relay.controller?.abort();renderConnectivity();if(relay.snapshot)renderSnapshot(relay.snapshot)});
  window.addEventListener('online',()=>{relay.transportOnline=true;renderConnectivity()});
  window.addEventListener('pagehide',()=>{relay.stopped=true;relay.controller?.abort()},{once:true});
  renderConnectivity();if(!await exchangeCapability())return;sessionLoop();
}
"""


_OVERVIEW_BODY = r"""
<main class="shell"><section class="head"><span id="role">轮巡工单总览</span><h1 id="title">正在加载...</h1><p id="summary"></p></section><div id="connectivity" class="connectivity waiting" role="status" aria-live="polite">正在连接...</div><div id="status" class="status" role="status" aria-live="polite">正在读取工单状态...</div><button id="retry" class="action" type="button" hidden>重试上传工单附件</button><div class="toolbar"><button id="release" class="secondary danger" type="button" hidden>取消当前选择</button></div><div id="pending" class="pending" aria-live="polite"></div><section id="overview" class="work-orders" aria-label="轮巡工单列表"></section><p class="links">选择一个未完成工单后，将进入独立步骤页面。页面关闭或浏览器返回不会自动释放选择。</p></main>
"""


_OVERVIEW_SCRIPT = r"""
function stepsUrl(runIndex){return `/polling-work-order/steps?run_index=${Number(runIndex)}`}
function orderCard(order){
  const button=document.createElement('button'),title=document.createElement('strong'),label=document.createElement('small'),progress=document.createElement('span');
  button.type='button';button.className=`work-order ${order.state||''}`;button.disabled=!order.selectable||!canWrite();
  title.textContent=`工单 ${Number(order.run_index||0)}`;label.textContent=order.label||`${order.from_unit||''}→${order.to_unit||''}`;
  progress.textContent=`已完成 ${Number(order.completed_steps||0)}/${Number(order.step_count||0)} 步 · ${order.state==='completed'?'已完成':order.state==='active'?'已选择，进入步骤':order.state==='available'?'可选择':'另一工单执行中'}`;
  button.append(title,label,progress);button.onclick=()=>openOrder(order);return button;
}
function renderSnapshot(data){
  if(!data)return;byId('role').textContent=`${data.role_label||data.role||'工单角色'}：${data.assigned_name||data.assigned_person?.name||'未命名'}`;byId('title').textContent=data.title||'轮巡工单';byId('summary').textContent=`${data.sop_name||''} · ${data.work_orders?.length||0} 个工单`;
  const orders=Array.isArray(data.work_orders)?data.work_orders:[];byId('overview').replaceChildren(...(orders.length?orders.map(orderCard):[Object.assign(document.createElement('div'),{className:'empty',textContent:'暂无可显示的工单'})]));
  byId('retry').hidden=data.state!=='upload_pending';byId('retry').disabled=!canWrite();byId('release').hidden=!data.can_release_selection;byId('release').disabled=!canWrite();byId('pending').textContent=pendingText(data);
  const active=orders.find(item=>item.state==='active'),available=orders.some(item=>item.state==='available'),finished=data.state==='completed',stopped=['cancelled','stopped'].includes(data.state);
  if(!relay.busy)setStatus(finished?'全部工单已完成，工单附件已上传。':data.state==='upload_pending'?'步骤已全部完成，正在生成并上传工单附件...':data.last_error||(active?`工单 ${active.run_index} 已被选择，请进入同一工单`:available?'请选择一个未完成工单':stopped?'工单已停止':'暂无可执行工单'),finished?'success':data.last_error||stopped?'error':'');
}
async function openOrder(order){
  if(!order.selectable||!canWrite())return;
  if(order.state==='active'&&Number(relay.snapshot?.current_run_index||0)===Number(order.run_index)){location.assign(stepsUrl(order.run_index));return}
  const ok=await runCommand('activate',{run_index:Number(order.run_index)},`正在进入工单 ${order.run_index}...`,'工单已选择');if(ok)location.assign(stepsUrl(relay.snapshot?.current_run_index||order.run_index));
}
byId('release').onclick=()=>runCommand('release',{run_index:Number(relay.snapshot?.current_run_index||0)},'正在取消当前选择...','已取消当前选择');
byId('retry').onclick=()=>runCommand('retry_attachment',{},'正在重试生成并上传工单附件...','工单附件已上传');
startRelayPage();
"""


_STEPS_BODY = r"""
<main class="shell"><section class="head"><span id="role">轮巡工单步骤</span><h1 id="title">正在加载...</h1><p id="summary"></p></section><div id="connectivity" class="connectivity waiting" role="status" aria-live="polite">正在连接...</div><div id="status" class="status" role="status" aria-live="polite">正在读取工单状态...</div><div class="toolbar"><button id="back" class="back" type="button">← 返回工单总览</button></div><div id="pending" class="pending" aria-live="polite"></div><section id="steps" class="steps" aria-label="当前工单步骤"></section><p class="links">页面只显示上一条、当前条和下一条；浏览器返回不会取消当前选择。</p></main>
"""


_STEPS_SCRIPT = r"""
const runIndex=Number.parseInt(new URLSearchParams(location.search).get('run_index')||'',10),overviewUrl='/polling-work-order';let clock=0;
function remaining(step){return Math.max(0,Math.ceil(Number(step.remaining_seconds||0)-(Date.now()-relay.receivedAt)/1000))}
function goOverview(){location.replace(overviewUrl)}
function goSelected(index){location.replace(`/polling-work-order/steps?run_index=${Number(index)}`)}
function mark(text,done=false,waiting=false){const node=document.createElement('em');node.textContent=text;node.className=done?'done':waiting?'waiting':'';return node}
function stepCard(step){
  const article=document.createElement('article');article.className=`step ${step.position||''}`;
  const head=document.createElement('header'),title=document.createElement('b'),badge=document.createElement('span');title.textContent=`第 ${Number(step.step_index||0)} 步`;badge.textContent=step.position==='current'?'当前步骤':step.position==='previous'?'上一步':'下一步';head.append(title,badge);
  const run=document.createElement('small'),content=document.createElement('p'),checks=document.createElement('div');run.textContent=step.run_label||'';content.textContent=step.content||'';checks.className='checks';
  checks.append(mark(step.operator_required?(step.operator_confirmed?'操作人已确认':'操作人待确认'):'操作人不需要',Boolean(step.operator_confirmed)),mark(step.reviewer_required?(step.reviewer_confirmed?'现场审核人已确认':'现场审核人待确认'):'现场审核人不需要',Boolean(step.reviewer_confirmed)));
  const photos=Array.isArray(step.photos)?step.photos:[];checks.append(mark(photos.length?`操作照片 ${photos.length} 张`:'操作照片待拍',photos.length>0,photos.length===0));const wait=remaining(step);if(step.position==='current'&&wait>0){const timer=mark(`倒计时 ${wait} 秒`,false,true);timer.dataset.countdown='1';checks.append(timer)}article.append(head,run,content,checks);
  if(photos.length){const gallery=document.createElement('div');gallery.className='photos';for(const photo of photos){const link=document.createElement('a'),image=document.createElement('img');link.href=photoUrl(photo);link.target='_blank';link.rel='noopener noreferrer';image.src=photoUrl(photo);image.alt=photo.name||'操作照片';image.className='photo-thumb';image.loading='eager';image.decoding='async';link.append(image);gallery.append(link)}article.append(gallery)}
  const canAddPhoto=step.position==='current'&&!step.reviewer_confirmed&&!(relay.snapshot?.role==='operator'&&step.operator_confirmed);if(canAddPhoto){const upload=document.createElement('label'),input=document.createElement('input');upload.className=`photo-upload ${canWrite()?'':'disabled'}`;upload.textContent=photos.length?'继续拍照/添加多张照片':'拍照/上传操作照片';input.type='file';input.accept='image/*';input.multiple=true;input.setAttribute('capture','environment');input.disabled=!canWrite();input.onchange=()=>uploadPhotos(step.step_key,[...(input.files||[])]);upload.append(input);article.append(upload)}
  if(step.position==='current'){const required=relay.snapshot?.role==='operator'?step.operator_required:step.reviewer_required,done=relay.snapshot?.role==='operator'?step.operator_confirmed:step.reviewer_confirmed,waiting=relay.snapshot?.role==='reviewer'&&step.operator_required&&!step.operator_confirmed,button=document.createElement('button');button.className='action';button.dataset.confirm='1';button.textContent=done?'已确认':waiting?'等待操作人确认':!photos.length?'请先拍照':wait>0?`等待 ${wait} 秒`:`确认当前步骤（${relay.snapshot?.role_label||''}）`;button.disabled=!canWrite()||!required||done||waiting||!photos.length||wait>0;button.onclick=()=>runCommand('confirm',{step_key:step.step_key},'正在确认当前步骤...','当前步骤已确认');article.append(button);
    if(relay.snapshot?.role==='reviewer'&&relay.snapshot?.can_rollback_previous){const rollback=document.createElement('button');rollback.className='secondary danger rollback';rollback.type='button';rollback.textContent='回退上一步';rollback.disabled=!canWrite();rollback.onclick=()=>rollbackStep(step.step_key);article.append(rollback)}}return article;
}
function renderSnapshot(data){
  if(!data)return;const selected=Number(data.current_run_index||0);if(!selected){goOverview();return}if(Number.isInteger(runIndex)&&runIndex>0&&selected!==runIndex){goSelected(selected);return}
  byId('role').textContent=`${data.role_label||data.role||'工单角色'}：${data.assigned_name||data.assigned_person?.name||'未命名'}`;byId('title').textContent=data.title||'轮巡工单';const order=(data.work_orders||[]).find(item=>Number(item.run_index)===selected);byId('summary').textContent=`${data.sop_name||''} · 工单 ${selected} · ${order?.label||''}`;byId('back').textContent=data.can_release_selection?'← 退出当前工单并重新选择':'← 返回工单总览';byId('back').disabled=relay.busy;byId('pending').textContent=pendingText(data);
  const steps=(data.steps||[]).filter(step=>Number(step.run_index)===selected);byId('steps').replaceChildren(...(steps.length?steps.map(stepCard):[Object.assign(document.createElement('div'),{className:'empty',textContent:'当前工单没有可显示的步骤'})]));if(!relay.busy)setStatus(data.last_error||`工单 ${selected} · ${order?.label||''}`,data.last_error?'error':'');
}
function refreshCountdown(){if(!relay.snapshot||relay.busy)return;const step=(relay.snapshot.steps||[]).find(item=>item.position==='current'&&Number(item.run_index)===Number(relay.snapshot.current_run_index));if(!step)return;const wait=remaining(step),timer=document.querySelector('[data-countdown]'),button=document.querySelector('[data-confirm]');if(timer){if(wait>0)timer.textContent=`倒计时 ${wait} 秒`;else timer.remove()}if(!button)return;const required=relay.snapshot.role==='operator'?step.operator_required:step.reviewer_required,done=relay.snapshot.role==='operator'?step.operator_confirmed:step.reviewer_confirmed,waiting=relay.snapshot.role==='reviewer'&&step.operator_required&&!step.operator_confirmed,hasPhoto=(step.photos||[]).length>0;button.textContent=done?'已确认':waiting?'等待操作人确认':!hasPhoto?'请先拍照':wait>0?`等待 ${wait} 秒`:`确认当前步骤（${relay.snapshot.role_label||''}）`;button.disabled=!canWrite()||!required||done||waiting||!hasPhoto||wait>0}
async function rollbackStep(stepKey){if(!canWrite()||relay.snapshot?.role!=='reviewer')return;if(!confirm('确定回退至上一步吗？\n\n回退后，上一步和当前步骤的确认记录、操作照片将被清空；上一步需重新倒计时、拍照并确认。'))return;await runCommand('rollback',{step_key:stepKey},'正在回退上一步...','已回退至上一步，请重新拍照并确认')}
byId('back').onclick=async()=>{if(relay.snapshot?.can_release_selection&&canWrite()){const ok=await runCommand('release',{run_index:Number(relay.snapshot.current_run_index||0)},'正在退出当前工单...','已退出当前工单');if(ok)goOverview()}else goOverview()};
if(!Number.isInteger(runIndex)||runIndex<1||runIndex>6)goOverview();else{clock=window.setInterval(refreshCountdown,1000);window.addEventListener('pagehide',()=>clearInterval(clock),{once:true});startRelayPage()}
"""


def _render(title: str, body: str, script: str) -> str:
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
        '<meta name="referrer" content="no-referrer">'
        f"<title>{title}</title><style>{_STYLE}</style></head><body>{body}"
        f"<script>{_COMMON_SCRIPT}\n{script}</script></body></html>"
    )


def render_polling_work_order_page() -> str:
    return _render("轮巡工单总览", _OVERVIEW_BODY, _OVERVIEW_SCRIPT)


def render_polling_work_order_steps_page() -> str:
    return _render("轮巡工单步骤", _STEPS_BODY, _STEPS_SCRIPT)


# Short aliases for relay application routers.
render_overview_page = render_polling_work_order_page
render_steps_page = render_polling_work_order_steps_page

