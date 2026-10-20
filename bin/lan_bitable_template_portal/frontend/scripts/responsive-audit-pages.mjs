export const auditPages=[
  ['login','登录','/?__audit=login'],
  ['home','个人工作台','/'],
  ['profile','个人信息','/?view=my'],
  ['password','修改密码','/?account=password'],
  ['accounts','账号与权限','/admin/settings'],
  ['events','事件管理','/?mode=events&detail=1&scope=A'],
  ...['maintenance','change','repair','polling','adjust','power'].map((type,i)=>['notice-'+type,['维保通告','变更通告','检修通告','设备轮巡','设备调整','上下电通告'][i],'/workbench-lite?scope=A&work_type='+type]),
  ['sops','SOP 与工单模板','/?view=sops&scope=A'],
  ['mop','维护单管理','/engineer/mop?scope=A'],
  ['repairs','维修项目','/repair-management?scope=A'],
  ['repair-followups','维修跟进','/repair-status?scope=A'],
  ['risk','风险排查','/risk-inspections?scope=A'],
  ['guard','重保管理','/critical-guard?scope=A'],
  ['daily','每日任务','/daily-tasks?scope=A'],
  ['pre-shift','班前检查','/?entry=pre_shift&scope=A'],
  ['cabinet','机柜管理','/cabinet-power?scope=A'],
  ['cabinet-batch','机柜批量待办','/cabinet-power/batches?scope=A'],
  ['water','水耗管理','/water-management?scope=A'],
  ['plan','计划收敛审查','/plan-convergence?scope=A'],
  ['drills','演练管理','/drill-management?scope=A'],
  ['learning','画像学练','/learning?scope=A'],
  ['knowledge','共享知识库','/knowledge-base'],
  ['links','多维表导航','/link-directory'],
  ['life','参考人生指南','/life-guide'],
  ['signatures','签名管理','/signature-management'],
  ['handover','交接班审核','/?view=handover'],
  ['work-handover','人员工作交接','/work-handover?scope=A'],
  ['assistant','灯塔助手','/?view=assistant'],
  ['history','历史记忆','/admin/history-memory'],
];

export function auditUrl(origin,version,logical){
  const url=new URL(logical,origin);
  if(version==='v2'){const pathname=url.pathname;url.pathname='/assets/workspace-v2/index.html';url.searchParams.set('__page',pathname);}
  else url.searchParams.set('frontend','legacy');
  return url.href;
}

export function layoutEvidence(){
  const width=innerWidth,height=innerHeight;
  const visible=e=>{const s=getComputedStyle(e),r=e.getBoundingClientRect();return s.visibility!=='hidden'&&s.display!=='none'&&Number(s.opacity)!==0&&r.width>0&&r.height>0;};
  const label=e=>(e.getAttribute('aria-label')||e.getAttribute('title')||e.innerText||e.name||e.tagName).trim().slice(0,90);
  const selector=e=>e.tagName.toLowerCase()+(e.id?'#'+e.id:'')+(typeof e.className==='string'?'.'+e.className.trim().split(/\s+/).slice(0,3).join('.'):'');
  const rect=e=>{const r=e.getBoundingClientRect();return {x:Math.round(r.x),y:Math.round(r.y),width:Math.round(r.width),height:Math.round(r.height)};};
  const controls=[...document.querySelectorAll('button,input:not([type=hidden]),select,textarea,[role=button],[role=tab]')].filter(visible);
  const clipped=[],occluded=[];
  for(const element of controls){
    const r=element.getBoundingClientRect();
    if(r.bottom<=0||r.top>=height)continue;
    let parent=element.parentElement,scrolled=false;
    while(parent&&parent!==document.body){const s=getComputedStyle(parent);if(/auto|scroll/.test(s.overflowX)&&parent.scrollWidth>parent.clientWidth+2){scrolled=true;break;}parent=parent.parentElement;}
    if(!scrolled&&(r.left < -2||r.right >width+2))clipped.push({label:label(element),selector:selector(element),...rect(element)});
    const x=Math.max(1,Math.min(width-1,r.x+r.width/2)),y=Math.max(1,Math.min(height-1,r.y+r.height/2));
    if(r.left<0||r.right>width||r.top<0||r.bottom>height)continue;
    const top=document.elementFromPoint(x,y);
    if(top&&!element.contains(top)&&!top.contains(element)&&!element.disabled){
      // Elements outside their own scroll pane are not overlapping controls.
      let ancestor=element.parentElement,inside=true;
      while(ancestor&&ancestor!==document.body){const a=ancestor.getBoundingClientRect(),s=getComputedStyle(ancestor);if(/hidden|auto|scroll|clip/.test(s.overflowY)&& (y<a.top||y>a.bottom)){inside=false;break;}ancestor=ancestor.parentElement;}
      if(inside)occluded.push({label:label(element),selector:selector(element),by:selector(top),...rect(element)});
    }
  }
  return {url:location.href,title:document.title,width,height,scrollWidth:document.documentElement.scrollWidth,
    text:document.body.innerText.slice(0,18000),clipped,occluded,
    controls:controls.slice(0,140).map(e=>({label:label(e),selector:selector(e),...rect(e)}))};
}
