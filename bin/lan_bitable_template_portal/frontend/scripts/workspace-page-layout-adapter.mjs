import {parse} from '@vue/compiler-sfc';

const cls=node=>(node.props?.find(p=>p.type===6&&p.name==='class')?.value?.content||'').split(/\s+/);
const has=(node,value)=>cls(node).includes(value)||node.tag===value;
const plans={
  'RepairWorkspace.vue':[{container:'v2-repair-body',id:'repair',label:'检修办理',blocks:[['records','维修项目',['v2-repair-list'],12,10],['editor','维修单与跟进',['Transition'],12,12,'r.projectDrawerOpen']]}],
  'WaterWorkspace.vue':[{container:'water-work-area',id:'water-records',label:'水耗办理',blocks:[['records','水耗记录',['water-records-area'],12,10],['editor','水耗填写',['WaterRecordEditor'],12,12,'editorOpen&&!mobile']]}],
  'NoticeSopManager.vue':[{container:'v2-sop-layout',id:'sops',label:'SOP模板',blocks:[['templates','SOP列表',['aside'],4,9],['editor','SOP步骤与附件',['NForm','NEmpty'],8,12]]}],
  'MopBindingWorkspace.vue':[{container:'mop-layout',id:'mop',label:'维护单办理',blocks:[['notices','维保通告',['MopNoticeList'],4,10],['binding','维护单选择与办理',['MopBindingPanel'],8,12]]}],
  'KnowledgeBasePage.vue':[{container:'kb-layout',id:'knowledge',label:'共享知识库',blocks:[['files','文档列表',['kb-pane'],12,10],['detail','文档详情',['kb-detail'],12,10,'!!detail']]}],
  'PlanConvergencePage.vue':[{container:'pc-review',id:'plan-review',label:'计划收敛核对',blocks:[['records','屏蔽记录',['pc-list'],4,10],['snapshot','实例快照与明细',['pc-main'],8,10]]},{container:'pc-maintenance',id:'plan-audit',label:'通告核对',blocks:[['results','核对结果',['pc-empty','pc-table-wrap'],12,10]]}],
  'CriticalGuardPage.vue':[{container:'fill-layout',id:'guard-building',label:'重保检查',blocks:[['tasks','检查任务',['task-list'],4,10],['form','检查填写',['sheet-workspace'],8,14]]},{container:'admin-layout',id:'guard-admin',label:'重保管理',blocks:[['tasks','发布任务',['task-list'],4,10],['results','检查结果',['admin-results'],8,12]]}],
  'DrillManagementPage.vue':[{container:'building-layout',id:'drill-building',label:'演练办理',blocks:[['tasks','演练任务',['drill-list'],4,10],['form','记录与评估',['execution-panel','empty-panel'],8,14]]},{container:'admin-layout',id:'drill-admin',label:'演练管理',blocks:[['tasks','演练模板',['drill-list'],4,10],['configuration','模板配置',['configuration-panel','empty-panel'],8,14]]}],
  'LearningPage.vue':[{container:'practice-layout',id:'learning-practice',label:'我的答题',blocks:[['questions','题目导航',['question-rail'],4,9],['answer','答题与解析',['question-body'],8,14]]}],
  'LearningDashboard.vue':[{container:'dashboard-panels',id:'learning-profile',label:'学练画像',blocks:[['completion','完成进度',['completion-panel'],4,7],['method','作答方式',['answer-method-panel'],4,7],['interview','问答自评',['interview-panel'],4,7],['trend','答题趋势',['section'],12,8]]}],
  'WaterOverview.vue':[{container:'water-overview-body',id:'water-overview',label:'水耗汇总',blocks:[['chart','楼栋水耗',['MotionChart'],6,8],['buildings','楼栋明细',['water-building-list'],6,8]]}],
  'CabinetPowerPage.vue':[{container:'template',has:'metrics',id:'cabinet',label:'机柜管理',blocks:[['summary','机柜状态',['metrics'],12,4],['workspace','包间、平面图与台账',['nav','section'],12,13]]}],
  'EventManagementPage.vue':[{container:'event-page',id:'events',label:'事件管理',blocks:[['overview','事件概览',['event-command-card'],12,7],['trend','事件趋势',['EventCharts'],12,7],['records','事件明细',['v2-event-detail-toggle','event-list-panel'],12,10]]}],
  'DailyTaskChecklistPage.vue':[{container:'daily-page',id:'daily',label:'每日任务',blocks:[['summary','任务概览',['summary-grid'],12,4],['tasks','任务办理',['category-tabs','task-content'],12,10]]}],
  'LinkDirectoryPage.vue':[{container:'link-directory-page',id:'links',label:'多维表导航',blocks:[['links','链接目录',['table-wrap','table-footer'],12,11]]}],
  'SignatureManagementPage.vue':[{container:'template',has:'stats',id:'signatures',label:'签名管理',blocks:[['summary','签名状态',['stats'],12,4],['people','人员目录',['directory-panel'],12,12],['duplicates','重复人员核对',['duplicates'],12,9,'showDuplicates']]}],
  'LifeGuidePage.vue':[{container:'life-guide-page',id:'guide',label:'人生指南',blocks:[['reader','指南阅读',['iframe'],12,12]]}],
  'AdminTools.vue':[{container:'admin-settings-layout',id:'accounts',label:'账号与设置',blocks:[['settings','账号与权限',['admin-settings-content'],12,12]]}],
};

function find(node,predicate){if(node.type===1&&predicate(node))return node;for(const child of node.children||[]){const found=find(child,predicate);if(found)return found;}return null;}
function canvas(source,group,plan){
  const nodes=(group.children||[]).filter(node=>node.type===1),selected=[],used=new Set();
  const blocks=plan.blocks.map(([id,title,matches,w,h,when])=>{
    const items=nodes.filter(node=>!used.has(node)&&matches.some(value=>has(node,value)));
    if(!items.length)return null;
    selected.push(...items);items.forEach(node=>used.add(node));return {id,title,w,h,minW:w<=4?4:6,minH:4,...when?{when}: {},source:items.map(node=>node.loc.source).join('\n')};
  }).filter(Boolean);
  if(!blocks.length)return source;
  const definitions='['+blocks.map(({source,when,...block})=>'{'+Object.entries(block).map(([key,value])=>key+':'+JSON.stringify(value)).join(',')+(when?',when:('+when+')':'')+'}').join(',')+']';
  const text=`<PageCanvas page="${plan.id}" label="${plan.label}" :blocks="${definitions.replaceAll('"','&quot;')}">${blocks.map(block=>`<template #${block.id}>${block.source}</template>`).join('')}</PageCanvas>`;
  const edits=selected.map(node=>[node.loc.start.offset,node.loc.end.offset,'']);
  const first=selected.reduce((a,b)=>a.loc.start.offset<b.loc.start.offset?a:b);
  edits.find(edit=>edit[0]===first.loc.start.offset)[2]=text;
  const classProp=group.props.find(p=>p.type===6&&p.name==='class');
  if(classProp?.value)edits.push([classProp.value.loc.start.offset+1,classProp.value.loc.end.offset-1,classProp.value.content+' v2-layout-host']);
  else edits.push([group.loc.start.offset+1+group.tag.length,group.loc.start.offset+1+group.tag.length,' class="v2-layout-host"']);
  for(const [start,end,replacement] of edits.sort((a,b)=>b[0]-a[0]))source=source.slice(0,start)+replacement+source.slice(end);
  return source;
}

// Slot extraction retains each module's controller, events, refs and conditional chains.
export function adaptPageLayouts(source,filename){
  const file=filename.replaceAll('\\','/').split('/').pop(),entries=plans[file];
  if(!entries||source.includes('page-layout-adapted'))return source;
  let code=source;
  for(const plan of entries){
    const {descriptor}=parse(code),part=descriptor.template;
    const group=find(part.ast,node=>has(node,plan.container)&&(!plan.has||(node.children||[]).some(child=>child.type===1&&has(child,plan.has))));
    if(!group)continue;
    code=canvas(code,group,plan);
  }
  if(code===source)return source;
  if(file==='WaterWorkspace.vue')code=code.replace(' :max-height="tableHeight"','');
  const importPath=filename.replaceAll('\\','/').includes('/workspace-v2/')?'./PageCanvas.vue':'../workspace-v2/PageCanvas.vue';
  return code.replace('<script setup lang="ts">',`<script setup lang="ts">\n// page-layout-adapted\nimport PageCanvas from '${importPath}';`);
}
export const pageLayoutPlans=plans;
