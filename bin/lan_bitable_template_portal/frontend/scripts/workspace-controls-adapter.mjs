import { parse } from '@vue/compiler-sfc';

export function markBinaryControls(source) {
  if(!source.includes('checkbox'))return source;
  const {descriptor}=parse(source),edits=[];
  function visit(node){
    if(node.type===1&&node.tag==='input'){
      const type=node.props.find(p=>p.type===6&&p.name==='type')?.value?.content;
      const model=node.props.find(p=>p.type===7&&p.name==='model')?.exp?.content||'';
      const hasValue=node.props.some(p=>(p.type===6&&['value','indeterminate'].includes(p.name))||(p.type===7&&['value','indeterminate','change'].includes(p.arg?.content)));
      if(type==='checkbox'&&!hasValue&&/(?:^|\.)(?:enabled|reminder_enabled|snap_back|tool_calls|image_input|custom_protocol|reasoning_only|allow_reasoning_off)$/.test(model)){
        edits.push(node.loc.start.offset+6);
      }
    }
    for(const child of node.children||[])visit(child);
  }
  visit(descriptor.template?.ast||{});
  for(const offset of edits.sort((a,b)=>b-a))source=source.slice(0,offset)+' data-v2-binary'+source.slice(offset);
  return source;
}

export function adaptPasswordLogin(source){
  let code=source.replaceAll('\r\n','\n');
  function replace(before,after){if(code.split(before).length!==2)throw new Error('Password V2 source contract changed');code=code.replace(before,after);}
  replace('<script setup lang="ts">','<script setup lang="ts">\nimport PasswordStrength from "../workspace-v2/PasswordStrength.vue";\nimport {usePasswordFeedback} from "../workspace-v2/passwordFeedback";');
  replace('const errorMessage = ref("");','const errorMessage = ref("");\nusePasswordFeedback(errorMessage,[password,currentPassword,newPassword,confirmPassword],busy);');
  for(const id of ['personnel-confirm-password','personnel-reset-confirm-password','personnel-change-confirm-password']){
    replace(`<label class="field" for="${id}">`,`<PasswordStrength :value="newPassword" />\n      <label class="field" for="${id}">`);
  }
  return code;
}

export function fuseLearningCharts(source){
  const {descriptor}=parse(source), groups=[];
  function visit(node){
    const classes=node.props?.find(p=>p.type===6&&p.name==='class')?.value?.content;
    if(node.type===1&&node.tag==='div'&&['dashboard-panels','charts'].includes(classes))groups.push(node);
    for(const child of node.children||[])visit(child);
  }
  visit(descriptor.template.ast);
  if(groups.length!==2)throw new Error('Learning chart layout changed');
  const sections=groups.flatMap(g=>g.children.filter(n=>n.type===1&&n.tag==='section'));
  if(sections.length!==6)throw new Error('Learning chart count changed');
  const keys=['progress','method','interview','trend','distribution','wrong'];
  const panels=sections.map((node,index)=>{
    const text=node.loc.source,condition=node.props.find(p=>p.type===7&&p.name==='if');
    return condition?text.replace(condition.loc.source,`v-if="(${condition.exp.content}) && dashboardChart==='${keys[index]}'"`):text.replace('<section',`<section v-if="dashboardChart==='${keys[index]}'"`);
  }).join('\n');
  let code=source.slice(0,groups[0].loc.start.offset)+`<div class="v2-learning-chart">
    <NSelect v-model:value="dashboardChart" :options="dashboardChartOptions" aria-label="学练统计维度" style="max-width:240px;margin:12px 0 16px auto" />
    <div class="charts dashboard-panels" style="display:block;padding:0">${panels}</div>
  </div>`+source.slice(groups[1].loc.end.offset);
  code=code.replace('<script setup lang="ts">',`<script setup lang="ts">
import {NSelect} from 'naive-ui';
const dashboardChart=ref('trend');
const dashboardChartOptions=computed(()=>[
  {label:props.person?'个人答题趋势':'每日参与趋势',value:'trend'},
  {label:'题目完成进度',value:'progress'},{label:'选择题作答方式',value:'method'},
  {label:'问答自评分布',value:'interview'},
  {label:props.person?'错题分类对比':'正确率分布',value:'distribution'},
  ...(!props.person?[{label:'错题分类对比',value:'wrong'}]:[])]);`);
  code=code.replace("const metric = ref('answered')", "watch(()=>props.person,()=>{if(props.person&&dashboardChart.value==='wrong')dashboardChart.value='distribution';});\nconst metric = ref('answered')");
  return code;
}
