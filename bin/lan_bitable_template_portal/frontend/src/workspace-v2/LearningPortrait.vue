<template>
  <section class="v2-learning-portrait" :aria-busy="loading">
    <div class="v2-learning-metrics">
      <div v-for="item in metrics" :key="item.label"><NSkeleton v-if="loading && !ready" height="58px" /><NButton v-else text data-v2-navigation :disabled="disabled || !ready" :aria-label="item.label+'，'+item.value+'，'+item.action" @click="metricAction(item.kind)"><span>{{item.label}}</span><strong>{{item.value}}</strong><small>{{item.action}}<ChevronRight :size="12" /></small></NButton></div>
    </div>
    <div v-if="ready" class="v2-learning-today">
      <template v-if="person"><span>今日 {{numberText(today.task_answered)}} / {{numberText(today.assigned)}} 题</span><span>复习 {{numberText(summary.practice_count)}} 次</span><NButton v-if="showContinue" data-v2-navigation type="primary" size="small" :disabled="disabled || !data.published" @click="emit('continue')">{{today.papers?'继续今日答题':'开始今日答题'}}</NButton></template>
      <template v-else><span>今日已答 {{numberText(today.answered_people)}} 人</span><span>已完成 {{numberText(today.completed_people)}} 人</span><span>已领取未开始 {{numberText(today.not_started_people)}} 人</span></template>
      <NTag v-if="failed" size="small" type="warning">本次读取失败，显示上次数据</NTag>
    </div>
    <div class="v2-learning-chart-head" data-v2-navigation><h2>学习分析</h2><NSelect v-model:value="chart" :options="chartOptions" aria-label="学练统计维度" :disabled="disabled" /><NRadioGroup v-if="chart==='trend'" v-model:value="metric" size="small" aria-label="趋势指标"><NRadioButton value="answered">{{person?'作答次数':'参与人数'}}</NRadioButton><NRadioButton value="accuracy">正确率</NRadioButton></NRadioGroup><NRadioGroup v-if="chart==='wrong'" v-model:value="classification" size="small" aria-label="错题分类"><NRadioButton value="banks">按题库</NRadioButton><NRadioButton value="topics">按专业</NRadioButton></NRadioGroup></div>
    <NSkeleton v-if="loading && !ready" height="260px" />
    <NEmpty v-else-if="!ready" :description="failed?'统计暂未读取成功':'暂无已读取的统计数据'" />
    <template v-else>
      <LearningTrend v-if="chart==='trend'" :items="trend" :unit="metric==='accuracy'?'%':person?'次':'人'" :percentage="metric==='accuracy'" :loading="loading" @select="item=>emit('drill',{date:item.label})" />
      <MotionChart v-else title="" :items="chartItems" :view="chartView" :unit="chart==='distribution'?'人':'题'" :loading="loading" @select="chartSelect" />
      <p v-if="chart==='method'" class="v2-learning-definition">独立作答首次正确率 {{methodRates.independent}} · 提示后首次正确率 {{methodRates.assisted}}</p>
      <p class="v2-learning-definition">{{chartNote}}</p>
      <p v-if="data.from && data.to" class="v2-learning-definition">统计区间 {{data.from}} 至 {{data.to}} · 正确率按有效选择题首次作答计算</p>
    </template>
    <section v-if="!person" ref="peopleHost" class="v2-learning-people" aria-label="人员学习汇总">
      <header data-v2-navigation><h2>{{allScopes?'全员学习汇总':'人员学习汇总'}} <small>{{filtered.length}} 人</small></h2><NInput v-model:value="search" placeholder="姓名 / 工号" aria-label="搜索人员" clearable @update:value="page=1"><template #prefix><Search :size="15" /></template></NInput><NSelect v-model:value="sort" :options="sortOptions" aria-label="人员汇总排序" @update:value="page=1" /><NCheckbox v-model:checked="includeUnanswered" @update:checked="page=1">{{allScopes?'包括未作答人员':'包括已领取未作答'}}</NCheckbox><NButton v-if="bucket" text @click="bucket='';page=1">{{bucket}} · 清除筛选<X :size="13" /></NButton></header>
      <NDataTable v-if="!mobile" size="small" :columns="columns" :data="visible" :row-key="row=>row.person_id" :loading="loading" :scroll-x="allScopes?860:780" />
      <div v-else class="v2-learning-person-list">
        <article v-for="row in visible" :key="row.person_id"><div class="v2-learning-person-title"><strong>{{row.name}}</strong><span>{{row.scope?row.scope+'楼':''}}</span><NButton text type="primary" data-v2-navigation :disabled="disabled" :aria-label="'查看'+row.name+'个人画像'" @click="selectPerson(row)">个人画像<ChevronRight :size="14" /></NButton></div><div class="v2-learning-person-values"><span>今日 <b>{{row.today?numberText(row.today.answered)+' / '+numberText(row.today.total):'未领取'}}</b></span><span>正确率 <b>{{percent(row.summary?.accuracy)}}</b></span><span>答题 <b>{{numberText(row.summary?.answered)}}</b></span></div><details><summary>更多记录</summary><p>工号 {{row.employee_no || '未填写'}} · 错题 {{numberText(row.summary?.wrong)}} · 复习 {{numberText(row.summary?.practice_count)}} 次</p><p>最近作答 {{String(row.last_answered_at||'尚未作答').replace('T',' ').slice(0,16)}}</p></details></article>
        <NSkeleton v-if="loading && !ready" height="110px" /><NEmpty v-else-if="!visible.length" description="没有匹配人员" />
      </div>
      <footer data-v2-navigation><span>每页20人</span><NPagination v-model:page="page" :item-count="filtered.length" :page-size="20" :page-slot="mobile?3:7" :show-quick-jumper="!mobile" :disabled="loading" /></footer>
    </section>
  </section>
</template>
<script setup lang="ts">
import {computed,h,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {NButton,NCheckbox,NDataTable,NEmpty,NInput,NPagination,NRadioButton,NRadioGroup,NSelect,NSkeleton,NTag,type DataTableColumns} from 'naive-ui';
import {ChevronRight,Search,X} from 'lucide-vue-next';
import type {Dict} from '../api/client';
import type {ChartItem,ChartView} from './vipboard';
import MotionChart from './MotionChart.vue';
import LearningTrend from './LearningTrend.vue';
const props=defineProps<{data:Dict;person?:Dict|null;disabled?:boolean;showContinue?:boolean;allScopes?:boolean;loading?:boolean;failed?:boolean}>();
const emit=defineEmits<{continue:[];select:[person:Dict];drill:[value:Dict]}>();
const chart=ref('trend'),metric=ref('answered'),classification=ref('banks'),search=ref(''),sort=ref('recent'),page=ref(1),includeUnanswered=ref(false),bucket=ref('');
const mobile=ref(matchMedia('(max-width:760px)').matches),peopleHost=ref<HTMLElement>();
const media=matchMedia('(max-width:760px)'),resize=()=>mobile.value=media.matches;
onMounted(()=>media.addEventListener('change',resize));onBeforeUnmount(()=>media.removeEventListener('change',resize));
const number=(value:unknown):number|null=>typeof value==='number'&&Number.isFinite(value)&&value>=0?value:null;
const numberText=(value:unknown)=>number(value)?.toLocaleString('zh-CN')??'—';
const percent=(value:unknown)=>number(value)===null?'无选择题成绩':Number(value).toFixed(1)+'%';
const summary=computed(()=>props.data.summary||{}),today=computed(()=>props.data.today_summary||{}),ready=computed(()=>Object.keys(summary.value).length>0);
const metrics=computed(()=>props.person?[
  {label:'首次答题数',value:numberText(summary.value.answered),kind:'history',action:'查看学习历史'},
  {label:'选择题错题',value:numberText(summary.value.wrong),kind:'wrong',action:'进入个人错题集'},
  {label:'选择题正确率',value:percent(summary.value.accuracy),kind:'history',action:'查看作答记录'},
  {label:'学习天数',value:numberText(summary.value.learning_days),kind:'history',action:'查看学习历史'},
]:[
  {label:'已答人数',value:numberText(summary.value.answered_people),kind:'people',action:'查看人员明细'},
  {label:'首次答题总数',value:numberText(summary.value.answered),kind:'history',action:'查看学习历史'},
  {label:'选择题错题',value:numberText(summary.value.wrong),kind:'wrong',action:'查看错题分类'},
  {label:'选择题正确率',value:percent(summary.value.accuracy),kind:'distribution',action:'查看正确率分布'},
]);
const chartOptions=computed(()=>[{value:'trend',label:props.person?'个人答题趋势':'每日参与趋势'},{value:'progress',label:'题目完成进度'},{value:'wrong',label:'错题分类对比'},{value:'method',label:'选择题作答方式'},{value:'interview',label:'问答自评分布'},...(!props.person?[{value:'distribution',label:'正确率分布'}]:[])]);
const methodRates=computed(()=>{
  const total=number(summary.value.choice_answered),right=number(summary.value.correct),own=number(summary.value.independent_answered),ownRight=number(summary.value.independent_correct);
  const valid=total!==null&&right!==null&&own!==null&&ownRight!==null&&right<=total&&own<=total&&ownRight<=own&&ownRight<=right&&right-ownRight<=total-own;
  return {independent:valid&&own?percent(ownRight*100/own):'无成绩',assisted:valid&&total>own?percent((right-ownRight)*100/(total-own)):'无成绩'};
});
const trend=computed(()=>(props.data.trend||[]).map((row:Dict)=>({label:String(row.date),value:number(metric.value==='accuracy'?row.accuracy:props.person?row.attempt_count??row.answered:row.people)})));
const chartView=computed<ChartView>(()=>['progress','interview'].includes(chart.value)?'donut':chart.value==='distribution'?'line':'horizontal');
const chartItems=computed<ChartItem[]>(()=>{
  if(chart.value==='progress'){
    const assigned=number(summary.value.assigned),done=number(summary.value.task_answered);
    return assigned===null||done===null||done>assigned?[]:[{label:'已完成',value:done},{label:'待完成',value:assigned-done}];
  }
  if(chart.value==='wrong')return (props.data[classification.value]||[]).filter((row:Dict)=>number(row.wrong)!==null).map((row:Dict)=>({label:String(row.label||row.topic||'未分类'),value:row.wrong,bank:row.bank,search:row.topic}));
  if(chart.value==='distribution')return (props.data.distribution||[]).filter((row:Dict)=>number(row.count)!==null).map((row:Dict)=>({label:String(row.label),value:row.count}));
  if(chart.value==='interview')return Object.entries(summary.value.interview_ratings||{}).filter(([,value])=>number(value)!==null).map(([label,value])=>({label,value:Number(value)}));
  if(chart.value==='method'){
    const total=number(summary.value.choice_answered),independent=number(summary.value.independent_answered);
    return total===null||independent===null||independent>total?[]:[{label:'独立作答',value:independent},{label:'使用提示后作答',value:total-independent}];
  }
  return [];
});
const chartNote=computed(()=>({trend:'无选择题成绩的日期不计为0%。',progress:'仅统计已领取且有效的题目，未领取备用题不计入。',wrong:'有效选择题首次答错数量。',method:'首次选择题作答量，使用提示后的作答单列。',interview:'最近一次自评，不计入选择题正确率。',distribution:'按现有统计区间汇总，无选择题成绩单列。'}[chart.value]));
const sortOptions=[{value:'recent',label:'最近作答'},{value:'name',label:'姓名'},{value:'answered',label:'答题量'}];
const buckets:Record<string,[number,number]>={'0-59%':[0,59.999],'60-79%':[60,79.999],'80-99%':[80,99.999],'100%':[100,100]};
const filtered=computed<Dict[]>(()=>[...(props.data.people||[])].filter((row:Dict)=>{
  if(!includeUnanswered.value&&!(Number(row.summary?.attempt_count??row.summary?.answered)>0))return false;
  if(!String(row.name+' '+(row.employee_no||'')).includes(search.value.trim()))return false;
  const bounds=buckets[bucket.value],rate=number(row.summary?.accuracy);
  return !bounds||rate!==null&&rate>=bounds[0]&&rate<=bounds[1];
}).sort((a:Dict,b:Dict)=>sort.value==='name'?String(a.name).localeCompare(String(b.name),'zh-CN'):sort.value==='answered'?Number(b.summary?.answered||0)-Number(a.summary?.answered||0):String(b.last_answered_at||'').localeCompare(String(a.last_answered_at||''))));
const visible=computed(()=>filtered.value.slice((page.value-1)*20,page.value*20));
function selectPerson(row:Dict){emit('select',{id:row.person_id,name:row.name,employee_no:row.employee_no,scope:row.scope});}
const columns=computed<DataTableColumns<Dict>>(()=>[
  {title:'姓名 / 工号',key:'name',width:145,render:row=>h('div',[h('strong',row.name),h('small',row.employee_no||'未填写工号')])},
  ...(props.allScopes?[{title:'楼栋',key:'scope',width:70,render:(row:Dict)=>row.scope?row.scope+'楼':'待核对'}]:[]),
  {title:'首次答题',key:'answered',width:85,render:row=>numberText(row.summary?.answered)},
  {title:'选择题错题',key:'wrong',width:100,render:row=>numberText(row.summary?.wrong)},
  {title:'正确率',key:'accuracy',width:125,render:row=>percent(row.summary?.accuracy)},
  {title:'今日进度',key:'today',width:95,render:row=>row.today?numberText(row.today.answered)+' / '+numberText(row.today.total):'未领取'},
  {title:'最近作答',key:'last_answered_at',width:150,render:row=>String(row.last_answered_at||'尚未作答').replace('T',' ').slice(0,16)},
  {title:'',key:'action',width:100,render:row=>h(NButton,{text:true,type:'primary',disabled:props.disabled,'data-v2-navigation':'',onClick:()=>selectPerson(row)},()=> '个人画像')},
]);
function people(){peopleHost.value?.scrollIntoView({block:'start',behavior:matchMedia('(prefers-reduced-motion:reduce)').matches?'auto':'smooth'});}
function metricAction(kind:string){if(kind==='people')people();else if(kind==='distribution'||!props.person&&kind==='wrong')chart.value=kind==='wrong'?'wrong':'distribution';else emit('drill',{kind});}
function chartSelect(item:ChartItem){if(props.disabled)return;if(chart.value==='wrong'){if(props.person)emit('drill',{kind:'wrong',bank:item.bank,search:item.search});else people();}else if(chart.value==='distribution'&&buckets[item.label]){bucket.value=item.label;page.value=1;people();}else emit('drill',{kind:'history'});}
watch(()=>props.data,()=>page.value=1);
watch(()=>props.allScopes,value=>{includeUnanswered.value=!!value;bucket.value='';},{immediate:true});
watch(()=>props.person?.id,()=>{chart.value='trend';bucket.value='';search.value='';page.value=1;});
</script>
