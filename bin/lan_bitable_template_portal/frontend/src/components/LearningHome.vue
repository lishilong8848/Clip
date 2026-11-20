<template>
  <LearningPersonalHome v-if="!management" :data="data" :person="person" :loading="loading" :failed="failed" :daily-duty="dailyDuty" @open="emit('open', $event)" @refresh="emit('refresh')" />
  <section v-else class="learning-home" :aria-busy="loading" aria-label="学习管理工作台">
    <header v-if="workspaceV2" class="v2-home-header"><h2>{{ management ? '学习管理' : '个人学习' }}</h2><div class="home-meta"><span class="home-date">{{ date }}</span><button v-if="!management" :disabled="loading" aria-label="刷新学习进度" title="刷新学习进度" @click="emit('refresh')"><RefreshCw :size="17" /></button></div></header>
    <header :class="{ 'v2-home-desktop-header': workspaceV2 }"><div><span class="eyebrow">{{ management ? '学习管理' : '我的学习' }}</span><h2>{{ management ? '安排学习，跟进完成情况' : `${person?.name || '欢迎回来'}，继续今天的学习` }}</h2><p>{{ management ? '发布、题库与人员进度集中管理。' : '每日题单与当班自主练习一起计入完成情况。' }}</p></div><div class="home-meta"><span class="home-date">{{ date }}</span><button v-if="!management" :disabled="loading" aria-label="刷新学习进度" title="刷新学习进度" @click="emit('refresh')"><RefreshCw :size="17" /></button></div></header>
    <div class="home-entries"><button v-for="entry in entries" :key="entry.id" :disabled="loading" @click="emit('open',entry.id)"><component :is="entry.icon" :size="22" /><span><strong>{{ entry.title }}</strong><small>{{ entry.description }}</small></span><ChevronRight :size="16" /></button></div>
  </section>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {BookOpen,ChevronRight,History,Settings,Users,MessageSquare,CheckCircle2,RefreshCw} from 'lucide-vue-next';
import LearningPersonalHome from './LearningPersonalHome.vue';
import type {Dict} from '../api/client';
const props=defineProps<{data:Dict;person?:Dict|null;date:string;loading:boolean;failed?:boolean;management?:boolean;isAdmin?:boolean;exemptionDate?:string;dailyDuty?:Dict|null}>();
const workspaceV2=document.documentElement.classList.contains('workspace-v2');
const emit=defineEmits<{open:[value:string];refresh:[]}>();
const selfProgress=computed(()=>props.data.people?.find((row:Dict)=>row.person_id===props.person?.id)?.today);
const total=computed(()=>Number(props.data.today_summary?.assigned)||0),answered=computed(()=>Number(props.data.today_summary?.task_answered)||0);
const completed=computed(()=>total.value>0&&answered.value>=total.value&&!props.data.today_summary?.has_uncertain_tasks);
const metric=(value:unknown)=>typeof value==='number'?String(value):'—';
const entries=computed(()=>[
  ...(props.isAdmin?[{id:'settings',title:'发布与提醒',description:'白夜班发布时间、发送结果与重试',icon:Settings},{id:'questions',title:'题库管理',description:'维护题目、答案与资料依据',icon:BookOpen}]:[]),
  {id:'completion',title:'完成情况',description:'按人员、日期与班次核对进度',icon:CheckCircle2},
  {id:'sharing',title:'分享会练习',description:props.isAdmin?'AI出题、人工录题、导入与发布':'查看已发布的练习',icon:Users},
  {id:'issues',title:workspaceV2?'题目反馈':'题目质疑',description:'查看各楼栋的反馈和处理进度',icon:MessageSquare},
  {id:'history',title:'学习记录',description:'查阅题单与作答情况',icon:History},
]);
</script>
<style scoped>
.home-meta{display:flex;align-items:center;gap:8px;flex-shrink:0}.home-meta button{display:grid;place-items:center;width:44px;padding:0}
.learning-home{margin:12px 0 20px;min-width:0}.learning-home header{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:16px}.eyebrow{color:var(--cf-muted,#587086);font-size:12px}.learning-home h2{font-size:22px;margin:6px 0}.learning-home p{margin:6px 0;color:var(--cf-muted,#587086);line-height:1.6}.home-date{font-size:13px;white-space:nowrap}.learning-today{display:flex;align-items:center;justify-content:space-between;gap:20px;border:1px solid var(--cf-border,#d8e5f4);border-radius:12px;padding:20px;background:var(--cf-surface,#fff)}.learning-today>div:first-child{flex:1}.learning-today h3{font-size:19px;margin:8px 0}.learning-today progress{width:100%;max-width:520px;height:8px;accent-color:var(--primary-color,#2472cc)}.home-task-actions{display:flex;flex-wrap:wrap;gap:8px}.learning-home button{font:inherit;cursor:pointer;border:1px solid var(--cf-border,#d8e5f4);border-radius:8px;background:var(--cf-surface,#fff);color:inherit;min-height:44px;padding:10px 14px}.home-task-actions .primary{display:flex;align-items:center;justify-content:center;gap:8px;background:var(--primary-color,#2472cc);border-color:transparent;color:white}.learning-home button:disabled{opacity:.6;cursor:wait}.learning-home button:focus-visible{outline:2px solid #2472cc;outline-offset:3px}.home-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:16px 0}.home-metrics>div{padding:12px 16px;border-bottom:1px solid var(--cf-border,#d8e5f4)}.home-metrics span{display:block;color:var(--cf-muted,#587086);font-size:13px}.home-metrics strong{display:block;font-size:24px;margin-top:8px}.home-metrics small{font-size:12px;font-weight:400;margin-left:4px}.home-entries{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:16px}.home-entries button{display:flex;align-items:center;text-align:left;gap:12px;padding:16px}.home-entries button>span{flex:1}.home-entries strong,.home-entries small{display:block}.home-entries strong{font-size:16px}.home-entries small{color:var(--cf-muted,#587086);margin-top:6px;font-size:13px;line-height:1.5}@media(max-width:760px){.learning-home h2{font-size:20px}.learning-home header{flex-wrap:wrap}.learning-today{align-items:stretch;flex-direction:column;padding:14px;gap:12px}.home-task-actions button{flex:1}.home-metrics{gap:4px}.home-metrics>div{padding:10px 4px}.home-metrics span{font-size:12px}.home-metrics strong{font-size:21px}.home-entries{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.home-entries button{padding:12px 10px;align-items:flex-start;gap:7px}.home-entries button>svg:last-child{display:none}.home-entries strong{font-size:14px}.home-entries small{font-size:12px}}
</style>
<style scoped>
.learning-home .v2-home-header{display:none}
.v2-home-header h2{margin:0;min-width:0;font-size:20px}
.v2-home-header .home-meta button{width:44px;min-width:44px;padding:0}
@media(max-width:760px){
  .learning-home .v2-home-header{display:flex;align-items:center;flex-wrap:nowrap;gap:8px;margin-bottom:12px}
  .learning-home .v2-home-desktop-header{display:none}
}
</style>
