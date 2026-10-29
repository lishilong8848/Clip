<template>
  <section class="learning-home" :aria-busy="loading" :aria-label="management ? '学习管理工作台' : '个人学习首页'">
    <header><div><span class="eyebrow">{{ management ? '学习管理' : '我的学习' }}</span><h2>{{ management ? '安排学习，跟进完成情况' : `${person?.name || '欢迎回来'}，继续今天的学习` }}</h2><p>{{ management ? '发布、题库与人员进度集中管理。' : '每日题单与当班自主练习一起计入完成情况。' }}</p></div><div class="home-meta"><span class="home-date">{{ date }}</span><button v-if="!management" :disabled="loading" aria-label="刷新学习进度" title="刷新学习进度" @click="emit('refresh')"><RefreshCw :size="17" /></button></div></header>
    <template v-if="!management">
      <div class="learning-today">
        <div><span class="eyebrow">今日学练</span><h3>{{ loading ? '正在读取学习进度' : failed ? '学习进度暂未更新' : completed ? '今日学习已完成' : total ? '完成今天的学习任务' : '今天还没有必答题单' }}</h3><p v-if="failed">暂时无法读取最新进度，请重试；也可直接打开题单核对。</p><p v-else-if="total">已答 <strong>{{ answered }}</strong> / {{ total }} 题，{{ completed ? '可以继续练习或复习错题。' : `还需完成 ${Math.max(0,total-answered)} 题。` }}</p><p v-else-if="!loading">可以先自主练习，也可以查看已发布的资料练习。</p><progress v-if="total && !failed" :value="Math.min(answered,total)" :max="total" aria-label="今日学练完成进度" /></div>
        <div class="home-task-actions"><button class="primary" :disabled="loading" @click="emit('open','today')">{{ completed ? '查看每日题单' : answered ? '继续每日题单' : '打开每日题单' }}<ChevronRight :size="18" /></button><button :disabled="loading" @click="emit('open','completion')">查看每日完成</button></div>
      </div>
      <div class="home-metrics"><div><span title="每日题单与自主练习的有效首次作答">今日首答</span><strong>{{ metric(data.today_summary?.answered) }}<small>题</small></strong></div><div><span title="今日首次客观题正确率">首答正确率</span><strong>{{ rate(data.today_summary?.accuracy) }}</strong></div><div><span>近7天学习</span><strong>{{ metric(data.summary?.learning_days) }}<small>天</small></strong></div></div>
    </template>
    <div class="home-entries"><button v-for="entry in entries" :key="entry.id" :disabled="loading" @click="emit('open',entry.id)"><component :is="entry.icon" :size="22" /><span><strong>{{ entry.title }}</strong><small>{{ entry.description }}</small></span><ChevronRight :size="16" /></button></div>
  </section>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {BookOpen,ChevronRight,RotateCcw,History,ChartNoAxesCombined,Settings,FileText,Users,MessageSquare,CheckCircle2,RefreshCw} from 'lucide-vue-next';
import type {Dict} from '../api/client';
const props=defineProps<{data:Dict;person?:Dict|null;date:string;loading:boolean;failed?:boolean;management?:boolean;isAdmin?:boolean}>();
const emit=defineEmits<{open:[value:string];refresh:[]}>();
const total=computed(()=>Number(props.data.today_summary?.assigned)||0),answered=computed(()=>Number(props.data.today_summary?.task_answered)||0);
const completed=computed(()=>total.value>0&&answered.value>=total.value);
const metric=(value:unknown)=>typeof value==='number'?String(value):'—';
const rate=(value:unknown)=>typeof value==='number'?`${value.toFixed(1)}%`:'暂无';
const entries=computed(()=>props.management?[
  ...(props.isAdmin?[{id:'settings',title:'发布与提醒',description:'白夜班发布时间、发送结果与重试',icon:Settings},{id:'questions',title:'题库管理',description:'维护题目、答案与资料依据',icon:BookOpen}]:[]),
  {id:'completion',title:'完成情况',description:'按人员、日期与班次核对进度',icon:CheckCircle2},
  {id:'sharing',title:'资料练习',description:props.isAdmin?'AI出题、人工录题、导入与发布':'查看已发布的练习',icon:Users},
  {id:'issues',title:'题目质疑',description:'查看反馈并跟进处理',icon:MessageSquare},
  {id:'history',title:'学习记录',description:'查阅题单与作答情况',icon:History},
]:[
  {id:'practice',title:'自主练习',description:'继续本次练习，或开启新一轮',icon:BookOpen},
  {id:'review',title:'复习中心',description:'错题、收藏与笔记集中回顾',icon:RotateCcw},
  {id:'sharing',title:'资料练习',description:'完成分配给自己的分享会练习',icon:FileText},
  {id:'history',title:'学习记录',description:'查看题单、首答与复习记录',icon:History},
  {id:'profile',title:'学习画像',description:'查看趋势与薄弱知识点',icon:ChartNoAxesCombined},
  {id:'issues',title:'题目反馈',description:'跟进已提交的题目质疑',icon:MessageSquare},
]);
</script>
<style scoped>
.home-meta{display:flex;align-items:center;gap:8px;flex-shrink:0}.home-meta button{display:grid;place-items:center;width:44px;padding:0}
.learning-home{margin:12px 0 20px;min-width:0}.learning-home header{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:16px}.eyebrow{color:var(--cf-muted,#587086);font-size:12px}.learning-home h2{font-size:22px;margin:6px 0}.learning-home p{margin:6px 0;color:var(--cf-muted,#587086);line-height:1.6}.home-date{font-size:13px;white-space:nowrap}.learning-today{display:flex;align-items:center;justify-content:space-between;gap:20px;border:1px solid var(--cf-border,#d8e5f4);border-radius:12px;padding:20px;background:var(--cf-surface,#fff)}.learning-today>div:first-child{flex:1}.learning-today h3{font-size:19px;margin:8px 0}.learning-today progress{width:100%;max-width:520px;height:8px;accent-color:var(--primary-color,#2472cc)}.home-task-actions{display:flex;flex-wrap:wrap;gap:8px}.learning-home button{font:inherit;cursor:pointer;border:1px solid var(--cf-border,#d8e5f4);border-radius:8px;background:var(--cf-surface,#fff);color:inherit;min-height:44px;padding:10px 14px}.home-task-actions .primary{display:flex;align-items:center;justify-content:center;gap:8px;background:var(--primary-color,#2472cc);border-color:transparent;color:white}.learning-home button:disabled{opacity:.6;cursor:wait}.learning-home button:focus-visible{outline:2px solid #2472cc;outline-offset:3px}.home-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:16px 0}.home-metrics>div{padding:12px 16px;border-bottom:1px solid var(--cf-border,#d8e5f4)}.home-metrics span{display:block;color:var(--cf-muted,#587086);font-size:13px}.home-metrics strong{display:block;font-size:24px;margin-top:8px}.home-metrics small{font-size:12px;font-weight:400;margin-left:4px}.home-entries{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:16px}.home-entries button{display:flex;align-items:center;text-align:left;gap:12px;padding:16px}.home-entries button>span{flex:1}.home-entries strong,.home-entries small{display:block}.home-entries strong{font-size:16px}.home-entries small{color:var(--cf-muted,#587086);margin-top:6px;font-size:13px;line-height:1.5}@media(max-width:760px){.learning-home h2{font-size:20px}.learning-home header{flex-wrap:wrap}.learning-today{align-items:stretch;flex-direction:column;padding:14px;gap:12px}.home-task-actions button{flex:1}.home-metrics{gap:4px}.home-metrics>div{padding:10px 4px}.home-metrics span{font-size:12px}.home-metrics strong{font-size:21px}.home-entries{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.home-entries button{padding:12px 10px;align-items:flex-start;gap:7px}.home-entries button>svg:last-child{display:none}.home-entries strong{font-size:14px}.home-entries small{font-size:12px}}
</style>
