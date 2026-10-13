<template>
  <section ref="element" class="v2-overview-panel" :class="{'tasks-only':tasksOnly}" :aria-label="title">
    <header v-if="!tasksOnly" class="v2-section-title"><h2><template v-if="expanded">{{ title }}</template><NButton v-else text class="v2-section-link" @click="$emit('open',target)">{{ title }}<ArrowRight :size="15" /></NButton></h2><NButton text :loading="loading" :aria-label="'刷新' + title" @click="load"><RefreshCw :size="15" /></NButton></header>
    <NSkeleton v-if="loading && !ready" :repeat="3" text />
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }}</NAlert>
    <div v-if="stats.length && !tasksOnly" class="v2-inline-stats"><div v-for="item in stats" :key="item.label"><strong>{{ item.value ?? '—' }}</strong><span>{{ item.label }}</span></div></div>
    <NProgress v-if="progress !== null && !tasksOnly" type="line" :percentage="progress" :show-indicator="false" :height="8" />
    <NList v-if="rows.length" :show-divider="true" class="v2-compact-list">
      <NListItem v-for="row in brief ? rows.slice(0,3) : tasksOnly ? rows.slice(0,12) : expanded || kind === 'water' ? rows : rows.slice(0,5)" :key="row.id"><NButton text class="v2-row-title" @click="$emit('open',row.url)">{{ row.title }}</NButton><template #suffix><NTag size="small" :bordered="false" :type="row.warning ? 'warning' : 'default'">{{ row.status }}</NTag></template></NListItem>
    </NList>
    <p v-else-if="ready && !error" class="v2-source-note">{{ empty }}</p>
    <footer v-if="note || tasksOnly" class="v2-section-foot"><span>{{ note }}{{ brief&&rows.length>3?' · 当前展示前3条':tasksOnly && rows.length > 12 ? ' · 当前展示前12条' : '' }}</span><NButton v-if="tasksOnly || !expanded" text type="primary" @click="$emit('open',footerTarget)">{{ brief?'查看待办':tasksOnly?'查看完整列表':'查看明细' }} <ArrowRight :size="15" /></NButton></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NEmpty, NList, NListItem, NProgress, NSkeleton, NTag } from 'naive-ui';
import { ArrowRight, RefreshCw } from 'lucide-vue-next';
import { requestJson, type Dict } from './client';
import { beijingDate, count, modules, moduleUrl } from './modules';
import { mopNoticeNeedsAction, noticeIsEnded } from '../mopSelectionUtils';
const props = defineProps<{kind:string;scope:string;scopes:{value:string;label:string}[];user:Dict;personal?:boolean;expanded?:boolean;tasksOnly?:boolean;brief?:boolean}>();
defineEmits<{open:[url:string]}>();
const element=ref<HTMLElement>(),loading=ref(false),ready=ref(false),error=ref(''),note=ref(''),empty=ref('当前没有待处理事项');
const rows=ref<Dict[]>([]),stats=ref<{label:string;value:string|number|null}[]>([]),progress=ref<number|null>(null);
const title=computed(()=>props.kind==='learning'?(props.personal?'我的学练':'楼栋学练'):props.kind==='plan_convergence'?'计划收敛 · 需关注':props.kind==='cabinet'?'机柜上下电待办':modules.find(m=>m.id===props.kind)?.label || '待处理');
const target=computed(()=>props.kind==='learning'?'/learning?view='+ (props.personal?'practice':'overview')+(props.personal?'':'&scope='+props.scope):props.kind==='cabinet'?'/cabinet-power/batches?'+new URLSearchParams({scope:props.scope,status:'todo'}):moduleUrl(modules.find(m=>m.id===props.kind)!,props.scope));
const buildingScopes=computed(()=>props.scopes.filter(s=>'ABCDE'.split('').includes(s.value)&&(['ALL','CAMPUS'].includes(props.scope)||s.value===props.scope)).map(s=>s.value));
const footerTarget=computed(()=>props.brief?'/?'+new URLSearchParams({view:'tasks',kind:props.kind,scope:props.scope}):target.value);
let abort:AbortController|undefined,observer:IntersectionObserver|undefined,visible=false,disposed=false;
function array(value:unknown):Dict[]{if(!Array.isArray(value))throw new Error('资料未完整返回，请进入详情核对。');return value;}
async function load(){
  if(disposed||document.hidden)return;
  abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';
  const get=(path:string,params:Dict={})=>requestJson(path+'?'+new URLSearchParams(params),{signal:controller.signal,timeoutMs:12000});
  let nextRows:Dict[]=[],nextStats:{label:string;value:string|number|null}[]=[],nextNote='',nextEmpty='当前没有待处理事项',nextProgress:number|null=null;
  try{
    if(props.kind==='plan_convergence'){
      let unchecked=0,unmatched=0,excluded=0;
      for(const kind of ['maintenance','change']){
        const data=await get('/api/plan-convergence/'+kind+'/records');
        for(const row of array(data)){
          const codes:string[]=(String(row.building||'').match(/110|[ABCDEH]+/g)||[]).flatMap(code=>code==='110'?[code]:code.split(''));
          if(!['ALL','CAMPUS'].includes(props.scope)&&!codes.length)throw new Error('部分核对记录缺少明确楼栋，暂不能确认本楼数量。');
          if(!['ALL','CAMPUS'].includes(props.scope)&&!codes.includes(props.scope))continue;
          if(row.check_status==='skipped'){excluded++;continue;}
          const checked=row.check_status==='ready'&&Array.isArray(row.hits);
          if(checked&&row.hits.length)continue;
          if(checked)unmatched++;else unchecked++;
          nextRows.push({id:kind+row.record_id,title:row.name||row.title,status:checked?'未匹配':'待核对',warning:checked,url:'/plan-convergence?tab='+kind});
        }
      }
      nextStats=[{label:'已核对未匹配',value:unmatched},{label:'待核对 / 待重核',value:unchecked}];
      nextNote=excluded?'仅现有核对结果；D/E楼按原规则不参与':'读取已有核对结果，不自动连接智航';
    }else if(props.kind==='learning'){
      const boot=await get('/api/learning/bootstrap');
      if(props.personal&&!boot.self_person?.id){nextEmpty='当前账号未关联个人学练身份';}
      else{
        const query=props.personal?{scope:boot.self_scope,person_id:boot.self_person.id,period:'7'}:{scope:props.scope,period:'7',...(props.scope==='ALL'?{all_people:'1'}:{})};
        const data=await get('/api/learning/profile',query),s=data.summary||{},t=data.today_summary||{};
        nextStats=props.personal?[{label:'近7天已答',value:count(s.answered)},{label:'选择题错题',value:count(s.wrong)},{label:'首次正确率',value:s.accuracy==null?null:Number(s.accuracy).toFixed(1)+'%'}]:[{label:'今日已答人数',value:count(t.answered_people)},{label:'今日完成人数',value:count(t.completed_people)},{label:'已领取未开始',value:count(t.not_started_people)}];
        const total=count(t.assigned),done=count(t.task_answered);
        nextProgress=total&&done!==null?Math.min(100,done/total*100):null;
        nextNote=props.personal?`今日进度 ${done??'—'} / ${total??'—'}；仅已领取题单`:'只统计人员任务，备用题不计入待办';
        nextEmpty=data.published?'今日已发布，可继续学练':'今日尚未发布，可进入自主练习';
      }
    }else if(props.kind==='engineer_mop'){
      const data=await get('/api/engineer/mop/bootstrap',{scope:props.scope});
      const items=array(data.notices).filter(item=>!noticeIsEnded(item)&&mopNoticeNeedsAction(item));
      nextStats=[{label:'维护单待处理',value:items.length}];
      nextRows=items.map(item=>({id:item.notice_key,title:item.title||item.name||'通告维护单',status:item.mop_binding?'待上传':'待绑定',url:'/engineer/mop?scope='+props.scope+'&notice_key='+encodeURIComponent(item.notice_key)}));
      nextNote='按原维护单待处理口径';
    }else if(props.kind==='water'){
      const scopes=props.scopes.filter(s=>'ABCDEH'.split('').includes(s.value)&&(['ALL','CAMPUS'].includes(props.scope)||s.value===props.scope));
      if(!scopes.length)throw new Error('当前范围未接入水耗管理。');
      const data=await Promise.all(scopes.map(async s=>({scope:s.value,label:s.label,data:await get('/api/capacity/water/bootstrap',{scope:s.value})})));
      const sum=(key:string)=>{const values=data.map(item=>count(item.data.summary?.[key]));return values.some(v=>v===null)?null:Number(values.reduce<number>((total,v)=>total+(v??0),0).toFixed(2));};
      nextStats=[{label:'本月记录',value:sum('record_count')},{label:'本月耗水量 / t',value:sum('total_usage')},{label:'有照片记录',value:sum('photo_records')}];
      if(data.length>1)nextRows=data.map(item=>({id:item.scope,title:item.label+'水耗',status:`${count(item.data.summary?.record_count)??'—'}条 · ${count(item.data.summary?.total_usage)??'—'} t`,url:'/water-management?scope='+item.scope}));
      const latest=Math.max(...data.map(item=>Number(item.data.summary?.latest_date_ms)||0));
      nextEmpty=latest?'最近统计 '+new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai'}).format(new Date(latest)):'暂无统计日期';
      nextNote='沿用水耗管理统计口径 · 仅汇总授权楼栋';
    }else if(['drills','critical_guard','cabinet'].includes(props.kind)){
      if(!buildingScopes.value.length)throw new Error('当前范围不在该模块支持的 A–E 楼内。');
      let total=0,complete=true;
      for(const scope of buildingScopes.value){
        if(props.kind==='drills'){
          const data=await get('/api/drills',{scope,month:beijingDate().slice(0,7)});
          const items=array(data.items).filter(item=>item.status==='published'&&!['synced','completed'].includes(item.execution?.status));
          total+=items.length;
          nextRows.push(...items.map(item=>({id:scope+item.drill_id,title:item.name,status:scope+'楼 · '+(item.execution?.status==='error'?'生成异常':'待完成'),warning:!!item.execution?.last_error,url:'/drill-management?scope='+scope+'&drill_id='+item.drill_id})));
        }else if(props.kind==='critical_guard'){
          const data=await get('/api/critical-guard/tasks',{scope});
          for(const item of array(data.tasks)){
            if(item.complete===true)continue;
            if(typeof item.complete!=='boolean'){complete=false;continue;}
            total++;
            nextRows.push({id:scope+item.task_id,title:item.name,status:scope+'楼 · 待填写',warning:true,url:'/critical-guard?scope='+scope+'&task_id='+item.task_id});
          }
        }else{
          const data=await get('/api/cabinet-power/batches',{scope,status:'todo',page:'1',page_size:'5'});
          const items=array(data.items).filter(item=>item.is_todo&&!item.source_notice_deleted);
          const amount=count(data.total);if(amount===null)complete=false;else total+=amount;
          nextRows.push(...items.map(item=>({id:item.batch_id,title:item.name||item.title||item.notice_title||'机柜上下电批次',status:scope+'楼 · 待确认',warning:true,url:'/cabinet-power/batches?scope='+scope+'&batch_id='+item.batch_id+'&status=todo'})));
        }
      }
      nextStats=[{label:props.kind==='critical_guard'?'未完成楼栋任务':'待完成',value:complete?total:null}];
      nextNote=complete?(props.kind==='drills'?'当前月份 · 已发布演练':'按授权楼栋查询，最多展示5条'):'部分汇总字段缺失，请进入详情核对';
    }
    if(controller.signal.aborted)return;
    rows.value=nextRows;stats.value=nextStats;note.value=nextNote;empty.value=nextEmpty;progress.value=nextProgress;ready.value=true;
  }catch(e:any){if(!controller.signal.aborted){error.value=e.message||'读取失败，不能判断为零';note.value=ready.value?'保留上次读取的数据':'';}}
  finally{if(abort===controller)loading.value=false;}
}
watch(()=>[props.scope,props.kind,props.personal],()=>{abort?.abort();ready.value=false;rows.value=[];stats.value=[];error.value='';if(visible)void load();});
function changed(){if(visible)void load();}
onMounted(()=>{observer=new IntersectionObserver(entries=>{visible=entries.some(e=>e.isIntersecting);if(visible&&!ready.value&&!loading.value)void load();},{rootMargin:'80px'});if(element.value)observer.observe(element.value);window.addEventListener('clipflow-business-changed',changed);});
onBeforeUnmount(()=>{disposed=true;abort?.abort();observer?.disconnect();window.removeEventListener('clipflow-business-changed',changed);});
</script>
