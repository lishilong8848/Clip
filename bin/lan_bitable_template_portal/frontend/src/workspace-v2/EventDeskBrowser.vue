<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue';
import type { Dict } from './client';

const props = defineProps<{
  ongoing:Dict[]; history:Dict[]; loading:boolean; failed:boolean;
  titleOf:(row:Dict)=>string; dateOf:(row:Dict)=>string;
}>();
const mode = ref<'ongoing'|'history'>('ongoing');
const selection = ref<Record<string,string>>({});
const rows = computed(()=>mode.value==='ongoing'?props.ongoing:props.history);
const active = computed(()=>rows.value.find(row=>row.event_id===selection.value[mode.value]) || rows.value[0]);
watch(()=>props.ongoing,()=>{
  const id=selection.value.ongoing;
  if(mode.value==='ongoing' && id && !props.ongoing.some(row=>row.event_id===id) && props.history.some(row=>row.event_id===id)){
    selection.value.history=id;
    mode.value='history';
  }
});
watch(active,row=>{if(row && selection.value[mode.value]!==row.event_id)selection.value[mode.value]=row.event_id;},{immediate:true});
const visited = ref<string[]>([]);
const body = ref<HTMLElement>();
// Keep visited controllers mounted across list switches, including any pending
// send or metadata save. Remove them when the record leaves the loaded scope.
const all = computed(()=>[...props.ongoing,...props.history]);
watch([all,()=>active.value?.event_id],([items,id])=>{
  const loaded=new Set(items.map(row=>row.event_id));
  const retained=visited.value.filter(key=>loaded.has(key));
  if(id&&!retained.includes(id))retained.push(id);
  visited.value=retained;
},{immediate:true});
const mountedRows=computed(()=>{const ids=new Set(visited.value);return all.value.filter(row=>ids.has(row.event_id));});
async function choose(row:Dict){
  selection.value[mode.value]=row.event_id;
  await nextTick();
  if(body.value && matchMedia('(max-width:800px)').matches){
    body.value.scrollIntoView({block:'start'});
    body.value.focus({preventScroll:true});
  }
}
function delivery(state?:string){
  return ({failed:'投递异常',uncertain:'待核对',posting:'发送中',pending:'待发送',sent:'已送达',superseded:'已替代'} as Record<string,string>)[state||''] || '';
}
</script>
<template>
  <div class="evt-browser">
    <section class="evt-index" aria-label="事件列表">
      <div class="evt-index-head">
        <div class="evt-segment" role="group" aria-label="事件状态">
          <button type="button" :aria-pressed="mode==='ongoing'" @click="mode='ongoing'">进行中 <b>{{ongoing.length}}</b></button>
          <button type="button" :aria-pressed="mode==='history'" @click="mode='history'">历史事件 <b>{{history.length}}</b></button>
        </div>
        <slot v-if="mode==='history'" name="month" />
      </div>
      <div class="evt-index-rows" :aria-busy="loading">
        <button v-for="row in rows" :key="row.event_id" type="button" :aria-current="active?.event_id===row.event_id?'true':undefined" @click="choose(row)">
          <span class="evt-index-meta"><span>{{[row.metadata?.building,row.metadata?.specialty,row.metadata?.level].filter(Boolean).join(' · ') || '资料待补充'}}</span>
            <b v-if="delivery(row.delivery?.state)" :data-state="row.delivery.state">{{delivery(row.delivery.state)}}</b></span>
          <strong>{{titleOf(row)}}</strong>
          <small>{{row.metadata?.occurrence_time || Number(row.created_at)>0 ? dateOf(row) : '时间待补充'}}<span v-if="row.missing?.length"> · 待补 {{row.missing.length}} 项</span></small>
        </button>
      </div>
      <p v-if="!rows.length" class="evt-empty" role="status">{{loading?'正在读取事件…':failed?'事件列表读取失败':mode==='history'?'所选月份暂无已结束事件':'暂无进行中事件'}}</p>
    </section>
    <div v-show="active" ref="body" class="evt-selected" tabindex="-1" aria-label="当前事件">
      <div v-for="row in mountedRows" :key="row.event_id" v-show="row.event_id===active?.event_id"><slot name="record" :row="row" /></div>
    </div>
  </div>
</template>
