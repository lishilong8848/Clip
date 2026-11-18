<template>
  <section v-if="rows.length || error || loading" class="wb-panel wb-cold-status" aria-labelledby="wb-cold-title">
    <header class="wb-section-heading">
      <h2 id="wb-cold-title">冷源运行状态</h2>
      <button type="button" class="wb-link" :disabled="loading" @click="load">刷新状态</button>
    </header>
    <p v-if="error" class="wb-note" role="status">{{ error }}</p>
    <p v-else-if="loading && !rows.length" class="wb-note" role="status">正在读取各楼状态…</p>
    <ul v-if="rows.length" class="wb-cold-list">
      <li v-for="row in rows" :key="row.scope">
        <strong>{{ row.scope==='110'?'110站':row.scope+'楼' }}</strong>
        <span>{{ row.mode || '模式待同步' }}</span>
        <span v-if="row.temperature!=null">湿球 {{ row.temperature }} ℃</span>
        <small v-if="row.date">{{ row.date }}</small>
      </li>
    </ul>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue';
import { requestJson } from '../client';
import type { ScopeOption } from './recipes';

type StatusRow={scope:string;mode:string;date:string;temperature:number|null};
const props=defineProps<{scope:string;scopes:ScopeOption[];userId:string}>();
const rows=ref<StatusRow[]>([]),error=ref(''),loading=ref(false);
let active:AbortController|undefined;

async function load() {
  active?.abort();
  const request=active=new AbortController();
  const authorized=new Set(props.scopes.map(option=>option.value));
  const allowed=(scope:string)=>/^[A-EH]$|^110$/.test(scope)
    && (authorized.has(scope)||authorized.has('ALL'))
    && (['ALL','CAMPUS'].includes(props.scope)||scope===props.scope);
  loading.value=true;error.value='';
  try {
    const data=await requestJson('/api/cold-source-monitor?status_only=1',{signal:request.signal,cache:'no-store'});
    if (request.signal.aborted) return;
    if (!Array.isArray(data.items)) throw new Error('冷源状态响应不完整。');
    const newest=new Map<string,StatusRow>();
    for (const row of data.items as StatusRow[]) {
      if (!allowed(row.scope)) continue;
      const current=newest.get(row.scope);
      if (!current||String(row.date||'')>String(current.date||'')) newest.set(row.scope,row);
    }
    rows.value=[...newest.values()].sort((a,b)=>a.scope.localeCompare(b.scope,'zh-CN',{numeric:true}));
  } catch (cause) {
    if (!request.signal.aborted) {
      if ([401,403].includes((cause as {status?:number})?.status||0)) rows.value=[];
      error.value='冷源状态暂未同步，请刷新核对。';
    }
  } finally { if (active===request) loading.value=false; }
}
watch(()=>[props.scope,props.scopes,props.userId],()=>{rows.value=[];void load();},{immediate:true});
onBeforeUnmount(()=>active?.abort());
</script>
