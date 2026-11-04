<template>
  <section class="cold-monitor">
    <div class="heading"><div><h1>冷源运行状态</h1><p>查看各楼湿球温度、冷源运行模式及提醒记录</p></div><NButton size="small" :loading="loading" @click="load()">刷新</NButton></div>
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }}</NAlert>
    <section class="panel"><h2>当前运行状态</h2>
      <div class="table-scroll"><table><thead><tr><th>日期</th><th>楼栋</th><th>湿球温度</th><th>冷源运行模式</th><th>模式切换提示</th></tr></thead>
        <tbody><tr v-for="row in items" :key="row.id"><td data-label="日期">{{row.date || '—'}}</td><td data-label="楼栋">{{label(row.scope)}}</td><td data-label="湿球温度">{{row.temperature == null ? '—' : row.temperature+' ℃'}}</td><td data-label="冷源运行模式">{{row.mode || '—'}}</td><td data-label="模式切换提示" class="full">{{row.hint || '暂无提示'}}</td></tr></tbody></table></div>
      <p v-if="!loading && !items.length" class="empty">暂无可查看的冷源记录。</p>
    </section>
    <section class="panel"><h2>自动推送记录</h2>
      <div class="table-scroll"><table><thead><tr><th>触发时间</th><th>涉及楼栋</th><th>推送对象</th><th>消息内容</th><th>状态</th></tr></thead>
        <tbody><tr v-for="job in history" :key="job.id"><td data-label="触发时间">{{formatTime(job.created_at)}}</td><td data-label="涉及楼栋">{{label(job.row.scope)}}</td><td data-label="推送对象" class="full"><span v-for="target in Object.values(job.targets || {})" :key="target.id" class="recipient">{{target.name}}（{{status(target.status)}}）</span><span v-if="!Object.keys(job.targets || {}).length">待确定</span></td><td data-label="消息内容" class="full"><b>{{job.row.temperature}} ℃ · {{job.row.mode}}</b><br>{{job.row.hint}}</td><td data-label="状态" class="full">{{status(job.status)}}<span v-if="job.error" class="detail">{{job.error}}</span></td></tr></tbody></table></div>
      <p v-if="!loading && !history.length" class="empty">暂无自动推送记录。</p>
      <div v-if="total>50" class="paging"><NButton size="small" :disabled="loading||page<=1" @click="load(page-1)">上一页</NButton><span>第 {{page}} / {{Math.ceil(total/50)}} 页 · {{total}} 条</span><NButton size="small" :disabled="loading||page*50>=total" @click="load(page+1)">下一页</NButton></div>
    </section>
  </section>
</template>
<script setup lang="ts">
import {onMounted,ref} from 'vue';
import {NAlert,NButton} from 'naive-ui';
import {requestJson} from '../api/client';
type Row={id:string;scope:string;date:string;temperature:number|null;mode:string;hint:string};
type Target={id:string;name:string;status:string};
type Job={id:string;row:Row;created_at:number;status:string;error?:string;targets:Record<string,Target>};
const items=ref<Row[]>([]),history=ref<Job[]>([]),loading=ref(false),error=ref(''),page=ref(1),total=ref(0);
const label=(scope:string)=>scope==='110'?'110站':scope+'楼';
const status=(value:string)=>({sent:'已发送',pending:'等待发送',uncertain:'结果待核实',superseded:'数据已更新'}[value] || value || '等待发送');
const formatTime=(value:number)=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(new Date(value*1000));
async function load(next=page.value){if(loading.value)return;loading.value=true;error.value='';try{const data=await requestJson('/api/cold-source-monitor?page='+next,{fresh:true}) as {items:Row[];history:Job[];history_page?:number;history_total?:number};items.value=data.items;history.value=data.history;page.value=data.history_page || 1;total.value=data.history_total || 0;}catch(e){error.value=e instanceof Error?e.message:'冷源数据读取失败，请重试。';}finally{loading.value=false;}}
onMounted(()=>load());
</script>
<style scoped>
.cold-monitor{display:grid;gap:14px;min-width:0;color:var(--cf-text,inherit)}.heading{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}.heading h1{font-size:20px;margin:0}.heading p{margin:4px 0 0;opacity:.7}.panel{background:var(--cf-surface,#fff);border:1px solid var(--cf-border,#dce4ed);padding:14px;min-width:0}.panel h2{font-size:16px;margin:0 0 10px}.table-scroll{overflow:auto}table{border-collapse:collapse;width:100%;min-width:650px;text-align:left}th,td{border-bottom:1px solid var(--cf-border,#dce4ed);padding:9px 8px;vertical-align:top}th{font-weight:600;white-space:nowrap}.recipient{display:block;white-space:nowrap}.detail{display:block;opacity:.75;font-size:12px}.empty{margin:12px 0;opacity:.7}.paging{display:flex;align-items:center;gap:8px;justify-content:flex-end;margin-top:10px}@media(max-width:700px){.panel{padding:10px}.heading h1{font-size:18px}table{display:block;min-width:0}thead{display:none}tbody{display:block}tr{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));padding:6px 0;border-bottom:1px solid var(--cf-border,#dce4ed)}td{display:block;min-width:0;border:0;padding:4px 5px;overflow-wrap:anywhere}td::before{content:attr(data-label);display:block;opacity:.65;font-size:12px}.full{grid-column:1/-1}.recipient{white-space:normal}}
</style>
