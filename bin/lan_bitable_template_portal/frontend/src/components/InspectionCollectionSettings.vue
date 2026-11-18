<template>
  <details class="collection-settings" @toggle="opened">
    <summary>巡检实时采集</summary>
    <p v-if="loading" role="status">正在读取设置…</p>
    <template v-else-if="settings">
      <label><input v-model="settings.enabled" type="checkbox" :disabled="saving">按开轮时间采集并学习提前采集</label>
      <label>全景平台外网端地址<input v-model.trim="settings.url" type="url" placeholder="http://192.168.224.157:18765" :disabled="saving"></label>
      <p>填写全景平台外网端的局域网地址，并在外网端启用巡检联动。</p>
      <p>开轮时核验本楼 10 分钟内的数据。积累至少 3 个排班日的稳定开轮时间后，提前 5 分钟采集。D、E楼分别学习。</p>
      <NButton size="small" :loading="saving" :disabled="loading" @click="save">保存采集设置</NButton>
    </template>
    <p v-if="notice" :role="failed?'alert':'status'">{{ notice }} <button v-if="failed && !settings" type="button" @click="load">重试</button></p>
  </details>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { NButton } from 'naive-ui';
import { requestJson } from '../api/client';

type Settings={revision:number;enabled:boolean;url:string};
const settings=ref<Settings>(),notice=ref(''),failed=ref(false),loading=ref(false),saving=ref(false);
const api='/api/daily-inspection/collection-settings';
function opened(event:Event){
  if((event.target as HTMLDetailsElement).open&&!settings.value&&!loading.value)void load();
}
async function load(){
  loading.value=true;notice.value='';failed.value=false;
  try{settings.value=await requestJson(api,{fresh:true,cache:'no-store'}) as Settings;}
  catch(error){failed.value=true;notice.value=error instanceof Error?error.message:'采集设置暂未读取。';}
  finally{loading.value=false;}
}
async function save(){
  if(saving.value||!settings.value)return;
  saving.value=true;failed.value=false;notice.value='';
  try{
    settings.value=await requestJson(api,{method:'POST',body:JSON.stringify(settings.value)}) as Settings;
    notice.value='采集设置已保存。';
  }catch(error){failed.value=true;notice.value=error instanceof Error?error.message:'采集设置未确认，请重试。';}
  finally{saving.value=false;}
}
</script>

<style scoped>
.collection-settings{padding-block:10px;border-top:1px solid var(--cf-border);font-size:13px}
summary{cursor:pointer;font-weight:600}
label{display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap}
label:has(input[type=url]){display:grid}
input[type=url]{min-width:0;width:100%;box-sizing:border-box;padding:8px;border:1px solid var(--cf-border);background:var(--cf-surface);color:inherit;font:inherit;border-radius:0}
p{margin:8px 0;line-height:1.6}
</style>
