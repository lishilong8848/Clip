<template>
  <fieldset v-if="available.length||selected.length||error" class="notice-dingtalk" :disabled="disabled">
    <legend>钉钉群推送 <small>可多选，不选则不推送钉钉</small></legend>
    <p v-if="error" role="alert">{{ error }} <button type="button" @click="load">重新读取</button></p>
    <div v-else class="notice-dingtalk-options">
      <label v-for="robot in available" :key="robot.id" :title="robot.remark"><input type="checkbox" :checked="selected.includes(robot.id)" @change="toggle(robot.id,($event.target as HTMLInputElement).checked)" />{{ robot.name }}<small v-if="robot.remark">{{ robot.remark }}</small></label>
      <small v-if="!available.length">{{ loading?'正在读取机器人…':'当前没有适用的已启用机器人' }}</small>
    </div>
    <p v-if="selected.some(id=>!available.some(robot=>robot.id===id))" role="alert">已选机器人有配置变化，请重新核对选择。<button type="button" @click="emit('update:modelValue',selected.filter(id=>available.some(robot=>robot.id===id)))">移除不适用选择</button></p>
  </fieldset>
</template>
<script setup lang="ts">
import {computed,ref,watch,onBeforeUnmount} from 'vue';
import {requestJson,type Dict} from '../workspace-v2/client';
const props=defineProps<{modelValue?:string[];workType:string;noticeType:string;buildings?:string[];disabled?:boolean}>();
const emit=defineEmits<{ 'update:modelValue':[value:string[]]; 'selected-names':[value:string[]] }>();
const robots=ref<Dict[]>([]),error=ref(''),loading=ref(false),selected=computed(()=>props.modelValue||[]);
let generation=0;
const available=computed(()=>robots.value.filter(r=>(!r.types.length||r.types.includes(['上电通告','下电通告'].includes(props.noticeType)?'上下电通告':props.noticeType))&&(!r.scopes.length||(props.buildings||[]).some(s=>r.scopes.includes(s)))));
async function load(){const seq=++generation;loading.value=true;error.value='';try{const data=await requestJson('/api/workbench/dingtalk-options?work_type='+encodeURIComponent(props.workType));if(seq===generation)robots.value=data.robots||[];}catch(e){if(seq===generation)error.value=e instanceof Error?e.message:'机器人读取失败';}finally{if(seq===generation)loading.value=false;}}
function toggle(id:string,checked:boolean){emit('update:modelValue',checked?[...new Set([...selected.value,id])]:selected.value.filter(value=>value!==id));}
watch(()=>props.workType,()=>{robots.value=[];void load();},{immediate:true});
watch([selected,available],()=>emit('selected-names',selected.value.map(id=>available.value.find(robot=>robot.id===id)?.name||'待核验的机器人')),{immediate:true});
onBeforeUnmount(()=>generation++);
</script>
<style scoped>
.notice-dingtalk{border:1px solid var(--border-color,#ccd6e2);border-radius:8px;padding:8px 12px;margin:8px 0;color:inherit;min-width:0}.notice-dingtalk legend{font-weight:600}.notice-dingtalk small{font-weight:400;opacity:.75;font-size:12px}.notice-dingtalk-options{display:flex;gap:8px 18px;flex-wrap:wrap}.notice-dingtalk label{display:flex;gap:6px;align-items:center;flex-wrap:wrap;min-height:36px;cursor:pointer}.notice-dingtalk label small{max-width:220px;overflow-wrap:anywhere}.notice-dingtalk input{width:18px;height:18px}.notice-dingtalk p{font-size:13px;margin:4px 0}.notice-dingtalk button{color:inherit}
</style>
