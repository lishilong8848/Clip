<template>
  <NCollapse class="v2-photo-location-settings">
    <NCollapseItem title="拍照地点解析设置" name="location">
      <NAlert v-if="error" type="error" :bordered="false" role="alert">{{error}}</NAlert>
      <div class="v2-photo-location-status" role="status">{{loading?'正在读取配置':saved?'已保存，下次照片定位时生效':settings?.configured?'已配置高德地点解析':'尚未配置地点解析'}}<NButton v-if="error" size="small" :loading="loading" @click="load">重试</NButton></div>
      <label for="photo-location-key">高德 Web 服务 Key</label>
      <NInput v-model:value="key" type="password" show-password-on="click" :maxlength="32" :disabled="loading||saving" placeholder="输入新的 Key，现有 Key 不回显" :input-props="{id:'photo-location-key',autocomplete:'off'}" />
      <NButton type="primary" size="small" :loading="saving" :disabled="!settings||!key.trim()||loading||saving" @click="save">保存地点解析</NButton>
    </NCollapseItem>
  </NCollapse>
</template>
<script setup lang="ts">
import {ref,onMounted,onBeforeUnmount} from 'vue';
import {NAlert,NButton,NCollapse,NCollapseItem,NInput} from 'naive-ui';
import {requestJson,type Dict} from '../client';
const settings=ref<Dict>(),key=ref(''),error=ref(''),loading=ref(false),saving=ref(false),saved=ref(false);
const emit=defineEmits<{busy:[value:boolean]}>();
let disposed=false;
const endpoint='/api/pre-shift-checks/location-settings';
async function load(){loading.value=true;error.value='';try{const result=await requestJson(endpoint);if(!disposed)settings.value=result;}catch(e){if(!disposed)error.value=(e as Error).message;}finally{loading.value=false;}}
async function save(){
  if(saving.value||!settings.value)return;
  if(!/^[a-f0-9]{32}$/i.test(key.value.trim())){error.value='请填写高德 Web 服务类型的32位 Key。';return;}
  saving.value=true;emit('busy',true);error.value='';saved.value=false;
  try{const result=await requestJson(endpoint,{method:'PUT',body:JSON.stringify({key:key.value.trim(),revision:settings.value.revision})});if(!disposed){settings.value=result;key.value='';saved.value=true;}}catch(e){if(!disposed)error.value=(e as Error).message;}finally{saving.value=false;emit('busy',false);}
}
onMounted(load);
onBeforeUnmount(()=>{disposed=true;key.value='';});
</script>
<style>
.v2-photo-location-settings{margin-top:16px;border-top:1px solid var(--v2-border,#dce2ea);padding-top:12px}.v2-photo-location-settings .n-collapse-item__content-inner{display:flex;flex-direction:column;gap:8px}.v2-photo-location-settings label,.v2-photo-location-status{font-size:13px}.v2-photo-location-status{color:var(--v2-muted,#637184);display:flex;gap:8px;align-items:center}.v2-photo-location-settings .n-button{align-self:flex-end;min-height:36px}@media(max-width:700px){.v2-photo-location-settings .n-button{min-height:44px}}
</style>
