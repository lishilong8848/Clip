<template>
  <NConfigProvider><NDialogProvider>
    <form @submit.prevent="submitted++">
      <label>数量<select v-model.number="count" name="count" required><option value="">请选择</option><option value="1">一个</option><option value="2">两个</option></select></label>
      <label>对象<select v-model="object" name="object"><option v-for="item in objects" :value="item">{{item.name}}</option></select></label>
      <fieldset :disabled="locked"><label>状态<select v-model="state"><option value="off">未开始</option><option value="on">进行中</option></select></label></fieldset>
      <button type="submit">保存</button><button type="button" @click="count=1">恢复数量</button><button type="button" @click="locked=!locked">锁定切换</button>
      <button type="button" @click="objects.push({name:'丙'})">新增选项</button><button type="reset">重置</button>
    </form>
    <output id="result">{{JSON.stringify({count,object,state,submitted})}}</output>
    <dialog ref="dialog"><div class="lighthouse-layer" style="--lh-surface:#303339;--lh-surface-subtle:#303339;--lh-charcoal:#eeeeee;--lh-accent:#65b8ff;--lh-accent-soft:#244a66;--lh-input-border:#70757f"><label>时段<select v-model="slot"><option value="am">08:00</option><option value="pm">17:00</option></select></label></div><button type="button" @click="dialog?.close()">关闭</button></dialog>
    <button type="button" @click="dialog?.showModal()">打开对话框</button>
    <NoticeRelations :form="form" :locked="false" @bound="bound=$event" />
    <output id="bound">{{JSON.stringify(bound)}}</output>
  </NDialogProvider></NConfigProvider>
</template>
<script setup lang="ts">
import {ref} from 'vue';
import {NConfigProvider,NDialogProvider} from 'naive-ui';
import NoticeRelations from '../../src/workspace-v2/NoticeRelations.vue';
const count=ref<number|string>(''),objects=ref([{name:'甲'},{name:'乙'}]),object=ref(objects.value[0]),locked=ref(false),state=ref('off'),slot=ref('am'),submitted=ref(0),dialog=ref<HTMLDialogElement>(),bound=ref({});
const form:any={values:{scope:'E',work_type:'maintenance',title:'测试通告',target_record_id:'existing',active_item_id:'active'},detailMode:'ongoing'};
</script>
<style>body{margin:16px;font-family:Arial,sans-serif}.workspace-v2{--v2-ink:#222;--v2-blue:#0071e3;--v2-border:#ddd;--v2-muted:#666}form{display:flex;gap:10px;flex-wrap:wrap}label{display:grid;width:170px;gap:6px}fieldset{padding:0;border:0}dialog{width:300px}.v2-notice-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.v2-notice-binding{display:flex}.v2-notice-relations{max-width:640px;margin-top:20px}output{display:block;overflow-wrap:anywhere}</style>
