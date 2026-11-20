<script setup lang="ts">
import { NSelect } from 'naive-ui';
defineProps<{r:Record<string,any>}>();
</script>
<template>
  <section class="td-stack" aria-label="工具责任配置">
    <p v-if="r.error" class="td-notice td-error" role="alert">{{ r.error }} <button @click="r.load()">重试</button></p>
    <p v-if="r.message" role="status" class="td-notice">{{ r.message }}</p>
    <label class="td-field">库存工具<NSelect v-model:value="r.selection" :options="r.assetOptions" :loading="r.loading" :disabled="r.saving" filterable clearable placeholder="按楼栋、名称或型号查找"/></label>
    <template v-if="r.asset && r.form"><p class="td-muted">维护周期 {{ r.asset.source_self_cycle || '未设置' }} · 送检周期 {{ r.asset.source_external_cycle || '未设置' }}</p>
      <div class="td-row"><label class="td-field supervisor">专业主管<NSelect v-model:value="r.form.supervisor" :options="r.supervisorOptions" :disabled="r.saving" filterable clearable placeholder="选择本楼主管"/></label>
      <button class="td-primary" :disabled="r.saving" @click="r.saveTool()">保存负责人</button></div></template>
    <p v-if="r.data" class="td-muted">报废审批人 · {{ r.data.scrap_approvers.join('、') || '暂无配置' }}</p>
  </section>
</template>
<style scoped>.supervisor{flex:1 1 180px}</style>
