<template><NSelect v-if="context?.changeScope && options.length>1" :value="context.scope.value" :options="options" class="v2-local-scope" size="small" aria-label="当前楼栋" :to="true" @update:value="context.changeScope" /></template>
<script setup lang="ts">
import {computed,inject} from 'vue';
import {NSelect} from 'naive-ui';
import {pageContext} from './pageLayout';
const context=inject(pageContext,null);
const options=computed(()=>context?.scopes.value.map(value=>({value,label:value==='ALL'?'全部楼栋':value==='CAMPUS'?'园区':value==='110'?'110站':value+'楼'}))||[]);
</script>
<style>.workspace-v2 .v2-local-scope{width:130px;min-width:105px;flex:none}@media(max-width:760px){.workspace-v2 .v2-local-scope{width:112px}}</style>
