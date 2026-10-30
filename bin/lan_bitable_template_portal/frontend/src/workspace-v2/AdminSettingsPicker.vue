<script setup lang="ts">
import {computed,inject,ref,type Ref} from 'vue';
import type {AdminTabKey} from '../adminToolsUtils';
const props=defineProps<{tabs:Array<{key:AdminTabKey;label:string;description:string;badge:string}>;activeTab:AdminTabKey;advancedVisible:boolean}>();
const selected=computed(()=>props.tabs.find(item=>item.key===props.activeTab));
const mobile=inject<Ref<boolean>>('workspace-mobile-navigation',ref(false));
const emit=defineEmits<{'select-tab':[tab:AdminTabKey]}>();
const options=computed(()=>[...props.tabs.map(item=>({value:item.key,label:item.label})),...(props.tabs.some(item=>item.key==='pressure')?[]:[{value:'pressure',label:'高级诊断'}])]);
function select(event:Event){const value=(event.target as HTMLSelectElement).value;if(options.value.some(item=>item.value===value))emit('select-tab',value as AdminTabKey);}
</script>
<template><header class="v2-settings-picker"><div><span>系统设置</span><h1>{{selected?.label||'账号与权限'}}</h1></div><select v-if="mobile" class="v2-mobile-settings-select" aria-label="设置分类" :value="activeTab" @change="select"><option v-for="item in options" :key="item.value" :value="item.value">{{item.label}}</option></select><slot name="admin-tools" /></header></template>
<style scoped>
.v2-settings-picker{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:4px 0 14px;border-bottom:1px solid var(--v2-border)}
span{font-size:12px;color:var(--v2-muted)}h1{font-size:20px;line-height:1.4;margin:4px 0 0}
.v2-mobile-settings-select{display:none;border:1px solid var(--v2-border);border-radius:8px;background:var(--v2-palette-surface);color:var(--v2-ink);font:inherit;min-height:40px;padding:6px 10px}
@media(max-width:760px),(hover:none) and (max-width:1024px){.v2-settings-picker{flex-wrap:wrap;gap:8px;padding-bottom:10px}.v2-settings-picker>div{min-width:0}h1{margin-top:2px;font-size:18px}.v2-mobile-settings-select{display:block;flex:1;min-width:140px;max-width:220px;min-height:44px;margin-left:auto}}
</style>
