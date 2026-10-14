<template><NModal :show="open" preset="dialog" :title="title" :content="message" :type="tone==='danger'?'error':tone==='warning'?'warning':'info'" :positive-text="confirmLabel || '确认'" :negative-text="hideCancel?undefined:cancelLabel || '取消'" :mask-closable="false" :on-positive-click="()=>resolve(true)" :on-negative-click="()=>resolve(false)" @update:show="value=>{if(!value)resolve(false)}"><template v-if="details?.length" #default><ul><li v-for="detail in details" :key="detail">{{detail}}</li></ul></template></NModal></template>
<script setup lang="ts">
import {NModal} from 'naive-ui';
defineProps<{open:boolean;title:string;message:string;tone?:'primary'|'danger'|'warning';confirmLabel?:string;cancelLabel?:string;hideCancel?:boolean;details?:string[]}>();
const emit=defineEmits<{resolve:[value:boolean]}>();
function resolve(value:boolean){emit('resolve',value);return false;}
</script>
