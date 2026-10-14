<template>
  <NModal :show="open" preset="dialog" :type="tone==='danger'?'error':'warning'" :title="title" :positive-text="confirmLabel||'确认'" negative-text="继续填写" :positive-button-props="{type:tone==='danger'?'error':'primary'}" :style="{width:'min(460px,calc(100vw - 28px))'}" @positive-click="emit('resolve',true)" @negative-click="emit('resolve',false)" @update:show="value=>{if(!value)emit('resolve',false)}">
    <p>{{message}}</p><ul v-if="details?.length"><li v-for="line in details" :key="line">{{line}}</li></ul>
  </NModal>
</template>
<script setup lang="ts">
import {NModal} from 'naive-ui';
defineProps<{open:boolean;title:string;message:string;details?:string[];tone?:string;kicker?:string;confirmLabel?:string}>();
const emit=defineEmits<{resolve:[value:boolean]}>();
</script>
<style scoped>
p,li{font-size:14px;line-height:1.6;color:#52636a}p{margin:4px 0 10px}ul{padding-left:18px;margin:8px 0}
</style>
