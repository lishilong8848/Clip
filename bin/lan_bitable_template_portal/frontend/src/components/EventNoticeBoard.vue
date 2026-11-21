<template><EventNoticeRawBoard ref="board" :scope="scope" :active="active" :focus-event-id="focusEventId" /></template>
<script setup lang="ts">
import {ref} from 'vue';
import EventNoticeRawBoard from './EventNoticeRawBoard.vue';
defineProps<{scope:string;active?:boolean;focusEventId?:string}>();
const board=ref<InstanceType<typeof EventNoticeRawBoard>>();
defineExpose({guardNavigation:()=>board.value?.guardNavigation()!==false});
</script>
