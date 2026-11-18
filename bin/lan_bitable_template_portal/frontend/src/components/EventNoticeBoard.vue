<template><EventNoticeRawBoard :scope="scope" :active="active" :focus-event-id="focusEventId" /></template>
<script setup lang="ts">
import EventNoticeRawBoard from './EventNoticeRawBoard.vue';
defineProps<{scope:string;active?:boolean;focusEventId?:string}>();
</script>
