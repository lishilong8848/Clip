<template><EventNoticeRawBoard :scope="scope" :active="active" /></template>
<script setup lang="ts">
import EventNoticeRawBoard from './EventNoticeRawBoard.vue';
defineProps<{scope:string;active?:boolean}>();
</script>
