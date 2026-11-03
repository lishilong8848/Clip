<template>
  <ToolSelfCheckPage v-if="view === 'tool-self-check'" :scope="scope" />
  <RiskInspectionPage v-else :scope="scope" :view="view" :release-id="releaseId" />
</template>

<script setup lang="ts">
import { defineAsyncComponent } from 'vue';

defineProps<{ scope?: string; view?: string; releaseId?: string }>();
const RiskInspectionPage = defineAsyncComponent(() => import('./RiskInspectionPage.vue'));
const ToolSelfCheckPage = defineAsyncComponent(() => import('./ToolSelfCheckPage.vue'));
</script>
