<template>
  <ToolSelfCheckPage v-if="view === 'tool-self-check'" :scope="scope" :tool-view="toolView"
    :tool-session="toolSession" :tool-asset="toolAsset" />
  <RiskInspectionPage v-else :scope="scope" :view="view" :release-id="releaseId" />
</template>

<script setup lang="ts">
import { defineAsyncComponent } from 'vue';

defineProps<{ scope?: string; view?: string; releaseId?: string; toolView?: string; toolSession?: string; toolAsset?: string }>();
const RiskInspectionPage = defineAsyncComponent(() => import('./RiskInspectionPage.vue'));
const ToolSelfCheckPage = defineAsyncComponent(() => import('./ToolSelfCheckPage.vue'));
</script>
