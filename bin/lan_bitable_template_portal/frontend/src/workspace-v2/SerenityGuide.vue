<script lang="ts">
export interface LibraryController {
  guideHtml: string;
  storageSeed: string;
  guideFrame: HTMLIFrameElement | null;
}
</script>
<script setup lang="ts">
import {inject,onBeforeUnmount,onMounted,toRef,watch} from 'vue';
import {pageContext} from './pageLayout';
import {nightMode,paletteId,palettes,saveNightMode} from './theme';
const props = defineProps<{model: LibraryController}>();
const model = props.model;
const guideFrame = toRef(model, 'guideFrame');
const account = inject(pageContext);
const openedOwner = account?.user.value.open_id;
const initialStorage = JSON.stringify({...JSON.parse(model.storageSeed),theme:nightMode.value?'dark':'light'});

function syncTheme() {
  const dark = nightMode.value;
  const colors = palettes[paletteId.value][dark ? 'dark' : 'light'];
  const palette = {page:colors.bg,panel:colors.surface,field:colors.soft,ink:colors.text,
    sub:colors.muted,line:colors.border,green:colors.accent,'on-accent':colors.onAccent};
  guideFrame.value?.contentWindow?.postMessage({kind:'clipflow-life-guide-theme',dark,palette}, '*');
}
function onThemeChange(event: MessageEvent) {
  if (event.source !== guideFrame.value?.contentWindow || event.data?.kind !== 'clipflow-life-guide-storage'
      || event.data.key !== 'theme' || !['light','dark'].includes(event.data.value)) return;
  if (openedOwner && openedOwner === account?.user.value.open_id) saveNightMode(String(openedOwner), event.data.value==='dark');
}
watch([nightMode,paletteId],syncTheme);
onMounted(()=>window.addEventListener('message',onThemeChange));
onBeforeUnmount(()=>window.removeEventListener('message',onThemeChange));
</script>
<template>
  <section class="serenity-guide life-guide-page" aria-label="参考人生指南">
    <iframe ref="guideFrame" title="参考人生指南" :srcdoc="model.guideHtml" :name="initialStorage"
      sandbox="allow-scripts allow-popups allow-downloads" referrerpolicy="no-referrer" @load="syncTheme"/>
  </section>
</template>
<style scoped>
:global(.v2-shell > .v2-main:has(> .serenity-guide)) {display:flex;flex-direction:column;min-height:0;overflow:hidden;}
:global(.v2-shell.is-mobile-workspace:has(> .v2-main > .serenity-guide)) {display:flex;flex-direction:column;height:100dvh;min-height:0;}
:global(.v2-shell.is-mobile-workspace > .v2-main:has(> .serenity-guide)) {flex:1;}
.serenity-guide {display:flex;flex:1;align-self:stretch;width:100%;min-height:min(68vh,520px);overflow:hidden;}
:global(.v2-shell > .v2-main > .serenity-guide) {min-height:0;}
.serenity-guide iframe {display:block;flex:1;width:100%;height:100%;min-height:inherit;border:0;background:var(--v2-bg);}
</style>
