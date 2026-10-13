<template>
  <section ref="surface" class="v2-native-surface">
    <WorkbenchLitePage v-if="!publicPage" :search="search" :active="active" />
    <template v-else><NSpin v-if="loading" class="v2-native-loading" /><iframe :src="nativeUrl" title="工单执行" @load="loaded" /></template>
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }}</NAlert>
  </section>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { NAlert, NSpin } from 'naive-ui';
import WorkbenchLitePage from '../components/WorkbenchLitePage.vue';
import nativeStyles from './native.css?inline';
import interactionStyles from './interactions.css?inline';
import { installActionMotion } from './actionMotion';
const props=withDefaults(defineProps<{url:string;publicPage?:boolean;active?:boolean}>(),{active:true});
const origin=window.location.origin,surface=ref<HTMLElement>(),loading=ref(true),error=ref('');
const search=computed(()=>new URL(props.url,origin).searchParams.toString());
const nativeUrl=computed(()=>{const url=new URL(props.url,origin);url.searchParams.set('_assistant_frame','1');return url.pathname+url.search+url.hash;});
let observer:MutationObserver|undefined;
const tracked=new Set<HTMLIFrameElement>();
const cleanup=new Map<HTMLIFrameElement,()=>void>();
function loaded(){loading.value=false;applyTheme();}
function applyTheme(){
  for(const frame of surface.value?.querySelectorAll('iframe')||[]){
    if(!tracked.has(frame)){tracked.add(frame);frame.addEventListener('load',loaded);}
    try{const doc=frame.contentDocument;if(!doc?.body||doc.getElementById('workspace-v2-native-theme'))continue;
      // Native forms keep their handlers/tokens intact; the theme exists only inside this V2-owned surface.
      doc.documentElement.classList.add('workspace-v2-native');const style=doc.createElement('style');style.id='workspace-v2-native-theme';style.textContent=nativeStyles+'\n'+interactionStyles;doc.head.append(style);
      cleanup.get(frame)?.();cleanup.set(frame,installActionMotion(doc));
    }catch{error.value='业务页面暂不可访问，请核对登录或工单链接。';}
  }
}
onMounted(()=>{applyTheme();observer=new MutationObserver(applyTheme);if(surface.value)observer.observe(surface.value,{childList:true,subtree:true});});
onBeforeUnmount(()=>{observer?.disconnect();for(const frame of tracked)frame.removeEventListener('load',loaded);for(const stop of cleanup.values())stop();cleanup.clear();tracked.clear();});
</script>
