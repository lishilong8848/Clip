<template>
  <div v-if="files.length" class="lh-downloads" aria-label="生成文件">
    <a v-for="file in visibleFiles" :key="file.url" :href="file.url" target="_blank" rel="noopener" :title="file.name">
      <FileText :size="17" aria-hidden="true" /><span>{{ file.name }}</span><Download :size="16" aria-hidden="true" />
    </a>
    <button v-if="files.length > 5" type="button" @click="expanded = !expanded"><ChevronUp v-if="expanded" :size="15" /><ChevronDown v-else :size="15" />{{ expanded ? '收起文件' : `更多文件（${files.length - 5}）` }}</button>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import { ChevronDown, ChevronUp, Download, FileText } from 'lucide-vue-next';

const props = defineProps<{ items?: { name: string; url: string }[] }>();
const expanded = ref(false);
function allowed(url: unknown): url is string {
  if (typeof url !== 'string') return false;
  return /^\/api\/drills\/[A-Za-z0-9_-]{1,160}\/download\?scope=[ABCDE]$/.test(url)
    || /^\/api\/cabinet-power\/exports\/[A-Za-z0-9_-]{1,160}\/download$/.test(url)
    || /^\/api\/critical-guard\/(images|workbooks)\/[A-Za-z0-9_-]{1,160}(\?v=[a-fA-F0-9]{1,64})?$/.test(url)
    || /^\/api\/critical-guard\/source-files\/[A-Za-z0-9_-]{1,160}$/.test(url)
    || /^\/api\/critical-guard\/tasks\/[A-Za-z0-9_-]{1,160}\/download\?sheet_type=(?:[A-Za-z0-9_.~-]|%[A-Fa-f0-9]{2}){1,400}$/.test(url)
    || /^\/api\/daily-tasks\/morning-meeting\/download\?date=\d{4}-\d{2}-\d{2}$/.test(url);
}
const files = computed(() => Array.isArray(props.items)
  ? [...new Map(props.items.filter(file => file && typeof file.name === 'string' && allowed(file.url)).map(file => [file.url, file])).values()]
  : []);
const visibleFiles = computed(() => expanded.value ? files.value : files.value.slice(0, 5));
</script>

<style scoped>
.lh-downloads { display: grid; gap: 6px; width: 100%; min-width: 0; margin-top: 8px; }
.lh-downloads a { display: flex; align-items: center; gap: 8px; min-width: 0; padding: 7px 0; color: inherit; border-bottom: 1px solid var(--lh-border); text-decoration: none; }
.lh-downloads span { flex: 1; min-width: 0; overflow-wrap: anywhere; font-size: 12px; line-height: 1.5; }
.lh-downloads svg { flex: 0 0 auto; }
.lh-downloads button { display: inline-flex; align-items: center; gap: 5px; justify-self: start; padding: 6px 0; border: 0; background: transparent; color: inherit; font: inherit; cursor: pointer; }
.lh-downloads button:hover { text-decoration: underline; }
.lh-downloads a:hover span { text-decoration: underline; }
</style>
