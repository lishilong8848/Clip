<script setup lang="ts">
import type { Dict } from '../api/client';
import { Download, Eye, FileText } from 'lucide-vue-next';
const props = defineProps<{ files: Dict[]; safeUrl: (value: unknown) => string }>();
const emit = defineEmits<{ preview: [file: Dict] }>();
</script>
<template>
  <div v-if="props.files?.length" class="ld-files output-files">
    <span v-for="(file, index) in files" :key="file.id || index" class="ld-file">
      <FileText aria-hidden="true" :size="15"/>
      <template v-if="safeUrl(file.url)">
        <a :href="safeUrl(file.url)" :download="file.name" target="_blank" rel="noopener"><img v-if="file.is_image" loading="lazy" :src="safeUrl(file.url)" alt="">{{ file.name || '附件' }} <Download :size="13" aria-hidden="true"/></a>
        <button type="button" :aria-label="'预览'+file.name" @click="emit('preview', file)"><Eye :size="14" aria-hidden="true"/></button>
      </template><span v-else>{{ file.name || '附件暂不可用' }}</span>
    </span>
  </div>
</template>
<style src="./lighthouseDesk.css"></style>
