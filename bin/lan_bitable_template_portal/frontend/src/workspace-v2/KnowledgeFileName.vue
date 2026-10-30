<template>
  <span class="v2-kb-file-label">
    <strong ref="titleElement" class="v2-kb-file-name" :title="name">{{ name }}</strong>
    <span class="v2-kb-file-identifiers">
      <span v-if="nameTail" class="v2-kb-file-tail" :title="name" aria-hidden="true">末尾 …{{ nameTail }}</span>
      <span>{{ fileType }}</span>
      <span v-if="version !== undefined && version !== null && version !== ''">版本 {{ version }}</span>
    </span>
  </span>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';

const props = defineProps<{
  name: string;
  version?: string | number | null;
}>();

const titleElement = ref<HTMLElement | null>(null);
const titleClipped = ref(false);
let resizeObserver: ResizeObserver | undefined;

function measureTitle(): void {
  const element = titleElement.value;
  titleClipped.value = !!element && element.scrollHeight > element.clientHeight;
}

onMounted(() => {
  resizeObserver = new ResizeObserver(measureTitle);
  if (titleElement.value) resizeObserver.observe(titleElement.value);
  measureTitle();
});
watch(() => props.name, measureTitle, { flush: 'post' });
onBeforeUnmount(() => resizeObserver?.disconnect());

const fileType = computed(() => props.name.match(/\.([a-z\d]{1,10})$/i)?.[1].toUpperCase() || '文件');
// Keep the distinguishing filename ending visible even when the two-line title clips.
const nameTail = computed(() => {
  return titleClipped.value ? Array.from(props.name).slice(-16).join('') : '';
});
</script>
