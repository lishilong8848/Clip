<template>
  <div class="assistant-rich-reply" v-html="html" @click="openLink" />
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { renderLighthouseReply } from '../lighthouseReply';
import { navigate } from '../navigation';

const props = defineProps<{ text: string }>();
const html = computed(() => renderLighthouseReply(props.text));
function openLink(event: MouseEvent): void {
  if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || !(event.target instanceof Element)) return;
  const link = event.target.closest<HTMLAnchorElement>('a[href]');
  if (!link || link.target === '_blank' || new URL(link.href).origin !== window.location.origin) return;
  event.preventDefault();
  navigate(link.href);
}
</script>

<style scoped>
.assistant-rich-reply { min-width: 0; max-width: 100%; font-size: 14px; line-height: 1.75; overflow-wrap: anywhere; }
.assistant-rich-reply :deep(p) { margin: 0 0 10px; white-space: normal; }
.assistant-rich-reply :deep(:first-child) { margin-top: 0; }
.assistant-rich-reply :deep(:last-child) { margin-bottom: 0; }
.assistant-rich-reply :deep(strong), .assistant-rich-reply :deep(b) { font-weight: 700; color: inherit; }
.assistant-rich-reply :deep(h1), .assistant-rich-reply :deep(h2), .assistant-rich-reply :deep(h3), .assistant-rich-reply :deep(h4), .assistant-rich-reply :deep(h5), .assistant-rich-reply :deep(h6) { margin: 18px 0 8px; font-weight: 700; line-height: 1.5; color: var(--lh-charcoal-strong); }
.assistant-rich-reply :deep(h1) { font-size: 19px; }
.assistant-rich-reply :deep(h2) { font-size: 17px; }
.assistant-rich-reply :deep(h3), .assistant-rich-reply :deep(h4), .assistant-rich-reply :deep(h5), .assistant-rich-reply :deep(h6) { font-size: 15px; }
.assistant-rich-reply :deep(ul), .assistant-rich-reply :deep(ol) { margin: 8px 0 12px; padding-left: 24px; }
.assistant-rich-reply :deep(li) { margin: 4px 0; padding-left: 2px; }
.assistant-rich-reply :deep(li > p) { margin-bottom: 4px; }
.assistant-rich-reply :deep(blockquote) { margin: 10px 0; padding: 3px 0 3px 12px; border-left: 3px solid var(--lh-accent); color: var(--lh-muted); }
.assistant-rich-reply :deep(code) { padding: 2px 5px; border-radius: 4px; background: var(--lh-surface-hover); color: var(--lh-accent-strong); font-size: 12px; }
.assistant-rich-reply :deep(pre) { max-width: 100%; overflow-x: auto; padding: 12px; margin: 12px 0; border: 1px solid var(--lh-border); border-radius: 6px; background: var(--lh-surface-subtle); white-space: pre; overflow-wrap: normal; }
.assistant-rich-reply :deep(pre code) { padding: 0; background: transparent; line-height: 1.65; color: inherit; }
.assistant-rich-reply :deep(table) { display: block; max-width: 100%; overflow-x: auto; border-collapse: collapse; margin: 12px 0; font-size: 13px; }
.assistant-rich-reply :deep(th), .assistant-rich-reply :deep(td) { border: 1px solid var(--lh-border-strong); padding: 6px 10px; min-width: 70px; text-align: left; }
.assistant-rich-reply :deep(th) { color: var(--lh-charcoal-strong); background: var(--lh-surface-hover); font-weight: 600; }
.assistant-rich-reply :deep(a[href]) { color: var(--lh-accent-strong); text-decoration: underline; text-underline-offset: 3px; }
.assistant-rich-reply :deep(a[href]:focus-visible) { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
.assistant-rich-reply :deep(hr) { border: 0; border-top: 1px solid var(--lh-border); margin: 14px 0; }
</style>
