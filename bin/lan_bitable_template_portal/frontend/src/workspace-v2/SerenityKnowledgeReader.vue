<script setup lang="ts">
import {nextTick, ref, watch} from 'vue';
import {NModal} from 'naive-ui';
import {ChevronLeft, ChevronRight, Download, X} from 'lucide-vue-next';
import type {LibraryController} from './SerenityKnowledge.vue';
const props = defineProps<{model: LibraryController}>();
const model = props.model;
const body = ref<HTMLElement | null>(null);
watch(() => model.sections, async () => {
  await nextTick();
  if (!body.value) return;
  const active = body.value.querySelector<HTMLElement>('[aria-current=true]');
  if (active) active.scrollIntoView({block:'nearest'});
  else body.value.scrollTop = 0;
}, {flush:'post'});
function replaceFile() {
  if (!model.detail) return;
  const doc = model.detail;
  model.closeDetail();
  model.pickReplace(doc);
}
</script>
<template>
  <NModal :show="!!model.detail" :mask-closable="true" @update:show="(value: boolean) => { if (!value) model.closeDetail() }">
    <div v-if="model.detail" class="sk-reader v2-kb-reader" role="dialog" aria-modal="true" :aria-label="model.detail.name">
      <header><div><small>{{ model.detail.historical ? '历史版本' : '文档原文' }}<template v-if="model.detail.view_version || model.detail.active_version"> · v{{ model.detail.view_version || model.detail.active_version }}</template></small>
        <h2>{{ model.detail.name }}</h2></div><button aria-label="关闭预览" @click="model.closeDetail"><X :size="19"/></button></header>
      <div class="reader-tools" v-if="!model.detailLoading && !model.readerError">
        <button class="primary" :disabled="model.actionBusy" @click="model.downloadActive(model.detail)"><Download :size="15"/>下载原文件</button>
        <template v-if="model.detail.can_edit && !model.detail.historical">
          <button :disabled="model.actionBusy || model.uploadBusy" @click="replaceFile">替换文件</button>
          <button v-if="model.detail.status==='failed'" :disabled="model.actionBusy" @click="model.retryTarget = model.detail">重新索引</button>
          <button :disabled="model.actionBusy" @click="model.deleteTarget = model.detail">移入回收站</button>
        </template>
      </div>
      <div ref="body" class="reader-body kb-sections" :aria-busy="model.detailLoading">
        <p v-if="model.detailLoading" role="status">正在读取原文…</p>
        <div v-else-if="model.readerError" role="alert">{{ model.readerError }} <button @click="model.retryRead">重新读取</button></div>
        <p v-else-if="!model.sections.length">{{ model.previewEmptyText }}</p>
        <article v-for="chunk in model.detailLoading || model.readerError ? [] : model.sections" :key="chunk.ordinal" class="kb-section" :aria-current="model.focusedOrdinal===chunk.ordinal">
          <small>片段 {{ chunk.ordinal+1 }}<template v-if="chunk.location"> · {{ chunk.location }}</template></small><p>{{ chunk.text }}</p>
        </article>
      </div>
      <footer><span>{{ model.detailTotal }} 个片段</span><button aria-label="上一页片段" :disabled="model.detailLoading || model.detailPage<=1" @click="model.readPage(model.detailPage-1)"><ChevronLeft :size="16"/></button>
        <span>{{ model.detailPage }} / {{ model.detailPages }}</span><button aria-label="下一页片段" :disabled="model.detailLoading || model.detailPage>=model.detailPages" @click="model.readPage(model.detailPage+1)"><ChevronRight :size="16"/></button></footer>
    </div>
  </NModal>
</template>
<style scoped>
.sk-reader {box-sizing:border-box;display:flex;flex-direction:column;gap:10px;width:min(780px,calc(100vw - 20px));max-height:min(90dvh,900px);margin:auto;padding:clamp(12px,2vw,20px);border-radius:18px;background:var(--soft-face,#f3f6f6);color:var(--v2-ink,#243f46);box-shadow:0 18px 60px #16303e33;}
header,footer,.reader-tools {display:flex;align-items:center;gap:8px;flex-wrap:wrap;} header {align-items:flex-start;flex-wrap:nowrap;} header div,footer span:first-child {flex:1;min-width:0;}
h2 {font-size:clamp(17px,3vw,22px);line-height:1.35;margin:2px 0;overflow-wrap:anywhere;} small,footer span {font-size:12px;}
button {display:inline-flex;align-items:center;gap:6px;min-height:34px;padding:6px 10px;font:inherit;font-size:13px;color:inherit;background:var(--soft-field,#e8eeee);border:1px solid #91a6aa70;border-radius:8px;cursor:pointer;}
button.primary {background:var(--v2-palette-accent);color:var(--v2-palette-on-accent);} button:disabled {opacity:.5;cursor:default;} button:focus-visible {outline:3px solid var(--v2-blue,#4399c1);}
.reader-body {overflow:auto;overscroll-behavior:contain;min-height:0;padding:0 5px;scrollbar-gutter:stable;} article {padding:12px;border-top:1px solid #91a6aa50;} article[aria-current=true] {background:var(--v2-palette-selection,#deefe6);border-inline-start:3px solid #228768;} article p {white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.75;margin:4px 0;font-size:15px;}
</style>
