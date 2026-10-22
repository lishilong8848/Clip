<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref } from 'vue';
import { ImageOff, Maximize2, Scan, X, ZoomIn, ZoomOut } from 'lucide-vue-next';
import type { Dict } from '../api/client';
import { acquireModal } from '../modalState';

const props = withDefaults(defineProps<{
  batchId: string; images?: Dict[]; sourceImages?: Dict[]; imageIds?: string[]; editable?: boolean; disabled?: boolean;
}>(), { images: () => [], sourceImages: () => [], imageIds: () => [], editable: false, disabled: false });
const emit = defineEmits<{ 'update:imageIds': [ids: string[]] }>();
const modeLabels: Record<string, string> = { ocr: '文字识别', vision: '图片识别，需核对', unreadable: '未能识别' };
function imageUrl(image: Dict) {
  if (!/^[a-f0-9]{64}$/.test(image.id)) return '';
  const url = `/api/learning/sharing/${encodeURIComponent(props.batchId)}/images/${image.id}`;
  return image.url === url ? url : '';
}
const sources = computed(() => props.sourceImages.filter(image => imageUrl(image)));
function select(image: Dict, checked: boolean) {
  if (!props.editable || props.disabled || !sources.value.some(item => item.id === image.id)) return;
  const ids = checked ? [...new Set([...props.imageIds, image.id])] : props.imageIds.filter(id => id !== image.id);
  if (ids.length <= 3) emit('update:imageIds', ids);
}

const dialog = ref<HTMLDialogElement | null>(null), viewport = ref<HTMLElement | null>(null);
const active = ref<Dict | null>(null), failed = ref(false), loaded = ref(false), scale = ref(1), fit = ref(true);
const natural = ref({ width: 0, height: 0 }), thumbnailErrors = ref(new Set<string>());
let modal: ReturnType<typeof acquireModal> | undefined, returnFocus: HTMLElement | null = null;
const imageStyle = computed(() => fit.value ? {} : { width: `${natural.value.width * scale.value}px`, height: `${natural.value.height * scale.value}px`, maxWidth: 'none', maxHeight: 'none', flexShrink: 0 });
async function preview(image: Dict) {
  if (!imageUrl(image)) return;
  returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  active.value = image; failed.value = false; loaded.value = false; fit.value = true; scale.value = 1;
  await nextTick();
  if (!dialog.value || !active.value) return;
  modal = acquireModal();
  dialog.value.showModal();
  dialog.value.querySelector<HTMLButtonElement>('.question-image-close')?.focus();
}
function release() { modal?.release(); modal = undefined; }
function closed() {
  active.value = null; release();
  if (returnFocus?.isConnected) returnFocus.focus();
  returnFocus = null;
}
function close() { dialog.value?.close(); }
function imageLoaded(event: Event) {
  const image = event.target as HTMLImageElement;
  natural.value = { width: image.naturalWidth, height: image.naturalHeight };
  loaded.value = true;
}
function zoom(factor: number) {
  if (!loaded.value || !viewport.value) return;
  const current = fit.value ? Math.min(1, viewport.value.clientWidth / natural.value.width, viewport.value.clientHeight / natural.value.height) : scale.value;
  scale.value = Math.max(0.1, Math.min(4, current * factor)); fit.value = false;
}
function originalSize() { scale.value = 1; fit.value = false; }
onBeforeUnmount(() => { active.value = null; dialog.value?.close(); release(); });
</script>

<template>
  <div v-if="images.length || (editable && sources.length)" class="question-images">
    <div v-if="images.length" class="question-image-strip" aria-label="题目图片">
      <figure v-for="(image, index) in images" :key="image.id">
        <button v-if="imageUrl(image)" type="button" class="question-image-thumb" :aria-label="`查看原图：${image.label || '题图' + (index + 1)}`" @click="preview(image)">
          <img v-if="!thumbnailErrors.has(image.id)" :src="imageUrl(image)" :alt="image.label || '题目图片'" loading="lazy" @error="thumbnailErrors.add(image.id)" />
          <span v-else class="question-image-error"><ImageOff :size="22" aria-hidden="true" />图片暂不可用</span>
        </button>
        <span v-else class="question-image-error">图片地址无效</span>
        <figcaption>{{ image.label || '题图' + (index + 1) }}</figcaption>
      </figure>
    </div>
    <details v-if="editable && sources.length" class="question-image-picker">
      <summary>选择题目图片（已选 {{ imageIds.length }} / 3）</summary>
      <div class="question-image-sources">
        <figure v-for="image in sources" :key="image.id">
          <button type="button" class="question-image-thumb" :aria-label="`查看源图：${image.label}`" @click="preview(image)">
            <img v-if="!thumbnailErrors.has(image.id)" :src="imageUrl(image)" :alt="image.label || '材料原图'" loading="lazy" @error="thumbnailErrors.add(image.id)" />
            <span v-else class="question-image-error"><ImageOff :size="22" aria-hidden="true" />图片暂不可用</span>
          </button>
          <label><input type="checkbox" :checked="imageIds.includes(image.id)" :disabled="disabled || (!imageIds.includes(image.id) && imageIds.length >= 3)" :aria-label="`绑定图片：${image.label}`" @change="select(image, ($event.target as HTMLInputElement).checked)" /><span>{{ image.label }}</span></label>
          <small>{{ modeLabels[image.mode] || '未能识别' }}</small>
          <details v-if="image.text" class="question-image-text"><summary>识别文本</summary><p>{{ image.text }}</p></details>
        </figure>
      </div>
    </details>
    <Teleport to="body">
      <dialog ref="dialog" class="question-image-preview" aria-label="题目原图预览" @cancel.prevent="close" @close="closed" @click.self="close">
        <header><strong>{{ active?.label || '题目原图' }}</strong><button type="button" class="question-image-close" title="关闭原图" aria-label="关闭原图" @click="close"><X :size="22" aria-hidden="true" /></button></header>
        <div class="question-image-tools" role="group" aria-label="图片缩放">
          <button type="button" title="缩小" aria-label="缩小图片" :disabled="!loaded || failed || (!fit && scale <= .1)" @click="zoom(.8)"><ZoomOut :size="20" aria-hidden="true" /></button>
          <button type="button" title="放大" aria-label="放大图片" :disabled="!loaded || failed || (!fit && scale >= 4)" @click="zoom(1.25)"><ZoomIn :size="20" aria-hidden="true" /></button>
          <button type="button" title="原大小" aria-label="原大小查看" :aria-pressed="!fit && scale === 1" :disabled="!loaded || failed" @click="originalSize"><Scan :size="20" aria-hidden="true" /></button>
          <button type="button" title="适应窗口" aria-label="适应窗口" :aria-pressed="fit" :disabled="!loaded || failed" @click="fit = true"><Maximize2 :size="20" aria-hidden="true" /></button>
          <output>{{ fit ? '适应窗口' : Math.round(scale * 100) + '%' }}</output>
        </div>
        <div ref="viewport" class="question-image-viewport" tabindex="0" aria-label="原图内容" :class="{ fitted: fit }">
          <p v-if="failed" role="alert">图片暂时无法读取，请关闭后重试。</p>
          <span v-else-if="!loaded && active" role="status" class="question-image-loading">正在读取原图…</span>
          <img v-if="active && !failed" :src="imageUrl(active)" :alt="active.label || '题目原图'" :style="imageStyle" @load="imageLoaded" @error="failed = true" />
        </div>
      </dialog>
    </Teleport>
  </div>
</template>

<style scoped>
.question-images,.question-image-preview {
  --qi-text:var(--v2-ink,var(--cf-text,#23354b));
  --qi-surface:var(--cf-surface,#fff);
  --qi-soft:var(--cf-surface-soft,#f3f6fa);
  --qi-border:var(--v2-border,var(--cf-border,#cad7e5));
  --qi-muted:var(--v2-muted,var(--cf-muted,#52667d));
  --qi-accent:var(--v2-blue,var(--cf-brand-blue,#28559a));
  color:var(--qi-text);font-family:inherit;font-size:14px;line-height:1.6;
}
.question-images{min-width:0;margin:12px 0}
.question-image-strip,.question-image-sources{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(140px,100%),1fr));gap:12px;min-width:0}
.question-image-strip{grid-template-columns:repeat(3,minmax(0,1fr));max-width:600px}
.question-images figure{min-width:0;margin:0}
.question-images figcaption{font-size:13px;overflow-wrap:anywhere;margin-top:4px}
.question-image-thumb{display:flex;align-items:center;justify-content:center;padding:0;width:100%;aspect-ratio:4/3;border:1px solid var(--qi-border);border-radius:6px;background:var(--qi-soft);color:inherit;cursor:zoom-in;overflow:hidden}
.question-image-thumb img{width:100%;height:100%;object-fit:contain}
.question-image-error{display:flex;align-items:center;justify-content:center;gap:6px;flex-wrap:wrap;font-size:12px;min-height:60px;padding:8px;overflow-wrap:anywhere}
.question-image-picker{margin-top:10px}
.question-images summary{cursor:pointer;min-height:44px;align-content:center;overflow-wrap:anywhere}
.question-image-sources{padding-top:8px}
.question-image-sources label{display:flex;align-items:flex-start;gap:8px;min-height:44px;margin:0;padding:8px 0;cursor:pointer}
.question-image-sources label span{min-width:0;overflow-wrap:anywhere}
.question-image-sources input{width:18px;height:18px;margin:3px 0 0;flex:none;accent-color:var(--qi-accent)}
.question-image-sources small{font-size:12px;color:var(--qi-muted)}
.question-image-text{font-size:12px}
.question-image-text p{white-space:pre-wrap;overflow-wrap:anywhere;margin:0 0 8px;max-height:180px;overflow:auto}
.question-images :is(button,input,summary):focus-visible,.question-image-preview :is(button,[tabindex]):focus-visible{outline:2px solid var(--qi-accent);outline-offset:2px}
.question-image-preview{box-sizing:border-box;width:min(1100px,calc(100vw - 24px));height:min(900px,calc(100dvh - 24px));max-width:calc(100vw - 24px);max-height:calc(100dvh - 24px);padding:0;border:1px solid var(--qi-border);border-radius:8px;background:var(--qi-surface);overflow:hidden;margin:auto}
.question-image-preview[open]{display:flex;flex-direction:column}
.question-image-preview::backdrop{background:rgb(12 23 36 / 65%)}
.question-image-preview header{display:flex;align-items:center;gap:10px;padding:8px 12px;flex:none;border-bottom:1px solid var(--qi-border)}
.question-image-preview header strong{flex:1;min-width:0;overflow-wrap:anywhere;max-height:80px;overflow:auto;font-size:15px}
.question-image-preview button{display:inline-flex;align-items:center;justify-content:center;flex:none;width:44px;height:44px;border:1px solid var(--qi-border);border-radius:6px;color:var(--qi-accent);background:var(--qi-surface);cursor:pointer}
.question-image-preview button:hover:not(:disabled),.question-image-preview button[aria-pressed=true]{background:color-mix(in srgb,var(--qi-accent) 12%,var(--qi-surface))}
.question-image-preview button:disabled{opacity:.45;cursor:not-allowed}
.question-image-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;flex:none;padding:8px 12px}
.question-image-tools output{font-size:12px;min-width:60px}
.question-image-viewport{position:relative;flex:1;min-height:0;min-width:0;overflow:auto;overscroll-behavior:contain;background:var(--qi-soft)}
.question-image-viewport.fitted{display:flex;align-items:center;justify-content:center}
.question-image-viewport img{display:block;max-width:100%;max-height:100%;object-fit:contain;margin:auto}
.question-image-viewport p{padding:16px;overflow-wrap:anywhere}
.question-image-loading{position:absolute;top:16px;left:16px;background:var(--qi-surface);padding:4px 8px}
.question-image-preview .question-image-close{color:var(--qi-text)}
:global(html[data-v2-theme="dark"]) .question-image-preview,:global(html[data-theme="dark"]) .question-image-preview{color-scheme:dark}
@media(max-width:700px){.question-image-strip{grid-template-columns:repeat(2,minmax(0,1fr))}.question-image-sources{grid-template-columns:repeat(2,minmax(0,1fr))}.question-image-preview{width:calc(100vw - 16px);max-width:calc(100vw - 16px);height:calc(100dvh - 16px);max-height:calc(100dvh - 16px)}.question-image-tools{gap:6px;padding:8px}.question-image-preview header{padding:8px}.question-image-tools output{min-width:0}}
</style>
