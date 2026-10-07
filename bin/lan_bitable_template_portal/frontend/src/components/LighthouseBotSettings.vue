<template>
  <Teleport :to="target">
    <UiTransition name="ui-overlay" appear>
      <div v-if="open" class="bot-settings-backdrop" @click.self="close">
        <form ref="dialog" class="bot-settings" role="dialog" aria-modal="true" aria-labelledby="bot-settings-title" @submit.prevent="save">
          <header><h2 id="bot-settings-title">图标设置</h2><button type="button" class="close" aria-label="关闭图标设置" :disabled="saving" @click="close"><X :size="18" /></button></header>
          <div class="settings-body">
            <div class="preview"><LighthouseBot :appearance="draft" /></div>
            <fieldset class="colors"><legend>颜色</legend><div>
              <button v-for="[id, label, color] in BOT_COLORS" :key="id" type="button" class="swatch" :aria-label="label" :title="label" :aria-pressed="draft.color === id" :style="{ '--swatch': color }" :disabled="saving" @click="draft.color = id"><Check v-if="draft.color === id" :size="15" /></button>
            </div></fieldset>
            <div class="fields">
              <label>形状<select v-model="draft.shape" aria-label="形状" :disabled="saving"><option v-for="[id, label] in BOT_SHAPES" :key="id" :value="id">{{ label }}</option></select></label>
            </div>
            <label class="size">尺寸 <output>{{ draft.size }} px</output><input v-model.number="draft.size" aria-label="尺寸" type="range" min="40" max="200" step="1" :disabled="saving" /></label>
            <div class="switches">
              <label><input v-model="draft.snap_back" type="checkbox" :disabled="saving" />拖拽后返回右下角</label>
            </div>
            <p v-if="error" class="error" role="alert">{{ error }}</p>
          </div>
          <footer><button type="button" class="reset" :disabled="saving" @click="draft = { ...DEFAULT_BOT }"><RotateCcw :size="14" />恢复默认</button><button type="button" :disabled="saving" @click="close">取消</button><button type="submit" class="save" :disabled="saving"><Loader2 v-if="saving" :size="15" class="spin" />{{ saving ? '保存中…' : '保存' }}</button></footer>
        </form>
      </div>
    </UiTransition>
  </Teleport>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from 'vue';
import { Check, Loader2, RotateCcw, X } from 'lucide-vue-next';
import { requestJson } from '../api/client';
import { acquireModal } from '../modalState';
import { BOT_COLORS, BOT_SHAPES, DEFAULT_BOT, normalizeBot, type BotAppearance } from '../botAppearance';
import LighthouseBot from './LighthouseBot.vue';

const props = withDefaults(defineProps<{ open: boolean; appearance: BotAppearance; target?: string | HTMLElement }>(), { target: 'body' });
const emit = defineEmits<{ close: []; saved: [value: BotAppearance]; preview: [value: BotAppearance] }>();
const dialog = ref<HTMLElement | null>(null), draft = ref({ ...DEFAULT_BOT }), saving = ref(false), error = ref('');
let modal: ReturnType<typeof acquireModal> | undefined, returnFocus: HTMLElement | null = null, disposed = false;
let controller: AbortController | undefined;
watch(draft, value => { if (props.open) emit('preview', { ...value }); }, { deep: true });
function close(): void { if (!saving.value) emit('close'); }
async function save(): Promise<void> {
  if (saving.value) return;
  saving.value = true; error.value = ''; controller = new AbortController();
  try {
    const response = await requestJson('/api/assistant/appearance', { method: 'PUT', body: JSON.stringify(draft.value), signal: controller.signal, timeoutMs: 12000 });
    if (!disposed) { emit('saved', normalizeBot(response)); emit('close'); }
  } catch (err) { if (!disposed) error.value = err instanceof Error ? err.message : '保存失败，请重试。'; }
  finally { saving.value = false; controller = undefined; }
}
function keydown(event: KeyboardEvent): void {
  if (!modal?.isTop(event, dialog.value) || !dialog.value) return;
  if (event.key === 'Escape') { event.preventDefault(); event.stopImmediatePropagation(); close(); }
  if (event.key !== 'Tab') return;
  const nodes = [...dialog.value.querySelectorAll<HTMLElement>('button:not(:disabled),select:not(:disabled),input:not(:disabled)')].filter(node => node.getClientRects().length);
  const first = nodes[0], last = nodes[nodes.length - 1];
  if (!dialog.value.contains(document.activeElement) || (event.shiftKey ? document.activeElement === first : document.activeElement === last)) {
    event.preventDefault(); (event.shiftKey ? last : first)?.focus();
  }
}
function release(): void { modal?.release(); modal = undefined; window.removeEventListener('keydown', keydown, true); }
watch(() => props.open, async value => {
  if (value) {
    draft.value = { ...props.appearance }; error.value = '';
    returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    modal = acquireModal(); window.addEventListener('keydown', keydown, true);
    await nextTick(); if (!disposed && props.open) dialog.value?.querySelector<HTMLElement>('button')?.focus();
  } else {
    release(); const target = returnFocus; returnFocus = null;
    await nextTick(); if (target?.isConnected) target.focus();
  }
}, { immediate: true });
onBeforeUnmount(() => { disposed = true; controller?.abort(); release(); });
</script>

<style scoped>
.bot-settings-backdrop { position: fixed; inset: 0; z-index: 10100; padding: 20px; display: grid; place-items: center; background: rgba(18, 24, 20, .42); pointer-events: auto; }
.bot-settings { width: min(420px, 100%); max-height: calc(100dvh - 40px); display: flex; flex-direction: column; overflow: hidden; border: 1px solid #d4ded6; border-radius: 8px; background: #fff; color: #27342d; box-shadow: 0 16px 60px #15281d30; font-size: 14px; color-scheme: light; }
header, footer { display: flex; align-items: center; gap: 8px; padding: 14px 18px; flex-shrink: 0; }
header { justify-content: space-between; border-bottom: 1px solid #e4eae5; }h2 { margin: 0; font-size: 16px; }
.settings-body { padding: 0 18px 18px; overflow-y: auto; overscroll-behavior: contain; }.preview { height: 224px; display: grid; place-items: center; }
fieldset { padding: 0; margin: 0 0 16px; border: 0; }legend { margin-bottom: 10px; }.colors > div { display: grid; grid-template-columns: repeat(12, 1fr); gap: 6px; }
button { border: 1px solid #cdd8d0; background: #fff; color: inherit; border-radius: 6px; min-height: 34px; padding: 6px 12px; font: inherit; display: inline-flex; align-items: center; justify-content: center; gap: 5px; cursor: pointer; }
button:disabled, input:disabled, select:disabled { opacity: .5; cursor: not-allowed; }button:hover:not(:disabled) { background: #edf4f0; }.close { padding: 6px; border: 0; }
.swatch { min-height: 0; width: 100%; aspect-ratio: 1; padding: 0; border-radius: 50%; background: var(--swatch); color: #fff; box-shadow: inset 0 0 0 1px #0002; border: 0; }.swatch:hover:not(:disabled) { background: var(--swatch); }.swatch[aria-pressed=true] { outline: 2px solid #397554; outline-offset: 2px; }.swatch:last-child { color: #27342d; }
.fields label { display: grid; gap: 6px; }
select { width: 100%; min-width: 0; border: 1px solid #cdd8d0; border-radius: 6px; background: #fff; color: inherit; font: inherit; padding: 8px; }
.size { margin: 16px 0; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }.size output { margin-left: auto; font-size: 12px; color: #526b5d; }.size input { width: 100%; margin: 0; accent-color: #397554; }
.switches { display: grid; gap: 12px; }.switches label { display: flex; gap: 8px; align-items: center; }.switches input { margin: 0; width: 16px; height: 16px; accent-color: #397554; }
footer { border-top: 1px solid #e4eae5; }.reset { margin-right: auto; font-size: 12px; padding: 6px 8px; }.save { background: #397554; color: #fff; border-color: #397554; }.save:hover:not(:disabled) { background: #2b6042; }.error { color: #a12d2d; font-size: 13px; margin-bottom: 0; overflow-wrap: anywhere; }
button:focus-visible, select:focus-visible, input:focus-visible { outline: 2px solid #397554; outline-offset: 3px; }
@media (max-width: 420px) { .colors > div { grid-template-columns: repeat(6, 1fr); gap: 10px; }.swatch { width: 28px; justify-self: center; } }
@media print { .bot-settings-backdrop { display: none; } }
</style>
