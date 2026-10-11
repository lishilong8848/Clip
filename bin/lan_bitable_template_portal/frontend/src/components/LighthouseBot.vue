<template>
  <span ref="host" class="lighthouse-bot" :class="{ interactive, arriving: interactive && !resumed }" :style="{ width: appearance.size + 'px', height: appearance.size + 'px' }" :data-bot-mood="mood" :data-animation-active="active" :data-motion-resumed="resumed" aria-hidden="true">
    <span v-if="!ready" class="bot-fallback"><i /><i /></span>
  </span>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { BloubBot } from '../vendor/bloub/entry';
import type { StateId } from '../vendor/bloub/engine/states';
import type { BotAppearance, BotMood } from '../botAppearance';

const props = withDefaults(defineProps<{ appearance: BotAppearance; interactive?: boolean; position?: { x: number; y: number } | null; mood?: BotMood; visible?: boolean; motionKey?: string }>(), { interactive: false, position: null, mood: 'idle', visible: true, motionKey: '' });
const emit = defineEmits<{ moved: [position: { x: number; y: number }] }>();
const host = ref<HTMLElement | null>(null), ready = ref(false), resumed = ref(false);
let bot: BloubBot | undefined, disposed = false;
const reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
const pageVisible = ref(!document.hidden), reducedMotion = ref(reduced.matches);
// A same-origin workbench iframe is still part of the active browser window.
const focusWindow = (() => { try { return window.top?.document ? window.top : window; } catch { return window; } })();
const windowFocused = ref(focusWindow.document.hasFocus());
const active = computed(() => props.visible && pageVisible.value && windowFocused.value);
const IDLE_CYCLE: StateId[] = ['idle', 'wink', 'wide', 'play', 'orbit', 'swirl', 'burst', 'comet', 'egg', 'hexagon'];
const QUIET_CYCLE: StateId[] = ['idle', 'wink', 'wide'];
let lastUrl = window.location.href;
function activate(): void {
  bot?.setReducedMotion(reducedMotion.value);
  bot?.setFollow(active.value && props.interactive);
  bot?.setActive(active.value);
}
function setMood(): void {
  if (!bot) return;
  const expression = { idle: 'neutre', engaged: 'curieux', thinking: 'attentif', success: 'heureux', error: 'confus' };
  const state: Record<BotMood, StateId> = { idle: 'idle', engaged: 'idle', thinking: 'thinking', success: 'notify', error: 'alert' };
  bot.setExpression(expression[props.mood]);
  if (props.mood === 'idle') bot.setCycle(reducedMotion.value ? QUIET_CYCLE : IDLE_CYCLE, true);
  else { bot.pause(); bot.setState(state[props.mood]); }
  activate();
}
function update(value: BotAppearance): void {
  if (!bot) return;
  bot.setSize(value.size); bot.setColor(value.color); bot.setShape(value.shape);
  bot.snapBack = value.snap_back;
  activate();
}
function visibility(): void { pageVisible.value = !document.hidden; reducedMotion.value = reduced.matches; }
function focus(): void { if (!disposed) windowFocused.value = focusWindow.document.hasFocus(); }
function blur(): void { window.setTimeout(focus, 0); }
async function reposition(animate = true): Promise<void> {
  await nextTick();
  if (disposed || !bot || !host.value || !props.interactive) return;
  const anchor = (host.value.closest('.lighthouse-launcher') || host.value).getBoundingClientRect();
  const base = !props.appearance.snap_back && props.position ? props.position : { x: anchor.left, y: anchor.top };
  const position = { x: Math.max(12, Math.min(base.x, window.innerWidth - props.appearance.size - 12)), y: Math.max(12, Math.min(base.y, window.innerHeight - props.appearance.size - 12)) };
  if (animate) bot.moveTo(position); else bot.setPosition(position);
}
watch(() => props.appearance, update, { deep: true });
watch(() => props.mood, setMood);
watch(reducedMotion, setMood);
watch(active, value => { activate(); if (value) void reposition(false); });
watch(() => [props.position, props.appearance.size, props.appearance.snap_back], () => { void reposition(); }, { deep: true });
function onResize(): void { void reposition(false); }
function saveMotion(): void {
  if (!props.motionKey || !bot) return;
  try { sessionStorage.setItem('lighthouse_bot_motion:' + props.motionKey, JSON.stringify({ at: Date.now(), mood: props.mood, playback: bot.playback() })); } catch { /* private mode */ }
}
function onNavigation(): void {
  if (lastUrl === window.location.href) return;
  lastUrl = window.location.href; bot?.greet();
}
function onFramePointer(event: Event): void {
  const point = (event as CustomEvent<{ x: number; y: number }>).detail;
  if (active.value && props.interactive && point) bot?.followPointer(point.x, point.y);
}
onMounted(() => {
  if (disposed || !host.value) return;
  const value = props.appearance;
  bot = new BloubBot(host.value, {
    size: value.size, color: value.color, shape: value.shape, expression: 'neutre',
    paper: '#ffffff', autoplay: true, cycle: IDLE_CYCLE, draggable: props.interactive, reactOnClick: props.interactive,
    snapBack: value.snap_back,
    dragTarget: props.interactive ? host.value.parentElement ?? undefined : undefined,
    onDragEnd: (position, moved) => { if (!moved) return; if (!props.appearance.snap_back) emit('moved', position); void reposition(); },
  });
  update(value);
  setMood();
  if (props.motionKey) try {
    const saved = JSON.parse(sessionStorage.getItem('lighthouse_bot_motion:' + props.motionKey) || 'null');
    const age = Date.now() - Number(saved?.at);
    const resting = (mood: unknown) => mood === 'idle' || mood === 'engaged';
    if (age >= 0 && age < 15000 && (saved.mood === props.mood || (resting(saved.mood) && resting(props.mood)))) resumed.value = bot.restorePlayback(saved.playback);
  } catch { /* Ignore an expired or invalid animation handoff. */ }
  if (!resumed.value && props.interactive) bot.greet();
  ready.value = true;
  void reposition(false);
  document.addEventListener('visibilitychange', visibility);
  reduced.addEventListener('change', visibility);
  window.addEventListener('resize', onResize);
  window.addEventListener('focus', focus);
  window.addEventListener('blur', blur);
  if (focusWindow !== window) { focusWindow.addEventListener('focus', focus); focusWindow.addEventListener('blur', blur); }
  if (props.interactive) {
    window.addEventListener('popstate', onNavigation);
    window.addEventListener('clipflow:workbench-pointer', onFramePointer);
    window.addEventListener('pagehide', saveMotion);
  }
});
onBeforeUnmount(() => {
  saveMotion();
  disposed = true;
  document.removeEventListener('visibilitychange', visibility);
  reduced.removeEventListener('change', visibility);
  window.removeEventListener('resize', onResize);
  window.removeEventListener('focus', focus);
  window.removeEventListener('blur', blur);
  if (focusWindow !== window) { focusWindow.removeEventListener('focus', focus); focusWindow.removeEventListener('blur', blur); }
  window.removeEventListener('popstate', onNavigation);
  window.removeEventListener('clipflow:workbench-pointer', onFramePointer);
  window.removeEventListener('pagehide', saveMotion);
  bot?.destroy();
});
</script>

<style scoped>
.lighthouse-bot { position: relative; display: inline-grid; place-items: center; flex: 0 0 auto; pointer-events: none; }
.lighthouse-bot > :deep(*) { grid-area: 1 / 1; }
.lighthouse-bot.arriving { animation: bot-arrive 360ms ease both; }
@keyframes bot-arrive { from { opacity: 0; } to { opacity: 1; } }
@media (prefers-reduced-motion: reduce) { .lighthouse-bot.arriving { animation: none; } }
.lighthouse-bot :deep(svg), .lighthouse-bot :deep(svg *) { pointer-events: none; }
.lighthouse-bot.interactive :deep(svg) { touch-action: none; user-select: none; cursor: grab; }
.lighthouse-bot:not(.interactive) :deep([data-bot-hit]) { pointer-events: none !important; }
.lighthouse-bot.interactive .bot-fallback { pointer-events: auto; }
.lighthouse-bot.interactive :deep(svg:active) { cursor: grabbing; }
.bot-fallback { width: 62%; height: 62%; border-radius: 50%; background: #0a0a0c; display: flex; justify-content: center; align-items: center; gap: 6px; }
.bot-fallback i { width: 4px; height: 8px; background: #fff; border-radius: 50%; }
</style>
