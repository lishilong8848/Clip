<template>
  <section
    ref="cardRef"
    class="home-broadcast-card"
    aria-label="当前账号任务播报"
    @focusin="handleFocusIn"
    @focusout="handleFocusOut"
  >
    <div class="broadcast-fixed">
      <span class="broadcast-dot" aria-hidden="true"></span>
      <strong>实时动态</strong>
      <small>{{ summary }}</small>
    </div>
    <div class="broadcast-viewport">
      <div
        ref="trackRef"
        class="broadcast-track"
        :class="{
          'is-static': items.length === 0,
          'is-reduced': prefersReducedMotion,
        }"
        :style="trackStyle"
      >
        <template v-for="item in renderItems" :key="item.instanceKey">
          <button
            v-if="item.action"
            type="button"
            class="broadcast-item interactive"
            :class="[item.tone, { 'is-active': item.instanceKey === activeInstanceKey }]"
            :aria-hidden="item.duplicate ? 'true' : undefined"
            :data-instance-key="item.instanceKey"
            :tabindex="item.duplicate ? -1 : 0"
            @focus="handleItemFocus(item)"
            @click="activate(item)"
          >
            <b>{{ item.label }}</b>
            <span>{{ item.text }}</span>
          </button>
          <span
            v-else
            class="broadcast-item"
            :class="[item.tone, { 'is-active': item.instanceKey === activeInstanceKey }]"
            :aria-hidden="item.duplicate ? 'true' : undefined"
          >
            <b>{{ item.label }}</b>
            <span>{{ item.text }}</span>
          </span>
        </template>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import type { ScopeHomeBroadcastItem } from "../scopeHomeUtils";

const props = defineProps<{
  items: ScopeHomeBroadcastItem[];
  summary: string;
}>();

const emit = defineEmits<{
  activate: [item: ScopeHomeBroadcastItem];
}>();

type RenderBroadcastItem = ScopeHomeBroadcastItem & {
  instanceKey: string;
  duplicate: boolean;
};

const renderItems = computed<RenderBroadcastItem[]>(() => {
  const list = props.items.length
    ? props.items
    : [{ key: "empty", label: "就绪", text: "当前账号暂无待处理任务", tone: "quiet" as const }];
  const primary = list.map((item) => ({
    ...item,
    instanceKey: `${item.key}-primary`,
    duplicate: false,
  }));
  if (props.items.length === 0 || (list.length === 1 && prefersReducedMotion.value)) return primary;
  return [
    ...primary,
    ...list.map((item) => ({
      ...item,
      instanceKey: `${item.key}-duplicate`,
      duplicate: true,
    })),
  ];
});

const primaryItems = computed<RenderBroadcastItem[]>(() =>
  renderItems.value.filter((item) => !item.duplicate),
);

// Continuous scroll is pure CSS. Duration adapts to the real track width so the
// text travels at roughly 18px/sec (~36s minimum) without any per-frame JS.
const trackRef = ref<HTMLDivElement | null>(null);
const durationSec = ref(36);
const SPEED_PX_PER_SEC = 18;
const MIN_DURATION_SEC = 36;
const TRACK_GAP_PX = 22;

const trackStyle = computed(() => ({
  "--broadcast-duration": `${durationSec.value}s`,
}));

// Reduced-motion users get an automatic feed without continuous motion: a
// low-frequency rotation reveals one item at a time. There is no visible
// control; focus pauses the rotation so keyboard users can read comfortably.
const prefersReducedMotion = ref(false);
const activeIndex = ref(0);
const focusActive = ref(false);
const cardRef = ref<HTMLElement | null>(null);
const ROTATE_INTERVAL_MS = 5000;
let mediaQuery: MediaQueryList | null = null;
let mediaChangeHandler: (() => void) | null = null;
let rotationTimer: ReturnType<typeof setInterval> | null = null;
let resizeObserver: ResizeObserver | null = null;

const hasRotation = computed(() => primaryItems.value.length > 1);

const activeInstanceKey = computed(() => {
  const item = primaryItems.value[activeIndex.value] ?? primaryItems.value[0];
  return item?.instanceKey;
});

function stopRotationTimer(): void {
  if (rotationTimer !== null) {
    clearInterval(rotationTimer);
    rotationTimer = null;
  }
}

function updateDuration(): void {
  const track = trackRef.value;
  if (!track || track.scrollWidth <= 0) return;
  // The keyframe travels from 0 to -50% (plus half the inter-item gap), which
  // is half the track width. Derive the loop duration from that distance.
  const distance = track.scrollWidth / 2 + TRACK_GAP_PX / 2;
  durationSec.value = Math.max(
    MIN_DURATION_SEC,
    Math.ceil(distance / SPEED_PX_PER_SEC),
  );
}

function syncRotation(): void {
  stopRotationTimer();
  const shouldRotate =
    prefersReducedMotion.value &&
    hasRotation.value &&
    !focusActive.value;
  if (!shouldRotate) return;
  rotationTimer = setInterval(() => {
    if (primaryItems.value.length === 0) return;
    activeIndex.value = (activeIndex.value + 1) % primaryItems.value.length;
  }, ROTATE_INTERVAL_MS);
}

function handleFocusIn(): void {
  focusActive.value = true;
}

function handleFocusOut(event: FocusEvent): void {
  const relatedTarget = event.relatedTarget as Node | null;
  if (relatedTarget && cardRef.value?.contains(relatedTarget)) return;
  focusActive.value = false;
}

function handleItemFocus(item: RenderBroadcastItem): void {
  const index = primaryItems.value.findIndex((it) => it.instanceKey === item.instanceKey);
  if (index >= 0) activeIndex.value = index;
}

watch(
  [prefersReducedMotion, focusActive, () => primaryItems.value.length],
  syncRotation,
);

watch(
  () => primaryItems.value.length,
  (length) => {
    if (activeIndex.value >= length) activeIndex.value = 0;
  },
);

onMounted(() => {
  mediaQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
  const query = mediaQuery;
  const handler = () => {
    prefersReducedMotion.value = query?.matches ?? false;
  };
  mediaChangeHandler = handler;
  handler();
  query?.addEventListener?.("change", handler);

  updateDuration();
  if (trackRef.value) {
    resizeObserver = new ResizeObserver(updateDuration);
    resizeObserver.observe(trackRef.value);
  }
  syncRotation();
});

onBeforeUnmount(() => {
  stopRotationTimer();
  resizeObserver?.disconnect();
  resizeObserver = null;
  if (mediaQuery && mediaChangeHandler) {
    mediaQuery.removeEventListener?.("change", mediaChangeHandler);
  }
  mediaQuery = null;
  mediaChangeHandler = null;
});

function activate(item: RenderBroadcastItem): void {
  if (item.duplicate || !item.action) return;
  emit("activate", item);
}
</script>

<style scoped>
.home-broadcast-card {
  min-height: 42px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 12px;
  overflow: hidden;
  padding: 5px 10px 5px 12px;
  border: 1px solid #d6e2f1;
  border-radius: 8px;
  background: rgba(248, 251, 255, 0.96);
}

.broadcast-fixed {
  display: flex;
  align-items: center;
  gap: 7px;
  min-width: 322px;
  padding-right: 12px;
  border-right: 1px solid rgba(195, 211, 234, 0.78);
}

.broadcast-dot {
  width: 8px;
  height: 8px;
  flex: 0 0 8px;
  border-radius: 999px;
  background: #1f6dff;
  box-shadow: 0 0 0 4px rgba(31, 109, 255, 0.1);
}

.broadcast-fixed strong {
  color: #071a39;
  font-size: 13px;
  font-weight: 700;
  white-space: nowrap;
}

.broadcast-fixed small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  color: #5e728f;
  font-size: 11px;
  font-weight: 600;
  white-space: nowrap;
}

.broadcast-viewport {
  min-width: 0;
  overflow: hidden;
  -webkit-mask-image: linear-gradient(90deg, transparent, #000 5%, #000 95%, transparent);
  mask-image: linear-gradient(90deg, transparent, #000 5%, #000 95%, transparent);
}

.broadcast-track {
  width: max-content;
  display: flex;
  align-items: center;
  gap: 22px;
  padding-left: 0;
  animation: broadcast-scroll var(--broadcast-duration, 36s) linear infinite;
  will-change: transform;
}

/* Keyboard users need the focused item to stay readable; mouse hover does not
   pause scrolling. */
.home-broadcast-card:focus-within .broadcast-track {
  animation-play-state: paused;
}

.broadcast-track.is-static {
  animation: none;
  width: 100%;
  padding-left: 0;
}

.broadcast-item {
  --broadcast-tone: #64748b;
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 28px;
  padding: 3px 1px 3px 12px;
  border: 0;
  border-radius: 5px;
  background: transparent;
  color: #24415f;
  white-space: nowrap;
}

.broadcast-item::before {
  content: "";
  position: absolute;
  left: 0;
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--broadcast-tone);
}

button.broadcast-item {
  font: inherit;
}

.broadcast-item.interactive {
  cursor: pointer;
}

.broadcast-item.interactive:hover {
  background: rgba(226, 238, 253, 0.72);
}

.broadcast-item.interactive:focus-visible {
  outline: none;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.16);
}

.broadcast-item[aria-hidden="true"] {
  pointer-events: none;
}

.broadcast-item b {
  display: inline-flex;
  align-items: center;
  font-size: 11px;
  font-weight: 700;
}

.broadcast-item span {
  font-size: 11px;
  font-weight: 600;
}

.broadcast-item.ongoing {
  --broadcast-tone: #1f6dff;
}

.broadcast-item.ongoing b {
  color: #0b5bd3;
}

.broadcast-item.pending {
  --broadcast-tone: #d98908;
}

.broadcast-item.pending b {
  color: #b45309;
}

.broadcast-item.event {
  --broadcast-tone: #df4058;
}

.broadcast-item.event b {
  color: #c92f47;
}

.broadcast-item.quiet {
  --broadcast-tone: #64748b;
}

.broadcast-item.quiet b {
  color: #475569;
}

@keyframes broadcast-scroll {
  from {
    transform: translate3d(0, 0, 0);
  }
  to {
    transform: translate3d(calc(-50% - 11px), 0, 0);
  }
}

@media (prefers-reduced-motion: reduce) {
  .broadcast-viewport {
    overflow: hidden;
    -webkit-mask-image: none;
    mask-image: none;
  }

  .broadcast-track {
    animation: none;
    will-change: auto;
    padding-left: 0;
  }

  .broadcast-item[aria-hidden="true"] {
    display: none;
  }

  .broadcast-track.is-reduced .broadcast-item {
    flex: 0 0 auto;
    order: 1;
  }

  .broadcast-track.is-reduced .broadcast-item.is-active {
    order: 0;
    max-width: 100%;
    min-width: 0;
    white-space: normal;
  }

  .broadcast-track.is-reduced .broadcast-item.is-active b,
  .broadcast-track.is-reduced .broadcast-item.is-active span {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

@media (max-width: 759px) {
  .home-broadcast-card {
    grid-template-columns: auto minmax(0, 1fr);
    gap: 8px;
    padding-left: 10px;
  }

  .broadcast-fixed {
    min-width: auto;
    padding-right: 9px;
  }

  .broadcast-fixed small {
    display: none;
  }
}
</style>
