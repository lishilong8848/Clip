<template>
  <section class="home-broadcast-card" aria-label="当前账号任务播报">
    <div class="broadcast-fixed">
      <span class="broadcast-dot" aria-hidden="true"></span>
      <strong>实时动态</strong>
      <small>{{ summary }}</small>
    </div>
    <div class="broadcast-viewport">
      <div class="broadcast-track" :class="{ 'is-static': items.length === 0 }" :style="trackStyle">
        <template v-for="item in renderItems" :key="item.instanceKey">
          <button
            v-if="item.action"
            type="button"
            class="broadcast-item interactive"
            :class="item.tone"
            :aria-hidden="item.duplicate ? 'true' : undefined"
            :tabindex="item.duplicate ? -1 : 0"
            @click="activate(item)"
          >
            <b>{{ item.label }}</b>
            <span>{{ item.text }}</span>
          </button>
          <span
            v-else
            class="broadcast-item"
            :class="item.tone"
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
import { computed } from "vue";
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
  if (list.length <= 1) return primary;
  return [
    ...primary,
    ...list.map((item) => ({
      ...item,
      instanceKey: `${item.key}-duplicate`,
      duplicate: true,
    })),
  ];
});

const trackStyle = computed(() => {
  const duration = Math.min(90, Math.max(26, props.items.length * 7));
  return { "--broadcast-duration": `${duration}s` };
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
  padding-left: 100%;
  animation: broadcast-scroll var(--broadcast-duration, 32s) linear infinite;
  will-change: transform;
}

.home-broadcast-card:hover .broadcast-track,
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
    transform: translate3d(-100%, 0, 0);
  }
}

@media (prefers-reduced-motion: reduce) {
  .broadcast-viewport {
    overflow-x: auto;
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
