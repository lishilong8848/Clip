<template>
  <section class="home-broadcast-card" aria-label="当前账号任务播报">
    <div class="broadcast-fixed">
      <span class="broadcast-dot" aria-hidden="true"></span>
      <strong>实时动态</strong>
      <small>{{ summary }}</small>
    </div>
    <div class="broadcast-viewport">
      <div class="broadcast-track" :class="{ 'is-static': items.length === 0 }" :style="trackStyle">
        <span
          v-for="(item, index) in repeatedItems"
          :key="`${item.key}-${index}`"
          class="broadcast-item"
          :class="item.tone"
        >
          <b>{{ item.label }}</b>
          <span>{{ item.text }}</span>
        </span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";

type HomeBroadcastTone = "ongoing" | "pending" | "event" | "quiet";
type HomeBroadcastItem = {
  key: string;
  label: string;
  text: string;
  tone: HomeBroadcastTone;
};

const props = defineProps<{
  items: HomeBroadcastItem[];
  summary: string;
}>();

const repeatedItems = computed(() => {
  const list = props.items.length
    ? props.items
    : [{ key: "empty", label: "就绪", text: "当前账号暂无待处理任务", tone: "quiet" as const }];
  return list.length > 1 ? [...list, ...list] : list;
});

const trackStyle = computed(() => {
  const duration = Math.min(90, Math.max(26, props.items.length * 7));
  return { "--broadcast-duration": `${duration}s` };
});
</script>

<style scoped>
.home-broadcast-card {
  min-height: 54px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 12px;
  overflow: hidden;
  padding: 8px 12px 8px 14px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(241, 247, 255, 0.92)),
    #fff;
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.11);
}

.broadcast-fixed {
  display: grid;
  grid-template-columns: 11px auto;
  grid-template-areas:
    "dot title"
    "dot summary";
  align-items: center;
  column-gap: 8px;
  min-width: 154px;
  padding-right: 12px;
  border-right: 1px solid rgba(195, 211, 234, 0.78);
}

.broadcast-dot {
  grid-area: dot;
  width: 9px;
  height: 9px;
  border-radius: 999px;
  background: linear-gradient(135deg, #1f6dff, #00b7d8);
  box-shadow: 0 0 0 6px rgba(31, 109, 255, 0.1);
}

.broadcast-fixed strong {
  grid-area: title;
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.broadcast-fixed small {
  grid-area: summary;
  color: #5e728f;
  font-size: 12px;
  font-weight: 800;
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
  gap: 9px;
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
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 30px;
  padding: 5px 10px;
  border: 1px solid rgba(216, 229, 247, 0.95);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.86);
  color: #24415f;
  box-shadow: 0 8px 18px rgba(25, 91, 176, 0.08);
  white-space: nowrap;
}

.broadcast-item b {
  display: inline-flex;
  align-items: center;
  min-height: 20px;
  padding: 2px 7px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 950;
}

.broadcast-item span {
  font-size: 12px;
  font-weight: 850;
}

.broadcast-item.ongoing b {
  background: rgba(31, 109, 255, 0.11);
  color: #0b5bd3;
}

.broadcast-item.pending b {
  background: rgba(245, 158, 11, 0.12);
  color: #b45309;
}

.broadcast-item.event b {
  background: rgba(239, 82, 96, 0.12);
  color: #c92f47;
}

.broadcast-item.quiet b {
  background: rgba(100, 116, 139, 0.12);
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
}

@media (max-width: 760px) {
  .home-broadcast-card {
    grid-template-columns: 1fr;
    gap: 8px;
  }

  .broadcast-fixed {
    min-width: 0;
    padding-right: 0;
    padding-bottom: 8px;
    border-right: 0;
    border-bottom: 1px solid rgba(195, 211, 234, 0.78);
  }
}
</style>
