<template>
  <div class="summary-strip">
    <article
      v-for="item in items"
      :key="item.key"
      :class="[item.tone, { zero: Number(item.value) === 0, focus: item.key === 'ongoing' && Number(item.value) > 0 }]"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
      <em>{{ item.hint }}</em>
    </article>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  started: number;
  updated: number;
  ended: number;
  ongoing: number;
}>();

const items = computed(() => [
  { key: "started", label: "已发起", value: props.started || 0, tone: "blue", hint: "今日开始" },
  { key: "updated", label: "有更新", value: props.updated || 0, tone: "cyan", hint: "今日更新" },
  { key: "ended", label: "已结束", value: props.ended || 0, tone: "emerald", hint: "今日闭环" },
  { key: "ongoing", label: "进行中", value: props.ongoing || 0, tone: "indigo", hint: "需关注" },
]);
</script>

<style scoped>
.summary-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.summary-strip article {
  position: relative;
  min-height: 68px;
  overflow: hidden;
  padding: 13px 16px 12px 58px;
  border: 1px solid rgba(191, 219, 254, 0.72);
  border-radius: 20px;
  background:
    radial-gradient(circle at right top, rgba(255, 255, 255, 0.98), rgba(239, 246, 255, 0.58)),
    rgba(255, 255, 255, 0.94);
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.08);
  transition: border-color 0.16s ease, box-shadow 0.16s ease, opacity 0.16s ease;
}

.summary-strip article::before {
  position: absolute;
  left: 16px;
  top: 16px;
  width: 30px;
  height: 30px;
  border-radius: 12px;
  content: "";
  box-shadow: 0 10px 20px rgba(37, 99, 235, 0.17);
}

.summary-strip article.blue::before {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.summary-strip article.cyan::before {
  background: linear-gradient(135deg, #06b6d4, #0284c7);
}

.summary-strip article.emerald::before {
  background: linear-gradient(135deg, #10b981, #059669);
}

.summary-strip article.indigo::before {
  background: linear-gradient(135deg, #2563eb, #4f46e5);
}

.summary-strip span {
  display: block;
  color: #475569;
  font-size: 12px;
  font-weight: 950;
}

.summary-strip strong {
  display: block;
  margin-top: 3px;
  color: #0757d7;
  font-size: 24px;
  line-height: 1;
  font-weight: 950;
}

.summary-strip em {
  display: block;
  margin-top: 4px;
  overflow: hidden;
  color: #64748b;
  font-size: 11px;
  font-style: normal;
  font-weight: 850;
  line-height: 1.1;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.summary-strip article.zero {
  opacity: 0.74;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.05);
}

.summary-strip article.zero::before {
  filter: saturate(0.7);
  opacity: 0.72;
}

.summary-strip article.zero strong {
  color: #64748b;
}

.summary-strip article.focus {
  border-color: rgba(79, 70, 229, 0.32);
  box-shadow: 0 16px 34px rgba(79, 70, 229, 0.14);
}

@media (max-width: 980px) {
  .summary-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 560px) {
  .summary-strip {
    grid-template-columns: 1fr;
  }
}
</style>
