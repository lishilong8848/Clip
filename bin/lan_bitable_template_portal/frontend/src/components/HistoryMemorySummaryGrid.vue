<template>
  <section class="summary-grid">
    <article><span>当前月事项</span><strong>{{ sourceCount }}</strong></article>
    <article><span>历史候选</span><strong>{{ candidateCount }}</strong></article>
    <article><span>已勾选</span><strong>{{ selectedCount }}</strong></article>
    <article><span>可一键填充</span><strong>{{ recommendedCount }}</strong></article>
    <article><span>可保存覆盖</span><strong>{{ overwriteHint }}</strong></article>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  sourceCount: number;
  candidateCount: number;
  selectedCount: number;
  recommendedCount: number;
  overwriteHint: string | number;
}>();
</script>

<style scoped>
.summary-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 12px;
}

.summary-grid article {
  min-width: 0;
  display: grid;
  gap: 5px;
  padding: 14px 16px;
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 12px 26px rgba(15, 73, 153, 0.06);
}

.summary-grid span {
  color: #60758f;
  font-size: 12px;
  font-weight: 850;
}

.summary-grid strong {
  color: #0e4fb2;
  font-size: 24px;
  font-weight: 950;
  line-height: 1;
}

@media (max-width: 1180px) {
  .summary-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .summary-grid {
    grid-template-columns: 1fr;
  }
}
</style>
