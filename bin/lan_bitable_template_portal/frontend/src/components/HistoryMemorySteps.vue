<template>
  <section class="history-steps" aria-label="历史记忆导入步骤">
    <article
      v-for="step in steps"
      :key="step.key"
      :class="{ active: activeKey === step.key, done: step.done }"
    >
      <span>{{ step.index }}</span>
      <div>
        <strong>{{ step.title }}</strong>
        <small>{{ step.text }}</small>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
export type HistoryMemoryStep = {
  key: string;
  index: string | number;
  title: string;
  text: string;
  done: boolean;
};

defineProps<{
  steps: HistoryMemoryStep[];
  activeKey: string;
}>();
</script>

<style scoped>
.history-steps {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.history-steps article {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 13px 14px;
  border: 1px solid #d8e7f8;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.82);
}

.history-steps article.active {
  border-color: #6aa6ff;
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.12);
}

.history-steps article.done span,
.history-steps article.active span {
  background: #1e63ff;
  color: #fff;
}

.history-steps span {
  width: 30px;
  height: 30px;
  flex: 0 0 auto;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: #edf5ff;
  color: #1d4ed8;
  font-weight: 950;
}

.history-steps div {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.history-steps strong {
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.history-steps small {
  overflow: hidden;
  color: #60758f;
  font-size: 12px;
  font-weight: 800;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 900px) {
  .history-steps {
    grid-template-columns: 1fr;
  }
}
</style>
