<template>
  <section class="mop-flow-steps" aria-label="MOP填写流程">
    <article v-for="step in steps" :key="step.key" :class="[step.state]">
      <b>{{ step.index }}</b>
      <span>
        <strong>{{ step.label }}</strong>
        <small>{{ step.text }}</small>
      </span>
    </article>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  steps: Array<{
    key: string;
    index: string;
    label: string;
    text: string;
    state: "done" | "active" | "pending" | string;
  }>;
}>();
</script>

<style scoped>
.mop-flow-steps {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  padding: 8px;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.06);
}

.mop-flow-steps article {
  min-width: 0;
  display: grid;
  grid-template-columns: 32px minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  padding: 9px 10px;
  border: 1px solid rgba(216, 229, 247, 0.78);
  border-radius: 14px;
  background: #f8fbff;
  color: #64748b;
}

.mop-flow-steps article b {
  width: 32px;
  height: 32px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: #eaf3ff;
  color: #1d4ed8;
  font-size: 13px;
  font-weight: 950;
}

.mop-flow-steps article span {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.mop-flow-steps article strong,
.mop-flow-steps article small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-flow-steps article strong {
  color: #0b1f3f;
  font-size: 13px;
  font-weight: 950;
}

.mop-flow-steps article small {
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.mop-flow-steps article.active {
  border-color: #9cc2ff;
  background: linear-gradient(135deg, #ffffff, #eef6ff);
  box-shadow: inset 0 0 0 1px rgba(30, 99, 255, 0.12);
}

.mop-flow-steps article.active b {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
}

.mop-flow-steps article.done {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.mop-flow-steps article.done b {
  background: #10b981;
  color: #ffffff;
}

@media (max-width: 1180px) {
  .mop-flow-steps {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .mop-flow-steps {
    grid-template-columns: 1fr;
  }
}
</style>
