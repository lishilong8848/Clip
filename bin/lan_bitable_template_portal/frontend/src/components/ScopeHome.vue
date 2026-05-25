<template>
  <section class="home-grid">
    <article
      v-for="scope in scopeOptions"
      :key="scope.value"
      class="scope-card"
    >
      <strong>{{ scope.label }}</strong>
      <span>{{ metricText(scope.value) }}</span>
      <div class="scope-actions">
        <button class="primary" @click="$emit('enter', scope.value)">灯塔工作台</button>
        <a
          v-if="handoverLinks[scope.value]"
          class="secondary"
          :href="handoverLinks[scope.value]"
          target="_blank"
          rel="noopener noreferrer"
        >
          交接班
        </a>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
type Dict = Record<string, any>;

const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
}>();

defineEmits<{
  enter: [scope: string];
}>();

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function metricText(scope: string): string {
  const item = props.overview[normalizeScopeValue(scope, "ALL")] || {};
  return [
    `维保 ${item.maintenance_pending || 0}`,
    `变更 ${item.change_pending || 0}`,
    `检修 ${item.repair_pending || 0}`,
    `进行中 ${(item.maintenance_ongoing || 0) + (item.change_ongoing || 0) + (item.repair_ongoing || 0)}`,
  ].join(" / ");
}
</script>

<style scoped>
.home-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 14px;
  padding: 24px;
}

.scope-card {
  min-height: 112px;
  display: grid;
  align-content: center;
  gap: 10px;
  padding: 18px;
  text-align: left;
  border: 1px solid #dbe3ee;
  border-radius: 6px;
  background: #ffffff;
  color: #0f172a;
}

.scope-card strong {
  font-size: 22px;
}

.scope-card span {
  color: #64748b;
  line-height: 1.5;
}

.scope-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.scope-actions button,
.scope-actions a {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 10px;
  color: #0f172a;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.scope-actions .primary {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

.scope-actions .secondary:hover {
  background: #f8fbff;
}
</style>
