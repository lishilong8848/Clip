<template>
  <div class="notice-completion-panel" aria-label="通告字段完成度">
    <span
      v-for="item in items"
      :key="item.key"
      :class="{ done: item.done, pending: !item.done }"
    >
      <b>{{ item.done ? "完成" : "待补" }}</b>
      <strong>{{ item.label }}</strong>
      <em>{{ item.text }}</em>
    </span>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  items: Array<{
    key: string;
    label: string;
    text: string;
    done: boolean;
  }>;
}>();
</script>

<style scoped>
.notice-completion-panel {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  margin-top: 8px;
}

.notice-completion-panel span {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 3px 7px;
  min-width: 0;
  padding: 8px 9px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.86);
}

.notice-completion-panel span.done {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.notice-completion-panel span.pending {
  border-color: #fde68a;
  background: #fffbeb;
}

.notice-completion-panel b {
  grid-row: 1 / span 2;
  align-self: center;
  border-radius: 999px;
  padding: 4px 7px;
  background: rgba(255, 255, 255, 0.78);
  font-size: 11px;
  font-weight: 950;
  line-height: 1;
  white-space: nowrap;
}

.notice-completion-panel strong {
  color: #09204a;
  font-size: 12px;
  font-weight: 820;
}

.notice-completion-panel em {
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 720px) {
  .notice-completion-panel {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
