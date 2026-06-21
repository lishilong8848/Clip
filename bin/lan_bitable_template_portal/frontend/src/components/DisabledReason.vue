<template>
  <p v-if="text" class="disabled-reason" :class="tone">
    <strong>{{ label }}</strong>
    <span>{{ text }}</span>
  </p>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  text?: string;
  tone?: "neutral" | "warning" | "danger";
}>();

const label = computed(() => {
  if (props.tone === "danger") return "无法操作";
  if (props.tone === "warning") return "需处理";
  return "提示";
});
</script>

<style scoped>
.disabled-reason {
  margin: 6px 0 0;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  max-width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  padding: 6px 9px;
  color: #48627f;
  background: #f8fbff;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.35;
  overflow-wrap: anywhere;
}

.disabled-reason strong {
  flex: 0 0 auto;
  border-radius: 999px;
  padding: 2px 6px;
  background: rgba(255, 255, 255, 0.72);
  color: inherit;
  font-size: 11px;
  font-weight: 950;
  white-space: nowrap;
}

.disabled-reason span {
  min-width: 0;
}

.disabled-reason.warning {
  border-color: #fde68a;
  color: #92400e;
  background: #fffbeb;
}

.disabled-reason.danger {
  border-color: #fecaca;
  color: #991b1b;
  background: #fef2f2;
}
</style>
