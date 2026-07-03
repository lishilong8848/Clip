<template>
  <div class="signature-guide-strip" aria-label="签名上传条件">
    <template v-for="item in items" :key="item.key">
      <button
        v-if="item.role"
        type="button"
        class="signature-guide-item actionable"
        :class="[{ ready: item.ready }, item.tone]"
        @click="$emit('select-role', item.role)"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.text }}</small>
      </button>
      <div
        v-else
        class="signature-guide-item"
        :class="[{ ready: item.ready }, item.tone]"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.text }}</small>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import type { MopSignatureRole } from "./MopSignatureRoleSummary.vue";

defineProps<{
  items: Array<{
    key: string;
    label: string;
    value: string;
    text: string;
    ready: boolean;
    tone: string;
    role: MopSignatureRole | "";
  }>;
}>();

defineEmits<{
  "select-role": [role: MopSignatureRole];
}>();
</script>

<style scoped>
.signature-guide-strip {
  min-width: 0;
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 5px;
}

.signature-guide-item {
  min-width: 0;
  min-height: 32px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 4px;
  border: 1px solid #fed7aa;
  border-radius: 999px;
  padding: 4px 8px;
  background:
    linear-gradient(135deg, rgba(255, 247, 237, 0.98), rgba(255, 255, 255, 0.92)),
    #ffffff;
  color: #9a3412;
  font: inherit;
  text-align: left;
}

button.signature-guide-item {
  cursor: pointer;
}

button.signature-guide-item:hover {
  border-color: #fb923c;
  box-shadow: 0 12px 24px rgba(249, 115, 22, 0.12);
  transform: translateY(-1px);
}

.signature-guide-item.ready {
  border-color: #bbf7d0;
  background:
    linear-gradient(135deg, rgba(236, 253, 245, 0.98), rgba(255, 255, 255, 0.92)),
    #ffffff;
  color: #047857;
}

.signature-guide-item.actionable.ready:hover {
  border-color: #34d399;
  box-shadow: 0 12px 24px rgba(4, 120, 87, 0.12);
}

.signature-guide-item span,
.signature-guide-item small,
.signature-guide-item strong {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
}

.signature-guide-item span {
  color: #64748b;
  font-size: 11px;
  font-weight: 950;
  white-space: nowrap;
}

.signature-guide-item strong {
  color: currentColor;
  font-size: 12px;
  font-weight: 950;
  line-height: 1.1;
  text-align: right;
  white-space: nowrap;
}

.signature-guide-item small {
  display: none;
}

@media (max-width: 1180px) {
  .signature-guide-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .signature-guide-strip {
    grid-template-columns: 1fr;
  }
}
</style>
