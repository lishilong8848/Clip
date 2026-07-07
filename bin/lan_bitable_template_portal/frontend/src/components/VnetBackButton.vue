<template>
  <button type="button"
    class="vnet-back-button"
    :disabled="disabled"
    :title="title || '返回'"
    @click="handleClick"
  >
    <span aria-hidden="true">‹</span>
    <slot>返回</slot>
  </button>
</template>

<script setup lang="ts">
import { navigate, navigateHard } from "../navigation";

const props = defineProps<{
  to?: string;
  hard?: boolean;
  disabled?: boolean;
  title?: string;
}>();

const emit = defineEmits<{
  click: [];
}>();

function handleClick(): void {
  if (props.disabled) return;
  if (props.to) {
    if (props.hard) {
      navigateHard(props.to);
    } else {
      navigate(props.to);
    }
    return;
  }
  emit("click");
}
</script>

<style scoped>
.vnet-back-button {
  min-height: 40px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  border: 1px solid #cfe0ff;
  border-radius: 16px;
  padding: 0 16px;
  background: rgba(255, 255, 255, 0.94);
  color: #0f4fb8;
  font: inherit;
  font-size: 13px;
  font-weight: 900;
  line-height: 1;
  box-shadow: 0 8px 18px rgba(15, 86, 228, 0.06);
  cursor: pointer;
  transition:
    transform 0.16s ease,
    border-color 0.16s ease,
    background 0.16s ease,
    box-shadow 0.16s ease;
}

.vnet-back-button:hover:not(:disabled) {
  border-color: #9cc7ff;
  background: #f5faff;
  transform: translateY(-1px);
  box-shadow: 0 12px 26px rgba(15, 86, 228, 0.1);
}

.vnet-back-button:disabled {
  cursor: not-allowed;
  opacity: 0.64;
  transform: none;
}

.vnet-back-button:focus-visible {
  outline: none;
  box-shadow:
    0 0 0 3px rgba(21, 93, 252, 0.14),
    0 10px 22px rgba(15, 86, 228, 0.08);
}

.vnet-back-button span {
  font-size: 20px;
  line-height: 1;
}
</style>
