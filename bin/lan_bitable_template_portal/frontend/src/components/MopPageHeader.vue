<template>
  <header class="mop-head">
    <div>
      <strong>工程师 MOP 填写</strong>
    </div>
    <div class="head-actions">
      <label v-if="scopeOptions.length" class="scope-select">
        楼栋
        <select :value="scope" :disabled="loading || previewLoading" aria-label="切换楼栋" @change="updateScope">
          <option v-for="item in scopeOptions" :key="item.value" :value="normalizeMopScope(item.value)">
            {{ item.label }}
          </option>
        </select>
      </label>
      <button
        class="btn ghost refresh-mini"
        type="button"
        :disabled="loading"
        :aria-busy="loading ? 'true' : 'false'"
        title="刷新本页数据"
        @click="$emit('refresh')"
      >
        {{ loading ? "刷新中" : "刷新" }}
      </button>
    </div>
  </header>
</template>

<script setup lang="ts">
import { normalizeMopScope } from "../mopSelectionUtils";

defineProps<{
  scope: string;
  scopeOptions: Array<{ value: string; label: string }>;
  loading: boolean;
  previewLoading: boolean;
}>();

const emit = defineEmits<{
  "update:scope": [value: string];
  refresh: [];
}>();

function updateScope(event: Event): void {
  emit("update:scope", normalizeMopScope((event.target as HTMLSelectElement).value));
}
</script>

<style scoped>
.mop-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 22px 24px;
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.88);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.mop-head strong {
  display: block;
  color: #0f172a;
  font-size: 24px;
  font-weight: 700;
}

.head-actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
}

.scope-select {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #475569;
  font-size: 13px;
  font-weight: 700;
}

select {
  min-height: 44px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 11px;
  background: #ffffff;
  color: #0f172a;
  outline: none;
}

select:focus,
select:focus-visible {
  border-color: #1e63ff;
  outline: 3px solid rgba(75, 153, 255, 0.38);
  outline-offset: 2px;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.btn {
  min-height: 44px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 14px;
  background: #ffffff;
  color: #0f172a;
  font-weight: 750;
  text-decoration: none;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease, background 0.16s ease;
}

.btn:hover:not(:disabled) {
  border-color: #9cc7ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.08);
  transform: translateY(-1px);
}

.btn:focus-visible {
  outline: 3px solid rgba(75, 153, 255, 0.38);
  outline-offset: 2px;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
  box-shadow: none;
  transform: none;
}

.btn.ghost {
  background: #f8fbff;
  color: #1d4ed8;
}

.btn[aria-busy="true"] {
  position: relative;
  color: transparent;
}

.btn[aria-busy="true"]::after {
  content: "";
  position: absolute;
  inset: 50% auto auto 50%;
  width: 16px;
  height: 16px;
  margin: -8px 0 0 -8px;
  border: 2px solid rgba(29, 78, 216, 0.25);
  border-top-color: #1d4ed8;
  border-radius: 999px;
  animation: mopSpin 0.8s linear infinite;
}

.refresh-mini {
  min-width: 56px;
  min-height: 44px;
  border-radius: 999px;
  padding: 6px 12px;
}

@keyframes mopSpin {
  to {
    transform: rotate(360deg);
  }
}
</style>
