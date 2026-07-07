<template>
  <section class="admin-overview" aria-label="管理员概览">
    <button type="button"
      v-for="item in overviewItems"
      :key="item.key"
      :class="item.tone"
      :aria-label="item.ariaLabel"
      :title="`打开${item.targetLabel}`"
      @click="emit('select-tab', item.target)"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
      <small>{{ item.hint }}</small>
    </button>
  </section>

  <nav class="tabs admin-workspace-tabs" aria-label="管理员工作区">
    <button type="button"
      v-for="item in tabs"
      :key="item.key"
      :class="{ active: activeTab === item.key }"
      @click="emit('select-tab', item.key)"
    >
      <strong>{{ item.label }}</strong>
      <small v-if="item.description">{{ item.description }}</small>
      <b v-if="item.badge">{{ item.badge }}</b>
    </button>
  </nav>

  <div class="admin-advanced-toggle">
    <button type="button" class="btn ghost" @click="emit('toggle-advanced')">
      {{ advancedVisible ? "收起高级诊断" : "显示高级诊断" }}
    </button>
  </div>

  <section class="admin-current-guide" :class="activeTab" aria-live="polite">
    <div>
      <strong>{{ activeGuide.title }}</strong>
    </div>
    <span>{{ activeGuide.badge }}</span>
  </section>
</template>

<script setup lang="ts">
import type { AdminTabKey } from "../adminToolsUtils";

type AdminOverviewItem = {
  key: string;
  label: string;
  value: string;
  hint: string;
  tone: string;
  target: AdminTabKey;
  targetLabel: string;
  ariaLabel: string;
};

type AdminTabItem = {
  key: AdminTabKey;
  label: string;
  description: string;
  badge: string;
};

defineProps<{
  overviewItems: AdminOverviewItem[];
  tabs: AdminTabItem[];
  activeTab: AdminTabKey;
  activeGuide: { title: string; text?: string; badge: string };
  advancedVisible: boolean;
}>();

const emit = defineEmits<{
  "select-tab": [tab: AdminTabKey];
  "toggle-advanced": [];
}>();
</script>

<style scoped>
.admin-overview {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.admin-overview button {
  min-width: 0;
  display: grid;
  gap: 4px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 12px 14px;
  background: rgba(255, 255, 255, 0.86);
  color: inherit;
  font: inherit;
  text-align: left;
  box-shadow: 0 10px 24px rgba(15, 73, 153, 0.07);
  cursor: pointer;
  transition:
    border-color 0.16s ease,
    box-shadow 0.16s ease,
    transform 0.16s ease;
}

.admin-overview button:hover {
  border-color: #a7c7ff;
  box-shadow: 0 14px 30px rgba(15, 73, 153, 0.12);
  transform: translateY(-1px);
}

.admin-overview button:focus-visible {
  outline: 3px solid rgba(30, 99, 255, 0.18);
  outline-offset: 2px;
}

.admin-overview button span,
.admin-overview button small {
  min-width: 0;
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.admin-overview button strong {
  min-width: 0;
  overflow: hidden;
  color: #075bd8;
  font-size: 20px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.admin-overview button.good {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.admin-overview button.good strong {
  color: #047857;
}

.admin-overview button.warn {
  border-color: #fde68a;
  background: #fffbeb;
}

.admin-overview button.warn strong {
  color: #b45309;
}

.tabs {
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  padding: 6px;
  background: rgba(255, 255, 255, 0.76);
}

.tabs button {
  border: 1px solid transparent;
  background: rgba(248, 251, 255, 0.72);
}

.admin-workspace-tabs {
  position: sticky;
  top: 72px;
  z-index: 2;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 6px;
  overflow: visible;
}

.admin-workspace-tabs button {
  position: relative;
  min-width: 0;
  min-height: 58px;
  display: grid;
  align-content: center;
  justify-items: start;
  gap: 4px;
  border-radius: 16px;
  padding: 9px 12px;
  color: #475569;
  font: inherit;
  text-align: left;
  cursor: pointer;
  white-space: normal;
}

.admin-workspace-tabs button.active {
  border-color: #1e63ff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.22);
}

.admin-workspace-tabs strong,
.admin-workspace-tabs small {
  min-width: 0;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.admin-workspace-tabs strong {
  font-size: 14px;
  font-weight: 950;
}

.admin-workspace-tabs small {
  color: inherit;
  font-size: 12px;
  font-weight: 750;
  opacity: 0.82;
}

.admin-workspace-tabs b {
  position: absolute;
  top: 6px;
  right: 7px;
  min-width: 22px;
  height: 22px;
  display: inline-grid;
  place-items: center;
  border-radius: 999px;
  padding: 0 6px;
  background: #fff7ed;
  color: #c2410c;
  font-size: 11px;
  font-weight: 950;
}

.admin-workspace-tabs button.active b {
  background: rgba(255, 255, 255, 0.92);
  color: #075bd8;
}

.admin-advanced-toggle {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  min-width: 0;
  margin-top: -4px;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
}

.admin-advanced-toggle .btn {
  min-height: 32px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 6px 12px;
  background: rgba(248, 251, 255, 0.86);
  color: #3156c9;
  font: inherit;
  font-size: 12px;
  font-weight: 850;
  cursor: pointer;
}

.admin-current-guide {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  border: 1px solid rgba(191, 219, 254, 0.86);
  border-radius: 18px;
  padding: 12px 14px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: 0 10px 22px rgba(0, 47, 135, 0.06);
}

.admin-current-guide strong {
  display: block;
  color: #071a39;
  font-size: 15px;
  font-weight: 950;
}

.admin-current-guide span {
  flex: 0 0 auto;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 6px 10px;
  background: #eff6ff;
  color: #075bd8;
  font-size: 12px;
  font-weight: 950;
  white-space: nowrap;
}

.admin-current-guide.permissions span {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #9a3412;
}

.admin-current-guide.mop span {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

@media (max-width: 980px) {
  .admin-overview,
  .admin-workspace-tabs {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .admin-workspace-tabs {
    position: static;
  }

  .admin-current-guide {
    align-items: flex-start;
    flex-direction: column;
  }
}

@media (max-width: 640px) {
  .admin-overview,
  .admin-workspace-tabs {
    grid-template-columns: 1fr;
  }
}
</style>
