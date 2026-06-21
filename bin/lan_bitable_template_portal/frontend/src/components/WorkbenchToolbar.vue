<template>
  <div class="toolbar">
    <div class="toolbar-main">
      <div class="toolbar-label">
        <strong>通告类型</strong>
        <span>{{ activeTypeHint }}</span>
      </div>
      <div class="segmented" aria-label="通告类型">
        <button
          v-for="type in filterWorkTypes"
          :key="type.value"
          :class="{ active: workType === type.value }"
          type="button"
          @click="$emit('select-work-type', type.value)"
        >
          <span>{{ type.label }}</span>
          <b>{{ recordTypeCounts[type.value] || 0 }}</b>
        </button>
      </div>
    </div>

    <div v-if="hasExtraFilters" class="active-filter-line" aria-label="当前筛选条件">
      <strong class="active-filter-title">当前筛选</strong>
      <button
        v-if="workType"
        class="filter-chip type-active removable"
        type="button"
        title="清除通告类型筛选"
        @click="$emit('select-work-type', '')"
      >
        类型：{{ activeTypeLabel }} <b>×</b>
      </button>
      <button
        v-if="specialtyFilter"
        class="filter-chip removable"
        type="button"
        title="清除专业筛选"
        @click="$emit('update:specialtyFilter', '')"
      >
        专业：{{ specialtyFilter }} <b>×</b>
      </button>
      <button
        v-if="searchText"
        class="filter-chip removable"
        type="button"
        title="清除搜索内容"
        @click="$emit('update:searchText', '')"
      >
        搜索：{{ searchText }} <b>×</b>
      </button>
      <button
        class="filter-clear"
        type="button"
        @click="$emit('clear-filters')"
      >
        清空筛选
      </button>
    </div>

    <div class="toolbar-bottom">
      <div class="toolbar-filters">
        <div class="search-box">
          <input
            :value="searchText"
            class="search"
            placeholder="搜索标题、楼栋、专业"
            @input="$emit('update:searchText', ($event.target as HTMLInputElement).value)"
          />
          <button
            v-if="searchText"
            class="search-clear"
            type="button"
            aria-label="清除搜索"
            title="清除搜索"
            @click="$emit('update:searchText', '')"
          >
            清除
          </button>
        </div>

        <label class="specialty-filter">
          <span>专业</span>
          <select
            :value="specialtyFilter"
            @change="$emit('update:specialtyFilter', ($event.target as HTMLSelectElement).value)"
          >
            <option value="">全部</option>
            <option v-for="item in specialtyFilterOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
      </div>

      <div class="toolbar-actions">
        <div class="action-cluster">
          <div class="action-cluster-label">
            <strong>新建通告</strong>
            <span>{{ newNoticeHint }}</span>
          </div>
          <div class="action-cluster-buttons">
            <ManualTypePicker
              :open="manualPickerOpen"
              :work-types="workTypes"
              :recent-types="manualRecentTypes"
              :prefill-types="manualPrefillTypes"
              @update:open="$emit('update:manualPickerOpen', $event)"
              @select="$emit('manual-select', $event)"
            />

            <button
              class="btn ghost paste-action"
              type="button"
              title="粘贴标准通告文本后自动解析成草稿"
              @click="$emit('toggle-paste')"
            >
              解析粘贴
            </button>
          </div>
        </div>
        <details v-if="isAdmin" class="low-frequency-menu">
          <summary title="打开管理员更多工具">更多工具</summary>
          <div class="low-frequency-actions">
            <button
              class="btn ghost memory-action"
              type="button"
              title="管理员批量导入历史通告记忆"
              @click="$emit('toggle-memory')"
            >
              历史记忆导入
            </button>
          </div>
        </details>

        <span v-if="draftSaveText" class="draft-save-status" :class="{ failed: draftSaveFailed }">
          {{ draftSaveText }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import ManualTypePicker from "./ManualTypePicker.vue";
import type { WorkTypeOption, WorkTypeValue } from "../types";

const props = defineProps<{
  workTypes: WorkTypeOption[];
  workType: string;
  recordTypeCounts: Record<string, number>;
  searchText: string;
  specialtyFilter: string;
  specialtyFilterOptions: string[];
  manualPickerOpen: boolean;
  manualRecentTypes: WorkTypeOption[];
  manualPrefillTypes: string[];
  isAdmin: boolean;
  draftSaveText: string;
  draftSaveFailed: boolean;
}>();

defineEmits<{
  "select-work-type": [value: string];
  "update:searchText": [value: string];
  "update:specialtyFilter": [value: string];
  "update:manualPickerOpen": [value: boolean];
  "manual-select": [value: WorkTypeValue];
  "toggle-paste": [];
  "toggle-memory": [];
  "clear-filters": [];
}>();

const filterWorkTypes = computed(() => [
  { value: "", label: "全部" },
  ...props.workTypes,
]);

const activeTypeLabel = computed(() => {
  if (!props.workType) return "全部类型";
  return props.workTypes.find((item) => item.value === props.workType)?.label || "当前类型";
});

const activeTypeHint = computed(() => {
  if (!props.workType) return "全部通告";
  return `仅看${activeTypeLabel.value}`;
});

const hasExtraFilters = computed(() => {
  return Boolean(props.workType || props.searchText.trim() || props.specialtyFilter);
});

const newNoticeHint = computed(() => {
  if (!props.workType) return "可选类型";
  return `新建${activeTypeLabel.value}`;
});
</script>

<style scoped>
.toolbar {
  display: grid;
  gap: 10px;
  padding: 12px;
  border: 1px solid rgba(191, 219, 254, 0.8);
  border-radius: 22px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.95), rgba(248, 251, 255, 0.86)),
    #fff;
  box-shadow: 0 12px 30px rgba(37, 99, 235, 0.08);
}

.toolbar-main,
.toolbar-bottom,
.toolbar-filters {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
}

.toolbar-main {
  padding-bottom: 8px;
  border-bottom: 1px solid rgba(216, 229, 247, 0.82);
}

.active-filter-line {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  min-width: 0;
  padding: 2px;
}

.active-filter-title {
  flex: 0 0 auto;
  color: #3156c9;
  font-size: 12px;
  font-weight: 950;
}

.filter-chip {
  min-height: 28px;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  max-width: min(100%, 320px);
  overflow: hidden;
  border: 1px solid #dbe7f5;
  border-radius: 999px;
  padding: 5px 10px;
  background: rgba(248, 251, 255, 0.88);
  color: #48627f;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.filter-chip.removable {
  cursor: pointer;
}

.filter-chip b {
  display: inline-grid;
  place-items: center;
  flex: 0 0 auto;
  width: 17px;
  height: 17px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.82);
  color: currentColor;
  font-size: 12px;
  line-height: 1;
}

.filter-chip:hover {
  border-color: #a7c7ff;
  background: #ffffff;
}

.filter-chip.type-active {
  border-color: #cfe0ff;
  background: #eff6ff;
  color: #0757d7;
}

.filter-clear {
  min-height: 28px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 4px 10px;
  background: #ffffff;
  color: #0757d7;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}

.filter-clear:hover {
  border-color: #8dbbfb;
  background: #f5faff;
}

.toolbar-bottom {
  display: grid;
  grid-template-columns: minmax(320px, 1fr) auto;
  align-items: start;
  justify-content: stretch;
}

.toolbar-filters {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) auto;
  flex: 1 1 auto;
  align-items: center;
}

.toolbar-label {
  flex: 0 0 auto;
  display: grid;
  gap: 2px;
  min-width: 76px;
}

.toolbar-label strong {
  color: #0f2f6a;
  font-size: 13px;
  font-weight: 950;
}

.toolbar-label span {
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.toolbar-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  align-items: stretch;
  flex-wrap: wrap;
  min-width: 0;
}

.action-cluster {
  flex: 0 1 auto;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 42px;
  min-width: 0;
  border: 1px solid rgba(191, 219, 254, 0.74);
  border-radius: 16px;
  padding: 4px 6px 4px 10px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.9), rgba(255, 255, 255, 0.86)),
    #fff;
}

.action-cluster-label {
  flex: 0 0 auto;
  display: grid;
  gap: 1px;
  color: #3156c9;
  white-space: nowrap;
}

.action-cluster-label strong {
  color: #0f2f6a;
  font-size: 12px;
  font-weight: 950;
}

.action-cluster-label span {
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.action-cluster-buttons {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  min-width: 0;
}

.segmented {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  min-width: 0;
}

.segmented button {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 35px;
  border: 1px solid rgba(191, 219, 254, 0.7);
  border-radius: 999px;
  padding: 6px 11px;
  background: rgba(255, 255, 255, 0.82);
  color: #334155;
  font-weight: 900;
  cursor: pointer;
  transition: background 0.15s ease, color 0.15s ease, box-shadow 0.15s ease;
}

.segmented button b {
  display: inline-grid;
  place-items: center;
  min-width: 24px;
  min-height: 22px;
  border-radius: 999px;
  padding: 0 7px;
  background: rgba(226, 232, 240, 0.72);
  color: #31506f;
  font-size: 12px;
  line-height: 1;
}

.segmented button.active {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
  box-shadow: 0 10px 22px rgba(37, 99, 235, 0.2);
}

.segmented button.active b {
  background: rgba(255, 255, 255, 0.22);
  color: #fff;
}

.search-box {
  position: relative;
  flex: 1 1 auto;
  min-width: 180px;
}

.search,
.specialty-filter select {
  min-height: 38px;
  border: 1px solid rgba(203, 213, 225, 0.88);
  border-radius: 15px;
  background: rgba(255, 255, 255, 0.9);
  color: #0f172a;
  font-weight: 700;
}

.search {
  width: 100%;
  min-width: 0;
  padding: 0 62px 0 14px;
}

.search-clear {
  position: absolute;
  top: 50%;
  right: 6px;
  min-height: 28px;
  border: 1px solid rgba(191, 219, 254, 0.78);
  border-radius: 999px;
  padding: 0 9px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
  transform: translateY(-50%);
}

.search-clear:hover {
  background: #dbeafe;
  transform: translateY(-50%);
}

.specialty-filter {
  display: inline-flex;
  gap: 8px;
  align-items: center;
  min-height: 38px;
  padding: 4px 6px 4px 12px;
  border: 1px solid rgba(191, 219, 254, 0.78);
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.9);
}

.specialty-filter span {
  color: #475569;
  font-size: 12px;
  font-weight: 900;
}

.specialty-filter select {
  min-width: 112px;
  padding: 0 10px;
}

.btn {
  min-height: 38px;
  border: 1px solid rgba(191, 219, 254, 0.78);
  border-radius: 15px;
  padding: 0 14px;
  background: rgba(255, 255, 255, 0.82);
  color: #1d4ed8;
  font-weight: 900;
  white-space: nowrap;
  cursor: pointer;
  box-shadow: 0 8px 18px rgba(37, 99, 235, 0.06);
}

.paste-action {
  border-color: #bdd7ff;
  background: #eff6ff;
  color: #0757d7;
}

.memory-action {
  border-color: #d8e5f7;
  color: #48627f;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.low-frequency-menu {
  position: relative;
  flex: 0 0 auto;
  z-index: var(--cf-z-dropdown, 720);
}

.low-frequency-menu summary {
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 15px;
  padding: 0 13px;
  background: rgba(248, 251, 255, 0.86);
  color: #48627f;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
  list-style: none;
  white-space: nowrap;
}

.low-frequency-menu summary::-webkit-details-marker {
  display: none;
}

.low-frequency-menu summary::after {
  content: "⌄";
  color: #0757d7;
  font-size: 13px;
  font-weight: 950;
  line-height: 1;
}

.low-frequency-menu[open] summary {
  border-color: #bdd2f4;
  background: #ffffff;
  box-shadow: 0 10px 22px rgba(15, 86, 228, 0.08);
}

.low-frequency-menu[open] summary::after {
  content: "⌃";
}

.low-frequency-actions {
  position: absolute;
  right: 0;
  top: calc(100% + 8px);
  display: grid;
  min-width: 190px;
  gap: 8px;
  padding: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: var(--cf-shadow-popover, 0 20px 44px rgba(7, 37, 86, 0.24));
}

.low-frequency-actions .btn {
  width: 100%;
  justify-content: center;
}

.draft-save-status {
  max-width: 180px;
  overflow: hidden;
  text-overflow: ellipsis;
  padding: 7px 10px;
  border-radius: 999px;
  background: rgba(220, 252, 231, 0.82);
  color: #047857;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.draft-save-status.failed {
  background: rgba(254, 226, 226, 0.9);
  color: #b91c1c;
}

@media (max-width: 1280px) {
  .toolbar-bottom {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .toolbar-actions {
    width: 100%;
    align-items: stretch;
    justify-content: flex-start;
  }

  .action-cluster {
    width: 100%;
    align-items: flex-start;
    flex-direction: column;
  }

  .memory-action {
    flex: 1 1 180px;
  }

  .low-frequency-menu {
    flex: 1 1 180px;
  }

  .low-frequency-menu summary {
    width: 100%;
  }

  .action-cluster-buttons {
    width: 100%;
    flex-wrap: wrap;
  }
}

@media (max-width: 760px) {
  .toolbar-main {
    align-items: stretch;
    flex-direction: column;
  }

  .active-filter-line {
    align-items: stretch;
  }

  .filter-chip,
  .filter-clear {
    max-width: 100%;
  }

  .toolbar-filters {
    align-items: stretch;
    grid-template-columns: 1fr;
  }

  .toolbar-label {
    min-width: 0;
  }

  .segmented,
  .specialty-filter,
  .toolbar-actions {
    width: 100%;
  }

  .segmented button {
    flex: 1 1 96px;
    justify-content: center;
  }

  .toolbar-actions > * {
    flex: 1 1 150px;
  }

  .action-cluster-buttons > * {
    flex: 1 1 150px;
  }
}
</style>
