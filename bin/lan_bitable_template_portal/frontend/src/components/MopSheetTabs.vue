<template>
  <section class="mop-sheet-tabs" aria-label="MOP Sheet 切换">
    <div class="sheet-tabs">
      <button type="button"
        v-for="sheet in sheets"
        :key="String(sheet.name || '')"
        :class="{ active: String(sheet.name || '') === modelValue }"
        @click="$emit('update:modelValue', String(sheet.name || ''))"
      >
        {{ sheet.name || "未命名 Sheet" }}
      </button>
    </div>
    <div v-if="activeSheet?.truncated" class="sheet-note">
      已显示 {{ activeSheet.row_count }} 行 / {{ activeSheet.column_count }} 列
    </div>
  </section>
</template>

<script setup lang="ts">
type Dict = Record<string, any>;

defineProps<{
  modelValue: string;
  sheets: Dict[];
  activeSheet: Dict | null;
}>();

defineEmits<{
  "update:modelValue": [value: string];
}>();
</script>

<style scoped>
.mop-sheet-tabs {
  display: grid;
  grid-column: 1;
  gap: 5px;
  min-width: 0;
}

.sheet-tabs {
  display: flex;
  gap: 5px;
  overflow-x: auto;
  padding: 3px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.94), rgba(248, 251, 255, 0.84)),
    #ffffff;
  scrollbar-width: thin;
  box-shadow: 0 8px 18px rgba(37, 99, 235, 0.05);
}

.sheet-tabs button {
  flex: 0 0 auto;
  min-height: 29px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 4px 10px;
  background: #ffffff;
  color: #475569;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}

.sheet-tabs button.active {
  border-color: #1e63ff;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.18);
}

.sheet-note {
  padding: 6px 9px;
  border: 1px solid #fde68a;
  border-radius: 12px;
  color: #92400e;
  background: #fffbeb;
  font-size: 12px;
  font-weight: 850;
}
</style>
