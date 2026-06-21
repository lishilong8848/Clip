<template>
  <div class="table-fill-toolbar">
    <div class="table-fill-toolbar-row">
      <div class="table-fill-title">
        <strong>表格填写</strong>
        <span>单击编辑 · 拖选复制/粘贴/还原</span>
      </div>
      <div class="table-fill-actions">
        <button v-if="bulkCount" class="btn blue" type="button" title="仅批量填写第10行之后识别到的正常/异常项" @click="emit('mark-all-normal')">
          第10行后全正常
        </button>
        <em v-if="filledCount">已填 {{ filledCount }}</em>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  bulkCount: number;
  filledCount: number;
}>();

const emit = defineEmits<{
  "mark-all-normal": [];
}>();
</script>

<style scoped>
.table-fill-toolbar {
  position: relative;
  top: 0;
  z-index: 6;
  display: grid;
  border-bottom: 1px solid rgba(216, 229, 247, 0.94);
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.98), rgba(255, 255, 255, 0.97)),
    #ffffff;
  padding: 5px 8px;
  backdrop-filter: blur(10px);
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.05);
}

.table-fill-toolbar-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}

.table-fill-title {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.table-fill-title strong {
  flex: 0 0 auto;
  border-radius: 999px;
  background: linear-gradient(135deg, #eff6ff, #dbeafe);
  color: #0757d7;
  padding: 4px 8px;
  font-size: 12px;
  font-weight: 950;
  line-height: 1;
  white-space: nowrap;
}

.table-fill-title span {
  min-width: 0;
  overflow: hidden;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.table-fill-actions {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
}

.table-fill-actions em {
  border-radius: 999px;
  background: #dcfce7;
  padding: 4px 8px;
  color: #15803d;
  font-size: 12px;
  font-style: normal;
  font-weight: 900;
  white-space: nowrap;
}

.btn {
  min-height: 28px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 4px 9px;
  color: #0f172a;
  font-size: 12px;
  font-weight: 850;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.btn.blue {
  border-color: transparent;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

@media (max-width: 1180px) {
  .table-fill-toolbar-row {
    align-items: stretch;
    flex-direction: column;
    gap: 6px;
  }

  .table-fill-title {
    align-items: flex-start;
    flex-direction: column;
    gap: 5px;
  }

  .table-fill-title span {
    white-space: normal;
  }

  .table-fill-actions {
    justify-content: flex-start;
  }
}
</style>
