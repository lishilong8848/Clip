<template>
  <aside class="panel records-panel">
    <div class="panel-head">
      <div>
        <h2><span class="step-badge">1</span>待发起事项</h2>
        <small>先选事项，再到中间核对通告</small>
      </div>
      <div class="panel-head-status">
        <b>{{ rows.length }} 项</b>
        <em>{{ headStepText }}</em>
      </div>
    </div>
    <VirtualNoticeList
      :rows="rows"
      :selected-id="selectedId"
      show-status
      empty-title="没有待发起事项"
      empty-text="当前筛选条件下没有可发起事项。"
      empty-hint="可切换通告类型、专业，或使用刷新数据。"
      @select="emit('select', $event)"
    />
  </aside>
</template>

<script setup lang="ts">
import { computed } from "vue";
import VirtualNoticeList, { type NoticeRow } from "./VirtualNoticeList.vue";

const props = defineProps<{
  rows: NoticeRow[];
  selectedId: string;
}>();

const emit = defineEmits<{
  select: [row: NoticeRow | undefined];
}>();

const headStepText = computed(() => {
  if (!props.rows.length) return "无匹配事项";
  if (props.selectedId) return "已选择，核对通告";
  return "下一步：选择事项";
});
</script>

<style scoped>
.records-panel {
  position: relative;
  overflow: hidden;
  display: grid;
  align-content: start;
  gap: 14px;
  min-height: 0;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  padding: 14px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
  margin: -14px -14px 0;
  border-radius: 22px 22px 0 0;
  border-bottom: 1px solid #e7f0fb;
  padding: 14px 16px 10px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.98)),
    linear-gradient(90deg, rgba(48, 128, 255, 0.06), transparent 42%);
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.04);
}

.panel-head h2 {
  margin: 0;
  color: #09204a;
  font-size: 16px;
  font-weight: 900;
  letter-spacing: 0;
}

.panel-head small {
  display: block;
  margin-top: 4px;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.35;
}

.step-badge {
  display: inline-grid;
  place-items: center;
  width: 22px;
  min-width: 22px;
  height: 22px;
  margin-right: 8px;
  border-radius: 8px;
  color: #ffffff;
  font-size: 12px;
  font-weight: 950;
  vertical-align: 1px;
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
  box-shadow: 0 6px 14px rgba(22, 120, 255, 0.18);
}

.panel-head-status {
  flex: 0 0 auto;
  display: grid;
  justify-items: end;
  gap: 4px;
  min-width: 0;
}

.panel-head-status b,
.panel-head-status em {
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 3px 8px;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
  font-size: 12px;
  font-weight: 900;
  font-style: normal;
  line-height: 1.2;
  white-space: nowrap;
}

.panel-head-status em {
  border-color: #d8e5f7;
  background: #ffffff;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}
</style>
