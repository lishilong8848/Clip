<template>
  <section class="paste-panel">
    <div class="panel-head compact-head">
      <h2>导入历史通告记忆</h2>
      <span>只写入记忆，不发送、不上传</span>
    </div>
    <textarea
      :value="memoryImportText"
      placeholder="可一次粘贴多条历史维保、变更、检修通告。导入后，同楼栋同标题/同维护总项的本月事项会自动回填。"
      @input="emit('update:memoryImportText', ($event.target as HTMLTextAreaElement).value)"
    ></textarea>
    <div class="card-actions">
      <span class="job-line" :class="{ success: memoryImportLineType === 'success', failed: memoryImportLineType === 'failed' }">
        {{ memoryImportLine }}
      </span>
      <button class="btn blue" :disabled="memoryImportBusy" @click="emit('import')">
        {{ memoryImportBusy ? "导入中" : "导入到记忆库" }}
      </button>
    </div>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  memoryImportText: string;
  memoryImportLine: string;
  memoryImportLineType: string;
  memoryImportBusy: boolean;
}>();

const emit = defineEmits<{
  "update:memoryImportText": [value: string];
  import: [];
}>();
</script>

<style scoped>
.paste-panel {
  display: grid;
  gap: 10px;
  margin-bottom: 16px;
  padding: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.panel-head.compact-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 0 0 4px;
}

.panel-head h2 {
  margin: 0;
  color: #0f2f6a;
  font-size: 16px;
  font-weight: 950;
}

.panel-head span {
  flex: 0 0 auto;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 4px 9px;
  background: rgba(239, 246, 255, 0.9);
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 850;
  white-space: nowrap;
}

.paste-panel textarea {
  min-height: 100px;
  width: 100%;
  resize: vertical;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.94);
  color: #0f172a;
  font: inherit;
  line-height: 1.55;
  outline: none;
}

.paste-panel textarea:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.card-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
}

.job-line {
  flex: 1 1 auto;
  color: #64748b;
  font-size: 13px;
  font-weight: 800;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}

.btn {
  min-height: 40px;
  border: 1px solid #cfe0ff;
  border-radius: 16px;
  padding: 0 14px;
  background: #fff;
  color: #1d4ed8;
  font-weight: 900;
  cursor: pointer;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

@media (max-width: 760px) {
  .panel-head.compact-head,
  .card-actions {
    align-items: stretch;
    flex-direction: column;
  }

  .panel-head span {
    width: fit-content;
  }

  .btn {
    width: 100%;
  }
}
</style>
