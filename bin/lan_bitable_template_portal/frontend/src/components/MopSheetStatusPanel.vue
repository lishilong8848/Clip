<template>
  <div v-if="localFile" class="mop-file-status">
    <span>表格附件</span>
    <strong>{{ localFile.relative_path || localFile.file_name }}</strong>
    <small :title="String(localFile.path || '')">已就绪</small>
  </div>

  <div
    v-if="showDetectPanel"
    class="mop-detect-panel"
  >
    <div class="detect-summary">
      <span :class="{ muted: !isCover }">{{ isCover ? "封面页" : "非封面页" }}</span>
      <strong>正常/异常 {{ checkboxCount }} 个</strong>
      <strong>日期/签名位 {{ maintenanceFieldCount }} 个</strong>
      <em v-if="!isCover && filledCount">已填写 {{ filledCount }} 项</em>
    </div>
    <div v-if="isCover" class="detect-empty">封面页</div>
    <div v-else class="detect-empty">未识别</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { Dict } from "../api/client";

const props = defineProps<{
  localFile: Dict | null;
  activeSheet: Dict | null;
  checkboxCount: number;
  maintenanceFieldCount: number;
  filledCount: number;
}>();

const isCover = computed(() => Boolean(props.activeSheet?.is_cover));
const showDetectPanel = computed(() => Boolean(
  props.activeSheet && (
    isCover.value || (!props.checkboxCount && !props.maintenanceFieldCount)
  )
));
</script>

<style scoped>
.mop-file-status,
.mop-detect-panel {
  border: 1px solid rgba(191, 219, 254, 0.82);
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(248, 251, 255, 0.84)),
    #fff;
  box-shadow: 0 6px 14px rgba(37, 99, 235, 0.055);
}

.mop-file-status {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  padding: 5px 8px;
}

.mop-file-status span,
.detect-summary span {
  display: inline-flex;
  align-items: center;
  min-height: 20px;
  border-radius: 999px;
  padding: 2px 7px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 11px;
  font-weight: 900;
}

.mop-file-status strong {
  overflow: hidden;
  color: #0f172a;
  font-size: 11px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-file-status small {
  flex: 0 0 auto;
  border: 1px solid #bbf7d0;
  border-radius: 999px;
  padding: 2px 6px;
  background: #ecfdf5;
  color: #047857;
  font-size: 11px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-detect-panel {
  display: grid;
  gap: 4px;
  padding: 6px 8px;
}

.detect-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  align-items: center;
}

.detect-summary span,
.detect-summary strong,
.detect-summary em {
  padding: 2px 6px;
  border-radius: 999px;
  font-size: 11px;
  line-height: 1.2;
}

.detect-summary span {
  background: rgba(219, 234, 254, 0.94);
}

.detect-summary span.muted {
  background: rgba(241, 245, 249, 0.92);
  color: #64748b;
}

.detect-summary strong {
  background: rgba(220, 252, 231, 0.9);
  color: #047857;
}

.detect-summary em {
  background: rgba(254, 243, 199, 0.94);
  color: #92400e;
  font-style: normal;
  font-weight: 900;
}

.detect-empty {
  color: #64748b;
  font-size: 11px;
  line-height: 1.45;
}
</style>
