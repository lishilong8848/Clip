<template>
  <header class="preview-head">
    <button type="button" class="btn ghost preview-back" @click="$emit('back')">
      <span aria-hidden="true">‹</span>
      返回
    </button>
    <div class="preview-title">
      <strong>{{ title }}</strong>
      <p>
        <template v-if="noticeTitle">{{ noticeTitle }}</template>
        <template v-if="sheetName"> · {{ sheetName }} · {{ rowCount }} 行</template>
      </p>
    </div>
    <div class="preview-head-status">
      <span
        v-for="item in completionItems"
        :key="`head:${item.key}`"
        :class="{ done: item.done, pending: !item.done }"
      >
        {{ item.label }}：{{ item.text }}
      </span>
    </div>
  </header>
</template>

<script setup lang="ts">
defineProps<{
  title: string;
  noticeTitle: string;
  sheetName: string;
  rowCount: number;
  completionItems: Array<{
    key: string;
    label: string;
    text: string;
    done: boolean;
  }>;
}>();

defineEmits<{
  back: [];
}>();
</script>

<style scoped>
.preview-head {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) minmax(180px, auto);
  gap: 7px;
  align-items: center;
  padding: 6px 8px;
  border: 1px solid rgba(191, 219, 254, 0.78);
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(239, 246, 255, 0.86)),
    #fff;
  box-shadow: 0 8px 20px rgba(37, 99, 235, 0.07);
}

.btn {
  min-height: 29px;
  border: 1px solid rgba(191, 219, 254, 0.8);
  border-radius: 999px;
  padding: 0 10px;
  background: rgba(255, 255, 255, 0.86);
  color: #1d4ed8;
  font-weight: 900;
  cursor: pointer;
}

.btn span {
  font-size: 18px;
  line-height: 1;
}

.preview-back {
  border-color: rgba(30, 99, 255, 0.22);
  background: #eff6ff;
  color: #0757d7;
}

.preview-title {
  min-width: 0;
}

.preview-title strong {
  display: block;
  overflow: hidden;
  color: #0f172a;
  font-size: 13px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-title p {
  overflow: hidden;
  margin: 2px 0 0;
  color: #64748b;
  font-size: 11px;
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-head-status {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 3px;
  max-width: 430px;
  overflow: visible;
}

.preview-head-status span {
  min-width: 0;
  overflow: hidden;
  max-width: 150px;
  padding: 2px 6px;
  border-radius: 999px;
  background: rgba(255, 251, 235, 0.94);
  color: #92400e;
  font-size: 10px;
  font-weight: 900;
  line-height: 1.3;
  text-overflow: ellipsis;
  white-space: nowrap;
  text-align: center;
}

.preview-head-status span.done {
  background: rgba(220, 252, 231, 0.94);
  color: #047857;
}

@media (max-width: 980px) {
  .preview-head {
    grid-template-columns: 1fr;
  }

  .preview-head-status {
    max-width: none;
    justify-content: flex-start;
  }
}
</style>
