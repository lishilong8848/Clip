<template>
  <div class="table-fill-toolbar">
    <div class="table-fill-toolbar-row">
      <div class="table-fill-title">
        <strong>表格填写</strong>
        <span v-if="showEditingTools" class="fill-instructions">单击编辑 · 拖选复制/粘贴/还原</span>
        <span class="fill-count" role="status">{{ filledCount }} / {{ totalCount }} 项 · 其它内容选填</span>
      </div>
      <div class="table-fill-actions">
        <button type="button" v-if="bulkCount && showEditingTools" class="btn blue" title="仅批量填写第10行之后识别到的正常/异常项" @click="emit('mark-all-normal')">
          第10行后全正常
        </button>
        <button v-if="pendingTargets.length" type="button" class="btn next-missing" :class="{ optional: !requiredCount }" @click="emit('next-missing')">{{ requiredCount ? '下一待签名处' : '下一选填项' }}</button>
        <span v-else class="all-filled">{{ totalCount ? '本页已填齐' : '无待填项' }}</span>
      </div>
    </div>
    <details v-if="pendingTargets.length" class="missing-items" :class="{ optional: !requiredCount }">
      <summary><span class="missing-indicator" aria-hidden="true"></span>{{ requiredCount ? `尚缺 ${requiredCount} 处签名 · 其它内容选填` : '签名已齐全 · 其它内容可选填' }} · 查看位置</summary>
      <div class="missing-item-list" aria-label="签名与选填项">
        <button v-for="target in orderedTargets" :key="`${target.row}:${target.col}`" type="button" class="missing-item" :class="{ optional: !target.required }"
          :title="target.label" @click="emit('locate-missing', target)">
          <b>{{ columnLabel(target.col) }}{{ target.row + 1 }}</b><span>{{ target.label }}</span><small>{{ target.required ? '待签名' : '选填' }}</small>
        </button>
      </div>
    </details>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { MopFillTarget } from '../mopFillProgress';
const props = withDefaults(defineProps<{
  bulkCount: number;
  filledCount: number;
  totalCount: number;
  pendingTargets: MopFillTarget[];
  columnLabel: (col: number) => string;
  showEditingTools?: boolean;
}>(), { showEditingTools: true });
const requiredCount = computed(() => props.pendingTargets.filter(target => target.required).length);
const orderedTargets = computed(() => [...props.pendingTargets].sort((a, b) => Number(Boolean(b.required)) - Number(Boolean(a.required))));

const emit = defineEmits<{
  "mark-all-normal": [];
  'next-missing': [];
  'locate-missing': [target: MopFillTarget];
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
  flex-wrap: wrap;
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
  flex: 0 1 auto;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  flex-wrap: wrap;
}

.fill-count { color: #475569; white-space: nowrap; }
.all-filled { color: #15803d; font-size: 12px; white-space: nowrap; }
.next-missing { border-color: #f3bb83; color: #9a3412; background: #fff7ed; }
.missing-items { min-width: 0; margin-top: 5px; color: #9a3412; font-size: 12px; }
.missing-items summary { display: flex; align-items: center; gap: 7px; width: fit-content; min-height: 32px; cursor: pointer; list-style: none; font-weight: 750; }
.missing-items summary::after { content: '⌄'; margin-left: 3px; }
.missing-items[open] summary::after { content: '⌃'; }
.missing-indicator { width: 8px; height: 8px; flex: 0 0 8px; border-radius: 50%; background: #c2410c; animation: mop-missing-breathe 2s ease-in-out infinite; }
.missing-item-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 240px), 1fr)); gap: 5px; max-height: 180px; overflow: auto; padding: 3px 1px 6px; }
.missing-item { display: flex; align-items: baseline; gap: 7px; min-width: 0; min-height: 34px; padding: 6px 9px; border: 1px solid #f5d2b0; border-radius: 6px; text-align: left; color: #7c2d12; background: #fffaf5; font: inherit; cursor: pointer; }
.missing-item b { flex: 0 0 auto; }
.missing-item span { overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.missing-item small { margin-left: auto; flex: 0 0 auto; font-size: 11px; }
.optional { color: #475569; }
.next-missing.optional, .missing-item.optional { border-color: #cbd5e1; background: #f8fafc; }
.missing-items.optional .missing-indicator { animation: none; background: #94a3b8; }
:is(button, summary):focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
@keyframes mop-missing-breathe { 0%, 100% { box-shadow: 0 0 0 1px #fed7aa; opacity: .65; } 50% { box-shadow: 0 0 0 4px #ffedd5; opacity: 1; } }
@media (prefers-reduced-motion: reduce) { .missing-indicator { animation: none; } }

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
    align-items: center;
    gap: 6px;
  }

  .table-fill-title {
    flex-wrap: wrap;
    gap: 5px;
  }

  .table-fill-title span {
    white-space: normal;
  }

  .table-fill-actions {
    justify-content: flex-start;
  }
}

@media (max-width: 720px) {
  .btn { min-height: 38px; }
  .missing-items summary { min-height: 40px; }
  .missing-item { min-height: 40px; }
}
</style>
