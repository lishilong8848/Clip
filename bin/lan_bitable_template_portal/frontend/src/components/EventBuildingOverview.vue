<template>
  <section class="building-panel">
    <div class="surface-head">
      <div>
        <h3>楼栋事件概览</h3>
      </div>
    </div>

    <div v-if="loading" class="event-empty compact">
      <strong>正在读取事件数据</strong>
    </div>
    <div v-else class="building-grid">
      <button type="button"
        v-for="card in cards"
        :key="card.code"
        class="building-card"
        :class="[card.tone, { active: activeCode === card.code, disabled: !card.allowed }]"
        :disabled="!card.allowed"
        :title="card.allowed ? '进入该楼栋事件明细' : '当前账号无该楼栋权限，仅展示态势数据'"
        @click="$emit('select', card.code)"
      >
        <span class="building-card__bar"></span>
        <div class="building-card__head">
          <span class="building-icon">▦</span>
          <strong>{{ card.label }}</strong>
          <em>{{ card.statusLabel }}</em>
        </div>
        <div class="building-card__numbers">
          <span><b>{{ card.total }}</b><small>本月</small></span>
          <span><b>{{ card.processing }}</b><small>处理中</small></span>
          <span><b>{{ card.pending }}</b><small>挂起</small></span>
          <span><b>{{ card.ended }}</b><small>已闭环</small></span>
        </div>
        <div class="building-card__action">{{ card.allowed ? "进入管理 ›" : "仅可查看态势" }}</div>
      </button>
    </div>
  </section>
</template>

<script setup lang="ts">
import type { EventBuildingCard } from "../eventManagementUtils";

defineProps<{
  cards: EventBuildingCard[];
  activeCode: string;
  loading?: boolean;
}>();

defineEmits<{
  select: [code: string];
}>();
</script>

<style scoped>
.building-panel {
  min-width: 0;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.74);
  padding: 14px;
}

.surface-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.surface-head h3 {
  margin: 0;
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
}

.building-grid {
  margin-top: 12px;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.building-card {
  position: relative;
  min-width: 0;
  overflow: hidden;
  display: grid;
  gap: 10px;
  padding: 14px 14px 12px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.96);
  text-align: left;
  cursor: pointer;
  box-shadow: 0 12px 26px rgba(15, 73, 153, 0.08);
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}

.building-card:hover,
.building-card.active {
  transform: translateY(-2px);
  border-color: #9cc7ff;
  box-shadow: 0 18px 36px rgba(15, 86, 228, 0.12);
}

.building-card.disabled {
  cursor: not-allowed;
  opacity: 0.72;
  transform: none;
}

.building-card.disabled:hover {
  border-color: #d8e5f7;
  box-shadow: 0 12px 26px rgba(15, 73, 153, 0.08);
}

.building-card.disabled .building-card__action {
  background: #f1f5f9;
  color: #64748b;
}

.building-card__bar {
  position: absolute;
  inset: 0 0 auto;
  height: 5px;
  background: #1e63ff;
}

.building-card.warning .building-card__bar { background: #f59e0b; }
.building-card.critical .building-card__bar { background: #e11d48; }
.building-card.stable .building-card__bar { background: #10b981; }

.building-card__head {
  min-width: 0;
  display: grid;
  grid-template-columns: 34px minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
}

.building-icon {
  width: 34px;
  height: 34px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: #edf5ff;
  color: #1d4ed8;
  font-size: 16px;
  font-weight: 950;
}

.building-card__head strong {
  min-width: 0;
  overflow: hidden;
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.building-card__head em {
  padding: 6px 10px;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-style: normal;
  font-weight: 950;
  white-space: nowrap;
}

.building-card.warning .building-card__head em { background: #fff7ed; color: #c2410c; }
.building-card.critical .building-card__head em { background: #fff1f2; color: #be123c; }
.building-card.stable .building-card__head em { background: #ecfdf5; color: #047857; }

.building-card__numbers {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 6px;
}

.building-card__numbers span {
  min-width: 0;
  display: grid;
  gap: 2px;
  padding-right: 8px;
  border-right: 1px solid #e5edf8;
}

.building-card__numbers span:last-child {
  border-right: 0;
}

.building-card__numbers b {
  color: #1d4ed8;
  font-size: 15px;
  font-weight: 950;
}

.building-card__numbers small {
  color: #6b7f9d;
  font-size: 11px;
  font-weight: 850;
}

.building-card__action {
  justify-self: end;
  min-height: 28px;
  display: inline-flex;
  align-items: center;
  padding: 0 11px;
  border-radius: 999px;
  background: #edf5ff;
  color: #0e5bd8;
  font-size: 12px;
  font-weight: 950;
}

.event-empty {
  color: #5e728f;
  display: grid;
  place-items: center;
  min-height: 160px;
  border: 1px dashed #cbd9ec;
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.88);
  text-align: center;
}

.event-empty.compact {
  min-height: 88px;
}

.event-empty strong {
  color: #10294f;
  font-size: 15px;
}

@media (max-width: 1180px) {
  .building-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .building-grid {
    grid-template-columns: 1fr;
  }
}
</style>
