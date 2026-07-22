<template>
  <div class="event-stats">
    <button
      v-for="card in cards"
      :key="card.key"
      type="button"
      class="event-stat-card"
      :class="[card.tone, { clickable: card.clickable }]"
      :disabled="!card.clickable"
      :title="card.clickable ? `查看${card.label}记录` : card.label"
      @click="card.clickable && $emit('select', card.key)"
    >
      <span class="stat-icon">{{ card.icon }}</span>
      <div class="stat-main">
        <small>{{ card.label }}</small>
        <strong>{{ card.value }}</strong>
        <em>{{ card.unit }}</em>
      </div>
      <span class="stat-badge">{{ card.badge }}</span>
      <span class="stat-bars" aria-hidden="true">
        <i v-for="index in 6" :key="index"></i>
      </span>
    </button>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  cards: Array<{
    key: string;
    icon: string;
    label: string;
    value: string | number;
    unit: string;
    badge: string;
    tone: string;
    clickable?: boolean;
  }>;
}>();

defineEmits<{
  select: [key: string];
}>();
</script>

<style scoped>
.event-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.event-stat-card {
  position: relative;
  min-height: 78px;
  display: grid;
  grid-template-columns: 48px 1fr auto;
  align-items: center;
  gap: 11px;
  overflow: hidden;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 12px 14px;
  background: #ffffff;
  color: inherit;
  font: inherit;
  text-align: left;
  box-shadow: 0 16px 38px rgba(0, 47, 135, 0.08);
}

.event-stat-card:disabled {
  opacity: 1;
  cursor: default;
}

.event-stat-card.clickable {
  cursor: pointer;
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}

.event-stat-card.clickable:hover,
.event-stat-card.clickable:focus-visible {
  transform: translateY(-2px);
  border-color: #8ab9ff;
  box-shadow: 0 20px 42px rgba(0, 86, 228, 0.14);
  outline: none;
}

.stat-icon {
  width: 44px;
  height: 44px;
  display: grid;
  place-items: center;
  border-radius: 13px;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  font-size: 20px;
  font-weight: 950;
}

.event-stat-card.amber .stat-icon {
  background: linear-gradient(135deg, #f59e0b, #fb923c);
}

.event-stat-card.rose .stat-icon {
  background: linear-gradient(135deg, #fb7185, #e11d48);
}

.event-stat-card.emerald .stat-icon {
  background: linear-gradient(135deg, #22c55e, #059669);
}

.stat-main {
  min-width: 0;
  display: grid;
  grid-template-columns: auto auto 1fr;
  align-items: baseline;
  column-gap: 6px;
}

.stat-main small {
  grid-column: 1 / -1;
  color: #475569;
  font-size: 12px;
  font-weight: 850;
}

.stat-main strong {
  color: #071a39;
  font-size: 25px;
  font-weight: 950;
}

.stat-main em {
  color: #64748b;
  font-style: normal;
  font-size: 12px;
}

.stat-badge {
  align-self: start;
  border-radius: 999px;
  padding: 5px 9px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 11px;
  font-weight: 850;
}

.event-stat-card.amber .stat-badge {
  background: #fff7ed;
  color: #c2410c;
}

.event-stat-card.rose .stat-badge {
  background: #fff1f2;
  color: #be123c;
}

.event-stat-card.emerald .stat-badge {
  background: #ecfdf5;
  color: #047857;
}

.stat-bars {
  position: absolute;
  right: 14px;
  bottom: 10px;
  display: flex;
  align-items: end;
  gap: 4px;
}

.stat-bars i {
  width: 6px;
  border-radius: 999px;
  background: #1e63ff;
}

.stat-bars i:nth-child(1) { height: 6px; opacity: 0.55; }
.stat-bars i:nth-child(2) { height: 10px; opacity: 0.65; }
.stat-bars i:nth-child(3) { height: 14px; opacity: 0.75; }
.stat-bars i:nth-child(4) { height: 18px; opacity: 0.84; }
.stat-bars i:nth-child(5) { height: 22px; opacity: 0.92; }
.stat-bars i:nth-child(6) { height: 27px; }

.event-stat-card.amber .stat-bars i {
  background: #f59e0b;
}

.event-stat-card.rose .stat-bars i {
  background: #e11d48;
}

.event-stat-card.emerald .stat-bars i {
  background: #059669;
}

@media (max-width: 1320px) {
  .event-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .event-stats {
    grid-template-columns: 1fr;
  }
}
</style>
