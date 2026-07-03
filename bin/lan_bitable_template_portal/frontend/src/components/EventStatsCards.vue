<template>
  <div class="event-stats">
    <article v-for="card in cards" :key="card.key" :class="card.tone">
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
    </article>
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
  }>;
}>();
</script>

<style scoped>
.event-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
}

.event-stats article {
  position: relative;
  min-height: 96px;
  display: grid;
  grid-template-columns: 60px 1fr auto;
  align-items: center;
  gap: 16px;
  overflow: hidden;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 16px 18px;
  background: #ffffff;
  box-shadow: 0 16px 38px rgba(0, 47, 135, 0.08);
}

.stat-icon {
  width: 56px;
  height: 56px;
  display: grid;
  place-items: center;
  border-radius: 14px;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  font-size: 24px;
  font-weight: 950;
}

.event-stats article.amber .stat-icon {
  background: linear-gradient(135deg, #f59e0b, #fb923c);
}

.event-stats article.rose .stat-icon {
  background: linear-gradient(135deg, #fb7185, #e11d48);
}

.event-stats article.emerald .stat-icon {
  background: linear-gradient(135deg, #22c55e, #059669);
}

.stat-main {
  min-width: 0;
  display: grid;
  grid-template-columns: auto auto 1fr;
  align-items: baseline;
  column-gap: 8px;
}

.stat-main small {
  grid-column: 1 / -1;
  color: #475569;
  font-size: 13px;
  font-weight: 850;
}

.stat-main strong {
  color: #071a39;
  font-size: 30px;
  font-weight: 950;
}

.stat-main em {
  color: #64748b;
  font-style: normal;
  font-size: 13px;
}

.stat-badge {
  align-self: start;
  border-radius: 999px;
  padding: 6px 12px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 850;
}

.event-stats article.amber .stat-badge {
  background: #fff7ed;
  color: #c2410c;
}

.event-stats article.rose .stat-badge {
  background: #fff1f2;
  color: #be123c;
}

.event-stats article.emerald .stat-badge {
  background: #ecfdf5;
  color: #047857;
}

.stat-bars {
  position: absolute;
  right: 18px;
  bottom: 12px;
  display: flex;
  align-items: end;
  gap: 5px;
}

.stat-bars i {
  width: 7px;
  border-radius: 999px;
  background: #1e63ff;
}

.stat-bars i:nth-child(1) { height: 7px; opacity: 0.55; }
.stat-bars i:nth-child(2) { height: 13px; opacity: 0.65; }
.stat-bars i:nth-child(3) { height: 18px; opacity: 0.75; }
.stat-bars i:nth-child(4) { height: 23px; opacity: 0.84; }
.stat-bars i:nth-child(5) { height: 29px; opacity: 0.92; }
.stat-bars i:nth-child(6) { height: 35px; }

.event-stats article.amber .stat-bars i {
  background: #f59e0b;
}

.event-stats article.rose .stat-bars i {
  background: #e11d48;
}

.event-stats article.emerald .stat-bars i {
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
