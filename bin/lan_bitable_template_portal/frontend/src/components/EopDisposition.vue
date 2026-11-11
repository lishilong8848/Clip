<template>
  <div v-if="groups.length" class="disposition-groups">
    <section v-for="(group, index) in groups" :key="index" class="disposition-group" :aria-label="group.heading || `处置分组 ${index + 1}`">
      <h3 v-if="group.heading">{{ group.heading }}</h3>
      <h3 v-else-if="groups.length > 1">第 {{ index + 1 }} 组</h3>
      <ul>
        <li v-for="(row, rowIndex) in group.rows" :key="rowIndex" :style="{ '--indent': row.depth }">
          <span v-if="row.marker" class="disposition-marker">{{ row.marker }}</span>
          <span>{{ row.text }}</span>
        </li>
      </ul>
    </section>
  </div>
  <p v-else class="disposition-empty">未填写处置正文</p>
</template>

<script setup lang="ts">
import type { EopDispositionGroup } from './eopDisposition';

defineProps<{groups: EopDispositionGroup[]}>();
</script>

<style scoped>
.disposition-groups { display:grid; gap:8px; }
.disposition-group { border-inline-start:2px solid var(--ep-line); padding:6px 10px; background:var(--ep-soft); }
.disposition-group h3 { margin:0 0 4px; font-size:14px; overflow-wrap:anywhere; }
.disposition-group ul { list-style:none; margin:0; padding:0; }
.disposition-group li { display:flex; gap:6px; padding:3px 0; padding-inline-start:calc(var(--indent) * 12px); line-height:1.55; overflow-wrap:anywhere; }
.disposition-group li span { min-width:0; }
.disposition-marker { flex:none; font-weight:600; color:var(--ep-accent); }
.disposition-empty { margin:4px 0; color:var(--ep-muted); }
</style>
