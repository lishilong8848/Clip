<script setup lang="ts">
import { ChevronRight } from 'lucide-vue-next';
import type { WorkItem } from './sources';

defineProps<{ entries: {key:string;time:string;label:string;title:string;meta:string;task?:WorkItem}[];view?:'cards'|'table' }>();
defineEmits<{ task:[item:WorkItem] }>();
</script>

<template>
  <ol class="timeline" :data-view="view">
    <li v-for="entry in entries" :key="entry.key">
      <time>{{ entry.time }}</time>
      <div>
        <small>{{ entry.label }}</small>
        <button v-if="entry.task" type="button" class="agenda-task" :title="entry.title" @click="$emit('task',entry.task)">{{ entry.title }} <ChevronRight :size="14" aria-hidden="true" /></button>
        <details v-else-if="entry.meta">
          <summary class="agenda-task" :title="entry.title">{{ entry.title }} <ChevronRight :size="14" aria-hidden="true" /></summary>
          <p>{{ entry.meta }}</p>
        </details>
        <b v-else>{{ entry.title }}</b>
        <p v-if="entry.task&&entry.meta">{{ entry.meta }}</p>
      </div>
    </li>
  </ol>
</template>

<style scoped>
.timeline { list-style:none; min-width:0; max-inline-size:100%; padding:0; margin:8px 0 0 }
.timeline li { display:grid; grid-template-columns:42px minmax(0,1fr); gap:8px; font-size:13px }
.timeline time { padding-top:8px; color:var(--desk-blue); font-size:12px; font-variant-numeric:tabular-nums }
.timeline li>div { position:relative; display:grid; gap:2px; border-left:1px solid var(--desk-line); padding:5px 0 8px 11px; overflow-wrap:anywhere }
.timeline li>div::before { content:""; position:absolute; left:-3px; top:12px; width:5px; height:5px; border-radius:50%; background:var(--desk-blue) }
.timeline small,.timeline p { margin:0; color:var(--desk-muted); font-size:12px }
.agenda-task { display:flex; align-items:center; gap:5px; text-align:left; padding:0; border:0; background:transparent; font:inherit; font-weight:650; color:var(--desk-green); cursor:pointer }
.agenda-task svg { flex:none }
details[open] summary svg{transform:rotate(90deg)}
.timeline[data-view=cards]{display:grid;gap:6px}
.timeline[data-view=cards] li{padding:4px 7px;border:1px solid var(--desk-line);border-radius:8px;background:var(--soft-face)}
.timeline[data-view=cards] li>div{border:0;padding-inline-start:0}
.timeline[data-view=cards] li>div::before{display:none}
.timeline[data-view=table] li{grid-template-columns:36px minmax(0,1fr);gap:6px;border-bottom:1px solid var(--desk-line)}
.timeline[data-view=table] li>div{border:0;padding:5px 0}
.timeline[data-view=table] li>div::before{display:none}
.agenda-task{min-width:0;overflow-wrap:anywhere}
/* The compact rail hides long titles behind an ellipsis with no tooltip; on touch show the words instead. */
@media(pointer:coarse){.timeline[data-view=table] li>div{overflow-wrap:anywhere}}
</style>
