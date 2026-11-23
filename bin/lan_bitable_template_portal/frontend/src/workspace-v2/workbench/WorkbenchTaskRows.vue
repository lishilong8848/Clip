<script setup lang="ts">
import { ChevronRight } from 'lucide-vue-next';
import type { WorkItem } from './sources';

defineProps<{ groups:{building:string;rows:WorkItem[]}[];view:'cards'|'table';label?: (item:WorkItem)=>string;
  action:(item:WorkItem)=>string; when:(item:WorkItem)=>string; seen:(item:WorkItem)=>boolean;
  late:(item:WorkItem)=>boolean; readOnly:(item:WorkItem)=>boolean; canDelete:(item:WorkItem)=>boolean }>();
defineEmits<{open:[item:WorkItem]}>();
</script>

<template>
  <div class="queue-list" :data-view="view" role="region" aria-label="待办清单">
    <section v-for="group in groups" :key="group.building" class="queue-building" :aria-label="group.building+'待办'">
      <h4>{{ group.building }} <small>{{ group.rows.length }}</small></h4>
      <ul>
        <li v-for="item in group.rows" :key="item.key" class="wb-task-row task-line" :data-source="item.kind" :class="{seen:seen(item),late:late(item)}">
          <button type="button" class="record-key" :class="{'wb-read-only':readOnly(item)}" :aria-label="action(item)+' '+item.title" @click="$emit('open',item)">
            <span class="read-pin"><span class="wb-sr-only">{{ seen(item)?'已读':'未读' }}</span></span>
            <span class="task-copy"><span class="task-title"><strong>{{ item.title }}</strong></span>
              <span class="task-meta"><span v-if="label">{{ label(item) }}</span><span v-if="item.status">{{ item.status }}</span>
                <span v-if="when(item)" class="due">{{ when(item) }}{{ late(item)?' · 逾期':'' }}</span></span>
            </span>
            <span class="item-action">{{ action(item) }}<ChevronRight :size="13" aria-hidden="true"/></span>
          </button>
          <button v-if="canDelete(item)" type="button" class="delete-key" aria-label="删除单据" @click="$emit('open',item)">删除</button>
        </li>
      </ul>
    </section>
  </div>
</template>

<style scoped>
.queue-building h4{display:flex;gap:6px;align-items:center;margin:0;padding:6px 8px;font-size:13px;color:var(--v2-palette-accent-text)}
.queue-building h4 small{color:var(--v2-muted);font-weight:normal}
.queue-building ul{list-style:none;padding:0;margin:0;min-width:0}
.task-line{display:flex;align-items:center;min-width:0;border-bottom:1px solid var(--desk-line)}
.record-key{flex:1;min-width:0;display:grid;grid-template-columns:6px minmax(0,1fr) auto;align-items:center;gap:7px;
  background:transparent;color:inherit;text-align:left;border:0;border-radius:7px;padding:7px;min-height:40px;font:inherit;cursor:pointer}
.record-key:is(:hover,:focus-visible){background:var(--desk-well)}
.task-copy{display:grid;gap:2px;min-width:0}.task-title strong{font-size:13px;line-height:1.35;overflow-wrap:anywhere}
.item-action{display:flex;gap:2px;align-items:center;font-size:12px;color:var(--desk-green);white-space:nowrap}
.wb-read-only .item-action{color:var(--desk-blue)}
.read-pin{width:6px;height:6px;background:var(--tile-accent,var(--desk-green));border:1px solid currentColor;border-radius:50%;box-sizing:border-box}
.seen .read-pin{background:transparent;border-color:var(--desk-muted)}
.task-meta{display:flex;flex-wrap:wrap;gap:2px 7px;color:var(--desk-muted);font-size:11px;line-height:1.35}
.late .due,.delete-key{color:var(--v2-amber,#99441d)}
.delete-key{min-height:34px;border:0;background:transparent;font-size:12px;cursor:pointer}
.queue-list[data-view=cards] .queue-building ul{display:grid;gap:5px;padding:0 5px 5px}
.queue-list[data-view=cards] .task-line{border:1px solid var(--v2-border);border-radius:8px;background:var(--soft-face)}
@media(pointer:coarse){.record-key,.delete-key{min-height:44px}}
</style>
