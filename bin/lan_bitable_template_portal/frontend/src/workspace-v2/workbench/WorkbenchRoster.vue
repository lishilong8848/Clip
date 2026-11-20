<script setup lang="ts">
import { Moon, Sun, UsersRound } from 'lucide-vue-next';

defineProps<{
  rows: {scope:string;day:Set<string>;night:Set<string>;other:Map<string,Set<string>>}[];
  message: string;
  canRetry: boolean;
}>();
defineEmits<{ retry: [] }>();
</script>

<template>
  <section class="workbench-roster" aria-labelledby="roster-heading">
    <header class="roster-head">
      <span class="roster-icon"><UsersRound :size="17" aria-hidden="true" /></span>
      <h3 id="roster-heading">今日上班人员</h3>
    </header>
    <table v-if="rows.length" class="roster-table">
      <thead>
        <tr><th scope="col">楼栋</th>
          <th scope="col" class="day-header"><span class="shift-label"><Sun :size="14" aria-hidden="true" />白班</span></th>
          <th scope="col" class="night-header"><span class="shift-label"><Moon :size="13" aria-hidden="true" />夜班</span></th>
        </tr>
      </thead>
      <tbody>
        <template v-for="row in rows" :key="row.scope">
          <tr class="building-row">
            <th scope="row"><span class="building-key">{{ row.scope==='110'?'110站':row.scope+'楼' }}</span></th>
            <td class="day-staff"><ul v-if="row.day.size" class="names"><li v-for="name in row.day" :key="name">{{ name }}</li></ul>
              <span v-else class="no-staff">暂无</span></td>
            <td class="night-staff"><ul v-if="row.night.size" class="names"><li v-for="name in row.night" :key="name">{{ name }}</li></ul>
              <span v-else class="no-staff">暂无</span></td>
          </tr>
          <tr v-if="row.other.size" class="extra-shifts">
            <td></td><td colspan="2">
              <dl><div v-for="[shift, people] in row.other" :key="shift">
                <dt>{{ shift }}</dt><dd>{{ [...people].join('、') }}</dd>
              </div></dl>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
    <div v-else class="empty-state" role="status">
      <p>{{ message || '暂无排班' }}</p>
      <button v-if="canRetry" type="button" @click="$emit('retry')">重试排班</button>
    </div>
  </section>
</template>

<style scoped>
.workbench-roster { min-width:0;overflow:hidden;color:var(--desk-ink) }
.roster-head { display:flex;align-items:center;gap:8px;padding:9px 12px 6px }
.roster-head h3 { font-size:14px;font-weight:730 }
.roster-icon { display:grid;place-items:center;width:29px;height:29px;border-radius:8px;background:var(--desk-green-wash);color:var(--desk-green) }
.roster-table { width:calc(100% - 18px);margin:0 9px 8px;border-collapse:separate;border-spacing:3px 3px;
  table-layout:fixed;font-size:13px;line-height:1.4 }
.roster-table thead th:first-child { width:48px;color:var(--desk-muted);text-align:center;font-size:12px }
.roster-table :is(th,td) { padding:4px 6px;border-radius:5px;text-align:left;vertical-align:middle;overflow-wrap:anywhere }
.building-row th { background:var(--desk-well);text-align:center }
.day-staff { background:color-mix(in srgb,var(--v2-amber,#a76712) 9%,var(--desk-paper)) }
.night-staff { background:color-mix(in srgb,var(--desk-blue) 11%,var(--desk-paper)) }
.day-header { background:color-mix(in srgb,var(--v2-amber,#a76712) 16%,var(--desk-paper)) }
.night-header { background:color-mix(in srgb,var(--desk-blue) 19%,var(--desk-paper)) }
.shift-label,.names,.extra-shifts dl>div { display:flex;align-items:center;flex-wrap:wrap;gap:3px 8px }
.shift-label { font-weight:700 }
.day-header svg { color:var(--v2-amber,#a76712) }
.night-header svg { color:var(--desk-blue) }
.building-key { display:inline-block;white-space:nowrap;min-width:34px;text-align:center;
  border-radius:6px;padding:1px 3px;background:var(--desk-green-wash);color:var(--desk-green);font-weight:680 }
.names { list-style:none;padding:0;margin:0;font-weight:600 }
.names li { min-width:0;max-width:100% }
.extra-shifts td { padding-top:0 }
.extra-shifts :is(dl,dd) { margin:0 }
.extra-shifts dt,.no-staff { color:var(--desk-muted);font-size:12px }
.empty-state { display:flex;justify-content:space-between;align-items:center;gap:8px;padding:9px 12px;
  color:var(--desk-muted);font-size:12px }
.empty-state button { min-height:36px;border:1px solid var(--desk-line);border-radius:7px;
  padding:3px 8px;background:var(--desk-well);color:var(--desk-green);white-space:nowrap }
@media (pointer:coarse) { .empty-state button { min-height:44px } }
</style>
