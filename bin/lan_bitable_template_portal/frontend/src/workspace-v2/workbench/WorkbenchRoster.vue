<script setup lang="ts">
const props = defineProps<{
  rows: {
    scope: string
    day: Set<string>
    night: Set<string>
    other: Map<string, Set<string>>
  }[]
  message: string
  canRetry: boolean
}>()

const emit = defineEmits<{ retry: [] }>()

/** 楼栋标签：A → A楼，110 → 110站，已带单位或空值原样保留 */
function scopeLabel(scope: string): string {
  const s = (scope ?? '').trim()
  if (!s) return '未提供'
  if (/[楼栋站区室]$/.test(s)) return s
  if (/^\d+$/.test(s)) return `${s}站`
  if (/^[A-Za-z]$/.test(s)) return `${s}楼`
  return s
}
</script>

<template>
  <section class="workbench-roster" aria-label="今日排班">
    <div v-if="props.rows.length > 0" class="table-wrap">
      <table class="roster-table">
        <caption class="sr-only">今日排班：楼栋、白班与夜班人员</caption>
        <thead>
          <tr>
            <th scope="col" class="col-scope">楼栋</th>
            <th scope="col">白班</th>
            <th scope="col">夜班</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="row in props.rows" :key="row.scope">
            <tr class="main-row">
              <th scope="row" class="scope-cell">{{ scopeLabel(row.scope) }}</th>
              <td class="day-cell">
                <span v-for="name in row.day" :key="name" class="name-chip day-chip">{{
                  name
                }}</span>
                <span v-if="row.day.size === 0" class="empty" aria-label="未提供">—</span>
              </td>
              <td class="night-cell">
                <span v-for="name in row.night" :key="name" class="name-chip night-chip">{{
                  name
                }}</span>
                <span v-if="row.night.size === 0" class="empty" aria-label="未提供">—</span>
              </td>
            </tr>
            <tr v-if="row.other.size > 0" class="other-row">
              <th scope="row" class="other-label">其它班次</th>
              <td colspan="2" class="other-cell">
                <ul class="other-list">
                  <li v-for="[shift, people] in row.other" :key="shift" class="other-item">
                    <span class="other-shift">{{ shift || '未命名班次' }}</span>
                    <span v-for="name in people" :key="name" class="name-chip other-chip">{{
                      name
                    }}</span>
                    <span v-if="people.size === 0" class="empty">未提供</span>
                  </li>
                </ul>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
    </div>

    <div v-else class="empty-state" role="status">
      <p class="empty-message">{{ props.message || '暂无排班数据' }}</p>
      <button v-if="props.canRetry" type="button" class="retry-btn" @click="emit('retry')">
        重试
      </button>
    </div>
  </section>
</template>

<style scoped>
.workbench-roster { min-width:0; padding:6px 9px; color:var(--desk-ink) }
.empty-state { display:flex; justify-content:space-between; gap:8px; align-items:center }
.empty-message { margin:0 }
.roster-table { width:100%; table-layout:fixed; border-collapse:collapse; font-size:12px }
.roster-table th,.roster-table td { text-align:left; vertical-align:top; padding:2px 3px; line-height:1.45; overflow-wrap:anywhere; border-bottom:1px solid var(--desk-line) }
.roster-table thead { color:var(--desk-muted) }
.col-scope { width:20% }
.roster-table tr:last-child>* { border-bottom:0 }
.name-chip { display:inline-block; max-width:100%; margin:1px 3px 1px 0; padding:0 4px; border-radius:5px }
.day-chip { color:var(--desk-blue); background:var(--desk-blue-wash) }
.night-chip,.other-chip { color:var(--desk-green); background:var(--desk-green-wash) }
.other-list { list-style:none; padding:0; margin:0 }
.other-shift { margin-right:5px }
.empty,.empty-state,.other-label { color:var(--desk-muted); font-size:12px }
.retry-btn { min-height:32px; border:1px solid var(--desk-line); border-radius:8px; padding:3px 9px; background:var(--desk-well); color:var(--desk-blue); cursor:pointer }
.sr-only { position:absolute; width:1px; height:1px; overflow:hidden; clip-path:inset(50%); white-space:nowrap }
</style>
