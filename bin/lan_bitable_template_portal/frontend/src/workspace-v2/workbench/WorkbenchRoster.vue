<script setup lang="ts">
import { computed, ref } from 'vue';
import { NPopover } from 'naive-ui';
import { MessageCircle, UsersRound } from 'lucide-vue-next';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';

export type RosterPerson = {name:string;open_id?:string;group?:string;position?:string;duty_kind?:string};
type BuildingStaff = {scope:string;people:RosterPerson[]};
export type ShiftRoster = {date:string;shift:'白'|'夜';verified:boolean;end?:string;staff:BuildingStaff[]};
export type Handover = {current:ShiftRoster;incoming:ShiftRoster};
const props = defineProps<{handover?:Handover; message:string; canRetry:boolean; impersonating?:boolean}>();
defineEmits<{retry:[]}>();
const view=ref<'cards'|'table'>('table');
const order=['H','A','B','C','D','E','110'];
const rows=computed(()=>order.flatMap(scope=>{
  const current=props.handover?.current.staff.filter(row=>row.scope===scope).flatMap(row=>row.people)||[];
  const incoming=props.handover?.incoming.staff.filter(row=>row.scope===scope).flatMap(row=>row.people)||[];
  return current.length||incoming.length?[{scope,current,incoming}]:[];
}));
const label=(scope:string)=>scope==='110'?'110站':scope+'楼';
const shiftText=(shift?:ShiftRoster)=>shift?shift.date.slice(5).replace('-','/')+' '+shift.shift+'班':'';
const columns=[{id:'current',label:'当班人员'},{id:'incoming',label:'接班人员'}] as const;
const isLongDayIntern=(person:RosterPerson)=>person.position==='运维实习生'&&(person.duty_kind==='长白'||person.group==='长白');
function chatLink(person:RosterPerson) {
  return !props.impersonating&&/^ou_[A-Za-z0-9]+$/.test(person.open_id||'')
    ?'https://applink.feishu.cn/client/chat/open?'+new URLSearchParams({openId:person.open_id!}):'';
}
</script>

<template>
  <section class="workbench-roster" aria-labelledby="roster-heading">
    <header class="roster-head">
      <UsersRound :size="17" aria-hidden="true"/><h3 id="roster-heading">交接班人员</h3>
      <RecordViewSwitch v-if="rows.length" v-model="view"/>
    </header>
    <div v-if="handover" class="roster-grid" :data-view="view" :role="view==='table'?'table':undefined">
      <div v-if="view==='cards'&&rows.length" class="roster-dates">
        <small>当班 {{ shiftText(handover.current) }}</small><small>接班 {{ shiftText(handover.incoming) }}</small>
      </div>
      <div v-if="view==='table'&&rows.length" class="roster-row roster-columns" role="row">
        <span role="columnheader">楼栋</span>
        <span v-for="column in columns" :key="column.id" :class="{'current-shift':column.id==='current'}" role="columnheader">
          <b>{{ column.label }}</b><small>{{ shiftText(handover[column.id]) }}</small>
        </span>
      </div>
      <div v-for="row in rows" :key="row.scope" class="roster-row" :role="view==='table'?'row':undefined">
        <b class="building-label" :role="view==='table'?'rowheader':undefined">{{ label(row.scope) }}</b>
        <div v-for="column in columns" :key="column.id" class="roster-shift" :class="{'current-shift':column.id==='current'}"
          :role="view==='table'?'cell':undefined">
          <div v-if="view==='cards'" class="shift-heading"><b>{{ column.label }}</b></div>
          <div class="roster-names">
            <NPopover v-for="(person,index) in row[column.id]" :key="(person.open_id||person.name)+'-'+index"
              trigger="click" placement="top" :show-arrow="false" raw>
              <template #trigger>
                <button type="button" class="person-key" :class="{'is-intern':isLongDayIntern(person)}"
                  :aria-label="'查看'+person.name+'的基本信息'+(isLongDayIntern(person)?'（长白实习生）':'')" aria-haspopup="dialog">
                  <span>{{ person.name }}</span><small v-if="isLongDayIntern(person)" aria-hidden="true">长白实习</small>
                </button>
              </template>
              <section class="roster-person" role="dialog" :aria-label="person.name+'的基本信息'">
                <strong>{{ person.name }}</strong>
                <dl><dt>楼栋</dt><dd>{{ label(row.scope) }}</dd><dt>班次</dt><dd>{{ isLongDayIntern(person)?'长白':shiftText(handover[column.id]) }}</dd>
                  <template v-if="person.group"><dt>班组</dt><dd>{{ person.group }}</dd></template>
                  <template v-if="person.position"><dt>岗位</dt><dd>{{ person.position }}</dd></template>
                </dl>
                <a v-if="chatLink(person)" :href="chatLink(person)" target="_blank" rel="noopener noreferrer">
                  <MessageCircle :size="16" aria-hidden="true"/>飞书对话</a>
                <small v-else-if="!impersonating">飞书身份待核对</small>
              </section>
            </NPopover>
            <small v-if="!row[column.id].length">{{ handover[column.id].verified?'暂无排班':'名单待同步' }}</small>
          </div>
        </div>
      </div>
      <p v-if="!rows.length" class="roster-feedback" role="status">{{ message || '暂无已核验人员' }}</p>
    </div>
    <div v-if="!handover || canRetry" class="roster-feedback" role="status">
      <span v-if="!handover">{{ message }}</span>
      <button v-if="canRetry" type="button" @click="$emit('retry')">重试排班</button>
    </div>
  </section>
</template>

<style scoped>
.workbench-roster{min-width:0;color:var(--desk-ink)}
.roster-head{display:flex;align-items:center;flex-wrap:wrap;gap:7px;padding:7px 10px}
.roster-head h3{margin:0 auto 0 0}.roster-head svg{color:var(--desk-green)}
.roster-grid{display:grid;gap:3px;padding:0 8px 8px;max-inline-size:100%}
.roster-row{display:grid;grid-template-columns:38px repeat(2,minmax(0,1fr));align-items:stretch;gap:4px;min-width:0}
.roster-columns>span,.roster-shift,.building-label{min-width:0;padding:4px 5px;border-radius:6px;background:var(--desk-well)}
.roster-columns>span,.shift-heading{font-size:.76rem;display:grid;align-content:start;gap:1px}
.roster-row .current-shift{background:var(--desk-green-wash);color:var(--desk-green);border-inline-start:2px solid var(--desk-green)}
.roster-shift{display:grid;align-content:center;gap:4px}
.roster-names{display:flex;align-items:center;flex-wrap:wrap;gap:2px 4px;min-width:0}
.building-label{display:grid;place-items:center;font-size:.77rem}
.person-key{display:inline-flex;align-items:center;flex-wrap:wrap;gap:2px 4px;max-width:100%;min-width:0;min-height:27px;
  border:0;border-radius:5px;padding:2px 4px;background:transparent;color:inherit;font:inherit;font-size:.82rem;font-weight:650;cursor:pointer;overflow-wrap:anywhere}
.person-key:is(:hover,:focus-visible){text-decoration:underline;background:var(--desk-paper)}
.person-key.is-intern{color:var(--v2-amber,#99441d);background:color-mix(in srgb,currentColor 10%,var(--desk-paper))}
.person-key small{font-size:.68rem;white-space:nowrap}
.roster-names>small,.roster-feedback,.roster-columns small,.roster-dates small{color:var(--desk-muted);font-size:.72rem}
.roster-grid[data-view=cards]{gap:6px}
.roster-dates{display:flex;flex-wrap:wrap;gap:2px 10px;padding:0 4px}
.roster-grid[data-view=cards] .roster-row{grid-template-columns:repeat(2,minmax(0,1fr));padding:6px;border:1px solid var(--desk-line);border-radius:8px}
.roster-grid[data-view=cards] .building-label{grid-column:1/-1;justify-self:start;padding:2px 7px}
.roster-grid[data-view=cards] .roster-shift{align-content:start}
.roster-feedback{display:flex;align-items:center;justify-content:space-between;gap:5px;padding:5px 9px}
.roster-feedback button{border:1px solid var(--desk-line);background:var(--desk-well);border-radius:5px;color:var(--desk-green);min-height:34px}
.roster-person{width:min(224px,calc(100vw - 24px));padding:10px 12px;border-radius:10px;border:1px solid var(--v2-border);
  background:var(--soft-face,#fff);color:var(--v2-ink);box-shadow:var(--soft-key);font-size:.86rem;overflow-wrap:anywhere}
.roster-person dl{display:grid;grid-template-columns:auto minmax(0,1fr);gap:4px 9px;margin:8px 0}
.roster-person dt,.roster-person small{color:var(--v2-muted)}
.roster-person dd{margin:0}.roster-person a{display:inline-flex;align-items:center;gap:5px;min-height:36px;color:var(--v2-palette-accent-text)}
@media(pointer:coarse){.person-key,.roster-person a,.roster-feedback button{min-height:44px}}
</style>
