<script setup lang="ts">
import { computed, ref, useId } from 'vue';
import { NModal, NPopover } from 'naive-ui';
import { ChevronRight, MessageCircle, Moon, ShieldCheck, Sun, UsersRound } from 'lucide-vue-next';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';

export type RosterPerson = {name:string;open_id?:string;group?:string;position?:string;system_position?:string;duty_kind?:string};
type BuildingStaff = {scope:string;people:RosterPerson[]};
export type ShiftRoster = {date:string;shift:'白'|'夜';verified:boolean;end?:string;staff:BuildingStaff[]};
export type Handover = {current?:ShiftRoster;incoming?:ShiftRoster};
const props = defineProps<{handover?:Handover; message:string; canRetry:boolean; impersonating?:boolean; expanded?:boolean}>();
defineEmits<{retry:[]}>();
const view=ref<'cards'|'table'>('table');
const showAll=ref(false), heading=useId();
const order=['H','A','B','C','D','E','110'];
const isLongDayIntern=(person:RosterPerson)=>person.position==='运维实习生'&&(person.duty_kind==='长白'||person.group==='长白');
function rosterPosition(person:RosterPerson){
  const position=person.position?.trim()||'';
  return /值班|站长|主管|实习|工程师|经理/.test(position)?position:person.system_position?.trim()||position;
}
const isLeader=(person:RosterPerson)=>rosterPosition(person).includes('值班长');
function dayOrder(person:RosterPerson){
  const position=rosterPosition(person);
  if(isLeader(person))return 0;
  if(/站长|主管/.test(position))return 1;
  if(position.includes('值班员'))return 2;
  return isLongDayIntern(person)||position.includes('实习')?3:4;
}
function staffAt(shift:ShiftRoster|undefined,scope:string){
  const members=shift?.staff?.filter(row=>row.scope===scope).flatMap(row=>row.people)||[];
  return [...new Map(members.map(person=>[person.open_id||person.name,person])).values()]
    .sort((a,b)=>dayOrder(a)-dayOrder(b));
}
const rows=computed(()=>order.flatMap(scope=>{
  const current=staffAt(props.handover?.current,scope);
  const incoming=staffAt(props.handover?.incoming,scope);
  return current.length||incoming.length?[{scope,current,incoming}]:[];
}));
const label=(scope:string)=>scope==='110'?'110站':scope+'楼';
const columns=[{id:'current',label:'当班人员'},{id:'incoming',label:'接班人员'}] as const;
function personRole(person:RosterPerson){
  return isLongDayIntern(person)?'长白实习生':rosterPosition(person).replace(/^运维(?=值班[长员]$)/,'');
}
function chatLink(person:RosterPerson) {
  return !props.impersonating&&/^ou_[A-Za-z0-9]+$/.test(person.open_id||'')
    ?'https://applink.feishu.cn/client/chat/open?'+new URLSearchParams({openId:person.open_id!}):'';
}
</script>

<template>
  <section class="workbench-roster" :aria-labelledby="heading">
    <header class="roster-head">
      <UsersRound :size="17" aria-hidden="true"/><h3 :id="heading">交接班人员</h3>
      <RecordViewSwitch v-if="expanded&&rows.length" v-model="view"/>
      <button v-if="!expanded&&rows.length" type="button" class="roster-all" aria-haspopup="dialog" @click="showAll=true">排班详情 <ChevronRight :size="14" aria-hidden="true"/></button>
    </header>
    <div v-if="handover" class="roster-grid" :data-view="view" :role="view==='table'?'table':undefined">
      <div v-if="view==='table'&&rows.length" class="roster-row roster-columns" role="row">
        <span role="columnheader" aria-label="楼栋"></span>
        <span v-for="column in columns" :key="column.id" :class="{'current-shift':column.id==='current'}" role="columnheader">
          <b>{{ column.label }}</b>
        </span>
      </div>
      <div v-for="row in rows" :key="row.scope" class="roster-row" :role="view==='table'?'row':undefined">
        <b class="building-label" :title="label(row.scope)" :aria-label="label(row.scope)" :role="view==='table'?'rowheader':undefined">{{ label(row.scope) }}</b>
        <div v-for="column in columns" :key="column.id" class="roster-shift" :class="{'current-shift':column.id==='current'}"
          :role="view==='table'?'cell':undefined">
          <div v-if="view==='cards'" class="shift-heading"><b>{{ column.label }}</b></div>
          <div class="shift-line" :data-shift="handover[column.id]?.shift">
            <span v-if="handover[column.id]?.shift" class="shift-badge" :data-shift="handover[column.id]?.shift"
              :aria-label="handover[column.id]?.shift==='白'?'白班':'夜班'">
              <Sun v-if="handover[column.id]?.shift==='白'" :size="12" aria-hidden="true"/><Moon v-else :size="12" aria-hidden="true"/>
              {{ handover[column.id]?.shift }}
            </span>
          <div class="roster-names">
            <NPopover v-for="(person,index) in (expanded?row[column.id]:row[column.id].slice(0,3))" :key="(person.open_id||person.name)+'-'+index"
              trigger="click" placement="top" :show-arrow="false" raw>
              <template #trigger>
                <button type="button" class="person-key" :class="{'is-intern':isLongDayIntern(person),'is-leader':isLeader(person)}"
                  :title="personRole(person)||undefined"
                  :aria-label="['查看'+person.name+'的基本信息',personRole(person)].filter(Boolean).join(' · ')" aria-haspopup="dialog">
                  <ShieldCheck v-if="isLeader(person)" class="leader-mark" :size="12" aria-hidden="true" />
                  <span>{{ person.name }}</span>
                </button>
              </template>
              <section class="roster-person" role="dialog" :aria-label="person.name+'的基本信息'">
                <strong>{{ person.name }}</strong>
                <dl><template v-if="personRole(person)"><dt>排班角色</dt><dd>{{ personRole(person) }}</dd></template>
                  <template v-if="person.group"><dt>班组</dt><dd>{{ person.group }}</dd></template>
                </dl>
                <a v-if="chatLink(person)" :href="chatLink(person)" target="_blank" rel="noopener noreferrer">
                  <MessageCircle :size="16" aria-hidden="true"/>飞书对话</a>
                <small v-else-if="!impersonating">飞书身份待核对</small>
              </section>
            </NPopover>
            <small v-if="!row[column.id].length">{{ handover[column.id]?.verified?'暂无排班':'名单待同步' }}</small>
          </div>
          </div>
        </div>
      </div>
      <p v-if="!rows.length" class="roster-feedback" role="status">{{ message || '暂无已核验人员' }}</p>
    </div>
    <div v-if="!handover || canRetry" class="roster-feedback" role="status">
      <span v-if="!handover">{{ message }}</span>
      <button v-if="canRetry" type="button" @click="$emit('retry')">重试排班</button>
    </div>
    <NModal v-if="!expanded" v-model:show="showAll" preset="card" title="全部排班" class="wb-roster-dialog" style="width:min(680px,calc(100vw - 24px))">
      <WorkbenchRoster :handover="handover" :message="message" :can-retry="canRetry" :impersonating="impersonating" expanded @retry="$emit('retry')"/>
    </NModal>
  </section>
</template>

<style scoped>
.workbench-roster{min-width:0;color:var(--desk-ink)}
:global(.wb-roster-dialog){--desk-ink:var(--v2-ink);--desk-muted:var(--v2-muted);--desk-paper:var(--soft-face);--desk-well:var(--soft-field);--desk-line:var(--soft-edge);--desk-green:var(--v2-palette-accent-text);--desk-green-wash:var(--v2-palette-selection)}
.roster-head{display:flex;align-items:center;flex-wrap:wrap;gap:7px;padding:7px 10px}
.roster-head h3{margin:0 auto 0 0}.roster-head svg{color:var(--desk-green)}
.roster-all{display:inline-flex;align-items:center;gap:4px;min-height:32px;padding:3px 6px;border:1px solid var(--desk-line);border-radius:7px;background:var(--desk-well);color:var(--desk-green);font:inherit;font-size:12px;cursor:pointer}
.roster-grid{display:grid;gap:4px;padding:0 8px 8px;max-inline-size:100%}
.roster-row{display:grid;grid-template-columns:38px repeat(2,minmax(0,1fr));align-items:center;gap:5px;min-width:0}
.roster-columns>span,.roster-shift,.building-label{min-width:0;padding:2px 3px;border-radius:4px}
.roster-columns>span,.shift-heading{font-size:.76rem;display:grid;place-items:center;gap:1px}
.roster-row .current-shift{background:var(--v2-palette-selection);color:var(--v2-blue)}
.roster-shift{display:grid;align-content:center;gap:4px}
.shift-line{min-width:0;display:flex;align-items:center;gap:3px}
.shift-badge{display:inline-flex;align-items:center;gap:2px;flex:none;padding:2px 3px;border-radius:4px;
  background:#e0ebff;color:#1e40af;font-size:11px;white-space:nowrap;font-weight:650}
.shift-badge[data-shift=白]{background:#fff3cd;color:#884b10}
.roster-names{display:flex;align-items:center;justify-content:flex-start;flex-wrap:wrap;gap:1px 2px;min-width:0}
.shift-line[data-shift=白] .roster-names{color:color-mix(in srgb,#b45c15 70%,var(--desk-ink))}
.shift-line[data-shift=夜] .roster-names{color:var(--v2-blue)}
.building-label{display:grid;place-items:center;min-height:22px;background:#087d66;color:white;font-size:var(--cf-font-meta);white-space:nowrap}
.person-key{display:inline-flex;align-items:center;justify-content:center;gap:2px;max-width:100%;min-height:24px;
  border:0;border-radius:4px;padding:1px 2px;background:transparent;color:inherit;font:inherit;font-size:.78rem;cursor:pointer;text-align:center;white-space:nowrap}
.person-key span{min-width:0;overflow:hidden;text-overflow:ellipsis}
.person-key.is-leader{font-weight:700}
.leader-mark{flex:none;color:var(--v2-blue)}
.person-key:is(:hover,:focus-visible){text-decoration:underline;background:var(--desk-paper)}
.person-key.is-intern{color:color-mix(in srgb,#895bb9 64%,var(--desk-ink));background:color-mix(in srgb,currentColor 9%,var(--desk-paper))}
.roster-names>small,.roster-feedback{color:var(--desk-muted);font-size:.72rem}
.roster-grid[data-view=cards]{gap:6px}
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
@media(pointer:coarse){.person-key,.roster-person a,.roster-feedback button,.roster-all{min-height:44px}}
</style>
