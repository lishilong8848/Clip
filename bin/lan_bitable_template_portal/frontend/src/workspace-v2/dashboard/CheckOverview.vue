<template>
  <div class="v2-check-overview" aria-label="各楼栋班前检查完成情况">
    <article v-for="building in buildings" :key="building.scope" class="v2-check-building" :class="{'v2-clickable-card':!disabled}" @click="!disabled && activateCard($event) && $emit('select',building.scope)">
      <header><NButton text :disabled="disabled" :aria-label="'查看'+scopeLabel(building.scope)+'检查'" @click="$emit('select',building.scope)">{{scopeLabel(building.scope)}}<ChevronRight :size="15" /></NButton><small>{{building.rows.filter(row=>row.status==='已完成').length}} / {{building.rows.length}} 班完成</small></header>
      <section v-for="row in building.rows" :key="row.id" class="v2-check-building-shift">
        <div><strong>{{row.shift}}班</strong><NTag size="small" :bordered="false" :type="row.status==='已完成'?'success':['待完成','逾期待补做'].includes(row.status)?'warning':'default'">{{row.status}}</NTag><span v-if="row.total">{{row.completed}} / {{row.total}} 项</span></div>
        <small>{{row.day}}<template v-if="row.abnormal"> · {{row.abnormal}} 项异常</template></small>
        <p v-if="row.submitted_by">{{row.submitted_by}} · {{dateLabel(row.submitted_at)}}</p>
        <NButton v-if="row.handover_photo" text size="small" tag="a" :href="row.handover_photo.url" target="_blank" rel="noopener noreferrer"><Image :size="14" />本班交接照片</NButton><small v-else>本班交接照片未上传</small>
      </section>
    </article>
    <NEmpty v-if="!buildings.length" description="暂无可查看的楼栋完成情况" size="small" />
  </div>
</template>
<script setup lang="ts">
import { activateCard } from '../cardActivation';
import {computed} from 'vue';
import {NButton,NEmpty,NTag} from 'naive-ui';
import {ChevronRight,Image} from 'lucide-vue-next';
import type {Dict} from '../client';
const props=defineProps<{rows:Dict[];disabled:boolean}>();
defineEmits<{select:[scope:string]}>();
const buildings=computed(()=>{const result=new Map<string,{scope:string;rows:Dict[]}>();for(const row of props.rows){if(!result.has(row.scope))result.set(row.scope,{scope:row.scope,rows:[]});result.get(row.scope)!.rows.push(row);}return [...result.values()];});
function scopeLabel(scope:string){return scope==='110'?'110站':scope+'楼';}
function dateLabel(value:string){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});}
</script>
<style>
.v2-check-overview{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,250px),1fr));gap:12px;align-items:start}
.v2-check-building{border:1px solid var(--v2-border,#d8e3f1);border-radius:6px;min-width:0;background:var(--cf-surface,#fff);overflow:hidden}
.v2-check-building>header{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:9px 12px;border-bottom:1px solid var(--v2-border,#d8e3f1)}
.v2-check-building>header .n-button__content{gap:5px;font-weight:600}.v2-check-building small{font-size:12px;color:var(--v2-muted,#647487);line-height:1.6}
.v2-check-building-shift{display:grid;gap:5px;padding:10px 12px}.v2-check-building-shift+.v2-check-building-shift{border-top:1px solid var(--v2-border,#d8e3f1)}
.v2-check-building-shift>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.v2-check-building-shift strong{font-size:13px}.v2-check-building-shift>div>span:last-child{margin-left:auto;font-size:12px;font-variant-numeric:tabular-nums}
.v2-check-building-shift p{margin:0;line-height:1.6;font-size:12px;overflow-wrap:anywhere}.v2-check-building-shift>.n-button{justify-self:start}.v2-check-building-shift .n-button__content{gap:5px}
</style>
