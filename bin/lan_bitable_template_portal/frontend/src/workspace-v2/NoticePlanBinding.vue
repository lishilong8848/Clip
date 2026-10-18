<template>
  <section class="v2-notice-field-section v2-plan-binding" data-notice-section="binding">
    <div class="v2-notice-tools"><h2>计划事项</h2><NTag v-if="!undecided" size="small" :type="form.values.source_record_id?'info':'default'" :bordered="false">{{form.values.source_record_id?'已关联计划':form.values.manual_binding_choice==='unbound'?'已选择独立通告':'待确认所选计划'}}</NTag><NButton :disabled="locked" :aria-expanded="expanded" :aria-describedby="undecided?'v2-plan-choice-hint':undefined" :class="{'needs-choice':undecided}" data-v2-navigation @click="$emit('toggle')">{{expanded?'收起':form.values.source_record_id?'更换关联':'选择办理方式'}}</NButton><span v-if="undecided" id="v2-plan-choice-hint" class="v2-plan-choice-hint" role="alert"><strong>独立通告 · 未关联计划</strong><span>请选择“关联计划”或“独立通告”，确认本次办理方式。</span></span></div>
    <div v-if="expanded" class="v2-plan-options">
      <NRadioGroup v-if="!form.values.source_record_id" :value="form.values.manual_binding_choice" :disabled="locked" aria-label="计划关联方式" @update:value="value=>$emit('mode',String(value))"><NRadio value="bind">关联计划</NRadio><NRadio value="unbound">独立通告</NRadio></NRadioGroup>
      <div v-if="form.values.source_record_id||form.values.manual_binding_choice==='bind'" class="v2-plan-picker" :class="{'has-recommendation':!form.values.source_record_id}">
        <NSelect :value="choice" filterable clearable remote :loading="loading" :options="options" aria-label="源表计划事项" placeholder="输入计划名称搜索并选择" :disabled="locked" @update:value="value=>$emit('update:choice',value)" @search="$emit('search',$event)" />
        <NButton :disabled="locked||loading||!choice||choice===form.values.source_record_id" @click="$emit('confirm')">确认关联</NButton>
        <div v-if="!form.values.source_record_id" class="v2-plan-recommendation" role="status"><span><small>推荐选择</small><strong>{{loading?'正在核对候选':recommendation?.label||'暂无唯一的高匹配计划，请搜索选择'}}</strong></span><NButton v-if="recommendation" size="small" secondary :disabled="locked||loading||choice===recommendation.value" aria-label="应用推荐计划" data-v2-navigation @click="$emit('update:choice',recommendation.value)">{{choice===recommendation.value?'已选择':'应用'}}</NButton></div>
      </div>
    </div>
  </section>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {NButton,NRadio,NRadioGroup,NSelect,NTag} from 'naive-ui';
import type {NoticeForm} from './noticeForm';
const props=defineProps<{form:NoticeForm;expanded:boolean;choice:string|null;options:{label:string;value:string;title?:string;building?:string;cycle?:string}[];loading:boolean;locked:boolean}>();
defineEmits<{toggle:[];mode:[value:string];search:[value:string];'update:choice':[value:string|null];confirm:[]}>();
const undecided=computed(()=>!props.form.values.source_record_id&&!['bind','unbound'].includes(props.form.values.manual_binding_choice));
function nameKey(value:string){return value.normalize('NFKC').replace(/^EA118(?:[_-]?C01)?(?:数据中心|机房|园区)?/i,'').replace(/[\s、，。:：;；()【】\[\]]/gu,'').replace(/通告$/,'').toLocaleLowerCase('zh-CN');}
const recommendation=computed(()=>{
  const title=nameKey(String(props.form.values.title||''));
  if(!title)return;
  const buildings=props.form.values.building_codes||[],cycle=nameKey(String(props.form.values.maintenance_cycle||''));
  const buildingKey=(value:string)=>value.replace(/楼|站|\s/g,'');
  // Recommend only a unique normalized-name match with no known building or cycle conflict.
  const matches=props.options.filter(option=>option.title&&nameKey(option.title)===title
    &&(!cycle||!option.cycle||nameKey(option.cycle)===cycle)
    &&(!buildings.length||!option.building||buildingKey(option.building)===buildingKey(buildings.map((b:string)=>b==='110'?'110站':b+'楼').join('、'))));
  return matches.length===1?matches[0]:undefined;
});
</script>
<style>
.v2-plan-choice-hint{display:inline-flex;gap:6px;flex-wrap:wrap;font-size:12px;line-height:1.5;color:var(--v2-muted);max-width:100%}.v2-plan-choice-hint>span{font-weight:400}.v2-plan-binding .needs-choice::after{content:'';position:absolute;inset:-3px;border:2px solid var(--v2-blue);border-radius:inherit;pointer-events:none;animation:v2-plan-attention 1.8s ease-in-out infinite}.v2-plan-binding .needs-choice{position:relative}
@keyframes v2-plan-attention{0%,100%{opacity:.15;transform:scale(.98)}50%{opacity:.8;transform:scale(1.03)}}
@media(prefers-reduced-motion:reduce){.v2-plan-binding .needs-choice::after{animation:none;opacity:.6}}
.v2-plan-options{display:flex;align-items:center;flex-wrap:wrap;gap:8px;min-width:0;width:100%}
.v2-plan-picker{display:grid;grid-template-columns:minmax(120px,320px) auto;align-items:center;gap:8px;min-width:0;max-width:100%}.v2-plan-picker.has-recommendation{grid-template-columns:minmax(120px,320px) auto minmax(180px,1fr);width:100%}.v2-plan-picker>.n-select{min-width:0}
.v2-plan-recommendation{display:flex;align-items:center;gap:8px;min-width:0;padding-left:8px;border-left:1px solid var(--v2-border)}.v2-plan-recommendation>span{display:grid;gap:3px;min-width:0}.v2-plan-recommendation small{font-size:12px;color:var(--v2-muted)}.v2-plan-recommendation strong{font-size:12px;font-weight:400;line-height:1.6;overflow-wrap:anywhere}.v2-plan-recommendation>.n-button{flex:none}
@media(max-width:760px){.v2-plan-picker,.v2-plan-picker.has-recommendation{grid-template-columns:minmax(0,1fr) auto;width:100%}.v2-plan-recommendation{grid-column:1/-1;padding:6px 0 0;border-left:0}.v2-plan-recommendation>span{flex:1}.v2-plan-options .n-radio,.v2-plan-picker .n-button{min-height:44px}.v2-plan-options .n-radio{align-items:center}}
</style>
