<template>
  <section ref="host" class="v2-repair-flow" aria-label="检修办理流程" :aria-busy="busy">
    <h3>检修办理流程</h3>
    <NSteps size="small" :theme-overrides="stepTheme" :vertical="narrow" :content-placement="narrow?'right':'bottom'">
      <NStep v-for="step in steps" :key="step.id" :status="step.status" :data-flow-step="step.id" :data-flow-status="step.status">
        <template #title>
          <NButton v-if="actions?.includes(step.id)" text data-v2-navigation :disabled="busy" :aria-label="step.actionLabel" @click="select(step.id)">{{step.title}}<ChevronRight :size="13" aria-hidden="true" /></NButton>
          <span v-else>{{step.title}}</span>
        </template>
        <span :key="step.description" class="v2-repair-flow-description" role="status">{{step.description}}</span>
      </NStep>
    </NSteps>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref } from 'vue';
import { NButton,NStep,NSteps,useThemeVars } from 'naive-ui';
import { ChevronRight } from 'lucide-vue-next';
import { repairFlowSteps,type RepairFlowAction } from './repairFlow';
import type { Dict } from './client';
const props=defineProps<{context:'event'|'project';record?:Dict|null;event?:Dict|null;syncStatus?:string;actions?:RepairFlowAction[];busy?:boolean}>();
const emit=defineEmits<{select:[action:RepairFlowAction]}>();
const steps=computed(()=>repairFlowSteps(props.context,props.record,props.event,props.syncStatus));
const colors=useThemeVars();
const stepTheme=computed(()=>({indicatorColorFinish:colors.value.successColor,indicatorBorderColorFinish:colors.value.successColor,indicatorTextColorFinish:colors.value.baseColor,splitorColorFinish:colors.value.successColor,headerTextColorFinish:colors.value.textColor1,headerTextColorWait:colors.value.textColor2,indicatorTextColorWait:colors.value.textColor2}));
const host=ref<HTMLElement>(),narrow=ref(false);let observer:ResizeObserver|undefined;
function select(action:RepairFlowAction){if(!props.busy&&props.actions?.includes(action))emit('select',action);}
onMounted(()=>{if(!host.value)return;narrow.value=host.value.clientWidth<540;observer=new ResizeObserver(([entry])=>{if(entry)narrow.value=entry.contentRect.width<540;});observer.observe(host.value);});
onBeforeUnmount(()=>observer?.disconnect());
</script>
<style scoped>
/* VIPBoard XSteps presentation, using Vue controls and per-step business evidence. */
.v2-repair-flow{min-width:0;padding:12px 0;border-block:1px solid var(--v2-border);color:var(--v2-ink)}
.v2-repair-flow h3{font-size:13px;font-weight:600;line-height:1.5;margin:0 0 12px}
.v2-repair-flow :deep(.n-step){min-width:0;flex:1}
.v2-repair-flow :deep(.n-step-content){min-width:0}
.v2-repair-flow :deep(.n-steps .n-step-content .n-step-content-header__title){flex:1;min-width:0;font-size:13px;line-height:1.5;white-space:normal;overflow-wrap:anywhere}
.v2-repair-flow :deep(.n-button){min-height:32px;height:auto;font-size:13px;max-width:100%;text-align:left}
.v2-repair-flow :deep(.n-button__content){white-space:normal;gap:2px;line-height:1.5}
.v2-repair-flow :deep(.n-step-content__description){margin-top:2px;font-size:12px;line-height:1.6;color:var(--v2-muted);overflow-wrap:anywhere}
.v2-repair-flow :deep(.n-step-indicator){transition:background-color 220ms ease,border-color 220ms ease,box-shadow 220ms ease}
.v2-repair-flow :deep(.n-step--process-status .n-step-indicator){box-shadow:0 0 0 4px color-mix(in srgb,var(--v2-blue) 12%,transparent)}
.v2-repair-flow :deep(.n-steps--vertical .n-step-content){padding-bottom:12px}
.v2-repair-flow-description{display:inline-block;animation:repair-flow-in 200ms ease-out}
@keyframes repair-flow-in{from{opacity:.35;transform:translateY(3px)}to{opacity:1;transform:none}}
@media(max-width:760px){.v2-repair-flow :deep(.n-button){min-height:44px}.v2-repair-flow :deep(.n-steps--vertical .n-step-content){padding-bottom:5px}}
@media(prefers-reduced-motion:reduce){.v2-repair-flow-description{animation:none}.v2-repair-flow :deep(*){transition:none!important;animation:none!important}}
</style>
