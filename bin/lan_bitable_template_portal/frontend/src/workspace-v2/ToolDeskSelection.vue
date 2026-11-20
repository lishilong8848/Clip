<script setup lang="ts">
import {computed,ref,watch} from 'vue';
import {ArrowRight, ClipboardCheck, FileCheck2, FileText, ListFilter, X} from 'lucide-vue-next';
type Asset={id:string;name:string;scope:string;model?:string;location?:string;brand?:string;template_id?:string;plan?:{mode:string;workspace_id:string}};
const props=defineProps<{r:Record<string,any>}>();
const limit=ref(40),templateSearch=ref(''),basket=ref<HTMLElement>();
watch(()=>[props.r.kind,props.r.search,props.r.scope],()=>{limit.value=40});
const matches=computed(()=>props.r.templates.filter((t:{id:string})=>props.r.templateName(t.id).toLowerCase().includes(templateSearch.value.trim().toLowerCase())));
const groups=computed(()=>{
  const r=props.r,groups=new Map<string,Asset[]>();
  for(const asset of r.pendingAssets as Asset[]){
    const key=r.kind==='external'?asset.scope:r.templateAssignments[asset.id] || '';
    if(!groups.has(key))groups.set(key,[]);
    groups.get(key)!.push(asset);
  }
  return [...groups].map(([key,assets])=>({key,assets,name:r.kind==='external'?r.scopeLabel(key)+' · 同一份报告':key?r.templateName(key):'待配模板'}));
});
function focusBatch(){basket.value?.focus({preventScroll:true});basket.value?.scrollIntoView({block:'nearest'})}
</script>
<template>
  <div class="pair-desk">
    <div class="pair-filters">
      <div class="td-tabs" aria-label="办理类型"><button :aria-pressed="r.kind==='self'" @click="r.chooseKind('self')"><ClipboardCheck :size="16"/>维护自检</button><button :aria-pressed="r.kind==='external'" @click="r.chooseKind('external')"><FileCheck2 :size="16"/>工具送检</button></div>
      <label class="pair-search"><ListFilter :size="16"/><input type="search" v-model="r.search" aria-label="搜索工具" placeholder="查找工具、型号或位置"/></label>
      <select data-native-select v-model="r.scope" aria-label="楼栋"><option v-for="s in r.scopeOptions" :key="s.value" :value="s.value">{{ s.label }}</option></select>
      <select data-native-select aria-label="继续已有批次" value="" @change="r.openSaved(($event.target as HTMLSelectElement).value)"><option value="">继续已有批次</option><option v-for="s in r.sessionOptions" :key="s.value" :value="s.value">{{ s.label }}</option></select>
      <button v-if="r.board?.history_cursor" @click="r.loadMoreHistory()">更多批次</button>
    </div>
    <div class="pair-columns" :class="{'pair-report':r.kind==='external'}">
      <section class="pair-assets">
        <div class="pair-head"><h2 id="tool-assets-title" tabindex="-1">工具清单 <small>{{ r.visibleAssets.length }}</small></h2>
          <label class="td-check"><input type="checkbox" :checked="r.allVisibleSelected" :indeterminate="r.someVisibleSelected && !r.allVisibleSelected" :disabled="!r.visibleEligible.length" @change="r.toggleVisible(($event.target as HTMLInputElement).checked)"/>全选可办</label></div>
        <article v-for="a in r.visibleAssets.slice(0,limit)" :key="a.id" class="pair-asset" :data-assigned="!!r.assigned[a.id]" :data-selected="r.selected.has(a.id)">
          <label class="td-check"><input type="checkbox" :aria-label="'选择'+a.name" :checked="r.selected.has(a.id) || !!r.assigned[a.id]" :disabled="!r.selectable(a)" @change="r.toggleAsset(a.id,($event.target as HTMLInputElement).checked)"/>
            <span><strong>{{ a.name }}</strong><small>{{ [r.scopeLabel(a.scope),a.location,a.model,a.brand].filter(Boolean).join(' · ') }}</small></span></label>
          <div class="pair-meta"><small>{{ r.cycleText(a) }}</small><small>{{ !r.eligible(a)?r.reason(a):r.assigned[a.id]?'已配 · '+r.templateName(r.assigned[a.id]):a.template_id?r.templateName(a.template_id):r.kind==='self'?'待配模板':'' }}</small></div>
          <div class="td-actions"><button class="td-link" @click="r.showRecords(a)">记录</button><button v-if="a.plan?.mode==='resume'" @click="r.openSaved(a.plan.workspace_id,a.id)">接续办理</button><button v-if="a.plan?.mode==='transfer'" @click="r.transferAsset(a)">接收移交</button></div>
        </article>
        <p v-if="!r.visibleAssets.length" class="td-empty">{{ r.loading?'正在读取工具…':'没有符合条件的工具' }}</p>
        <button v-if="limit<r.visibleAssets.length" @click="limit+=40">显示更多工具</button>
        <button v-if="r.pendingIds.length" class="pair-jump" @click="focusBatch">查看本批 {{ r.pendingIds.length }} 台 ↓</button>
      </section>
      <section v-if="r.kind==='self'" class="pair-templates">
        <div class="pair-head"><h2>配对模板</h2><small>待配 {{ r.selectedIds.length }} 台</small></div>
        <label class="td-field"><span>点选模板，配给勾选工具</span><input v-model="templateSearch" type="search" aria-label="筛选可用模板" placeholder="筛选模板"/></label>
        <button v-if="r.allAssociated" class="td-link" @click="r.chooseTemplate(r.existingTemplate)">沿用各工具已关联模板</button>
        <div class="pair-template-list"><button v-for="t in matches" :key="t.id" class="pair-option" :disabled="!r.selectedIds.length" :aria-label="'配对至'+r.templateName(t.id)" @click="r.chooseTemplate(t.id)">
          <FileText :size="17"/><strong>{{ r.templateName(t.id) }}</strong><ArrowRight :size="15"/></button></div>
        <small v-if="!matches.length">没有匹配的可用模板</small>
      </section>
      <section class="pair-batch">
        <div class="pair-head"><h2 ref="basket" tabindex="-1">已选批次</h2><strong>{{ r.pendingIds.length }} 台</strong></div>
        <p v-if="!groups.length" class="td-muted">{{ r.kind==='self'?'配对后在此按模板核对。':'选择同楼栋的工具，共用一份送检报告。' }}</p>
        <div v-for="g in groups" :key="g.key" class="pair-group" :data-group="g.key"><div class="pair-group-title"><strong>{{ g.name }}</strong><small>{{ g.assets.length }} 台</small></div>
          <div class="pair-members"><span v-for="a in g.assets" :key="a.id">{{ a.name }}<small>{{ r.scopeLabel(a.scope) }}</small><button :aria-label="'移除'+a.name" @click="r.removeSelection(a.id)"><X :size="13"/></button></span></div>
        </div>
        <p v-if="r.mixedReportBuildings" class="td-notice">请按楼栋分别送检。</p><p v-else-if="r.kind==='self' && r.unassignedCount" class="td-muted">{{ r.unassignedCount }} 台尚待配对</p>
        <button class="td-primary" :disabled="!r.canOpen || !!r.busy" @click="r.requestOpen()">开始{{ r.kind==='self'?'填写自检':'填写送检' }} <ArrowRight :size="15"/></button>
      </section>
    </div>
  </div>
</template>
<style scoped>
.pair-desk{min-width:0;background:var(--td-face);border-block:1px solid var(--td-line);container:pairDesk / inline-size}.pair-desk h2{font-size:1.02rem}.pair-head,.pair-group-title,.pair-filters{display:flex;align-items:center;justify-content:space-between;gap:.6rem;flex-wrap:wrap}
.pair-filters{justify-content:start;padding:.7rem .8rem;border-bottom:1px solid var(--td-line)}.pair-search{position:relative;flex:1 1 160px;max-width:28rem}.pair-search svg{position:absolute;left:.65rem;top:.72rem}.tool-desk .pair-search input{width:100%;padding-left:2rem}
.pair-columns{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,.8fr) minmax(0,1fr)}.pair-columns.pair-report{grid-template-columns:minmax(0,1.7fr) minmax(0,1fr)}
.pair-columns>section{padding:.85rem;min-width:0}.pair-templates,.pair-batch{border-inline-start:1px solid var(--td-line)}
.pair-batch{background:color-mix(in srgb,var(--td-mint) 60%,var(--td-face));display:flex;flex-direction:column;align-items:stretch;gap:.85rem}.pair-templates{display:flex;flex-direction:column;gap:.6rem}
.pair-head{min-height:35px}.pair-asset{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(0,.85fr) auto;align-items:center;gap:.35rem;padding:.5rem .25rem;border-top:1px solid var(--td-line);overflow-wrap:anywhere}.pair-assets{container:inventory / inline-size}
.pair-asset .td-check{flex-wrap:nowrap;cursor:pointer}.pair-asset input{flex:none}.pair-asset small{display:block}.pair-asset[data-selected=true]{background:var(--td-sky)}.pair-asset[data-assigned=true]{background:var(--td-mint)}
.pair-template-list{max-height:23rem;overflow:auto}.tool-desk .pair-option{width:100%;justify-content:flex-start;text-align:start;margin:.16rem 0;padding:.55rem;box-shadow:var(--soft-key);border-inline-start:3px solid var(--td-blue)}
.pair-option strong{flex:1;overflow-wrap:anywhere}.tool-desk .pair-option:disabled{opacity:.8}.pair-option svg{flex:none}
.pair-group{border-inline-start:3px solid var(--td-green);padding-inline-start:.55rem}.pair-group-title{border-bottom:1px solid var(--td-line);padding-bottom:.25rem}.pair-members{display:flex;flex-wrap:wrap;gap:.35rem;margin-top:.44rem}
.pair-members>span{display:inline-flex;align-items:center;gap:.25rem;flex-wrap:wrap;border-radius:6px;padding:.12rem .3rem;background:var(--td-face);font-size:.86rem}.tool-desk .pair-members button{min-height:25px;padding:0 .2rem;background:transparent;border:0}
.tool-desk .pair-jump{display:none}
@container pairDesk (min-width:680px) and (max-width:980px){.pair-columns{grid-template-columns:minmax(0,1.25fr) minmax(0,1fr)}.pair-assets{grid-row:1/3}.pair-batch{border-top:1px solid var(--td-line)}}
@container pairDesk (max-width:679px){.pair-columns,.pair-columns.pair-report{grid-template-columns:minmax(0,1fr)}.pair-templates,.pair-batch{border-inline-start:0;border-block-start:1px solid var(--td-line)}.tool-desk .pair-jump{display:inline-flex}.pair-template-list{max-height:13rem}}
@container inventory (max-width:340px){.pair-asset{grid-template-columns:minmax(0,1fr) auto}.pair-meta{grid-row:2;grid-column:1 / -1;display:flex;gap:.5rem;flex-wrap:wrap;padding-inline-start:1.6rem}}
</style>