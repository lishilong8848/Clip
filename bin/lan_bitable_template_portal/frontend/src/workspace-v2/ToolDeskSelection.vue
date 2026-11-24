<script setup lang="ts">
import {ref,watch} from 'vue';
import {ArrowRight, CheckCircle2, ClipboardCheck, FileCheck2, ListFilter, X} from 'lucide-vue-next';
const props=defineProps<{r:Record<string,any>}>();
const limit=ref(40),basket=ref<HTMLElement>();
watch(()=>[props.r.kind,props.r.search,props.r.scope,props.r.profession],()=>{limit.value=40});
function focusPanel(panel?:HTMLElement){panel?.focus({preventScroll:true});panel?.scrollIntoView({block:'start'})}
</script>
<template>
  <div class="pair-desk">
    <div class="pair-filters">
      <div class="td-tabs" aria-label="办理类型"><button :aria-pressed="r.kind==='self'" @click="r.chooseKind('self')"><ClipboardCheck :size="16"/>维护自检</button><button :aria-pressed="r.kind==='external'" @click="r.chooseKind('external')"><FileCheck2 :size="16"/>工具送检</button></div>
      <label class="pair-search"><ListFilter :size="16"/><input type="search" v-model="r.search" aria-label="搜索工具" placeholder="工具、专业、型号或位置"/></label>
      <select data-native-select v-model="r.scope" aria-label="楼栋"><option v-for="s in r.scopeOptions" :key="s.value" :value="s.value">{{ s.label }}</option></select>
      <select data-native-select v-model="r.profession" aria-label="筛选工具专业"><option v-for="p in r.professionOptions" :key="p.value" :value="p.value">{{ p.label }}</option></select>
    </div>
    <div class="pair-columns">
      <section class="pair-assets">
        <div class="pair-head"><h2 id="tool-assets-title" tabindex="-1">工具清单 <small>{{ r.visibleAssets.length }}</small></h2>
          <label class="td-check"><input type="checkbox" :checked="r.allVisibleSelected" :indeterminate="r.someVisibleSelected && !r.allVisibleSelected" :disabled="!r.visibleEligible.length" @change="r.toggleVisible(($event.target as HTMLInputElement).checked)"/>全选可办</label></div>
        <div v-if="r.visibleAssets.length" class="pair-asset-list" role="region" aria-label="筛选后的工具" tabindex="0">
          <article v-for="a in r.visibleAssets.slice(0,limit)" :key="a.id" class="pair-asset"
            :data-selected="r.selected.has(a.id)" :data-filled="a.filled_this_month||a.monthly_locked||undefined">
          <label class="td-check"><input type="checkbox" :aria-label="'选择'+a.name" :checked="r.selected.has(a.id)" :disabled="!r.selectable(a)" @change="r.toggleAsset(a.id,($event.target as HTMLInputElement).checked)"/>
            <span><strong>{{ a.name }}</strong><small>{{ [r.scopeLabel(a.scope),a.profession || '专业未填写',a.location,a.model,a.brand].filter(Boolean).join(' · ') }}</small><small v-if="a.responsible_name">负责人 {{ a.responsible_name }}</small></span></label>
          <div class="pair-meta"><span v-if="a.filled_this_month||a.monthly_locked" class="pair-filled"><CheckCircle2 :size="14" aria-hidden="true"/>本月已填写</span>
            <small>{{ r.cycleText(a) }}</small><small v-if="!r.eligible(a)&&!a.monthly_locked">{{ r.reason(a) }}</small><small v-else-if="r.kind==='self'&&!a.monthly_locked">{{ a.template_name }}</small></div>
          <div class="td-actions"><button class="td-link" @click="r.showRecords(a)">记录</button><button v-if="a.plan?.mode==='resume'" @click="r.openSaved(a.plan.workspace_id,a.id)">接续办理</button><button v-if="a.plan?.mode==='transfer'" @click="r.transferAsset(a)">接收移交</button></div>
          </article>
          <button v-if="limit<r.visibleAssets.length" class="td-link" @click="limit+=40">显示更多工具</button>
        </div>
        <p v-else class="td-empty">{{ r.loading?'正在读取工具…':'没有符合条件的工具' }}</p>
        <div v-if="r.selectedIds.length" class="pair-jump td-actions">
          <button class="td-link" @click="focusPanel(basket)">查看本批 {{ r.selectedIds.length }} 台 <ArrowRight :size="15"/></button>
        </div>
      </section>
      <section class="pair-batch">
        <div class="pair-head"><h2 ref="basket" tabindex="-1">已选批次</h2><div class="td-actions"><strong>{{ r.selectedIds.length }} 台</strong><button v-if="r.selectedIds.length" type="button" class="td-link" @click="r.clearSelection()">取消全部</button></div></div>
        <p v-if="!r.pendingAssets.length" class="td-muted">{{ r.kind==='self'?'从工具清单勾选。':'选择同楼栋的工具，共用一份送检报告。' }}</p>
        <ul v-else class="pair-members" aria-label="本批工具">
          <li v-for="a in r.pendingAssets" :key="a.id"><div><strong>{{ a.name }}</strong><small>{{ [r.scopeLabel(a.scope),a.profession || '专业未填写',r.kind==='self'?a.template_name:''].filter(Boolean).join(' · ') }}</small></div><button :aria-label="'移除'+a.name" @click="r.removeSelection(a.id)"><X :size="15"/></button></li>
        </ul>
        <p v-if="r.mixedReportBuildings" class="td-notice">请按楼栋分别送检。</p>
        <button class="td-primary" :disabled="!r.canOpen || !!r.busy" @click="r.requestOpen()">开始{{ r.kind==='self'?'填写自检':'填写送检' }} <ArrowRight :size="15"/></button>
      </section>
    </div>
  </div>
</template>
<style scoped>
.pair-desk{min-width:0;container:pairDesk / inline-size}.pair-desk h2{font-size:1.02rem}.pair-head,.pair-filters{display:flex;align-items:center;justify-content:space-between;gap:.6rem;flex-wrap:wrap}
.pair-filters{justify-content:start;padding:.55rem .65rem;margin-bottom:.65rem;border:1px solid var(--td-line);border-radius:10px;background:var(--td-face)}
.pair-search{position:relative;flex:1 1 160px;max-width:34rem}.pair-search svg{position:absolute;left:.65rem;top:.72rem;pointer-events:none}.tool-desk .pair-search input{width:100%;padding-left:2rem}
.pair-columns{display:grid;grid-template-columns:minmax(0,2fr) minmax(215px,1fr);gap:.65rem;align-items:start}
.pair-columns>section{padding:.65rem;min-width:0;border:1px solid var(--td-line);border-radius:12px;background:var(--td-face)}
.pair-columns>.pair-batch{background:color-mix(in srgb,var(--td-mint) 60%,var(--td-face));display:flex;flex-direction:column;align-items:stretch;gap:.65rem}
.pair-head{min-height:32px}.pair-head h2[tabindex]{scroll-margin-block-start:1rem}
.pair-asset{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(0,.85fr) auto;align-items:center;gap:.35rem;padding:.35rem .3rem;border-top:1px solid var(--td-line);overflow-wrap:anywhere}.pair-assets{container:inventory / inline-size}
.pair-asset .td-check{flex-wrap:nowrap;cursor:pointer}.pair-asset input{flex:none}.pair-asset small{display:block}
.pair-asset[data-filled=true]{box-shadow:inset 3px 0 var(--cf-success,#17824f);background:color-mix(in srgb,var(--cf-success,#17824f) 8%,var(--td-face))}
.pair-asset[data-selected=true]{background:var(--td-sky)}
.pair-filled{display:inline-flex;align-items:center;gap:.25rem;padding:.13rem .4rem;border-radius:5px;color:var(--cf-success,#17824f);font-size:.8rem;font-weight:700}
.pair-asset-list{max-height:min(63dvh,38rem);overflow:auto;scrollbar-gutter:stable}
.pair-members{list-style:none;max-height:min(48dvh,28rem);overflow:auto;display:grid;gap:.35rem;margin:0;padding:2px;scrollbar-gutter:stable}
.pair-members li{display:flex;align-items:center;justify-content:space-between;gap:.45rem;border-radius:6px;padding:.4rem .55rem;background:var(--td-face);overflow-wrap:anywhere}
.pair-members small{display:block}.tool-desk .pair-members button{flex:none;min-height:32px;padding:0 .35rem;background:transparent;border:0}
.tool-desk .pair-jump{display:none}
@container pairDesk (max-width:679px){.pair-columns{grid-template-columns:minmax(0,1fr)}.tool-desk .pair-jump{display:flex}.pair-asset-list{max-height:min(45dvh,23rem)}}
@container inventory (max-width:340px){.pair-asset{grid-template-columns:minmax(0,1fr) auto}.pair-meta{grid-row:2;grid-column:1 / -1;display:flex;gap:.5rem;flex-wrap:wrap;padding-inline-start:1.6rem}}
</style>
