<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue';
import { NSelect } from 'naive-ui';
import { Search, UserRound } from 'lucide-vue-next';

type Asset = { id: string; name: string; scope: string; model?: string; location?: string; supervisor?: string };
const props=defineProps<{r:Record<string,any>}>();
const query=ref(''), building=ref(''), page=ref(1);
const editor=ref<HTMLElement>();
const pageSize=8;
const scopeLabel=(scope:string)=>scope==='110'?'110站':scope ? scope+'楼' : '';
const assets=computed<Asset[]>(()=>props.r.data?.items || []);
const peopleNames=computed(()=>new Map<string,string>(props.r.people.map((person:{id:string;name:string})=>[person.id,person.name])));
const buildings=computed(()=>[...new Set(assets.value.map(asset=>asset.scope).filter(Boolean))].sort());
const filtered=computed(()=>{
  const terms=query.value.toLocaleLowerCase().trim().split(/\s+/).filter(Boolean);
  return assets.value.filter(asset=>(!building.value || asset.scope===building.value) && terms.every(term=>
    [asset.name,scopeLabel(asset.scope),asset.model,asset.location].filter(Boolean).join(' ').toLocaleLowerCase().includes(term)));
});
const pageCount=computed(()=>Math.max(1,Math.ceil(filtered.value.length/pageSize)));
const visible=computed(()=>filtered.value.slice((page.value-1)*pageSize,page.value*pageSize));
const dirty=computed(()=>props.r.form && props.r.form.supervisor!==(props.r.asset?.supervisor || null));
watch([query,building],()=>{page.value=1;});
watch(pageCount,total=>{page.value=Math.min(page.value,total);});

async function choose(id:string) {
  if (props.r.selection!==id) props.r.message='';
  props.r.selection=id;
  await nextTick();
  editor.value?.focus({preventScroll:true});
  editor.value?.scrollIntoView({block:'nearest'});
}
</script>
<template>
  <section class="td-stack" aria-label="工具责任配置">
    <p v-if="r.error" class="td-notice td-error" role="alert">{{ r.error }}</p>
    <p v-if="r.message" role="status" class="td-notice">{{ r.message }}</p>
    <div class="responsibility-layout">
      <section class="td-stack responsibility-picker" aria-labelledby="tool-responsibility-list">
        <div class="td-heading"><div><h3 id="tool-responsibility-list">工具台账</h3><small v-if="r.data">{{ filtered.length }} 条记录</small></div>
          <button type="button" :disabled="r.loading || r.saving" @click="r.load()">{{ r.loading?'正在读取…':'刷新责任配置' }}</button>
        </div>
        <label class="responsibility-search"><Search :size="16" aria-hidden="true"/><input v-model="query" type="search" aria-label="搜索库存工具" placeholder="名称、型号或存放位置"/></label>
        <div v-if="buildings.length>1" class="td-tabs responsibility-scopes" role="group" aria-label="工具楼栋">
          <button type="button" :aria-pressed="!building" @click="building=''">全部楼栋</button>
          <button v-for="scope in buildings" :key="scope" type="button" :aria-pressed="building===scope" @click="building=scope">{{ scopeLabel(scope) }}</button>
        </div>
        <div v-if="visible.length" class="responsibility-assets" :aria-busy="r.loading">
          <button v-for="asset in visible" :key="asset.id" type="button" class="responsibility-asset" :aria-pressed="r.selection===asset.id" :disabled="r.saving" @click="choose(asset.id)">
            <span><strong>{{ asset.name }}</strong><small>{{ [scopeLabel(asset.scope),asset.model,asset.location].filter(Boolean).join(' · ') }}</small></span>
            <span class="td-chip" :data-assigned="!!asset.supervisor">{{ asset.supervisor ? peopleNames.get(asset.supervisor) || '已指定' : '待指定' }}</span>
          </button>
        </div>
        <p v-else-if="!r.error" class="td-empty">{{ r.loading?'正在读取工具…':'没有匹配的工具' }}</p>
        <div v-if="pageCount>1" class="td-row"><small>{{ page }} / {{ pageCount }} 页</small><div class="td-actions">
          <button type="button" :disabled="page===1" @click="page--">上一页</button><button type="button" :disabled="page===pageCount" @click="page++">下一页</button>
        </div></div>
      </section>
      <section class="td-stack responsibility-editor" aria-labelledby="tool-responsibility-editor">
        <div class="td-heading"><div><h3 id="tool-responsibility-editor" ref="editor" tabindex="-1">{{ r.asset?.name || '责任人配置' }}</h3>
          <small v-if="r.asset">{{ [scopeLabel(r.asset.scope),r.asset.model,r.asset.location].filter(Boolean).join(' · ') }}</small></div>
          <UserRound :size="18" aria-hidden="true"/>
        </div>
        <form v-if="r.asset && r.form" class="td-stack" @submit.prevent="dirty && r.saveTool()">
          <div class="td-actions"><span v-if="r.asset.source_self_cycle" class="td-chip">维护 {{ r.asset.source_self_cycle }}</span>
            <span v-if="r.asset.source_external_cycle" class="td-chip">送检 {{ r.asset.source_external_cycle }}</span></div>
          <label class="td-field">专业主管
            <NSelect v-model:value="r.form.supervisor" :options="r.supervisorOptions" :disabled="r.saving || r.loading" filterable clearable placeholder="选择本楼主管"/>
          </label>
          <div class="td-actions"><button type="submit" class="td-primary" :disabled="!dirty || r.saving || r.loading">{{ r.saving?'保存中…':'保存负责人' }}</button></div>
        </form>
        <p v-else class="td-muted">选择工具后可以查看和调整主管。</p>
        <div v-if="r.data?.scrap_approvers?.length" class="responsibility-approvers">
          <small>报废审批人</small><div class="td-actions"><span v-for="name in r.data.scrap_approvers" :key="name" class="td-chip">{{ name }}</span><small>任一位同意即可</small></div>
        </div>
      </section>
    </div>
  </section>
</template>
<style scoped>
.responsibility-layout{display:grid;grid-template-columns:minmax(255px,.9fr) minmax(0,1fr);gap:1rem;align-items:start}
.tool-desk .responsibility-picker,.tool-desk .responsibility-editor{gap:.65rem;min-width:0}
.responsibility-editor{padding:1rem;border:1px solid var(--td-line);border-radius:14px;background:var(--td-field)}
.responsibility-editor .td-heading{align-items:start;overflow-wrap:anywhere}
.responsibility-editor h3:focus{outline:none}
.responsibility-search{display:flex;align-items:center;gap:.4rem;min-width:0;color:var(--td-muted)}
.responsibility-search input{flex:1;width:100%}
.responsibility-assets{display:grid;gap:.25rem;min-width:0}
.tool-desk button.responsibility-asset{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.25rem .6rem;justify-items:start;padding:.48rem .65rem;text-align:left}
.responsibility-asset>span:first-child{display:grid;gap:.12rem;min-width:0;overflow-wrap:anywhere}
.responsibility-asset>.td-chip{max-width:8rem;overflow-wrap:anywhere}
.responsibility-asset[aria-pressed=true]{border-color:var(--td-blue);background:var(--td-sky)}
.responsibility-asset [data-assigned=false]{background:var(--td-field);color:var(--td-muted)}
.responsibility-approvers{display:grid;gap:.25rem;border-top:1px solid var(--td-line);padding-top:.7rem}
@container(max-width:680px){.responsibility-layout{grid-template-columns:minmax(0,1fr)}}
@media(pointer:coarse){.tool-desk .responsibility-layout button{min-height:44px}}
</style>
