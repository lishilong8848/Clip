<script setup lang="ts">
import { computed } from 'vue';
const props = defineProps<{r:Record<string,any>}>();
const p = computed(() => props.r.props);
const names = computed(() => new Map<string,string>((p.value.assets || []).map((a:{id:string;name:string;model?:string})=>[a.id,[a.name,a.model].filter(Boolean).join(' · ')])));
const ids = computed<string[]>(() => p.value.assetIds.length ? p.value.assetIds : p.value.model.confirmed_asset_ids);
function defaultResult(event:Event) {
  p.value.model.result=(event.target as HTMLSelectElement).value;
  if(p.value.model.result!=='qualified') p.value.model.expires_on='';
}
</script>
<template>
  <fieldset class="td-stack" :disabled="p.disabled || !p.editable">
    <div class="td-grid">
      <label class="td-field">送检报告编号<input v-model="p.model.report_no" required/></label>
      <label class="td-field">检测机构<input v-model="p.model.institution" required/></label>
      <label class="td-field">报告结论<select data-native-select aria-label="报告结论" :value="p.model.result" @change="defaultResult"><option value="">待填写</option><option value="qualified">合格</option><option value="unqualified">不合格</option></select></label>
      <label class="td-field">有效期至<input v-model="p.model.expires_on" type="date" :min="p.actual" :disabled="p.model.result!=='qualified'"/><small>{{ p.expiresOn || '可按工具送检周期计算' }}</small></label>
    </div>
    <div v-if="p.assets?.length" class="td-list">
      <div v-for="id in ids" :key="id" class="td-grid">
        <strong>{{ names.get(id) || id }}</strong>
        <label class="td-field">本台结论<select data-native-select :aria-label="'本台结论 · '+(names.get(id) || id)" :value="p.model.tools?.[id]?.result || ''" @change="r.changeToolResult(id,($event.target as HTMLSelectElement).value)">
          <option value="">沿用报告结论</option><option value="qualified">合格</option><option value="unqualified">不合格</option></select></label>
        <label v-if="p.model.tools?.[id]?.result==='qualified'" class="td-field">本台有效期至<input v-model="p.model.tools[id].expires_on" type="date" :min="p.actual"/></label>
      </div>
    </div>
    <label v-if="p.editable" class="td-check"><input type="checkbox" :checked="r.confirmed" @change="p.model.confirmed_asset_ids=($event.target as HTMLInputElement).checked?[...p.assetIds]:[]"/>已核对报告覆盖本次办理的 {{ p.assetIds.length }} 台工具</label>
    <p v-else class="td-muted">已核对 {{ p.model.confirmed_asset_ids.length }} 台工具</p>
  </fieldset>
</template>