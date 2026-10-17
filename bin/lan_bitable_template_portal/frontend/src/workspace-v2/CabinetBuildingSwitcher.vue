<template>
  <nav class="v2-cabinet-building-switcher" aria-label="机柜楼栋切换">
    <NButton data-v2-navigation :type="scope ? 'default' : 'primary'" :secondary="!scope" :aria-pressed="!scope" :disabled="!options.length" @click="navigate('/cabinet-power?view=overview')"><template #icon><PanelsTopLeft :size="17" /></template>{{ options.length === 5 ? '五楼总览' : '楼栋总览' }}</NButton>
    <NativeSingleSelectV2 class="v2-cabinet-building-select"><select :value="scope" :disabled="!options.length" aria-label="选择机柜楼栋" @change="selectBuilding(($event.target as HTMLSelectElement).value)"><option value="" disabled>选择单楼查看</option><option v-for="option in options" :key="option.value" :value="option.value">{{ option.label }}</option></select></NativeSingleSelectV2>
  </nav>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { NButton } from 'naive-ui';
import { PanelsTopLeft } from 'lucide-vue-next';
import NativeSingleSelectV2 from './NativeSingleSelect.vue';
import { navigate } from './navigation';

const props = defineProps<{ scope: string; scopeOptions?: { value:string; label:string }[] }>();
const options = computed(() => 'ABCDE'.split('').filter(value => props.scopeOptions?.some(option => option.value === 'ALL' || option.value === value)).map(value => ({ value, label:value + '楼' })));
function selectBuilding(value:string) { if (options.value.some(option => option.value === value)) navigate('/cabinet-power?scope=' + value); }
</script>

<style scoped>
.v2-cabinet-building-switcher{display:flex;align-items:center;flex-wrap:wrap;gap:8px;min-width:0}
.v2-cabinet-building-switcher .v2-cabinet-building-select{width:156px}
@media(max-width:760px){.v2-cabinet-building-switcher{width:100%}.v2-cabinet-building-switcher .v2-cabinet-building-select{flex:1;min-width:120px}}
</style>
