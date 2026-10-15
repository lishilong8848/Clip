<script setup lang="ts">
import { computed } from 'vue';
import { NButton, NSelect, NTag } from 'naive-ui';
import { Settings2, Wrench } from 'lucide-vue-next';
import type { AdminTabKey } from '../adminToolsUtils';
const props = defineProps<{
  tabs: Array<{ key: AdminTabKey; label: string; description: string; badge: string }>;
  activeTab: AdminTabKey;
  advancedVisible: boolean;
}>();
const emit = defineEmits<{ 'select-tab': [tab: AdminTabKey]; 'toggle-advanced': [] }>();
const selected = computed(() => props.tabs.find(item => item.key === props.activeTab));
const options = computed(() => props.tabs.map(item => ({ value:item.key, label:item.label })));
function select(value: AdminTabKey) { if (props.tabs.some(item => item.key === value)) emit('select-tab',value); }
</script>
<template>
  <div class="v2-settings-picker" data-v2-navigation>
    <label for="v2-settings-category"><Settings2 :size="17" />设置分类</label>
    <NSelect :value="activeTab" :options="options" :input-props="{id:'v2-settings-category'}" aria-label="设置分类" @update:value="select" />
    <NTag v-if="selected?.badge" size="small" :bordered="false">{{selected.badge}}</NTag>
    <NButton v-if="!advancedVisible" size="small" quaternary @click="emit('toggle-advanced')"><Wrench :size="15" />高级诊断</NButton>
  </div>
</template>
<style scoped>
.v2-settings-picker{display:flex;align-items:center;gap:10px;flex-wrap:wrap;border-bottom:1px solid var(--v2-border);padding:4px 0 14px}
label{display:flex;gap:7px;align-items:center;font-size:13px;color:var(--v2-muted)}
.n-select{width:240px;max-width:100%}.n-button{margin-left:auto}.n-button :deep(.n-button__content){gap:6px}
@media(max-width:600px){.n-select{width:calc(100% - 90px)}.n-button{margin-left:0}}
</style>
