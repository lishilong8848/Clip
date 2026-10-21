<template>
  <nav class="admin-workspace-tabs" aria-label="管理员工作区">
    <button type="button" :class="{ active: activeTab === 'permissions' }" :aria-current="activeTab === 'permissions' ? 'page' : undefined" @click="emit('select-tab', 'permissions')">
      <UsersRound :size="18" /><span>账号与权限</span><b v-if="accountBadge">{{ accountBadge }}</b>
    </button>
    <button type="button" class="other-toggle" :aria-expanded="otherOpen" aria-controls="admin-other-settings" @click="otherOpen = !otherOpen">
      <Settings2 :size="18" /><span>其他设置</span><ChevronDown :size="16" :class="{ rotated: otherOpen }" />
    </button>
    <UiTransition name="ui-page">
      <div v-if="otherOpen" id="admin-other-settings" class="other-settings">
        <button v-for="item in otherTabs" :key="item.key" type="button" :class="{ active: activeTab === item.key }" :aria-current="activeTab === item.key ? 'page' : undefined" @click="emit('select-tab', item.key)">
          <component :is="icons[item.key]" :size="16" /><span>{{ item.label }}</span><b v-if="item.badge">{{ item.badge }}</b>
        </button>
        <button v-if="!advancedVisible" type="button" @click="emit('toggle-advanced')"><Wrench :size="16" /><span>高级诊断</span></button>
      </div>
    </UiTransition>
  </nav>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { Activity, ChevronDown, ClipboardList, FileCog, MessageSquare, NotebookPen, Settings2, UsersRound, Wrench } from 'lucide-vue-next';
import type { AdminTabKey } from '../adminToolsUtils';
const props = defineProps<{
  tabs: Array<{ key: AdminTabKey; label: string; description: string; badge: string }>;
  activeTab: AdminTabKey;
  advancedVisible: boolean;
}>();
const emit = defineEmits<{ 'select-tab': [tab: AdminTabKey]; 'toggle-advanced': [] }>();
const otherOpen = ref(props.activeTab !== 'permissions');
const accountBadge = computed(() => props.tabs.find(item => item.key === 'permissions')?.badge);
const otherTabs = computed(() => props.tabs.filter(item => item.key !== 'permissions'));
const icons = { status: Activity, permissions: UsersRound, handover: ClipboardList, mop: FileCog, feishu: MessageSquare, pressure: Wrench, development: NotebookPen };
watch(() => props.activeTab, tab => { if (tab !== 'permissions') otherOpen.value = true; });
</script>

<style scoped>
.admin-workspace-tabs { display: grid; gap: 6px; position: sticky; top: 90px; }
button { width: 100%; min-height: 42px; display: flex; align-items: center; gap: 9px; padding: 10px 12px; border: 1px solid transparent; border-radius: 7px; background: transparent; color: #49627d; font: inherit; font-size: 14px; text-align: left; cursor: pointer; transition: background 160ms ease, color 160ms ease; }
button span { flex: 1; min-width: 0; overflow-wrap: anywhere; }button svg { flex-shrink: 0; }
button:hover { background: #e8f0fa; }button.active { background: #e6effe; color: #1858bd; font-weight: 650; border-color: #c9dcfa; }
button:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
b { min-width: 20px; padding: 2px 5px; background: #e9eef5; color: #526b84; font-size: 11px; text-align: center; border-radius: 4px; }
.other-toggle { margin-top: 8px; border-top: 1px solid #dce6f0; border-radius: 0; padding-top: 15px; }
.other-toggle svg:last-child { transition: transform 180ms ease; }.rotated { transform: rotate(180deg); }
.other-settings { display: grid; gap: 4px; padding-left: 10px; }.other-settings button { font-size: 13px; min-height: 38px; }
@media(max-width:640px) { .admin-workspace-tabs { position: static; }.other-settings { grid-template-columns: repeat(2, minmax(0,1fr)); padding: 0; } }
@media(prefers-reduced-motion:reduce) { button,.other-toggle svg:last-child { transition: none; } }
</style>
