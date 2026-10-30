<template>
  <aside
    class="kb-tree-pane v2-kb-tree-pane v2-kb-folder-navigation"
    :class="{ 'is-open': open, 'is-managing': managing }"
    aria-label="文件夹导航"
    @keydown.esc="closePanel"
  >
    <div class="v2-kb-folder-summary">
      <div class="v2-kb-current-folder">
        <span>当前目录</span>
        <strong :title="currentLabel">{{ currentLabel }}</strong>
      </div>
      <button
        ref="trigger"
        type="button"
        class="v2-kb-directory-toggle"
        data-v2-navigation
        :aria-controls="panelId"
        :aria-expanded="open"
        @click="open ? closePanel() : open = true"
      >
        <FolderOpen :size="16" aria-hidden="true" />
        {{ open ? '收起目录' : '选择文件夹' }}
        <ChevronDown :size="14" aria-hidden="true" />
      </button>
    </div>

    <div :id="panelId" class="v2-kb-directory-panel">
      <div class="kb-tree-pane-head" role="group" aria-label="文档范围">
        <button type="button" :class="{ active: selectedId === 'all' }" :aria-pressed="selectedId === 'all'" :disabled="busy" @click="selectFolder('all')">全部</button>
        <button type="button" :class="{ active: selectedId === 'ungrouped' }" :aria-pressed="selectedId === 'ungrouped'" :disabled="busy" @click="selectFolder('ungrouped')">未分组</button>
        <button type="button" class="v2-kb-management-toggle" :aria-expanded="managing" :disabled="busy" @click="managing = !managing">
          <Settings2 :size="15" aria-hidden="true" />{{ managing ? '完成管理' : '管理目录' }}
        </button>
      </div>
      <KnowledgeFolders
        :groups="groups"
        :busy="busy"
        :selected-id="selectedId"
        @select="selectFolder"
        @create="(name, parentId) => emit('create', name, parentId)"
        @rename="(id, name, parentId) => emit('rename', id, name, parentId)"
        @delete="group => emit('delete', group)"
      />
    </div>
  </aside>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, useId } from 'vue';
import { ChevronDown, FolderOpen, Settings2 } from 'lucide-vue-next';
import KnowledgeFolders from './KnowledgeFolders.vue';
import { folderOptions, type GroupItem } from './knowledgeFolders';

const props = defineProps<{
  groups: GroupItem[];
  busy: boolean;
  selectedId?: string;
}>();
const emit = defineEmits<{
  (e: 'select', id: string): void;
  (e: 'create', name: string, parentId?: string): void;
  (e: 'rename', id: string, name: string, parentId?: string): void;
  (e: 'delete', group: GroupItem): void;
}>();

const open = ref(false);
const managing = ref(false);
const trigger = ref<HTMLButtonElement | null>(null);
const panelId = `v2-kb-directory-${useId()}`;
const currentLabel = computed(() => {
  if (!props.selectedId || props.selectedId === 'all') return '全部文件';
  if (props.selectedId === 'ungrouped') return '未分组';
  return folderOptions(props.groups).find(option => option.value === props.selectedId)?.label || '文件夹';
});

function closePanel(): void {
  open.value = false;
  managing.value = false;
  // The trigger is mobile-only; keep desktop tree focus unchanged.
  if (window.matchMedia('(max-width: 860px)').matches) {
    void nextTick(() => trigger.value?.focus());
  }
}

function selectFolder(id: string): void {
  if (props.busy) return;
  emit('select', id);
  closePanel();
}
</script>
