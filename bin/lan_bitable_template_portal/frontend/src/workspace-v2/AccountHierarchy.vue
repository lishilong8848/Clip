<script setup lang="ts">
import { computed, ref } from 'vue';
import {
  buildAccountHierarchy,
  flattenHierarchyPeople,
  type AccountLike,
  type PersonTreeItem,
} from './accountHierarchy';
import './accountPresentation.css';

type Mode = 'list' | 'tree';

const props = withDefaults(
  defineProps<{
    accounts: AccountLike[];
    /** Controlled mode. When omitted the component manages its own state. */
    mode?: Mode;
    /** Show the internal list/tree switch. Defaults to true. */
    showToggle?: boolean;
    levelLabel?: (level?: string) => string;
    buildingLabel?: (value: string) => string;
    scopeLabel?: (scopes: string[]) => string;
  }>(),
  {
    mode: undefined,
    showToggle: true,
    levelLabel: (level?: string) => level || '未分级',
    buildingLabel: (value: string) => value,
    scopeLabel: (scopes: string[]) => scopes.join('、') || '未配置',
  },
);

const emit = defineEmits<{
  select: [source: AccountLike];
  'update:mode': [mode: Mode];
}>();

const internalMode = ref<Mode>('list');
const currentMode = computed<Mode>(() => props.mode ?? internalMode.value);
function setMode(mode: Mode) {
  internalMode.value = mode;
  emit('update:mode', mode);
}

const tree = computed(() =>
  buildAccountHierarchy(props.accounts, {
    levelLabel: props.levelLabel,
    buildingLabel: props.buildingLabel,
  }),
);
const flat = computed<PersonTreeItem[]>(() => flattenHierarchyPeople(tree.value));
const collapsedLevels = ref<Set<string>>(new Set());
const collapsedBuildings = ref<Set<string>>(new Set());

function toggleLevel(key: string) {
  const next = new Set(collapsedLevels.value);
  next.has(key) ? next.delete(key) : next.add(key);
  collapsedLevels.value = next;
}
function toggleBuilding(key: string) {
  const next = new Set(collapsedBuildings.value);
  next.has(key) ? next.delete(key) : next.add(key);
  collapsedBuildings.value = next;
}
function select(person: PersonTreeItem) {
  emit('select', person.source);
}
</script>

<template>
  <div class="v2-account-hierarchy" data-v2-account-hierarchy>
    <div v-if="showToggle" class="v2-account-mode-switch" role="tablist" aria-label="账号视图切换">
      <button
        type="button"
        role="tab"
        :aria-selected="currentMode === 'list'"
        @click="setMode('list')"
      >列表</button>
      <button
        type="button"
        role="tab"
        :aria-selected="currentMode === 'tree'"
        @click="setMode('tree')"
      >树形分组</button>
    </div>

    <!-- Flat list -->
    <div v-if="currentMode === 'list'" class="v2-account-list" role="list" aria-label="账号列表">
      <div v-if="!flat.length" class="v2-account-empty">没有匹配的账号</div>
      <article v-for="person in flat" :key="person.key" class="v2-account-row" role="listitem">
        <div class="v2-account-row-main">
          <strong>{{ person.name }}</strong>
          <span v-if="person.meta" class="v2-account-row-meta">{{ person.meta }}</span>
          <small v-if="person.levelLabel" class="v2-account-row-level">{{ person.levelLabel }}</small>
        </div>
        <div class="v2-account-row-scopes">{{ scopeLabel([person.building]) }}</div>
        <div class="v2-account-row-actions">
          <button type="button" class="v2-account-manage" :aria-label="`管理${person.name}`" @click="select(person)">
            <span>管理</span>
          </button>
        </div>
      </article>
    </div>

    <!-- Accessible tree -->
    <div v-else class="v2-account-tree-wrap" :data-roots="tree.length">
      <p v-if="!tree.length" class="v2-account-empty" role="status">没有可展示的账号分组</p>
      <ul v-else class="v2-tree" role="tree" aria-label="按权限级别与楼宇分组的账号树">
        <li v-for="level in tree" :key="level.key" role="none">
          <button
            type="button"
            class="v2-tree-level"
            role="treeitem"
            :aria-expanded="!collapsedLevels.has(level.key)"
            :aria-level="1"
            @click="toggleLevel(level.key)"
          >
            <span class="v2-tree-caret" aria-hidden="true">▾</span>
            <span class="v2-tree-label">{{ level.label }}</span>
            <span class="v2-tree-count">{{ level.buildings.length }} 栋</span>
          </button>
          <ul v-if="!collapsedLevels.has(level.key)" role="group" class="v2-tree-children">
            <li v-for="building in level.buildings" :key="building.key" role="none">
              <button
                type="button"
                class="v2-tree-building"
                role="treeitem"
                :aria-expanded="!collapsedBuildings.has(building.key)"
                :aria-level="2"
                @click="toggleBuilding(building.key)"
              >
                <span class="v2-tree-caret" aria-hidden="true">▾</span>
                <span class="v2-tree-label">{{ building.label }}</span>
                <span class="v2-tree-count">{{ building.people.length }} 人</span>
              </button>
              <ul
                v-if="!collapsedBuildings.has(building.key)"
                role="group"
                class="v2-tree-children"
              >
                <li v-for="person in building.people" :key="person.key" role="none">
                  <button type="button" class="v2-tree-person" role="treeitem" :aria-level="3" @click="select(person)">
                    <span class="v2-tree-person-name">{{ person.name }}</span>
                    <span v-if="person.meta" class="v2-tree-person-meta">{{ person.meta }}</span>
                  </button>
                </li>
              </ul>
            </li>
          </ul>
        </li>
      </ul>
    </div>
  </div>
</template>
