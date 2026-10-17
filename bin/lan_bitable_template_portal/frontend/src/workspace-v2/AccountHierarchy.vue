<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NButton, NRadioButton, NRadioGroup, NTooltip, NTree, type TreeOption } from 'naive-ui';
import { BriefcaseBusiness, Building2, ChevronsDownUp, ChevronsUpDown, Eye, ShieldCheck, UserRound } from 'lucide-vue-next';
import NativeSingleSelectV2 from './NativeSingleSelect.vue';
import {
  buildAccountHierarchy, buildAccountManagementTree, flattenHierarchyPeople,
  type AccountLike, type AccountGroupBy, type AccountGroupDepth, type AccountTreeNode,
} from './accountHierarchy';
import './accountPresentation.css';

type Mode = 'list' | 'tree';
const props = withDefaults(defineProps<{
  accounts: AccountLike[];
  directory?: AccountLike[];
  mode?: Mode;
  showToggle?: boolean;
  levelLabel?: (level?: string) => string;
  buildingLabel?: (value: string) => string;
  categoryLabel?: (value: string) => string;
  scopeLabel?: (scopes: string[]) => string;
}>(), {
  showToggle: true,
  levelLabel: (level?: string) => level || '未分级',
  buildingLabel: (value: string) => value === '110' ? '110站' : value + '楼',
  scopeLabel: (scopes: string[]) => scopes.join('、') || '未配置',
});
const emit = defineEmits<{select: [source: AccountLike]; 'update:mode': [mode: Mode]}>();
const internalMode = ref<Mode>('list');
const currentMode = computed(() => props.mode ?? internalMode.value);
function setMode(mode: Mode) { internalMode.value = mode; emit('update:mode', mode); }
const groupBy = ref<AccountGroupBy>('building'), depth = ref<AccountGroupDepth>(3);
const loadedPeople = ref(new Set<string>()), narrow = ref(false);
let media: MediaQueryList | undefined;
function updateWidth() { narrow.value = !!media?.matches; }
onMounted(() => { media = window.matchMedia('(max-width: 760px)'); updateWidth(); media.addEventListener('change', updateWidth); });
onBeforeUnmount(() => media?.removeEventListener('change', updateWidth));
const depthOptions = computed(() => {
  const order = groupBy.value === 'building' ? ['楼栋', '级别', '岗位'] : ['级别', '楼栋', '岗位'];
  return [1, 2, 3].map(value => ({value, label: [...order.slice(0, value), '人员'].join(' → ')}));
});
const tree = computed(() => buildAccountManagementTree(props.accounts, props.directory ?? props.accounts, {
  groupBy: groupBy.value, depth: depth.value, levelLabel: props.levelLabel,
  buildingLabel: props.buildingLabel, categoryLabel: props.categoryLabel, loadedPeople: loadedPeople.value,
}));
const flat = computed(() => flattenHierarchyPeople(buildAccountHierarchy(props.accounts, {
  levelLabel: props.levelLabel, buildingLabel: props.buildingLabel,
})));
const branches = computed(() => {
  const result: {key: string; depth: number; loaded: boolean}[] = [];
  function walk(nodes: AccountTreeNode[]) {
    for (const node of nodes) if (node.type !== 'person' || node.managementCount) {
      result.push({key: node.key, depth: node.depth, loaded: node.type !== 'person' || !!node.children});
      walk(node.children || []);
    }
  }
  walk(tree.value);
  return result;
});
const expandedKeys = ref<(string | number)[]>([]);
let lastGrouping = '', hadRows = false;
watch(tree, nodes => {
  const grouping = groupBy.value + ':' + depth.value;
  const valid = new Set(branches.value.map(node => node.key));
  expandedKeys.value = lastGrouping !== grouping || !hadRows
    ? branches.value.filter(node => node.loaded && node.depth <= 2).map(node => node.key)
    : expandedKeys.value.filter(key => valid.has(String(key)));
  lastGrouping = grouping;
  hadRows = nodes.length > 0;
  const validPeople = new Set(branches.value.filter(node => loadedPeople.value.has(node.key)).map(node => node.key));
  if (validPeople.size !== loadedPeople.value.size) loadedPeople.value = validPeople;
}, {immediate: true});
function loadPeople(option: TreeOption): Promise<void> {
  if (option.type === 'person' && option.managementCount) loadedPeople.value = new Set([...loadedPeople.value, String(option.key)]);
  return Promise.resolve();
}
function select(_keys: (string | number)[], options: (TreeOption | null)[]) {
  const node = options[0] as AccountTreeNode | null;
  if (node?.type === 'person') emit('select', node.source);
}
function nodeProps({option}: {option: TreeOption}) {
  const node = option as AccountTreeNode;
  return node.type === 'person'
    ? {role: 'treeitem', 'aria-level': node.depth, 'aria-expanded': node.managementCount ? expandedKeys.value.includes(node.key) : undefined,
      'data-account-person': node.id, 'data-management-depth': node.managementDepth,
      'aria-label': node.managementCount ? node.name + '，管理范围' + node.managementCount + '人' : '查看账号：' + node.name}
    : {role: 'treeitem', 'aria-level': node.depth, 'aria-expanded': expandedKeys.value.includes(node.key), 'data-account-group': node.type, 'data-group-depth': node.depth, 'aria-label': node.label + '，' + node.count + '人'};
}
function renderPrefix({option}: {option: TreeOption}) {
  const icon = {building: Building2, level: ShieldCheck, category: BriefcaseBusiness, person: UserRound}[option.type as AccountTreeNode['type']];
  return h(icon, {size: 16, 'aria-hidden': true});
}
function renderLabel({option}: {option: TreeOption}) {
  const node = option as AccountTreeNode;
  if (node.type !== 'person') return h('span', {class: 'v2-tree-group-label'}, [
    h('span', {class: 'v2-tree-label'}, node.label), h('span', {class: 'v2-tree-count'}, node.count + ' 人'),
  ]);
  const meta = [node.source.employee_no, node.source.position || props.categoryLabel?.(String(node.source.category || 'other')),
    depth.value === 1 && groupBy.value === 'building' ? node.levelLabel : ''].filter(Boolean).join(' · ');
  return h('span', {class: 'v2-tree-person'}, [
    h('span', {class: 'v2-tree-person-main'}, [
      h('strong', {class: 'v2-tree-person-name'}, node.name),
      meta ? h('span', {class: 'v2-tree-person-meta', title: meta}, meta) : null,
      node.source.enabled === false ? h('small', {class: 'v2-tree-person-state'}, '已停用') : null,
    ]),
    node.managementCount ? h('span', {class: 'v2-tree-count', title: '当前可见管理范围'}, node.managementCount + ' 人') : null,
    h(NButton, {quaternary: true, circle: true, size: 'small', class: 'v2-tree-person-detail',
      'aria-label': '查看' + node.name + '的详细信息',
      title: '查看' + node.name + '的详细信息',
      onKeydown: (event: KeyboardEvent) => { if (event.key === 'Enter' || event.key === ' ') event.stopPropagation(); },
      onClick: (event: MouseEvent) => { event.stopPropagation(); emit('select', node.source); }},
      {default: () => h(Eye, {size: 16, 'aria-hidden': true})}),
  ]);
}
</script>

<template>
  <div class="v2-account-hierarchy" data-v2-account-hierarchy>
    <div v-if="showToggle" class="v2-account-mode-switch" role="tablist" aria-label="账号视图切换" data-v2-navigation>
      <button type="button" role="tab" :aria-selected="currentMode === 'list'" @click="setMode('list')">列表</button>
      <button type="button" role="tab" :aria-selected="currentMode === 'tree'" @click="setMode('tree')">树形分组</button>
    </div>
    <div v-if="currentMode === 'list'" class="v2-account-list" role="list" aria-label="账号列表">
      <div v-if="!flat.length" class="v2-account-empty">没有匹配的账号</div>
      <article v-for="person in flat" :key="person.key" class="v2-account-row" role="listitem">
        <div class="v2-account-row-main"><strong>{{person.name}}</strong><span v-if="person.meta" class="v2-account-row-meta">{{person.meta}}</span><small class="v2-account-row-level">{{person.levelLabel}}</small></div>
        <div class="v2-account-row-scopes">{{scopeLabel(person.source.scopes || [])}}</div>
        <div class="v2-account-row-actions"><button type="button" class="v2-account-manage" :aria-label="'管理'+person.name" @click="emit('select', person.source)">管理</button></div>
      </article>
    </div>
    <template v-else>
      <div class="v2-account-tree-toolbar" data-v2-navigation>
        <NRadioGroup v-model:value="groupBy" name="account-grouping" size="small" aria-label="账号分组方式">
          <NRadioButton value="building">按楼栋</NRadioButton><NRadioButton value="level">按级别</NRadioButton>
        </NRadioGroup>
        <NativeSingleSelectV2 class="v2-account-tree-depth"><select v-model.number="depth" aria-label="分组层级"><option v-for="option in depthOptions" :key="option.value" :value="option.value">{{option.label}}</option></select></NativeSingleSelectV2>
        <div class="v2-account-tree-actions">
          <NTooltip><template #trigger><NButton quaternary circle size="small" aria-label="展开全部" :disabled="!tree.length" @click="expandedKeys=branches.filter(node=>node.loaded).map(node=>node.key)"><ChevronsUpDown :size="17" /></NButton></template>展开已加载分组</NTooltip>
          <NTooltip><template #trigger><NButton quaternary circle size="small" aria-label="收起全部" :disabled="!tree.length" @click="expandedKeys=[]"><ChevronsDownUp :size="17" /></NButton></template>收起全部</NTooltip>
        </div>
      </div>
      <div class="v2-account-tree-wrap" :data-roots="tree.length" :data-group-by="groupBy" :data-group-depth="depth">
        <p v-if="!tree.length" class="v2-account-empty" role="status">没有可展示的账号分组</p>
        <NTree v-else v-model:expanded-keys="expandedKeys" class="v2-account-native-tree" :data="tree" :selected-keys="[]" block-line show-line :indent="narrow ? 8 : 18" :node-props="nodeProps" :render-label="renderLabel" :render-prefix="renderPrefix" :on-load="loadPeople" :override-default-node-click-behavior="({option})=>option.type==='person'&&!option.managementCount?'toggleSelect':'toggleExpand'" role="tree" aria-label="账号多层分组树" @update:selected-keys="select" />
      </div>
    </template>
  </div>
</template>
