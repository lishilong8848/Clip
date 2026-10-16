<template>
  <div class="kb-groups-panel v2-kb-folders" role="group" aria-label="共享知识库文件夹">
    <div class="kb-groups-create">
      <NInput
        v-model:value="newName"
        aria-label="新根文件夹名称"
        placeholder="新根文件夹名称（≤50字）"
        maxlength="50"
        :disabled="busy"
        @keyup.enter="createRoot"
      />
      <NButton type="primary" size="small" :disabled="!newName.trim() || busy" @click="createRoot">
        <FolderPlus :size="15" />新建根文件夹
      </NButton>
    </div>

    <div v-if="treeOptions.length" class="v2-kb-folders-tree">
      <NTree
        :data="treeOptions"
        block-line
        :default-expand-all="true"
        :selectable="false"
        :selected-keys="selectedKeys"
        :render-label="renderLabel"
      />
    </div>
    <p v-else class="muted kb-groups-empty">
      暂无文件夹
    </p>

    <NModal
      v-if="modal.kind === 'child'"
      preset="dialog"
      title="新建子文件夹"
      aria-label="新建子文件夹"
      :show="true"
      positive-text="创建"
      negative-text="取消"
      :loading="busy"
      :mask-closable="!busy"
      :close-on-esc="!busy"
      @update:show="value=>{if(!value)closeModal()}"
      @positive-click="confirmCreateChild"
      @negative-click="closeModal"
    >
      <p class="v2-kb-folders-modal-hint">在「{{ currentName }}」下新建子文件夹：</p>
      <input
        v-model="childName"
        class="v2-kb-folders-input"
        aria-label="子文件夹名称"
        placeholder="子文件夹名称（≤50字）"
        maxlength="50"
        @keyup.enter="confirmCreateChild"
      />
    </NModal>

    <NModal
      v-if="modal.kind === 'rename'"
      preset="dialog"
      title="重命名文件夹"
      aria-label="重命名文件夹"
      :show="true"
      positive-text="保存"
      negative-text="取消"
      :loading="busy"
      :mask-closable="!busy"
      :close-on-esc="!busy"
      @update:show="value=>{if(!value)closeModal()}"
      @positive-click="confirmRename"
      @negative-click="closeModal"
    >
      <input
        v-model="renameValue"
        class="v2-kb-folders-input"
        aria-label="文件夹名称"
        placeholder="文件夹名称（≤50字）"
        maxlength="50"
        @keyup.enter="confirmRename"
      />
    </NModal>

    <NModal
      v-if="modal.kind === 'move'"
      preset="dialog"
      title="移动文件夹"
      aria-label="移动文件夹"
      :show="true"
      positive-text="移动"
      negative-text="取消"
      :loading="busy"
      :mask-closable="!busy"
      :close-on-esc="!busy"
      @update:show="value=>{if(!value)closeModal()}"
      @positive-click="confirmMove"
      @negative-click="closeModal"
    >
      <p class="v2-kb-folders-modal-hint">将「{{ currentName }}」移动到哪个文件夹？</p>
      <NSelect
        v-model:value="moveParentId"
        class="v2-kb-folders-select"
        :options="moveOptions"
        placeholder="选择目标父文件夹"
        aria-label="目标父文件夹"
        filterable
      />
    </NModal>
  </div>
</template>

<script setup lang="ts">
import { computed, h, reactive, ref } from "vue";
import { NModal, NTree, NSelect, NInput, NButton, type SelectOption, type TreeOption } from "naive-ui";
import { Folder, FolderPlus, Move, Pencil, Trash2 } from "lucide-vue-next";
import { buildFolderTree, toTreeOptions, type FolderTreeOption, type GroupItem } from "./knowledgeFolders";

const props = defineProps<{
  groups: GroupItem[];
  busy: boolean;
  selectedId?: string;
}>();
const emit = defineEmits<{
  (e: "select", id: string): void;
  (e: "create", name: string, parentId?: string): void;
  (e: "rename", id: string, name: string, parentId?: string): void;
  (e: "delete", group: GroupItem): void;
}>();

const newName = ref("");
const childName = ref("");
const renameValue = ref("");

/** Simple explicit modal shape: the active dialog kind and the folder it acts on. */
interface ModalState {
  kind: "none" | "child" | "rename" | "move";
  node: FolderTreeOption | null;
}

const modal = reactive<ModalState>({ kind: "none", node: null });
const moveParentId = ref<string | null>(null);

const tree = computed(() => buildFolderTree(props.groups));
const treeOptions = computed(() => toTreeOptions(tree.value));
const selectedKeys = computed(() => (props.selectedId && props.selectedId !== "all" && props.selectedId !== "ungrouped" ? [props.selectedId] : []));

const currentName = computed(()=>modal.node?.label||'');

/** Validation helper shared by NModal positive clicks (kept for @keyup handlers). */
function validName(value: string): string {
  return value.trim();
}

function createRoot(): void {
  const name = validName(newName.value);
  if (!name || props.busy) return;
  emit("create", name);
  newName.value = "";
}

function createChild(): void {
  const name = validName(childName.value);
  if (!name || props.busy || modal.kind !== "child" || !modal.node) return;
  emit("create", name, modal.node.key);
  childName.value = "";
  closeModal();
}

function confirmCreateChild(): boolean {
  const name = validName(childName.value);
  if (!name || props.busy) return false;
  createChild();
  return true;
}

function beginChild(node: FolderTreeOption): void {
  childName.value = "";
  modal.kind = "child";
  modal.node = node;
}

function beginRename(node: FolderTreeOption): void {
  renameValue.value = node.label;
  modal.kind = "rename";
  modal.node = node;
}

function confirmRename(): boolean {
  const name = validName(renameValue.value);
  if (!name || props.busy || modal.kind !== "rename" || !modal.node) return false;
  emit("rename", modal.node.key, name, currentParent(modal.node.key));
  closeModal();
  return true;
}

function currentParent(key: string): string | undefined {
  const target = props.groups.find((g) => g.id === key);
  return target?.parent_id || undefined;
}

function beginMove(node: FolderTreeOption): void {
  modal.kind = "move";
  modal.node = node;
  moveParentId.value = node.data.parent_id || null;
}

const moveOptions = computed<SelectOption[]>(() => {
  if (modal.kind !== "move" || !modal.node) return [];
  const forbidden = new Set<string>([modal.node.key]);
  const stack = [...(modal.node.children || [])];
  while (stack.length) {
    const item = stack.pop()!;
    forbidden.add(item.key);
    if (item.children) stack.push(...item.children);
  }
  const walk = (options: FolderTreeOption[],path=''): SelectOption[] => options.flatMap(option=>{
    if(forbidden.has(option.key))return [];
    const label=path?path+' / '+option.label:option.label;
    return [{label,value:option.key},...walk(option.children||[],label)];
  });
  return [{label:'根目录',value:''},...walk(treeOptions.value)];
});

function confirmMove(): boolean {
  if (props.busy || modal.kind !== "move" || !modal.node) return false;
  const parent: string | undefined = moveParentId.value || undefined;
  emit("rename", modal.node.key, modal.node.label, parent);
  closeModal();
  return true;
}

function requestDelete(node: FolderTreeOption): void {
  const group = props.groups.find((g) => g.id === node.key);
  if (group) emit("delete", group);
}

function selectOption(option: FolderTreeOption): void {
  emit("select", option.key);
}

function closeModal(): void {
  modal.kind = "none";
  modal.node = null;
  moveParentId.value = null;
  childName.value = "";
  renameValue.value = "";
}

/**
 * NTree label renderer: folder icon, clickable name+count, and management actions.
 * Naive UI calls the renderer with `TreeRenderProps {option, checked, selected}`;
 * `option` is our `FolderTreeOption` at runtime (identical union member fields).
 */
function renderLabel(props: { option: TreeOption; checked: boolean; selected: boolean }): ReturnType<typeof h> {
  const option = props.option as FolderTreeOption;
  return h("span", { class: "v2-folder-node" }, [
    h(Folder, { size: 14, "aria-hidden": true, class: "v2-folder-icon" }),
    h("button", { class: "v2-folder-name", type: "button", title: `查看「${option.label}」`, onClick: () => selectOption(option) }, [
      h("span", option.label),
      option.count > 0 ? h("em", { class: "v2-folder-count" }, option.count) : null,
    ]),
    option.canManage
      ? h("span", { class: "v2-folder-actions" }, [
          h("button", { class: "icon-button", type: "button", title: "新建子文件夹", "aria-label": `在「${option.label}」下新建子文件夹`, onClick: () => beginChild(option) }, [h(FolderPlus, { size: 14 })]),
          h("button", { class: "icon-button", type: "button", title: "重命名", "aria-label": `重命名「${option.label}」`, onClick: () => beginRename(option) }, [h(Pencil, { size: 14 })]),
          h("button", { class: "icon-button", type: "button", title: "移动", "aria-label": `移动「${option.label}」`, onClick: () => beginMove(option) }, [h(Move, { size: 14 })]),
          h("button", { class: "icon-button danger", type: "button", title: "删除", "aria-label": `删除「${option.label}」`, onClick: () => requestDelete(option) }, [h(Trash2, { size: 14 })]),
        ])
      : h("span", { class: "muted v2-folder-readonly" }, "只读"),
  ]);
}
</script>
