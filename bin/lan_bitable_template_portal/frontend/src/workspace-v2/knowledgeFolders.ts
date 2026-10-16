// Folder-tree helpers shared by the V2 Knowledge folders tree.  Pure functions:
// no HTTP, no component state.  The KnowledgeBasePage still owns the authoritative
// `groups`/`groupFilter` refs and the group CRUD workflow; this module only flattens
// the flat group list (with an optional parent_id) into NTree-compatible nodes and
// helps with moving/selection bookkeeping.

export interface GroupItem {
  id: string;
  name: string;
  owner_name: string;
  count: number;
  can_manage: boolean;
  created_at: unknown;
  parent_id?: string | null;
}

/** A folder node in a nested tree (children optional). */
export interface FolderNode extends GroupItem {
  children?: FolderNode[];
  /** stable index used by the tree to identify the node */
  key: string;
  /** direct file count shown for this folder */
  count: number;
}

const ROOT_PARENTS = new Set(['', null, undefined]);

/** Normalize an optional parent id from the API into the tree's notion of root. */
export function isRoot(parentId: unknown): boolean {
  if (parentId == null) return true;
  const text = String(parentId).trim().toLowerCase();
  return ROOT_PARENTS.has(text);
}

/**
 * Build a stable nested folder structure from the flat group list.
 * Every group keeps its original `id`; parents that existed in the list are
 * linked by `parent_id`.  Groups whose parent is missing are treated as roots
 * (background data drift never crashes the UI).
 */
export function buildFolderTree(groups: GroupItem[]): FolderNode[] {
  const byId = new Map<string, FolderNode>();
  for (const g of groups) {
    byId.set(g.id, { ...g, key: g.id, children: [] });
  }
  const roots: FolderNode[] = [];
  for (const node of byId.values()) {
    const parentId = node.parent_id;
    const parent = parentId != null && !isRoot(parentId) ? byId.get(String(parentId)) : undefined;
    const chain = new Set([node.id]);
    let ancestor=parent,cycle=false;
    while(ancestor){if(chain.has(ancestor.id)){cycle=true;break;}chain.add(ancestor.id);ancestor=byId.get(String(ancestor.parent_id||''));}
    if (parent && !cycle) {
      parent.children!.push(node);
    } else {
      roots.push(node);
    }
  }
  // Rebuild roots deterministically (stable API order preserved by caller's list).
  const result = roots.slice().sort((a, b) => humanSort(a.name, b.name));
  return result;
}

function humanSort(a: string, b: string): number {
  return a.localeCompare(b, 'zh-Hans-CN', { numeric: true, sensitivity: 'base' });
}

/**
 * NTree `TreeOption` shape: stable key + label + children plus our folder fields.
 * The index signature matches Naive UI's `TreeOption = TreeOptionBase & Record<string, unknown>`
 * so `FolderTreeOption[]` is directly assignable to `TreeOption[]`.
 */
export interface FolderTreeOption {
  key: string;
  label: string;
  count: number;
  canManage: boolean;
  children?: FolderTreeOption[];
  data: FolderNode;
  [key: string]: unknown;
}

export function toTreeOptions(nodes: FolderNode[]): FolderTreeOption[] {
  return nodes.map((node) => ({
    key: node.key,
    label: node.name,
    count: node.count,
    canManage: node.can_manage,
    data: node,
    children: node.children && node.children.length ? toTreeOptions(node.children) : undefined,
  }));
}

