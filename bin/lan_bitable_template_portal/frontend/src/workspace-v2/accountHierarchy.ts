// Pure, framework-free account hierarchy builder used by the V2 account
// presentation. It only groups records that the existing controller already
// provided -- it never invents permissions, levels or scope values.
//
// Behaviour guarantees this module owns:
//   * same-identity dedup (same `id`, or same `open_id` when `id` differs)
//   * group by permission level (`level`) first, then building (`scopes`)
//   * scope-authorization: a person only appears under buildings that are both
//     present in their own `scopes` and allowed by the optional allowed-scope set.

export type AccountLike = {
  id: string;
  open_id?: string;
  name?: string;
  employee_no?: string;
  position?: string;
  category?: string;
  level?: string;
  role?: string;
  enabled?: boolean;
  scopes?: string[];
  [key: string]: unknown;
};

export type PersonTreeItem<T extends AccountLike = AccountLike> = {
  key: string;
  id: string;
  name: string;
  level: string;
  levelLabel: string;
  building: string;
  buildingLabel: string;
  meta: string;
  source: T;
};

export type BuildingNode<T extends AccountLike = AccountLike> = {
  key: string;
  type: 'building';
  label: string;
  value: string;
  people: PersonTreeItem<T>[];
};

export type LevelNode<T extends AccountLike = AccountLike> = {
  key: string;
  type: 'level';
  label: string;
  level: string;
  buildings: BuildingNode<T>[];
};

export type Hierarchy<T extends AccountLike = AccountLike> = LevelNode<T>[];

export type HierarchyOptions<T extends AccountLike = AccountLike> = {
  /** Display label for a permission level. Defaults to the raw level value. */
  levelLabel?: (level?: string) => string;
  /** Display label for a building/scope value. Defaults to the raw value. */
  buildingLabel?: (value: string) => string;
  /**
   * The authorized scope values for one account. Defaults to the account's
   * own `scopes` array. Returned values must already reflect the caller's
   * granted view; this module never widens them.
   */
  scopeValue?: (account: T) => string[];
  /**
   * Optional allow-list of scope values. When provided, a person is only shown
   * for buildings that are both in their own `scopes` and in this set.
   */
  allowedScopes?: string[];
  /**
   * Optional ordering rank for level values (lower rank sorts first).
   * Without it, levels keep their first-seen order.
   */
  levelRank?: (level?: string) => number;
  /** Group label for accounts that carry no level. Defaults to `未分级`.*/
  noLevelLabel?: string;
  /** Group label for the ''/ALL/CAMPUS global placeholder scopes. Defaults to `全域`.*/
  globalScopeLabel?: string;
};

const GLOBAL_SCOPE = new Set(['', 'ALL', 'CAMPUS', '*']);

/** Stable identity for a person: prefer `open_id`, fall back to `id`. */
export function personKey<T extends AccountLike>(account: T): string {
  return String(account.open_id || account.id || '');
}

/** Normalize one account into its authorized scope values. */
export function authorizedScopes<T extends AccountLike>(
  account: T,
  options: HierarchyOptions<T> = {},
): string[] {
  const raw = options.scopeValue
    ? options.scopeValue(account)
    : Array.isArray(account.scopes)
      ? (account.scopes as string[])
      : [];
  const allowed = options.allowedScopes
    ? new Set(options.allowedScopes)
    : null;
  const values = raw.map(String);
  const present = values.length ? values : account.open_id ? [''] : [];
  if (!allowed) return present;
  return present.filter(value => GLOBAL_SCOPE.has(value) || allowed.has(value));
}

/** Drop same-identity duplicates, keeping the first occurrence per identity. */
export function dedupeAccounts<T extends AccountLike>(
  accounts: readonly T[],
  keyFn: (account: T) => string = personKey,
): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const account of accounts) {
    const key = keyFn(account);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(account);
  }
  return out;
}

/**
 * Build the level → building → people tree. A person is included only if at
 * least one authorized scope is present (an empty scope value is treated as
 * a global placeholder node when the account has no concrete scopes).
 */
export function buildAccountHierarchy<T extends AccountLike>(
  accounts: readonly T[],
  options: HierarchyOptions<T> = {},
): Hierarchy<T> {
  const levelLabel = options.levelLabel ?? ((level?: string) => level || '未分级');
  const buildingLabel = options.buildingLabel ?? ((value: string) => value);
  const rank = options.levelRank;
  const noLevelLabel = options.noLevelLabel ?? '未分级';
  const globalScopeLabel = options.globalScopeLabel ?? '全域';

  const levelMap = new Map<string, LevelNode<T>>();
  const buildingMap = new Map<string, Map<string, BuildingNode<T>>>();

  const ensureLevel = (level: string) => {
    let node = levelMap.get(level);
    if (!node) {
      node = {
        key: 'level:' + level,
        type: 'level',
        label: levelLabel(level || undefined),
        level,
        buildings: [],
      };
      levelMap.set(level, node);
      buildingMap.set(level, new Map());
    }
    return node;
  };

  const ensureBuilding = (level: string, value: string) => {
    ensureLevel(level);
    const levelBuildings = buildingMap.get(level)!;
    let node = levelBuildings.get(value);
    if (!node) {
      node = {
        key: `level:${level}|building:${value}`,
        type: 'building',
        label: GLOBAL_SCOPE.has(value) ? globalScopeLabel : buildingLabel(value) || value,
        value,
        people: [],
      };
      levelBuildings.set(value, node);
      ensureLevel(level).buildings.push(node);
    }
    return node;
  };

  const seenPeople = new Set<string>();
  for (const account of dedupeAccounts<T>(accounts, personKey)) {
    const identity = personKey(account);
    const levelValue = String(account.level || '');
    const level = levelValue || noLevelLabel;
    const scopes = authorizedScopes<T>(account, options);
    if (!scopes.length) continue;

    if (seenPeople.has(identity)) continue;
    seenPeople.add(identity);

    const name = String(account.name || '未命名');
    for (const value of scopes) {
      const buildingNode = ensureBuilding(level, value);
      buildingNode.people.push({
        key: `${identity}@${value}`,
        id: String(account.id),
        name,
        level: levelValue,
        levelLabel: levelValue ? levelLabel(levelValue) : noLevelLabel,
        building: value,
        buildingLabel: GLOBAL_SCOPE.has(value) ? globalScopeLabel : buildingLabel(value) || value,
        meta: buildMeta(account),
        source: account,
      });
    }
  }

  const levels = Array.from(levelMap.values());
  if (rank) {
    levels.sort((a, b) => rank(a.level) - rank(b.level));
  }
  return levels;
}

function buildMeta<T extends AccountLike>(account: T): string {
  const bits: string[] = [];
  if (typeof account.role === 'string' && account.role === 'admin') bits.push('管理员');
  if (typeof account.employee_no === 'string' && account.employee_no) bits.push(account.employee_no);
  if (typeof account.position === 'string' && account.position) bits.push(account.position);
  if (typeof account.category === 'string' && account.category) bits.push(account.category);
  return bits.join(' · ');
}

/** Count of distinct people in the whole tree (dedup applied). */
export function hierarchyPersonCount<T extends AccountLike>(tree: Hierarchy<T>): number {
  const seen = new Set<string>();
  for (const level of tree) {
    for (const building of level.buildings) {
      for (const person of building.people) {
        seen.add(person.key.split('@')[0]);
      }
    }
  }
  return seen.size;
}

/** Flatten tree into distinct people (one row per identity, first building wins). */
export function flattenHierarchyPeople<T extends AccountLike>(tree: Hierarchy<T>): PersonTreeItem<T>[] {
  const seen = new Set<string>();
  const out: PersonTreeItem<T>[] = [];
  for (const level of tree) {
    for (const building of level.buildings) {
      for (const person of building.people) {
        const identity = person.key.split('@')[0];
        if (seen.has(identity)) continue;
        seen.add(identity);
        out.push(person);
      }
    }
  }
  return out;
}