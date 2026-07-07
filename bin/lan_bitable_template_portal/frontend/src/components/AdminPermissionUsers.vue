<template>
  <section class="permission-user-tools">
    <div>
      <strong>已授权用户</strong>
      <span>共 {{ total }} 人，当前显示 {{ users.length }} 人</span>
    </div>
    <label class="permission-user-search">
      <span>搜索用户</span>
      <input
        :value="search"
        placeholder="姓名、openid、楼栋"
        @input="$emit('update:search', ($event.target as HTMLInputElement).value)"
      />
    </label>
    <label class="permission-user-filter">
      <span>筛选</span>
      <select
        :value="filter"
        @change="$emit('update:filter', ($event.target as HTMLSelectElement).value)"
      >
        <option value="all">全部用户</option>
        <option value="admin">管理员</option>
        <option value="building">普通用户</option>
        <option value="disabled">已禁用</option>
        <option value="locked">固定管理员</option>
      </select>
    </label>
  </section>

  <div class="permission-list">
    <div v-if="!users.length" class="permission-empty">
      没有符合当前筛选条件的用户。
    </div>
    <article
      v-for="(user, index) in users"
      :key="String(user.open_id || user.name || index)"
      class="permission-row"
      :class="{ locked: user.locked, disabled: user.enabled === false }"
    >
      <div class="permission-row-head">
        <div>
          <strong>{{ user.name || "未命名用户" }}</strong>
          <span>{{ user.open_id || "未填写 openid" }}</span>
        </div>
        <div class="permission-badges">
          <b v-if="user.locked" class="locked">固定管理员</b>
          <b v-else-if="String(user.role || 'building') === 'admin'" class="admin">管理员</b>
          <b v-else class="user">普通用户</b>
          <b :class="user.enabled === false ? 'disabled' : 'enabled'">{{ user.enabled === false ? "已禁用" : "已启用" }}</b>
        </div>
      </div>
      <label class="permission-field">
        <span>姓名</span>
        <input v-model="user.name" placeholder="姓名" :disabled="Boolean(user.locked)" />
      </label>
      <label class="permission-field openid-field">
        <span>openid</span>
        <input v-model="user.open_id" placeholder="openid" :disabled="Boolean(user.locked)" />
      </label>
      <label class="permission-field role-field">
        <span>角色</span>
        <select v-model="user.role" :disabled="Boolean(user.locked)">
          <option value="building">用户</option>
          <option value="admin">管理员</option>
        </select>
      </label>
      <label class="permission-enable">
        <input v-model="user.enabled" type="checkbox" :disabled="Boolean(user.locked)" />
        启用
      </label>
      <button type="button" class="btn danger" :disabled="Boolean(user.locked)" @click="$emit('remove', user)">
        删除
      </button>
      <details class="scope-checks-wrap">
        <summary>
          <span>楼栋权限</span>
          <b>{{ scopeSummary(user) }}</b>
        </summary>
        <div class="scope-checks">
          <label v-for="scope in scopeOptions" :key="scope.value">
            <input
              type="checkbox"
              :checked="Array.isArray(user.scopes) && user.scopes.includes(scope.value)"
              :disabled="Boolean(user.locked) || user.role === 'admin'"
              @change="$emit('toggle-scope', user, scope.value, ($event.target as HTMLInputElement).checked)"
            />
            {{ scope.label }}
          </label>
        </div>
      </details>
    </article>
  </div>
</template>

<script setup lang="ts">
import type { LooseDict, ScopeOption } from "../types";

defineProps<{
  users: LooseDict[];
  total: number;
  search: string;
  filter: string;
  scopeOptions: ScopeOption[];
  scopeSummary: (user: LooseDict) => string;
}>();

defineEmits<{
  "update:search": [value: string];
  "update:filter": [value: string];
  "toggle-scope": [user: LooseDict, scope: string, checked: boolean];
  remove: [user: LooseDict];
}>();
</script>

<style scoped>
.permission-user-tools {
  display: grid;
  grid-template-columns: minmax(180px, 1fr) minmax(220px, 1.2fr) minmax(160px, auto);
  align-items: end;
  gap: 9px;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 18px;
  padding: 10px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.9), rgba(255, 255, 255, 0.94)),
    #ffffff;
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.permission-user-tools > div {
  display: grid;
  gap: 4px;
}

.permission-user-tools strong {
  color: #071a39;
  font-size: 15px;
  font-weight: 950;
}

.permission-user-tools span {
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.permission-user-search,
.permission-user-filter {
  display: grid;
  gap: 6px;
}

.permission-user-search input,
.permission-user-filter select {
  min-height: 34px;
  border-radius: 999px;
}

.permission-list {
  display: grid;
  gap: 8px;
  max-height: min(54vh, 620px);
  overflow: auto;
  padding-right: 4px;
  overscroll-behavior: contain;
}

.permission-empty {
  border: 1px dashed #cfe0ff;
  border-radius: 18px;
  padding: 16px;
  background: #f8fbff;
  color: #48627f;
  font-size: 13px;
  font-weight: 850;
  text-align: center;
}

.permission-row {
  display: grid;
  grid-template-columns: minmax(140px, 0.75fr) minmax(200px, 1fr) 104px 82px auto;
  align-items: stretch;
  gap: 6px;
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  padding: 8px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background-color 0.14s ease;
}

.permission-row:hover {
  border-color: #a8c9fb;
  box-shadow: 0 14px 32px rgba(22, 78, 151, 0.11);
}

.permission-row.locked {
  border-color: #cfe0ff;
  background:
    linear-gradient(180deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.permission-row.disabled {
  opacity: 0.72;
}

.permission-row-head {
  grid-column: 1 / -1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border-bottom: 1px solid rgba(216, 229, 247, 0.75);
  padding: 0 1px 7px;
}

.permission-row-head > div:first-child {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.permission-row-head strong {
  overflow: hidden;
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.permission-row-head span {
  overflow: hidden;
  color: #64748b;
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
  font-size: 11px;
  font-weight: 760;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.permission-badges {
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px;
}

.permission-badges b {
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 4px 8px;
  background: #ffffff;
  color: #48627f;
  font-size: 11px;
  font-weight: 950;
  line-height: 1;
  white-space: nowrap;
}

.permission-badges b.admin,
.permission-badges b.locked {
  border-color: #bfdbfe;
  background: #eff6ff;
  color: #075bd8;
}

.permission-badges b.enabled {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.permission-badges b.disabled {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}

.permission-row input,
.permission-row select {
  width: 100%;
  min-width: 0;
  min-height: 34px;
  border: 1px solid #c8dcf3;
  border-radius: 999px;
  padding: 6px 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

.permission-row input:focus,
.permission-row select:focus,
.permission-user-search input:focus,
.permission-user-filter select:focus {
  outline: none;
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.permission-row > .permission-field {
  min-height: auto;
  display: grid;
  align-items: stretch;
  gap: 4px;
  color: #37536f;
  font-size: 13px;
  font-weight: 850;
}

.permission-row > .permission-field span {
  color: #64748b;
  font-size: 11px;
  font-weight: 950;
  line-height: 1.1;
}

.permission-row > .permission-field.openid-field input {
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}

.permission-row > .permission-enable {
  display: flex;
  align-items: center;
  justify-content: center;
  align-self: end;
  gap: 6px;
  min-height: 34px;
  color: #475569;
  font-size: 13px;
  white-space: nowrap;
}

.permission-row > .permission-enable input {
  width: auto;
  min-height: 0;
}

.permission-row > .btn.danger {
  align-self: end;
  min-height: 34px;
  border-color: transparent;
  border-radius: 999px;
  padding: 7px 12px;
  background: linear-gradient(135deg, #dc2626, #f05656);
  color: #ffffff;
  font-size: 13px;
}

.scope-checks-wrap {
  grid-column: 1 / -1;
}

.scope-checks-wrap summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  min-height: 34px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 7px 10px;
  background: #f8fbff;
  color: #23486f;
  cursor: pointer;
  font-size: 12px;
  font-weight: 900;
}

.scope-checks-wrap summary b {
  color: #075bd8;
}

.scope-checks {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  max-height: 104px;
  overflow: auto;
  padding: 9px;
  overscroll-behavior: contain;
}

.scope-checks label {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-height: 28px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 4px 9px;
  background: #ffffff;
  color: #31506f;
  font-size: 12px;
  font-weight: 850;
}

.scope-checks input {
  width: auto;
  min-height: 0;
}

.scope-checks label:has(input:checked) {
  border-color: #9dc3ff;
  background: #eff6ff;
  color: #075bd8;
}

.scope-checks label:has(input:disabled) {
  opacity: 0.64;
}

@media (max-width: 820px) {
  .permission-list {
    max-height: none;
  }

  .permission-user-tools {
    grid-template-columns: 1fr;
  }

  .permission-row {
    grid-template-columns: 1fr;
  }

  .permission-row-head {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
