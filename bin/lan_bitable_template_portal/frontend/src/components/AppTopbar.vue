<template>
  <header class="topbar">
    <div class="brand">
      <img class="brand-logo" :src="brandLogoSrc" alt="世纪互联官方标识" />
      <div>
        <h1>南通基地-运维灯塔工作台</h1>
        <p>{{ headerSubtitle }}</p>
      </div>
    </div>
    <div class="topbar-actions">
      <span v-if="auth.loggedIn" class="user-chip">
        {{ auth.user?.name || auth.user?.open_id || "已登录" }}
      </span>
      <button
        v-if="auth.loggedIn && (isWorkbench || isEngineerMopPage)"
        class="btn ghost"
        type="button"
        @click="emit('return-home')"
      >
        功能选择
      </button>
      <label v-if="auth.loggedIn && isWorkbench && visibleScopeOptions.length > 1" class="scope-switch">
        <span>切换楼栋</span>
        <select :value="currentScope" :disabled="loading" @change="onScopeChange">
          <option v-for="item in visibleScopeOptions" :key="item.value" :value="item.value">
            {{ item.label }}
          </option>
        </select>
      </label>
      <RefreshDataMenu
        v-if="auth.loggedIn && isWorkbench"
        :open="refreshMenuOpen"
        :loading="loading"
        :repair-refreshing="repairRefreshing"
        :change-refreshing="changeRefreshing"
        :cooldown-workbench="Boolean(refreshCooldown.workbench)"
        :cooldown-repair="Boolean(refreshCooldown.repair)"
        :cooldown-change="Boolean(refreshCooldown.change)"
        :workbench-title="workbenchRefreshTitle"
        :repair-title="repairRefreshTitle"
        :change-title="changeRefreshTitle"
        @update:open="emit('update:refreshMenuOpen', $event)"
        @refresh-workbench="emit('refresh-workbench')"
        @refresh-repair="emit('refresh-repair')"
        @refresh-change="emit('refresh-change')"
      />
      <button v-if="isAdmin" class="btn ghost" type="button" @click="emit('open-admin')">管理/诊断</button>
      <button v-if="auth.loggedIn" class="btn danger-text" type="button" @click="emit('logout')">退出</button>
    </div>
  </header>
</template>

<script setup lang="ts">
import RefreshDataMenu from "./RefreshDataMenu.vue";
import type { LooseDict, ScopeOption } from "../types";

const props = defineProps<{
  brandLogoSrc: string;
  headerSubtitle: string;
  auth: {
    loggedIn: boolean;
    user?: LooseDict;
  };
  isWorkbench: boolean;
  isEngineerMopPage: boolean;
  visibleScopeOptions: ScopeOption[];
  currentScope: string;
  loading: boolean;
  refreshMenuOpen: boolean;
  repairRefreshing: boolean;
  changeRefreshing: boolean;
  refreshCooldown: Record<string, boolean>;
  workbenchRefreshTitle: string;
  repairRefreshTitle: string;
  changeRefreshTitle: string;
  isAdmin: boolean;
}>();

const emit = defineEmits<{
  "return-home": [];
  "switch-scope": [value: string];
  "update:refreshMenuOpen": [value: boolean];
  "refresh-workbench": [];
  "refresh-repair": [];
  "refresh-change": [];
  "open-admin": [];
  logout: [];
}>();

function onScopeChange(event: Event): void {
  emit("switch-scope", (event.target as HTMLSelectElement).value);
}
</script>

