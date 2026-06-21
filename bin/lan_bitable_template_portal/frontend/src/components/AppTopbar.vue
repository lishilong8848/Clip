<template>
  <header class="app-topbar">
    <div class="brand">
      <img class="brand-logo" :src="brandLogoSrc" alt="世纪互联官方标识" />
      <div>
        <h1>南通基地-运维灯塔工作台</h1>
        <p>{{ headerSubtitle }}</p>
      </div>
    </div>
    <div class="topbar-actions">
      <span v-if="auth.loggedIn" class="user-chip" :title="auth.user?.open_id ? `飞书身份：${auth.user.open_id}` : ''">
        {{ auth.user?.name || "已登录" }}
      </span>
      <button
        v-if="auth.loggedIn && (isWorkbench || isEngineerMopPage || isEventPage)"
        class="btn ghost"
        type="button"
        title="返回功能选择页"
        @click="emit('return-home')"
      >
        功能选择
      </button>
      <label v-if="auth.loggedIn && (isWorkbench || isEventPage) && visibleScopeOptions.length > 1" class="scope-switch">
        <span>切换楼栋</span>
        <select :value="currentScope" :disabled="loading" @change="onScopeChange">
          <option v-for="item in visibleScopeOptions" :key="item.value" :value="item.value">
            {{ item.label }}
          </option>
        </select>
      </label>
      <RefreshDataMenu
        v-if="auth.loggedIn && (isWorkbench || isEventPage)"
        :open="refreshMenuOpen"
        :loading="loading"
        :repair-refreshing="repairRefreshing"
        :change-refreshing="changeRefreshing"
        :event-refreshing="eventRefreshing"
        :event-mode="isEventPage"
        :cooldown-workbench="Boolean(refreshCooldown.workbench)"
        :cooldown-repair="Boolean(refreshCooldown.repair)"
        :cooldown-change="Boolean(refreshCooldown.change)"
        :cooldown-event="Boolean(refreshCooldown.event)"
        :workbench-title="workbenchRefreshTitle"
        :repair-title="repairRefreshTitle"
        :change-title="changeRefreshTitle"
        :event-title="eventRefreshTitle"
        @update:open="emit('update:refreshMenuOpen', $event)"
        @refresh-workbench="emit('refresh-workbench')"
        @refresh-repair="emit('refresh-repair')"
        @refresh-change="emit('refresh-change')"
        @refresh-event="emit('refresh-event')"
      />
      <button v-if="isAdmin" class="btn ghost admin-entry" type="button" title="打开管理员诊断和权限管理" @click="emit('open-admin')">管理/诊断</button>
      <button v-if="auth.loggedIn" class="btn danger-text" type="button" title="退出当前飞书登录" @click="emit('logout')">退出</button>
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
  isEventPage: boolean;
  visibleScopeOptions: ScopeOption[];
  currentScope: string;
  loading: boolean;
  refreshMenuOpen: boolean;
  repairRefreshing: boolean;
  changeRefreshing: boolean;
  eventRefreshing: boolean;
  refreshCooldown: Record<string, boolean>;
  workbenchRefreshTitle: string;
  repairRefreshTitle: string;
  changeRefreshTitle: string;
  eventRefreshTitle: string;
  isAdmin: boolean;
}>();

const emit = defineEmits<{
  "return-home": [];
  "switch-scope": [value: string];
  "update:refreshMenuOpen": [value: boolean];
  "refresh-workbench": [];
  "refresh-repair": [];
  "refresh-change": [];
  "refresh-event": [];
  "open-admin": [];
  logout: [];
}>();

function onScopeChange(event: Event): void {
  emit("switch-scope", (event.target as HTMLSelectElement).value);
}
</script>

<style scoped>
header.app-topbar {
  position: relative;
  z-index: var(--cf-z-topbar, 40);
  min-height: 112px;
  display: flex;
  align-items: center;
  gap: 28px;
  padding: 20px 40px;
  overflow: visible;
  border-bottom: 1px solid rgba(191, 219, 254, 0.24);
  background:
    linear-gradient(115deg, #064fc5 0%, #00359b 52%, #012a7d 100%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.16), transparent);
  box-shadow: 0 18px 42px rgba(0, 47, 135, 0.28);
  isolation: isolate;
}

header.app-topbar::before {
  content: "";
  position: absolute;
  inset: 0;
  z-index: -2;
  opacity: 0.34;
  background:
    linear-gradient(90deg, transparent 0 18%, rgba(255, 255, 255, 0.16) 18.05% 18.14%, transparent 18.2%),
    linear-gradient(135deg, transparent 0 58%, rgba(255, 255, 255, 0.14) 58.1% 58.25%, transparent 58.35%),
    repeating-linear-gradient(90deg, rgba(255, 255, 255, 0.07) 0 1px, transparent 1px 62px),
    repeating-linear-gradient(0deg, rgba(255, 255, 255, 0.05) 0 1px, transparent 1px 46px);
}

header.app-topbar::after {
  content: "";
  position: absolute;
  right: 34%;
  bottom: 4px;
  z-index: -1;
  width: 170px;
  height: 86px;
  opacity: 0.24;
  background:
    linear-gradient(90deg, transparent 48%, rgba(255, 255, 255, 0.32) 49% 51%, transparent 52%),
    linear-gradient(22deg, transparent 45%, rgba(255, 255, 255, 0.28) 46% 48%, transparent 49%),
    linear-gradient(-28deg, transparent 45%, rgba(255, 255, 255, 0.28) 46% 48%, transparent 49%);
}

.brand {
  min-width: 0;
  flex: 1 1 auto;
  display: flex;
  align-items: center;
  gap: 24px;
}

.brand > div {
  min-width: 0;
}

.brand-logo {
  width: clamp(116px, 10vw, 142px);
  height: 54px;
  flex: 0 0 auto;
  object-fit: contain;
  padding-right: 24px;
  border-right: 1px solid rgba(255, 255, 255, 0.38);
  filter: brightness(0) invert(1);
}

.brand h1 {
  max-width: min(620px, 48vw);
  overflow: hidden;
  text-overflow: ellipsis;
  margin: 0;
  color: #ffffff;
  font-size: 30px;
  line-height: 1.12;
  font-weight: 950;
  letter-spacing: 0;
  white-space: nowrap;
}

.brand p {
  margin: 8px 0 0;
  color: rgba(255, 255, 255, 0.74);
  font-size: 14px;
  font-weight: 800;
  letter-spacing: 0;
}

.topbar-actions {
  flex: 0 1 auto;
  min-width: 0;
  max-width: min(760px, 58vw);
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
  margin-left: auto;
}

header.app-topbar .btn,
header.app-topbar button,
header.app-topbar .user-chip,
header.app-topbar .scope-switch {
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 0 16px;
  border: 1px solid rgba(255, 255, 255, 0.34);
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.14);
  color: #ffffff;
  font: inherit;
  font-size: 14px;
  font-weight: 900;
  line-height: 1;
  text-decoration: none;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.2),
    0 10px 22px rgba(4, 46, 145, 0.08);
  backdrop-filter: blur(10px);
  transition:
    transform 0.16s ease,
    background 0.16s ease,
    border-color 0.16s ease,
    box-shadow 0.16s ease;
}

header.app-topbar button {
  cursor: pointer;
}

header.app-topbar .btn:hover,
header.app-topbar button:hover {
  border-color: rgba(255, 255, 255, 0.52);
  background: rgba(255, 255, 255, 0.22);
  transform: translateY(-1px);
}

header.app-topbar .btn:focus-visible,
header.app-topbar button:focus-visible,
header.app-topbar select:focus-visible {
  outline: none;
  box-shadow:
    0 0 0 3px rgba(255, 255, 255, 0.24),
    0 0 0 5px rgba(48, 128, 255, 0.28);
}

header.app-topbar .user-chip {
  max-width: 190px;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  cursor: default;
}

header.app-topbar .scope-switch {
  padding: 4px 8px 4px 14px;
}

header.app-topbar .scope-switch span {
  color: rgba(255, 255, 255, 0.78);
  font-size: 13px;
  font-weight: 900;
}

header.app-topbar .scope-switch select {
  min-width: 96px;
  max-width: 156px;
  height: 34px;
  border: 1px solid rgba(255, 255, 255, 0.52);
  border-radius: 14px;
  padding: 0 32px 0 12px;
  background: #ffffff;
  color: #07439f;
  font-size: 14px;
  font-weight: 900;
  cursor: pointer;
}

header.app-topbar .scope-switch select:disabled {
  cursor: wait;
  opacity: 0.72;
}

header.app-topbar .danger-text {
  border-color: rgba(255, 255, 255, 0.72);
  background: #ffffff;
  color: #d03535;
  box-shadow: 0 12px 24px rgba(4, 46, 145, 0.12);
}

.admin-entry {
  opacity: 0.92;
}

@media (max-width: 1360px) {
  header.app-topbar {
    align-items: flex-start;
    flex-wrap: wrap;
    gap: 14px 22px;
  }

  .brand {
    flex: 1 1 520px;
  }

  .brand h1 {
    max-width: min(620px, calc(100vw - 360px));
  }

  .topbar-actions {
    flex: 1 1 100%;
    max-width: none;
    justify-content: flex-start;
    margin-left: 0;
  }
}

@media (max-width: 920px) {
  header.app-topbar {
    min-height: auto;
    align-items: flex-start;
    flex-direction: column;
    padding: 20px;
  }

  .brand-logo {
    width: 118px;
    height: 46px;
    padding-right: 18px;
  }

  .brand h1 {
    max-width: none;
    font-size: 22px;
    white-space: normal;
  }

  .topbar-actions {
    width: 100%;
    min-width: 0;
    justify-content: flex-start;
  }
}
</style>
