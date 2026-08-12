<template>
  <section class="home-dashboard" aria-labelledby="home-module-heading">
    <HomeBroadcastTicker
      :items="broadcastItems"
      :summary="broadcastSummary"
      @activate="emit('activate-broadcast', $event)"
    />

    <header class="dashboard-toolbar">
      <div class="dashboard-title">
        <h2 id="home-module-heading">业务模块</h2>
        <span>已开放 {{ enabledModuleCount }} / {{ modules.length }}</span>
      </div>
      <button
        v-if="canRequestMoreScopes"
        type="button"
        class="permission-action"
        @click="emit('request-permission')"
      >
        <KeyRound :size="16" aria-hidden="true" />
        申请其他楼权限
      </button>
    </header>

    <div class="module-grid">
      <article
        v-for="module in modules"
        :key="module.key"
        class="module-card"
        :class="[module.tone, `module-${module.key}`, { disabled: module.disabled }]"
        :aria-disabled="module.disabled ? 'true' : undefined"
      >
        <button
          type="button"
          class="module-card__main"
          :disabled="module.disabled || module.primaryAction.disabled"
          :aria-label="module.primaryAction.label"
          @click="selectAction(module.primaryAction, module.disabled)"
        >
          <span class="module-card__head">
            <span class="module-icon" aria-hidden="true">
              <component :is="moduleIcon(module.icon)" :size="22" :stroke-width="2.2" />
            </span>
            <span class="module-badge">{{ module.disabled ? "建设中" : module.badge }}</span>
          </span>
          <strong>{{ module.title }}</strong>
          <span class="module-tags" aria-hidden="true">
            <span v-for="tag in module.tags.slice(0, 2)" :key="tag">{{ tag }}</span>
          </span>
        </button>

        <div v-if="!module.disabled" class="module-actions">
          <button
            type="button"
            class="module-primary-action"
            @click.stop="selectAction(module.primaryAction)"
          >
            <span>{{ module.primaryAction.label }}</span>
            <ChevronRight :size="16" aria-hidden="true" />
          </button>
          <button
            v-for="action in module.secondaryActions || []"
            :key="action.key"
            type="button"
            class="module-secondary-action"
            :disabled="action.disabled"
            @click.stop="selectAction(action)"
          >
            {{ action.label }}
          </button>
        </div>
        <div v-else class="module-disabled-action" aria-hidden="true">暂未开放</div>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import type { Component } from "vue";
import {
  Activity,
  ChevronRight,
  CircleAlert,
  ClipboardList,
  Droplets,
  KeyRound,
  RefreshCw,
  Settings2,
  ShieldCheck,
  Wrench,
} from "lucide-vue-next";
import type {
  ScopeHomeBroadcastItem,
  ScopeHomeModuleAction,
  ScopeHomeModuleCard,
} from "../scopeHomeUtils";
import HomeBroadcastTicker from "./HomeBroadcastTicker.vue";

defineProps<{
  modules: ScopeHomeModuleCard[];
  enabledModuleCount: number;
  canRequestMoreScopes: boolean;
  broadcastItems: ScopeHomeBroadcastItem[];
  broadcastSummary: string;
}>();

const emit = defineEmits<{
  "select-action": [action: ScopeHomeModuleAction];
  "activate-broadcast": [item: ScopeHomeBroadcastItem];
  "request-permission": [];
}>();

const moduleIcons: Record<string, Component> = {
  event: CircleAlert,
  wrench: Wrench,
  switch: RefreshCw,
  repair: ClipboardList,
  risk: ShieldCheck,
  capacity: Droplets,
  more: Settings2,
  drill: Activity,
};

function moduleIcon(name: string): Component {
  return moduleIcons[name] || Settings2;
}

function selectAction(action: ScopeHomeModuleAction, disabled = false): void {
  if (disabled || action.disabled || !action.key) return;
  emit("select-action", action);
}
</script>

<style scoped>
.home-dashboard {
  width: min(100%, 1680px);
  margin: 0 auto;
  padding: 14px 22px 22px;
  display: grid;
  gap: 10px;
}

.dashboard-toolbar {
  min-height: 38px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.dashboard-title {
  min-width: 0;
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.dashboard-title h2 {
  margin: 0;
  color: #0b1f3a;
  font-size: 18px;
  line-height: 1.2;
  font-weight: 900;
}

.dashboard-title span {
  color: #60738d;
  font-size: 12px;
  font-weight: 750;
}

.permission-action {
  min-height: 36px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  padding: 0 13px;
  border: 1px solid #cbdcf3;
  border-radius: 8px;
  background: #ffffff;
  color: #1559bb;
  font: inherit;
  font-size: 12px;
  font-weight: 850;
  cursor: pointer;
  box-shadow: 0 4px 12px rgba(20, 76, 150, 0.06);
}

.permission-action:hover {
  border-color: #91b9ec;
  background: #f7fbff;
}

.permission-action:focus-visible,
.module-card__main:focus-visible,
.module-actions button:focus-visible {
  outline: none;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.18);
}

.module-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.module-card {
  --module-accent: #1e63ff;
  --module-soft: #edf4ff;
  position: relative;
  min-width: 0;
  min-height: 182px;
  display: grid;
  grid-template-rows: minmax(0, 1fr) auto;
  overflow: hidden;
  border: 1px solid #d7e3f3;
  border-top: 3px solid var(--module-accent);
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 8px 22px rgba(16, 66, 132, 0.07);
  transition: border-color 160ms ease, box-shadow 160ms ease, transform 160ms ease;
}

.module-card.orange { --module-accent: #e66f24; --module-soft: #fff3e9; }
.module-card.violet { --module-accent: #7657d8; --module-soft: #f2efff; }
.module-card.rose { --module-accent: #dc4960; --module-soft: #fff0f3; }
.module-card.emerald { --module-accent: #15966c; --module-soft: #eaf9f3; }
.module-card.slate { --module-accent: #3e73b8; --module-soft: #edf4fb; }
.module-card.cyan { --module-accent: #1598aa; --module-soft: #eaf9fb; }

.module-card:not(.disabled):hover,
.module-card:not(.disabled):focus-within {
  border-color: #a9c5e9;
  box-shadow: 0 12px 28px rgba(16, 66, 132, 0.11);
  transform: translateY(-1px);
}

.module-card__main {
  min-width: 0;
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 8px;
  padding: 13px 14px 10px;
  border: 0;
  border-radius: 5px 5px 0 0;
  background: transparent;
  color: inherit;
  text-align: left;
  font: inherit;
  cursor: pointer;
}

.module-card__main:disabled {
  cursor: not-allowed;
}

.module-card__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.module-icon {
  width: 40px;
  height: 40px;
  flex: 0 0 40px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  background: var(--module-soft);
  color: var(--module-accent);
}

.module-badge {
  min-width: 0;
  padding: 4px 8px;
  border: 1px solid #dce6f2;
  border-radius: 999px;
  background: var(--module-soft);
  color: var(--module-accent);
  font-size: 11px;
  line-height: 1.15;
  font-weight: 850;
  white-space: nowrap;
}

.module-card__main strong {
  min-width: 0;
  color: #10213a;
  font-size: 17px;
  line-height: 1.2;
  font-weight: 900;
}

.module-tags {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 5px;
  overflow: hidden;
}

.module-tags span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  padding: 3px 7px;
  border: 1px solid #e1e9f4;
  border-radius: 999px;
  background: #f8fafc;
  color: #566a84;
  font-size: 11px;
  line-height: 1.2;
  font-weight: 750;
  white-space: nowrap;
}

.module-actions {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 9px 10px 10px;
  border-top: 1px solid #e8eef6;
}

.module-actions button {
  min-width: 0;
  min-height: 38px;
  border-radius: 7px;
  font: inherit;
  font-size: 11px;
  line-height: 1.15;
  font-weight: 850;
  cursor: pointer;
}

.module-primary-action {
  flex: 1 1 auto;
  display: inline-flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  padding: 0 10px;
  border: 1px solid var(--module-accent);
  background: var(--module-accent);
  color: #ffffff;
}

.module-primary-action span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.module-secondary-action {
  flex: 0 1 auto;
  padding: 0 9px;
  border: 1px solid #cfdced;
  background: #ffffff;
  color: #31567f;
  white-space: nowrap;
}

.module-secondary-action:hover {
  border-color: var(--module-accent);
  color: var(--module-accent);
  background: var(--module-soft);
}

.module-card.disabled {
  border-top-color: #aab8ca;
  background: #f6f8fb;
  box-shadow: none;
}

.module-card.disabled .module-card__main,
.module-card.disabled .module-disabled-action {
  opacity: 0.68;
}

.module-card.disabled .module-icon,
.module-card.disabled .module-badge {
  background: #e9edf3;
  color: #64748b;
  border-color: #d8e0ea;
}

.module-disabled-action {
  min-height: 54px;
  display: flex;
  align-items: center;
  padding: 9px 14px;
  border-top: 1px solid #e1e7ef;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

@media (prefers-reduced-motion: reduce) {
  .module-card {
    transition: none;
  }
}

@media (max-width: 1179px) {
  .module-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 759px) {
  .home-dashboard {
    padding: 12px 14px 20px;
  }

  .dashboard-toolbar {
    align-items: flex-start;
    flex-direction: column;
    gap: 8px;
  }

  .permission-action,
  .module-actions button {
    min-height: 44px;
  }

  .module-grid {
    grid-template-columns: minmax(0, 1fr);
  }

  .module-card {
    min-height: 174px;
  }

  .module-tags span:nth-child(n + 2) {
    display: none;
  }
}
</style>
