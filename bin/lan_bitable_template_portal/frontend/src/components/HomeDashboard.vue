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
        <span>{{ enabledModuleCount }} 个可用</span>
      </div>
      <button
        v-if="canRequestMoreScopes"
        type="button"
        class="permission-action"
        @click="emit('request-permission')"
      >
        <KeyRound :size="16" aria-hidden="true" />
        申请楼栋权限
      </button>
    </header>

    <div class="module-grid">
      <article
        v-for="(module, index) in modules"
        :key="module.key"
        class="module-card"
        :class="[
          module.tone,
          `module-${module.key}`,
          { core: index < 4, disabled: module.disabled, 'has-secondary': module.secondaryActions?.length },
        ]"
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
              <component :is="moduleIcon(module.icon)" :size="21" :stroke-width="2.1" />
            </span>
            <span v-if="module.disabled" class="module-state">建设中</span>
          </span>

          <strong>{{ module.title }}</strong>

          <span v-if="moduleMetrics[module.key]" class="module-metrics" aria-hidden="true">
            <span>
              <b>{{ moduleMetrics[module.key].primaryValue }}</b>
              <small>{{ moduleMetrics[module.key].primaryLabel }}</small>
            </span>
            <span>
              <b>{{ moduleMetrics[module.key].secondaryValue }}</b>
              <small>{{ moduleMetrics[module.key].secondaryLabel }}</small>
            </span>
          </span>
          <span v-else class="module-summary">{{ module.tags.slice(0, 2).join(" · ") }}</span>

          <span class="module-entry-cue" aria-hidden="true">
            <span>{{ module.disabled ? "暂未开放" : module.primaryAction.label }}</span>
            <ChevronRight v-if="!module.disabled" :size="16" />
          </span>
        </button>

        <div v-if="!module.disabled && module.secondaryActions?.length" class="module-actions">
          <button
            v-for="action in module.secondaryActions"
            :key="action.key"
            type="button"
            class="module-secondary-action"
            :disabled="action.disabled"
            @click.stop="selectAction(action)"
          >
            <span>{{ action.label }}</span>
            <ChevronRight :size="15" aria-hidden="true" />
          </button>
        </div>
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
  ScopeHomeModuleMetric,
} from "../scopeHomeUtils";
import HomeBroadcastTicker from "./HomeBroadcastTicker.vue";

defineProps<{
  modules: ScopeHomeModuleCard[];
  enabledModuleCount: number;
  canRequestMoreScopes: boolean;
  broadcastItems: ScopeHomeBroadcastItem[];
  broadcastSummary: string;
  moduleMetrics: Record<string, ScopeHomeModuleMetric>;
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
  padding: 12px 22px 24px;
  display: grid;
  gap: 11px;
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
  color: #10213a;
  font-size: 18px;
  line-height: 1.2;
  font-weight: 700;
}

.dashboard-title span {
  color: #6a7d96;
  font-size: 12px;
  font-weight: 600;
}

.permission-action {
  min-height: 36px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  padding: 0 13px;
  border: 1px solid #c9d9ed;
  border-radius: 8px;
  background: #ffffff;
  color: #175ab7;
  font: inherit;
  font-size: 12px;
  font-weight: 650;
  cursor: pointer;
  box-shadow: 0 3px 10px rgba(20, 76, 150, 0.05);
}

.permission-action:hover {
  border-color: #8fb7e9;
  background: #f6faff;
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
  gap: 11px;
}

.module-card {
  --module-accent: #1e63ff;
  --module-soft: #edf4ff;
  height: 186px;
  min-width: 0;
  display: grid;
  grid-template-rows: minmax(0, 1fr) auto;
  overflow: hidden;
  border: 1px solid #d8e3f1;
  border-radius: 8px;
  background: #fbfdff;
  box-shadow: 0 5px 16px rgba(16, 66, 132, 0.055);
  transition: border-color 170ms ease, box-shadow 170ms ease, background 170ms ease;
}

.module-card.core {
  background: #ffffff;
  box-shadow: 0 7px 20px rgba(16, 66, 132, 0.075);
}

.module-card.orange { --module-accent: #d95e22; --module-soft: #fff2e9; }
.module-card.violet { --module-accent: #6652c8; --module-soft: #f1efff; }
.module-card.rose { --module-accent: #d64158; --module-soft: #fff0f3; }
.module-card.emerald { --module-accent: #128765; --module-soft: #eaf8f2; }
.module-card.slate { --module-accent: #356cae; --module-soft: #edf4fb; }
.module-card.cyan { --module-accent: #168899; --module-soft: #eaf8fa; }

.module-card:not(.disabled):hover,
.module-card:not(.disabled):focus-within {
  border-color: #8fb7e9;
  background: #ffffff;
  box-shadow: 0 10px 25px rgba(16, 66, 132, 0.1);
}

.module-card__main {
  min-width: 0;
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 7px;
  padding: 13px 14px 10px;
  border: 0;
  border-radius: 7px;
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
  min-height: 38px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.module-icon {
  width: 38px;
  height: 38px;
  flex: 0 0 38px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  background: var(--module-soft);
  color: var(--module-accent);
}

.module-state {
  padding: 3px 7px;
  border: 1px solid #dbe3ed;
  border-radius: 999px;
  background: #eef2f6;
  color: #64748b;
  font-size: 11px;
  line-height: 1.2;
  font-weight: 650;
  white-space: nowrap;
}

.module-card__main > strong {
  min-width: 0;
  color: #10213a;
  font-size: 17px;
  line-height: 1.2;
  font-weight: 700;
}

.module-metrics {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 20px;
}

.module-metrics > span {
  min-width: 0;
  display: inline-flex;
  align-items: baseline;
  gap: 5px;
}

.module-metrics b {
  color: #155bc4;
  font-size: 20px;
  line-height: 1;
  font-weight: 700;
}

.module-metrics small,
.module-summary {
  color: #6a7d96;
  font-size: 11px;
  line-height: 1.35;
  font-weight: 600;
}

.module-summary {
  min-height: 20px;
  display: flex;
  align-items: center;
}

.module-entry-cue {
  min-width: 0;
  margin-top: auto;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  color: #1b5fbf;
  font-size: 12px;
  line-height: 1.2;
  font-weight: 650;
}

.module-entry-cue > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.module-actions {
  min-width: 0;
  padding: 6px 9px 8px;
  border-top: 1px solid #e7eef7;
  background: #f8fbff;
}

.module-actions button {
  width: 100%;
  min-width: 0;
  min-height: 34px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 9px;
  border: 1px solid #ccdaec;
  border-radius: 7px;
  background: #ffffff;
  color: #31567f;
  font: inherit;
  font-size: 11px;
  line-height: 1.15;
  font-weight: 650;
  cursor: pointer;
}

.module-actions button:hover {
  border-color: #8fb7e9;
  color: #155bc4;
  background: #f4f9ff;
}

.module-card.disabled {
  border-color: #dfe5ed;
  background: #f4f6f9;
  box-shadow: none;
}

.module-card.disabled .module-card__main {
  opacity: 0.68;
}

.module-card.disabled .module-icon {
  background: #e7ebf0;
  color: #64748b;
}

.module-card.disabled .module-entry-cue {
  color: #64748b;
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
    padding: 10px 12px 18px;
  }

  .dashboard-toolbar {
    min-height: 36px;
  }

  .dashboard-title h2 {
    font-size: 17px;
  }

  .permission-action {
    min-height: 44px;
    padding: 0 11px;
  }

  .module-grid {
    grid-template-columns: minmax(0, 1fr);
    gap: 8px;
  }

  .module-card {
    height: auto;
    min-height: 100px;
  }

  .module-card__main {
    min-height: 100px;
    display: grid;
    grid-template-columns: 34px minmax(0, 1fr) auto;
    grid-template-rows: auto auto;
    align-content: center;
    gap: 5px 10px;
    padding: 10px 12px;
  }

  .module-card__head {
    grid-column: 1;
    grid-row: 1 / 3;
    min-height: 34px;
    align-self: start;
  }

  .module-icon {
    width: 34px;
    height: 34px;
    flex-basis: 34px;
  }

  .module-state {
    display: none;
  }

  .module-card__main > strong {
    grid-column: 2;
    grid-row: 1;
    align-self: end;
  }

  .module-metrics,
  .module-summary {
    grid-column: 2;
    grid-row: 2;
    align-self: start;
  }

  .module-entry-cue {
    grid-column: 3;
    grid-row: 1 / 3;
    align-self: center;
    margin-top: 0;
  }

  .module-entry-cue > span {
    display: none;
  }

  .module-card.disabled .module-entry-cue > span {
    display: inline;
  }

  .module-actions button {
    min-height: 44px;
  }
}
</style>
