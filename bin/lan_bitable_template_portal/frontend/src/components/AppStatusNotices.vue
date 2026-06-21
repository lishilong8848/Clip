<template>
  <div v-if="connectionNotice || pageStatusText" class="app-status-notices" :class="{ stacked: connectionNotice && pageStatusText }">
    <div v-if="connectionNotice" class="status-banner" :class="connectionNotice.tone || 'info'" role="status" aria-live="polite">
      <div class="status-banner__main">
        <b>{{ connectionLabel(connectionNotice.tone) }}</b>
        <span class="status-banner__text" :title="connectionNotice.text">{{ connectionNotice.text }}</span>
      </div>
      <button
        v-if="connectionNotice.action"
        class="btn ghost small"
        type="button"
        @click="connectionNotice.action"
      >
        {{ connectionNotice.actionLabel || "处理" }}
      </button>
    </div>
    <div v-if="pageStatusText" class="page-status">
      <b>页面状态</b>
      <span :title="pageStatusText" role="status" aria-live="polite">{{ pageStatusText }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  connectionNotice: {
    tone?: "info" | "warning" | "failed" | string;
    text: string;
    actionLabel?: string;
    action?: () => void;
  } | null;
  pageStatusText: string;
}>();

function connectionLabel(tone?: string): string {
  if (tone === "failed") return "连接异常";
  if (tone === "warning") return "需要注意";
  return "实时同步";
}
</script>

<style scoped>
.app-status-notices {
  width: min(1040px, calc(100% - 56px));
  display: grid;
  gap: 6px;
  margin: 10px auto 0;
}

.app-status-notices.stacked {
  gap: 5px;
}

.status-banner,
.page-status {
  width: 100%;
  border: 1px solid rgba(191, 219, 254, 0.78);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.94), rgba(255, 255, 255, 0.9)),
    #ffffff;
  box-shadow: 0 10px 22px rgba(37, 99, 235, 0.07);
  color: #1e3a8a;
  font-size: 12px;
  font-weight: 850;
}

.status-banner {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
  min-height: 38px;
  padding: 7px 8px 7px 12px;
}

.status-banner__main,
.page-status {
  min-width: 0;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 8px;
}

.status-banner__main > b,
.page-status > b {
  flex: 0 0 auto;
  border-radius: 999px;
  padding: 3px 8px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 11px;
  font-weight: 950;
  line-height: 1.2;
  white-space: nowrap;
}

.status-banner__text {
  min-width: 0;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  line-height: 1.4;
  overflow: hidden;
  overflow-wrap: anywhere;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.status-banner__text::before {
  content: "";
  flex: 0 0 auto;
  width: 9px;
  height: 9px;
  border-radius: 999px;
  background: #1e63ff;
  box-shadow: 0 0 0 5px rgba(30, 99, 255, 0.1);
}

.status-banner.warning {
  border-color: rgba(245, 158, 11, 0.34);
  background: rgba(255, 251, 235, 0.95);
  color: #92400e;
}

.status-banner.warning .status-banner__main > b {
  background: #ffedd5;
  color: #c2410c;
}

.status-banner.warning .status-banner__text,
.status-banner.failed .status-banner__text {
  display: -webkit-inline-box;
  overflow: hidden;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.status-banner.warning .status-banner__text::before {
  background: #f59e0b;
  box-shadow: 0 0 0 5px rgba(245, 158, 11, 0.12);
}

.status-banner.failed {
  border-color: rgba(239, 68, 68, 0.32);
  background: rgba(254, 242, 242, 0.95);
  color: #b91c1c;
}

.status-banner.failed .status-banner__main > b {
  background: #fee2e2;
  color: #b91c1c;
}

.status-banner.failed .status-banner__text::before {
  background: #e11d48;
  box-shadow: 0 0 0 5px rgba(225, 29, 72, 0.1);
}

.page-status {
  min-height: 34px;
  padding: 7px 12px;
  text-align: left;
  color: #31516f;
}

.page-status span {
  display: -webkit-box;
  overflow: hidden;
  overflow-wrap: anywhere;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.btn {
  min-height: 30px;
  flex: 0 0 auto;
  border: 1px solid rgba(191, 219, 254, 0.82);
  border-radius: 999px;
  padding: 0 12px;
  background: rgba(255, 255, 255, 0.88);
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}

@media (max-width: 720px) {
  .app-status-notices {
    width: calc(100% - 24px);
  }

  .status-banner,
  .page-status {
    width: 100%;
    border-radius: 18px;
  }

  .status-banner {
    align-items: stretch;
    flex-direction: column;
  }

  .status-banner__main,
  .page-status {
    grid-template-columns: 1fr;
    gap: 5px;
  }

  .status-banner__text,
  .page-status span {
    white-space: normal;
  }
}
</style>
