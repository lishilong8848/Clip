<template>
  <Teleport to="body">
    <div v-if="open" class="signature-drawer-shell">
      <button type="button"
        class="signature-drawer-backdrop"
        :aria-label="`关闭${title}`"
        @click="emit('close')"
      ></button>
      <div
        class="selected-signature-drawer"
        :class="{ 'temporary-signature-drawer': tone === 'temporary' }"
        role="dialog"
        aria-modal="true"
        :aria-label="title"
        tabindex="-1"
        @click.stop
        @mousedown.stop
      >
        <div class="drawer-head">
          <strong>{{ title }}</strong>
          <button type="button" @click="emit('close')">{{ closeLabel }}</button>
        </div>
        <div class="drawer-signature-list">
          <slot />
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
withDefaults(defineProps<{
  open: boolean;
  title: string;
  tone?: "company" | "temporary";
  closeLabel?: string;
}>(), {
  tone: "company",
  closeLabel: "收起",
});

const emit = defineEmits<{
  close: [];
}>();
</script>

<style scoped>
.signature-drawer-shell {
  position: fixed;
  top: 76px;
  right: 24px;
  bottom: 24px;
  z-index: var(--cf-z-signature-drawer, 860);
  width: min(1080px, calc(100vw - 48px));
  height: auto;
  max-width: calc(100vw - 48px);
  pointer-events: none;
}

.signature-drawer-backdrop {
  position: fixed;
  z-index: 0;
  inset: 0;
  border: 0;
  background: transparent;
  cursor: default;
  pointer-events: auto;
}

.selected-signature-drawer {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  height: 100%;
  width: 100%;
  max-height: none;
  border: 1px solid #cfe0ff;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 24px 58px rgba(12, 46, 108, 0.24);
  padding: 0;
  overflow: hidden;
  overscroll-behavior: contain;
  pointer-events: auto;
}

:global(.mop-preview-page) .signature-drawer-shell {
  top: 76px;
  right: 24px;
  bottom: 24px;
  width: min(1080px, calc(100vw - 48px));
  height: auto;
}

:global(.mop-preview-page .mop-sign-panel.manager-open) .signature-drawer-shell {
  position: fixed;
  top: 76px;
  right: 24px;
  bottom: 24px;
  width: min(1080px, calc(100vw - 48px));
  height: auto;
  max-width: calc(100vw - 48px);
  max-height: none;
}

:global(.mop-preview-page .mop-sign-panel.manager-open) .signature-drawer-backdrop {
  position: fixed;
  inset: 0;
  background: transparent;
  backdrop-filter: none;
}

:global(.mop-preview-page) .selected-signature-drawer {
  max-height: none;
  overflow: hidden;
}

:global(.mop-preview-page .mop-sign-panel.manager-open) .selected-signature-drawer {
  width: 100%;
  height: 100%;
  max-height: none;
  margin-left: 0;
  border-radius: 18px;
  box-shadow: 0 18px 44px rgba(12, 46, 108, 0.22);
}

.temporary-signature-drawer {
  border-color: #fed7aa;
  background: rgba(255, 253, 248, 0.98);
  box-shadow: 0 22px 50px rgba(154, 52, 18, 0.2);
}

.drawer-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  min-height: 38px;
  border-bottom: 1px solid rgba(207, 224, 255, 0.82);
  padding: 7px 10px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.94)),
    #ffffff;
}

.drawer-head::before {
  content: "";
  flex: 0 0 auto;
  width: 6px;
  height: 22px;
  border-radius: 999px;
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
}

.temporary-signature-drawer .drawer-head {
  border-bottom-color: rgba(254, 215, 170, 0.88);
  background:
    linear-gradient(135deg, rgba(255, 247, 237, 0.98), rgba(255, 255, 255, 0.94)),
    #ffffff;
}

.temporary-signature-drawer .drawer-head::before {
  background: linear-gradient(180deg, #f97316, #fbbf24);
}

.drawer-head strong {
  flex: 1 1 auto;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #0f172a;
  font-size: 13px;
  font-weight: 900;
}

.drawer-head button {
  flex: 0 0 auto;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  background: #ffffff;
  color: #3156c9;
  padding: 5px 10px;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.drawer-signature-list {
  display: grid;
  align-content: start;
  gap: 4px;
  min-height: 0;
  max-height: none;
  overflow-x: hidden;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 6px;
  scrollbar-width: thin;
  scrollbar-gutter: stable;
  contain: layout paint;
}

.drawer-signature-list::-webkit-scrollbar {
  width: 8px;
}

.drawer-signature-list::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(148, 163, 184, 0.46);
}

:global(.mop-preview-page) .drawer-signature-list {
  max-height: none;
}

:global(.mop-preview-page .mop-sign-panel.manager-open) .drawer-signature-list {
  max-height: none;
}

.drawer-signature-list :deep(article) {
  display: grid;
  grid-template-columns: 66px minmax(0, 1fr) minmax(224px, auto);
  align-items: center;
  gap: 5px 7px;
  min-height: 46px;
  border: 1px solid #d8e5f7;
  border-radius: 13px;
  background: #f8fbff;
  padding: 4px 5px;
  cursor: default;
}

.drawer-signature-list :deep(article.ready) {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.drawer-signature-list :deep(article.pending) {
  border-color: #fed7aa;
  background: #fff7ed;
}

.drawer-signature-list :deep(img) {
  width: 58px;
  height: 22px;
  object-fit: contain;
}

.drawer-signature-list :deep(input) {
  width: 100%;
  min-width: 0;
  height: 26px;
  border: 1px solid rgba(234, 88, 12, 0.22);
  border-radius: 999px;
  background: #ffffff;
  color: #0f172a;
  padding: 0 8px;
  font-size: 12px;
  font-weight: 800;
  outline: none;
}

.drawer-signature-list :deep(input:focus) {
  border-color: #f97316;
  box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.12);
}

.drawer-signature-list :deep(strong),
.drawer-signature-list :deep(small) {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.drawer-signature-list :deep(strong) {
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
}

.drawer-signature-list :deep(small) {
  margin-top: 1px;
  color: #64748b;
  font-size: 11px;
}

.drawer-signature-list :deep(.drawer-actions) {
  display: grid;
  grid-column: auto;
  grid-template-columns: repeat(3, minmax(66px, 1fr));
  align-items: center;
  justify-content: stretch;
  gap: 4px;
  max-width: none;
  min-width: 0;
  padding-top: 0;
}

.drawer-signature-list :deep(.drawer-action),
.drawer-signature-list :deep(.drawer-remove) {
  min-width: 0;
  border: 0;
  border-radius: 999px;
  min-height: 22px;
  padding: 2px 6px;
  font-size: 11px;
  font-weight: 900;
  overflow: hidden;
  text-overflow: ellipsis;
  cursor: pointer;
  white-space: nowrap;
}

.drawer-signature-list :deep(.drawer-action) {
  background: #dbeafe;
  color: #1d4ed8;
}

.drawer-signature-list :deep(.drawer-action.link-action) {
  background: #e0f2fe;
  color: #0369a1;
}

.drawer-signature-list :deep(.drawer-remove) {
  background: #eef2ff;
  color: #3156c9;
}

.drawer-signature-list :deep(.drawer-remove) {
  grid-column: auto;
}

.drawer-signature-list :deep(.drawer-action:disabled),
.drawer-signature-list :deep(.drawer-remove:disabled) {
  cursor: not-allowed;
  opacity: 0.55;
}

@media (max-width: 760px) {
  .signature-drawer-shell,
  :global(.mop-preview-page .mop-sign-panel.manager-open) .signature-drawer-shell,
  :global(.mop-preview-page) .signature-drawer-shell {
    position: fixed;
    top: auto;
    right: 12px;
    bottom: 12px;
    left: 12px;
    width: auto;
    height: min(72vh, 620px);
    max-width: none;
  }

  .selected-signature-drawer,
  :global(.mop-preview-page) .selected-signature-drawer {
    max-height: min(72vh, 620px);
    height: min(72vh, 620px);
  }

  .drawer-signature-list,
  :global(.mop-preview-page) .drawer-signature-list {
    max-height: min(58vh, 520px);
  }

  .drawer-signature-list :deep(.drawer-actions) {
    grid-column: 1 / -1;
    grid-template-columns: repeat(3, minmax(68px, 1fr));
  }
}

@media (max-width: 980px) {
  .drawer-signature-list :deep(article) {
    grid-template-columns: 62px minmax(0, 1fr);
  }

  .drawer-signature-list :deep(.drawer-actions) {
    grid-column: 1 / -1;
    grid-template-columns: repeat(3, minmax(68px, 1fr));
  }
}
</style>
