<template>
  <EngineerMopPage ref="engineer" v-bind="props" class="v2-mop-workspace" @click="followSelection" />
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, provide, ref } from 'vue';
import './MopDesk.css';
import EngineerMopPage from '../components/EngineerMopPage.vue';

// Keep mobile navigation alive while the original controller opens/closes the editor.
provide('workspace-v2:mop-work-open', ref(false));

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

const engineer = ref<InstanceType<typeof EngineerMopPage> | null>(null);
let layoutObserver: ResizeObserver | undefined;
let modeObserver: MutationObserver | undefined;
let frame = 0;

// The shell header wraps on tablets and the bottom navigation is present only
// at narrow widths. Fit the sheet between their actual boundaries.
function fitEditor() {
  cancelAnimationFrame(frame);
  frame = requestAnimationFrame(() => {
    const root = engineer.value?.$el as HTMLElement | undefined;
    if (!root?.classList.contains('preview-open')) return;
    const nav = document.querySelector('.v2-mobile-nav')?.getBoundingClientRect();
    const viewport = window.visualViewport;
    const bottom = viewport ? viewport.height + viewport.offsetTop : window.innerHeight;
    const available = Math.min(bottom, nav?.height ? nav.top : bottom) - root.getBoundingClientRect().top - 6;
    root.style.setProperty('--mop-desk-editor-height', Math.max(180, Math.floor(available)) + 'px');
  });
}
onMounted(() => {
  const root = engineer.value?.$el as HTMLElement | undefined;
  if (!root) return;
  modeObserver = new MutationObserver(fitEditor);
  modeObserver.observe(root, {attributes:true, attributeFilter:['class']});
  layoutObserver = new ResizeObserver(fitEditor);
  for (const el of document.querySelectorAll('.v2-topnav,.v2-mobile-nav')) layoutObserver.observe(el);
  window.addEventListener('resize', fitEditor);
  window.visualViewport?.addEventListener('resize', fitEditor);
  fitEditor();
});
onBeforeUnmount(() => {
  cancelAnimationFrame(frame);
  modeObserver?.disconnect();
  layoutObserver?.disconnect();
  window.removeEventListener('resize', fitEditor);
  window.visualViewport?.removeEventListener('resize', fitEditor);
});

async function followSelection(event: MouseEvent) {
  if (!matchMedia('(max-width: 900px)').matches || !(event.target instanceof Element)) return;
  const root = event.currentTarget as HTMLElement;
  const target = event.target.closest('.mop-desk-file') ? '.mop-desk-attachment' : '';
  if (!target) return;
  await nextTick();
  root.querySelector(target)?.scrollIntoView({
    block: 'start', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
  });
}
</script>

<style scoped>
.v2-mop-workspace.preview-open { display: flex; flex-direction: column; height:var(--mop-desk-editor-height,calc(100dvh - var(--head-height,60px) - 48px));box-sizing:border-box;min-height:0; padding: 0; }
.v2-mop-workspace.preview-open > :deep(:not(.mop-preview-page)) { flex-shrink: 0; }
.v2-mop-workspace :deep(.mop-preview-page) { display: flex; flex-direction: column; align-items: stretch; flex: 1; min-height: 0; width: 100%; padding: 0; gap: 8px; --mop-upload-footer-safe-space: 0px; }
.v2-mop-workspace :deep(.mop-preview-page > :not(.preview-scroll)) { flex-shrink: 0; }
.v2-mop-workspace :deep(.preview-head) { display: flex; flex-wrap: wrap; padding: 4px 0 12px; gap: 10px 18px; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; background: transparent; box-shadow: none; }
.v2-mop-workspace :deep(.preview-title) { flex: 1; min-width: min(320px, 100%); }
.v2-mop-workspace :deep(.preview-title strong) { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; font-size: 17px; font-weight: 600; white-space: normal; line-height: 1.5; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(.preview-title p) { font-size: 12px; font-weight: 400; line-height: 1.6; white-space: normal; }
.v2-mop-workspace :deep(.preview-head-status span) { font-size: 12px; font-weight: 500; border-radius: 4px; max-width: none; padding: 3px 6px; }
.v2-mop-workspace :deep(:is(.mop-file-status, .mop-detect-panel, .sheet-tabs, .floating-upload-box, .mop-sign-panel:not(.manager-open))) { border-color: var(--mop-line); border-radius: 6px; background: #fff; box-shadow: none; }
.v2-mop-workspace :deep(.mop-file-status) { padding: 7px 10px; }
.v2-mop-workspace :deep(.mop-file-status strong) { font-size: 12px; font-weight: 500; }
.v2-mop-workspace :deep(.mop-sign-panel:not(.manager-open)) { padding: 8px 10px; }
.v2-mop-workspace :deep(.mop-sign-panel:not(.manager-open)::before) { display: none; }
.v2-mop-workspace :deep(.sheet-tabs button) { border: 0; border-radius: 4px; min-height: 34px; font-size: 13px; font-weight: 500; background: transparent; }
.v2-mop-workspace :deep(.sheet-tabs button.active) { color: var(--v2-blue); background: #eaf3ff; box-shadow: none; }
.v2-mop-workspace :deep(.table-fill-toolbar) { padding: 6px 8px; background: #fff; box-shadow: none; backdrop-filter: none; }
.v2-mop-workspace :deep(.table-fill-title strong) { padding: 0; background: none; color: var(--mop-ink); font-weight: 600; }
.v2-mop-workspace :deep(.table-fill-title span) { font-weight: 400; }
.v2-mop-workspace :deep(.table-fill-actions em) { border-radius: 4px; font-weight: 500; }
.v2-mop-workspace :deep(.preview-scroll) { flex: 1; min-height: 120px; max-height: none; overflow: auto; padding-bottom: 0; border-color: var(--mop-line); border-radius: 6px; box-shadow: none; scrollbar-width: thin; overscroll-behavior: auto; }
.v2-mop-workspace :deep(.floating-upload-box) { position: relative; display: flex; flex-direction: column; align-items: stretch; width: 100%; margin: 0; padding: 8px 10px; gap: 6px; }
.v2-mop-workspace :deep(.floating-upload-checks) { position: absolute; z-index: 5; bottom: calc(100% + 8px); inset-inline: 0; max-height: min(240px, 40dvh); overflow: auto; padding: 10px; border: 1px solid var(--mop-line); border-radius: 6px; background: #fff; box-shadow: 0 4px 18px #00000018; }
.v2-mop-workspace :deep(.floating-upload-hint) { display: none; }
.v2-mop-workspace :deep(.floating-upload-compact) { display: flex; justify-content: space-between; gap: 8px; }
.v2-mop-workspace :deep(.floating-upload-summary) { display: none; }
.v2-mop-workspace :deep(.floating-upload-action-row) { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.v2-mop-workspace :deep(.floating-upload-action-row > button) { flex: none; min-width: 0; padding-inline: 12px; }
.v2-mop-workspace :deep(.mop-footer-signatures) { flex: 1; min-width: 0; margin-right: auto; }
.v2-mop-workspace :deep(.mop-footer-signatures .signature-role-summary) { display: grid; grid-template-columns: repeat(2,minmax(0,1fr)); gap: 8px; }
.v2-mop-workspace :deep(.mop-footer-signatures button) { min-height: 40px; padding: 6px 9px; border-radius: 6px; }
.v2-mop-workspace :deep(.mop-footer-signatures .role-state-row) { display: none; }
.v2-mop-workspace :deep(.mop-footer-signatures .role-state-row:has(.warn)) { display: block; }
.v2-mop-workspace :deep(.mop-footer-signatures .role-state-row small) { display: none; }
.v2-mop-workspace :deep(.mop-footer-signatures .role-title-row) { flex-wrap: wrap; gap: 3px 6px; }
.v2-mop-workspace :deep(.floating-uploaded-at) { flex: none; max-width: 180px; font-size: 11px; box-shadow: none; }
.v2-mop-workspace :deep(.floating-upload-blocked) { font-weight: 400; }
.v2-mop-workspace :deep(.floating-upload-title) { font-size: 13px; font-weight: 600; }
.v2-mop-workspace :deep(.floating-upload-summary) { justify-content: flex-start; }
@media(max-width:600px) {
  .v2-mop-workspace :deep(.mop-preview-page .floating-upload-action-row) {display:grid;grid-template-columns:minmax(0,1fr) auto;gap:4px 8px}
  .v2-mop-workspace :deep(.floating-upload-action-row .mop-footer-signatures) {grid-column:1/-1;width:100%;margin:0}
  .v2-mop-workspace :deep(.floating-upload-action-row:has(.floating-uploaded-at) .mop-footer-signatures) {grid-area:1/1}
  .v2-mop-workspace :deep(.floating-upload-action-row:has(.floating-uploaded-at) .mop-download) {grid-area:1/2}
  .v2-mop-workspace :deep(.floating-upload-action-row .floating-uploaded-at) {grid-area:2/1;justify-self:start}
  .v2-mop-workspace :deep(.floating-upload-action-row .floating-upload-signed-mop) {grid-area:2/2}
}
.v2-mop-workspace :deep(.signature-manager-backdrop) { backdrop-filter: none; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open) { border-radius: 8px; background: #fff; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open .sign-panel-head) { border-radius: 8px 8px 0 0; background: #f6f8fa; backdrop-filter: none; }

@media (max-width:900px) {
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] > .notice-panel),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] > .binding-panel),
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .v2-page-canvas:not(.is-customizing) .grid-stack-item[data-widget-id="block_notices"]),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] .v2-page-canvas:not(.is-customizing) .grid-stack-item[data-widget-id="block_binding"]),
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .v2-embedded-block:has(>.notice-panel)),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] .v2-embedded-block:has(>.binding-panel)) { display: none !important; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) > .v2-widget-grid) { display: flex !important; flex-direction: column; height: auto !important; min-height: 0 !important; gap: 12px; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) .grid-stack-item) { position: relative !important; inset: auto !important; width: 100% !important; height: auto !important; min-height: 0 !important; transform: none !important; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) :is(.v2-widget,.v2-widget-body,.v2-responsive-body,.v2-module-block)) { position: relative; inset: auto; height: auto !important; min-height: 0 !important; max-height: none !important; overflow: visible; }
}
@media (max-width: 760px) {
  .v2-mop-workspace.preview-open { height:var(--mop-desk-editor-height,calc(100dvh - var(--head-height,56px) - 94px - env(safe-area-inset-bottom))); padding: 0; }
}
@media (max-width: 720px) {
  .v2-mop-workspace :deep(.mop-preview-page .preview-scroll),
  .v2-mop-workspace :deep(.mop-preview-page .preview-scroll:has(.mop-mobile-form)) { flex: 1; min-height: 100px; max-height: none; overflow: auto; scroll-padding-block: 12px; }
  .v2-mop-workspace :deep(.mop-preview-page.mobile-page-details .floating-upload-box) { display: flex; }
  .v2-mop-workspace :deep(.mop-footer-signatures button) { min-height: 44px; }
  .v2-mop-workspace :deep(.mop-preview-page .floating-upload-action-row button) { min-height: 44px; }
  .v2-mop-workspace :deep(.mop-mobile-form :is(input, select, textarea)) { min-height: 44px; font-size: 16px; }
  .v2-mop-workspace :deep(.preview-head) { display: grid; grid-template-columns: auto minmax(0, 1fr); }
  .v2-mop-workspace :deep(.preview-title) { min-width: 0; }
  .v2-mop-workspace :deep(.preview-title strong) { font-size: 14px; }
  .v2-mop-workspace :deep(.preview-title p) { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
}
/* The step tabs already provide navigation; reserve short-screen space for the form and actions. */
@media (max-width: 720px) and (max-height: 700px) {
  .v2-mop-workspace :deep(.mop-preview-page) { gap: 6px; }
  .v2-mop-workspace :deep(.mop-mobile-continue) { display: none; }
  .v2-mop-workspace :deep(.preview-head) { padding-bottom: 6px; }
  .v2-mop-workspace :deep(.preview-title strong) { -webkit-line-clamp: 1; }
  .v2-mop-workspace :deep(.mop-preview-page .preview-scroll),
  .v2-mop-workspace :deep(.mop-preview-page .preview-scroll:has(.mop-mobile-form)) { min-height: 80px; }
  .v2-mop-workspace :deep(.floating-upload-box) { padding: 6px 8px; }
  .v2-mop-workspace :deep(:is(.floating-upload-blocked, .floating-upload-result)) { max-height: 60px; overflow-y: auto; }
}
</style>
