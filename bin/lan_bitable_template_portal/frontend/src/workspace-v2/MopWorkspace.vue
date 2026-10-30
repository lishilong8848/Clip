<template>
  <EngineerMopPage v-bind="props" class="v2-mop-workspace" @click="followSelection" />
</template>

<script setup lang="ts">
import { nextTick, provide, ref } from 'vue';
import EngineerMopPage from '../components/EngineerMopPage.vue';

// Keep mobile navigation alive while the original controller opens/closes the editor.
provide('workspace-v2:mop-work-open', ref(false));

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

async function followSelection(event: MouseEvent) {
  if (!matchMedia('(max-width: 900px)').matches || !(event.target instanceof Element)) return;
  const root = event.currentTarget as HTMLElement;
  const target = event.target.closest('.mop-row') ? '.mop-selected-file-card' : '';
  if (!target) return;
  await nextTick();
  root.querySelector(target)?.scrollIntoView({
    block: 'start', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
  });
}
</script>

<style scoped>
.v2-mop-workspace.engineer-mop {
  --mop-line: var(--v2-border, #dbe3df);
  --mop-ink: var(--v2-ink, #24312e);
  --mop-muted: var(--v2-muted, #63736d);
  --mop-accent: #0071e3;
  width: 100%; max-width: none; margin: 0; padding: 0 0 20px; gap: 12px;
}
.v2-mop-workspace :deep(.page-back-row:not(:has(button, a))) { display: none; }
.v2-mop-workspace :deep(.mop-head) { padding: 4px 0 14px; background: transparent; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; box-shadow: none; backdrop-filter: none; }
.v2-mop-workspace :deep(.mop-head strong) { color: var(--mop-ink); font-size: 23px; font-weight: 650; }
.v2-mop-workspace :deep(.scope-select) { white-space: nowrap; flex-shrink: 0; }
.v2-mop-workspace :deep(:is(button, input, select)) { font-family: inherit; letter-spacing: 0; }
.v2-mop-workspace :deep(:is(.btn, .refresh-mini, input, select)) { min-height: 36px; border-radius: 6px; font-size: 13px; font-weight: 500; }
.v2-mop-workspace :deep(:is(input, select)) { min-width: 0; max-width: 100%; background: #fff; border-color: var(--mop-line); }
.v2-mop-workspace :deep(.btn.blue) { background: var(--mop-accent); color: #fff; box-shadow: none; }
.v2-mop-workspace :deep(:is(button, input, select):focus-visible) { outline: 2px solid var(--mop-accent); outline-offset: 2px; }
.v2-mop-workspace :deep(.btn:hover:not(:disabled)) { transform: none; box-shadow: none; }
.v2-mop-workspace :deep(.notice-box) { border-radius: 8px; background: #fff; box-shadow: none; backdrop-filter: none; }

.v2-mop-workspace > :deep(.mop-flow-steps) { background: transparent; border: 0; padding: 0; box-shadow: none; gap: 12px; grid-template-columns: repeat(4, minmax(0, 1fr)); }
.v2-mop-workspace > :deep(.mop-flow-steps article) { padding: 6px 0; border: 0; border-radius: 0; background: transparent; grid-template-columns: 26px minmax(0, 1fr); box-shadow: none; }
.v2-mop-workspace > :deep(.mop-flow-steps article b) { width: 26px; height: 26px; border-radius: 50%; font-size: 12px; font-weight: 600; }
.v2-mop-workspace > :deep(.mop-flow-steps article.done b) { background:#087758;color:#fff; }
.v2-mop-workspace > :deep(.mop-flow-steps article strong) { font-size: 13px; font-weight: 550; color: var(--mop-ink); }
.v2-mop-workspace > :deep(.mop-flow-steps article small) { font-size: 12px; font-weight: 400; color: var(--mop-muted); }
.v2-mop-workspace :deep(.mop-summary) { display: flex; flex-wrap: wrap; gap: 8px 22px; padding: 9px 0 12px; border-bottom: 1px solid var(--mop-line); }
.v2-mop-workspace :deep(.mop-summary article) { display: flex; align-items: baseline; gap: 8px; border: 0; border-radius: 0; padding: 0; background: none; box-shadow: none; }
.v2-mop-workspace :deep(.mop-summary span) { color: var(--mop-muted); font-size: 12px; font-weight: 400; }
.v2-mop-workspace :deep(.mop-summary strong) { margin: 0; font-size: 17px; font-weight: 600; color: var(--mop-ink); font-variant-numeric: tabular-nums; }

.v2-mop-workspace :deep(.mop-layout) { container: mop-selection / inline-size; grid-template-columns: minmax(270px, .7fr) minmax(0, 1.6fr); gap: 18px; align-items: start; }
.v2-mop-workspace :deep(.panel) { min-width: 0; border: 0; border-radius: 0; background: transparent; box-shadow: none; padding: 0; }
.v2-mop-workspace :deep(.panel-head) { align-items: center; min-height: 32px; gap: 10px; }
.v2-mop-workspace :deep(.panel-head h2) { color: var(--mop-ink); font-size: 15px; font-weight: 600; }
.v2-mop-workspace :deep(.panel-head > span) { padding: 2px 7px; border: 0; border-radius: 4px; background: #e8edf2; color: var(--mop-muted); font-size: 12px; font-weight: 500; }
.v2-mop-workspace :deep(.notice-panel) { height: auto; min-height: 0; grid-template-rows: auto auto minmax(0, 1fr); padding-right: 18px; border-right: 1px solid var(--mop-line); gap: 10px; }
.v2-mop-workspace :deep(.notice-panel .filters) { grid-template-columns: minmax(0, 1fr); gap: 8px; }
.v2-mop-workspace :deep(.notice-list) { min-height: 220px; max-height: max(280px, calc(100dvh - 520px)); gap: 0; padding: 0; scrollbar-width: thin; overscroll-behavior: auto; }
.v2-mop-workspace :deep(.notice-row) { display: block; padding: 12px 10px 12px 14px; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; background: #fff; box-shadow: none; transform: none; transition: background-color 160ms ease, border-color 160ms ease; }
.v2-mop-workspace :deep(.notice-row.pending) { background: #fff; }
.v2-mop-workspace :deep(.notice-row:hover) { background: #f3f7fc; box-shadow: none; transform: none; }
.v2-mop-workspace :deep(.notice-row.active) { background: #eaf3ff; box-shadow: inset 3px 0 var(--mop-accent); }
.v2-mop-workspace :deep(.notice-row::before) { display: none; }
.v2-mop-workspace :deep(.notice-row strong) { margin-top: 5px; line-height: 1.55; font-size: 13px; font-weight: 550; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(:is(.notice-row small, .notice-mop-tags em)) { font-size: 12px; font-weight: 400; line-height: 1.5; }
.v2-mop-workspace :deep(.notice-row small),.v2-mop-workspace :deep(.preview-title p),.v2-mop-workspace :deep(.detect-summary span.muted) { color:var(--mop-muted); }
.v2-mop-workspace :deep(.notice-mop-tags em) { border: 0; padding: 2px 5px; border-radius: 4px; }
.v2-mop-workspace :deep(.row-status) { min-height: 20px; border-radius: 4px; font-weight: 500; padding: 2px 6px; }
.v2-mop-workspace :deep(.empty-box) { min-height: 170px; border: 0; border-radius: 0; background: transparent; font-weight: 400; }

.v2-mop-workspace :deep(.binding-panel) { min-height: 0; grid-template-columns: minmax(0, 1fr) minmax(220px, .75fr); grid-template-rows: auto auto auto auto auto minmax(100px, 1fr); grid-template-areas: 'heading heading' 'notice notice' 'upload chosen' 'search chosen' 'count chosen' 'candidates chosen'; gap: 10px 16px; align-content: start; }
.v2-mop-workspace :deep(.binding-panel > .panel-head) { grid-area: heading; }
.v2-mop-workspace :deep(.binding-panel > .mop-flow-steps) { display: none; }
.v2-mop-workspace :deep(.selected-notice) { grid-area: notice; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; padding: 4px 0 12px; background: transparent; }
.v2-mop-workspace :deep(.selected-notice strong) { color: var(--mop-ink); font-weight: 600; font-size: 16px; line-height: 1.6; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(.selected-notice :is(span, em)) { padding: 2px 0; font-weight: 500; font-size: 12px; background: transparent; }
.v2-mop-workspace :deep(.selected-notice p) { line-height: 1.6; color: var(--mop-muted); }
.v2-mop-workspace :deep(.binding-panel > .search-field) { grid-area: search; min-width: 0; font-weight: 500; font-size: 12px; }
.v2-mop-workspace :deep(.candidate-summary) { grid-area: count; border: 0; border-radius: 0; padding: 0; background: transparent; font-weight: 400; font-size: 12px; }
.v2-mop-workspace :deep(.candidate-summary strong) { font-weight: 500; }
.v2-mop-workspace :deep(.mop-candidate-list) { grid-area: candidates; height: auto !important; min-height: 0; max-height: max(100px, calc(100dvh - 600px)) !important; overflow-y: auto !important; gap: 0; padding: 0; scrollbar-width: thin; overscroll-behavior: auto; }
.v2-mop-workspace :deep(.binding-panel > .empty-box) { grid-area: candidates; }
.v2-mop-workspace :deep(.mop-row) { display: block; width: 100%; min-width: 0; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; padding: 11px 10px; background: #fff; box-shadow: none; transform: none; text-align: left; }
.v2-mop-workspace :deep(.mop-row:hover) { background: #f3f7fc; box-shadow: none; transform: none; }
.v2-mop-workspace :deep(.mop-row.active) { background: #eaf3ff; box-shadow: inset 3px 0 var(--mop-accent); }
.v2-mop-workspace :deep(.mop-row strong) { font-size: 13px; font-weight: 550; line-height: 1.55; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(.mop-row small) { line-height: 1.5; font-weight: 400; color: var(--mop-muted); }
.v2-mop-workspace :deep(.mop-row :is(em, .mop-selected-mark)) { font-weight: 500; border-radius: 4px; }
.v2-mop-workspace :deep(.mop-selected-file-card) { grid-area: chosen; align-self: start; display: flex; flex-direction: column; gap: 14px; padding: 14px; border: 1px solid var(--mop-line); border-radius: 8px; background: #fff; box-shadow: none; min-width: 0; }
.v2-mop-workspace :deep(.selected-file-main) { gap: 6px; }
.v2-mop-workspace :deep(.selected-file-main strong) { font-size: 14px; font-weight: 600; line-height: 1.6; }
.v2-mop-workspace :deep(:is(.selected-file-main span, .attachment-select span, .attachment-select select)) { font-size: 12px; font-weight: 500; }
.v2-mop-workspace :deep(.attachment-count) { display: none; }
.v2-mop-workspace :deep(.selected-file-actions) { display: flex; flex-direction: column; align-items: stretch; padding-top: 12px; border-color: var(--mop-line); gap: 8px; }
.v2-mop-workspace :deep(.selected-file-actions .btn) { width: 100%; min-height: 40px; }
.v2-mop-workspace :deep(.local-mop-upload) { grid-area: upload; min-height: 68px; border: 1px dashed #a9bfd6; border-radius: 8px; padding: 10px 12px; background: #f7fafc; box-shadow: none; }
.v2-mop-workspace :deep(.local-upload-icon) { width: 32px; height: 32px; border-radius: 6px; background: #e8f2ff; color: var(--mop-accent); box-shadow: none; }
.v2-mop-workspace :deep(.local-upload-copy strong) { font-size: 13px; font-weight: 550; }
.v2-mop-workspace :deep(.local-upload-copy small) { font-size: 12px; font-weight: 400; line-height: 1.5; }
.v2-mop-workspace :deep(.local-upload-button) { border-radius: 6px; min-height: 36px; font-weight: 500; }

.v2-mop-workspace.preview-open { display: flex; flex-direction: column; height: calc(100dvh - var(--head-height, 60px) - 48px); padding: 0; }
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
.v2-mop-workspace :deep(.signature-manager-backdrop) { backdrop-filter: none; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open) { border-radius: 8px; background: #fff; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open .sign-panel-head) { border-radius: 8px 8px 0 0; background: #f6f8fa; backdrop-filter: none; }

@media (min-width: 901px) and (max-width: 1250px) {
  .v2-mop-workspace :deep(.mop-layout) { grid-template-columns: 280px minmax(0, 1fr); }
  .v2-mop-workspace :deep(.binding-panel) { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'notice' 'chosen' 'upload' 'search' 'count' 'candidates'; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { display: grid; grid-template-columns: minmax(0, 1fr) minmax(180px, .8fr); gap: 10px; }
  .v2-mop-workspace :deep(.selected-file-actions) { grid-column: 1 / -1; padding-top: 8px; }
}
@media (max-width: 900px) {
  .v2-mop-workspace :deep(.mop-layout) { grid-template-columns: minmax(0, 1fr); gap: 20px; }
  .v2-mop-workspace :deep(.notice-panel) { padding: 0; border-right: 0; }
  .v2-mop-workspace :deep(.notice-list) { max-height: 300px; min-height: 100px; }
  .v2-mop-workspace :deep(.binding-panel) { scroll-margin-top: calc(var(--head-height, 68px) + 12px); }
  /* Display changes preserve the list, selectors and the controller's in-memory draft. */
  .v2-mop-workspace :deep(.mop-summary.v2-mop-summary-in-work) { display: none; }
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] > .notice-panel),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] > .binding-panel),
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .v2-page-canvas:not(.is-customizing) [data-widget-id="block_notices"]),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] .v2-page-canvas:not(.is-customizing) [data-widget-id="block_binding"]),
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .v2-embedded-block:has(>.notice-panel)),
  .v2-mop-workspace :deep([data-v2-mop-panel="list"] .v2-embedded-block:has(>.binding-panel)) { display: none !important; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) > .v2-widget-grid) { display: flex !important; flex-direction: column; height: auto !important; min-height: 0 !important; gap: 12px; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) .grid-stack-item) { position: relative !important; inset: auto !important; width: 100% !important; height: auto !important; min-height: 0 !important; transform: none !important; }
  .v2-mop-workspace :deep([data-v2-mop-panel] .v2-page-canvas:not(.is-customizing) :is(.v2-widget,.v2-widget-body,.v2-module-block)) { position: relative; inset: auto; height: auto !important; min-height: 0 !important; max-height: none !important; overflow: visible; }
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .selected-notice) { display: none; }
  .v2-mop-workspace :deep([data-v2-mop-panel="work"] .binding-panel) { grid-template-columns: minmax(0,1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'chosen' 'upload' 'search' 'count' 'candidates'; }
  .v2-mop-workspace :deep(.selected-file-main .v2-mop-attachment-repeated) { display: none; }
  .v2-mop-workspace :deep(.selected-file-main strong) { display: block; overflow: visible; -webkit-line-clamp: unset; overflow-wrap: anywhere; }
}
@container mop-selection (max-width: 680px) {
  .v2-mop-workspace :deep(.binding-panel) { grid-template-columns: minmax(0,1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'notice' 'chosen' 'upload' 'search' 'count' 'candidates'; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { display: flex; }
  .v2-mop-workspace :deep(.mop-candidate-list) { height: auto; max-height: 280px; }
}
@media (min-width: 1251px) and (max-height: 820px) {
  .v2-mop-workspace.engineer-mop { gap: 8px; }
  .v2-mop-workspace :deep(.mop-head) { padding-bottom: 8px; }
  .v2-mop-workspace > :deep(.mop-flow-steps article) { padding: 2px 0; }
  .v2-mop-workspace > :deep(.mop-flow-steps article small) { display: none; }
  .v2-mop-workspace :deep(.mop-summary) { padding: 6px 0 8px; }
  .v2-mop-workspace :deep(.notice-list) { min-height: 180px; max-height: max(180px,calc(100dvh - 480px)); }
  .v2-mop-workspace :deep(.mop-candidate-list) { min-height: 100px; max-height: max(100px,calc(100dvh - 660px)); }
  .v2-mop-workspace :deep(.binding-panel) { gap: 8px 14px; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { padding: 10px; gap: 6px; }
  .v2-mop-workspace :deep(.selected-file-actions) { padding-top: 8px; }
  .v2-mop-workspace :deep(.selected-file-actions .btn) { min-height: 36px; }
  .v2-mop-workspace :deep(.local-mop-upload) { min-height: 56px; padding: 8px 10px; }
}
@media (max-width: 600px) {
  .v2-mop-workspace.engineer-mop { gap: 10px; padding-bottom: 12px; }
  .v2-mop-workspace :deep(.mop-head) { gap: 8px; padding-bottom: 10px; }
  .v2-mop-workspace :deep(.mop-head strong) { font-size: 19px; }
  .v2-mop-workspace > :deep(.mop-flow-steps) { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 14px; }
  .v2-mop-workspace :deep(.mop-summary) { gap: 8px 16px; padding: 8px 0; }
  .v2-mop-workspace :deep(.mop-summary strong) { font-size: 16px; }
  .v2-mop-workspace :deep(.notice-panel .filters) { grid-template-columns: minmax(0, 1fr); }
  .v2-mop-workspace :deep(.binding-panel) { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'notice' 'chosen' 'upload' 'search' 'count' 'candidates'; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { scroll-margin-top: calc(var(--head-height, 68px) + 12px); }
  .v2-mop-workspace :deep(.mop-candidate-list) { height: auto; min-height: 0; max-height: 320px; }
  .v2-mop-workspace :deep(.local-mop-upload) { grid-template-columns: 32px minmax(0, 1fr); }
  .v2-mop-workspace :deep(.local-upload-button) { grid-column: 1 / -1; }
  .v2-mop-workspace :deep(:is(.btn, .refresh-mini, input, select, .sheet-tabs button)) { min-height: 44px; }
  .v2-mop-workspace :deep(.preview-title) { min-width: 100%; }
  .v2-mop-workspace :deep(.preview-head-status) { justify-content: flex-start; }
  .v2-mop-workspace :deep(.floating-upload-action-row) { display: grid; grid-template-columns: minmax(0,1fr) minmax(0,1.4fr); }
  .v2-mop-workspace :deep(.mop-footer-signatures) { grid-column: 1 / -1; width: 100%; }
  .v2-mop-workspace :deep(.floating-uploaded-at) { grid-column: 1 / -1; max-width: none; }
}
@media (prefers-reduced-motion: reduce) {
  .v2-mop-workspace :deep(:is(.notice-row, .mop-row, .local-mop-upload)) { transition: none; }
}
@media (max-width: 760px) {
  .v2-mop-workspace.preview-open { height: calc(100dvh - var(--head-height, 56px) - 94px - env(safe-area-inset-bottom)); padding: 0; }
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
