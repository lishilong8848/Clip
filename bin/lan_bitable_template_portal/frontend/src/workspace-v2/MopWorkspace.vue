<template>
  <EngineerMopPage v-bind="props" class="v2-mop-workspace" @click="followSelection" />
</template>

<script setup lang="ts">
import { nextTick } from 'vue';
import EngineerMopPage from '../components/EngineerMopPage.vue';

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: Array<{ value: string; label: string }>;
}>();

async function followSelection(event: MouseEvent) {
  if (!matchMedia('(max-width: 900px)').matches || !(event.target instanceof Element)) return;
  const root = event.currentTarget as HTMLElement;
  const target = event.target.closest('.notice-row') ? '.binding-panel' :
    event.target.closest('.mop-row') ? '.mop-selected-file-card' : '';
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
.v2-mop-workspace > :deep(.mop-flow-steps article strong) { font-size: 13px; font-weight: 550; color: var(--mop-ink); }
.v2-mop-workspace > :deep(.mop-flow-steps article small) { font-size: 12px; font-weight: 400; color: var(--mop-muted); }
.v2-mop-workspace :deep(.mop-summary) { display: flex; flex-wrap: wrap; gap: 8px 22px; padding: 9px 0 12px; border-bottom: 1px solid var(--mop-line); }
.v2-mop-workspace :deep(.mop-summary article) { display: flex; align-items: baseline; gap: 8px; border: 0; border-radius: 0; padding: 0; background: none; box-shadow: none; }
.v2-mop-workspace :deep(.mop-summary span) { color: var(--mop-muted); font-size: 12px; font-weight: 400; }
.v2-mop-workspace :deep(.mop-summary strong) { margin: 0; font-size: 17px; font-weight: 600; color: var(--mop-ink); font-variant-numeric: tabular-nums; }

.v2-mop-workspace :deep(.mop-layout) { grid-template-columns: minmax(270px, .7fr) minmax(0, 1.6fr); gap: 18px; align-items: stretch; }
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
.v2-mop-workspace :deep(.notice-mop-tags em) { border: 0; padding: 2px 5px; border-radius: 4px; }
.v2-mop-workspace :deep(.row-status) { min-height: 20px; border-radius: 4px; font-weight: 500; padding: 2px 6px; }
.v2-mop-workspace :deep(.empty-box) { min-height: 170px; border: 0; border-radius: 0; background: transparent; font-weight: 400; }

.v2-mop-workspace :deep(.binding-panel) { min-height: 0; grid-template-columns: minmax(0, 1fr) minmax(220px, .75fr); grid-template-rows: auto auto auto auto minmax(180px, 1fr) auto; grid-template-areas: 'heading heading' 'notice notice' 'search chosen' 'count chosen' 'candidates chosen' 'upload upload'; gap: 10px 16px; align-content: start; }
.v2-mop-workspace :deep(.binding-panel > .panel-head) { grid-area: heading; }
.v2-mop-workspace :deep(.binding-panel > .mop-flow-steps) { display: none; }
.v2-mop-workspace :deep(.selected-notice) { grid-area: notice; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; padding: 4px 0 12px; background: transparent; }
.v2-mop-workspace :deep(.selected-notice strong) { color: var(--mop-ink); font-weight: 600; font-size: 16px; line-height: 1.6; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(.selected-notice :is(span, em)) { padding: 2px 0; font-weight: 500; font-size: 12px; background: transparent; }
.v2-mop-workspace :deep(.selected-notice p) { line-height: 1.6; color: var(--mop-muted); }
.v2-mop-workspace :deep(.binding-panel > .search-field) { grid-area: search; min-width: 0; font-weight: 500; font-size: 12px; }
.v2-mop-workspace :deep(.candidate-summary) { grid-area: count; border: 0; border-radius: 0; padding: 0; background: transparent; font-weight: 400; font-size: 12px; }
.v2-mop-workspace :deep(.candidate-summary strong) { font-weight: 500; }
.v2-mop-workspace :deep(.mop-candidate-list) { grid-area: candidates; height: 100%; min-height: 180px; max-height: max(180px, calc(100dvh - 700px)); gap: 0; padding: 0; scrollbar-width: thin; overscroll-behavior: auto; }
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

.v2-mop-workspace.preview-open { padding: 0 0 20px; }
.v2-mop-workspace :deep(.mop-preview-page) { min-height: 0; width: 100%; padding: 0; gap: 10px; --mop-upload-footer-safe-space: 0px; }
.v2-mop-workspace :deep(.preview-head) { display: flex; flex-wrap: wrap; padding: 4px 0 12px; gap: 10px 18px; border: 0; border-bottom: 1px solid var(--mop-line); border-radius: 0; background: transparent; box-shadow: none; }
.v2-mop-workspace :deep(.preview-title) { flex: 1; min-width: min(320px, 100%); }
.v2-mop-workspace :deep(.preview-title strong) { font-size: 17px; font-weight: 600; white-space: normal; line-height: 1.5; overflow-wrap: anywhere; }
.v2-mop-workspace :deep(.preview-title p) { font-size: 12px; font-weight: 400; line-height: 1.6; white-space: normal; }
.v2-mop-workspace :deep(.preview-head-status span) { font-size: 12px; font-weight: 500; border-radius: 4px; max-width: none; padding: 3px 6px; }
.v2-mop-workspace :deep(:is(.mop-file-status, .mop-detect-panel, .sheet-tabs, .floating-upload-box, .mop-sign-panel:not(.manager-open))) { border-color: var(--mop-line); border-radius: 6px; background: #fff; box-shadow: none; }
.v2-mop-workspace :deep(.mop-file-status) { padding: 7px 10px; }
.v2-mop-workspace :deep(.mop-file-status strong) { font-size: 12px; font-weight: 500; }
.v2-mop-workspace :deep(.mop-sign-panel:not(.manager-open)) { padding: 8px 10px; }
.v2-mop-workspace :deep(.mop-sign-panel:not(.manager-open)::before) { display: none; }
.v2-mop-workspace :deep(.sheet-tabs button) { border: 0; border-radius: 4px; min-height: 34px; font-size: 13px; font-weight: 500; background: transparent; }
.v2-mop-workspace :deep(.sheet-tabs button.active) { color: var(--mop-accent); background: #eaf3ff; box-shadow: none; }
.v2-mop-workspace :deep(.table-fill-toolbar) { padding: 6px 8px; background: #fff; box-shadow: none; backdrop-filter: none; }
.v2-mop-workspace :deep(.table-fill-title strong) { padding: 0; background: none; color: var(--mop-ink); font-weight: 600; }
.v2-mop-workspace :deep(.table-fill-title span) { font-weight: 400; }
.v2-mop-workspace :deep(.table-fill-actions em) { border-radius: 4px; font-weight: 500; }
.v2-mop-workspace :deep(.preview-scroll) { min-height: 260px; max-height: max(260px, calc(100dvh - var(--head-height, 74px) - 370px)); overflow: auto; padding-bottom: 0; border-color: var(--mop-line); border-radius: 6px; box-shadow: none; scrollbar-width: thin; overscroll-behavior: auto; }
.v2-mop-workspace :deep(.floating-upload-box) { margin: 0; padding: 12px; gap: 10px 16px; }
.v2-mop-workspace :deep(.floating-upload-action-row) { gap: 8px; }
.v2-mop-workspace :deep(.floating-upload-title) { font-size: 13px; font-weight: 600; }
.v2-mop-workspace :deep(.floating-upload-summary) { justify-content: flex-start; }
.v2-mop-workspace :deep(.signature-manager-backdrop) { backdrop-filter: none; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open) { border-radius: 8px; background: #fff; }
.v2-mop-workspace :deep(.mop-sign-panel.manager-open .sign-panel-head) { border-radius: 8px 8px 0 0; background: #f6f8fa; backdrop-filter: none; }

@media (min-width: 901px) and (max-width: 1250px) {
  .v2-mop-workspace :deep(.mop-layout) { grid-template-columns: 280px minmax(0, 1fr); }
  .v2-mop-workspace :deep(.binding-panel) { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'notice' 'chosen' 'search' 'count' 'candidates' 'upload'; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { display: grid; grid-template-columns: minmax(0, 1fr) minmax(180px, .8fr); gap: 10px; }
  .v2-mop-workspace :deep(.selected-file-actions) { grid-column: 1 / -1; padding-top: 8px; }
}
@media (max-width: 900px) {
  .v2-mop-workspace :deep(.mop-layout) { grid-template-columns: minmax(0, 1fr); gap: 20px; }
  .v2-mop-workspace :deep(.notice-panel) { padding: 0; border-right: 0; }
  .v2-mop-workspace :deep(.notice-list) { max-height: 300px; min-height: 100px; }
  .v2-mop-workspace :deep(.binding-panel) { scroll-margin-top: calc(var(--head-height, 68px) + 12px); }
}
@media (max-width: 600px) {
  .v2-mop-workspace.engineer-mop { gap: 10px; padding-bottom: 12px; }
  .v2-mop-workspace :deep(.mop-head) { gap: 8px; padding-bottom: 10px; }
  .v2-mop-workspace :deep(.mop-head strong) { font-size: 19px; }
  .v2-mop-workspace > :deep(.mop-flow-steps) { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 14px; }
  .v2-mop-workspace :deep(.mop-summary) { gap: 8px 16px; padding: 8px 0; }
  .v2-mop-workspace :deep(.mop-summary strong) { font-size: 16px; }
  .v2-mop-workspace :deep(.notice-panel .filters) { grid-template-columns: minmax(0, 1fr); }
  .v2-mop-workspace :deep(.binding-panel) { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto; grid-template-areas: 'heading' 'notice' 'chosen' 'search' 'count' 'candidates' 'upload'; }
  .v2-mop-workspace :deep(.mop-selected-file-card) { scroll-margin-top: calc(var(--head-height, 68px) + 12px); }
  .v2-mop-workspace :deep(.mop-candidate-list) { height: auto; min-height: 0; max-height: 320px; }
  .v2-mop-workspace :deep(.local-mop-upload) { grid-template-columns: 32px minmax(0, 1fr); }
  .v2-mop-workspace :deep(.local-upload-button) { grid-column: 1 / -1; }
  .v2-mop-workspace :deep(:is(.btn, .refresh-mini, input, select, .sheet-tabs button)) { min-height: 44px; }
  .v2-mop-workspace :deep(.preview-title) { min-width: 100%; }
  .v2-mop-workspace :deep(.preview-head-status) { justify-content: flex-start; }
  .v2-mop-workspace :deep(.preview-scroll) { min-height: 300px; max-height: 58dvh; }
  .v2-mop-workspace :deep(.floating-upload-box) { grid-template-columns: minmax(0, 1fr); padding: 10px; }
  .v2-mop-workspace :deep(.floating-upload-compact) { grid-template-columns: minmax(0, 1fr) auto; row-gap: 8px; }
  .v2-mop-workspace :deep(.floating-upload-summary) { grid-row: 2; grid-column: 1 / -1; }
  .v2-mop-workspace :deep(.floating-upload-action-row) { flex-wrap: wrap; }
}
@media (prefers-reduced-motion: reduce) {
  .v2-mop-workspace :deep(:is(.notice-row, .mop-row, .local-mop-upload)) { transition: none; }
}
</style>
