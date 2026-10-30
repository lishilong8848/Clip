<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';
import { MoreHorizontal } from 'lucide-vue-next';

const root = ref<HTMLDetailsElement>();
let openedDialog: HTMLDialogElement | null = null;
function detachDialog() {
  openedDialog?.removeEventListener('close', restoreAfterDialog);
  openedDialog = null;
}
function restoreAfterDialog() {
  const reopened = openedDialog?.open;
  detachDialog();
  // The original action is now hidden inside the collapsed menu. Return to
  // its visible trigger after closing, without stealing focus from a dialog.
  if (!reopened && !document.querySelector('dialog:modal')) root.value?.querySelector('summary')?.focus();
}
function close(restoreFocus = false) {
  if (!root.value?.open) return;
  root.value.open = false;
  if (restoreFocus) root.value.querySelector('summary')?.focus();
}
function outside(event: PointerEvent) {
  if (event.target instanceof Node && !root.value?.contains(event.target)) close();
}
function finish(event: MouseEvent) {
  const button = event.target instanceof Element ? event.target.closest('button') : null;
  // Return keyboard focus before hiding the action, unless its original handler
  // has already moved focus into a dialog. Never activate a disabled action.
  if (button && !button.disabled) {
    detachDialog();
    openedDialog = root.value?.closest('.signature-management')?.querySelector<HTMLDialogElement>('dialog[open]') || null;
    openedDialog?.addEventListener('close', restoreAfterDialog, { once: true });
    close(!openedDialog && !!root.value?.contains(document.activeElement));
  }
}
onMounted(() => document.addEventListener('pointerdown', outside));
onBeforeUnmount(() => { document.removeEventListener('pointerdown', outside); detachDialog(); });
</script>

<template>
  <details ref="root" class="v2-signature-more" @keydown.esc.stop.prevent="close(true)">
    <summary><MoreHorizontal :size="17" aria-hidden="true" />更多管理</summary>
    <div class="v2-signature-more-panel" role="group" aria-label="签名管理操作" @click="finish">
      <slot />
    </div>
  </details>
</template>

<style scoped>
.v2-signature-more { position: relative; flex: none; }
summary { display: flex; align-items: center; justify-content: center; gap: 6px; min-height: 36px; padding: 6px 10px; border: 1px solid var(--v2-border); border-radius: 8px; background: var(--v2-palette-surface); color: var(--v2-ink); font-size: 13px; cursor: pointer; list-style: none; }
summary::-webkit-details-marker { display: none; }
summary:hover, details[open] > summary { border-color: var(--v2-blue); color: var(--v2-blue); }
summary:focus-visible { outline: 2px solid var(--v2-blue); outline-offset: 2px; }
.v2-signature-more-panel { position: absolute; top: calc(100% + 6px); right: 0; z-index: 30; display: grid; gap: 8px; width: max-content; max-width: calc(100vw - 40px); padding: 8px; border: 1px solid var(--v2-border); border-radius: 10px; background: var(--v2-palette-surface); box-shadow: 0 8px 24px #0002; }
.v2-signature-more-panel :slotted(button) { width: 100%; justify-content: flex-start; min-height: 44px; white-space: normal; }
@media (max-width: 760px) { summary { min-height: 44px; } }
</style>
