<script setup lang="ts">
import { NModal } from 'naive-ui';
import { X } from 'lucide-vue-next';

defineProps<{ show: boolean; title: string; context: string; wide?: boolean; locked?: boolean }>();
const emit = defineEmits<{ dismiss: [] }>();
</script>

<template>
  <NModal :show="show" :mask-closable="!locked" :close-on-esc="!locked"
    @update:show="value => { if (!value && !locked) emit('dismiss'); }">
    <section class="zh-dialog" :class="{ wide }" role="dialog" aria-modal="true" :aria-label="title" tabindex="-1">
      <header class="zh-dialog-head">
        <div>
          <p>{{ context }}</p>
          <h2>{{ title }}</h2>
        </div>
        <div class="zh-dialog-tools">
          <slot name="actions" />
        <button type="button" class="zh-close" aria-label="关闭弹窗" :disabled="locked" @click="emit('dismiss')">
          <X :size="18" aria-hidden="true" />
        </button>
        </div>
      </header>
      <div class="zh-dialog-body"><slot /></div>
    </section>
  </NModal>
</template>

<style scoped>
.zh-dialog {
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  width: min(590px, calc(100vw - 28px));
  max-height: calc(100vh - 36px);
  max-height: calc(100dvh - 36px);
  margin: 18px auto;
  overflow: hidden;
  color: var(--v2-ink, #172f40);
  background: var(--soft-face, #f0f4f6);
  border: 1px solid rgba(255,255,255,.88);
  border-radius: 22px;
  box-shadow: 0 22px 72px rgba(26,54,67,.24);
}
.zh-dialog.wide { width: min(1110px, calc(100vw - 28px)); }
.zh-dialog-head { flex: none; display: flex; align-items: center; gap: 6px 12px; flex-wrap:wrap; padding: 12px 16px; }
.zh-dialog-tools { display:flex;align-items:center;gap:6px;margin-inline-start:auto; }
.zh-dialog-head { justify-content: space-between; background: linear-gradient(106deg,#dfedf9,#e6f4ed); border-bottom: 1px solid #d5e2e4; }
.zh-dialog-head p { margin: 0 0 2px; font-size: 12px; color: #486677; }
.zh-dialog-head h2 { margin: 0; font-size: 20px; font-weight: 700; line-height: 1.35; }
.zh-close { display: grid; place-items: center; width: 36px; height: 36px; padding: 0; border: 1px solid #c8d9d7; border-radius: 50%; background: #f5faf8; color: #37596a; cursor: pointer; }
.zh-dialog-head button:focus-visible { outline: 2px solid #257294; outline-offset: 2px; }
.zh-dialog-head button:disabled { opacity: .5; cursor: wait; }
.zh-dialog-body { min-height: 0; overflow: auto; overscroll-behavior: contain; padding: 16px 20px; }
@media (max-width: 620px) {
  .zh-dialog, .zh-dialog.wide { width: calc(100vw - 16px); max-height: calc(100dvh - 18px); margin: 9px auto; border-radius: 16px; }
  .zh-dialog-head, .zh-dialog-body { padding: 10px 12px; }
  .zh-close { width: 40px; height: 40px; }
}
</style>
