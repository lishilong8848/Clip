<template>
  <div v-if="open" class="signature-pad-backdrop" @click.self="emit('close')">
    <section class="signature-pad-modal" role="dialog" aria-modal="true" aria-label="手写签名">
      <header>
        <div>
          <strong>{{ title || "手写签名" }}</strong>
        </div>
        <button type="button" class="btn ghost" :disabled="saving" @click="emit('close')">关闭</button>
      </header>
      <slot />
      <footer>
        <span class="sign-status" :class="{ failed: messageType === 'failed', success: messageType === 'success' }">
          {{ message || saveDisabledReason || roleLabel }}
        </span>
        <div class="signature-pad-actions">
          <button class="btn ghost" type="button" :disabled="saving" @click="emit('clear')">清空</button>
          <button
            class="btn blue"
            type="button"
            :disabled="saving || Boolean(saveDisabledReason)"
            :title="saveDisabledReason"
            @click="emit('save')"
          >
            {{ saving ? "保存中" : "保存签名" }}
          </button>
        </div>
      </footer>
    </section>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  open: boolean;
  title: string;
  roleLabel: string;
  saving: boolean;
  message: string;
  messageType: string;
  saveDisabledReason: string;
}>();

const emit = defineEmits<{
  close: [];
  clear: [];
  save: [];
}>();
</script>

<style scoped>
.signature-pad-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-signature-pad, 920);
  display: grid;
  place-items: center;
  padding: 18px;
  background: rgba(7, 20, 48, 0.42);
  backdrop-filter: blur(7px);
}

.signature-pad-modal {
  width: min(980px, calc(100vw - 36px));
  max-height: calc(100vh - 36px);
  display: grid;
  grid-template-rows: auto minmax(280px, 1fr) auto;
  overflow: hidden;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: #ffffff;
  box-shadow: 0 28px 76px rgba(0, 47, 135, 0.24);
}

.signature-pad-modal header,
.signature-pad-modal footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 10px 12px;
  background: #f8fbff;
}

.signature-pad-modal header {
  border-bottom: 1px solid #d8e5f7;
}

.signature-pad-modal header strong {
  display: block;
  color: #09204a;
  font-size: 15px;
  font-weight: 900;
}

.signature-pad-modal header p {
  margin: 2px 0 0;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
}

.signature-pad-modal footer {
  border-top: 1px solid #d8e5f7;
}

.sign-status {
  flex: 1 1 auto;
  min-width: 0;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 6px 10px;
  background: #ffffff;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.25;
  display: -webkit-box;
  overflow: hidden;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.sign-status.success {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.sign-status.failed {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #b45309;
}

.signature-pad-actions {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
}

.btn {
  min-height: 32px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 0 11px;
  background: rgba(255, 255, 255, 0.94);
  color: #1d4ed8;
  font: inherit;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(30, 99, 255, 0.22);
}

.btn.ghost {
  background: #ffffff;
}

@media (max-width: 720px) {
  .signature-pad-backdrop {
    padding: 10px;
  }

  .signature-pad-modal {
    width: calc(100vw - 20px);
    max-height: calc(100vh - 20px);
  }

  .signature-pad-modal header,
  .signature-pad-modal footer {
    align-items: stretch;
    flex-direction: column;
  }

  .signature-pad-modal footer .btn {
    width: 100%;
  }

  .signature-pad-actions {
    width: 100%;
    display: grid;
    grid-template-columns: 1fr 1fr;
  }
}
</style>
