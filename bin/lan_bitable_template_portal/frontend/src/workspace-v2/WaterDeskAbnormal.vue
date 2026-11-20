<script setup lang="ts">
import { NModal } from 'naive-ui'

const props = defineProps<{
    show: boolean
    note: string
    message: string
    error: string
    saving: boolean
    previousDate: string
    oldReading: string
    newReading: string
    change: string
}>()

const emit = defineEmits<{
  (e: 'cancel'): void
  (e: 'confirm'): void
  (e: 'update:note', value: string): void
}>()

const title = '异常读数确认'

function handleCancel() {
  if (props.saving) return
  emit('cancel')
}

function handleSubmit() {
  if (props.saving) return
  emit('confirm')
}
</script>

<template>
  <NModal
    class="water-abnormal-sheet"
    :show="show"
    @update:show="visible => { if (!visible) handleCancel() }"
    preset="card"
    :title="title"
    :mask-closable="!saving"
    :close-on-esc="!saving"
    :closable="!saving"
    :bordered="false"
    transform-origin="center"
    :style="{ width: 'min(440px, calc(100vw - 24px))', maxHeight: '92dvh', overflowY: 'auto' }"
  >
    <form class="abnormal-form" novalidate @submit.prevent="handleSubmit">
      <p v-if="message" class="line line--message" role="status">{{ message }}</p>
      <p v-if="error" class="line line--error" role="alert">{{ error }}</p>

      <section class="compare" :aria-label="title">
        <p class="compare__date">
          <span class="compare__label">上期统计日期</span>
          <span class="compare__value compare__value--date">{{ previousDate }}</span>
        </p>
        <dl class="compare__grid">
          <div class="compare__cell">
            <dt class="compare__label">上期读数</dt>
            <dd class="compare__value">{{ oldReading }}</dd>
          </div>
          <div class="compare__cell">
            <dt class="compare__label">本次读数</dt>
            <dd class="compare__value">{{ newReading }}</dd>
          </div>
          <div class="compare__cell compare__cell--accent">
            <dt class="compare__label">变化</dt>
            <dd class="compare__value">{{ change }}</dd>
          </div>
        </dl>
      </section>

      <label class="note">
        <span class="note__label">异常备注</span>
        <textarea
          :value="note"
          aria-label="异常备注"
          :disabled="saving"
          rows="3" maxlength="1000"
          placeholder="填写变化原因或现场核查情况"
          @input="emit('update:note', ($event.target as HTMLTextAreaElement).value)"
        ></textarea>
      </label>

      <div class="actions">
        <button type="button" class="btn btn--ghost" :disabled="saving" @click="handleCancel">
          返回核对
        </button>
        <button type="submit" class="btn btn--primary" :disabled="saving" :aria-busy="saving">
          {{ saving ? '保存中' : '确认并保存' }}
        </button>
      </div>
    </form>
  </NModal>
</template>

<style scoped>
.abnormal-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.line {
  margin: 0;
  padding: 8px 10px;
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.5;
  overflow-wrap: anywhere;
}

.line--message {
  background: #eaf4f4;
  color: #22595a;
}

.line--error {
  background: #fdecec;
  color: #a02424;
}

.compare {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 10px;
  border-radius: 10px;
  background: #f5f7f7;
  border: 1px solid #e3e8e7;
}

.compare__date {
  margin: 0;
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 6px;
  font-size: 12px;
}

.compare__grid {
  margin: 0;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.compare__cell {
  margin: 0;
  min-width: 0;
  padding: 6px 8px;
  border-radius: 8px;
  background: #ffffff;
  border: 1px solid #e6ecea;
}

.compare__cell--accent {
  grid-column: 1 / -1;
  background: #e6f4ee;
  border-color: #cfe8dd;
}

.compare__label {
  display: block;
  font-size: 12px;
  color: #5c6b68;
  letter-spacing: 0.02em;
}

.compare__value {
  display: block;
  margin: 2px 0 0;
  font-size: 15px;
  font-weight: 600;
  color: #1f2b29;
  font-variant-numeric: tabular-nums;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.compare__value--date {
  margin: 0;
  font-size: 13px;
  font-weight: 500;
}

.note {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.note__label {
  font-size: 12px;
  color: #5c6b68;
}
.note textarea {
  width: 100%;
  box-sizing: border-box;
  border: 1px solid #96ada8;
  border-radius: 8px;
  padding: 9px;
  resize: vertical;
  color: inherit;
  font: inherit;
  background: var(--soft-field, #f3f6f6);
}
.note textarea:focus-visible { outline: 2px solid var(--v2-blue, #34769d); outline-offset: 2px; }

.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  justify-content: flex-end;
}

.btn {
  flex: 1 1 120px;
  min-height: 44px;
  padding: 0 14px;
  border-radius: 8px;
  border: 1px solid transparent;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: background-color 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease;
}

.btn--ghost {
  background: #ffffff;
  border-color: #d6dedb;
  color: #3d4b48;
}

.btn--ghost:hover:not(:disabled) {
  background: #f3f6f5;
}

.btn--primary {
  background: #1b7a4f;
  color: #ffffff;
}

.btn--primary:hover:not(:disabled) {
  background: #166542;
}

.btn:focus-visible {
  outline: 2px solid #1b7a4f;
  outline-offset: 2px;
  box-shadow: 0 0 0 4px rgba(27, 122, 79, 0.18);
}

.btn--ghost:focus-visible {
  outline-color: #2b6d7a;
  box-shadow: 0 0 0 4px rgba(43, 109, 122, 0.18);
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

@media (max-width: 360px) {
  .compare__grid {
    gap: 6px;
  }

  .compare__cell {
    padding: 6px;
  }

  .compare__value {
    font-size: 14px;
  }

  .btn {
    flex: 1 1 100%;
  }
}
</style>
