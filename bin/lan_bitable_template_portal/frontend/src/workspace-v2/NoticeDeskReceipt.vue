<script setup lang="ts">
import { computed } from 'vue'
import { NSpin } from 'naive-ui'
import {
  Clock3,
  CheckCircle2,
  CircleAlert,
  Users,
  User,
  MessageSquare,
  Archive,
  FileText,
  Info,
} from 'lucide-vue-next'

interface Props {
  job: Record<string, any> | null
  title: string
  ambiguous: boolean
  succeeded: boolean
  terminal: boolean
  eventWaiting: boolean
  deliveryLabel: (state: string) => string
}

const props = defineProps<Props>()

const receiptMessage = computed<string>(() => {
  if (props.eventWaiting) {
    return '已保存本次正文、时间和附件；事件结束或删除后自动发送，可返回列表继续办理其它通告。'
  }
  const job = props.job
  const fromJob = job?.error || job?.upload_message || job?.message
  if (fromJob) return String(fromJob)
  if (props.ambiguous) return '提交响应未确认，已保留原操作标识，请勿重新创建。'
  if (props.succeeded) return '已按原业务流程完成，列表正在同步。'
  return '任务已受理，可返回列表继续办理其它通告。'
})

type LeadingTone = 'waiting' | 'pending' | 'success' | 'alert'
const leadingTone = computed<LeadingTone>(() => {
  if (props.eventWaiting) return 'waiting'
  if (props.job && !props.terminal) return 'pending'
  if (props.succeeded) return 'success'
  return 'alert'
})

interface DeliveryRow {
  key: string
  label: string
  state: string
}

const messageRows = computed<DeliveryRow[]>(() => {
  const job = props.job
  if (!job) return []
  return [
    { key: 'feishuGroup', label: '飞书群', state: props.deliveryLabel(job.feishu_group_state) },
    { key: 'feishuPersonal', label: '飞书个人', state: props.deliveryLabel(job.message_delivery_state) },
    { key: 'dingtalk', label: '钉钉', state: props.deliveryLabel(job.dingtalk_state) },
  ]
})

const archiveLabel = computed<string>(() => {
  if (props.succeeded) return '已同步'
  const state = props.job?.archive_state
  if (state === 'failed') return '同步失败'
  if (state === 'verification_required') return '待核验'
  return '后台同步中'
})

const archiveTone = computed<'success' | 'warning'>(() => (props.succeeded ? 'success' : 'warning'))
</script>

<template>
  <section
    class="notice-receipt"
    role="status"
    aria-live="polite"
    aria-atomic="false"
    :aria-label="title"
  >
    <header class="notice-receipt__head">
      <span class="notice-receipt__glyph" :class="`is-${leadingTone}`" aria-hidden="true">
        <Clock3 v-if="leadingTone === 'waiting'" :size="36" />
        <NSpin v-else-if="leadingTone === 'pending'" size="large" />
        <CheckCircle2 v-else-if="leadingTone === 'success'" :size="38" />
        <CircleAlert v-else :size="36" />
      </span>
      <div class="notice-receipt__heading">
        <h2 class="notice-receipt__title">{{ title }}</h2>
        <p v-if="job?.work_order_hint" class="notice-receipt__hint">
          <FileText :size="15" class="notice-receipt__inline-icon" aria-hidden="true" />
          <span class="notice-receipt__hint-text">{{ job?.work_order_hint }}</span>
        </p>
      </div>
      <div v-if="$slots.default" class="notice-receipt__actions"><slot /></div>
    </header>

    <p class="notice-receipt__message">
      <Info :size="15" class="notice-receipt__inline-icon" aria-hidden="true" />
      <span class="notice-receipt__message-text">{{ receiptMessage }}</span>
    </p>

    <div
      v-if="job?.message_first"
      class="notice-receipt__delivery"
      data-notice-delivery-status
      role="group"
      aria-label="消息投递状态"
    >
      <div class="notice-receipt__group">
        <span class="notice-receipt__group-head">
          <Users :size="15" aria-hidden="true" />
          <span>消息投递</span>
        </span>
        <ul class="notice-receipt__rows">
          <li v-for="row in messageRows" :key="row.key" class="notice-receipt__row">
            <span class="notice-receipt__row-label">
              <User v-if="row.key === 'feishuPersonal'" :size="15" aria-hidden="true" />
              <MessageSquare v-else :size="15" aria-hidden="true" />
              <span>{{ row.label }}</span>
            </span>
            <span class="notice-receipt__state">{{ row.state }}</span>
          </li>
        </ul>
      </div>

      <div class="notice-receipt__group notice-receipt__group--archive">
        <span class="notice-receipt__group-head">
          <Archive :size="15" aria-hidden="true" />
          <span>归档</span>
        </span>
        <ul class="notice-receipt__rows">
          <li class="notice-receipt__row">
            <span class="notice-receipt__row-label">
              <Archive :size="15" aria-hidden="true" />
              <span>多维</span>
            </span>
            <span class="notice-receipt__state" :class="`is-${archiveTone}`">{{ archiveLabel }}</span>
          </li>
        </ul>
      </div>
    </div>


  </section>
</template>

<style scoped>
.notice-receipt {
  --nd-face: var(--soft-face, #ffffff);
  --nd-field: var(--soft-field, #f4f7fb);
  --nd-inset: var(--soft-field, #eef3f9);
  --nd-ink: var(--v2-ink, #1f2a37);
  --nd-blue: var(--v2-blue, #2563eb);
  --nd-sky: var(--nd-accent-sky, #38bdf8);
  --nd-mint: var(--nd-accent-mint, #34d399);

  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 24px;
  border-radius: 18px;
  background: var(--nd-face);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05), 0 8px 24px -12px rgba(15, 23, 42, 0.18);
  color: var(--nd-ink);
  font-size: 14px;
  line-height: 1.55;
  box-sizing: border-box;
  min-width: 0;
}

.notice-receipt__head {
  flex-wrap:wrap;
  display: flex;
  align-items: flex-start;
  gap: 14px;
  min-width: 0;
}

.notice-receipt__glyph {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 52px;
  height: 52px;
  border-radius: 14px;
  background: var(--nd-inset);
  box-shadow: var(--soft-inset);
  color: var(--nd-blue);
}

.notice-receipt__glyph.is-waiting {
  color: var(--nd-sky);
}

.notice-receipt__glyph.is-success {
  color: var(--nd-mint);
  background: color-mix(in srgb, var(--nd-mint) 14%, var(--nd-inset));
}

.notice-receipt__glyph.is-alert {
  color: #d97706;
  background: color-mix(in srgb, #f59e0b 14%, var(--nd-inset));
}

.notice-receipt__heading {
  flex:1 1 9em;
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.notice-receipt__title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  line-height: 1.35;
  color: var(--nd-ink);
  overflow-wrap: anywhere;
}

.notice-receipt__hint {
  display: inline-flex;
  align-items: flex-start;
  gap: 6px;
  margin: 0;
  font-size: 13px;
  color: color-mix(in srgb, var(--nd-ink) 62%, var(--nd-face));
  overflow-wrap: anywhere;
}

.notice-receipt__inline-icon {
  flex: 0 0 auto;
  margin-top: 2px;
  color: var(--nd-sky);
}

.notice-receipt__message {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 0;
  padding: 12px 14px;
  border-radius: 12px;
  background: var(--nd-field);
  color: var(--nd-ink);
  overflow-wrap: anywhere;
}

.notice-receipt__message-text {
  min-width: 0;
}

.notice-receipt__delivery {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
  padding: 12px;
  border-radius: 14px;
  background: var(--nd-inset);
  box-shadow: var(--soft-inset);
}

.notice-receipt__group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 12px;
  border-radius: 12px;
  background: var(--nd-face);
  min-width: 0;
}

.notice-receipt__group-head {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.04em;
  color: color-mix(in srgb, var(--nd-ink) 58%, var(--nd-face));
  text-transform: none;
}

.notice-receipt__rows {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.notice-receipt__row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 10px;
  border-radius: 10px;
  background: var(--nd-field);
  min-width: 0;
}

.notice-receipt__row-label {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  color: var(--nd-ink);
  overflow-wrap: anywhere;
}

.notice-receipt__row-label svg {
  flex: 0 0 auto;
  color: var(--nd-blue);
}

.notice-receipt__state {
  flex: 0 0 auto;
  padding: 2px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  color: var(--nd-blue);
  background: color-mix(in srgb, var(--nd-blue) 10%, transparent);
  white-space: nowrap;
}

.notice-receipt__state.is-success {
  color: #047857;
  background: color-mix(in srgb, var(--nd-mint) 18%, transparent);
}

.notice-receipt__state.is-warning {
  color: #b45309;
  background: color-mix(in srgb, #f59e0b 16%, transparent);
}

.notice-receipt__group--archive .notice-receipt__row-label svg {
  color: var(--nd-sky);
}

.notice-receipt__actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
}

@media (max-width: 520px) {
  .notice-receipt {
    padding: 18px;
    gap: 14px;
    border-radius: 16px;
  }

  .notice-receipt__glyph {
    width: 44px;
    height: 44px;
  }

  .notice-receipt__title {
    font-size: 16px;
  }

  .notice-receipt__delivery {
    grid-template-columns: 1fr;
  }

  .notice-receipt__actions :deep(*) {
    min-width: 0;
  }
}

@media (max-width: 360px) {
  .notice-receipt {
    padding: 14px;
    gap: 12px;
  }

  .notice-receipt__head {
    gap: 10px;
  }

  .notice-receipt__glyph {
    width: 38px;
    height: 38px;
    border-radius: 11px;
  }

  .notice-receipt__row {
    flex-wrap: wrap;
    align-items: flex-start;
  }

  .notice-receipt__state {
    white-space: normal;
    max-width: 100%;
  }

  .notice-receipt__actions :deep(.n-button) {
    flex: 0 1 auto;
  }
}

@media (prefers-reduced-motion: no-preference) {
  .notice-receipt__glyph {
    transition: background-color 0.2s ease, color 0.2s ease;
  }
}
</style>
