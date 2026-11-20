<script lang="ts">
interface DigestTarget {
  label: string
  url?: string
}

interface DigestLatest {
  slot?: number | string | null
  status?: string
  message?: string
}

export interface ZhihangDigest {
  kind: string
  label: string
  times: string[]
  prepare_at?: string
  start_day?: number | null
  modules: string[]
  enabled_modules: string[]
  target: DigestTarget
  latest: DigestLatest
}

</script>

<script setup lang="ts">
defineProps<{
  digests: ZhihangDigest[]
  schedule: (d: ZhihangDigest) => string
  taskLabel: (kind: string) => string
  dateText: (stamp: number | string | null | undefined) => string
  statusText: (status?: string) => string
}>()

const hasSlot = (d: ZhihangDigest): boolean => {
  const slot = d.latest?.slot
  return slot !== undefined && slot !== null && slot !== ''
}

const hasReceipt = (d: ZhihangDigest): boolean =>
  hasSlot(d) || !!d.latest?.status || !!d.latest?.message
</script>

<template>
  <section class="zh">
    <details class="zh-panel">
      <summary class="zh-summary">
        汇总通知
        <span class="zh-count">{{ digests.length }} 项</span>
      </summary>

      <div class="zh-list">
        <article v-for="d in digests" :key="d.kind" class="zh-item">
          <header class="zh-head">
            <span class="zh-title">{{ d.label }}</span>
            <span class="zh-sched">{{ schedule(d) }}</span>
          </header>

          <dl class="zh-fields">
            <div class="zh-field">
              <dt>参与模块</dt>
              <dd>
                <span
                  v-for="kind in d.modules"
                  :key="kind"
                  class="zh-tag"
                  :class="d.enabled_modules.includes(kind) ? 'zh-on' : 'zh-off'"
                >{{ taskLabel(kind) }} · {{ d.enabled_modules.includes(kind) ? '启用' : '停用' }}</span>
                <span v-if="!d.modules?.length" class="zh-muted">未配置模块</span>
              </dd>
            </div>

            <div class="zh-field">
              <dt>送达对象</dt>
              <dd>{{ d.target?.label || '未设置' }}</dd>
            </div>

            <div class="zh-field">
              <dt>最近回执</dt>
              <dd>
                <template v-if="hasReceipt(d)">
                  <span v-if="hasSlot(d)" class="zh-time">{{ dateText(d.latest?.slot) }}</span>
                  <span v-if="d.latest?.status" class="zh-status">{{ statusText(d.latest.status) }}</span>
                  <span v-if="d.latest?.message" class="zh-msg">{{ d.latest.message }}</span>
                </template>
                <span v-else class="zh-muted">无回执记录</span>
              </dd>
            </div>
          </dl>
        </article>
      </div>
    </details>
  </section>
</template>

<style scoped>
.zh {
  --line: #e3e6ea;
  --text: #2c3238;
  --muted: #536876;
  --blue: #3b73c4;
  --blue-soft: #eaf1fb;
  --mint: #286f59;
  --mint-soft: #e7f4ef;

  font-size: 13px;
  line-height: 1.5;
  color: var(--text);
  background: var(--soft-face, #f2f5f6);
  border: 1px solid var(--line);
  border-radius: 14px;
}

.zh-summary {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  font-weight: 600;
  cursor: pointer;
  list-style: none;
  user-select: none;
}

.zh-summary::-webkit-details-marker {
  display: none;
}

.zh-summary::before {
  content: '';
  flex: none;
  width: 6px;
  height: 6px;
  border-right: 2px solid var(--blue);
  border-bottom: 2px solid var(--blue);
  transform: rotate(-45deg);
  transition: transform 0.15s ease;
}

.zh-panel[open] > .zh-summary {
  border-bottom: 1px solid var(--line);
}

.zh-panel[open] > .zh-summary::before {
  transform: rotate(45deg);
}

.zh-summary:focus-visible {
  outline: 2px solid var(--blue);
  outline-offset: 2px;
}

.zh-count {
  font-weight: 400;
  font-size: 12px;
  color: var(--muted);
}

.zh-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 300px), 1fr));
  gap: 10px;
  padding: 10px 14px;
}

.zh-item {
  min-width: 0;
  padding: 10px;
  background: #fff;
  border: 1px solid var(--line);
  border-radius: 10px;
}

.zh-head {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 4px 8px;
  padding-bottom: 6px;
  margin-bottom: 6px;
  border-bottom: 1px dashed var(--line);
}

.zh-title {
  font-weight: 600;
  overflow-wrap: anywhere;
}

.zh-sched {
  margin-left: auto;
  padding: 0 6px;
  font-size: 12px;
  line-height: 18px;
  color: var(--mint);
  background: var(--mint-soft);
  border-radius: 4px;
  white-space: normal;
  overflow-wrap: anywhere;
}

.zh-fields {
  display: grid;
  gap: 4px;
  margin: 0;
}

.zh-field {
  display: flex;
  flex-wrap: wrap;
  gap: 2px 6px;
  min-width: 0;
  font-size: 12px;
}

.zh-field dt {
  flex: none;
  width: 56px;
  color: var(--muted);
}

.zh-field dd {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
}

.zh-tag {
  padding: 0 6px;
  font-size: 12px;
  line-height: 18px;
  border-radius: 4px;
  overflow-wrap: anywhere;
}

.zh-on {
  color: #2a6b56;
  background: var(--mint-soft);
}

.zh-off {
  color: var(--muted);
  background: #f2f3f5;
}

.zh-status {
  padding: 0 6px;
  font-size: 12px;
  line-height: 18px;
  color: var(--blue);
  background: var(--blue-soft);
  border-radius: 4px;
}

.zh-time {
  font-variant-numeric: tabular-nums;
}

.zh-msg,
.zh-muted {
  color: var(--muted);
  font-size: 12px;
}

.zh-msg {
  flex: 1 1 100%;
}

@media (max-width: 400px) {
  .zh-field dt {
    width: 100%;
  }
}

@media (prefers-reduced-motion: reduce) {
  .zh-summary::before {
    transition: none;
  }
}
</style>
