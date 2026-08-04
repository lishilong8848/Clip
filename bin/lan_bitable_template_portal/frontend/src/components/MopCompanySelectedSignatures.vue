<template>
  <section class="company-selected-panel">
    <header class="selected-panel-head">
      <div>
        <strong>当前公司人员</strong>
        <small>{{ summaryText }}</small>
      </div>
      <div class="bulk-actions">
        <button type="button"
          :disabled="!unsignedSignatureCount || bulkLinkSending"
          title="给当前角色下所有未签名公司人员发送签名链接"
          @click="emit('send-unsigned-links')"
        >
          {{ bulkLinkSending ? "发送中" : `发送未签名 ${unsignedSignatureCount}` }}
        </button>
        <button type="button"
          :disabled="!confirmableCount || confirmSending"
          title="向已签名但尚未确认的人员发送本次使用确认"
          @click="emit('send-confirmations')"
        >
          {{ confirmSending ? "发送中" : `发送待确认 ${confirmableCount}` }}
        </button>
      </div>
    </header>

    <div v-if="people.length" class="task-toolbar">
      <input v-model="taskSearch" type="search" placeholder="搜索已选人员" />
      <div class="task-filter-tabs" role="group" aria-label="公司人员签名筛选">
        <button type="button" :aria-pressed="taskFilter === 'all'" :class="{ active: taskFilter === 'all' }" @click="taskFilter = 'all'">
          全部 {{ people.length }}
        </button>
        <button type="button" :aria-pressed="taskFilter === 'pending'" :class="{ active: taskFilter === 'pending' }" @click="taskFilter = 'pending'">
          待处理 {{ unsignedCount }}
        </button>
        <button type="button" :aria-pressed="taskFilter === 'ready'" :class="{ active: taskFilter === 'ready' }" @click="taskFilter = 'ready'">
          可用 {{ readyCount }}
        </button>
      </div>
    </div>

    <div v-if="visiblePeople.length" class="signature-task-list">
      <article
        v-for="person in visiblePeople"
        :key="`${role}:${personKey(person)}`"
        :class="{
          active: person.record_id === activeRecordId && (!person.source || person.source === 'staff'),
          ready: hasUsableSignature(person),
          awaiting: personHasStoredSignature(person) && !hasUsableSignature(person) && !person.usage_rejected,
          pending: !personHasStoredSignature(person),
          rejected: Boolean(person.usage_rejected)
        }"
      >
        <button type="button" class="signature-preview" @click="emit('activate', person)">
          <img
            v-if="personHasStoredSignature(person) && showSignaturePreview !== false"
            :src="person.signature_preview_url"
            alt="人员签名预览"
            loading="lazy"
            @error="emit('image-error', person)"
          />
          <span v-else>{{ personHasStoredSignature(person) ? "已签名" : "未签名" }}</span>
        </button>
        <button type="button" class="person-summary" @click="emit('activate', person)">
          <strong>{{ displayName(person) }}</strong>
          <small :class="{ failed: Boolean(linkErrorById[person.record_id]) }">
            {{ personStatus(person) }}
          </small>
        </button>
        <div class="task-actions">
          <button type="button"
            :disabled="Boolean(webSignDisabledReason(person))"
            :title="webSignDisabledReason(person) || '在当前网页手写并保存到该人员签名库'"
            @click="emit('web-sign', person)"
          >
            {{ personHasStoredSignature(person) ? "网页重签" : "网页手写" }}
          </button>
          <button type="button"
            class="link-action"
            :disabled="Boolean(linkSendingById[person.record_id]) || !person.record_id"
            :title="linkTitle(person)"
            @click="emit('send-link', person, personHasStoredSignature(person))"
          >
            {{ linkSendingById[person.record_id] ? "发送中" : (personHasStoredSignature(person) ? "重发链接" : "发送链接") }}
          </button>
          <button type="button" class="remove-action" @click="emit('remove', personKey(person))">移除</button>
        </div>
      </article>
    </div>
    <div v-else-if="people.length" class="panel-empty">当前筛选下没有人员。</div>
    <div v-else class="panel-empty">请从左侧选择公司人员。</div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";

type Dict = Record<string, any>;
type SignatureRole = "implementer" | "auditor" | "inspector";

const props = defineProps<{
  role: SignatureRole;
  people: Dict[];
  activeRecordId: string;
  unsignedCount: number;
  unsignedSignatureCount: number;
  linkSendingById: Record<string, boolean>;
  linkSentAtById: Record<string, string>;
  linkErrorById: Record<string, string>;
  hasUsableSignature: (person: Dict | null | undefined) => boolean;
  personKey: (person: Dict) => string;
  displayName: (person: Dict) => string;
  linkTitle: (person: Dict) => string;
  webSignDisabledReason: (person: Dict | null | undefined) => string;
  bulkLinkSending: boolean;
  confirmSending: boolean;
  confirmableCount: number;
  readyTargetLabel?: string;
  showSignaturePreview?: boolean;
}>();

const emit = defineEmits<{
  activate: [person: Dict];
  "image-error": [person: Dict];
  "web-sign": [person: Dict];
  "send-link": [person: Dict, forceResign: boolean];
  "send-unsigned-links": [];
  "send-confirmations": [];
  remove: [personKey: string];
}>();

const taskSearch = ref("");
const taskFilter = ref<"all" | "pending" | "ready">("all");
const readyCount = computed(() => props.people.filter((person) => props.hasUsableSignature(person)).length);
const pendingConfirmationCount = computed(() => props.people.filter((person) => (
  personHasStoredSignature(person)
  && !props.hasUsableSignature(person)
  && !person?.usage_rejected
)).length);
const rejectedCount = computed(() => props.people.filter((person) => Boolean(person?.usage_rejected)).length);
const summaryText = computed(() => {
  if (!props.people.length) return "未选择";
  const details: string[] = [`可用 ${readyCount.value}/${props.people.length}`];
  if (props.unsignedSignatureCount) details.push(`未签 ${props.unsignedSignatureCount}`);
  if (pendingConfirmationCount.value) details.push(`待确认 ${pendingConfirmationCount.value}`);
  if (rejectedCount.value) details.push(`已拒绝 ${rejectedCount.value}`);
  return details.join(" · ");
});
const visiblePeople = computed(() => {
  const query = taskSearch.value.trim().toLowerCase();
  return props.people.filter((person) => {
    const ready = props.hasUsableSignature(person);
    if (taskFilter.value === "pending" && ready) return false;
    if (taskFilter.value === "ready" && !ready) return false;
    if (!query) return true;
    return [
      person.name,
      person.display_name,
      person.employee_no,
      person.position,
      person.team,
    ].some((value) => String(value || "").toLowerCase().includes(query));
  });
});

watch(() => props.role, () => {
  taskSearch.value = "";
  taskFilter.value = "all";
});

function personHasStoredSignature(person: Dict | null | undefined): boolean {
  if (!person?.has_signature) return false;
  return props.showSignaturePreview === false
    || Boolean(String(person?.signature_preview_url || "").trim());
}

function personStatus(person: Dict): string {
  const recordId = String(person?.record_id || "");
  if (props.linkErrorById[recordId]) return `发送失败：${props.linkErrorById[recordId]}`;
  if (person?.usage_rejected) return "已拒绝本次使用";
  if (props.hasUsableSignature(person)) return person?.usage_confirmed
    ? `已确认，可写入${props.readyTargetLabel || "MOP"}`
    : "当前登录人签名，可直接使用";
  if (personHasStoredSignature(person)) return "已有签名，等待本人确认";
  if (props.linkSentAtById[recordId]) return `签名链接已发送 ${props.linkSentAtById[recordId]}`;
  return "尚未签名";
}
</script>

<style scoped>
.company-selected-panel {
  min-width: 0;
  display: grid;
  align-content: start;
  gap: 8px;
  border: 1px solid #b8d7ff;
  border-radius: 12px;
  padding: 8px;
  background: #f8fbff;
  box-shadow: inset 3px 0 0 #1e63ff;
}

.selected-panel-head {
  min-width: 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.selected-panel-head > div:first-child {
  min-width: 0;
}

.selected-panel-head strong,
.selected-panel-head small {
  display: block;
}

.selected-panel-head strong {
  color: #0f172a;
  font-size: 13px;
  font-weight: 950;
}

.selected-panel-head small {
  margin-top: 2px;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}

.bulk-actions {
  flex: 0 0 auto;
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 5px;
}

.bulk-actions button,
.task-actions button {
  min-height: 30px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  padding: 0 9px;
  background: #ffffff;
  color: #1d4ed8;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
  white-space: nowrap;
}

.bulk-actions button:disabled,
.task-actions button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
}

.task-toolbar {
  position: sticky;
  top: 0;
  z-index: 2;
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(180px, 1fr) auto;
  gap: 8px;
  padding: 6px;
  border: 1px solid #d8e5f7;
  border-radius: 10px;
  background: rgba(248, 251, 255, 0.96);
  backdrop-filter: blur(8px);
}

.task-toolbar input {
  min-width: 0;
  height: 32px;
  border: 1px solid #cfe0ff;
  border-radius: 8px;
  padding: 0 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
  font-size: 12px;
  outline: none;
}

.task-toolbar input:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.task-filter-tabs {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 3px;
  border: 1px solid #d8e5f7;
  border-radius: 8px;
  background: #ffffff;
}

.task-filter-tabs button {
  min-height: 26px;
  border: 0;
  border-radius: 6px;
  padding: 0 8px;
  background: transparent;
  color: #64748b;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.task-filter-tabs button.active {
  background: #1e63ff;
  color: #ffffff;
}

.signature-task-list {
  display: grid;
  gap: 6px;
}

.signature-task-list article {
  min-width: 0;
  min-height: 52px;
  display: grid;
  grid-template-columns: 72px minmax(0, 1fr) minmax(220px, auto);
  align-items: center;
  gap: 8px;
  border: 1px solid #d8e5f7;
  border-radius: 10px;
  padding: 6px;
  background: #ffffff;
}

.signature-task-list article.ready {
  border-color: #a7e8c2;
  background: #f0fdf4;
}

.signature-task-list article.awaiting,
.signature-task-list article.pending {
  border-color: #fed7aa;
  background: #fffaf2;
}

.signature-task-list article.rejected {
  border-color: #fecaca;
  background: #fef2f2;
}

.signature-task-list article.active {
  box-shadow: inset 3px 0 0 #1e63ff;
}

.signature-preview,
.person-summary {
  min-width: 0;
  border: 0;
  padding: 0;
  background: transparent;
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.signature-preview {
  width: 68px;
  min-height: 30px;
  display: grid;
  place-items: center;
  border-radius: 7px;
  background: #ffffff;
}

.signature-preview img {
  width: 62px;
  height: 26px;
  object-fit: contain;
}

.signature-preview span {
  color: #c2410c;
  font-size: 11px;
  font-weight: 900;
}

.person-summary strong,
.person-summary small {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.person-summary strong {
  color: #0f172a;
  font-size: 12px;
  font-weight: 950;
}

.person-summary small {
  margin-top: 3px;
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.person-summary small.failed {
  color: #b91c1c;
}

.task-actions {
  display: grid;
  grid-template-columns: repeat(3, minmax(68px, 1fr));
  gap: 5px;
}

.task-actions .link-action {
  border-color: #bae6fd;
  color: #0369a1;
  background: #f0f9ff;
}

.task-actions .remove-action {
  border-color: #e2e8f0;
  color: #475569;
  background: #f8fafc;
}

.panel-empty {
  border: 1px dashed #bfdbfe;
  border-radius: 10px;
  padding: 18px 12px;
  background: rgba(255, 255, 255, 0.74);
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  text-align: center;
}

@media (max-width: 760px) {
  .selected-panel-head,
  .task-toolbar {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .selected-panel-head {
    display: grid;
  }

  .bulk-actions {
    justify-content: stretch;
  }

  .bulk-actions button {
    flex: 1 1 auto;
    min-height: 44px;
  }

  .task-toolbar input,
  .task-filter-tabs button {
    min-height: 44px;
  }

  .signature-task-list article {
    grid-template-columns: 64px minmax(0, 1fr);
  }

  .task-actions {
    grid-column: 1 / -1;
  }

  .task-actions button {
    min-height: 44px;
  }
}
</style>
