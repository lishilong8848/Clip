<template>
  <div class="selected-sign-person company-selected-panel">
    <div class="signature-subsection-title company-title">
      <span>公司人员</span>
      <em :class="statusTone">{{ statusText }}</em>
    </div>
    <div v-if="people.length" class="selected-signatures">
      <span
        v-for="person in people.slice(0, 1)"
        :key="`${role}:${personKey(person)}`"
        class="selected-signature-chip"
        :class="{ active: person.record_id === activeRecordId && (!person.source || person.source === 'staff') }"
        @click="emit('activate', person)"
      >
        <img
          v-if="personHasStoredSignature(person)"
          :src="person.signature_preview_url"
          alt="已有签名"
          @error="emit('image-error', person)"
        />
        <span v-else class="signature-chip-state">待签名</span>
        <strong>{{ displayName(person) }}</strong>
        <em
          v-if="personBadgeText(person)"
          class="signature-chip-badge"
          :class="personBadgeTone(person)"
        >
          {{ personBadgeText(person) }}
        </em>
      </span>
      <button type="button"
        class="selected-signature-open"
        :class="{ ready: people.length > 0 && !unsignedCount, pending: unsignedCount > 0 }"
        :aria-expanded="drawerOpen"
        :title="unsignedCount ? `${unsignedCount} 人待处理，点击处理` : '签名已齐全，点击查看人员'"
        @click.stop="emit('toggle-drawer')"
      >
        {{ drawerButtonText }}
        <em v-if="unsignedCount">{{ unsignedCount }} 待处理</em>
      </button>
      <MopSignatureDrawer
        :open="drawerOpen"
        :title="`${roleLabel} · 公司人员`"
        @close="emit('close-drawer')"
      >
        <div class="drawer-filter-bar">
          <div class="drawer-progress">
            <strong>{{ signedCount }}/{{ people.length }}</strong>
            <span>{{ unsignedCount ? `待处理 ${unsignedCount} 人` : "签名已齐" }}</span>
          </div>
          <input
            v-model="drawerSearch"
            type="search"
            placeholder="搜索已选人员"
          />
          <div class="drawer-filter-tabs" aria-label="公司人员签名筛选">
            <button type="button" :class="{ active: drawerFilter === 'all' }" @click="drawerFilter = 'all'">全部</button>
            <button type="button" :class="{ active: drawerFilter === 'unsigned' }" @click="drawerFilter = 'unsigned'">待处理</button>
            <button type="button" :class="{ active: drawerFilter === 'signed' }" @click="drawerFilter = 'signed'">可用</button>
          </div>
          <div class="drawer-bulk-actions">
            <button type="button"
              class="drawer-bulk-action"
              :disabled="!unsignedSignatureCount || bulkLinkSending"
              title="给当前角色下所有未签名公司人员发送签名链接"
              @click.stop="emit('send-unsigned-links')"
            >
              {{ bulkLinkSending ? "发送中" : `发未签 ${unsignedSignatureCount}` }}
            </button>
            <button type="button"
              class="drawer-bulk-action"
              :disabled="!confirmableCount || confirmSending"
              title="向已选且非当前登录人的已签名人员发送使用确认"
              @click.stop="emit('send-confirmations')"
            >
              {{ confirmSending ? "发送中" : `发确认 ${confirmableCount}` }}
            </button>
          </div>
        </div>
        <article
          v-for="person in drawerVisiblePeople"
          :key="`drawer:${role}:${personKey(person)}`"
          :class="{ ready: hasUsableSignature(person), pending: !hasUsableSignature(person) }"
        >
          <img
            v-if="personHasStoredSignature(person)"
            :src="person.signature_preview_url"
            alt="已有签名"
            @error="emit('image-error', person)"
          />
          <span v-else class="signature-chip-state">待签名</span>
          <div>
            <strong>{{ person.name || person.display_name || "未命名" }}</strong>
            <small :class="{ failed: Boolean(linkErrorById[person.record_id]) }">{{ drawerPersonStatus(person) }}</small>
          </div>
          <div class="drawer-actions">
            <button type="button"
              v-if="!person.source || person.source === 'staff'"
              class="drawer-action"
              :disabled="Boolean(webSignDisabledReason(person))"
              :title="webSignDisabledReason(person) || '在当前网页手写并保存到该人员签名库'"
              @click.stop="emit('web-sign', person)"
            >
              {{ personHasStoredSignature(person) ? "网页重签" : "网页手写" }}
            </button>
            <button type="button"
              v-if="!person.source || person.source === 'staff'"
              class="drawer-action link-action"
              :disabled="Boolean(linkSendingById[person.record_id]) || !person.record_id"
              :title="linkTitle(person)"
              @click.stop="emit('send-link', person, personHasStoredSignature(person))"
            >
              {{ linkSendingById[person.record_id] ? "发送中" : (personHasStoredSignature(person) ? "重发链接" : "发链接") }}
            </button>
            <button type="button" class="drawer-remove" @click.stop="emit('remove', personKey(person))">移除</button>
          </div>
        </article>
        <div v-if="!drawerVisiblePeople.length" class="drawer-empty">当前筛选下没有人员。</div>
      </MopSignatureDrawer>
    </div>
    <div v-else class="company-empty">
      未选择公司人员。
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import MopSignatureDrawer from "./MopSignatureDrawer.vue";

type Dict = Record<string, any>;
type SignatureRole = "implementer" | "auditor";

const props = defineProps<{
  role: SignatureRole;
  people: Dict[];
  activeRecordId: string;
  unsignedCount: number;
  unsignedSignatureCount: number;
  drawerOpen: boolean;
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
}>();

const emit = defineEmits<{
  activate: [person: Dict];
  "toggle-drawer": [];
  "close-drawer": [];
  "image-error": [person: Dict];
  "web-sign": [person: Dict];
  "send-link": [person: Dict, forceResign: boolean];
  "send-unsigned-links": [];
  "send-confirmations": [];
  remove: [personKey: string];
}>();

const roleLabel = computed(() => props.role === "implementer" ? "维护实施人" : "维护审核人");
const signedCount = computed(() => Math.max(0, props.people.length - props.unsignedCount));
const drawerSearch = ref("");
const drawerFilter = ref<"all" | "unsigned" | "signed">("all");
const drawerVisiblePeople = computed(() => {
  const query = drawerSearch.value.trim().toLowerCase();
  return props.people.filter((person) => {
    const signed = props.hasUsableSignature(person);
    if (drawerFilter.value === "unsigned" && signed) return false;
    if (drawerFilter.value === "signed" && !signed) return false;
    if (!query) return true;
    return [
      person.name,
      person.display_name,
      person.employee_no,
      person.open_id,
      person.position,
      person.team,
    ].some((value) => String(value || "").toLowerCase().includes(query));
  });
});
const statusText = computed(() => {
  if (!props.people.length) return "未选择";
  if (!props.unsignedCount) return "签名齐全";
  return `${signedCount.value}/${props.people.length} 可用`;
});
const statusTone = computed(() => ({
  ready: props.people.length > 0 && props.unsignedCount === 0,
  pending: props.unsignedCount > 0,
  empty: props.people.length === 0,
}));
const drawerButtonText = computed(() => {
  return props.unsignedCount ? "处理公司人员" : "查看公司人员";
});

function drawerPersonStatus(person: Dict): string {
  const recordId = String(person?.record_id || "");
  if (props.linkErrorById[recordId]) return `链接失败：${props.linkErrorById[recordId]}`;
  if (props.linkSentAtById[recordId]) return `链接已发送 ${props.linkSentAtById[recordId]}`;
  if (props.hasUsableSignature(person)) return person?.usage_confirmed ? "已确认可用" : "本人签名可用";
  if (person?.usage_rejected) return "已拒绝使用";
  if (person?.usage_confirmation_required || person?.usage_confirmation_pending) return "待本人确认使用";
  if (personHasStoredSignature(person)) return "已有签名，待确认";
  return "待签名";
}

function personHasStoredSignature(person: Dict | null | undefined): boolean {
  return Boolean(person?.has_signature && String(person?.signature_preview_url || "").trim());
}

function personBadgeText(person: Dict): string {
  if (props.hasUsableSignature(person)) return person?.usage_confirmed ? "已确认" : "可用";
  if (person?.usage_rejected) return "已拒绝";
  if (personHasStoredSignature(person)) return "待确认";
  return "";
}

function personBadgeTone(person: Dict): Record<string, boolean> {
  return {
    ready: props.hasUsableSignature(person),
    rejected: Boolean(person?.usage_rejected),
    pending: !props.hasUsableSignature(person) && !person?.usage_rejected && personHasStoredSignature(person),
  };
}
</script>

<style scoped>
.company-selected-panel {
  border-color: #b8d7ff;
  background: linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.98));
  box-shadow: inset 4px 0 0 #1e63ff;
}

.selected-sign-person {
  position: relative;
  display: grid;
  gap: 5px;
  border: 1px solid #bfdbfe;
  border-radius: 16px;
  padding: 8px;
}

.company-empty {
  min-height: 30px;
  display: flex;
  align-items: center;
  border: 1px dashed #bfdbfe;
  border-radius: 14px;
  padding: 6px 8px;
  background: rgba(239, 246, 255, 0.72);
  color: #1d4ed8;
  font-size: 11px;
  font-weight: 850;
}

.signature-subsection-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}

.signature-subsection-title span {
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 950;
}

.signature-subsection-title em {
  flex: 0 0 auto;
  border-radius: 999px;
  background: #dbeafe;
  padding: 2px 6px;
  color: #1d4ed8;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
}

.signature-subsection-title em.ready {
  background: #dcfce7;
  color: #047857;
}

.signature-subsection-title em.pending {
  background: #fff7ed;
  color: #c2410c;
}

.signature-subsection-title em.empty {
  background: #eff6ff;
  color: #3156c9;
}

.selected-signatures {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(92px, auto);
  align-items: center;
  gap: 7px;
  min-height: 34px;
  margin-top: 2px;
  overflow: visible;
  padding-bottom: 2px;
  isolation: isolate;
}

.selected-signature-chip {
  position: relative;
  display: inline-grid;
  grid-template-columns: 52px minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  min-width: 0;
  width: 100%;
  height: 32px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  padding: 4px 6px;
  cursor: pointer;
  transition:
    border-color 0.16s ease,
    box-shadow 0.16s ease,
    background 0.16s ease;
}

.selected-signature-chip.active {
  border-color: #1e63ff;
  background: linear-gradient(135deg, #ffffff, #eef6ff);
  box-shadow:
    0 0 0 3px rgba(30, 99, 255, 0.14),
    0 8px 18px rgba(30, 99, 255, 0.14);
}

.selected-signature-chip.active::after {
  content: "已选";
  position: absolute;
  right: 6px;
  top: -8px;
  border-radius: 999px;
  padding: 1px 6px;
  background: #1e63ff;
  color: #ffffff;
  font-size: 10px;
  font-weight: 950;
  line-height: 1.45;
}

.selected-signature-chip img {
  width: 50px;
  height: 22px;
  object-fit: contain;
}

.signature-chip-state {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 50px;
  min-height: 22px;
  border-radius: 999px;
  background: #fff7ed;
  color: #c2410c;
  font-size: 11px;
  font-weight: 850;
}

.selected-signature-chip strong {
  overflow: hidden;
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.signature-chip-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 42px;
  height: 20px;
  border-radius: 999px;
  padding: 0 6px;
  font-size: 10px;
  font-style: normal;
  font-weight: 950;
  white-space: nowrap;
}

.signature-chip-badge.ready {
  background: #dcfce7;
  color: #047857;
}

.signature-chip-badge.pending {
  background: #fff7ed;
  color: #c2410c;
}

.signature-chip-badge.rejected {
  background: #fef2f2;
  color: #b91c1c;
}

.selected-signature-open {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  flex: 0 0 auto;
  min-width: 0;
  max-width: 100%;
  min-height: 30px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  padding: 0 9px;
  font-size: 11px;
  font-weight: 900;
  white-space: nowrap;
  cursor: pointer;
}

.selected-signature-open.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.selected-signature-open.pending {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.selected-signature-open em {
  border-radius: 999px;
  background: #fff7ed;
  color: #c2410c;
  padding: 2px 6px;
  font-style: normal;
  font-size: 11px;
  line-height: 1;
}

.drawer-filter-bar {
  display: grid;
  grid-template-columns: minmax(104px, auto) minmax(180px, 1fr) auto auto;
  align-items: center;
  gap: 8px;
  position: sticky;
  top: 0;
  z-index: 2;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: rgba(248, 251, 255, 0.96);
  padding: 8px;
  backdrop-filter: blur(10px);
}

.drawer-progress {
  display: grid;
  gap: 2px;
  color: #1d4ed8;
}

.drawer-progress strong {
  font-size: 13px;
  font-weight: 950;
}

.drawer-progress span {
  font-size: 11px;
  font-weight: 850;
}

.drawer-filter-bar input {
  min-width: 0;
  height: 30px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 0 11px;
  color: #0f172a;
  background: #ffffff;
  font: inherit;
  font-size: 12px;
  font-weight: 850;
  outline: none;
}

.drawer-filter-bar input:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.drawer-filter-tabs {
  display: inline-flex;
  gap: 4px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  padding: 3px;
}

.drawer-filter-tabs button {
  border: 0;
  border-radius: 999px;
  background: transparent;
  color: #64748b;
  padding: 5px 9px;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.drawer-filter-tabs button.active {
  background: #1e63ff;
  color: #ffffff;
}

.drawer-bulk-actions {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 5px;
  justify-content: flex-end;
  min-width: 0;
}

.drawer-bulk-action {
  min-height: 30px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: #ffffff;
  color: #1d4ed8;
  padding: 5px 9px;
  font-size: 11px;
  font-weight: 950;
  cursor: pointer;
  white-space: nowrap;
}

.drawer-bulk-action:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}

.drawer-empty {
  border: 1px dashed #bfdbfe;
  border-radius: 14px;
  padding: 14px;
  background: #f8fbff;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  text-align: center;
}

:deep(small.failed) {
  color: #b91c1c !important;
}

@media (max-width: 760px) {
  .selected-signatures {
    display: grid;
    grid-template-columns: 1fr;
    max-height: none;
  }

  .selected-signature-chip {
    width: auto;
  }

  .selected-signature-open {
    padding: 0 10px;
  }

  .drawer-filter-bar {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 1160px) {
  .drawer-filter-bar {
    grid-template-columns: minmax(104px, auto) minmax(0, 1fr);
  }

  .drawer-filter-tabs,
  .drawer-bulk-actions {
    justify-content: flex-start;
  }
}
</style>
