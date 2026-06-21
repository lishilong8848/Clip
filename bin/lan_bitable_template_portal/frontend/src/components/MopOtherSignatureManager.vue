<template>
  <section class="other-signature-panel">
    <div class="other-signature-head">
      <div>
        <strong>临时/外部人员</strong>
        <small :class="summaryTone">{{ summaryText }}</small>
      </div>
      <button
        class="btn ghost"
        type="button"
        :disabled="Boolean(addDisabledReason)"
        :title="addDisabledReason"
        @click="emit('add-other')"
      >
        添加
      </button>
    </div>
    <p v-if="addDisabledReason" class="other-signature-disabled">{{ addDisabledReason }}</p>
    <div v-if="displayRows.length" class="other-signature-list">
      <div class="other-signature-summary">
        <template v-if="previewRow">
          <span
            :class="['other-signature-row', 'summary-chip', previewRow.signed ? 'signed' : 'pending']"
          >
            <img
              v-if="previewRow.kind === 'person' && previewRow.signed"
              :src="previewRow.person.signature_preview_url"
              alt="其他人员签名"
              @error="emit('image-error', previewRow.person)"
            />
            <em v-else class="other-signature-state">
              {{ previewRow.kind === 'draft' ? draftStatusText(previewRow.draft) : '待签名' }}
            </em>
            <strong>{{ previewRow.display_name }}</strong>
          </span>
        </template>
        <button
          class="selected-signature-open temporary-open"
          :class="{ ready: displayRows.length > 0 && !unsignedCount, pending: unsignedCount > 0 }"
          type="button"
          :aria-expanded="drawerOpen"
          :title="unsignedCount ? `${unsignedCount} 人未签名，点击查看处理` : '临时/外部人员签名已齐全，点击查看'"
          @click.stop="emit('update:drawerOpen', !drawerOpen)"
        >
          {{ drawerButtonText }}
          <em v-if="unsignedCount">{{ unsignedCount }} 未签</em>
        </button>
        <MopSignatureDrawer
          :open="drawerOpen"
          tone="temporary"
          :title="`${role === 'implementer' ? '维护实施人' : '维护审核人'}临时/外部人员`"
          @close="emit('update:drawerOpen', false)"
        >
          <div class="drawer-filter-bar temporary-filter-bar">
            <div class="drawer-progress">
              <strong>{{ signedCount }}/{{ displayRows.length }}</strong>
              <span>{{ unsignedCount ? `待签 ${unsignedCount} 人` : "签名已齐" }}</span>
            </div>
            <input
              v-model="drawerSearch"
              type="search"
              placeholder="搜索临时/外部人员"
            />
            <div class="drawer-filter-tabs" aria-label="临时人员签名筛选">
              <button type="button" :class="{ active: drawerFilter === 'all' }" @click="drawerFilter = 'all'">全部</button>
              <button type="button" :class="{ active: drawerFilter === 'unsigned' }" @click="drawerFilter = 'unsigned'">未签</button>
              <button type="button" :class="{ active: drawerFilter === 'signed' }" @click="drawerFilter = 'signed'">已签</button>
            </div>
          </div>
          <template
            v-for="row in drawerVisibleRows"
            :key="row.row_key"
          >
            <article
              v-if="row.kind === 'person'"
              :class="{ ready: row.signed, pending: !row.signed }"
            >
              <img
                v-if="row.signed"
                :src="row.person.signature_preview_url"
                alt="其他人员签名"
                @error="emit('image-error', row.person)"
              />
              <span v-else class="signature-chip-state">待签名</span>
              <div>
                <strong>{{ row.display_name }}</strong>
                <small :class="{ failed: Boolean(row.person?.temp_id && temporaryLinkErrorById[row.person.temp_id]) }">
                  {{ drawerPersonStatus(row) }}
                </small>
              </div>
              <div class="drawer-actions">
                <button
                  class="drawer-action"
                  type="button"
                  :disabled="Boolean(personWebSignDisabledReason(row.person))"
                  :title="personWebSignDisabledReason(row.person)"
                  @click.stop="emit('web-sign-person', row.person)"
                >
                  {{ row.signed ? "网页重签" : "网页签名" }}
                </button>
                <button
                  v-if="row.person.source !== 'external'"
                  class="drawer-action link-action"
                  type="button"
                  :disabled="Boolean(temporaryLinkSendingById[row.person.temp_id]) || !row.person.temp_id"
                  :title="row.person.temp_id ? '重新发送该临时人员签名链接' : '该临时人员签名会话不完整，无法发送链接'"
                  @click.stop="emit('send-temp-person', row.person)"
                >
                  {{ temporaryLinkSendingById[row.person.temp_id] ? "发送中" : (row.signed ? "重发链接" : "发链接") }}
                </button>
                <button class="drawer-remove" type="button" @click.stop="emit('remove-person', signaturePersonKey(row.person))">移除</button>
              </div>
            </article>
            <article
              v-else
              :class="['draft', String(row.draft.status || ''), 'pending']"
            >
              <span class="signature-chip-state">{{ draftStatusText(row.draft) }}</span>
              <div>
                <input
                  :value="row.draft.display_name"
                  placeholder="姓名可不填，默认临时人员N"
                  :disabled="Boolean(draftSendingById[String(row.draft.draft_id || '')])"
                  @input="emit('update-draft-name', String(row.draft.draft_id || ''), ($event.target as HTMLInputElement).value)"
                  @blur="emit('ensure-draft-name', row.draft)"
                />
                <small v-if="row.draft.error">{{ row.draft.error }}</small>
                <small v-else-if="draftDisabledReason(row.draft)" class="row-disabled-reason">
                  {{ draftDisabledReason(row.draft) }}
                </small>
                <small v-else>可网页签名或发送链接</small>
              </div>
              <div class="drawer-actions">
                <button
                  class="drawer-action"
                  type="button"
                  :disabled="Boolean(draftSendingById[String(row.draft.draft_id || '')])"
                  :title="draftSendingById[String(row.draft.draft_id || '')] ? '正在创建临时人员' : '在当前网页手写签名'"
                  @click.stop="emit('web-sign-draft', row.draft)"
                >
                  网页签名
                </button>
                <button
                  class="drawer-action link-action"
                  type="button"
                  :disabled="Boolean(draftDisabledReason(row.draft))"
                  :title="draftDisabledReason(row.draft)"
                  @click.stop="emit('send-draft-link', row.draft)"
                >
                  {{ draftSendingById[String(row.draft.draft_id || '')] ? '发送中' : '发送链接' }}
                </button>
                <button class="drawer-remove" type="button" @click.stop="emit('remove-draft', String(row.draft.draft_id || ''))">移除</button>
              </div>
            </article>
          </template>
          <div v-if="!drawerVisibleRows.length" class="drawer-empty">当前筛选下没有临时/外部人员。</div>
        </MopSignatureDrawer>
      </div>
    </div>
    <div v-else class="other-signature-empty">
      未添加人员。
    </div>
    <div class="external-signature-reuse">
      <button
        class="reuse-toggle"
        type="button"
        :class="{ open: externalReuseOpen }"
        :aria-expanded="externalReuseOpen"
        @click="externalReuseOpen = !externalReuseOpen"
      >
        {{ externalReuseOpen ? "收起" : "选已有签名" }}
        <em v-if="externalPeople.length">{{ externalPeople.length }}</em>
      </button>
      <div v-if="externalReuseOpen" class="external-signature-reuse-body">
        <label class="field external-signature-search">
          <span>搜索其他人员签名</span>
          <div class="inline-search">
            <input
              :value="externalSearch"
              placeholder="姓名、楼栋、专业"
              @input="emit('update:externalSearch', ($event.target as HTMLInputElement).value)"
            />
            <button
              class="btn ghost signature-refresh"
              type="button"
              :disabled="externalLoading"
              title="重新读取其他人员签名"
              @click="emit('refresh-external')"
            >
              {{ externalLoading ? "读取中" : "刷新" }}
            </button>
          </div>
          <small class="search-inline-status">{{ externalStatusText }}</small>
        </label>
        <div v-if="externalPeople.length" class="external-signature-results">
          <button
            v-for="person in externalPeople"
            :key="String(person.record_id || person.name || '')"
            type="button"
            @click="emit('add-external', person)"
          >
            <img :src="person.signature_preview_url" alt="已有其他人员签名" @error="emit('image-error', person)" />
            <span>
              <strong>{{ person.name || '其他人员' }}</strong>
              <small>
                <template v-if="person.building">{{ person.building }} · </template>
                <template v-if="person.specialty">{{ person.specialty }} · </template>
                已保存
              </small>
            </span>
            <em>加入</em>
          </button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import MopSignatureDrawer from "./MopSignatureDrawer.vue";

type Dict = Record<string, any>;
type SignatureRole = "implementer" | "auditor";
type OtherSignatureRow = {
  kind: string;
  row_key: string;
  person: Dict;
  draft: Dict;
  signed: boolean;
  display_name: string;
};

const props = defineProps<{
  role: SignatureRole;
  addDisabledReason: string;
  displayRows: OtherSignatureRow[];
  previewRow: OtherSignatureRow | null;
  unsignedCount: number;
  drawerOpen: boolean;
  temporaryLinkSendingById: Record<string, boolean>;
  temporaryLinkSentAtById: Record<string, string>;
  temporaryLinkErrorById: Record<string, string>;
  draftSendingById: Record<string, boolean>;
  externalSearch: string;
  externalLoading: boolean;
  externalStatusText: string;
  externalPeople: Dict[];
  personStatusText: (person: Dict) => string;
  personWebSignDisabledReason: (person: Dict) => string;
  draftStatusText: (draft: Dict) => string;
  draftDisabledReason: (draft: Dict) => string;
}>();

const externalReuseOpen = ref(false);
const drawerSearch = ref("");
const drawerFilter = ref<"all" | "unsigned" | "signed">("all");
const signedCount = computed(() => Math.max(0, props.displayRows.length - props.unsignedCount));
const drawerVisibleRows = computed(() => {
  const query = drawerSearch.value.trim().toLowerCase();
  return props.displayRows.filter((row) => {
    if (drawerFilter.value === "unsigned" && row.signed) return false;
    if (drawerFilter.value === "signed" && !row.signed) return false;
    if (!query) return true;
    const values = [
      row.display_name,
      row.person?.name,
      row.person?.display_name,
      row.person?.building,
      row.person?.specialty,
      row.draft?.display_name,
      row.draft?.status,
    ];
    return values.some((value) => String(value || "").toLowerCase().includes(query));
  });
});
const summaryText = computed(() => {
  if (!props.displayRows.length) return "未添加";
  if (!props.unsignedCount) return "签名齐全";
  return `${signedCount.value}/${props.displayRows.length} 已签`;
});
const summaryTone = computed(() => ({
  ready: props.displayRows.length > 0 && props.unsignedCount === 0,
  pending: props.unsignedCount > 0,
  empty: props.displayRows.length === 0,
}));
const drawerButtonText = computed(() => {
  return `查看临时 ${props.displayRows.length}`;
});

const emit = defineEmits<{
  "add-other": [];
  "update:drawerOpen": [value: boolean];
  "image-error": [person: Dict];
  "web-sign-person": [person: Dict];
  "send-temp-person": [person: Dict];
  "remove-person": [key: string];
  "update-draft-name": [draftId: string, value: string];
  "ensure-draft-name": [draft: Dict];
  "web-sign-draft": [draft: Dict];
  "send-draft-link": [draft: Dict];
  "remove-draft": [draftId: string];
  "update:externalSearch": [value: string];
  "refresh-external": [];
  "add-external": [person: Dict];
}>();

function signaturePersonKey(person: Dict): string {
  const source = String(person?.source || "");
  if (source === "external") return `external:${String(person?.record_id || "")}`;
  if (source === "temporary" || person?.temp_id) return `temporary:${String(person?.temp_id || person?.record_id || "")}`;
  return String(person?.record_id || "");
}

function drawerPersonStatus(row: OtherSignatureRow): string {
  if (row.kind !== "person") return row.draft?.error || props.draftStatusText(row.draft);
  const tempId = String(row.person?.temp_id || "");
  if (tempId && props.temporaryLinkErrorById[tempId]) return `链接失败：${props.temporaryLinkErrorById[tempId]}`;
  if (tempId && props.temporaryLinkSentAtById[tempId]) return `链接已发送 ${props.temporaryLinkSentAtById[tempId]}`;
  return props.personStatusText(row.person);
}
</script>

<style scoped>
.other-signature-panel {
  display: grid;
  gap: 5px;
  min-width: 0;
  border: 1px solid #fed7aa;
  border-radius: 16px;
  background: linear-gradient(135deg, rgba(255, 247, 237, 0.98), rgba(255, 255, 255, 0.94));
  box-shadow: inset 4px 0 0 #f97316;
  padding: 7px;
  overflow: visible;
}

.other-signature-panel .btn {
  min-height: 24px;
  border-radius: 999px;
  padding: 3px 8px;
  font-size: 11px;
  font-weight: 850;
}

.other-signature-head {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  min-width: 0;
}

.other-signature-head > div {
  min-width: 0;
}

.other-signature-head strong {
  display: block;
  color: #9a3412;
  font-size: 11px;
  font-weight: 950;
}

.other-signature-head small,
.other-signature-empty {
  color: #9a3412;
  font-size: 11px;
  line-height: 1.35;
}

.other-signature-head small {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  min-height: 18px;
  margin-top: 2px;
  border-radius: 999px;
  padding: 2px 5px;
  background: #fff7ed;
  font-weight: 900;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.other-signature-head small.ready {
  background: #dcfce7;
  color: #047857;
}

.other-signature-head small.pending {
  background: #fff7ed;
  color: #c2410c;
}

.other-signature-head small.empty {
  background: #fffbeb;
  color: #9a3412;
}

.other-signature-disabled {
  margin: 0;
  border: 1px solid #fed7aa;
  border-radius: 12px;
  padding: 5px 8px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 11px;
  font-weight: 900;
  line-height: 1.35;
  overflow-wrap: anywhere;
}

.other-signature-empty {
  border: 1px dashed rgba(251, 146, 60, 0.45);
  border-radius: 12px;
  background: rgba(255, 251, 235, 0.72);
  padding: 6px 8px;
  font-weight: 850;
}

.other-signature-list {
  display: grid;
  gap: 6px;
  overflow: visible;
}

.other-signature-summary {
  position: relative;
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(86px, auto);
  align-items: center;
  gap: 6px;
  min-height: 32px;
  min-width: 0;
  overflow: visible;
  isolation: isolate;
}

.other-signature-row {
  display: grid;
  grid-template-columns: 64px minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  min-height: 40px;
  border: 1px solid #d8e5f7;
  border-radius: 13px;
  background: #ffffff;
  padding: 6px;
}

.other-signature-summary .summary-chip {
  min-width: 0;
  width: 100%;
  height: 32px;
  grid-template-columns: 48px minmax(56px, 1fr);
  padding: 4px 6px;
}

.other-signature-summary .summary-chip img {
  width: 46px;
  height: 20px;
}

.other-signature-summary .summary-chip .other-signature-state {
  width: 46px;
  min-height: 20px;
}

.other-signature-row.signed {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.other-signature-row.pending,
.other-signature-row.draft {
  border-color: #fed7aa;
  background: #fff7ed;
}

.other-signature-row.failed {
  border-color: #fecaca;
  background: #fef2f2;
}

.other-signature-row img {
  width: 62px;
  height: 22px;
  object-fit: contain;
}

.other-signature-row strong,
.other-signature-row small {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.other-signature-row strong {
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
}

.other-signature-row small {
  margin-top: 2px;
  color: #64748b;
  font-size: 11px;
}

.other-signature-row .row-disabled-reason {
  color: #c2410c;
  font-weight: 850;
}

.other-signature-state,
.signature-chip-state {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 22px;
  border-radius: 999px;
  background: #fff7ed;
  color: #c2410c;
  padding: 3px 7px;
  font-size: 11px;
  font-weight: 850;
  white-space: nowrap;
}

.signature-chip-state {
  min-width: 54px;
}

.selected-signature-open {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  flex: 0 0 auto;
  max-width: 100%;
  min-width: 0;
  min-height: 28px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  padding: 0 8px;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
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

.temporary-open {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.temporary-open.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.temporary-open.pending {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.inline-search {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 6px;
}

.inline-search input {
  min-width: 0;
}

.signature-refresh {
  min-width: 42px;
  min-height: 30px;
  border-radius: 999px;
  padding: 4px 9px;
  font-size: 11px;
  line-height: 1;
}

.external-signature-reuse {
  display: grid;
  gap: 6px;
  border-top: 1px dashed rgba(251, 146, 60, 0.32);
  padding-top: 6px;
}

.drawer-filter-bar {
  display: grid;
  grid-template-columns: minmax(104px, auto) minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  position: sticky;
  top: 0;
  z-index: 2;
  border: 1px solid #fed7aa;
  border-radius: 14px;
  background: rgba(255, 251, 235, 0.96);
  padding: 8px;
  backdrop-filter: blur(10px);
}

.drawer-progress {
  display: grid;
  gap: 2px;
  color: #c2410c;
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
  border: 1px solid #fed7aa;
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
  border-color: #f97316;
  box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.12);
}

.drawer-filter-tabs {
  display: inline-flex;
  gap: 4px;
  border: 1px solid rgba(254, 215, 170, 0.9);
  border-radius: 999px;
  background: #ffffff;
  padding: 3px;
}

.drawer-filter-tabs button {
  border: 0;
  border-radius: 999px;
  background: transparent;
  color: #9a3412;
  padding: 5px 9px;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.drawer-filter-tabs button.active {
  background: #f97316;
  color: #ffffff;
}

.drawer-empty {
  border: 1px dashed #fed7aa;
  border-radius: 14px;
  padding: 14px;
  background: #fffbeb;
  color: #9a3412;
  font-size: 12px;
  font-weight: 850;
  text-align: center;
}

:deep(small.failed) {
  color: #b91c1c !important;
}

.reuse-toggle {
  justify-self: start;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 26px;
  border: 1px solid rgba(251, 146, 60, 0.42);
  border-radius: 999px;
  padding: 0 9px;
  background: rgba(255, 247, 237, 0.78);
  color: #9a3412;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.reuse-toggle.open {
  border-color: #fed7aa;
  background: #fff7ed;
}

.reuse-toggle em {
  display: inline-grid;
  place-items: center;
  min-width: 20px;
  height: 20px;
  border-radius: 999px;
  background: #ffffff;
  color: #c2410c;
  font-size: 11px;
  font-style: normal;
  font-weight: 950;
}

.external-signature-reuse-body {
  display: grid;
  gap: 6px;
  max-height: min(24vh, 180px);
  overflow: auto;
  border: 1px solid rgba(254, 215, 170, 0.72);
  border-radius: 13px;
  padding: 7px;
  background: rgba(255, 251, 235, 0.56);
}

.search-inline-status {
  display: block;
  margin-top: 5px;
  color: #64748b;
  font-size: 11px;
  line-height: 1.4;
}

.external-signature-search {
  display: grid;
  gap: 5px;
  margin-top: 2px;
  color: #475569;
  font-size: 11px;
  font-weight: 900;
}

.external-signature-results {
  display: grid;
  gap: 6px;
  max-height: min(16vh, 126px);
  overflow: auto;
  padding-right: 3px;
  scrollbar-width: thin;
}

.external-signature-results button {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  border: 1px solid #d8e5f7;
  border-radius: 13px;
  background: #ffffff;
  padding: 6px;
  text-align: left;
  cursor: pointer;
}

.external-signature-results button:hover {
  border-color: #1e63ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.12);
}

.external-signature-results img {
  width: 56px;
  height: 22px;
  object-fit: contain;
}

.external-signature-results strong,
.external-signature-results small {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.external-signature-results strong {
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
}

.external-signature-results small {
  margin-top: 2px;
  color: #64748b;
  font-size: 11px;
}

.external-signature-results button > em {
  border: 0;
  border-radius: 999px;
  background: #dbeafe;
  color: #1d4ed8;
  padding: 6px 9px;
  font-size: 11px;
  font-style: normal;
  font-weight: 850;
  white-space: nowrap;
}

@media (max-width: 760px) {
  .other-signature-head {
    grid-template-columns: 1fr;
  }

  .other-signature-summary {
    display: grid;
    grid-template-columns: 1fr;
  }

  .other-signature-summary .summary-chip {
    width: auto;
  }

  .temporary-open {
    min-width: max-content;
    padding: 0 10px;
  }

  .external-signature-results button {
    grid-template-columns: 64px minmax(0, 1fr);
  }

  .external-signature-results button > em {
    grid-column: 1 / -1;
    justify-self: stretch;
    text-align: center;
  }

  .drawer-filter-bar {
    grid-template-columns: 1fr;
  }
}
</style>
