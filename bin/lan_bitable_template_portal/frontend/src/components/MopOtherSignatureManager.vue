<template>
  <section class="other-signature-panel">
    <header class="other-signature-head">
      <div>
        <strong>当前临时/外部人员</strong>
        <small :class="summaryTone">{{ summaryText }}</small>
      </div>
    </header>

    <div v-if="displayRows.length" class="task-toolbar">
      <input v-model="taskSearch" type="search" placeholder="搜索临时/外部人员" />
      <div class="task-filter-tabs" role="group" aria-label="临时人员签名筛选">
        <button type="button" :aria-pressed="taskFilter === 'all'" :class="{ active: taskFilter === 'all' }" @click="taskFilter = 'all'">
          全部 {{ displayRows.length }}
        </button>
        <button type="button" :aria-pressed="taskFilter === 'unsigned'" :class="{ active: taskFilter === 'unsigned' }" @click="taskFilter = 'unsigned'">
          未签 {{ unsignedCount }}
        </button>
        <button type="button" :aria-pressed="taskFilter === 'signed'" :class="{ active: taskFilter === 'signed' }" @click="taskFilter = 'signed'">
          已签 {{ signedCount }}
        </button>
      </div>
    </div>

    <div v-if="visibleRows.length" class="other-signature-task-list">
      <template v-for="row in visibleRows" :key="row.row_key">
        <article
          :class="{ ready: row.signed, pending: !row.signed }"
        >
          <div class="signature-preview">
            <span>{{ row.signed ? "已签名" : "未签名" }}</span>
          </div>
          <div class="person-summary">
            <strong>{{ row.display_name }}</strong>
            <small>
              {{ personStatus(row) }}
            </small>
          </div>
          <div class="task-actions">
            <button type="button" class="remove-action" @click="emit('remove-person', signaturePersonKey(row.person))">
              移除
            </button>
          </div>
        </article>
      </template>
    </div>
    <div v-else-if="displayRows.length" class="panel-empty">当前筛选下没有人员。</div>
    <div v-else class="panel-empty">尚未添加临时或外部人员。</div>

    <section class="external-signature-reuse">
      <button type="button"
        class="reuse-toggle"
        :class="{ open: externalReuseOpen }"
        :aria-expanded="externalReuseOpen"
        @click="externalReuseOpen = !externalReuseOpen"
      >
        {{ externalReuseOpen ? "收起已有签名" : "选择已有外部签名" }}
        <em v-if="externalPeople.length">{{ externalPeople.length }}</em>
      </button>
      <div v-if="externalReuseOpen" class="external-signature-reuse-body">
        <label class="external-signature-search">
          <span>搜索其他人员签名</span>
          <div class="inline-search">
            <input
              :value="externalSearch"
              placeholder="姓名、楼栋、专业"
              @input="emit('update:externalSearch', ($event.target as HTMLInputElement).value)"
            />
            <button type="button"
              class="signature-refresh"
              :disabled="externalLoading"
              title="重新读取其他人员签名"
              @click="emit('refresh-external')"
            >
              {{ externalLoading ? "读取中" : "刷新签名" }}
            </button>
          </div>
          <small>{{ externalStatusText }}</small>
        </label>
        <div v-if="externalPeople.length" class="external-signature-results">
          <button type="button"
            v-for="person in externalPeople"
            :key="String(person.record_id || person.name || '')"
            :disabled="!person.has_signature"
            :title="person.signature_reason || (!person.has_signature ? '请在首页指纹入口完成签名后刷新' : '选择该签名')"
            @click="chooseExternal(person)"
          >
            <span class="protected-signature">{{ person.has_signature ? '已签名' : person.signature_requires_resign ? '需重新签名' : '未签名' }}</span>
            <span>
              <strong>{{ person.name || "其他人员" }}</strong>
              <small>
                <template v-if="person.building">{{ person.building }} · </template>
                <template v-if="person.specialty">{{ person.specialty }} · </template>
                {{ person.has_signature ? '已有签名' : '请先完成签名' }}
              </small>
            </span>
            <em>加入</em>
          </button>
        </div>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";

type Dict = Record<string, any>;
type SignatureRole = "implementer" | "auditor" | "inspector";
type OtherSignatureRow = {
  kind: string;
  row_key: string;
  person: Dict;
  signed: boolean;
  display_name: string;
};

const props = defineProps<{
  role: SignatureRole;
  displayRows: OtherSignatureRow[];
  unsignedCount: number;
  externalSearch: string;
  externalLoading: boolean;
  externalStatusText: string;
  externalPeople: Dict[];
  personStatusText: (person: Dict) => string;
  active?: boolean;
}>();

const emit = defineEmits<{
  "remove-person": [key: string];
  "update:externalSearch": [value: string];
  "refresh-external": [];
  "add-external": [person: Dict];
}>();

const externalReuseOpen = ref(true);
const taskSearch = ref("");
const taskFilter = ref<"all" | "unsigned" | "signed">("all");
const signedCount = computed(() => Math.max(0, props.displayRows.length - props.unsignedCount));
const summaryText = computed(() => {
  if (!props.displayRows.length) return "未添加";
  return props.unsignedCount
    ? `已签 ${signedCount.value}/${props.displayRows.length} · 未签 ${props.unsignedCount}`
    : `全部已签 ${props.displayRows.length}`;
});
const summaryTone = computed(() => ({
  ready: props.displayRows.length > 0 && props.unsignedCount === 0,
  pending: props.unsignedCount > 0,
  empty: props.displayRows.length === 0,
}));
const visibleRows = computed(() => {
  const query = taskSearch.value.trim().toLowerCase();
  return props.displayRows.filter((row) => {
    if (taskFilter.value === "unsigned" && row.signed) return false;
    if (taskFilter.value === "signed" && !row.signed) return false;
    if (!query) return true;
    return [
      row.display_name,
      row.person?.name,
      row.person?.display_name,
      row.person?.building,
      row.person?.specialty,
    ].some((value) => String(value || "").toLowerCase().includes(query));
  });
});

watch(() => props.role, () => {
  taskSearch.value = "";
  taskFilter.value = "all";
  externalReuseOpen.value = false;
});

watch(() => props.active, (active) => {
  if (!active) externalReuseOpen.value = false;
});

function chooseExternal(person: Dict): void {
  externalReuseOpen.value = false;
  emit("add-external", person);
}

function signaturePersonKey(person: Dict): string {
  const source = String(person?.source || "");
  if (source === "external") return `external:${String(person?.record_id || "")}`;
  if (source === "temporary" || person?.temp_id) return `temporary:${String(person?.temp_id || person?.record_id || "")}`;
  return String(person?.record_id || "");
}

function personStatus(row: OtherSignatureRow): string {
  return row.person?.signature_reason || props.personStatusText(row.person);
}
</script>

<style scoped>
.other-signature-panel {
  min-width: 0;
  display: grid;
  align-content: start;
  gap: 8px;
  border: 1px solid #fed7aa;
  border-radius: 12px;
  padding: 9px;
  background: #fffaf2;
  box-shadow: inset 3px 0 0 #f97316;
}

.other-signature-head {
  min-width: 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.other-signature-head > div {
  min-width: 0;
}

.other-signature-head strong,
.other-signature-head small {
  display: block;
}

.other-signature-head strong {
  color: #7c2d12;
  font-size: 13px;
  font-weight: 950;
}

.other-signature-head small {
  margin-top: 2px;
  color: #9a3412;
  font-size: 11px;
  font-weight: 850;
}

.other-signature-head small.ready {
  color: #047857;
}

.add-person-button,
.signature-refresh {
  flex: 0 0 auto;
  min-height: 30px;
  border: 1px solid #fdba74;
  border-radius: 8px;
  padding: 0 10px;
  background: #ffffff;
  color: #c2410c;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.add-person-button:disabled,
.signature-refresh:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.other-signature-disabled {
  margin: 0;
  border: 1px solid #fed7aa;
  border-radius: 8px;
  padding: 6px 8px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 11px;
  font-weight: 850;
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
  border: 1px solid #fed7aa;
  border-radius: 10px;
  background: rgba(255, 250, 242, 0.96);
  backdrop-filter: blur(8px);
}

.task-toolbar input,
.draft-name input,
.external-signature-search input {
  min-width: 0;
  height: 32px;
  border: 1px solid #fed7aa;
  border-radius: 8px;
  padding: 0 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
  font-size: 12px;
  outline: none;
}

.task-toolbar input:focus,
.draft-name input:focus,
.external-signature-search input:focus {
  border-color: #f97316;
  box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.12);
}

.task-filter-tabs {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 3px;
  border: 1px solid #fed7aa;
  border-radius: 8px;
  background: #ffffff;
}

.task-filter-tabs button {
  min-height: 26px;
  border: 0;
  border-radius: 6px;
  padding: 0 8px;
  background: transparent;
  color: #9a3412;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.task-filter-tabs button.active {
  background: #f97316;
  color: #ffffff;
}

.other-signature-task-list {
  display: grid;
  gap: 6px;
}

.other-signature-task-list article {
  min-width: 0;
  min-height: 52px;
  display: grid;
  grid-template-columns: 72px minmax(0, 1fr) minmax(220px, auto);
  align-items: center;
  gap: 8px;
  border: 1px solid #fed7aa;
  border-radius: 10px;
  padding: 6px;
  background: #ffffff;
}

.other-signature-task-list article.ready {
  border-color: #a7e8c2;
  background: #f0fdf4;
}

.other-signature-task-list article.pending {
  background: #fff7ed;
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
.protected-signature { min-width: 58px; min-height: 36px; display: grid; place-items: center; border-radius: 7px; background: #eef5ff; color: #175ab9; font-size: 11px; font-weight: 900; }

.person-summary {
  min-width: 0;
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

.draft-name {
  display: grid;
  gap: 3px;
}

.task-actions {
  display: flex;
  justify-content: flex-end;
  gap: 5px;
}

.task-actions button {
  min-height: 30px;
  border: 1px solid #fdba74;
  border-radius: 8px;
  padding: 0 9px;
  background: #ffffff;
  color: #c2410c;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
  white-space: nowrap;
}

.task-actions button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
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
  border: 1px dashed #fdba74;
  border-radius: 10px;
  padding: 18px 12px;
  background: rgba(255, 255, 255, 0.7);
  color: #9a3412;
  font-size: 12px;
  font-weight: 850;
  text-align: center;
}

.external-signature-reuse {
  display: grid;
  gap: 7px;
  border-top: 1px dashed #fdba74;
  padding-top: 8px;
}

.reuse-toggle {
  justify-self: start;
  min-height: 30px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid #fdba74;
  border-radius: 8px;
  padding: 0 10px;
  background: #ffffff;
  color: #9a3412;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.reuse-toggle.open {
  background: #fff7ed;
}

.reuse-toggle em {
  min-width: 20px;
  height: 20px;
  display: grid;
  place-items: center;
  border-radius: 999px;
  background: #ffedd5;
  color: #c2410c;
  font-size: 10px;
  font-style: normal;
}

.external-signature-reuse-body {
  display: grid;
  gap: 7px;
  border: 1px solid #fed7aa;
  border-radius: 10px;
  padding: 8px;
  background: rgba(255, 255, 255, 0.72);
}

.external-signature-search {
  min-width: 0;
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 11px;
  font-weight: 900;
}

.inline-search {
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 6px;
}

.external-signature-search small {
  color: #64748b;
  font-size: 11px;
}

.external-signature-results {
  max-height: 240px;
  display: grid;
  gap: 6px;
  overflow: auto;
  overscroll-behavior: contain;
  scrollbar-width: thin;
}

.external-signature-results button {
  min-width: 0;
  display: grid;
  grid-template-columns: 62px minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  border: 1px solid #d8e5f7;
  border-radius: 9px;
  padding: 6px;
  background: #ffffff;
  text-align: left;
  cursor: pointer;
}

.external-signature-results button:hover {
  border-color: #1e63ff;
}

.external-signature-results img {
  width: 58px;
  height: 24px;
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
  border-radius: 7px;
  padding: 5px 9px;
  background: #dbeafe;
  color: #1d4ed8;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
}

@media (max-width: 760px) {
  .other-signature-head,
  .task-toolbar {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .other-signature-head {
    display: grid;
  }

  .add-person-button {
    min-height: 44px;
  }

  .task-toolbar input,
  .draft-name input,
  .external-signature-search input,
  .signature-refresh,
  .reuse-toggle,
  .task-filter-tabs button,
  .external-signature-results button {
    min-height: 44px;
  }

  .other-signature-task-list article {
    grid-template-columns: 64px minmax(0, 1fr);
  }

  .task-actions {
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .task-actions button {
    min-height: 44px;
  }
}
</style>
