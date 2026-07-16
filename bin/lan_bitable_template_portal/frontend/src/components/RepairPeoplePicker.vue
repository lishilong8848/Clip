<template>
  <div ref="rootRef" class="repair-people-picker">
    <label class="repair-people-label" :for="inputId">{{ label }}</label>

    <div v-if="selectedPeople.length" class="repair-people-selected">
      <span v-for="person in selectedPeople" :key="personKey(person)" class="repair-person-chip">
        <UserRound :size="14" aria-hidden="true" />
        <b>{{ personName(person) }}</b>
        <button
          type="button"
          :aria-label="`移除${personName(person)}`"
          :disabled="disabled"
          @click="removePerson(person)"
        >
          <X :size="13" aria-hidden="true" />
        </button>
      </span>
    </div>

    <div class="repair-people-search" :class="{ active: open }">
      <Search :size="16" aria-hidden="true" />
      <input
        :id="inputId"
        v-model.trim="query"
        type="search"
        autocomplete="off"
        placeholder="输入姓名搜索"
        :disabled="disabled"
        aria-haspopup="listbox"
        :aria-expanded="open"
        @focus="openPicker"
        @keydown.esc.stop="open = false"
      />
      <LoaderCircle v-if="loading" :size="16" class="spinning" aria-hidden="true" />
    </div>

    <div v-if="open" class="repair-people-popover">
      <div v-if="errorText" class="repair-people-state failed">{{ errorText }}</div>
      <div v-else-if="loading && !results.length" class="repair-people-state">正在搜索</div>
      <div v-else-if="!results.length" class="repair-people-state">未找到人员</div>
      <div v-else class="repair-people-results" role="listbox" aria-label="随工人员搜索结果">
        <button
          v-for="person in results"
          :key="resultKey(person)"
          type="button"
          role="option"
          :aria-selected="isSelected(person)"
          :disabled="!person.selectable"
          :class="{ selected: isSelected(person) }"
          @click="togglePerson(person)"
        >
          <span class="repair-person-avatar">{{ personName(person).slice(0, 1) }}</span>
          <span class="repair-person-info">
            <b>{{ personName(person) }}</b>
            <small>{{ personMeta(person) }}</small>
          </span>
          <Check v-if="isSelected(person)" :size="16" aria-hidden="true" />
          <small v-else-if="!person.selectable" class="repair-person-unavailable">资料不完整</small>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from "vue";
import { Check, LoaderCircle, Search, UserRound, X } from "lucide-vue-next";
import { requestJson } from "../api/client";
import type { LooseDict } from "../types";

const props = withDefaults(defineProps<{
  scope: string;
  inputId: string;
  label?: string;
  modelValue?: LooseDict[];
  disabled?: boolean;
}>(), {
  label: "随工人员（我方维修人员）",
  modelValue: () => [],
  disabled: false,
});

const emit = defineEmits<{
  "update:modelValue": [value: LooseDict[]];
  edited: [];
}>();

const rootRef = ref<HTMLElement | null>(null);
const query = ref("");
const open = ref(false);
const loading = ref(false);
const errorText = ref("");
const results = shallowRef<LooseDict[]>([]);
let queryTimer: ReturnType<typeof setTimeout> | undefined;
let requestVersion = 0;
let peopleAbortController: AbortController | null = null;
const peopleCache = new Map<string, { expiresAt: number; people: LooseDict[] }>();
const PEOPLE_CACHE_TTL_MS = 45_000;

const selectedPeople = computed(() => props.modelValue);

function personKey(person: LooseDict): string {
  return String(person.user_id || person.id || person.open_id || person.person_record_id || "").trim();
}

function resultKey(person: LooseDict): string {
  return personKey(person) || `${String(person.name || "")}-${String(person.employee_no || "")}`;
}

function personName(person: LooseDict): string {
  return String(person.name || "已选人员").trim() || "已选人员";
}

function personMeta(person: LooseDict): string {
  return [person.employee_no, person.building, person.position]
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .join(" · ") || "人员信息";
}

function isSelected(person: LooseDict): boolean {
  const key = personKey(person);
  return Boolean(key && props.modelValue.some((item) => personKey(item) === key));
}

function normalizedPerson(person: LooseDict): LooseDict {
  return {
    user_id: personKey(person),
    name: personName(person),
    employee_no: String(person.employee_no || "").trim(),
    building: String(person.building || "").trim(),
    position: String(person.position || "").trim(),
  };
}

function publish(value: LooseDict[]): void {
  emit("update:modelValue", value);
  emit("edited");
}

function togglePerson(person: LooseDict): void {
  const key = personKey(person);
  if (!key || !person.selectable) return;
  if (isSelected(person)) {
    publish(props.modelValue.filter((item) => personKey(item) !== key));
    return;
  }
  publish([...props.modelValue, normalizedPerson(person)]);
}

function removePerson(person: LooseDict): void {
  const key = personKey(person);
  publish(props.modelValue.filter((item) => personKey(item) !== key));
}

async function loadPeople(): Promise<void> {
  const currentVersion = ++requestVersion;
  const scope = props.scope || "ALL";
  const normalizedQuery = query.value.trim();
  const cacheKey = `${scope}\n${normalizedQuery.toLowerCase()}`;
  peopleAbortController?.abort();
  peopleAbortController = null;
  const cached = peopleCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    results.value = cached.people;
    errorText.value = "";
    loading.value = false;
    return;
  }
  const abortController = new AbortController();
  peopleAbortController = abortController;
  loading.value = true;
  errorText.value = "";
  try {
    const params = new URLSearchParams({
      scope,
      q: normalizedQuery,
      limit: "80",
    });
    const payload = await requestJson(
      `/api/repair-management/people?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (currentVersion !== requestVersion) return;
    results.value = Array.isArray(payload.people) ? payload.people : [];
    peopleCache.set(cacheKey, {
      expiresAt: Date.now() + PEOPLE_CACHE_TTL_MS,
      people: results.value,
    });
    if (peopleCache.size > 24) {
      peopleCache.delete(peopleCache.keys().next().value || "");
    }
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (currentVersion !== requestVersion) return;
    results.value = [];
    errorText.value = error instanceof Error ? error.message : "人员搜索失败";
  } finally {
    if (peopleAbortController === abortController) peopleAbortController = null;
    if (currentVersion === requestVersion) loading.value = false;
  }
}

function openPicker(): void {
  if (props.disabled) return;
  open.value = true;
  void loadPeople();
}

function handleDocumentPointerDown(event: PointerEvent): void {
  if (!open.value) return;
  const target = event.target as Node | null;
  if (target && rootRef.value?.contains(target)) return;
  open.value = false;
}

watch(query, () => {
  if (!open.value) return;
  if (queryTimer) clearTimeout(queryTimer);
  queryTimer = setTimeout(() => void loadPeople(), 250);
});

watch(() => props.scope, () => {
  peopleAbortController?.abort();
  peopleAbortController = null;
  requestVersion += 1;
  results.value = [];
  if (open.value) void loadPeople();
});

onMounted(() => document.addEventListener("pointerdown", handleDocumentPointerDown));
onBeforeUnmount(() => {
  document.removeEventListener("pointerdown", handleDocumentPointerDown);
  if (queryTimer) clearTimeout(queryTimer);
  peopleAbortController?.abort();
  peopleAbortController = null;
});
</script>

<style scoped>
.repair-people-picker {
  position: relative;
  min-width: 0;
  grid-column: 1 / -1;
  display: grid;
  gap: 5px;
}

.repair-people-label {
  color: #425c79;
  font-size: 11px;
  font-weight: 650;
  line-height: 1.2;
}

.repair-people-selected {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.repair-person-chip {
  min-width: 0;
  height: 28px;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 0 5px 0 8px;
  border: 1px solid #b9d2f6;
  border-radius: 8px;
  background: #eef6ff;
  color: #1557a7;
  font-size: 12px;
}

.repair-person-chip b {
  max-width: 128px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repair-person-chip button {
  width: 21px;
  height: 21px;
  display: inline-grid;
  place-items: center;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #557493;
  cursor: pointer;
}

.repair-person-chip button:hover { background: #dcecff; color: #174f91; }

.repair-people-search {
  min-height: 32px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  padding: 0 9px;
  border: 1px solid #cbd8e8;
  border-radius: 8px;
  background: #fff;
  color: #6b819a;
}

.repair-people-search.active {
  border-color: #3483e8;
  box-shadow: 0 0 0 3px rgba(52, 131, 232, 0.12);
}

.repair-people-search input {
  min-width: 0;
  height: 30px;
  border: 0;
  outline: 0;
  background: transparent;
  color: #17324f;
  font-size: 13px;
}

.repair-people-popover {
  position: absolute;
  z-index: 80;
  top: calc(100% + 5px);
  right: 0;
  left: 0;
  max-height: 280px;
  overflow: hidden;
  border: 1px solid #c7d7e9;
  border-radius: 10px;
  background: #fff;
  box-shadow: 0 16px 34px rgba(25, 67, 112, 0.18);
}

.repair-people-results {
  max-height: 278px;
  overflow-y: auto;
  padding: 5px;
}

.repair-people-results > button {
  width: 100%;
  min-height: 48px;
  display: grid;
  grid-template-columns: 32px minmax(0, 1fr) auto;
  align-items: center;
  gap: 9px;
  padding: 6px 9px;
  border: 0;
  border-radius: 8px;
  background: transparent;
  text-align: left;
  cursor: pointer;
}

.repair-people-results > button:hover,
.repair-people-results > button.selected { background: #edf5ff; }
.repair-people-results > button.selected { color: #0f63ca; }
.repair-people-results > button:disabled { cursor: not-allowed; opacity: 0.55; }

.repair-person-avatar {
  width: 32px;
  height: 32px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  background: #e2efff;
  color: #1267cb;
  font-size: 13px;
  font-weight: 750;
}

.repair-person-info { min-width: 0; display: grid; gap: 2px; }
.repair-person-info b,
.repair-person-info small { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.repair-person-info b { color: #17324f; font-size: 13px; }
.repair-person-info small { color: #71879d; font-size: 11px; }
.repair-person-unavailable { color: #a05c32; font-size: 11px; }

.repair-people-state {
  padding: 16px;
  color: #6f8499;
  font-size: 12px;
  text-align: center;
}
.repair-people-state.failed { color: #b63c4d; }

.spinning { animation: repair-people-spin 0.8s linear infinite; }
@keyframes repair-people-spin { to { transform: rotate(360deg); } }
</style>
