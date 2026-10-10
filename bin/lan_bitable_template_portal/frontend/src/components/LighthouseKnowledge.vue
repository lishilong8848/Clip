<template>
  <section class="lighthouse-knowledge" aria-label="知识库检索">
    <div class="lk-search">
      <Search :size="15" aria-hidden="true" />
      <input
        v-model="query"
        type="search"
        placeholder="检索公司文档"
        aria-label="检索知识库"
        spellcheck="false"
        @keydown.enter.prevent="submit"
      />
      <Loader2 v-if="searching" :size="14" class="spin" aria-hidden="true" />
    </div>

    <p v-if="error" class="lk-state error" role="alert">{{ error }}</p>
    <p v-if="warning" class="lk-warning" role="status"><AlertTriangle :size="14" aria-hidden="true" /><span>{{ warning }}</span></p>
    <p v-if="mode" class="lk-mode">{{ mode === 'hybrid' ? '混合检索' : '关键词检索' }}<template v-if="!query.trim()"> · 未检索</template></p>

    <div v-if="searching && !items.length" class="lk-state" role="status"><Loader2 :size="16" class="spin" aria-hidden="true" />检索中…</div>
    <div v-else-if="!items.length && query.trim().length >= 2" class="lk-state">未找到匹配文档</div>
    <div v-else-if="items.length" class="lk-results">
      <button v-for="result in items" :key="resultKey(result)" type="button" class="lk-result" @click="open(result)">
        <span class="lk-result-file"><FileText :size="15" aria-hidden="true" /></span>
        <strong>{{ result.name }}</strong>
        <small class="lk-location">{{ result.location }}</small>
        <span class="lk-snippet">{{ result.text }}</span>
      </button>
    </div>

    <button type="button" class="lk-manage" @click="navigate('/knowledge-base')">
      <Database :size="15" aria-hidden="true" />管理知识库
    </button>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";
import { AlertTriangle, Database, FileText, Loader2, Search } from "lucide-vue-next";
import { requestJson } from "../api/client";
import { navigate } from "../navigation";

interface SearchItem {
  document_id: string;
  version: string | number;
  ordinal: number;
  name: string;
  location?: string;
  text?: string;
  url?: string;
  score?: number;
}

const props = withDefaults(defineProps<{ active?: boolean }>(), { active: true });

const query = ref("");
const items = ref<SearchItem[]>([]);
const mode = ref("");
const warning = ref("");
const searching = ref(false);
const error = ref("");

let debounceTimer = 0;
let controller: AbortController | undefined;
let suppressedSearch = false;

function resultKey(item: SearchItem): string {
  return `${item.document_id}:${String(item.version)}:${item.ordinal}`;
}

function cancelPending(): void {
  window.clearTimeout(debounceTimer);
  if (controller) {
    controller.abort();
    controller = undefined;
    searching.value = false;
  }
}

async function runSearch(immediate = false): Promise<void> {
  const q = query.value.trim();
  if (q.length < 2) {
    cancelPending();
    suppressedSearch = false;
    items.value = [];
    mode.value = "";
    warning.value = "";
    error.value = "";
    searching.value = false;
    return;
  }
  const timer = immediate ? 0 : 450;
  cancelPending();
  debounceTimer = window.setTimeout(async () => {
    if (!props.active) {
      suppressedSearch = true;
      return;
    }
    suppressedSearch = false;
    const current = new AbortController();
    controller = current;
    searching.value = true;
    error.value = "";
    try {
      const params = new URLSearchParams({ q });
      const data = await requestJson(`/api/assistant/knowledge/search?${params}`, { signal: current.signal, cache: "no-store" });
      if (controller !== current || current.signal.aborted || !props.active) return;
      items.value = data.items || [];
      mode.value = data.mode || "";
      warning.value = data.warning || "";
    } catch (cause) {
      if (controller === current && !current.signal.aborted && props.active) {
        error.value = cause instanceof Error ? cause.message : "检索失败，请重试。";
      }
    } finally {
      if (controller === current) searching.value = false;
    }
  }, timer);
}

function submit(): void {
  if (query.value.trim().length >= 2) void runSearch(true);
}

watch(query, () => void runSearch());
watch(() => props.active, active => {
  if (!active) {
    cancelPending();
    // Remember that a search was deferred while hidden so we can resume on next activation.
    if (query.value.trim().length >= 2) suppressedSearch = true;
  } else if (suppressedSearch && query.value.trim().length >= 2) {
    suppressedSearch = false;
    void runSearch(true);
  }
});

function open(item: SearchItem): void {
  const params = new URLSearchParams({
    document: item.document_id,
    version: String(item.version),
    chunk: String(item.ordinal),
  });
  navigate("/knowledge-base?" + params);
}

onBeforeUnmount(() => {
  cancelPending();
});
</script>

<style scoped>
.lighthouse-knowledge {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
  padding: 8px;
  box-sizing: border-box;
  color: var(--lh-charcoal, #1e293b);
}
.lk-search {
  flex: 0 0 36px;
  box-sizing: border-box;
  display: flex;
  align-items: center;
  gap: 7px;
  border: 1px solid var(--lh-border, #d8e5f7);
  border-radius: var(--lh-radius, 10px);
  padding: 7px 9px;
  background: var(--lh-surface-hover, #fff);
}
.lk-search > svg { flex: 0 0 auto; color: var(--lh-muted, #64748b); }
.lk-search input {
  flex: 1 1 auto;
  min-width: 0;
  border: 0;
  outline: 0;
  background: transparent;
  color: inherit;
  font: inherit;
  font-size: 13px;
}
.lk-search .spin { flex: 0 0 auto; color: var(--lh-accent, #1e63ff); }
.lk-state { margin: 0; padding: 10px 4px; color: var(--lh-muted, #64748b); font-size: 12px; display: flex; align-items: center; gap: 6px; overflow-wrap: anywhere; }
.lk-state.error { color: var(--lh-danger, #991b1b); }
.lk-warning {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  margin: 0;
  padding: 7px 8px;
  border: 1px solid #fde68a;
  border-radius: 8px;
  background: #fffbeb;
  color: var(--lh-warning, #92400e);
  font-size: 12px;
  line-height: 1.5;
  overflow-wrap: anywhere;
}
.lk-warning svg { flex: 0 0 auto; margin-top: 1px; }
.lk-mode { margin: 0; color: var(--lh-muted, #64748b); font-size: 11px; }
.lk-results { display: grid; align-content: start; gap: 4px; min-height: 0; overflow: auto; overscroll-behavior: contain; }
.lk-result {
  display: grid;
  gap: 3px;
  width: 100%;
  min-width: 0;
  text-align: left;
  border: 0;
  border-bottom: 1px solid var(--lh-border, #e5edf8);
  background: transparent;
  color: inherit;
  font: inherit;
  padding: 7px 3px;
  cursor: pointer;
}
.lk-result:hover { background: var(--lh-accent-soft, #eef4ff); }
.lk-result strong {
  font-size: 13px;
  overflow-wrap: anywhere;
  line-height: 1.4;
}
.lk-result-file { display: flex; align-items: center; gap: 6px; color: var(--lh-muted, #64748b); font-size: 11px; }
.lk-location { color: var(--lh-muted, #64748b); font-size: 11px; overflow-wrap: anywhere; }
.lk-snippet {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  color: var(--lh-muted, #64748b);
  font-size: 12px;
  line-height: 1.5;
  overflow-wrap: anywhere;
}
.lk-manage {
  margin-top: auto;
  flex: none;
  align-self: flex-start;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  justify-self: start;
  min-height: 28px;
  padding: 4px 8px;
  border: 0;
  border-radius: 999px;
  background: transparent;
  color: var(--lh-accent, #1e63ff);
  font: inherit;
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
}
.lk-manage:hover { background: var(--lh-accent-soft, #eef4ff); text-decoration: underline; }
.spin { animation: lk-spin 1s linear infinite; }
@keyframes lk-spin { to { transform: rotate(360deg); } }
</style>
