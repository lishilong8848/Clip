<template>
  <div class="command-anchor">
    <div v-if="modelValue.length" class="command-chips" aria-label="本轮技能和工具">
      <span v-for="item in modelValue" :key="item.kind + item.id">
        <BookOpen v-if="item.kind === 'skill'" :size="13" /><Wrench v-else :size="13" />
        <b>{{ item.label }}</b><button type="button" :aria-label="'移除 ' + item.label" @click="remove(item)"><X :size="13" /></button>
      </span>
    </div>
    <UiTransition name="ui-popover">
      <section v-if="visible" class="command-menu" aria-label="选择技能或工具" @keydown.esc.stop.prevent="dismiss">
        <header>
          <div class="command-tabs" role="tablist" aria-label="命令类型">
            <button type="button" role="tab" :aria-selected="kind === 'skills'" @click="changeKind('skills')"><BookOpen :size="14" />技能</button>
            <button type="button" role="tab" :aria-selected="kind === 'tools'" @click="changeKind('tools')"><Wrench :size="14" />工具</button>
          </div>
          <button type="button" class="command-icon" title="管理技能" aria-label="管理技能" @click="$emit('manage')"><Settings2 :size="16" /></button>
          <button type="button" class="command-icon" title="收起命令" aria-label="收起命令" @click="dismiss"><X :size="16" /></button>
        </header>
        <select v-if="kind === 'tools'" v-model="group" aria-label="工具分类" @change="page = 1; load()"><option value="">全部分类</option><option v-for="name in groups" :key="name">{{ name }}</option></select>
        <div v-if="loading" class="command-state" role="status"><Loader2 :size="16" class="spin" />读取中</div>
        <div v-else-if="error" class="command-state error" role="alert">{{ error }}<button type="button" @click="load">重试</button></div>
        <div v-else ref="list" class="command-list" role="listbox" aria-label="可选技能和工具" id="assistant-commands">
          <button v-for="(item, index) in items" :id="'assistant-command-' + index" :key="item.kind + item.id" type="button" role="option" :aria-selected="index === active"
            :class="{ active: index === active }" @pointermove="active = index" @click="choose(item)">
            <BookOpen v-if="item.kind === 'skill'" :size="17" /><Wrench v-else :size="17" />
            <span><strong>{{ item.label }}</strong><small>{{ item.description }}</small></span>
            <em>{{ item.kind === 'skill' ? item.source === 'shared' ? '共享' : '内置' : item.read_only ? '只读' : '需确认' }}</em>
          </button>
          <p v-if="!items.length" class="command-state">没有匹配项</p>
        </div>
        <footer v-if="!loading && !error"><span>{{ total }} 项</span><div><button type="button" class="command-icon" aria-label="上一页命令" :disabled="page <= 1" @click="page--; load()"><ChevronLeft :size="15" /></button><span>{{ page }} / {{ pages }}</span><button type="button" class="command-icon" aria-label="下一页命令" :disabled="page >= pages" @click="page++; load()"><ChevronRight :size="15" /></button></div></footer>
        <p v-if="selectionError" class="selection-error" role="alert">{{ selectionError }}</p>
      </section>
    </UiTransition>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue';
import { BookOpen, ChevronLeft, ChevronRight, Loader2, Settings2, Wrench, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import UiTransition from './UiTransition.vue';

const props = defineProps<{ draft: string; modelValue: Dict[] }>();
const emit = defineEmits<{ 'update:modelValue': [Dict[]]; 'update:draft': [string]; manage: []; focus: [] }>();
const kind = ref('skills'), group = ref(''), page = ref(1), total = ref(0), active = ref(0);
const items = ref<Dict[]>([]), groups = ref<string[]>([]), loading = ref(false), error = ref(''), selectionError = ref('');
const list = ref<HTMLElement | null>(null), dismissed = ref<string | null>(null);
const match = computed(() => /(?:^|\n)\/([^\n]*)$/.exec(props.draft));
const query = computed(() => match.value?.[1].trim() || '');
const visible = computed(() => !!match.value && dismissed.value !== props.draft);
const pages = computed(() => Math.max(1, Math.ceil(total.value / 20)));
let timer = 0, controller: AbortController | undefined;

watch(() => props.draft, () => {
  if (dismissed.value !== props.draft) dismissed.value = null;
  window.clearTimeout(timer); controller?.abort();
  if (!visible.value) { loading.value = false; return; }
  page.value = 1; loading.value = true;
  timer = window.setTimeout(() => { void load(); }, 160);
}, { immediate: true });
async function load(): Promise<void> {
  window.clearTimeout(timer); controller?.abort();
  if (!visible.value) return;
  const current = new AbortController(); controller = current;
  loading.value = true; error.value = ''; selectionError.value = '';
  try {
    const params = new URLSearchParams({ kind: kind.value, keyword: query.value, group: group.value, page: String(page.value) });
    const data = await requestJson('/api/assistant/commands?' + params, { signal: current.signal, timeoutMs: 20000 });
    if (controller !== current || current.signal.aborted) return;
    items.value = data.items || []; groups.value = data.groups || []; total.value = data.total || 0; page.value = data.page || 1; active.value = 0;
  } catch (cause) {
    if (controller === current && !current.signal.aborted) error.value = cause instanceof Error ? cause.message : '命令读取失败';
  } finally { if (controller === current) loading.value = false; }
}
function changeKind(value: string): void { kind.value = value; group.value = ''; page.value = 1; void load(); }
function dismiss(): void { dismissed.value = props.draft; controller?.abort(); loading.value = false; }
function choose(item: Dict): void {
  const duplicate = props.modelValue.some(old => old.kind === item.kind && old.id === item.id);
  if (!duplicate && props.modelValue.length >= 4) { selectionError.value = '每条消息最多选择4项'; return; }
  if (!duplicate) emit('update:modelValue', [...props.modelValue, { kind: item.kind, id: item.id, label: item.label }]);
  const found = match.value;
  if (found) emit('update:draft', props.draft.slice(0, found.index).trimEnd());
  emit('focus');
}
function remove(item: Dict): void { emit('update:modelValue', props.modelValue.filter(old => old.kind !== item.kind || old.id !== item.id)); }
function handleKey(event: KeyboardEvent): boolean {
  if (!visible.value || event.isComposing) return false;
  if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); dismiss(); return true; }
  if (['ArrowDown', 'ArrowUp'].includes(event.key)) {
    event.preventDefault(); active.value = Math.max(0, Math.min(items.value.length - 1, active.value + (event.key === 'ArrowDown' ? 1 : -1)));
    void nextTick(() => list.value?.querySelector('.active')?.scrollIntoView({ block: 'nearest' })); return true;
  }
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault(); if (!loading.value && !error.value && items.value[active.value]) choose(items.value[active.value]); return true;
  }
  return false;
}
function refresh(): void { if (visible.value) void load(); }
function reopen(): void { dismissed.value = null; void load(); }
defineExpose({ handleKey, visible, active, refresh, reopen });
onBeforeUnmount(() => { window.clearTimeout(timer); controller?.abort(); });
</script>

<style scoped>
.command-anchor { position: relative; flex-shrink: 0; }
.command-chips { display: flex; flex-wrap: wrap; gap: 5px; }
.command-chips > span { max-width: 100%; display: flex; align-items: center; gap: 5px; border: 1px solid var(--lh-border); border-radius: 5px; padding: 3px 5px; font-size: 12px; }
.command-chips b { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }
.command-chips button, .command-icon { border: 0; background: transparent; color: inherit; display: inline-flex; align-items: center; justify-content: center; cursor: pointer; padding: 4px; }
.command-menu { position: absolute; z-index: 8; bottom: 100%; left: 0; width: 100%; margin-bottom: 9px; background: var(--lh-surface); color: var(--lh-charcoal); border: 1px solid var(--lh-border); border-radius: 8px; box-shadow: 0 6px 25px #0002; overflow: hidden; }
header { display: flex; align-items: center; gap: 4px; padding: 7px 9px; border-bottom: 1px solid var(--lh-border); }
.command-tabs { display: flex; gap: 4px; flex: 1; }
.command-tabs button { display: flex; gap: 5px; align-items: center; font: inherit; font-size: 12px; border: 0; border-radius: 5px; padding: 6px 10px; background: transparent; color: var(--lh-muted); cursor: pointer; }
.command-tabs button[aria-selected=true] { color: var(--lh-accent); background: var(--lh-accent-soft); }
select { margin: 7px 10px 0; max-width: calc(100% - 20px); font: inherit; font-size: 12px; color: inherit; border: 1px solid var(--lh-border); background: var(--lh-surface); border-radius: 5px; padding: 4px 6px; }
.command-list { max-height: min(280px, 40vh); overflow: auto; overscroll-behavior: contain; padding: 4px; }
.command-list > button { display: flex; align-items: center; gap: 9px; width: 100%; min-height: 48px; text-align: left; background: transparent; color: inherit; border: 0; border-radius: 5px; padding: 7px 9px; cursor: pointer; font: inherit; }
.command-list > button.active { background: var(--lh-accent-soft); }
.command-list svg { flex-shrink: 0; color: var(--lh-muted); }
.command-list span { min-width: 0; flex: 1; }
.command-list strong { display: block; font-size: 13px; font-weight: 600; overflow-wrap: anywhere; }
.command-list small { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; font-size: 11px; line-height: 1.5; color: var(--lh-muted); margin-top: 2px; overflow-wrap: anywhere; }
.command-list em { font-size: 11px; color: var(--lh-muted); font-style: normal; white-space: nowrap; }
footer { display: flex; align-items: center; justify-content: space-between; padding: 5px 10px; border-top: 1px solid var(--lh-border); font-size: 11px; color: var(--lh-muted); }
footer > div { display: flex; align-items: center; gap: 5px; }
button:disabled { opacity: .4; cursor: default; }
.command-state { display: flex; align-items: center; justify-content: center; gap: 6px; min-height: 75px; margin: 0; padding: 10px; font-size: 12px; }
.error, .selection-error { color: var(--lh-danger); }
.selection-error { margin: 0; padding: 5px 10px; font-size: 12px; }
.spin { animation: command-spin 1s linear infinite; }
@keyframes command-spin { to { transform: rotate(360deg); } }
</style>
