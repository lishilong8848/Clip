<template>
  <div ref="rootRef" class="vnet-select" :class="{ open, invalid: Boolean(error), disabled }">
    <div v-if="allowCustom" class="vnet-combobox-control">
      <input
        :id="inputId"
        ref="triggerRef"
        type="text"
        role="combobox"
        autocomplete="off"
        :value="modelValue"
        :placeholder="placeholder"
        :aria-label="label || placeholder"
        aria-haspopup="listbox"
        :aria-expanded="open"
        :aria-controls="listboxId"
        :aria-activedescendant="activeDescendant"
        :aria-invalid="Boolean(error)"
        :aria-required="required"
        :disabled="disabled"
        @focus="openCustomMenu"
        @input="handleCustomInput"
        @keydown="handleCustomKeydown"
      />
      <button
        v-if="clearable && modelValue"
        type="button"
        class="vnet-combobox-clear"
        :disabled="disabled"
        :aria-label="`清空${label || '内容'}`"
        :title="`清空${label || '内容'}`"
        @pointerdown.prevent
        @click.stop.prevent="clearValue"
      >
        <X :size="16" aria-hidden="true" />
      </button>
      <button
        type="button"
        class="vnet-combobox-toggle"
        :disabled="disabled"
        :aria-label="`选择${label || '选项'}`"
        :aria-expanded="open"
        @click.stop="toggleCustomMenu"
      >
        <ChevronDown :size="16" aria-hidden="true" />
      </button>
    </div>
    <button
      v-else
      :id="inputId"
      ref="triggerRef"
      type="button"
      class="vnet-select-trigger"
      role="combobox"
      aria-haspopup="listbox"
      :aria-expanded="open"
      :aria-controls="listboxId"
      :aria-activedescendant="activeDescendant"
      :aria-invalid="Boolean(error)"
      :aria-required="required"
      :disabled="disabled"
      @click="toggle"
      @keydown="handleTriggerKeydown"
    >
      <span :class="{ placeholder: !modelValue }">{{ modelValue || placeholder }}</span>
      <ChevronDown :size="16" aria-hidden="true" />
    </button>

    <Teleport :to="menuHost || 'body'">
      <UiTransition name="ui-popover" appear>
      <div
        v-if="open"
        ref="menuRef"
        class="vnet-select-menu"
        :popover="menuHost ? 'manual' : undefined"
        :data-assistant-control="assistantControl ? 'true' : undefined"
        :style="menuStyle"
        @keydown="handleMenuKeydown"
      >
        <label v-if="searchable" class="vnet-select-search">
          <Search :size="15" aria-hidden="true" />
          <input
            ref="searchRef"
            v-model.trim="query"
            type="search"
            :placeholder="`搜索${label || '选项'}`"
            autocomplete="off"
          />
        </label>
        <div
          :id="listboxId"
          class="vnet-select-options"
          role="listbox"
          :aria-label="label || placeholder"
        >
          <div v-if="hasLegacyValue" class="vnet-select-legacy">
            {{ modelValue }}（不在当前选项中）
          </div>
          <button
            v-for="(option, index) in filteredOptions"
            :id="optionId(index)"
            :key="option"
            type="button"
            role="option"
            class="vnet-select-option"
            :class="{ active: index === activeIndex, selected: option === modelValue }"
            :aria-selected="option === modelValue"
            @pointermove="activeIndex = index"
            @click="selectOption(option)"
          >
            <span>{{ option }}</span>
            <Check v-if="option === modelValue" :size="16" aria-hidden="true" />
          </button>
          <div v-if="!filteredOptions.length" class="vnet-select-empty">没有匹配选项</div>
        </div>
      </div>
      </UiTransition>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { Check, ChevronDown, Search, X } from "lucide-vue-next";
import { inheritedControlTheme } from "../controlTheme";

const props = withDefaults(defineProps<{
  modelValue?: string;
  options: string[];
  inputId: string;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  error?: string;
  allowCustom?: boolean;
  clearable?: boolean;
  menuZIndex?: number;
}>(), {
  modelValue: "",
  label: "",
  placeholder: "请选择",
  disabled: false,
  required: false,
  error: "",
  allowCustom: false,
  clearable: false,
});

const emit = defineEmits<{
  "update:modelValue": [value: string];
  change: [value: string];
}>();

const instanceId = Math.random().toString(36).slice(2, 9);
const listboxId = `vnet-select-list-${instanceId}`;
const rootRef = ref<HTMLElement | null>(null);
const triggerRef = ref<HTMLButtonElement | HTMLInputElement | null>(null);
const menuRef = ref<HTMLElement | null>(null);
const menuHost = ref<HTMLElement | null>(null), assistantControl = ref(false);
const searchRef = ref<HTMLInputElement | null>(null);
const open = ref(false);
const query = ref("");
const activeIndex = ref(-1);
const menuPosition = ref({ top: 0, left: 0, width: 220, maxHeight: 340 });
const menuTheme = ref<Record<string, string>>({});
let positionFrame = 0;
let globalListenersAttached = false;
let restoringTriggerFocus = false;

const normalizedOptions = computed(() => Array.from(new Set(
  props.options.map((item) => String(item || "").trim()).filter(Boolean),
)));
const searchable = computed(() => !props.allowCustom && normalizedOptions.value.length > 8);
const filteredOptions = computed(() => {
  const keyword = query.value.toLocaleLowerCase("zh-CN");
  if (!keyword) return normalizedOptions.value;
  return normalizedOptions.value.filter((option) => option.toLocaleLowerCase("zh-CN").includes(keyword));
});
const hasLegacyValue = computed(() => Boolean(
  !props.allowCustom && props.modelValue && !normalizedOptions.value.includes(props.modelValue),
));
const activeDescendant = computed(() => (
  open.value && activeIndex.value >= 0 ? optionId(activeIndex.value) : undefined
));
const menuStyle = computed(() => ({
  ...menuTheme.value,
  top: `${menuPosition.value.top}px`,
  left: `${menuPosition.value.left}px`,
  width: `${menuPosition.value.width}px`,
  maxHeight: `${menuPosition.value.maxHeight}px`,
  ...(props.menuZIndex == null ? {} : { zIndex: props.menuZIndex }),
}));

function optionId(index: number): string {
  return `vnet-select-option-${instanceId}-${index}`;
}

function updatePosition(): void {
  if (!open.value) return;
  const trigger = triggerRef.value;
  const menu = menuRef.value;
  const options = menu?.querySelector<HTMLElement>('.vnet-select-options');
  if (!trigger || !menu || !options) return;
  const rect = (trigger.closest('.vnet-combobox-control') || trigger).getBoundingClientRect();
  const viewport = window.visualViewport;
  const viewportPadding = 10;
  const top = (viewport?.offsetTop || 0) + viewportPadding;
  const left = (viewport?.offsetLeft || 0) + viewportPadding;
  const bottom = top + (viewport?.height || window.innerHeight) - viewportPadding * 2;
  const right = left + (viewport?.width || window.innerWidth) - viewportPadding * 2;
  const width = Math.min(Math.max(180, rect.width), right - left);
  // Measure untransformed content and fit it outside the input, including the software keyboard.
  const naturalHeight = menu.offsetHeight - options.offsetHeight + Math.min(280, options.scrollHeight);
  const belowTop = Math.max(top, rect.bottom + 6);
  const aboveBottom = Math.min(bottom, rect.top - 6);
  const spaceBelow = Math.max(0, bottom - belowTop);
  const spaceAbove = Math.max(0, aboveBottom - top);
  const openAbove = spaceBelow < Math.min(220, naturalHeight) && spaceAbove > spaceBelow;
  const maxHeight = Math.min(naturalHeight, openAbove ? spaceAbove : spaceBelow);
  menuPosition.value = {
    top: openAbove ? aboveBottom - maxHeight : belowTop,
    left: Math.min(Math.max(left, rect.left), right - width),
    width,
    maxHeight,
  };
}

function schedulePositionUpdate(): void {
  if (!open.value || positionFrame) return;
  positionFrame = window.requestAnimationFrame(() => {
    positionFrame = 0;
    updatePosition();
  });
}

function attachGlobalListeners(): void {
  if (globalListenersAttached) return;
  globalListenersAttached = true;
  document.addEventListener("pointerdown", handleDocumentPointerDown, true);
  window.addEventListener("resize", schedulePositionUpdate);
  window.addEventListener("scroll", schedulePositionUpdate, true);
  window.visualViewport?.addEventListener("resize", schedulePositionUpdate);
  window.visualViewport?.addEventListener("scroll", schedulePositionUpdate);
}

function detachGlobalListeners(): void {
  if (!globalListenersAttached) return;
  globalListenersAttached = false;
  document.removeEventListener("pointerdown", handleDocumentPointerDown, true);
  window.removeEventListener("resize", schedulePositionUpdate);
  window.removeEventListener("scroll", schedulePositionUpdate, true);
  window.visualViewport?.removeEventListener("resize", schedulePositionUpdate);
  window.visualViewport?.removeEventListener("scroll", schedulePositionUpdate);
  if (positionFrame) {
    window.cancelAnimationFrame(positionFrame);
    positionFrame = 0;
  }
}

function initialActiveIndex(): number {
  const selectedIndex = filteredOptions.value.indexOf(props.modelValue);
  return selectedIndex >= 0 ? selectedIndex : (filteredOptions.value.length ? 0 : -1);
}

async function showMenu(initialQuery = ""): Promise<void> {
  if (props.disabled || open.value) return;
  menuTheme.value = inheritedControlTheme(rootRef.value);
  menuHost.value = rootRef.value?.closest<HTMLElement>('.lighthouse-layer, dialog:modal') || null;
  assistantControl.value = !!rootRef.value?.closest('.lighthouse');
  query.value = initialQuery;
  open.value = true;
  attachGlobalListeners();
  activeIndex.value = initialActiveIndex();
  await nextTick();
  if (menuHost.value) menuRef.value?.showPopover?.();
  updatePosition();
  await nextTick();
  if (!open.value) return;
  if (searchable.value) searchRef.value?.focus();
  else scrollActiveIntoView();
}

function closeMenu(focusTrigger = false): void {
  if (!open.value) return;
  open.value = false;
  detachGlobalListeners();
  query.value = "";
  activeIndex.value = -1;
  if (focusTrigger) nextTick(() => {
    restoringTriggerFocus = true;
    try { triggerRef.value?.focus(); }
    finally { restoringTriggerFocus = false; }
  });
}

function toggle(): void {
  if (open.value) closeMenu();
  else void showMenu();
}

function openCustomMenu(): void {
  if (!open.value && !restoringTriggerFocus) void showMenu();
}

async function handleCustomInput(event: Event): Promise<void> {
  const value = (event.target as HTMLInputElement).value;
  emit("update:modelValue", value);
  emit("change", value);
  query.value = value;
  if (!open.value) await showMenu(value);
}

function clearValue(): void {
  if (props.disabled) return;
  emit("update:modelValue", "");
  emit("change", "");
  closeMenu();
  restoringTriggerFocus = true;
  try { triggerRef.value?.focus({ preventScroll: true }); }
  finally { restoringTriggerFocus = false; }
}

function toggleCustomMenu(): void {
  if (open.value) closeMenu(true);
  else void showMenu();
}

function handleCustomKeydown(event: KeyboardEvent): void {
  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    event.preventDefault();
    if (!open.value) {
      void showMenu();
      return;
    }
    moveActive(event.key === "ArrowDown" ? 1 : -1);
  } else if (event.key === "Enter" && open.value) {
    event.preventDefault();
    selectActive();
  } else if (event.key === "Escape" && open.value) {
    event.preventDefault();
    event.stopPropagation();
    closeMenu(true);
  }
}

function selectOption(option: string): void {
  emit("update:modelValue", option);
  emit("change", option);
  closeMenu(true);
}

function moveActive(delta: number): void {
  if (!filteredOptions.value.length) return;
  const current = activeIndex.value < 0 ? 0 : activeIndex.value;
  activeIndex.value = (current + delta + filteredOptions.value.length) % filteredOptions.value.length;
  nextTick(scrollActiveIntoView);
}

function scrollActiveIntoView(): void {
  if (activeIndex.value < 0) return;
  const option = document.getElementById(optionId(activeIndex.value));
  const list = option?.parentElement;
  if (!option || !list) return;
  const top = option.offsetTop - list.offsetTop;
  if (top < list.scrollTop) list.scrollTop = top;
  else if (top + option.offsetHeight > list.scrollTop + list.clientHeight) {
    list.scrollTop = top + option.offsetHeight - list.clientHeight;
  }
}

function selectActive(): void {
  const option = filteredOptions.value[activeIndex.value];
  if (option) selectOption(option);
}

function handleTriggerKeydown(event: KeyboardEvent): void {
  if (["ArrowDown", "ArrowUp", "Enter", " "].includes(event.key)) {
    event.preventDefault();
    if (!open.value) {
      void showMenu();
      return;
    }
    if (event.key === "ArrowDown") moveActive(1);
    else if (event.key === "ArrowUp") moveActive(-1);
    else selectActive();
  } else if (event.key === "Escape" && open.value) {
    event.preventDefault();
    event.stopPropagation();
    closeMenu(true);
  }
}

function handleMenuKeydown(event: KeyboardEvent): void {
  if (event.key === "ArrowDown") {
    event.preventDefault();
    moveActive(1);
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    moveActive(-1);
  } else if (event.key === "Enter") {
    event.preventDefault();
    selectActive();
  } else if (event.key === "Escape") {
    event.preventDefault();
    event.stopPropagation();
    closeMenu(true);
  }
}

function handleDocumentPointerDown(event: PointerEvent): void {
  if (!open.value) return;
  const target = event.target as Node | null;
  if (target && (rootRef.value?.contains(target) || menuRef.value?.contains(target))) return;
  closeMenu();
}

watch(filteredOptions, () => {
  activeIndex.value = initialActiveIndex();
  if (open.value) nextTick(schedulePositionUpdate);
});
watch(() => props.disabled, (disabled) => {
  if (disabled) closeMenu();
});

onBeforeUnmount(() => {
  detachGlobalListeners();
});
</script>

<style scoped>
.vnet-select {
  min-width: 0;
}
.vnet-select-menu[popover] { margin: 0; pointer-events: auto; }

.vnet-combobox-control {
  width: 100%;
  min-height: 36px;
  display: flex;
  align-items: stretch;
  overflow: hidden;
  border: 1px solid var(--lh-input-border, #cbd8e8);
  border-radius: 8px;
  background: var(--lh-surface-subtle, #fff);
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.vnet-combobox-control input {
  min-width: 0;
  flex: 1;
  border: 0;
  outline: 0;
  padding: 0 11px;
  background: transparent;
  color: var(--lh-charcoal, #142b49);
  font: inherit;
  font-size: 14px;
  font-weight: 500;
}

.vnet-combobox-control input::placeholder {
  color: var(--lh-faint-muted, #8291a6);
}

.vnet-combobox-toggle {
  width: 36px;
  flex: 0 0 36px;
  display: grid;
  place-items: center;
  border: 0;
  border-left: 1px solid var(--lh-border, #dbe5f1);
  background: var(--lh-surface-hover, #f7faff);
  color: var(--lh-accent, #47709e);
  cursor: pointer;
}

.vnet-combobox-clear {
  flex: 0 0 36px;
  display: grid;
  place-items: center;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--lh-muted, #56708f);
  cursor: pointer;
}
.vnet-combobox-clear:hover:not(:disabled),
.vnet-combobox-clear:focus-visible {
  background: var(--lh-surface-hover, #edf4ff);
  color: var(--lh-accent-strong, #0b56bd);
}

.vnet-select.open .vnet-combobox-control,
.vnet-combobox-control:focus-within {
  border-color: var(--lh-accent, #1e63ff);
  box-shadow: 0 0 0 3px var(--lh-accent-ring, rgba(30, 99, 255, 0.14));
}

.vnet-select.open .vnet-combobox-toggle svg {
  transform: rotate(180deg);
}

.vnet-select.invalid .vnet-combobox-control {
  border-color: var(--lh-danger, #e1495b);
}

.vnet-select.disabled .vnet-combobox-control {
  background: var(--lh-surface-subtle, #f3f6fa);
  color: var(--lh-faint-muted, #8a99ac);
}

.vnet-combobox-control input:disabled,
.vnet-combobox-clear:disabled,
.vnet-combobox-toggle:disabled {
  cursor: not-allowed;
}

.vnet-select-trigger {
  width: 100%;
  min-height: 36px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border: 1px solid var(--lh-input-border, #cbd8e8);
  border-radius: 8px;
  padding: 0 10px 0 11px;
  background: var(--lh-surface-subtle, #fff);
  color: var(--lh-charcoal, #142b49);
  font: inherit;
  font-size: 14px;
  font-weight: 500;
  text-align: left;
  cursor: pointer;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.vnet-select-trigger > span {
  min-width: 0;
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.vnet-select-trigger .placeholder {
  color: var(--lh-faint-muted, #8291a6);
}

.vnet-select-trigger svg {
  flex: 0 0 auto;
  color: var(--lh-accent, #47709e);
  transition: transform 160ms ease;
}

.vnet-select.open .vnet-select-trigger,
.vnet-select-trigger:focus-visible {
  border-color: var(--lh-accent, #1e63ff);
  outline: 0;
  box-shadow: 0 0 0 3px var(--lh-accent-ring, rgba(30, 99, 255, 0.14));
}

.vnet-select.open .vnet-select-trigger svg {
  transform: rotate(180deg);
}

.vnet-select.invalid .vnet-select-trigger {
  border-color: var(--lh-danger, #e1495b);
}

.vnet-select-trigger:disabled {
  cursor: not-allowed;
  background: var(--lh-surface-subtle, #f3f6fa);
  color: var(--lh-faint-muted, #8a99ac);
}

.vnet-select-menu {
  position: fixed;
  display: flex;
  flex-direction: column;
  z-index: 2100;
  overflow: hidden;
  border: 1px solid var(--lh-border-strong, #c9d8eb);
  border-radius: 10px;
  background: var(--lh-surface, #fff);
  color: var(--lh-charcoal, #142b49);
  box-shadow: 0 16px 42px rgba(22, 58, 105, 0.2);
}

.vnet-select-search {
  flex-shrink: 0;
  min-height: 42px;
  display: flex;
  align-items: center;
  gap: 7px;
  margin: 7px;
  border: 1px solid var(--lh-input-border, #d5e1ef);
  border-radius: 8px;
  padding: 0 9px;
  color: var(--lh-muted, #56708f);
}

.vnet-select-search:focus-within {
  border-color: var(--lh-accent, #1e63ff);
  box-shadow: 0 0 0 3px var(--lh-accent-ring, rgba(30, 99, 255, 0.12));
}

.vnet-select-search input {
  min-width: 0;
  flex: 1;
  border: 0;
  outline: 0;
  background: transparent;
  color: var(--lh-charcoal, #142b49);
  font: inherit;
  font-size: 13px;
}

.vnet-select-options {
  min-height: 0;
  max-height: 280px;
  overflow-x: hidden;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 5px;
}

.vnet-select-option {
  width: 100%;
  min-height: 36px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border: 0;
  border-radius: 7px;
  padding: 7px 9px;
  background: transparent;
  color: var(--lh-charcoal, #183353);
  font: inherit;
  font-size: 13px;
  text-align: left;
  cursor: pointer;
}

.vnet-select-option span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.vnet-select-option.active {
  background: var(--lh-surface-hover, #edf4ff);
}

.vnet-select-option.selected {
  background: var(--lh-accent-soft, #e4efff);
  color: var(--lh-accent-strong, #0b56bd);
  font-weight: 700;
}

.vnet-select-option svg {
  flex: 0 0 auto;
}

.vnet-select-legacy,
.vnet-select-empty {
  margin: 3px;
  border-radius: 7px;
  padding: 8px 9px;
  font-size: 12px;
}

.vnet-select-legacy {
  background: var(--lh-warn-soft, #fff7ed);
  color: var(--lh-warn, #9a4c10);
}

.vnet-select-empty {
  color: var(--lh-muted, #6c7f96);
  text-align: center;
}
</style>
