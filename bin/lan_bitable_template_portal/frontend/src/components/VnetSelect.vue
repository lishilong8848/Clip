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

    <Teleport to="body">
      <div
        v-if="open"
        ref="menuRef"
        class="vnet-select-menu"
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
            @mouseenter="activeIndex = index"
            @click="selectOption(option)"
          >
            <span>{{ option }}</span>
            <Check v-if="option === modelValue" :size="16" aria-hidden="true" />
          </button>
          <div v-if="!filteredOptions.length" class="vnet-select-empty">没有匹配选项</div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { Check, ChevronDown, Search } from "lucide-vue-next";

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
}>(), {
  modelValue: "",
  label: "",
  placeholder: "请选择",
  disabled: false,
  required: false,
  error: "",
  allowCustom: false,
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
const searchRef = ref<HTMLInputElement | null>(null);
const open = ref(false);
const query = ref("");
const activeIndex = ref(-1);
const menuPosition = ref({ top: 0, left: 0, width: 220 });
let positionFrame = 0;
let globalListenersAttached = false;

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
  top: `${menuPosition.value.top}px`,
  left: `${menuPosition.value.left}px`,
  width: `${menuPosition.value.width}px`,
}));

function optionId(index: number): string {
  return `vnet-select-option-${instanceId}-${index}`;
}

function updatePosition(): void {
  if (!open.value) return;
  const trigger = triggerRef.value;
  if (!trigger) return;
  const rect = trigger.getBoundingClientRect();
  const viewportPadding = 10;
  const width = Math.max(180, rect.width);
  const estimatedHeight = Math.min(330, 56 + filteredOptions.value.length * 38);
  const spaceBelow = window.innerHeight - rect.bottom - viewportPadding;
  const openAbove = spaceBelow < Math.min(220, estimatedHeight) && rect.top > spaceBelow;
  menuPosition.value = {
    top: openAbove ? Math.max(viewportPadding, rect.top - estimatedHeight - 6) : rect.bottom + 6,
    left: Math.min(Math.max(viewportPadding, rect.left), window.innerWidth - width - viewportPadding),
    width,
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
}

function detachGlobalListeners(): void {
  if (!globalListenersAttached) return;
  globalListenersAttached = false;
  document.removeEventListener("pointerdown", handleDocumentPointerDown, true);
  window.removeEventListener("resize", schedulePositionUpdate);
  window.removeEventListener("scroll", schedulePositionUpdate, true);
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
  query.value = initialQuery;
  open.value = true;
  attachGlobalListeners();
  activeIndex.value = initialActiveIndex();
  await nextTick();
  updatePosition();
  if (searchable.value) searchRef.value?.focus();
  else scrollActiveIntoView();
}

function closeMenu(focusTrigger = false): void {
  if (!open.value) return;
  open.value = false;
  detachGlobalListeners();
  query.value = "";
  activeIndex.value = -1;
  if (focusTrigger) nextTick(() => triggerRef.value?.focus());
}

function toggle(): void {
  if (open.value) closeMenu();
  else void showMenu();
}

function openCustomMenu(): void {
  if (!open.value) void showMenu();
}

async function handleCustomInput(event: Event): Promise<void> {
  const value = (event.target as HTMLInputElement).value;
  emit("update:modelValue", value);
  emit("change", value);
  query.value = value;
  if (!open.value && !props.disabled) {
    open.value = true;
    activeIndex.value = initialActiveIndex();
    await nextTick();
    updatePosition();
  }
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
  } else if (event.key === "Escape") {
    event.preventDefault();
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
  document.getElementById(optionId(activeIndex.value))?.scrollIntoView({ block: "nearest" });
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
  } else if (event.key === "Escape") {
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

.vnet-combobox-control {
  width: 100%;
  min-height: 36px;
  display: flex;
  align-items: stretch;
  overflow: hidden;
  border: 1px solid #cbd8e8;
  border-radius: 8px;
  background: #fff;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.vnet-combobox-control input {
  min-width: 0;
  flex: 1;
  border: 0;
  outline: 0;
  padding: 0 11px;
  background: transparent;
  color: #142b49;
  font: inherit;
  font-size: 14px;
  font-weight: 500;
}

.vnet-combobox-control input::placeholder {
  color: #8291a6;
}

.vnet-combobox-toggle {
  width: 36px;
  flex: 0 0 36px;
  display: grid;
  place-items: center;
  border: 0;
  border-left: 1px solid #dbe5f1;
  background: #f7faff;
  color: #47709e;
  cursor: pointer;
}

.vnet-select.open .vnet-combobox-control,
.vnet-combobox-control:focus-within {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.14);
}

.vnet-select.open .vnet-combobox-toggle svg {
  transform: rotate(180deg);
}

.vnet-select.invalid .vnet-combobox-control {
  border-color: #e1495b;
}

.vnet-select.disabled .vnet-combobox-control {
  background: #f3f6fa;
  color: #8a99ac;
}

.vnet-combobox-control input:disabled,
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
  border: 1px solid #cbd8e8;
  border-radius: 8px;
  padding: 0 10px 0 11px;
  background: #fff;
  color: #142b49;
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
  color: #8291a6;
}

.vnet-select-trigger svg {
  flex: 0 0 auto;
  color: #47709e;
  transition: transform 160ms ease;
}

.vnet-select.open .vnet-select-trigger,
.vnet-select-trigger:focus-visible {
  border-color: #1e63ff;
  outline: 0;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.14);
}

.vnet-select.open .vnet-select-trigger svg {
  transform: rotate(180deg);
}

.vnet-select.invalid .vnet-select-trigger {
  border-color: #e1495b;
}

.vnet-select-trigger:disabled {
  cursor: not-allowed;
  background: #f3f6fa;
  color: #8a99ac;
}

.vnet-select-menu {
  position: fixed;
  z-index: 2100;
  overflow: hidden;
  border: 1px solid #c9d8eb;
  border-radius: 10px;
  background: #fff;
  box-shadow: 0 16px 42px rgba(22, 58, 105, 0.2);
}

.vnet-select-search {
  min-height: 42px;
  display: flex;
  align-items: center;
  gap: 7px;
  margin: 7px;
  border: 1px solid #d5e1ef;
  border-radius: 8px;
  padding: 0 9px;
  color: #56708f;
}

.vnet-select-search:focus-within {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.vnet-select-search input {
  min-width: 0;
  flex: 1;
  border: 0;
  outline: 0;
  background: transparent;
  color: #142b49;
  font: inherit;
  font-size: 13px;
}

.vnet-select-options {
  max-height: 280px;
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
  color: #183353;
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
  background: #edf4ff;
}

.vnet-select-option.selected {
  background: #e4efff;
  color: #0b56bd;
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
  background: #fff7ed;
  color: #9a4c10;
}

.vnet-select-empty {
  color: #6c7f96;
  text-align: center;
}
</style>
