<template>
  <fieldset class="planned-choices" :disabled="disabled" :aria-label="field.label">
    <label v-for="option in (field.options || []).slice(0, limit)" :key="option.value" :class="{ selected: modelValue === option.value }">
      <input type="radio" :name="id" :value="option.value" :checked="modelValue === option.value" @change="$emit('update:modelValue', option.value)" />
      <span><strong>{{ option.label }}</strong><small v-if="option.detail">{{ option.detail }}</small></span>
    </label>
    <button v-if="(field.options || []).length > limit" type="button" @click="limit += 5"><ChevronDown :size="15" />查看更多（剩余 {{ field.options.length - limit }} 条）</button>
  </fieldset>
</template>
<script setup lang="ts">
import { ref, watch } from 'vue';
import { ChevronDown } from 'lucide-vue-next';
const props = defineProps<{ field: Record<string, any>; modelValue: string; id: string; disabled: boolean }>();
defineEmits<{ 'update:modelValue': [value: string] }>();
const limit = ref(5);
watch(() => props.field.options, () => { limit.value = 5; });
</script>
<style scoped>
.planned-choices { display: grid; gap: 6px; border: 0; padding: 0; margin: 0; min-width: 0; }
.planned-choices label { display: flex; align-items: flex-start; gap: 9px; padding: 10px; border: 1px solid var(--lh-border, #dbe3ea); border-radius: 6px; cursor: pointer; transition: background .16s, border-color .16s; }
.planned-choices label.selected { background: var(--lh-accent-soft, #eff6ff); border-color: var(--lh-accent, #4582be); }
.planned-choices input { margin: 3px 0 0; flex: 0 0 auto; }
.planned-choices span { min-width: 0; overflow-wrap: anywhere; }
.planned-choices strong { font-size: 13px; font-weight: 600; }
.planned-choices small { display: block; margin-top: 4px; color: var(--lh-muted, #617284); }
.planned-choices button { display: flex; align-items: center; justify-content: center; gap: 5px; min-height: 32px; }
</style>
