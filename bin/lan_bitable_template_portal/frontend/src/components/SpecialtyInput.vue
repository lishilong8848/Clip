<template>
  <input
    :list="listId"
    :value="modelValue"
    :placeholder="placeholder"
    autocomplete="off"
    @input="emit('update:modelValue', ($event.target as HTMLInputElement).value)"
  />
  <datalist :id="listId">
    <option v-for="item in specialtyOptions" :key="item" :value="item" />
  </datalist>
</template>

<script setup lang="ts">
const specialtyOptions = ["电气", "暖通", "消防", "弱电"];

withDefaults(
  defineProps<{
    modelValue: string;
    listId: string;
    placeholder?: string;
  }>(),
  {
    placeholder: "选择或输入专业",
  },
);

const emit = defineEmits<{
  "update:modelValue": [value: string];
}>();
</script>
