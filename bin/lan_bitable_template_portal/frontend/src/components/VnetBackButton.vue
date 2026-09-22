<template>
  <Teleport v-if="active" defer to="#page-back-slot">
  <button type="button"
    class="vnet-back-button"
    :disabled="disabled"
    :title="title || '返回'"
    aria-label="返回"
    @click="handleClick"
  >
    <ArrowLeft :size="16" aria-hidden="true" />
    <slot>返回</slot>
  </button>
  </Teleport>
</template>

<script setup lang="ts">
import { navigateBack } from "../navigation";
import { onActivated, onDeactivated, ref } from "vue";
import { ArrowLeft } from "lucide-vue-next";

const active = ref(true);
onActivated(() => { active.value = true; });
onDeactivated(() => { active.value = false; });

const props = defineProps<{
  to?: string;
  hard?: boolean;
  disabled?: boolean;
  title?: string;
}>();

const emit = defineEmits<{
  click: [];
}>();

function handleClick(): void {
  if (props.disabled) return;
  if (props.to) {
    navigateBack(props.to, props.hard);
    return;
  }
  emit("click");
}
</script>
