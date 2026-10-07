<template>
  <Transition :name="name" :appear="appear" :css="css" @before-leave="disable" @before-enter="restore" @leave-cancelled="restore">
    <slot />
  </Transition>
</template>

<script setup lang="ts">
withDefaults(defineProps<{ name?: string; appear?: boolean; css?: boolean }>(), {
  name: 'ui-overlay', appear: true, css: true,
});

// Closing panels remain visible briefly, but cannot accept another action.
const previousInert = new WeakMap<Element, boolean>();
function disable(element: Element): void {
  previousInert.set(element, element.hasAttribute('inert'));
  element.setAttribute('inert', '');
}
function restore(element: Element): void {
  if (previousInert.get(element) === false) element.removeAttribute('inert');
  previousInert.delete(element);
}
</script>
