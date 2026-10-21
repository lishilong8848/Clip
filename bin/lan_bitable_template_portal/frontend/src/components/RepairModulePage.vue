<template>
  <component :is="page" v-bind="pageProps" />
</template>

<script setup lang="ts">
import type { Component } from 'vue';
import type { LooseDict } from '../types';

// KeepAlive owns this synchronous host, so a late async load cannot revive its unmounted cache.
defineProps<{ page: Component; pageProps: LooseDict }>();
</script>
