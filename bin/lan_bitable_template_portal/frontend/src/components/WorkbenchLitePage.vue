<template>
  <section class="workbench-frame-page" :aria-busy="loading">
    <iframe ref="frame" :src="frameUrl" title="通告管理" @load="onLoaded" />
    <div v-if="loading || error" class="frame-state" role="status">
      <AsyncPageState :error="error ? new Error(error) : undefined" :retry="reload" />
    </div>
    <p v-if="navigationError" class="navigation-error" role="alert">{{ navigationError }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import AsyncPageState from './AsyncPageState.vue';
import { navigate, registerNavigationGuard } from '../navigation';

const props = withDefaults(defineProps<{ search: string; active?: boolean }>(), { active: true });
const frame = ref<HTMLIFrameElement | null>(null), loading = ref(true), error = ref('');
const navigationError = ref('');
const nativeSearch = ref(props.search), retry = ref(0);
let loadTimer = 0;
watch(loading, busy => {
  window.clearTimeout(loadTimer);
  if (busy) loadTimer = window.setTimeout(() => { loading.value = false; error.value = '通告页面读取超时，请重试。'; }, 15000);
}, { immediate: true });
const frameUrl = computed(() => {
  const params = new URLSearchParams(nativeSearch.value);
  params.set('_assistant_frame', '1');
  if (retry.value) params.set('_frame_retry', String(retry.value));
  return '/workbench-lite?' + params.toString();
});
function clean(url: string): URL {
  const result = new URL(url, window.location.origin);
  result.searchParams.delete('_assistant_frame'); result.searchParams.delete('_frame_retry');
  return result;
}
async function syncSearch(search: string): Promise<void> {
  if (!props.active) return;
  try {
    if (frame.value?.contentWindow && clean(frame.value.contentWindow.location.href).searchParams.toString() === clean('/workbench-lite?' + search).searchParams.toString()) return;
  } catch { /* An expired authentication redirect may no longer be same-origin. */ }
  error.value = '';
  try {
    const child = frame.value?.contentWindow as (Window & { navigateLite?: (url: string, options: object) => Promise<void> }) | null;
    if (child?.navigateLite) { await child.navigateLite('/workbench-lite?' + search, { push: false, workspaceSwitch: true, reuseWorkspace: true }); navigationError.value = ''; return; }
  } catch (exc) { navigationError.value = exc instanceof Error ? exc.message : '通告页面读取失败，原填写已保留。'; return; }
  loading.value = true;
  if (frame.value?.contentWindow) {
    const params = new URLSearchParams(search); params.set('_assistant_frame', '1');
    frame.value.contentWindow.location.replace('/workbench-lite?' + params.toString());
  } else nativeSearch.value = search;
}
function reload(): void { loading.value = true; error.value = ''; nativeSearch.value = props.search; retry.value++; }
function onLoaded(): void {
  loading.value = false;
  error.value = '';
  const child = frame.value?.contentWindow;
  if (!child) return;
  try {
    const path = new URL(child.location.href);
    if (path.origin !== location.origin || path.pathname.startsWith('/api/auth/')) {
      error.value = '登录状态已失效，请重新登录。'; return;
    }
    if (path.pathname !== '/workbench-lite' && path.pathname !== '/workbench-lite/') {
      if (props.active) navigate(clean(path.href)); return;
    }
    if (!child.document.querySelector('.workspace')) error.value = '通告页面暂未加载成功，请重试。';
    else if (props.active) navigate(clean(path.href), { replace: true, bypassGuard: true });
    setVisibility();
  } catch { error.value = '登录状态已失效，请重新登录。'; }
}
function onMessage(event: MessageEvent): void {
  if (event.origin !== location.origin || event.source !== frame.value?.contentWindow || !event.data) return;
  if (event.data.type === 'clipflow:business-changed') {
    window.dispatchEvent(new Event('clipflow-business-changed')); return;
  }
  if (!props.active) return;
  if (event.data.type === 'clipflow:workbench-pointer') {
    const { x, y } = event.data, rect = frame.value?.getBoundingClientRect();
    if (rect && Number.isFinite(x) && Number.isFinite(y) && x >= 0 && y >= 0 && x <= rect.width && y <= rect.height) {
      window.dispatchEvent(new CustomEvent('clipflow:workbench-pointer', { detail: { x: rect.left + x, y: rect.top + y } }));
    }
    return;
  }
  if (typeof event.data.url !== 'string') return;
  if (!['clipflow:workbench-location', 'clipflow:workbench-navigate'].includes(event.data.type)) return;
  let url: URL;
  try { url = clean(event.data.url); } catch { return; }
  if (url.origin !== location.origin || url.username || url.password || url.pathname.startsWith('/api/') || url.pathname.startsWith('/polling-work-order')) return;
  if (event.data.type === 'clipflow:workbench-location') {
    if (url.pathname.replace(/\/$/, '') !== '/workbench-lite') return;
    navigate(url, { replace: event.data.replace === true, bypassGuard: true });
  } else navigate(url);
}
let unregister: (() => void) | undefined;
function setVisibility(): void {
  const child = frame.value?.contentWindow;
  try { child?.dispatchEvent(new CustomEvent('clipflow:workbench-visibility', { detail: { active: props.active } })); } catch { /* Authentication may have redirected the frame. */ }
}
function setGuard(): void {
  unregister?.(); unregister = undefined;
  setVisibility();
  if (!props.active) return;
  unregister = registerNavigationGuard((_target, proceed) => {
    let child: (Window & { prepareLiteNavigation?: () => Promise<boolean>; isLiteNavigationDirty?: () => boolean }) | null;
    try {
      child = frame.value?.contentWindow as typeof child;
      if (!child || typeof child.prepareLiteNavigation !== 'function') return true;
      if (child.isLiteNavigationDirty && !child.isLiteNavigationDirty()) return true;
    } catch { return true; }
    void child.prepareLiteNavigation().then(allowed => { if (allowed) proceed(); }).catch(() => { navigationError.value = '通告草稿未保存，请在通告中核对后再离开。'; });
    return false;
  });
}
watch(() => [props.search, props.active] as const, ([search, active]) => { setGuard(); if (active) void syncSearch(search); });
onMounted(() => { window.addEventListener('message', onMessage); setGuard(); });
onBeforeUnmount(() => { window.clearTimeout(loadTimer); unregister?.(); window.removeEventListener('message', onMessage); });
</script>

<style scoped>
.workbench-frame-page { position: fixed; inset: 0; background: #eef3f8; }
iframe { display: block; width: 100%; height: 100%; border: 0; }
.frame-state { position: absolute; inset: 0; padding-top: 112px; background: #eef3f8; }
.navigation-error { position: absolute; bottom: 16px; left: 20px; max-width: 460px; background: #fff4e8; border: 1px solid #ebc99c; color: #805218; padding: 10px 14px; font-size: 13px; pointer-events: none; }
</style>
