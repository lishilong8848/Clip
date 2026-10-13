<template>
  <button v-if="enabled || login" type="button" class="frontend-version-switch" :class="{'is-login':login}" :disabled="versionBusy" :title="versionBusy?'正在提交，请等待结果':undefined" @pointerenter="prepareWorkspaceEntry" @focus="prepareWorkspaceEntry" @click="requestVersion(open)"><PanelsTopLeft :size="16" aria-hidden="true" /><span>切换新版</span></button>
  <NModal v-model:show="confirmVersion" preset="dialog" title="切换后需要重新填写" content="当前填写内容尚未提交，切换版本不会保留密码或身份信息。" positive-text="放弃填写并切换" negative-text="继续填写" @positive-click="finishVersion" />
</template>
<script setup lang="ts">
import { watch, onBeforeUnmount } from 'vue';
import { PanelsTopLeft } from 'lucide-vue-next';
import { NModal } from 'naive-ui';
import { useAuthVersionGuard } from '../workspace-v2/authVersionGuard';
import { readVersion, saveVersion, workspaceUrl, loginVersionUrl, WORKSPACE_ENTRY,prepareWorkspaceEntry } from '../workspace-v2/version';
const props = defineProps<{ identity: string; enabled: boolean; login?: boolean }>();
const {busy:versionBusy,confirming:confirmVersion,request:requestVersion,confirm:finishVersion}=useAuthVersionGuard();
let switching=false;
function cancelledVersionTransition(event:PromiseRejectionEvent){if(switching&&event.reason instanceof DOMException&&event.reason.name==='AbortError'&&event.reason.message==='Transition was skipped')event.preventDefault();}
window.addEventListener('unhandledrejection',cancelledVersionTransition);
function finishVersionTransition(event:Event){
  const transition=(event as Event & {viewTransition?:{ready:Promise<void>;finished:Promise<void>;updateCallbackDone?:Promise<void>}}).viewTransition;
  void transition?.ready.catch(()=>undefined);void transition?.finished.catch(()=>undefined);void transition?.updateCallbackDone?.catch(()=>undefined);
}
for(const event of ['pageswap','pagereveal'])window.addEventListener(event,finishVersionTransition);
onBeforeUnmount(()=>{for(const event of ['pageswap','pagereveal'])window.removeEventListener(event,finishVersionTransition);window.removeEventListener('unhandledrejection',cancelledVersionTransition);});
const incoming = new URL(window.location.href);
try { if (incoming.searchParams.get('auth_notice') === 'cancelled') {
  const next = new URL(incoming.searchParams.get('next') || '/', incoming.origin);
  if (next.origin === incoming.origin && next.pathname === WORKSPACE_ENTRY) {
    const returnTarget = next.pathname + next.search + next.hash;
    next.searchParams.set('login', 'feishu'); next.searchParams.set('auth_notice', 'cancelled');
    next.searchParams.set('next', returnTarget); next.searchParams.set('__page', '/');
    switching=true;window.location.replace(next.href);
  }
} } catch { /* Invalid return links leave the original login page available. */ }
function open() { if(!props.login)saveVersion(props.identity, 'workspace'); switching=true;window.location.assign(props.login ? loginVersionUrl('workspace') : workspaceUrl('/')); }
watch(() => [props.identity, props.enabled] as const, ([identity, enabled]) => {
  if (!identity || !enabled) return;
  const params = new URLSearchParams(window.location.search);
  if (params.get('frontend') === 'legacy') { saveVersion(identity, 'legacy'); return; }
  if (readVersion(identity) === 'workspace') {switching=true;window.location.replace(workspaceUrl('/'));}
}, { immediate: true });
</script>
<style scoped>
.frontend-version-switch { display: inline-flex; gap: 7px; align-items: center; justify-content: center; min-height: 36px; padding: 7px 11px; border: 1px solid #ffffff55; border-radius: 7px; background: #ffffff1a; color: #fff; font: inherit; font-size: 13px; cursor: pointer; }
.frontend-version-switch:hover { background: #ffffff2b; }.frontend-version-switch:focus-visible { outline: 2px solid white; outline-offset: 3px; }
.frontend-version-switch.is-login { min-height:44px; margin-left:auto; }
.frontend-version-switch:disabled { cursor:wait; opacity:.55; }
</style>
