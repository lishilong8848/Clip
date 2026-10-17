<template>
  <section class="legacy-pre-shift" aria-labelledby="pre-shift-title">
    <header class="legacy-pre-shift-head">
      <VnetBackButton @click="navigate('/')" />
      <h2 id="pre-shift-title">班前检查清单</h2>
    </header>
    <NConfigProvider :theme-overrides="theme">
      <NMessageProvider><NDialogProvider>
        <div v-if="available.length" class="legacy-pre-shift-buildings">
          <PreShiftCheck v-for="scope in available" :key="user.open_id+':'+scope" :user="user" :fixed-scope="scope" :register-guard="registerCheckGuard" :aria-label="(scope==='110'?'110站':scope+'楼')+'班前检查'" />
        </div>
        <NAlert v-else type="info" :bordered="false">当前账号没有班前检查权限。</NAlert>
      </NDialogProvider></NMessageProvider>
    </NConfigProvider>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount } from 'vue';
import { NAlert, NConfigProvider, NDialogProvider, NMessageProvider } from 'naive-ui';
import type { Dict } from '../api/client';
import { navigate, registerNavigationGuard } from '../navigation';
import { preShiftScopes } from '../preShiftUtils';
import VnetBackButton from './VnetBackButton.vue';
import PreShiftCheck from '../workspace-v2/dashboard/PreShiftCheck.vue';

const props = defineProps<{ user: Dict }>();
const available = computed(() => preShiftScopes(props.user));
type CheckGuard = (target:string, proceed:()=>void)=>boolean;
const guards = new Set<CheckGuard>();
function registerCheckGuard(guard:CheckGuard) { guards.add(guard); return () => { guards.delete(guard); }; }
const unregister = registerNavigationGuard((target, proceed) => {
  const pending = [...guards];
  function check(index:number):boolean {
    for (;index<pending.length;index++) {
      const next=index+1;
      if (!pending[index](target, () => { if(check(next)) proceed(); })) return false;
    }
    return true;
  }
  return check(0);
});
onBeforeUnmount(unregister);
const theme = { common: { primaryColor: '#1e63ff', primaryColorHover: '#1b56db',
  primaryColorPressed: '#1849bd', borderRadius: '6px', fontFamily: 'inherit' } };
</script>

<style scoped>
.legacy-pre-shift{--v2-blue:#1e63ff;--v2-border:#d8e3f1;--v2-muted:#647487;max-width:1480px;margin:0 auto;padding:18px 22px 30px}
.legacy-pre-shift-head{display:flex;align-items:center;gap:16px;margin-bottom:18px}
.legacy-pre-shift-head h2{margin:0;font-size:22px;color:#173354}
.legacy-pre-shift-buildings{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:20px;align-items:start}
.legacy-pre-shift-buildings>.v2-shift-check{padding:16px;border:1px solid var(--v2-border);border-radius:6px;min-width:0}
@media(max-width:640px){.legacy-pre-shift{padding:12px}.legacy-pre-shift-head{gap:10px;margin-bottom:12px}.legacy-pre-shift-head h2{font-size:19px}}
</style>
