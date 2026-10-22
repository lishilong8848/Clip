<template>
  <NConfigProvider :theme="dark?darkTheme:null"><NMessageProvider><NDialogProvider>
    <NoticeWorkspace v-if="mode==='workspace'" :url="'/workbench-lite?work_type='+kind" scope="A" :scopes="[{value:'A',label:'A楼'}]" :user="user" active />
    <form v-else-if="mode==='structured'" class="assistant-fixture" :class="{'is-dark':dark}" @submit.prevent="submitted.push(JSON.parse(JSON.stringify(values)))">
      <LighthouseStructuredField id="fixture-notice" :field="control" v-model="values" />
      <button type="submit">补充并继续</button>
    </form>
    <LighthouseNoticePanel v-else user-id="cycle-fixture" active />
  </NDialogProvider></NMessageProvider></NConfigProvider>
</template>
<script setup lang="ts">
import { computed, provide, ref } from 'vue';
import { NConfigProvider,NMessageProvider,NDialogProvider,darkTheme } from 'naive-ui';
import NoticeWorkspace from '../src/workspace-v2/NoticeWorkspace.vue';
import LighthouseStructuredField from '../src/components/LighthouseStructuredField.vue';
import LighthouseNoticePanel from '../src/components/LighthouseNoticePanel.vue';
import { pageContext } from '../src/workspace-v2/pageLayout';
import { embeddedContextKey } from '../src/workspace-v2/embeddedContext';
import { setModulePermissions } from '../src/api/modulePermissions';
import type { Dict } from '../src/api/client';
const fixture = (window as any).cycleFixture;
const query = new URLSearchParams(location.search), mode = query.get('mode') || 'workspace';
const kind = query.get('kind') || 'maintenance';
const dark = ref(query.get('dark')==='1');
const control = ref<Dict>(fixture.descriptors[kind]);
const values = ref<Dict>({...fixture.values, notice_type:kind==='maintenance'?'维保通告':'变更通告'});
const submitted: Dict[] = [];
const user = { open_id:'cycle-fixture',name:'隔离检查',modules:['maintenance','change','assistant'] };
setModulePermissions(user.modules);
provide(pageContext,{user:computed(()=>user),scopes:computed(()=>['A']),scope:computed(()=>'A'),module:computed(()=>'notices')});
provide(embeddedContextKey,{id:'cycle-check',route:ref(new URL(location.href)),filterTarget:ref(null),go:()=>false,back:()=>{}});
(window as any).cycleControls = {
  setValues:(patch:Dict)=>Object.assign(values.value,patch),
  setControl:(patch:Dict)=>Object.assign(control.value,patch),
  submitted,
};
</script>
<style>
body{margin:0;padding:12px;box-sizing:border-box;font-family:system-ui,sans-serif}
.assistant-fixture{max-width:620px;min-width:0;margin:auto}
.assistant-fixture *{box-sizing:border-box}
.assistant-fixture.is-dark{--lh-surface:#252c2a;--lh-surface-subtle:#262f2a;--lh-charcoal:#e5e7eb;--lh-danger:#eea99b;--lh-border:#505d5a;--lh-input-border:#66736e;background:var(--lh-surface);color:var(--lh-charcoal);padding:10px}
.notice-panel{max-width:100%;--notice-panel-width:540px;--notice-panel-height:820px}
</style>
