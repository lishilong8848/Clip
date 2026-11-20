<template>
  <section class="serenity-desk identity-desk v2-my" aria-label="个人信息">
    <header class="identity-cover">
      <div class="identity-monogram" aria-hidden="true">{{ String(user.name||'').slice(-2) }}</div>
      <div><p>个人信息</p><h1>{{ user.name }}</h1><small>{{ level }} · {{ user.category_label || category }}</small></div>
      <div class="identity-tools"><slot name="tools"/></div>
    </header>
    <div class="identity-body">
      <section class="serenity-panel identity-access">
        <h2>我的工作范围</h2><p>可访问楼栋</p><div class="identity-chips"><span v-for="scope in scopes" :key="scope.value">{{ scope.label }}</span><small v-if="!scopes.length">暂无</small></div>
        <p>登录方式</p><strong>{{ user.login_method==='password'?'姓名密码登录':'飞书登录' }}</strong>
        <div class="identity-actions"><NButton type="primary" @click="emit('open','/?view=permission')"><ShieldCheck :size="16"/>申请权限</NButton>
          <NButton v-if="user.login_method==='password'" @click="emit('open','/?account=password')">修改密码</NButton>
          <NButton v-if="mobile && settings" @click="emit('settings')">{{ user.role==='admin'?'系统设置':'账号与权限' }}</NButton></div>
      </section>
      <section class="serenity-panel identity-modules"><h2>功能权限</h2>
        <div v-for="group in permissions" :key="group.key" class="identity-module-row"><strong>{{ group.label }}</strong>
          <span>{{ group.labels.join('、') }}</span></div><p v-if="!permissions.length">暂无业务功能授权</p>
      </section>
    </div>
    <slot/>
  </section>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {NButton} from 'naive-ui';
import {ShieldCheck} from 'lucide-vue-next';
import type {Dict} from './client';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];level:string;category:string;groups:SidebarGroup[];settings:boolean;mobile:boolean}>();
const emit=defineEmits<{open:[url:string];settings:[]}>();
const permissions=computed(()=>props.groups.filter(group=>group.key!=='home').map(group=>({
  key:group.key,label:group.label,labels:group.direct?[group.direct.label]:group.branches?.flatMap(branch=>branch.entries.map(item=>item.label))||[],
})));
</script>
<style scoped>
.identity-desk{max-width:none;padding:0}.identity-cover{display:flex;align-items:center;gap:14px;padding:16px;border-radius:17px;background:var(--soft-title-line);box-shadow:var(--soft-raised)}
.identity-monogram{width:54px;height:54px;flex:none;display:grid;place-items:center;font-size:22px;font-weight:700;border-radius:17px;color:var(--v2-blue);background:var(--soft-face);box-shadow:var(--soft-key)}
.identity-cover h1,.identity-body h2{margin:0}.identity-cover p,.identity-cover small{font-size:12px;margin:0;color:var(--v2-muted)}.identity-tools{margin-left:auto;display:flex;align-items:center}
.identity-body{display:grid;grid-template-columns:minmax(235px,.8fr) minmax(0,1.7fr);gap:12px;align-items:start}.identity-body h2{font-size:17px}.identity-body p{margin:10px 0 5px;color:var(--v2-muted)}
.identity-actions,.identity-chips{display:flex;gap:6px;flex-wrap:wrap}.identity-actions{margin-top:15px}.identity-chips span{background:var(--soft-field);box-shadow:var(--soft-inset);padding:4px 8px;border-radius:8px}
.identity-module-row{display:grid;grid-template-columns:minmax(5em,auto) 1fr;gap:10px;font-size:13px;line-height:1.55;padding:7px 0;border-bottom:1px solid var(--soft-edge)}.identity-module-row:last-child{border:0}
@media(max-width:730px){.identity-body{grid-template-columns:1fr}.identity-cover{padding:10px;gap:8px}.identity-monogram{width:42px;height:42px}}
</style>
