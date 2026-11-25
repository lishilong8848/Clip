<template>
  <section class="serenity-desk identity-desk v2-my" aria-label="个人信息">
    <header class="identity-cover">
      <div class="identity-monogram" aria-hidden="true">{{ String(user.name||'').slice(-2) }}</div>
      <div class="identity-name"><p>个人信息</p><h1>{{ user.name }}</h1></div>
      <span class="identity-level">{{ level }}</span>
      <div class="identity-tools"><slot name="tools"/></div>
    </header>
    <div class="identity-body">
      <section class="serenity-panel identity-access">
        <h2><UserRound :size="17" aria-hidden="true"/>账号资料</h2>
        <dl class="identity-facts">
          <div><dt>岗位</dt><dd>{{ user.category_label || category }}</dd></div>
          <div><dt>登录方式</dt><dd>{{ user.login_method==='password'?'姓名密码登录':'飞书登录' }}</dd></div>
          <div class="identity-scopes"><dt>可访问楼栋</dt><dd class="identity-chips"><span v-for="scope in scopes" :key="scope.value">{{ scope.label }}</span><small v-if="!scopes.length">暂无</small></dd></div>
        </dl>
        <div class="identity-actions"><NButton type="primary" @click="emit('open','/?view=permission')"><ShieldCheck :size="16"/>申请权限</NButton>
          <NButton v-if="user.login_method==='password'" @click="emit('open','/?account=password')">修改密码</NButton>
          <NButton v-if="mobile && settings" @click="emit('settings')">{{ user.role==='admin'?'系统设置':'账号与权限' }}</NButton></div>
      </section>
      <section class="serenity-panel identity-modules"><h2><LayoutGrid :size="17" aria-hidden="true"/>功能权限 <small v-if="permissionCount">{{ permissionCount }} 项</small></h2>
        <div v-for="group in permissions" :key="group.key" class="identity-module-row"><strong>{{ group.label }}</strong>
          <div class="identity-chips"><span v-for="label in group.labels" :key="label">{{ label }}</span></div></div><p v-if="!permissions.length">暂无业务功能授权</p>
      </section>
    </div>
    <slot/>
  </section>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {NButton} from 'naive-ui';
import {LayoutGrid,ShieldCheck,UserRound} from 'lucide-vue-next';
import type {Dict} from './client';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];level:string;category:string;groups:SidebarGroup[];settings:boolean;mobile:boolean}>();
const emit=defineEmits<{open:[url:string];settings:[]}>();
const permissions=computed(()=>props.groups.filter(group=>group.key!=='home').map(group=>({
  key:group.key,label:group.label,labels:group.direct?[group.direct.label]:group.branches?.flatMap(branch=>branch.entries.map(item=>item.label))||[],
})));
const permissionCount=computed(()=>permissions.value.reduce((total, group)=>total+group.labels.length,0));
</script>
<style scoped>
.identity-desk{width:min(100%,1180px);margin-inline:auto;padding:0}
.identity-cover{display:flex;align-items:center;gap:12px;padding:13px 16px;border:1px solid var(--soft-edge);border-radius:12px;background:var(--soft-title-line)}
.identity-monogram{width:46px;height:46px;flex:none;display:grid;place-items:center;font-size:18px;font-weight:700;border-radius:11px;color:var(--v2-blue);background:var(--soft-face);border:1px solid var(--soft-edge)}
.identity-name{min-width:0}.identity-cover h1,.identity-body h2{margin:0}.identity-cover h1{font-size:22px;overflow-wrap:anywhere}
.identity-cover p,.identity-body small,.identity-facts dt{font-size:var(--cf-font-meta);color:var(--v2-muted);margin:0}
.identity-level{font-size:12px;color:var(--v2-palette-accent-text);background:var(--v2-palette-selection);border-radius:6px;padding:4px 7px;white-space:nowrap}
.identity-tools{margin-inline-start:auto;display:flex;align-items:center;flex:none}.identity-tools:empty{display:none}
.identity-body{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.55fr);gap:10px;align-items:start}
.identity-body h2{font-size:17px;display:flex;gap:7px;align-items:center}
.identity-body h2 svg{color:var(--v2-blue)}.identity-body h2 small{margin-inline-start:auto}
.identity-facts{margin:10px 0;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.identity-facts dd{margin:4px 0 0;overflow-wrap:anywhere}.identity-scopes{grid-column:1/-1}
.identity-actions,.identity-chips{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.identity-actions{border-top:1px solid var(--soft-edge);padding-top:10px}
.identity-actions :deep(.n-button__content){gap:4px}
.identity-chips span{background:var(--soft-field);padding:3px 7px;border-radius:5px;line-height:1.5;font-size:var(--cf-font-meta)}
.identity-module-row{display:grid;grid-template-columns:6em minmax(0,1fr);align-items:start;gap:9px;font-size:13px;padding:8px 0;border-bottom:1px solid var(--soft-edge)}
.identity-module-row:last-child{border:0;padding-bottom:0}
@media(max-width:730px){.identity-body{grid-template-columns:minmax(0,1fr)}.identity-cover{padding:10px;gap:8px}.identity-monogram{width:40px;height:40px}.identity-cover h1{font-size:18px}.identity-actions :deep(.n-button){min-height:44px}}
</style>
