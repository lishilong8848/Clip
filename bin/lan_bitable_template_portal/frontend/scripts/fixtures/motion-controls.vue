<template>
  <NConfigProvider :theme="dark ? darkTheme : undefined" :theme-overrides="dark ? nightTheme : theme">
    <main class="controls-fixture">
      <h1>新版交互控件</h1>
      <section><h2>操作与分页</h2><div class="fixture-row"><NButton type="primary">主要操作</NButton><NButton type="success">强调操作</NButton><NButton>次要操作</NButton><NButton quaternary>次要文字</NButton><NButton type="error">删除</NButton><NButton disabled>禁用</NButton></div><NPagination v-model:page="page" :page-count="12" :page-slot="5" /></section>
      <section><h2>复选、单选与状态开关</h2>
        <div class="fixture-row"><label><input id="check-a" v-model="checked" type="checkbox" value="a">复选 A</label><label><input id="check-b" v-model="checked" type="checkbox" value="b">复选 B</label><label><input id="check-disabled" type="checkbox" disabled>禁用</label><label><input id="check-mixed" type="checkbox" :indeterminate="true">部分选择</label></div>
        <div class="fixture-row"><label><input v-model="choice" type="radio" name="choice" value="a">方案甲</label><label><input v-model="choice" type="radio" name="choice" value="b">方案乙</label><NCheckbox v-model:checked="naiveChecked">组件库复选</NCheckbox><NRadio :checked="true">组件库单选</NRadio></div>
        <div class="fixture-row"><label><input id="binary" v-model="enabled" data-v2-binary type="checkbox">启用提醒</label><NSwitch v-model:value="enabled" aria-label="启用功能" /><NSwitch size="small" aria-label="小号开关" /><NSwitch disabled aria-label="禁用开关" /></div>
      </section>
      <section><h2>实时进度与滑块</h2>
        <MotionProgress :value="progress" :max="10" aria-label="机柜处理进度" />
        <MotionProgress :max="0" :value="0" aria-label="尚未取得总数" />
        <NProgress type="line" :percentage="progress*10" />
        <div class="seg-bar" role="progressbar" aria-label="分步进度" aria-valuemin="0" aria-valuemax="5" aria-valuenow="3"><i v-for="n in 5" :key="n" :class="{on:n<=3}"></i></div>
        <label class="fixture-range">跟进进度 <output>{{range}}%</output><input id="range" v-motion-range v-model.number="range" type="range" min="0" max="100"></label>
        <label class="fixture-range">锁定项<input v-motion-range type="range" disabled value="50"></label>
        <NSlider v-model:value="range" aria-label="组件库进度" />
      </section>
      <section><h2>下拉菜单</h2><div class="fixture-row"><NDropdown trigger="click" :options="menu" @select="selected=String($event)"><NButton id="actions">操作菜单</NButton></NDropdown><NSelect v-model:value="selected" :options="options" style="width:180px" aria-label="状态筛选" /><VnetSelect v-model="nativeSelected" input-id="native-select" label="楼栋" :options="['A楼','B楼','C楼']" /><select id="native-status" v-model="selected" aria-label="原生状态"><option value="edit">进行中</option><option value="done">已完成</option><option value="disabled" disabled>不可选</option></select></div></section>
      <section data-assistant-control="true" style="--lh-action:#a6c8ff;--lh-accent:#a6c8ff;--lh-charcoal:#eef2f8;--lh-surface:#282c34;--lh-border:#697481;background:#282c34;color:#eef2f8;padding:18px"><h2>助手深色主题</h2><div class="fixture-row"><label><input type="checkbox" checked>选择通告</label><label><input type="radio" checked>本次工单</label><NButton type="primary">确认</NButton></div><MotionProgress :value="6" :max="10" aria-label="助手上传进度" /></section>
      <output id="values">{{JSON.stringify({checked,choice,enabled,range,selected,page})}}</output>
    </main>
  </NConfigProvider>
</template>
<script setup lang="ts">
import {ref} from 'vue';
import {NConfigProvider,NButton,NCheckbox,NRadio,NSwitch,NSlider,NPagination,NSelect,NDropdown,NProgress,darkTheme} from 'naive-ui';
import MotionProgress from '../../src/workspace-v2/MotionProgress.vue';
import VnetSelect from '../../src/components/VnetSelect.vue';
import {vMotionRange} from '../../src/workspace-v2/motionRange';
import {theme,nightTheme,applyNightMode} from '../../src/workspace-v2/theme';
import '../../src/workspace-v2/workspace.css';
import '../../src/workspace-v2/interactions.css';
import '../../src/workspace-v2/motionBlocks.css';
import '../../src/workspace-v2/night.css';
document.documentElement.classList.add('workspace-v2');
const checked=ref(['a']),choice=ref('a'),naiveChecked=ref(true),enabled=ref(false),range=ref(30),progress=ref(3),page=ref(1),selected=ref('edit'),nativeSelected=ref('A楼'),dark=ref(false);
const options=[{label:'进行中',value:'edit'},{label:'已完成',value:'done'}],menu=[{label:'编辑条目',key:'edit'},{label:'复制链接',key:'copy'},{label:'归档（无权限）',key:'archive',disabled:true},{type:'divider',key:'divider'},{label:'删除',key:'delete',props:{style:'color:#c93d4c'}}];
(window as any).controls={progress:(n:number)=>progress.value=n,range:(n:number)=>range.value=n,theme:(value:boolean)=>{dark.value=value;applyNightMode(value);}};
</script>
<style>
.controls-fixture{max-width:900px;margin:24px auto;padding:20px;color:var(--v2-ink);background:var(--v2-bg)}.controls-fixture h1{font-size:22px}.controls-fixture section{border-top:1px solid var(--v2-border);padding:18px 0}.controls-fixture h2{font-size:14px;margin:0 0 16px}.fixture-row{display:flex;align-items:center;gap:18px;flex-wrap:wrap;margin-bottom:18px}.fixture-row label{display:flex;align-items:center;gap:8px;font-size:13px}.controls-fixture .v2-motion-progress{margin-bottom:16px}.fixture-range{display:flex;gap:12px;align-items:center;margin:20px 0;font-size:13px}.fixture-range input{flex:1}.controls-fixture .seg-bar{margin-top:16px}#values{display:block;font-size:12px;overflow-wrap:anywhere;margin-top:12px}@media(max-width:600px){.controls-fixture{margin:0;padding:14px}.fixture-row{gap:14px}.controls-fixture .n-select{max-width:140px}}
</style>
