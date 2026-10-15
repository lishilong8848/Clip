<template>
  <NPopover trigger="click" placement="bottom-end" :show-arrow="false" :width="232">
    <template #trigger><NButton data-v2-navigation quaternary circle class="v2-theme-toggle" title="导航配色" aria-label="导航配色"><Palette :size="18" /></NButton></template>
    <section class="v2-theme-picker" aria-label="导航配色设置" data-v2-navigation>
      <strong>导航配色</strong>
      <div class="v2-night-mode"><span><Moon v-if="nightMode" :size="17" /><Sun v-else :size="17" />夜间模式</span><NSwitch :value="nightMode" aria-label="夜间模式" :title="nightMode?'切换为白天':'切换为夜间'" @update:value="chooseMode" /></div>
      <div class="v2-theme-swatches" role="group" aria-label="预设配色">
        <button v-for="item in presets" :key="item.color" type="button" :style="{background:item.color}" :aria-label="item.label" :title="item.label" :aria-pressed="color===item.color" @click="choose(item.color)"><Check v-if="color===item.color" :size="19" /></button>
      </div>
      <label>自定义颜色<NColorPicker :value="color" :modes="['hex']" :show-alpha="false" size="small" aria-label="自定义导航颜色" @update:value="preview" @complete="choose" /></label>
      <p v-if="storageError" role="status">当前浏览器无法保存外观设置，刷新后可能恢复默认。</p>
    </section>
  </NPopover>
</template>
<script setup lang="ts">
import {ref,watch} from 'vue';
import {NButton,NColorPicker,NPopover,NSwitch} from 'naive-ui';
import {Check,Palette,Moon,Sun} from 'lucide-vue-next';
import {DEFAULT_NAV_COLOR,nightMode,saveNightMode} from './theme';
const props=defineProps<{userId:string}>();
const emit=defineEmits<{change:[color:string]}>();
const color=ref(DEFAULT_NAV_COLOR),storageError=ref(false);
const presets=[{label:'海湾蓝',color:DEFAULT_NAV_COLOR},{label:'松石青',color:'#16836d'},{label:'石墨灰',color:'#637084'},{label:'莓果红',color:'#a84963'}];
const valid=(value:unknown):value is string=>typeof value==='string'&&/^#[\da-f]{6}$/i.test(value);
const key=()=> 'clipflow:workspace-nav-color:'+(props.userId||'visitor');
function preview(value:string|null){if(!valid(value))return;color.value=value.toLowerCase();emit('change',color.value);}
function choose(value:string|null){if(!valid(value))return;preview(value);try{localStorage.setItem(key(),color.value);storageError.value=false;}catch{storageError.value=true;}}
function chooseMode(value:boolean){storageError.value=!saveNightMode(props.userId,value);}
watch(()=>props.userId,()=>{let saved:string|null=null;storageError.value=false;try{saved=localStorage.getItem(key());}catch{/* In-memory color selection remains available. */}preview(valid(saved)?saved:DEFAULT_NAV_COLOR);},{immediate:true});
</script>
<style>
.v2-night-mode{display:flex;align-items:center;justify-content:space-between;gap:20px;min-height:40px;border-bottom:1px solid var(--v2-border);padding-bottom:10px}.v2-night-mode>span{display:flex;align-items:center;gap:8px}
.v2-theme-picker{display:grid;gap:13px;color:#253342;font-size:13px}.v2-theme-picker>strong{font-size:14px;font-weight:600}.v2-theme-swatches{display:flex;justify-content:space-between;gap:10px}.v2-theme-swatches>button{width:40px;height:40px;flex:none;display:grid;place-items:center;border:2px solid transparent;border-radius:50%;color:#fff;cursor:pointer;box-shadow:inset 0 0 0 1px #00000012;transition:transform 160ms ease-out,box-shadow 160ms ease-out}.v2-theme-swatches>button:hover{transform:translateY(-2px)}.v2-theme-swatches>button[aria-pressed=true]{box-shadow:0 0 0 2px #fff,0 0 0 4px #53687f}.v2-theme-swatches>button:focus-visible{outline:2px solid #1762cf;outline-offset:4px}.v2-theme-picker>label{display:grid;gap:6px}.v2-theme-picker>p{margin:0;color:#946100;font-size:12px;line-height:1.6}.v2-theme-toggle.n-button{width:36px;height:36px;flex:none}@media(prefers-reduced-motion:reduce){.v2-theme-swatches>button{transition:none}.v2-theme-swatches>button:hover{transform:none}}
</style>
