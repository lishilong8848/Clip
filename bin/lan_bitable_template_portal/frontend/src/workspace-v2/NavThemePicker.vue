<template>
  <NPopover trigger="click" placement="bottom-end" :show-arrow="false" :width="304" style="max-width:calc(100vw - 24px)">
    <template #trigger><NButton data-v2-navigation quaternary circle class="v2-theme-toggle" title="主题配色" aria-label="导航配色"><Palette :size="18" /></NButton></template>
    <section class="v2-theme-picker" aria-label="导航配色设置" data-v2-navigation>
      <strong>主题配色</strong>
      <div class="v2-night-mode"><span><Moon v-if="nightMode" :size="17" /><Sun v-else :size="17" />夜间模式</span><NSwitch :value="nightMode" aria-label="夜间模式" :title="nightMode?'切换为白天':'切换为夜间'" @update:value="chooseMode" /></div>
      <div class="v2-theme-swatches" role="group" aria-label="预设配色">
        <button v-for="item in PALETTES" :key="item.id" type="button" :aria-label="item.label" :title="item.label" :aria-pressed="paletteId===item.id" @click="choosePreset(item.id)">
          <span class="v2-palette-preview" aria-hidden="true"><i :style="{background:item[nightMode?'dark':'light'].bg}" /><i :style="{background:item[nightMode?'dark':'light'].surface}" /><i :style="{background:item[nightMode?'dark':'light'].accent}" /></span>
          <span>{{ item.label }}<Check v-if="paletteId===item.id" :size="15" /></span>
        </button>
      </div>
      <label>自定义强调色<NColorPicker :value="color" :modes="['hex']" :show-alpha="false" size="small" aria-label="自定义导航颜色" @update:value="preview" @complete="choose" /></label>
      <p v-if="storageError" role="status">当前浏览器无法保存外观设置，刷新后可能恢复默认。</p>
    </section>
  </NPopover>
</template>
<script setup lang="ts">
import {computed,ref,watch} from 'vue';
import {NButton,NColorPicker,NPopover,NSwitch} from 'naive-ui';
import {Check,Palette,Moon,Sun} from 'lucide-vue-next';
import {PALETTES,paletteId,activeColors,nightMode,saveNightMode,validColor,applyPalette,loadPalette,savePalette} from './theme';
const props=defineProps<{userId:string}>();
const emit=defineEmits<{change:[color:string]}>();
const color=computed(()=>activeColors.value.accent),storageError=ref(false);
function preview(value:string|null){if(validColor(value))applyPalette('custom',value);}
function choose(value:string|null){if(validColor(value))storageError.value=!savePalette(props.userId,'custom',value.toLowerCase());}
function choosePreset(id:string){storageError.value=!savePalette(props.userId,id);}
function chooseMode(value:boolean){storageError.value=!saveNightMode(props.userId,value);}
watch(()=>props.userId,()=>{storageError.value=false;loadPalette(props.userId);},{immediate:true,flush:'sync'});
watch(color,value=>emit('change',value),{immediate:true,flush:'sync'});
</script>
<style>
.v2-night-mode{display:flex;align-items:center;justify-content:space-between;gap:20px;min-height:40px;border-bottom:1px solid var(--v2-border);padding-bottom:10px}.v2-night-mode>span{display:flex;align-items:center;gap:8px}
.v2-theme-picker{display:grid;gap:12px;color:var(--v2-ink);font-size:13px;max-height:calc(100dvh - 100px);overflow:auto}.v2-theme-picker>strong{font-size:14px;font-weight:600}.v2-theme-swatches{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.v2-theme-swatches>button{min-width:0;min-height:64px;display:grid;gap:7px;padding:7px;border:1px solid var(--v2-border);border-radius:6px;background:var(--v2-palette-surface);color:var(--v2-ink);cursor:pointer;transition:border-color 160ms ease-out,background-color 160ms ease-out}.v2-theme-swatches>button>span:last-child{display:flex;align-items:center;justify-content:space-between;gap:5px;font-size:12px;line-height:18px}.v2-palette-preview{display:grid;grid-template-columns:6fr 3fr 1fr;height:21px;overflow:hidden;border:1px solid var(--v2-border);border-radius:3px}.v2-theme-swatches>button:hover{background:var(--v2-palette-soft)}.v2-theme-swatches>button[aria-pressed=true]{border-color:var(--v2-blue);box-shadow:inset 0 0 0 1px var(--v2-blue)}.v2-theme-swatches>button:focus-visible{outline:2px solid var(--v2-blue);outline-offset:2px}.v2-theme-picker>label{display:grid;gap:6px}.v2-theme-picker>p{margin:0;color:var(--cf-warning);font-size:12px;line-height:1.6}.v2-theme-toggle.n-button{width:36px;height:36px;flex:none}@media(prefers-reduced-motion:reduce){.v2-theme-swatches>button{transition:none}}
</style>
