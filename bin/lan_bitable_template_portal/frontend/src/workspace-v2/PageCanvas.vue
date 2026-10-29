<template>
  <div v-if="embedded||plain" class="v2-embedded-canvas"><section v-for="block in blocks.filter(block=>block.when!==false)" :key="block.id" :style="{'--block-span':plain?12:block.w||12}" class="v2-embedded-block"><slot :name="'header-'+block.id" /><slot :name="block.id" /></section></div>
  <section v-else class="v2-page-canvas" :class="{'is-customizing':editing}" :data-page-layout="page">
    <Teleport v-if="toolsTarget && !editing && active" :to="toolsTarget"><NButton size="small" text data-v2-navigation class="v2-layout-trigger" :disabled="!ready||loading" :aria-label="'自定义'+label+'布局'" :title="loading?'正在同步账号布局':'自定义布局'" @click="edit"><SlidersHorizontal :size="14" />自定义布局</NButton></Teleport>
    <header v-if="editing" class="v2-page-layout-tools" data-v2-navigation>
      <span v-if="editing" class="v2-page-layout-hint"><GripVertical :size="15" />拖动调整位置，右下角调整大小</span><span v-else></span>
      <template v-if="editing"><NButton size="small" data-v2-navigation :disabled="saving" @click="cancel">取消</NButton><NButton size="small" data-v2-navigation :disabled="saving" @click="restore">恢复默认</NButton><NButton size="small" data-v2-navigation :disabled="saving" @click="library=true"><Plus :size="14" />添加组件</NButton><NButton size="small" type="primary" :loading="saving" :disabled="saving" @click="save"><Check :size="14" />保存布局</NButton></template>
    </header>
    <NAlert v-if="error" type="warning" :bordered="false">{{error}} <NButton text data-v2-navigation :disabled="saving||loading" @click="reload">重新读取</NButton></NAlert>
    <WidgetGrid :items="visible" :editing="editing&&!saving" :inert="saving||undefined" :cell-height="64" :label="label+'布局'" :content-sized-ids="page==='repair'?['block_records']:undefined" adaptive @layout="positions">
      <template #tools="{item}">
        <slot v-if="item.builtin&&!editing" :name="'header-'+item.id.slice(6)" />
        <NButton v-if="!toolsTarget&&!editing&&item.id===visible[0]?.id" size="tiny" text data-v2-navigation :disabled="!ready||loading" :aria-label="'自定义'+label+'布局'" title="自定义布局" @click="edit"><SlidersHorizontal :size="14" />自定义布局</NButton>
        <div v-if="editing&&!saving" class="v2-widget-tools" data-v2-navigation>
          <NButton quaternary circle :aria-label="'上移'+item.title" @click="move(item.id,-1)"><ArrowUp :size="14" /></NButton><NButton quaternary circle :aria-label="'下移'+item.title" @click="move(item.id,1)"><ArrowDown :size="14" /></NButton>
          <NButton quaternary circle :aria-label="'设置'+item.title" @click="selected=item"><Settings2 :size="15" /></NButton>
          <NButton v-if="!item.builtin||blocks.find(block=>'block_'+block.id===item.id)?.required===false" quaternary circle :aria-label="'移除'+item.title" @click="remove(item)"><X :size="15" /></NButton>
        </div>
      </template>
      <template #default="{item}"><div class="v2-module-block" :class="['view-'+(presentations[item.id]||'default'),{'is-business-block':item.builtin}]" :data-business-block="item.builtin?item.id:undefined"><slot v-if="item.builtin" :name="item.id.slice(6)" /><WidgetView v-else :item="item" :user="context.user.value" :scope="context.scope.value" :scopes="scopeOptions" @open="navigate" /></div></template>
    </WidgetGrid>
    <NModal v-model:show="library" preset="card" title="添加当前功能组件" class="v2-page-layout-library" :bordered="false">
      <div class="v2-page-library-items" data-v2-navigation><NButton v-for="block in missing" :key="block.id" @click="addBlock(block)"><Plus :size="16" />{{block.title}}</NButton><NButton v-for="entry in additions" :key="entry.kind" @click="add(entry.kind)"><Plus :size="16" />{{entry.title}}</NButton></div><NEmpty v-if="!missing.length&&!additions.length" description="本页业务区块已全部显示" />
    </NModal>
    <NModal :show="!!selected" preset="card" title="区块显示设置" class="v2-page-layout-library" :bordered="false" @update:show="value=>{if(!value)selected=null}">
      <template v-if="selected"><NFormItem label="宽度"><NSelect :value="selected.w" :options="widthOptions(selected)" @update:value="value=>resizeItem('w',value)" /></NFormItem><NFormItem label="高度"><NSelect :value="selected.h" :options="heightOptions(selected)" @update:value="value=>resizeItem('h',value)" /></NFormItem>
      <NFormItem v-if="selected.builtin" label="列表显示"><NSelect :value="presentations[selected.id]||'default'" :options="[{label:'自动适应尺寸',value:'default'},{label:'紧凑列表',value:'compact'},{label:'舒展列表',value:'comfortable'}]" @update:value="value=>presentations[selected!.id]=value" /></NFormItem>
      <NFormItem v-if="!selected.builtin&&['events','notices'].includes(selected.kind)" label="替换图表"><NSelect :value="selected.display" :options="displayOptions(selected.kind)" @update:value="value=>{selected!.display=value}" /></NFormItem>
      <NButton data-v2-navigation type="primary" @click="selected=null">完成设置</NButton></template>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import { computed,inject,onBeforeUnmount,onMounted,onUpdated,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NFormItem,NModal,NSelect,useDialog,useMessage } from 'naive-ui';
import { ArrowDown,ArrowUp,Check,GripVertical,Plus,Settings2,SlidersHorizontal,X } from 'lucide-vue-next';
import WidgetGrid from './dashboard/WidgetGrid.vue';
import WidgetView from './dashboard/WidgetView.vue';
import { catalog,displayOptions,widget,reorderWidgets,type WidgetKind } from './dashboard/layout';
import { defaultPageLayout,extraKinds,PAGE_WIDGET_LIMIT,pageContext,pageLayoutKey,sanitizePageLayout,type PageBlock,type PageLayout,type PageWidget } from './pageLayout';
import { navigate,registerNavigationGuard } from './navigation';
import { embeddedContextKey } from './embeddedContext';
import { useLayoutSync } from './useLayoutSync';
const props=withDefaults(defineProps<{page:string;label:string;blocks:PageBlock[];active?:boolean;plain?:boolean}>(),{active:true,plain:false});
const context=inject(pageContext)!;
const embedded=inject(embeddedContextKey,null);
const toolsTarget=ref<HTMLElement|null>(null);
function findTools(){const next=embedded||props.plain?null:document.querySelector<HTMLElement>('[data-v2-layout-tools="'+props.page+'"]');if(toolsTarget.value!==next)toolsTarget.value=next;}
onMounted(findTools);onUpdated(findTools);
const dialog=useDialog(),message=useMessage(),editing=ref(false),library=ref(false),selectedId=ref<string|null>(null);
const layout=ref<PageLayout>(defaultPageLayout(props.blocks,context.user.value)),presentations=ref<Record<string,string>>({});
// GridStack replaces item objects; keep the settings panel attached by stable ID.
const selected=computed<PageWidget|null>({get:()=>layout.value.items.find(item=>item.id===selectedId.value)||null,set:item=>{selectedId.value=item?.id||null;}});
const scopeOptions=computed(()=>context.scopes.value.map(value=>({value,label:value==='ALL'?'全部楼栋':value+'楼'})));
let saved='',removeGuard:(()=>void)|undefined;
const snapshot=()=>JSON.stringify({layout:layout.value,presentations:presentations.value});
const visible=computed(()=>layout.value.items.filter(item=>!item.builtin||props.blocks.some(block=>'block_'+block.id===item.id&&block.when!==false)));
const missing=computed(()=>props.blocks.filter(block=>block.when!==false&&!layout.value.items.some(item=>item.id==='block_'+block.id)));
const additions=computed(()=>catalog.filter(entry=>extraKinds(context.module.value,context.user.value).includes(entry.kind)));
function normalize(value:any){
  const normalized=sanitizePageLayout(value?.layout,props.blocks,context.user.value,context.scopes.value,context.module.value);
  const styles=Object.fromEntries(Object.entries(value?.presentations||{}).filter(([id,entry])=>normalized.items.some(item=>item.id===id)&&['default','compact','comfortable'].includes(String(entry)))) as Record<string,string>;
  return {layout:normalized,presentations:styles};
}
const sync=useLayoutSync({user:()=>context.user.value,page:()=>props.page,legacyKey:()=>pageLayoutKey(context.user.value,props.page),enabled:()=>!embedded&&!props.plain,locked:()=>editing.value,normalize,apply:value=>{
  layout.value=value.layout;presentations.value=value.presentations;saved=snapshot();editing.value=false;selected.value=null;library.value=false;removeGuard?.();removeGuard=undefined;
}});
const {loading,saving,error,ready}=sync;
function reload(){if(saving.value)return;if(editing.value&&snapshot()!==saved){dialog.warning({title:'重新读取账号布局',content:'本次未保存的布局调整将被放弃。',positiveText:'重新读取',negativeText:'继续调整',onPositiveClick:()=>void sync.reload()});}else void sync.reload();}
function edit(){if(!ready.value||loading.value||saving.value)return;if(document.querySelector('[role="dialog"][aria-modal="true"],.n-modal-container .n-card')){message.warning('请先完成当前弹窗中的填写。');return;}editing.value=true;removeGuard?.();removeGuard=registerNavigationGuard((_target,proceed)=>{if(saving.value){message.warning('账号布局正在保存，请稍候。');return false;}if(!props.active||snapshot()===saved)return true;dialog.warning({title:'布局尚未保存',content:'离开将放弃本次布局调整。',positiveText:'放弃并离开',negativeText:'继续调整',onPositiveClick:()=>{cancel();proceed();}});return false;});}
function cancel(){const value=JSON.parse(saved);layout.value=value.layout;presentations.value=value.presentations;editing.value=false;selected.value=null;library.value=false;error.value='';removeGuard?.();removeGuard=undefined;}
async function save(){try{const value=normalize({layout:layout.value,presentations:presentations.value});if(!await sync.save(value))return;layout.value=value.layout;presentations.value=value.presentations;saved=snapshot();selected.value=null;library.value=false;editing.value=false;removeGuard?.();removeGuard=undefined;message.success('本页布局已同步到账号');}catch(e){error.value=e instanceof Error?e.message:'布局保存失败，请重试。';}}
function restore(){dialog.warning({title:'恢复本页默认布局',content:'仅调整本页显示位置和尺寸，业务填写内容保持不变。',positiveText:'恢复默认',negativeText:'取消',onPositiveClick:()=>{layout.value=defaultPageLayout(props.blocks,context.user.value);presentations.value={};}});}
function room(){if(layout.value.items.length<PAGE_WIDGET_LIMIT)return true;message.warning('每页最多保留'+PAGE_WIDGET_LIMIT+'个区块，请先移除不常用组件。');return false;}
function bottom(){return Math.max(0,...layout.value.items.map(item=>item.y+item.h));}
function addBlock(block:PageBlock){if(!room())return;const item=defaultPageLayout([block],context.user.value).items[0]!;layout.value.items.push({...item,y:bottom()});library.value=false;}
function add(kind:WidgetKind){if(!room())return;const module=context.module.value==='notices'?(context.user.value.modules||[]).find((id:string)=>['maintenance','change','repair','polling','adjust','power'].includes(id)):context.module.value;layout.value.items.push({...widget(kind,context.user.value,{y:bottom(),module,w:6,h:7}),minW:4,minH:4});library.value=false;}
function remove(item:PageWidget){if(saving.value||item.builtin&&props.blocks.find(block=>'block_'+block.id===item.id)?.required!==false)return;layout.value.items=layout.value.items.filter(row=>row.id!==item.id);}
function positions(items:PageWidget[]){if(editing.value&&!saving.value){const updates=new Map(items.map(item=>[item.id,item]));layout.value.items=layout.value.items.map(item=>updates.get(item.id)||item);}}
function move(id:string,direction:number){const ids=new Set(visible.value.map(item=>item.id));layout.value.items=[...reorderWidgets(visible.value,id,direction),...layout.value.items.filter(item=>!ids.has(item.id))];}
function resizeItem(key:'w'|'h',value:number){if(!selected.value)return;selected.value[key]=value;selected.value.x=Math.min(selected.value.x,12-selected.value.w);}
function widthOptions(item:PageWidget){return Array.from({length:12},(_,i)=>i+1).filter(value=>value>=(item.minW||4)).map(value=>({value,label:value===12?'整行':value+' / 12 列'}));}
function heightOptions(item:PageWidget){return Array.from({length:30},(_,i)=>i+1).filter(value=>value>=(item.minH||4)).map(value=>({value,label:value*64+' 像素'}));}
function unload(event:BeforeUnloadEvent){if(saving.value||editing.value&&snapshot()!==saved){event.preventDefault();event.returnValue='';}}
watch(()=>props.blocks,()=>{if(!editing.value){layout.value=sanitizePageLayout(layout.value,props.blocks,context.user.value,context.scopes.value,context.module.value);saved=snapshot();}},{deep:true});
window.addEventListener('beforeunload',unload);
onBeforeUnmount(()=>{removeGuard?.();window.removeEventListener('beforeunload',unload);});
</script>
