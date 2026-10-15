<template>
  <div ref="host" class="grid-stack v2-widget-grid" :class="{'is-editing':editing}" :aria-label="label">
    <TransitionGroup :css="false" @enter="enter" @leave="leave">
    <div v-for="item in items" :key="item.id" class="grid-stack-item" :style="{'--widget-order':item.y*12+item.x}" :gs-id="item.id" :data-widget-id="item.id" :data-widget-kind="item.kind">
      <section class="grid-stack-item-content v2-widget" :aria-label="item.title">
        <header class="v2-widget-head">
          <button v-if="editing" class="v2-widget-drag" data-v2-navigation type="button" :aria-label="'拖动'+item.title" :title="'拖动'+item.title"><GripVertical :size="16" /></button>
          <h2><slot name="title" :item="item">{{ item.title }}</slot></h2><slot name="tools" :item="item" />
        </header>
        <WidgetViewport :editing="editing" :adaptive="adaptive||item.kind==='module'"><slot :item="item" /></WidgetViewport>
      </section>
    </div>
    </TransitionGroup>
  </div>
</template>
<script setup lang="ts" generic="T extends Widget & {minW?:number;minH?:number}">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { GridStack, type GridItemHTMLElement, type GridStackWidget } from 'gridstack';
import { GripVertical } from 'lucide-vue-next';
import 'gridstack/dist/gridstack.min.css';
import type { Widget } from './layout';
import WidgetViewport from '../WidgetViewport.vue';
const props=withDefaults(defineProps<{items:T[];editing:boolean;cellHeight?:number;label?:string;adaptive?:boolean}>(),{cellHeight:80,label:'个人工作台组件',adaptive:false});
const emit=defineEmits<{layout:[items:T[]]}>();
const host=ref<HTMLElement>();
let grid:GridStack|null=null,syncing=false,disposed=false;
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
const mobile=window.matchMedia('(max-width:760px)');
function transition(el:Element,done:()=>void,leaving:boolean){
  const panel=el.querySelector('.v2-widget');if(!panel||reduced.matches){done();return;}
  const frames=[{opacity:0,transform:'translateY(8px) scale(.98)'},{opacity:1,transform:'translateY(0) scale(1)'}];
  // Animate the panel only; GridStack owns the surrounding layout geometry.
  const animation=panel.animate(leaving?frames.reverse():frames,{duration:leaving?180:300,easing:leaving?'ease-in':'cubic-bezier(.22,1.08,.4,1)',fill:'both'});
  animation.onfinish=()=>{done();animation.cancel();};animation.oncancel=done;
}
function enter(el:Element,done:()=>void){transition(el,done,false);}
function leave(el:Element,done:()=>void){transition(el,done,true);}
const geometry=(w:T):GridStackWidget=>({id:w.id,x:w.x,y:w.y,w:w.w,h:w.h,minW:w.minW||(w.kind==='module'?4:3),minH:w.minH||(w.kind==='module'?6:3),maxH:props.adaptive||w.kind==='module'?30:12});
async function sync(){
  await nextTick();if(!grid||disposed)return;syncing=true;grid.batchUpdate();
  const ids=new Set(props.items.map(w=>w.id));
  for(const el of grid.getGridItems())if(!ids.has(el.gridstackNode?.id||''))grid.removeWidget(el,false,false);
  for(const el of host.value!.querySelectorAll<GridItemHTMLElement>('.grid-stack-item')){
    const item=props.items.find(w=>w.id===el.dataset.widgetId);if(item&&!el.gridstackNode)grid.makeWidget(el,geometry(item));
  }
  grid.load(props.items.map(geometry),false);grid.batchUpdate(false);grid.setStatic(!props.editing||mobile.matches);syncing=false;changed();
}
function changed(){
  if(syncing||!grid||!props.editing)return;
  // Mobile uses content-sized CSS flow, without rewriting desktop geometry.
  const positions=grid.save(false,false,undefined,12) as GridStackWidget[];
  const next=props.items.map(w=>{const p=positions.find(p=>p.id===w.id);return p?{...w,x:p.x??0,y:p.y??0,w:p.w??p.minW??1,h:p.h??p.minH??1}:w;});
  if(next.some((w,i)=>['x','y','w','h'].some(k=>w[k as keyof Widget]!==props.items[i]?.[k as keyof Widget])))emit('layout',next);
}
function motion(){grid?.setAnimation(!reduced.matches);}
async function controls(){await nextTick();grid?.setStatic(!props.editing||mobile.matches);for(const el of grid?.getGridItems()||[])grid?.refreshDragHandles(el);}
onMounted(()=>{
  grid=GridStack.init({column:12,mode:'compact',cellHeight:props.cellHeight,margin:8,animate:!reduced.matches,staticGrid:!props.editing||mobile.matches,handle:'.v2-widget-drag',draggable:{scroll:true},resizable:{handles:'se'},alwaysShowResizeHandle:true},host.value!);
  void sync();grid?.on('change',changed);grid?.on('dragstop resizestop',()=>queueMicrotask(changed));reduced.addEventListener('change',motion);mobile.addEventListener('change',controls);
});
watch(()=>JSON.stringify(props.items.map(item=>[item.id,item.x,item.y,item.w,item.h,item.minW,item.minH])),()=>void sync());
watch(()=>props.editing,controls);
onBeforeUnmount(()=>{disposed=true;reduced.removeEventListener('change',motion);mobile.removeEventListener('change',controls);grid?.destroy(false);grid=null;});
</script>
