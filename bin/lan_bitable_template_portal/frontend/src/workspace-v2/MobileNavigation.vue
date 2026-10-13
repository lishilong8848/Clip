<template>
  <nav ref="nav" class="v2-mobile-nav v2-liquid-nav" aria-label="手机导航" data-v2-navigation>
    <span ref="capsule" class="v2-nav-capsule" aria-hidden="true" />
    <NButton v-for="tab in tabs" :key="tab.value" quaternary :class="{'is-active':active===tab.value,'is-popping':popping===tab.value}"
      :aria-current="active===tab.value?'page':undefined" @click="$emit('select',tab.value)"><component :is="tab.icon" :size="20" /><span>{{ tab.label }}</span></NButton>
  </nav>
</template>
<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch, type Component } from 'vue';
import { NButton } from 'naive-ui';
const props=defineProps<{tabs:Array<{value:string;label:string;icon:Component}>;active:string}>();
const emit=defineEmits<{select:[value:string];direction:[value:number]}>();
const nav=ref<HTMLElement>(),capsule=ref<HTMLElement>(),popping=ref('');
let frame=0,timer=0,resize:ResizeObserver|undefined,left=0,right=0,index=0;
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
function spring(t:number,hz:number,damping:number,duration:number){
  const w=2*Math.PI*hz,d=Math.sqrt(1-damping*damping);
  const value=(time:number)=>1-Math.exp(-damping*w*time)*(Math.cos(w*d*time)+damping/d*Math.sin(w*d*time));
  return Math.min(1.08,Math.max(0,value(Math.max(0,t))/value(duration)));
}
function paint(width:number){if(!capsule.value)return;const stretch=Math.max(1,(right-left)/width);capsule.value.style.width=width+'px';capsule.value.style.transform=`translateX(${left}px) scaleX(${(right-left)/width}) scaleY(${1-Math.min(.25,(stretch-1)*.25)})`;}
function move(animated:boolean){
  if(!nav.value||!nav.value.clientWidth)return;
  cancelAnimationFrame(frame);
  const next=Math.max(0,props.tabs.findIndex(tab=>tab.value===props.active)),cell=nav.value.clientWidth/props.tabs.length,width=cell-12;
  const targetLeft=next*cell+6,targetRight=targetLeft+width;
  if(!animated||reduced.matches||!right){left=targetLeft;right=targetRight;index=next;paint(width);return;}
  emit('direction',next>=index?1:-1);index=next;
  popping.value=props.active;clearTimeout(timer);timer=window.setTimeout(()=>{popping.value='';},400);
  const start=performance.now(),oldLeft=left,oldRight=right,movingRight=targetLeft>left;
  function tick(now:number){const elapsed=now-start,lead=spring(Math.min(elapsed,400)/1000,3.3,.6,.4),tail=spring(Math.min(Math.max(0,elapsed-60),340)/1000,2.2,.72,.34);
    left=oldLeft+(targetLeft-oldLeft)*(movingRight?tail:lead);right=oldRight+(targetRight-oldRight)*(movingRight?lead:tail);paint(width);
    if(elapsed<400)frame=requestAnimationFrame(tick);else{left=targetLeft;right=targetRight;paint(width);}
  }
  frame=requestAnimationFrame(tick);
}
watch(()=>props.active,()=>move(true),{flush:'post'});
onMounted(()=>{resize=new ResizeObserver(()=>move(false));if(nav.value)resize.observe(nav.value);move(false);});
onBeforeUnmount(()=>{cancelAnimationFrame(frame);clearTimeout(timer);resize?.disconnect();});
</script>
<style scoped>
.v2-liquid-nav{isolation:isolate}.v2-nav-capsule{position:absolute;left:0;top:5px;height:52px;background:#eaf2ff;border-radius:8px;pointer-events:none;transform-origin:left center;z-index:-1}.v2-liquid-nav>.n-button{min-width:0;color:#636366;transition:color 250ms ease-out}.v2-liquid-nav>.n-button.is-active{color:#0071e3}.v2-liquid-nav .is-popping :deep(svg){animation:v2-nav-icon 400ms cubic-bezier(.2,.8,.2,1.04)}@keyframes v2-nav-icon{0%{transform:scale(1)}22.5%{transform:scale(.72)}75%{transform:scale(1.06)}100%{transform:scale(1)}}@media(prefers-reduced-motion:reduce){.v2-liquid-nav .is-popping :deep(svg){animation:none}.v2-liquid-nav>.n-button{transition:none}}
</style>
