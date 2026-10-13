<template><div ref="host" class="v2-reply"><LegacyReply :text="text" /></div></template>
<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import LegacyReply from '../components/LighthouseReply.vue';
import { followBottom } from './chatMotion';
const props=defineProps<{text:string}>();
const host=ref<HTMLElement>();
let previous='',animation:Animation|undefined,observer:ResizeObserver|undefined,disposed=false;
function decorate(animate:boolean){
  const root=host.value?.firstElementChild;if(!root)return;
  root.querySelectorAll('.v2-reply-caret').forEach(caret=>caret.remove());
  const previousLength=previous.length;previous=root.textContent||'';
  const walker=document.createTreeWalker(root,NodeFilter.SHOW_TEXT),nodes:Text[]=[];
  for(let node=walker.nextNode();node;node=walker.nextNode())nodes.push(node as Text);
  let offset=0,sequence=0;
  for(const node of nodes){
    const value=node.textContent||'';
    if(!value.trim()||node.parentElement?.closest('pre,code,table,.v2-word')){offset+=value.length;continue;}
    const fragment=document.createDocumentFragment();
    for(const part of value.match(/[\u3400-\u9fff]{1,3}|[^\u3400-\u9fff\s]+\s*|\s+/gu)||[]){
      offset+=part.length;
      if(animate&&offset>previousLength&&sequence<160){const span=document.createElement('span');span.className='v2-word';span.textContent=part;span.style.animationDelay=Math.min(sequence++*24,500)+'ms';fragment.append(span);}
      else fragment.append(document.createTextNode(part));
    }
    node.replaceWith(fragment);
  }
  const caret=document.createElement('span');caret.className='v2-reply-caret';caret.setAttribute('aria-hidden','true');
  const last=nodes[nodes.length-1]?.parentElement;
  const leaves=root.querySelectorAll('p,li,td,pre');(last||leaves.item(leaves.length-1)||root).append(caret);
}
onMounted(()=>{
  const pending=host.value?.closest('.turn')?.classList.contains('is-pending');decorate(!!pending);
  observer=new ResizeObserver(()=>{const thread=host.value?.closest<HTMLElement>('.thread');if(thread)followBottom(thread);});
  if(host.value)observer.observe(host.value);
});
watch(()=>props.text,async()=>{
  const oldHeight=host.value?.getBoundingClientRect().height||0;
  animation?.cancel();await nextTick();if(disposed||!host.value)return;
  const reduced=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  decorate(!reduced);
  const height=host.value.scrollHeight;
  if(!reduced&&oldHeight&&height>oldHeight){animation=host.value.animate([{height:oldHeight+'px',overflow:'hidden'},{height:height+'px',overflow:'hidden'}],{duration:260,easing:'ease-out'});}
  const thread=host.value.closest<HTMLElement>('.thread');if(thread)followBottom(thread);
},{flush:'pre'});
onBeforeUnmount(()=>{disposed=true;observer?.disconnect();animation?.cancel();});
</script>
