<template><div ref="host" class="v2-reply"><LegacyReply :text="text" /></div></template>
<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import LegacyReply from '../components/LighthouseReply.vue';
import { followBottom } from './chatMotion';
const props=defineProps<{text:string;streaming?:boolean}>();
const host=ref<HTMLElement>();
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
let previous='',revealed=0,timer=0,height=0,disposed=false,animation:Animation|undefined,observer:ResizeObserver|undefined;
let queue:{node:HTMLElement;end:number}[]=[],caret:HTMLElement|undefined;
let fading=new Map<number,number>();
function reveal(){
  window.clearTimeout(timer);if(disposed||!host.value)return;
  // Catch up buffered answers in bounded batches, without slowing real model streaming.
  for(let i=0,count=reduced.matches||props.streaming?queue.length:Math.max(1,Math.ceil(queue.length/60));i<count;i++){
    const part=queue.shift();if(!part)break;
    part.node.hidden=false;revealed=part.end;part.node.after(caret!);
  }
  host.value.dataset.revealing=String(queue.length>0);
  const thread=host.value.closest<HTMLElement>('.thread');if(thread)followBottom(thread);
  if(queue.length)timer=window.setTimeout(reveal,24);
  else revealed=previous.length;
}
function decorate(animate:boolean){
  const root=host.value?.firstElementChild;if(!root)return;
  const text=root.textContent||'';
  if(text===previous&&caret&&root.contains(caret))return;
  window.clearTimeout(timer);queue=[];
  root.querySelectorAll('.v2-reply-caret').forEach(node=>node.remove());
  let shared=0;while(shared<revealed&&shared<text.length&&previous[shared]===text[shared])shared++;
  revealed=animate?shared:text.length;previous=text;
  caret=document.createElement('span');caret.className='v2-reply-caret';caret.setAttribute('aria-hidden','true');
  const walker=document.createTreeWalker(root,NodeFilter.SHOW_TEXT),nodes:Text[]=[];
  for(let node=walker.nextNode();node;node=walker.nextNode())nodes.push(node as Text);
  let offset=0,words=0,lastVisible:Node|undefined;
  const now=performance.now(),nextFading=new Map<number,number>();
  for(const node of nodes){
    const value=node.textContent||'';
    if(!value.trim()||node.parentElement?.closest('pre,code,table,.v2-word')){offset+=value.length;lastVisible=node;continue;}
    const fragment=document.createDocumentFragment();
    const parts=value.match(/[\u3400-\u9fff]{1,3}|[^\u3400-\u9fff\s]+\s*|\s+/gu)||[];
    for(let index=0;index<parts.length;index++){
      const part=words>=600?parts.slice(index).join(''):parts[index]!;
      if(words>=600)index=parts.length;
      const start=offset;offset+=part.length;
      const began=offset<=revealed?fading.get(start):undefined;
      if(animate&&(offset>revealed||began!==undefined&&now-began<380)){
        const span=document.createElement('span');span.textContent=part;span.hidden=offset>revealed;
        if(words++<600)span.className='v2-word cword';
        if(props.streaming){
          const at=began??now;nextFading.set(start,at);
          // Keep an in-flight fade at its elapsed time when Markdown replaces the DOM.
          span.style.animationDelay=Math.min(0,at-now)+'ms';
        }
        fragment.append(span);
        if(span.hidden)queue.push({node:span,end:offset});else lastVisible=span;
      }else{lastVisible=document.createTextNode(part);fragment.append(lastVisible);}
    }
    node.replaceWith(fragment);
  }
  fading=nextFading;
  if(!text)return;
  if(queue.length)queue[0]!.node.before(caret);
  else if(lastVisible?.parentNode)lastVisible.parentNode.appendChild(caret);
  else root.append(caret);
  reveal();
}
onMounted(()=>{
  decorate(!reduced.matches&&!!host.value?.closest('.turn')?.classList.contains('is-pending'));
  const root=host.value?.firstElementChild;
  observer=new ResizeObserver(entries=>{
    if(!host.value||disposed)return;
    const entry=entries.find(item=>item.target===root);
    if(entry){
      const next=entry.contentRect.height,from=animation?.playState==='running'?host.value.getBoundingClientRect().height:height;
      height=next;animation?.cancel();
      if(!reduced.matches&&from>0&&next>from)animation=host.value.animate([{height:from+'px',overflow:'hidden'},{height:next+'px',overflow:'hidden'}],{duration:250,easing:'cubic-bezier(.16,1,.3,1)'});
    }
    const thread=host.value.closest<HTMLElement>('.thread');if(thread)followBottom(thread);
  });
  if(host.value)observer.observe(host.value);if(root)observer.observe(root);
});
watch(()=>props.text,async()=>{await nextTick();if(!disposed)decorate(!reduced.matches);},{flush:'pre'});
function motionChanged(){if(reduced.matches){animation?.cancel();reveal();}}
onMounted(()=>reduced.addEventListener('change',motionChanged));
onBeforeUnmount(()=>{disposed=true;window.clearTimeout(timer);queue=[];observer?.disconnect();animation?.cancel();reduced.removeEventListener('change',motionChanged);});
</script>
