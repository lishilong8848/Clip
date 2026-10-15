import { h, render } from 'vue';
import { CheckCircle2, CircleAlert, Info, TriangleAlert, X } from 'lucide-vue-next';

// Keep Vue's nodes, events and modal focus ancestry intact; only their presentation moves.
const selector = [
  '.n-alert', '.n-message', '[role="alert"]', '.message-banner', '.inline-message',
  '.directory-message', '.water-message', '.dialog-message', '.error-message', '.login-notice',
  '.notice-error', '.notice-warning', '.assistant-error', '.selection-error', '.skill-error',
  '.skill-notice', '.navigation-error', '.prefill-warning', '.form-warning', '.config-warning',
  '.lhp-notice', '.lk-warning', '.lhs-error', '.lh-sf-error', '.page-unsaved-notice',
  '.form-error', '.error-text', '.input-error', '.image-error', '.scope-file-preview-error',
  '.drawer-state--error', '.abnormal-note-error',
  'p.error', 'div.error', 'p.success', 'div.success', 'p.warning', 'div.warning',
  '.daily-page .message', '.feedback', '.notice-box', '.notice[role="status"]',
  '.notice.danger', '.notice.success', '.notice.warning', '.status-banner', '.page-status',
].join(',');
const excluded = '[data-v2-inline-feedback],.async-page-state,.page-state,.save-conflicts,.n-form-item-feedback-wrapper,td,th,.answer-feedback,.notice-tags';
type Tone = 'success' | 'error' | 'warning' | 'info';
type Entry = { el:HTMLElement; signature:string; tone:Tone; dismissed:boolean; shown:boolean;
  persistent:boolean; order:number; timer:number; remaining:number; since:number; cleanup:()=>void };

export function installFeedbackMotion() {
  const supported = typeof HTMLElement.prototype.showPopover === 'function';
  const entries = new Map<HTMLElement,Entry>();
  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  let scheduled=0,stopped=false,order=0,offset=0;
  const counter=document.createElement('button');counter.type='button';counter.dataset.v2FeedbackCounter='';
  counter.setAttribute('data-v2-navigation','');counter.hidden=true;
  if(supported)counter.setAttribute('popover','manual');
  document.body.append(counter);
  function schedule(){if(!scheduled&&!stopped)scheduled=window.setTimeout(flush,50);}
  function setStyle(el:HTMLElement,key:string,value:string){if(el.style.getPropertyValue(key)!==value)el.style.setProperty(key,value);}
  function sourceVisible(el:HTMLElement){
    if(el.hidden||el.style.display==='none'||el.getAttribute('aria-hidden')==='true')return false;
    for(let parent:HTMLElement|null=el.parentElement;parent;parent=parent.parentElement){
      if(parent.hidden||parent.getAttribute('aria-hidden')==='true'||getComputedStyle(parent).display==='none')return false;
    }
    return true;
  }
  function toneOf(el:HTMLElement):Tone{
    const declared=el.dataset.v2FeedbackKind;
    if(declared&&['success','error','warning','info'].includes(declared))return declared as Tone;
    const classes=el.className+' '+[...el.querySelectorAll('.n-alert,.n-message')].map(n=>n.className).join(' ');
    if(/error|danger|failed/.test(classes))return 'error';
    if(/warning|\bwarn\b/.test(classes))return 'warning';
    if(/success/.test(classes))return 'success';
    return el.getAttribute('role')==='alert'&&!el.classList.contains('n-alert')?'error':'info';
  }
  function stopTimer(item:Entry){if(item.timer){clearTimeout(item.timer);item.timer=0;item.remaining=Math.max(0,item.remaining-(Date.now()-item.since));}}
  function startTimer(item:Entry){
    if(item.persistent||!item.shown||item.timer||document.hidden||item.el.matches(':hover')||item.el.contains(document.activeElement))return;
    item.since=Date.now();item.timer=window.setTimeout(()=>dismiss(item),Math.max(1,item.remaining));
  }
  function hide(item:Entry){
    stopTimer(item);item.shown=false;
    if(supported&&item.el.matches(':popover-open'))item.el.hidePopover();
    item.el.dataset.v2FeedbackVisible='false';
  }
  function dismiss(item:Entry){
    if(item.dismissed)return;
    const signature=item.signature;
    const affected=[...entries.values()].filter(e=>e.signature===signature);
    for(const e of affected){e.dismissed=true;stopTimer(e);}
    const finish=()=>{
      for(const e of affected){
        // A later result must not be hidden by the previous message's exit animation.
        if(e.signature!==signature||!e.dismissed)continue;
        hide(e);if(e.el.classList.contains('n-message'))e.el.querySelector<HTMLButtonElement>('.n-base-close')?.click();
      }
      schedule();
    };
    if(item.shown&&!reduced.matches){const a=item.el.animate([{opacity:1,transform:'translateY(0)'},{opacity:0,transform:'translateY(10px)'}],{duration:150,easing:'cubic-bezier(.7,0,.84,0)'});a.onfinish=finish;a.oncancel=finish;}
    else finish();
  }
  function decorate(item:Entry){
    const el=item.el;
    if(!el.querySelector(':scope > [data-v2-feedback-icon]')){
      const icon=document.createElement('span');icon.dataset.v2FeedbackIcon='';icon.setAttribute('aria-hidden','true');el.prepend(icon);
    }
    const icon=el.querySelector<HTMLElement>(':scope > [data-v2-feedback-icon]')!;
    if(icon.dataset.tone!==item.tone){icon.dataset.tone=item.tone;render(h({success:CheckCircle2,error:CircleAlert,warning:TriangleAlert,info:Info}[item.tone],{size:20}),icon);}
    if(!el.querySelector(':scope > [data-v2-feedback-close]')){
      const close=document.createElement('button');close.type='button';close.dataset.v2FeedbackClose='';close.setAttribute('data-v2-navigation','');
      close.setAttribute('aria-label','收起此提示');close.title='收起此提示';render(h(X,{size:16}),close);
      close.addEventListener('click',event=>{event.stopPropagation();dismiss(item);});el.append(close);
    }
  }
  function add(el:HTMLElement){
    if(entries.has(el)||el.hasAttribute('popover')||el.closest(excluded)||!supported&&!el.classList.contains('n-message'))return;
    if(el.parentElement?.closest(selector)||el.querySelector('form,table,input,select,textarea'))return;
    const item:Entry={el,signature:'',tone:'info',dismissed:false,shown:false,persistent:true,order:++order,timer:0,remaining:6000,since:0,cleanup:()=>{}};
    const pause=()=>stopTimer(item),resume=()=>startTimer(item);
    el.addEventListener('mouseenter',pause);el.addEventListener('mouseleave',resume);el.addEventListener('focusin',pause);el.addEventListener('focusout',resume);
    item.cleanup=()=>{stopTimer(item);el.removeEventListener('mouseenter',pause);el.removeEventListener('mouseleave',resume);el.removeEventListener('focusin',pause);el.removeEventListener('focusout',resume);};
    entries.set(el,item);el.dataset.v2Feedback='';
    if(supported)el.setAttribute('popover','manual');
    resize.observe(el);schedule();
  }
  function scan(node:Node){
    if(!(node instanceof HTMLElement)||node.closest('[data-v2-feedback-icon],[data-v2-feedback-close]'))return;
    if(node.matches(selector))add(node);
    for(const el of node.querySelectorAll<HTMLElement>(selector))add(el);
  }
  const resize=new ResizeObserver(schedule);
  const observer=new MutationObserver(records=>{
    let dirty=false;
    for(const record of records){
      const target=record.target instanceof HTMLElement?record.target:record.target.parentElement;
      if(!target||target.closest('[data-v2-feedback-icon],[data-v2-feedback-close],[data-v2-feedback-counter]'))continue;
      if(record.type==='childList'){for(const node of record.addedNodes)scan(node);if(record.removedNodes.length)dirty=true;}
      if(target.closest(selector))dirty=true;
      if(record.type==='attributes'){
        if(target.matches(selector))add(target);
        if([...entries.keys()].some(el=>target!==el&&target.contains(el)))dirty=true;
      }
    }
    if(dirty)schedule();
  });
  function flush(){
    scheduled=0;if(stopped)return;
    const unique=new Map<string,Entry>();
    for(const item of entries.values()){
      if(!item.el.isConnected){item.cleanup();resize.unobserve(item.el);entries.delete(item.el);continue;}
      const text=(item.el.textContent||'').replace(/\s+/g,' ').trim(),tone=toneOf(item.el),signature=tone+':'+text;
      if(!text){hide(item);continue;}
      if(signature!==item.signature){stopTimer(item);item.signature=signature;item.tone=tone;item.dismissed=false;item.remaining=tone==='success'?4000:6000;item.order=++order;}
      item.persistent=tone==='error'||tone==='warning'||item.el.classList.contains('n-message--loading-type')||!!item.el.querySelector('a,button:not([data-v2-feedback-close]):not(.n-base-close),progress,[role="progressbar"],.n-spin')||/正在|处理中|等待|上传中|提交中|同步中/.test(text);
      item.el.dataset.v2FeedbackTone=tone;decorate(item);
      if(!sourceVisible(item.el)){hide(item);continue;}
      if(item.dismissed&&!item.persistent){hide(item);continue;}
      const prior=unique.get(signature);if(!prior||prior.order<item.order)unique.set(signature,item);
    }
    const pending=[...unique.values()].filter(e=>!e.dismissed).sort((a,b)=>b.order-a.order);
    const recalled=[...unique.values()].filter(e=>e.dismissed&&e.persistent);
    if(offset>=pending.length)offset=0;
    const visible=pending.slice(offset,offset+3);
    let bottom=innerWidth<=760?82:20;
    const noticeFooter=document.querySelector<HTMLElement>('.v2-notice-editor-footer');
    if(noticeFooter?.getClientRects().length)bottom=Math.max(bottom,innerHeight-noticeFooter.getBoundingClientRect().top+12);
    const more=pending.length>visible.length||recalled.length>0;
    counter.hidden=!more;
    if(more){
      const owner=visible[0]?.el.closest('.n-modal,.n-dialog,.n-drawer')||document.body;
      if(counter.parentElement!==owner){if(supported&&counter.matches(':popover-open'))counter.hidePopover();owner.append(counter);}
      const text=pending.length>visible.length?`查看其它 ${pending.length-visible.length} 条提示`:`查看已收起的 ${recalled.length} 条提示`;
      if(counter.textContent!==text)counter.textContent=text;
      setStyle(counter,'--v2-feedback-bottom',bottom+'px');
      if(supported&&!counter.matches(':popover-open'))counter.showPopover();
      bottom+=40;
    }else if(supported&&counter.matches(':popover-open'))counter.hidePopover();
    const maxHeight=Math.max(90,Math.min(300,(innerHeight-bottom-70)/Math.max(1,visible.length)-12));
    for(const item of entries.values())if(!visible.includes(item))hide(item);
    for(const item of visible){
      setStyle(item.el,'--v2-feedback-bottom',bottom+'px');setStyle(item.el,'--v2-feedback-max-height',maxHeight+'px');
      if(!item.shown){
        item.el.dataset.v2FeedbackVisible='true';
        try{if(supported)item.el.showPopover();}catch{item.el.dataset.v2FeedbackVisible='false';continue;}
        item.shown=true;
        if(!reduced.matches)item.el.animate([{opacity:0,transform:'translateX(24px) scale(.98)'},{opacity:1,transform:'translateX(0) scale(1)'}],{duration:350,easing:'cubic-bezier(.16,1,.3,1)'});
      }
      bottom+=item.el.getBoundingClientRect().height+10;startTimer(item);
    }
  }
  counter.addEventListener('click',()=>{const hidden=[...entries.values()].filter(e=>e.dismissed&&e.persistent);if(hidden.length){for(const e of hidden){e.dismissed=false;e.order=++order;}offset=0;}else offset+=3;schedule();});
  function visibility(){for(const item of entries.values())document.hidden?stopTimer(item):startTimer(item);schedule();}
  observer.observe(document.body,{childList:true,subtree:true,characterData:true,attributes:true,attributeFilter:['class','style','hidden','aria-hidden','data-v2-feedback-kind']});
  scan(document.body);window.addEventListener('resize',schedule);document.addEventListener('visibilitychange',visibility);
  return ()=>{stopped=true;clearTimeout(scheduled);observer.disconnect();resize.disconnect();window.removeEventListener('resize',schedule);document.removeEventListener('visibilitychange',visibility);for(const item of entries.values()){item.cleanup();hide(item);item.el.removeAttribute('popover');delete item.el.dataset.v2Feedback;delete item.el.dataset.v2FeedbackVisible;delete item.el.dataset.v2FeedbackTone;item.el.style.removeProperty('--v2-feedback-bottom');item.el.style.removeProperty('--v2-feedback-max-height');item.el.querySelectorAll(':scope > [data-v2-feedback-icon],:scope > [data-v2-feedback-close]').forEach(el=>{render(null,el);el.remove();});}entries.clear();counter.remove();};
}
