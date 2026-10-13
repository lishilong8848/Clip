const states=new WeakMap<HTMLElement,{paused:boolean;touchY:number;frame:number;lastTop:number}>();
function state(element:HTMLElement){
  let saved=states.get(element);if(saved)return saved;
  saved={paused:false,touchY:0,frame:0,lastTop:element.scrollTop};states.set(element,saved);const value=saved;
  const pause=()=>{value.paused=true;value.lastTop=element.scrollTop;cancelAnimationFrame(value.frame);value.frame=0;element.scrollTo({top:element.scrollTop,behavior:'instant'});};
  element.addEventListener('wheel',event=>{if(event.deltaY<0)pause();},{passive:true});
  element.addEventListener('touchstart',event=>{value.touchY=event.touches[0]?.clientY||0;},{passive:true});
  element.addEventListener('touchmove',event=>{const y=event.touches[0]?.clientY||0;if(y>value.touchY)pause();value.touchY=y;},{passive:true});
  element.addEventListener('keydown',event=>{if(['ArrowUp','PageUp','Home'].includes(event.key))pause();});
  element.addEventListener('pointerdown',event=>{if(event.clientX>=element.getBoundingClientRect().right-16)pause();},{passive:true});
  element.addEventListener('scroll',()=>{if(element.scrollTop>value.lastTop&&element.scrollHeight-element.scrollTop-element.clientHeight<8)value.paused=false;value.lastTop=element.scrollTop;},{passive:true});
  return value;
}
export function shouldFollow(element:HTMLElement|null):boolean{return !element||!state(element).paused;}
export function followBottom(element:HTMLElement){
  const value=state(element);if(value.paused||value.frame)return;
  const reduced=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  let previous=performance.now();
  function step(now:number){
    value.frame=0;if(value.paused||!element.isConnected)return;
    const bottom=Math.max(0,element.scrollHeight-element.clientHeight),gap=bottom-element.scrollTop;
    const fraction=reduced?1:1-Math.exp(-Math.min(64,now-previous)/55);previous=now;
    element.scrollTo({top:Math.abs(gap)<2?bottom:element.scrollTop+gap*fraction,behavior:'instant'});
    if(Math.abs(bottom-element.scrollTop)>1)value.frame=requestAnimationFrame(step);
  }
  value.frame=requestAnimationFrame(step);
}
