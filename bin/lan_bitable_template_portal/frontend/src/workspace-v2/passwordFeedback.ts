import { nextTick, onBeforeUnmount, watch, type Ref } from 'vue';
export function passwordStrength(value:string){
  if(!value)return 0;
  const kinds=[/[a-z]/,/[A-Z]/,/\d/,/[^a-zA-Z0-9]/].filter(pattern=>pattern.test(value)).length;
  return value.length<8?1:Math.min(value.length>=12?4:3,kinds);
}
export function usePasswordFeedback(error:Ref<string>,passwords:Ref<string>[],busy?:Ref<boolean>){
  let timer=0,target:HTMLInputElement|null=null,disposed=false;
  let submittedFrom:Element|null=null,userMoved=false,tracking=false;
  if(busy)watch(busy,value=>{if(value){submittedFrom=document.activeElement;userMoved=false;tracking=true;}},{flush:'sync'});
  function focusChanged(event:FocusEvent){if(tracking&&event.target!==submittedFrom&&event.target!==target&&event.target!==document.body)userMoved=true;}
  function pointerChanged(event:PointerEvent){if(tracking&&event.target instanceof Element&&!event.target.closest('.pwd-form button[type=submit]')&&event.target!==target)userMoved=true;}
  document.addEventListener('focusin',focusChanged);document.addEventListener('pointerdown',pointerChanged,true);
  function clear(){window.clearTimeout(timer);target?.classList.remove('v2-password-invalid','v2-password-shake');target?.removeAttribute('aria-invalid');target=null;}
  watch(error,async message=>{
    clear();
    if(!/(?:密码|credentials).*(?:错误|不正确|无效)|(?:invalid|incorrect).*password/i.test(message))return;
    await nextTick();if(disposed||message!==error.value)return;
    target=document.querySelector<HTMLInputElement>('#personnel-password, #personnel-current-password');
    if(!target)return;
    target.classList.add('v2-password-invalid','v2-password-shake');target.setAttribute('aria-invalid','true');
    const reduced=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if(!reduced&&window.matchMedia('(pointer:coarse)').matches)try{navigator.vibrate?.(20);}catch{/* Vibration is optional. */}
    timer=window.setTimeout(()=>{if(target){target.classList.remove('v2-password-shake');if(!userMoved&&(tracking||document.activeElement===target||document.activeElement===document.body||document.activeElement?.tagName==='BUTTON')){target.focus({preventScroll:true});target.select();}}tracking=false;},reduced?0:400);
  });
  watch(passwords,()=>{if(!busy?.value){clear();tracking=false;userMoved=false;if(error.value)error.value='';}});
  onBeforeUnmount(()=>{disposed=true;clear();document.removeEventListener('focusin',focusChanged);document.removeEventListener('pointerdown',pointerChanged,true);});
}
