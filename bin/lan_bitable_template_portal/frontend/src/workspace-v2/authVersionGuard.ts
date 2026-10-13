import { onBeforeUnmount,onMounted,ref } from 'vue';

export function useAuthVersionGuard(){
  const busy=ref(false),confirming=ref(false);
  let observer:MutationObserver|undefined,form:HTMLFormElement|null=null,proceed:(()=>void)|undefined;
  function read(){
    const current=document.querySelector<HTMLFormElement>('form.pwd-form');
    if(form!==current){observer?.disconnect();form=current;if(form){observer=new MutationObserver(read);observer.observe(form,{subtree:true,childList:true,attributes:true,attributeFilter:['disabled']});}}
    busy.value=!!form?.querySelector('button[type=submit] .spin');
    return form;
  }
  function request(action:()=>void){
    const current=read();if(busy.value)return;
    if(current&&Array.from(current.querySelectorAll<HTMLInputElement>('input')).some(input=>input.value)){
      proceed=action;confirming.value=true;
    }else action();
  }
  function confirm(){read();if(busy.value)return false;const action=proceed;proceed=undefined;confirming.value=false;action?.();}
  onMounted(()=>{read();document.addEventListener('submit',read,true);document.addEventListener('input',read,true);});
  onBeforeUnmount(()=>{observer?.disconnect();proceed=undefined;document.removeEventListener('submit',read,true);document.removeEventListener('input',read,true);});
  return {busy,confirming,request,confirm};
}
