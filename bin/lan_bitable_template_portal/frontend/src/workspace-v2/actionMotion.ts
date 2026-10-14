import { h, render } from 'vue';
import { Check, LoaderCircle, X } from 'lucide-vue-next';

type Action = { button:HTMLButtonElement; feedback:HTMLElement; started:number; pending:number; observed:boolean; succeeded:boolean; failed:boolean; waiting:boolean; timer:number; finished:boolean; presentation?:boolean; minimum:number };
const actions=new Map<HTMLButtonElement,Action>(),jobs=new Map<string,Action>();
let current:Action|undefined;
const reduced=()=>window.matchMedia('(prefers-reduced-motion: reduce)').matches;
function clear(action:Action){
  window.clearTimeout(action.timer);actions.delete(action.button);action.finished=true;
  for(const [key,value] of jobs)if(value===action)jobs.delete(key);
  render(null,action.feedback);action.feedback.remove();delete action.button.dataset.v2Action;
  delete action.button.dataset.v2ActionPosition;
  action.button.style.removeProperty('--v2-action-size');
  if(current===action)current=undefined;
}
function settle(action:Action){
  if(action.finished)return;
  if(!action.button.isConnected){clear(action);return;}
  // Vue can replace a text-only button's children when its loading label changes.
  if(action.feedback.parentElement!==action.button)action.button.append(action.feedback);
  const remaining=action.failed?0:action.minimum-(performance.now()-action.started);
  if(remaining>0||action.pending||action.waiting||action.button.getAttribute('aria-busy')==='true'){
    action.timer=window.setTimeout(()=>settle(action),Math.max(100,remaining));return;
  }
  if(!action.succeeded&&!action.failed){clear(action);return;}
  action.button.dataset.v2Action=action.failed?'error':'success';
  action.feedback.setAttribute('aria-label',action.failed?'操作未完成':'操作完成');
  render(h(action.failed?X:Check,{size:22,strokeWidth:2.5}),action.feedback);
  action.timer=window.setTimeout(()=>{action.button.dataset.v2Action='restore';action.timer=window.setTimeout(()=>clear(action),300);},1000);
}
function result(action:Action,value:any){
  if(action.finished)return;
  if(value?.error||value?.ok===false||['failed','error','cancelled'].includes(value?.phase||value?.status))action.failed=true;
  const phase=value?.phase||value?.status;
  const id=value?.job_id;
  if(id&&!['success','succeeded','completed','failed','error','cancelled'].includes(phase)){
    jobs.set(String(id),action);action.waiting=true;
  }else{action.succeeded=!action.failed&&value?.requires_password_change!==true;if(id&&jobs.has(String(id))){action.waiting=false;jobs.delete(String(id));}}
}
function observe(action:Action){action.observed=true;action.pending++;}
function completeRequest(action:Action){action.pending--;if(action.failed&&action.button.dataset.v2Action){window.clearTimeout(action.timer);settle(action);}}
export function trackButtonRequest<T>(promise:Promise<T>):Promise<T>{
  const owner=current?.presentation?undefined:current;if(owner)observe(owner);
  return promise.then(data=>{const task=jobs.get(String((data as any)?.job_id||''));if(task)result(task,data);if(owner)result(owner,data);return data;},error=>{if(owner)owner.failed=true;throw error;}).finally(()=>{if(owner)completeRequest(owner);});
}
export function trackButtonPresentation(promise:Promise<unknown>):void{
  const owner=current;if(!owner)return;
  // Opening a drawer completes at render; its data reads keep their own loading feedback.
  owner.presentation=true;owner.minimum=300;observe(owner);
  void promise.then(()=>result(owner,{ok:true}),()=>{owner.failed=true;}).finally(()=>completeRequest(owner));
}
export function installActionMotion(doc:Document=document):()=>void{
  const changed=(event:Event)=>{const input=event.target as HTMLInputElement;if(input?.type==='checkbox'&&!input.disabled)input.dataset.v2Toggled='true';};
  const view=doc.defaultView;
  const nativeRequest=doc!==document&&view?view.fetch:undefined;
  if(nativeRequest&&view){
    // Observe the existing iframe transport; request URLs, bodies and responses are unchanged.
    view.fetch=function(...args:Parameters<typeof fetch>){
      const owner=current;if(owner)observe(owner);
      return nativeRequest.apply(view,args).then(response=>{
        if(owner&&!response.ok)owner.failed=true;
        if(response.headers.get('content-type')?.includes('application/json')&&(owner||jobs.size)){
          void response.clone().json().then(payload=>{const data=payload?.data||payload;const task=jobs.get(String(data?.job_id||''));if(task)result(task,data);if(owner)result(owner,payload?.ok===false?payload:data);}).catch(()=>{if(owner)owner.failed=true;}).finally(()=>{if(owner)completeRequest(owner);});
        }else if(owner){owner.succeeded=response.ok;completeRequest(owner);}
        return response;
      },error=>{if(owner){owner.failed=true;completeRequest(owner);}throw error;});
    };
  }
  const click=(event:MouseEvent)=>{
    const check=(event.target as Element)?.closest?.('[role=checkbox]') as HTMLElement|null;
    if(check&&check.getAttribute('aria-disabled')!=='true')check.dataset.v2Toggled='true';
    const button=(event.target as Element)?.closest?.('button') as HTMLButtonElement|null;
    if(!button||button.disabled||button.getAttribute('aria-disabled')==='true'||button.closest('[role=checkbox],[role=switch],[data-v2-navigation],.login-tabs,.pwd-form')||reduced())return;
    if(button.type==='submit'&&button.form&&!button.form.noValidate&&!button.form.checkValidity())return;
    if(actions.has(button)){event.preventDefault();event.stopImmediatePropagation();return;}
    const feedback=doc.createElement('span');feedback.className='v2-action-feedback';feedback.setAttribute('role','status');feedback.setAttribute('aria-label','处理中');
    const action:Action={button,feedback,started:performance.now(),pending:0,observed:false,succeeded:false,failed:false,waiting:false,timer:0,finished:false,minimum:1200};
    actions.set(button,action);current=action;
    if(getComputedStyle(button).position==='static')button.dataset.v2ActionPosition='relative';
    button.style.setProperty('--v2-action-size',Math.max(1,Math.min(48,button.clientHeight,button.clientWidth))+'px');
    // Defer decoration until the original click handlers have inspected the target.
    window.setTimeout(()=>{
      if(!button.isConnected||!action.observed){clear(action);return;}
      button.dataset.v2Action='preparing';button.append(feedback);render(h(LoaderCircle,{size:21}),feedback);
      // Establish the full-width clip before transitioning, instead of jumping from clip-path:none.
      void button.getBoundingClientRect();
      window.requestAnimationFrame(()=>{if(!action.finished){button.dataset.v2Action='pending';settle(action);}});
    },0);
    window.setTimeout(()=>{if(current===action)current=undefined;},0);
  };
  doc.addEventListener('click',click,true);
  doc.addEventListener('change',changed);
  return()=>{doc.removeEventListener('click',click,true);doc.removeEventListener('change',changed);try{if(nativeRequest&&view?.document===doc)view.fetch=nativeRequest;}catch{/* The frame may have navigated outside this origin. */}for(const action of actions.values())if(action.button.ownerDocument===doc)clear(action);};
}
