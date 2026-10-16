import { computed,onBeforeUnmount,ref,watch } from 'vue';
import { requestJson,type Dict } from './client';

type Options<T> = {
  user:()=>Dict; page:()=>string; legacyKey:()=>string; enabled?:()=>boolean;
  locked:()=>boolean; normalize:(value:unknown)=>T; apply:(value:T)=>void;
};
type RecordValue<T>={revision:number;value:T|null};

// Account data is authoritative; local storage is only a cache and upgrade input.
export function useLayoutSync<T>(options:Options<T>){
  const media=window.matchMedia('(max-width:760px)'),device=ref(media.matches?'mobile':'desktop');
  const loading=ref(false),saving=ref(false),error=ref(''),ready=ref(false);
  const owner=computed(()=>String(options.user().open_id||''));
  const enabled=computed(()=>!!owner.value&&!['guest','temporary'].includes(options.user().role)&&options.enabled?.()!==false);
  const context=computed(()=>JSON.stringify([owner.value,device.value,options.page(),enabled.value]));
  let revision:number|null=null,generation=0,disposed=false,reader:AbortController|undefined;
  const url=()=>'/api/workspace-layouts/'+device.value+'/'+encodeURIComponent(options.page());
  const cacheKey=()=> 'clipflow:account-layout:v1:'+owner.value+':'+device.value+':'+options.page();
  function cache(value:T,revision:number){try{localStorage.setItem(cacheKey(),JSON.stringify({value,revision}));}catch{/* Server saving must still work when browser storage is disabled. */}}
  function local():{value:T;candidate:T|null}{
    try{
      const raw=localStorage.getItem(cacheKey());
      // Legacy layouts have no viewport dimension; migrate them to desktop only.
      const legacy=!raw&&device.value==='desktop'?localStorage.getItem(options.legacyKey()):null;
      const cached=raw?JSON.parse(raw):null,parsed=cached?cached.value:legacy?JSON.parse(legacy):null;
      const value=options.normalize(parsed);
      return {value,candidate:parsed&&typeof parsed==='object'&&(legacy||cached?.revision>0)?value:null};
    }catch{return {value:options.normalize(null),candidate:null};}
  }
  function checked(value:Dict):RecordValue<T>{
    if(!Number.isSafeInteger(value.revision)||value.revision<0||!Object.prototype.hasOwnProperty.call(value,'value')||
      (value.revision===0?value.value!==null:!value.value||typeof value.value!=='object'||Array.isArray(value.value)))throw new Error('账号布局响应不完整，未覆盖原布局。');
    return value as RecordValue<T>;
  }
  async function reload(){
    const run=++generation,key=context.value;reader?.abort();revision=null;ready.value=false;error.value='';
    if(!enabled.value){loading.value=false;options.apply(options.normalize(null));return;}
    const fallback=local();options.apply(fallback.value);
    const controller=reader=new AbortController();loading.value=true;
    const current=()=>!disposed&&run===generation&&key===context.value&&!controller.signal.aborted;
    try{
      let record=checked(await requestJson(url(),{signal:controller.signal,cache:'no-store',timeoutMs:10000}));
      if(!current())return;
      if(record.revision===0&&fallback.candidate){
        try{
          record=checked(await requestJson(url(),{method:'PUT',signal:controller.signal,body:JSON.stringify({revision:0,value:fallback.candidate}),timeoutMs:10000}));
        }catch(e:any){
          if(e.status!==409)throw e;
          // Another client migrated first. Read its layout instead of replacing it.
          record=checked(await requestJson(url(),{signal:controller.signal,cache:'no-store',timeoutMs:10000}));
        }
      }
      if(!current())return;
      const value=record.value===null?fallback.value:options.normalize(record.value);
      revision=record.revision;ready.value=true;options.apply(value);cache(value,record.revision);
    }catch(e){if(current())error.value=(e instanceof Error?e.message:'账号布局读取失败。')+' 当前显示本机缓存或默认布局，重新读取成功后可编辑。';}
    finally{if(current())loading.value=false;}
  }
  async function save(value:T):Promise<boolean>{
    if(saving.value||loading.value||!ready.value||revision===null||!enabled.value)throw new Error('请先完成账号布局读取，再保存。');
    const run=generation,key=context.value,expected=revision,normalized=options.normalize(value);saving.value=true;error.value='';
    try{
      const record=checked(await requestJson(url(),{method:'PUT',body:JSON.stringify({revision:expected,value:normalized}),timeoutMs:15000}));
      if(disposed||run!==generation||key!==context.value)return false;
      revision=record.revision;cache(normalized,record.revision);return true;
    }catch(e){if(!disposed&&run===generation){error.value=e instanceof Error?e.message:'账号布局保存失败，调整已保留。';}throw e;}
    finally{saving.value=false;}
  }
  function resize(){if(!options.locked()&&!saving.value)device.value=media.matches?'mobile':'desktop';}
  media.addEventListener('change',resize);
  watch(()=>options.locked()||saving.value,locked=>{if(!locked)resize();});
  watch(context,()=>void reload(),{immediate:true});
  onBeforeUnmount(()=>{disposed=true;generation++;reader?.abort();media.removeEventListener('change',resize);});
  return {loading,saving,error,ready,device,reload,save};
}
