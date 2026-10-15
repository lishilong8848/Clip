import { getCurrentInstance,inject,type InjectionKey,type Ref } from 'vue';
import type { NavigateOptions } from './navigation';
import {workspaceHistory,workspaceLocation} from './browser';
export type EmbeddedContext={id:string;route:Ref<URL>;filterTarget:Ref<HTMLElement|null>;go:(url:string|URL,options?:NavigateOptions)=>boolean;back:()=>void};
export const embeddedContextKey:InjectionKey<EmbeddedContext>=Symbol('embedded-module');
export function currentEmbedded(){return getCurrentInstance()?inject(embeddedContextKey,null):null;}
export function useEmbeddedNavigation(fallback:(url:string|URL,options?:NavigateOptions)=>void){const embedded=currentEmbedded();return (url:string|URL,options?:NavigateOptions)=>{if(!embedded?.go(url,options))fallback(url,options);};}
export function useEmbeddedRoute(fallback:(url?:string)=>URL){const embedded=currentEmbedded();return (url?:string)=>url?fallback(url):embedded?.route.value||fallback();}
export function useEmbeddedBack(fallback:(url?:string,hard?:boolean)=>void){const embedded=currentEmbedded();return (url?:string,hard?:boolean)=>embedded?embedded.back():fallback(url,hard);}
export function useEmbeddedBrowser(){
  const embedded=currentEmbedded();if(!embedded)return {location:workspaceLocation,history:workspaceHistory};
  function go(url:string|URL,replace=false){if(!embedded!.go(url,{replace}))workspaceLocation.assign(url);}
  const location=new Proxy(workspaceLocation,{
    get(target,key){if(['href','pathname','search','hash'].includes(String(key)))return embedded.route.value[key as 'href'|'pathname'|'search'|'hash'];if(key==='assign')return (url:string|URL)=>go(url);if(key==='replace')return (url:string|URL)=>go(url,true);return Reflect.get(target,key);},
    set(target,key,value){if(key==='href'){go(value);return true;}if(key==='hash'){const url=new URL(embedded.route.value);url.hash=value;go(url);return true;}return Reflect.set(target,key,value);},
  });
  const history=new Proxy(workspaceHistory,{get(target,key){if(key==='replaceState'||key==='pushState')return (_data:unknown,_unused:string,url?:string|URL|null)=>{if(url!=null)go(url,key==='replaceState');};if(key==='back')return embedded.back;return Reflect.get(target,key);}});
  return {location,history};
}
