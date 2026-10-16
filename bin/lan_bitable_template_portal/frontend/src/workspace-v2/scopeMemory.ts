import type {Dict} from './client';
export function scopeOwner(user:Dict){return String(user.union_id||user.open_id||'');}
export function scopeFamily(module:string){return ['all','maintenance','change','repair','polling','adjust','power','notices'].includes(module)?'notices':module||'workspace';}
export function moduleScope(module:string,options:{value:string}[],remembered:string,preferred=''){
 const supported=options.filter(s=>!['cabinet','critical_guard','drills'].includes(module)||!['H','110'].includes(s.value));
 const choices=supported.length?supported:options;
 return [remembered,preferred,'ALL',choices[0]?.value].find(value=>!!value&&choices.some(s=>s.value===value))||'';
}
export function readScopeMemory(user:Dict):Record<string,string>{
 if(!scopeOwner(user))return {};
 try{const value=JSON.parse(localStorage.getItem('clipflow:module-scopes:v1:'+scopeOwner(user))||'{}');return Object.fromEntries(Object.entries(value).filter(([key,value])=>key.length<60&&typeof value==='string'&&value.length<20)) as Record<string,string>;}catch{return {};}
}
export function saveScopeMemory(user:Dict,value:Record<string,string>){if(scopeOwner(user))try{localStorage.setItem('clipflow:module-scopes:v1:'+scopeOwner(user),JSON.stringify(value));}catch{/* A restricted browser still keeps the current session filters. */}}
