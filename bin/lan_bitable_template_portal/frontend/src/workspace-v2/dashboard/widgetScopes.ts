import type { Widget } from './layout';

export type ScopeOption = { value:string; label:string };
export function widgetScopeOptions(item:Pick<Widget,'kind'|'module'>,options:ScopeOption[]):ScopeOption[]{
  if(['pre_shift','learning','shortcuts'].includes(item.kind))return [];
  const module=item.kind==='module'?item.module:item.kind;
  if(['knowledge','links','signatures','accounts','life_guide','handover','learning'].includes(module))return [];
  const supported=['cabinet','drills','critical_guard'].includes(module)?'ABCDE'.split(''):module==='water'?'ABCDEH'.split(''):['110',...'ABCDEH'];
  return options.filter(option=>supported.includes(option.value));
}
export function selectedWidgetScopes(scope:string,current:string,options:ScopeOption[]):string[]{
  const value=scope==='current'?current:scope;
  if(['ALL','CAMPUS'].includes(value))return options.map(option=>option.value);
  const selected=value.split(',').filter(code=>options.some(option=>option.value===code));
  return [...new Set(selected.length?selected:options.slice(0,1).map(option=>option.value))];
}
export function sanitizeWidgetScope(scope:unknown,allowed:string[]):string{
  if(typeof scope!=='string'||scope==='current')return 'current';
  if(scope==='ALL'||scope==='CAMPUS')return allowed.includes(scope)?scope:'current';
  const values=[...new Set(scope.split(',').filter(value=>allowed.includes(value)&&['110',...'ABCDEH'].includes(value)))];
  return values.length?values.join(','):'current';
}
