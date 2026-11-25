import { inject } from 'vue';
import { pageContext } from './pageLayout';
import type { Dict } from './client';

const shared = new Set(['','-','—','无','未填写','未分类','未设置','不限','通用','全部','全部专业']);
const aliases: Record<string,string> = {'强电':'电气','供配电':'电气','空调':'暖通','暖通空调':'暖通'};
export function professionCodes(value: unknown): string[] {
  if (Array.isArray(value)) return value.flatMap(professionCodes);
  if (typeof value !== 'string') return [];
  return value.split(/[,，、;；/／|＋+&\n]+/).map(part=>part.trim()).filter(part=>!shared.has(part))
    .map(part=>part.replace(/专业$/,'').trim()).map(part=>aliases[part]||part).filter(part=>!shared.has(part));
}
export const isProfessionEngineer = (user?: Dict) => !!user && user.role!=='admin' && !['L0','L1'].includes(user.level)
  && (user.category==='engineer'||!user.category&&user.level==='L2');
export const defaultProfessionView = (user?: Dict) => isProfessionEngineer(user)?'own':'';
export function professionMatches(view: string, value: unknown, user?: Dict): boolean {
  if (!view||view==='all') return true;
  if (view==='未填写') return !professionCodes(value).length;
  const match = new Set(professionCodes(view==='own'?user?.specialties:view));
  return professionCodes(value).some(code=>match.has(code));
}
export function professionChoices(user?: Dict, available: string[] = ['电气','暖通','弱电','消防']) {
  const own = [...new Set(professionCodes(user?.specialties))];
  return [
    ...(isProfessionEngineer(user)?[{value:'own',label:own.length?'本专业 · '+own.join('、'):'本专业'}]:[]),
    {value:'',label:'全部专业'},
    ...[...new Set([...available,...own])].filter(Boolean).map(value=>({value,label:value})),
  ];
}
export const useProfessionUser = () => inject(pageContext,null)?.user;
