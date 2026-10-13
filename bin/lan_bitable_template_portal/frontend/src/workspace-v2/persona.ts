import type { Dict } from './client';

export type Persona = 'global' | 'duty' | 'engineer' | 'supervisor' | 'personal';
export function personaFor(user: Dict): Persona {
  if (['L0', 'L1'].includes(user.level) || user.role === 'admin') return 'global';
  if (user.category === 'duty') return 'duty';
  if (user.level === 'L3' || user.category === 'supervisor') return 'supervisor';
  if (user.level === 'L2' || user.category === 'engineer') return 'engineer';
  return 'personal';
}
export const personaTitles: Record<Persona, string> = {
  global: '个人工作台', duty: '值班工作台', engineer: '工程工作台', supervisor: '主管工作台', personal: '我的工作台',
};
