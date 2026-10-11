import type { ColorId, ShapeId } from './vendor/bloub/engine/skins';
import type { StateId } from './vendor/bloub/engine/states';
export type BotMood = 'idle' | 'engaged' | 'thinking' | 'success' | 'error';

export type BotAppearance = {
  color: ColorId; shape: ShapeId; expression: string; state: StateId | 'cycle';
  size: number; animated: boolean; follow: boolean; snap_back: boolean;
};
export const DEFAULT_BOT: BotAppearance = {
  color: 'encre', shape: 'cercle', expression: 'neutre', state: 'idle', size: 56, animated: true, follow: false, snap_back: true,
};
export const BOT_COLORS = [
  ['encre', '黑色', '#0a0a0c'], ['brun', '棕色', '#8b5e3c'], ['rouge', '红色', '#e8483f'],
  ['orange', '橙色', '#f08a24'], ['ambre', '黄色', '#f0b429'], ['vert', '绿色', '#3ecf8e'],
  ['turquoise', '青色', '#2fbfa0'], ['bleu', '蓝色', '#3b93f0'], ['violet', '紫色', '#8b5cf6'],
  ['rose', '粉色', '#e152b0'], ['gris', '灰色', '#a3a3a3'], ['creme', '米白', '#f1efe9'],
] as const;
export const BOT_SHAPES = [
  ['cercle', '圆形'], ['galet', '鹅卵石'], ['squircle', '圆角方形'], ['capsule', '胶囊'],
  ['triangle', '三角形'], ['hexagone', '六边形'], ['nuage', '云朵'], ['goutte', '水滴'],
  ['swirl-pile', '卷尖便便'], ['heart', '爱心'], ['star', '星星'],
  ['flower', '花朵'], ['diamond', '菱形'], ['shield', '盾牌'],
] as const;

export function normalizeBot(value: unknown): BotAppearance {
  const result = { ...DEFAULT_BOT };
  if (!value || typeof value !== 'object') return result;
  const source = value as Record<string, unknown>;
  const groups = { color: BOT_COLORS, shape: BOT_SHAPES };
  for (const key of Object.keys(groups) as (keyof typeof groups)[]) {
    if (groups[key].some(option => option[0] === source[key])) Object.assign(result, { [key]: source[key] });
  }
  if (Number.isInteger(source.size) && Number(source.size) >= 40 && Number(source.size) <= 200) result.size = Number(source.size);
  // Previous appearance payloads still round-trip, but behavior is automatic now.
  if (typeof source.snap_back === 'boolean') result.snap_back = source.snap_back;
  return result;
}
