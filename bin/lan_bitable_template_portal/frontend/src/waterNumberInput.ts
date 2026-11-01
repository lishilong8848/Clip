/** Normalize user-entered readings without guessing ambiguous decimal separators. */
export function normalizeWaterNumberText(value: unknown): string {
  const text = String(value ?? '').normalize('NFKC').trim();
  return /^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d*)?(?:[eE][+-]?\d+)?$/.test(text)
    ? text.replace(/,/g, '')
    : text;
}

export function parseWaterNumberText(value: unknown): number | null {
  const text = normalizeWaterNumberText(value);
  if (!text) return null;
  if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(text)) return Number.NaN;
  const result = Number(text);
  return Number.isFinite(result) ? result : Number.NaN;
}
