type Option = { value?: unknown; label?: unknown; disabled?: boolean };

export function labelledOptions(options: Option[] = []): { value: unknown; label: string; disabled?: boolean }[] {
  const names = options.map(option => String(option.label ?? option.value));
  const counts = new Map<string, number>();
  names.forEach(name => counts.set(name, (counts.get(name) || 0) + 1));
  return options.map((option, index) => ({ ...option, value: option.value, label: (counts.get(names[index]) || 0) > 1
    ? `${names[index]}（候选 ${index + 1}）` : names[index] }));
}

export function selectedLabel(options: Option[], value: unknown): string {
  if (value == null || value === '') return '';
  return labelledOptions(options).find(option => String(option.value) === String(value))?.label || '已选择';
}

export function selectedValue(options: Option[], label: string): unknown {
  return label === '' ? '' : labelledOptions(options).find(option => option.label === label)?.value;
}

export function smallSingleChoice(options: Option[] = []): boolean {
  return options.length > 0 && options.length <= 4 && !options.some(option => option.disabled);
}
