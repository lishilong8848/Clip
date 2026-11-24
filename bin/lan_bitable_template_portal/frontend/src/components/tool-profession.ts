export const toolProfession = (value?: string) => value?.trim() || '未填写';

export function toolProfessionOptions(rows: { profession?: string }[]) {
  const values = [...new Set(rows.map(row => toolProfession(row.profession)))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  return [{ value: '', label: '全部专业' }, ...values.map(value => ({ value, label: value === '未填写' ? '专业未填写' : value }))];
}
