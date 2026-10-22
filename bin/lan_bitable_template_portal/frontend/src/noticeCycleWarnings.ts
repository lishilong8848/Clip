export type MaintenanceCycleRule = { value: string; aliases: string[]; interval?: [string, number] };
export type NoticeCycleWarnings = Partial<Record<'content' | 'reason', string>>;

export function maintenanceCycleRules(raw: unknown): MaintenanceCycleRule[] {
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw); } catch { return []; }
  }
  if (!Array.isArray(raw)) return [];
  return raw.flatMap(rule => {
    if (!rule || typeof rule.value !== 'string' || !rule.value.trim() || !Array.isArray(rule.aliases)) return [];
    const value = rule.value.trim();
    const interval = Array.isArray(rule.interval) && ['month','day','unplanned'].includes(rule.interval[0]) && Number.isFinite(rule.interval[1]) && rule.interval[1]>0 ? rule.interval as [string,number] : undefined;
    return [{ value, aliases: [...new Set([...rule.aliases.filter((alias: unknown): alias is string => typeof alias === 'string' && !!alias.trim()).map((alias: string) => alias.trim()), value])], ...(interval?{interval}:{}) }];
  });
}

export function isMaintenanceNotice(values: Record<string, unknown>): boolean {
  return values.work_type === 'maintenance' && (values.notice_type == null || values.notice_type === '' || values.notice_type === '维保通告');
}

const durationPattern = String.raw`(?<![\d零〇一二两三四五六七八九十百千年月日号])(?:每隔|每|按|隔)?\s*([0-9]+(?:\.[0-9]+)?|[零〇一二两三四五六七八九十百千]+|半)\s*(?:个\s*)?(季度|年度|星期|个月|月|季|年|周|天|日)(?:\s*(?:一次|1次|/次))?(?![\d])`;
const datePattern = /(?<!\d)(?:\d{4}\s*年(?:\s*\d{1,2}\s*月)?(?:\s*\d{1,2}\s*[日号])?|\d{1,2}\s*月\s*\d{1,2}\s*[日号])/g;
function duration(term: string): string | undefined {
  const match=new RegExp('^(?:'+durationPattern+')$').exec(term);if(!match)return;
  const [,quantity,unit]=match;
  if(unit==='月'&&!term.includes('个')&&!/^(每|按|隔)/.test(term)&&!/(一次|1次|\/次)$/.test(term))return;
  let count=quantity==='半'?0.5:Number(quantity);
  if(Number.isNaN(count)){
    count=0;let digit=0;
    const digits:Record<string,number>={零:0,〇:0,一:1,二:2,两:2,三:3,四:4,五:5,六:6,七:7,八:8,九:9};
    for(const char of quantity)if(char in digits)digit=digits[char];else{count+=(digit||1)*({十:10,百:100,千:1000}[char as '十'|'百'|'千']);digit=0;}
    count+=digit;
  }
  if(count<=0||['年','年度'].includes(unit)&&count>=1000)return;
  const factor:Record<string,number>={季度:3,季:3,年:12,年度:12,星期:7,周:7};
  return (['星期','周','天','日'].includes(unit)?'day':'month')+':'+count*(factor[unit]||1);
}

export function noticeCycleWarnings(values: Record<string, unknown>, rawRules: unknown): NoticeCycleWarnings {
  if (!isMaintenanceNotice(values) || typeof values.maintenance_cycle !== 'string') return {};
  const rules = maintenanceCycleRules(rawRules), owners = new Map<string, Set<string>>();
  if(!rules.length)return {};
  for (const rule of rules) for (const alias of rule.aliases) {
    if (!owners.has(alias)) owners.set(alias, new Set());
    owners.get(alias)!.add(rule.value);
  }
  const selected = owners.get(values.maintenance_cycle.trim());
  if (selected && selected.size !== 1) return {};
  const intervals=new Map(rules.map(rule=>[rule.value,rule.interval?.join(':')
    ||rule.aliases.map(alias=>duration(alias.replace(/^每(?=月|季度?|年度?|星期|周|天|日)/,'每1'))).find(Boolean)||rule.value]));
  const current = selected ? [...selected][0] : values.maintenance_cycle.trim();
  const selectedInterval=selected?intervals.get(current):duration(current);
  if(!selectedInterval)return {};
  // One longest-first scan consumes 半年度/每两年 before their shorter 年度/年 aliases.
  const pattern = [...owners.keys()].sort((a, b) => b.length - a.length).map(alias => alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')+'|'+durationPattern;
  const warnings: NoticeCycleWarnings = {};
  for (const [key, label] of [['content', '内容'], ['reason', '原因']] as const) {
    if (typeof values[key] !== 'string') continue;
    const conflicts = new Set<string>();
    for (const match of values[key].replace(datePattern,' ').matchAll(new RegExp(pattern, 'g'))) {
      const term=match[0].trim(),cycles=owners.get(term);
      const interval=cycles?.size===1?intervals.get([...cycles][0]):cycles?undefined:duration(term);
      if (interval && interval!==selectedInterval) conflicts.add(term);
    }
    if (conflicts.size) warnings[key] = `已选维保周期“${current}”，【${label}】含不一致的周期“${[...conflicts].join('、')}”，请修改一致后再发送开始、更新或结束。`;
  }
  return warnings;
}
