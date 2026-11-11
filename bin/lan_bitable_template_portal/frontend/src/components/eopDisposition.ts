export interface EopDispositionGroup {
  heading?: string;
  rows: { marker?: string; text: string; depth: number }[];
}

export function groupEopDisposition(source: string): EopDispositionGroup[] {
  const groups: EopDispositionGroup[] = [];
  let group: EopDispositionGroup = { rows: [] };
  let lastStep: number | undefined;

  const startGroup = (heading?: string) => {
    if (group.heading || group.rows.length) groups.push(group);
    group = { heading, rows: [] };
    lastStep = undefined;
  };

  const lines = source.split(/\r\n|\r|\n/).flatMap(line =>
    /^\s*\d{1,3}(?:[、)）]|\.(?=\s))/.test(line)
      ? line.split(/[^\S\r\n]+(?=\d{1,3}(?:[、)）]|\.(?=\s)))/)
      : [line]);
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    if (/^(?:[一二三四五六七八九十百]+[、.．：:]|第[一二三四五六七八九十百\d]+[章节部分]|场景[一二三四五六七八九十\d]+[、.．：:])/.test(line)) {
      startGroup(line);
      continue;
    }
    const sub = line.match(/^([（(][一二三四五六七八九十百\d]+[）)])\s*/);
    const dotted = line.match(/^(\d+(?:[.．]\d+)+)(?=$|[\s、:：\u3400-\u9fff])(?:[、:：])?\s*/);
    const number = line.match(/^第\s*(\d{1,3})\s*步[：:]?\s*/)
      || line.match(/^(\d{1,3})(?:[、)）．]|\.(?!\d)|[：:])\s*/);
    const prefix = sub || dotted || number;
    const indented = /^(?:\t| {2}|　)/.test(raw);
    const depth = dotted ? Math.min(3, dotted[1].split(/[.．]/).length - 1) : sub || indented ? 1 : 0;
    if (number && !sub && !dotted && depth === 0) {
      const step = Number(number[1]);
      if (step === 1 && lastStep !== undefined) startGroup();
      lastStep = step;
    }
    if (prefix) {
      group.rows.push({ marker: prefix[0].trim(), text: line.slice(prefix[0].length), depth });
    } else {
      group.rows.push({ text: line, depth: Math.max(depth, group.rows[group.rows.length - 1]?.depth || 0) });
    }
  }
  startGroup();
  return groups;
}
