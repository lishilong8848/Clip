export type ToolDateKind = 'date' | 'datetime-local' | 'time' | 'month';

// Match only empty or marked boxes. Bracketed prose remains ordinary text.
const box = /\[\s*[√✓✔xX]?\s*\]|［\s*[√✓✔xX]?\s*］|【\s*[√✓✔xX]?\s*】|[□☐☑☒]/gu;
export function boxParts(text: string) {
  const parts: {text: string; mark: string; offset: number; checked:boolean}[] = [];
  let start = 0;
  for (const match of text.matchAll(box)) {
    const offset = match.index!;
    parts.push({text:text.slice(start,offset),mark:match[0],offset,checked:/[√✓✔xX☑☒]/u.test(match[0])});
    start = offset + match[0].length;
  }
  return {parts,tail:text.slice(start)};
}
export function completionPair(value:string) {
  return boxParts(value).parts.length===2 && /^(已完成未完成|未完成已完成)$/.test(value.replace(box,'').replace(/[\s,，、;；]+/g,''));
}
export function tickBox(value:string,original:string,index:number,checked:boolean) {
  if(checked && completionPair(value)) value=tickBox(value,original,1-index,false);
  const part=boxParts(value).parts[index];
  if (!part) return value;
  const empty=boxParts(original).parts[index]?.mark;
  const mark=part.mark;
  let replacement=mark[0]+(checked?'√':' ')+mark[mark.length-1];
  if(mark.length===1) replacement=checked?'☑':'☐';
  if(!checked && empty && (empty[0]===mark[0] || empty.length===1 && mark.length===1) &&
    !/[√✓✔xX☑☒]/u.test(empty)) replacement=empty;
  return value.slice(0,part.offset)+replacement+value.slice(part.offset+mark.length);
}

export function pickerValue(kind:ToolDateKind,value:string) {
  const chinese=/^\s*(\d{4})\s*年\s*[/.-]?\s*(\d{1,2})\s*月(?:\s*[/.-]?\s*(\d{1,2})\s*日)?(?:\s*(\d{1,2})\s*时(?:\s*(\d{1,2})\s*分)?(?:\s*(\d{1,2})\s*秒)?)?\s*$/u.exec(value);
  const pad=(n:string|undefined)=>String(n||0).padStart(2,'0');
  if (chinese) value=chinese[1]+'-'+pad(chinese[2])+'-'+pad(chinese[3]||'1')+'T'+pad(chinese[4])+':'+pad(chinese[5])+':'+pad(chinese[6]);
  if(kind==='time'){
    const clock=/^(\d{1,2})\s*时\s*(\d{1,2})\s*分(?:\s*(\d{1,2})\s*秒)?$/u.exec(value.trim());
    if(clock)return pad(clock[1])+':'+pad(clock[2])+(clock[3]?':'+pad(clock[3]):'');
    return /^\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/.test(value)?value:'';
  }
  if (!/^\d{4}-\d{2}/.test(value)) return '';
  if(kind==='month')return value.slice(0,7);
  if(kind==='date')return value.slice(0,10);
  return value.length===10?value+'T00:00':value.replace(' ','T');
}

export function displayToolDate(kind:ToolDateKind,value:string,template:string) {
  const iso=pickerValue(kind,value);
  if(!iso)return value;
  if(!/^[\s\d＿_年月日时分秒:：/.-]+$/u.test(template) || !/[年月日时分秒]/u.test(template))return iso.replace('T',' ');
  const [day,clock='']=kind==='time'?['',iso]:iso.split('T');
  const [year,month,date]=day.split('-');
  const [hour,minute,second]=clock.split(':');
  const units:Record<string,string>={年:year,月:month,日:date,时:hour,分:minute,秒:second};
  if(kind==='datetime-local' && value.length===10 && template.includes('日'))template=template.split('日')[0]+'日';
  let text=template.replace(/[\s\d＿_]*([年月日时分秒])/gu,(_,unit:string)=>(units[unit]||'00')+unit).trim();
  if(kind==='datetime-local' && !template.includes('分') && Number(minute))text+=minute+'分';
  if(!template.includes('秒') && Number(second))text+=second+'秒';
  return text;
}
