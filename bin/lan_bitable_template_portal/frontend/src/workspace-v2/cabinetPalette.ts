import type {Dict} from './client';
function rgb(value:string):number[]|null{
 const hex=value.replace(/^#/,'');
 if(/^[\da-f]{3}$/i.test(hex))return [...hex].map(c=>parseInt(c+c,16));
 if(/^[\da-f]{6}$/i.test(hex))return [0,2,4].map(i=>parseInt(hex.slice(i,i+2),16));
 const match=value.match(/^rgba?\((\d+)[, ]+(\d+)[, ]+(\d+)/i);return match?match.slice(1).map(Number):null;
}
function lightness(color:number[]){return color.reduce((n,v,i)=>{const s=v/255;return n+[.2126,.7152,.0722][i]!*(s<=.04045?s/12.92:((s+.055)/1.055)**2.4);},0);}
export function cabinetPalette(cell:Dict,dark:boolean){
 let background=String(cell.color||cell.style?.fill||'transparent');
 let color=String(cell.rack&&cell.state==='formal'?'#fff':cell.style?.font_color||'#1e293b');
 if(dark){
  const fill=rgb(background),neutral=!fill||Math.max(...fill)-Math.min(...fill)<20&&lightness(fill)>.65;
  if(neutral)background='#232b34';
  const light=lightness(neutral?[35,43,52]:fill!);
  // Spreadsheet fonts are thin at fitted zoom: use the strongest available contrast.
  color=(light+.05)/.05>=1.05/(light+.05)?'#000000':'#ffffff';
 }
 return {'--v2-cell-background':background,'--v2-cell-ink':color,'--v2-cell-font':dark?'system-ui, "Microsoft YaHei", sans-serif':String(cell.style?.font_name||'inherit'),'--v2-cell-weight':cell.style?.bold?'700':dark?'600':'400',background:'var(--v2-cell-background)',color:'var(--v2-cell-ink)'};
}
