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
  const light=lightness(neutral?[35,43,52]:fill!),text=rgb(color),ratio=text?(Math.max(light,lightness(text))+.05)/(Math.min(light,lightness(text))+.05):0;
  if(ratio<4.5)color=light>.179?'#000000':'#ffffff';
 }
 return {'--v2-cell-background':background,'--v2-cell-ink':color,background:'var(--v2-cell-background)',color:'var(--v2-cell-ink)'};
}
