import type { GlobalThemeOverrides } from 'naive-ui';
import { computed, ref, watchEffect } from 'vue';

type Shades = { bg:string; surface:string; accent:string; text:string; muted:string; border:string };
type Palette = { id:string; label:string; light:Shades; dark:Shades };
export const PALETTES:Palette[] = [
  {id:'cobalt',label:'钴蓝银白',light:{bg:'#f3f5f7',surface:'#ffffff',accent:'#2457e6',text:'#20252d',muted:'#596473',border:'#d5dce5'},dark:{bg:'#101114',surface:'#1d2026',accent:'#8aa8ff',text:'#f4f6fa',muted:'#b9c2d2',border:'#434b59'}},
  {id:'gold',label:'黑金',light:{bg:'#f4f4f4',surface:'#ffffff',accent:'#fdc941',text:'#171717',muted:'#626262',border:'#d8d8d8'},dark:{bg:'#000000',surface:'#191919',accent:'#fdc941',text:'#ffffff',muted:'#bdbdbd',border:'#555555'}},
  {id:'vermilion',label:'朱砂红橙',light:{bg:'#f6f5f4',surface:'#ffffff',accent:'#f5402b',text:'#24201e',muted:'#6c605b',border:'#ded7d3'},dark:{bg:'#141110',surface:'#25201e',accent:'#ff705d',text:'#f7f4f2',muted:'#cbbcb4',border:'#574942'}},
  {id:'mint',label:'松绿薄荷',light:{bg:'#f3f6f4',surface:'#ffffff',accent:'#087a61',text:'#202823',muted:'#58665e',border:'#d2dfd7'},dark:{bg:'#101412',surface:'#1c2420',accent:'#68dbb1',text:'#f0f6f2',muted:'#b5c7bc',border:'#40574b'}},
  {id:'cyan',label:'石墨冰青',light:{bg:'#f3f5f6',surface:'#ffffff',accent:'#007c91',text:'#20282d',muted:'#56666e',border:'#d2dce1'},dark:{bg:'#101315',surface:'#20272b',accent:'#67d4e8',text:'#f1f6f7',muted:'#b8c8cd',border:'#43555e'}},
  {id:'berry',label:'莓红石白',light:{bg:'#f6f4f5',surface:'#ffffff',accent:'#b52d54',text:'#292127',muted:'#6b5c65',border:'#ded4d9'},dark:{bg:'#151114',surface:'#251c22',accent:'#f38aa7',text:'#faf3f6',muted:'#ccbac4',border:'#59444f'}},
];
export const DEFAULT_NAV_COLOR = PALETTES[0].light.accent;
export const nightMode = ref(false);
export const paletteId = ref('cobalt');
export const customNavColor = ref(DEFAULT_NAV_COLOR);
export const validColor = (value:unknown):value is string => typeof value==='string' && /^#[\da-f]{6}$/i.test(value);
const key = (name:string,userId:string) => 'clipflow:workspace-'+name+':'+(userId||'visitor');
const rgb = (color:string) => [1,3,5].map(i=>parseInt(color.slice(i,i+2),16));
const mix = (a:string,b:string,amount:number) => '#'+rgb(a).map((v,i)=>Math.round(v*(1-amount)+rgb(b)[i]*amount).toString(16).padStart(2,'0')).join('');
function luminance(color:string) { return rgb(color).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0); }
export function contrast(a:string,b:string) { const x=luminance(a),y=luminance(b);return (Math.max(x,y)+.05)/(Math.min(x,y)+.05); }
const onColor = (color:string) => contrast(color,'#000000') >= contrast(color,'#ffffff') ? '#000000' : '#ffffff';

export function paletteColors(id:string,dark:boolean,custom=DEFAULT_NAV_COLOR) {
  const palette=PALETTES.find(item=>item.id===id)||PALETTES[0];
  const base={...palette[dark?'dark':'light']};
  if(id==='custom'&&validColor(custom))base.accent=custom;
  // Accent fills and readable accent text differ, especially yellow on white.
  const soft=mix(base.surface,base.accent,dark?.10:.055);
  let accentText=base.accent;
  for(let i=0;i<24&&Math.min(...[base.bg,base.surface,soft].map(bg=>contrast(accentText,bg)))<4.5;i++)accentText=mix(accentText,dark?'#ffffff':'#000000',.1);
  const hover=id==='vermilion'?'#f97243':mix(base.accent,onColor(base.accent)==='#000000'?'#ffffff':'#000000',.08);
  const pressed=id==='vermilion'?'#fa932c':mix(base.accent,onColor(base.accent)==='#000000'?'#ffffff':'#000000',.16);
  let header=mix(base.accent,'#10151f',dark?.67:.30);
  for(let i=0;i<24&&contrast(header,'#f4f8ff')<5;i++)header=mix(header,'#10151f',.1);
  const sidebar=mix(base.surface,base.accent,dark?.20:.27),sidebarEnd=mix(base.surface,base.accent,dark?.12:.17);
  const navSelected=mix(base.surface,base.accent,dark?.32:.42),navHover=mix(base.surface,base.accent,dark?.26:.34);
  const readable=(initial:string)=>{let text=initial;for(let i=0;i<24&&Math.min(...[sidebar,sidebarEnd,navSelected,navHover].map(bg=>contrast(text,bg)))<4.5;i++)text=mix(text,dark?'#ffffff':'#000000',.1);return text;};
  return {...base,soft,accentText,hover,pressed,onAccent:onColor(base.accent),onAccentText:onColor(accentText),
    header,sidebar,sidebarEnd,navSelected,navHover,navText:readable(base.text),navMuted:readable(base.muted),
    selection:mix(base.surface,id==='vermilion'?'#fcb833':base.accent,dark?.16:.09)};
}
export const activeColors = computed(()=>paletteColors(paletteId.value,nightMode.value,customNavColor.value));
export function applyNightMode(value:boolean) { nightMode.value=value; }
export function loadNightMode(userId:string) {
  let dark=false;
  try { dark=localStorage.getItem(key('night-mode',userId))==='dark'; } catch { /* Switching remains available without storage. */ }
  applyNightMode(dark);
}
export function saveNightMode(userId:string,value:boolean):boolean {
  applyNightMode(value);
  try {localStorage.setItem(key('night-mode',userId),value?'dark':'light');return true;} catch {return false;}
}
export function applyPalette(id:string,custom=customNavColor.value) {
  if(id!=='custom'&&!PALETTES.some(item=>item.id===id))return;
  if(id==='custom'&&!validColor(custom))return;
  paletteId.value=id;
  customNavColor.value=custom.toLowerCase();
}
export function loadPalette(userId:string) {
  let id:string|null=null,color:string|null=null;
  try {id=localStorage.getItem(key('palette',userId));color=localStorage.getItem(key('nav-color',userId));} catch { /* Account preferences remain isolated. */ }
  if(PALETTES.some(item=>item.id===id))applyPalette(id!);
  else if(validColor(color)&&color!=='#2364d2')applyPalette('custom',color);
  else applyPalette('cobalt');
}
export function savePalette(userId:string,id:string,custom=customNavColor.value):boolean {
  const color=id==='custom'?custom:PALETTES.find(item=>item.id===id)?.light.accent;
  if(!validColor(color))return false;
  applyPalette(id,color);
  try {localStorage.setItem(key('palette',userId),id);localStorage.setItem(key('nav-color',userId),color);return true;} catch {return false;}
}

watchEffect(()=>{
  const root=document.documentElement,colors=activeColors.value;
  root.dataset.v2Theme=nightMode.value?'dark':'light';
  root.dataset.v2Palette=paletteId.value;
  for(const [name,value] of Object.entries(colors))root.style.setProperty('--v2-palette-'+name.replace(/[A-Z]/g,c=>'-'+c.toLowerCase()),value);
});

const controls:GlobalThemeOverrides = {
  common:{borderRadius:'8px',borderRadiusSmall:'4px',fontSize:'14px',fontFamily:'-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif',
    cubicBezierEaseInOut:'cubic-bezier(.4,0,.2,1)',cubicBezierEaseOut:'cubic-bezier(.22,.61,.36,1)',cubicBezierEaseIn:'cubic-bezier(.55,0,1,.45)'},
  Switch:{railWidthMedium:'48px',railHeightMedium:'28px',buttonWidthMedium:'22px',buttonHeightMedium:'22px',buttonWidthPressedMedium:'30.8px',railWidthSmall:'36px',railHeightSmall:'20px',buttonWidthSmall:'14px',buttonHeightSmall:'14px',buttonWidthPressedSmall:'17px'},
};
function overrides(dark:boolean):GlobalThemeOverrides {
  const c=paletteColors(paletteId.value,dark,customNavColor.value);
  return {common:{...controls.common,primaryColor:c.accentText,primaryColorHover:c.accentText,primaryColorPressed:c.accentText,primaryColorSuppl:c.accentText,
      infoColor:dark?'#83baff':'#155bb8',successColor:dark?'#65cda9':'#0a735c',warningColor:dark?'#efbd70':'#92520a',errorColor:dark?'#f38f99':'#b5263a',
      textColorBase:c.text,textColor1:c.text,textColor2:c.muted,textColor3:c.muted,placeholderColor:c.muted,
      bodyColor:c.bg,cardColor:c.surface,modalColor:c.surface,popoverColor:c.surface,tableColor:c.surface,inputColor:c.surface,
      borderColor:c.border,dividerColor:c.border,hoverColor:c.soft},
    Empty:{textColor:c.muted},
    Button:{fontWeight:'500',heightMedium:'36px',colorPrimary:c.accent,colorHoverPrimary:c.hover,colorPressedPrimary:c.pressed,colorFocusPrimary:c.hover,
      textColorPrimary:c.onAccent,textColorHoverPrimary:onColor(c.hover),textColorPressedPrimary:onColor(c.pressed),textColorFocusPrimary:onColor(c.hover),
      borderPrimary:'1px solid '+c.accentText,borderHoverPrimary:'1px solid '+c.accentText,borderPressedPrimary:'1px solid '+c.accentText,borderFocusPrimary:'1px solid '+c.accentText,
      textColorTextPrimary:c.accentText,textColorGhostPrimary:c.accentText},
    Switch:{...controls.Switch,railColorActive:c.accentText},
    DataTable:{thColor:c.soft,tdColor:c.surface,tdColorHover:c.soft,borderColor:c.border},
    Menu:{itemHeight:'38px',itemColorActive:c.selection,itemTextColorActive:c.accentText,itemTextColorChildActive:c.accentText},
  };
}
export const theme = computed(()=>overrides(false));
export const nightTheme = computed(()=>overrides(true));
