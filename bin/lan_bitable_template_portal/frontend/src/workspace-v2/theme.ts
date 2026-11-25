import type { GlobalThemeOverrides } from 'naive-ui';
import { computed, ref, watchEffect } from 'vue';

// Shared colours for templates, charts, forms and teleported dialogs.
const navigation={
  header:'#294b49', sidebar:'#f5f5f5', sidebarEnd:'#f5f5f5', navSelected:'#deeee4',
  navHover:'#ebf1ed', navText:'#2d3748', navMuted:'#536175',
};
export const mineral={
  light:{...navigation, bg:'#f4f5f6',surface:'#fafafb',soft:'#f0f2f4',text:'#2d3748',
    muted:'#536175',border:'#cdd5da',accent:'#168047',accentText:'#146541',
    hover:'#116d3c',pressed:'#0e5b34',onAccent:'#ffffff',onAccentText:'#ffffff',selection:'#e0f2e6'},
  dark:{...navigation, sidebar:'#162c32',sidebarEnd:'#102126', navText:'#eaf5f2',navMuted:'#b7cccd', bg:'#1b282d',surface:'#27373c',
    soft:'#233237',text:'#eaf5f2',muted:'#b7cccd',border:'#51666b',accent:'#80dccc',
    accentText:'#94e4d5',hover:'#a1e9dc',pressed:'#6ad1c0',onAccent:'#102e2c',
    onAccentText:'#102e2c',selection:'#314e50'},
};
export const nightMode=ref(false);
export const palettes={
  panorama:{
    light:{...mineral.light,bg:'#f8fafc',surface:'#ffffff',soft:'#f1f5f9',text:'#0f172a',muted:'#475569',
      border:'#e2e8f0',header:'#f8fafc',sidebar:'#f8fafc',sidebarEnd:'#f8fafc',navText:'#334155',navMuted:'#475569',
      accent:'#2563eb',accentText:'#1d4ed8',hover:'#1d4ed8',pressed:'#1e40af',selection:'#eff6ff',
      navSelected:'#eff6ff',navHover:'#f1f5f9'},
    dark:{...mineral.dark,bg:'#111827',surface:'#1e293b',soft:'#182234',text:'#f1f5f9',muted:'#cbd5e1',
      border:'#3b4b62',header:'#1e293b',sidebar:'#182234',sidebarEnd:'#111827',navText:'#f1f5f9',navMuted:'#cbd5e1',
      accent:'#93c5fd',accentText:'#bfdbfe',hover:'#bfdbfe',pressed:'#7db3f7',onAccent:'#172554',onAccentText:'#172554',
      selection:'#223961',navSelected:'#223961',navHover:'#27364c'},
  },
  mineral,
  ocean:{
    light:{...mineral.light,bg:'#f1f5fb',surface:'#f9fbff',soft:'#eaf0f8',header:'#203c60',sidebar:'#f1f5fb',sidebarEnd:'#f1f5fb',
      accent:'#255db2',accentText:'#214f97',hover:'#204e98',pressed:'#173e7b',selection:'#deebfc',navSelected:'#deebfc',navHover:'#eaf1fb'},
    dark:{...mineral.dark,bg:'#182538',surface:'#23334a',soft:'#1d2c41',header:'#23334a',sidebar:'#162238',sidebarEnd:'#121d30',
      accent:'#91bbfc',accentText:'#accdff',hover:'#adcdff',pressed:'#82aff1',onAccent:'#152943',onAccentText:'#152943',
      selection:'#2c4568',navSelected:'#2c4568',navHover:'#233750'},
  },
  violet:{
    light:{...mineral.light,bg:'#f5f2fa',surface:'#fcfaff',soft:'#efebf6',header:'#473160',sidebar:'#f5f2fa',sidebarEnd:'#f5f2fa',
      accent:'#6a459e',accentText:'#5f3b91',hover:'#5b378c',pressed:'#4c2e78',selection:'#eee4fa',navSelected:'#eee4fa',navHover:'#f0eaf7'},
    dark:{...mineral.dark,bg:'#221d32',surface:'#302641',soft:'#2a223a',header:'#302641',sidebar:'#211a31',sidebarEnd:'#191427',
      accent:'#c6a8f5',accentText:'#d5bbff',hover:'#d7bdfc',pressed:'#b698e7',onAccent:'#2c1a47',onAccentText:'#2c1a47',
      selection:'#48345f',navSelected:'#48345f',navHover:'#36294b'},
  },
  cyber:{
    light:{...mineral.light,bg:'#eaf4f7',surface:'#f6fcfd',soft:'#deeef2',text:'#172f38',muted:'#3e5966',
      border:'#adc9d1',header:'#123e4b',sidebar:'#eaf4f7',sidebarEnd:'#eaf4f7',navText:'#172f38',navMuted:'#3e5966',
      accent:'#006477',accentText:'#005363',hover:'#00566a',pressed:'#044656',selection:'#d4edf4',navSelected:'#d4edf4',navHover:'#deeef2'},
    dark:{...mineral.dark,bg:'#0c1821',surface:'#142635',soft:'#10212e',text:'#e9f8ff',muted:'#abceda',
      border:'#35576a',header:'#142635',sidebar:'#0c1821',sidebarEnd:'#0a141c',navText:'#e9f8ff',navMuted:'#abceda',
      accent:'#59e1ee',accentText:'#76eefa',hover:'#89eff7',pressed:'#40cbda',onAccent:'#062731',onAccentText:'#062731',
      selection:'#1e4050',navSelected:'#1e4050',navHover:'#183444'},
  },
  neon:{
    light:{...mineral.light,bg:'#f7eff6',surface:'#fff8fd',soft:'#f2e6f1',text:'#391f38',muted:'#65435f',
      border:'#d7bacf',header:'#54213f',sidebar:'#f7eff6',sidebarEnd:'#f7eff6',navText:'#391f38',navMuted:'#65435f',
      accent:'#a02675',accentText:'#80235f',hover:'#8a2065',pressed:'#731a55',selection:'#f4deee',navSelected:'#f4deee',navHover:'#f2e6f1'},
    dark:{...mineral.dark,bg:'#1d1123',surface:'#301b35',soft:'#28162e',text:'#fff1fb',muted:'#dfbbd7',
      border:'#624263',header:'#301b35',sidebar:'#1d1123',sidebarEnd:'#190e1e',navText:'#fff1fb',navMuted:'#dfbbd7',
      accent:'#ff91d4',accentText:'#ffb2e1',hover:'#ffb2e1',pressed:'#ee78bf',onAccent:'#3f1533',onAccentText:'#3f1533',
      selection:'#512747',navSelected:'#512747',navHover:'#422039'},
  },
  lava:{
    light:{...mineral.light,bg:'#f9f2e9',surface:'#fffcf6',soft:'#f4ebdf',text:'#392a20',muted:'#634d3d',
      border:'#d8c3ab',header:'#55341e',sidebar:'#f9f2e9',sidebarEnd:'#f9f2e9',navText:'#392a20',navMuted:'#634d3d',
      accent:'#9b431b',accentText:'#813810',hover:'#873916',pressed:'#6f2c0d',selection:'#f5e1ca',navSelected:'#f5e1ca',navHover:'#f4ebdf'},
    dark:{...mineral.dark,bg:'#201711',surface:'#30241d',soft:'#281d17',text:'#fff3e8',muted:'#e2c7ad',
      border:'#665043',header:'#30241d',sidebar:'#201711',sidebarEnd:'#190f0b',navText:'#fff3e8',navMuted:'#e2c7ad',
      accent:'#ffb568',accentText:'#ffc68f',hover:'#ffcc9a',pressed:'#eda553',onAccent:'#392012',onAccentText:'#392012',
      selection:'#503222',navSelected:'#503222',navHover:'#432b1e'},
  },
  graphite:{
    light:{...mineral.light,bg:'#edf0f4',surface:'#f9fafc',soft:'#e6eaf0',text:'#222e40',muted:'#4a566a',
      border:'#c0c9d5',header:'#323c4b',sidebar:'#edf0f4',sidebarEnd:'#edf0f4',navText:'#222e40',navMuted:'#4a566a',
      accent:'#45536a',accentText:'#3c485b',hover:'#39465b',pressed:'#303a4e',selection:'#dee5ee',navSelected:'#dee5ee',navHover:'#e6eaf0'},
    dark:{...mineral.dark,bg:'#181c24',surface:'#242b36',soft:'#1f252f',text:'#eff4ff',muted:'#c0cbd9',
      border:'#4b596d',header:'#242b36',sidebar:'#181c24',sidebarEnd:'#12161e',navText:'#eff4ff',navMuted:'#c0cbd9',
      accent:'#c2d0eb',accentText:'#d3dff7',hover:'#d6e1f4',pressed:'#aabbdc',onAccent:'#263048',onAccentText:'#263048',
      selection:'#374359',navSelected:'#374359',navHover:'#303a4b'},
  },
};
export type PaletteId=keyof typeof palettes;
export const paletteId=ref<PaletteId>('panorama');
const activeColors=computed(()=>palettes[paletteId.value][nightMode.value?'dark':'light']);
const modeKey=(id:string)=>'clipflow:workspace-night-mode:'+(id||'visitor');
const paletteKey=(id:string)=>'clipflow:workspace-color-palette:'+(id||'visitor');
export function applyNightMode(value:boolean) { nightMode.value=value; }
export function loadNightMode(userId:string) {
  let dark=false;
  try { dark=localStorage.getItem(modeKey(userId))==='dark'; } catch { /* Switching remains available without storage. */ }
  applyNightMode(dark);
}
export function loadPalette(userId:string) {
  paletteId.value='panorama';
  try {
    const value=localStorage.getItem(paletteKey(userId));
    if(value&&Object.prototype.hasOwnProperty.call(palettes,value))paletteId.value=value as PaletteId;
  } catch { /* Use the default when storage is unavailable. */ }
}
export function savePalette(userId:string,value:PaletteId):boolean {
  paletteId.value=value;
  try {localStorage.setItem(paletteKey(userId),value);return true;} catch {return false;}
}
export function saveNightMode(userId:string,value:boolean):boolean {
  applyNightMode(value);
  try {localStorage.setItem(modeKey(userId),value?'dark':'light');return true;} catch {return false;}
}

watchEffect(()=>{
  const root=document.documentElement,colors=activeColors.value;
  root.dataset.v2Theme=nightMode.value?'dark':'light';
  root.dataset.v2Palette=paletteId.value;
  for(const [name,value] of Object.entries(colors))root.style.setProperty('--v2-palette-'+name.replace(/[A-Z]/g,c=>'-'+c.toLowerCase()),value);
});

const controls:GlobalThemeOverrides = {
  common:{borderRadius:'8px',borderRadiusSmall:'6px',fontSize:'var(--cf-font-body)',fontFamily:'var(--cf-font-family)',
    cubicBezierEaseInOut:'cubic-bezier(.4,0,.2,1)',cubicBezierEaseOut:'cubic-bezier(.22,.61,.36,1)',cubicBezierEaseIn:'cubic-bezier(.55,0,1,.45)'},
  Switch:{railWidthMedium:'48px',railHeightMedium:'28px',buttonWidthMedium:'22px',buttonHeightMedium:'22px',buttonWidthPressedMedium:'30.8px',railWidthSmall:'36px',railHeightSmall:'20px',buttonWidthSmall:'14px',buttonHeightSmall:'14px',buttonWidthPressedSmall:'17px'},
};
function overrides(dark:boolean):GlobalThemeOverrides {
  const c=palettes[paletteId.value][dark?'dark':'light'];
  return {common:{...controls.common,primaryColor:c.accentText,primaryColorHover:c.accentText,primaryColorPressed:c.accentText,primaryColorSuppl:c.accentText,
      infoColor:dark?'#83baff':'#155bb8',successColor:dark?'#65cda9':'#0a735c',warningColor:dark?'#efbd70':'#92520a',errorColor:dark?'#f38f99':'#b5263a',
      textColorBase:c.text,textColor1:c.text,textColor2:c.muted,textColor3:c.muted,placeholderColor:c.muted,
      bodyColor:c.bg,cardColor:c.surface,modalColor:c.surface,popoverColor:c.surface,tableColor:c.surface,inputColor:c.surface,
      borderColor:c.border,dividerColor:c.border,hoverColor:c.soft},
    Empty:{textColor:c.muted},
    Button:{fontWeight:'500',heightMedium:'34px',colorPrimary:c.accent,colorHoverPrimary:c.hover,colorPressedPrimary:c.pressed,colorFocusPrimary:c.hover,
      textColorPrimary:c.onAccent,textColorHoverPrimary:c.onAccent,textColorPressedPrimary:c.onAccent,textColorFocusPrimary:c.onAccent,
      borderPrimary:'1px solid '+c.accentText,borderHoverPrimary:'1px solid '+c.accentText,borderPressedPrimary:'1px solid '+c.accentText,borderFocusPrimary:'1px solid '+c.accentText,
      textColorTextPrimary:c.accentText,textColorGhostPrimary:c.accentText},
    Switch:{...controls.Switch,railColorActive:c.accentText},
    DataTable:{thColor:c.soft,tdColor:c.surface,tdColorHover:c.soft,borderColor:c.border},
    Menu:{itemHeight:'38px',itemColorActive:c.selection,itemTextColorActive:c.accentText,itemTextColorChildActive:c.accentText},
  };
}
export const theme = computed(()=>overrides(false));
export const nightTheme = computed(()=>overrides(true));
