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
const activeColors=computed(()=>nightMode.value?mineral.dark:mineral.light);
const modeKey=(id:string)=>'clipflow:workspace-night-mode:'+(id||'visitor');
export function applyNightMode(value:boolean) { nightMode.value=value; }
export function loadNightMode(userId:string) {
  let dark=false;
  try { dark=localStorage.getItem(modeKey(userId))==='dark'; } catch { /* Switching remains available without storage. */ }
  applyNightMode(dark);
}
export function saveNightMode(userId:string,value:boolean):boolean {
  applyNightMode(value);
  try {localStorage.setItem(modeKey(userId),value?'dark':'light');return true;} catch {return false;}
}

watchEffect(()=>{
  const root=document.documentElement,colors=activeColors.value;
  root.dataset.v2Theme=nightMode.value?'dark':'light';
  root.dataset.v2Palette='mineral';
  for(const [name,value] of Object.entries(colors))root.style.setProperty('--v2-palette-'+name.replace(/[A-Z]/g,c=>'-'+c.toLowerCase()),value);
});

const controls:GlobalThemeOverrides = {
  common:{borderRadius:'10px',borderRadiusSmall:'6px',fontSize:'14px',fontFamily:'-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif',
    cubicBezierEaseInOut:'cubic-bezier(.4,0,.2,1)',cubicBezierEaseOut:'cubic-bezier(.22,.61,.36,1)',cubicBezierEaseIn:'cubic-bezier(.55,0,1,.45)'},
  Switch:{railWidthMedium:'48px',railHeightMedium:'28px',buttonWidthMedium:'22px',buttonHeightMedium:'22px',buttonWidthPressedMedium:'30.8px',railWidthSmall:'36px',railHeightSmall:'20px',buttonWidthSmall:'14px',buttonHeightSmall:'14px',buttonWidthPressedSmall:'17px'},
};
function overrides(dark:boolean):GlobalThemeOverrides {
  const c=dark?mineral.dark:mineral.light;
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
