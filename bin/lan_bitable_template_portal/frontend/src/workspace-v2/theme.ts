import type { GlobalThemeOverrides } from 'naive-ui';
import { ref } from 'vue';
export const DEFAULT_NAV_COLOR = '#2364d2';
export const nightMode = ref(false);
const modeKey = (userId:string) => 'clipflow:workspace-night-mode:' + (userId || 'visitor');
export function applyNightMode(value:boolean) {
  nightMode.value = value;
  document.documentElement.dataset.v2Theme = value ? 'dark' : 'light';
}
export function loadNightMode(userId:string) {
  let dark = false;
  try { dark = localStorage.getItem(modeKey(userId)) === 'dark'; } catch { /* In-memory switching stays available. */ }
  applyNightMode(dark);
}
export function saveNightMode(userId:string, value:boolean):boolean {
  applyNightMode(value);
  try { localStorage.setItem(modeKey(userId), value ? 'dark' : 'light'); return true; } catch { return false; }
}
// UI/UX Pro Max: restrained enterprise surfaces, compact data and semantic accents.
export const theme: GlobalThemeOverrides = {
  common: { primaryColor: '#0071e3', primaryColorHover: '#0063c7', primaryColorPressed: '#0055aa', primaryColorSuppl: '#0071e3',
    infoColor: '#1762cf', successColor: '#13836c', warningColor: '#af650d', errorColor: '#c93d4c',
    cubicBezierEaseInOut:'cubic-bezier(.4,0,.2,1)', cubicBezierEaseOut:'cubic-bezier(.22,.61,.36,1)', cubicBezierEaseIn:'cubic-bezier(.55,0,1,.45)',
    borderRadius: '8px', borderRadiusSmall: '4px', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif',
    fontSize: '14px', textColorBase: '#24312e', textColor2: '#63736d', borderColor: '#dbe3df' },
  Button: { fontWeight: '500', heightMedium: '36px' },
  DataTable: { thColor: '#f1f6f2', tdColor: '#ffffff', tdColorHover: '#f2f8f3', borderColor: '#e0e8e2' },
  Menu: { itemHeight: '38px', itemColorActive: '#eaf2ff', itemTextColorActive: '#0066cc', itemTextColorChildActive: '#0066cc' },
};
export const nightTheme: GlobalThemeOverrides = {
  common: { ...theme.common, primaryColor:'#75baff',primaryColorHover:'#9aceff',primaryColorPressed:'#57a7f7',primaryColorSuppl:'#75baff',
    infoColor:'#83baff',successColor:'#65cda9',warningColor:'#efbd70',errorColor:'#f38f99',
    textColorBase:'#edf0f4',textColor1:'#edf0f4',textColor2:'#bec5cf',textColor3:'#929daa',
    bodyColor:'#16191d',cardColor:'#202429',modalColor:'#252a31',popoverColor:'#292e35',tableColor:'#202429',
    inputColor:'#191d22',borderColor:'#414952',dividerColor:'#363e47' },
  Button:theme.Button,
  DataTable:{thColor:'#292f36',tdColor:'#202429',tdColorHover:'#2c3540',borderColor:'#3a434e'},
  Menu:{itemHeight:'38px',itemColorActive:'#2c4054',itemTextColorActive:'#a4cfff',itemTextColorChildActive:'#a4cfff'},
};
