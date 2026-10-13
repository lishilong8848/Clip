import type { GlobalThemeOverrides } from 'naive-ui';
// UI/UX Pro Max: restrained enterprise surfaces, compact data and semantic accents.
export const theme: GlobalThemeOverrides = {
  common: { primaryColor: '#087b69', primaryColorHover: '#096958', primaryColorPressed: '#085748', primaryColorSuppl: '#087b69',
    infoColor: '#1762cf', successColor: '#13836c', warningColor: '#af650d', errorColor: '#c93d4c',
    cubicBezierEaseInOut:'cubic-bezier(.4,0,.2,1)', cubicBezierEaseOut:'cubic-bezier(.22,.61,.36,1)', cubicBezierEaseIn:'cubic-bezier(.55,0,1,.45)',
    borderRadius: '8px', borderRadiusSmall: '4px', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif',
    fontSize: '14px', textColorBase: '#24312e', textColor2: '#63736d', borderColor: '#dbe3df' },
  Button: { fontWeight: '500', heightMedium: '36px' },
  DataTable: { thColor: '#f1f6f2', tdColor: '#ffffff', tdColorHover: '#f2f8f3', borderColor: '#e0e8e2' },
  Menu: { itemHeight: '38px', itemColorActive: '#eaf2ff', itemTextColorActive: '#0066cc', itemTextColorChildActive: '#0066cc' },
};
