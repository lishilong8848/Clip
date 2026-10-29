import{t as o,_ as h}from"./c-BIy6hk9M.js";const{cubicBezierEaseInOut:$,cubicBezierEaseOut:f,cubicBezierEaseIn:r}=h;function x({overflow:p="hidden",duration:t=".3s",originalTransition:i="",leavingDelay:e="0s",foldPadding:m=!1,enterToProps:d=void 0,leaveToProps:c=void 0,reverse:s=!1}={}){const n=s?"leave":"enter",a=s?"enter":"leave";return[o(`&.fade-in-height-expand-transition-${a}-from,
 &.fade-in-height-expand-transition-${n}-to`,{...d,opacity:1}),o(`&.fade-in-height-expand-transition-${a}-to,
 &.fade-in-height-expand-transition-${n}-from`,{...c,opacity:0,marginTop:"0 !important",marginBottom:"0 !important",paddingTop:m?"0 !important":void 0,paddingBottom:m?"0 !important":void 0}),o(`&.fade-in-height-expand-transition-${a}-active`,`
 overflow: ${p};
 transition:
 max-height ${t} ${$} ${e},
 opacity ${t} ${f} ${e},
 margin-top ${t} ${$} ${e},
 margin-bottom ${t} ${$} ${e},
 padding-top ${t} ${$} ${e},
 padding-bottom ${t} ${$} ${e}
 ${i?`,${i}`:""}
 `),o(`&.fade-in-height-expand-transition-${n}-active`,`
 overflow: ${p};
 transition:
 max-height ${t} ${$},
 opacity ${t} ${r},
 margin-top ${t} ${$},
 margin-bottom ${t} ${$},
 padding-top ${t} ${$},
 padding-bottom ${t} ${$}
 ${i?`,${i}`:""}
 `)]}export{x as f};
