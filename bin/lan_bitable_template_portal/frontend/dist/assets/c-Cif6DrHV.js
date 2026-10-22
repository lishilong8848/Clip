import{aF as Kt,aA as ze,b4 as Jt,bk as Qt,T as dt,b5 as Zt,a0 as ct,aO as $,aR as S,S as bt,aQ as ve,c as er,a7 as Ne,bb as Ue,bj as ft,bf as tr,G as rr,a6 as ar,aV as or,aM as nr,u as f,_ as Te,v as n,y as d,x as A,ac as ir,bm as Re,L as sr,az as lr,be as dr,bn as ut,a9 as cr,A as Z,bq as br,b6 as fr,C as pt,t as ur,Q as pr,d as ht,aX as hr,z as gr,B as vr,ae as Ie,V as Me,R as te,ah as Se,b1 as Ze}from"./index-oZnp7oc3.js";import{o as mr,k as et,J as De,t as tt,y as xr,H as rt}from"./c-CGGMLMk9.js";import{z as ie,I as gt,ac as F,aK as $e,aF as je,a6 as i,s as m,q as _,e as vt,Y as Q,U as ne,M as Ge,aI as mt,aH as ce,a0 as yr,n as Y,a8 as ge,au as J,p as oe,F as U,V as he,a3 as wr,f as Cr,m as Sr}from"./c-BtX9fwA2.js";import{o as zr}from"./c-BeM9uyQQ.js";import{A as $r}from"./c-CYf_KDt7.js";import{a as Tr,C as Rr}from"./c-DjVkdi_n.js";var kr=/\s/;function Br(e){for(var r=e.length;r--&&kr.test(e.charAt(r)););return r}var Pr=/^\s+/;function Lr(e){return e&&e.slice(0,Br(e)+1).replace(Pr,"")}var at=NaN,Er=/^[-+]0x[0-9a-f]+$/i,Wr=/^0b[01]+$/i,Ar=/^0o[0-7]+$/i,_r=parseInt;function ot(e){if(typeof e=="number")return e;if(Kt(e))return at;if(ze(e)){var r=typeof e.valueOf=="function"?e.valueOf():e;e=ze(r)?r+"":r}if(typeof e!="string")return e===0?e:+e;e=Lr(e);var a=Wr.test(e);return a||Ar.test(e)?_r(e.slice(2),a?2:8):Er.test(e)?at:+e}var Fe=function(){return Jt.Date.now()},Ir="Expected a function",Mr=Math.max,Fr=Math.min;function Hr(e,r,a){var p,c,z,b,u,v,x=0,L=!1,k=!1,B=!0;if(typeof e!="function")throw new TypeError(Ir);r=ot(r)||0,ze(a)&&(L=!!a.leading,k="maxWait"in a,z=k?Mr(ot(a.maxWait)||0,r):z,B="trailing"in a?!!a.trailing:B);function T(s){var y=p,P=c;return p=c=void 0,x=s,b=e.apply(P,y),b}function w(s){return x=s,u=setTimeout(H,r),L?T(s):b}function R(s){var y=s-v,P=s-x,g=r-y;return k?Fr(g,z-P):g}function V(s){var y=s-v,P=s-x;return v===void 0||y>=r||y<0||k&&P>=z}function H(){var s=Fe();if(V(s))return q(s);u=setTimeout(H,R(s))}function q(s){return u=void 0,B&&p?T(s):(p=c=void 0,b)}function K(){u!==void 0&&clearTimeout(u),x=0,p=v=c=u=void 0}function G(){return u===void 0?b:q(Fe())}function O(){var s=Fe(),y=V(s);if(p=arguments,c=this,v=s,y){if(u===void 0)return w(v);if(k)return clearTimeout(u),u=setTimeout(H,r),T(v)}return u===void 0&&(u=setTimeout(H,r)),b}return O.cancel=K,O.flush=G,O}var Or="Expected a function";function Dr(e,r,a){var p=!0,c=!0;if(typeof e!="function")throw new TypeError(Or);return ze(a)&&(p="leading"in a?!!a.leading:p,c="trailing"in a?!!a.trailing:c),Hr(e,r,{leading:p,maxWait:r,trailing:c})}const jr=et(".v-x-scroll",{overflow:"auto",scrollbarWidth:"none"},[et("&::-webkit-scrollbar",{width:0,height:0})]),Vr=ie({name:"XScroll",props:{disabled:Boolean,onScroll:Function},setup(){const e=F(null);function r(c){!(c.currentTarget.offsetWidth<c.currentTarget.scrollWidth)||c.deltaY===0||(c.currentTarget.scrollLeft+=c.deltaY+c.deltaX,c.preventDefault())}const a=Qt();return jr.mount({id:"vueuc/x-scroll",head:!0,anchorMetaName:mr,ssr:a}),Object.assign({selfRef:e,handleWheel:r},{scrollTo(...c){var z;(z=e.value)===null||z===void 0||z.scrollTo(...c)}})},render(){return gt("div",{ref:"selfRef",onScroll:this.onScroll,onWheel:this.disabled?void 0:this.handleWheel,class:"v-x-scroll"},this.$slots)}});function Nr(e){const{modalColor:r,textColor1:a,textColor2:p,boxShadow3:c,lineHeight:z,fontWeightStrong:b,dividerColor:u,closeColorHover:v,closeColorPressed:x,closeIconColor:L,closeIconColorHover:k,closeIconColorPressed:B,borderRadius:T,primaryColorHover:w}=e;return{bodyPadding:"16px 24px",borderRadius:T,headerPadding:"16px 24px",footerPadding:"16px 24px",color:r,textColor:p,titleTextColor:a,titleFontSize:"18px",titleFontWeight:b,boxShadow:c,lineHeight:z,headerBorderBottom:`1px solid ${u}`,footerBorderTop:`1px solid ${u}`,closeIconColor:L,closeIconColorHover:k,closeIconColorPressed:B,closeSize:"22px",closeIconSize:"18px",closeColorHover:v,closeColorPressed:x,closeBorderRadius:T,resizableTriggerColorHover:w}}const Ur=dt({name:"Drawer",common:ct,peers:{Scrollbar:Zt},self:Nr}),Gr=["onMouseenter","onMouseleave","onMousedown"],Xr={key:1,role:"none"};var Yr=ie({name:"NDrawerContent",inheritAttrs:!1,props:{blockScroll:Boolean,show:{type:Boolean,default:void 0},displayDirective:{type:String,required:!0},placement:{type:String,required:!0},contentClass:String,contentStyle:[Object,String],nativeScrollbar:{type:Boolean,required:!0},scrollbarProps:Object,trapFocus:{type:Boolean,default:!0},autoFocus:{type:Boolean,default:!0},showMask:{type:[Boolean,String],required:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,onClickoutside:Function,onAfterLeave:Function,onAfterEnter:Function,onEsc:Function},setup(e){const r=F(!!e.show),a=F(null),p=Ge(Ne);let c=0,z="",b=null;const u=F(!1),v=F(!1),x=Y(()=>e.placement==="top"||e.placement==="bottom"),{mergedClsPrefixRef:L,mergedRtlRef:k}=Ue(e),B=ft("Drawer",k,L),T=s,w=g=>{v.value=!0,c=x.value?g.clientY:g.clientX,z=document.body.style.cursor,document.body.style.cursor=x.value?"ns-resize":"ew-resize",document.body.addEventListener("mousemove",O),document.body.addEventListener("mouseleave",T),document.body.addEventListener("mouseup",s)},R=()=>{b!==null&&(window.clearTimeout(b),b=null),v.value?u.value=!0:b=window.setTimeout(()=>{u.value=!0},300)},V=()=>{b!==null&&(window.clearTimeout(b),b=null),u.value=!1},{doUpdateHeight:H,doUpdateWidth:q}=p,K=g=>{const{maxWidth:j}=e;if(j&&g>j)return j;const{minWidth:I}=e;return I&&g<I?I:g},G=g=>{const{maxHeight:j}=e;if(j&&g>j)return j;const{minHeight:I}=e;return I&&g<I?I:g};function O(g){var j,I;if(v.value)if(x.value){let C=((j=a.value)==null?void 0:j.offsetHeight)||0;const E=c-g.clientY;C+=e.placement==="bottom"?E:-E,C=G(C),H(C),c=g.clientY}else{let C=((I=a.value)==null?void 0:I.offsetWidth)||0;const E=c-g.clientX;C+=e.placement==="right"?E:-E,C=K(C),q(C),c=g.clientX}}function s(){v.value&&(c=0,v.value=!1,document.body.style.cursor=z,document.body.removeEventListener("mousemove",O),document.body.removeEventListener("mouseup",s),document.body.removeEventListener("mouseleave",T))}mt(()=>{e.show&&(r.value=!0)}),ce(()=>e.show,g=>{g||s()}),yr(()=>{s()});const y=Y(()=>{const{show:g}=e,j=[[je,g]];return e.showMask||j.push([rr,e.onClickoutside,void 0,{capture:!0}]),j});function P(){var g;r.value=!1,(g=e.onAfterLeave)==null||g.call(e)}return tr(Y(()=>e.blockScroll&&r.value)),ge(ar,a),ge(or,null),ge(nr,null),{bodyRef:a,rtlEnabled:B,mergedClsPrefix:p.mergedClsPrefixRef,isMounted:p.isMountedRef,mergedTheme:p.mergedThemeRef,displayed:r,transitionName:Y(()=>({right:"slide-in-from-right-transition",left:"slide-in-from-left-transition",top:"slide-in-from-top-transition",bottom:"slide-in-from-bottom-transition"})[e.placement]),handleAfterLeave:P,bodyDirectives:y,handleMousedownResizeTrigger:w,handleMouseenterResizeTrigger:R,handleMouseleaveResizeTrigger:V,isDragging:v,isHoverOnResizeTrigger:u}},render(){const{$slots:e,mergedClsPrefix:r}=this;return this.displayDirective==="show"||this.displayed||this.show?$e((i(),m("div",Xr,[(i(),_(er,{disabled:!this.showMask||!this.trapFocus,active:this.show,autoFocus:this.autoFocus,onEsc:this.onEsc},{default:()=>(i(),_(vt,{name:this.transitionName,appear:this.isMounted,onAfterEnter:this.onAfterEnter,onAfterLeave:this.handleAfterLeave},{default:()=>$e(gt("div",ne(this.$attrs,{role:"dialog",ref:"bodyRef","aria-modal":"true",class:[`${r}-drawer`,this.rtlEnabled&&`${r}-drawer--rtl`,`${r}-drawer--${this.placement}-placement`,this.isDragging&&`${r}-drawer--unselectable`,this.nativeScrollbar&&`${r}-drawer--native-scrollbar`]}),[this.resizable?(i(),m("div",{key:2,class:$([`${r}-drawer__resize-trigger`,(this.isDragging||this.isHoverOnResizeTrigger)&&`${r}-drawer__resize-trigger--hover`]),onMouseenter:this.handleMouseenterResizeTrigger,onMouseleave:this.handleMouseleaveResizeTrigger,onMousedown:this.handleMousedownResizeTrigger},null,42,Gr)):null,this.nativeScrollbar?(i(),m("div",{key:3,class:$([`${r}-drawer-content-wrapper`,this.contentClass]),style:Q(this.contentStyle),role:"none"},[S(()=>{var a;return(a=e.default)==null?void 0:a.call(e)})],6)):(i(),_(bt,ne({key:4},this.scrollbarProps,{contentStyle:this.contentStyle,contentClass:[`${r}-drawer-content-wrapper`,this.contentClass],theme:this.mergedTheme.peers.Scrollbar,themeOverrides:this.mergedTheme.peerOverrides.Scrollbar}),ve(e),1040,["contentStyle","contentClass","theme","themeOverrides"]))]),this.bodyDirectives)},1032,["name","appear","onAfterEnter","onAfterLeave"]))},1032,["disabled","active","autoFocus","onEsc"]))])),[[je,this.displayDirective==="if"||this.displayed||this.show]]):null}});const{cubicBezierEaseIn:qr,cubicBezierEaseOut:Kr}=Te;function Jr({duration:e="0.3s",leaveDuration:r="0.2s",name:a="slide-in-from-bottom"}={}){return[f(`&.${a}-transition-leave-active`,{transition:`transform ${r} ${qr}`}),f(`&.${a}-transition-enter-active`,{transition:`transform ${e} ${Kr}`}),f(`&.${a}-transition-enter-to`,{transform:"translateY(0)"}),f(`&.${a}-transition-enter-from`,{transform:"translateY(100%)"}),f(`&.${a}-transition-leave-from`,{transform:"translateY(0)"}),f(`&.${a}-transition-leave-to`,{transform:"translateY(100%)"})]}const{cubicBezierEaseIn:Qr,cubicBezierEaseOut:Zr}=Te;function ea({duration:e="0.3s",leaveDuration:r="0.2s",name:a="slide-in-from-left"}={}){return[f(`&.${a}-transition-leave-active`,{transition:`transform ${r} ${Qr}`}),f(`&.${a}-transition-enter-active`,{transition:`transform ${e} ${Zr}`}),f(`&.${a}-transition-enter-to`,{transform:"translateX(0)"}),f(`&.${a}-transition-enter-from`,{transform:"translateX(-100%)"}),f(`&.${a}-transition-leave-from`,{transform:"translateX(0)"}),f(`&.${a}-transition-leave-to`,{transform:"translateX(-100%)"})]}const{cubicBezierEaseIn:ta,cubicBezierEaseOut:ra}=Te;function aa({duration:e="0.3s",leaveDuration:r="0.2s",name:a="slide-in-from-right"}={}){return[f(`&.${a}-transition-leave-active`,{transition:`transform ${r} ${ta}`}),f(`&.${a}-transition-enter-active`,{transition:`transform ${e} ${ra}`}),f(`&.${a}-transition-enter-to`,{transform:"translateX(0)"}),f(`&.${a}-transition-enter-from`,{transform:"translateX(100%)"}),f(`&.${a}-transition-leave-from`,{transform:"translateX(0)"}),f(`&.${a}-transition-leave-to`,{transform:"translateX(100%)"})]}const{cubicBezierEaseIn:oa,cubicBezierEaseOut:na}=Te;function ia({duration:e="0.3s",leaveDuration:r="0.2s",name:a="slide-in-from-top"}={}){return[f(`&.${a}-transition-leave-active`,{transition:`transform ${r} ${oa}`}),f(`&.${a}-transition-enter-active`,{transition:`transform ${e} ${na}`}),f(`&.${a}-transition-enter-to`,{transform:"translateY(0)"}),f(`&.${a}-transition-enter-from`,{transform:"translateY(-100%)"}),f(`&.${a}-transition-leave-from`,{transform:"translateY(0)"}),f(`&.${a}-transition-leave-to`,{transform:"translateY(-100%)"})]}var sa=f([n("drawer",`
 word-break: break-word;
 line-height: var(--n-line-height);
 position: absolute;
 pointer-events: all;
 box-shadow: var(--n-box-shadow);
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 background-color: var(--n-color);
 color: var(--n-text-color);
 box-sizing: border-box;
 `,[aa(),ea(),ia(),Jr(),d("unselectable",`
 user-select: none; 
 -webkit-user-select: none;
 `),d("native-scrollbar",[n("drawer-content-wrapper",`
 overflow: auto;
 height: 100%;
 `)]),A("resize-trigger",`
 position: absolute;
 background-color: #0000;
 transition: background-color .3s var(--n-bezier);
 `,[d("hover",`
 background-color: var(--n-resize-trigger-color-hover);
 `)]),n("drawer-content-wrapper",`
 box-sizing: border-box;
 `),n("drawer-content",`
 height: 100%;
 display: flex;
 flex-direction: column;
 `,[d("native-scrollbar",[n("drawer-body-content-wrapper",`
 height: 100%;
 overflow: auto;
 `)]),n("drawer-body",`
 flex: 1 0 0;
 overflow: hidden;
 `),n("drawer-body-content-wrapper",`
 box-sizing: border-box;
 padding: var(--n-body-padding);
 `),n("drawer-header",`
 font-weight: var(--n-title-font-weight);
 line-height: 1;
 font-size: var(--n-title-font-size);
 color: var(--n-title-text-color);
 padding: var(--n-header-padding);
 transition: border .3s var(--n-bezier);
 border-bottom: 1px solid var(--n-divider-color);
 border-bottom: var(--n-header-border-bottom);
 display: flex;
 justify-content: space-between;
 align-items: center;
 `,[A("main",`
 flex: 1;
 `),A("close",`
 margin-left: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `)]),n("drawer-footer",`
 display: flex;
 justify-content: flex-end;
 border-top: var(--n-footer-border-top);
 transition: border .3s var(--n-bezier);
 padding: var(--n-footer-padding);
 `)]),d("right-placement",`
 top: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-bottom-left-radius: var(--n-border-radius);
 `,[A("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 left: 0;
 transform: translateX(-1.5px);
 cursor: ew-resize;
 `)]),d("left-placement",`
 top: 0;
 bottom: 0;
 left: 0;
 border-top-right-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[A("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 right: 0;
 transform: translateX(1.5px);
 cursor: ew-resize;
 `)]),d("top-placement",`
 top: 0;
 left: 0;
 right: 0;
 border-bottom-left-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[A("resize-trigger",`
 width: 100%;
 height: 3px;
 bottom: 0;
 left: 0;
 transform: translateY(1.5px);
 cursor: ns-resize;
 `)]),d("bottom-placement",`
 left: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-top-right-radius: var(--n-border-radius);
 `,[A("resize-trigger",`
 width: 100%;
 height: 3px;
 top: 0;
 left: 0;
 transform: translateY(-1.5px);
 cursor: ns-resize;
 `)])]),f("body",[f(">",[n("drawer-container",`
 position: fixed;
 `)])]),n("drawer-container",`
 position: relative;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 pointer-events: none;
 `,[f("> *",`
 pointer-events: all;
 `)]),n("drawer-mask",`
 background-color: rgba(0, 0, 0, .3);
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 `,[d("invisible",`
 background-color: rgba(0, 0, 0, 0)
 `),ir({enterDuration:"0.2s",leaveDuration:"0.2s",enterCubicBezier:"var(--n-bezier-in)",leaveCubicBezier:"var(--n-bezier-out)"})])]);const la=["onClick"],da={...Re.props,show:Boolean,width:[Number,String],height:[Number,String],placement:{type:String,default:"right"},maskClosable:{type:Boolean,default:!0},showMask:{type:[Boolean,String],default:!0},to:[String,Object],displayDirective:{type:String,default:"if"},nativeScrollbar:{type:Boolean,default:!0},zIndex:Number,onMaskClick:Function,scrollbarProps:Object,contentClass:String,contentStyle:[Object,String],trapFocus:{type:Boolean,default:!0},onEsc:Function,autoFocus:{type:Boolean,default:!0},closeOnEsc:{type:Boolean,default:!0},blockScroll:{type:Boolean,default:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,defaultWidth:{type:[Number,String],default:251},defaultHeight:{type:[Number,String],default:251},onUpdateWidth:[Function,Array],onUpdateHeight:[Function,Array],"onUpdate:width":[Function,Array],"onUpdate:height":[Function,Array],"onUpdate:show":[Function,Array],onUpdateShow:[Function,Array],onAfterEnter:Function,onAfterLeave:Function,drawerStyle:[String,Object],drawerClass:String,target:null,onShow:Function,onHide:Function};var $a=ie({name:"Drawer",inheritAttrs:!1,props:da,setup(e){const{mergedClsPrefixRef:r,namespaceRef:a,inlineThemeDisabled:p}=Ue(e),c=lr(),z=Re("Drawer","-drawer",sa,Ur,e,r),b=F(e.defaultWidth),u=F(e.defaultHeight),v=De(J(e,"width"),b),x=De(J(e,"height"),u),L=Y(()=>{const{placement:s}=e;return s==="top"||s==="bottom"?"":tt(v.value)}),k=Y(()=>{const{placement:s}=e;return s==="left"||s==="right"?"":tt(x.value)}),B=s=>{const{onUpdateWidth:y,"onUpdate:width":P}=e;y&&Z(y,s),P&&Z(P,s),b.value=s},T=s=>{const{onUpdateHeight:y,"onUpdate:width":P}=e;y&&Z(y,s),P&&Z(P,s),u.value=s},w=Y(()=>[{width:L.value,height:k.value},e.drawerStyle||""]);function R(s){const{onMaskClick:y,maskClosable:P}=e;P&&K(!1),y&&y(s)}function V(s){R(s)}const H=dr();function q(s){var y;(y=e.onEsc)==null||y.call(e),e.show&&e.closeOnEsc&&cr(s)&&(H.value||K(!1))}function K(s){const{onHide:y,onUpdateShow:P,"onUpdate:show":g}=e;P&&Z(P,s),g&&Z(g,s),y&&!s&&Z(y,s)}ge(Ne,{isMountedRef:c,mergedThemeRef:z,mergedClsPrefixRef:r,doUpdateShow:K,doUpdateHeight:T,doUpdateWidth:B});const G=Y(()=>{const{common:{cubicBezierEaseInOut:s,cubicBezierEaseIn:y,cubicBezierEaseOut:P},self:{color:g,textColor:j,boxShadow:I,lineHeight:C,headerPadding:E,footerPadding:re,borderRadius:be,bodyPadding:se,titleFontSize:fe,titleTextColor:ee,titleFontWeight:ke,headerBorderBottom:Be,footerBorderTop:Pe,closeIconColor:me,closeIconColorHover:xe,closeIconColorPressed:Le,closeColorHover:Ee,closeColorPressed:We,closeIconSize:Ae,closeSize:ue,closeBorderRadius:pe,resizableTriggerColorHover:ae}}=z.value;return{"--n-line-height":C,"--n-color":g,"--n-border-radius":be,"--n-text-color":j,"--n-box-shadow":I,"--n-bezier":s,"--n-bezier-out":P,"--n-bezier-in":y,"--n-header-padding":E,"--n-body-padding":se,"--n-footer-padding":re,"--n-title-text-color":ee,"--n-title-font-size":fe,"--n-title-font-weight":ke,"--n-header-border-bottom":Be,"--n-footer-border-top":Pe,"--n-close-icon-color":me,"--n-close-icon-color-hover":xe,"--n-close-icon-color-pressed":Le,"--n-close-size":ue,"--n-close-color-hover":Ee,"--n-close-color-pressed":We,"--n-close-icon-size":Ae,"--n-close-border-radius":pe,"--n-resize-trigger-color-hover":ae}}),O=p?ut("drawer",void 0,G,e):void 0;return{mergedClsPrefix:r,namespace:a,mergedBodyStyle:w,handleOutsideClick:V,handleMaskClick:R,handleEsc:q,mergedTheme:z,cssVars:p?void 0:G,themeClass:O==null?void 0:O.themeClass,onRender:O==null?void 0:O.onRender,isMounted:c}},render(){const{mergedClsPrefix:e}=this;return i(),_(sr,{to:this.to,show:this.show},{default:()=>{var r;return(r=this.onRender)==null||r.call(this),$e((i(),m("div",{class:$([`${e}-drawer-container`,this.namespace,this.themeClass]),style:Q(this.cssVars),role:"none"},[this.showMask?(i(),_(vt,{key:0,name:"fade-in-transition",appear:this.isMounted},{default:()=>this.show?(i(),m("div",{key:1,"aria-hidden":!0,class:$([`${e}-drawer-mask`,this.showMask==="transparent"&&`${e}-drawer-mask--invisible`]),onClick:this.handleMaskClick},null,10,la)):null},1032,["appear"])):S(()=>null),(i(),_(Yr,ne(this.$attrs,{class:[this.drawerClass,this.$attrs.class],style:[this.mergedBodyStyle,this.$attrs.style],blockScroll:this.blockScroll,contentStyle:this.contentStyle,contentClass:this.contentClass,placement:this.placement,scrollbarProps:this.scrollbarProps,show:this.show,displayDirective:this.displayDirective,nativeScrollbar:this.nativeScrollbar,onAfterEnter:this.onAfterEnter,onAfterLeave:this.onAfterLeave,trapFocus:this.trapFocus,autoFocus:this.autoFocus,resizable:this.resizable,maxHeight:this.maxHeight,minHeight:this.minHeight,maxWidth:this.maxWidth,minWidth:this.minWidth,showMask:this.showMask,onEsc:this.handleEsc,onClickoutside:this.handleOutsideClick}),ve(this.$slots),1040,["class","style","blockScroll","contentStyle","contentClass","placement","scrollbarProps","show","displayDirective","nativeScrollbar","onAfterEnter","onAfterLeave","trapFocus","autoFocus","resizable","maxHeight","minHeight","maxWidth","minWidth","showMask","onEsc","onClickoutside"]))],6)),[[br,{zIndex:this.zIndex,enabled:this.show}]])}},1032,["to","show"])}});const ca={title:String,headerClass:String,headerStyle:[Object,String],footerClass:String,footerStyle:[Object,String],bodyClass:String,bodyStyle:[Object,String],bodyContentClass:String,bodyContentStyle:[Object,String],nativeScrollbar:{type:Boolean,default:!0},scrollbarProps:Object,closable:Boolean};var Ta=ie({name:"DrawerContent",props:ca,slots:Object,setup(){const e=Ge(Ne,null);e||fr("drawer-content","`n-drawer-content` must be placed inside `n-drawer`.");const{doUpdateShow:r}=e;function a(){r(!1)}return{handleCloseClick:a,mergedTheme:e.mergedThemeRef,mergedClsPrefix:e.mergedClsPrefixRef}},render(){const{title:e,mergedClsPrefix:r,nativeScrollbar:a,mergedTheme:p,bodyClass:c,bodyStyle:z,bodyContentClass:b,bodyContentStyle:u,headerClass:v,headerStyle:x,footerClass:L,footerStyle:k,scrollbarProps:B,closable:T,$slots:w}=this;return i(),m("div",{role:"none",class:$([`${r}-drawer-content`,a&&`${r}-drawer-content--native-scrollbar`])},[w.header||e||T?(i(),m("div",{key:0,class:$([`${r}-drawer-header`,v]),style:Q(x),role:"none"},[oe("div",{class:$(`${r}-drawer-header__main`),role:"heading","aria-level":"1"},[w.header!==void 0?(i(),m(U,{key:0},[S(()=>w.header())],64)):(i(),m(U,{key:1},[S(()=>e)],64))],2),S(()=>T&&(i(),_(pt,{onClick:this.handleCloseClick,clsPrefix:r,class:$(`${r}-drawer-header__close`),absolute:!0},null,8,["onClick","clsPrefix","class"])))],6)):S(()=>null),a?(i(),m("div",{key:2,class:$([`${r}-drawer-body`,c]),style:Q(z),role:"none"},[oe("div",{class:$([`${r}-drawer-body-content-wrapper`,b]),style:Q(u),role:"none"},[S(()=>{var R;return(R=w.default)==null?void 0:R.call(w)})],6)],6)):(i(),_(bt,ne({key:3,themeOverrides:p.peerOverrides.Scrollbar,theme:p.peers.Scrollbar},B,{class:`${r}-drawer-body`,contentClass:[`${r}-drawer-body-content-wrapper`,b],contentStyle:u}),ve(w),1040,["themeOverrides","theme","class","contentClass","contentStyle"])),w.footer?(i(),m("div",{key:4,class:$([`${r}-drawer-footer`,L]),style:Q(k),role:"none"},[S(()=>w.footer())],6)):S(()=>null)],2)}}),ba={tabFontSizeSmall:"14px",tabFontSizeMedium:"14px",tabFontSizeLarge:"16px",tabGapSmallLine:"36px",tabGapMediumLine:"36px",tabGapLargeLine:"36px",tabGapSmallLineVertical:"8px",tabGapMediumLineVertical:"8px",tabGapLargeLineVertical:"8px",tabPaddingSmallLine:"6px 0",tabPaddingMediumLine:"10px 0",tabPaddingLargeLine:"14px 0",tabPaddingVerticalSmallLine:"6px 12px",tabPaddingVerticalMediumLine:"8px 16px",tabPaddingVerticalLargeLine:"10px 20px",tabGapSmallBar:"36px",tabGapMediumBar:"36px",tabGapLargeBar:"36px",tabGapSmallBarVertical:"8px",tabGapMediumBarVertical:"8px",tabGapLargeBarVertical:"8px",tabPaddingSmallBar:"4px 0",tabPaddingMediumBar:"6px 0",tabPaddingLargeBar:"10px 0",tabPaddingVerticalSmallBar:"6px 12px",tabPaddingVerticalMediumBar:"8px 16px",tabPaddingVerticalLargeBar:"10px 20px",tabGapSmallCard:"4px",tabGapMediumCard:"4px",tabGapLargeCard:"4px",tabGapSmallCardVertical:"4px",tabGapMediumCardVertical:"4px",tabGapLargeCardVertical:"4px",tabPaddingSmallCard:"8px 16px",tabPaddingMediumCard:"10px 20px",tabPaddingLargeCard:"12px 24px",tabPaddingSmallSegment:"4px 0",tabPaddingMediumSegment:"6px 0",tabPaddingLargeSegment:"8px 0",tabPaddingVerticalLargeSegment:"0 8px",tabPaddingVerticalSmallCard:"8px 12px",tabPaddingVerticalMediumCard:"10px 16px",tabPaddingVerticalLargeCard:"12px 20px",tabPaddingVerticalSmallSegment:"0 4px",tabPaddingVerticalMediumSegment:"0 6px",tabGapSmallSegment:"0",tabGapMediumSegment:"0",tabGapLargeSegment:"0",tabGapSmallSegmentVertical:"0",tabGapMediumSegmentVertical:"0",tabGapLargeSegmentVertical:"0",panePaddingSmall:"8px 0 0 0",panePaddingMedium:"12px 0 0 0",panePaddingLarge:"16px 0 0 0",closeSize:"18px",closeIconSize:"14px"};function fa(e){const{textColor2:r,primaryColor:a,textColorDisabled:p,closeIconColor:c,closeIconColorHover:z,closeIconColorPressed:b,closeColorHover:u,closeColorPressed:v,tabColor:x,baseColor:L,dividerColor:k,fontWeight:B,textColor1:T,borderRadius:w,fontSize:R,fontWeightStrong:V}=e;return{...ba,colorSegment:x,tabFontSizeCard:R,tabTextColorLine:T,tabTextColorActiveLine:a,tabTextColorHoverLine:a,tabTextColorDisabledLine:p,tabTextColorSegment:T,tabTextColorActiveSegment:r,tabTextColorHoverSegment:r,tabTextColorDisabledSegment:p,tabTextColorBar:T,tabTextColorActiveBar:a,tabTextColorHoverBar:a,tabTextColorDisabledBar:p,tabTextColorCard:T,tabTextColorHoverCard:T,tabTextColorActiveCard:a,tabTextColorDisabledCard:p,barColor:a,closeIconColor:c,closeIconColorHover:z,closeIconColorPressed:b,closeColorHover:u,closeColorPressed:v,closeBorderRadius:w,tabColor:x,tabColorSegment:L,tabBorderColor:k,tabFontWeightActive:B,tabFontWeight:B,tabBorderRadius:w,paneTextColor:r,fontWeightStrong:V}}const ua=dt({name:"Tabs",common:ct,peers:{Button:ur},self:fa}),xt=pr("n-tabs"),pa={tab:[String,Number,Object,Function],name:{type:[String,Number],required:!0},disabled:Boolean,displayDirective:{type:String,default:"if"},closable:{type:Boolean,default:void 0},tabProps:Object,label:[String,Number,Object,Function]},ha=["data-name","data-disabled"],ga={internalLeftPadded:Boolean,internalAddable:Boolean,internalCreatedByPane:Boolean,...zr(pa,["displayDirective"])};var Ve=ie({__TAB__:!0,inheritAttrs:!1,name:"Tab",props:ga,setup(e){const{mergedClsPrefixRef:r,valueRef:a,typeRef:p,closableRef:c,tabStyleRef:z,addTabStyleRef:b,tabClassRef:u,addTabClassRef:v,tabChangeIdRef:x,onBeforeLeaveRef:L,triggerRef:k,handleAdd:B,activateTab:T,handleClose:w}=Ge(xt);return{trigger:k,mergedClosable:Y(()=>{if(e.internalAddable)return!1;const{closable:R}=e;return R===void 0?c.value:R}),style:z,addStyle:b,tabClass:u,addTabClass:v,clsPrefix:r,value:a,type:p,handleClose(R){R.stopPropagation(),!e.disabled&&w(e.name)},activateTab(){if(e.disabled)return;if(e.internalAddable){B();return}const{name:R}=e,V=++x.id;if(R!==a.value){const{value:H}=L;H?Promise.resolve(H(e.name,a.value)).then(q=>{q&&x.id===V&&T(R)}):T(R)}}}},render(){const{internalAddable:e,clsPrefix:r,name:a,disabled:p,label:c,tab:z,value:b,mergedClosable:u,trigger:v,$slots:{default:x}}=this,L=c??z;return i(),m("div",{class:$(`${r}-tabs-tab-wrapper`)},[this.internalLeftPadded?(i(),m("div",{key:0,class:$(`${r}-tabs-tab-pad`)},null,2)):S(()=>null),(i(),m("div",ne({key:a,"data-name":a,"data-disabled":p?!0:void 0},ne({class:[`${r}-tabs-tab`,b===a&&`${r}-tabs-tab--active`,p&&`${r}-tabs-tab--disabled`,u&&`${r}-tabs-tab--closable`,e&&`${r}-tabs-tab--addable`,e?this.addTabClass:this.tabClass],onClick:v==="click"?this.activateTab:void 0,onMouseenter:v==="hover"?this.activateTab:void 0,style:e?this.addStyle:this.style},this.internalCreatedByPane?this.tabProps||{}:this.$attrs)),[oe("span",{class:$(`${r}-tabs-tab__label`)},[e?(i(),m(U,{key:0},[oe("div",{class:$(`${r}-tabs-tab__height-placeholder`)}," ",2),(i(),_(ht,{clsPrefix:r},{default:()=>(i(),_($r))},1032,["clsPrefix"]))],64)):(i(),m(U,{key:1},[x?(i(),m(U,{key:0},[S(()=>x())],64)):(i(),m(U,{key:1},[typeof L=="object"?(i(),m(U,{key:0},[S(()=>L)],64)):(i(),m(U,{key:1},[S(()=>hr(L??a))],64))],64))],64))],2),u&&this.type==="card"?(i(),_(pt,{key:0,clsPrefix:r,class:$(`${r}-tabs-tab__close`),onClick:this.handleClose,disabled:p},null,8,["clsPrefix","class","onClick","disabled"])):S(()=>null)],16,ha))],2)}}),va=n("tabs",`
 box-sizing: border-box;
 width: 100%;
 display: flex;
 flex-direction: column;
 transition:
 background-color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
`,[f("&.transition-disabled",[n("tabs-tab",`
 transition: none !important;
 `),n("tabs-nav-scroll-content",`
 transition: none !important;
 `),n("tabs-tab-pad",`
 transition: none !important;
 `)]),d("segment-type",[n("tabs-rail",[f("&.transition-disabled",[n("tabs-capsule",`
 transition: none;
 `)])])]),d("top",[n("tab-pane",`
 padding: var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left);
 `)]),d("left",[n("tab-pane",`
 padding: var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left) var(--n-pane-padding-top);
 `)]),d("left, right",`
 flex-direction: row;
 `,[n("tabs-bar",`
 width: 2px;
 right: 0;
 transition:
 top .2s var(--n-bezier),
 max-height .2s var(--n-bezier),
 background-color .3s var(--n-bezier);
 `),n("tabs-tab",`
 padding: var(--n-tab-padding-vertical); 
 `)]),d("right",`
 flex-direction: row-reverse;
 `,[n("tab-pane",`
 padding: var(--n-pane-padding-left) var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom);
 `),n("tabs-bar",`
 left: 0;
 `)]),d("bottom",`
 flex-direction: column-reverse;
 justify-content: flex-end;
 `,[n("tab-pane",`
 padding: var(--n-pane-padding-bottom) var(--n-pane-padding-right) var(--n-pane-padding-top) var(--n-pane-padding-left);
 `),n("tabs-bar",`
 top: 0;
 `)]),n("tabs-rail",`
 position: relative;
 padding: 3px;
 border-radius: var(--n-tab-border-radius);
 width: 100%;
 background-color: var(--n-color-segment);
 transition: background-color .3s var(--n-bezier);
 display: flex;
 align-items: center;
 `,[n("tabs-capsule",`
 border-radius: var(--n-tab-border-radius);
 position: absolute;
 left: 0;
 top: 0;
 pointer-events: none;
 background-color: var(--n-tab-color-segment);
 box-shadow: 0 1px 3px 0 rgba(0, 0, 0, .08);
 transition: transform 0.3s var(--n-bezier);
 `),n("tabs-tab-wrapper",`
 flex-basis: 0;
 flex-grow: 1;
 display: flex;
 align-items: center;
 justify-content: center;
 `,[n("tabs-tab",`
 overflow: hidden;
 border-radius: var(--n-tab-border-radius);
 width: 100%;
 display: flex;
 align-items: center;
 justify-content: center;
 `,[d("active",`
 font-weight: var(--n-font-weight-strong);
 color: var(--n-tab-text-color-active);
 `),f("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])])]),d("flex",[n("tabs-nav",`
 width: 100%;
 position: relative;
 `,[n("tabs-wrapper",`
 width: 100%;
 `,[n("tabs-tab",`
 margin-right: 0;
 `)])])]),n("tabs-nav",`
 box-sizing: border-box;
 line-height: 1.5;
 display: flex;
 transition: border-color .3s var(--n-bezier);
 `,[A("prefix, suffix",`
 display: flex;
 align-items: center;
 `),A("prefix","padding-right: 16px;"),A("suffix","padding-left: 16px;")]),d("top, bottom",[f(">",[n("tabs-nav",[n("tabs-nav-scroll-wrapper",[f("&::before",`
 top: 0;
 bottom: 0;
 left: 0;
 width: 20px;
 `),f("&::after",`
 top: 0;
 bottom: 0;
 right: 0;
 width: 20px;
 `),d("shadow-start",[f("&::before",`
 box-shadow: inset 10px 0 8px -8px rgba(0, 0, 0, .12);
 `)]),d("shadow-end",[f("&::after",`
 box-shadow: inset -10px 0 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),d("left, right",[n("tabs-nav-scroll-content",`
 flex-direction: column;
 `),f(">",[n("tabs-nav",[n("tabs-nav-scroll-wrapper",[f("&::before",`
 top: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),f("&::after",`
 bottom: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),d("shadow-start",[f("&::before",`
 box-shadow: inset 0 10px 8px -8px rgba(0, 0, 0, .12);
 `)]),d("shadow-end",[f("&::after",`
 box-shadow: inset 0 -10px 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),n("tabs-nav-scroll-wrapper",`
 flex: 1;
 position: relative;
 overflow: hidden;
 `,[n("tabs-nav-y-scroll",`
 height: 100%;
 width: 100%;
 overflow-y: auto; 
 scrollbar-width: none;
 `,[f("&::-webkit-scrollbar, &::-webkit-scrollbar-track-piece, &::-webkit-scrollbar-thumb",`
 width: 0;
 height: 0;
 display: none;
 `)]),f("&::before, &::after",`
 transition: box-shadow .3s var(--n-bezier);
 pointer-events: none;
 content: "";
 position: absolute;
 z-index: 1;
 `),f("&.transition-disabled",[f("&::before, &::after",`
 transition: none;
 `)])]),n("tabs-nav-scroll-content",`
 display: flex;
 position: relative;
 min-width: 100%;
 min-height: 100%;
 width: fit-content;
 box-sizing: border-box;
 `),n("tabs-wrapper",`
 display: inline-flex;
 flex-wrap: nowrap;
 position: relative;
 `),n("tabs-tab-wrapper",`
 display: flex;
 flex-wrap: nowrap;
 flex-shrink: 0;
 flex-grow: 0;
 `),n("tabs-tab",`
 cursor: pointer;
 white-space: nowrap;
 flex-wrap: nowrap;
 display: inline-flex;
 align-items: center;
 color: var(--n-tab-text-color);
 font-size: var(--n-tab-font-size);
 background-clip: padding-box;
 padding: var(--n-tab-padding);
 transition:
 box-shadow .3s var(--n-bezier),
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 `,[d("disabled",{cursor:"not-allowed"}),A("close",`
 margin-inline-start: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `),A("label",`
 display: flex;
 align-items: center;
 z-index: 1;
 `)]),n("tabs-bar",`
 position: absolute;
 bottom: 0;
 height: 2px;
 border-radius: 1px;
 background-color: var(--n-bar-color);
 transition:
 left .2s var(--n-bezier),
 max-width .2s var(--n-bezier),
 opacity .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 `,[f("&.transition-disabled",`
 transition: none;
 `),d("disabled",`
 background-color: var(--n-tab-text-color-disabled)
 `)]),n("tabs-pane-wrapper",`
 position: relative;
 overflow: hidden;
 transition: max-height .2s var(--n-bezier);
 `),n("tab-pane",`
 color: var(--n-pane-text-color);
 width: 100%;
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 opacity .2s var(--n-bezier);
 left: 0;
 right: 0;
 top: 0;
 `,[f("&.next-transition-leave-active, &.prev-transition-leave-active, &.next-transition-enter-active, &.prev-transition-enter-active",`
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 transform .2s var(--n-bezier),
 opacity .2s var(--n-bezier);
 `),f("&.next-transition-leave-active, &.prev-transition-leave-active",`
 position: absolute;
 `),f("&.next-transition-enter-from, &.prev-transition-leave-to",`
 transform: translateX(32px);
 opacity: 0;
 `),f("&.next-transition-leave-to, &.prev-transition-enter-from",`
 transform: translateX(-32px);
 opacity: 0;
 `),f("&.next-transition-leave-from, &.next-transition-enter-to, &.prev-transition-leave-from, &.prev-transition-enter-to",`
 transform: translateX(0);
 opacity: 1;
 `)]),n("tabs-tab-pad",`
 box-sizing: border-box;
 width: var(--n-tab-gap);
 flex-grow: 0;
 flex-shrink: 0;
 `),d("line-type, bar-type",[n("tabs-tab",`
 font-weight: var(--n-tab-font-weight);
 box-sizing: border-box;
 vertical-align: bottom;
 `,[f("&:hover",{color:"var(--n-tab-text-color-hover)"}),d("active",`
 color: var(--n-tab-text-color-active);
 font-weight: var(--n-tab-font-weight-active);
 `),d("disabled",{color:"var(--n-tab-text-color-disabled)"})])]),n("tabs-nav",[A("prefix, suffix",`
 border-color: var(--n-tab-border-color);
 `),n("tabs-nav-scroll-content",`
 border-color: var(--n-tab-border-color);
 `),d("line-type",[d("top",[A("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),n("tabs-nav-scroll-content",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),n("tabs-bar",`
 bottom: -1px;
 `)]),d("left",[A("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),n("tabs-nav-scroll-content",`
 border-right: 1px solid var(--n-tab-border-color);
 `),n("tabs-bar",`
 right: -1px;
 `)]),d("right",[A("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),n("tabs-nav-scroll-content",`
 border-left: 1px solid var(--n-tab-border-color);
 `),n("tabs-bar",`
 left: -1px;
 `)]),d("bottom",[A("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),n("tabs-nav-scroll-content",`
 border-top: 1px solid var(--n-tab-border-color);
 `),n("tabs-bar",`
 top: -1px;
 `)]),A("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),n("tabs-nav-scroll-content",`
 transition: border-color .3s var(--n-bezier);
 `),n("tabs-bar",`
 border-radius: 0;
 `)]),d("card-type",[A("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),n("tabs-pad",`
 flex-grow: 1;
 transition: border-color .3s var(--n-bezier);
 `),n("tabs-tab-pad",`
 transition: border-color .3s var(--n-bezier);
 `),n("tabs-tab",`
 font-weight: var(--n-tab-font-weight);
 border: 1px solid var(--n-tab-border-color);
 background-color: var(--n-tab-color);
 box-sizing: border-box;
 position: relative;
 vertical-align: bottom;
 display: flex;
 justify-content: space-between;
 font-size: var(--n-tab-font-size);
 color: var(--n-tab-text-color);
 `,[d("addable",`
 padding-left: 8px;
 padding-right: 8px;
 font-size: 16px;
 justify-content: center;
 `,[A("height-placeholder",`
 width: 0;
 font-size: var(--n-tab-font-size);
 `),gr("disabled",[f("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])]),d("closable","padding-inline-end: 8px;"),d("active",`
 background-color: #0000;
 font-weight: var(--n-tab-font-weight-active);
 color: var(--n-tab-text-color-active);
 `),d("disabled","color: var(--n-tab-text-color-disabled);")])]),d("left, right",`
 flex-direction: column; 
 `,[A("prefix, suffix",`
 padding: var(--n-tab-padding-vertical);
 `),n("tabs-wrapper",`
 flex-direction: column;
 `),n("tabs-tab-wrapper",`
 flex-direction: column;
 `,[n("tabs-tab-pad",`
 height: var(--n-tab-gap-vertical);
 width: 100%;
 `)])]),d("top",[d("card-type",[n("tabs-scroll-padding","border-bottom: 1px solid var(--n-tab-border-color);"),A("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),n("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-top-right-radius: var(--n-tab-border-radius);
 `,[d("active",`
 border-bottom: 1px solid #0000;
 `)]),n("tabs-tab-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),n("tabs-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `)])]),d("left",[d("card-type",[n("tabs-scroll-padding","border-right: 1px solid var(--n-tab-border-color);"),A("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),n("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-bottom-left-radius: var(--n-tab-border-radius);
 `,[d("active",`
 border-right: 1px solid #0000;
 `)]),n("tabs-tab-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `),n("tabs-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `)])]),d("right",[d("card-type",[n("tabs-scroll-padding","border-left: 1px solid var(--n-tab-border-color);"),A("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),n("tabs-tab",`
 border-top-right-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[d("active",`
 border-left: 1px solid #0000;
 `)]),n("tabs-tab-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `),n("tabs-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `)])]),d("bottom",[d("card-type",[n("tabs-scroll-padding","border-top: 1px solid var(--n-tab-border-color);"),A("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),n("tabs-tab",`
 border-bottom-left-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[d("active",`
 border-top: 1px solid #0000;
 `)]),n("tabs-tab-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `),n("tabs-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `)])])]),n("tabs-scroll-button",[d("start",`
 padding-left: 10px;
 padding-right: 6px;
 `),d("end",`
 padding-right: 10px;
 padding-left: 6px;
 `),d("up",`
 padding-bottom: 10px;
 `),d("down",`
 padding-top: 10px;
 `)])]),nt=ie({name:"TabsButton",props:{type:{type:String,default:"next"},mergedClsPrefix:{type:String,required:!0},vertical:Boolean,disabled:Boolean,rtl:Boolean,theme:Object,themeOverrides:Object,onClick:Function},setup(e){return{handleClick:()=>{var a;e.disabled||(a=e.onClick)==null||a.call(e,e.type)}}},render(){const{mergedClsPrefix:e,disabled:r,type:a,vertical:p,rtl:c,theme:z,themeOverrides:b,handleClick:u}=this,v=a==="next",x=p?v:c?!v:v;return i(),_(vr,{text:!0,disabled:r,size:"small",theme:z,themeOverrides:b,onClick:u,class:$([`${e}-tabs-scroll-button`,!p&&a==="prev"&&`${e}-tabs-scroll-button--start`,!p&&a==="next"&&`${e}-tabs-scroll-button--end`,p&&a==="prev"&&`${e}-tabs-scroll-button--up`,p&&a==="next"&&`${e}-tabs-scroll-button--down`])},{icon:()=>(i(),_(ht,{clsPrefix:e,style:Q(p?{transform:"rotate(90deg)"}:void 0)},{default:()=>x?(i(),_(Tr,{key:1})):(i(),_(Rr,{key:2}))},1032,["clsPrefix","style"]))},1032,["disabled","theme","themeOverrides","onClick","class"])}});const He=Dr,ma={...Re.props,value:[String,Number],defaultValue:[String,Number],trigger:{type:String,default:"click"},type:{type:String,default:"bar"},closable:Boolean,justifyContent:String,size:String,placement:{type:String,default:"top"},tabStyle:[String,Object],tabClass:String,addTabStyle:[String,Object],addTabClass:String,barWidth:Number,paneClass:String,paneStyle:[String,Object],paneWrapperClass:String,paneWrapperStyle:[String,Object],addable:[Boolean,Object],tabsPadding:{type:Number,default:0},animated:Boolean,onBeforeLeave:Function,onAdd:Function,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onClose:[Function,Array],labelSize:String,activeName:[String,Number],onActiveNameChange:[Function,Array],showScrollButton:Boolean,centerActiveTab:Boolean};var Ra=ie({name:"Tabs",props:ma,slots:Object,setup(e,{slots:r}){var Je,Qe;const{mergedClsPrefixRef:a,inlineThemeDisabled:p,mergedComponentPropsRef:c,mergedRtlRef:z}=Ue(e),b=ft("Tabs",z,a),u=Y(()=>{const{placement:t}=e;return t==="start"?b!=null&&b.value?"right":"left":t==="end"?b!=null&&b.value?"left":"right":t}),v=Re("Tabs","-tabs",va,ua,e,a),x=F(null),L=F(null),k=F(null),B=F(null),T=F(null),w=F(null),R=F(null),V=F(!0),H=F(!0),q=rt(e,["labelSize","size"]),K=Y(()=>{var o,l;if(q.value)return q.value;const t=(l=(o=c==null?void 0:c.value)==null?void 0:o.Tabs)==null?void 0:l.size;return t||"medium"}),G=rt(e,["activeName","value"]),O=F(G.value??e.defaultValue??(r.default?(Qe=(Je=Ie(r.default())[0])==null?void 0:Je.props)==null?void 0:Qe.name:null)),s=De(G,O),y={id:0},P=Y(()=>{if(!(!e.justifyContent||e.type==="card"))return{display:"flex",justifyContent:e.justifyContent}});ce(s,()=>{y.id=0,E(),he(()=>{be()})});function g(){var o;const{value:t}=s;return t===null?null:(o=x.value)==null?void 0:o.querySelector(`[data-name="${t}"]`)}function j(t){if(e.type==="card")return;const{value:o}=k;if(!o)return;const l=o.style.opacity==="0";if(t){const h=`${a.value}-tabs-bar--disabled`,{barWidth:W}=e,D=u.value;if(t.dataset.disabled==="true"?o.classList.add(h):o.classList.remove(h),["top","bottom"].includes(D)){if(C(["top","maxHeight","height"]),typeof W=="number"&&t.offsetWidth>=W){const M=Math.floor((t.offsetWidth-W)/2)+t.offsetLeft;o.style.left=`${M}px`,o.style.maxWidth=`${W}px`}else o.style.left=`${t.offsetLeft}px`,o.style.maxWidth=`${t.offsetWidth}px`;o.style.width="8192px",l&&(o.style.transition="none"),o.offsetWidth,l&&(o.style.transition="",o.style.opacity="1")}else{if(C(["left","maxWidth","width"]),typeof W=="number"&&t.offsetHeight>=W){const M=Math.floor((t.offsetHeight-W)/2)+t.offsetTop;o.style.top=`${M}px`,o.style.maxHeight=`${W}px`}else o.style.top=`${t.offsetTop}px`,o.style.maxHeight=`${t.offsetHeight}px`;o.style.height="8192px",l&&(o.style.transition="none"),o.offsetHeight,l&&(o.style.transition="",o.style.opacity="1")}}}function I(){if(e.type==="card")return;const{value:t}=k;t&&(t.style.opacity="0")}function C(t){const{value:o}=k;if(o)for(const l of t)o.style[l]=""}function E(){if(e.type==="card")return;const t=g();t?j(t):I()}function re(t,o,l,h){const W=t.getBoundingClientRect(),D=o.getBoundingClientRect(),M=l?"left":"top",N=l?"right":"bottom";let X=0;h?X=(D[M]+D[N])/2-(W[M]+W[N])/2:D[M]<W[M]?X=D[M]-W[M]:D[N]>W[N]&&(X=D[N]-W[N]),X!==0&&t.scrollBy({[M]:X,behavior:"smooth"})}function be(){var l;const t=["top","bottom"].includes(u.value),o=g();if(o)if(t){const h=(l=w.value)==null?void 0:l.$el;if(!h)return;re(h,o,t,e.centerActiveTab)}else{const{value:h}=R;if(!h)return;re(h,o,t,e.centerActiveTab)}}const se=F(null);let fe=0,ee=null;function ke(t){const o=se.value;if(o){fe=t.getBoundingClientRect().height;const l=`${fe}px`,h=()=>{o.style.height=l,o.style.maxHeight=l};ee?(h(),ee(),ee=null):ee=h}}function Be(t){const o=se.value;if(o){const l=t.getBoundingClientRect().height,h=()=>{document.body.offsetHeight,o.style.maxHeight=`${l}px`,o.style.height=`${Math.max(fe,l)}px`};ee?(ee(),ee=null,h()):ee=h}}function Pe(){const t=se.value;if(t){t.style.maxHeight="",t.style.height="";const{paneWrapperStyle:o}=e;if(typeof o=="string")t.style.cssText=o;else if(o){const{maxHeight:l,height:h}=o;l!==void 0&&(t.style.maxHeight=l),h!==void 0&&(t.style.height=h)}}}const me={value:[]},xe=F("next");function Le(t){const o=s.value;let l="next";for(const h of me.value){if(h===o)break;if(h===t){l="prev";break}}xe.value=l,Ee(t)}function Ee(t){const{onActiveNameChange:o,onUpdateValue:l,"onUpdate:value":h}=e;o&&Z(o,t),l&&Z(l,t),h&&Z(h,t),O.value=t}function We(t){const{onClose:o}=e;o&&Z(o,t)}function Ae(t){if(["top","bottom"].includes(u.value)){const{value:o}=w;if(!o)return;const l=o.$el;if(!l)return;const h=l.offsetWidth,W=!!(b!=null&&b.value),D=t==="next"?h:-h;l.scrollBy({left:W?-D:D,behavior:"smooth"})}else{const{value:o}=R;if(!o)return;const l=o.offsetHeight,h=t==="next"?o.scrollTop+l:o.scrollTop-l;o.scrollTo({top:h,left:0,behavior:"smooth"})}}let ue=!0;function pe(){const{value:t}=k;if(!t)return;ue&&(ue=!1);const o="transition-disabled";t.classList.add(o),E(),t.classList.remove(o)}const ae=F(null);function ye({transitionDisabled:t}){const o=x.value;if(!o)return;t&&o.classList.add("transition-disabled");const l=g();l&&ae.value&&(ae.value.style.width=`${l.offsetWidth}px`,ae.value.style.height=`${l.offsetHeight}px`,ae.value.style.transform=`translate(${l.offsetLeft}px, ${l.offsetTop}px)`,t&&ae.value.offsetWidth),t&&o.classList.remove("transition-disabled")}ce([s],()=>{e.type==="segment"&&he(()=>{ye({transitionDisabled:!1})})}),wr(()=>{e.type==="segment"&&ye({transitionDisabled:!0})});let Xe=0;function yt(t){var l;if(t.contentRect.width===0&&t.contentRect.height===0||Xe===t.contentRect.width)return;Xe=t.contentRect.width;const{type:o}=e;(o==="line"||o==="bar")&&(ue||(l=e.justifyContent)!=null&&l.startsWith("space"))&&pe(),o!=="segment"&&we(qe())}const wt=He(yt,64);function Ye(){const{type:t}=e;t==="line"||t==="bar"?pe():t==="segment"&&ye({transitionDisabled:!0})}ce([()=>e.justifyContent,()=>e.size],()=>{he(()=>{(e.type==="line"||e.type==="bar")&&pe()})}),ce([u,()=>b==null?void 0:b.value],()=>{he(()=>{Ye(),we(qe(),{instantly:!0})})}),ce(()=>e.type,()=>{he(()=>{const t=L.value;t&&(t.classList.add("transition-disabled"),Ye(),t.offsetWidth,t.classList.remove("transition-disabled"))})});const le=F(!1);function Ct(t){var N;const{target:o,contentRect:{width:l,height:h}}=t,W=o.parentElement.parentElement.offsetWidth,D=o.parentElement.parentElement.offsetHeight,M=u.value;if(!le.value)M==="top"||M==="bottom"?W<l&&(le.value=!0):D<h&&(le.value=!0);else{const{value:X}=T;if(!X)return;M==="top"||M==="bottom"?W-l>X.$el.offsetWidth&&(le.value=!1):D-h>X.$el.offsetHeight&&(le.value=!1)}we(((N=w.value)==null?void 0:N.$el)||null)}const St=He(Ct,64);function zt(){const{onAdd:t}=e;t&&t()}const _e=F(!1);function qe(){var o;const t=u.value;return(t==="top"||t==="bottom"?(o=w.value)==null?void 0:o.$el:R.value)||null}function we(t,o={instantly:!1}){if(!t)return;const l=o.instantly?B.value:null;l&&l.classList.add("transition-disabled");const h=1,W=u.value;if(W==="top"||W==="bottom"){const{scrollLeft:D,scrollWidth:M,offsetWidth:N}=t,X=Math.abs(D);V.value=X<=h,H.value=X+N>=M-h,_e.value=N<M-h}else{const{scrollTop:D,scrollHeight:M,offsetHeight:N}=t;V.value=D<=h,H.value=D+N>=M-h,_e.value=N<M-h}l&&(l.offsetWidth,l.classList.remove("transition-disabled"))}const $t=He(t=>{we(t.target)},64);ge(xt,{triggerRef:J(e,"trigger"),tabStyleRef:J(e,"tabStyle"),tabClassRef:J(e,"tabClass"),addTabStyleRef:J(e,"addTabStyle"),addTabClassRef:J(e,"addTabClass"),paneClassRef:J(e,"paneClass"),paneStyleRef:J(e,"paneStyle"),mergedClsPrefixRef:a,typeRef:J(e,"type"),closableRef:J(e,"closable"),valueRef:s,tabChangeIdRef:y,onBeforeLeaveRef:J(e,"onBeforeLeave"),activateTab:Le,handleClose:We,handleAdd:zt}),xr(()=>{E(),be()}),mt(()=>{const{value:t}=B;if(!t)return;const{value:o}=a,l=`${o}-tabs-nav-scroll-wrapper--shadow-start`,h=`${o}-tabs-nav-scroll-wrapper--shadow-end`;V.value?t.classList.remove(l):t.classList.add(l),H.value?t.classList.remove(h):t.classList.add(h)});const Tt={syncBarPosition:()=>{E()},scrollToCurrentTab:()=>{be()}},Rt=()=>{ye({transitionDisabled:!0})},Ke=Y(()=>{const{value:t}=K,{type:o}=e,l=`${t}${{card:"Card",bar:"Bar",line:"Line",segment:"Segment"}[o]}`,{self:{barColor:h,closeIconColor:W,closeIconColorHover:D,closeIconColorPressed:M,tabColor:N,tabBorderColor:X,paneTextColor:kt,tabFontWeight:Bt,tabBorderRadius:Pt,tabFontWeightActive:Lt,colorSegment:Et,fontWeightStrong:Wt,tabColorSegment:At,closeSize:_t,closeIconSize:It,closeColorHover:Mt,closeColorPressed:Ft,closeBorderRadius:Ht,[te("panePadding",t)]:Ce,[te("tabPadding",l)]:Ot,[te("tabPaddingVertical",l)]:Dt,[te("tabGap",l)]:jt,[te("tabGap",`${l}Vertical`)]:Vt,[te("tabTextColor",o)]:Nt,[te("tabTextColorActive",o)]:Ut,[te("tabTextColorHover",o)]:Gt,[te("tabTextColorDisabled",o)]:Xt,[te("tabFontSize",t)]:Yt},common:{cubicBezierEaseInOut:qt}}=v.value;return{"--n-bezier":qt,"--n-color-segment":Et,"--n-bar-color":h,"--n-tab-font-size":Yt,"--n-tab-text-color":Nt,"--n-tab-text-color-active":Ut,"--n-tab-text-color-disabled":Xt,"--n-tab-text-color-hover":Gt,"--n-pane-text-color":kt,"--n-tab-border-color":X,"--n-tab-border-radius":Pt,"--n-close-size":_t,"--n-close-icon-size":It,"--n-close-color-hover":Mt,"--n-close-color-pressed":Ft,"--n-close-border-radius":Ht,"--n-close-icon-color":W,"--n-close-icon-color-hover":D,"--n-close-icon-color-pressed":M,"--n-tab-color":N,"--n-tab-font-weight":Bt,"--n-tab-font-weight-active":Lt,"--n-tab-padding":Ot,"--n-tab-padding-vertical":Dt,"--n-tab-gap":jt,"--n-tab-gap-vertical":Vt,"--n-pane-padding-left":Se(Ce,"left"),"--n-pane-padding-right":Se(Ce,"right"),"--n-pane-padding-top":Se(Ce,"top"),"--n-pane-padding-bottom":Se(Ce,"bottom"),"--n-font-weight-strong":Wt,"--n-tab-color-segment":At}}),de=p?ut("tabs",Y(()=>`${K.value[0]}${e.type[0]}`),Ke,e):void 0;return{mergedClsPrefix:a,mergedValue:s,renderedNames:new Set,segmentCapsuleElRef:ae,tabsPaneWrapperRef:se,tabsElRef:x,selfElRef:L,barElRef:k,addTabInstRef:T,xScrollInstRef:w,scrollWrapperElRef:B,addTabFixed:le,tabWrapperStyle:P,handleNavResize:wt,mergedSize:K,handleScroll:$t,handleTabsResize:St,cssVars:p?void 0:Ke,themeClass:de==null?void 0:de.themeClass,animationDirection:xe,renderNameListRef:me,yScrollElRef:R,handleSegmentResize:Rt,onAnimationBeforeLeave:ke,onAnimationEnter:Be,onAnimationAfterEnter:Pe,onRender:de==null?void 0:de.onRender,startReachedRef:V,endReachedRef:H,isOverflow:_e,handleButtonClick:Ae,mergedTheme:v,rtlEnabled:b,mergedPlacement:u,...Tt}},render(){const{mergedClsPrefix:e,type:r,mergedPlacement:a,addTabFixed:p,addable:c,mergedSize:z,renderNameListRef:b,onRender:u,paneWrapperClass:v,paneWrapperStyle:x,startReachedRef:L,endReachedRef:k,isOverflow:B,showScrollButton:T,handleButtonClick:w,mergedTheme:R,rtlEnabled:V,$slots:{default:H,prefix:q,suffix:K}}=this;u==null||u();const G=H?Ie(H()).filter(C=>C.type.__TAB_PANE__===!0):[],O=H?Ie(H()).filter(C=>C.type.__TAB__===!0):[],s=!O.length,y=r==="card",P=r==="segment",g=!y&&!P&&this.justifyContent;b.value=[];const j=()=>{const C=(i(),m("div",{style:Q(this.tabWrapperStyle),class:$(`${e}-tabs-wrapper`)},[g?S(()=>null):(i(),m("div",{key:1,class:$(`${e}-tabs-scroll-padding`),style:Q(a==="top"||a==="bottom"?{width:`${this.tabsPadding}px`}:{height:`${this.tabsPadding}px`})},null,6)),s?(i(),m(U,{key:2},[S(()=>G.map((E,re)=>(b.value.push(E.props.name),Oe((i(),_(Ve,ne(E.props,{internalCreatedByPane:!0,internalLeftPadded:re!==0&&(!g||g==="center"||g==="start"||g==="end")}),ve(E.children?{default:E.children.tab}:void 0),1040,["internalLeftPadded"]))))))],64)):(i(),m(U,{key:3},[S(()=>O.map((E,re)=>(b.value.push(E.props.name),Oe(re!==0&&!g?lt(E):E))))],64)),!p&&c&&y?(i(),m(U,{key:4},[S(()=>st(c,(s?G.length:O.length)!==0))],64)):S(()=>null),g?S(()=>null):(i(),m("div",{key:7,class:$(`${e}-tabs-scroll-padding`),style:Q({width:`${this.tabsPadding}px`})},null,6)),y?S(()=>null):(i(),m("div",{key:9,ref:"barElRef",class:$(`${e}-tabs-bar`)},null,2))],6));return i(),m("div",{ref:"tabsElRef",class:$(`${e}-tabs-nav-scroll-content`)},[y&&c?(i(),_(Me,{key:0,onResize:this.handleTabsResize},{default:()=>C},1032,["onResize"])):(i(),m(U,{key:1},[S(()=>C)],64)),y?(i(),m("div",{key:2,class:$(`${e}-tabs-pad`)},null,2)):S(()=>null)],2)},I=P?"top":a;return i(),m("div",{ref:"selfElRef",class:$([`${e}-tabs`,this.themeClass,`${e}-tabs--${r}-type`,`${e}-tabs--${z}-size`,g&&`${e}-tabs--flex`,`${e}-tabs--${I}`,V&&`${e}-tabs--rtl`]),style:Q(this.cssVars)},[oe("div",{class:$([`${e}-tabs-nav--${r}-type`,`${e}-tabs-nav--${I}`,`${e}-tabs-nav`])},[S(()=>Ze(q,C=>C&&(i(),m("div",{class:$(`${e}-tabs-nav__prefix`)},[S(()=>C)],2)))),P?(i(),_(Me,{key:0,onResize:this.handleSegmentResize},{default:()=>(i(),m("div",{class:$(`${e}-tabs-rail`),ref:"tabsElRef"},[oe("div",{class:$(`${e}-tabs-capsule`),ref:"segmentCapsuleElRef"},[oe("div",{class:$(`${e}-tabs-wrapper`)},[oe("div",{class:$(`${e}-tabs-tab`)},null,2)],2)],2),s?(i(),m(U,{key:0},[S(()=>G.map((C,E)=>(b.value.push(C.props.name),i(),_(Ve,ne(C.props,{internalCreatedByPane:!0,internalLeftPadded:E!==0}),ve(C.children?{default:C.children.tab}:void 0),1040,["internalLeftPadded"]))))],64)):(i(),m(U,{key:1},[S(()=>O.map((C,E)=>(b.value.push(C.props.name),E===0?C:lt(C))))],64))],2))},1032,["onResize"])):(i(),m(U,{key:1},[S(()=>T&&B&&(i(),_(nt,{mergedClsPrefix:e,type:"prev",vertical:I==="left"||I==="right",disabled:L,rtl:!!V,theme:R.peers.Button,themeOverrides:R.peerOverrides.Button,onClick:w},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"]))),(i(),_(Me,{onResize:this.handleNavResize},{default:()=>(i(),m("div",{class:$(`${e}-tabs-nav-scroll-wrapper`),ref:"scrollWrapperElRef"},[["top","bottom"].includes(I)?(i(),_(Vr,{key:0,ref:"xScrollInstRef",onScroll:this.handleScroll},{default:j},1032,["onScroll"])):(i(),m("div",{key:1,class:$(`${e}-tabs-nav-y-scroll`),onScroll:this.handleScroll,ref:"yScrollElRef"},[S(()=>j())],42,["onScroll"]))],2))},1032,["onResize"])),S(()=>T&&B&&(i(),_(nt,{mergedClsPrefix:e,type:"next",vertical:I==="left"||I==="right",disabled:k,rtl:!!V,theme:R.peers.Button,themeOverrides:R.peerOverrides.Button,onClick:w},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"])))],64)),p&&c&&y?(i(),m(U,{key:2},[S(()=>st(c,!0))],64)):S(()=>null),S(()=>Ze(K,C=>C&&(i(),m("div",{class:$(`${e}-tabs-nav__suffix`)},[S(()=>C)],2))))],2),S(()=>s&&(this.animated&&(I==="top"||I==="bottom")?(i(),m("div",{key:1,ref:"tabsPaneWrapperRef",style:Q(x),class:$([`${e}-tabs-pane-wrapper`,v])},[S(()=>it(G,this.mergedValue,this.renderedNames,this.onAnimationBeforeLeave,this.onAnimationEnter,this.onAnimationAfterEnter,this.animationDirection))],6)):it(G,this.mergedValue,this.renderedNames)))],6)}});function it(e,r,a,p,c,z,b){const u=[];return e.forEach(v=>{const{name:x,displayDirective:L,"display-directive":k}=v.props,B=w=>L===w||k===w,T=r===x;if(v.key!==void 0&&(v.key=x),T||B("show")||B("show:lazy")&&a.has(x)){a.has(x)||a.add(x);const w=!B("if");u.push(w?$e(v,[[je,T]]):v)}}),b?(i(),_(Cr,{name:`${b}-transition`,onBeforeLeave:p,onEnter:c,onAfterEnter:z},{default:()=>u},1032,["name","onBeforeLeave","onEnter","onAfterEnter"])):u}function st(e,r){return i(),_(Ve,{ref:"addTabInstRef",key:"__addable",name:"__addable",internalCreatedByPane:!0,internalAddable:!0,internalLeftPadded:r,disabled:typeof e=="object"&&e.disabled},null,8,["internalLeftPadded","disabled"])}function lt(e){const r=Sr(e);return r.props?r.props.internalLeftPadded=!0:r.props={internalLeftPadded:!0},r}function Oe(e){return Array.isArray(e.dynamicProps)?e.dynamicProps.includes("internalLeftPadded")||e.dynamicProps.push("internalLeftPadded"):e.dynamicProps=["internalLeftPadded"],e}export{Ta as D,Ve as T,$a as a,Ra as b};
