import{T as xe,b5 as $e,a0 as ke,aO as y,aR as k,S as re,aQ as V,c as Be,a7 as q,bb as oe,bj as Ee,bf as Re,G as Te,a6 as Fe,aV as Me,aM as He,u as n,_ as U,v,y as $,x as O,ac as Oe,bm as ne,L as Pe,az as Ie,be as De,bn as We,a9 as Ae,A as P,bq as _e,b6 as Le,C as Ue}from"./index-EtNgnfcI.js";import{J as G,t as J}from"./c-CrvpGC0v.js";import{z as K,aL as X,aG as Q,a6 as a,s as p,q as E,e as ie,I as Ne,Y as I,U as L,ad as D,M as ae,aJ as je,aI as Ye,a0 as Xe,n as B,a8 as _,av as Z,p as ee,F as te}from"./c-1SqeWcLE.js";function Ve(e){const{modalColor:t,textColor1:r,textColor2:b,boxShadow3:h,lineHeight:S,fontWeightStrong:l,dividerColor:f,closeColorHover:m,closeColorPressed:z,closeIconColor:R,closeIconColorHover:T,closeIconColorPressed:F,borderRadius:C,primaryColorHover:d}=e;return{bodyPadding:"16px 24px",borderRadius:C,headerPadding:"16px 24px",footerPadding:"16px 24px",color:t,textColor:b,titleTextColor:r,titleFontSize:"18px",titleFontWeight:l,boxShadow:h,lineHeight:S,headerBorderBottom:`1px solid ${f}`,footerBorderTop:`1px solid ${f}`,closeIconColor:R,closeIconColorHover:T,closeIconColorPressed:F,closeSize:"22px",closeIconSize:"18px",closeColorHover:m,closeColorPressed:z,closeBorderRadius:C,resizableTriggerColorHover:d}}const qe=xe({name:"Drawer",common:ke,peers:{Scrollbar:$e},self:Ve}),Ke=["onMouseenter","onMouseleave","onMousedown"],Ge={key:1,role:"none"};var Je=K({name:"NDrawerContent",inheritAttrs:!1,props:{blockScroll:Boolean,show:{type:Boolean,default:void 0},displayDirective:{type:String,required:!0},placement:{type:String,required:!0},contentClass:String,contentStyle:[Object,String],nativeScrollbar:{type:Boolean,required:!0},scrollbarProps:Object,trapFocus:{type:Boolean,default:!0},autoFocus:{type:Boolean,default:!0},showMask:{type:[Boolean,String],required:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,onClickoutside:Function,onAfterLeave:Function,onAfterEnter:Function,onEsc:Function},setup(e){const t=D(!!e.show),r=D(null),b=ae(q);let h=0,S="",l=null;const f=D(!1),m=D(!1),z=B(()=>e.placement==="top"||e.placement==="bottom"),{mergedClsPrefixRef:R,mergedRtlRef:T}=oe(e),F=Ee("Drawer",T,R),C=o,d=i=>{m.value=!0,h=z.value?i.clientY:i.clientX,S=document.body.style.cursor,document.body.style.cursor=z.value?"ns-resize":"ew-resize",document.body.addEventListener("mousemove",x),document.body.addEventListener("mouseleave",C),document.body.addEventListener("mouseup",o)},M=()=>{l!==null&&(window.clearTimeout(l),l=null),m.value?f.value=!0:l=window.setTimeout(()=>{f.value=!0},300)},N=()=>{l!==null&&(window.clearTimeout(l),l=null),f.value=!1},{doUpdateHeight:j,doUpdateWidth:Y}=b,W=i=>{const{maxWidth:c}=e;if(c&&i>c)return c;const{minWidth:g}=e;return g&&i<g?g:i},A=i=>{const{maxHeight:c}=e;if(c&&i>c)return c;const{minHeight:g}=e;return g&&i<g?g:i};function x(i){var c,g;if(m.value)if(z.value){let w=((c=r.value)==null?void 0:c.offsetHeight)||0;const H=h-i.clientY;w+=e.placement==="bottom"?H:-H,w=A(w),j(w),h=i.clientY}else{let w=((g=r.value)==null?void 0:g.offsetWidth)||0;const H=h-i.clientX;w+=e.placement==="right"?H:-H,w=W(w),Y(w),h=i.clientX}}function o(){m.value&&(h=0,m.value=!1,document.body.style.cursor=S,document.body.removeEventListener("mousemove",x),document.body.removeEventListener("mouseup",o),document.body.removeEventListener("mouseleave",C))}je(()=>{e.show&&(t.value=!0)}),Ye(()=>e.show,i=>{i||o()}),Xe(()=>{o()});const s=B(()=>{const{show:i}=e,c=[[Q,i]];return e.showMask||c.push([Te,e.onClickoutside,void 0,{capture:!0}]),c});function u(){var i;t.value=!1,(i=e.onAfterLeave)==null||i.call(e)}return Re(B(()=>e.blockScroll&&t.value)),_(Fe,r),_(Me,null),_(He,null),{bodyRef:r,rtlEnabled:F,mergedClsPrefix:b.mergedClsPrefixRef,isMounted:b.isMountedRef,mergedTheme:b.mergedThemeRef,displayed:t,transitionName:B(()=>({right:"slide-in-from-right-transition",left:"slide-in-from-left-transition",top:"slide-in-from-top-transition",bottom:"slide-in-from-bottom-transition"})[e.placement]),handleAfterLeave:u,bodyDirectives:s,handleMousedownResizeTrigger:d,handleMouseenterResizeTrigger:M,handleMouseleaveResizeTrigger:N,isDragging:m,isHoverOnResizeTrigger:f}},render(){const{$slots:e,mergedClsPrefix:t}=this;return this.displayDirective==="show"||this.displayed||this.show?X((a(),p("div",Ge,[(a(),E(Be,{disabled:!this.showMask||!this.trapFocus,active:this.show,autoFocus:this.autoFocus,onEsc:this.onEsc},{default:()=>(a(),E(ie,{name:this.transitionName,appear:this.isMounted,onAfterEnter:this.onAfterEnter,onAfterLeave:this.handleAfterLeave},{default:()=>X(Ne("div",L(this.$attrs,{role:"dialog",ref:"bodyRef","aria-modal":"true",class:[`${t}-drawer`,this.rtlEnabled&&`${t}-drawer--rtl`,`${t}-drawer--${this.placement}-placement`,this.isDragging&&`${t}-drawer--unselectable`,this.nativeScrollbar&&`${t}-drawer--native-scrollbar`]}),[this.resizable?(a(),p("div",{key:2,class:y([`${t}-drawer__resize-trigger`,(this.isDragging||this.isHoverOnResizeTrigger)&&`${t}-drawer__resize-trigger--hover`]),onMouseenter:this.handleMouseenterResizeTrigger,onMouseleave:this.handleMouseleaveResizeTrigger,onMousedown:this.handleMousedownResizeTrigger},null,42,Ke)):null,this.nativeScrollbar?(a(),p("div",{key:3,class:y([`${t}-drawer-content-wrapper`,this.contentClass]),style:I(this.contentStyle),role:"none"},[k(()=>{var r;return(r=e.default)==null?void 0:r.call(e)})],6)):(a(),E(re,L({key:4},this.scrollbarProps,{contentStyle:this.contentStyle,contentClass:[`${t}-drawer-content-wrapper`,this.contentClass],theme:this.mergedTheme.peers.Scrollbar,themeOverrides:this.mergedTheme.peerOverrides.Scrollbar}),V(e),1040,["contentStyle","contentClass","theme","themeOverrides"]))]),this.bodyDirectives)},1032,["name","appear","onAfterEnter","onAfterLeave"]))},1032,["disabled","active","autoFocus","onEsc"]))])),[[Q,this.displayDirective==="if"||this.displayed||this.show]]):null}});const{cubicBezierEaseIn:Qe,cubicBezierEaseOut:Ze}=U;function et({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-bottom"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${Qe}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${Ze}`}),n(`&.${r}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateY(100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateY(100%)"})]}const{cubicBezierEaseIn:tt,cubicBezierEaseOut:rt}=U;function ot({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-left"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${tt}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${rt}`}),n(`&.${r}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateX(-100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateX(-100%)"})]}const{cubicBezierEaseIn:nt,cubicBezierEaseOut:it}=U;function at({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-right"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${nt}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${it}`}),n(`&.${r}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateX(100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateX(100%)"})]}const{cubicBezierEaseIn:st,cubicBezierEaseOut:lt}=U;function dt({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-top"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${st}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${lt}`}),n(`&.${r}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateY(-100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateY(-100%)"})]}var ct=n([v("drawer",`
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
 `,[at(),ot(),dt(),et(),$("unselectable",`
 user-select: none; 
 -webkit-user-select: none;
 `),$("native-scrollbar",[v("drawer-content-wrapper",`
 overflow: auto;
 height: 100%;
 `)]),O("resize-trigger",`
 position: absolute;
 background-color: #0000;
 transition: background-color .3s var(--n-bezier);
 `,[$("hover",`
 background-color: var(--n-resize-trigger-color-hover);
 `)]),v("drawer-content-wrapper",`
 box-sizing: border-box;
 `),v("drawer-content",`
 height: 100%;
 display: flex;
 flex-direction: column;
 `,[$("native-scrollbar",[v("drawer-body-content-wrapper",`
 height: 100%;
 overflow: auto;
 `)]),v("drawer-body",`
 flex: 1 0 0;
 overflow: hidden;
 `),v("drawer-body-content-wrapper",`
 box-sizing: border-box;
 padding: var(--n-body-padding);
 `),v("drawer-header",`
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
 `,[O("main",`
 flex: 1;
 `),O("close",`
 margin-left: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `)]),v("drawer-footer",`
 display: flex;
 justify-content: flex-end;
 border-top: var(--n-footer-border-top);
 transition: border .3s var(--n-bezier);
 padding: var(--n-footer-padding);
 `)]),$("right-placement",`
 top: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-bottom-left-radius: var(--n-border-radius);
 `,[O("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 left: 0;
 transform: translateX(-1.5px);
 cursor: ew-resize;
 `)]),$("left-placement",`
 top: 0;
 bottom: 0;
 left: 0;
 border-top-right-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[O("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 right: 0;
 transform: translateX(1.5px);
 cursor: ew-resize;
 `)]),$("top-placement",`
 top: 0;
 left: 0;
 right: 0;
 border-bottom-left-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[O("resize-trigger",`
 width: 100%;
 height: 3px;
 bottom: 0;
 left: 0;
 transform: translateY(1.5px);
 cursor: ns-resize;
 `)]),$("bottom-placement",`
 left: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-top-right-radius: var(--n-border-radius);
 `,[O("resize-trigger",`
 width: 100%;
 height: 3px;
 top: 0;
 left: 0;
 transform: translateY(-1.5px);
 cursor: ns-resize;
 `)])]),n("body",[n(">",[v("drawer-container",`
 position: fixed;
 `)])]),v("drawer-container",`
 position: relative;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 pointer-events: none;
 `,[n("> *",`
 pointer-events: all;
 `)]),v("drawer-mask",`
 background-color: rgba(0, 0, 0, .3);
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 `,[$("invisible",`
 background-color: rgba(0, 0, 0, 0)
 `),Oe({enterDuration:"0.2s",leaveDuration:"0.2s",enterCubicBezier:"var(--n-bezier-in)",leaveCubicBezier:"var(--n-bezier-out)"})])]);const ut=["onClick"],ht={...ne.props,show:Boolean,width:[Number,String],height:[Number,String],placement:{type:String,default:"right"},maskClosable:{type:Boolean,default:!0},showMask:{type:[Boolean,String],default:!0},to:[String,Object],displayDirective:{type:String,default:"if"},nativeScrollbar:{type:Boolean,default:!0},zIndex:Number,onMaskClick:Function,scrollbarProps:Object,contentClass:String,contentStyle:[Object,String],trapFocus:{type:Boolean,default:!0},onEsc:Function,autoFocus:{type:Boolean,default:!0},closeOnEsc:{type:Boolean,default:!0},blockScroll:{type:Boolean,default:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,defaultWidth:{type:[Number,String],default:251},defaultHeight:{type:[Number,String],default:251},onUpdateWidth:[Function,Array],onUpdateHeight:[Function,Array],"onUpdate:width":[Function,Array],"onUpdate:height":[Function,Array],"onUpdate:show":[Function,Array],onUpdateShow:[Function,Array],onAfterEnter:Function,onAfterLeave:Function,drawerStyle:[String,Object],drawerClass:String,target:null,onShow:Function,onHide:Function};var vt=K({name:"Drawer",inheritAttrs:!1,props:ht,setup(e){const{mergedClsPrefixRef:t,namespaceRef:r,inlineThemeDisabled:b}=oe(e),h=Ie(),S=ne("Drawer","-drawer",ct,qe,e,t),l=D(e.defaultWidth),f=D(e.defaultHeight),m=G(Z(e,"width"),l),z=G(Z(e,"height"),f),R=B(()=>{const{placement:o}=e;return o==="top"||o==="bottom"?"":J(m.value)}),T=B(()=>{const{placement:o}=e;return o==="left"||o==="right"?"":J(z.value)}),F=o=>{const{onUpdateWidth:s,"onUpdate:width":u}=e;s&&P(s,o),u&&P(u,o),l.value=o},C=o=>{const{onUpdateHeight:s,"onUpdate:width":u}=e;s&&P(s,o),u&&P(u,o),f.value=o},d=B(()=>[{width:R.value,height:T.value},e.drawerStyle||""]);function M(o){const{onMaskClick:s,maskClosable:u}=e;u&&W(!1),s&&s(o)}function N(o){M(o)}const j=De();function Y(o){var s;(s=e.onEsc)==null||s.call(e),e.show&&e.closeOnEsc&&Ae(o)&&(j.value||W(!1))}function W(o){const{onHide:s,onUpdateShow:u,"onUpdate:show":i}=e;u&&P(u,o),i&&P(i,o),s&&!o&&P(s,o)}_(q,{isMountedRef:h,mergedThemeRef:S,mergedClsPrefixRef:t,doUpdateShow:W,doUpdateHeight:C,doUpdateWidth:F});const A=B(()=>{const{common:{cubicBezierEaseInOut:o,cubicBezierEaseIn:s,cubicBezierEaseOut:u},self:{color:i,textColor:c,boxShadow:g,lineHeight:w,headerPadding:H,footerPadding:se,borderRadius:le,bodyPadding:de,titleFontSize:ce,titleTextColor:ue,titleFontWeight:he,headerBorderBottom:fe,footerBorderTop:be,closeIconColor:me,closeIconColorHover:ge,closeIconColorPressed:ve,closeColorHover:we,closeColorPressed:ye,closeIconSize:pe,closeSize:Se,closeBorderRadius:Ce,resizableTriggerColorHover:ze}}=S.value;return{"--n-line-height":w,"--n-color":i,"--n-border-radius":le,"--n-text-color":c,"--n-box-shadow":g,"--n-bezier":o,"--n-bezier-out":u,"--n-bezier-in":s,"--n-header-padding":H,"--n-body-padding":de,"--n-footer-padding":se,"--n-title-text-color":ue,"--n-title-font-size":ce,"--n-title-font-weight":he,"--n-header-border-bottom":fe,"--n-footer-border-top":be,"--n-close-icon-color":me,"--n-close-icon-color-hover":ge,"--n-close-icon-color-pressed":ve,"--n-close-size":Se,"--n-close-color-hover":we,"--n-close-color-pressed":ye,"--n-close-icon-size":pe,"--n-close-border-radius":Ce,"--n-resize-trigger-color-hover":ze}}),x=b?We("drawer",void 0,A,e):void 0;return{mergedClsPrefix:t,namespace:r,mergedBodyStyle:d,handleOutsideClick:N,handleMaskClick:M,handleEsc:Y,mergedTheme:S,cssVars:b?void 0:A,themeClass:x==null?void 0:x.themeClass,onRender:x==null?void 0:x.onRender,isMounted:h}},render(){const{mergedClsPrefix:e}=this;return a(),E(Pe,{to:this.to,show:this.show},{default:()=>{var t;return(t=this.onRender)==null||t.call(this),X((a(),p("div",{class:y([`${e}-drawer-container`,this.namespace,this.themeClass]),style:I(this.cssVars),role:"none"},[this.showMask?(a(),E(ie,{key:0,name:"fade-in-transition",appear:this.isMounted},{default:()=>this.show?(a(),p("div",{key:1,"aria-hidden":!0,class:y([`${e}-drawer-mask`,this.showMask==="transparent"&&`${e}-drawer-mask--invisible`]),onClick:this.handleMaskClick},null,10,ut)):null},1032,["appear"])):k(()=>null),(a(),E(Je,L(this.$attrs,{class:[this.drawerClass,this.$attrs.class],style:[this.mergedBodyStyle,this.$attrs.style],blockScroll:this.blockScroll,contentStyle:this.contentStyle,contentClass:this.contentClass,placement:this.placement,scrollbarProps:this.scrollbarProps,show:this.show,displayDirective:this.displayDirective,nativeScrollbar:this.nativeScrollbar,onAfterEnter:this.onAfterEnter,onAfterLeave:this.onAfterLeave,trapFocus:this.trapFocus,autoFocus:this.autoFocus,resizable:this.resizable,maxHeight:this.maxHeight,minHeight:this.minHeight,maxWidth:this.maxWidth,minWidth:this.minWidth,showMask:this.showMask,onEsc:this.handleEsc,onClickoutside:this.handleOutsideClick}),V(this.$slots),1040,["class","style","blockScroll","contentStyle","contentClass","placement","scrollbarProps","show","displayDirective","nativeScrollbar","onAfterEnter","onAfterLeave","trapFocus","autoFocus","resizable","maxHeight","minHeight","maxWidth","minWidth","showMask","onEsc","onClickoutside"]))],6)),[[_e,{zIndex:this.zIndex,enabled:this.show}]])}},1032,["to","show"])}});const ft={title:String,headerClass:String,headerStyle:[Object,String],footerClass:String,footerStyle:[Object,String],bodyClass:String,bodyStyle:[Object,String],bodyContentClass:String,bodyContentStyle:[Object,String],nativeScrollbar:{type:Boolean,default:!0},scrollbarProps:Object,closable:Boolean};var wt=K({name:"DrawerContent",props:ft,slots:Object,setup(){const e=ae(q,null);e||Le("drawer-content","`n-drawer-content` must be placed inside `n-drawer`.");const{doUpdateShow:t}=e;function r(){t(!1)}return{handleCloseClick:r,mergedTheme:e.mergedThemeRef,mergedClsPrefix:e.mergedClsPrefixRef}},render(){const{title:e,mergedClsPrefix:t,nativeScrollbar:r,mergedTheme:b,bodyClass:h,bodyStyle:S,bodyContentClass:l,bodyContentStyle:f,headerClass:m,headerStyle:z,footerClass:R,footerStyle:T,scrollbarProps:F,closable:C,$slots:d}=this;return a(),p("div",{role:"none",class:y([`${t}-drawer-content`,r&&`${t}-drawer-content--native-scrollbar`])},[d.header||e||C?(a(),p("div",{key:0,class:y([`${t}-drawer-header`,m]),style:I(z),role:"none"},[ee("div",{class:y(`${t}-drawer-header__main`),role:"heading","aria-level":"1"},[d.header!==void 0?(a(),p(te,{key:0},[k(()=>d.header())],64)):(a(),p(te,{key:1},[k(()=>e)],64))],2),k(()=>C&&(a(),E(Ue,{onClick:this.handleCloseClick,clsPrefix:t,class:y(`${t}-drawer-header__close`),absolute:!0},null,8,["onClick","clsPrefix","class"])))],6)):k(()=>null),r?(a(),p("div",{key:2,class:y([`${t}-drawer-body`,h]),style:I(S),role:"none"},[ee("div",{class:y([`${t}-drawer-body-content-wrapper`,l]),style:I(f),role:"none"},[k(()=>{var M;return(M=d.default)==null?void 0:M.call(d)})],6)],6)):(a(),E(re,L({key:3,themeOverrides:b.peerOverrides.Scrollbar,theme:b.peers.Scrollbar},F,{class:`${t}-drawer-body`,contentClass:[`${t}-drawer-body-content-wrapper`,l],contentStyle:f}),V(d),1040,["themeOverrides","theme","class","contentClass","contentStyle"])),d.footer?(a(),p("div",{key:4,class:y([`${t}-drawer-footer`,R]),style:I(T),role:"none"},[k(()=>d.footer())],6)):k(()=>null)],2)}});export{wt as D,vt as a};
