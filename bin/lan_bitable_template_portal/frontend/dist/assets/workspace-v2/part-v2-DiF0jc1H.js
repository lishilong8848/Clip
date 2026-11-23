import{by as K,fE as _,fm as j,dC as h,bb as R,b9 as B,aa as G,ck as ye,db as I,dh as J,di as Q,a1 as ze,cS as Y,dg as Z,x as $e,d$ as k,co as Se,bF as ee,eY as te,fb as xe,fq as ke,fp as Be,du as Ee,f6 as Ce,b3 as w,dT as H,a$ as Re,bE as Fe,dN as Me,cY as Te,aJ as n,aq as O,aL as c,aN as g,aM as $,c5 as He,ff as re,fO as Ie,L as Oe,cB as Ae,bG as De,f9 as X,eG as q,f4 as We,fg as Le,bO as Pe,aQ as S,cc as V}from"./app-v2-CV6DtNf8.js";const Ne=["onMouseenter","onMouseleave","onMousedown"],Ue={key:1,role:"none"};var _e=K({name:"NDrawerContent",inheritAttrs:!1,props:{blockScroll:Boolean,show:{type:Boolean,default:void 0},displayDirective:{type:String,required:!0},placement:{type:String,required:!0},contentClass:String,contentStyle:[Object,String],nativeScrollbar:{type:Boolean,required:!0},scrollbarProps:Object,trapFocus:{type:Boolean,default:!0},autoFocus:{type:Boolean,default:!0},showMask:{type:[Boolean,String],required:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,onClickoutside:Function,onAfterLeave:Function,onAfterEnter:Function,onEsc:Function},setup(e){const o=k(!!e.show),t=k(null),p=Se(ee);let m=0,x="",f=null;const y=k(!1),v=k(!1),E=w(()=>e.placement==="top"||e.placement==="bottom"),{mergedClsPrefixRef:A,mergedRtlRef:D}=te(e),W=xe("Drawer",D,A),F=r,L=i=>{v.value=!0,m=E.value?i.clientY:i.clientX,x=document.body.style.cursor,document.body.style.cursor=E.value?"ns-resize":"ew-resize",document.body.addEventListener("mousemove",b),document.body.addEventListener("mouseleave",F),document.body.addEventListener("mouseup",r)},M=()=>{f!==null&&(window.clearTimeout(f),f=null),v.value?y.value=!0:f=window.setTimeout(()=>{y.value=!0},300)},P=()=>{f!==null&&(window.clearTimeout(f),f=null),y.value=!1},{doUpdateHeight:N,doUpdateWidth:U}=p,C=i=>{const{maxWidth:s}=e;if(s&&i>s)return s;const{minWidth:d}=e;return d&&i<d?d:i},T=i=>{const{maxHeight:s}=e;if(s&&i>s)return s;const{minHeight:d}=e;return d&&i<d?d:i};function b(i){var s,d;if(v.value)if(E.value){let u=((s=t.value)==null?void 0:s.offsetHeight)||0;const z=m-i.clientY;u+=e.placement==="bottom"?z:-z,u=T(u),N(u),m=i.clientY}else{let u=((d=t.value)==null?void 0:d.offsetWidth)||0;const z=m-i.clientX;u+=e.placement==="right"?z:-z,u=C(u),U(u),m=i.clientX}}function r(){v.value&&(m=0,v.value=!1,document.body.style.cursor=x,document.body.removeEventListener("mousemove",b),document.body.removeEventListener("mouseup",r),document.body.removeEventListener("mouseleave",F))}ke(()=>{e.show&&(o.value=!0)}),Be(()=>e.show,i=>{i||r()}),Ee(()=>{r()});const a=w(()=>{const{show:i}=e,s=[[j,i]];return e.showMask||s.push([Re,e.onClickoutside,void 0,{capture:!0}]),s});function l(){var i;o.value=!1,(i=e.onAfterLeave)==null||i.call(e)}return Ce(w(()=>e.blockScroll&&o.value)),H(Fe,t),H(Me,null),H(Te,null),{bodyRef:t,rtlEnabled:W,mergedClsPrefix:p.mergedClsPrefixRef,isMounted:p.isMountedRef,mergedTheme:p.mergedThemeRef,displayed:o,transitionName:w(()=>({right:"slide-in-from-right-transition",left:"slide-in-from-left-transition",top:"slide-in-from-top-transition",bottom:"slide-in-from-bottom-transition"})[e.placement]),handleAfterLeave:l,bodyDirectives:a,handleMousedownResizeTrigger:L,handleMouseenterResizeTrigger:M,handleMouseleaveResizeTrigger:P,isDragging:v,isHoverOnResizeTrigger:y}},render(){const{$slots:e,mergedClsPrefix:o}=this;return this.displayDirective==="show"||this.displayed||this.show?_((h(),R("div",Ue,[(h(),B($e,{disabled:!this.showMask||!this.trapFocus,active:this.show,autoFocus:this.autoFocus,onEsc:this.onEsc},{default:()=>(h(),B(G,{name:this.transitionName,appear:this.isMounted,onAfterEnter:this.onAfterEnter,onAfterLeave:this.handleAfterLeave},{default:()=>_(ye("div",Y(this.$attrs,{role:"dialog",ref:"bodyRef","aria-modal":"true",class:[`${o}-drawer`,this.rtlEnabled&&`${o}-drawer--rtl`,`${o}-drawer--${this.placement}-placement`,this.isDragging&&`${o}-drawer--unselectable`,this.nativeScrollbar&&`${o}-drawer--native-scrollbar`]}),[this.resizable?(h(),R("div",{key:2,class:I([`${o}-drawer__resize-trigger`,(this.isDragging||this.isHoverOnResizeTrigger)&&`${o}-drawer__resize-trigger--hover`]),onMouseenter:this.handleMouseenterResizeTrigger,onMouseleave:this.handleMouseleaveResizeTrigger,onMousedown:this.handleMousedownResizeTrigger},null,42,Ne)):null,this.nativeScrollbar?(h(),R("div",{key:3,class:I([`${o}-drawer-content-wrapper`,this.contentClass]),style:J(this.contentStyle),role:"none"},[Q(()=>{var t;return(t=e.default)==null?void 0:t.call(e)})],6)):(h(),B(ze,Y({key:4},this.scrollbarProps,{contentStyle:this.contentStyle,contentClass:[`${o}-drawer-content-wrapper`,this.contentClass],theme:this.mergedTheme.peers.Scrollbar,themeOverrides:this.mergedTheme.peerOverrides.Scrollbar}),Z(e),1040,["contentStyle","contentClass","theme","themeOverrides"]))]),this.bodyDirectives)},1032,["name","appear","onAfterEnter","onAfterLeave"]))},1032,["disabled","active","autoFocus","onEsc"]))])),[[j,this.displayDirective==="if"||this.displayed||this.show]]):null}});const{cubicBezierEaseIn:Ye,cubicBezierEaseOut:je}=O;function Xe({duration:e="0.3s",leaveDuration:o="0.2s",name:t="slide-in-from-bottom"}={}){return[n(`&.${t}-transition-leave-active`,{transition:`transform ${o} ${Ye}`}),n(`&.${t}-transition-enter-active`,{transition:`transform ${e} ${je}`}),n(`&.${t}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${t}-transition-enter-from`,{transform:"translateY(100%)"}),n(`&.${t}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${t}-transition-leave-to`,{transform:"translateY(100%)"})]}const{cubicBezierEaseIn:qe,cubicBezierEaseOut:Ve}=O;function Ke({duration:e="0.3s",leaveDuration:o="0.2s",name:t="slide-in-from-left"}={}){return[n(`&.${t}-transition-leave-active`,{transition:`transform ${o} ${qe}`}),n(`&.${t}-transition-enter-active`,{transition:`transform ${e} ${Ve}`}),n(`&.${t}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${t}-transition-enter-from`,{transform:"translateX(-100%)"}),n(`&.${t}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${t}-transition-leave-to`,{transform:"translateX(-100%)"})]}const{cubicBezierEaseIn:Ge,cubicBezierEaseOut:Je}=O;function Qe({duration:e="0.3s",leaveDuration:o="0.2s",name:t="slide-in-from-right"}={}){return[n(`&.${t}-transition-leave-active`,{transition:`transform ${o} ${Ge}`}),n(`&.${t}-transition-enter-active`,{transition:`transform ${e} ${Je}`}),n(`&.${t}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${t}-transition-enter-from`,{transform:"translateX(100%)"}),n(`&.${t}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${t}-transition-leave-to`,{transform:"translateX(100%)"})]}const{cubicBezierEaseIn:Ze,cubicBezierEaseOut:et}=O;function tt({duration:e="0.3s",leaveDuration:o="0.2s",name:t="slide-in-from-top"}={}){return[n(`&.${t}-transition-leave-active`,{transition:`transform ${o} ${Ze}`}),n(`&.${t}-transition-enter-active`,{transition:`transform ${e} ${et}`}),n(`&.${t}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${t}-transition-enter-from`,{transform:"translateY(-100%)"}),n(`&.${t}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${t}-transition-leave-to`,{transform:"translateY(-100%)"})]}var rt=n([c("drawer",`
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
 `,[Qe(),Ke(),tt(),Xe(),g("unselectable",`
 user-select: none; 
 -webkit-user-select: none;
 `),g("native-scrollbar",[c("drawer-content-wrapper",`
 overflow: auto;
 height: 100%;
 `)]),$("resize-trigger",`
 position: absolute;
 background-color: #0000;
 transition: background-color .3s var(--n-bezier);
 `,[g("hover",`
 background-color: var(--n-resize-trigger-color-hover);
 `)]),c("drawer-content-wrapper",`
 box-sizing: border-box;
 `),c("drawer-content",`
 height: 100%;
 display: flex;
 flex-direction: column;
 `,[g("native-scrollbar",[c("drawer-body-content-wrapper",`
 height: 100%;
 overflow: auto;
 `)]),c("drawer-body",`
 flex: 1 0 0;
 overflow: hidden;
 `),c("drawer-body-content-wrapper",`
 box-sizing: border-box;
 padding: var(--n-body-padding);
 `),c("drawer-header",`
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
 `,[$("main",`
 flex: 1;
 `),$("close",`
 margin-left: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `)]),c("drawer-footer",`
 display: flex;
 justify-content: flex-end;
 border-top: var(--n-footer-border-top);
 transition: border .3s var(--n-bezier);
 padding: var(--n-footer-padding);
 `)]),g("right-placement",`
 top: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-bottom-left-radius: var(--n-border-radius);
 `,[$("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 left: 0;
 transform: translateX(-1.5px);
 cursor: ew-resize;
 `)]),g("left-placement",`
 top: 0;
 bottom: 0;
 left: 0;
 border-top-right-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[$("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 right: 0;
 transform: translateX(1.5px);
 cursor: ew-resize;
 `)]),g("top-placement",`
 top: 0;
 left: 0;
 right: 0;
 border-bottom-left-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[$("resize-trigger",`
 width: 100%;
 height: 3px;
 bottom: 0;
 left: 0;
 transform: translateY(1.5px);
 cursor: ns-resize;
 `)]),g("bottom-placement",`
 left: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-top-right-radius: var(--n-border-radius);
 `,[$("resize-trigger",`
 width: 100%;
 height: 3px;
 top: 0;
 left: 0;
 transform: translateY(-1.5px);
 cursor: ns-resize;
 `)])]),n("body",[n(">",[c("drawer-container",`
 position: fixed;
 `)])]),c("drawer-container",`
 position: relative;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 pointer-events: none;
 `,[n("> *",`
 pointer-events: all;
 `)]),c("drawer-mask",`
 background-color: rgba(0, 0, 0, .3);
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 `,[g("invisible",`
 background-color: rgba(0, 0, 0, 0)
 `),He({enterDuration:"0.2s",leaveDuration:"0.2s",enterCubicBezier:"var(--n-bezier-in)",leaveCubicBezier:"var(--n-bezier-out)"})])]);const ot=["onClick"],nt={...re.props,show:Boolean,width:[Number,String],height:[Number,String],placement:{type:String,default:"right"},maskClosable:{type:Boolean,default:!0},showMask:{type:[Boolean,String],default:!0},to:[String,Object],displayDirective:{type:String,default:"if"},nativeScrollbar:{type:Boolean,default:!0},zIndex:Number,onMaskClick:Function,scrollbarProps:Object,contentClass:String,contentStyle:[Object,String],trapFocus:{type:Boolean,default:!0},onEsc:Function,autoFocus:{type:Boolean,default:!0},closeOnEsc:{type:Boolean,default:!0},blockScroll:{type:Boolean,default:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,defaultWidth:{type:[Number,String],default:251},defaultHeight:{type:[Number,String],default:251},onUpdateWidth:[Function,Array],onUpdateHeight:[Function,Array],"onUpdate:width":[Function,Array],"onUpdate:height":[Function,Array],"onUpdate:show":[Function,Array],onUpdateShow:[Function,Array],onAfterEnter:Function,onAfterLeave:Function,drawerStyle:[String,Object],drawerClass:String,target:null,onShow:Function,onHide:Function};var at=K({name:"Drawer",inheritAttrs:!1,props:nt,setup(e){const{mergedClsPrefixRef:o,namespaceRef:t,inlineThemeDisabled:p}=te(e),m=Ae(),x=re("Drawer","-drawer",rt,De,e,o),f=k(e.defaultWidth),y=k(e.defaultHeight),v=X(q(e,"width"),f),E=X(q(e,"height"),y),A=w(()=>{const{placement:r}=e;return r==="top"||r==="bottom"?"":V(v.value)}),D=w(()=>{const{placement:r}=e;return r==="left"||r==="right"?"":V(E.value)}),W=r=>{const{onUpdateWidth:a,"onUpdate:width":l}=e;a&&S(a,r),l&&S(l,r),f.value=r},F=r=>{const{onUpdateHeight:a,"onUpdate:width":l}=e;a&&S(a,r),l&&S(l,r),y.value=r},L=w(()=>[{width:A.value,height:D.value},e.drawerStyle||""]);function M(r){const{onMaskClick:a,maskClosable:l}=e;l&&C(!1),a&&a(r)}function P(r){M(r)}const N=We();function U(r){var a;(a=e.onEsc)==null||a.call(e),e.show&&e.closeOnEsc&&Pe(r)&&(N.value||C(!1))}function C(r){const{onHide:a,onUpdateShow:l,"onUpdate:show":i}=e;l&&S(l,r),i&&S(i,r),a&&!r&&S(a,r)}H(ee,{isMountedRef:m,mergedThemeRef:x,mergedClsPrefixRef:o,doUpdateShow:C,doUpdateHeight:F,doUpdateWidth:W});const T=w(()=>{const{common:{cubicBezierEaseInOut:r,cubicBezierEaseIn:a,cubicBezierEaseOut:l},self:{color:i,textColor:s,boxShadow:d,lineHeight:u,headerPadding:z,footerPadding:oe,borderRadius:ne,bodyPadding:ie,titleFontSize:ae,titleTextColor:se,titleFontWeight:le,headerBorderBottom:de,footerBorderTop:ce,closeIconColor:ue,closeIconColorHover:he,closeIconColorPressed:fe,closeColorHover:me,closeColorPressed:be,closeIconSize:ge,closeSize:ve,closeBorderRadius:we,resizableTriggerColorHover:pe}}=x.value;return{"--n-line-height":u,"--n-color":i,"--n-border-radius":ne,"--n-text-color":s,"--n-box-shadow":d,"--n-bezier":r,"--n-bezier-out":l,"--n-bezier-in":a,"--n-header-padding":z,"--n-body-padding":ie,"--n-footer-padding":oe,"--n-title-text-color":se,"--n-title-font-size":ae,"--n-title-font-weight":le,"--n-header-border-bottom":de,"--n-footer-border-top":ce,"--n-close-icon-color":ue,"--n-close-icon-color-hover":he,"--n-close-icon-color-pressed":fe,"--n-close-size":ve,"--n-close-color-hover":me,"--n-close-color-pressed":be,"--n-close-icon-size":ge,"--n-close-border-radius":we,"--n-resize-trigger-color-hover":pe}}),b=p?Le("drawer",void 0,T,e):void 0;return{mergedClsPrefix:o,namespace:t,mergedBodyStyle:L,handleOutsideClick:P,handleMaskClick:M,handleEsc:U,mergedTheme:x,cssVars:p?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender,isMounted:m}},render(){const{mergedClsPrefix:e}=this;return h(),B(Oe,{to:this.to,show:this.show},{default:()=>{var o;return(o=this.onRender)==null||o.call(this),_((h(),R("div",{class:I([`${e}-drawer-container`,this.namespace,this.themeClass]),style:J(this.cssVars),role:"none"},[this.showMask?(h(),B(G,{key:0,name:"fade-in-transition",appear:this.isMounted},{default:()=>this.show?(h(),R("div",{key:1,"aria-hidden":!0,class:I([`${e}-drawer-mask`,this.showMask==="transparent"&&`${e}-drawer-mask--invisible`]),onClick:this.handleMaskClick},null,10,ot)):null},1032,["appear"])):Q(()=>null),(h(),B(_e,Y(this.$attrs,{class:[this.drawerClass,this.$attrs.class],style:[this.mergedBodyStyle,this.$attrs.style],blockScroll:this.blockScroll,contentStyle:this.contentStyle,contentClass:this.contentClass,placement:this.placement,scrollbarProps:this.scrollbarProps,show:this.show,displayDirective:this.displayDirective,nativeScrollbar:this.nativeScrollbar,onAfterEnter:this.onAfterEnter,onAfterLeave:this.onAfterLeave,trapFocus:this.trapFocus,autoFocus:this.autoFocus,resizable:this.resizable,maxHeight:this.maxHeight,minHeight:this.minHeight,maxWidth:this.maxWidth,minWidth:this.minWidth,showMask:this.showMask,onEsc:this.handleEsc,onClickoutside:this.handleOutsideClick}),Z(this.$slots),1040,["class","style","blockScroll","contentStyle","contentClass","placement","scrollbarProps","show","displayDirective","nativeScrollbar","onAfterEnter","onAfterLeave","trapFocus","autoFocus","resizable","maxHeight","minHeight","maxWidth","minWidth","showMask","onEsc","onClickoutside"]))],6)),[[Ie,{zIndex:this.zIndex,enabled:this.show}]])}},1032,["to","show"])}});export{at as D};
