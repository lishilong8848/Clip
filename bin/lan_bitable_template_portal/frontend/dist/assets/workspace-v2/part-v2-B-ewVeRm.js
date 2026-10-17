import{bn as V,eT as Y,eN as G,dk as a,b2 as v,b0 as x,ae as re,c5 as $e,c_ as g,d4 as O,d5 as C,a2 as oe,cD as U,d3 as q,z as ke,dC as H,c9 as ne,bu as K,ep as ie,eD as xe,eQ as Be,eP as Ee,dc as Re,ez as Fe,aX as k,dw as W,aU as Te,bt as Me,ds as Oe,cK as He,aE as n,au as N,aG as f,aI as z,aH as T,bU as De,eH as ae,e_ as Pe,N as Ie,cn as _e,bv as Ae,eC as Q,ec as Z,ew as We,eI as Ue,bC as Ne,aL as M,bZ as J,a$ as ee,G as te,q as Le,e7 as je}from"./app-v2-DiNMOab8.js";const Xe=["onMouseenter","onMouseleave","onMousedown"],Ye={key:1,role:"none"};var Ve=V({name:"NDrawerContent",inheritAttrs:!1,props:{blockScroll:Boolean,show:{type:Boolean,default:void 0},displayDirective:{type:String,required:!0},placement:{type:String,required:!0},contentClass:String,contentStyle:[Object,String],nativeScrollbar:{type:Boolean,required:!0},scrollbarProps:Object,trapFocus:{type:Boolean,default:!0},autoFocus:{type:Boolean,default:!0},showMask:{type:[Boolean,String],required:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,onClickoutside:Function,onAfterLeave:Function,onAfterEnter:Function,onEsc:Function},setup(e){const t=H(!!e.show),r=H(null),w=ne(K);let b=0,$="",c=null;const y=H(!1),p=H(!1),B=k(()=>e.placement==="top"||e.placement==="bottom"),{mergedClsPrefixRef:D,mergedRtlRef:P}=ie(e),I=xe("Drawer",P,D),E=o,u=i=>{p.value=!0,b=B.value?i.clientY:i.clientX,$=document.body.style.cursor,document.body.style.cursor=B.value?"ns-resize":"ew-resize",document.body.addEventListener("mousemove",S),document.body.addEventListener("mouseleave",E),document.body.addEventListener("mouseup",o)},R=()=>{c!==null&&(window.clearTimeout(c),c=null),p.value?y.value=!0:c=window.setTimeout(()=>{y.value=!0},300)},L=()=>{c!==null&&(window.clearTimeout(c),c=null),y.value=!1},{doUpdateHeight:j,doUpdateWidth:X}=w,_=i=>{const{maxWidth:l}=e;if(l&&i>l)return l;const{minWidth:h}=e;return h&&i<h?h:i},A=i=>{const{maxHeight:l}=e;if(l&&i>l)return l;const{minHeight:h}=e;return h&&i<h?h:i};function S(i){var l,h;if(p.value)if(B.value){let m=((l=r.value)==null?void 0:l.offsetHeight)||0;const F=b-i.clientY;m+=e.placement==="bottom"?F:-F,m=A(m),j(m),b=i.clientY}else{let m=((h=r.value)==null?void 0:h.offsetWidth)||0;const F=b-i.clientX;m+=e.placement==="right"?F:-F,m=_(m),X(m),b=i.clientX}}function o(){p.value&&(b=0,p.value=!1,document.body.style.cursor=$,document.body.removeEventListener("mousemove",S),document.body.removeEventListener("mouseup",o),document.body.removeEventListener("mouseleave",E))}Be(()=>{e.show&&(t.value=!0)}),Ee(()=>e.show,i=>{i||o()}),Re(()=>{o()});const s=k(()=>{const{show:i}=e,l=[[G,i]];return e.showMask||l.push([Te,e.onClickoutside,void 0,{capture:!0}]),l});function d(){var i;t.value=!1,(i=e.onAfterLeave)==null||i.call(e)}return Fe(k(()=>e.blockScroll&&t.value)),W(Me,r),W(Oe,null),W(He,null),{bodyRef:r,rtlEnabled:I,mergedClsPrefix:w.mergedClsPrefixRef,isMounted:w.isMountedRef,mergedTheme:w.mergedThemeRef,displayed:t,transitionName:k(()=>({right:"slide-in-from-right-transition",left:"slide-in-from-left-transition",top:"slide-in-from-top-transition",bottom:"slide-in-from-bottom-transition"})[e.placement]),handleAfterLeave:d,bodyDirectives:s,handleMousedownResizeTrigger:u,handleMouseenterResizeTrigger:R,handleMouseleaveResizeTrigger:L,isDragging:p,isHoverOnResizeTrigger:y}},render(){const{$slots:e,mergedClsPrefix:t}=this;return this.displayDirective==="show"||this.displayed||this.show?Y((a(),v("div",Ye,[(a(),x(ke,{disabled:!this.showMask||!this.trapFocus,active:this.show,autoFocus:this.autoFocus,onEsc:this.onEsc},{default:()=>(a(),x(re,{name:this.transitionName,appear:this.isMounted,onAfterEnter:this.onAfterEnter,onAfterLeave:this.handleAfterLeave},{default:()=>Y($e("div",U(this.$attrs,{role:"dialog",ref:"bodyRef","aria-modal":"true",class:[`${t}-drawer`,this.rtlEnabled&&`${t}-drawer--rtl`,`${t}-drawer--${this.placement}-placement`,this.isDragging&&`${t}-drawer--unselectable`,this.nativeScrollbar&&`${t}-drawer--native-scrollbar`]}),[this.resizable?(a(),v("div",{key:2,class:g([`${t}-drawer__resize-trigger`,(this.isDragging||this.isHoverOnResizeTrigger)&&`${t}-drawer__resize-trigger--hover`]),onMouseenter:this.handleMouseenterResizeTrigger,onMouseleave:this.handleMouseleaveResizeTrigger,onMousedown:this.handleMousedownResizeTrigger},null,42,Xe)):null,this.nativeScrollbar?(a(),v("div",{key:3,class:g([`${t}-drawer-content-wrapper`,this.contentClass]),style:O(this.contentStyle),role:"none"},[C(()=>{var r;return(r=e.default)==null?void 0:r.call(e)})],6)):(a(),x(oe,U({key:4},this.scrollbarProps,{contentStyle:this.contentStyle,contentClass:[`${t}-drawer-content-wrapper`,this.contentClass],theme:this.mergedTheme.peers.Scrollbar,themeOverrides:this.mergedTheme.peerOverrides.Scrollbar}),q(e),1040,["contentStyle","contentClass","theme","themeOverrides"]))]),this.bodyDirectives)},1032,["name","appear","onAfterEnter","onAfterLeave"]))},1032,["disabled","active","autoFocus","onEsc"]))])),[[G,this.displayDirective==="if"||this.displayed||this.show]]):null}});const{cubicBezierEaseIn:qe,cubicBezierEaseOut:Ke}=N;function Ge({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-bottom"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${qe}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${Ke}`}),n(`&.${r}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateY(100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateY(100%)"})]}const{cubicBezierEaseIn:Qe,cubicBezierEaseOut:Ze}=N;function Je({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-left"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${Qe}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${Ze}`}),n(`&.${r}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateX(-100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateX(-100%)"})]}const{cubicBezierEaseIn:et,cubicBezierEaseOut:tt}=N;function rt({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-right"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${et}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${tt}`}),n(`&.${r}-transition-enter-to`,{transform:"translateX(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateX(100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateX(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateX(100%)"})]}const{cubicBezierEaseIn:ot,cubicBezierEaseOut:nt}=N;function it({duration:e="0.3s",leaveDuration:t="0.2s",name:r="slide-in-from-top"}={}){return[n(`&.${r}-transition-leave-active`,{transition:`transform ${t} ${ot}`}),n(`&.${r}-transition-enter-active`,{transition:`transform ${e} ${nt}`}),n(`&.${r}-transition-enter-to`,{transform:"translateY(0)"}),n(`&.${r}-transition-enter-from`,{transform:"translateY(-100%)"}),n(`&.${r}-transition-leave-from`,{transform:"translateY(0)"}),n(`&.${r}-transition-leave-to`,{transform:"translateY(-100%)"})]}var at=n([f("drawer",`
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
 `,[rt(),Je(),it(),Ge(),z("unselectable",`
 user-select: none; 
 -webkit-user-select: none;
 `),z("native-scrollbar",[f("drawer-content-wrapper",`
 overflow: auto;
 height: 100%;
 `)]),T("resize-trigger",`
 position: absolute;
 background-color: #0000;
 transition: background-color .3s var(--n-bezier);
 `,[z("hover",`
 background-color: var(--n-resize-trigger-color-hover);
 `)]),f("drawer-content-wrapper",`
 box-sizing: border-box;
 `),f("drawer-content",`
 height: 100%;
 display: flex;
 flex-direction: column;
 `,[z("native-scrollbar",[f("drawer-body-content-wrapper",`
 height: 100%;
 overflow: auto;
 `)]),f("drawer-body",`
 flex: 1 0 0;
 overflow: hidden;
 `),f("drawer-body-content-wrapper",`
 box-sizing: border-box;
 padding: var(--n-body-padding);
 `),f("drawer-header",`
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
 `,[T("main",`
 flex: 1;
 `),T("close",`
 margin-left: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `)]),f("drawer-footer",`
 display: flex;
 justify-content: flex-end;
 border-top: var(--n-footer-border-top);
 transition: border .3s var(--n-bezier);
 padding: var(--n-footer-padding);
 `)]),z("right-placement",`
 top: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-bottom-left-radius: var(--n-border-radius);
 `,[T("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 left: 0;
 transform: translateX(-1.5px);
 cursor: ew-resize;
 `)]),z("left-placement",`
 top: 0;
 bottom: 0;
 left: 0;
 border-top-right-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[T("resize-trigger",`
 width: 3px;
 height: 100%;
 top: 0;
 right: 0;
 transform: translateX(1.5px);
 cursor: ew-resize;
 `)]),z("top-placement",`
 top: 0;
 left: 0;
 right: 0;
 border-bottom-left-radius: var(--n-border-radius);
 border-bottom-right-radius: var(--n-border-radius);
 `,[T("resize-trigger",`
 width: 100%;
 height: 3px;
 bottom: 0;
 left: 0;
 transform: translateY(1.5px);
 cursor: ns-resize;
 `)]),z("bottom-placement",`
 left: 0;
 bottom: 0;
 right: 0;
 border-top-left-radius: var(--n-border-radius);
 border-top-right-radius: var(--n-border-radius);
 `,[T("resize-trigger",`
 width: 100%;
 height: 3px;
 top: 0;
 left: 0;
 transform: translateY(-1.5px);
 cursor: ns-resize;
 `)])]),n("body",[n(">",[f("drawer-container",`
 position: fixed;
 `)])]),f("drawer-container",`
 position: relative;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 pointer-events: none;
 `,[n("> *",`
 pointer-events: all;
 `)]),f("drawer-mask",`
 background-color: rgba(0, 0, 0, .3);
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 `,[z("invisible",`
 background-color: rgba(0, 0, 0, 0)
 `),De({enterDuration:"0.2s",leaveDuration:"0.2s",enterCubicBezier:"var(--n-bezier-in)",leaveCubicBezier:"var(--n-bezier-out)"})])]);const st=["onClick"],lt={...ae.props,show:Boolean,width:[Number,String],height:[Number,String],placement:{type:String,default:"right"},maskClosable:{type:Boolean,default:!0},showMask:{type:[Boolean,String],default:!0},to:[String,Object],displayDirective:{type:String,default:"if"},nativeScrollbar:{type:Boolean,default:!0},zIndex:Number,onMaskClick:Function,scrollbarProps:Object,contentClass:String,contentStyle:[Object,String],trapFocus:{type:Boolean,default:!0},onEsc:Function,autoFocus:{type:Boolean,default:!0},closeOnEsc:{type:Boolean,default:!0},blockScroll:{type:Boolean,default:!0},maxWidth:Number,maxHeight:Number,minWidth:Number,minHeight:Number,resizable:Boolean,defaultWidth:{type:[Number,String],default:251},defaultHeight:{type:[Number,String],default:251},onUpdateWidth:[Function,Array],onUpdateHeight:[Function,Array],"onUpdate:width":[Function,Array],"onUpdate:height":[Function,Array],"onUpdate:show":[Function,Array],onUpdateShow:[Function,Array],onAfterEnter:Function,onAfterLeave:Function,drawerStyle:[String,Object],drawerClass:String,target:null,onShow:Function,onHide:Function};var ut=V({name:"Drawer",inheritAttrs:!1,props:lt,setup(e){const{mergedClsPrefixRef:t,namespaceRef:r,inlineThemeDisabled:w}=ie(e),b=_e(),$=ae("Drawer","-drawer",at,Ae,e,t),c=H(e.defaultWidth),y=H(e.defaultHeight),p=Q(Z(e,"width"),c),B=Q(Z(e,"height"),y),D=k(()=>{const{placement:o}=e;return o==="top"||o==="bottom"?"":J(p.value)}),P=k(()=>{const{placement:o}=e;return o==="left"||o==="right"?"":J(B.value)}),I=o=>{const{onUpdateWidth:s,"onUpdate:width":d}=e;s&&M(s,o),d&&M(d,o),c.value=o},E=o=>{const{onUpdateHeight:s,"onUpdate:width":d}=e;s&&M(s,o),d&&M(d,o),y.value=o},u=k(()=>[{width:D.value,height:P.value},e.drawerStyle||""]);function R(o){const{onMaskClick:s,maskClosable:d}=e;d&&_(!1),s&&s(o)}function L(o){R(o)}const j=We();function X(o){var s;(s=e.onEsc)==null||s.call(e),e.show&&e.closeOnEsc&&Ne(o)&&(j.value||_(!1))}function _(o){const{onHide:s,onUpdateShow:d,"onUpdate:show":i}=e;d&&M(d,o),i&&M(i,o),s&&!o&&M(s,o)}W(K,{isMountedRef:b,mergedThemeRef:$,mergedClsPrefixRef:t,doUpdateShow:_,doUpdateHeight:E,doUpdateWidth:I});const A=k(()=>{const{common:{cubicBezierEaseInOut:o,cubicBezierEaseIn:s,cubicBezierEaseOut:d},self:{color:i,textColor:l,boxShadow:h,lineHeight:m,headerPadding:F,footerPadding:se,borderRadius:le,bodyPadding:de,titleFontSize:ce,titleTextColor:ue,titleFontWeight:he,headerBorderBottom:fe,footerBorderTop:be,closeIconColor:me,closeIconColorHover:ge,closeIconColorPressed:ve,closeColorHover:we,closeColorPressed:ye,closeIconSize:pe,closeSize:Se,closeBorderRadius:ze,resizableTriggerColorHover:Ce}}=$.value;return{"--n-line-height":m,"--n-color":i,"--n-border-radius":le,"--n-text-color":l,"--n-box-shadow":h,"--n-bezier":o,"--n-bezier-out":d,"--n-bezier-in":s,"--n-header-padding":F,"--n-body-padding":de,"--n-footer-padding":se,"--n-title-text-color":ue,"--n-title-font-size":ce,"--n-title-font-weight":he,"--n-header-border-bottom":fe,"--n-footer-border-top":be,"--n-close-icon-color":me,"--n-close-icon-color-hover":ge,"--n-close-icon-color-pressed":ve,"--n-close-size":Se,"--n-close-color-hover":we,"--n-close-color-pressed":ye,"--n-close-icon-size":pe,"--n-close-border-radius":ze,"--n-resize-trigger-color-hover":Ce}}),S=w?Ue("drawer",void 0,A,e):void 0;return{mergedClsPrefix:t,namespace:r,mergedBodyStyle:u,handleOutsideClick:L,handleMaskClick:R,handleEsc:X,mergedTheme:$,cssVars:w?void 0:A,themeClass:S==null?void 0:S.themeClass,onRender:S==null?void 0:S.onRender,isMounted:b}},render(){const{mergedClsPrefix:e}=this;return a(),x(Ie,{to:this.to,show:this.show},{default:()=>{var t;return(t=this.onRender)==null||t.call(this),Y((a(),v("div",{class:g([`${e}-drawer-container`,this.namespace,this.themeClass]),style:O(this.cssVars),role:"none"},[this.showMask?(a(),x(re,{key:0,name:"fade-in-transition",appear:this.isMounted},{default:()=>this.show?(a(),v("div",{key:1,"aria-hidden":!0,class:g([`${e}-drawer-mask`,this.showMask==="transparent"&&`${e}-drawer-mask--invisible`]),onClick:this.handleMaskClick},null,10,st)):null},1032,["appear"])):C(()=>null),(a(),x(Ve,U(this.$attrs,{class:[this.drawerClass,this.$attrs.class],style:[this.mergedBodyStyle,this.$attrs.style],blockScroll:this.blockScroll,contentStyle:this.contentStyle,contentClass:this.contentClass,placement:this.placement,scrollbarProps:this.scrollbarProps,show:this.show,displayDirective:this.displayDirective,nativeScrollbar:this.nativeScrollbar,onAfterEnter:this.onAfterEnter,onAfterLeave:this.onAfterLeave,trapFocus:this.trapFocus,autoFocus:this.autoFocus,resizable:this.resizable,maxHeight:this.maxHeight,minHeight:this.minHeight,maxWidth:this.maxWidth,minWidth:this.minWidth,showMask:this.showMask,onEsc:this.handleEsc,onClickoutside:this.handleOutsideClick}),q(this.$slots),1040,["class","style","blockScroll","contentStyle","contentClass","placement","scrollbarProps","show","displayDirective","nativeScrollbar","onAfterEnter","onAfterLeave","trapFocus","autoFocus","resizable","maxHeight","minHeight","maxWidth","minWidth","showMask","onEsc","onClickoutside"]))],6)),[[Pe,{zIndex:this.zIndex,enabled:this.show}]])}},1032,["to","show"])}});const dt={title:String,headerClass:String,headerStyle:[Object,String],footerClass:String,footerStyle:[Object,String],bodyClass:String,bodyStyle:[Object,String],bodyContentClass:String,bodyContentStyle:[Object,String],nativeScrollbar:{type:Boolean,default:!0},scrollbarProps:Object,closable:Boolean};var ht=V({name:"DrawerContent",props:dt,slots:Object,setup(){const e=ne(K,null);e||je("drawer-content","`n-drawer-content` must be placed inside `n-drawer`.");const{doUpdateShow:t}=e;function r(){t(!1)}return{handleCloseClick:r,mergedTheme:e.mergedThemeRef,mergedClsPrefix:e.mergedClsPrefixRef}},render(){const{title:e,mergedClsPrefix:t,nativeScrollbar:r,mergedTheme:w,bodyClass:b,bodyStyle:$,bodyContentClass:c,bodyContentStyle:y,headerClass:p,headerStyle:B,footerClass:D,footerStyle:P,scrollbarProps:I,closable:E,$slots:u}=this;return a(),v("div",{role:"none",class:g([`${t}-drawer-content`,r&&`${t}-drawer-content--native-scrollbar`])},[u.header||e||E?(a(),v("div",{key:0,class:g([`${t}-drawer-header`,p]),style:O(B),role:"none"},[ee("div",{class:g(`${t}-drawer-header__main`),role:"heading","aria-level":"1"},[u.header!==void 0?(a(),v(te,{key:0},[C(()=>u.header())],64)):(a(),v(te,{key:1},[C(()=>e)],64))],2),C(()=>E&&(a(),x(Le,{onClick:this.handleCloseClick,clsPrefix:t,class:g(`${t}-drawer-header__close`),absolute:!0},null,8,["onClick","clsPrefix","class"])))],6)):C(()=>null),r?(a(),v("div",{key:2,class:g([`${t}-drawer-body`,b]),style:O($),role:"none"},[ee("div",{class:g([`${t}-drawer-body-content-wrapper`,c]),style:O(y),role:"none"},[C(()=>{var R;return(R=u.default)==null?void 0:R.call(u)})],6)],6)):(a(),x(oe,U({key:3,themeOverrides:w.peerOverrides.Scrollbar,theme:w.peers.Scrollbar},I,{class:`${t}-drawer-body`,contentClass:[`${t}-drawer-body-content-wrapper`,c],contentStyle:y}),q(u),1040,["themeOverrides","theme","class","contentClass","contentStyle"])),u.footer?(a(),v("div",{key:4,class:g([`${t}-drawer-footer`,D]),style:O(P),role:"none"},[C(()=>u.footer())],6)):C(()=>null)],2)}});export{ht as D,ut as a};
