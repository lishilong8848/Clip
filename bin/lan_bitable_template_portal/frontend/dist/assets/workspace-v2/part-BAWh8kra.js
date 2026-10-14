import{c$ as At,d9 as Ee,bJ as Ue,b7 as se,bw as It,dy as Et,b4 as Mt,cI as E,ax as Me,a$ as Vt,av as Ot,b9 as Ht,aU as jt,cx as l,aS as p,cb as k,ci as C,bS as ge,aP as K,v as H,aQ as M,w as Xe,cM as Gt,q as Ft,bA as Dt,aL as ne,cn as Nt,ay as r,aw as R,aA as s,az as I,aB as Ut,ch as oe,m as Xt,i as qt,dA as qe,bl as Se,cY as Ve,cg as Oe,ah as we,dl as Yt,dw as Jt,dv as Kt,dI as ie,c9 as le,cu as Ye,cD as Qt,ct as Zt,dJ as ea,dB as ta,dk as He,aV as q,bt as pe,aI as aa,dL as ra,dG as na,ad as oa,da as Y,aC as ve,cp as ia,cs as la,cq as sa,cU as da,d8 as Z,dK as ca,a_ as ba,cN as fa,aR as je,cS as ua,ap as pa}from"./app-DydgIY7W.js";import{A as va}from"./part-B9JsYkNk.js";import{C as ga}from"./part-CUWA_5Mh.js";var Te=function(){return At.Date.now()},ha="Expected a function",ma=Math.max,xa=Math.min;function ya(e,i,n){var c,f,m,b,u,v,g=0,B=!1,L=!1,y=!0;if(typeof e!="function")throw new TypeError(ha);i=Ee(i)||0,Ue(n)&&(B=!!n.leading,L="maxWait"in n,m=L?ma(Ee(n.maxWait)||0,i):m,y="trailing"in n?!!n.trailing:y);function S(h){var A=c,N=f;return c=f=void 0,g=h,b=e.apply(N,A),b}function x(h){return g=h,u=setTimeout(_,i),B?S(h):b}function w(h){var A=h-v,N=h-g,j=i-A;return L?xa(j,m-N):j}function W(h){var A=h-v,N=h-g;return v===void 0||A>=i||A<0||L&&N>=m}function _(){var h=Te();if(W(h))return F(h);u=setTimeout(_,w(h))}function F(h){return u=void 0,y&&c?S(h):(c=f=void 0,b)}function J(){u!==void 0&&clearTimeout(u),g=0,c=v=f=u=void 0}function X(){return u===void 0?b:F(Te())}function D(){var h=Te(),A=W(h);if(c=arguments,f=this,v=h,A){if(u===void 0)return x(v);if(L)return clearTimeout(u),u=setTimeout(_,i),S(v)}return u===void 0&&(u=setTimeout(_,i)),b}return D.cancel=J,D.flush=X,D}var Ca="Expected a function";function Sa(e,i,n){var c=!0,f=!0;if(typeof e!="function")throw new TypeError(Ca);return Ue(n)&&(c="leading"in n?!!n.leading:c,f="trailing"in n?!!n.trailing:f),ya(e,i,{leading:c,maxWait:i,trailing:f})}const wa=Me(".v-x-scroll",{overflow:"auto",scrollbarWidth:"none"},[Me("&::-webkit-scrollbar",{width:0,height:0})]),Ta=se({name:"XScroll",props:{disabled:Boolean,onScroll:Function},setup(){const e=E(null);function i(f){!(f.currentTarget.offsetWidth<f.currentTarget.scrollWidth)||f.deltaY===0||(f.currentTarget.scrollLeft+=f.deltaY+f.deltaX,f.preventDefault())}const n=Et();return wa.mount({id:"vueuc/x-scroll",head:!0,anchorMetaName:Mt,ssr:n}),Object.assign({selfRef:e,handleWheel:i},{scrollTo(...f){var m;(m=e.value)===null||m===void 0||m.scrollTo(...f)}})},render(){return It("div",{ref:"selfRef",onScroll:this.onScroll,onWheel:this.disabled?void 0:this.handleWheel,class:"v-x-scroll"},this.$slots)}});var Ra={tabFontSizeSmall:"14px",tabFontSizeMedium:"14px",tabFontSizeLarge:"16px",tabGapSmallLine:"36px",tabGapMediumLine:"36px",tabGapLargeLine:"36px",tabGapSmallLineVertical:"8px",tabGapMediumLineVertical:"8px",tabGapLargeLineVertical:"8px",tabPaddingSmallLine:"6px 0",tabPaddingMediumLine:"10px 0",tabPaddingLargeLine:"14px 0",tabPaddingVerticalSmallLine:"6px 12px",tabPaddingVerticalMediumLine:"8px 16px",tabPaddingVerticalLargeLine:"10px 20px",tabGapSmallBar:"36px",tabGapMediumBar:"36px",tabGapLargeBar:"36px",tabGapSmallBarVertical:"8px",tabGapMediumBarVertical:"8px",tabGapLargeBarVertical:"8px",tabPaddingSmallBar:"4px 0",tabPaddingMediumBar:"6px 0",tabPaddingLargeBar:"10px 0",tabPaddingVerticalSmallBar:"6px 12px",tabPaddingVerticalMediumBar:"8px 16px",tabPaddingVerticalLargeBar:"10px 20px",tabGapSmallCard:"4px",tabGapMediumCard:"4px",tabGapLargeCard:"4px",tabGapSmallCardVertical:"4px",tabGapMediumCardVertical:"4px",tabGapLargeCardVertical:"4px",tabPaddingSmallCard:"8px 16px",tabPaddingMediumCard:"10px 20px",tabPaddingLargeCard:"12px 24px",tabPaddingSmallSegment:"4px 0",tabPaddingMediumSegment:"6px 0",tabPaddingLargeSegment:"8px 0",tabPaddingVerticalLargeSegment:"0 8px",tabPaddingVerticalSmallCard:"8px 12px",tabPaddingVerticalMediumCard:"10px 16px",tabPaddingVerticalLargeCard:"12px 20px",tabPaddingVerticalSmallSegment:"0 4px",tabPaddingVerticalMediumSegment:"0 6px",tabGapSmallSegment:"0",tabGapMediumSegment:"0",tabGapLargeSegment:"0",tabGapSmallSegmentVertical:"0",tabGapMediumSegmentVertical:"0",tabGapLargeSegmentVertical:"0",panePaddingSmall:"8px 0 0 0",panePaddingMedium:"12px 0 0 0",panePaddingLarge:"16px 0 0 0",closeSize:"18px",closeIconSize:"14px"};function Pa(e){const{textColor2:i,primaryColor:n,textColorDisabled:c,closeIconColor:f,closeIconColorHover:m,closeIconColorPressed:b,closeColorHover:u,closeColorPressed:v,tabColor:g,baseColor:B,dividerColor:L,fontWeight:y,textColor1:S,borderRadius:x,fontSize:w,fontWeightStrong:W}=e;return{...Ra,colorSegment:g,tabFontSizeCard:w,tabTextColorLine:S,tabTextColorActiveLine:n,tabTextColorHoverLine:n,tabTextColorDisabledLine:c,tabTextColorSegment:S,tabTextColorActiveSegment:i,tabTextColorHoverSegment:i,tabTextColorDisabledSegment:c,tabTextColorBar:S,tabTextColorActiveBar:n,tabTextColorHoverBar:n,tabTextColorDisabledBar:c,tabTextColorCard:S,tabTextColorHoverCard:S,tabTextColorActiveCard:n,tabTextColorDisabledCard:c,barColor:n,closeIconColor:f,closeIconColorHover:m,closeIconColorPressed:b,closeColorHover:u,closeColorPressed:v,closeBorderRadius:x,tabColor:g,tabColorSegment:B,tabBorderColor:L,tabFontWeightActive:y,tabFontWeight:y,tabBorderRadius:x,paneTextColor:i,fontWeightStrong:W}}const za=Vt({name:"Tabs",common:Ht,peers:{Button:Ot},self:Pa}),Je=jt("n-tabs"),La={tab:[String,Number,Object,Function],name:{type:[String,Number],required:!0},disabled:Boolean,displayDirective:{type:String,default:"if"},closable:{type:Boolean,default:void 0},tabProps:Object,label:[String,Number,Object,Function]},ka=["data-name","data-disabled"],$a={internalLeftPadded:Boolean,internalAddable:Boolean,internalCreatedByPane:Boolean,...Nt(La,["displayDirective"])};var ze=se({__TAB__:!0,inheritAttrs:!1,name:"Tab",props:$a,setup(e){const{mergedClsPrefixRef:i,valueRef:n,typeRef:c,closableRef:f,tabStyleRef:m,addTabStyleRef:b,tabClassRef:u,addTabClassRef:v,tabChangeIdRef:g,onBeforeLeaveRef:B,triggerRef:L,handleAdd:y,activateTab:S,handleClose:x}=Dt(Je);return{trigger:L,mergedClosable:ne(()=>{if(e.internalAddable)return!1;const{closable:w}=e;return w===void 0?f.value:w}),style:m,addStyle:b,tabClass:u,addTabClass:v,clsPrefix:i,value:n,type:c,handleClose(w){w.stopPropagation(),!e.disabled&&x(e.name)},activateTab(){if(e.disabled)return;if(e.internalAddable){y();return}const{name:w}=e,W=++g.id;if(w!==n.value){const{value:_}=B;_?Promise.resolve(_(e.name,n.value)).then(F=>{F&&g.id===W&&S(w)}):S(w)}}}},render(){const{internalAddable:e,clsPrefix:i,name:n,disabled:c,label:f,tab:m,value:b,mergedClosable:u,trigger:v,$slots:{default:g}}=this,B=f??m;return l(),p("div",{class:k(`${i}-tabs-tab-wrapper`)},[this.internalLeftPadded?(l(),p("div",{key:0,class:k(`${i}-tabs-tab-pad`)},null,2)):C(()=>null),(l(),p("div",ge({key:n,"data-name":n,"data-disabled":c?!0:void 0},ge({class:[`${i}-tabs-tab`,b===n&&`${i}-tabs-tab--active`,c&&`${i}-tabs-tab--disabled`,u&&`${i}-tabs-tab--closable`,e&&`${i}-tabs-tab--addable`,e?this.addTabClass:this.tabClass],onClick:v==="click"?this.activateTab:void 0,onMouseenter:v==="hover"?this.activateTab:void 0,style:e?this.addStyle:this.style},this.internalCreatedByPane?this.tabProps||{}:this.$attrs)),[K("span",{class:k(`${i}-tabs-tab__label`)},[e?(l(),p(H,{key:0},[K("div",{class:k(`${i}-tabs-tab__height-placeholder`)}," ",2),(l(),M(Xe,{clsPrefix:i},{default:()=>(l(),M(va))},1032,["clsPrefix"]))],64)):(l(),p(H,{key:1},[g?(l(),p(H,{key:0},[C(()=>g())],64)):(l(),p(H,{key:1},[typeof B=="object"?(l(),p(H,{key:0},[C(()=>B)],64)):(l(),p(H,{key:1},[C(()=>Gt(B??n))],64))],64))],64))],2),u&&this.type==="card"?(l(),M(Ft,{key:0,clsPrefix:i,class:k(`${i}-tabs-tab__close`),onClick:this.handleClose,disabled:c},null,8,["clsPrefix","class","onClick","disabled"])):C(()=>null)],16,ka))],2)}}),Ba=r("tabs",`
 box-sizing: border-box;
 width: 100%;
 display: flex;
 flex-direction: column;
 transition:
 background-color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
`,[R("&.transition-disabled",[r("tabs-tab",`
 transition: none !important;
 `),r("tabs-nav-scroll-content",`
 transition: none !important;
 `),r("tabs-tab-pad",`
 transition: none !important;
 `)]),s("segment-type",[r("tabs-rail",[R("&.transition-disabled",[r("tabs-capsule",`
 transition: none;
 `)])])]),s("top",[r("tab-pane",`
 padding: var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left);
 `)]),s("left",[r("tab-pane",`
 padding: var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left) var(--n-pane-padding-top);
 `)]),s("left, right",`
 flex-direction: row;
 `,[r("tabs-bar",`
 width: 2px;
 right: 0;
 transition:
 top .2s var(--n-bezier),
 max-height .2s var(--n-bezier),
 background-color .3s var(--n-bezier);
 `),r("tabs-tab",`
 padding: var(--n-tab-padding-vertical); 
 `)]),s("right",`
 flex-direction: row-reverse;
 `,[r("tab-pane",`
 padding: var(--n-pane-padding-left) var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom);
 `),r("tabs-bar",`
 left: 0;
 `)]),s("bottom",`
 flex-direction: column-reverse;
 justify-content: flex-end;
 `,[r("tab-pane",`
 padding: var(--n-pane-padding-bottom) var(--n-pane-padding-right) var(--n-pane-padding-top) var(--n-pane-padding-left);
 `),r("tabs-bar",`
 top: 0;
 `)]),r("tabs-rail",`
 position: relative;
 padding: 3px;
 border-radius: var(--n-tab-border-radius);
 width: 100%;
 background-color: var(--n-color-segment);
 transition: background-color .3s var(--n-bezier);
 display: flex;
 align-items: center;
 `,[r("tabs-capsule",`
 border-radius: var(--n-tab-border-radius);
 position: absolute;
 left: 0;
 top: 0;
 pointer-events: none;
 background-color: var(--n-tab-color-segment);
 box-shadow: 0 1px 3px 0 rgba(0, 0, 0, .08);
 transition: transform 0.3s var(--n-bezier);
 `),r("tabs-tab-wrapper",`
 flex-basis: 0;
 flex-grow: 1;
 display: flex;
 align-items: center;
 justify-content: center;
 `,[r("tabs-tab",`
 overflow: hidden;
 border-radius: var(--n-tab-border-radius);
 width: 100%;
 display: flex;
 align-items: center;
 justify-content: center;
 `,[s("active",`
 font-weight: var(--n-font-weight-strong);
 color: var(--n-tab-text-color-active);
 `),R("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])])]),s("flex",[r("tabs-nav",`
 width: 100%;
 position: relative;
 `,[r("tabs-wrapper",`
 width: 100%;
 `,[r("tabs-tab",`
 margin-right: 0;
 `)])])]),r("tabs-nav",`
 box-sizing: border-box;
 line-height: 1.5;
 display: flex;
 transition: border-color .3s var(--n-bezier);
 `,[I("prefix, suffix",`
 display: flex;
 align-items: center;
 `),I("prefix","padding-right: 16px;"),I("suffix","padding-left: 16px;")]),s("top, bottom",[R(">",[r("tabs-nav",[r("tabs-nav-scroll-wrapper",[R("&::before",`
 top: 0;
 bottom: 0;
 left: 0;
 width: 20px;
 `),R("&::after",`
 top: 0;
 bottom: 0;
 right: 0;
 width: 20px;
 `),s("shadow-start",[R("&::before",`
 box-shadow: inset 10px 0 8px -8px rgba(0, 0, 0, .12);
 `)]),s("shadow-end",[R("&::after",`
 box-shadow: inset -10px 0 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),s("left, right",[r("tabs-nav-scroll-content",`
 flex-direction: column;
 `),R(">",[r("tabs-nav",[r("tabs-nav-scroll-wrapper",[R("&::before",`
 top: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),R("&::after",`
 bottom: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),s("shadow-start",[R("&::before",`
 box-shadow: inset 0 10px 8px -8px rgba(0, 0, 0, .12);
 `)]),s("shadow-end",[R("&::after",`
 box-shadow: inset 0 -10px 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),r("tabs-nav-scroll-wrapper",`
 flex: 1;
 position: relative;
 overflow: hidden;
 `,[r("tabs-nav-y-scroll",`
 height: 100%;
 width: 100%;
 overflow-y: auto; 
 scrollbar-width: none;
 `,[R("&::-webkit-scrollbar, &::-webkit-scrollbar-track-piece, &::-webkit-scrollbar-thumb",`
 width: 0;
 height: 0;
 display: none;
 `)]),R("&::before, &::after",`
 transition: box-shadow .3s var(--n-bezier);
 pointer-events: none;
 content: "";
 position: absolute;
 z-index: 1;
 `),R("&.transition-disabled",[R("&::before, &::after",`
 transition: none;
 `)])]),r("tabs-nav-scroll-content",`
 display: flex;
 position: relative;
 min-width: 100%;
 min-height: 100%;
 width: fit-content;
 box-sizing: border-box;
 `),r("tabs-wrapper",`
 display: inline-flex;
 flex-wrap: nowrap;
 position: relative;
 `),r("tabs-tab-wrapper",`
 display: flex;
 flex-wrap: nowrap;
 flex-shrink: 0;
 flex-grow: 0;
 `),r("tabs-tab",`
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
 `,[s("disabled",{cursor:"not-allowed"}),I("close",`
 margin-inline-start: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `),I("label",`
 display: flex;
 align-items: center;
 z-index: 1;
 `)]),r("tabs-bar",`
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
 `,[R("&.transition-disabled",`
 transition: none;
 `),s("disabled",`
 background-color: var(--n-tab-text-color-disabled)
 `)]),r("tabs-pane-wrapper",`
 position: relative;
 overflow: hidden;
 transition: max-height .2s var(--n-bezier);
 `),r("tab-pane",`
 color: var(--n-pane-text-color);
 width: 100%;
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 opacity .2s var(--n-bezier);
 left: 0;
 right: 0;
 top: 0;
 `,[R("&.next-transition-leave-active, &.prev-transition-leave-active, &.next-transition-enter-active, &.prev-transition-enter-active",`
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 transform .2s var(--n-bezier),
 opacity .2s var(--n-bezier);
 `),R("&.next-transition-leave-active, &.prev-transition-leave-active",`
 position: absolute;
 `),R("&.next-transition-enter-from, &.prev-transition-leave-to",`
 transform: translateX(32px);
 opacity: 0;
 `),R("&.next-transition-leave-to, &.prev-transition-enter-from",`
 transform: translateX(-32px);
 opacity: 0;
 `),R("&.next-transition-leave-from, &.next-transition-enter-to, &.prev-transition-leave-from, &.prev-transition-enter-to",`
 transform: translateX(0);
 opacity: 1;
 `)]),r("tabs-tab-pad",`
 box-sizing: border-box;
 width: var(--n-tab-gap);
 flex-grow: 0;
 flex-shrink: 0;
 `),s("line-type, bar-type",[r("tabs-tab",`
 font-weight: var(--n-tab-font-weight);
 box-sizing: border-box;
 vertical-align: bottom;
 `,[R("&:hover",{color:"var(--n-tab-text-color-hover)"}),s("active",`
 color: var(--n-tab-text-color-active);
 font-weight: var(--n-tab-font-weight-active);
 `),s("disabled",{color:"var(--n-tab-text-color-disabled)"})])]),r("tabs-nav",[I("prefix, suffix",`
 border-color: var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-color: var(--n-tab-border-color);
 `),s("line-type",[s("top",[I("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 bottom: -1px;
 `)]),s("left",[I("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 right: -1px;
 `)]),s("right",[I("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 left: -1px;
 `)]),s("bottom",[I("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 top: -1px;
 `)]),I("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-nav-scroll-content",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-bar",`
 border-radius: 0;
 `)]),s("card-type",[I("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-pad",`
 flex-grow: 1;
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-tab-pad",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-tab",`
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
 `,[s("addable",`
 padding-left: 8px;
 padding-right: 8px;
 font-size: 16px;
 justify-content: center;
 `,[I("height-placeholder",`
 width: 0;
 font-size: var(--n-tab-font-size);
 `),Ut("disabled",[R("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])]),s("closable","padding-inline-end: 8px;"),s("active",`
 background-color: #0000;
 font-weight: var(--n-tab-font-weight-active);
 color: var(--n-tab-text-color-active);
 `),s("disabled","color: var(--n-tab-text-color-disabled);")])]),s("left, right",`
 flex-direction: column; 
 `,[I("prefix, suffix",`
 padding: var(--n-tab-padding-vertical);
 `),r("tabs-wrapper",`
 flex-direction: column;
 `),r("tabs-tab-wrapper",`
 flex-direction: column;
 `,[r("tabs-tab-pad",`
 height: var(--n-tab-gap-vertical);
 width: 100%;
 `)])]),s("top",[s("card-type",[r("tabs-scroll-padding","border-bottom: 1px solid var(--n-tab-border-color);"),I("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-top-right-radius: var(--n-tab-border-radius);
 `,[s("active",`
 border-bottom: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `)])]),s("left",[s("card-type",[r("tabs-scroll-padding","border-right: 1px solid var(--n-tab-border-color);"),I("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-bottom-left-radius: var(--n-tab-border-radius);
 `,[s("active",`
 border-right: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `)])]),s("right",[s("card-type",[r("tabs-scroll-padding","border-left: 1px solid var(--n-tab-border-color);"),I("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-right-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[s("active",`
 border-left: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `)])]),s("bottom",[s("card-type",[r("tabs-scroll-padding","border-top: 1px solid var(--n-tab-border-color);"),I("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-bottom-left-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[s("active",`
 border-top: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `)])])]),r("tabs-scroll-button",[s("start",`
 padding-left: 10px;
 padding-right: 6px;
 `),s("end",`
 padding-right: 10px;
 padding-left: 6px;
 `),s("up",`
 padding-bottom: 10px;
 `),s("down",`
 padding-top: 10px;
 `)])]),Ge=se({name:"TabsButton",props:{type:{type:String,default:"next"},mergedClsPrefix:{type:String,required:!0},vertical:Boolean,disabled:Boolean,rtl:Boolean,theme:Object,themeOverrides:Object,onClick:Function},setup(e){return{handleClick:()=>{var n;e.disabled||(n=e.onClick)==null||n.call(e,e.type)}}},render(){const{mergedClsPrefix:e,disabled:i,type:n,vertical:c,rtl:f,theme:m,themeOverrides:b,handleClick:u}=this,v=n==="next",g=c?v:f?!v:v;return l(),M(qt,{text:!0,disabled:i,size:"small",theme:m,themeOverrides:b,onClick:u,class:k([`${e}-tabs-scroll-button`,!c&&n==="prev"&&`${e}-tabs-scroll-button--start`,!c&&n==="next"&&`${e}-tabs-scroll-button--end`,c&&n==="prev"&&`${e}-tabs-scroll-button--up`,c&&n==="next"&&`${e}-tabs-scroll-button--down`])},{icon:()=>(l(),M(Xe,{clsPrefix:e,style:oe(c?{transform:"rotate(90deg)"}:void 0)},{default:()=>g?(l(),M(Xt,{key:1})):(l(),M(ga,{key:2}))},1032,["clsPrefix","style"]))},1032,["disabled","theme","themeOverrides","onClick","class"])}});const Re=Sa,_a={...qe.props,value:[String,Number],defaultValue:[String,Number],trigger:{type:String,default:"click"},type:{type:String,default:"bar"},closable:Boolean,justifyContent:String,size:String,placement:{type:String,default:"top"},tabStyle:[String,Object],tabClass:String,addTabStyle:[String,Object],addTabClass:String,barWidth:Number,paneClass:String,paneStyle:[String,Object],paneWrapperClass:String,paneWrapperStyle:[String,Object],addable:[Boolean,Object],tabsPadding:{type:Number,default:0},animated:Boolean,onBeforeLeave:Function,onAdd:Function,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onClose:[Function,Array],labelSize:String,activeName:[String,Number],onActiveNameChange:[Function,Array],showScrollButton:Boolean,centerActiveTab:Boolean};var ja=se({name:"Tabs",props:_a,slots:Object,setup(e,{slots:i}){var Ae,Ie;const{mergedClsPrefixRef:n,inlineThemeDisabled:c,mergedComponentPropsRef:f,mergedRtlRef:m}=Yt(e),b=Jt("Tabs",m,n),u=ne(()=>{const{placement:t}=e;return t==="start"?b!=null&&b.value?"right":"left":t==="end"?b!=null&&b.value?"left":"right":t}),v=qe("Tabs","-tabs",Ba,za,e,n),g=E(null),B=E(null),L=E(null),y=E(null),S=E(null),x=E(null),w=E(null),W=E(!0),_=E(!0),F=He(e,["labelSize","size"]),J=ne(()=>{var a,o;if(F.value)return F.value;const t=(o=(a=f==null?void 0:f.value)==null?void 0:a.Tabs)==null?void 0:o.size;return t||"medium"}),X=He(e,["activeName","value"]),D=E(X.value??e.defaultValue??(i.default?(Ie=(Ae=Se(i.default())[0])==null?void 0:Ae.props)==null?void 0:Ie.name:null)),h=Kt(X,D),A={id:0},N=ne(()=>{if(!(!e.justifyContent||e.type==="card"))return{display:"flex",justifyContent:e.justifyContent}});ie(h,()=>{A.id=0,V(),le(()=>{he()})});function j(){var a;const{value:t}=h;return t===null?null:(a=g.value)==null?void 0:a.querySelector(`[data-name="${t}"]`)}function de(t){if(e.type==="card")return;const{value:a}=L;if(!a)return;const o=a.style.opacity==="0";if(t){const d=`${n.value}-tabs-bar--disabled`,{barWidth:T}=e,$=u.value;if(t.dataset.disabled==="true"?a.classList.add(d):a.classList.remove(d),["top","bottom"].includes($)){if(P(["top","maxHeight","height"]),typeof T=="number"&&t.offsetWidth>=T){const z=Math.floor((t.offsetWidth-T)/2)+t.offsetLeft;a.style.left=`${z}px`,a.style.maxWidth=`${T}px`}else a.style.left=`${t.offsetLeft}px`,a.style.maxWidth=`${t.offsetWidth}px`;a.style.width="8192px",o&&(a.style.transition="none"),a.offsetWidth,o&&(a.style.transition="",a.style.opacity="1")}else{if(P(["left","maxWidth","width"]),typeof T=="number"&&t.offsetHeight>=T){const z=Math.floor((t.offsetHeight-T)/2)+t.offsetTop;a.style.top=`${z}px`,a.style.maxHeight=`${T}px`}else a.style.top=`${t.offsetTop}px`,a.style.maxHeight=`${t.offsetHeight}px`;a.style.height="8192px",o&&(a.style.transition="none"),a.offsetHeight,o&&(a.style.transition="",a.style.opacity="1")}}}function U(){if(e.type==="card")return;const{value:t}=L;t&&(t.style.opacity="0")}function P(t){const{value:a}=L;if(a)for(const o of t)a.style[o]=""}function V(){if(e.type==="card")return;const t=j();t?de(t):U()}function ee(t,a,o,d){const T=t.getBoundingClientRect(),$=a.getBoundingClientRect(),z=o?"left":"top",O=o?"right":"bottom";let G=0;d?G=($[z]+$[O])/2-(T[z]+T[O])/2:$[z]<T[z]?G=$[z]-T[z]:$[O]>T[O]&&(G=$[O]-T[O]),G!==0&&t.scrollBy({[z]:G,behavior:"smooth"})}function he(){var o;const t=["top","bottom"].includes(u.value),a=j();if(a)if(t){const d=(o=x.value)==null?void 0:o.$el;if(!d)return;ee(d,a,t,e.centerActiveTab)}else{const{value:d}=w;if(!d)return;ee(d,a,t,e.centerActiveTab)}}const ce=E(null);let me=0,Q=null;function Ke(t){const a=ce.value;if(a){me=t.getBoundingClientRect().height;const o=`${me}px`,d=()=>{a.style.height=o,a.style.maxHeight=o};Q?(d(),Q(),Q=null):Q=d}}function Qe(t){const a=ce.value;if(a){const o=t.getBoundingClientRect().height,d=()=>{document.body.offsetHeight,a.style.maxHeight=`${o}px`,a.style.height=`${Math.max(me,o)}px`};Q?(Q(),Q=null,d()):Q=d}}function Ze(){const t=ce.value;if(t){t.style.maxHeight="",t.style.height="";const{paneWrapperStyle:a}=e;if(typeof a=="string")t.style.cssText=a;else if(a){const{maxHeight:o,height:d}=a;o!==void 0&&(t.style.maxHeight=o),d!==void 0&&(t.style.height=d)}}}const Le={value:[]},ke=E("next");function et(t){const a=h.value;let o="next";for(const d of Le.value){if(d===a)break;if(d===t){o="prev";break}}ke.value=o,tt(t)}function tt(t){const{onActiveNameChange:a,onUpdateValue:o,"onUpdate:value":d}=e;a&&ve(a,t),o&&ve(o,t),d&&ve(d,t),D.value=t}function at(t){const{onClose:a}=e;a&&ve(a,t)}function rt(t){if(["top","bottom"].includes(u.value)){const{value:a}=x;if(!a)return;const o=a.$el;if(!o)return;const d=o.offsetWidth,T=!!(b!=null&&b.value),$=t==="next"?d:-d;o.scrollBy({left:T?-$:$,behavior:"smooth"})}else{const{value:a}=w;if(!a)return;const o=a.offsetHeight,d=t==="next"?a.scrollTop+o:a.scrollTop-o;a.scrollTo({top:d,left:0,behavior:"smooth"})}}let xe=!0;function ye(){const{value:t}=L;if(!t)return;xe&&(xe=!1);const a="transition-disabled";t.classList.add(a),V(),t.classList.remove(a)}const te=E(null);function be({transitionDisabled:t}){const a=g.value;if(!a)return;t&&a.classList.add("transition-disabled");const o=j();o&&te.value&&(te.value.style.width=`${o.offsetWidth}px`,te.value.style.height=`${o.offsetHeight}px`,te.value.style.transform=`translate(${o.offsetLeft}px, ${o.offsetTop}px)`,t&&te.value.offsetWidth),t&&a.classList.remove("transition-disabled")}ie([h],()=>{e.type==="segment"&&le(()=>{be({transitionDisabled:!1})})}),Ye(()=>{e.type==="segment"&&be({transitionDisabled:!0})});let $e=0;function nt(t){var o;if(t.contentRect.width===0&&t.contentRect.height===0||$e===t.contentRect.width)return;$e=t.contentRect.width;const{type:a}=e;(a==="line"||a==="bar")&&(xe||(o=e.justifyContent)!=null&&o.startsWith("space"))&&ye(),a!=="segment"&&fe(_e())}const ot=Re(nt,64);function Be(){const{type:t}=e;t==="line"||t==="bar"?ye():t==="segment"&&be({transitionDisabled:!0})}ie([()=>e.justifyContent,()=>e.size],()=>{le(()=>{(e.type==="line"||e.type==="bar")&&ye()})}),ie([u,()=>b==null?void 0:b.value],()=>{le(()=>{Be(),fe(_e(),{instantly:!0})})}),ie(()=>e.type,()=>{le(()=>{const t=B.value;t&&(t.classList.add("transition-disabled"),Be(),t.offsetWidth,t.classList.remove("transition-disabled"))})});const ae=E(!1);function it(t){var O;const{target:a,contentRect:{width:o,height:d}}=t,T=a.parentElement.parentElement.offsetWidth,$=a.parentElement.parentElement.offsetHeight,z=u.value;if(!ae.value)z==="top"||z==="bottom"?T<o&&(ae.value=!0):$<d&&(ae.value=!0);else{const{value:G}=S;if(!G)return;z==="top"||z==="bottom"?T-o>G.$el.offsetWidth&&(ae.value=!1):$-d>G.$el.offsetHeight&&(ae.value=!1)}fe(((O=x.value)==null?void 0:O.$el)||null)}const lt=Re(it,64);function st(){const{onAdd:t}=e;t&&t()}const Ce=E(!1);function _e(){var a;const t=u.value;return(t==="top"||t==="bottom"?(a=x.value)==null?void 0:a.$el:w.value)||null}function fe(t,a={instantly:!1}){if(!t)return;const o=a.instantly?y.value:null;o&&o.classList.add("transition-disabled");const d=1,T=u.value;if(T==="top"||T==="bottom"){const{scrollLeft:$,scrollWidth:z,offsetWidth:O}=t,G=Math.abs($);W.value=G<=d,_.value=G+O>=z-d,Ce.value=O<z-d}else{const{scrollTop:$,scrollHeight:z,offsetHeight:O}=t;W.value=$<=d,_.value=$+O>=z-d,Ce.value=O<z-d}o&&(o.offsetWidth,o.classList.remove("transition-disabled"))}const dt=Re(t=>{fe(t.target)},64);Qt(Je,{triggerRef:Y(e,"trigger"),tabStyleRef:Y(e,"tabStyle"),tabClassRef:Y(e,"tabClass"),addTabStyleRef:Y(e,"addTabStyle"),addTabClassRef:Y(e,"addTabClass"),paneClassRef:Y(e,"paneClass"),paneStyleRef:Y(e,"paneStyle"),mergedClsPrefixRef:n,typeRef:Y(e,"type"),closableRef:Y(e,"closable"),valueRef:h,tabChangeIdRef:A,onBeforeLeaveRef:Y(e,"onBeforeLeave"),activateTab:et,handleClose:at,handleAdd:st}),Zt(()=>{V(),he()}),ea(()=>{const{value:t}=y;if(!t)return;const{value:a}=n,o=`${a}-tabs-nav-scroll-wrapper--shadow-start`,d=`${a}-tabs-nav-scroll-wrapper--shadow-end`;W.value?t.classList.remove(o):t.classList.add(o),_.value?t.classList.remove(d):t.classList.add(d)});const ct={syncBarPosition:()=>{V()},scrollToCurrentTab:()=>{he()}},bt=()=>{be({transitionDisabled:!0})},We=ne(()=>{const{value:t}=J,{type:a}=e,o=`${t}${{card:"Card",bar:"Bar",line:"Line",segment:"Segment"}[a]}`,{self:{barColor:d,closeIconColor:T,closeIconColorHover:$,closeIconColorPressed:z,tabColor:O,tabBorderColor:G,paneTextColor:ft,tabFontWeight:ut,tabBorderRadius:pt,tabFontWeightActive:vt,colorSegment:gt,fontWeightStrong:ht,tabColorSegment:mt,closeSize:xt,closeIconSize:yt,closeColorHover:Ct,closeColorPressed:St,closeBorderRadius:wt,[q("panePadding",t)]:ue,[q("tabPadding",o)]:Tt,[q("tabPaddingVertical",o)]:Rt,[q("tabGap",o)]:Pt,[q("tabGap",`${o}Vertical`)]:zt,[q("tabTextColor",a)]:Lt,[q("tabTextColorActive",a)]:kt,[q("tabTextColorHover",a)]:$t,[q("tabTextColorDisabled",a)]:Bt,[q("tabFontSize",t)]:_t},common:{cubicBezierEaseInOut:Wt}}=v.value;return{"--n-bezier":Wt,"--n-color-segment":gt,"--n-bar-color":d,"--n-tab-font-size":_t,"--n-tab-text-color":Lt,"--n-tab-text-color-active":kt,"--n-tab-text-color-disabled":Bt,"--n-tab-text-color-hover":$t,"--n-pane-text-color":ft,"--n-tab-border-color":G,"--n-tab-border-radius":pt,"--n-close-size":xt,"--n-close-icon-size":yt,"--n-close-color-hover":Ct,"--n-close-color-pressed":St,"--n-close-border-radius":wt,"--n-close-icon-color":T,"--n-close-icon-color-hover":$,"--n-close-icon-color-pressed":z,"--n-tab-color":O,"--n-tab-font-weight":ut,"--n-tab-font-weight-active":vt,"--n-tab-padding":Tt,"--n-tab-padding-vertical":Rt,"--n-tab-gap":Pt,"--n-tab-gap-vertical":zt,"--n-pane-padding-left":pe(ue,"left"),"--n-pane-padding-right":pe(ue,"right"),"--n-pane-padding-top":pe(ue,"top"),"--n-pane-padding-bottom":pe(ue,"bottom"),"--n-font-weight-strong":ht,"--n-tab-color-segment":mt}}),re=c?ta("tabs",ne(()=>`${J.value[0]}${e.type[0]}`),We,e):void 0;return{mergedClsPrefix:n,mergedValue:h,renderedNames:new Set,segmentCapsuleElRef:te,tabsPaneWrapperRef:ce,tabsElRef:g,selfElRef:B,barElRef:L,addTabInstRef:S,xScrollInstRef:x,scrollWrapperElRef:y,addTabFixed:ae,tabWrapperStyle:N,handleNavResize:ot,mergedSize:J,handleScroll:dt,handleTabsResize:lt,cssVars:c?void 0:We,themeClass:re==null?void 0:re.themeClass,animationDirection:ke,renderNameListRef:Le,yScrollElRef:w,handleSegmentResize:bt,onAnimationBeforeLeave:Ke,onAnimationEnter:Qe,onAnimationAfterEnter:Ze,onRender:re==null?void 0:re.onRender,startReachedRef:W,endReachedRef:_,isOverflow:Ce,handleButtonClick:rt,mergedTheme:v,rtlEnabled:b,mergedPlacement:u,...ct}},render(){const{mergedClsPrefix:e,type:i,mergedPlacement:n,addTabFixed:c,addable:f,mergedSize:m,renderNameListRef:b,onRender:u,paneWrapperClass:v,paneWrapperStyle:g,startReachedRef:B,endReachedRef:L,isOverflow:y,showScrollButton:S,handleButtonClick:x,mergedTheme:w,rtlEnabled:W,$slots:{default:_,prefix:F,suffix:J}}=this;u==null||u();const X=_?Se(_()).filter(P=>P.type.__TAB_PANE__===!0):[],D=_?Se(_()).filter(P=>P.type.__TAB__===!0):[],h=!D.length,A=i==="card",N=i==="segment",j=!A&&!N&&this.justifyContent;b.value=[];const de=()=>{const P=(l(),p("div",{style:oe(this.tabWrapperStyle),class:k(`${e}-tabs-wrapper`)},[j?C(()=>null):(l(),p("div",{key:1,class:k(`${e}-tabs-scroll-padding`),style:oe(n==="top"||n==="bottom"?{width:`${this.tabsPadding}px`}:{height:`${this.tabsPadding}px`})},null,6)),h?(l(),p(H,{key:2},[C(()=>X.map((V,ee)=>(b.value.push(V.props.name),Pe((l(),M(ze,ge(V.props,{internalCreatedByPane:!0,internalLeftPadded:ee!==0&&(!j||j==="center"||j==="start"||j==="end")}),Oe(V.children?{default:V.children.tab}:void 0),1040,["internalLeftPadded"]))))))],64)):(l(),p(H,{key:3},[C(()=>D.map((V,ee)=>(b.value.push(V.props.name),Pe(ee!==0&&!j?Ne(V):V))))],64)),!c&&f&&A?(l(),p(H,{key:4},[C(()=>De(f,(h?X.length:D.length)!==0))],64)):C(()=>null),j?C(()=>null):(l(),p("div",{key:7,class:k(`${e}-tabs-scroll-padding`),style:oe({width:`${this.tabsPadding}px`})},null,6)),A?C(()=>null):(l(),p("div",{key:9,ref:"barElRef",class:k(`${e}-tabs-bar`)},null,2))],6));return l(),p("div",{ref:"tabsElRef",class:k(`${e}-tabs-nav-scroll-content`)},[A&&f?(l(),M(we,{key:0,onResize:this.handleTabsResize},{default:()=>P},1032,["onResize"])):(l(),p(H,{key:1},[C(()=>P)],64)),A?(l(),p("div",{key:2,class:k(`${e}-tabs-pad`)},null,2)):C(()=>null)],2)},U=N?"top":n;return l(),p("div",{ref:"selfElRef",class:k([`${e}-tabs`,this.themeClass,`${e}-tabs--${i}-type`,`${e}-tabs--${m}-size`,j&&`${e}-tabs--flex`,`${e}-tabs--${U}`,W&&`${e}-tabs--rtl`]),style:oe(this.cssVars)},[K("div",{class:k([`${e}-tabs-nav--${i}-type`,`${e}-tabs-nav--${U}`,`${e}-tabs-nav`])},[C(()=>Ve(F,P=>P&&(l(),p("div",{class:k(`${e}-tabs-nav__prefix`)},[C(()=>P)],2)))),N?(l(),M(we,{key:0,onResize:this.handleSegmentResize},{default:()=>(l(),p("div",{class:k(`${e}-tabs-rail`),ref:"tabsElRef"},[K("div",{class:k(`${e}-tabs-capsule`),ref:"segmentCapsuleElRef"},[K("div",{class:k(`${e}-tabs-wrapper`)},[K("div",{class:k(`${e}-tabs-tab`)},null,2)],2)],2),h?(l(),p(H,{key:0},[C(()=>X.map((P,V)=>(b.value.push(P.props.name),l(),M(ze,ge(P.props,{internalCreatedByPane:!0,internalLeftPadded:V!==0}),Oe(P.children?{default:P.children.tab}:void 0),1040,["internalLeftPadded"]))))],64)):(l(),p(H,{key:1},[C(()=>D.map((P,V)=>(b.value.push(P.props.name),V===0?P:Ne(P))))],64))],2))},1032,["onResize"])):(l(),p(H,{key:1},[C(()=>S&&y&&(l(),M(Ge,{mergedClsPrefix:e,type:"prev",vertical:U==="left"||U==="right",disabled:B,rtl:!!W,theme:w.peers.Button,themeOverrides:w.peerOverrides.Button,onClick:x},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"]))),(l(),M(we,{onResize:this.handleNavResize},{default:()=>(l(),p("div",{class:k(`${e}-tabs-nav-scroll-wrapper`),ref:"scrollWrapperElRef"},[["top","bottom"].includes(U)?(l(),M(Ta,{key:0,ref:"xScrollInstRef",onScroll:this.handleScroll},{default:de},1032,["onScroll"])):(l(),p("div",{key:1,class:k(`${e}-tabs-nav-y-scroll`),onScroll:this.handleScroll,ref:"yScrollElRef"},[C(()=>de())],42,["onScroll"]))],2))},1032,["onResize"])),C(()=>S&&y&&(l(),M(Ge,{mergedClsPrefix:e,type:"next",vertical:U==="left"||U==="right",disabled:L,rtl:!!W,theme:w.peers.Button,themeOverrides:w.peerOverrides.Button,onClick:x},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"])))],64)),c&&f&&A?(l(),p(H,{key:2},[C(()=>De(f,!0))],64)):C(()=>null),C(()=>Ve(J,P=>P&&(l(),p("div",{class:k(`${e}-tabs-nav__suffix`)},[C(()=>P)],2))))],2),C(()=>h&&(this.animated&&(U==="top"||U==="bottom")?(l(),p("div",{key:1,ref:"tabsPaneWrapperRef",style:oe(g),class:k([`${e}-tabs-pane-wrapper`,v])},[C(()=>Fe(X,this.mergedValue,this.renderedNames,this.onAnimationBeforeLeave,this.onAnimationEnter,this.onAnimationAfterEnter,this.animationDirection))],6)):Fe(X,this.mergedValue,this.renderedNames)))],6)}});function Fe(e,i,n,c,f,m,b){const u=[];return e.forEach(v=>{const{name:g,displayDirective:B,"display-directive":L}=v.props,y=x=>B===x||L===x,S=i===g;if(v.key!==void 0&&(v.key=g),S||y("show")||y("show:lazy")&&n.has(g)){n.has(g)||n.add(g);const x=!y("if");u.push(x?ra(v,[[na,S]]):v)}}),b?(l(),M(oa,{name:`${b}-transition`,onBeforeLeave:c,onEnter:f,onAfterEnter:m},{default:()=>u},1032,["name","onBeforeLeave","onEnter","onAfterEnter"])):u}function De(e,i){return l(),M(ze,{ref:"addTabInstRef",key:"__addable",name:"__addable",internalCreatedByPane:!0,internalAddable:!0,internalLeftPadded:i,disabled:typeof e=="object"&&e.disabled},null,8,["internalLeftPadded","disabled"])}function Ne(e){const i=aa(e);return i.props?i.props.internalLeftPadded=!0:i.props={internalLeftPadded:!0},i}function Pe(e){return Array.isArray(e.dynamicProps)?e.dynamicProps.includes("internalLeftPadded")||e.dynamicProps.push("internalLeftPadded"):e.dynamicProps=["internalLeftPadded"],e}const Wa={key:0,class:"notice-tags","aria-live":"polite"},Aa={key:0},Ia={key:3},Ea={key:4},Ma=se({__name:"NoticeAlertTags",props:{recordId:{},workType:{}},setup(e){const i=e,n=E(null),c=E("");let f=0,m=0,b=null,u=!0;function v(){m++,window.clearTimeout(f),b==null||b.abort(),b=null}async function g(){var y,S;if(v(),!u||!i.recordId||document.hidden)return;const L=m;b=new AbortController;try{const x=await ua("/api/notice-alert-tags?"+new URLSearchParams({target_record_id:i.recordId,work_type:i.workType}),{signal:b.signal,timeoutMs:8e3});if(L!==m)return;n.value=x!=null&&x.status?x:null,c.value="",((y=n.value)==null?void 0:y.status)==="pending"?f=window.setTimeout(g,5e3):((S=n.value)==null?void 0:S.status)==="failed"&&(f=window.setTimeout(g,Math.max(1e4,Math.min(6e4,Number(n.value.retry_after||0)*1e3-Date.now()))))}catch{L===m&&(c.value="推荐标签暂不可用，通告业务不受影响。")}}ie(()=>[i.recordId,i.workType],()=>{n.value=null,c.value="",g()},{immediate:!0});function B(){document.hidden?v():g()}return Ye(()=>document.addEventListener("visibilitychange",B)),ia(()=>{u=!0,g()}),la(()=>{u=!1,v()}),sa(()=>{v(),document.removeEventListener("visibilitychange",B)}),(L,y)=>{var x,w,W,_;const S=da("LoadingIndicator");return n.value||c.value?(l(),p("section",Wa,[y[1]||(y[1]=K("h4",null,"通告推荐标签（仅供现场核对，不代表已给告警打标）",-1)),c.value?(l(),p("p",Aa,Z(c.value),1)):((x=n.value)==null?void 0:x.status)==="pending"?(l(),M(S,{key:1},{default:ca(()=>[...y[0]||(y[0]=[ba("正在后台生成推荐标签…",-1)])]),_:1})):((w=n.value)==null?void 0:w.status)==="ready"?(l(),p(H,{key:2},[K("p",null,Z(n.value.title?n.value.title+" · ":"")+Z(n.value.action==="start"?"开始":"更新"),1),(l(!0),p(H,null,fa(n.value.tags,(F,J)=>(l(),p("div",{key:J},[K("strong",null,"【"+Z(F.label)+"】"+Z(F.content),1)]))),128))],64)):(l(),p("p",Ia,Z(((W=n.value)==null?void 0:W.error)||"推荐标签获取失败，通告业务不受影响。"),1)),(_=n.value)!=null&&_.message_warning?(l(),p("p",Ea,Z(n.value.message_warning),1)):je("",!0)])):je("",!0)}}}),Ga=pa(Ma,[["__scopeId","data-v-a3e342bf"]]);export{Ga as N,ze as T,ja as a};
