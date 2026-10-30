import{aF as Wt,aA as ue,b3 as At,bj as _t,T as Et,s as It,a0 as Mt,Q as Vt,aN as w,aQ as x,d as Ue,aW as Ot,C as Ht,u as r,t as y,x as s,w as W,y as jt,B as Ft,bl as Xe,ae as Se,b0 as Ie,aP as Me,V as Te,ba as Gt,bi as Dt,bm as Nt,R as X,ah as fe,z as pe}from"./c-_NZm9A8_.js";import{o as Ut}from"./c-CdFryC3x.js";import{A as Xt}from"./c-SodnSrgK.js";import{t as ge,x as Yt,$ as V,W as l,m as v,G as ve,j as ne,F,k as M,z as qt,h as ae,M as re,aj as oe,H as ie,S as Kt,ak as Jt,g as Qt,am as Zt,ai as ea,d as ta,X as aa,a8 as Y}from"./c-xWaq51p2.js";import{a as ra,C as na}from"./c-CHdiLA8w.js";import{o as oa,k as Ve,J as ia,y as la,H as Oe}from"./c-D5EDxlau.js";var sa=/\s/;function da(e){for(var o=e.length;o--&&sa.test(e.charAt(o)););return o}var ca=/^\s+/;function ba(e){return e&&e.slice(0,da(e)+1).replace(ca,"")}var He=NaN,fa=/^[-+]0x[0-9a-f]+$/i,pa=/^0b[01]+$/i,ua=/^0o[0-7]+$/i,va=parseInt;function je(e){if(typeof e=="number")return e;if(Wt(e))return He;if(ue(e)){var o=typeof e.valueOf=="function"?e.valueOf():e;e=ue(o)?o+"":o}if(typeof e!="string")return e===0?e:+e;e=ba(e);var i=pa.test(e);return i||ua.test(e)?va(e.slice(2),i?2:8):fa.test(e)?He:+e}var we=function(){return At.Date.now()},ga="Expected a function",ha=Math.max,xa=Math.min;function ma(e,o,i){var f,c,T,p,b,g,h=0,A=!1,L=!1,B=!0;if(typeof e!="function")throw new TypeError(ga);o=je(o)||0,ue(i)&&(A=!!i.leading,L="maxWait"in i,T=L?ha(je(i.maxWait)||0,o):T,B="trailing"in i?!!i.trailing:B);function R(u){var k=f,D=c;return f=c=void 0,h=u,p=e.apply(D,k),p}function $(u){return h=u,b=setTimeout(_,o),A?R(u):p}function P(u){var k=u-g,D=u-h,H=o-k;return L?xa(H,T-D):H}function O(u){var k=u-g,D=u-h;return g===void 0||k>=o||k<0||L&&D>=T}function _(){var u=we();if(O(u))return q(u);b=setTimeout(_,P(u))}function q(u){return b=void 0,B&&f?R(u):(f=c=void 0,p)}function J(){b!==void 0&&clearTimeout(b),h=0,f=g=c=b=void 0}function U(){return b===void 0?p:q(we())}function G(){var u=we(),k=O(u);if(f=arguments,c=this,g=u,k){if(b===void 0)return $(g);if(L)return clearTimeout(b),b=setTimeout(_,o),R(g)}return b===void 0&&(b=setTimeout(_,o)),p}return G.cancel=J,G.flush=U,G}var ya="Expected a function";function Ca(e,o,i){var f=!0,c=!0;if(typeof e!="function")throw new TypeError(ya);return ue(i)&&(f="leading"in i?!!i.leading:f,c="trailing"in i?!!i.trailing:c),ma(e,o,{leading:f,maxWait:o,trailing:c})}const Sa=Ve(".v-x-scroll",{overflow:"auto",scrollbarWidth:"none"},[Ve("&::-webkit-scrollbar",{width:0,height:0})]),Ta=ge({name:"XScroll",props:{disabled:Boolean,onScroll:Function},setup(){const e=V(null);function o(c){!(c.currentTarget.offsetWidth<c.currentTarget.scrollWidth)||c.deltaY===0||(c.currentTarget.scrollLeft+=c.deltaY+c.deltaX,c.preventDefault())}const i=_t();return Sa.mount({id:"vueuc/x-scroll",head:!0,anchorMetaName:oa,ssr:i}),Object.assign({selfRef:e,handleWheel:o},{scrollTo(...c){var T;(T=e.value)===null||T===void 0||T.scrollTo(...c)}})},render(){return Yt("div",{ref:"selfRef",onScroll:this.onScroll,onWheel:this.disabled?void 0:this.handleWheel,class:"v-x-scroll"},this.$slots)}});var wa={tabFontSizeSmall:"14px",tabFontSizeMedium:"14px",tabFontSizeLarge:"16px",tabGapSmallLine:"36px",tabGapMediumLine:"36px",tabGapLargeLine:"36px",tabGapSmallLineVertical:"8px",tabGapMediumLineVertical:"8px",tabGapLargeLineVertical:"8px",tabPaddingSmallLine:"6px 0",tabPaddingMediumLine:"10px 0",tabPaddingLargeLine:"14px 0",tabPaddingVerticalSmallLine:"6px 12px",tabPaddingVerticalMediumLine:"8px 16px",tabPaddingVerticalLargeLine:"10px 20px",tabGapSmallBar:"36px",tabGapMediumBar:"36px",tabGapLargeBar:"36px",tabGapSmallBarVertical:"8px",tabGapMediumBarVertical:"8px",tabGapLargeBarVertical:"8px",tabPaddingSmallBar:"4px 0",tabPaddingMediumBar:"6px 0",tabPaddingLargeBar:"10px 0",tabPaddingVerticalSmallBar:"6px 12px",tabPaddingVerticalMediumBar:"8px 16px",tabPaddingVerticalLargeBar:"10px 20px",tabGapSmallCard:"4px",tabGapMediumCard:"4px",tabGapLargeCard:"4px",tabGapSmallCardVertical:"4px",tabGapMediumCardVertical:"4px",tabGapLargeCardVertical:"4px",tabPaddingSmallCard:"8px 16px",tabPaddingMediumCard:"10px 20px",tabPaddingLargeCard:"12px 24px",tabPaddingSmallSegment:"4px 0",tabPaddingMediumSegment:"6px 0",tabPaddingLargeSegment:"8px 0",tabPaddingVerticalLargeSegment:"0 8px",tabPaddingVerticalSmallCard:"8px 12px",tabPaddingVerticalMediumCard:"10px 16px",tabPaddingVerticalLargeCard:"12px 20px",tabPaddingVerticalSmallSegment:"0 4px",tabPaddingVerticalMediumSegment:"0 6px",tabGapSmallSegment:"0",tabGapMediumSegment:"0",tabGapLargeSegment:"0",tabGapSmallSegmentVertical:"0",tabGapMediumSegmentVertical:"0",tabGapLargeSegmentVertical:"0",panePaddingSmall:"8px 0 0 0",panePaddingMedium:"12px 0 0 0",panePaddingLarge:"16px 0 0 0",closeSize:"18px",closeIconSize:"14px"};function Pa(e){const{textColor2:o,primaryColor:i,textColorDisabled:f,closeIconColor:c,closeIconColorHover:T,closeIconColorPressed:p,closeColorHover:b,closeColorPressed:g,tabColor:h,baseColor:A,dividerColor:L,fontWeight:B,textColor1:R,borderRadius:$,fontSize:P,fontWeightStrong:O}=e;return{...wa,colorSegment:h,tabFontSizeCard:P,tabTextColorLine:R,tabTextColorActiveLine:i,tabTextColorHoverLine:i,tabTextColorDisabledLine:f,tabTextColorSegment:R,tabTextColorActiveSegment:o,tabTextColorHoverSegment:o,tabTextColorDisabledSegment:f,tabTextColorBar:R,tabTextColorActiveBar:i,tabTextColorHoverBar:i,tabTextColorDisabledBar:f,tabTextColorCard:R,tabTextColorHoverCard:R,tabTextColorActiveCard:i,tabTextColorDisabledCard:f,barColor:i,closeIconColor:c,closeIconColorHover:T,closeIconColorPressed:p,closeColorHover:b,closeColorPressed:g,closeBorderRadius:$,tabColor:h,tabColorSegment:A,tabBorderColor:L,tabFontWeightActive:B,tabFontWeight:B,tabBorderRadius:$,paneTextColor:o,fontWeightStrong:O}}const Ra=Et({name:"Tabs",common:Mt,peers:{Button:It},self:Pa}),Ye=Vt("n-tabs"),za={tab:[String,Number,Object,Function],name:{type:[String,Number],required:!0},disabled:Boolean,displayDirective:{type:String,default:"if"},closable:{type:Boolean,default:void 0},tabProps:Object,label:[String,Number,Object,Function]},$a=["data-name","data-disabled"],La={internalLeftPadded:Boolean,internalAddable:Boolean,internalCreatedByPane:Boolean,...Ut(za,["displayDirective"])};var ze=ge({__TAB__:!0,inheritAttrs:!1,name:"Tab",props:La,setup(e){const{mergedClsPrefixRef:o,valueRef:i,typeRef:f,closableRef:c,tabStyleRef:T,addTabStyleRef:p,tabClassRef:b,addTabClassRef:g,tabChangeIdRef:h,onBeforeLeaveRef:A,triggerRef:L,handleAdd:B,activateTab:R,handleClose:$}=qt(Ye);return{trigger:L,mergedClosable:ae(()=>{if(e.internalAddable)return!1;const{closable:P}=e;return P===void 0?c.value:P}),style:T,addStyle:p,tabClass:b,addTabClass:g,clsPrefix:o,value:i,type:f,handleClose(P){P.stopPropagation(),!e.disabled&&$(e.name)},activateTab(){if(e.disabled)return;if(e.internalAddable){B();return}const{name:P}=e,O=++h.id;if(P!==i.value){const{value:_}=A;_?Promise.resolve(_(e.name,i.value)).then(q=>{q&&h.id===O&&R(P)}):R(P)}}}},render(){const{internalAddable:e,clsPrefix:o,name:i,disabled:f,label:c,tab:T,value:p,mergedClosable:b,trigger:g,$slots:{default:h}}=this,A=c??T;return l(),v("div",{class:w(`${o}-tabs-tab-wrapper`)},[this.internalLeftPadded?(l(),v("div",{key:0,class:w(`${o}-tabs-tab-pad`)},null,2)):x(()=>null),(l(),v("div",ve({key:i,"data-name":i,"data-disabled":f?!0:void 0},ve({class:[`${o}-tabs-tab`,p===i&&`${o}-tabs-tab--active`,f&&`${o}-tabs-tab--disabled`,b&&`${o}-tabs-tab--closable`,e&&`${o}-tabs-tab--addable`,e?this.addTabClass:this.tabClass],onClick:g==="click"?this.activateTab:void 0,onMouseenter:g==="hover"?this.activateTab:void 0,style:e?this.addStyle:this.style},this.internalCreatedByPane?this.tabProps||{}:this.$attrs)),[ne("span",{class:w(`${o}-tabs-tab__label`)},[e?(l(),v(F,{key:0},[ne("div",{class:w(`${o}-tabs-tab__height-placeholder`)}," ",2),(l(),M(Ue,{clsPrefix:o},{default:()=>(l(),M(Xt))},1032,["clsPrefix"]))],64)):(l(),v(F,{key:1},[h?(l(),v(F,{key:0},[x(()=>h())],64)):(l(),v(F,{key:1},[typeof A=="object"?(l(),v(F,{key:0},[x(()=>A)],64)):(l(),v(F,{key:1},[x(()=>Ot(A??i))],64))],64))],64))],2),b&&this.type==="card"?(l(),M(Ht,{key:0,clsPrefix:o,class:w(`${o}-tabs-tab__close`),onClick:this.handleClose,disabled:f},null,8,["clsPrefix","class","onClick","disabled"])):x(()=>null)],16,$a))],2)}}),Ba=r("tabs",`
 box-sizing: border-box;
 width: 100%;
 display: flex;
 flex-direction: column;
 transition:
 background-color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
`,[y("&.transition-disabled",[r("tabs-tab",`
 transition: none !important;
 `),r("tabs-nav-scroll-content",`
 transition: none !important;
 `),r("tabs-tab-pad",`
 transition: none !important;
 `)]),s("segment-type",[r("tabs-rail",[y("&.transition-disabled",[r("tabs-capsule",`
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
 `),y("&:hover",`
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
 `,[W("prefix, suffix",`
 display: flex;
 align-items: center;
 `),W("prefix","padding-right: 16px;"),W("suffix","padding-left: 16px;")]),s("top, bottom",[y(">",[r("tabs-nav",[r("tabs-nav-scroll-wrapper",[y("&::before",`
 top: 0;
 bottom: 0;
 left: 0;
 width: 20px;
 `),y("&::after",`
 top: 0;
 bottom: 0;
 right: 0;
 width: 20px;
 `),s("shadow-start",[y("&::before",`
 box-shadow: inset 10px 0 8px -8px rgba(0, 0, 0, .12);
 `)]),s("shadow-end",[y("&::after",`
 box-shadow: inset -10px 0 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),s("left, right",[r("tabs-nav-scroll-content",`
 flex-direction: column;
 `),y(">",[r("tabs-nav",[r("tabs-nav-scroll-wrapper",[y("&::before",`
 top: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),y("&::after",`
 bottom: 0;
 left: 0;
 right: 0;
 height: 20px;
 `),s("shadow-start",[y("&::before",`
 box-shadow: inset 0 10px 8px -8px rgba(0, 0, 0, .12);
 `)]),s("shadow-end",[y("&::after",`
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
 `,[y("&::-webkit-scrollbar, &::-webkit-scrollbar-track-piece, &::-webkit-scrollbar-thumb",`
 width: 0;
 height: 0;
 display: none;
 `)]),y("&::before, &::after",`
 transition: box-shadow .3s var(--n-bezier);
 pointer-events: none;
 content: "";
 position: absolute;
 z-index: 1;
 `),y("&.transition-disabled",[y("&::before, &::after",`
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
 `,[s("disabled",{cursor:"not-allowed"}),W("close",`
 margin-inline-start: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `),W("label",`
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
 `,[y("&.transition-disabled",`
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
 `,[y("&.next-transition-leave-active, &.prev-transition-leave-active, &.next-transition-enter-active, &.prev-transition-enter-active",`
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 transform .2s var(--n-bezier),
 opacity .2s var(--n-bezier);
 `),y("&.next-transition-leave-active, &.prev-transition-leave-active",`
 position: absolute;
 `),y("&.next-transition-enter-from, &.prev-transition-leave-to",`
 transform: translateX(32px);
 opacity: 0;
 `),y("&.next-transition-leave-to, &.prev-transition-enter-from",`
 transform: translateX(-32px);
 opacity: 0;
 `),y("&.next-transition-leave-from, &.next-transition-enter-to, &.prev-transition-leave-from, &.prev-transition-enter-to",`
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
 `,[y("&:hover",{color:"var(--n-tab-text-color-hover)"}),s("active",`
 color: var(--n-tab-text-color-active);
 font-weight: var(--n-tab-font-weight-active);
 `),s("disabled",{color:"var(--n-tab-text-color-disabled)"})])]),r("tabs-nav",[W("prefix, suffix",`
 border-color: var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-color: var(--n-tab-border-color);
 `),s("line-type",[s("top",[W("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 bottom: -1px;
 `)]),s("left",[W("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 right: -1px;
 `)]),s("right",[W("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 left: -1px;
 `)]),s("bottom",[W("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 top: -1px;
 `)]),W("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-nav-scroll-content",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-bar",`
 border-radius: 0;
 `)]),s("card-type",[W("prefix, suffix",`
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
 `,[W("height-placeholder",`
 width: 0;
 font-size: var(--n-tab-font-size);
 `),jt("disabled",[y("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])]),s("closable","padding-inline-end: 8px;"),s("active",`
 background-color: #0000;
 font-weight: var(--n-tab-font-weight-active);
 color: var(--n-tab-text-color-active);
 `),s("disabled","color: var(--n-tab-text-color-disabled);")])]),s("left, right",`
 flex-direction: column; 
 `,[W("prefix, suffix",`
 padding: var(--n-tab-padding-vertical);
 `),r("tabs-wrapper",`
 flex-direction: column;
 `),r("tabs-tab-wrapper",`
 flex-direction: column;
 `,[r("tabs-tab-pad",`
 height: var(--n-tab-gap-vertical);
 width: 100%;
 `)])]),s("top",[s("card-type",[r("tabs-scroll-padding","border-bottom: 1px solid var(--n-tab-border-color);"),W("prefix, suffix",`
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
 `)])]),s("left",[s("card-type",[r("tabs-scroll-padding","border-right: 1px solid var(--n-tab-border-color);"),W("prefix, suffix",`
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
 `)])]),s("right",[s("card-type",[r("tabs-scroll-padding","border-left: 1px solid var(--n-tab-border-color);"),W("prefix, suffix",`
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
 `)])]),s("bottom",[s("card-type",[r("tabs-scroll-padding","border-top: 1px solid var(--n-tab-border-color);"),W("prefix, suffix",`
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
 `)])]),Fe=ge({name:"TabsButton",props:{type:{type:String,default:"next"},mergedClsPrefix:{type:String,required:!0},vertical:Boolean,disabled:Boolean,rtl:Boolean,theme:Object,themeOverrides:Object,onClick:Function},setup(e){return{handleClick:()=>{var i;e.disabled||(i=e.onClick)==null||i.call(e,e.type)}}},render(){const{mergedClsPrefix:e,disabled:o,type:i,vertical:f,rtl:c,theme:T,themeOverrides:p,handleClick:b}=this,g=i==="next",h=f?g:c?!g:g;return l(),M(Ft,{text:!0,disabled:o,size:"small",theme:T,themeOverrides:p,onClick:b,class:w([`${e}-tabs-scroll-button`,!f&&i==="prev"&&`${e}-tabs-scroll-button--start`,!f&&i==="next"&&`${e}-tabs-scroll-button--end`,f&&i==="prev"&&`${e}-tabs-scroll-button--up`,f&&i==="next"&&`${e}-tabs-scroll-button--down`])},{icon:()=>(l(),M(Ue,{clsPrefix:e,style:re(f?{transform:"rotate(90deg)"}:void 0)},{default:()=>h?(l(),M(ra,{key:1})):(l(),M(na,{key:2}))},1032,["clsPrefix","style"]))},1032,["disabled","theme","themeOverrides","onClick","class"])}});const Pe=Ca,ka={...Xe.props,value:[String,Number],defaultValue:[String,Number],trigger:{type:String,default:"click"},type:{type:String,default:"bar"},closable:Boolean,justifyContent:String,size:String,placement:{type:String,default:"top"},tabStyle:[String,Object],tabClass:String,addTabStyle:[String,Object],addTabClass:String,barWidth:Number,paneClass:String,paneStyle:[String,Object],paneWrapperClass:String,paneWrapperStyle:[String,Object],addable:[Boolean,Object],tabsPadding:{type:Number,default:0},animated:Boolean,onBeforeLeave:Function,onAdd:Function,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onClose:[Function,Array],labelSize:String,activeName:[String,Number],onActiveNameChange:[Function,Array],showScrollButton:Boolean,centerActiveTab:Boolean};var Va=ge({name:"Tabs",props:ka,slots:Object,setup(e,{slots:o}){var _e,Ee;const{mergedClsPrefixRef:i,inlineThemeDisabled:f,mergedComponentPropsRef:c,mergedRtlRef:T}=Gt(e),p=Dt("Tabs",T,i),b=ae(()=>{const{placement:t}=e;return t==="start"?p!=null&&p.value?"right":"left":t==="end"?p!=null&&p.value?"left":"right":t}),g=Xe("Tabs","-tabs",Ba,Ra,e,i),h=V(null),A=V(null),L=V(null),B=V(null),R=V(null),$=V(null),P=V(null),O=V(!0),_=V(!0),q=Oe(e,["labelSize","size"]),J=ae(()=>{var a,n;if(q.value)return q.value;const t=(n=(a=c==null?void 0:c.value)==null?void 0:a.Tabs)==null?void 0:n.size;return t||"medium"}),U=Oe(e,["activeName","value"]),G=V(U.value??e.defaultValue??(o.default?(Ee=(_e=Se(o.default())[0])==null?void 0:_e.props)==null?void 0:Ee.name:null)),u=ia(U,G),k={id:0},D=ae(()=>{if(!(!e.justifyContent||e.type==="card"))return{display:"flex",justifyContent:e.justifyContent}});oe(u,()=>{k.id=0,E(),ie(()=>{he()})});function H(){var a;const{value:t}=u;return t===null?null:(a=h.value)==null?void 0:a.querySelector(`[data-name="${t}"]`)}function le(t){if(e.type==="card")return;const{value:a}=L;if(!a)return;const n=a.style.opacity==="0";if(t){const d=`${i.value}-tabs-bar--disabled`,{barWidth:m}=e,z=b.value;if(t.dataset.disabled==="true"?a.classList.add(d):a.classList.remove(d),["top","bottom"].includes(z)){if(C(["top","maxHeight","height"]),typeof m=="number"&&t.offsetWidth>=m){const S=Math.floor((t.offsetWidth-m)/2)+t.offsetLeft;a.style.left=`${S}px`,a.style.maxWidth=`${m}px`}else a.style.left=`${t.offsetLeft}px`,a.style.maxWidth=`${t.offsetWidth}px`;a.style.width="8192px",n&&(a.style.transition="none"),a.offsetWidth,n&&(a.style.transition="",a.style.opacity="1")}else{if(C(["left","maxWidth","width"]),typeof m=="number"&&t.offsetHeight>=m){const S=Math.floor((t.offsetHeight-m)/2)+t.offsetTop;a.style.top=`${S}px`,a.style.maxHeight=`${m}px`}else a.style.top=`${t.offsetTop}px`,a.style.maxHeight=`${t.offsetHeight}px`;a.style.height="8192px",n&&(a.style.transition="none"),a.offsetHeight,n&&(a.style.transition="",a.style.opacity="1")}}}function N(){if(e.type==="card")return;const{value:t}=L;t&&(t.style.opacity="0")}function C(t){const{value:a}=L;if(a)for(const n of t)a.style[n]=""}function E(){if(e.type==="card")return;const t=H();t?le(t):N()}function Q(t,a,n,d){const m=t.getBoundingClientRect(),z=a.getBoundingClientRect(),S=n?"left":"top",I=n?"right":"bottom";let j=0;d?j=(z[S]+z[I])/2-(m[S]+m[I])/2:z[S]<m[S]?j=z[S]-m[S]:z[I]>m[I]&&(j=z[I]-m[I]),j!==0&&t.scrollBy({[S]:j,behavior:"smooth"})}function he(){var n;const t=["top","bottom"].includes(b.value),a=H();if(a)if(t){const d=(n=$.value)==null?void 0:n.$el;if(!d)return;Q(d,a,t,e.centerActiveTab)}else{const{value:d}=P;if(!d)return;Q(d,a,t,e.centerActiveTab)}}const se=V(null);let xe=0,K=null;function qe(t){const a=se.value;if(a){xe=t.getBoundingClientRect().height;const n=`${xe}px`,d=()=>{a.style.height=n,a.style.maxHeight=n};K?(d(),K(),K=null):K=d}}function Ke(t){const a=se.value;if(a){const n=t.getBoundingClientRect().height,d=()=>{document.body.offsetHeight,a.style.maxHeight=`${n}px`,a.style.height=`${Math.max(xe,n)}px`};K?(K(),K=null,d()):K=d}}function Je(){const t=se.value;if(t){t.style.maxHeight="",t.style.height="";const{paneWrapperStyle:a}=e;if(typeof a=="string")t.style.cssText=a;else if(a){const{maxHeight:n,height:d}=a;n!==void 0&&(t.style.maxHeight=n),d!==void 0&&(t.style.height=d)}}}const $e={value:[]},Le=V("next");function Qe(t){const a=u.value;let n="next";for(const d of $e.value){if(d===a)break;if(d===t){n="prev";break}}Le.value=n,Ze(t)}function Ze(t){const{onActiveNameChange:a,onUpdateValue:n,"onUpdate:value":d}=e;a&&pe(a,t),n&&pe(n,t),d&&pe(d,t),G.value=t}function et(t){const{onClose:a}=e;a&&pe(a,t)}function tt(t){if(["top","bottom"].includes(b.value)){const{value:a}=$;if(!a)return;const n=a.$el;if(!n)return;const d=n.offsetWidth,m=!!(p!=null&&p.value),z=t==="next"?d:-d;n.scrollBy({left:m?-z:z,behavior:"smooth"})}else{const{value:a}=P;if(!a)return;const n=a.offsetHeight,d=t==="next"?a.scrollTop+n:a.scrollTop-n;a.scrollTo({top:d,left:0,behavior:"smooth"})}}let me=!0;function ye(){const{value:t}=L;if(!t)return;me&&(me=!1);const a="transition-disabled";t.classList.add(a),E(),t.classList.remove(a)}const Z=V(null);function de({transitionDisabled:t}){const a=h.value;if(!a)return;t&&a.classList.add("transition-disabled");const n=H();n&&Z.value&&(Z.value.style.width=`${n.offsetWidth}px`,Z.value.style.height=`${n.offsetHeight}px`,Z.value.style.transform=`translate(${n.offsetLeft}px, ${n.offsetTop}px)`,t&&Z.value.offsetWidth),t&&a.classList.remove("transition-disabled")}oe([u],()=>{e.type==="segment"&&ie(()=>{de({transitionDisabled:!1})})}),Kt(()=>{e.type==="segment"&&de({transitionDisabled:!0})});let Be=0;function at(t){var n;if(t.contentRect.width===0&&t.contentRect.height===0||Be===t.contentRect.width)return;Be=t.contentRect.width;const{type:a}=e;(a==="line"||a==="bar")&&(me||(n=e.justifyContent)!=null&&n.startsWith("space"))&&ye(),a!=="segment"&&ce(We())}const rt=Pe(at,64);function ke(){const{type:t}=e;t==="line"||t==="bar"?ye():t==="segment"&&de({transitionDisabled:!0})}oe([()=>e.justifyContent,()=>e.size],()=>{ie(()=>{(e.type==="line"||e.type==="bar")&&ye()})}),oe([b,()=>p==null?void 0:p.value],()=>{ie(()=>{ke(),ce(We(),{instantly:!0})})}),oe(()=>e.type,()=>{ie(()=>{const t=A.value;t&&(t.classList.add("transition-disabled"),ke(),t.offsetWidth,t.classList.remove("transition-disabled"))})});const ee=V(!1);function nt(t){var I;const{target:a,contentRect:{width:n,height:d}}=t,m=a.parentElement.parentElement.offsetWidth,z=a.parentElement.parentElement.offsetHeight,S=b.value;if(!ee.value)S==="top"||S==="bottom"?m<n&&(ee.value=!0):z<d&&(ee.value=!0);else{const{value:j}=R;if(!j)return;S==="top"||S==="bottom"?m-n>j.$el.offsetWidth&&(ee.value=!1):z-d>j.$el.offsetHeight&&(ee.value=!1)}ce(((I=$.value)==null?void 0:I.$el)||null)}const ot=Pe(nt,64);function it(){const{onAdd:t}=e;t&&t()}const Ce=V(!1);function We(){var a;const t=b.value;return(t==="top"||t==="bottom"?(a=$.value)==null?void 0:a.$el:P.value)||null}function ce(t,a={instantly:!1}){if(!t)return;const n=a.instantly?B.value:null;n&&n.classList.add("transition-disabled");const d=1,m=b.value;if(m==="top"||m==="bottom"){const{scrollLeft:z,scrollWidth:S,offsetWidth:I}=t,j=Math.abs(z);O.value=j<=d,_.value=j+I>=S-d,Ce.value=I<S-d}else{const{scrollTop:z,scrollHeight:S,offsetHeight:I}=t;O.value=z<=d,_.value=z+I>=S-d,Ce.value=I<S-d}n&&(n.offsetWidth,n.classList.remove("transition-disabled"))}const lt=Pe(t=>{ce(t.target)},64);aa(Ye,{triggerRef:Y(e,"trigger"),tabStyleRef:Y(e,"tabStyle"),tabClassRef:Y(e,"tabClass"),addTabStyleRef:Y(e,"addTabStyle"),addTabClassRef:Y(e,"addTabClass"),paneClassRef:Y(e,"paneClass"),paneStyleRef:Y(e,"paneStyle"),mergedClsPrefixRef:i,typeRef:Y(e,"type"),closableRef:Y(e,"closable"),valueRef:u,tabChangeIdRef:k,onBeforeLeaveRef:Y(e,"onBeforeLeave"),activateTab:Qe,handleClose:et,handleAdd:it}),la(()=>{E(),he()}),Jt(()=>{const{value:t}=B;if(!t)return;const{value:a}=i,n=`${a}-tabs-nav-scroll-wrapper--shadow-start`,d=`${a}-tabs-nav-scroll-wrapper--shadow-end`;O.value?t.classList.remove(n):t.classList.add(n),_.value?t.classList.remove(d):t.classList.add(d)});const st={syncBarPosition:()=>{E()},scrollToCurrentTab:()=>{he()}},dt=()=>{de({transitionDisabled:!0})},Ae=ae(()=>{const{value:t}=J,{type:a}=e,n=`${t}${{card:"Card",bar:"Bar",line:"Line",segment:"Segment"}[a]}`,{self:{barColor:d,closeIconColor:m,closeIconColorHover:z,closeIconColorPressed:S,tabColor:I,tabBorderColor:j,paneTextColor:ct,tabFontWeight:bt,tabBorderRadius:ft,tabFontWeightActive:pt,colorSegment:ut,fontWeightStrong:vt,tabColorSegment:gt,closeSize:ht,closeIconSize:xt,closeColorHover:mt,closeColorPressed:yt,closeBorderRadius:Ct,[X("panePadding",t)]:be,[X("tabPadding",n)]:St,[X("tabPaddingVertical",n)]:Tt,[X("tabGap",n)]:wt,[X("tabGap",`${n}Vertical`)]:Pt,[X("tabTextColor",a)]:Rt,[X("tabTextColorActive",a)]:zt,[X("tabTextColorHover",a)]:$t,[X("tabTextColorDisabled",a)]:Lt,[X("tabFontSize",t)]:Bt},common:{cubicBezierEaseInOut:kt}}=g.value;return{"--n-bezier":kt,"--n-color-segment":ut,"--n-bar-color":d,"--n-tab-font-size":Bt,"--n-tab-text-color":Rt,"--n-tab-text-color-active":zt,"--n-tab-text-color-disabled":Lt,"--n-tab-text-color-hover":$t,"--n-pane-text-color":ct,"--n-tab-border-color":j,"--n-tab-border-radius":ft,"--n-close-size":ht,"--n-close-icon-size":xt,"--n-close-color-hover":mt,"--n-close-color-pressed":yt,"--n-close-border-radius":Ct,"--n-close-icon-color":m,"--n-close-icon-color-hover":z,"--n-close-icon-color-pressed":S,"--n-tab-color":I,"--n-tab-font-weight":bt,"--n-tab-font-weight-active":pt,"--n-tab-padding":St,"--n-tab-padding-vertical":Tt,"--n-tab-gap":wt,"--n-tab-gap-vertical":Pt,"--n-pane-padding-left":fe(be,"left"),"--n-pane-padding-right":fe(be,"right"),"--n-pane-padding-top":fe(be,"top"),"--n-pane-padding-bottom":fe(be,"bottom"),"--n-font-weight-strong":vt,"--n-tab-color-segment":gt}}),te=f?Nt("tabs",ae(()=>`${J.value[0]}${e.type[0]}`),Ae,e):void 0;return{mergedClsPrefix:i,mergedValue:u,renderedNames:new Set,segmentCapsuleElRef:Z,tabsPaneWrapperRef:se,tabsElRef:h,selfElRef:A,barElRef:L,addTabInstRef:R,xScrollInstRef:$,scrollWrapperElRef:B,addTabFixed:ee,tabWrapperStyle:D,handleNavResize:rt,mergedSize:J,handleScroll:lt,handleTabsResize:ot,cssVars:f?void 0:Ae,themeClass:te==null?void 0:te.themeClass,animationDirection:Le,renderNameListRef:$e,yScrollElRef:P,handleSegmentResize:dt,onAnimationBeforeLeave:qe,onAnimationEnter:Ke,onAnimationAfterEnter:Je,onRender:te==null?void 0:te.onRender,startReachedRef:O,endReachedRef:_,isOverflow:Ce,handleButtonClick:tt,mergedTheme:g,rtlEnabled:p,mergedPlacement:b,...st}},render(){const{mergedClsPrefix:e,type:o,mergedPlacement:i,addTabFixed:f,addable:c,mergedSize:T,renderNameListRef:p,onRender:b,paneWrapperClass:g,paneWrapperStyle:h,startReachedRef:A,endReachedRef:L,isOverflow:B,showScrollButton:R,handleButtonClick:$,mergedTheme:P,rtlEnabled:O,$slots:{default:_,prefix:q,suffix:J}}=this;b==null||b();const U=_?Se(_()).filter(C=>C.type.__TAB_PANE__===!0):[],G=_?Se(_()).filter(C=>C.type.__TAB__===!0):[],u=!G.length,k=o==="card",D=o==="segment",H=!k&&!D&&this.justifyContent;p.value=[];const le=()=>{const C=(l(),v("div",{style:re(this.tabWrapperStyle),class:w(`${e}-tabs-wrapper`)},[H?x(()=>null):(l(),v("div",{key:1,class:w(`${e}-tabs-scroll-padding`),style:re(i==="top"||i==="bottom"?{width:`${this.tabsPadding}px`}:{height:`${this.tabsPadding}px`})},null,6)),u?(l(),v(F,{key:2},[x(()=>U.map((E,Q)=>(p.value.push(E.props.name),Re((l(),M(ze,ve(E.props,{internalCreatedByPane:!0,internalLeftPadded:Q!==0&&(!H||H==="center"||H==="start"||H==="end")}),Me(E.children?{default:E.children.tab}:void 0),1040,["internalLeftPadded"]))))))],64)):(l(),v(F,{key:3},[x(()=>G.map((E,Q)=>(p.value.push(E.props.name),Re(Q!==0&&!H?Ne(E):E))))],64)),!f&&c&&k?(l(),v(F,{key:4},[x(()=>De(c,(u?U.length:G.length)!==0))],64)):x(()=>null),H?x(()=>null):(l(),v("div",{key:7,class:w(`${e}-tabs-scroll-padding`),style:re({width:`${this.tabsPadding}px`})},null,6)),k?x(()=>null):(l(),v("div",{key:9,ref:"barElRef",class:w(`${e}-tabs-bar`)},null,2))],6));return l(),v("div",{ref:"tabsElRef",class:w(`${e}-tabs-nav-scroll-content`)},[k&&c?(l(),M(Te,{key:0,onResize:this.handleTabsResize},{default:()=>C},1032,["onResize"])):(l(),v(F,{key:1},[x(()=>C)],64)),k?(l(),v("div",{key:2,class:w(`${e}-tabs-pad`)},null,2)):x(()=>null)],2)},N=D?"top":i;return l(),v("div",{ref:"selfElRef",class:w([`${e}-tabs`,this.themeClass,`${e}-tabs--${o}-type`,`${e}-tabs--${T}-size`,H&&`${e}-tabs--flex`,`${e}-tabs--${N}`,O&&`${e}-tabs--rtl`]),style:re(this.cssVars)},[ne("div",{class:w([`${e}-tabs-nav--${o}-type`,`${e}-tabs-nav--${N}`,`${e}-tabs-nav`])},[x(()=>Ie(q,C=>C&&(l(),v("div",{class:w(`${e}-tabs-nav__prefix`)},[x(()=>C)],2)))),D?(l(),M(Te,{key:0,onResize:this.handleSegmentResize},{default:()=>(l(),v("div",{class:w(`${e}-tabs-rail`),ref:"tabsElRef"},[ne("div",{class:w(`${e}-tabs-capsule`),ref:"segmentCapsuleElRef"},[ne("div",{class:w(`${e}-tabs-wrapper`)},[ne("div",{class:w(`${e}-tabs-tab`)},null,2)],2)],2),u?(l(),v(F,{key:0},[x(()=>U.map((C,E)=>(p.value.push(C.props.name),l(),M(ze,ve(C.props,{internalCreatedByPane:!0,internalLeftPadded:E!==0}),Me(C.children?{default:C.children.tab}:void 0),1040,["internalLeftPadded"]))))],64)):(l(),v(F,{key:1},[x(()=>G.map((C,E)=>(p.value.push(C.props.name),E===0?C:Ne(C))))],64))],2))},1032,["onResize"])):(l(),v(F,{key:1},[x(()=>R&&B&&(l(),M(Fe,{mergedClsPrefix:e,type:"prev",vertical:N==="left"||N==="right",disabled:A,rtl:!!O,theme:P.peers.Button,themeOverrides:P.peerOverrides.Button,onClick:$},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"]))),(l(),M(Te,{onResize:this.handleNavResize},{default:()=>(l(),v("div",{class:w(`${e}-tabs-nav-scroll-wrapper`),ref:"scrollWrapperElRef"},[["top","bottom"].includes(N)?(l(),M(Ta,{key:0,ref:"xScrollInstRef",onScroll:this.handleScroll},{default:le},1032,["onScroll"])):(l(),v("div",{key:1,class:w(`${e}-tabs-nav-y-scroll`),onScroll:this.handleScroll,ref:"yScrollElRef"},[x(()=>le())],42,["onScroll"]))],2))},1032,["onResize"])),x(()=>R&&B&&(l(),M(Fe,{mergedClsPrefix:e,type:"next",vertical:N==="left"||N==="right",disabled:L,rtl:!!O,theme:P.peers.Button,themeOverrides:P.peerOverrides.Button,onClick:$},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"])))],64)),f&&c&&k?(l(),v(F,{key:2},[x(()=>De(c,!0))],64)):x(()=>null),x(()=>Ie(J,C=>C&&(l(),v("div",{class:w(`${e}-tabs-nav__suffix`)},[x(()=>C)],2))))],2),x(()=>u&&(this.animated&&(N==="top"||N==="bottom")?(l(),v("div",{key:1,ref:"tabsPaneWrapperRef",style:re(h),class:w([`${e}-tabs-pane-wrapper`,g])},[x(()=>Ge(U,this.mergedValue,this.renderedNames,this.onAnimationBeforeLeave,this.onAnimationEnter,this.onAnimationAfterEnter,this.animationDirection))],6)):Ge(U,this.mergedValue,this.renderedNames)))],6)}});function Ge(e,o,i,f,c,T,p){const b=[];return e.forEach(g=>{const{name:h,displayDirective:A,"display-directive":L}=g.props,B=$=>A===$||L===$,R=o===h;if(g.key!==void 0&&(g.key=h),R||B("show")||B("show:lazy")&&i.has(h)){i.has(h)||i.add(h);const $=!B("if");b.push($?Zt(g,[[ea,R]]):g)}}),p?(l(),M(ta,{name:`${p}-transition`,onBeforeLeave:f,onEnter:c,onAfterEnter:T},{default:()=>b},1032,["name","onBeforeLeave","onEnter","onAfterEnter"])):b}function De(e,o){return l(),M(ze,{ref:"addTabInstRef",key:"__addable",name:"__addable",internalCreatedByPane:!0,internalAddable:!0,internalLeftPadded:o,disabled:typeof e=="object"&&e.disabled},null,8,["internalLeftPadded","disabled"])}function Ne(e){const o=Qt(e);return o.props?o.props.internalLeftPadded=!0:o.props={internalLeftPadded:!0},o}function Re(e){return Array.isArray(e.dynamicProps)?e.dynamicProps.includes("internalLeftPadded")||e.dynamicProps.push("internalLeftPadded"):e.dynamicProps=["internalLeftPadded"],e}export{ze as T,Va as a};
