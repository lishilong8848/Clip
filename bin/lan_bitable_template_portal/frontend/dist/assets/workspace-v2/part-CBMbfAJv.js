import{db as Bt,bO as De,b7 as ve,bz as Wt,dP as _t,b1 as At,cS as I,ar as Ee,aY as Et,ap as Mt,b9 as Vt,aR as It,cD as l,aP as v,ck as w,cq as x,b_ as ue,aM as ne,t as G,aN as V,u as Ne,cW as Ot,o as Ht,bD as jt,aI as ae,ct as Gt,as as r,aq as y,au as s,at as W,av as Ft,cp as re,k as Dt,g as Nt,dR as Ue,bo as Ce,d8 as Me,co as Ve,aa as Se,dz as Ut,dN as Xt,dM as Yt,dZ as oe,ci as ie,cA as qt,cM as Kt,cz as Jt,d_ as Zt,dS as Qt,dy as Ie,aS as X,bw as fe,aG as ea,e1 as ta,dX as aa,a6 as ra,dm as Y,aw as pe}from"./app-DXg8PPQD.js";import{Y as Oe,A as na}from"./part-DowNqKys.js";import{C as oa}from"./part-Cefiinsm.js";var Te=function(){return Bt.Date.now()},ia="Expected a function",la=Math.max,sa=Math.min;function da(e,i,o){var f,c,T,p,b,g,h=0,_=!1,$=!1,k=!0;if(typeof e!="function")throw new TypeError(ia);i=Oe(i)||0,De(o)&&(_=!!o.leading,$="maxWait"in o,T=$?la(Oe(o.maxWait)||0,i):T,k="trailing"in o?!!o.trailing:k);function P(u){var B=f,D=c;return f=c=void 0,h=u,p=e.apply(D,B),p}function L(u){return h=u,b=setTimeout(A,i),_?P(u):p}function R(u){var B=u-g,D=u-h,H=i-B;return $?sa(H,T-D):H}function O(u){var B=u-g,D=u-h;return g===void 0||B>=i||B<0||$&&D>=T}function A(){var u=Te();if(O(u))return q(u);b=setTimeout(A,R(u))}function q(u){return b=void 0,k&&f?P(u):(f=c=void 0,p)}function J(){b!==void 0&&clearTimeout(b),h=0,f=g=c=b=void 0}function U(){return b===void 0?p:q(Te())}function F(){var u=Te(),B=O(u);if(f=arguments,c=this,g=u,B){if(b===void 0)return L(g);if($)return clearTimeout(b),b=setTimeout(A,i),P(g)}return b===void 0&&(b=setTimeout(A,i)),p}return F.cancel=J,F.flush=U,F}var ca="Expected a function";function ba(e,i,o){var f=!0,c=!0;if(typeof e!="function")throw new TypeError(ca);return De(o)&&(f="leading"in o?!!o.leading:f,c="trailing"in o?!!o.trailing:c),da(e,i,{leading:f,maxWait:i,trailing:c})}const fa=Ee(".v-x-scroll",{overflow:"auto",scrollbarWidth:"none"},[Ee("&::-webkit-scrollbar",{width:0,height:0})]),pa=ve({name:"XScroll",props:{disabled:Boolean,onScroll:Function},setup(){const e=I(null);function i(c){!(c.currentTarget.offsetWidth<c.currentTarget.scrollWidth)||c.deltaY===0||(c.currentTarget.scrollLeft+=c.deltaY+c.deltaX,c.preventDefault())}const o=_t();return fa.mount({id:"vueuc/x-scroll",head:!0,anchorMetaName:At,ssr:o}),Object.assign({selfRef:e,handleWheel:i},{scrollTo(...c){var T;(T=e.value)===null||T===void 0||T.scrollTo(...c)}})},render(){return Wt("div",{ref:"selfRef",onScroll:this.onScroll,onWheel:this.disabled?void 0:this.handleWheel,class:"v-x-scroll"},this.$slots)}});var ua={tabFontSizeSmall:"14px",tabFontSizeMedium:"14px",tabFontSizeLarge:"16px",tabGapSmallLine:"36px",tabGapMediumLine:"36px",tabGapLargeLine:"36px",tabGapSmallLineVertical:"8px",tabGapMediumLineVertical:"8px",tabGapLargeLineVertical:"8px",tabPaddingSmallLine:"6px 0",tabPaddingMediumLine:"10px 0",tabPaddingLargeLine:"14px 0",tabPaddingVerticalSmallLine:"6px 12px",tabPaddingVerticalMediumLine:"8px 16px",tabPaddingVerticalLargeLine:"10px 20px",tabGapSmallBar:"36px",tabGapMediumBar:"36px",tabGapLargeBar:"36px",tabGapSmallBarVertical:"8px",tabGapMediumBarVertical:"8px",tabGapLargeBarVertical:"8px",tabPaddingSmallBar:"4px 0",tabPaddingMediumBar:"6px 0",tabPaddingLargeBar:"10px 0",tabPaddingVerticalSmallBar:"6px 12px",tabPaddingVerticalMediumBar:"8px 16px",tabPaddingVerticalLargeBar:"10px 20px",tabGapSmallCard:"4px",tabGapMediumCard:"4px",tabGapLargeCard:"4px",tabGapSmallCardVertical:"4px",tabGapMediumCardVertical:"4px",tabGapLargeCardVertical:"4px",tabPaddingSmallCard:"8px 16px",tabPaddingMediumCard:"10px 20px",tabPaddingLargeCard:"12px 24px",tabPaddingSmallSegment:"4px 0",tabPaddingMediumSegment:"6px 0",tabPaddingLargeSegment:"8px 0",tabPaddingVerticalLargeSegment:"0 8px",tabPaddingVerticalSmallCard:"8px 12px",tabPaddingVerticalMediumCard:"10px 16px",tabPaddingVerticalLargeCard:"12px 20px",tabPaddingVerticalSmallSegment:"0 4px",tabPaddingVerticalMediumSegment:"0 6px",tabGapSmallSegment:"0",tabGapMediumSegment:"0",tabGapLargeSegment:"0",tabGapSmallSegmentVertical:"0",tabGapMediumSegmentVertical:"0",tabGapLargeSegmentVertical:"0",panePaddingSmall:"8px 0 0 0",panePaddingMedium:"12px 0 0 0",panePaddingLarge:"16px 0 0 0",closeSize:"18px",closeIconSize:"14px"};function va(e){const{textColor2:i,primaryColor:o,textColorDisabled:f,closeIconColor:c,closeIconColorHover:T,closeIconColorPressed:p,closeColorHover:b,closeColorPressed:g,tabColor:h,baseColor:_,dividerColor:$,fontWeight:k,textColor1:P,borderRadius:L,fontSize:R,fontWeightStrong:O}=e;return{...ua,colorSegment:h,tabFontSizeCard:R,tabTextColorLine:P,tabTextColorActiveLine:o,tabTextColorHoverLine:o,tabTextColorDisabledLine:f,tabTextColorSegment:P,tabTextColorActiveSegment:i,tabTextColorHoverSegment:i,tabTextColorDisabledSegment:f,tabTextColorBar:P,tabTextColorActiveBar:o,tabTextColorHoverBar:o,tabTextColorDisabledBar:f,tabTextColorCard:P,tabTextColorHoverCard:P,tabTextColorActiveCard:o,tabTextColorDisabledCard:f,barColor:o,closeIconColor:c,closeIconColorHover:T,closeIconColorPressed:p,closeColorHover:b,closeColorPressed:g,closeBorderRadius:L,tabColor:h,tabColorSegment:_,tabBorderColor:$,tabFontWeightActive:k,tabFontWeight:k,tabBorderRadius:L,paneTextColor:i,fontWeightStrong:O}}const ga=Et({name:"Tabs",common:Vt,peers:{Button:Mt},self:va}),Xe=It("n-tabs"),ha={tab:[String,Number,Object,Function],name:{type:[String,Number],required:!0},disabled:Boolean,displayDirective:{type:String,default:"if"},closable:{type:Boolean,default:void 0},tabProps:Object,label:[String,Number,Object,Function]},xa=["data-name","data-disabled"],ma={internalLeftPadded:Boolean,internalAddable:Boolean,internalCreatedByPane:Boolean,...Gt(ha,["displayDirective"])};var Pe=ve({__TAB__:!0,inheritAttrs:!1,name:"Tab",props:ma,setup(e){const{mergedClsPrefixRef:i,valueRef:o,typeRef:f,closableRef:c,tabStyleRef:T,addTabStyleRef:p,tabClassRef:b,addTabClassRef:g,tabChangeIdRef:h,onBeforeLeaveRef:_,triggerRef:$,handleAdd:k,activateTab:P,handleClose:L}=jt(Xe);return{trigger:$,mergedClosable:ae(()=>{if(e.internalAddable)return!1;const{closable:R}=e;return R===void 0?c.value:R}),style:T,addStyle:p,tabClass:b,addTabClass:g,clsPrefix:i,value:o,type:f,handleClose(R){R.stopPropagation(),!e.disabled&&L(e.name)},activateTab(){if(e.disabled)return;if(e.internalAddable){k();return}const{name:R}=e,O=++h.id;if(R!==o.value){const{value:A}=_;A?Promise.resolve(A(e.name,o.value)).then(q=>{q&&h.id===O&&P(R)}):P(R)}}}},render(){const{internalAddable:e,clsPrefix:i,name:o,disabled:f,label:c,tab:T,value:p,mergedClosable:b,trigger:g,$slots:{default:h}}=this,_=c??T;return l(),v("div",{class:w(`${i}-tabs-tab-wrapper`)},[this.internalLeftPadded?(l(),v("div",{key:0,class:w(`${i}-tabs-tab-pad`)},null,2)):x(()=>null),(l(),v("div",ue({key:o,"data-name":o,"data-disabled":f?!0:void 0},ue({class:[`${i}-tabs-tab`,p===o&&`${i}-tabs-tab--active`,f&&`${i}-tabs-tab--disabled`,b&&`${i}-tabs-tab--closable`,e&&`${i}-tabs-tab--addable`,e?this.addTabClass:this.tabClass],onClick:g==="click"?this.activateTab:void 0,onMouseenter:g==="hover"?this.activateTab:void 0,style:e?this.addStyle:this.style},this.internalCreatedByPane?this.tabProps||{}:this.$attrs)),[ne("span",{class:w(`${i}-tabs-tab__label`)},[e?(l(),v(G,{key:0},[ne("div",{class:w(`${i}-tabs-tab__height-placeholder`)}," ",2),(l(),V(Ne,{clsPrefix:i},{default:()=>(l(),V(na))},1032,["clsPrefix"]))],64)):(l(),v(G,{key:1},[h?(l(),v(G,{key:0},[x(()=>h())],64)):(l(),v(G,{key:1},[typeof _=="object"?(l(),v(G,{key:0},[x(()=>_)],64)):(l(),v(G,{key:1},[x(()=>Ot(_??o))],64))],64))],64))],2),b&&this.type==="card"?(l(),V(Ht,{key:0,clsPrefix:i,class:w(`${i}-tabs-tab__close`),onClick:this.handleClose,disabled:f},null,8,["clsPrefix","class","onClick","disabled"])):x(()=>null)],16,xa))],2)}}),ya=r("tabs",`
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
 `),Ft("disabled",[y("&:hover",`
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
 `)])]),He=ve({name:"TabsButton",props:{type:{type:String,default:"next"},mergedClsPrefix:{type:String,required:!0},vertical:Boolean,disabled:Boolean,rtl:Boolean,theme:Object,themeOverrides:Object,onClick:Function},setup(e){return{handleClick:()=>{var o;e.disabled||(o=e.onClick)==null||o.call(e,e.type)}}},render(){const{mergedClsPrefix:e,disabled:i,type:o,vertical:f,rtl:c,theme:T,themeOverrides:p,handleClick:b}=this,g=o==="next",h=f?g:c?!g:g;return l(),V(Nt,{text:!0,disabled:i,size:"small",theme:T,themeOverrides:p,onClick:b,class:w([`${e}-tabs-scroll-button`,!f&&o==="prev"&&`${e}-tabs-scroll-button--start`,!f&&o==="next"&&`${e}-tabs-scroll-button--end`,f&&o==="prev"&&`${e}-tabs-scroll-button--up`,f&&o==="next"&&`${e}-tabs-scroll-button--down`])},{icon:()=>(l(),V(Ne,{clsPrefix:e,style:re(f?{transform:"rotate(90deg)"}:void 0)},{default:()=>h?(l(),V(Dt,{key:1})):(l(),V(oa,{key:2}))},1032,["clsPrefix","style"]))},1032,["disabled","theme","themeOverrides","onClick","class"])}});const we=ba,Ca={...Ue.props,value:[String,Number],defaultValue:[String,Number],trigger:{type:String,default:"click"},type:{type:String,default:"bar"},closable:Boolean,justifyContent:String,size:String,placement:{type:String,default:"top"},tabStyle:[String,Object],tabClass:String,addTabStyle:[String,Object],addTabClass:String,barWidth:Number,paneClass:String,paneStyle:[String,Object],paneWrapperClass:String,paneWrapperStyle:[String,Object],addable:[Boolean,Object],tabsPadding:{type:Number,default:0},animated:Boolean,onBeforeLeave:Function,onAdd:Function,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onClose:[Function,Array],labelSize:String,activeName:[String,Number],onActiveNameChange:[Function,Array],showScrollButton:Boolean,centerActiveTab:Boolean};var Ra=ve({name:"Tabs",props:Ca,slots:Object,setup(e,{slots:i}){var _e,Ae;const{mergedClsPrefixRef:o,inlineThemeDisabled:f,mergedComponentPropsRef:c,mergedRtlRef:T}=Ut(e),p=Xt("Tabs",T,o),b=ae(()=>{const{placement:t}=e;return t==="start"?p!=null&&p.value?"right":"left":t==="end"?p!=null&&p.value?"left":"right":t}),g=Ue("Tabs","-tabs",ya,ga,e,o),h=I(null),_=I(null),$=I(null),k=I(null),P=I(null),L=I(null),R=I(null),O=I(!0),A=I(!0),q=Ie(e,["labelSize","size"]),J=ae(()=>{var a,n;if(q.value)return q.value;const t=(n=(a=c==null?void 0:c.value)==null?void 0:a.Tabs)==null?void 0:n.size;return t||"medium"}),U=Ie(e,["activeName","value"]),F=I(U.value??e.defaultValue??(i.default?(Ae=(_e=Ce(i.default())[0])==null?void 0:_e.props)==null?void 0:Ae.name:null)),u=Yt(U,F),B={id:0},D=ae(()=>{if(!(!e.justifyContent||e.type==="card"))return{display:"flex",justifyContent:e.justifyContent}});oe(u,()=>{B.id=0,E(),ie(()=>{ge()})});function H(){var a;const{value:t}=u;return t===null?null:(a=h.value)==null?void 0:a.querySelector(`[data-name="${t}"]`)}function le(t){if(e.type==="card")return;const{value:a}=$;if(!a)return;const n=a.style.opacity==="0";if(t){const d=`${o.value}-tabs-bar--disabled`,{barWidth:m}=e,z=b.value;if(t.dataset.disabled==="true"?a.classList.add(d):a.classList.remove(d),["top","bottom"].includes(z)){if(C(["top","maxHeight","height"]),typeof m=="number"&&t.offsetWidth>=m){const S=Math.floor((t.offsetWidth-m)/2)+t.offsetLeft;a.style.left=`${S}px`,a.style.maxWidth=`${m}px`}else a.style.left=`${t.offsetLeft}px`,a.style.maxWidth=`${t.offsetWidth}px`;a.style.width="8192px",n&&(a.style.transition="none"),a.offsetWidth,n&&(a.style.transition="",a.style.opacity="1")}else{if(C(["left","maxWidth","width"]),typeof m=="number"&&t.offsetHeight>=m){const S=Math.floor((t.offsetHeight-m)/2)+t.offsetTop;a.style.top=`${S}px`,a.style.maxHeight=`${m}px`}else a.style.top=`${t.offsetTop}px`,a.style.maxHeight=`${t.offsetHeight}px`;a.style.height="8192px",n&&(a.style.transition="none"),a.offsetHeight,n&&(a.style.transition="",a.style.opacity="1")}}}function N(){if(e.type==="card")return;const{value:t}=$;t&&(t.style.opacity="0")}function C(t){const{value:a}=$;if(a)for(const n of t)a.style[n]=""}function E(){if(e.type==="card")return;const t=H();t?le(t):N()}function Z(t,a,n,d){const m=t.getBoundingClientRect(),z=a.getBoundingClientRect(),S=n?"left":"top",M=n?"right":"bottom";let j=0;d?j=(z[S]+z[M])/2-(m[S]+m[M])/2:z[S]<m[S]?j=z[S]-m[S]:z[M]>m[M]&&(j=z[M]-m[M]),j!==0&&t.scrollBy({[S]:j,behavior:"smooth"})}function ge(){var n;const t=["top","bottom"].includes(b.value),a=H();if(a)if(t){const d=(n=L.value)==null?void 0:n.$el;if(!d)return;Z(d,a,t,e.centerActiveTab)}else{const{value:d}=R;if(!d)return;Z(d,a,t,e.centerActiveTab)}}const se=I(null);let he=0,K=null;function Ye(t){const a=se.value;if(a){he=t.getBoundingClientRect().height;const n=`${he}px`,d=()=>{a.style.height=n,a.style.maxHeight=n};K?(d(),K(),K=null):K=d}}function qe(t){const a=se.value;if(a){const n=t.getBoundingClientRect().height,d=()=>{document.body.offsetHeight,a.style.maxHeight=`${n}px`,a.style.height=`${Math.max(he,n)}px`};K?(K(),K=null,d()):K=d}}function Ke(){const t=se.value;if(t){t.style.maxHeight="",t.style.height="";const{paneWrapperStyle:a}=e;if(typeof a=="string")t.style.cssText=a;else if(a){const{maxHeight:n,height:d}=a;n!==void 0&&(t.style.maxHeight=n),d!==void 0&&(t.style.height=d)}}}const ze={value:[]},Le=I("next");function Je(t){const a=u.value;let n="next";for(const d of ze.value){if(d===a)break;if(d===t){n="prev";break}}Le.value=n,Ze(t)}function Ze(t){const{onActiveNameChange:a,onUpdateValue:n,"onUpdate:value":d}=e;a&&pe(a,t),n&&pe(n,t),d&&pe(d,t),F.value=t}function Qe(t){const{onClose:a}=e;a&&pe(a,t)}function et(t){if(["top","bottom"].includes(b.value)){const{value:a}=L;if(!a)return;const n=a.$el;if(!n)return;const d=n.offsetWidth,m=!!(p!=null&&p.value),z=t==="next"?d:-d;n.scrollBy({left:m?-z:z,behavior:"smooth"})}else{const{value:a}=R;if(!a)return;const n=a.offsetHeight,d=t==="next"?a.scrollTop+n:a.scrollTop-n;a.scrollTo({top:d,left:0,behavior:"smooth"})}}let xe=!0;function me(){const{value:t}=$;if(!t)return;xe&&(xe=!1);const a="transition-disabled";t.classList.add(a),E(),t.classList.remove(a)}const Q=I(null);function de({transitionDisabled:t}){const a=h.value;if(!a)return;t&&a.classList.add("transition-disabled");const n=H();n&&Q.value&&(Q.value.style.width=`${n.offsetWidth}px`,Q.value.style.height=`${n.offsetHeight}px`,Q.value.style.transform=`translate(${n.offsetLeft}px, ${n.offsetTop}px)`,t&&Q.value.offsetWidth),t&&a.classList.remove("transition-disabled")}oe([u],()=>{e.type==="segment"&&ie(()=>{de({transitionDisabled:!1})})}),qt(()=>{e.type==="segment"&&de({transitionDisabled:!0})});let $e=0;function tt(t){var n;if(t.contentRect.width===0&&t.contentRect.height===0||$e===t.contentRect.width)return;$e=t.contentRect.width;const{type:a}=e;(a==="line"||a==="bar")&&(xe||(n=e.justifyContent)!=null&&n.startsWith("space"))&&me(),a!=="segment"&&ce(Be())}const at=we(tt,64);function ke(){const{type:t}=e;t==="line"||t==="bar"?me():t==="segment"&&de({transitionDisabled:!0})}oe([()=>e.justifyContent,()=>e.size],()=>{ie(()=>{(e.type==="line"||e.type==="bar")&&me()})}),oe([b,()=>p==null?void 0:p.value],()=>{ie(()=>{ke(),ce(Be(),{instantly:!0})})}),oe(()=>e.type,()=>{ie(()=>{const t=_.value;t&&(t.classList.add("transition-disabled"),ke(),t.offsetWidth,t.classList.remove("transition-disabled"))})});const ee=I(!1);function rt(t){var M;const{target:a,contentRect:{width:n,height:d}}=t,m=a.parentElement.parentElement.offsetWidth,z=a.parentElement.parentElement.offsetHeight,S=b.value;if(!ee.value)S==="top"||S==="bottom"?m<n&&(ee.value=!0):z<d&&(ee.value=!0);else{const{value:j}=P;if(!j)return;S==="top"||S==="bottom"?m-n>j.$el.offsetWidth&&(ee.value=!1):z-d>j.$el.offsetHeight&&(ee.value=!1)}ce(((M=L.value)==null?void 0:M.$el)||null)}const nt=we(rt,64);function ot(){const{onAdd:t}=e;t&&t()}const ye=I(!1);function Be(){var a;const t=b.value;return(t==="top"||t==="bottom"?(a=L.value)==null?void 0:a.$el:R.value)||null}function ce(t,a={instantly:!1}){if(!t)return;const n=a.instantly?k.value:null;n&&n.classList.add("transition-disabled");const d=1,m=b.value;if(m==="top"||m==="bottom"){const{scrollLeft:z,scrollWidth:S,offsetWidth:M}=t,j=Math.abs(z);O.value=j<=d,A.value=j+M>=S-d,ye.value=M<S-d}else{const{scrollTop:z,scrollHeight:S,offsetHeight:M}=t;O.value=z<=d,A.value=z+M>=S-d,ye.value=M<S-d}n&&(n.offsetWidth,n.classList.remove("transition-disabled"))}const it=we(t=>{ce(t.target)},64);Kt(Xe,{triggerRef:Y(e,"trigger"),tabStyleRef:Y(e,"tabStyle"),tabClassRef:Y(e,"tabClass"),addTabStyleRef:Y(e,"addTabStyle"),addTabClassRef:Y(e,"addTabClass"),paneClassRef:Y(e,"paneClass"),paneStyleRef:Y(e,"paneStyle"),mergedClsPrefixRef:o,typeRef:Y(e,"type"),closableRef:Y(e,"closable"),valueRef:u,tabChangeIdRef:B,onBeforeLeaveRef:Y(e,"onBeforeLeave"),activateTab:Je,handleClose:Qe,handleAdd:ot}),Jt(()=>{E(),ge()}),Zt(()=>{const{value:t}=k;if(!t)return;const{value:a}=o,n=`${a}-tabs-nav-scroll-wrapper--shadow-start`,d=`${a}-tabs-nav-scroll-wrapper--shadow-end`;O.value?t.classList.remove(n):t.classList.add(n),A.value?t.classList.remove(d):t.classList.add(d)});const lt={syncBarPosition:()=>{E()},scrollToCurrentTab:()=>{ge()}},st=()=>{de({transitionDisabled:!0})},We=ae(()=>{const{value:t}=J,{type:a}=e,n=`${t}${{card:"Card",bar:"Bar",line:"Line",segment:"Segment"}[a]}`,{self:{barColor:d,closeIconColor:m,closeIconColorHover:z,closeIconColorPressed:S,tabColor:M,tabBorderColor:j,paneTextColor:dt,tabFontWeight:ct,tabBorderRadius:bt,tabFontWeightActive:ft,colorSegment:pt,fontWeightStrong:ut,tabColorSegment:vt,closeSize:gt,closeIconSize:ht,closeColorHover:xt,closeColorPressed:mt,closeBorderRadius:yt,[X("panePadding",t)]:be,[X("tabPadding",n)]:Ct,[X("tabPaddingVertical",n)]:St,[X("tabGap",n)]:Tt,[X("tabGap",`${n}Vertical`)]:wt,[X("tabTextColor",a)]:Rt,[X("tabTextColorActive",a)]:Pt,[X("tabTextColorHover",a)]:zt,[X("tabTextColorDisabled",a)]:Lt,[X("tabFontSize",t)]:$t},common:{cubicBezierEaseInOut:kt}}=g.value;return{"--n-bezier":kt,"--n-color-segment":pt,"--n-bar-color":d,"--n-tab-font-size":$t,"--n-tab-text-color":Rt,"--n-tab-text-color-active":Pt,"--n-tab-text-color-disabled":Lt,"--n-tab-text-color-hover":zt,"--n-pane-text-color":dt,"--n-tab-border-color":j,"--n-tab-border-radius":bt,"--n-close-size":gt,"--n-close-icon-size":ht,"--n-close-color-hover":xt,"--n-close-color-pressed":mt,"--n-close-border-radius":yt,"--n-close-icon-color":m,"--n-close-icon-color-hover":z,"--n-close-icon-color-pressed":S,"--n-tab-color":M,"--n-tab-font-weight":ct,"--n-tab-font-weight-active":ft,"--n-tab-padding":Ct,"--n-tab-padding-vertical":St,"--n-tab-gap":Tt,"--n-tab-gap-vertical":wt,"--n-pane-padding-left":fe(be,"left"),"--n-pane-padding-right":fe(be,"right"),"--n-pane-padding-top":fe(be,"top"),"--n-pane-padding-bottom":fe(be,"bottom"),"--n-font-weight-strong":ut,"--n-tab-color-segment":vt}}),te=f?Qt("tabs",ae(()=>`${J.value[0]}${e.type[0]}`),We,e):void 0;return{mergedClsPrefix:o,mergedValue:u,renderedNames:new Set,segmentCapsuleElRef:Q,tabsPaneWrapperRef:se,tabsElRef:h,selfElRef:_,barElRef:$,addTabInstRef:P,xScrollInstRef:L,scrollWrapperElRef:k,addTabFixed:ee,tabWrapperStyle:D,handleNavResize:at,mergedSize:J,handleScroll:it,handleTabsResize:nt,cssVars:f?void 0:We,themeClass:te==null?void 0:te.themeClass,animationDirection:Le,renderNameListRef:ze,yScrollElRef:R,handleSegmentResize:st,onAnimationBeforeLeave:Ye,onAnimationEnter:qe,onAnimationAfterEnter:Ke,onRender:te==null?void 0:te.onRender,startReachedRef:O,endReachedRef:A,isOverflow:ye,handleButtonClick:et,mergedTheme:g,rtlEnabled:p,mergedPlacement:b,...lt}},render(){const{mergedClsPrefix:e,type:i,mergedPlacement:o,addTabFixed:f,addable:c,mergedSize:T,renderNameListRef:p,onRender:b,paneWrapperClass:g,paneWrapperStyle:h,startReachedRef:_,endReachedRef:$,isOverflow:k,showScrollButton:P,handleButtonClick:L,mergedTheme:R,rtlEnabled:O,$slots:{default:A,prefix:q,suffix:J}}=this;b==null||b();const U=A?Ce(A()).filter(C=>C.type.__TAB_PANE__===!0):[],F=A?Ce(A()).filter(C=>C.type.__TAB__===!0):[],u=!F.length,B=i==="card",D=i==="segment",H=!B&&!D&&this.justifyContent;p.value=[];const le=()=>{const C=(l(),v("div",{style:re(this.tabWrapperStyle),class:w(`${e}-tabs-wrapper`)},[H?x(()=>null):(l(),v("div",{key:1,class:w(`${e}-tabs-scroll-padding`),style:re(o==="top"||o==="bottom"?{width:`${this.tabsPadding}px`}:{height:`${this.tabsPadding}px`})},null,6)),u?(l(),v(G,{key:2},[x(()=>U.map((E,Z)=>(p.value.push(E.props.name),Re((l(),V(Pe,ue(E.props,{internalCreatedByPane:!0,internalLeftPadded:Z!==0&&(!H||H==="center"||H==="start"||H==="end")}),Ve(E.children?{default:E.children.tab}:void 0),1040,["internalLeftPadded"]))))))],64)):(l(),v(G,{key:3},[x(()=>F.map((E,Z)=>(p.value.push(E.props.name),Re(Z!==0&&!H?Fe(E):E))))],64)),!f&&c&&B?(l(),v(G,{key:4},[x(()=>Ge(c,(u?U.length:F.length)!==0))],64)):x(()=>null),H?x(()=>null):(l(),v("div",{key:7,class:w(`${e}-tabs-scroll-padding`),style:re({width:`${this.tabsPadding}px`})},null,6)),B?x(()=>null):(l(),v("div",{key:9,ref:"barElRef",class:w(`${e}-tabs-bar`)},null,2))],6));return l(),v("div",{ref:"tabsElRef",class:w(`${e}-tabs-nav-scroll-content`)},[B&&c?(l(),V(Se,{key:0,onResize:this.handleTabsResize},{default:()=>C},1032,["onResize"])):(l(),v(G,{key:1},[x(()=>C)],64)),B?(l(),v("div",{key:2,class:w(`${e}-tabs-pad`)},null,2)):x(()=>null)],2)},N=D?"top":o;return l(),v("div",{ref:"selfElRef",class:w([`${e}-tabs`,this.themeClass,`${e}-tabs--${i}-type`,`${e}-tabs--${T}-size`,H&&`${e}-tabs--flex`,`${e}-tabs--${N}`,O&&`${e}-tabs--rtl`]),style:re(this.cssVars)},[ne("div",{class:w([`${e}-tabs-nav--${i}-type`,`${e}-tabs-nav--${N}`,`${e}-tabs-nav`])},[x(()=>Me(q,C=>C&&(l(),v("div",{class:w(`${e}-tabs-nav__prefix`)},[x(()=>C)],2)))),D?(l(),V(Se,{key:0,onResize:this.handleSegmentResize},{default:()=>(l(),v("div",{class:w(`${e}-tabs-rail`),ref:"tabsElRef"},[ne("div",{class:w(`${e}-tabs-capsule`),ref:"segmentCapsuleElRef"},[ne("div",{class:w(`${e}-tabs-wrapper`)},[ne("div",{class:w(`${e}-tabs-tab`)},null,2)],2)],2),u?(l(),v(G,{key:0},[x(()=>U.map((C,E)=>(p.value.push(C.props.name),l(),V(Pe,ue(C.props,{internalCreatedByPane:!0,internalLeftPadded:E!==0}),Ve(C.children?{default:C.children.tab}:void 0),1040,["internalLeftPadded"]))))],64)):(l(),v(G,{key:1},[x(()=>F.map((C,E)=>(p.value.push(C.props.name),E===0?C:Fe(C))))],64))],2))},1032,["onResize"])):(l(),v(G,{key:1},[x(()=>P&&k&&(l(),V(He,{mergedClsPrefix:e,type:"prev",vertical:N==="left"||N==="right",disabled:_,rtl:!!O,theme:R.peers.Button,themeOverrides:R.peerOverrides.Button,onClick:L},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"]))),(l(),V(Se,{onResize:this.handleNavResize},{default:()=>(l(),v("div",{class:w(`${e}-tabs-nav-scroll-wrapper`),ref:"scrollWrapperElRef"},[["top","bottom"].includes(N)?(l(),V(pa,{key:0,ref:"xScrollInstRef",onScroll:this.handleScroll},{default:le},1032,["onScroll"])):(l(),v("div",{key:1,class:w(`${e}-tabs-nav-y-scroll`),onScroll:this.handleScroll,ref:"yScrollElRef"},[x(()=>le())],42,["onScroll"]))],2))},1032,["onResize"])),x(()=>P&&k&&(l(),V(He,{mergedClsPrefix:e,type:"next",vertical:N==="left"||N==="right",disabled:$,rtl:!!O,theme:R.peers.Button,themeOverrides:R.peerOverrides.Button,onClick:L},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"])))],64)),f&&c&&B?(l(),v(G,{key:2},[x(()=>Ge(c,!0))],64)):x(()=>null),x(()=>Me(J,C=>C&&(l(),v("div",{class:w(`${e}-tabs-nav__suffix`)},[x(()=>C)],2))))],2),x(()=>u&&(this.animated&&(N==="top"||N==="bottom")?(l(),v("div",{key:1,ref:"tabsPaneWrapperRef",style:re(h),class:w([`${e}-tabs-pane-wrapper`,g])},[x(()=>je(U,this.mergedValue,this.renderedNames,this.onAnimationBeforeLeave,this.onAnimationEnter,this.onAnimationAfterEnter,this.animationDirection))],6)):je(U,this.mergedValue,this.renderedNames)))],6)}});function je(e,i,o,f,c,T,p){const b=[];return e.forEach(g=>{const{name:h,displayDirective:_,"display-directive":$}=g.props,k=L=>_===L||$===L,P=i===h;if(g.key!==void 0&&(g.key=h),P||k("show")||k("show:lazy")&&o.has(h)){o.has(h)||o.add(h);const L=!k("if");b.push(L?ta(g,[[aa,P]]):g)}}),p?(l(),V(ra,{name:`${p}-transition`,onBeforeLeave:f,onEnter:c,onAfterEnter:T},{default:()=>b},1032,["name","onBeforeLeave","onEnter","onAfterEnter"])):b}function Ge(e,i){return l(),V(Pe,{ref:"addTabInstRef",key:"__addable",name:"__addable",internalCreatedByPane:!0,internalAddable:!0,internalLeftPadded:i,disabled:typeof e=="object"&&e.disabled},null,8,["internalLeftPadded","disabled"])}function Fe(e){const i=ea(e);return i.props?i.props.internalLeftPadded=!0:i.props={internalLeftPadded:!0},i}function Re(e){return Array.isArray(e.dynamicProps)?e.dynamicProps.includes("internalLeftPadded")||e.dynamicProps.push("internalLeftPadded"):e.dynamicProps=["internalLeftPadded"],e}export{Pe as T,Ra as a};
