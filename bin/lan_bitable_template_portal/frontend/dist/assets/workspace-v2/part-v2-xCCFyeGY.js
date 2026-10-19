import{e2 as Wt,cs as Ne,bq as ve,c9 as Lt,eS as _t,bh as At,dG as I,aH as Ee,b7 as Et,dp as s,b4 as v,d2 as S,d9 as g,cH as pe,b1 as ne,H as M,b2 as A,J as Ue,dK as It,r as Ot,cd as jt,aZ as ae,dd as Ht,aI as r,aG as y,aK as l,aJ as k,aL as Ft,d8 as re,m as Mt,i as Dt,eU as Xe,bZ as we,d$ as Ie,d7 as Oe,am as Ce,eB as Vt,eQ as Nt,ed as Ut,eP as Xt,f1 as oe,c$ as ie,dk as Gt,dA as Kt,dj as qt,f2 as Yt,eV as Jt,eA as je,b8 as G,c6 as fe,aX as Zt,fg as Qt,e_ as ea,ah as ta,ek as K,aN as ue}from"./app-v2-m6eYLNOI.js";import{A as aa}from"./part-v2-CN9Nk4uH.js";import{C as ra}from"./part-v2-d8s2OMuJ.js";import{t as He}from"./part-v2-C2x3b4zQ.js";var Se=function(){return Wt.Date.now()},na="Expected a function",oa=Math.max,ia=Math.min;function sa(e,o,i){var u,c,R,f,b,h,m=0,O=!1,P=!1,E=!0;if(typeof e!="function")throw new TypeError(na);o=He(o)||0,Ne(i)&&(O=!!i.leading,P="maxWait"in i,R=P?oa(He(i.maxWait)||0,o):R,E="trailing"in i?!!i.trailing:E);function j(p){var $=u,N=c;return u=c=void 0,m=p,f=e.apply(N,$),f}function B(p){return m=p,b=setTimeout(W,o),O?j(p):f}function z(p){var $=p-h,N=p-m,H=o-$;return P?ia(H,R-N):H}function D(p){var $=p-h,N=p-m;return h===void 0||$>=o||$<0||P&&N>=R}function W(){var p=Se();if(D(p))return q(p);b=setTimeout(W,z(p))}function q(p){return b=void 0,E&&u?j(p):(u=c=void 0,f)}function J(){b!==void 0&&clearTimeout(b),m=0,u=h=c=b=void 0}function X(){return b===void 0?f:q(Se())}function V(){var p=Se(),$=D(p);if(u=arguments,c=this,h=p,$){if(b===void 0)return B(h);if(P)return clearTimeout(b),b=setTimeout(W,o),j(h)}return b===void 0&&(b=setTimeout(W,o)),f}return V.cancel=J,V.flush=X,V}var la="Expected a function";function da(e,o,i){var u=!0,c=!0;if(typeof e!="function")throw new TypeError(la);return Ne(i)&&(u="leading"in i?!!i.leading:u,c="trailing"in i?!!i.trailing:c),sa(e,o,{leading:u,maxWait:o,trailing:c})}const ca=Ee(".v-x-scroll",{overflow:"auto",scrollbarWidth:"none"},[Ee("&::-webkit-scrollbar",{width:0,height:0})]),ba=ve({name:"XScroll",props:{disabled:Boolean,onScroll:Function},setup(){const e=I(null);function o(c){!(c.currentTarget.offsetWidth<c.currentTarget.scrollWidth)||c.deltaY===0||(c.currentTarget.scrollLeft+=c.deltaY+c.deltaX,c.preventDefault())}const i=_t();return ca.mount({id:"vueuc/x-scroll",head:!0,anchorMetaName:At,ssr:i}),Object.assign({selfRef:e,handleWheel:o},{scrollTo(...c){var R;(R=e.value)===null||R===void 0||R.scrollTo(...c)}})},render(){return Lt("div",{ref:"selfRef",onScroll:this.onScroll,onWheel:this.disabled?void 0:this.handleWheel,class:"v-x-scroll"},this.$slots)}}),Ge=Et("n-tabs"),fa={tab:[String,Number,Object,Function],name:{type:[String,Number],required:!0},disabled:Boolean,displayDirective:{type:String,default:"if"},closable:{type:Boolean,default:void 0},tabProps:Object,label:[String,Number,Object,Function]},ua=["data-name","data-disabled"],pa={internalLeftPadded:Boolean,internalAddable:Boolean,internalCreatedByPane:Boolean,...Ht(fa,["displayDirective"])};var ze=ve({__TAB__:!0,inheritAttrs:!1,name:"Tab",props:pa,setup(e){const{mergedClsPrefixRef:o,valueRef:i,typeRef:u,closableRef:c,tabStyleRef:R,addTabStyleRef:f,tabClassRef:b,addTabClassRef:h,tabChangeIdRef:m,onBeforeLeaveRef:O,triggerRef:P,handleAdd:E,activateTab:j,handleClose:B}=jt(Ge);return{trigger:P,mergedClosable:ae(()=>{if(e.internalAddable)return!1;const{closable:z}=e;return z===void 0?c.value:z}),style:R,addStyle:f,tabClass:b,addTabClass:h,clsPrefix:o,value:i,type:u,handleClose(z){z.stopPropagation(),!e.disabled&&B(e.name)},activateTab(){if(e.disabled)return;if(e.internalAddable){E();return}const{name:z}=e,D=++m.id;if(z!==i.value){const{value:W}=O;W?Promise.resolve(W(e.name,i.value)).then(q=>{q&&m.id===D&&j(z)}):j(z)}}}},render(){const{internalAddable:e,clsPrefix:o,name:i,disabled:u,label:c,tab:R,value:f,mergedClosable:b,trigger:h,$slots:{default:m}}=this,O=c??R;return s(),v("div",{class:S(`${o}-tabs-tab-wrapper`)},[this.internalLeftPadded?(s(),v("div",{key:0,class:S(`${o}-tabs-tab-pad`)},null,2)):g(()=>null),(s(),v("div",pe({key:i,"data-name":i,"data-disabled":u?!0:void 0},pe({class:[`${o}-tabs-tab`,f===i&&`${o}-tabs-tab--active`,u&&`${o}-tabs-tab--disabled`,b&&`${o}-tabs-tab--closable`,e&&`${o}-tabs-tab--addable`,e?this.addTabClass:this.tabClass],onClick:h==="click"?this.activateTab:void 0,onMouseenter:h==="hover"?this.activateTab:void 0,style:e?this.addStyle:this.style},this.internalCreatedByPane?this.tabProps||{}:this.$attrs)),[ne("span",{class:S(`${o}-tabs-tab__label`)},[e?(s(),v(M,{key:0},[ne("div",{class:S(`${o}-tabs-tab__height-placeholder`)}," ",2),(s(),A(Ue,{clsPrefix:o},{default:()=>(s(),A(aa))},1032,["clsPrefix"]))],64)):(s(),v(M,{key:1},[m?(s(),v(M,{key:0},[g(()=>m())],64)):(s(),v(M,{key:1},[typeof O=="object"?(s(),v(M,{key:0},[g(()=>O)],64)):(s(),v(M,{key:1},[g(()=>It(O??i))],64))],64))],64))],2),b&&this.type==="card"?(s(),A(Ot,{key:0,clsPrefix:o,class:S(`${o}-tabs-tab__close`),onClick:this.handleClose,disabled:u},null,8,["clsPrefix","class","onClick","disabled"])):g(()=>null)],16,ua))],2)}}),va=r("tabs",`
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
 `)]),l("segment-type",[r("tabs-rail",[y("&.transition-disabled",[r("tabs-capsule",`
 transition: none;
 `)])])]),l("top",[r("tab-pane",`
 padding: var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left);
 `)]),l("left",[r("tab-pane",`
 padding: var(--n-pane-padding-right) var(--n-pane-padding-bottom) var(--n-pane-padding-left) var(--n-pane-padding-top);
 `)]),l("left, right",`
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
 `)]),l("right",`
 flex-direction: row-reverse;
 `,[r("tab-pane",`
 padding: var(--n-pane-padding-left) var(--n-pane-padding-top) var(--n-pane-padding-right) var(--n-pane-padding-bottom);
 `),r("tabs-bar",`
 left: 0;
 `)]),l("bottom",`
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
 `,[l("active",`
 font-weight: var(--n-font-weight-strong);
 color: var(--n-tab-text-color-active);
 `),y("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])])]),l("flex",[r("tabs-nav",`
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
 `,[k("prefix, suffix",`
 display: flex;
 align-items: center;
 `),k("prefix","padding-right: 16px;"),k("suffix","padding-left: 16px;")]),l("top, bottom",[y(">",[r("tabs-nav",[r("tabs-nav-scroll-wrapper",[y("&::before",`
 top: 0;
 bottom: 0;
 left: 0;
 width: 20px;
 `),y("&::after",`
 top: 0;
 bottom: 0;
 right: 0;
 width: 20px;
 `),l("shadow-start",[y("&::before",`
 box-shadow: inset 10px 0 8px -8px rgba(0, 0, 0, .12);
 `)]),l("shadow-end",[y("&::after",`
 box-shadow: inset -10px 0 8px -8px rgba(0, 0, 0, .12);
 `)])])])])]),l("left, right",[r("tabs-nav-scroll-content",`
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
 `),l("shadow-start",[y("&::before",`
 box-shadow: inset 0 10px 8px -8px rgba(0, 0, 0, .12);
 `)]),l("shadow-end",[y("&::after",`
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
 `,[l("disabled",{cursor:"not-allowed"}),k("close",`
 margin-inline-start: 6px;
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `),k("label",`
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
 `),l("disabled",`
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
 `),l("line-type, bar-type",[r("tabs-tab",`
 font-weight: var(--n-tab-font-weight);
 box-sizing: border-box;
 vertical-align: bottom;
 `,[y("&:hover",{color:"var(--n-tab-text-color-hover)"}),l("active",`
 color: var(--n-tab-text-color-active);
 font-weight: var(--n-tab-font-weight-active);
 `),l("disabled",{color:"var(--n-tab-text-color-disabled)"})])]),r("tabs-nav",[k("prefix, suffix",`
 border-color: var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-color: var(--n-tab-border-color);
 `),l("line-type",[l("top",[k("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 bottom: -1px;
 `)]),l("left",[k("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 right: -1px;
 `)]),l("right",[k("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 left: -1px;
 `)]),l("bottom",[k("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-nav-scroll-content",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-bar",`
 top: -1px;
 `)]),k("prefix, suffix",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-nav-scroll-content",`
 transition: border-color .3s var(--n-bezier);
 `),r("tabs-bar",`
 border-radius: 0;
 `)]),l("card-type",[k("prefix, suffix",`
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
 `,[l("addable",`
 padding-left: 8px;
 padding-right: 8px;
 font-size: 16px;
 justify-content: center;
 `,[k("height-placeholder",`
 width: 0;
 font-size: var(--n-tab-font-size);
 `),Ft("disabled",[y("&:hover",`
 color: var(--n-tab-text-color-hover);
 `)])]),l("closable","padding-inline-end: 8px;"),l("active",`
 background-color: #0000;
 font-weight: var(--n-tab-font-weight-active);
 color: var(--n-tab-text-color-active);
 `),l("disabled","color: var(--n-tab-text-color-disabled);")])]),l("left, right",`
 flex-direction: column; 
 `,[k("prefix, suffix",`
 padding: var(--n-tab-padding-vertical);
 `),r("tabs-wrapper",`
 flex-direction: column;
 `),r("tabs-tab-wrapper",`
 flex-direction: column;
 `,[r("tabs-tab-pad",`
 height: var(--n-tab-gap-vertical);
 width: 100%;
 `)])]),l("top",[l("card-type",[r("tabs-scroll-padding","border-bottom: 1px solid var(--n-tab-border-color);"),k("prefix, suffix",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-top-right-radius: var(--n-tab-border-radius);
 `,[l("active",`
 border-bottom: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-bottom: 1px solid var(--n-tab-border-color);
 `)])]),l("left",[l("card-type",[r("tabs-scroll-padding","border-right: 1px solid var(--n-tab-border-color);"),k("prefix, suffix",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-left-radius: var(--n-tab-border-radius);
 border-bottom-left-radius: var(--n-tab-border-radius);
 `,[l("active",`
 border-right: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-right: 1px solid var(--n-tab-border-color);
 `)])]),l("right",[l("card-type",[r("tabs-scroll-padding","border-left: 1px solid var(--n-tab-border-color);"),k("prefix, suffix",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-top-right-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[l("active",`
 border-left: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-left: 1px solid var(--n-tab-border-color);
 `)])]),l("bottom",[l("card-type",[r("tabs-scroll-padding","border-top: 1px solid var(--n-tab-border-color);"),k("prefix, suffix",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-tab",`
 border-bottom-left-radius: var(--n-tab-border-radius);
 border-bottom-right-radius: var(--n-tab-border-radius);
 `,[l("active",`
 border-top: 1px solid #0000;
 `)]),r("tabs-tab-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `),r("tabs-pad",`
 border-top: 1px solid var(--n-tab-border-color);
 `)])])]),r("tabs-scroll-button",[l("start",`
 padding-left: 10px;
 padding-right: 6px;
 `),l("end",`
 padding-right: 10px;
 padding-left: 6px;
 `),l("up",`
 padding-bottom: 10px;
 `),l("down",`
 padding-top: 10px;
 `)])]),Fe=ve({name:"TabsButton",props:{type:{type:String,default:"next"},mergedClsPrefix:{type:String,required:!0},vertical:Boolean,disabled:Boolean,rtl:Boolean,theme:Object,themeOverrides:Object,onClick:Function},setup(e){return{handleClick:()=>{var i;e.disabled||(i=e.onClick)==null||i.call(e,e.type)}}},render(){const{mergedClsPrefix:e,disabled:o,type:i,vertical:u,rtl:c,theme:R,themeOverrides:f,handleClick:b}=this,h=i==="next",m=u?h:c?!h:h;return s(),A(Dt,{text:!0,disabled:o,size:"small",theme:R,themeOverrides:f,onClick:b,class:S([`${e}-tabs-scroll-button`,!u&&i==="prev"&&`${e}-tabs-scroll-button--start`,!u&&i==="next"&&`${e}-tabs-scroll-button--end`,u&&i==="prev"&&`${e}-tabs-scroll-button--up`,u&&i==="next"&&`${e}-tabs-scroll-button--down`])},{icon:()=>(s(),A(Ue,{clsPrefix:e,style:re(u?{transform:"rotate(90deg)"}:void 0)},{default:()=>m?(s(),A(Mt,{key:1})):(s(),A(ra,{key:2}))},1032,["clsPrefix","style"]))},1032,["disabled","theme","themeOverrides","onClick","class"])}});const Re=da,ha={...Xe.props,value:[String,Number],defaultValue:[String,Number],trigger:{type:String,default:"click"},type:{type:String,default:"bar"},closable:Boolean,justifyContent:String,size:String,placement:{type:String,default:"top"},tabStyle:[String,Object],tabClass:String,addTabStyle:[String,Object],addTabClass:String,barWidth:Number,paneClass:String,paneStyle:[String,Object],paneWrapperClass:String,paneWrapperStyle:[String,Object],addable:[Boolean,Object],tabsPadding:{type:Number,default:0},animated:Boolean,onBeforeLeave:Function,onAdd:Function,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onClose:[Function,Array],labelSize:String,activeName:[String,Number],onActiveNameChange:[Function,Array],showScrollButton:Boolean,centerActiveTab:Boolean};var wa=ve({name:"Tabs",props:ha,slots:Object,setup(e,{slots:o}){var _e,Ae;const{mergedClsPrefixRef:i,inlineThemeDisabled:u,mergedComponentPropsRef:c,mergedRtlRef:R}=Vt(e),f=Nt("Tabs",R,i),b=ae(()=>{const{placement:t}=e;return t==="start"?f!=null&&f.value?"right":"left":t==="end"?f!=null&&f.value?"left":"right":t}),h=Xe("Tabs","-tabs",va,Ut,e,i),m=I(null),O=I(null),P=I(null),E=I(null),j=I(null),B=I(null),z=I(null),D=I(!0),W=I(!0),q=je(e,["labelSize","size"]),J=ae(()=>{var a,n;if(q.value)return q.value;const t=(n=(a=c==null?void 0:c.value)==null?void 0:a.Tabs)==null?void 0:n.size;return t||"medium"}),X=je(e,["activeName","value"]),V=I(X.value??e.defaultValue??(o.default?(Ae=(_e=we(o.default())[0])==null?void 0:_e.props)==null?void 0:Ae.name:null)),p=Xt(X,V),$={id:0},N=ae(()=>{if(!(!e.justifyContent||e.type==="card"))return{display:"flex",justifyContent:e.justifyContent}});oe(p,()=>{$.id=0,L(),ie(()=>{he()})});function H(){var a;const{value:t}=p;return t===null?null:(a=m.value)==null?void 0:a.querySelector(`[data-name="${t}"]`)}function se(t){if(e.type==="card")return;const{value:a}=P;if(!a)return;const n=a.style.opacity==="0";if(t){const d=`${i.value}-tabs-bar--disabled`,{barWidth:x}=e,T=b.value;if(t.dataset.disabled==="true"?a.classList.add(d):a.classList.remove(d),["top","bottom"].includes(T)){if(w(["top","maxHeight","height"]),typeof x=="number"&&t.offsetWidth>=x){const C=Math.floor((t.offsetWidth-x)/2)+t.offsetLeft;a.style.left=`${C}px`,a.style.maxWidth=`${x}px`}else a.style.left=`${t.offsetLeft}px`,a.style.maxWidth=`${t.offsetWidth}px`;a.style.width="8192px",n&&(a.style.transition="none"),a.offsetWidth,n&&(a.style.transition="",a.style.opacity="1")}else{if(w(["left","maxWidth","width"]),typeof x=="number"&&t.offsetHeight>=x){const C=Math.floor((t.offsetHeight-x)/2)+t.offsetTop;a.style.top=`${C}px`,a.style.maxHeight=`${x}px`}else a.style.top=`${t.offsetTop}px`,a.style.maxHeight=`${t.offsetHeight}px`;a.style.height="8192px",n&&(a.style.transition="none"),a.offsetHeight,n&&(a.style.transition="",a.style.opacity="1")}}}function U(){if(e.type==="card")return;const{value:t}=P;t&&(t.style.opacity="0")}function w(t){const{value:a}=P;if(a)for(const n of t)a.style[n]=""}function L(){if(e.type==="card")return;const t=H();t?se(t):U()}function Z(t,a,n,d){const x=t.getBoundingClientRect(),T=a.getBoundingClientRect(),C=n?"left":"top",_=n?"right":"bottom";let F=0;d?F=(T[C]+T[_])/2-(x[C]+x[_])/2:T[C]<x[C]?F=T[C]-x[C]:T[_]>x[_]&&(F=T[_]-x[_]),F!==0&&t.scrollBy({[C]:F,behavior:"smooth"})}function he(){var n;const t=["top","bottom"].includes(b.value),a=H();if(a)if(t){const d=(n=B.value)==null?void 0:n.$el;if(!d)return;Z(d,a,t,e.centerActiveTab)}else{const{value:d}=z;if(!d)return;Z(d,a,t,e.centerActiveTab)}}const le=I(null);let ge=0,Y=null;function Ke(t){const a=le.value;if(a){ge=t.getBoundingClientRect().height;const n=`${ge}px`,d=()=>{a.style.height=n,a.style.maxHeight=n};Y?(d(),Y(),Y=null):Y=d}}function qe(t){const a=le.value;if(a){const n=t.getBoundingClientRect().height,d=()=>{document.body.offsetHeight,a.style.maxHeight=`${n}px`,a.style.height=`${Math.max(ge,n)}px`};Y?(Y(),Y=null,d()):Y=d}}function Ye(){const t=le.value;if(t){t.style.maxHeight="",t.style.height="";const{paneWrapperStyle:a}=e;if(typeof a=="string")t.style.cssText=a;else if(a){const{maxHeight:n,height:d}=a;n!==void 0&&(t.style.maxHeight=n),d!==void 0&&(t.style.height=d)}}}const $e={value:[]},ke=I("next");function Je(t){const a=p.value;let n="next";for(const d of $e.value){if(d===a)break;if(d===t){n="prev";break}}ke.value=n,Ze(t)}function Ze(t){const{onActiveNameChange:a,onUpdateValue:n,"onUpdate:value":d}=e;a&&ue(a,t),n&&ue(n,t),d&&ue(d,t),V.value=t}function Qe(t){const{onClose:a}=e;a&&ue(a,t)}function et(t){if(["top","bottom"].includes(b.value)){const{value:a}=B;if(!a)return;const n=a.$el;if(!n)return;const d=n.offsetWidth,x=!!(f!=null&&f.value),T=t==="next"?d:-d;n.scrollBy({left:x?-T:T,behavior:"smooth"})}else{const{value:a}=z;if(!a)return;const n=a.offsetHeight,d=t==="next"?a.scrollTop+n:a.scrollTop-n;a.scrollTo({top:d,left:0,behavior:"smooth"})}}let me=!0;function xe(){const{value:t}=P;if(!t)return;me&&(me=!1);const a="transition-disabled";t.classList.add(a),L(),t.classList.remove(a)}const Q=I(null);function de({transitionDisabled:t}){const a=m.value;if(!a)return;t&&a.classList.add("transition-disabled");const n=H();n&&Q.value&&(Q.value.style.width=`${n.offsetWidth}px`,Q.value.style.height=`${n.offsetHeight}px`,Q.value.style.transform=`translate(${n.offsetLeft}px, ${n.offsetTop}px)`,t&&Q.value.offsetWidth),t&&a.classList.remove("transition-disabled")}oe([p],()=>{e.type==="segment"&&ie(()=>{de({transitionDisabled:!1})})}),Gt(()=>{e.type==="segment"&&de({transitionDisabled:!0})});let Pe=0;function tt(t){var n;if(t.contentRect.width===0&&t.contentRect.height===0||Pe===t.contentRect.width)return;Pe=t.contentRect.width;const{type:a}=e;(a==="line"||a==="bar")&&(me||(n=e.justifyContent)!=null&&n.startsWith("space"))&&xe(),a!=="segment"&&ce(We())}const at=Re(tt,64);function Be(){const{type:t}=e;t==="line"||t==="bar"?xe():t==="segment"&&de({transitionDisabled:!0})}oe([()=>e.justifyContent,()=>e.size],()=>{ie(()=>{(e.type==="line"||e.type==="bar")&&xe()})}),oe([b,()=>f==null?void 0:f.value],()=>{ie(()=>{Be(),ce(We(),{instantly:!0})})}),oe(()=>e.type,()=>{ie(()=>{const t=O.value;t&&(t.classList.add("transition-disabled"),Be(),t.offsetWidth,t.classList.remove("transition-disabled"))})});const ee=I(!1);function rt(t){var _;const{target:a,contentRect:{width:n,height:d}}=t,x=a.parentElement.parentElement.offsetWidth,T=a.parentElement.parentElement.offsetHeight,C=b.value;if(!ee.value)C==="top"||C==="bottom"?x<n&&(ee.value=!0):T<d&&(ee.value=!0);else{const{value:F}=j;if(!F)return;C==="top"||C==="bottom"?x-n>F.$el.offsetWidth&&(ee.value=!1):T-d>F.$el.offsetHeight&&(ee.value=!1)}ce(((_=B.value)==null?void 0:_.$el)||null)}const nt=Re(rt,64);function ot(){const{onAdd:t}=e;t&&t()}const ye=I(!1);function We(){var a;const t=b.value;return(t==="top"||t==="bottom"?(a=B.value)==null?void 0:a.$el:z.value)||null}function ce(t,a={instantly:!1}){if(!t)return;const n=a.instantly?E.value:null;n&&n.classList.add("transition-disabled");const d=1,x=b.value;if(x==="top"||x==="bottom"){const{scrollLeft:T,scrollWidth:C,offsetWidth:_}=t,F=Math.abs(T);D.value=F<=d,W.value=F+_>=C-d,ye.value=_<C-d}else{const{scrollTop:T,scrollHeight:C,offsetHeight:_}=t;D.value=T<=d,W.value=T+_>=C-d,ye.value=_<C-d}n&&(n.offsetWidth,n.classList.remove("transition-disabled"))}const it=Re(t=>{ce(t.target)},64);Kt(Ge,{triggerRef:K(e,"trigger"),tabStyleRef:K(e,"tabStyle"),tabClassRef:K(e,"tabClass"),addTabStyleRef:K(e,"addTabStyle"),addTabClassRef:K(e,"addTabClass"),paneClassRef:K(e,"paneClass"),paneStyleRef:K(e,"paneStyle"),mergedClsPrefixRef:i,typeRef:K(e,"type"),closableRef:K(e,"closable"),valueRef:p,tabChangeIdRef:$,onBeforeLeaveRef:K(e,"onBeforeLeave"),activateTab:Je,handleClose:Qe,handleAdd:ot}),qt(()=>{L(),he()}),Yt(()=>{const{value:t}=E;if(!t)return;const{value:a}=i,n=`${a}-tabs-nav-scroll-wrapper--shadow-start`,d=`${a}-tabs-nav-scroll-wrapper--shadow-end`;D.value?t.classList.remove(n):t.classList.add(n),W.value?t.classList.remove(d):t.classList.add(d)});const st={syncBarPosition:()=>{L()},scrollToCurrentTab:()=>{he()}},lt=()=>{de({transitionDisabled:!0})},Le=ae(()=>{const{value:t}=J,{type:a}=e,n=`${t}${{card:"Card",bar:"Bar",line:"Line",segment:"Segment"}[a]}`,{self:{barColor:d,closeIconColor:x,closeIconColorHover:T,closeIconColorPressed:C,tabColor:_,tabBorderColor:F,paneTextColor:dt,tabFontWeight:ct,tabBorderRadius:bt,tabFontWeightActive:ft,colorSegment:ut,fontWeightStrong:pt,tabColorSegment:vt,closeSize:ht,closeIconSize:gt,closeColorHover:mt,closeColorPressed:xt,closeBorderRadius:yt,[G("panePadding",t)]:be,[G("tabPadding",n)]:wt,[G("tabPaddingVertical",n)]:Ct,[G("tabGap",n)]:St,[G("tabGap",`${n}Vertical`)]:Rt,[G("tabTextColor",a)]:Tt,[G("tabTextColorActive",a)]:zt,[G("tabTextColorHover",a)]:$t,[G("tabTextColorDisabled",a)]:kt,[G("tabFontSize",t)]:Pt},common:{cubicBezierEaseInOut:Bt}}=h.value;return{"--n-bezier":Bt,"--n-color-segment":ut,"--n-bar-color":d,"--n-tab-font-size":Pt,"--n-tab-text-color":Tt,"--n-tab-text-color-active":zt,"--n-tab-text-color-disabled":kt,"--n-tab-text-color-hover":$t,"--n-pane-text-color":dt,"--n-tab-border-color":F,"--n-tab-border-radius":bt,"--n-close-size":ht,"--n-close-icon-size":gt,"--n-close-color-hover":mt,"--n-close-color-pressed":xt,"--n-close-border-radius":yt,"--n-close-icon-color":x,"--n-close-icon-color-hover":T,"--n-close-icon-color-pressed":C,"--n-tab-color":_,"--n-tab-font-weight":ct,"--n-tab-font-weight-active":ft,"--n-tab-padding":wt,"--n-tab-padding-vertical":Ct,"--n-tab-gap":St,"--n-tab-gap-vertical":Rt,"--n-pane-padding-left":fe(be,"left"),"--n-pane-padding-right":fe(be,"right"),"--n-pane-padding-top":fe(be,"top"),"--n-pane-padding-bottom":fe(be,"bottom"),"--n-font-weight-strong":pt,"--n-tab-color-segment":vt}}),te=u?Jt("tabs",ae(()=>`${J.value[0]}${e.type[0]}`),Le,e):void 0;return{mergedClsPrefix:i,mergedValue:p,renderedNames:new Set,segmentCapsuleElRef:Q,tabsPaneWrapperRef:le,tabsElRef:m,selfElRef:O,barElRef:P,addTabInstRef:j,xScrollInstRef:B,scrollWrapperElRef:E,addTabFixed:ee,tabWrapperStyle:N,handleNavResize:at,mergedSize:J,handleScroll:it,handleTabsResize:nt,cssVars:u?void 0:Le,themeClass:te==null?void 0:te.themeClass,animationDirection:ke,renderNameListRef:$e,yScrollElRef:z,handleSegmentResize:lt,onAnimationBeforeLeave:Ke,onAnimationEnter:qe,onAnimationAfterEnter:Ye,onRender:te==null?void 0:te.onRender,startReachedRef:D,endReachedRef:W,isOverflow:ye,handleButtonClick:et,mergedTheme:h,rtlEnabled:f,mergedPlacement:b,...st}},render(){const{mergedClsPrefix:e,type:o,mergedPlacement:i,addTabFixed:u,addable:c,mergedSize:R,renderNameListRef:f,onRender:b,paneWrapperClass:h,paneWrapperStyle:m,startReachedRef:O,endReachedRef:P,isOverflow:E,showScrollButton:j,handleButtonClick:B,mergedTheme:z,rtlEnabled:D,$slots:{default:W,prefix:q,suffix:J}}=this;b==null||b();const X=W?we(W()).filter(w=>w.type.__TAB_PANE__===!0):[],V=W?we(W()).filter(w=>w.type.__TAB__===!0):[],p=!V.length,$=o==="card",N=o==="segment",H=!$&&!N&&this.justifyContent;f.value=[];const se=()=>{const w=(s(),v("div",{style:re(this.tabWrapperStyle),class:S(`${e}-tabs-wrapper`)},[H?g(()=>null):(s(),v("div",{key:1,class:S(`${e}-tabs-scroll-padding`),style:re(i==="top"||i==="bottom"?{width:`${this.tabsPadding}px`}:{height:`${this.tabsPadding}px`})},null,6)),p?(s(),v(M,{key:2},[g(()=>X.map((L,Z)=>(f.value.push(L.props.name),Te((s(),A(ze,pe(L.props,{internalCreatedByPane:!0,internalLeftPadded:Z!==0&&(!H||H==="center"||H==="start"||H==="end")}),Oe(L.children?{default:L.children.tab}:void 0),1040,["internalLeftPadded"]))))))],64)):(s(),v(M,{key:3},[g(()=>V.map((L,Z)=>(f.value.push(L.props.name),Te(Z!==0&&!H?Ve(L):L))))],64)),!u&&c&&$?(s(),v(M,{key:4},[g(()=>De(c,(p?X.length:V.length)!==0))],64)):g(()=>null),H?g(()=>null):(s(),v("div",{key:7,class:S(`${e}-tabs-scroll-padding`),style:re({width:`${this.tabsPadding}px`})},null,6)),$?g(()=>null):(s(),v("div",{key:9,ref:"barElRef",class:S(`${e}-tabs-bar`)},null,2))],6));return s(),v("div",{ref:"tabsElRef",class:S(`${e}-tabs-nav-scroll-content`)},[$&&c?(s(),A(Ce,{key:0,onResize:this.handleTabsResize},{default:()=>w},1032,["onResize"])):(s(),v(M,{key:1},[g(()=>w)],64)),$?(s(),v("div",{key:2,class:S(`${e}-tabs-pad`)},null,2)):g(()=>null)],2)},U=N?"top":i;return s(),v("div",{ref:"selfElRef",class:S([`${e}-tabs`,this.themeClass,`${e}-tabs--${o}-type`,`${e}-tabs--${R}-size`,H&&`${e}-tabs--flex`,`${e}-tabs--${U}`,D&&`${e}-tabs--rtl`]),style:re(this.cssVars)},[ne("div",{class:S([`${e}-tabs-nav--${o}-type`,`${e}-tabs-nav--${U}`,`${e}-tabs-nav`])},[g(()=>Ie(q,w=>w&&(s(),v("div",{class:S(`${e}-tabs-nav__prefix`)},[g(()=>w)],2)))),N?(s(),A(Ce,{key:0,onResize:this.handleSegmentResize},{default:()=>(s(),v("div",{class:S(`${e}-tabs-rail`),ref:"tabsElRef"},[ne("div",{class:S(`${e}-tabs-capsule`),ref:"segmentCapsuleElRef"},[ne("div",{class:S(`${e}-tabs-wrapper`)},[ne("div",{class:S(`${e}-tabs-tab`)},null,2)],2)],2),p?(s(),v(M,{key:0},[g(()=>X.map((w,L)=>(f.value.push(w.props.name),s(),A(ze,pe(w.props,{internalCreatedByPane:!0,internalLeftPadded:L!==0}),Oe(w.children?{default:w.children.tab}:void 0),1040,["internalLeftPadded"]))))],64)):(s(),v(M,{key:1},[g(()=>V.map((w,L)=>(f.value.push(w.props.name),L===0?w:Ve(w))))],64))],2))},1032,["onResize"])):(s(),v(M,{key:1},[g(()=>j&&E&&(s(),A(Fe,{mergedClsPrefix:e,type:"prev",vertical:U==="left"||U==="right",disabled:O,rtl:!!D,theme:z.peers.Button,themeOverrides:z.peerOverrides.Button,onClick:B},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"]))),(s(),A(Ce,{onResize:this.handleNavResize},{default:()=>(s(),v("div",{class:S(`${e}-tabs-nav-scroll-wrapper`),ref:"scrollWrapperElRef"},[["top","bottom"].includes(U)?(s(),A(ba,{key:0,ref:"xScrollInstRef",onScroll:this.handleScroll},{default:se},1032,["onScroll"])):(s(),v("div",{key:1,class:S(`${e}-tabs-nav-y-scroll`),onScroll:this.handleScroll,ref:"yScrollElRef"},[g(()=>se())],42,["onScroll"]))],2))},1032,["onResize"])),g(()=>j&&E&&(s(),A(Fe,{mergedClsPrefix:e,type:"next",vertical:U==="left"||U==="right",disabled:P,rtl:!!D,theme:z.peers.Button,themeOverrides:z.peerOverrides.Button,onClick:B},null,8,["mergedClsPrefix","vertical","disabled","rtl","theme","themeOverrides","onClick"])))],64)),u&&c&&$?(s(),v(M,{key:2},[g(()=>De(c,!0))],64)):g(()=>null),g(()=>Ie(J,w=>w&&(s(),v("div",{class:S(`${e}-tabs-nav__suffix`)},[g(()=>w)],2))))],2),g(()=>p&&(this.animated&&(U==="top"||U==="bottom")?(s(),v("div",{key:1,ref:"tabsPaneWrapperRef",style:re(m),class:S([`${e}-tabs-pane-wrapper`,h])},[g(()=>Me(X,this.mergedValue,this.renderedNames,this.onAnimationBeforeLeave,this.onAnimationEnter,this.onAnimationAfterEnter,this.animationDirection))],6)):Me(X,this.mergedValue,this.renderedNames)))],6)}});function Me(e,o,i,u,c,R,f){const b=[];return e.forEach(h=>{const{name:m,displayDirective:O,"display-directive":P}=h.props,E=B=>O===B||P===B,j=o===m;if(h.key!==void 0&&(h.key=m),j||E("show")||E("show:lazy")&&i.has(m)){i.has(m)||i.add(m);const B=!E("if");b.push(B?Qt(h,[[ea,j]]):h)}}),f?(s(),A(ta,{name:`${f}-transition`,onBeforeLeave:u,onEnter:c,onAfterEnter:R},{default:()=>b},1032,["name","onBeforeLeave","onEnter","onAfterEnter"])):b}function De(e,o){return s(),A(ze,{ref:"addTabInstRef",key:"__addable",name:"__addable",internalCreatedByPane:!0,internalAddable:!0,internalLeftPadded:o,disabled:typeof e=="object"&&e.disabled},null,8,["internalLeftPadded","disabled"])}function Ve(e){const o=Zt(e);return o.props?o.props.internalLeftPadded=!0:o.props={internalLeftPadded:!0},o}function Te(e){return Array.isArray(e.dynamicProps)?e.dynamicProps.includes("internalLeftPadded")||e.dynamicProps.push("internalLeftPadded"):e.dynamicProps=["internalLeftPadded"],e}export{ze as T,wa as a};
