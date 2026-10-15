import{aY as X,be as H,ax as x,az as g,ay as i,av as k,bq as J,aA as Y,bc as A,dW as U,cI as h,aU as z,cv as y,cu as G,cp as v,dE as W,cX as Q,dR as Z,cR as ee,dS as V,dX as ae,aN as S,aW as re,aB as P,aS as E,cs as te,aR as D,e6 as oe,e0 as le,F as ie,dJ as se,ds as M,dc as F,l as ne,x as de,de as ce,ct as pe,aV as me,dP as fe,bI as ue,dp as xe,bF as B}from"./app-v2-BtC_jOn5.js";import{C as he}from"./part-v2-CP6rGyNA.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const _e=X("EllipsisIcon",[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]]);function ge(e){const{fontWeight:s,textColor1:o,textColor2:r,textColorDisabled:d,dividerColor:a,fontSize:n}=e;return{titleFontSize:n,titleFontWeight:s,dividerColor:a,titleTextColor:o,titleTextColorDisabled:d,fontSize:n,textColor:r,arrowColor:r,arrowColorDisabled:d,itemMargin:"16px 0 0 0",titlePadding:"16px 0 0 0"}}const ve={common:H,self:ge};var Ce=x("collapse","width: 100%;",[x("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[g("disabled",[i("header","cursor: not-allowed;",[i("header-main",`
 color: var(--n-title-text-color-disabled);
 `),x("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),x("collapse-item","margin-left: 32px;"),k("&:first-child","margin-top: 0;"),k("&:first-child >",[i("header","padding-top: 0;")]),g("left-arrow-placement",[i("header",[x("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[i("header",[x("collapse-item-arrow","margin-left: 4px;")])]),i("content-wrapper",[i("content-inner","padding-top: 16px;"),J({duration:"0.15s"})]),g("active",[i("header",[g("active",[x("collapse-item-arrow","transform: rotate(90deg);")])])]),k("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),Y("disabled",[g("trigger-area-main",[i("header",[i("header-main","cursor: pointer;"),x("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[i("header",[x("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[i("header",[i("header-extra","cursor: pointer;")])])]),i("header",`
 font-size: var(--n-title-font-size);
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 transition: color .3s var(--n-bezier);
 position: relative;
 padding: var(--n-title-padding);
 color: var(--n-title-text-color);
 `,[i("header-main",`
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 font-weight: var(--n-title-font-weight);
 transition: color .3s var(--n-bezier);
 flex: 1;
 color: var(--n-title-text-color);
 `),i("header-extra",`
 display: flex;
 align-items: center;
 transition: color .3s var(--n-bezier);
 color: var(--n-text-color);
 `),x("collapse-item-arrow",`
 display: flex;
 transition:
 transform .15s var(--n-bezier),
 color .3s var(--n-bezier);
 font-size: 18px;
 color: var(--n-arrow-color);
 `)])])]);const we={...U.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=re("n-collapse");var Ie=A({name:"Collapse",props:we,slots:Object,setup(e,{slots:s}){const{mergedClsPrefixRef:o,inlineThemeDisabled:r,mergedRtlRef:d}=W(e),a=Q(e.defaultExpandedNames),n=S(()=>e.expandedNames),p=Z(n,a),C=U("Collapse","-collapse",Ce,ve,e,o);function m(f){const{"onUpdate:expandedNames":l,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&P(u,f),l&&P(l,f),N&&P(N,f),a.value=f}function t(f){const{onItemHeaderClick:l}=e;l&&P(l,f)}function c(f,l,u){const{accordion:N}=e,{value:_}=p;if(N)f?(m([l]),t({name:l,expanded:!0,event:u})):(m([]),t({name:l,expanded:!1,event:u}));else if(!Array.isArray(_))m([l]),t({name:l,expanded:!0,event:u});else{const w=_.slice(),I=w.findIndex($=>l===$);~I?(w.splice(I,1),m(w),t({name:l,expanded:!1,event:u})):(w.push(l),m(w),t({name:l,expanded:!0,event:u}))}}ee(j,{props:e,mergedClsPrefixRef:o,expandedNamesRef:p,slots:s,toggleItem:c});const R=V("Collapse",d,o),T=S(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:l,dividerColor:u,titlePadding:N,titleTextColor:_,titleTextColorDisabled:w,textColor:I,arrowColor:$,fontSize:q,titleFontSize:L,arrowColorDisabled:K,itemMargin:O}}=C.value;return{"--n-font-size":q,"--n-bezier":f,"--n-text-color":I,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":L,"--n-title-text-color":_,"--n-title-text-color-disabled":w,"--n-title-font-weight":l,"--n-arrow-color":$,"--n-arrow-color-disabled":K,"--n-item-margin":O}}),b=r?ae("collapse",void 0,T,e):void 0;return{rtlEnabled:R,mergedTheme:C,mergedClsPrefix:o,cssVars:r?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),h(),z("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:G(this.cssVars)},[y(()=>{var s,o;return(o=(s=this.$slots).default)==null?void 0:o.call(s)})],6)}}),ye=A({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(M(e,"show"))}},render(){return h(),E(ie,null,{_:1,default:te(()=>{const{show:e,displayDirective:s,onceTrue:o,clsPrefix:r}=this,d=s==="show"&&o,a=(h(),z("div",{class:v(`${r}-collapse-item__content-wrapper`)},[D("div",{class:v(`${r}-collapse-item__content-inner`)},[y(()=>{var n,p;return(p=(n=this.$slots).default)==null?void 0:p.call(n)})],2)],2));return d?oe(a,[[le,e]]):e?a:null})})}});const be=["onClick"],Ne=["onClick"],Ee={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Pe=A({name:"CollapseItem",props:Ee,setup(e){const{mergedRtlRef:s}=W(e),o=me(),r=fe(()=>e.name??o),d=ue(j);d||xe("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:n,mergedClsPrefixRef:p,slots:C}=d,m=S(()=>{const{value:t}=a;if(Array.isArray(t)){const{value:c}=r;return!~t.findIndex(R=>R===c)}else if(t){const{value:c}=r;return c!==t}return!0});return{rtlEnabled:V("Collapse",s,p),collapseSlots:C,randomName:o,mergedClsPrefix:p,collapsed:m,triggerAreas:M(n,"triggerAreas"),mergedDisplayDirective:S(()=>{const{displayDirective:t}=e;return t||n.displayDirective}),arrowPlacement:S(()=>n.arrowPlacement),handleClick(t){let c="main";B(t,"arrow")&&(c="arrow"),B(t,"extra")&&(c="extra"),n.triggerAreas.includes(c)&&d&&!e.disabled&&d.toggleItem(m.value,r.value,t)}}},render(){const{collapseSlots:e,$slots:s,arrowPlacement:o,collapsed:r,mergedDisplayDirective:d,mergedClsPrefix:a,disabled:n,triggerAreas:p}=this,C=F(s.header,{collapsed:r},()=>[this.title]),m=s["header-extra"]||e["header-extra"],t=s.arrow||e.arrow;return h(),z("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${o}-arrow-placement`,n&&`${a}-collapse-item--disabled`,!r&&`${a}-collapse-item--active`,p.map(c=>`${a}-collapse-item--trigger-area-${c}`)])},[D("div",{class:v([`${a}-collapse-item__header`,!r&&`${a}-collapse-item__header--active`])},[D("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[y(()=>o==="right"&&C),(h(),z("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[y(()=>F(t,{collapsed:r},()=>[(h(),E(de,{clsPrefix:a},{default:()=>this.rtlEnabled?(h(),E(he,{key:1})):(h(),E(ne,{key:2}))},1032,["clsPrefix"]))]))],2)),y(()=>o==="left"&&C)],10,Ne),y(()=>ce(m,{collapsed:r},c=>(h(),z("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[y(()=>c)],10,be))))],2),(h(),E(ye,{clsPrefix:a,displayDirective:d,show:!r},pe(s),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Pe as C,_e as E,Ie as a};
