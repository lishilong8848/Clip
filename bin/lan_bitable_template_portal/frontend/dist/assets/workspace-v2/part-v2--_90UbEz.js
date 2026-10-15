import{a_ as X,az as x,aB as g,aA as s,ax as $,bu as Y,aC as H,bg as D,e2 as U,cN as h,aW as E,cA as C,cz as Z,cu as v,dM as M,d3 as G,dZ as J,aO as Q,cY as ee,d_ as V,e3 as ae,aP as z,aY as re,aD as R,aU as _,cx as te,aT as A,ee as le,e8 as oe,F as se,dR as ie,dB as j,dk as B,l as ne,x as de,dm as ce,cy as pe,aX as me,dX as fe,bN as ue,dx as xe,bK as F}from"./app-v2-dLiaPnr4.js";import{C as he}from"./part-v2-BvNGK7wP.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ee=X("EllipsisIcon",[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]]);var ge=x("collapse","width: 100%;",[x("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[g("disabled",[s("header","cursor: not-allowed;",[s("header-main",`
 color: var(--n-title-text-color-disabled);
 `),x("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),x("collapse-item","margin-left: 32px;"),$("&:first-child","margin-top: 0;"),$("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[x("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[x("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),Y({duration:"0.15s"})]),g("active",[s("header",[g("active",[x("collapse-item-arrow","transform: rotate(90deg);")])])]),$("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),H("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),x("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[x("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
 font-size: var(--n-title-font-size);
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 transition: color .3s var(--n-bezier);
 position: relative;
 padding: var(--n-title-padding);
 color: var(--n-title-text-color);
 `,[s("header-main",`
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 font-weight: var(--n-title-font-weight);
 transition: color .3s var(--n-bezier);
 flex: 1;
 color: var(--n-title-text-color);
 `),s("header-extra",`
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
 `)])])]);const ve={...U.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},W=re("n-collapse");var ze=D({name:"Collapse",props:ve,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=M(e),a=G(e.defaultExpandedNames),d=z(()=>e.expandedNames),p=J(d,a),w=U("Collapse","-collapse",ge,Q,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),t&&R(t,f),N&&R(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&R(t,f)}function n(f,t,u){const{accordion:N}=e,{value:P}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(P))m([t]),r({name:t,expanded:!0,event:u});else{const y=P.slice(),I=y.findIndex(k=>t===k);~I?(y.splice(I,1),m(y),r({name:t,expanded:!1,event:u})):(y.push(t),m(y),r({name:t,expanded:!0,event:u}))}}ee(W,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const S=V("Collapse",c,l),T=z(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:P,titleTextColorDisabled:y,textColor:I,arrowColor:k,fontSize:K,titleFontSize:L,arrowColorDisabled:O,itemMargin:q}}=w.value;return{"--n-font-size":K,"--n-bezier":f,"--n-text-color":I,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":L,"--n-title-text-color":P,"--n-title-text-color-disabled":y,"--n-title-font-weight":t,"--n-arrow-color":k,"--n-arrow-color-disabled":O,"--n-item-margin":q}}),b=o?ae("collapse",void 0,T,e):void 0;return{rtlEnabled:S,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),h(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:Z(this.cssVars)},[C(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),we=D({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:ie(j(e,"show"))}},render(){return h(),_(se,null,{_:1,default:te(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(h(),E("div",{class:v(`${o}-collapse-item__content-wrapper`)},[A("div",{class:v(`${o}-collapse-item__content-inner`)},[C(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?le(a,[[oe,e]]):e?a:null})})}});const ye=["onClick"],Ce=["onClick"],be={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Pe=D({name:"CollapseItem",props:be,setup(e){const{mergedRtlRef:i}=M(e),l=me(),o=fe(()=>e.name??l),c=ue(W);c||xe("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=z(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(S=>S===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:V("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:j(d,"triggerAreas"),mergedDisplayDirective:z(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:z(()=>d.arrowPlacement),handleClick(r){let n="main";F(r,"arrow")&&(n="arrow"),F(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return h(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[A("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[A("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[C(()=>l==="right"&&w),(h(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[C(()=>B(r,{collapsed:o},()=>[(h(),_(de,{clsPrefix:a},{default:()=>this.rtlEnabled?(h(),_(he,{key:1})):(h(),_(ne,{key:2}))},1032,["clsPrefix"]))]))],2)),C(()=>l==="left"&&w)],10,Ce),C(()=>ce(m,{collapsed:o},n=>(h(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[C(()=>n)],10,ye))))],2),(h(),_(we,{clsPrefix:a,displayDirective:c,show:!o},pe(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Pe as C,Ee as E,ze as a};
