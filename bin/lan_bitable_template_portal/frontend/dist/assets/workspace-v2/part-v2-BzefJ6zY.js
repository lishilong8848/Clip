import{b9 as K,aI as h,aK as g,aJ as s,aG as $,bV as J,aL as O,bq as D,eU as M,dp as x,b4 as E,d9 as C,d8 as Q,d2 as v,eB as U,dG as Y,eP as Z,aY as X,dA as ee,eQ as V,eV as ae,aZ as I,b7 as re,aN as S,b2 as _,d6 as te,b1 as A,fg as oe,e_ as le,F as se,eG as ie,ek as L,d_ as B,m as ne,J as de,e0 as ce,d7 as pe,b5 as me,eN as fe,cd as ue,ef as he,ca as F}from"./app-v2-BD_Wzjxu.js";import{C as xe}from"./part-v2-impxv0AH.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ee=K("HistoryIcon",[["path",{d:"M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8",key:"1357e3"}],["path",{d:"M3 3v5h5",key:"1xhq8a"}],["path",{d:"M12 7v5l4 2",key:"1fdv2h"}]]);var ge=h("collapse","width: 100%;",[h("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[g("disabled",[s("header","cursor: not-allowed;",[s("header-main",`
 color: var(--n-title-text-color-disabled);
 `),h("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),h("collapse-item","margin-left: 32px;"),$("&:first-child","margin-top: 0;"),$("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),J({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),$("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),O("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `),h("collapse-item-arrow",`
 display: flex;
 transition:
 transform .15s var(--n-bezier),
 color .3s var(--n-bezier);
 font-size: 18px;
 color: var(--n-arrow-color);
 `)])])]);const ve={...M.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=re("n-collapse");var Ie=D({name:"Collapse",props:ve,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:o,inlineThemeDisabled:l,mergedRtlRef:c}=U(e),a=Y(e.defaultExpandedNames),d=I(()=>e.expandedNames),p=Z(d,a),w=M("Collapse","-collapse",ge,X,e,o);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&S(u,f),t&&S(t,f),N&&S(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&S(t,f)}function n(f,t,u){const{accordion:N}=e,{value:P}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(P))m([t]),r({name:t,expanded:!0,event:u});else{const y=P.slice(),z=y.findIndex(k=>t===k);~z?(y.splice(z,1),m(y),r({name:t,expanded:!1,event:u})):(y.push(t),m(y),r({name:t,expanded:!0,event:u}))}}ee(j,{props:e,mergedClsPrefixRef:o,expandedNamesRef:p,slots:i,toggleItem:n});const R=V("Collapse",c,o),T=I(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:P,titleTextColorDisabled:y,textColor:z,arrowColor:k,fontSize:q,titleFontSize:H,arrowColorDisabled:W,itemMargin:G}}=w.value;return{"--n-font-size":q,"--n-bezier":f,"--n-text-color":z,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":H,"--n-title-text-color":P,"--n-title-text-color-disabled":y,"--n-title-font-weight":t,"--n-arrow-color":k,"--n-arrow-color-disabled":W,"--n-item-margin":G}}),b=l?ae("collapse",void 0,T,e):void 0;return{rtlEnabled:R,mergedTheme:w,mergedClsPrefix:o,cssVars:l?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:Q(this.cssVars)},[C(()=>{var i,o;return(o=(i=this.$slots).default)==null?void 0:o.call(i)})],6)}}),we=D({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:ie(L(e,"show"))}},render(){return x(),_(se,null,{_:1,default:te(()=>{const{show:e,displayDirective:i,onceTrue:o,clsPrefix:l}=this,c=i==="show"&&o,a=(x(),E("div",{class:v(`${l}-collapse-item__content-wrapper`)},[A("div",{class:v(`${l}-collapse-item__content-inner`)},[C(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?oe(a,[[le,e]]):e?a:null})})}});const ye=["onClick"],Ce=["onClick"],be={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Pe=D({name:"CollapseItem",props:be,setup(e){const{mergedRtlRef:i}=U(e),o=me(),l=fe(()=>e.name??o),c=ue(j);c||he("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=I(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=l;return!~r.findIndex(R=>R===n)}else if(r){const{value:n}=l;return n!==r}return!0});return{rtlEnabled:V("Collapse",i,p),collapseSlots:w,randomName:o,mergedClsPrefix:p,collapsed:m,triggerAreas:L(d,"triggerAreas"),mergedDisplayDirective:I(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:I(()=>d.arrowPlacement),handleClick(r){let n="main";F(r,"arrow")&&(n="arrow"),F(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,l.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:o,collapsed:l,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:l},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${o}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!l&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[A("div",{class:v([`${a}-collapse-item__header`,!l&&`${a}-collapse-item__header--active`])},[A("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[C(()=>o==="right"&&w),(x(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[C(()=>B(r,{collapsed:l},()=>[(x(),_(de,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(xe,{key:1})):(x(),_(ne,{key:2}))},1032,["clsPrefix"]))]))],2)),C(()=>o==="left"&&w)],10,Ce),C(()=>ce(m,{collapsed:l},n=>(x(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[C(()=>n)],10,ye))))],2),(x(),_(we,{clsPrefix:a,displayDirective:c,show:!l},pe(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Pe as C,Ee as H,Ie as a};
