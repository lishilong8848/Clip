import{b6 as O,av as x,ax as g,aw as i,at as $,bf as Q,ay as G,b4 as A,dw as M,ct as h,aP as E,cf as y,ce as J,c8 as v,dh as U,cE as X,dr as Y,cz as Z,ds as V,dx as ee,aI as _,aR as ae,az as I,aN as z,cc as re,aM as D,dH as te,dC as oe,F as le,di as ie,d6 as W,cT as F,k as se,u as ne,cV as de,cd as ce,aQ as pe,dp as me,bx as fe,d2 as ue,bu as B,aT as xe}from"./app-CFgK42zr.js";import{C as he}from"./part-ZqyeSAak.js";function ge(e){const{fontWeight:s,textColor1:o,textColor2:r,textColorDisabled:d,dividerColor:a,fontSize:n}=e;return{titleFontSize:n,titleFontWeight:s,dividerColor:a,titleTextColor:o,titleTextColorDisabled:d,fontSize:n,textColor:r,arrowColor:r,arrowColorDisabled:d,itemMargin:"16px 0 0 0",titlePadding:"16px 0 0 0"}}const ve={common:O,self:ge};var we=x("collapse","width: 100%;",[x("collapse-item",`
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
 `)])]),x("collapse-item","margin-left: 32px;"),$("&:first-child","margin-top: 0;"),$("&:first-child >",[i("header","padding-top: 0;")]),g("left-arrow-placement",[i("header",[x("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[i("header",[x("collapse-item-arrow","margin-left: 4px;")])]),i("content-wrapper",[i("content-inner","padding-top: 16px;"),Q({duration:"0.15s"})]),g("active",[i("header",[g("active",[x("collapse-item-arrow","transform: rotate(90deg);")])])]),$("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),G("disabled",[g("trigger-area-main",[i("header",[i("header-main","cursor: pointer;"),x("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[i("header",[x("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[i("header",[i("header-extra","cursor: pointer;")])])]),i("header",`
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
 `)])])]);const Ce={...M.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=ae("n-collapse");var Pe=A({name:"Collapse",props:Ce,slots:Object,setup(e,{slots:s}){const{mergedClsPrefixRef:o,inlineThemeDisabled:r,mergedRtlRef:d}=U(e),a=X(e.defaultExpandedNames),n=_(()=>e.expandedNames),p=Y(n,a),w=M("Collapse","-collapse",we,ve,e,o);function m(f){const{"onUpdate:expandedNames":l,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&I(u,f),l&&I(l,f),N&&I(N,f),a.value=f}function t(f){const{onItemHeaderClick:l}=e;l&&I(l,f)}function c(f,l,u){const{accordion:N}=e,{value:P}=p;if(N)f?(m([l]),t({name:l,expanded:!0,event:u})):(m([]),t({name:l,expanded:!1,event:u}));else if(!Array.isArray(P))m([l]),t({name:l,expanded:!0,event:u});else{const C=P.slice(),S=C.findIndex(k=>l===k);~S?(C.splice(S,1),m(C),t({name:l,expanded:!1,event:u})):(C.push(l),m(C),t({name:l,expanded:!0,event:u}))}}Z(j,{props:e,mergedClsPrefixRef:o,expandedNamesRef:p,slots:s,toggleItem:c});const R=V("Collapse",d,o),T=_(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:l,dividerColor:u,titlePadding:N,titleTextColor:P,titleTextColorDisabled:C,textColor:S,arrowColor:k,fontSize:L,titleFontSize:q,arrowColorDisabled:H,itemMargin:K}}=w.value;return{"--n-font-size":L,"--n-bezier":f,"--n-text-color":S,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":q,"--n-title-text-color":P,"--n-title-text-color-disabled":C,"--n-title-font-weight":l,"--n-arrow-color":k,"--n-arrow-color-disabled":H,"--n-item-margin":K}}),b=r?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:R,mergedTheme:w,mergedClsPrefix:o,cssVars:r?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),h(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:J(this.cssVars)},[y(()=>{var s,o;return(o=(s=this.$slots).default)==null?void 0:o.call(s)})],6)}}),ye=A({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:ie(W(e,"show"))}},render(){return h(),z(le,null,{_:1,default:re(()=>{const{show:e,displayDirective:s,onceTrue:o,clsPrefix:r}=this,d=s==="show"&&o,a=(h(),E("div",{class:v(`${r}-collapse-item__content-wrapper`)},[D("div",{class:v(`${r}-collapse-item__content-inner`)},[y(()=>{var n,p;return(p=(n=this.$slots).default)==null?void 0:p.call(n)})],2)],2));return d?te(a,[[oe,e]]):e?a:null})})}});const be=["onClick"],Ne=["onClick"],ze={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Se=A({name:"CollapseItem",props:ze,setup(e){const{mergedRtlRef:s}=U(e),o=pe(),r=me(()=>e.name??o),d=fe(j);d||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:n,mergedClsPrefixRef:p,slots:w}=d,m=_(()=>{const{value:t}=a;if(Array.isArray(t)){const{value:c}=r;return!~t.findIndex(R=>R===c)}else if(t){const{value:c}=r;return c!==t}return!0});return{rtlEnabled:V("Collapse",s,p),collapseSlots:w,randomName:o,mergedClsPrefix:p,collapsed:m,triggerAreas:W(n,"triggerAreas"),mergedDisplayDirective:_(()=>{const{displayDirective:t}=e;return t||n.displayDirective}),arrowPlacement:_(()=>n.arrowPlacement),handleClick(t){let c="main";B(t,"arrow")&&(c="arrow"),B(t,"extra")&&(c="extra"),n.triggerAreas.includes(c)&&d&&!e.disabled&&d.toggleItem(m.value,r.value,t)}}},render(){const{collapseSlots:e,$slots:s,arrowPlacement:o,collapsed:r,mergedDisplayDirective:d,mergedClsPrefix:a,disabled:n,triggerAreas:p}=this,w=F(s.header,{collapsed:r},()=>[this.title]),m=s["header-extra"]||e["header-extra"],t=s.arrow||e.arrow;return h(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${o}-arrow-placement`,n&&`${a}-collapse-item--disabled`,!r&&`${a}-collapse-item--active`,p.map(c=>`${a}-collapse-item--trigger-area-${c}`)])},[D("div",{class:v([`${a}-collapse-item__header`,!r&&`${a}-collapse-item__header--active`])},[D("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[y(()=>o==="right"&&w),(h(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[y(()=>F(t,{collapsed:r},()=>[(h(),z(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(h(),z(he,{key:1})):(h(),z(se,{key:2}))},1032,["clsPrefix"]))]))],2)),y(()=>o==="left"&&w)],10,Ne),y(()=>de(m,{collapsed:r},c=>(h(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[y(()=>c)],10,be))))],2),(h(),z(ye,{clsPrefix:a,displayDirective:d,show:!r},ce(s),1032,["clsPrefix","displayDirective","show"]))],2)}});/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ie=xe("EllipsisIcon",[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]]);export{Se as C,Ie as E,Pe as a};
